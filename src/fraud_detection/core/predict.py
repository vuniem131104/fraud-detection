from __future__ import annotations

import asyncio
import json
import math
import os
from dotenv import load_dotenv
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import httpx
import pandas as pd
from redis import asyncio as aioredis
from structlog import get_logger

from fraud_detection.core.utils import (
    build_fake_current_transaction,
    build_model_inputs,
    default_model_dir,
    enrich_current_transaction_with_redis_features,
    first_value,
    redis_connection_settings,
)
from fraud_detection.features.feature_store import RedisFeatureStore
from database.postgres import PostgresDatabase
from fraud_detection.core.models import (
    Transaction,
)

load_dotenv()

logger = get_logger(__name__)


@dataclass(frozen=True)
class PredictionDetails:
    transaction_id: int | str | None
    probability: float
    is_fraud: bool
    cold_start: bool
    history_rows: int
    matched_card_history_rows: int
    matched_uid2_history_rows: int
    cold_lookups: int
    missing_columns: int
    redis_ms: float
    feature_ms: float
    predict_ms: float
    elapsed_ms: float


class FraudDetectionService:
    def __init__(
        self,
        schema: dict[str, Any],
        feature_store: RedisFeatureStore,
        database: PostgresDatabase,
        threshold: float = 0.5,
    ) -> None:
        self.schema = schema
        self.feature_store = feature_store
        self.database = database
        self.kserve_url = os.getenv("KSERVE_URL", "")
        if not self.kserve_url:
            logger.error("KSERVE_URL environment variable is not set")
            raise ValueError("KSERVE_URL environment variable is not set")
        self.threshold = threshold
        
    @staticmethod
    def to_float(value: Any, feature: str, default: float = 0.0) -> float:
        if value is None or pd.isna(value):
            return default
        if isinstance(value, str) and not value.strip():
            return default
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "Invalid numeric feature value",
                extra={
                    "feature": feature,
                    "value": repr(value),
                },
            )
            raise ValueError(f"Feature {feature!r} must be numeric, got {value!r}") from exc
        return number if math.isfinite(number) else default

    async def predict_with_kserve(self, model_inputs: Any) -> float:
        rows = model_inputs.to_dict(orient="records")
        feature_names = list(model_inputs.columns)
        data = [
            [self.to_float(row.get(feature), feature) for feature in feature_names]
            for row in rows
        ]
        payload = {
            "inputs": [
                {
                    "name": "input-0",
                    "shape": [len(data), len(feature_names)],
                    "datatype": "FP32",
                    "data": data,
                }
            ]
        }

        try:
            logger.info(
                "Sending inference request to KServe",
                extra={
                    "kserve_url": self.kserve_url,
                    "row_count": len(data),
                    "feature_count": len(feature_names),
                },
            )
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(self.kserve_url, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            logger.exception(
                "KServe inference returned unsuccessful status",
                extra={
                    "kserve_url": self.kserve_url,
                    "status_code": exc.response.status_code,
                    "response_text": exc.response.text,
                },
            )
            raise RuntimeError(
                f"KServe inference failed with status {exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.TimeoutException as exc:
            logger.exception(
                "KServe inference request timed out",
                extra={
                    "kserve_url": self.kserve_url,
                    "timeout_seconds": 30.0,
                },
            )
            raise RuntimeError("KServe inference request timed out") from exc
        except httpx.RequestError as exc:
            logger.exception(
                "KServe inference request failed",
                extra={
                    "kserve_url": self.kserve_url,
                    "error": str(exc),
                },
            )
            raise RuntimeError(f"KServe inference request failed: {exc}") from exc

        try:
            result = response.json()
            outputs = result.get("outputs")
            if not outputs or not isinstance(outputs, list):
                raise ValueError("missing outputs")

            output_data = outputs[0].get("data") if isinstance(outputs[0], Mapping) else None
            if not output_data:
                raise ValueError("missing output data")

            probability = float(output_data[0])
        except (json.JSONDecodeError, TypeError, ValueError, IndexError, AttributeError) as exc:
            logger.exception(
                "KServe inference response is invalid",
                extra={
                    "kserve_url": self.kserve_url,
                    "status_code": response.status_code,
                    "response_text": response.text,
                },
            )
            raise RuntimeError("KServe inference response is invalid") from exc

        logger.info(
            "Received KServe prediction",
            extra={
                "kserve_url": self.kserve_url,
                "probability": probability,
            },
        )
        return probability

    async def predict_async(self, current_transaction: Transaction | Mapping[str, Any]) -> PredictionDetails:
        if self.feature_store is None:
            logger.error("FraudDetectionService initialized without feature store")
            raise RuntimeError("FraudDetectionService was initialized without redis_client")

        started_at = time.perf_counter()
        try:
            current = (
                current_transaction.model_dump(by_alias=True)
                if isinstance(current_transaction, Transaction)
                else dict(current_transaction)
            )
        except Exception as exc:
            logger.exception("Failed to normalize current transaction")
            raise ValueError("current_transaction must be a Transaction or mapping") from exc

        user_id = first_value(current, "user_id")
        card_id = first_value(current, "card_id", "card1")
        if user_id is None or card_id is None:
            logger.warning(
                "Prediction request missing required identifiers",
                extra={
                    "has_user_id": user_id is not None,
                    "has_card_id": card_id is not None,
                },
            )
            raise ValueError("current_transaction must include user_id and card_id")

        user_id_str = str(user_id)
        card_id_str = str(card_id)
        logger.info(
            "Starting fraud prediction",
            extra={
                "user_id": user_id_str,
                "card_id": card_id_str,
            },
        )

        redis_started_at = time.perf_counter()
        try:
            redis_state = await self.feature_store.get_txs(user_id_str, card_id_str)
        except Exception as exc:
            redis_ms = round((time.perf_counter() - redis_started_at) * 1000, 2)
            logger.exception(
                "Failed to load Redis features for prediction",
                extra={
                    "user_id": user_id_str,
                    "card_id": card_id_str,
                    "redis_ms": redis_ms,
                },
            )
            raise RuntimeError("Failed to load Redis features for prediction") from exc

        redis_ms = round((time.perf_counter() - redis_started_at) * 1000, 2)
        if not redis_state.get("features"):
            logger.warning(
                "No Redis features found for prediction",
                extra={
                    "user_id": user_id_str,
                    "card_id": card_id_str,
                    "redis_ms": redis_ms,
                },
            )
            raise RuntimeError(
                f"No Redis features found for user_id={user_id}, card_id={card_id}. "
                "Check Redis connection or load features first."
            )
        logger.info(
            "Loaded Redis features for prediction",
            extra={
                "user_id": user_id_str,
                "card_id": card_id_str,
                "feature_count": len(redis_state.get("features", {})),
                "transaction_count": len(redis_state.get("transactions", [])),
                "redis_ms": redis_ms,
            },
        )

        feature_started_at = time.perf_counter()
        try:
            current = enrich_current_transaction_with_redis_features(current, redis_state)
            payload = {
                **redis_state,
                "current_transaction": current,
            }
            model_inputs, current, history_stats, cold_lookups, missing_columns = build_model_inputs(
                payload,
                self.schema,
            )
            model_inputs.to_json("model_inputs.json", orient="records", lines=True)
        except Exception as exc:
            feature_ms = round((time.perf_counter() - feature_started_at) * 1000, 2)
            logger.exception(
                "Failed to build model inputs for prediction",
                extra={
                    "user_id": user_id_str,
                    "card_id": card_id_str,
                    "feature_ms": feature_ms,
                },
            )
            raise RuntimeError("Failed to build model inputs for prediction") from exc

        feature_ms = round((time.perf_counter() - feature_started_at) * 1000, 2)
        logger.info(
            "Built model inputs for prediction",
            extra={
                "user_id": user_id_str,
                "card_id": card_id_str,
                "row_count": len(model_inputs),
                "feature_count": len(model_inputs.columns),
                "cold_lookups": cold_lookups,
                "missing_columns": len(missing_columns),
                "feature_ms": feature_ms,
            },
        )

        predict_started_at = time.perf_counter()
        try:
            probability = await self.predict_with_kserve(model_inputs)
        except Exception:
            predict_ms = round((time.perf_counter() - predict_started_at) * 1000, 2)
            logger.exception(
                "Failed to generate fraud prediction",
                extra={
                    "user_id": user_id_str,
                    "card_id": card_id_str,
                    "predict_ms": predict_ms,
                },
            )
            raise

        predict_ms = round((time.perf_counter() - predict_started_at) * 1000, 2)
        try:
            await self.feature_store.refresh_features_for_user_card(user_id_str, card_id_str)
        except Exception:
            logger.exception(
                "Failed to refresh Redis features after prediction",
                extra={
                    "user_id": user_id_str,
                    "card_id": card_id_str,
                },
            )
            raise

        cold_start = not (
            history_stats["matched_uid2_rows"] or history_stats["matched_card_rows"]
        )
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        logger.info(
            "Finished fraud prediction",
            extra={
                "user_id": user_id_str,
                "card_id": card_id_str,
                "transaction_id": current.get("tx_id"),
                "probability": probability,
                "is_fraud": probability >= self.threshold,
                "cold_start": cold_start,
                "elapsed_ms": elapsed_ms,
            },
        )

        return PredictionDetails(
            transaction_id=current.get("tx_id"),
            probability=probability,
            is_fraud=probability >= self.threshold,
            cold_start=cold_start,
            history_rows=history_stats["history_rows"],
            matched_card_history_rows=history_stats["matched_card_rows"],
            matched_uid2_history_rows=history_stats["matched_uid2_rows"],
            cold_lookups=cold_lookups,
            missing_columns=len(missing_columns),
            redis_ms=redis_ms,
            feature_ms=feature_ms,
            predict_ms=predict_ms,
            elapsed_ms=elapsed_ms,
        )
        
    async def save_transaction(self, transaction: dict[str, Any]) -> None:
        try:
            async with self.database.transaction() as conn:
                _ = await conn.fetchrow(
                    """
                    INSERT INTO application.transactions
                        (
                            user_id,
                            card_id,
                            status,
                            amount_usd,
                            channel,
                            billing_zone,
                            billing_country,
                            email_purchaser,
                            email_recipient,
                            device_info,
                            device_type,
                            os_raw,
                            browser_raw,
                            screen_resolution
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7,
                        $8, $9, $10, $11, $12, $13, $14
                    )
                    RETURNING *
                    """,
                    transaction["user_id"],
                    transaction["card_id"],
                    transaction["status"],
                    transaction["amount_usd"],
                    transaction["channel"],
                    transaction["billing_zone"],
                    transaction["billing_country"],
                    transaction["email_purchaser"],
                    transaction["email_recipient"],
                    transaction["device_info"],
                    transaction["device_type"],
                    transaction["os_raw"],
                    transaction["browser_raw"],
                    transaction["screen_resolution"],
                )
        except Exception:
            logger.exception(
                "Failed to save transaction",
                extra={
                    "user_id": transaction.get("user_id"),
                    "card_id": transaction.get("card_id"),
                    "status": transaction.get("status"),
                },
            )
            raise

        logger.info(
            "Saved transaction",
            extra={
                "user_id": transaction.get("user_id"),
                "card_id": transaction.get("card_id"),
                "status": transaction.get("status"),
            },
        )


async def main_async() -> int:
    user_id = "1"
    card_id = "1"
    redis_client = aioredis.Redis(**redis_connection_settings())
    try:
        service = FraudDetectionService.from_artifacts(redis_client=redis_client)
        current_transaction = build_fake_current_transaction(user_id=user_id, card_id=card_id)
        details = await service.predict_async(current_transaction)
    except Exception:
        logger.exception(
            "Fraud prediction CLI failed",
            extra={
                "user_id": user_id,
                "card_id": card_id,
            },
        )
        raise
    finally:
        await redis_client.aclose()

    result = {
        "transaction_id": details.transaction_id,
        "user_id": user_id,
        "card_id": card_id,
        "probability": round(details.probability, 6),
        "is_fraud": details.is_fraud,
        "cold_start": details.cold_start,
        "history_rows": details.history_rows,
        "matched_card_history_rows": details.matched_card_history_rows,
        "matched_uid2_history_rows": details.matched_uid2_history_rows,
        "cold_lookups": details.cold_lookups,
        "missing_columns": details.missing_columns,
        "redis_ms": details.redis_ms,
        "feature_ms": details.feature_ms,
        "predict_ms": details.predict_ms,
        "elapsed_ms": details.elapsed_ms,
    }
    print(json.dumps(result, indent=2))
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
