from __future__ import annotations

import json
import os
from typing import Any

import httpx
from structlog import get_logger

from fraud_detection.core.utils import (
    build_model_inputs,
    enrich_current_transaction_with_redis_features,
    MODEL_TO_CHANNEL,
    parse_datetime,
    to_float,
)
from fraud_detection.features.feature_store import RedisFeatureStore
from database.postgres import PostgresDatabase
from fraud_detection.core.models import (
    FraudDetectionInputs,
    FraudDetectionOutputs,
)

logger = get_logger(__name__)

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

    async def predict_with_kserve(self, model_inputs: Any) -> float:
        rows = model_inputs.to_dict(orient="records")
        feature_names = list(model_inputs.columns)
        data = [
            [to_float(row.get(feature), feature) for feature in feature_names]
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

            output_data = outputs[0].get("data") if isinstance(outputs[0], dict) else None
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

    async def predict(self, inputs: FraudDetectionInputs) -> FraudDetectionOutputs:
        current = inputs.model_dump(by_alias=True)
        user_id = current.get("user_id", None)
        card_id = current.get("card_id", None)

        if user_id is None or card_id is None:
            raise ValueError("current_transaction must include user_id and card_id")

        redis_state = await self.feature_store.get_txs(user_id, card_id)
        if not redis_state.get("features") or not redis_state.get("transactions"):
            logger.warning(
                "No Redis features or transactions found for user-card pair",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                },
            )

        try:
            current = enrich_current_transaction_with_redis_features(current, redis_state)
            redis_transaction = current
            payload = {
                **redis_state,
                "current_transaction": current,
            }
            model_inputs, current, _, _, _ = build_model_inputs(
                payload,
                self.schema,
            )
        except Exception as exc:
            raise RuntimeError("Failed to build model inputs for prediction") from exc

        probability = await self.predict_with_kserve(model_inputs)

        status = "review" if probability >= self.threshold else "approved"
        logger.info(
            "Finished fraud prediction",
            extra={
                "transaction_id": current.get("tx_id"),
                "probability": probability,
                "status": status,
            },
        )

        record_to_save = {
            **redis_transaction,
            "status": status,
        }

        await self.save_transaction(record_to_save)
        await self.feature_store.refresh_features_for_user_card(
            user_id,
            card_id,
            redis_transaction,
        )

        return FraudDetectionOutputs(
            transaction_id=current.get("tx_id"),
            probability=probability,
            status=status,
        )

    async def save_transaction(self, transaction: dict[str, Any]) -> None:
        try:
            await self.database.execute(
                """
                INSERT INTO application.transactions
                    (
                        id,
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
                        screen_resolution,
                        created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14, $15, $16
                )
                """,
                (
                    transaction["transaction_id"],
                    transaction["user_id"],
                    transaction["card_id"],
                    transaction["status"],
                    transaction["amount"],
                    MODEL_TO_CHANNEL.get(transaction["channel"], transaction["channel"]),
                    transaction["billing_zone"],
                    transaction["billing_country"],
                    transaction["email_purchaser"],
                    transaction["email_recipient"],
                    transaction["device_info"],
                    transaction["device_type"],
                    transaction["os_raw"],
                    transaction["browser_raw"],
                    transaction["screen_resolution"],
                    parse_datetime(transaction["event_timestamp"]),
                ),
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
