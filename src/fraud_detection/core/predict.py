"""Fraud detection scoring service orchestrating end-to-end prediction.

Defines ``FraudDetectionService``, which owns the connections to Postgres,
Redis (feature store), the KServe inference endpoint and a Kafka producer. For
each transaction it fetches Redis history, builds the model feature vector,
calls KServe for a probability, persists a feature snapshot, emits the result
to Kafka and returns the structured prediction.
"""

from __future__ import annotations

import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
from typing import Any, Optional

import asyncio
import ssl
import httpx
import pandas as pd
from structlog import get_logger

from fraud_detection.core.utils import (
    build_model_inputs,
    enrich_current_transaction_with_redis_features,
    normalize_email,
)
from fraud_detection.features.feature_store import RedisFeatureStore
from database.postgres import PostgresDatabase
from fraud_detection.core.models import (
    FraudDetectionInputs,
    FraudDetectionOutputs,
)
from aiokafka import AIOKafkaProducer
from uuid import uuid4

logger = get_logger(__name__)

class FraudDetectionService:
    """Coordinates feature retrieval, model inference and result publishing.

    Holds long-lived clients (database, Redis feature store, KServe HTTP client,
    Kafka producer) and a thread pool for CPU-bound feature building. Lifecycle
    is managed via :meth:`open` and :meth:`close`.
    """

    def __init__(
        self,
        schema: dict[str, Any],
        feature_store: RedisFeatureStore,
        database: PostgresDatabase,
        threshold: float = 0.5,
    ) -> None:
        """Initialize the service and its inference client configuration.

        Reads KServe and feature-build settings from the environment and sets up
        the async HTTP client used for inference. The Kafka producer is created
        later in :meth:`open`.

        Args:
            schema: Feature schema describing how to build the model input vector.
            feature_store: Redis-backed store for per-user/card history.
            database: Postgres database used to persist feature snapshots.
            threshold: Decision threshold above which a transaction is flagged.

        Raises:
            ValueError: If the ``KSERVE_URL`` environment variable is not set.
        """
        self.schema = schema
        self.feature_store = feature_store
        self.database = database
        self.feature_build_executor = ThreadPoolExecutor(
            max_workers=int(os.getenv("FEATURE_BUILD_WORKERS", "1")),
            thread_name_prefix="fraud-feature-build",
        )
        self.kserve_url = os.getenv("KSERVE_URL", "")
        if not self.kserve_url:
            logger.error("KSERVE_URL environment variable is not set")
            raise ValueError("KSERVE_URL environment variable is not set")
        self.threshold = threshold
        self.kserve_timeout_s = float(os.getenv("KSERVE_TIMEOUT_S", "30"))
        self.kserve_client = httpx.AsyncClient(
            timeout=self.kserve_timeout_s,
            limits=httpx.Limits(
                max_connections=int(os.getenv("KSERVE_MAX_CONNECTIONS", "100")),
                max_keepalive_connections=int(
                    os.getenv("KSERVE_MAX_KEEPALIVE_CONNECTIONS", "100")
                ),
            ),
        )
        self.producer: Optional[AIOKafkaProducer] = None
        self.predictions_topic = os.getenv("PREDICTIONS_TOPIC")

    async def open(self) -> None:
        """Open all backing resources and start the Kafka producer.

        Opens the database, pings Redis, enters the KServe HTTP client context
        and starts an SSL-secured Kafka producer using environment-provided
        bootstrap servers and certificates.
        """
        await self.database.open()
        await self.feature_store.redis_client.ping()
        await self.kserve_client.__aenter__()
        ssl_context = ssl.create_default_context(
            cafile=os.getenv("KAFKA_SSL_CAFILE"),
        )
        ssl_context.load_cert_chain(
            certfile=os.getenv("KAFKA_SSL_CERTFILE"),
            keyfile=os.getenv("KAFKA_SSL_KEYFILE"),
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=os.getenv("BOOTSTRAP_SERVERS"),
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
            ssl_context=ssl_context,
        )
        await self.producer.start()
        logger.info("FraudDetectionService is ready")

    async def close(self) -> None:
        """Release all backing resources.

        Closes the database, Redis client, KServe HTTP client and Kafka producer
        (if started), and shuts down the feature-build thread pool.
        """
        await self.database.close()
        await self.feature_store.redis_client.aclose()
        await self.kserve_client.aclose()
        if self.producer:
            await self.producer.stop()
        self.feature_build_executor.shutdown(wait=True, cancel_futures=True)
        logger.info("FraudDetectionService has been closed")
        
    @staticmethod
    def to_float(value: Any, feature: str, default: float = 0.0) -> float:
        """Coerce a feature value to a finite float.

        ``None``, NaN and blank strings yield ``default``; non-finite numbers
        also fall back to ``default``.

        Args:
            value: The raw feature value to convert.
            feature: Feature name, used only for logging/error messages.
            default: Value returned for missing or non-finite inputs.

        Returns:
            The parsed finite float, or ``default``.

        Raises:
            ValueError: If ``value`` is non-empty but cannot be parsed as a number.
        """
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

    async def predict_with_kserve(self, model_inputs: pd.DataFrame) -> float:
        """Send the feature vector to KServe and return the fraud probability.

        Builds a KServe v2 inference payload from ``model_inputs``, posts it to
        the configured endpoint and extracts the first output value.

        Args:
            model_inputs: Single-row (or multi-row) feature DataFrame whose
                columns map to the model's expected features.

        Returns:
            The fraud probability returned by the model.

        Raises:
            RuntimeError: If the request fails, times out, returns an error
                status, or the response cannot be parsed.
        """
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
            response = await self.kserve_client.post(self.kserve_url, json=payload)
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
                    "timeout_seconds": self.kserve_timeout_s,
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
        """Run the full fraud scoring pipeline for a single transaction.

        Fetches Redis history for the user-card pair, enriches and normalizes the
        transaction, builds the model feature vector off the event loop, calls
        KServe for a probability, persists a feature snapshot, publishes the
        result to Kafka, and returns the structured prediction. Failures to save
        the snapshot or publish to Kafka are logged but do not abort scoring.

        Args:
            inputs: The validated transaction to score.

        Returns:
            The transaction id, fraud probability and binary prediction.

        Raises:
            ValueError: If the transaction lacks ``user_id`` or ``card_id``.
            RuntimeError: If building the model inputs or KServe inference fails.
        """
        request_id = uuid4().hex
        predict_started_at = perf_counter()

        def log_time_perf(operation: str, started_at: float) -> None:
            """Log the elapsed time of ``operation`` since ``started_at`` in milliseconds."""
            logger.debug(
                "Fraud prediction operation timing",
                extra={
                    "operation": operation,
                    "time_perf": round((perf_counter() - started_at) * 1000, 3),
                    "time_unit": "milliseconds",
                },
            )

        current_transaction = inputs.model_dump(by_alias=True)
        user_id = current_transaction.get("user_id", None)
        card_id = current_transaction.get("card_id", None)

        if user_id is None or card_id is None:
            raise ValueError("current_transaction must include user_id and card_id")

        operation_started_at = perf_counter()
        redis_state = await self.feature_store.get_txs(user_id, card_id)
        log_time_perf("fetch_redis_state", operation_started_at)
        if not redis_state.get("features") or not redis_state.get("transactions"):
            logger.warning(
                "No Redis features or transactions found for user-card pair",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                },
            )

        try:
            operation_started_at = perf_counter()
            current_transaction = enrich_current_transaction_with_redis_features(current_transaction, redis_state)

            normalized_email_transaction = normalize_email(current_transaction)
            payload = {
                **redis_state,
                "current_transaction": normalized_email_transaction,
            }
            operation_started_at = perf_counter()
            model_inputs = await asyncio.get_running_loop().run_in_executor(
                self.feature_build_executor,
                build_model_inputs,
                payload,
                self.schema,
            )
            log_time_perf("build_model_inputs", operation_started_at)
        except Exception as exc:
            raise RuntimeError("Failed to build model inputs for prediction") from exc

        operation_started_at = perf_counter()
        probability = await self.predict_with_kserve(model_inputs)
        log_time_perf("kserve_inference", operation_started_at)

        prediction = 1 if probability >= self.threshold else 0
        tx_id = current_transaction.get("tx_id")

        try:
            raw_features = model_inputs.to_dict(orient="records")[0]
            sanitized_features = {
                k: (None if isinstance(v, float) and not math.isfinite(v) else v)
                for k, v in raw_features.items()
            }
            feature_snapshot = json.dumps(sanitized_features)
            await self.database.execute(
                """
                INSERT INTO application.feature_snapshots (tx_id, features)
                VALUES ($1, $2)
                """,
                (tx_id, feature_snapshot)
            )
            logger.info("Saved feature snapshot", extra={"tx_id": tx_id})
        except Exception:
            logger.exception("Failed to save feature snapshot", extra={"tx_id": tx_id})

        latency = round((perf_counter() - predict_started_at) * 1000, 3)
        logger.info(
            "Finished fraud prediction",
            extra={
                "request_id": request_id,
                "transaction_id": tx_id,
                "probability": probability,
                "prediction": prediction,
                "latency": latency,
                "latency_unit": "milliseconds",
            },
        )

        try:
            await self.producer.send_and_wait(
                self.predictions_topic,
                value=json.dumps({
                    "request_id": request_id,
                    "fraud_score": probability,
                    "prediction": prediction,
                    "latency_ms": latency,
                    "model_name": os.getenv("MODEL_NAME"),
                    "model_version": os.getenv("MODEL_VERSION"),
                    "threshold": self.threshold,
                    "current_transaction": current_transaction,
                }).encode("utf-8"),
                key=request_id.encode(),
            )
            logger.info(
                "Sent prediction result to Kafka",
                extra={
                    "request_id": request_id,
                    "transaction_id": tx_id,
                },
            )
        except Exception as e:
            logger.exception(
                "Failed to send prediction result to Kafka",
                extra={
                    "request_id": request_id,
                    "transaction_id": tx_id,
                    "error": str(e),
                },
            )

        log_time_perf("predict_total", predict_started_at)
        return FraudDetectionOutputs(
            tx_id=tx_id,
            probability=probability,
            prediction=prediction,
        )
