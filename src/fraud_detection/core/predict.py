"""Fraud detection scoring service orchestrating end-to-end prediction.

Defines ``FraudDetectionService``, which owns the connections to Postgres, the
Feast online feature store, the KServe inference endpoint and (optionally) a
Kafka producer. For each transaction it reads the precomputed feature vector for
the ``(user_id, card_id)`` pair from the online store, encodes it per the model
schema, calls KServe for a probability, persists a prediction log, emits the
result to Kafka and returns the structured prediction.
"""

from __future__ import annotations

import json
import math
import os
import ssl
from time import perf_counter
from typing import Any, Optional
from uuid import uuid4

import httpx
from aiokafka import AIOKafkaProducer
from structlog import get_logger

from database.postgres import PostgresDatabase
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.core.utils import build_model_inputs
from fraud_detection.core.feature_store import FeastFeatureStore

logger = get_logger(__name__)


class FraudDetectionService:
    """Coordinates feature retrieval, model inference and result publishing.

    Holds long-lived clients (database, Feast online feature store, KServe HTTP
    client, optional Kafka producer). Lifecycle is managed via :meth:`open` and
    :meth:`close`.
    """

    def __init__(
        self,
        schema: dict[str, Any],
        feature_store: FeastFeatureStore,
        database: PostgresDatabase,
        threshold: float = 0.5,
    ) -> None:
        """Initialize the service and its inference client configuration.

        Args:
            schema: Feature schema describing the model's feature columns and
                categorical encoders.
            feature_store: Feast-backed online store for the model features.
            database: Postgres database used to persist feature snapshots.
            threshold: Decision threshold above which a transaction is flagged.

        Raises:
            ValueError: If the ``KSERVE_URL`` environment variable is not set.
        """
        self.schema = schema
        self.feature_columns: list[str] = schema["feature_columns"]
        self.feature_store = feature_store
        self.database = database
        self.threshold = threshold

        self.kserve_url = os.getenv("KSERVE_URL", "")
        if not self.kserve_url:
            logger.error("KSERVE_URL environment variable is not set")
            raise ValueError("KSERVE_URL environment variable is not set")
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
        """Open all backing resources.

        Opens the database, opens and warms up the Feast online store, enters the
        KServe HTTP client context and starts the Kafka producer when publishing
        is enabled. Kafka is best-effort: when disabled or unreachable (e.g.
        local runs) the service still starts and simply skips result publishing.
        """
        await self.database.open()
        await self.feature_store.open()
        await self.kserve_client.__aenter__()
        await self._start_producer()
        logger.info(
            "FraudDetectionService is ready",
            extra={"kafka_enabled": self.producer is not None},
        )

    async def _start_producer(self) -> None:
        """Start the Kafka producer if enabled, tolerating failures for local runs.

        Skips startup when ``KAFKA_ENABLED`` is falsy or ``BOOTSTRAP_SERVERS`` is
        unset. Uses an SSL context only when the security protocol requires it,
        so a local PLAINTEXT broker needs no certificates. Any startup failure is
        logged and leaves the producer disabled rather than aborting the service.
        """
        if os.getenv("KAFKA_ENABLED", "true").strip().lower() not in ("1", "true", "yes"):
            logger.info("Kafka publishing disabled; skipping producer startup")
            return

        bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
        if not bootstrap_servers:
            logger.info("BOOTSTRAP_SERVERS not set; skipping Kafka producer startup")
            return

        try:
            security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
            ssl_context = None
            if security_protocol in ("SSL", "SASL_SSL"):
                ssl_context = ssl.create_default_context(
                    cafile=os.getenv("KAFKA_SSL_CAFILE"),
                )
                ssl_context.load_cert_chain(
                    certfile=os.getenv("KAFKA_SSL_CERTFILE"),
                    keyfile=os.getenv("KAFKA_SSL_KEYFILE"),
                )
            producer = AIOKafkaProducer(
                bootstrap_servers=bootstrap_servers,
                security_protocol=security_protocol,
                ssl_context=ssl_context,
            )
            await producer.start()
            self.producer = producer
            logger.info(
                "Kafka producer started",
                extra={"bootstrap_servers": bootstrap_servers},
            )
        except Exception as exc:
            logger.warning(
                "Failed to start Kafka producer; prediction publishing disabled",
                extra={"error": str(exc)},
            )
            self.producer = None

    async def close(self) -> None:
        """Release all backing resources.

        Closes the database, the Feast store, the KServe HTTP client and the
        Kafka producer (if started).
        """
        await self.database.close()
        await self.feature_store.close()
        await self.kserve_client.aclose()
        if self.producer:
            await self.producer.stop()
        logger.info("FraudDetectionService has been closed")

    async def predict_with_kserve(self, vector: list[float]) -> float:
        """Send an encoded feature vector to KServe and return the fraud probability.

        Builds a KServe v2 inference payload from ``vector`` (non-finite values
        become JSON ``null`` so the model treats them as missing), posts it to
        the configured endpoint and extracts the first output value.

        Args:
            vector: The ordered, encoded feature values in schema column order.

        Returns:
            The fraud probability returned by the model.

        Raises:
            RuntimeError: If the request fails, times out, returns an error
                status, or the response cannot be parsed.
        """
        data = [
            [None if (isinstance(x, float) and not math.isfinite(x)) else float(x) for x in vector]
        ]
        payload = {
            "inputs": [
                {
                    "name": "input-0",
                    "shape": [len(data), len(vector)],
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
                extra={"kserve_url": self.kserve_url, "error": str(exc)},
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

        return probability

    async def predict(self, inputs: FraudDetectionInputs) -> FraudDetectionOutputs:
        """Run the full fraud scoring pipeline for a single transaction.

        Reads the precomputed feature vector for the ``(user_id, card_id)`` pair
        from the Feast online store, encodes it per the model schema, calls KServe
        for a probability, persists a prediction log, publishes the result to
        Kafka, and returns the structured prediction. Failures to save the
        prediction log or publish to Kafka are logged but do not abort scoring.

        Args:
            inputs: The validated transaction identifiers to score.

        Returns:
            The transaction id, fraud probability and binary prediction.

        Raises:
            RuntimeError: If reading online features or KServe inference fails.
        """
        request_id = uuid4().hex
        predict_started_at = perf_counter()

        def log_time_perf(operation: str, started_at: float) -> None:
            """Log the elapsed time of ``operation`` since ``started_at`` in milliseconds."""
            logger.info(
                "Prediction step timing",
                extra={
                    "operation": operation,
                    "elapsed_ms": round((perf_counter() - started_at) * 1000, 3),
                    "transaction_id": transaction_id,
                },
            )

        transaction_id = inputs.transaction_id
        user_id = inputs.user_id
        card_id = inputs.card_id

        operation_started_at = perf_counter()
        try:
            features = await self.feature_store.get_online_features(user_id, card_id)
        except Exception as exc:
            raise RuntimeError("Failed to read online features for prediction") from exc
        log_time_perf("fetch_online_features", operation_started_at)

        if all(features.get(column) is None for column in self.feature_columns):
            logger.warning(
                "No online features materialised for user-card pair",
                extra={"user_id": user_id, "card_id": card_id, "transaction_id": transaction_id},
            )

        operation_started_at = perf_counter()
        encoded = build_model_inputs(features, self.schema)
        vector = [encoded[column] for column in self.feature_columns]
        log_time_perf("build_model_inputs", operation_started_at)

        operation_started_at = perf_counter()
        probability = await self.predict_with_kserve(vector)
        log_time_perf("kserve_inference", operation_started_at)

        prediction = 1 if probability >= self.threshold else 0
        latency = round((perf_counter() - predict_started_at) * 1000, 3)

        # Persisting to prediction_logs is temporarily disabled: the database is
        # remote (GCP Cloud SQL via proxy), so awaiting the insert adds ~400ms of
        # WAN round-trip to every request. Re-enable (or move to a fire-and-forget
        # task / the Kafka -> writer path) when persistence is needed.
        # try:
        #     await self.database.execute(
        #         """
        #         INSERT INTO application.prediction_logs (
        #             transaction_id, model_name, model_version,
        #             fraud_score, prediction, threshold, latency_ms
        #         )
        #         VALUES ($1, $2, $3, $4, $5, $6, $7)
        #         """,
        #         (
        #             transaction_id,
        #             os.getenv("MODEL_NAME"),
        #             os.getenv("MODEL_VERSION"),
        #             probability,
        #             prediction,
        #             self.threshold,
        #             latency,
        #         ),
        #     )
        #     logger.info("Saved prediction log", extra={"transaction_id": transaction_id})
        # except Exception:
        #     logger.exception("Failed to save prediction log", extra={"transaction_id": transaction_id})

        logger.info(
            "Finished fraud prediction",
            extra={
                "request_id": request_id,
                "transaction_id": transaction_id,
                "probability": probability,
                "prediction": prediction,
                "latency": latency,
                "latency_unit": "milliseconds",
            },
        )

        if self.producer is not None:
            try:
                await self.producer.send_and_wait(
                    self.predictions_topic,
                    value=json.dumps({
                        "request_id": request_id,
                        "transaction_id": transaction_id,
                        "user_id": user_id,
                        "card_id": card_id,
                        "fraud_score": probability,
                        "prediction": prediction,
                        "latency_ms": latency,
                        "model_name": os.getenv("MODEL_NAME"),
                        "model_version": os.getenv("MODEL_VERSION"),
                        "threshold": self.threshold,
                    }).encode("utf-8"),
                    key=request_id.encode(),
                )
                logger.info(
                    "Sent prediction result to Kafka",
                    extra={"request_id": request_id, "transaction_id": transaction_id},
                )
            except Exception as e:
                logger.exception(
                    "Failed to send prediction result to Kafka",
                    extra={
                        "request_id": request_id,
                        "transaction_id": transaction_id,
                        "error": str(e),
                    },
                )

        log_time_perf("predict_total", predict_started_at)
        return FraudDetectionOutputs(
            transaction_id=transaction_id,
            probability=probability,
            prediction=prediction,
        )
