"""Fraud detection scoring service orchestrating end-to-end prediction.

Defines ``FraudDetectionService``, which owns the connections to Postgres, the
Feast online feature store, the KServe inference endpoint and (optionally) a
Kafka producer. For each transaction it reads the precomputed feature vector for
the ``(user_id, card_id)`` pair from the online store, encodes it per the model
schema, calls KServe for a probability, persists a prediction log, emits the
result to Kafka and returns the structured prediction.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import ssl
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Optional
from uuid import uuid4
import random
import httpx
from aiokafka import AIOKafkaProducer
from redis import asyncio as aioredis
from structlog import get_logger

from database.postgres import PostgresDatabase
from fraud_detection.core.models import FraudDetectionInputs, FraudDetectionOutputs
from fraud_detection.core.utils import build_model_inputs
from fraud_detection.core.feature_store import FeastFeatureStore

logger = get_logger(__name__)

_VELOCITY_RETENTION_S = 25 * 3600 # we choose 25h to capture 24h + 1h buffer
_VELOCITY_AGG_TTL_S = 90 * 86400 # we choose 90 days to keep the historical data for 3 monthes. Any user having no activity in 90 days will be cleaned up

_LUA_CARD_VELOCITY = """
-- KEYS[1]=card:transactions:{card_id}  KEYS[2]=card:declines:{card_id}  KEYS[3]=card:aggregate:{card_id}
-- ARGV[1]=T (epoch seconds)  ARGV[2]=amount_usd  ARGV[3]=member "tx_id|amount"
-- ARGV[4]=retention_s  ARGV[5]=agg_ttl_s
local t = tonumber(ARGV[1])
local ret = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, t - ret)
redis.call('ZREMRANGEBYSCORE', KEYS[2], 0, t - ret)

-- 1) read prior state (before appending the transaction being scored)
local cnt_1h = redis.call('ZCOUNT', KEYS[1], t - 3600, t)
local prior24 = redis.call('ZRANGEBYSCORE', KEYS[1], t - 86400, t)
local cnt_24, sum_24 = #prior24, 0.0
for _, m in ipairs(prior24) do
  sum_24 = sum_24 + tonumber(string.sub(m, string.find(m, '|') + 1))
end
local declines_24 = redis.call('ZCOUNT', KEYS[2], t - 86400, t)
local agg = redis.call('HMGET', KEYS[3], 'count_so_far', 'sum_so_far', 'sum_square', 'last_txn_at')

-- 2) append this transaction — idempotent on member, so retries don't double-count
if not redis.call('ZSCORE', KEYS[1], ARGV[3]) then
  redis.call('ZADD', KEYS[1], t, ARGV[3])
  redis.call('HINCRBY',      KEYS[3], 'count_so_far', 1)
  redis.call('HINCRBYFLOAT', KEYS[3], 'sum_so_far',   ARGV[2])
  redis.call('HINCRBYFLOAT', KEYS[3], 'sum_square',   tonumber(ARGV[2]) ^ 2)
  redis.call('HSET',         KEYS[3], 'last_txn_at',  ARGV[1])
end

redis.call('EXPIRE', KEYS[1], ret)
redis.call('EXPIRE', KEYS[2], ret)
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[5]))

return {cnt_1h, cnt_24, tostring(sum_24), declines_24,
        agg[1] or 0, agg[2] or '0', agg[3] or '0', agg[4] or false}
"""

_LUA_USER_VELOCITY = """
-- KEYS[1]=user:transactions:{user_id}  KEYS[2]=user:aggregate:{user_id}
-- ARGV[1]=T  ARGV[2]=amount_usd  ARGV[3]=member "tx_id|amount"
-- ARGV[4]=retention_s  ARGV[5]=agg_ttl_s
local t = tonumber(ARGV[1])
local ret = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, t - ret)

local prior24 = redis.call('ZRANGEBYSCORE', KEYS[1], t - 86400, t)
local cnt_24, sum_24 = #prior24, 0.0
for _, m in ipairs(prior24) do
  sum_24 = sum_24 + tonumber(string.sub(m, string.find(m, '|') + 1))
end
local last_txn_at = redis.call('HGET', KEYS[2], 'last_txn_at')

if not redis.call('ZSCORE', KEYS[1], ARGV[3]) then
  redis.call('ZADD', KEYS[1], t, ARGV[3])
  redis.call('HSET', KEYS[2], 'last_txn_at', ARGV[1])
end

redis.call('EXPIRE', KEYS[1], ret)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[5]))

return {cnt_24, tostring(sum_24), last_txn_at or false}
"""


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
        redis_client: aioredis.Redis,
    ) -> None:
        """Initialize the service and its inference client configuration.

        Args:
            schema: Feature schema describing the model's feature columns and
                categorical encoders.
            feature_store: Feast-backed online store for the model features.
            database: Postgres database used to persist feature snapshots.

        Raises:
            ValueError: If the ``KSERVE_URL`` environment variable is not set.
        """
        self.schema = schema
        self.feature_columns: list[str] = schema["feature_columns"]
        self.feature_store = feature_store
        self.database = database
        self.redis_client = redis_client
        self.threshold = float(os.getenv("DECISION_THRESHOLD", "0.8"))
        self.card_transactions_key = os.getenv("CARD_TRANSACTIONS_KEY", "")
        self.card_aggregate_key = os.getenv("CARD_AGGREGATE_KEY", "")
        self.card_declines_key = os.getenv("CARD_DECLINES_KEY", "")
        self.user_transactions_key = os.getenv("USER_TRANSACTIONS_KEY", "")
        self.user_aggregate_key = os.getenv("USER_AGGREGATE_KEY", "")
        if not all([
            self.card_transactions_key,
            self.card_aggregate_key,
            self.card_declines_key,
            self.user_transactions_key,
            self.user_aggregate_key,
        ]):
            logger.error(
                "One or more Redis key environment variables are not set",
                extra={
                    "CARD_TRANSACTIONS_KEY": self.card_transactions_key,
                    "CARD_AGGREGATE_KEY": self.card_aggregate_key,
                    "CARD_DECLINES_KEY": self.card_declines_key,
                    "USER_TRANSACTIONS_KEY": self.user_transactions_key,
                    "USER_AGGREGATE_KEY": self.user_aggregate_key,
                },
            )
            raise ValueError("One or more Redis key environment variables are not set")
        self._card_velocity_script = self.redis_client.register_script(_LUA_CARD_VELOCITY)
        self._user_velocity_script = self.redis_client.register_script(_LUA_USER_VELOCITY)

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
        await self.redis_client.ping()
        await self.kserve_client.__aenter__()
        await self._start_producer()
        logger.info(
            "FraudDetectionService is ready",
        )

    async def _start_producer(self) -> None:
        """Start the Kafka producer if enabled, tolerating failures for local runs.

        Uses an SSL context only when the security protocol requires it,
        so a local PLAINTEXT broker needs no certificates. Any startup failure is
        logged and leaves the producer disabled rather than aborting the service.
        """

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
        await self.redis_client.aclose()
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
                operation,
                extra={
                    "elapsed_ms": round((perf_counter() - started_at) * 1000, 3),
                },
            )

        transaction_id = inputs.transaction_id
        user_id = inputs.user_id
        card_id = inputs.card_id
        timestamp = inputs.timestamp
        amount_usd = inputs.amount_usd
        # randomly create status (approved or declined)
        status = "approved" if random.random() < 0.99 else "declined"

        operation_started_at = perf_counter()
        try:
            features = await self.feature_store.get_online_features(user_id, card_id)
        except Exception as exc:
            raise RuntimeError("Failed to read online features for prediction") from exc
        log_time_perf("Fetch online features", operation_started_at)

        if all(features.get(column) is None for column in self.feature_columns):
            logger.warning(
                "No online features materialised for user-card pair",
                extra={"user_id": user_id, "card_id": card_id, "transaction_id": transaction_id},
            )

        operation_started_at = perf_counter()    
        try:
            features["amount_usd"] = amount_usd
            features["log_amount"] = math.log(1 + amount_usd)

            created_at_utc = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
            features["hour"] = created_at_utc.hour
            features["weekday"] = created_at_utc.weekday()
            features["is_night"] = int(features["hour"] < 6 or features["hour"] >= 23)
            features["channel"] = inputs.channel
            features["merchant_category"] = inputs.merchant_category
            features["merchant_risk_level"] = inputs.merchant_risk_level
            features["geo_mismatch"] = int(inputs.billing_country_code != inputs.ip_country_code)
            features["foreign_ip"] = int(inputs.ip_country_code != features.get("user_country"))
            features["recipient_differs"] = int(inputs.email_purchaser != inputs.email_recipient)
            features["account_age_days"] = (created_at_utc - datetime.fromisoformat(
                str(features.get("account_created_at")).replace("Z", "+00:00")
            ).astimezone(timezone.utc)).days
            features["card_age_days"] = (created_at_utc - datetime.fromisoformat(
                str(features.get("card_created_at")).replace("Z", "+00:00")
            ).astimezone(timezone.utc)).days

            t_epoch = created_at_utc.timestamp()
            member = f"{transaction_id}|{amount_usd}"
            velocity_args = [f"{t_epoch:.6f}", str(amount_usd), member, _VELOCITY_RETENTION_S, _VELOCITY_AGG_TTL_S]

            card_raw, user_raw = await asyncio.gather(
                self._card_velocity_script(
                    keys=[
                        f"{self.card_transactions_key}:{card_id}",
                        f"{self.card_declines_key}:{card_id}",
                        f"{self.card_aggregate_key}:{card_id}",
                    ],
                    args=velocity_args,
                ),
                self._user_velocity_script(
                    keys=[
                        f"{self.user_transactions_key}:{user_id}",
                        f"{self.user_aggregate_key}:{user_id}",
                    ],
                    args=velocity_args,
                ),
            )

            cnt_1h, cnt_24h, sum_24h, declines_24h, n, s, ss, last_txn_at = card_raw
            u_cnt_24h, u_sum_24h, u_last_txn_at = user_raw
            n = int(n)

            if n < 2:
                zscore = math.nan
            else:
                s, ss = float(s), float(ss)
                mean = s / n
                variance = (ss - s * s / n) / (n - 1)
                zscore = math.nan if variance <= 0 else (amount_usd - mean) / math.sqrt(variance)

            features["card_tx_count_1h"] = cnt_1h + 1
            features["card_tx_count_24h"] = cnt_24h + 1
            features["card_amount_sum_24h"] = float(sum_24h) + amount_usd
            features["card_seconds_since_last_tx"] = (
                (t_epoch - float(last_txn_at)) if last_txn_at else math.nan
            )
            features["card_amount_zscore"] = zscore
            features["card_tx_seq"] = n + 1
            features["card_declines_24h"] = declines_24h
            features["user_tx_count_24h"] = u_cnt_24h + 1
            features["user_amount_sum_24h"] = float(u_sum_24h) + amount_usd
            features["user_seconds_since_last_tx"] = (
                (t_epoch - float(u_last_txn_at)) if u_last_txn_at else math.nan
            )
        except Exception as exc:
            logger.exception("Failed to calculate derived features", extra={"transaction_id": transaction_id})
            raise RuntimeError("Failed to calculate derived features") from exc
        log_time_perf("Calculate derived features", operation_started_at)

        operation_started_at = perf_counter()
        try:
            encoded = build_model_inputs(features, self.schema)
            vector = [encoded[column] for column in self.feature_columns]
        except Exception as exc:
            logger.exception("Failed to build model inputs", extra={"transaction_id": transaction_id})
            raise RuntimeError("Failed to build model inputs") from exc
        log_time_perf("Build model inputs", operation_started_at)

        operation_started_at = perf_counter()
        try:
            probability = await self.predict_with_kserve(vector)
        except Exception as exc:
            logger.exception("Failed to run inference", extra={"transaction_id": transaction_id})
            raise RuntimeError("Failed to run inference") from exc
        log_time_perf("KServe inference", operation_started_at)

        prediction = 1 if probability >= self.threshold else 0
        latency = round((perf_counter() - predict_started_at) * 1000, 3)

        if self.producer is not None:
            try:
                await self.producer.send_and_wait(
                    self.predictions_topic,
                    value=json.dumps({
                        "request_id": request_id,
                        "transaction_id": transaction_id,
                        "user_id": user_id,
                        "card_id": card_id,
                        "merchant_id": "892f2f376b8d48dcad990996e23f37c0", # hard coded here because it is not much important for now
                        "device_id": "f63be9e313d44d3d990f96e50e0465b3", # hard coded here because it is not much important for now
                        "amount_usd": amount_usd,
                        "currency": "VND", # hard coded here because it is not much important for now
                        "channel": inputs.channel,
                        "billing_country_code": inputs.billing_country_code,
                        "ip_country_code": inputs.ip_country_code,
                        "email_purchaser": inputs.email_purchaser,
                        "email_recipient": inputs.email_recipient,
                        "status": status,
                        "transaction_time": timestamp,
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

        return FraudDetectionOutputs(
            transaction_id=transaction_id,
            probability=probability,
            prediction=prediction,
        )
