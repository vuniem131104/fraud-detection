"""Kafka worker that maintains rolling per-user-card features in Redis.

Defines :class:`RedisFeaturesRefresher`, a :class:`BaseKafkaWorker` subclass
that consumes transaction messages and updates the online feature store in
Redis. For each transaction it appends the event to a sorted set of recent
transactions, evicts entries older than a configurable cutoff window, and
recomputes derived features (rolling 30-day transaction count, card age, time
since the last transaction) used at scoring time. The module-level :func:`main`
builds the Redis connection pool and worker from environment variables and runs
it.
"""

import asyncio
import json
import os
from datetime import timedelta
from typing import Any

from redis import asyncio as aioredis
from structlog import get_logger

from workers.base_worker import BaseKafkaWorker
from utils import (
    REFRESH_KEY_SCRIPT,
    build_features_key,
    build_transactions_key,
    days_between,
    decode_redis_value,
    iso_local,
    local_now,
    normalize_email_domains,
    parse_datetime,
)

logger = get_logger(__name__)


class RedisFeaturesRefresher(BaseKafkaWorker):
    """Kafka worker that refreshes rolling per-user-card features in Redis."""

    def __init__(
        self,
        *,
        redis_client: aioredis.Redis,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        cutoff_days: int = 30,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> None:
        """Initialise the worker with a Redis client and Kafka settings.

        Args:
            redis_client: Async Redis client used for the feature store.
            bootstrap_servers: Kafka bootstrap server address(es).
            topic: Topic to consume transactions from.
            group_id: Consumer group id.
            cutoff_days: Size in days of the rolling window of recent
                transactions to retain; older entries are evicted.
            max_records: Maximum records fetched per poll.
            timeout_ms: Poll timeout in milliseconds.

        Raises:
            ValueError: If ``cutoff_days`` is less than 1.
        """
        if cutoff_days < 1:
            raise ValueError("cutoff_days must be greater than 0")

        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            max_records=max_records,
            timeout_ms=timeout_ms,
        )
        self.redis_client = redis_client
        self.cutoff_days = cutoff_days

    async def start(self) -> None:
        """Verify the Redis connection, run the worker, then close the client.

        Pings Redis before consuming and always closes the client once the base
        :meth:`start` returns.
        """
        logger.info("Pinging Redis")
        await self.redis_client.ping()

        try:
            await super().start()
        finally:
            logger.info("Closing Redis client")
            await self.redis_client.aclose()
            logger.info("Redis features refresh worker stopped")

    async def handle(self, inputs: dict[str, Any]) -> None:
        """Update the Redis feature store from one transaction message.

        Normalises the transaction (email domains, numeric ``D4``/``D15``
        fields, localised event timestamp), then runs the
        :data:`REFRESH_KEY_SCRIPT` Lua script to atomically add the transaction
        to the per-card sorted set, evict entries older than the cutoff window,
        and read back the removed/remaining counts and the stored card creation
        time. Finally it recomputes and writes the derived feature hash
        (rolling 30-day transaction count, card age in days, last transaction
        time, and days since the last transaction).

        Args:
            inputs: Decoded message containing ``current_transaction``.

        Raises:
            Exception: Re-raised after logging on any failure.
        """
        transaction_to_store = normalize_email_domains(inputs["current_transaction"])

        try:
            user_id = transaction_to_store["user_id"]
            card_id = transaction_to_store["card_id"]
            now = local_now()
            cutoff = now - timedelta(days=self.cutoff_days)
            cutoff_score = int(cutoff.timestamp())
            transactions_key = build_transactions_key(user_id, card_id)
            features_key = build_features_key(user_id, card_id)
            event_timestamp = parse_datetime(transaction_to_store["event_timestamp"])
            transaction_to_store["event_timestamp"] = iso_local(event_timestamp)
            transaction_score = int(event_timestamp.timestamp())

            for feature in ("D4", "D15"):
                value = transaction_to_store.get(feature)
                if value is not None:
                    try:
                        transaction_to_store[feature] = float(value)
                    except (TypeError, ValueError) as exc:
                        raise ValueError(
                            f"{feature} must be numeric, got {value!r}"
                        ) from exc

            serialized_transaction = json.dumps(
                transaction_to_store,
                separators=(",", ":"),
            )

            refresh_result = await self.redis_client.eval(
                REFRESH_KEY_SCRIPT,
                2,
                transactions_key,
                features_key,
                cutoff_score,
                serialized_transaction,
                transaction_score,
            )
            values = list(refresh_result or [])
            values.extend([None] * (3 - len(values)))
            removed_count, remaining_count, card_created_at = values[:3]
            card_created_at = decode_redis_value(card_created_at)
            feature_updates = {
                "no_transactions_30_days": remaining_count,
            }

            if card_created_at:
                feature_updates["card_age_days"] = days_between(
                    parse_datetime(card_created_at),
                    now,
                )

            last_txn_at = transaction_to_store["event_timestamp"]
            feature_updates["last_txn_at"] = last_txn_at
            feature_updates["no_days_since_last_txn"] = days_between(
                parse_datetime(last_txn_at),
                now,
            )
            await self.redis_client.hset(features_key, mapping=feature_updates)

            logger.info(
                "Finished refreshing features for user card",
                extra={
                    "user_id": user_id,
                    "card_id": card_id,
                    "transaction_id": transaction_to_store.get("tx_id"),
                    "removed_count": removed_count,
                    "remaining_count": remaining_count,
                },
            )

        except Exception:
            logger.exception(
                "Failed to refresh features for user card",
                extra={
                    "user_id": inputs.get("user_id"),
                    "card_id": inputs.get("card_id"),
                    "transaction_id": inputs.get("tx_id"),
                },
            )
            raise


async def main():
    """Build the Redis features-refresh worker from env vars and run it.

    Reads Kafka and Redis configuration from environment variables, builds a
    blocking Redis connection pool, constructs the
    :class:`RedisFeaturesRefresher` and runs it until shutdown.
    """
    topic = os.getenv("PREDICTIONS_TOPIC")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    group_id = os.getenv("KAFKA_GROUP_ID", "features-refresher")
    cutoff_days = int(os.getenv("REDIS_FEATURE_CUTOFF_DAYS"))
    max_records = int(os.getenv("KAFKA_MAX_RECORDS", "100"))
    timeout_ms = int(os.getenv("KAFKA_TIMEOUT_MS", "1000"))

    logger.info(
        "Starting Redis features refresh worker",
        extra={
            "topic": topic,
            "group_id": group_id,
        },
    )

    redis_pool = aioredis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST"),
        port=int(os.getenv("REDIS_PORT")),
        db=int(os.getenv("REDIS_DB")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "64")),
        timeout=float(os.getenv("REDIS_POOL_TIMEOUT_S", "5")),
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)
    worker = RedisFeaturesRefresher(
        redis_client=redis_client,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        cutoff_days=cutoff_days,
        max_records=max_records,
        timeout_ms=timeout_ms,
    )
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
