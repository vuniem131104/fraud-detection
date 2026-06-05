from aiokafka import AIOKafkaConsumer
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from redis import asyncio as aioredis
from structlog import get_logger

from fraud_detection.features.utils import (
    build_features_key,
    build_transactions_key,
    days_between,
    parse_datetime,
    refresh_key_script,
)


logger = get_logger(__name__)


def decode_redis_value(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode()

    return value


def normalize_email_domains(transaction: dict[str, Any]) -> dict[str, Any]:
    normalized_transaction = transaction.copy()
    for field in ("request_id", "probability", "status"):
        normalized_transaction.pop(field, None)

    for field in ("email_purchaser", "email_recipient"):
        email = normalized_transaction.get(field)
        if isinstance(email, str):
            normalized_transaction[field] = email.strip().lower().split("@")[-1]

    return normalized_transaction


async def refresh_features_for_user_card(
    redis_client: aioredis.Redis,
    transaction: dict[str, Any],
    cutoff_days: int = 30,
) -> None:
    if cutoff_days < 1:
        raise ValueError("cutoff_days must be greater than 0")

    transaction_to_store = normalize_email_domains(transaction)
    user_id = transaction_to_store["user_id"]
    card_id = transaction_to_store["card_id"]

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=cutoff_days)
    cutoff_score = int(cutoff.timestamp())
    transactions_key = build_transactions_key(user_id, card_id)
    features_key = build_features_key(user_id, card_id)
    transaction_score = int(
        parse_datetime(transaction_to_store["event_timestamp"]).timestamp()
    )

    for feature in ("D4", "D15"):
        value = transaction_to_store.get(feature)
        if value is not None:
            try:
                transaction_to_store[feature] = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{feature} must be numeric, got {value!r}") from exc

    serialized_transaction = json.dumps(transaction_to_store, separators=(",", ":"))

    try:
        logger.info(
            "Starting to refresh features for user card",
            extra={
                "user_id": user_id,
                "card_id": card_id,
                "transaction_id": transaction_to_store.get("tx_id"),
                "cutoff_days": cutoff_days,
            },
        )
        refresh_result = await redis_client.eval(
            refresh_key_script,
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
        await redis_client.hset(features_key, mapping=feature_updates)

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
                "user_id": user_id,
                "card_id": card_id,
                "transaction_id": transaction_to_store.get("tx_id"),
            },
        )
        raise


async def main():
    topic = os.getenv("PREDICTIONS_TOPIC", "fraud_predictions")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = "redis_features_refresher"

    logger.info(
        "Starting Redis features refresh worker",
        extra={
            "topic": topic,
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
        },
    )

    redis_pool = aioredis.BlockingConnectionPool(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=int(os.getenv("REDIS_DB", "0")),
        decode_responses=True,
        max_connections=int(os.getenv("REDIS_POOL_MAX_CONNECTIONS", "64")),
        timeout=float(os.getenv("REDIS_POOL_TIMEOUT_S", "5")),
    )
    redis_client = aioredis.Redis(connection_pool=redis_pool)

    logger.info("Pinging Redis")
    await redis_client.ping()

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    logger.info(
        "Starting Kafka consumer",
        extra={
            "topic": topic,
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
        },
    )
    await consumer.start()
    logger.info("Kafka consumer started")

    try:
        async for msg in consumer:
            logger.info(
                "Received transaction message",
                extra={
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": (
                        msg.key.decode("utf-8", errors="replace")
                        if msg.key
                        else None
                    ),
                },
            )
            transaction = json.loads(msg.value.decode("utf-8"))
            await refresh_features_for_user_card(redis_client, transaction)
            await consumer.commit()
            logger.info(
                "Committed transaction message",
                extra={
                    "transaction_id": transaction.get("tx_id"),
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                },
            )
    except Exception:
        logger.exception("Redis features refresh worker failed")
        raise
    finally:
        logger.info("Stopping Kafka consumer")
        await consumer.stop()
        logger.info("Kafka consumer stopped")
        logger.info("Closing Redis client")
        await redis_client.aclose()
        logger.info("Redis features refresh worker stopped")


if __name__ == "__main__":
    asyncio.run(main())
