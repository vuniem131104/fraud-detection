from aiokafka import AIOKafkaConsumer
import asyncio
from database.postgres import PostgresDatabase
import os
from typing import Any
from structlog import get_logger
import json
from datetime import datetime

logger = get_logger(__name__)

def parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))

async def save_transaction(database: PostgresDatabase, transaction: dict[str, Any]) -> None:
    try:
        await database.execute(
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
                transaction["tx_id"],
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

async def main():
    topic = os.getenv("PREDICTIONS_TOPIC", "fraud_predictions")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    group_id = "transaction_saver"

    logger.info(
        "Starting transaction saver worker",
        extra={
            "topic": topic,
            "bootstrap_servers": bootstrap_servers,
            "group_id": group_id,
        },
    )

    database = PostgresDatabase.from_env()
    logger.info("Opening Postgres connection pool")
    await database.open()

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
            transaction = json.loads(msg.value.decode('utf-8'))
            await save_transaction(database, transaction)
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
        logger.exception("Transaction saver worker failed")
        raise
    finally:
        logger.info("Stopping Kafka consumer")
        await consumer.stop()
        logger.info("Kafka consumer stopped")
        logger.info("Closing Postgres connection pool")
        await database.close()
        logger.info("Transaction saver worker stopped")
if __name__ == "__main__":
    asyncio.run(main())
