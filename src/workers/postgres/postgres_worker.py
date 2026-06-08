import asyncio
import os
from typing import Any

from database.postgres import PostgresDatabase
from structlog import get_logger

from workers.base_worker import BaseKafkaWorker
from utils import parse_datetime

reversed_mapping_channel = {
    "W": "web",
    "C": "mobile_app",
    "R": "pos",
}

logger = get_logger(__name__)


def latency_from_inputs(inputs: dict[str, Any]) -> float:
    latency = float(inputs.get("latency") or 0.0)
    if latency < 0:
        raise ValueError(f"latency must be greater than or equal to 0, got {latency}")
    return latency


class TransactionWriter(BaseKafkaWorker):
    def __init__(
        self,
        *,
        database: PostgresDatabase,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> None:
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            max_records=max_records,
            timeout_ms=timeout_ms,
        )
        self.database = database

    async def start(self) -> None:
        logger.info("Opening Postgres connection pool")
        await self.database.open()

        try:
            await super().start()
        finally:
            logger.info("Closing Postgres connection pool")
            await self.database.close()
            logger.info("Transaction saver worker stopped")

    async def handle(self, inputs: dict[str, Any]) -> None:
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
                        latency,
                        created_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
                )
                """,
                (
                    inputs["tx_id"],
                    inputs["user_id"],
                    inputs["card_id"],
                    inputs["status"],
                    inputs["amount_usd"],
                    reversed_mapping_channel.get(inputs["channel"], inputs["channel"]),
                    inputs["billing_zone"],
                    inputs["billing_country"],
                    inputs["email_purchaser"],
                    inputs["email_recipient"],
                    inputs["device_info"],
                    inputs["device_type"],
                    inputs["os_raw"],
                    inputs["browser_raw"],
                    inputs["screen_resolution"],
                    latency_from_inputs(inputs),
                    parse_datetime(inputs["event_timestamp"]),
                ),
            )
            
            logger.info(
                "Saved transaction",
                extra={
                    "user_id": inputs.get("user_id"),
                    "card_id": inputs.get("card_id"),
                    "status": inputs.get("status"),
                },
            )
        except Exception:
            logger.exception(
                "Failed to save transaction",
                extra={
                    "user_id": inputs.get("user_id"),
                    "card_id": inputs.get("card_id"),
                    "status": inputs.get("status"),
                },
            )
            raise


async def main():
    topic = os.getenv("PREDICTIONS_TOPIC")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    group_id = os.getenv("TRANSACTION_WRITER_GROUP_ID")
    max_records = int(os.getenv("KAFKA_MAX_RECORDS", "100"))
    timeout_ms = int(os.getenv("KAFKA_TIMEOUT_MS", "1000"))

    logger.info(
        "Starting transaction saver worker",
        extra={
            "topic": topic,
            "group_id": group_id,
        },
    )

    database = PostgresDatabase.from_env()
    worker = TransactionWriter(
        database=database,
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        max_records=max_records,
        timeout_ms=timeout_ms,
    )
    await worker.start()


if __name__ == "__main__":
    asyncio.run(main())
