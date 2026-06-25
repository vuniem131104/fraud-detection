import asyncio
import os
from typing import Any

from database.postgres import PostgresDatabase
from structlog import get_logger

from workers.base_worker import BaseKafkaWorker
from utils import parse_datetime
from uuid import uuid4

logger = get_logger(__name__)

CHANNEL_MAP = {
    "W": "web",
    "C": "mobile_app",
    "R": "pos",
}


class PredictionWriter(BaseKafkaWorker):
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

    async def handle(self, msg: dict[str, Any]) -> None:
        tx = msg.get("current_transaction", {})
        tx_id = tx.get("tx_id") or tx.get("tx_id")

        # ── 1. transactions ──────────────────────────────────────────────────
        try:
            await self.database.execute(
                """
                INSERT INTO application.transactions (
                    id, user_id, card_id,
                    amount_usd, channel,
                    billing_zone, billing_country,
                    email_purchaser, email_recipient,
                    device_info, device_type, os_raw, browser_raw,
                    screen_resolution, created_at
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5,
                    $6, $7,
                    $8, $9,
                    $10, $11, $12, $13,
                    $14, $15
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    tx_id,
                    tx["user_id"],
                    tx["card_id"],
                    float(tx["amount_usd"]),
                    CHANNEL_MAP.get(tx.get("channel", ""), tx.get("channel")),
                    int(tx["billing_zone"]),
                    int(tx["billing_country"]),
                    tx.get("email_purchaser"),
                    tx.get("email_recipient"),
                    tx.get("device_info"),
                    tx.get("device_type"),
                    tx.get("os_raw"),
                    tx.get("browser_raw"),
                    tx.get("screen_resolution"),
                    parse_datetime(tx["event_timestamp"]),
                ),
            )
            logger.info("Saved transaction", extra={"tx_id": tx_id})
        except Exception:
            logger.exception("Failed to save transaction", extra={"tx_id": tx_id})
            raise

        # ── 2. prediction_logs ───────────────────────────────────────────────
        try:
            await self.database.execute(
                """
                INSERT INTO application.prediction_logs (
                    tx_id, model_name, model_version,
                    fraud_score, prediction, threshold, latency_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                (
                    tx_id,
                    msg.get("model_name"),
                    msg.get("model_version"),
                    float(msg["fraud_score"]),
                    int(msg["prediction"]),
                    float(msg["threshold"]),
                    float(msg.get("latency_ms") or 0.0),
                ),
            )
            logger.info("Saved prediction log", extra={"tx_id": tx_id})
        except Exception:
            logger.exception("Failed to save prediction log", extra={"tx_id": tx_id})
            raise


async def main() -> None:
    topic = os.getenv("PREDICTIONS_TOPIC")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    group_id = f"postgres-prediction-writer-{uuid4()}"
    max_records = int(os.getenv("KAFKA_MAX_RECORDS", "100"))
    timeout_ms = int(os.getenv("KAFKA_TIMEOUT_MS", "1000"))

    logger.info(
        "Starting prediction writer worker",
        extra={"topic": topic, "group_id": group_id},
    )

    database = PostgresDatabase.from_env()
    worker = PredictionWriter(
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
