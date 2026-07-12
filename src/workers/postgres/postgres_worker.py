"""Kafka worker that persists predictions and transactions to Postgres.

Defines :class:`PredictionWriter`, a :class:`BaseKafkaWorker` subclass that
consumes scored-transaction messages from the predictions topic and writes them
to two Postgres tables: ``application.transactions`` (the transaction details)
and ``application.prediction_logs`` (the model's fraud score and verdict).
The module-level :func:`main` builds the database connection and worker from
environment variables and runs it.
"""

import asyncio
import os
from datetime import datetime
from typing import Any

from database.postgres import PostgresDatabase
from structlog import get_logger

from workers.base_worker import BaseKafkaWorker

logger = get_logger(__name__)



class PredictionWriter(BaseKafkaWorker):
    """Kafka worker that writes transactions and prediction logs to Postgres."""

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
        """Initialise the worker with a Postgres database and Kafka settings.

        Args:
            database: Postgres database wrapper used to execute inserts.
            bootstrap_servers: Kafka bootstrap server address(es).
            topic: Topic to consume scored transactions from.
            group_id: Consumer group id.
            max_records: Maximum records fetched per poll.
            timeout_ms: Poll timeout in milliseconds.
        """
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            max_records=max_records,
            timeout_ms=timeout_ms,
        )
        self.database = database

    async def start(self) -> None:
        """Open the Postgres connection pool, run the worker, then close it.

        Wraps the base :meth:`start` so the database pool is opened before
        consuming and always closed afterwards.
        """
        logger.info("Opening Postgres connection pool")
        await self.database.open()
        try:
            await super().start()
        finally:
            logger.info("Closing Postgres connection pool")
            await self.database.close()

    async def handle(self, msg: dict[str, Any]) -> None:
        """Persist one scored transaction message to Postgres.

        Reads fields from the flat Kafka message produced by
        :class:`~fraud_detection.core.predict.FraudDetectionService` and
        inserts them into two tables:

        * ``application.transactions`` – core transaction details.
        * ``application.prediction_logs`` – model fraud score and verdict.

        The ``transactions`` insert uses ``ON CONFLICT (transaction_id) DO NOTHING``
        to stay idempotent.

        Args:
            msg: Flat decoded message with keys such as ``id``,
                ``user_id``, ``card_id``, ``amount_usd``, ``channel``,
                ``billing_country_code``, ``ip_country_code``,
                ``email_purchaser``, ``email_recipient``, ``status``,
                ``transaction_time``, ``fraud_score``, ``prediction``,
                ``threshold``, ``latency_ms``, ``model_name``,
                ``model_version``.

        Raises:
            Exception: Re-raised after logging if either insert fails.
        """
        transaction_id = msg.get("transaction_id")

        # ── 1. transactions ──────────────────────────────────────────────────
        try:
            await self.database.execute(
                """
                INSERT INTO application.transactions (
                    id, user_id, card_id,
                    merchant_id, device_id,
                    amount_usd, currency, channel,
                    billing_country_code, ip_country_code,
                    email_purchaser, email_recipient,
                    status, created_at
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5,
                    $6, $7, $8,
                    $9, $10,
                    $11, $12,
                    $13, $14
                )
                ON CONFLICT (id) DO NOTHING
                """,
                (
                    transaction_id,
                    msg.get("user_id"),
                    msg.get("card_id"),
                    msg.get("merchant_id"),
                    msg.get("device_id"),
                    float(msg["amount_usd"]),
                    msg.get("currency"),
                    msg.get("channel"),
                    msg.get("billing_country_code"),
                    msg.get("ip_country_code"),
                    msg.get("email_purchaser"),
                    msg.get("email_recipient"),
                    msg.get("status"),
                    datetime.fromisoformat(msg["transaction_time"]) if msg.get("transaction_time") else None,
                ),
            )
            logger.info("Saved transaction", extra={"transaction_id": transaction_id})
        except Exception:
            logger.exception("Failed to save transaction", extra={"transaction_id": transaction_id})
            raise

        # ── 2. prediction_logs ───────────────────────────────────────────────
        try:
            await self.database.execute(
                """
                INSERT INTO application.prediction_logs (
                    transaction_id, model_name, model_version,
                    fraud_score, prediction, threshold, latency_ms
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (
                    transaction_id,
                    msg.get("model_name"),
                    msg.get("model_version"),
                    float(msg["fraud_score"]),
                    int(msg["prediction"]),
                    float(msg["threshold"]),
                    float(msg.get("latency_ms") or 0.0),
                ),
            )
            logger.info("Saved prediction log", extra={"transaction_id": transaction_id})
        except Exception:
            logger.exception("Failed to save prediction log", extra={"transaction_id": transaction_id})
            raise


async def main() -> None:
    """Build the Postgres prediction-writer worker from env vars and run it.

    Reads Kafka and database configuration from environment variables, creates a
    unique consumer group id, constructs the :class:`PredictionWriter` and runs
    it until shutdown.
    """
    topic = os.getenv("PREDICTIONS_TOPIC")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
    group_id = os.getenv("KAFKA_GROUP_ID", "prediction-writer")
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
