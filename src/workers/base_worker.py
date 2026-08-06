"""Base Kafka consumer worker used by the fraud-detection background workers.

Provides :class:`BaseKafkaWorker`, a reusable asyncio Kafka consumer that
connects over SSL, polls messages in batches, dispatches each decoded message
to a subclass-defined ``handle`` hook, and commits offsets manually. It also
wires up graceful shutdown on ``SIGINT``/``SIGTERM``. Concrete workers (e.g. the
Postgres prediction writer and the Redis features refresher) subclass it and
implement ``handle``.
"""

import asyncio
import os
from structlog import get_logger
import signal
import json
import ssl
from aiokafka import AIOKafkaConsumer, ConsumerRecord
from typing import Any

logger = get_logger(__name__)


class BaseKafkaWorker:
    """Reusable asyncio Kafka consumer with batched polling and manual commits.

    Subclasses implement :meth:`handle` to process each decoded message. The
    base class owns the consumer lifecycle, signal handling, batched polling,
    per-message dispatch, and offset committing.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> None:
        """Configure the worker and build the underlying SSL Kafka consumer.

        Args:
            bootstrap_servers: Kafka bootstrap server address(es).
            topic: Topic to subscribe to.
            group_id: Consumer group id used for offset tracking.
            max_records: Maximum number of records to fetch per poll.
            timeout_ms: Poll timeout in milliseconds.

        The SSL context and certificates are read from the ``KAFKA_SSL_CAFILE``,
        ``KAFKA_SSL_CERTFILE`` and ``KAFKA_SSL_KEYFILE`` environment variables,
        and the security protocol from ``KAFKA_SECURITY_PROTOCOL`` (default
        ``SSL``). Auto-commit is disabled so offsets are committed manually only
        after a batch has been processed.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.max_records = max_records
        self.timeout_ms = timeout_ms

        self._stopping = asyncio.Event()

        ssl_context = ssl.create_default_context(
            cafile=os.getenv("KAFKA_SSL_CAFILE"),
        )
        ssl_context.load_cert_chain(
            certfile=os.getenv("KAFKA_SSL_CERTFILE"),
            keyfile=os.getenv("KAFKA_SSL_KEYFILE"),
        )
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=self.max_records,
            security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL"),
            ssl_context=ssl_context,
        )

    async def start(self) -> None:
        """Start the consumer and run the consume loop until stopped.

        Registers ``SIGINT``/``SIGTERM`` handlers for graceful shutdown, starts
        the Kafka consumer, runs :meth:`consume_loop`, and always stops the
        consumer on exit.
        """
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop)
            except NotImplementedError:
                pass

        await self.consumer.start()
        logger.info(
            "Kafka consumer started",
            extra={
                "topic": self.topic,
                "group_id": self.group_id,
            },
        )

        try:
            await self.consume_loop()
        finally:
            await self.consumer.stop()
            logger.info(
                "Kafka consumer stopped",
                extra={
                    "topic": self.topic,
                    "group_id": self.group_id,
                },
            )

    def stop(self) -> None:
        """Signal the consume loop to stop after the current iteration."""
        self._stopping.set()

    async def consume_loop(self) -> None:
        """Poll Kafka in batches and dispatch each message until stopped.

        Fetches up to ``max_records`` messages per poll, processes every message
        via :meth:`process_one`, then commits offsets for the batch. Any
        exception in an iteration is logged and the loop retries after a short
        delay so a transient failure does not kill the worker.
        """
        while not self._stopping.is_set():
            try:
                batch = await self.consumer.getmany(
                    timeout_ms=self.timeout_ms,
                    max_records=self.max_records,
                )

                if not batch:
                    continue

                for _topic_partition, messages in batch.items():
                    for msg in messages:
                        await self.process_one(msg)

                await self.consumer.commit()

            except Exception as e:
                logger.exception(
                    "Error in consume loop, will retry after delay",
                    extra={                        
                        "error": str(e),
                    },
                )
                await asyncio.sleep(1)

    async def process_one(self, msg: ConsumerRecord) -> None:
        """Decode a single Kafka record and pass it to :meth:`handle`.

        Args:
            msg: The raw Kafka record whose value is a UTF-8 JSON payload.

        Raises:
            Exception: Re-raised after logging if decoding or handling fails, so
                the batch is not committed. (In production this could instead be
                routed to a dead-letter queue.)
        """
        try:
            inputs = json.loads(msg.value.decode("utf-8"))
            await self.handle(inputs)
        except Exception as e:
            logger.exception(
                "Failed to process Kafka message",
                extra={
                    "error": str(e),
                },
            )
            # in production, we might want to send the message to a dead-letter queue instead of just logging and skipping
            raise
        
    async def handle(self, inputs: dict[str, Any]) -> None:
        """Process a single decoded message; must be implemented by subclasses.

        Args:
            inputs: The decoded JSON payload of one Kafka message.

        Raises:
            NotImplementedError: Always, unless overridden by a subclass.
        """
        raise NotImplementedError