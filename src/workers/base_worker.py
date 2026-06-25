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
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        max_records: int = 100,
        timeout_ms: int = 1000,
    ) -> None:
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
        self._stopping.set()

    async def consume_loop(self) -> None:
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
        raise NotImplementedError