"""
Kafka Producer — publishes domain events to Redpanda.

Key design decisions:
1. Partition by aggregate_id (order_id) — guarantees ordering per order.
   All events for order-X land on the same partition, in sequence.
   DDIA: "Partitioning by key" — same key always same partition.

2. Idempotency key stored in Redis before publish — prevents double-writes
   if the client retries the same command.

3. acks="all" — leader + all ISR replicas acknowledge before we return.
   DDIA: This is the "sync replication" tradeoff — durability over latency.
"""

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from redis.asyncio import Redis
from tenacity import retry, stop_after_attempt, wait_exponential

from api.models.events import BaseEvent
from config import settings

logger = structlog.get_logger()


class KafkaEventPublisher:
    def __init__(self, producer: AIOKafkaProducer, redis: Redis):
        self._producer = producer
        self._redis = redis

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
    )
    async def publish(self, event: BaseEvent) -> None:
        """
        Publish an event to Kafka.

        Idempotency check: if we've already published an event with this
        idempotency key (event_id), skip. This handles client retries safely.
        """
        idem_key = f"published:{event.event_id}"
        already_published = await self._redis.exists(idem_key)
        if already_published:
            logger.info("skipping_duplicate_event", event_id=event.event_id)
            return

        try:
            await self._producer.send_and_wait(
                topic=settings.kafka_topic_orders,
                key=event.aggregate_id.encode(),   # partition key = order_id
                value=event.to_kafka_bytes(),
                headers=[
                    ("event_type", event.event_type.value.encode()),
                    ("correlation_id", event.correlation_id.encode()),
                ],
            )
            # Mark as published — TTL 24h (long enough to catch retries)
            await self._redis.setex(idem_key, 86400, "1")
            logger.info(
                "event_published",
                event_id=event.event_id,
                event_type=event.event_type,
                order_id=event.aggregate_id,
                version=event.aggregate_version,
            )
        except KafkaError as e:
            logger.error("kafka_publish_failed", error=str(e), event_id=event.event_id)
            raise
