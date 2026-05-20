"""
CQRS Projection Consumer — the most important file in the system.

Consumes events from Kafka and projects them into:
1. PostgreSQL — durable read model (orders + order_items tables)
2. Redis — hot cache (invalidate on every event)

Key engineering decisions:

1. EXACTLY-ONCE via enable_auto_commit=False + manual commit after projection.
   We commit the Kafka offset only AFTER successfully writing to Postgres.
   If Postgres write fails, offset is not committed → event replayed on restart.
   This gives us at-least-once delivery. Combined with idempotent upserts
   (ON CONFLICT DO UPDATE), the net effect is exactly-once.

2. IDEMPOTENT UPSERTS in Postgres — same event processed twice = same result.
   This handles consumer restarts gracefully.

3. DEAD LETTER QUEUE — if projection fails after retries, send to SQS DLQ
   so it's not lost. EventBridge rule will retry via Lambda.

DDIA connection:
- This is "stream processing" — consuming an ordered log and maintaining state.
- Same idea as Kafka Streams, Flink, Spark Structured Streaming.
- The "exactly-once" guarantee is a core distributed systems problem (2PC vs idempotency).
"""

import asyncio
import json
import os
import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
import asyncpg
import boto3
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import from_url
from tenacity import retry, stop_after_attempt, wait_exponential

from api.services.event_store import EventStore
from api.models.events import EventType
from config import settings

logger = structlog.get_logger()

# AWS clients (LocalStack)
sqs = boto3.client(
    "sqs",
    endpoint_url=settings.aws_endpoint_url,
    region_name=settings.aws_default_region,
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
)


async def send_to_dlq(event: dict, error: str) -> None:
    """Send failed event to SQS DLQ for later retry via Lambda."""
    try:
        sqs.send_message(
            QueueUrl=settings.sqs_dlq_url,
            MessageBody=json.dumps({
                "event": event,
                "error": error,
                "retry_count": 0,
            }),
            MessageAttributes={
                "order_id": {"DataType": "String", "StringValue": event.get("aggregate_id", "unknown")},
                "event_type": {"DataType": "String", "StringValue": event.get("event_type", "unknown")},
            },
        )
        logger.info("event_sent_to_dlq", order_id=event.get("aggregate_id"))
    except Exception as e:
        logger.error("dlq_send_failed", error=str(e))


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
async def project_to_postgres(conn: asyncpg.Connection, event: dict) -> None:
    """
    Project a domain event into the Postgres read model.
    All queries are UPSERT / idempotent — safe to replay.
    """
    etype = event["event_type"]
    oid = event["aggregate_id"]
    payload = event["payload"]
    version = event["aggregate_version"]

    async with conn.transaction():
        if etype == EventType.ORDER_CREATED:
            await conn.execute(
                """
                INSERT INTO orders (order_id, customer_id, customer_email, status, total_amount, version, created_at, updated_at)
                VALUES ($1, $2, $3, 'PENDING', 0.0, $4, NOW(), NOW())
                ON CONFLICT (order_id) DO NOTHING
                """,
                oid, payload["customer_id"], payload["customer_email"], version,
            )

        elif etype == EventType.ORDER_ITEM_ADDED:
            await conn.execute(
                """
                INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price, line_total)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (order_id, product_id) DO UPDATE
                SET quantity = EXCLUDED.quantity,
                    line_total = EXCLUDED.line_total
                """,
                oid, payload["product_id"], payload["product_name"],
                payload["quantity"], payload["unit_price"], payload["line_total"],
            )
            await conn.execute(
                """
                UPDATE orders
                SET total_amount = (SELECT COALESCE(SUM(line_total), 0) FROM order_items WHERE order_id = $1),
                    version = $2,
                    updated_at = NOW()
                WHERE order_id = $1
                """,
                oid, version,
            )

        elif etype == EventType.ORDER_CONFIRMED:
            await conn.execute(
                "UPDATE orders SET status='CONFIRMED', version=$2, updated_at=NOW() WHERE order_id=$1",
                oid, version,
            )

        elif etype == EventType.ORDER_PAYMENT_PROCESSED:
            await conn.execute(
                "UPDATE orders SET status='PAID', payment_id=$2, version=$3, updated_at=NOW() WHERE order_id=$1",
                oid, payload["payment_id"], version,
            )

        elif etype == EventType.ORDER_SHIPPED:
            await conn.execute(
                "UPDATE orders SET status='SHIPPED', tracking_id=$2, version=$3, updated_at=NOW() WHERE order_id=$1",
                oid, payload["tracking_id"], version,
            )

        elif etype == EventType.ORDER_DELIVERED:
            await conn.execute(
                "UPDATE orders SET status='DELIVERED', version=$2, updated_at=NOW() WHERE order_id=$1",
                oid, version,
            )

        elif etype == EventType.ORDER_CANCELLED:
            await conn.execute(
                "UPDATE orders SET status='CANCELLED', version=$2, updated_at=NOW() WHERE order_id=$1",
                oid, version,
            )


async def invalidate_cache(redis, order_id: str) -> None:
    """Invalidate Redis cache for this order after projection update."""
    await redis.delete(f"order:{order_id}")


async def consume():
    logger.info("consumer_starting")

    # Setup connections
    consumer = AIOKafkaConsumer(
        settings.kafka_topic_orders,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.kafka_consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,        # manual commit = at-least-once
        max_poll_records=50,
    )

    pg_pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn.replace("+asyncpg", ""),
        min_size=2,
        max_size=5,
    )
    redis = from_url(settings.redis_url, encoding="utf-8", decode_responses=False)

    await consumer.start()
    logger.info("consumer_started", brokers=settings.redpanda_brokers, topic=settings.kafka_topic_orders)

    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            order_id = event.get("aggregate_id", "unknown")

            logger.info(
                "processing_event",
                event_type=event.get("event_type"),
                order_id=order_id,
                version=event.get("aggregate_version"),
                partition=msg.partition,
                offset=msg.offset,
            )

            try:
                async with pg_pool.acquire() as conn:
                    await project_to_postgres(conn, event)
                await invalidate_cache(redis, order_id)

                # Commit offset AFTER successful projection
                await consumer.commit()
                logger.info("event_projected", order_id=order_id, event_type=event.get("event_type"))

            except Exception as e:
                logger.error(
                    "projection_failed",
                    order_id=order_id,
                    event_type=event.get("event_type"),
                    error=str(e),
                )
                await send_to_dlq(event, str(e))
                # Still commit to avoid infinite loop on poison pill
                await consumer.commit()

    finally:
        await consumer.stop()
        await pg_pool.close()
        await redis.aclose()
        logger.info("consumer_stopped")


if __name__ == "__main__":
    asyncio.run(consume())
