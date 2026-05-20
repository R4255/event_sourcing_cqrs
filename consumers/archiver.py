"""
Event Archiver — Kafka → S3 (LocalStack).

Why archive to S3?
Kafka has finite retention (e.g. 7 days by default).
S3 is infinite, cheap cold storage.
If we need to replay events from 6 months ago → read from S3.

Pattern:
- Consume events in batches of N (default: 100)
- Serialize batch as newline-delimited JSON (NDJSON)
- Upload to S3: event-snapshots/year=YYYY/month=MM/day=DD/batch-{offset}.ndjson
- Partitioned by date → efficient querying (Athena, Spark, etc.)

This is the "Lambda Architecture" cold path:
  Hot path  → Kafka → consumers → Postgres/Redis (real-time)
  Cold path → Kafka → archiver  → S3 (historical, replayable)

DDIA connection:
- S3 as an immutable log — same idea as Kafka but for cold storage
- Date partitioning = partition pruning in analytics queries
- NDJSON = row-oriented, append-friendly (like a log file)
"""

import asyncio
import io
import json
import time
import structlog
import boto3
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from config import settings

logger = structlog.get_logger()

s3 = boto3.client(
    "s3",
    endpoint_url=settings.aws_endpoint_url,
    region_name=settings.aws_default_region,
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
)


def ensure_bucket():
    try:
        existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if settings.s3_bucket not in existing:
            s3.create_bucket(Bucket=settings.s3_bucket)
    except Exception as e:
        logger.warning("bucket_ensure_failed", error=str(e))


def upload_batch_to_s3(events: list[dict], last_offset: int) -> str:
    """
    Upload batch as NDJSON to S3 with date-partitioned key.
    Returns the S3 key.
    """
    now = datetime.utcnow()
    s3_key = (
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch-offset-{last_offset}-ts-{int(time.time())}.ndjson"
    )

    ndjson = "\n".join(json.dumps(e) for e in events)
    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=s3_key,
        Body=ndjson.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={
            "event_count": str(len(events)),
            "last_offset": str(last_offset),
        },
    )
    return s3_key


async def archive():
    logger.info("archiver_starting")
    ensure_bucket()

    consumer = AIOKafkaConsumer(
        settings.kafka_topic_orders,
        bootstrap_servers=settings.redpanda_brokers,
        group_id=settings.kafka_archiver_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    await consumer.start()
    logger.info("archiver_consumer_started")

    batch: list[dict] = []
    last_offset = 0

    try:
        async for msg in consumer:
            event = json.loads(msg.value)
            batch.append(event)
            last_offset = msg.offset

            if len(batch) >= settings.snapshot_batch_size:
                try:
                    s3_key = upload_batch_to_s3(batch, last_offset)
                    await consumer.commit()
                    logger.info(
                        "batch_archived",
                        s3_key=s3_key,
                        count=len(batch),
                        last_offset=last_offset,
                    )
                    batch = []
                except Exception as e:
                    logger.error("archive_upload_failed", error=str(e))
                    # Don't commit — retry on restart

    finally:
        # Archive remaining events on shutdown
        if batch:
            try:
                s3_key = upload_batch_to_s3(batch, last_offset)
                await consumer.commit()
                logger.info("final_batch_archived", s3_key=s3_key, count=len(batch))
            except Exception as e:
                logger.error("final_archive_failed", error=str(e))

        await consumer.stop()
        logger.info("archiver_stopped")


if __name__ == "__main__":
    asyncio.run(archive())
