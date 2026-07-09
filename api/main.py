"""
FastAPI Application — entry point.

Lifespan pattern (FastAPI 0.95+):
- startup: init all connections, create Kafka topic, setup indexes
- shutdown: graceful close of all connections

All shared resources stored on app.state to avoid globals.
"""

import asyncpg
import boto3
import structlog
from aiokafka import AIOKafkaProducer
from contextlib import asynccontextmanager
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis, from_url

from api.routers import command_router, query_router
from api.services.event_store import EventStore
from api.services.publisher import KafkaEventPublisher
from config import settings

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("starting_up")

    # Kafka Producer
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.redpanda_brokers,
        acks="all",                    # wait for all ISR replicas
        enable_idempotence=True,       # exactly-once producer semantics
        compression_type="lz4",
    )
    await producer.start()
    app.state.producer = producer

    # Redis
    redis: Redis = from_url(settings.redis_url, encoding="utf-8", decode_responses=False)
    app.state.redis = redis

    # Postgres (asyncpg connection pool)
    pg_pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn.replace("+asyncpg", ""),
        min_size=2,
        max_size=10,
    )
    app.state.pg_pool = pg_pool

    # MongoDB
    mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    event_store = EventStore(mongo_client)
    await event_store.setup_indexes()
    app.state.event_store = event_store
    app.state.mongo_client = mongo_client

    # Publisher (combines producer + redis for idempotency)
    app.state.publisher = KafkaEventPublisher(producer, redis)

    # LocalStack S3 — ensure bucket exists
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=settings.aws_endpoint_url,
            region_name=settings.aws_default_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if settings.s3_bucket not in existing:
            s3.create_bucket(Bucket=settings.s3_bucket)
            logger.info("s3_bucket_created", bucket=settings.s3_bucket)
    except Exception as e:
        logger.warning("s3_setup_failed", error=str(e))

    logger.info("startup_complete")
    yield

    # Shutdown
    await producer.stop()
    await redis.aclose()
    await pg_pool.close()
    mongo_client.close()
    logger.info("shutdown_complete")


app = FastAPI(
    title="Event Sourcing + CQRS Platform",
    description="""
    ## Distributed Order Management via Event Sourcing + CQRS

    **Write side (Commands):** All state changes published as immutable events to Kafka.
    Kafka is the source of truth.

    **Read side (Queries):** Consumers project events into Postgres read model + Redis cache.
    Reads are fast, denormalized, eventually consistent.

    **Event Store:** MongoDB — append-only audit log of every event ever.

    **Archival:** Events batch-archived to S3 (LocalStack) every 100 events.

    **Fault tolerance:** Failed projections → SQS DLQ → EventBridge → Lambda retry.
    """,
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(command_router.router)
app.include_router(query_router.router)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "service": "event-sourcing-cqrs"}
