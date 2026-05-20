from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Kafka / Redpanda
    redpanda_brokers: str = "localhost:9092"
    kafka_topic_orders: str = "order-events"
    kafka_topic_dlq: str = "order-events-dlq"
    kafka_consumer_group: str = "cqrs-projection-group"
    kafka_archiver_group: str = "cqrs-archiver-group"

    # Postgres
    postgres_dsn: str = "postgresql+asyncpg://cqrs_user:cqrs_pass@localhost:5432/orders_db"

    # Redis
    redis_url: str = "redis://localhost:6379"
    redis_cache_ttl: int = 3600  # 1 hour

    # MongoDB
    mongo_uri: str = "mongodb://event_user:event_pass@localhost:27017/event_store?authSource=admin"
    mongo_db: str = "event_store"
    mongo_collection: str = "events"

    # AWS / LocalStack
    aws_endpoint_url: str = "http://localhost:4566"
    aws_default_region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    s3_bucket: str = "event-snapshots"
    sqs_dlq_url: str = "http://localhost:4566/000000000000/event-dlq"
    eventbridge_bus: str = "event-retry-bus"

    # Archiver
    snapshot_batch_size: int = 100  # archive to S3 every 100 events

    class Config:
        env_file = ".env"


settings = Settings()
