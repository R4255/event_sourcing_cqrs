# Event Sourcing + CQRS Platform

A production-grade distributed order management system demonstrating **Event Sourcing**, **CQRS**, **exactly-once Kafka processing**, and **cloud-native AWS patterns** — all runnable locally via Docker Compose + LocalStack.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         WRITE SIDE (Commands)                       │
│                                                                     │
│   Client → FastAPI → MongoDB EventStore → Kafka (Redpanda)          │
│             (validate)   (append-only)    (source of truth)         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────┴──────────────┐
                    ▼                            ▼
         ┌─────────────────┐          ┌──────────────────┐
         │  CQRS Consumer  │          │  S3 Archiver     │
         │  (projection)   │          │  (cold storage)  │
         └────────┬────────┘          └────────┬─────────┘
                  │                            │
         ┌────────▼────────┐          ┌────────▼─────────┐
         │  PostgreSQL      │          │  S3 (LocalStack) │
         │  (read model)    │          │  NDJSON batches  │
         │  Redis (cache)   │          │  date-partitioned│
         └────────┬────────┘          └──────────────────┘
                  │ on failure
         ┌────────▼────────┐
         │  SQS DLQ        │
         └────────┬────────┘
                  │ EventBridge (1 min schedule)
         ┌────────▼────────┐
         │  Lambda         │
         │  (retry/replay) │
         └─────────────────┘
                                  │
┌─────────────────────────────────────────────────────────────────────┐
│                         READ SIDE (Queries)                         │
│                                                                     │
│   Client → FastAPI → Redis (cache-aside) → PostgreSQL               │
└─────────────────────────────────────────────────────────────────────┘
```

## Tech Stack

| Component | Technology | Role |
|-----------|-----------|------|
| API | FastAPI (async) | Command + Query endpoints |
| Event Broker | Redpanda (Kafka-compatible) | Durable ordered event log |
| Event Store | MongoDB | Append-only audit log |
| Read Model | PostgreSQL | Denormalized query-optimized state |
| Cache | Redis | Hot path, idempotency keys |
| Cold Storage | S3 (LocalStack) | Event archival, snapshots |
| DLQ | SQS (LocalStack) | Failed projection retry queue |
| Scheduler | EventBridge (LocalStack) | Triggers Lambda retry every 1 min |
| Retry | Lambda (LocalStack) | Replays failed events to Kafka |

## Key Concepts Demonstrated

### 1. Event Sourcing
Kafka + MongoDB are the source of truth. PostgreSQL is a *projection* — it can be wiped and rebuilt by replaying all events. State is never stored directly; it's derived by folding events.

### 2. CQRS (Command Query Responsibility Segregation)
Write path and read path are completely separate:
- **Commands** → validate → append to MongoDB → publish to Kafka
- **Queries** → Redis cache → PostgreSQL read model (never touches Kafka/Mongo)

### 3. Exactly-Once Processing
Consumer commits Kafka offset **only after** successful Postgres write. Combined with idempotent `ON CONFLICT DO UPDATE` upserts, the system achieves exactly-once semantics without distributed transactions.

### 4. Optimistic Concurrency Control
`(aggregate_id, aggregate_version)` unique index in MongoDB. Two concurrent writers for the same order at the same version → one fails with a duplicate key error. No distributed locks needed.

### 5. Idempotency Keys
Client sends `idempotency_key` with `CreateOrder`. Server maps it to `order_id` in Redis. Retry with same key → same order returned, no duplicate.

### 6. DLQ + EventBridge + Lambda Retry
Failed projections → SQS DLQ → EventBridge scheduled rule (1 min) → Lambda polls DLQ → re-publishes event to Kafka → consumer retries. After 3 failures → S3 dead letter storage.

### 7. S3 Archival (Lambda/Kappa Architecture)
Every 100 events archived to S3 as date-partitioned NDJSON. Kafka retention = short (hot). S3 = infinite cold storage. Historical replay → read from S3.

## Getting Started

### Prerequisites
- Docker + Docker Compose (or Podman Compose)
- Python 3.12+ (for scripts only)
- `awslocal` CLI: `pip install awscli-local`

### 1. Start all services
```bash
docker compose up -d
```

Wait ~30 seconds for all services to be healthy.

### 2. Provision LocalStack infrastructure
```bash
pip install boto3 httpx
python scripts/setup_localstack.py
```

### 3. Run smoke test
```bash
python scripts/smoke_test.py
```

### 4. Explore
- **FastAPI docs**: http://localhost:8000/docs
- **Redpanda Console**: http://localhost:8080 (see topics, messages, consumer groups)
- **Check S3 archives**: `awslocal s3 ls s3://event-snapshots --recursive`
- **Check DLQ**: `awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/event-dlq --attribute-names All`

## API Reference

### Commands (Write Side)
```
POST /commands/orders                     # Create order (idempotent)
POST /commands/orders/{id}/items          # Add item
POST /commands/orders/{id}/confirm        # Confirm order
POST /commands/orders/{id}/payment        # Process payment
POST /commands/orders/{id}/ship           # Ship order
POST /commands/orders/{id}/cancel         # Cancel order
```

### Queries (Read Side)
```
GET /queries/orders/{id}                  # Get order (Redis → Postgres)
GET /queries/orders/{id}/events           # Full audit trail (MongoDB)
GET /queries/orders?status=PAID           # List with filters
GET /queries/stats                        # System-wide stats
```

## Interview Talking Points

**"Walk me through your event sourcing project"**

> "I built a distributed order management system using Event Sourcing and CQRS. Kafka (Redpanda) is the source of truth — all state changes are immutable events partitioned by order_id, so all events for an order land on the same partition in order. An async consumer projects events into a PostgreSQL read model with idempotent upserts, and a Redis cache-aside layer sits in front. For fault tolerance: failed projections go to SQS DLQ, EventBridge triggers a Lambda every minute to retry, and after 3 failures the event is archived to S3 as a dead letter. I also built a separate archiver consumer that batches events to S3 as date-partitioned NDJSON for cold storage and historical replay."

**"How do you handle exactly-once semantics?"**

> "I use at-least-once delivery from Kafka — manual offset commit only after successful Postgres write. Combined with idempotent upserts (ON CONFLICT DO UPDATE), the net effect is exactly-once. If the consumer crashes mid-projection, the offset isn't committed, so the event replays on restart and the upsert is a no-op."

**"How do you prevent version conflicts?"**

> "Optimistic concurrency — MongoDB has a unique index on (aggregate_id, aggregate_version). Two concurrent writes for the same order at the same version will result in one getting a duplicate key error. No distributed lock needed."

## Project Structure
```
.
├── api/
│   ├── main.py               # FastAPI app + lifespan
│   ├── routers/
│   │   ├── command_router.py  # Write side
│   │   └── query_router.py    # Read side
│   ├── models/
│   │   ├── events.py          # Domain events
│   │   └── schemas.py         # Pydantic schemas
│   └── services/
│       ├── publisher.py       # Kafka producer
│       ├── event_store.py     # MongoDB event store
│       └── aggregate.py       # Order aggregate + business rules
├── consumers/
│   ├── main.py                # Projection consumer (Kafka → Postgres + Redis)
│   └── archiver.py            # S3 archiver consumer
├── lambda_handlers/
│   └── retry_handler.py       # Lambda: DLQ → Kafka retry
├── infra/
│   └── postgres_init.sql      # Read model schema
├── scripts/
│   ├── setup_localstack.py    # Provision AWS resources
│   └── smoke_test.py          # End-to-end test
├── config.py                  # Pydantic settings
├── dependencies.py            # FastAPI DI
├── docker-compose.yml
├── Dockerfile.api
└── requirements.txt
```
# event_sourcing_cqrs
