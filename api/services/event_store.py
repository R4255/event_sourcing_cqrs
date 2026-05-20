"""
MongoDB Event Store — append-only log of all domain events.

Design:
- Collection: events (capped or time-series optional)
- Index on (aggregate_id, aggregate_version) — unique constraint
  This enforces optimistic concurrency at DB level.
- Index on (aggregate_id, occurred_at) — for fast event replay per order
- Index on event_type — for analytics queries

Why MongoDB here instead of Postgres?
- Flexible payload (dict) — each event type has different payload shape
- Horizontal scale via sharding on aggregate_id (DDIA: hash partitioning)
- Document model fits event shape naturally

DDIA connection: This is the "log" abstraction. Like Kafka, like WAL.
Append-only, ordered by version, replayable.
"""

import structlog
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from config import settings

logger = structlog.get_logger()


class EventStore:
    def __init__(self, client: AsyncIOMotorClient):
        self._db: AsyncIOMotorDatabase = client[settings.mongo_db]
        self._col = self._db[settings.mongo_collection]

    async def setup_indexes(self):
        """Called at startup. Idempotent."""
        await self._col.create_index(
            [("aggregate_id", 1), ("aggregate_version", 1)],
            unique=True,
            name="idx_aggregate_version",
        )
        await self._col.create_index(
            [("aggregate_id", 1), ("occurred_at", 1)],
            name="idx_aggregate_time",
        )
        await self._col.create_index("event_type", name="idx_event_type")
        await self._col.create_index("event_id", unique=True, name="idx_event_id")
        logger.info("event_store_indexes_ready")

    async def append(self, event: dict) -> None:
        """
        Append event. Fails on duplicate (aggregate_id, aggregate_version).
        This is the optimistic concurrency guard — no distributed lock needed.
        """
        try:
            await self._col.insert_one({**event, "_id": event["event_id"]})
        except Exception as e:
            if "duplicate key" in str(e).lower() or "E11000" in str(e):
                logger.warning(
                    "optimistic_concurrency_conflict",
                    order_id=event["aggregate_id"],
                    version=event["aggregate_version"],
                )
                raise ValueError(
                    f"Version conflict: order {event['aggregate_id']} "
                    f"already at version {event['aggregate_version']}"
                )
            raise

    async def get_events(self, aggregate_id: str) -> list[dict]:
        """Load all events for an order, sorted by version."""
        cursor = self._col.find(
            {"aggregate_id": aggregate_id},
            {"_id": 0},
        ).sort("aggregate_version", 1)
        return await cursor.to_list(length=None)

    async def get_latest_version(self, aggregate_id: str) -> int:
        """Get current version of an aggregate. 0 means doesn't exist."""
        doc = await self._col.find_one(
            {"aggregate_id": aggregate_id},
            {"aggregate_version": 1},
            sort=[("aggregate_version", -1)],
        )
        return doc["aggregate_version"] if doc else 0

    async def get_events_by_type(self, event_type: str, limit: int = 100) -> list[dict]:
        cursor = self._col.find(
            {"event_type": event_type},
            {"_id": 0},
        ).sort("occurred_at", -1).limit(limit)
        return await cursor.to_list(length=None)
