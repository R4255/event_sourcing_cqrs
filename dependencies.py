"""
Dependency injection for FastAPI.
All resources come from app.state (set in lifespan).
"""

from fastapi import Request

from api.services.event_store import EventStore
from api.services.publisher import KafkaEventPublisher


async def get_redis(request: Request):
    return request.app.state.redis


async def get_postgres(request: Request):
    async with request.app.state.pg_pool.acquire() as conn:
        yield conn


async def get_event_store(request: Request) -> EventStore:
    return request.app.state.event_store


async def get_publisher(request: Request) -> KafkaEventPublisher:
    return request.app.state.publisher
