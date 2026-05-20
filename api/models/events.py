"""
Domain Events — the immutable facts of what happened in the system.
In Event Sourcing, these ARE the source of truth. The DB is just a projection.

DDIA connection: This is the "event log" pattern — append-only, immutable,
replayable. Same idea as Kafka's log, Postgres WAL, or Dynamo Streams.
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Any
from uuid import uuid4

import orjson
from pydantic import BaseModel, Field


class EventType(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_ITEM_ADDED = "ORDER_ITEM_ADDED"
    ORDER_CONFIRMED = "ORDER_CONFIRMED"
    ORDER_PAYMENT_PROCESSED = "ORDER_PAYMENT_PROCESSED"
    ORDER_SHIPPED = "ORDER_SHIPPED"
    ORDER_DELIVERED = "ORDER_DELIVERED"
    ORDER_CANCELLED = "ORDER_CANCELLED"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    PAID = "PAID"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class BaseEvent(BaseModel):
    """
    Every event in the system inherits from this.
    - event_id: globally unique (UUID4), used for idempotency checks
    - aggregate_id: the order_id — partitioning key in Kafka
    - aggregate_version: monotonically increasing per aggregate (optimistic locking)
    - occurred_at: epoch ms — wall clock when event happened
    - correlation_id: trace ID across services (like a request ID)
    """
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    aggregate_id: str          # order_id
    aggregate_version: int
    occurred_at: float = Field(default_factory=time.time)
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    payload: dict[str, Any]

    def to_kafka_bytes(self) -> bytes:
        return orjson.dumps(self.model_dump())

    @classmethod
    def from_kafka_bytes(cls, data: bytes) -> "BaseEvent":
        return cls(**orjson.loads(data))


# ─── Concrete Event Factories ────────────────────────────────────────────────

def order_created_event(
    order_id: str,
    customer_id: str,
    customer_email: str,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_CREATED,
        aggregate_id=order_id,
        aggregate_version=1,
        payload={
            "customer_id": customer_id,
            "customer_email": customer_email,
            "items": [],
            "total_amount": 0.0,
        },
    )


def order_item_added_event(
    order_id: str,
    version: int,
    product_id: str,
    product_name: str,
    quantity: int,
    unit_price: float,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_ITEM_ADDED,
        aggregate_id=order_id,
        aggregate_version=version,
        payload={
            "product_id": product_id,
            "product_name": product_name,
            "quantity": quantity,
            "unit_price": unit_price,
            "line_total": quantity * unit_price,
        },
    )


def order_confirmed_event(order_id: str, version: int) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_CONFIRMED,
        aggregate_id=order_id,
        aggregate_version=version,
        payload={"confirmed_at": time.time()},
    )


def order_payment_processed_event(
    order_id: str,
    version: int,
    payment_id: str,
    amount: float,
    method: str,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_PAYMENT_PROCESSED,
        aggregate_id=order_id,
        aggregate_version=version,
        payload={
            "payment_id": payment_id,
            "amount": amount,
            "method": method,
            "processed_at": time.time(),
        },
    )


def order_shipped_event(
    order_id: str,
    version: int,
    tracking_id: str,
    carrier: str,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_SHIPPED,
        aggregate_id=order_id,
        aggregate_version=version,
        payload={
            "tracking_id": tracking_id,
            "carrier": carrier,
            "shipped_at": time.time(),
        },
    )


def order_cancelled_event(
    order_id: str,
    version: int,
    reason: str,
) -> BaseEvent:
    return BaseEvent(
        event_type=EventType.ORDER_CANCELLED,
        aggregate_id=order_id,
        aggregate_version=version,
        payload={"reason": reason, "cancelled_at": time.time()},
    )
