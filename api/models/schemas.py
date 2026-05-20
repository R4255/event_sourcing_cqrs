from typing import Any
from pydantic import BaseModel, EmailStr, Field


# ─── Commands (write side) ───────────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    customer_id: str
    customer_email: str
    idempotency_key: str = Field(
        description="Client-generated unique key. Same key = same order, no duplicates."
    )


class AddItemRequest(BaseModel):
    product_id: str
    product_name: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(gt=0)


class ProcessPaymentRequest(BaseModel):
    payment_id: str
    amount: float = Field(gt=0)
    method: str = Field(examples=["CARD", "UPI", "NETBANKING"])


class ShipOrderRequest(BaseModel):
    tracking_id: str
    carrier: str


class CancelOrderRequest(BaseModel):
    reason: str


# ─── Responses ───────────────────────────────────────────────────────────────

class CommandResponse(BaseModel):
    event_id: str
    aggregate_id: str
    aggregate_version: int
    message: str


class OrderItemView(BaseModel):
    product_id: str
    product_name: str
    quantity: int
    unit_price: float
    line_total: float


class OrderView(BaseModel):
    """
    This is the QUERY side read model — projected from events.
    Notice it's denormalized and flat — perfect for fast reads.
    CQRS: reads and writes have different models.
    """
    order_id: str
    customer_id: str
    customer_email: str
    status: str
    items: list[OrderItemView]
    total_amount: float
    version: int
    created_at: float | None
    updated_at: float | None
    payment_id: str | None = None
    tracking_id: str | None = None


class EventHistoryItem(BaseModel):
    event_id: str
    event_type: str
    aggregate_version: int
    occurred_at: float
    payload: dict[str, Any]
    correlation_id: str


class HealthResponse(BaseModel):
    status: str
    kafka: str
    postgres: str
    redis: str
    mongo: str
    localstack: str
