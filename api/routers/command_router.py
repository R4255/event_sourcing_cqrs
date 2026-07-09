"""
Command Router — the WRITE side of CQRS.

Commands mutate state. Each command:
1. Loads the current aggregate version from event store
2. Validates business rules
3. Creates a new event
4. Persists to MongoDB event store (source of truth)
5. Publishes to Kafka (triggers projections)

CQRS principle: writes go through here, reads go through query_router.
These can scale independently. Write side can be synchronous and consistent.
Read side can be eventually consistent and horizontally scaled.
"""

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from uuid import uuid4

from api.models.events import (
    order_cancelled_event,
    order_confirmed_event,
    order_created_event,
    order_item_added_event,
    order_payment_processed_event,
    order_shipped_event,
)
from api.models.schemas import (
    AddItemRequest,
    CancelOrderRequest,
    CommandResponse,
    CreateOrderRequest,
    ProcessPaymentRequest,
    ShipOrderRequest,
)
from api.services.aggregate import OrderAggregate
from api.services.event_store import EventStore
from api.services.publisher import KafkaEventPublisher
from api.services.payment_gateway import payment_gateway
from dependencies import get_event_store, get_publisher, get_redis

logger = structlog.get_logger()
router = APIRouter(prefix="/commands", tags=["Commands (Write Side)"])


async def _load_aggregate(order_id: str, event_store: EventStore) -> OrderAggregate:
    events = await event_store.get_events(order_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return OrderAggregate.rebuild_from_events(events)


@router.post("/orders", response_model=CommandResponse, status_code=status.HTTP_201_CREATED)
async def create_order(
    req: CreateOrderRequest,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
    redis=Depends(get_redis),
):
    """
    Create a new order. Idempotent via idempotency_key.
    Same key from client = same order_id returned, no duplicate.
    """
    # Idempotency: map client key → order_id
    idem_cache_key = f"idem:create:{req.idempotency_key}"
    cached_order_id = await redis.get(idem_cache_key)
    if cached_order_id:
        order_id = cached_order_id.decode()
        logger.info("idempotent_create_order", order_id=order_id)
        version = await event_store.get_latest_version(order_id)
        return CommandResponse(
            event_id="cached",
            aggregate_id=order_id,
            aggregate_version=version,
            message="Order already created (idempotent)",
        )

    order_id = str(uuid4())
    event = order_created_event(order_id, req.customer_id, req.customer_email)

    await event_store.append(event.model_dump())
    await publisher.publish(event)
    await redis.setex(idem_cache_key, 86400, order_id)

    logger.info("order_created", order_id=order_id, customer=req.customer_id)
    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message="Order created",
    )


@router.post("/orders/{order_id}/items", response_model=CommandResponse)
async def add_item(
    order_id: str,
    req: AddItemRequest,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
):
    agg = await _load_aggregate(order_id, event_store)
    agg.assert_can_add_item()

    event = order_item_added_event(
        order_id=order_id,
        version=agg.version + 1,
        product_id=req.product_id,
        product_name=req.product_name,
        quantity=req.quantity,
        unit_price=req.unit_price,
    )
    await event_store.append(event.model_dump())
    await publisher.publish(event)

    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message=f"Item {req.product_name} added",
    )


@router.post("/orders/{order_id}/confirm", response_model=CommandResponse)
async def confirm_order(
    order_id: str,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
):
    agg = await _load_aggregate(order_id, event_store)
    agg.assert_can_confirm()

    event = order_confirmed_event(order_id, agg.version + 1)
    await event_store.append(event.model_dump())
    await publisher.publish(event)

    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message="Order confirmed",
    )


@router.post("/orders/{order_id}/payment", response_model=CommandResponse)
async def process_payment(
    order_id: str,
    req: ProcessPaymentRequest,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
):
    agg = await _load_aggregate(order_id, event_store)
    agg.assert_can_pay()

    # ── STEP 1: SYNC payment gateway call ──────────────────────────────
    # We WAIT here. No Kafka, no event, nothing until we hear back.
    result = await payment_gateway.charge(
        payment_id=req.payment_id,
        amount=req.amount,
        method=req.method,
    )

    if not result["success"]:
        # Payment failed — raise immediately, no event published
        raise HTTPException(
            status_code=402,  # 402 = Payment Required
            detail=f"Payment failed: {result['error']}"
        )

    # ── STEP 2: Only reach here if payment CONFIRMED ────────────────────
    # Now it's safe to record the event and async-project it
    event = order_payment_processed_event(
        order_id, agg.version + 1, req.payment_id, req.amount, req.method
    )
    await event_store.append(event.model_dump())
    await publisher.publish(event)

    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message="Payment processed",
    )

@router.post("/orders/{order_id}/ship", response_model=CommandResponse)
async def ship_order(
    order_id: str,
    req: ShipOrderRequest,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
):
    agg = await _load_aggregate(order_id, event_store)
    agg.assert_can_ship()

    event = order_shipped_event(order_id, agg.version + 1, req.tracking_id, req.carrier)
    await event_store.append(event.model_dump())
    await publisher.publish(event)

    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message=f"Order shipped via {req.carrier}",
    )


@router.post("/orders/{order_id}/cancel", response_model=CommandResponse)
async def cancel_order(
    order_id: str,
    req: CancelOrderRequest,
    event_store: EventStore = Depends(get_event_store),
    publisher: KafkaEventPublisher = Depends(get_publisher),
):
    agg = await _load_aggregate(order_id, event_store)
    agg.assert_can_cancel()

    event = order_cancelled_event(order_id, agg.version + 1, req.reason)
    await event_store.append(event.model_dump())
    await publisher.publish(event)

    return CommandResponse(
        event_id=event.event_id,
        aggregate_id=order_id,
        aggregate_version=event.aggregate_version,
        message="Order cancelled",
    )
