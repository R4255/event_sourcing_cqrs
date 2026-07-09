"""
Query Router — the READ side of CQRS.

Reads come from projected read models — NOT from event store.
This is the key insight: reads are fast, denormalized, purpose-built.

Read path:
1. Check Redis cache first (sub-millisecond)
2. Miss → query Postgres read model
3. Cache result in Redis
4. Never touch MongoDB event store for reads

The read model is eventually consistent with the event store.
After a command, Kafka consumer will update Postgres + Redis async.
Typical lag: <100ms in local setup.

DDIA connection: Read replicas, materialized views, CQRS are all
the same idea — maintain a separate, optimized representation of data
for read-heavy workloads.
"""

import orjson
import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from api.models.schemas import EventHistoryItem, OrderView
from api.services.event_store import EventStore
from config import settings
from dependencies import get_event_store, get_postgres, get_redis

logger = structlog.get_logger()
router = APIRouter(prefix="/queries", tags=["Queries (Read Side)"])


@router.get("/orders/{order_id}", response_model=OrderView)
async def get_order(
    order_id: str,
    redis=Depends(get_redis),
    pg=Depends(get_postgres),
):
    """
    Get current order state from read model.
    Cache-aside pattern: Redis → Postgres → cache result.
    """
    # 1. Redis cache check
    cache_key = f"order:{order_id}"
    cached = await redis.get(cache_key)
    if cached:
        return JSONResponse(content=orjson.loads(cached))

    # 2. Postgres read model
    row = await pg.fetchrow(
        """
        SELECT o.order_id, o.customer_id, o.customer_email, o.status,
               o.total_amount, o.version, o.payment_id, o.tracking_id,
               o.created_at, o.updated_at,
               COALESCE(
                 json_agg(
                   json_build_object(
                     'product_id', oi.product_id,
                     'product_name', oi.product_name,
                     'quantity', oi.quantity,
                     'unit_price', oi.unit_price,
                     'line_total', oi.line_total
                   )
                 ) FILTER (WHERE oi.product_id IS NOT NULL),
                 '[]'
               ) AS items
        FROM orders o
        LEFT JOIN order_items oi ON oi.order_id = o.order_id
        WHERE o.order_id = $1
        GROUP BY o.order_id
        """,
        order_id,
    )

    if not row:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")

    result = dict(row)
    result["items"] = result["items"] if isinstance(result["items"], list) else orjson.loads(result["items"])
    result["created_at"] = result["created_at"].timestamp() if result["created_at"] else None
    result["updated_at"] = result["updated_at"].timestamp() if result["updated_at"] else None

    # 3. Cache it
    await redis.setex(cache_key, settings.redis_cache_ttl, orjson.dumps(result, default=float))

    return result


@router.get("/orders/{order_id}/events", response_model=list[EventHistoryItem])
async def get_order_events(
    order_id: str,
    event_store: EventStore = Depends(get_event_store),
):
    """
    Full audit trail — every event that ever happened to this order.
    This is one of the killer features of Event Sourcing.
    """
    events = await event_store.get_events(order_id)
    if not events:
        raise HTTPException(status_code=404, detail=f"Order {order_id} not found")
    return events


@router.get("/orders", response_model=list[OrderView])
async def list_orders(
    status: str | None = None,
    customer_id: str | None = None,
    limit: int = 20,
    offset: int = 0,
    pg=Depends(get_postgres),
    redis=Depends(get_redis),
):
    """List orders with optional filters. Direct Postgres query."""
    conditions = []
    params = []
    idx = 1

    if status:
        conditions.append(f"o.status = ${idx}")
        params.append(status.upper())
        idx += 1
    if customer_id:
        conditions.append(f"o.customer_id = ${idx}")
        params.append(customer_id)
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    params.extend([limit, offset])

    rows = await pg.fetch(
        f"""
        SELECT o.order_id, o.customer_id, o.customer_email, o.status,
               o.total_amount, o.version, o.payment_id, o.tracking_id,
               o.created_at, o.updated_at,
               COALESCE(
                 json_agg(
                   json_build_object(
                     'product_id', oi.product_id,
                     'product_name', oi.product_name,
                     'quantity', oi.quantity,
                     'unit_price', oi.unit_price,
                     'line_total', oi.line_total
                   )
                 ) FILTER (WHERE oi.product_id IS NOT NULL),
                 '[]'
               ) AS items
        FROM orders o
        LEFT JOIN order_items oi ON oi.order_id = o.order_id
        {where}
        GROUP BY o.order_id
        ORDER BY o.created_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
    )

    result = []
    for row in rows:
        r = dict(row)
        r["items"] = r["items"] if isinstance(r["items"], list) else orjson.loads(r["items"])
        r["created_at"] = r["created_at"].timestamp() if r["created_at"] else None
        r["updated_at"] = r["updated_at"].timestamp() if r["updated_at"] else None
        result.append(r)

    return result


@router.get("/stats")
async def get_stats(pg=Depends(get_postgres)):
    """Aggregate stats across all orders."""
    row = await pg.fetchrow(
        """
        SELECT
            COUNT(*) as total_orders,
            COUNT(*) FILTER (WHERE status = 'PENDING') as pending,
            COUNT(*) FILTER (WHERE status = 'CONFIRMED') as confirmed,
            COUNT(*) FILTER (WHERE status = 'PAID') as paid,
            COUNT(*) FILTER (WHERE status = 'SHIPPED') as shipped,
            COUNT(*) FILTER (WHERE status = 'DELIVERED') as delivered,
            COUNT(*) FILTER (WHERE status = 'CANCELLED') as cancelled,
            COALESCE(SUM(total_amount) FILTER (WHERE status = 'PAID'), 0) as total_revenue
        FROM orders
        """
    )
    return dict(row)
