"""
Order Aggregate — holds business rules and version state.

In Event Sourcing, the aggregate is rebuilt by replaying events from
the event store (MongoDB). We don't store "current state" — we compute it.

DDIA connection: This is exactly how databases work internally.
State = fold(initial_state, events). Same as log compaction:
the latest snapshot is just a compact representation of all prior events.

Optimistic concurrency: version field prevents two concurrent writers
from both thinking they're at version N. The one that goes second will
get a version conflict and must retry. No distributed locks needed.
"""

from dataclasses import dataclass, field
from typing import Any

from api.models.events import EventType, OrderStatus


@dataclass
class OrderAggregate:
    order_id: str
    version: int = 0
    status: OrderStatus = OrderStatus.PENDING
    customer_id: str = ""
    customer_email: str = ""
    items: list[dict[str, Any]] = field(default_factory=list)
    total_amount: float = 0.0
    payment_id: str | None = None
    tracking_id: str | None = None

    @classmethod
    def rebuild_from_events(cls, events: list[dict]) -> "OrderAggregate":
        """
        Replay all events for an order to reconstruct current state.
        This is the core of Event Sourcing.
        O(n) where n = number of events per order.
        Production: use snapshots to bound this (see archiver).
        """
        if not events:
            raise ValueError("Cannot rebuild aggregate from empty event list")

        agg = cls(order_id=events[0]["aggregate_id"])
        for ev in sorted(events, key=lambda e: e["aggregate_version"]):
            agg._apply(ev)
        return agg

    def _apply(self, event: dict) -> None:
        """Mutate state based on event type."""
        etype = event["event_type"]
        payload = event["payload"]
        self.version = event["aggregate_version"]

        if etype == EventType.ORDER_CREATED:
            self.customer_id = payload["customer_id"]
            self.customer_email = payload["customer_email"]
            self.status = OrderStatus.PENDING

        elif etype == EventType.ORDER_ITEM_ADDED:
            self.items.append({
                "product_id": payload["product_id"],
                "product_name": payload["product_name"],
                "quantity": payload["quantity"],
                "unit_price": payload["unit_price"],
                "line_total": payload["line_total"],
            })
            self.total_amount += payload["line_total"]

        elif etype == EventType.ORDER_CONFIRMED:
            self.status = OrderStatus.CONFIRMED

        elif etype == EventType.ORDER_PAYMENT_PROCESSED:
            self.status = OrderStatus.PAID
            self.payment_id = payload["payment_id"]

        elif etype == EventType.ORDER_SHIPPED:
            self.status = OrderStatus.SHIPPED
            self.tracking_id = payload["tracking_id"]

        elif etype == EventType.ORDER_DELIVERED:
            self.status = OrderStatus.DELIVERED

        elif etype == EventType.ORDER_CANCELLED:
            self.status = OrderStatus.CANCELLED

    def assert_can_add_item(self):
        if self.status not in (OrderStatus.PENDING,):
            raise ValueError(f"Cannot add item to order in status {self.status}")

    def assert_can_confirm(self):
        if not self.items:
            raise ValueError("Cannot confirm empty order")
        if self.status != OrderStatus.PENDING:
            raise ValueError(f"Cannot confirm order in status {self.status}")

    def assert_can_pay(self):
        if self.status != OrderStatus.CONFIRMED:
            raise ValueError(f"Cannot pay order in status {self.status}")

    def assert_can_ship(self):
        if self.status != OrderStatus.PAID:
            raise ValueError(f"Cannot ship unpaid order in status {self.status}")

    def assert_can_cancel(self):
        if self.status in (OrderStatus.SHIPPED, OrderStatus.DELIVERED):
            raise ValueError(f"Cannot cancel order in status {self.status}")
