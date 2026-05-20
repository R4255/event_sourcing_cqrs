#!/usr/bin/env python3
"""
End-to-end smoke test.
Walks through the full order lifecycle and verifies each step.

Usage:
    python scripts/smoke_test.py
"""

import json
import time
import uuid
import httpx

BASE = "http://localhost:8000"


def pr(label: str, resp: httpx.Response):
    status_icon = "✓" if resp.status_code < 300 else "✗"
    print(f"{status_icon} [{resp.status_code}] {label}")
    data = resp.json()
    print(f"  → {json.dumps(data, indent=2)[:300]}")
    return data


def run():
    client = httpx.Client(base_url=BASE, timeout=10)
    idempotency_key = str(uuid.uuid4())

    print("\n=== Event Sourcing + CQRS Smoke Test ===\n")

    # 1. Create order
    resp = client.post("/commands/orders", json={
        "customer_id": "cust-rohit-001",
        "customer_email": "rohit@spinny.com",
        "idempotency_key": idempotency_key,
    })
    order_data = pr("CREATE ORDER", resp)
    order_id = order_data["aggregate_id"]

    # 2. Idempotency check — same key, should return same order
    resp2 = client.post("/commands/orders", json={
        "customer_id": "cust-rohit-001",
        "customer_email": "rohit@spinny.com",
        "idempotency_key": idempotency_key,
    })
    data2 = pr("CREATE ORDER (duplicate key — should be idempotent)", resp2)
    assert data2["aggregate_id"] == order_id, "Idempotency broken!"
    print("  ✓ Idempotency confirmed — same order_id returned")

    time.sleep(0.5)

    # 3. Add items
    resp = client.post(f"/commands/orders/{order_id}/items", json={
        "product_id": "prod-swift-001",
        "product_name": "Maruti Swift VXI 2022",
        "quantity": 1,
        "unit_price": 650000.00,
    })
    pr("ADD ITEM (car)", resp)

    resp = client.post(f"/commands/orders/{order_id}/items", json={
        "product_id": "prod-insurance-001",
        "product_name": "1 Year Insurance",
        "quantity": 1,
        "unit_price": 15000.00,
    })
    pr("ADD ITEM (insurance)", resp)

    time.sleep(0.5)

    # 4. Confirm
    resp = client.post(f"/commands/orders/{order_id}/confirm")
    pr("CONFIRM ORDER", resp)

    # 5. Process payment
    resp = client.post(f"/commands/orders/{order_id}/payment", json={
        "payment_id": f"pay-{uuid.uuid4()}",
        "amount": 665000.00,
        "method": "NETBANKING",
    })
    pr("PROCESS PAYMENT", resp)

    # 6. Ship
    resp = client.post(f"/commands/orders/{order_id}/ship", json={
        "tracking_id": f"TRK-{uuid.uuid4().hex[:8].upper()}",
        "carrier": "Spinny Logistics",
    })
    pr("SHIP ORDER", resp)

    time.sleep(1)  # Allow consumer to project

    # 7. Query read model
    print("\n--- Query Side (Read Model) ---\n")
    resp = client.get(f"/queries/orders/{order_id}")
    pr("GET ORDER (Postgres read model)", resp)

    # 8. Full audit trail
    resp = client.get(f"/queries/orders/{order_id}/events")
    events = resp.json()
    print(f"\n✓ AUDIT TRAIL — {len(events)} events for order {order_id}")
    for ev in events:
        print(f"  v{ev['aggregate_version']} | {ev['event_type']} | {ev['event_id'][:8]}...")

    # 9. Stats
    resp = client.get("/queries/stats")
    pr("SYSTEM STATS", resp)

    print("\n=== Smoke test complete ===")
    print(f"\nOrder ID: {order_id}")
    print("Check Redpanda console: http://localhost:8080")
    print("Check S3 archival:      awslocal s3 ls s3://event-snapshots --recursive")
    print("Check SQS DLQ:          awslocal sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/event-dlq --attribute-names All")


if __name__ == "__main__":
    run()
