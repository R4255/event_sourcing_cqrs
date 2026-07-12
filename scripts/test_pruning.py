#!/usr/bin/env python3
import time
import uuid
import httpx
from pymongo import MongoClient

BASE = "http://localhost:8000"
MONGO_URI = "mongodb://event_user:event_pass@localhost:27017/event_store?authSource=admin"

def verify_pruning():
    client = httpx.Client(base_url=BASE, timeout=10)
    idem_key = str(uuid.uuid4())
    
    print("\n--- Starting Event Pruning Test ---\n")
    
    # 1. Create order
    print("1. Creating order...")
    resp = client.post("/commands/orders", json={
        "customer_id": "cust-prune-test",
        "customer_email": "prune@test.com",
        "idempotency_key": idem_key
    })
    assert resp.status_code == 201
    order_id = resp.json()["aggregate_id"]
    print(f"   Created order: {order_id}")
    
    # 2. Add an item
    print("2. Adding item...")
    resp = client.post(f"/commands/orders/{order_id}/items", json={
        "product_id": "prod-x-999",
        "product_name": "Premium Polish",
        "quantity": 1,
        "unit_price": 450.00
    })
    assert resp.status_code == 200
    
    # 3. Cancel the order (Terminal state)
    print("3. Cancelling order (terminal state)...")
    resp = client.post(f"/commands/orders/{order_id}/cancel", json={
        "reason": "Test pruning of event store"
    })
    assert resp.status_code == 200
    
    print("4. Waiting for consumer to process events...")
    time.sleep(1.5)
    
    # 5. Connect directly to MongoDB and inspect the documents
    print("5. Inspecting events in MongoDB...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["event_store"]
    col = db["events"]
    
    events = list(col.find({"aggregate_id": order_id}))
    assert len(events) == 3, f"Expected 3 events in DB, found {len(events)}"
    
    for ev in events:
        expires_at = ev.get("expires_at")
        print(f"   Event {ev.get('event_type')} (v{ev.get('aggregate_version')}): expires_at = {expires_at}")
        assert expires_at is not None, "Error: expires_at is missing!"
        
    print("\n✓ SUCCESS: All events for the cancelled order have an expires_at TTL stamp populated!")
    mongo_client.close()

if __name__ == "__main__":
    verify_pruning()
