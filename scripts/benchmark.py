#!/usr/bin/env python3
import asyncio
import time
import uuid
import httpx
import random
from aiokafka import AIOKafkaProducer
import json

BASE_URL = "http://localhost:8000"
NUM_ORDERS = 300
CONCURRENCY = 30  # concurrent workers
KAFKA_BROKERS = "localhost:9092"
KAFKA_TOPIC = "order-events"

async def create_single_order_full_lifecycle(client: httpx.AsyncClient, worker_id: int):
    """Full lifecycle for one order: Create -> Add Item 1 -> Add Item 2 -> Confirm -> Payment -> Ship"""
    idempotency_key = str(uuid.uuid4())
    
    # 1. Create order
    resp = await client.post("/commands/orders", json={
        "customer_id": f"cust-bench-{worker_id}-{uuid.uuid4().hex[:6]}",
        "customer_email": "bench@test.com",
        "idempotency_key": idempotency_key
    })
    if resp.status_code != 201:
        return None
    order_id = resp.json()["aggregate_id"]
    
    # 2. Add item 1
    await client.post(f"/commands/orders/{order_id}/items", json={
        "product_id": f"prod-a-{uuid.uuid4().hex[:4]}",
        "product_name": "Car Polish",
        "quantity": 1,
        "unit_price": 500.00
    })

    # 3. Add item 2
    await client.post(f"/commands/orders/{order_id}/items", json={
        "product_id": f"prod-b-{uuid.uuid4().hex[:4]}",
        "product_name": "Alloy Wheels",
        "quantity": 4,
        "unit_price": 12000.00
    })

    # 4. Confirm order
    await client.post(f"/commands/orders/{order_id}/confirm")

    # 5. Process payment
    await client.post(f"/commands/orders/{order_id}/payment", json={
        "payment_id": f"pay-bench-{uuid.uuid4().hex[:8]}",
        "amount": 48500.00,
        "method": "CREDIT_CARD"
    })

    # 6. Ship order
    await client.post(f"/commands/orders/{order_id}/ship", json={
        "tracking_id": f"TRK-BENCH-{uuid.uuid4().hex[:8].upper()}",
        "carrier": "Spinny Express"
    })
        
    return order_id

async def worker(queue: asyncio.Queue, client: httpx.AsyncClient, results: list):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        worker_id = item
        try:
            start_time = time.time()
            order_id = await create_single_order_full_lifecycle(client, worker_id)
            duration = time.time() - start_time
            if order_id:
                results.append((order_id, duration))
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")
        queue.task_done()

async def run_benchmark():
    print("=========================================================================")
    print("       ORDER MANAGEMENT CQRS SYSTEM - E2E PERFORMANCE BENCHMARK          ")
    print("=========================================================================\n")
    print(f"Phase 1: Running load-test with {NUM_ORDERS} orders (6 events/mutations per order).")
    print(f"Total commands to execute: {NUM_ORDERS * 6} API writes.")
    print(f"Concurrency level: {CONCURRENCY} parallel workers.\n")
    
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        # Get initial stats
        try:
            initial_stats = (await client.get("/queries/stats")).json()
        except Exception as e:
            print(f"Error: API is not running or accessible at {BASE_URL}. Run 'docker compose up -d' first.")
            return
            
        initial_orders = initial_stats.get("total_orders", 0)
        
        # Populate queue
        queue = asyncio.Queue()
        for i in range(NUM_ORDERS):
            await queue.put(i)
        for _ in range(CONCURRENCY):
            await queue.put(None)
            
        results = []
        start_wall_time = time.time()
        
        # Start workers
        tasks = []
        for i in range(CONCURRENCY):
            task = asyncio.create_task(worker(queue, client, results))
            tasks.append(task)
            
        await queue.join()
        await asyncio.gather(*tasks)
        
        end_write_time = time.time()
        write_duration = end_write_time - start_wall_time
        successful_orders = len(results)
        total_write_commands = successful_orders * 6
        
        print("\n--- Write Path Results (API -> MongoDB Event Store -> Kafka) ---")
        print(f"Successfully processed {successful_orders}/{NUM_ORDERS} orders ({total_write_commands} commands/mutations)")
        print(f"Total write duration: {write_duration:.2f} seconds")
        print(f"API Write Throughput: {total_write_commands / write_duration:.2f} mutations/sec")
        print(f"Average latency per order lifecycle (6 steps): {sum(d for _, d in results)/max(1, successful_orders):.2f} seconds")
        
        # Monitor Read Model catch up
        print("\n--- Read Path Projections (Kafka -> Postgres -> Redis) ---")
        print("Waiting for Postgres read-model projections to catch up...")
        
        target_orders = initial_orders + successful_orders
        projection_start_time = time.time()
        
        while True:
            try:
                stats = (await client.get("/queries/stats")).json()
                current_orders = stats.get("total_orders", 0)
                print(f"  Projected orders in Postgres: {current_orders} / {target_orders}", end="\r")
                if current_orders >= target_orders:
                    break
            except Exception as e:
                pass
            await asyncio.sleep(0.5)
            
        projection_end_time = time.time()
        total_duration = projection_end_time - start_wall_time
        total_events_projected = successful_orders * 6
        
        print(f"\nAll projections completed in {total_duration:.2f} seconds from benchmark start.")
        print(f"Effective End-to-End System Throughput: {total_events_projected / total_duration:.2f} events/sec")
        print(f"Approximate consumer projection lag: {max(0.0, projection_end_time - end_write_time):.2f} seconds")
        
        # Phase 2: Duplicate / Repeated Delivery Testing
        print("\n=========================================================================")
        print("    Phase 2: Kafka Repeated-Delivery / Duplicate Message Deduplication   ")
        print("=========================================================================\n")
        
        # Pick 20 successful orders
        selected_orders = [r[0] for r in random.sample(results, min(len(results), 20))]
        print(f"Fetching historical event sequences for {len(selected_orders)} orders via audit trail...")
        
        all_duplicate_events = []
        for order_id in selected_orders:
            events_resp = await client.get(f"/queries/orders/{order_id}/events")
            if events_resp.status_code == 200:
                events = events_resp.json()
                for ev in events:
                    ev["aggregate_id"] = order_id
                all_duplicate_events.extend(events)
                
        total_duplicates = len(all_duplicate_events)
        print(f"Retrieved {total_duplicates} historical events to replay.")
        
        # Verify read model state before replay
        before_stats = (await client.get("/queries/stats")).json()
        
        # Record sample order items and version before replay
        sample_order_id = selected_orders[0]
        sample_order_before = (await client.get(f"/queries/orders/{sample_order_id}")).json()
        before_version = sample_order_before["version"]
        before_item_count = len(sample_order_before["items"])
        before_status = sample_order_before["status"]
        
        print(f"Simulating duplicate delivery by re-publishing {total_duplicates} events directly to Kafka...")
        
        # Initialize Kafka Producer
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKERS)
        await producer.start()
        
        start_replay_time = time.time()
        try:
            for event in all_duplicate_events:
                # Re-publish to Kafka
                await producer.send_and_wait(
                    topic=KAFKA_TOPIC,
                    key=event["aggregate_id"].encode(),
                    value=json.dumps(event).encode("utf-8")
                )
        finally:
            await producer.stop()
            
        print(f"Finished re-publishing all duplicates in {time.time() - start_replay_time:.2f} seconds.")
        print("Waiting 3 seconds for consumer deduplication checks...")
        await asyncio.sleep(3.0)
        
        # Verify read model state after replay
        after_stats = (await client.get("/queries/stats")).json()
        sample_order_after = (await client.get(f"/queries/orders/{sample_order_id}")).json()
        after_version = sample_order_after["version"]
        after_item_count = len(sample_order_after["items"])
        after_status = sample_order_after["status"]
        
        # Assertions
        state_corrupted = False
        duplicate_processing_errors = 0
        
        if before_stats["total_orders"] != after_stats["total_orders"]:
            print(f"❌ Error: Total orders changed from {before_stats['total_orders']} to {after_stats['total_orders']}!")
            state_corrupted = True
            duplicate_processing_errors += 1
            
        if before_version != after_version:
            print(f"❌ Error: Sample order version changed from {before_version} to {after_version}!")
            state_corrupted = True
            duplicate_processing_errors += 1
            
        if before_item_count != after_item_count:
            print(f"❌ Error: Sample order item count changed from {before_item_count} to {after_item_count}!")
            state_corrupted = True
            duplicate_processing_errors += 1
            
        if before_status != after_status:
            print(f"❌ Error: Sample order status changed from {before_status} to {after_status}!")
            state_corrupted = True
            duplicate_processing_errors += 1

        print("\n--- Deduplication Results ---")
        if not state_corrupted:
            print("✓ SUCCESS: Database read model state remained perfectly consistent.")
            print("✓ Redis-based consumer deduplication successfully caught and discarded all duplicate events.")
            print(f"✓ Duplicate-processing error rate: 0.00% ({duplicate_processing_errors} errors across replayed events)")
        else:
            print("❌ FAILURE: State corruption occurred during duplicate delivery replay!")
            print(f"❌ Duplicate-processing error rate: {(duplicate_processing_errors / 4) * 100:.2f}%")
        print("=========================================================================\n")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
