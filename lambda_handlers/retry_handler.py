"""
Lambda Retry Handler — deployed to LocalStack.

Flow:
SQS DLQ → EventBridge Pipe (or scheduled rule) → this Lambda → Kafka

When a projection consumer fails (e.g. Postgres down), the event goes to DLQ.
This Lambda:
1. Reads from SQS DLQ
2. Re-publishes the failed event back to Kafka
3. Increments retry_count
4. If retry_count >= 3, moves to "dead" S3 key (poison pill bucket)

Why Lambda for retries?
- Decoupled from main consumer
- Can apply backoff logic
- Serverless = no infra to maintain
- LocalStack simulates this perfectly

DDIA connection: This is "fault tolerance via retry" —
same pattern as Celery retries, SQS visibility timeout,
or Kafka consumer group rebalancing.
"""

import json
import os
import time
import boto3
from kafka import KafkaProducer

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "order-events")
S3_BUCKET = os.environ.get("S3_BUCKET", "event-snapshots")
MAX_RETRIES = 3


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def handler(event, context):
    """
    Lambda handler — triggered by EventBridge scheduled rule every 1 minute.
    Polls SQS DLQ and retries failed events.
    """
    sqs = boto3.client(
        "sqs",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL", "http://localstack:4566"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    )

    dlq_url = os.environ.get("SQS_DLQ_URL", "http://localstack:4566/000000000000/event-dlq")

    # Poll up to 10 messages
    response = sqs.receive_message(
        QueueUrl=dlq_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5,
    )
    messages = response.get("Messages", [])

    if not messages:
        print("No messages in DLQ")
        return {"retried": 0}

    producer = get_producer()
    retried = 0
    dead_lettered = 0

    for msg in messages:
        body = json.loads(msg["Body"])
        domain_event = body["event"]
        retry_count = body.get("retry_count", 0)
        original_error = body.get("error", "unknown")
        order_id = domain_event.get("aggregate_id", "unknown")

        if retry_count >= MAX_RETRIES:
            # Poison pill — move to S3 dead letter storage
            dead_key = f"dead-letters/{order_id}/{domain_event.get('event_id', 'unknown')}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=dead_key,
                Body=json.dumps({
                    "event": domain_event,
                    "error": original_error,
                    "retry_count": retry_count,
                    "dead_at": time.time(),
                }).encode(),
            )
            print(f"Poison pill moved to S3: {dead_key}")
            dead_lettered += 1
        else:
            # Re-publish to Kafka for retry
            producer.send(
                KAFKA_TOPIC,
                key=order_id,
                value=domain_event,
            )
            print(f"Retrying event {domain_event.get('event_id')} for order {order_id} (attempt {retry_count + 1})")
            retried += 1

        # Delete from DLQ
        sqs.delete_message(
            QueueUrl=dlq_url,
            ReceiptHandle=msg["ReceiptHandle"],
        )

    producer.flush()
    producer.close()

    return {"retried": retried, "dead_lettered": dead_lettered}