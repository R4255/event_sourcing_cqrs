#!/usr/bin/env python3
"""
LocalStack Infrastructure Setup Script.

Run this once after `docker compose up` to provision all AWS resources:
- S3 bucket for event archival
- SQS queue for DLQ
- EventBridge rule for scheduled Lambda retry
- Lambda function for retry handler
- IAM role for Lambda

Usage:
    python scripts/setup_localstack.py
"""

import boto3
import json
import os
import sys
import time
import zipfile
import io

ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
REGION = "us-east-1"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}


def client(service):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION, **CREDS)


def wait_for_localstack():
    import urllib.request
    print("Waiting for LocalStack...")
    for _ in range(30):
        try:
            urllib.request.urlopen(f"{ENDPOINT}/_localstack/health", timeout=2)
            print("LocalStack is up!")
            return
        except Exception:
            time.sleep(2)
    print("LocalStack not ready, proceeding anyway...")


def setup_s3():
    s3 = client("s3")
    bucket = "event-snapshots"
    try:
        existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            print(f"✓ S3 bucket created: {bucket}")
        else:
            print(f"✓ S3 bucket already exists: {bucket}")
    except Exception as e:
        print(f"✗ S3 setup failed: {e}")


def setup_sqs():
    sqs = client("sqs")
    try:
        resp = sqs.create_queue(
            QueueName="event-dlq",
            Attributes={"MessageRetentionPeriod": "86400"},  # 24h
        )
        print(f"✓ SQS DLQ created: {resp['QueueUrl']}")
        return resp["QueueUrl"]
    except sqs.exceptions.QueueAlreadyExists:
        resp = sqs.get_queue_url(QueueName="event-dlq")
        print(f"✓ SQS DLQ already exists: {resp['QueueUrl']}")
        return resp["QueueUrl"]
    except Exception as e:
        print(f"✗ SQS setup failed: {e}")
        return None


def setup_iam():
    iam = client("iam")
    role_name = "lambda-retry-role"
    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    })
    try:
        resp = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = resp["Role"]["Arn"]
        print(f"✓ IAM role created: {role_arn}")
        return role_arn
    except Exception as e:
        if "already exists" in str(e).lower():
            resp = iam.get_role(RoleName=role_name)
            role_arn = resp["Role"]["Arn"]
            print(f"✓ IAM role already exists: {role_arn}")
            return role_arn
        print(f"✗ IAM setup failed: {e}")
        return "arn:aws:iam::000000000000:role/lambda-retry-role"


def package_lambda():
    """Zip the retry handler for Lambda deployment."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        handler_path = os.path.join(
            os.path.dirname(__file__), "..", "lambda_handlers", "retry_handler.py"
        )
        zf.write(handler_path, "retry_handler.py")
    buf.seek(0)
    return buf.read()


def setup_lambda(role_arn: str, dlq_url: str):
    lmb = client("lambda")
    fn_name = "event-retry-handler"
    try:
        zip_bytes = package_lambda()
        resp = lmb.create_function(
            FunctionName=fn_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler="retry_handler.handler",
            Code={"ZipFile": zip_bytes},
            Environment={
                "Variables": {
                    "KAFKA_BROKERS": "redpanda:29092",
                    "KAFKA_TOPIC": "order-events",
                    "S3_BUCKET": "event-snapshots",
                    "SQS_DLQ_URL": dlq_url or "",
                    "AWS_ENDPOINT_URL": "http://localstack:4566",
                    "AWS_DEFAULT_REGION": "us-east-1",
                    "AWS_ACCESS_KEY_ID": "test",
                    "AWS_SECRET_ACCESS_KEY": "test",
                }
            },
            Timeout=30,
        )
        fn_arn = resp["FunctionArn"]
        print(f"✓ Lambda created: {fn_arn}")
        return fn_arn
    except Exception as e:
        if "already exists" in str(e).lower() or "ResourceConflictException" in str(e):
            resp = lmb.get_function(FunctionName=fn_name)
            fn_arn = resp["Configuration"]["FunctionArn"]
            print(f"✓ Lambda already exists: {fn_arn}")
            return fn_arn
        print(f"✗ Lambda setup failed: {e}")
        return None


def setup_eventbridge(lambda_arn: str):
    """Create EventBridge rule to trigger Lambda every minute for DLQ retry."""
    eb = client("events")
    lmb = client("lambda")

    rule_name = "dlq-retry-rule"
    try:
        resp = eb.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 minute)",
            State="ENABLED",
            Description="Trigger Lambda to retry failed events from DLQ",
        )
        rule_arn = resp["RuleArn"]
        print(f"✓ EventBridge rule created: {rule_arn}")

        if lambda_arn:
            eb.put_targets(
                Rule=rule_name,
                Targets=[{"Id": "RetryLambda", "Arn": lambda_arn}],
            )
            try:
                lmb.add_permission(
                    FunctionName="event-retry-handler",
                    StatementId="eventbridge-invoke",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=rule_arn,
                )
            except Exception:
                pass  # Permission may already exist
            print(f"✓ EventBridge target set to Lambda")
    except Exception as e:
        print(f"✗ EventBridge setup failed: {e}")


def setup_kafka_topic():
    """Create Kafka topic via rpk (Redpanda CLI)."""
    import subprocess
    try:
        result = subprocess.run(
            [
                "docker", "exec", "redpanda",
                "rpk", "topic", "create", "order-events",
                "--partitions", "6",
                "--replicas", "1",
            ],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0 or "already exists" in result.stderr:
            print("✓ Kafka topic 'order-events' ready (6 partitions)")
        else:
            print(f"✗ Topic creation: {result.stderr}")

        # Also create DLQ topic
        subprocess.run(
            [
                "docker", "exec", "redpanda",
                "rpk", "topic", "create", "order-events-dlq",
                "--partitions", "1",
                "--replicas", "1",
            ],
            capture_output=True, text=True, timeout=15,
        )
        print("✓ Kafka topic 'order-events-dlq' ready")
    except Exception as e:
        print(f"✗ Kafka topic setup failed: {e}")


if __name__ == "__main__":
    wait_for_localstack()
    print("\n=== Setting up LocalStack infrastructure ===\n")

    setup_s3()
    dlq_url = setup_sqs()
    role_arn = setup_iam()
    time.sleep(1)
    lambda_arn = setup_lambda(role_arn, dlq_url)
    time.sleep(1)
    setup_eventbridge(lambda_arn)
    setup_kafka_topic()

    print("\n=== Infrastructure setup complete ===")
    print(f"\nS3 bucket:     s3://event-snapshots")
    print(f"SQS DLQ URL:   {dlq_url}")
    print(f"Lambda fn:     event-retry-handler")
    print(f"EventBridge:   dlq-retry-rule (every 1 min)")
    print(f"\nAccess LocalStack at: {ENDPOINT}")
    print("Access Redpanda console at: http://localhost:8080")
    print("Access FastAPI docs at:     http://localhost:8000/docs")
