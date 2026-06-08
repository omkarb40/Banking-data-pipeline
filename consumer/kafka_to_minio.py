"""
Kafka → MinIO Consumer
========================
Subscribes to Debezium CDC topics, batches messages,
and writes JSON files to MinIO in a date-partitioned structure.

Path format: banking-raw/{table}/year=YYYY/month=MM/day=DD/{timestamp}.json

Usage:
  python kafka_to_minio.py
  python kafka_to_minio.py --batch-size 200 --flush-interval 60
"""

import os
import json
import time
import argparse
import logging
from datetime import datetime

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError, KafkaException

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
    "group.id": "banking-minio-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # manual commit after MinIO write
    "max.poll.interval.ms": 300000,
}

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minio_password"),
}

TOPICS = [
    "banking.public.customers",
    "banking.public.accounts",
    "banking.public.transactions",
    "banking.public.loans",
    "banking.public.audit_log",
]

RAW_BUCKET = "banking-raw"
PROCESSED_BUCKET = "banking-processed"


# ── MinIO Client ─────────────────────────────────────────────
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_CONFIG["endpoint_url"],
        aws_access_key_id=MINIO_CONFIG["aws_access_key_id"],
        aws_secret_access_key=MINIO_CONFIG["aws_secret_access_key"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_buckets(s3_client):
    """Create required buckets if they don't exist."""
    for bucket in [RAW_BUCKET, PROCESSED_BUCKET]:
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f"  Bucket '{bucket}' exists")
        except Exception:
            s3_client.create_bucket(Bucket=bucket)
            logger.info(f"  ✅ Created bucket '{bucket}'")


def extract_table_name(topic: str) -> str:
    """Extract table name from Debezium topic.

    'banking.public.customers' → 'customers'
    """
    parts = topic.split(".")
    return parts[-1] if len(parts) >= 3 else topic


def build_s3_key(table_name: str) -> str:
    """Build date-partitioned S3 key.

    Returns: customers/year=2024/month=06/day=15/1718451234_abcdef.json
    """
    now = datetime.utcnow()
    timestamp = int(now.timestamp())
    micro = now.strftime("%f")[:6]
    return (
        f"{table_name}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{timestamp}_{micro}.json"
    )


def flush_batch(s3_client, table_name: str, messages: list):
    """Write a batch of messages to MinIO as a single JSON file."""
    if not messages:
        return

    key = build_s3_key(table_name)
    payload = json.dumps(messages, default=str, indent=None)

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=payload.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(
        f"  📦 Wrote {len(messages)} records → "
        f"s3://{RAW_BUCKET}/{key} "
        f"({len(payload)} bytes)"
    )


# ── Main Consumer Loop ───────────────────────────────────────
def run_consumer(batch_size: int, flush_interval: int):
    s3_client = get_s3_client()
    logger.info("Connecting to MinIO...")
    ensure_buckets(s3_client)

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(TOPICS)
    logger.info(f"Subscribed to topics: {TOPICS}")
    logger.info(f"Batch size: {batch_size}, Flush interval: {flush_interval}s")

    # Per-table message buffer
    buffers: dict[str, list] = {extract_table_name(t): [] for t in TOPICS}
    last_flush = time.time()
    total_consumed = 0
    total_flushed = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No message — check if we should flush on time interval
                elapsed = time.time() - last_flush
                if elapsed >= flush_interval and any(buffers.values()):
                    for table_name, msgs in buffers.items():
                        if msgs:
                            flush_batch(s3_client, table_name, msgs)
                            total_flushed += len(msgs)
                            buffers[table_name] = []
                    consumer.commit()
                    last_flush = time.time()
                    logger.info(f"  ⏰ Time-based flush (total: {total_flushed})")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Parse message
            topic = msg.topic()
            table_name = extract_table_name(topic)

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(f"  ⚠ Skipping unparseable message: {e}")
                continue

            # Skip tombstone messages (value is null after a delete)
            if value is None:
                continue

            # Add metadata
            value["_metadata"] = {
                "topic": topic,
                "partition": msg.partition(),
                "offset": msg.offset(),
                "timestamp": msg.timestamp()[1],
                "consumed_at": datetime.utcnow().isoformat(),
            }

            buffers[table_name].append(value)
            total_consumed += 1

            # Check if any buffer hit batch size
            if len(buffers[table_name]) >= batch_size:
                flush_batch(s3_client, table_name, buffers[table_name])
                total_flushed += len(buffers[table_name])
                buffers[table_name] = []
                consumer.commit()
                last_flush = time.time()
                logger.info(
                    f"  📊 Batch flush (consumed: {total_consumed}, "
                    f"flushed: {total_flushed})"
                )

    except KeyboardInterrupt:
        logger.info("\n⏹ Consumer stopped by user.")
        # Final flush
        for table_name, msgs in buffers.items():
            if msgs:
                flush_batch(s3_client, table_name, msgs)
                total_flushed += len(msgs)
        consumer.commit()
        logger.info(f"Final flush complete. Total flushed: {total_flushed}")
    finally:
        consumer.close()
        logger.info("Consumer closed.")


def main():
    parser = argparse.ArgumentParser(description="Kafka → MinIO Consumer")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Messages per file (default: 100)",
    )
    parser.add_argument(
        "--flush-interval",
        type=int,
        default=30,
        help="Max seconds between flushes (default: 30)",
    )
    args = parser.parse_args()
    run_consumer(args.batch_size, args.flush_interval)


if __name__ == "__main__":
    main()
