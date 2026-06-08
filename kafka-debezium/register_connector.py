"""
Register Debezium PostgreSQL Connector
=======================================
Registers a CDC connector with Kafka Connect that captures
changes from the banking_oltp PostgreSQL database.

Usage:
  python register_connector.py
  python register_connector.py --check    # check status only
  python register_connector.py --delete   # remove connector
"""

import argparse
import json
import sys
import time

import requests

KAFKA_CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "banking-postgres-connector"

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "banking_user",
        "database.password": "banking_pass",
        "database.dbname": "banking_oltp",
        "topic.prefix": "banking",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.name": "banking_pub",
        "publication.autocreate.mode": "filtered",
        "table.include.list": (
            "public.customers,"
            "public.accounts,"
            "public.transactions,"
            "public.loans,"
            "public.audit_log"
        ),
        # Converters — schemas disabled for simpler JSON
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        # Snapshot mode — initial snapshot then streaming
        "snapshot.mode": "initial",
        # Heartbeat to keep replication slot active
        "heartbeat.interval.ms": "30000",
        # Tombstone events on deletes
        "tombstones.on.delete": "true",
        # Include transaction metadata (useful for ordering in Snowflake)
        "provide.transaction.metadata": "true",
    },
}


def wait_for_connect(max_retries: int = 30, interval: int = 5):
    """Wait for Kafka Connect to be ready."""
    print(f"Waiting for Kafka Connect at {KAFKA_CONNECT_URL}...")
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
            if resp.status_code == 200:
                print(f"  ✅ Kafka Connect is ready (attempt {attempt})")
                return True
        except requests.ConnectionError:
            pass
        print(f"  Attempt {attempt}/{max_retries} — not ready yet...")
        time.sleep(interval)
    print("  ❌ Kafka Connect did not become ready in time.")
    sys.exit(1)


def register_connector():
    """Register the Debezium connector."""
    # Check if already exists
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        print(f"Connector '{CONNECTOR_NAME}' already exists. Updating config...")
        resp = requests.put(
            f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/config",
            headers={"Content-Type": "application/json"},
            json=CONNECTOR_CONFIG["config"],
        )
    else:
        print(f"Registering connector '{CONNECTOR_NAME}'...")
        resp = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            headers={"Content-Type": "application/json"},
            json=CONNECTOR_CONFIG,
        )

    if resp.status_code in (200, 201):
        print(f"  ✅ Connector registered successfully")
        print(f"  Topics will be created:")
        for table in CONNECTOR_CONFIG["config"]["table.include.list"].split(","):
            topic = f"banking.{table.strip()}"
            print(f"    • {topic}")
    else:
        print(f"  ❌ Registration failed: {resp.status_code}")
        print(f"  Response: {resp.text}")
        sys.exit(1)


def check_status():
    """Check connector status."""
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
    if resp.status_code == 200:
        status = resp.json()
        connector_state = status["connector"]["state"]
        print(f"Connector: {connector_state}")
        for task in status.get("tasks", []):
            print(f"  Task {task['id']}: {task['state']}")
            if task["state"] == "FAILED":
                print(f"    Trace: {task.get('trace', 'N/A')[:200]}")
    else:
        print(f"Connector '{CONNECTOR_NAME}' not found.")


def delete_connector():
    """Delete the connector."""
    resp = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
    if resp.status_code == 204:
        print(f"  ✅ Connector '{CONNECTOR_NAME}' deleted.")
    else:
        print(f"  ❌ Delete failed: {resp.status_code} — {resp.text}")


def main():
    parser = argparse.ArgumentParser(description="Debezium Connector Manager")
    parser.add_argument("--check", action="store_true", help="Check connector status")
    parser.add_argument("--delete", action="store_true", help="Delete connector")
    parser.add_argument(
        "--no-wait", action="store_true", help="Skip waiting for Connect"
    )
    args = parser.parse_args()

    if not args.no_wait:
        wait_for_connect()

    if args.check:
        check_status()
    elif args.delete:
        delete_connector()
    else:
        register_connector()
        time.sleep(3)
        check_status()


if __name__ == "__main__":
    main()
