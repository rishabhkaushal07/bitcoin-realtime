#!/usr/bin/env python3
"""Finality status updater — upserts block finality via PyIceberg.

Consumes finality/reorg events from Kafka and upserts the btc.blocks
table to update finality_status:
  - OBSERVED → CONFIRMED (6+ confirmations)
  - OBSERVED/CONFIRMED → REORGED (block disconnected)

Uses PyIceberg's upsert with identifier_field_ids=[block_hash] so
re-applying the same event is idempotent (COW rewrite).

Usage:
    python pyiceberg-sidecar/finality_updater.py
"""

import argparse
import json
import logging
import signal
import sys
import time
from datetime import datetime

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("finality-updater")

CONTROL_TOPIC = "control-iceberg"

CATALOG_CONFIG = {
    "type": "hive",
    "uri": "thrift://localhost:9083",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "warehouse": "s3://warehouse/",
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "finality-updater",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}


def build_upsert_row(event: dict, arrow_schema: pa.Schema) -> pa.Table:
    """Build a single-row PyArrow table for upsert from a finality event.

    Event format:
    {
        "type": "finality" | "reorg",
        "block_hash": "...",
        "height": 123,
        "finality_status": "CONFIRMED" | "REORGED",
        "timestamp": "2026-03-27T00:00:00Z"
    }
    """
    block_hash = event["block_hash"]
    height = event["height"]
    new_status = event["finality_status"]
    ts = event.get("timestamp", datetime.utcnow().isoformat())

    # Build a row with only the fields needed for upsert
    # PyIceberg upsert matches on identifier fields (block_hash)
    # and updates the entire row — we need to provide all required fields
    columns = {}
    for field in arrow_schema:
        name = field.name
        if name == "block_hash":
            columns[name] = pa.array([block_hash], type=field.type)
        elif name == "height":
            columns[name] = pa.array([height], type=field.type)
        elif name == "finality_status":
            columns[name] = pa.array([new_status], type=field.type)
        elif name == "observed_at":
            columns[name] = pa.array([ts], type=field.type)
        else:
            # Null for fields we don't update
            columns[name] = pa.array([None], type=field.type)

    return pa.table(columns, schema=arrow_schema)


class FinalityUpdater:
    """Consumes finality/reorg events and upserts btc.blocks."""

    def __init__(
        self,
        catalog_config: dict = None,
        kafka_config: dict = None,
    ):
        self.catalog_config = catalog_config or CATALOG_CONFIG
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self._running = False
        self._catalog = None
        self._blocks_table = None
        self._arrow_schema = None
        self._consumer = None

    def _init(self):
        """Initialize catalog and consumer."""
        log.info("Connecting to Iceberg catalog...")
        self._catalog = load_catalog("hive", **self.catalog_config)
        self._blocks_table = self._catalog.load_table("btc.blocks")
        self._arrow_schema = schema_to_pyarrow(self._blocks_table.schema())
        log.info(f"  Loaded btc.blocks: {len(self._blocks_table.schema().fields)} cols")

        log.info("Creating Kafka consumer...")
        self._consumer = Consumer(self.kafka_config)
        self._consumer.subscribe([CONTROL_TOPIC])
        log.info(f"  Subscribed to {CONTROL_TOPIC}")

    def process_event(self, event: dict) -> bool:
        """Process a single finality/reorg event. Returns True on success."""
        event_type = event.get("type")
        block_hash = event.get("block_hash", "?")[:16]
        new_status = event.get("finality_status")

        if event_type not in ("finality", "reorg"):
            log.warning(f"Unknown event type: {event_type}")
            return False

        if new_status not in ("CONFIRMED", "REORGED"):
            log.warning(f"Invalid finality_status: {new_status}")
            return False

        log.info(f"Processing {event_type}: {block_hash}... → {new_status}")

        # Read current block to get full row for upsert
        block_hash_full = event["block_hash"]
        scan = self._blocks_table.scan(
            row_filter=f"block_hash == '{block_hash_full}'"
        )
        existing = scan.to_arrow()

        if len(existing) == 0:
            log.warning(f"Block {block_hash}... not found in Iceberg, skipping")
            return False

        # Update finality_status in the existing row
        updated = existing.set_column(
            existing.schema.get_field_index("finality_status"),
            "finality_status",
            pa.array([new_status] * len(existing), type=pa.large_string()),
        )

        self._blocks_table.overwrite(updated, overwrite_filter=f"block_hash == '{block_hash_full}'")
        log.info(f"  Updated {block_hash}... to {new_status}")
        return True

    def run(self):
        """Main consumer loop."""
        self._init()
        self._running = True
        processed = 0
        errors = 0

        log.info("Starting finality updater loop...")

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    if self.process_event(event):
                        processed += 1
                    else:
                        errors += 1
                    self._consumer.commit()
                except Exception as e:
                    log.error(f"Error processing event: {e}")
                    errors += 1

        except KeyboardInterrupt:
            log.info("Interrupted")
        finally:
            self._consumer.close()
            log.info(f"Stopped. Processed={processed}, Errors={errors}")

    def stop(self):
        self._running = False


def main():
    parser = argparse.ArgumentParser(description="Finality status updater")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--hms-uri", default="thrift://localhost:9083")
    parser.add_argument("--s3-endpoint", default="http://localhost:9000")
    args = parser.parse_args()

    kafka_config = {**KAFKA_CONFIG, "bootstrap.servers": args.bootstrap_servers}
    catalog_config = {
        **CATALOG_CONFIG,
        "uri": args.hms_uri,
        "s3.endpoint": args.s3_endpoint,
    }

    updater = FinalityUpdater(
        catalog_config=catalog_config,
        kafka_config=kafka_config,
    )

    def handle_signal(sig, frame):
        updater.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    updater.run()


if __name__ == "__main__":
    main()
