#!/usr/bin/env python3
"""Kafka → Iceberg writer using PyIceberg.

Consumes from 4 Kafka data topics and writes to corresponding Iceberg tables.
Uses PyIceberg's append for new data and upsert for finality/reorg updates.

Batches records by topic and flushes either when the batch reaches a
configurable size or after a configurable time interval.

Usage:
    python pyiceberg-sidecar/iceberg_writer.py
    python pyiceberg-sidecar/iceberg_writer.py --batch-size 500 --flush-interval 10
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
log = logging.getLogger("iceberg-writer")

# Topic → Iceberg table mapping
TOPIC_TABLE_MAP = {
    "btc.blocks.v1": "btc.blocks",
    "btc.transactions.v1": "btc.transactions",
    "btc.tx_inputs.v1": "btc.tx_in",
    "btc.tx_outputs.v1": "btc.tx_out",
}

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
    "group.id": "iceberg-writer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "max.poll.interval.ms": 300000,
}


class IcebergWriter:
    """Consumes from Kafka topics and writes to Iceberg tables."""

    def __init__(
        self,
        catalog_config: dict = None,
        kafka_config: dict = None,
        batch_size: int = 100,
        flush_interval_sec: float = 30.0,
    ):
        self.catalog_config = catalog_config or CATALOG_CONFIG
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.batch_size = batch_size
        self.flush_interval = flush_interval_sec
        self._running = False

        # Buffers: topic -> list[dict]
        self._buffers: dict[str, list[dict]] = {t: [] for t in TOPIC_TABLE_MAP}
        self._last_flush: float = time.time()

        # Load catalog and table handles
        self._catalog = None
        self._tables = {}
        self._arrow_schemas = {}
        self._consumer = None

    def _init_catalog(self):
        """Connect to Iceberg catalog and load table handles."""
        log.info("Connecting to Iceberg catalog...")
        self._catalog = load_catalog("hive", **self.catalog_config)
        for topic, table_name in TOPIC_TABLE_MAP.items():
            table = self._catalog.load_table(table_name)
            self._tables[topic] = table
            self._arrow_schemas[topic] = schema_to_pyarrow(table.schema())
            log.info(f"  Loaded {table_name}: {len(table.schema().fields)} cols")

    def _init_consumer(self):
        """Create Kafka consumer and subscribe to topics."""
        log.info("Creating Kafka consumer...")
        self._consumer = Consumer(self.kafka_config)
        self._consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))
        log.info(f"  Subscribed to {len(TOPIC_TABLE_MAP)} topics")

    def _parse_message(self, topic: str, value_bytes: bytes) -> dict:
        """Parse a Kafka message value into a dict."""
        record = json.loads(value_bytes.decode("utf-8"))
        # Remove schema_version (not in Iceberg schema)
        record.pop("schema_version", None)
        return record

    def _records_to_arrow(self, topic: str, records: list[dict]) -> pa.Table:
        """Convert a list of record dicts to a PyArrow table matching schema."""
        schema = self._arrow_schemas[topic]
        iceberg_table = self._tables[topic]
        iceberg_schema = iceberg_table.schema()

        # Build column arrays matching the Iceberg schema
        columns = {}
        for field in iceberg_schema.fields:
            pa_field = schema.field(field.name)
            values = [r.get(field.name) for r in records]

            # Handle timestamp fields: parse ISO strings to datetime
            if isinstance(field.field_type, __import__("pyiceberg.types", fromlist=["TimestampType"]).TimestampType):
                parsed = []
                for v in values:
                    if v is None:
                        parsed.append(None)
                    elif isinstance(v, str):
                        # Parse ISO format timestamp
                        try:
                            parsed.append(datetime.fromisoformat(v.replace("Z", "+00:00").replace("+00:00", "")))
                        except (ValueError, TypeError):
                            parsed.append(None)
                    else:
                        parsed.append(v)
                columns[field.name] = pa.array(parsed, type=pa_field.type)
            else:
                columns[field.name] = pa.array(values, type=pa_field.type)

        return pa.table(columns, schema=schema)

    def flush_topic(self, topic: str) -> int:
        """Flush buffered records for a topic to Iceberg. Returns count."""
        records = self._buffers[topic]
        if not records:
            return 0

        table = self._tables[topic]
        table_name = TOPIC_TABLE_MAP[topic]
        count = len(records)

        try:
            arrow_table = self._records_to_arrow(topic, records)
            table.append(arrow_table)
            log.info(f"  Flushed {count} records to {table_name}")
            self._buffers[topic] = []
            return count
        except Exception as e:
            log.error(f"  Failed to flush {table_name}: {e}")
            raise

    def flush_all(self) -> int:
        """Flush all topic buffers. Returns total records flushed."""
        total = 0
        for topic in TOPIC_TABLE_MAP:
            total += self.flush_topic(topic)
        if total > 0:
            # Commit Kafka offsets after successful Iceberg write
            self._consumer.commit()
            log.info(f"Committed offsets after flushing {total} records")
        self._last_flush = time.time()
        return total

    def _should_flush(self) -> bool:
        """Check if any buffer is full or flush interval has elapsed."""
        for records in self._buffers.values():
            if len(records) >= self.batch_size:
                return True
        if time.time() - self._last_flush >= self.flush_interval:
            return True
        return False

    def run(self):
        """Main consumer loop."""
        self._init_catalog()
        self._init_consumer()
        self._running = True
        self._last_flush = time.time()

        log.info(f"Starting consumer loop (batch={self.batch_size}, "
                 f"flush_interval={self.flush_interval}s)")

        total_consumed = 0
        total_flushed = 0

        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is not None:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        log.error(f"Consumer error: {msg.error()}")
                        continue

                    topic = msg.topic()
                    if topic in TOPIC_TABLE_MAP:
                        record = self._parse_message(topic, msg.value())
                        self._buffers[topic].append(record)
                        total_consumed += 1

                if self._should_flush():
                    flushed = self.flush_all()
                    total_flushed += flushed

        except KeyboardInterrupt:
            log.info("Interrupted")
        finally:
            # Final flush
            flushed = self.flush_all()
            total_flushed += flushed
            self._consumer.close()
            log.info(f"Stopped. Consumed={total_consumed}, Flushed={total_flushed}")

    def stop(self):
        """Signal the consumer loop to stop."""
        self._running = False


def main():
    parser = argparse.ArgumentParser(description="Kafka → Iceberg writer")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Records per batch before flush (default: 100)")
    parser.add_argument("--flush-interval", type=float, default=30.0,
                        help="Max seconds between flushes (default: 30)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092",
                        help="Kafka bootstrap servers")
    parser.add_argument("--hms-uri", default="thrift://localhost:9083",
                        help="Hive Metastore Thrift URI")
    parser.add_argument("--s3-endpoint", default="http://localhost:9000",
                        help="MinIO/S3 endpoint")
    args = parser.parse_args()

    kafka_config = {**KAFKA_CONFIG, "bootstrap.servers": args.bootstrap_servers}
    catalog_config = {
        **CATALOG_CONFIG,
        "uri": args.hms_uri,
        "s3.endpoint": args.s3_endpoint,
    }

    writer = IcebergWriter(
        catalog_config=catalog_config,
        kafka_config=kafka_config,
        batch_size=args.batch_size,
        flush_interval_sec=args.flush_interval,
    )

    def handle_signal(sig, frame):
        log.info(f"Received signal {sig}, stopping...")
        writer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    writer.run()


if __name__ == "__main__":
    main()
