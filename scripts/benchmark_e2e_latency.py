#!/usr/bin/env python3
"""End-to-end latency benchmark for the Bitcoin real-time pipeline.

Pushes a range of blocks through the full pipeline and measures latency
at each hop:

  1. RPC fetch         — Bitcoin Core getblock(hash, 2)
  2. Normalize         — JSON → 4 flat record types
  3. Kafka produce     — Publish to 4 Kafka topics (with flush)
  4. Kafka → Iceberg   — Consume from Kafka, write to Iceberg tables
  5. StarRocks query   — Query Iceberg via external catalog

Usage:
  python scripts/benchmark_e2e_latency.py --start 100 --count 5
  python scripts/benchmark_e2e_latency.py --start 200000 --count 10
  python scripts/benchmark_e2e_latency.py --start 500000 --count 3  # large blocks
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project paths
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "live-normalizer"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pyiceberg-sidecar"))

from rpc_client import BitcoinRPC
from normalizer import normalize_block
from kafka_producer import BlockKafkaProducer

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("benchmark")

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

TOPIC_TABLE_MAP = {
    "btc.blocks.v1": "btc.blocks",
    "btc.transactions.v1": "btc.transactions",
    "btc.tx_inputs.v1": "btc.tx_in",
    "btc.tx_outputs.v1": "btc.tx_out",
}


def measure(label: str, func, *args, **kwargs):
    """Run func, return (result, elapsed_ms)."""
    t0 = time.perf_counter()
    result = func(*args, **kwargs)
    elapsed = (time.perf_counter() - t0) * 1000
    return result, elapsed


def consume_and_write_iceberg(catalog, tables, arrow_schemas, expected_records: int,
                              timeout_sec: float = 30.0):
    """Consume from Kafka and write to Iceberg. Returns (records_written, elapsed_ms)."""
    from pyiceberg.types import TimestampType

    kafka_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"benchmark-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 30000,
    }
    consumer = Consumer(kafka_config)
    consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))

    buffers = {t: [] for t in TOPIC_TABLE_MAP}
    total = 0
    t0 = time.perf_counter()
    deadline = t0 + timeout_sec

    # Wait for assignment
    while time.perf_counter() < t0 + 5:
        msg = consumer.poll(0.5)
        if msg and not msg.error():
            topic = msg.topic()
            if topic in TOPIC_TABLE_MAP:
                record = json.loads(msg.value().decode("utf-8"))
                record.pop("schema_version", None)
                buffers[topic].append(record)
                total += 1
            if total >= expected_records:
                break
        assigned = consumer.assignment()
        if assigned:
            break

    # Consume remaining
    while total < expected_records and time.perf_counter() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            continue
        topic = msg.topic()
        if topic in TOPIC_TABLE_MAP:
            record = json.loads(msg.value().decode("utf-8"))
            record.pop("schema_version", None)
            buffers[topic].append(record)
            total += 1

    consume_ms = (time.perf_counter() - t0) * 1000

    # Write to Iceberg
    t1 = time.perf_counter()
    flushed = 0
    for topic, records in buffers.items():
        if not records:
            continue
        table = tables[topic]
        schema = arrow_schemas[topic]
        iceberg_schema = table.schema()

        columns = {}
        for field in iceberg_schema.fields:
            pa_field = schema.field(field.name)
            values = [r.get(field.name) for r in records]
            if isinstance(field.field_type, TimestampType):
                parsed = []
                for v in values:
                    if v is None:
                        parsed.append(None)
                    elif isinstance(v, str):
                        try:
                            parsed.append(datetime.fromisoformat(
                                v.replace("Z", "+00:00").replace("+00:00", "")))
                        except (ValueError, TypeError):
                            parsed.append(None)
                    else:
                        parsed.append(v)
                columns[field.name] = pa.array(parsed, type=pa_field.type)
            else:
                columns[field.name] = pa.array(values, type=pa_field.type)

        arrow_table = pa.table(columns, schema=schema)
        table.append(arrow_table)
        flushed += len(records)

    write_ms = (time.perf_counter() - t1) * 1000
    consumer.close()

    return total, consume_ms, flushed, write_ms


def query_starrocks(heights: list[int]):
    """Query StarRocks external catalog for the given heights. Returns elapsed_ms."""
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host="127.0.0.1", port=9030, user="root", database="",
        )
        cursor = conn.cursor()

        t0 = time.perf_counter()
        # Refresh metadata first
        for tbl in ["blocks", "transactions", "tx_in", "tx_out"]:
            cursor.execute(f"REFRESH EXTERNAL TABLE iceberg_catalog.btc.{tbl}")

        refresh_ms = (time.perf_counter() - t0) * 1000

        heights_str = ",".join(str(h) for h in heights)

        results = {}
        t1 = time.perf_counter()
        cursor.execute(f"SELECT COUNT(*) FROM iceberg_catalog.btc.blocks WHERE height IN ({heights_str})")
        results["blocks"] = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM iceberg_catalog.btc.transactions WHERE block_height IN ({heights_str})")
        results["transactions"] = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM iceberg_catalog.btc.tx_in WHERE block_height IN ({heights_str})")
        results["tx_in"] = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM iceberg_catalog.btc.tx_out WHERE height IN ({heights_str})")
        results["tx_out"] = cursor.fetchone()[0]

        query_ms = (time.perf_counter() - t1) * 1000

        cursor.close()
        conn.close()
        return results, refresh_ms, query_ms
    except Exception as e:
        log.warning(f"StarRocks query failed: {e}")
        return None, 0, 0


def run_benchmark(start_height: int, count: int):
    """Run the full pipeline benchmark."""
    print("=" * 78)
    print("  BITCOIN REAL-TIME PIPELINE — END-TO-END LATENCY BENCHMARK")
    print("=" * 78)
    print(f"  Blocks: {start_height} to {start_height + count - 1} ({count} blocks)")
    print(f"  Time:   {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 78)

    # --- Stage 0: Init ---
    rpc = BitcoinRPC()
    producer = BlockKafkaProducer()
    catalog = load_catalog("hive", **CATALOG_CONFIG)

    tables = {}
    arrow_schemas = {}
    for topic, table_name in TOPIC_TABLE_MAP.items():
        table = catalog.load_table(table_name)
        tables[topic] = table
        arrow_schemas[topic] = schema_to_pyarrow(table.schema())

    # Check max available height
    tip = rpc.getblockcount()
    if start_height + count - 1 > tip:
        print(f"\n  WARNING: Requested up to height {start_height + count - 1} but tip is {tip}")
        count = max(1, tip - start_height + 1)
        print(f"  Adjusted to {count} blocks\n")

    # Set up Kafka consumer BEFORE producing (so it catches messages)
    from confluent_kafka import Consumer as C
    pre_consumer = C({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"benchmark-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    pre_consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))
    # Trigger assignment by polling
    for _ in range(5):
        pre_consumer.poll(0.5)
    pre_consumer.close()

    # --- Stage 1 & 2 & 3: RPC + Normalize + Kafka ---
    block_results = []
    total_records = 0

    print("\n--- Stage 1-3: RPC Fetch + Normalize + Kafka Produce ---\n")
    print(f"  {'Height':>8}  {'Txs':>6}  {'Inputs':>7}  {'Outputs':>8}  "
          f"{'RPC ms':>8}  {'Norm ms':>8}  {'Kafka ms':>9}  {'Records':>8}")
    print(f"  {'─' * 8}  {'─' * 6}  {'─' * 7}  {'─' * 8}  "
          f"{'─' * 8}  {'─' * 8}  {'─' * 9}  {'─' * 8}")

    for i in range(count):
        h = start_height + i

        # RPC fetch
        block_hash, rpc_ms = measure("rpc", rpc.getblockhash, h)
        rpc_block, rpc_ms2 = measure("rpc", rpc.getblock, block_hash, 2)
        rpc_total = rpc_ms + rpc_ms2

        # Normalize
        normalized, norm_ms = measure("normalize", normalize_block, rpc_block)

        n_tx = len(normalized["transactions"])
        n_in = len(normalized["tx_inputs"])
        n_out = len(normalized["tx_outputs"])
        n_records = 1 + n_tx + n_in + n_out

        # Kafka produce
        _, kafka_ms = measure("kafka", producer.publish_block, normalized)

        total_records += n_records

        block_results.append({
            "height": h,
            "txs": n_tx,
            "inputs": n_in,
            "outputs": n_out,
            "records": n_records,
            "rpc_ms": rpc_total,
            "norm_ms": norm_ms,
            "kafka_ms": kafka_ms,
        })

        print(f"  {h:>8}  {n_tx:>6}  {n_in:>7}  {n_out:>8}  "
              f"{rpc_total:>8.1f}  {norm_ms:>8.2f}  {kafka_ms:>9.1f}  {n_records:>8}")

    producer.close()

    # Summaries for stages 1-3
    avg_rpc = sum(b["rpc_ms"] for b in block_results) / count
    avg_norm = sum(b["norm_ms"] for b in block_results) / count
    avg_kafka = sum(b["kafka_ms"] for b in block_results) / count
    total_txs = sum(b["txs"] for b in block_results)
    total_ins = sum(b["inputs"] for b in block_results)
    total_outs = sum(b["outputs"] for b in block_results)

    print(f"\n  {'AVERAGE':>8}  {total_txs/count:>6.0f}  {total_ins/count:>7.0f}  "
          f"{total_outs/count:>8.0f}  {avg_rpc:>8.1f}  {avg_norm:>8.2f}  "
          f"{avg_kafka:>9.1f}  {total_records/count:>8.0f}")

    # --- Stage 4: Kafka → Iceberg ---
    print("\n--- Stage 4: Kafka Consume + Iceberg Write ---\n")

    # Need a fresh consumer group to read the just-produced messages
    kafka_config_consume = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"benchmark-write-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(kafka_config_consume)
    consumer.subscribe(list(TOPIC_TABLE_MAP.keys()))

    buffers = {t: [] for t in TOPIC_TABLE_MAP}
    consumed = 0
    t_consume_start = time.perf_counter()

    # Consume all available messages (with timeout)
    deadline = t_consume_start + 30
    while time.perf_counter() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            if consumed >= total_records:
                break
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                if consumed >= total_records:
                    break
                continue
            continue
        topic = msg.topic()
        if topic in TOPIC_TABLE_MAP:
            record = json.loads(msg.value().decode("utf-8"))
            record.pop("schema_version", None)
            buffers[topic].append(record)
            consumed += 1

    consume_ms = (time.perf_counter() - t_consume_start) * 1000
    consumer.close()

    print(f"  Kafka consume: {consumed} records in {consume_ms:.1f} ms "
          f"({consumed / (consume_ms / 1000):.0f} records/sec)" if consume_ms > 0 else "")

    # Write to Iceberg
    from pyiceberg.types import TimestampType, IntegerType, LongType

    t_write_start = time.perf_counter()
    flushed = 0
    write_details = {}

    def _safe_int_array(values, pa_type):
        """Handle unsigned 32-bit values that overflow signed int32.

        Bitcoin nonce, nBits, sequence can exceed 2^31-1.
        Wrap to signed representation for int32 Iceberg columns.
        """
        safe = []
        for v in values:
            if v is None:
                safe.append(None)
            elif pa_type == pa.int32() and isinstance(v, int) and v > 2147483647:
                safe.append(v - 4294967296)  # wrap uint32 -> int32
            else:
                safe.append(v)
        return pa.array(safe, type=pa_type)

    for topic, records in buffers.items():
        if not records:
            write_details[TOPIC_TABLE_MAP[topic]] = (0, 0)
            continue
        table = tables[topic]
        schema = arrow_schemas[topic]
        iceberg_schema = table.schema()

        columns = {}
        for field in iceberg_schema.fields:
            pa_field = schema.field(field.name)
            values = [r.get(field.name) for r in records]
            if isinstance(field.field_type, TimestampType):
                parsed = []
                for v in values:
                    if v is None:
                        parsed.append(None)
                    elif isinstance(v, str):
                        try:
                            parsed.append(datetime.fromisoformat(
                                v.replace("Z", "+00:00").replace("+00:00", "")))
                        except (ValueError, TypeError):
                            parsed.append(None)
                    else:
                        parsed.append(v)
                columns[field.name] = pa.array(parsed, type=pa_field.type)
            elif isinstance(field.field_type, (IntegerType, LongType)):
                columns[field.name] = _safe_int_array(values, pa_field.type)
            else:
                columns[field.name] = pa.array(values, type=pa_field.type)

        t_table = time.perf_counter()
        arrow_table = pa.table(columns, schema=schema)
        table.append(arrow_table)
        table_ms = (time.perf_counter() - t_table) * 1000
        write_details[TOPIC_TABLE_MAP[topic]] = (len(records), table_ms)
        flushed += len(records)

    write_ms = (time.perf_counter() - t_write_start) * 1000

    print(f"  Iceberg write:  {flushed} records in {write_ms:.1f} ms "
          f"({flushed / (write_ms / 1000):.0f} records/sec)" if write_ms > 0 else "")
    print()
    print(f"  {'Table':<20}  {'Records':>8}  {'Write ms':>10}")
    print(f"  {'─' * 20}  {'─' * 8}  {'─' * 10}")
    for tbl, (cnt, ms) in write_details.items():
        print(f"  {tbl:<20}  {cnt:>8}  {ms:>10.1f}")

    # --- Stage 5: StarRocks Query ---
    print("\n--- Stage 5: StarRocks Query via External Catalog ---\n")

    heights = [start_height + i for i in range(count)]
    sr_results, refresh_ms, query_ms = query_starrocks(heights)

    if sr_results:
        print(f"  Metadata refresh: {refresh_ms:.1f} ms")
        print(f"  Query time:       {query_ms:.1f} ms")
        print()
        print(f"  {'Table':<20}  {'Rows Found':>10}")
        print(f"  {'─' * 20}  {'─' * 10}")
        for tbl, cnt in sr_results.items():
            print(f"  {tbl:<20}  {cnt:>10}")
    else:
        print("  StarRocks query skipped or failed (install mysql-connector-python)")

    # --- Summary ---
    e2e_per_block = avg_rpc + avg_norm + avg_kafka + (write_ms / count)
    print("\n" + "=" * 78)
    print("  LATENCY SUMMARY (per block average)")
    print("=" * 78)
    print(f"""
  +-------------------+------------+------------------------------------------+
  | Stage             | Avg ms     | Description                              |
  +-------------------+------------+------------------------------------------+
  | RPC fetch         | {avg_rpc:>8.1f}   | getblockhash + getblock(hash, 2)         |
  | Normalize         | {avg_norm:>8.2f}   | JSON -> 4 flat record types              |
  | Kafka produce     | {avg_kafka:>8.1f}   | Publish to 4 topics (with flush)         |
  | Kafka consume     | {consume_ms/count:>8.1f}   | Consumer poll + deserialize              |
  | Iceberg write     | {write_ms/count:>8.1f}   | PyArrow table.append to MinIO            |
  | StarRocks refresh | {refresh_ms:>8.1f}   | REFRESH EXTERNAL TABLE (4 tables)        |
  | StarRocks query   | {query_ms:>8.1f}   | COUNT(*) on 4 tables with height filter  |
  +-------------------+------------+------------------------------------------+
  | TOTAL e2e         | {e2e_per_block:>8.1f}   | RPC -> Iceberg (per block)               |
  | + StarRocks       | {e2e_per_block + refresh_ms + query_ms:>8.1f}   | Full pipeline to queryable               |
  +-------------------+------------+------------------------------------------+
""")

    print(f"  Volume: {count} blocks, {total_txs} txs, {total_ins} inputs, "
          f"{total_outs} outputs = {total_records} total records")

    throughput = total_records / ((sum(b["rpc_ms"] + b["norm_ms"] + b["kafka_ms"]
                                      for b in block_results) + write_ms) / 1000)
    print(f"  Throughput: {throughput:.0f} records/sec (end-to-end)")
    print()

    # Return results for programmatic use
    return {
        "blocks": count,
        "total_records": total_records,
        "total_txs": total_txs,
        "total_inputs": total_ins,
        "total_outputs": total_outs,
        "avg_rpc_ms": avg_rpc,
        "avg_norm_ms": avg_norm,
        "avg_kafka_ms": avg_kafka,
        "kafka_consume_ms": consume_ms,
        "iceberg_write_ms": write_ms,
        "starrocks_refresh_ms": refresh_ms,
        "starrocks_query_ms": query_ms,
        "e2e_per_block_ms": e2e_per_block,
        "throughput_records_sec": throughput,
        "block_details": block_results,
    }


def main():
    parser = argparse.ArgumentParser(
        description="End-to-end latency benchmark for the Bitcoin real-time pipeline")
    parser.add_argument("--start", type=int, default=100,
                        help="Starting block height (default: 100)")
    parser.add_argument("--count", type=int, default=5,
                        help="Number of blocks to process (default: 5)")
    args = parser.parse_args()

    run_benchmark(args.start, args.count)


if __name__ == "__main__":
    main()
