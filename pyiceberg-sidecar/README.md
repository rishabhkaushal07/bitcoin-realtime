# PyIceberg Sidecar

Python services that write normalized Bitcoin data from Kafka to Iceberg tables, and handle finality status updates (confirmation promotions and reorg soft-deletes).

```
+===================================================================+
|                    PyIceberg Sidecar                               |
+===================================================================+
|                                                                   |
|  +-------------------+     +--------------------------------+     |
|  | Kafka             |     | iceberg_writer.py              |     |
|  | 4 data topics:    |---->| (batched Kafka consumer)       |     |
|  |  btc.blocks.v1    |     |                                |     |
|  |  btc.transactions |     | - Polls 4 topics               |     |
|  |  btc.tx_inputs    |     | - Buffers records per table    |     |
|  |  btc.tx_outputs   |     | - Flushes every 100 records    |     |
|  +-------------------+     |   or every 30 seconds          |     |
|                            | - PyArrow schema from Iceberg  |     |
|                            | - Commits Kafka offsets after   |     |
|                            |   successful Iceberg append    |     |
|                            +---------------+----------------+     |
|                                            |                      |
|                                            v                      |
|  +-------------------+     +--------------------------------+     |
|  | Kafka             |     | finality_updater.py            |     |
|  | control-iceberg   |---->| (event consumer)               |     |
|  | topic:            |     |                                |     |
|  |  finality events  |     | Events:                        |     |
|  |  reorg events     |     |  {type: "finality",            |     |
|  +-------------------+     |   block_hash: "...",           |     |
|                            |   finality_status: "CONFIRMED"}|     |
|                            |  {type: "reorg",               |     |
|                            |   block_hash: "...",           |     |
|                            |   finality_status: "REORGED"}  |     |
|                            +---------------+----------------+     |
|                                            |                      |
|                                            v                      |
|                            +--------------------------------+     |
|                            | Iceberg on MinIO               |     |
|                            | s3://warehouse/btc.db/         |     |
|                            |                                |     |
|                            | btc.blocks      (append/upsert)|    |
|                            | btc.transactions (append/upsert)|    |
|                            | btc.tx_in       (append only)  |    |
|                            | btc.tx_out      (append only)  |    |
|                            +--------------------------------+     |
+===================================================================+
```

---

## Status: COMPLETE and LIVE (Phase 1b)

Both components are implemented, tested, and running since 2026-03-28:
- `iceberg_writer.py` — 197 lines, batched Kafka consumer (live, draining blocks)
- `finality_updater.py` — 176 lines, event-driven upserts

---

## Components

### iceberg_writer.py (Append Path)

Batched Kafka consumer that reads from 4 data topics and writes to corresponding Iceberg tables.

| Setting | Default | Description |
|---------|---------|-------------|
| `batch_size` | 100 | Records per table before flush |
| `flush_interval` | 30s | Max time between flushes |
| `consumer_group` | `iceberg-writer` | Kafka consumer group |

```
Topic-to-Table Mapping:
+----------------------+--------------------+
| Kafka Topic          | Iceberg Table      |
+----------------------+--------------------+
| btc.blocks.v1        | btc.blocks         |
| btc.transactions.v1  | btc.transactions   |
| btc.tx_inputs.v1     | btc.tx_in          |
| btc.tx_outputs.v1    | btc.tx_out         |
+----------------------+--------------------+
```

Key behaviors:
- Strips `schema_version` field from Kafka messages before Iceberg write
- Uses `schema_to_pyarrow(table.schema())` for exact Arrow schema matching
- Commits Kafka offsets only after successful Iceberg append
- Graceful shutdown via SIGINT/SIGTERM signal handling

### finality_updater.py (Upsert Path)

Consumes finality and reorg events from the `control-iceberg` Kafka topic and applies row-level updates to Iceberg tables.

```
Finality State Machine:
+----------+     6+ confirms     +-----------+
| OBSERVED |-------------------->| CONFIRMED |
+----------+                     +-----------+
      |
      |  block disconnected
      v
+----------+
| REORGED  |
+----------+
```

| Operation | Event Type | Iceberg Action |
|-----------|-----------|----------------|
| Confirm block | `finality` | Read existing row, update `finality_status` to `CONFIRMED`, overwrite with filter |
| Reorg block | `reorg` | Read existing row, update `finality_status` to `REORGED`, overwrite with filter |

Uses `table.overwrite(updated_data, overwrite_filter=f"block_hash == '{hash}'")`

---

## Why PyIceberg Instead of Kafka Connect?

The original V3 architecture planned Kafka Connect Iceberg Sink for appends.
However, the runtime ZIP is not available as a pre-built download and must be
compiled from source. PyIceberg was the documented "valid fallback" path.

| Concern | Kafka Connect Sink | PyIceberg Writer |
|---------|-------------------|-----------------|
| Append new rows | Yes | Yes |
| Update existing rows | No (append-only) | Yes (identifier fields) |
| Delete / soft-delete | No | Yes (overwrite with filter) |
| Pre-built binary | Not available | pip install |
| Schema flexibility | Limited | Full control |

---

## Usage

```bash
# Start the Iceberg writer (requires Kafka + HMS + MinIO)
python pyiceberg-sidecar/iceberg_writer.py

# Start with custom batch size and flush interval
python pyiceberg-sidecar/iceberg_writer.py --batch-size 500 --flush-interval 10

# Start the finality updater
python pyiceberg-sidecar/finality_updater.py
```

### Running as Background Service

```bash
# Iceberg writer (background, logging to file)
nohup .venv/bin/python pyiceberg-sidecar/iceberg_writer.py \
    > logs/iceberg_writer.log 2>&1 &

# Monitor progress
tail -f logs/iceberg_writer.log

# Check Kafka consumer lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:29092 --describe --group iceberg-writer
```

---

## Tests

| Suite | File | Tests | External Deps |
|-------|------|------:|---------------|
| Unit (writer) | `tests/unit/test_iceberg_writer.py` | 21 | None (mocked) |
| Unit (updater) | `tests/unit/test_finality_updater.py` | 7 | None (mocked) |
| Integration | `tests/integration/test_kafka_to_iceberg.py` | 12 | Docker services |
| Integration | `tests/integration/test_iceberg_tables.py` | 22 | Docker services |

```bash
# Unit tests
pytest tests/unit/test_iceberg_writer.py tests/unit/test_finality_updater.py -v

# Integration tests
pytest tests/integration/test_kafka_to_iceberg.py -m integration -v
pytest tests/integration/test_iceberg_tables.py -m integration -v
```

---

## Measured Performance (baseline benchmark, 2026-03-27)

| Metric | Value | Notes |
|--------|------:|-------|
| Kafka consume throughput | ~16,000 records/sec | confluent-kafka consumer poll |
| Iceberg write throughput | ~36,000 records/sec | PyArrow table.append to MinIO |
| Per-table write (27K records) | ~525 ms | tx_in, largest table |
| Per-table write (20 records) | ~364 ms | blocks, fixed overhead dominates |
| Fixed overhead per flush | ~300-400 ms | Parquet file creation + S3 upload + metadata commit |

The per-table write has a fixed ~300-400ms floor for Parquet file creation and S3/MinIO
round-trip, regardless of record count. This means small batches are proportionally more
expensive — the writer's batch-size tuning matters more for small blocks.

Full benchmark: [docs/latency-benchmark-baseline.md](../docs/latency-benchmark-baseline.md)

---

## Schema Evolution (2026-03-28)

Several unsigned 32-bit Bitcoin fields caused `ArrowInvalid: Value too large to fit in
C integer type` at runtime. Fixed via Iceberg schema evolution (no table recreation):

| Table | Column | Issue | Fix |
|-------|--------|-------|-----|
| `btc.blocks` | `nNonce` | Values > 2^31 overflow INT32 | Evolved to LONG |
| `btc.blocks` | `nBits` | Compact target can exceed INT32 | Evolved to LONG |
| `btc.tx_in` | `indexPrevOut` | Coinbase uses 0xFFFFFFFF (4,294,967,295) | Evolved to LONG |
| `btc.transactions` | `lockTime` | Unsigned 32-bit field | Evolved to LONG |

Schema evolution was applied live using PyIceberg's `table.update_schema()` API.
Existing Parquet files are read with automatic type promotion (INT -> LONG).

---

## Roadblocks Encountered and Fixes

| Problem | Root Cause | Fix |
|---------|-----------|-----|
| PyArrow schema mismatch on write | Default inferred types (int64, optional) don't match Iceberg schema (int32, required) | Use `schema_to_pyarrow(table.schema())` from `pyiceberg.io.pyarrow` |
| `pa.null()` not supported in Iceberg v2 | Null type arrays rejected by Iceberg format | Use string placeholder `"unknown"` instead of `None` for nullable fields |
| `indexPrevOut` int32 overflow (runtime crash) | Value 4294967295 (0xFFFFFFFF for coinbase) exceeds signed int32 max | Iceberg schema evolution: INT -> LONG for `indexPrevOut`, `nNonce`, `nBits`, `lockTime` |
| HMS Hive 4.2.0 Thrift incompatible | PyIceberg uses Thrift client, Hive 4.2.0 changed Thrift API | Downgrade to Hive 3.1.3 |
| HMS `IOStatisticsSource` ClassNotFoundException | hadoop-aws-3.4.1 needs Hadoop 3.3+, Hive 3.1.3 has 3.1.0 | Use hadoop-aws-3.1.0 + aws-sdk-1.11.271 |
