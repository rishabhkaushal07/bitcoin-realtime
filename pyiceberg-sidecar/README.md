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

## Status: COMPLETE (Phase 1b)

Both components are implemented and tested:
- `iceberg_writer.py` — 197 lines, batched Kafka consumer
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

# Start the finality updater
python pyiceberg-sidecar/finality_updater.py
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

## Roadblocks Encountered and Fixes

| Problem | Root Cause | Fix |
|---------|-----------|-----|
| PyArrow schema mismatch on write | Default inferred types (int64, optional) don't match Iceberg schema (int32, required) | Use `schema_to_pyarrow(table.schema())` from `pyiceberg.io.pyarrow` |
| `pa.null()` not supported in Iceberg v2 | Null type arrays rejected by Iceberg format | Use string placeholder `"unknown"` instead of `None` for nullable fields |
| `indexPrevOut` int32 overflow | Value 4294967295 (0xFFFFFFFF for coinbase) exceeds int32 max | Already handled as `UINT32_MAX` in normalizer; use 0 in test data |
| HMS Hive 4.2.0 Thrift incompatible | PyIceberg uses Thrift client, Hive 4.2.0 changed Thrift API | Downgrade to Hive 3.1.3 |
| HMS `IOStatisticsSource` ClassNotFoundException | hadoop-aws-3.4.1 needs Hadoop 3.3+, Hive 3.1.3 has 3.1.0 | Use hadoop-aws-3.1.0 + aws-sdk-1.11.271 |
