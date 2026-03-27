# Iceberg Raw Tables

Apache Iceberg table definitions for the raw source-of-truth layer, stored on MinIO (S3-compatible).

```
+===================================================================+
|                    Iceberg on MinIO                                |
|                    s3://warehouse/btc.db/                         |
+===================================================================+
|                                                                   |
|  Write Paths:                                                     |
|                                                                   |
|  +--------------------+       +-----------------------------+     |
|  | PyIceberg Writer   |       | PyIceberg Finality Updater  |     |
|  | (Kafka consumer)   |       | (event consumer)            |     |
|  | APPEND new data    |       | UPSERT finality_status      |     |
|  +--------+-----------+       +-------------+---------------+     |
|           |                                 |                     |
|           +----------------+----------------+                     |
|                            |                                      |
|                            v                                      |
|  +-----------------------------------------------------+         |
|  |                                                     |         |
|  |  +-------------------+    +---------------------+   |         |
|  |  | btc.blocks        |    | btc.transactions    |   |         |
|  |  | PK: block_hash    |    | PK: txid            |   |         |
|  |  | Part: bucket(10,  |    | Part: bucket(10,    |   |         |
|  |  |   height)         |    |   block_height)     |   |         |
|  |  +-------------------+    +---------------------+   |         |
|  |                                                     |         |
|  |  +-------------------+    +---------------------+   |         |
|  |  | btc.tx_in         |    | btc.tx_out          |   |         |
|  |  | PK: txid +        |    | PK: txid +          |   |         |
|  |  |   hashPrevOut +   |    |   indexOut           |   |         |
|  |  |   indexPrevOut    |    | Part: bucket(10,     |   |         |
|  |  | Part: bucket(10,  |    |   height)            |   |         |
|  |  |   block_height)   |    +---------------------+   |         |
|  |  +-------------------+                               |         |
|  +-----------------------------------------------------+         |
+===================================================================+
```

---

## Status: COMPLETE (Phase 1b)

Tables are created via PyIceberg (not Spark SQL), using `scripts/create_iceberg_tables.py`.

---

## Tables

| Table | Identifier Fields (PK) | Partition | Approx Rows (full chain) |
|-------|------------------------|-----------|------------------------:|
| `btc.blocks` | `block_hash` | `bucket(10, height)` | ~900K |
| `btc.transactions` | `txid` | `bucket(10, block_height)` | ~1B |
| `btc.tx_in` | `txid, hashPrevOut, indexPrevOut` | `bucket(10, block_height)` | ~2.5B |
| `btc.tx_out` | `txid, indexOut` | `bucket(10, height)` | ~2.7B |

---

## Table Properties

| Property | Value | Why |
|----------|-------|-----|
| `format-version` | `2` | Required for row-level deletes (reorg handling) |
| `write.format.default` | `parquet` | Columnar, compressible, industry standard |
| `write.parquet.compression-codec` | `zstd` | Best ratio-to-speed tradeoff |

---

## Write Paths

| Path | Writer | Semantics | Use Case |
|------|--------|-----------|----------|
| Append | PyIceberg writer (Kafka consumer) | Batched append | New block data from Kafka |
| Upsert | PyIceberg finality updater | Row-level overwrite via identifier fields | Finality promotion, reorg soft-delete |
| Backfill | Spark batch (Phase 2) | Bulk overwrite by partition | Historical CSV load |
| Maintenance | Spark scheduled (Phase 3) | Compaction, snapshot expiry, orphan cleanup | Ongoing table health |

**Note:** Original plan used Kafka Connect Iceberg Sink for appends. Replaced with
PyIceberg writer because the Kafka Connect runtime ZIP is not available as a
pre-built download.

---

## How to Create Tables

```bash
# Via PyIceberg script (recommended)
source .venv/bin/activate
python scripts/create_iceberg_tables.py

# Requires:
#   - HMS running and healthy (port 9083)
#   - MinIO running and healthy (port 9000)
#   - warehouse bucket exists in MinIO
```

---

## HMS JAR Compatibility

Hive Metastore 3.1.3 bundles Hadoop 3.1.0. The S3/MinIO JARs must match:

| JAR | Version | Why |
|-----|---------|-----|
| `hadoop-aws-3.1.0.jar` | 3.1.0 | Must match Hive's bundled Hadoop version |
| `aws-java-sdk-bundle-1.11.271.jar` | 1.11.271 | Must match hadoop-aws-3.1.0's AWS SDK dependency |
| `mysql-connector-j-8.4.0.jar` | 8.4.0 | MySQL 8.4 JDBC driver for HMS schema storage |

**Do NOT use hadoop-aws-3.4.1 or aws-java-sdk-bundle-1.12.x** -- they require Hadoop
3.3+ classes (`IOStatisticsSource`) that Hive 3.1.3 does not have.

---

## Files

| File | Purpose |
|------|---------|
| `create_raw_tables.sql` | Spark SQL DDL for all 4 tables (reference; use PyIceberg script instead) |

---

## Tests

| Suite | File | Tests |
|-------|------|------:|
| Integration | `tests/integration/test_iceberg_tables.py` | 22 |

Covers: HMS connectivity, schema validation, partition specs, table properties,
write/read roundtrip, idempotent creation.
