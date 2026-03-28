# StarRocks

StarRocks serving layer -- reads raw Iceberg tables via external catalog and maintains a native flat serving table on local BE disks.

```
+===================================================================+
|                   StarRocks Cluster                                |
|                (shared-nothing mode)                               |
+===================================================================+
|                                                                   |
|  +---------------------+              Ports:                      |
|  | FE (Frontend)       |                8030 HTTP                 |
|  | Query planning      |                9020 RPC                  |
|  | Metadata management |                9030 MySQL protocol       |
|  | MySQL protocol      |                                         |
|  +----------+----------+                                          |
|             |                                                     |
|  +----------v----------+              Ports:                      |
|  | BE (Backend)        |                8040 HTTP                 |
|  | Storage engine      |                9050 Heartbeat            |
|  | Query execution     |                9060 Thrift (be_port)     |
|  | Data cache          |                                         |
|  +----------+----------+                                          |
|             |                                                     |
+-------------|-----------------------------------------------------+
              |
    +---------+---------+
    |                   |
    v                   v
+-------------+   +------------------+
| Iceberg     |   | Native Tables    |
| External    |   | (local BE disks) |
| Catalog     |   |                  |
| (via HMS)   |   | bitcoin_flat_v3  |
+------+------+   +------------------+
       |
       v
+-------------+
| MinIO / S3  |
| btc.blocks  |
| btc.txs     |
| btc.tx_in   |
| btc.tx_out  |
+-------------+
```

---

## Status: COMPLETE (Phase 1c)

All components implemented and tested:
- External Iceberg catalog DDL
- Native flat serving table DDL
- Incremental flat table builder (Python)

---

## Components

| Component | Image | Ports | Data Path |
|-----------|-------|-------|-----------|
| FE (Frontend) | `starrocks/fe-ubuntu:4.0.8` | 8030, 9020, 9030 | `/local-scratch4/bitcoin_2025/starrocks-fe-meta/` |
| BE (Backend) | `starrocks/be-ubuntu:4.0.8` | 8040, 9050, 9060 | `/local-scratch4/bitcoin_2025/starrocks-data/` |

---

## SQL Files

| File | Purpose |
|------|---------|
| `iceberg_catalog_v3.sql` | External catalog pointing to HMS + MinIO (with data cache) |
| `flat_serving_v3.sql` | Native flat serving table with ARRAY<STRUCT> columns |
| `flat_table_builder.py` | Incremental flat table builder (Python, 3 modes) |

---

## External Iceberg Catalog

```sql
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://hive-metastore:9083",
    "aws.s3.endpoint" = "http://minio:9000",
    "aws.s3.access_key" = "minioadmin",
    "aws.s3.secret_key" = "minioadmin",
    "aws.s3.enable_path_style_access" = "true",
    "enable_datacache" = "true"
);
```

---

## Flat Serving Table

```
+===================================================================+
|  bitcoin_v3.bitcoin_flat_v3  (PRIMARY KEY: txid)                  |
+===================================================================+
|                                                                   |
|  Block columns:                                                   |
|    block_hash VARCHAR(64)                                         |
|    block_height INT                                               |
|    block_timestamp DATETIME                                       |
|    nTime INT                                                      |
|                                                                   |
|  Transaction columns:                                             |
|    txid VARCHAR(64)  <-- PRIMARY KEY                              |
|    tx_version INT                                                 |
|    lockTime INT                                                   |
|                                                                   |
|  Input columns:                                                   |
|    input_count INT                                                |
|    inputs ARRAY<STRUCT<                                           |
|      hashPrevOut VARCHAR(64),                                     |
|      indexPrevOut INT,                                             |
|      scriptSig VARCHAR(4096),                                     |
|      sequence BIGINT>>                                            |
|                                                                   |
|  Output columns:                                                  |
|    output_count INT                                               |
|    total_output_value BIGINT  (satoshis)                          |
|    outputs ARRAY<STRUCT<                                          |
|      indexOut INT,                                                 |
|      value BIGINT,                                                |
|      scriptPubKey VARCHAR(4096),                                  |
|      address VARCHAR(128)>>                                       |
|                                                                   |
|  Metadata:                                                        |
|    finality_status VARCHAR(16)  (OBSERVED/CONFIRMED/REORGED)      |
+===================================================================+
```

Partitioned by `block_height` ranges of 100K blocks, distributed by `HASH(txid)`.

---

## Flat Table Builder

```
flat_table_builder.py modes:

  Single:      python starrocks/flat_table_builder.py --height 170
  Range:       python starrocks/flat_table_builder.py --from-height 0 --to-height 1000
  Incremental: python starrocks/flat_table_builder.py --incremental
```

The builder reads from Iceberg via the StarRocks external catalog (4-table JOIN with
ARRAY_AGG + NAMED_STRUCT) and inserts denormalized rows into the native flat table.
Incremental mode queries the max height in both flat table and Iceberg, then builds
only the gap.

---

## Metadata Refresh Strategy

StarRocks caches Iceberg metadata. After new data lands in Iceberg:

```sql
-- Option A: Explicit refresh (recommended for near-real-time)
REFRESH EXTERNAL TABLE iceberg_catalog.btc.blocks;
REFRESH EXTERNAL TABLE iceberg_catalog.btc.transactions;
REFRESH EXTERNAL TABLE iceberg_catalog.btc.tx_in;
REFRESH EXTERNAL TABLE iceberg_catalog.btc.tx_out;

-- Option B: Short TTL (trades latency for freshness)
ALTER CATALOG iceberg_catalog SET ("iceberg_meta_cache_ttl_sec" = "30");
```

---

## Quick Connect

```bash
# Connect via MySQL protocol
mysql -h 127.0.0.1 -P 9030 -u root

# Check cluster health
curl http://localhost:8030/api/health
curl http://localhost:8040/api/health

# Check BE registration
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G"
```

---

## Tests

| Suite | File | Tests | External Deps |
|-------|------|------:|---------------|
| Unit (builder) | `tests/unit/test_flat_table_builder.py` | 21 | None (mocked) |
| Integration | `tests/integration/test_starrocks.py` | 11 | Docker services |

```bash
# Unit tests
pytest tests/unit/test_flat_table_builder.py -v

# Integration tests
pytest tests/integration/test_starrocks.py -m integration -v
```

---

## Measured Query Latency (baseline benchmark, 2026-03-27)

| Scenario | Latency | Notes |
|----------|--------:|-------|
| Cold start (first metadata load) | ~180,000 ms | Full HMS metadata + Iceberg manifest parsing + data cache population |
| Metadata refresh (4 tables) | ~9,500 ms | One-time cost after new Iceberg commits |
| Warm-cache COUNT(*) with height filter | **23-29 ms** | Data cached on BE SSD |

**Warm-cache query detail (5 blocks at height 500K, ~67K records):**

| Table | Rows | Query ms |
|-------|-----:|--------:|
| `btc.blocks` | 5 | 29 |
| `btc.transactions` | 12,786 | 23 |
| `btc.tx_in` | 24,941 | 24 |
| `btc.tx_out` | 29,385 | 23 |

Once metadata is refreshed and data is cached on BE SSD, external catalog queries are
sub-100ms. The cold-start penalty is a one-time cost per session. For production, set
`iceberg_meta_cache_ttl_sec = 30` to keep metadata warm.

Full benchmark: [docs/latency-benchmark-baseline.md](../docs/latency-benchmark-baseline.md)

---

## Roadblocks Encountered and Fixes

| Problem | Root Cause | Fix |
|---------|-----------|-----|
| StarRocks FE unhealthy after Docker restart | FE metadata references old container IPs | Clear `/local-scratch4/bitcoin_2025/starrocks-fe-meta/*` and restart |
| BE "Unmatched cluster id" | BE has old cluster metadata after FE meta wipe | Clear `/local-scratch4/bitcoin_2025/starrocks-data/*` and restart |
| BE "Invalid method name: 'heartbeat'" | Heartbeat port is 9050, not 9060 (Thrift) | Register BE with correct port: `ALTER SYSTEM ADD BACKEND '<ip>:9050'` |
| Missing pymysql | flat_table_builder.py needs pymysql for MySQL protocol | `uv pip install pymysql` |
