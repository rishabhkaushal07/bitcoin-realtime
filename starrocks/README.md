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

## Status: COMPLETE and LIVE (Phase 1c)

All components implemented, tested, and serving live data since 2026-03-28:
- External Iceberg catalog DDL
- Native flat serving table DDL (composite PK: `txid, block_height`)
- Incremental flat table builder (Python)
- Custom BE configuration (40GB mem_limit, spill-to-disk, tuned timeouts)

---

## Components

| Component | Image | Ports | Data Path |
|-----------|-------|-------|-----------|
| FE (Frontend) | `starrocks/fe-ubuntu:4.0.8` | 8030, 9020, 9030 | `/local-scratch4/bitcoin_2025/starrocks-fe-meta/` |
| BE (Backend) | `starrocks/be-ubuntu:4.0.8` | 8040, 9050, 9060 | `/local-scratch4/bitcoin_2025/starrocks-data/` |
| BE Spill dir | (same container) | — | `/local-scratch4/bitcoin_2025/starrocks-spill/` |

---

## SQL Files

| File | Purpose |
|------|---------|
| `iceberg_catalog_v3.sql` | External catalog pointing to HMS + MinIO (with data cache) |
| `flat_serving_v3.sql` | Native flat serving table with ARRAY<STRUCT> columns |
| `flat_table_builder.py` | Incremental flat table builder (Python, 3 modes) |
| `be.conf` | Custom BE configuration (40GB mem_limit, spill-to-disk paths) |

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
|  bitcoin_v3.bitcoin_flat_v3                                       |
|  PRIMARY KEY: (txid, block_height)                                |
+===================================================================+
|                                                                   |
|  Key columns (must be first for StarRocks PRIMARY KEY model):     |
|    txid VARCHAR(64)            <-- part of PK                     |
|    block_height INT            <-- part of PK + partition column   |
|                                                                   |
|  Block columns:                                                   |
|    block_hash VARCHAR(64)                                         |
|    block_timestamp DATETIME                                       |
|    nTime INT                                                      |
|                                                                   |
|  Transaction columns:                                             |
|    tx_version INT                                                 |
|    lockTime INT                                                   |
|                                                                   |
|  Input columns:                                                   |
|    input_count INT                                                |
|    inputs ARRAY<STRUCT<                                           |
|      hashPrevOut VARCHAR(64),                                     |
|      indexPrevOut INT,                                             |
|      scriptSig STRING,                                            |
|      sequence BIGINT>>                                            |
|                                                                   |
|  Output columns:                                                  |
|    output_count INT                                               |
|    total_output_value BIGINT  (satoshis)                          |
|    outputs ARRAY<STRUCT<                                          |
|      indexOut INT,                                                 |
|      value BIGINT,                                                |
|      scriptPubKey STRING,                                         |
|      address VARCHAR(128)>>                                       |
|                                                                   |
|  Metadata:                                                        |
|    finality_status VARCHAR(16)  (OBSERVED/CONFIRMED/REORGED)      |
+===================================================================+
```

Partitioned by `RANGE(block_height)` in 100K-block chunks (p0 through p900k),
distributed by `HASH(txid)`.

**Note:** StarRocks PRIMARY KEY model requires key columns to be the first columns in
the schema, and partition columns must be part of the key. Hence the composite PK
`(txid, block_height)` with `block_height` as the second key column.

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

## BE Configuration and Memory Tuning

The BE runs with a custom `be.conf` mounted from `starrocks/be.conf`. This is critical
for a shared machine where other services (Bitcoin Core, Kafka, MinIO, Python pipelines)
compete for RAM.

| Parameter | Value | Why |
|-----------|-------|-----|
| `mem_limit` | `40G` | Fixed limit. Default is 90% of system RAM (~56G on 62G machine), starving other services. 40G leaves headroom for Bitcoin Core (~12G), Kafka, MinIO, and Python processes |
| `spill_local_storage_dir` | `/opt/starrocks/be/spill` | Bound to SSD at `/local-scratch4/bitcoin_2025/starrocks-spill/`. Overflow destination for large queries |
| `storage_root_path` | `/opt/starrocks/be/storage` | Bound to SSD at `/local-scratch4/bitcoin_2025/starrocks-data/` |

### Global Session Variables (set once, persist across sessions)

```sql
SET GLOBAL enable_spill = true;          -- Allow spill-to-disk for memory-heavy queries
SET GLOBAL spill_mode = 'auto';          -- Spill triggered by memory pressure (not forced)
SET GLOBAL query_timeout = 3600;         -- 1 hour for complex Iceberg queries
SET GLOBAL insert_timeout = 7200;        -- 2 hours for large flat table batch inserts
```

### Docker Compose Volumes

```yaml
starrocks-be:
  volumes:
    - /local-scratch4/bitcoin_2025/starrocks-data:/opt/starrocks/be/storage
    - /local-scratch4/bitcoin_2025/starrocks-spill:/opt/starrocks/be/spill
    - ./starrocks/be.conf:/opt/starrocks/be/conf/be.conf:ro
```

### Verifying BE Configuration

```bash
# Check mem_limit and spill settings
curl -s http://localhost:8040/varz | grep -E "^mem_limit|^spill_local_storage"

# Check via SHOW BACKENDS (MemLimit column)
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" | grep MemLimit

# Check spill variables
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW VARIABLES LIKE '%spill%'"
```

### Historical Reference

For detailed notes on StarRocks memory management, spill-to-disk debugging, query
profiling, and heap analysis, see the old repo documentation at
the legacy batch pipeline's `starrocks/README.md`. That repo ran StarRocks 3.1.3 (all-in-one) on
much larger machines (256GB-1.5TB RAM) and contains extensive notes on tuning for
full-chain analytics workloads.

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

## Live Pipeline Integration (2026-03-28)

The flat table builder has been verified against live Iceberg data from the running
pipeline. Block 942,725 (5,684 transactions) was denormalized in **1.4 seconds**:

```bash
$ python starrocks/flat_table_builder.py --height 942725
2026-03-28 19:12:54 [INFO] Building flat table for block 942725
2026-03-28 19:12:55 [INFO] Inserted 5684 transactions
2026-03-28 19:12:55 [INFO] Done in 1.4s
```

For continuous operation, run `--incremental` periodically to catch up with new blocks:

```bash
# Cron or watch loop:
python starrocks/flat_table_builder.py --incremental
```

---

## Roadblocks Encountered and Fixes

| Problem | Root Cause | Fix |
|---------|-----------|-----|
| StarRocks FE unhealthy after Docker restart | FE metadata references old container IPs | Clear `/local-scratch4/bitcoin_2025/starrocks-fe-meta/*` and restart |
| BE "Unmatched cluster id" | BE has old cluster metadata after FE meta wipe | Clear `/local-scratch4/bitcoin_2025/starrocks-data/*` and restart |
| BE "Invalid method name: 'heartbeat'" | Heartbeat port is 9050, not 9060 (Thrift) | Register BE with correct port: `ALTER SYSTEM ADD BACKEND '<ip>:9050'` |
| Missing pymysql | flat_table_builder.py needs pymysql for MySQL protocol | `uv pip install pymysql` |
| `PRIMARY KEY (txid)` rejected | StarRocks requires PK columns first in schema AND partition column in PK | Changed to composite PK `(txid, block_height)` with txid first, block_height second |
| BE OOM risk on 62GB shared machine | Default `mem_limit=90%` takes ~56GB, starving other services | Custom `be.conf` with `mem_limit=40G`, spill-to-disk enabled |
| Large queries timeout | Default `query_timeout=300s` too short for Iceberg joins | `SET GLOBAL query_timeout = 3600` (1 hour) |
