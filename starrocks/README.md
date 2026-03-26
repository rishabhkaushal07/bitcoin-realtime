# StarRocks

StarRocks serving layer — reads raw Iceberg tables via external catalog and maintains a native flat serving table on local BE disks.

```
+---------------------------+
|      StarRocks Cluster    |
|  (shared-nothing mode)    |
+---------------------------+
|                           |
|  +---------------------+  |
|  | FE (Frontend)       |  |     Ports:
|  | Query planning      |  |       8030 HTTP
|  | Metadata management |  |       9020 RPC
|  | MySQL protocol      |  |       9030 MySQL
|  +----------+----------+  |
|             |              |
|  +----------v----------+  |
|  | BE (Backend)        |  |     Ports:
|  | Storage engine      |  |       8040 HTTP
|  | Query execution     |  |       9060 Thrift
|  | Data cache          |  |
|  +----------+----------+  |
|             |              |
+-------------|---+----------+
              |   |
    +---------+   +---------+
    |                       |
    v                       v
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
| btc.tx_in   |
| btc.tx_out  |
| btc.txs     |
+-------------+
```

---

## Components

| Component | Image | Port | Data Path |
|-----------|-------|------|-----------|
| FE (Frontend) | `starrocks/fe-ubuntu:4.0.8` | 8030, 9020, 9030 | `/local-scratch4/bitcoin_2025/starrocks-fe-meta/` |
| BE (Backend) | `starrocks/be-ubuntu:4.0.8` | 8040, 9060 | `/local-scratch4/bitcoin_2025/starrocks-data/` |

---

## SQL Files

| File | Purpose |
|------|---------|
| `iceberg_catalog_v3.sql` | External catalog pointing to HMS + MinIO (with data cache) |
| `flat_serving_v3.sql` | Native flat serving table + per-block incremental INSERT |

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

| Field | Type | Description |
|-------|------|-------------|
| `block_hash` | VARCHAR(64) | Block hash |
| `block_height` | INT | Block height (partition key) |
| `block_timestamp` | DATETIME | Block time |
| `txid` | VARCHAR(64) | Transaction ID (primary key) |
| `input_count` | INT | Number of inputs |
| `inputs` | ARRAY<STRUCT> | Nested input details |
| `output_count` | INT | Number of outputs |
| `total_output_value` | BIGINT | Sum of output satoshis |
| `outputs` | ARRAY<STRUCT> | Nested output details |
| `finality_status` | VARCHAR(16) | OBSERVED / CONFIRMED / REORGED |

Partitioned by `block_height` ranges of 100K blocks, distributed by `HASH(txid)`.

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
```
