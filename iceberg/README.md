# Iceberg Raw Tables

Apache Iceberg table definitions for the raw source-of-truth layer, stored on MinIO (S3-compatible).

```
+---------------------+     +---------------------+
| Kafka Connect       |     | PyIceberg Sidecar   |
| Iceberg Sink        |     | (finality/reorg)    |
| (append path)       |     | (upsert path)       |
+----------+----------+     +----------+----------+
           |                           |
           +-------------+-------------+
                         |
                         v
              +---------------------+
              |  Iceberg on MinIO   |
              |  s3://warehouse/    |
              |  btc.db/            |
              +---------------------+
              |                     |
    +---------+---------+  +--------+---------+
    | blocks            |  | transactions     |
    | (block_hash PK)   |  | (txid PK)        |
    +---------+---------+  +--------+---------+
    | tx_in             |  | tx_out           |
    | (txid+prev PK)    |  | (txid+idx PK)    |
    +-------------------+  +------------------+
```

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

```
                    Append Path                    Upsert Path
                    (new blocks)                   (status updates)
                         |                              |
                         v                              v
              Kafka Connect Iceberg Sink     PyIceberg overwrite_with_filter
              (exactly-once semantics)       (OBSERVED -> CONFIRMED / REORGED)
                         |                              |
                         +---------- Iceberg -----------+
                                   on MinIO
```

| Path | Writer | Semantics | Use Case |
|------|--------|-----------|----------|
| Append | Kafka Connect Iceberg Sink | Exactly-once append | New block data from Kafka |
| Upsert | PyIceberg sidecar | Row-level update via identifier fields | Finality promotion, reorg soft-delete |
| Backfill | Spark batch | Bulk overwrite by partition | Historical CSV load |
| Maintenance | Spark scheduled | Compaction, snapshot expiry, orphan cleanup | Ongoing table health |

---

## Files

| File | Purpose |
|------|---------|
| `create_raw_tables.sql` | Spark SQL DDL for all 4 tables + identifier field setup |

---

## How to Create Tables

```bash
# Via PySpark connected to HMS
pyspark --conf spark.sql.catalog.btc=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.btc.type=hive \
        --conf spark.sql.catalog.btc.uri=thrift://hive-metastore:9083

# Then run:
# spark.sql(open("iceberg/create_raw_tables.sql").read())
```
