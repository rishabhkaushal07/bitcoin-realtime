# Spark

PySpark jobs for historical backfill (CSV -> Iceberg) and ongoing table maintenance (compaction, snapshot expiry, orphan cleanup).

```
Historical Backfill:

  rusty-blockparser        Spark Batch Load
  csvdump                  (one-time)
       |                        |
       v                        v
  +----------+          +----------------+
  | CSV files|  ------> | Iceberg tables |
  | blocks   |          | on MinIO       |
  | txs      |          | btc.blocks     |
  | tx_in    |          | btc.txs        |
  | tx_out   |          | btc.tx_in      |
  +----------+          | btc.tx_out     |
                        +----------------+

Table Maintenance (scheduled):

  +-------------------------------------------+
  | Spark Maintenance Jobs                    |
  |                                           |
  |  1. Expire snapshots (keep 3 days)        |
  |  2. Remove orphan files (older than 3d)   |
  |  3. Rewrite data files (compact smalls)   |
  |  4. Rewrite manifests (compact metadata)  |
  +-------------------------------------------+
```

---

## Status: NOT STARTED

This component will be implemented in Phase 2 (historical backfill) and Phase 3 (maintenance).

---

## Planned Jobs

| Job | Schedule | Purpose |
|-----|----------|---------|
| `backfill_csv_to_iceberg.py` | One-time | Load rusty-blockparser CSVs into Iceberg |
| `compact_tables.py` | Daily | Rewrite small files into larger ones |
| `expire_snapshots.py` | Daily | Remove old snapshots (keep 3 days) |
| `remove_orphans.py` | Weekly | Delete orphaned data files |

---

## Backfill Strategy

```
1. rusty-blockparser csvdump -> 4 CSV files (~754 GB total)
2. Spark reads CSVs with schema mapping to match Iceberg columns
3. Spark writes to Iceberg tables partitioned by height buckets
4. Verify row counts match between CSV and Iceberg
5. Catch-up normalizer fills gap from last historical block to live tip
```

---

## Files (Planned)

| File | Purpose |
|------|---------|
| `backfill_csv_to_iceberg.py` | Historical CSV -> Iceberg batch load |
| `compact_tables.py` | File compaction job |
| `expire_snapshots.py` | Snapshot expiry job |
| `remove_orphans.py` | Orphan file cleanup |
| `spark_session.py` | Shared SparkSession factory with Iceberg + S3 config |
