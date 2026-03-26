# Kafka Connect — Iceberg Sink

Kafka Connect configuration for writing normalized Bitcoin data from Kafka topics to Iceberg tables on MinIO.

```
+-------------------+     +-------------------+     +-------------------+
| btc.blocks.v1     |     | btc.txs.v1        |     | btc.tx_in/out.v1  |
| (Kafka topic)     |     | (Kafka topic)     |     | (Kafka topics)    |
+--------+----------+     +--------+----------+     +--------+----------+
         |                         |                          |
         +------------+------------+--------------------------+
                      |
                      v
         +---------------------------+
         | Kafka Connect             |
         | (distributed mode)        |
         |                           |
         | 4x Iceberg Sink Connectors|
         | - blocks-sink             |
         | - transactions-sink       |
         | - tx-inputs-sink          |
         | - tx-outputs-sink         |
         +-------------+-------------+
                       |
                       v
         +---------------------------+
         | Iceberg on MinIO          |
         | s3://warehouse/btc.db/    |
         +---------------------------+
```

---

## Status: NOT STARTED

This component will be implemented in Phase 1b.

---

## Planned Configuration

| Setting | Value | Why |
|---------|-------|-----|
| Connector | `io.tabular.iceberg.connect.IcebergSinkConnector` | Official Iceberg sink |
| Commit interval | 60s | Balance between latency and file count |
| Control topic | `control-iceberg` | Commit coordination across workers |
| Format | Parquet + ZSTD | Matches Iceberg table properties |

---

## Files (Planned)

| File | Purpose |
|------|---------|
| `connect-distributed.properties` | Kafka Connect worker config |
| `blocks-sink.json` | Sink connector for blocks topic |
| `transactions-sink.json` | Sink connector for transactions topic |
| `tx-inputs-sink.json` | Sink connector for tx_inputs topic |
| `tx-outputs-sink.json` | Sink connector for tx_outputs topic |
| `Dockerfile` | Custom image with Iceberg connector JARs |
