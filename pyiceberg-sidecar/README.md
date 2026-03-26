# PyIceberg Sidecar

Python service that handles status updates (finality promotions and reorg soft-deletes) on Iceberg tables via row-level upserts.

```
+--------------------+
| Kafka              |
| control/finality   |
| events             |
+---------+----------+
          |
          v
+--------------------+
| PyIceberg Sidecar  |
|                    |
| Consumes events:   |
|  CONFIRMED (6+     |
|    blocks deep)    |
|  REORGED (block    |
|    disconnected)   |
+---------+----------+
          |
          v
+--------------------+
| Iceberg on MinIO   |
| (row-level update) |
|                    |
| finality_status:   |
|  OBSERVED          |
|    -> CONFIRMED    |
|    -> REORGED      |
+--------------------+
```

---

## Status: NOT STARTED

This component will be implemented in Phase 1b alongside Kafka Connect.

---

## Why PyIceberg Instead of Kafka Connect?

| Concern | Kafka Connect Sink | PyIceberg |
|---------|-------------------|-----------|
| Append new rows | Yes (primary use) | Yes |
| Update existing rows | No (append-only) | Yes (via identifier fields) |
| Delete / soft-delete | No | Yes (overwrite with filter) |

The Kafka Connect Iceberg Sink is append-only. Finality promotions (`OBSERVED -> CONFIRMED`) and reorg handling (`-> REORGED`) require row-level updates, which PyIceberg provides natively via `overwrite()` with filter expressions.

---

## Planned Operations

| Operation | Trigger | Iceberg Action |
|-----------|---------|----------------|
| Confirm block | Block is 6+ deep | Update `finality_status` from `OBSERVED` to `CONFIRMED` |
| Reorg block | ZMQ disconnect event | Update `finality_status` to `REORGED` on all 4 tables |

---

## Files (Planned)

| File | Purpose |
|------|---------|
| `sidecar.py` | Main event loop — consume Kafka, apply upserts |
| `iceberg_client.py` | PyIceberg catalog + table operations |
| `finality.py` | Finality promotion logic (depth tracking) |
| `reorg.py` | Reorg detection and soft-delete logic |
