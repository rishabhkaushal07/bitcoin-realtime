# Validation

Parity checks and data quality validation for the Bitcoin real-time pipeline.

```
+-------------------+     +-------------------+
| rusty-blockparser |     | Live Pipeline     |
| (CSV oracle)      |     | (Kafka -> Iceberg)|
+--------+----------+     +--------+----------+
         |                          |
         +------------+-------------+
                      |
                      v
         +---------------------------+
         | Validation Suite          |
         |                           |
         | 1. Row count parity       |
         | 2. Value checksums        |
         | 3. Block hash continuity  |
         | 4. Latency monitoring     |
         | 5. Gap detection          |
         +---------------------------+
```

---

## Status: NOT STARTED

This component will be implemented in Phase 2 (backfill verification) and Phase 3 (ongoing monitoring).

---

## Planned Checks

| Check | Type | Frequency | Description |
|-------|------|-----------|-------------|
| Row count parity | Correctness | After backfill | CSV rows == Iceberg rows per table |
| Block hash chain | Correctness | Continuous | Each block's `hashPrev` matches previous block's `hash` |
| Height continuity | Completeness | Continuous | No gaps in block height sequence |
| Value checksum | Correctness | After backfill | Sum of satoshis matches between sources |
| Tip freshness | Latency | Continuous | Time since last block < 30 min |
| Kafka lag | Throughput | Continuous | Consumer group lag < 10 messages |
| Iceberg-StarRocks parity | Consistency | Hourly | Raw layer rows == flat table rows |

---

## Files (Planned)

| File | Purpose |
|------|---------|
| `parity_check.py` | Compare row counts and checksums between data sources |
| `chain_continuity.py` | Verify block hash chain and height sequence |
| `freshness_monitor.py` | Alert if pipeline falls behind |
| `kafka_lag_check.py` | Monitor consumer group lag |
