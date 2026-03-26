# Bitcoin Real-Time Pipeline — Implementation Plan

## Architecture Overview

```
+=========================================================================+
|                        BITCOIN REAL-TIME PIPELINE                       |
+=========================================================================+
|                                                                         |
|   +-------------+     +------------------+     +--------------------+   |
|   | Bitcoin Core |     | Live Normalizer  |     | Kafka (KRaft)      |   |
|   | (full node)  |     | (Python)         |     | 4 data topics      |   |
|   |              |     |                  |     | 1 control topic    |   |
|   | ZMQ -------->|---->| normalize ------>|---->|                    |   |
|   | RPC <--------|<----| (4 record types) |     |                    |   |
|   +-------------+     +------------------+     +---------+----------+   |
|                                                          |              |
|                                   +----------------------+              |
|                                   |                      |              |
|                                   v                      v              |
|                    +--------------------+  +---------------------+      |
|                    | Kafka Connect      |  | PyIceberg Sidecar   |      |
|                    | Iceberg Sink       |  | (finality/reorg)    |      |
|                    | (append path)      |  | (upsert path)       |      |
|                    +---------+----------+  +----------+----------+      |
|                              |                        |                 |
|                              +------------+-----------+                 |
|                                           |                             |
|                                           v                             |
|                              +------------------------+                 |
|                              | Iceberg on MinIO       |                 |
|                              | (raw source of truth)  |                 |
|                              | btc.blocks             |                 |
|                              | btc.transactions       |                 |
|                              | btc.tx_in              |                 |
|                              | btc.tx_out             |                 |
|                              +------------+-----------+                 |
|                                           |                             |
|                                           v                             |
|                              +------------------------+                 |
|                              | StarRocks              |                 |
|                              | (serving layer)        |                 |
|                              | External Iceberg cat.  |                 |
|                              | Native flat table      |                 |
|                              +------------------------+                 |
|                                                                         |
|   HISTORICAL BACKFILL:                                                  |
|   blk*.dat -> rusty-blockparser -> CSV -> Spark -> Iceberg on MinIO     |
|                                                                         |
|   TABLE MAINTENANCE (Spark scheduled):                                  |
|   Compact files | Expire snapshots | Remove orphans | Rewrite manifests |
+=========================================================================+
```

---

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Blockchain node | Bitcoin Core | v22.0.0 | Full node with RPC + ZMQ + txindex |
| Event trigger | ZMQ (hashblock) | via bitcoind | Real-time block notifications |
| Data decoder | RPC getblock(hash,2) | via bitcoind | Decoded block JSON |
| Normalizer | Python | 3.10 | Map RPC JSON to 4 record types |
| Event backbone | Apache Kafka | 4.0.2 | Replay, buffering, fan-out |
| Object storage | MinIO | 2025-09-07 | S3-compatible, Iceberg warehouse |
| Table format | Apache Iceberg | v2 | Raw source of truth |
| Catalog | Hive Metastore | 4.2.0 | Iceberg catalog backend |
| Catalog DB | MySQL | 8.4.8 | HMS metadata storage |
| Lake writer (append) | Kafka Connect | Iceberg Sink | Exactly-once appends |
| Lake writer (upsert) | PyIceberg | 0.11.1 | Finality/reorg updates |
| Serving layer | StarRocks | 4.0.8 | OLAP queries, flat table |
| Historical parser | rusty-blockparser | latest | CSV backfill + oracle |
| Batch processing | PySpark | 4.1.1 | Backfill + table maintenance |
| Package manager | UV | 0.11.2 | Fast Python dependency management |
| Testing | pytest | 9.0.2 | Unit + integration tests |

---

## Phase Plan

### Phase 0 — Prerequisites [COMPLETE]

| Task | Status | Notes |
|------|--------|-------|
| Bitcoin Core installed + config | Done | v22.0.0, RPC+ZMQ+txindex |
| Bitcoin Core IBD started | Done | Syncing in background |
| Rust toolchain + rusty-blockparser | Done | Built in 20.45s |
| Project repo initialized | Done | `bitcoin-realtime/` |
| Docker Compose scaffolded | Done | 6 services, all on SSD |
| UV + pyproject.toml | Done | All deps installed |
| .gitignore | Done | Python, Rust, Kafka, Docker |
| Test framework | Done | 71 unit tests, all passing |
| Subfolder READMEs | Done | ASCII art + tables |

---

### Phase 1a — Event Backbone [IN PROGRESS]

```
Bitcoin Core ZMQ --> Live Normalizer --> Kafka (4 topics)
```

| Task | Status | Notes |
|------|--------|-------|
| Python normalizer modules | Done | 6 modules, tested |
| Kafka topic creation script | Done | 4 data + 1 control |
| `docker compose up -d` | TODO | Start Kafka + all services |
| Run topic creation script | TODO | After Kafka healthy |
| End-to-end test: normalizer -> Kafka | TODO | Verify messages in topics |
| Catchup mode test | TODO | Replay blocks 0-N |

**Next immediate steps:**
1. `docker compose up -d` — bring up all containers
2. `bash scripts/create-kafka-topics.sh` — create topics
3. Test normalizer `--test-block 0` against live RPC
4. Test normalizer with Kafka: produce to topics, consume to verify

---

### Phase 1b — Raw Lake [NOT STARTED]

```
Kafka --> Kafka Connect Iceberg Sink --> Iceberg on MinIO
          PyIceberg Sidecar         --> Iceberg on MinIO (upserts)
```

| Task | Status | Notes |
|------|--------|-------|
| MinIO + HMS healthy | TODO | Part of compose up |
| Iceberg tables created via Spark SQL | TODO | `create_raw_tables.sql` |
| Kafka Connect Docker image | TODO | Custom with Iceberg JARs |
| 4 Iceberg sink connectors | TODO | One per topic |
| PyIceberg sidecar | TODO | Finality + reorg upserts |
| Commit interval tuning | TODO | Target: 60s |
| End-to-end: Kafka -> Iceberg verify | TODO | Check Parquet files in MinIO |

---

### Phase 1c — Serving Bridge [NOT STARTED]

```
Iceberg on MinIO --> StarRocks External Catalog --> Flat Table
```

| Task | Status | Notes |
|------|--------|-------|
| StarRocks external catalog | TODO | `iceberg_catalog_v3.sql` |
| Metadata refresh strategy | TODO | Explicit REFRESH or TTL |
| Data cache enabled | TODO | On BE local disks |
| Flat serving table | TODO | `flat_serving_v3.sql` |
| Per-block incremental insert | TODO | After each Iceberg commit |
| Query validation | TODO | Compare against raw layer |

---

### Phase 2 — Historical Backfill + Cutover [NOT STARTED]

```
blk*.dat --> rusty-blockparser --> CSV --> Spark --> Iceberg
                                                      |
Live normalizer catches up from last historical ------+
```

| Task | Status | Notes |
|------|--------|-------|
| rusty-blockparser CSV dump | TODO | After IBD complete |
| Spark batch load: CSV -> Iceberg | TODO | With schema mapping |
| Verify row counts | TODO | CSV vs Iceberg parity |
| Catch-up normalizer | TODO | Last historical -> live tip |
| Flat table full build | TODO | Incremental from Iceberg |
| rusty-blockparser parity check | TODO | Cross-validate counts |

---

### Phase 3 — Operational Hardening [NOT STARTED]

| Task | Status |
|------|--------|
| Freshness monitoring (tip age < 30min) | TODO |
| Reorg handling tested (simnet or testnet) | TODO |
| Replay and restart recovery tested | TODO |
| Flat table incremental refresh automated | TODO |
| Alerting on Kafka consumer lag | TODO |

---

### Phase 4 — Optional Scale-Out [NOT STARTED]

| Task | Status |
|------|--------|
| Second StarRocks shared-data cluster | TODO |
| Spark analytics on raw Iceberg layer | TODO |
| Async MVs for forensic query patterns | TODO |

---

## Testing Strategy

```
+-------------------------------------------+
|              Test Pyramid                 |
+-------------------------------------------+
|                                           |
|          /  Integration Tests  \          |  Docker services health
|         /    (require Docker)   \         |  Live RPC normalization
|        /                         \        |  End-to-end Kafka flow
|       +---------------------------+       |
|      /      Unit Tests             \      |  Normalizer correctness
|     /    (no external deps)         \     |  RPC client mocking
|    /                                 \    |  Kafka producer routing
|   /                                   \   |  Checkpoint persistence
|  /                                     \  |  DDL consistency
| +---------------------------------------+ |  Schema validation
+-------------------------------------------+
```

| Test Suite | Count | Scope | Command |
|-----------|------:|-------|---------|
| Unit tests | 71 | No external deps | `pytest tests/unit/` |
| Integration tests | ~15 | Docker + Bitcoin Core | `pytest tests/integration/ -m integration` |
| DDL validation | 13 | File consistency | `pytest tests/unit/test_ddl_validation.py` |

---

## Data Storage (all on SSD: /local-scratch4/bitcoin_2025/)

| Store | Path | Expected Size |
|-------|------|---------------|
| Bitcoin Core | `bitcoin-core-data/` | ~600 GB |
| MinIO (Iceberg) | `minio-data/` | ~1 TB |
| StarRocks BE | `starrocks-data/` | ~200+ GB |
| Kafka | `kafka-data/` | ~50 GB |
| HMS MySQL | `hms-mysql-data/` | minimal |
| StarRocks FE | `starrocks-fe-meta/` | minimal |

---

## Code Organization Best Practices

| Rule | Rationale |
|------|-----------|
| Max ~200 LOC per module | Maintainability, easy code review |
| One responsibility per file | Clear ownership, testable |
| Flat record types, no ORMs | Simple, debuggable, Kafka-friendly |
| Decimal for BTC math | No IEEE 754 precision loss |
| Idempotent producers | Exactly-once Kafka semantics |
| File-based checkpoints | Simple, atomic, survives restarts |
| UV for dependency management | Fast, deterministic |
| pytest with markers | Separate unit/integration/slow |
