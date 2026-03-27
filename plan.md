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
|                    | PyIceberg Writer   |  | PyIceberg Finality  |      |
|                    | (Kafka consumer)   |  | Updater (upsert)    |      |
|                    | (append path)      |  | (CONFIRMED/REORGED) |      |
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
| Catalog | Hive Metastore | 3.1.3 | Iceberg catalog backend |
| Catalog DB | MySQL | 8.4.8 | HMS metadata storage |
| Lake writer (append) | PyIceberg | 0.11.1 | Kafka consumer → Iceberg append |
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
| Test framework | Done | 133 unit tests, all passing |
| Subfolder READMEs | Done | ASCII art + tables |

---

### Phase 1a — Event Backbone [COMPLETE]

```
Bitcoin Core ZMQ --> Live Normalizer --> Kafka (4 topics)
```

| Task | Status | Notes |
|------|--------|-------|
| Python normalizer modules | Done | 6 modules, tested |
| Kafka topic creation script | Done | 4 data + 1 control |
| `docker compose up -d` | Done | All 6 services started |
| Run topic creation script | Done | 5 topics created |
| End-to-end test: normalizer -> Kafka | Done | 15 integration tests |
| Catchup mode test | Done | 5 integration tests |
| Docker fixes | Done | Dual Kafka listeners, custom subnet, StarRocks CMD, MySQL port |

---

### Phase 1b — Raw Lake [COMPLETE]

```
Kafka --> PyIceberg Writer --> Iceberg on MinIO (appends)
          PyIceberg Finality Updater --> Iceberg on MinIO (upserts)
```

Note: Kafka Connect Iceberg Sink was replaced with PyIceberg writer (V3 plan
"valid fallback" path) because the runtime ZIP is not available as pre-built
download and must be compiled from source. PyIceberg provides native upsert
with identifier fields for idempotent writes.

| Task | Status | Notes |
|------|--------|-------|
| MinIO + HMS healthy | Done | HMS downgraded 4.2.0 → 3.1.3 (Thrift compat) |
| Iceberg tables created via PyIceberg | Done | 4 tables with identifier fields |
| HMS JAR compatibility | Done | hadoop-aws-3.1.0 + aws-sdk-1.11.271 |
| PyIceberg Kafka → Iceberg writer | Done | Batched consumer, configurable flush |
| PyIceberg finality updater | Done | OBSERVED→CONFIRMED, reorg soft-delete |
| Iceberg table integration tests | Done | 22 tests (schema, write/read, idempotent) |
| Kafka → Iceberg integration tests | Done | 12 tests (flush, data integrity, finality) |
| Unit tests (writer + updater) | Done | 28 tests |

---

### Phase 1c — Serving Bridge [COMPLETE]

```
Iceberg on MinIO --> StarRocks External Catalog --> Flat Table
```

| Task | Status | Notes |
|------|--------|-------|
| StarRocks external catalog SQL | Done | `iceberg_catalog_v3.sql` |
| Metadata refresh strategy | Done | Explicit REFRESH + TTL option documented |
| Data cache enabled | Done | In catalog properties |
| Flat serving table SQL | Done | `flat_serving_v3.sql`, PRIMARY KEY(txid) |
| Flat table builder script | Done | `flat_table_builder.py` (single/range/incremental) |
| StarRocks integration tests | Done | 11 tests (catalog, flat table, builder) |
| Unit tests (builder) | Done | 21 tests (SQL structure, range, incremental) |

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
| Unit tests | 133 | No external deps | `pytest tests/unit/` |
| Integration tests | ~60 | Docker + Bitcoin Core | `pytest tests/integration/ -m integration` |
| DDL validation | 17 | File consistency | `pytest tests/unit/test_ddl_validation.py` |

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
