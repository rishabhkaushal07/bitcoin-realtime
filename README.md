# Bitcoin Real-Time Ingestion Pipeline

A real-time Bitcoin blockchain analytics platform that ingests blocks within seconds of being mined, lands them in an Iceberg lakehouse on MinIO, and serves denormalized flat tables from StarRocks.

```
 ____  _ _            _         ____            _       _____ _
| __ )(_) |_ ___ ___ (_)_ __   |  _ \ ___  __ _| |     |_   _(_)_ __ ___   ___
|  _ \| | __/ __/ _ \| | '_ \  | |_) / _ \/ _` | |_____  | | | | '_ ` _ \ / _ \
| |_) | | || (_| (_) | | | | | |  _ <  __/ (_| | |_____| | | | | | | | | |  __/
|____/|_|\__\___\___/|_|_| |_| |_| \_\___|\__,_|_|       |_| |_|_| |_| |_|\___|
```

**Started:** 2026-03-26
**Reference repo:** `/local-scratch4/bitcoin_2025/dmg-bitcoin/` (batch-only predecessor)
**Architecture plan:** [nifty-sauteeing-spring-evaluation-report-v3.md](../nifty-sauteeing-spring-evaluation-report-v3.md)
**Implementation plan:** [plan.md](plan.md)

---

## Architecture

```
+==========================================================================+
|                     BITCOIN REAL-TIME PIPELINE                           |
+==========================================================================+
|                                                                          |
|  LIVE PATH (new blocks, ~250ms end-to-end):                             |
|                                                                          |
|  +-------------+  ZMQ   +----------------+  JSON  +------------------+   |
|  | Bitcoin Core |------->| Live Normalizer|------->| Kafka (KRaft)    |   |
|  | (full node)  |<-------| (Python)       |        | 4 data topics    |   |
|  +-------------+  RPC   +----------------+        +--------+---------+   |
|                                                             |            |
|                                      +----------------------+            |
|                                      |                      |            |
|                                      v                      v            |
|                       +--------------------+  +---------------------+    |
|                       | Kafka Connect      |  | PyIceberg Sidecar   |    |
|                       | Iceberg Sink       |  | (finality/reorg)    |    |
|                       | (append: new data) |  | (upsert: status)    |    |
|                       +---------+----------+  +----------+----------+    |
|                                 |                        |               |
|                                 +------------+-----------+               |
|                                              |                           |
|                                              v                           |
|                               +----------------------------+             |
|                               |    Iceberg on MinIO        |             |
|                               |    (raw source of truth)   |             |
|                               |                            |             |
|                               |  btc.blocks      ~900K    |             |
|                               |  btc.transactions ~1B     |             |
|                               |  btc.tx_in       ~2.5B    |             |
|                               |  btc.tx_out      ~2.7B    |             |
|                               +-------------+--------------+             |
|                                             |                            |
|                                             v                            |
|                               +----------------------------+             |
|                               |    StarRocks (4.0.8)       |             |
|                               |    External Iceberg cat.   |             |
|                               |    Native flat serving tbl |             |
|                               +----------------------------+             |
|                                                                          |
|  HISTORICAL PATH (one-time backfill):                                    |
|                                                                          |
|  blk*.dat --> rusty-blockparser --> CSV --> Spark --> Iceberg on MinIO    |
|                                                                          |
|  TABLE MAINTENANCE (Spark, scheduled):                                   |
|  Compact files | Expire snapshots | Remove orphans | Rewrite manifests   |
+==========================================================================+
```

---

## Quick Start

```bash
# 1. Install UV and dependencies
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv .venv --python 3.10
source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Start infrastructure
docker compose up -d

# 3. Create Kafka topics
bash scripts/create-kafka-topics.sh

# 4. Test normalizer (no Kafka needed)
python live-normalizer/main.py --test-block 0

# 5. Run unit tests
pytest tests/unit/

# 6. Run integration tests (requires Docker services)
pytest tests/integration/ -m integration
```

---

## Project Structure

```
bitcoin-realtime/
|
+-- README.md                     # This file
+-- CLAUDE.md                     # Project conventions for Claude Code
+-- plan.md                       # Full implementation plan with phases
+-- pyproject.toml                # UV/Python dependencies + tool config
+-- docker-compose.yml            # All infrastructure services
+-- .gitignore                    # Python, Rust, Kafka, Docker, etc.
|
+-- live-normalizer/              # Python: ZMQ + RPC + Kafka producer
|   +-- main.py                   # Entry point (--test-block, --catchup, live)
|   +-- zmq_listener.py           # ZMQ hashblock subscriber
|   +-- rpc_client.py             # Bitcoin Core JSON-RPC client
|   +-- normalizer.py             # Block JSON -> 4 record types
|   +-- kafka_producer.py         # Publish to 4 Kafka topics
|   +-- checkpoint_store.py       # File-based restart recovery
|   +-- README.md                 # Module docs with data flow diagrams
|
+-- kafka-connect/                # Kafka Connect Iceberg Sink config
|   +-- README.md                 # Architecture + planned files
|
+-- pyiceberg-sidecar/            # Finality/reorg upserts via PyIceberg
|   +-- README.md                 # Architecture + planned files
|
+-- iceberg/                      # Iceberg raw table DDL
|   +-- create_raw_tables.sql     # 4 tables with partitioning + identifiers
|   +-- README.md                 # Table schemas + write paths
|
+-- starrocks/                    # StarRocks DDL
|   +-- iceberg_catalog_v3.sql    # External catalog -> HMS -> MinIO
|   +-- flat_serving_v3.sql       # Native flat table + incremental INSERT
|   +-- README.md                 # Components + metadata refresh
|
+-- spark/                        # PySpark: backfill + maintenance
|   +-- README.md                 # Planned jobs
|
+-- validation/                   # Parity checks + monitoring
|   +-- README.md                 # Planned checks
|
+-- scripts/                      # Operational scripts
|   +-- create-kafka-topics.sh    # Create 4 data + 1 control topic
|   +-- README.md                 # Script docs
|
+-- tests/
    +-- conftest.py               # Shared fixtures (sample RPC blocks)
    +-- unit/                     # Unit tests (no external deps)
    |   +-- test_normalizer.py    # 30 tests: genesis, block 170, large, edge
    |   +-- test_rpc_client.py    # 6 tests: payload, auth, errors
    |   +-- test_kafka_producer.py # 9 tests: routing, keys, serialization
    |   +-- test_checkpoint_store.py # 7 tests: persistence, atomicity
    |   +-- test_zmq_listener.py  # 4 tests: parsing, gap detection
    |   +-- test_ddl_validation.py # 13 tests: schema consistency
    +-- integration/              # Integration tests (require Docker)
        +-- test_docker_services.py    # Container health checks
        +-- test_normalizer_live.py    # Live RPC normalization
```

---

## Data Storage Policy

> **Everything lives under `/local-scratch4/bitcoin_2025/`** (SSD).
> Docker named volumes are NOT used — all persistent data is bind-mounted.

| Data Store | Path | Expected Size |
|-----------|------|---------------|
| Bitcoin Core | `bitcoin-core-data/` | ~600 GB |
| MinIO (Iceberg) | `minio-data/` | ~1 TB |
| StarRocks BE | `starrocks-data/` | ~200+ GB |
| StarRocks FE | `starrocks-fe-meta/` | minimal |
| Kafka | `kafka-data/` | ~50 GB |
| HMS MySQL | `hms-mysql-data/` | minimal |

---

## Docker Image Versions (pinned 2026-03-26)

| Service | Image | Version |
|---------|-------|---------|
| Kafka | `apache/kafka` | 4.0.2 |
| MinIO | `minio/minio` | RELEASE.2025-09-07 |
| MinIO client | `minio/mc` | RELEASE.2025-08-13 |
| MySQL | `mysql` | 8.4.8 |
| Hive Metastore | `apache/hive` | 4.2.0 |
| StarRocks FE | `starrocks/fe-ubuntu` | 4.0.8 |
| StarRocks BE | `starrocks/be-ubuntu` | 4.0.8 |

---

## Implementation Phases

| Phase | Name | Status |
|-------|------|--------|
| 0 | Prerequisites | COMPLETE |
| 1a | Event Backbone (normalizer + Kafka) | IN PROGRESS |
| 1b | Raw Lake (Kafka Connect + PyIceberg -> Iceberg) | NOT STARTED |
| 1c | Serving Bridge (StarRocks external + flat table) | NOT STARTED |
| 2 | Historical Backfill + Cutover | NOT STARTED |
| 3 | Operational Hardening | NOT STARTED |
| 4 | Optional Scale-Out | NOT STARTED |

See [plan.md](plan.md) for detailed task breakdowns per phase.

---

## Testing

```bash
# All unit tests (no external deps needed)
pytest tests/unit/ -v

# Integration tests (Docker services must be running)
pytest tests/integration/ -m integration -v

# DDL consistency checks only
pytest tests/unit/test_ddl_validation.py -v

# With coverage
pytest tests/unit/ --cov=live-normalizer --cov-report=term-missing
```

| Suite | Tests | External Deps |
|-------|------:|---------------|
| Unit | 71 | None |
| Integration | ~15 | Docker + Bitcoin Core |

---

## Bitcoin Core Node

| Field | Value |
|-------|-------|
| Binary | `bitcoind` v22.0.0 |
| Data dir | `/local-scratch4/bitcoin_2025/bitcoin-core-data/` |
| RPC port | 8332 |
| ZMQ hashblock | tcp://0.0.0.0:28332 |
| ZMQ rawblock | tcp://0.0.0.0:28333 |
| ZMQ sequence | tcp://0.0.0.0:28334 |
| txindex | enabled |

```bash
# Check sync progress
bitcoin-cli -datadir=/local-scratch4/bitcoin_2025/bitcoin-core-data \
    getblockchaininfo | grep -E '"blocks"|"headers"|"verificationprogress"'
```

---

## Machine Specs

| Resource | Value |
|----------|-------|
| CPU | 16 cores |
| RAM | 62 GB |
| Disk (local-scratch4) | 3.6 TB total, ~2.6 TB free |
| OS | Linux 6.2.0-26-generic |
| Docker | 26.0.0 |
| Python | 3.10.12 |
| UV | 0.11.2 |
