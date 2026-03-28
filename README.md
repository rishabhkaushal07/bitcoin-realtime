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
|  +-------------+  RPC   +----------------+        | 1 control topic  |   |
|                                                    +--------+---------+   |
|                                                             |            |
|                                      +----------------------+            |
|                                      |                      |            |
|                                      v                      v            |
|                       +--------------------+  +---------------------+    |
|                       | PyIceberg Writer   |  | PyIceberg Finality  |    |
|                       | (Kafka consumer)   |  | Updater             |    |
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

**Key change from original plan:** Kafka Connect Iceberg Sink was replaced with a
PyIceberg-based writer because the runtime ZIP is not available as a pre-built
download. PyIceberg provides native upsert with identifier fields for idempotent
writes and was the "valid fallback" path from the V3 architecture document.

---

## Quick Start

```bash
# 1. Install UV and dependencies
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv .venv --python 3.10
source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Start infrastructure
sudo docker compose up -d

# 3. Create Kafka topics
bash scripts/create-kafka-topics.sh

# 4. Create Iceberg tables (requires HMS healthy)
python scripts/create_iceberg_tables.py

# 5. Test normalizer (no Kafka needed)
python live-normalizer/main.py --test-block 0

# 6. Run unit tests (133 tests, no external deps)
pytest tests/unit/

# 7. Run integration tests (requires Docker services)
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
+-- live-normalizer/              # Phase 1a: ZMQ + RPC + Kafka producer
|   +-- main.py                   # Entry point (--test-block, --catchup, live)
|   +-- zmq_listener.py           # ZMQ hashblock subscriber
|   +-- rpc_client.py             # Bitcoin Core JSON-RPC client
|   +-- normalizer.py             # Block JSON -> 4 record types
|   +-- kafka_producer.py         # Publish to 4 Kafka topics
|   +-- checkpoint_store.py       # File-based restart recovery
|   +-- README.md
|
+-- pyiceberg-sidecar/            # Phase 1b: Kafka -> Iceberg + finality
|   +-- iceberg_writer.py         # Batched Kafka consumer -> Iceberg append
|   +-- finality_updater.py       # OBSERVED->CONFIRMED, reorg soft-delete
|   +-- README.md
|
+-- iceberg/                      # Iceberg raw table DDL
|   +-- create_raw_tables.sql     # 4 tables with partitioning + identifiers
|   +-- README.md
|
+-- starrocks/                    # Phase 1c: Serving layer
|   +-- iceberg_catalog_v3.sql    # External catalog -> HMS -> MinIO
|   +-- flat_serving_v3.sql       # Native flat table DDL
|   +-- flat_table_builder.py     # Incremental flat table builder
|   +-- README.md
|
+-- hive-metastore/               # HMS JAR dependencies
|   +-- mysql-connector-j-8.4.0.jar
|   +-- hadoop-aws-3.1.0.jar
|   +-- aws-java-sdk-bundle-1.11.271.jar
|   +-- hive-site.xml
|
+-- scripts/                      # Operational scripts
|   +-- create-kafka-topics.sh    # Create 4 data + 1 control topic
|   +-- create_iceberg_tables.py  # PyIceberg table creation script
|   +-- benchmark_e2e_latency.py  # End-to-end latency benchmark
|   +-- README.md
|
+-- docs/                         # Documentation
|   +-- latency-benchmark-baseline.md  # Baseline latency measurements
|
+-- kafka-connect/                # DEPRECATED (replaced by PyIceberg writer)
|   +-- README.md
|
+-- spark/                        # Phase 2: PySpark backfill + maintenance
|   +-- README.md
|
+-- validation/                   # Phase 3: Parity checks + monitoring
|   +-- README.md
|
+-- tests/
    +-- conftest.py               # Shared fixtures (sample RPC blocks)
    +-- README.md
    +-- unit/                     # 133 tests (no external deps)
    |   +-- test_normalizer.py         # 28 tests
    |   +-- test_rpc_client.py         # 6 tests
    |   +-- test_kafka_producer.py     # 9 tests
    |   +-- test_checkpoint_store.py   # 7 tests
    |   +-- test_zmq_listener.py       # 4 tests
    |   +-- test_ddl_validation.py     # 17 tests
    |   +-- test_main.py               # 13 tests
    |   +-- test_iceberg_writer.py     # 21 tests
    |   +-- test_finality_updater.py   # 7 tests
    |   +-- test_flat_table_builder.py # 21 tests
    +-- integration/              # ~60 tests (require Docker services)
        +-- test_docker_services.py    # Container health checks
        +-- test_normalizer_live.py    # Live RPC normalization
        +-- test_kafka_e2e.py          # 15 tests: normalizer -> Kafka
        +-- test_iceberg_tables.py     # 22 tests: schema, write/read
        +-- test_kafka_to_iceberg.py   # 12 tests: flush, integrity
        +-- test_starrocks.py          # 11 tests: catalog, flat table
```

---

## Data Storage Policy

> **Everything lives under `/local-scratch4/bitcoin_2025/`** (SSD).
> Docker named volumes are NOT used -- all persistent data is bind-mounted.

| Data Store | Path | Expected Size |
|-----------|------|---------------|
| Bitcoin Core | `bitcoin-core-data/` | ~600 GB |
| MinIO (Iceberg) | `minio-data/` | ~1 TB |
| StarRocks BE | `starrocks-data/` | ~200+ GB |
| StarRocks FE | `starrocks-fe-meta/` | minimal |
| Kafka | `kafka-data/` | ~50 GB |
| HMS MySQL | `hms-mysql-data/` | minimal |

---

## Technology Choices and Rationale

| Component | Technology | Why This Over Alternatives |
|-----------|-----------|---------------------------|
| **Event trigger** | ZMQ (hashblock) | Push-based, sub-second latency. Polling RPC `getblockcount` wastes CPU and adds seconds of delay. `.dat` file watching is unreliable for live blocks. |
| **Data decoder** | RPC `getblock(hash,2)` | Returns fully decoded JSON with all transactions. No need to parse raw binary formats. Available out-of-the-box with Bitcoin Core. |
| **Event backbone** | Apache Kafka (KRaft) | Durable, replayable log. Decouples normalizer from lake writer. KRaft mode eliminates ZooKeeper dependency. Single broker is sufficient for Bitcoin's ~1 block/10min throughput. |
| **Object storage** | MinIO | S3-compatible, self-hosted. Avoids cloud vendor lock-in. Iceberg and StarRocks both speak S3 natively. |
| **Table format** | Apache Iceberg v2 | Format v2 required for row-level deletes (reorg handling). Schema evolution, hidden partitioning, time travel. Open standard read by Spark, StarRocks, and PyIceberg. |
| **Catalog** | Hive Metastore 3.1.3 | Mature, widely supported. PyIceberg, Spark, and StarRocks all integrate natively. 3.1.3 specifically because 4.x broke Thrift compatibility with PyIceberg. |
| **Lake writer** | PyIceberg 0.11.1 | Native Python, no JVM. Supports identifier-field upserts for finality updates. Replaced Kafka Connect Iceberg Sink (no pre-built runtime ZIP available). V3 plan's documented "valid fallback" path. |
| **Serving layer** | StarRocks 4.0.8 | Sub-second OLAP queries on billions of rows. External Iceberg catalog reads the lake directly. Native flat table on local SSD for hot queries. |
| **Batch processing** | PySpark 4.1.1 | Required for historical CSV backfill at scale (5.6B+ rows). Also handles Iceberg table maintenance (compaction, snapshot expiry, orphan cleanup). |
| **Partitioning** | `bucket(10, height)` | Distributes data evenly across 10 buckets regardless of height skew. Bucket partitioning avoids the problem of too many partitions (one per height = 900K+ partitions) while still enabling partition pruning on height-range queries. |
| **Package manager** | UV 0.11.2 | 10-100x faster than pip. Deterministic resolution. Single `pyproject.toml` for deps + tool config. |

---

## Docker Image Versions (pinned 2026-03-26)

| Service | Image | Version |
|---------|-------|---------|
| Kafka | `apache/kafka` | 4.0.2 |
| MinIO | `minio/minio` | RELEASE.2025-09-07 |
| MinIO client | `minio/mc` | RELEASE.2025-08-13 |
| MySQL | `mysql` | 8.4.8 |
| Hive Metastore | `apache/hive` | 3.1.3 |
| StarRocks FE | `starrocks/fe-ubuntu` | 4.0.8 |
| StarRocks BE | `starrocks/be-ubuntu` | 4.0.8 |

---

## Docker Networking

All services run on a custom bridge network `bitcoin-realtime` (subnet 172.28.0.0/16).

### Known Issues and Fixes

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| Container-to-container networking broken | Stale Calico/MicroK8s iptables-legacy rules with DROP-all catch-all for "Unknown interface" | Uninstall MicroK8s: `sudo snap remove microk8s --purge`, flush: `sudo iptables-legacy -F && sudo iptables-legacy -X` |
| Container-to-container still broken after iptables flush | Zombie bridge interfaces from deleted Docker networks (`br-*`) with conflicting NAT/masquerade rules for same subnet | Remove stale bridges: `sudo ip link delete <br-name>`, restart Docker: `sudo systemctl restart docker` |
| HMS marked "unhealthy" | Hive 3.1.3 JVM startup takes 2-5 min, exceeds 120s healthcheck start_period | Restart HMS once more after it's fully running; Docker healthcheck resets |
| StarRocks FE won't start after Docker restart | Stale FE metadata references old container IPs | Clear FE meta: `sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-fe-meta/*` |
| StarRocks BE "Unmatched cluster id" | BE has old cluster metadata after FE meta wipe | Clear BE storage: `sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-data/*` |
| StarRocks BE heartbeat error | BE heartbeat port is 9050, not 9060 | Register with correct port: `ALTER SYSTEM ADD BACKEND '<ip>:9050'` |
| HMS crashes on restart: "Error creating transactional connection factory" | HMS tries to re-init schema on restart; OR MySQL not ready yet | Set `IS_RESUME: "true"` env var (maps to `SKIP_SCHEMA_INIT=true`) |
| HMS `IOStatisticsSource` ClassNotFoundException | hadoop-aws-3.4.1 requires Hadoop 3.3+, but Hive 3.1.3 bundles Hadoop 3.1.0 | Use hadoop-aws-3.1.0.jar + aws-java-sdk-bundle-1.11.271.jar |

### Startup Procedure (after fresh Docker restart)

```bash
# 1. Start all services
sudo docker compose up -d

# 2. Wait for HMS to fully start (2-5 minutes)
#    Check with: sudo docker logs hive-metastore 2>&1 | grep "Started the new metaserver"

# 3. If StarRocks FE is unhealthy, clear metadata and restart:
sudo docker stop starrocks-fe starrocks-be
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-fe-meta/*
sudo docker start starrocks-fe
sleep 30

# 4. Clear BE storage if needed and start:
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-data/*
sudo docker start starrocks-be
sleep 30

# 5. Register BE with FE:
BE_IP=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' starrocks-be)
mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050'"

# 6. Verify all services:
sudo docker compose ps
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" | grep Alive
```

---

## Implementation Phases

| Phase | Name | Status |
|-------|------|--------|
| 0 | Prerequisites | COMPLETE |
| 1a | Event Backbone (normalizer + Kafka) | COMPLETE |
| 1b | Raw Lake (PyIceberg writer -> Iceberg on MinIO) | COMPLETE |
| 1c | Serving Bridge (StarRocks external + flat table) | COMPLETE |
| 2 | Historical Backfill + Cutover | NOT STARTED |
| 3 | Operational Hardening | NOT STARTED |
| 4 | Optional Scale-Out | NOT STARTED |

See [plan.md](plan.md) for detailed task breakdowns per phase.

---

## Pipeline Latency (baseline benchmark, 2026-03-27)

Measured end-to-end with historical blocks via RPC during Bitcoin Core IBD.
Full report: [docs/latency-benchmark-baseline.md](docs/latency-benchmark-baseline.md)

```
  Block → RPC(426ms) → Normalize(14ms) → Kafka(77ms) → Iceberg(392ms) → StarRocks(25ms)
          ─────────────────── 909ms total (RPC → Iceberg) ───────────────────
```

| Stage | Small (1 tx) | Medium (~100 tx) | Large (~2,500 tx) | Bottleneck? |
|-------|----------:|---------------:|-----------------:|:-----------:|
| RPC fetch | 5 ms | 138 ms | **426 ms** | At scale |
| Normalize | 2 ms | 0.5 ms | 14 ms | Never |
| Kafka produce | 3 ms | 7 ms | 77 ms | No |
| Iceberg write | **777 ms** | 434 ms | 392 ms | At small scale |
| StarRocks query (warm) | 25 ms | 25 ms | 25 ms | No |
| **e2e per block** | **787 ms** | **580 ms** | **909 ms** | |
| **Throughput** | 5 rec/s | 1,411 rec/s | **14,768 rec/s** | |

Bitcoin produces ~1 block / 10 minutes → pipeline has **~660x headroom**.

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
| Unit | 133 | None |
| Integration | ~60 | Docker + Bitcoin Core |

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
