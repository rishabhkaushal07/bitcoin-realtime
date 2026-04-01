# SSD Crash Recovery Runbook

**Last updated:** 2026-04-01
**Alert email:** bdh_dev@sfu.ca
**SSD device:** `/dev/sdd2` mounted at `/local-scratch4` (4TB external, ext4)

---

## Why This Exists

The external 4TB SSD at `/local-scratch4` is unreliable and crashes periodically,
causing I/O errors that take down the Bitcoin real-time pipeline. When the SSD
crashes, all services that depend on data stored there stop working. Recovery
requires unmounting and remounting the SSD, then restarting all services in the
correct order.

This document covers:

1. What services exist and their dependency order
2. How to manually restart everything
3. How to use the automated recovery script
4. How to set up the monitoring watchdog
5. Known failure modes and their fixes
6. Post-recovery verification checklist
7. Recovery session log (2026-03-31)

### Related documents

| Document | Location | Purpose |
|----------|----------|---------|
| This runbook | `docs/ssd-crash-recovery.md` | Full recovery procedures and failure modes |
| Watchdog docs | `docs/watchdog-monitor.md` | Comprehensive monitoring script documentation |
| Recovery script | `scripts/recover-pipeline.sh` | One-click automated recovery |
| Watchdog script | `scripts/watchdog.sh` | Continuous health monitoring + email alerts |
| Architecture plan | `../nifty-sauteeing-spring-evaluation-report-v3.md` | V3 pipeline architecture (for reference) |
| Project README | `README.md` | Pipeline overview, quick start, data model |

---

## Service Inventory

There are **11 checkpoints** across 3 tiers that must be healthy for the pipeline
to function. They must start in dependency order.

### Tier 1: Docker Infrastructure

| # | Service | Container | Image | Ports | Data on SSD |
|---|---------|-----------|-------|-------|-------------|
| 1 | MySQL (HMS backend) | `mysql-hms` | mysql:8.4.8 | 3307->3306 | `hms-mysql-data/` |
| 2 | Hive Metastore | `hive-metastore` | apache/hive:3.1.3 | 9083 | None (MySQL backend) |
| 3 | MinIO | `minio` | minio/minio:2025-09-07 | 9000, 9001 | `minio-data/` |
| 4 | Kafka | `kafka` | apache/kafka:4.0.2 | 9092, 9093 | `kafka-data/` |
| 5 | StarRocks FE | `starrocks-fe` | starrocks/fe-ubuntu:4.0.8 | 8030, 9020, 9030 | `starrocks-fe-meta/` |
| 6 | StarRocks BE | `starrocks-be` | starrocks/be-ubuntu:4.0.8 | 8040, 9050, 9060 | `starrocks-data/`, `starrocks-spill/` |

### Tier 2: Bitcoin Core (host process)

| # | Service | Binary | Ports | Data on SSD |
|---|---------|--------|-------|-------------|
| 7 | Bitcoin Core | `bitcoind` | 8332 (RPC), 28332-28334 (ZMQ) | `bitcoin-core-data/` (~849 GB) |

### Tier 3: Pipeline Processes (host Python)

| # | Service | Script | Ports | Data on SSD |
|---|---------|--------|-------|-------------|
| 8 | Live Normalizer | `live-normalizer/main.py` | None | `checkpoint.json`, `logs/normalizer.log` |
| 9 | Iceberg Writer | `pyiceberg-sidecar/iceberg_writer.py` | None | `logs/iceberg_writer.log` |

### Dependency Graph

```
                  mysql-hms
                      |
                      v
               hive-metastore
                /          \
               v            v
           minio          kafka
              \           /    \
               v         v      v
          starrocks-fe   normalizer
               |              |
               v              v
          starrocks-be   iceberg-writer
```

---

## Quick Recovery (Automated)

After remounting the SSD:

```bash
# One-click recovery
bash /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/recover-pipeline.sh
```

The script is idempotent -- safe to run multiple times. It handles all 5 phases:
SSD verification, Docker infrastructure, Bitcoin Core, Kafka topics, pipeline
processes, and end-to-end verification.

See [Automated Recovery Script](#automated-recovery-script) for details.

---

## Manual Recovery Procedure

### Step 0: Remount the SSD

If the SSD disconnected:

```bash
# Check if mounted
mount | grep local-scratch4

# If not mounted, mount it
sudo mount /dev/sdd2 /local-scratch4

# Verify
df -h /local-scratch4
ls /local-scratch4/bitcoin_2025/
```

### Step 1: Start Docker infrastructure

```bash
cd /local-scratch4/bitcoin_2025/bitcoin-realtime
sudo docker compose up -d
```

### Step 2: Wait for HMS (2-5 minutes)

Hive Metastore JVM startup is slow. Wait for port 9083:

```bash
# Option A: Watch docker logs
sudo docker logs -f hive-metastore 2>&1 | grep "Started the new metaserver"

# Option B: Check port
timeout 3 bash -c "cat < /dev/tcp/localhost/9083" 2>/dev/null && echo "OPEN" || echo "CLOSED"
```

### Step 3: Restart MinIO if needed

After SSD crash, MinIO often enters a "0 drives" state. Check:

```bash
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec minio mc ls myminio/warehouse/btc.db/
```

If you see errors like "0 drives provided" or "internal error":

```bash
sudo docker restart minio
sleep 10
# Verify again
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec minio mc ls myminio/warehouse/btc.db/
```

### Step 4: Handle StarRocks FE (conditional)

Check FE health:

```bash
curl -sf http://localhost:8030/api/health
```

If FE is unhealthy (no response or stuck in UNKNOWN state), the BDB journal
is corrupted. Wipe and restart:

```bash
sudo docker stop starrocks-fe starrocks-be
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-fe-meta/*
sudo docker start starrocks-fe
sleep 30
curl -sf http://localhost:8030/api/health
```

### Step 5: Handle StarRocks BE (conditional)

If FE meta was wiped, BE must also be wiped (cluster ID mismatch):

```bash
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-data/*
sudo docker start starrocks-be
sleep 10

# Register BE with FE
BE_IP=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' starrocks-be)
mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050'"

# Verify
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" | grep Alive
```

### Step 6: Recreate StarRocks catalog and flat table (only if wiped)

```bash
mysql -h 127.0.0.1 -P 9030 -u root < starrocks/iceberg_catalog_v3.sql
mysql -h 127.0.0.1 -P 9030 -u root < starrocks/flat_serving_v3.sql
```

### Step 7: Start Bitcoin Core

```bash
bitcoind -datadir=/local-scratch4/bitcoin_2025/bitcoin-core-data -daemon

# Wait for RPC (may take 60-120s after crash)
for i in $(seq 1 12); do
    bitcoin-cli -datadir=/local-scratch4/bitcoin_2025/bitcoin-core-data \
        getblockchaininfo 2>&1 | grep -q '"blocks"' && echo "RPC ready" && break
    echo "Waiting for RPC... ($i)"
    sleep 10
done
```

### Step 8: Verify Kafka topics

```bash
bash scripts/create-kafka-topics.sh
```

Uses `--if-not-exists`, safe to run repeatedly.

### Step 9: Start pipeline processes

```bash
cd /local-scratch4/bitcoin_2025/bitcoin-realtime

# Kill any stale processes
pkill -f "live-normalizer/main.py" 2>/dev/null
pkill -f "iceberg_writer.py" 2>/dev/null
sleep 2

# Start normalizer with catchup (fills gap from checkpoint to current tip)
nohup .venv/bin/python live-normalizer/main.py \
    --rpc-url http://127.0.0.1:8332 \
    --rpc-user bitcoinrpc \
    --rpc-password changeme_strong_password_here \
    --kafka-bootstrap localhost:9092 \
    --zmq-url tcp://127.0.0.1:28332 \
    --catchup \
    > logs/normalizer.log 2>&1 &

# Start Iceberg writer
nohup .venv/bin/python pyiceberg-sidecar/iceberg_writer.py \
    >> logs/iceberg_writer.log 2>&1 &
```

### Step 10: Verify

```bash
# Docker containers
sudo docker compose ps

# StarRocks
mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" | grep Alive

# Kafka lag
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --all-groups

# Pipeline processes
pgrep -a -f "live-normalizer/main.py"
pgrep -a -f "iceberg_writer.py"

# Logs
tail -5 logs/normalizer.log
tail -5 logs/iceberg_writer.log
```

### Step 11: Rebuild flat table (optional)

After the writer drains the Kafka backlog:

```bash
python starrocks/flat_table_builder.py --incremental
```

---

## Automated Recovery Script

**Location:** `scripts/recover-pipeline.sh`

### Usage

```bash
# Full recovery (interactive)
bash scripts/recover-pipeline.sh

# Skip Bitcoin Core start (if managing it separately)
SKIP_BITCOIND=1 bash scripts/recover-pipeline.sh

# Dry run (check what would happen)
DRY_RUN=1 bash scripts/recover-pipeline.sh
```

### What it does

| Phase | Steps | Timeout |
|-------|-------|---------|
| 0: SSD Health | Check mount, I/O errors, key directories | Immediate |
| 1: Docker | `docker compose up -d`, wait for healthchecks, repair StarRocks if needed | 5 min |
| 2: Bitcoin Core | Start daemon, wait for RPC | 2 min |
| 3: Kafka Topics | Run `create-kafka-topics.sh` | 30s |
| 4: Pipeline | Kill stale procs, start normalizer + writer | 30s |
| 5: Verify | Docker ps, BE alive, Kafka lag, process check | 30s |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | All services healthy |
| 1 | SSD not mounted |
| 2 | Docker services failed |
| 3 | Bitcoin Core failed to start |
| 4 | Pipeline processes failed |
| 5 | Verification failed |

---

## Monitoring Watchdog

**Location:** `scripts/watchdog.sh` (also copy to internal HDD)

### Setup

```bash
# Copy watchdog to internal disk (survives SSD unmount)
mkdir -p /localhome/rka73/btc-watchdog
cp scripts/watchdog.sh /localhome/rka73/btc-watchdog/

# Option A: Cron (every minute)
crontab -e
# Add: * * * * * /localhome/rka73/btc-watchdog/watchdog.sh

# Option B: Long-running loop
nohup /localhome/rka73/btc-watchdog/watchdog.sh --loop --interval 60 \
    > /localhome/rka73/btc-watchdog/watchdog.log 2>&1 &
```

### What it monitors

| Check | Method | Alert if |
|-------|--------|----------|
| SSD mounted | `mountpoint -q /local-scratch4` | Not mounted |
| SSD I/O errors | `dmesg` last 5 min | I/O error on sdd |
| MySQL | Docker health | Not healthy |
| HMS | Docker health | Not healthy |
| MinIO | Docker health | Not healthy |
| Kafka | Docker health | Not healthy |
| StarRocks FE | `curl localhost:8030/api/health` | Failed |
| StarRocks BE | `curl localhost:8040/api/health` | Failed |
| Bitcoin Core | `bitcoin-cli getblockchaininfo` | Connection refused |
| Normalizer | `pgrep -f main.py` | No process |
| Iceberg Writer | `pgrep -f iceberg_writer.py` | No process |

### Email alerts

- **To:** bdh_dev@sfu.ca
- **Subject:** `[ALERT] Bitcoin Pipeline - <service> DOWN on <hostname>`
- **Cooldown:** 24 hours between repeat alerts for the same failure
- **Requires:** `mailutils` or `sendmail` installed

---

## Known Failure Modes

### StarRocks FE: Exit code 255 / stuck in UNKNOWN state

**Symptom:** FE starts but `curl localhost:8030/api/health` never responds. Warn
log shows "It took too much time for FE to transfer to a stable state".

**Cause:** BDB journal files on SSD corrupted during unclean shutdown.

**Fix:** Wipe FE metadata:
```bash
sudo docker stop starrocks-fe starrocks-be
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-fe-meta/*
sudo docker start starrocks-fe
```

**Side effect:** All StarRocks native tables (flat table) and catalog definitions
are lost. Must re-apply DDL and rebuild flat table.

### StarRocks BE: Exit code 134 / "Unmatched cluster id"

**Symptom:** BE fails to start after FE meta wipe.

**Cause:** BE metadata contains old cluster ID that no longer matches new FE.

**Fix:** Wipe BE storage and re-register:
```bash
sudo rm -rf /local-scratch4/bitcoin_2025/starrocks-data/*
sudo docker start starrocks-be
BE_IP=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' starrocks-be)
mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050'"
```

**Side effect:** Native flat table data is lost. Must rebuild from Iceberg.

### MinIO: "0 drives provided" / 503 errors

**Symptom:** MinIO container is "healthy" but cannot list or read objects.
Log shows `Write quorum could not be established` and `input/output error`.

**Cause:** MinIO's internal state (`.minio.sys/`) was corrupted during SSD crash.

**Fix:** Restart MinIO:
```bash
sudo docker restart minio
sleep 10
docker exec minio mc alias set m http://localhost:9000 minioadmin minioadmin
docker exec minio mc ls m/warehouse/btc.db/
```

**Side effect:** None if Parquet files are intact. If Parquet files were mid-write
during crash, they may be truncated (see Iceberg data corruption below).

### HMS: Exit code 139 / "Error creating transactional connection factory"

**Symptom:** HMS crashes immediately on startup.

**Cause:** Without `IS_RESUME: "true"`, HMS tries to re-initialize the MySQL
schema on every start. Also: MySQL may not be ready when HMS starts.

**Fix:** Ensure `IS_RESUME: "true"` is set in docker-compose.yml (already done).
Restart HMS:
```bash
sudo docker restart hive-metastore
```

### HMS: Port 9083 open but Docker reports "unhealthy"

**Symptom:** HMS is processing connections (visible in `/tmp/hive/hive.log`)
but Docker healthcheck still shows "unhealthy" or "starting".

**Cause:** Hive 3.1.3 JVM startup is very slow (2-5 min). The healthcheck
`timeout 3 cat < /dev/tcp/localhost/9083` may time out for Thrift protocol.
Docker's `start_period: 300s` helps but after restart the counter resets.

**Fix:** This is cosmetic. If you can successfully load Iceberg tables via
PyIceberg (`python scripts/create_iceberg_tables.py`), HMS is working regardless
of what Docker reports.

### MySQL: Exit code 2

**Symptom:** MySQL container fails to start after crash.

**Cause:** InnoDB crash recovery needed from unclean shutdown.

**Fix:** Usually auto-recovers on restart:
```bash
sudo docker restart mysql-hms
```

### Iceberg Parquet corruption (partial writes)

**Symptom:** Iceberg writer fails with "file not found" or "corrupt Parquet".
StarRocks queries fail on specific data files.

**Cause:** The Iceberg writer was mid-flush when the SSD crashed. Parquet files
can be truncated.

**Fix:** Iceberg's metadata tracks which files are committed. Uncommitted partial
files are orphans and should not affect queries. If specific files are corrupt:
1. Check the error message for the file path
2. Remove the orphan file from MinIO
3. The Iceberg writer will re-write the data from Kafka on the next flush

### Bitcoin Core database corruption

**Symptom:** `bitcoind` fails to start or gets stuck at a low block height.

**Cause:** Chainstate or block index corrupted during SSD crash.

**Fix:** Start with reindex:
```bash
bitcoind -datadir=/local-scratch4/bitcoin_2025/bitcoin-core-data -daemon -reindex
```

**Warning:** Reindex takes many hours. Only do this if normal startup fails.

### Normalizer checkpoint staleness

**Symptom:** Normalizer starts but misses blocks between crash and recovery.

**Cause:** Checkpoint was last updated before the crash.

**Fix:** Start normalizer with `--catchup` flag:
```bash
nohup .venv/bin/python live-normalizer/main.py \
    --rpc-url http://127.0.0.1:8332 \
    --rpc-user bitcoinrpc \
    --rpc-password changeme_strong_password_here \
    --kafka-bootstrap localhost:9092 \
    --zmq-url tcp://127.0.0.1:28332 \
    --catchup \
    > logs/normalizer.log 2>&1 &
```

The `--catchup` flag fetches all blocks from the checkpoint height to the current
tip via RPC before switching to live ZMQ mode.

---

## Post-Recovery Verification Checklist

Run through this after every recovery:

- [ ] SSD mounted at `/local-scratch4` (`mount | grep local-scratch4`)
- [ ] No recent I/O errors (`dmesg -T | grep -i "i/o error" | tail -5`)
- [ ] MySQL container healthy (`docker inspect mysql-hms --format='{{.State.Health.Status}}'`)
- [ ] HMS container running (`docker inspect hive-metastore --format='{{.State.Status}}'`)
- [ ] MinIO healthy and serving objects (`docker exec minio mc ls myminio/warehouse/btc.db/`)
- [ ] Kafka healthy (`docker inspect kafka --format='{{.State.Health.Status}}'`)
- [ ] StarRocks FE healthy (`curl -sf http://localhost:8030/api/health`)
- [ ] StarRocks BE alive (`mysql -P 9030 -u root -e "SHOW BACKENDS\G" | grep Alive`)
- [ ] Bitcoin Core synced (`bitcoin-cli -datadir=... getblockchaininfo | grep initialblockdownload`)
- [ ] Normalizer process running (`pgrep -f "live-normalizer/main.py"`)
- [ ] Iceberg Writer process running (`pgrep -f "iceberg_writer.py"`)
- [ ] Kafka consumer lag decreasing (`docker exec kafka ... --describe --all-groups`)
- [ ] Iceberg tables queryable (`mysql -P 9030 -u root -e "SET CATALOG iceberg_catalog; SELECT MAX(height) FROM btc.blocks;"`)
- [ ] Normalizer log shows new blocks (`tail -5 logs/normalizer.log`)
- [ ] Writer log shows flushes (`tail -5 logs/iceberg_writer.log`)

---

## Running Recovery via AI

If you want an AI assistant (e.g., Claude in Cursor) to perform the recovery:

1. Open this file and the plan at `nifty-sauteeing-spring-evaluation-report-v3.md`
2. Tell the AI: "The external SSD at /local-scratch4 crashed and has been remounted. Follow the SSD Crash Recovery Runbook at docs/ssd-crash-recovery.md to restart all pipeline services."
3. The AI should:
   - Run `scripts/recover-pipeline.sh` OR follow the manual steps
   - Work through the verification checklist
   - Report which services recovered and which need attention

Alternatively, just run the recovery script yourself:
```bash
bash /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/recover-pipeline.sh
```

---

## Recovery Session Log: 2026-03-31

This section documents the actual recovery performed after the SSD crash on
2026-03-31. It serves as a reference for what to expect during future recoveries.

### Pre-recovery state

| Component | Status | Exit Code | Notes |
|-----------|--------|-----------|-------|
| SSD | Mounted (remounted manually) | N/A | `/dev/sdd2` at `/local-scratch4`, 54% used |
| MySQL | Exited | 2 | InnoDB unclean shutdown |
| HMS | Exited | 139 | JVM killed by crash |
| MinIO | Running | N/A | Container up but **internally broken** |
| Kafka | Running | N/A | Survived crash, auto-recovered |
| StarRocks FE | Exited | 255 | BDB journal corrupted |
| StarRocks BE | Exited | 134 | Cluster ID mismatch after FE issue |
| Bitcoin Core | Not running | N/A | Process killed by crash |
| Normalizer | Not running | N/A | Was in live mode at block 943,039 |
| Iceberg Writer | Not running | N/A | Was mid-flush of `btc.tx_out` at 01:57 UTC |

### Recovery timeline

```
22:05  docker compose up -d — mysql-hms, hive-metastore, starrocks-fe started
22:06  StarRocks FE stuck in UNKNOWN state (BDB journal corrupt)
22:07  FE metadata wiped: rm -rf starrocks-fe-meta/*
22:07  FE restarted — healthy in 10s
22:08  BE storage wiped: rm -rf starrocks-data/*
22:08  BE restarted — healthy immediately
22:08  BE registered with FE: ALTER SYSTEM ADD BACKEND
22:09  Iceberg catalog DDL + flat table DDL applied
22:10  Bitcoin Core started: bitcoind -daemon
22:11  Bitcoin Core RPC ready at block 942,989 (syncing to 943,037)
22:11  Bitcoin Core fully synced (initialblockdownload: false) at 943,123
22:11  Kafka topics verified (all 5 present from before crash)
22:12  Normalizer started with --catchup flag
22:12  Normalizer caught up blocks 943,040 → 943,162 in ~40s
22:13  Normalizer entered live ZMQ mode
22:15  Iceberg writer first attempt FAILED — MinIO returning 503
22:15  Discovered MinIO "0 drives" issue from SSD crash
22:16  MinIO restarted: sudo docker restart minio
22:16  MinIO recovered — all 4 Iceberg table directories visible
22:16  HMS restarted (depends on MinIO for S3A operations)
22:33  HMS JVM still in "starting" — but actually functional (Thrift active)
22:44  Iceberg writer started successfully — draining Kafka backlog
22:45  Blocks table fully consumed (lag 0)
22:45  Transactions, tx_in, tx_out still draining (~6M total lag)
22:58  Normalizer receiving live blocks via ZMQ (block 943,167)
23:12  Watchdog confirms all 11 checks passing
23:16  Iceberg writer still draining tx backlog
```

### Issues discovered during recovery

1. **MinIO "0 drives" post-crash (CRITICAL):** MinIO's internal state at
   `.minio.sys/buckets/.healing.bin` had I/O errors from the crash. MinIO
   reported healthy to Docker but could not serve any S3 requests (HTTP 503).
   Fix: `sudo docker restart minio`. This is now handled in `recover-pipeline.sh`
   Phase 1 with a MinIO functional test.

2. **HMS Docker "unhealthy" but functional:** HMS JVM takes 2-5 min to start.
   After restart, Docker reports "unhealthy"/"starting" for the full duration.
   However, HMS is accepting Thrift connections on port 9083 and serving metadata
   fine. The watchdog handles this by accepting "running" state for HMS even
   when Docker health says "starting".

3. **Normalizer `--catchup` is critical:** Without `--catchup`, the normalizer
   loads the checkpoint and goes directly to live ZMQ mode, skipping all blocks
   between the checkpoint (943,039) and the current tip (~943,162). Always use
   `--catchup` after a crash. The recovery script does this automatically.

4. **Iceberg writer log file truncation:** Using `>` truncates the existing log
   (184K lines lost). Use `>>` to append instead. The recovery script uses `>>`.

5. **StarRocks FE always needs meta wipe after SSD crash:** Every SSD crash
   corrupts the BDB journal. This is expected and handled automatically.
   Side effect: loses the flat table and catalog DDL (reapplied by script).

### Data integrity after recovery

| Layer | Status | Notes |
|-------|--------|-------|
| Kafka | Clean | Survived crash, retained all messages |
| Iceberg metadata (HMS) | Clean | MySQL-backed, recovered cleanly |
| Iceberg Parquet files (MinIO) | Clean after restart | MinIO needed restart; orphan files from mid-flush may exist but do not affect queries |
| StarRocks native flat table | **Lost** | Wiped with BE storage; must be rebuilt via `flat_table_builder.py --incremental` |
| Normalizer checkpoint | Valid | Block 943,039, used for catchup |
| Bitcoin Core chainstate | Clean | Synced normally without reindex |

---

## Future Improvements

### Immediate priorities

- [ ] **Start watchdog on internal HDD:** Run from `/localhome/rka73/btc-watchdog/` so it survives SSD crashes
- [ ] **Install mailutils:** `sudo apt install mailutils` to enable email alerts from watchdog
- [ ] **Rebuild flat table:** Run `python starrocks/flat_table_builder.py --incremental` after writer drains backlog

### Short-term improvements

- [ ] **Auto-remount:** udev rule or systemd mount unit to automatically remount the SSD on reconnect
- [ ] **SMART monitoring:** `smartctl` checks for SSD health degradation before crashes happen
- [ ] **systemd services:** Convert pipeline processes from `nohup` to systemd units for auto-restart
- [ ] **Backup checkpoint:** Cron job to copy `checkpoint.json` to internal HDD every 10 minutes

### Longer-term improvements

- [ ] **Auto-recovery:** Have watchdog automatically run `recover-pipeline.sh` instead of just alerting
- [ ] **Disk health dashboard:** Grafana panel showing SSD I/O errors and SMART attributes
- [ ] **Iceberg maintenance:** Scheduled Spark job for snapshot expiry, compaction, orphan cleanup
- [ ] **Phase 2 backfill:** Historical CSV-to-Iceberg bulk load (separate from crash recovery)
- [ ] **Phase 3 hardening:** Full monitoring, alerting, and dashboards
