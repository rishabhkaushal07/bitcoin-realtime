# Watchdog Monitor — Comprehensive Documentation

**Script:** `scripts/watchdog.sh`
**Internal HDD copy:** `/localhome/rka73/btc-watchdog/watchdog.sh`
**Alert email:** bdh_dev@sfu.ca
**Created:** 2026-03-31

---

## What It Does

The watchdog is a lightweight bash script that continuously monitors the health
of the Bitcoin real-time pipeline and the external SSD it runs on. When any
service fails, it sends an email alert and logs the failure. It does **not**
automatically fix anything — its job is detection and notification only.

The companion script `scripts/recover-pipeline.sh` performs the actual recovery.

### At a glance

```
  watchdog.sh                        recover-pipeline.sh
  ~~~~~~~~~~~~                       ~~~~~~~~~~~~~~~~~~~~
  Monitors 11 health checks          Performs 5-phase recovery
  Runs every 60 seconds              Runs once, manually
  Sends email on failure             Fixes all services
  Lives on INTERNAL HDD              Lives on EXTERNAL SSD
  Does NOT modify anything           Modifies Docker, processes, data
```

---

## Architecture

```
 +--------------------------------------------------+
 |  INTERNAL HDD (/localhome/rka73/btc-watchdog/)   |
 |                                                    |
 |  watchdog.sh  ──── runs every 60 seconds ────┐   |
 |                                               |   |
 +-----------------------------------------------+---+
                                                 |
         checks 11 health points                 |
                |                                |
                v                                v
 +--------------------------------------------------+
 |  EXTERNAL SSD (/local-scratch4/)                  |
 |                                                    |
 |  Docker containers:                                |
 |    mysql-hms, hive-metastore, minio, kafka,       |
 |    starrocks-fe, starrocks-be                     |
 |                                                    |
 |  Host processes:                                   |
 |    bitcoind, live-normalizer, iceberg-writer       |
 |                                                    |
 +--------------------------------------------------+
                |
                v  (on failure)
 +--------------------------------------------------+
 |  EMAIL ALERT to bdh_dev@sfu.ca                    |
 |                                                    |
 |  Subject: [ALERT] Bitcoin Pipeline - <svc> DOWN   |
 |  Body: which service, timestamp, suggested fix     |
 |  Cooldown: 15 min per service (no spam)            |
 +--------------------------------------------------+
```

---

## Health Checks (11 total)

The watchdog performs 11 health checks every cycle. If the SSD is not mounted,
all other checks are skipped (the SSD failure is the root cause).

### Check 1: SSD Mount

| Property | Value |
|----------|-------|
| **Name** | `ssd_mount` |
| **Command** | `mountpoint -q /local-scratch4` |
| **Passes when** | `/local-scratch4` is a mounted filesystem |
| **Fails when** | SSD disconnected, unmounted, or mount point doesn't exist |
| **Priority** | CRITICAL — if this fails, all other checks are skipped |
| **Why it matters** | All data (Docker volumes, Bitcoin Core, Iceberg files) lives on this SSD |

### Check 2: SSD I/O Errors

| Property | Value |
|----------|-------|
| **Name** | `ssd_io` |
| **Command** | `dmesg -T \| grep -c "I/O error.*sd[d-z]"` |
| **Passes when** | No I/O errors found in kernel log |
| **Fails when** | One or more I/O errors on sd[d-z] devices detected |
| **Priority** | HIGH — I/O errors precede full SSD crash |
| **Why it matters** | Early warning that the SSD is about to fail; allows preemptive action |

### Check 3: MySQL (HMS backend)

| Property | Value |
|----------|-------|
| **Name** | `docker_mysql-hms` |
| **Command** | `docker inspect --format='{{.State.Health.Status}}' mysql-hms` |
| **Passes when** | Status is `healthy` |
| **Fails when** | Status is `unhealthy`, `starting`, or container is missing |
| **Why it matters** | HMS stores Iceberg catalog metadata in MySQL; without it, no table lookups work |

### Check 4: Hive Metastore

| Property | Value |
|----------|-------|
| **Name** | `docker_hive-metastore` |
| **Command** | `docker inspect --format='{{.State.Health.Status}}' hive-metastore` |
| **Passes when** | Status is `healthy` OR container is `running` (special case) |
| **Fails when** | Container is stopped, exited, or missing |
| **Special handling** | HMS JVM takes 2-5 minutes to start. Docker often reports `starting` or `unhealthy` while HMS is actually serving Thrift connections on port 9083. The watchdog accepts `running` as healthy for this container specifically. |
| **Why it matters** | All Iceberg operations (reads and writes) require HMS for table metadata |

### Check 5: MinIO

| Property | Value |
|----------|-------|
| **Name** | `docker_minio` |
| **Command** | `docker inspect --format='{{.State.Health.Status}}' minio` |
| **Passes when** | Status is `healthy` |
| **Fails when** | Status is `unhealthy` or container is missing |
| **Known issue** | After SSD crash, MinIO may report `healthy` to Docker but return HTTP 503 for all S3 operations ("0 drives provided"). The recovery script has a deeper functional test for this. |
| **Why it matters** | All Iceberg Parquet files are stored in MinIO; both reads and writes fail without it |

### Check 6: Kafka

| Property | Value |
|----------|-------|
| **Name** | `docker_kafka` |
| **Command** | `docker inspect --format='{{.State.Health.Status}}' kafka` |
| **Passes when** | Status is `healthy` |
| **Fails when** | Status is `unhealthy` or container is missing |
| **Why it matters** | The normalizer writes to Kafka; the Iceberg writer reads from Kafka; without it, the real-time pipeline cannot move data |

### Check 7: StarRocks FE

| Property | Value |
|----------|-------|
| **Name** | `starrocks_fe` |
| **Command** | `curl -sf http://localhost:8030/api/health` |
| **Passes when** | HTTP 200 response |
| **Fails when** | No response, connection refused, or non-200 status |
| **Why it matters** | FE handles all SQL queries, catalog operations, and flat table management |

### Check 8: StarRocks BE

| Property | Value |
|----------|-------|
| **Name** | `starrocks_be` |
| **Command** | `curl -sf http://localhost:8040/api/health` |
| **Passes when** | HTTP 200 response |
| **Fails when** | No response or non-200 status |
| **Why it matters** | BE stores native flat table data and executes queries; without it, all queries fail |

### Check 9: Bitcoin Core

| Property | Value |
|----------|-------|
| **Name** | `bitcoind` |
| **Command** | `bitcoin-cli -datadir=<path> getblockchaininfo` |
| **Passes when** | RPC responds with blockchain info |
| **Fails when** | Connection refused (process not running) |
| **Why it matters** | The normalizer needs Bitcoin Core for ZMQ notifications (new blocks) and RPC (block data) |

### Check 10: Live Normalizer

| Property | Value |
|----------|-------|
| **Name** | `normalizer` |
| **Command** | `pgrep -f "live-normalizer/main.py"` |
| **Passes when** | At least one matching process exists |
| **Fails when** | No matching process found |
| **Why it matters** | Without the normalizer, new Bitcoin blocks are not ingested into Kafka |

### Check 11: Iceberg Writer

| Property | Value |
|----------|-------|
| **Name** | `iceberg_writer` |
| **Command** | `pgrep -f "iceberg_writer.py"` |
| **Passes when** | At least one matching process exists |
| **Fails when** | No matching process found |
| **Why it matters** | Without the writer, Kafka records are not flushed to Iceberg Parquet files; data accumulates in Kafka until retention expires |

---

## Email Alerting

### How it works

When a check fails, the watchdog sends an email to `bdh_dev@sfu.ca`:

```
Subject: [ALERT] Bitcoin Pipeline - starrocks_fe DOWN on bdh-server
Body:
  Service: starrocks_fe
  Status: DOWN
  Time: 2026-03-31T22:06:15
  Host: bdh-server

  StarRocks FE health endpoint not responding. May need FE meta wipe.

  Suggested action:
    bash /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/recover-pipeline.sh

  See: /local-scratch4/bitcoin_2025/bitcoin-realtime/docs/ssd-crash-recovery.md
```

### Anti-spam cooldown

To avoid email flooding when a service stays down, the watchdog enforces a
**24-hour cooldown** per service. State is tracked in `/tmp/btc-watchdog/`:

```
/tmp/btc-watchdog/
  starrocks_fe.last_alert      # Unix timestamp of last alert
  ssd_mount.last_alert         # etc.
```

If a service fails and was already alerted within the last 24 hours, no
new email is sent. When a service recovers (check passes), its cooldown state
is cleared, so the next failure triggers an immediate alert.

### Email delivery requirements

The script tries `mail` (from `mailutils`) first, then `sendmail` as a fallback.

```bash
# Install mailutils (Ubuntu/Debian)
sudo apt install mailutils

# Test email delivery
echo "Test from watchdog" | mail -s "Watchdog test" bdh_dev@sfu.ca
```

If neither is available, the alert is logged to stderr instead.

---

## Usage

### Single check (cron mode)

```bash
bash /localhome/rka73/btc-watchdog/watchdog.sh
```

Runs all 11 checks once, prints a status line, and exits.

Output format:
```
2026-03-31T23:16:10 OK ssd_mount=ok ssd_io=ok docker_mysql-hms=ok docker_hive-metastore=ok docker_minio=ok docker_kafka=ok starrocks_fe=ok starrocks_be=ok bitcoind=ok normalizer=ok iceberg_writer=ok
```

Or on failure:
```
2026-03-31T23:16:10 FAIL(2) ssd_mount=ok ssd_io=ok docker_mysql-hms=ok docker_hive-metastore=ok docker_minio=ok docker_kafka=ok starrocks_fe=FAIL starrocks_be=FAIL bitcoind=ok normalizer=ok iceberg_writer=ok
```

### Loop mode (recommended)

```bash
nohup /localhome/rka73/btc-watchdog/watchdog.sh --loop --interval 60 \
    > /localhome/rka73/btc-watchdog/watchdog.log 2>&1 &
```

Runs checks every 60 seconds in a continuous loop. Recommended over cron because:

1. Cron can miss checks if the system is under I/O stress during SSD failures
2. Loop mode provides more reliable SSD mount detection
3. No crontab management needed

### Custom interval

```bash
# Check every 30 seconds (more aggressive)
bash watchdog.sh --loop --interval 30

# Check every 5 minutes (lighter load)
bash watchdog.sh --loop --interval 300
```

### Cron setup (alternative)

```bash
crontab -e
# Add this line:
* * * * * /localhome/rka73/btc-watchdog/watchdog.sh >> /localhome/rka73/btc-watchdog/watchdog.log 2>&1
```

---

## Installation

### First time setup

```bash
# 1. Copy watchdog to internal HDD (CRITICAL — must survive SSD crash)
mkdir -p /localhome/rka73/btc-watchdog
cp /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/watchdog.sh \
   /localhome/rka73/btc-watchdog/watchdog.sh
chmod +x /localhome/rka73/btc-watchdog/watchdog.sh

# 2. Install email support
sudo apt install mailutils  # if not already installed

# 3. Test single run
/localhome/rka73/btc-watchdog/watchdog.sh

# 4. Start in loop mode
nohup /localhome/rka73/btc-watchdog/watchdog.sh --loop --interval 60 \
    > /localhome/rka73/btc-watchdog/watchdog.log 2>&1 &

# 5. Verify it's running
pgrep -a -f watchdog.sh
tail -3 /localhome/rka73/btc-watchdog/watchdog.log
```

### After updating the script

When the script is updated on the SSD, copy the new version to the internal HDD:

```bash
cp /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/watchdog.sh \
   /localhome/rka73/btc-watchdog/watchdog.sh
# Restart the running loop
pkill -f "watchdog.sh --loop"
nohup /localhome/rka73/btc-watchdog/watchdog.sh --loop --interval 60 \
    > /localhome/rka73/btc-watchdog/watchdog.log 2>&1 &
```

---

## Why the Watchdog Lives on the Internal HDD

The entire Bitcoin pipeline lives on `/local-scratch4` (the external SSD). When
the SSD crashes:

- All processes reading/writing to the SSD get I/O errors and die
- Docker containers with bind-mounted volumes become unresponsive
- Scripts stored on the SSD cannot be executed

If the watchdog script itself were on the SSD, it would also crash and could
never detect the SSD failure. By placing it on the internal HDD at
`/localhome/rka73/btc-watchdog/`, the watchdog can:

1. Detect that `/local-scratch4` is no longer mounted
2. Send an email alert immediately
3. Keep running and re-check every 60 seconds
4. Report when the SSD is remounted (recovery can begin)

```
 SSD crashes
     |
     v
 All SSD processes die
     |
     v
 Watchdog (on internal HDD) detects mount failure
     |
     v
 Email: "[ALERT] Bitcoin Pipeline - ssd_mount DOWN"
     |
     v
 You receive email, SSH in, remount SSD
     |
     v
 Run: bash /local-scratch4/.../scripts/recover-pipeline.sh
     |
     v
 Watchdog detects all services healthy
     |
     v
 Email cooldown clears, status shows "OK"
```

---

## Log Format

Each check cycle produces one line:

```
<timestamp> <result> <check1>=<status> <check2>=<status> ...
```

Examples:

```
2026-03-31T23:12:28 OK ssd_mount=ok ssd_io=ok docker_mysql-hms=ok ...
2026-03-31T23:13:28 FAIL(1) ssd_mount=ok ssd_io=ok ... normalizer=FAIL
2026-03-31T23:14:28 CRITICAL ssd_mount=FAIL (all other checks skipped)
```

The log file can be analyzed for uptime reports:

```bash
# Count failures in last 24 hours
grep "FAIL" /localhome/rka73/btc-watchdog/watchdog.log | wc -l

# Find all SSD mount failures
grep "ssd_mount=FAIL" /localhome/rka73/btc-watchdog/watchdog.log

# Last 10 status lines
tail -10 /localhome/rka73/btc-watchdog/watchdog.log
```

---

## Configuration Reference

All configuration is at the top of `watchdog.sh`:

| Variable | Default | Description |
|----------|---------|-------------|
| `ALERT_EMAIL` | `bdh_dev@sfu.ca` | Email address for failure alerts |
| `ALERT_COOLDOWN_SEC` | `86400` (24 hrs) | Minimum seconds between repeat alerts for same service |
| `ALERT_STATE_DIR` | `/tmp/btc-watchdog` | Directory for cooldown state files |
| `BITCOIN_DATADIR` | `/local-scratch4/bitcoin_2025/bitcoin-core-data` | Bitcoin Core data directory |
| `PROJECT_DIR` | `/local-scratch4/bitcoin_2025/bitcoin-realtime` | Pipeline project root |
| `LOOP_MODE` | `false` | Set via `--loop` flag |
| `INTERVAL` | `60` | Seconds between checks in loop mode |

---

## Relationship to Other Scripts

```
 watchdog.sh (monitoring)
     |
     | detects failure
     | sends email alert
     |
     v
 YOU (human operator)
     |
     | SSH in, remount SSD if needed
     |
     v
 recover-pipeline.sh (recovery)
     |
     | Phase 0: verify SSD
     | Phase 1: Docker infrastructure
     | Phase 2: Bitcoin Core
     | Phase 3: Kafka topics
     | Phase 4: pipeline processes
     | Phase 5: verification
     |
     v
 Pipeline healthy
     |
     v
 watchdog.sh confirms all OK
```

The watchdog is read-only — it never modifies containers, processes, or files.
The recovery script is write-heavy — it starts/stops containers, wipes metadata,
and starts processes. They are intentionally separated so the watchdog can never
cause accidental damage.
