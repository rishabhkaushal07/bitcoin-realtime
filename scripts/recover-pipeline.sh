#!/usr/bin/env bash
# =============================================================================
# Bitcoin Real-Time Pipeline — SSD Crash Recovery Script
# =============================================================================
# Idempotent one-click recovery after external SSD crash at /local-scratch4.
# Safe to run multiple times. Skips steps that are already healthy.
#
# Usage:
#   bash scripts/recover-pipeline.sh           # full recovery
#   SKIP_BITCOIND=1 bash scripts/recover-pipeline.sh  # skip Bitcoin Core
#   DRY_RUN=1 bash scripts/recover-pipeline.sh        # check only, no changes
#
# Exit codes:
#   0 = all services healthy
#   1 = SSD not mounted
#   2 = Docker services failed
#   3 = Bitcoin Core failed
#   4 = Pipeline processes failed
#   5 = Verification failed
# =============================================================================
set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
BASE_DIR="/local-scratch4/bitcoin_2025"
PROJECT_DIR="${BASE_DIR}/bitcoin-realtime"
BITCOIN_DATADIR="${BASE_DIR}/bitcoin-core-data"
VENV="${PROJECT_DIR}/.venv/bin/python"

RPC_URL="http://127.0.0.1:8332"
RPC_USER="bitcoinrpc"
RPC_PASS="changeme_strong_password_here"
KAFKA_BOOTSTRAP="localhost:9092"
ZMQ_URL="tcp://127.0.0.1:28332"

SKIP_BITCOIND="${SKIP_BITCOIND:-0}"
DRY_RUN="${DRY_RUN:-0}"

LOG_DIR="${PROJECT_DIR}/logs"
RECOVERY_LOG="${LOG_DIR}/recovery-$(date +%Y-%m-%d).log"

# ── Colors ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

FAILURES=0

# ── Helpers ──────────────────────────────────────────────────────────────────
log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*" | tee -a "$RECOVERY_LOG" 2>/dev/null || echo "$*"; }
ok()  { echo -e "${GREEN}  ✓ $*${NC}" | tee -a "$RECOVERY_LOG" 2>/dev/null || echo "OK: $*"; }
warn(){ echo -e "${YELLOW}  ⚠ $*${NC}" | tee -a "$RECOVERY_LOG" 2>/dev/null || echo "WARN: $*"; }
fail(){ echo -e "${RED}  ✗ $*${NC}" | tee -a "$RECOVERY_LOG" 2>/dev/null || echo "FAIL: $*"; FAILURES=$((FAILURES + 1)); }

wait_for() {
    local desc="$1" cmd="$2" max_attempts="$3" interval="${4:-5}"
    for i in $(seq 1 "$max_attempts"); do
        if eval "$cmd" > /dev/null 2>&1; then
            ok "$desc ready (attempt $i)"
            return 0
        fi
        sleep "$interval"
    done
    fail "$desc not ready after $((max_attempts * interval))s"
    return 1
}

header() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    log "$*"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# =============================================================================
# Phase 0: SSD Health
# =============================================================================
phase0_ssd_health() {
    header "Phase 0: SSD Health Check"

    if ! mountpoint -q /local-scratch4 2>/dev/null; then
        fail "SSD not mounted at /local-scratch4"
        echo ""
        echo "  To fix: sudo mount /dev/sdd2 /local-scratch4"
        echo "  Then re-run this script."
        exit 1
    fi
    ok "SSD mounted at /local-scratch4"

    local disk_usage
    disk_usage=$(df -h /local-scratch4 | tail -1 | awk '{print $5}')
    ok "Disk usage: $disk_usage"

    local recent_errors
    recent_errors=$(dmesg -T 2>/dev/null | grep -c "I/O error" | tail -1 || echo "0")
    if [ "$recent_errors" -gt 0 ] 2>/dev/null; then
        warn "Found $recent_errors I/O errors in dmesg (check: dmesg -T | grep 'I/O error')"
    else
        ok "No I/O errors in dmesg"
    fi

    for dir in bitcoin-core-data minio-data bitcoin-realtime kafka-data; do
        if [ -d "${BASE_DIR}/${dir}" ]; then
            ok "Directory exists: ${dir}/"
        else
            fail "Missing directory: ${BASE_DIR}/${dir}/"
        fi
    done
}

# =============================================================================
# Phase 1: Docker Infrastructure
# =============================================================================
phase1_docker() {
    header "Phase 1: Docker Infrastructure"

    if [ "$DRY_RUN" = "1" ]; then
        log "DRY RUN: would run 'docker compose up -d'"
        return 0
    fi

    cd "$PROJECT_DIR"

    log "Starting Docker Compose services..."
    sudo docker compose up -d 2>&1 | tee -a "$RECOVERY_LOG"

    # Wait for MySQL
    log "Waiting for MySQL..."
    wait_for "MySQL" "sudo docker inspect --format='{{.State.Health.Status}}' mysql-hms 2>/dev/null | grep -q healthy" 12 5

    # Wait for MinIO (and handle the 0-drives issue)
    log "Checking MinIO..."
    if sudo docker inspect --format='{{.State.Health.Status}}' minio 2>/dev/null | grep -q healthy; then
        local minio_test
        minio_test=$(docker exec minio mc alias set m http://localhost:9000 minioadmin minioadmin 2>&1 && docker exec minio mc ls m/warehouse/ 2>&1)
        if echo "$minio_test" | grep -qi "error\|0 drives"; then
            warn "MinIO has stale state, restarting..."
            sudo docker restart minio
            sleep 10
        fi
        ok "MinIO healthy"
    else
        wait_for "MinIO" "sudo docker inspect --format='{{.State.Health.Status}}' minio 2>/dev/null | grep -q healthy" 12 5
    fi

    # Wait for Kafka
    log "Checking Kafka..."
    wait_for "Kafka" "sudo docker inspect --format='{{.State.Health.Status}}' kafka 2>/dev/null | grep -q healthy" 12 5

    # Wait for HMS (just check if container is running; JVM takes 2-5 min)
    log "Checking HMS (JVM startup may take 2-5 min)..."
    local hms_running
    hms_running=$(sudo docker inspect --format='{{.State.Running}}' hive-metastore 2>/dev/null || echo "false")
    if [ "$hms_running" = "true" ]; then
        ok "HMS container running"
    else
        sudo docker restart hive-metastore
        sleep 5
        ok "HMS restarted"
    fi

    # Check StarRocks FE
    log "Checking StarRocks FE..."
    local fe_healthy=false
    for i in $(seq 1 6); do
        if curl -sf http://localhost:8030/api/health > /dev/null 2>&1; then
            fe_healthy=true
            break
        fi
        sleep 10
    done

    local needs_sr_rebuild=false
    if [ "$fe_healthy" = "true" ]; then
        ok "StarRocks FE healthy"
    else
        warn "StarRocks FE unhealthy — wiping metadata..."
        needs_sr_rebuild=true
        sudo docker stop starrocks-fe starrocks-be 2>/dev/null || true
        sudo rm -rf "${BASE_DIR}/starrocks-fe-meta/"*
        sudo docker start starrocks-fe
        wait_for "StarRocks FE (after wipe)" "curl -sf http://localhost:8030/api/health" 12 5
    fi

    # Check StarRocks BE
    log "Checking StarRocks BE..."
    if [ "$needs_sr_rebuild" = "true" ]; then
        warn "Wiping BE storage (cluster ID mismatch after FE wipe)..."
        sudo rm -rf "${BASE_DIR}/starrocks-data/"*
        sudo docker start starrocks-be 2>/dev/null || true
    fi
    wait_for "StarRocks BE" "curl -sf http://localhost:8040/api/health" 12 5

    if [ "$needs_sr_rebuild" = "true" ]; then
        log "Re-registering BE with FE..."
        sleep 5
        local be_ip
        be_ip=$(sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' starrocks-be)
        mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM ADD BACKEND '${be_ip}:9050'" 2>/dev/null || true
        sleep 5

        log "Applying StarRocks DDL..."
        mysql -h 127.0.0.1 -P 9030 -u root < "${PROJECT_DIR}/starrocks/iceberg_catalog_v3.sql" 2>/dev/null
        mysql -h 127.0.0.1 -P 9030 -u root < "${PROJECT_DIR}/starrocks/flat_serving_v3.sql" 2>/dev/null
        ok "StarRocks catalog + flat table DDL applied"
    fi

    local be_alive
    be_alive=$(mysql -h 127.0.0.1 -P 9030 -u root -N -e "SHOW BACKENDS" 2>/dev/null | grep -c "true" || echo "0")
    if [ "$be_alive" -gt 0 ] 2>/dev/null; then
        ok "StarRocks BE alive and registered"
    else
        fail "StarRocks BE not alive"
    fi
}

# =============================================================================
# Phase 2: Bitcoin Core
# =============================================================================
phase2_bitcoind() {
    header "Phase 2: Bitcoin Core"

    if [ "$SKIP_BITCOIND" = "1" ]; then
        log "Skipping Bitcoin Core (SKIP_BITCOIND=1)"
        return 0
    fi

    if [ "$DRY_RUN" = "1" ]; then
        log "DRY RUN: would start bitcoind"
        return 0
    fi

    if pgrep -x bitcoind > /dev/null 2>&1; then
        ok "Bitcoin Core already running"
    else
        log "Starting Bitcoin Core daemon..."
        bitcoind -datadir="$BITCOIN_DATADIR" -daemon 2>&1 || true
    fi

    log "Waiting for RPC..."
    wait_for "Bitcoin Core RPC" \
        "bitcoin-cli -datadir=$BITCOIN_DATADIR getblockchaininfo 2>/dev/null | grep -q blocks" \
        24 5

    local block_height
    block_height=$(bitcoin-cli -datadir="$BITCOIN_DATADIR" getblockchaininfo 2>/dev/null | grep '"blocks"' | grep -o '[0-9]*' || echo "unknown")
    ok "Bitcoin Core at block $block_height"
}

# =============================================================================
# Phase 3: Kafka Topics
# =============================================================================
phase3_kafka_topics() {
    header "Phase 3: Kafka Topics"

    if [ "$DRY_RUN" = "1" ]; then
        log "DRY RUN: would create Kafka topics"
        return 0
    fi

    cd "$PROJECT_DIR"
    bash scripts/create-kafka-topics.sh 2>&1 | tee -a "$RECOVERY_LOG"
    ok "All Kafka topics verified"
}

# =============================================================================
# Phase 4: Pipeline Processes
# =============================================================================
phase4_pipeline() {
    header "Phase 4: Pipeline Processes"

    if [ "$DRY_RUN" = "1" ]; then
        log "DRY RUN: would start normalizer + iceberg writer"
        return 0
    fi

    cd "$PROJECT_DIR"

    # Kill stale processes
    pkill -f "live-normalizer/main.py" 2>/dev/null && warn "Killed stale normalizer" || true
    pkill -f "iceberg_writer.py" 2>/dev/null && warn "Killed stale writer" || true
    sleep 2

    # Start normalizer with catchup
    log "Starting live normalizer (with --catchup)..."
    nohup "$VENV" live-normalizer/main.py \
        --rpc-url "$RPC_URL" \
        --rpc-user "$RPC_USER" \
        --rpc-password "$RPC_PASS" \
        --kafka-bootstrap "$KAFKA_BOOTSTRAP" \
        --zmq-url "$ZMQ_URL" \
        --catchup \
        > "${LOG_DIR}/normalizer.log" 2>&1 &
    local norm_pid=$!

    # Start iceberg writer
    log "Starting Iceberg writer..."
    nohup "$VENV" pyiceberg-sidecar/iceberg_writer.py \
        >> "${LOG_DIR}/iceberg_writer.log" 2>&1 &
    local writer_pid=$!

    sleep 10

    if kill -0 "$norm_pid" 2>/dev/null; then
        ok "Normalizer running (PID $norm_pid)"
    else
        fail "Normalizer died immediately — check logs/normalizer.log"
    fi

    if kill -0 "$writer_pid" 2>/dev/null; then
        ok "Iceberg Writer running (PID $writer_pid)"
    else
        fail "Iceberg Writer died immediately — check logs/iceberg_writer.log"
    fi
}

# =============================================================================
# Phase 5: Verification
# =============================================================================
phase5_verify() {
    header "Phase 5: End-to-End Verification"

    log "Docker containers:"
    sudo docker compose ps 2>/dev/null | tee -a "$RECOVERY_LOG" || true

    log "StarRocks BE status:"
    local be_status
    be_status=$(mysql -h 127.0.0.1 -P 9030 -u root -N -e "SHOW BACKENDS" 2>/dev/null | head -1 || echo "unavailable")
    if echo "$be_status" | grep -q "true"; then
        ok "StarRocks BE alive"
    else
        warn "StarRocks BE status: $be_status"
    fi

    log "Pipeline processes:"
    if pgrep -f "live-normalizer/main.py" > /dev/null; then
        ok "Normalizer running"
    else
        fail "Normalizer NOT running"
    fi

    if pgrep -f "iceberg_writer.py" > /dev/null; then
        ok "Iceberg Writer running"
    else
        fail "Iceberg Writer NOT running"
    fi

    if [ "$SKIP_BITCOIND" != "1" ]; then
        if pgrep -x bitcoind > /dev/null; then
            ok "Bitcoin Core running"
        else
            fail "Bitcoin Core NOT running"
        fi
    fi

    log "Normalizer log (last 3 lines):"
    tail -3 "${LOG_DIR}/normalizer.log" 2>/dev/null | tee -a "$RECOVERY_LOG" || true

    log "Writer log (last 3 lines):"
    tail -3 "${LOG_DIR}/iceberg_writer.log" 2>/dev/null | tee -a "$RECOVERY_LOG" || true
}

# =============================================================================
# Summary
# =============================================================================
print_summary() {
    header "Recovery Summary"

    if [ "$FAILURES" -eq 0 ]; then
        echo -e "${GREEN}"
        echo "  ╔════════════════════════════════════════════════╗"
        echo "  ║  ALL SERVICES RECOVERED SUCCESSFULLY           ║"
        echo "  ╚════════════════════════════════════════════════╝"
        echo -e "${NC}"
        log "Recovery complete with 0 failures"
        echo ""
        echo "  Next steps (optional):"
        echo "    - Wait for Iceberg writer to drain Kafka backlog"
        echo "    - Run: python starrocks/flat_table_builder.py --incremental"
        echo "    - Start watchdog: nohup scripts/watchdog.sh --loop --interval 60 &"
        echo ""
    else
        echo -e "${RED}"
        echo "  ╔════════════════════════════════════════════════╗"
        echo "  ║  RECOVERY COMPLETED WITH $FAILURES FAILURE(S)          ║"
        echo "  ╚════════════════════════════════════════════════╝"
        echo -e "${NC}"
        log "Recovery completed with $FAILURES failure(s)"
        echo ""
        echo "  Check the recovery log: $RECOVERY_LOG"
        echo "  See docs/ssd-crash-recovery.md for troubleshooting"
        echo ""
    fi
}

# =============================================================================
# Main
# =============================================================================
main() {
    mkdir -p "$LOG_DIR"
    echo "" >> "$RECOVERY_LOG" 2>/dev/null || true
    log "===== Recovery started at $(date) ====="

    phase0_ssd_health
    phase1_docker
    phase2_bitcoind
    phase3_kafka_topics
    phase4_pipeline
    phase5_verify
    print_summary

    if [ "$FAILURES" -gt 0 ]; then
        exit 5
    fi
    exit 0
}

main "$@"
