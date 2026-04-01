#!/usr/bin/env bash
# =============================================================================
# Bitcoin Real-Time Pipeline — Watchdog Monitor
# =============================================================================
# Monitors SSD health and all pipeline services. Sends email alert on failure.
#
# IMPORTANT: Copy this script to the INTERNAL HDD so it can detect SSD crashes:
#   cp scripts/watchdog.sh /home/rka73/btc-watchdog/watchdog.sh
#
# Usage:
#   bash watchdog.sh                         # single check, exit
#   bash watchdog.sh --loop                  # loop every 60s
#   bash watchdog.sh --loop --interval 30    # loop every 30s
#
# Cron setup (every minute):
#   * * * * * /home/rka73/btc-watchdog/watchdog.sh >> /home/rka73/btc-watchdog/watchdog.log 2>&1
#
# Long-running mode:
#   nohup /home/rka73/btc-watchdog/watchdog.sh --loop --interval 60 \
#       > /home/rka73/btc-watchdog/watchdog.log 2>&1 &
# =============================================================================
set -uo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
ALERT_EMAIL="bdh_dev@sfu.ca"
ALERT_COOLDOWN_SEC=86400  # 24 hours between repeat alerts
ALERT_STATE_DIR="/tmp/btc-watchdog"
HOSTNAME_SHORT=$(hostname -s 2>/dev/null || echo "unknown")

BITCOIN_DATADIR="/local-scratch4/bitcoin_2025/bitcoin-core-data"
PROJECT_DIR="/local-scratch4/bitcoin_2025/bitcoin-realtime"

LOOP_MODE=false
INTERVAL=60

# ── Parse args ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --loop) LOOP_MODE=true; shift ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        *) shift ;;
    esac
done

mkdir -p "$ALERT_STATE_DIR"

# ── Helpers ──────────────────────────────────────────────────────────────────
timestamp() { date '+%Y-%m-%dT%H:%M:%S'; }

check_alert_cooldown() {
    local service="$1"
    local state_file="${ALERT_STATE_DIR}/${service}.last_alert"
    if [ -f "$state_file" ]; then
        local last_alert
        last_alert=$(cat "$state_file" 2>/dev/null || echo "0")
        local now
        now=$(date +%s)
        local diff=$((now - last_alert))
        if [ "$diff" -lt "$ALERT_COOLDOWN_SEC" ]; then
            return 1  # still in cooldown
        fi
    fi
    return 0  # ok to alert
}

record_alert() {
    local service="$1"
    date +%s > "${ALERT_STATE_DIR}/${service}.last_alert"
}

clear_alert() {
    local service="$1"
    rm -f "${ALERT_STATE_DIR}/${service}.last_alert"
}

send_alert() {
    local service="$1"
    local message="$2"

    if ! check_alert_cooldown "$service"; then
        return 0  # cooldown active, skip
    fi

    local subject="[ALERT] Bitcoin Pipeline - ${service} DOWN on ${HOSTNAME_SHORT}"
    local body
    body="Service: ${service}
Status: DOWN
Time: $(timestamp)
Host: ${HOSTNAME_SHORT}

${message}

Suggested action:
  bash /local-scratch4/bitcoin_2025/bitcoin-realtime/scripts/recover-pipeline.sh

See: /local-scratch4/bitcoin_2025/bitcoin-realtime/docs/ssd-crash-recovery.md"

    if command -v mail > /dev/null 2>&1; then
        echo "$body" | mail -s "$subject" "$ALERT_EMAIL" 2>/dev/null && record_alert "$service"
    elif command -v sendmail > /dev/null 2>&1; then
        {
            echo "To: ${ALERT_EMAIL}"
            echo "Subject: ${subject}"
            echo ""
            echo "$body"
        } | sendmail "$ALERT_EMAIL" 2>/dev/null && record_alert "$service"
    else
        echo "$(timestamp) ALERT: Cannot send email (no mail/sendmail). $subject" >&2
        record_alert "$service"
    fi
}

# ── Check Functions ──────────────────────────────────────────────────────────
FAILED_SERVICES=()
ALL_STATUS=""

check_service() {
    local name="$1"
    local status="$2"
    local fail_msg="${3:-}"

    if [ "$status" = "ok" ]; then
        ALL_STATUS="${ALL_STATUS} ${name}=ok"
        clear_alert "$name"
    else
        ALL_STATUS="${ALL_STATUS} ${name}=FAIL"
        FAILED_SERVICES+=("$name")
        send_alert "$name" "$fail_msg"
    fi
}

check_ssd_mount() {
    if mountpoint -q /local-scratch4 2>/dev/null; then
        check_service "ssd_mount" "ok"
    else
        check_service "ssd_mount" "fail" "SSD /local-scratch4 is NOT mounted. Unmount/remount the disk."
    fi
}

check_ssd_io_errors() {
    local recent_errors=0
    if command -v dmesg > /dev/null 2>&1; then
        local five_min_ago
        five_min_ago=$(date -d '5 minutes ago' '+%Y-%m-%dT%H:%M' 2>/dev/null || echo "")
        if [ -n "$five_min_ago" ]; then
            recent_errors=$(dmesg -T 2>/dev/null | grep -c "I/O error.*sd[d-z]" || echo "0")
        fi
    fi
    if [ "$recent_errors" -gt 0 ] 2>/dev/null; then
        check_service "ssd_io" "fail" "Found $recent_errors I/O errors on SSD in dmesg. Disk may be failing."
    else
        check_service "ssd_io" "ok"
    fi
}

check_docker_health() {
    local container="$1"
    local status
    status=$(sudo docker inspect --format='{{.State.Health.Status}}' "$container" 2>/dev/null || echo "missing")
    if [ "$status" = "healthy" ]; then
        check_service "docker_${container}" "ok"
    else
        local running
        running=$(sudo docker inspect --format='{{.State.Running}}' "$container" 2>/dev/null || echo "false")
        if [ "$running" = "true" ] && [ "$container" = "hive-metastore" ]; then
            check_service "docker_${container}" "ok"  # HMS is slow to report healthy
        else
            check_service "docker_${container}" "fail" "Container $container status: $status (running: $running)"
        fi
    fi
}

check_starrocks_fe() {
    if curl -sf http://localhost:8030/api/health > /dev/null 2>&1; then
        check_service "starrocks_fe" "ok"
    else
        check_service "starrocks_fe" "fail" "StarRocks FE health endpoint not responding. May need FE meta wipe."
    fi
}

check_starrocks_be() {
    if curl -sf http://localhost:8040/api/health > /dev/null 2>&1; then
        check_service "starrocks_be" "ok"
    else
        check_service "starrocks_be" "fail" "StarRocks BE health endpoint not responding."
    fi
}

check_bitcoind() {
    if bitcoin-cli -datadir="$BITCOIN_DATADIR" getblockchaininfo > /dev/null 2>&1; then
        check_service "bitcoind" "ok"
    else
        check_service "bitcoind" "fail" "Bitcoin Core RPC not responding."
    fi
}

check_normalizer() {
    if pgrep -f "live-normalizer/main.py" > /dev/null 2>&1; then
        check_service "normalizer" "ok"
    else
        check_service "normalizer" "fail" "Live normalizer process not running."
    fi
}

check_iceberg_writer() {
    if pgrep -f "iceberg_writer.py" > /dev/null 2>&1; then
        check_service "iceberg_writer" "ok"
    else
        check_service "iceberg_writer" "fail" "Iceberg writer process not running."
    fi
}

# ── Main Check Cycle ────────────────────────────────────────────────────────
run_checks() {
    FAILED_SERVICES=()
    ALL_STATUS=""

    check_ssd_mount

    if ! mountpoint -q /local-scratch4 2>/dev/null; then
        echo "$(timestamp) CRITICAL ssd_mount=FAIL (all other checks skipped)"
        return
    fi

    check_ssd_io_errors
    check_docker_health "mysql-hms"
    check_docker_health "hive-metastore"
    check_docker_health "minio"
    check_docker_health "kafka"
    check_starrocks_fe
    check_starrocks_be
    check_bitcoind
    check_normalizer
    check_iceberg_writer

    local result="OK"
    if [ ${#FAILED_SERVICES[@]} -gt 0 ]; then
        result="FAIL(${#FAILED_SERVICES[@]})"
    fi

    echo "$(timestamp) ${result}${ALL_STATUS}"
}

# ── Entry Point ──────────────────────────────────────────────────────────────
if [ "$LOOP_MODE" = true ]; then
    echo "$(timestamp) Watchdog starting in loop mode (interval=${INTERVAL}s, alert=${ALERT_EMAIL})"
    while true; do
        run_checks
        sleep "$INTERVAL"
    done
else
    run_checks
fi
