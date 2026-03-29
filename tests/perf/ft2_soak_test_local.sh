#!/bin/bash
# FT=2 Soak Test — runs directly on .111 (mgmt+client node)
# Prerequisites: cluster deployed with deploy_ft2_lab.sh, all nodes online, pool created
# Usage: bash ft2_soak_test_local.sh [duration_secs] [vol_count]
set -euo pipefail

DURATION_SECS="${1:-86400}"
VOL_COUNT="${2:-4}"
LOGFILE="/tmp/ft2_soak_$(date +%Y%m%d_%H%M%S).log"
START_TIME=$(date +%s)
RUN_ID="$(date +%Y%m%d_%H%M%S)"
POOL="pool01"
FIO_RUNTIME=90000  # 25h — longer than max test duration so fio never expires
OUTAGE_GAP_SECS=30
OVERLAP_VERIFY_SECS=15
POST_RESTART_SETTLE_SECS=120
EXPECTED_NODE_COUNT=4
FIO_STATUS_INTERVAL_SECS=10
FIO_STALL_TIMEOUT_SECS=45
FIO_ZERO_PROGRESS_LIMIT=3
MIGRATION_WAIT_TIMEOUT_SECS=1800
MIGRATION_POLL_INTERVAL_SECS=10
iteration=0
passes=0
failures=0
CLUSTER_ID=""

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOGFILE"
}

extract_uuid() {
    grep -Eo '[a-f0-9]{8}(-[a-f0-9]{4}){3}-[a-f0-9]{12}' | tail -1
}

get_cluster_id() {
    if [ -n "$CLUSTER_ID" ]; then
        echo "$CLUSTER_ID"
        return 0
    fi

    CLUSTER_ID=$(sbctl cluster list 2>/dev/null | awk -F'|' '/ACTIVE/ {gsub(/ /, "", $2); print $2; exit}')
    if [ -z "$CLUSTER_ID" ]; then
        log "ERROR: Unable to determine active cluster ID"
        return 1
    fi
    echo "$CLUSTER_ID"
}

get_pending_migration_count() {
    local cluster_id
    cluster_id="$(get_cluster_id)" || return 1

    CLUSTER_ID_ENV="$cluster_id" python3 - <<'PY'
import json
import os
import sys

try:
    import fdb
except Exception as exc:
    print(f"ERROR: failed to import fdb: {exc}", file=sys.stderr)
    sys.exit(2)

cluster_id = os.environ["CLUSTER_ID_ENV"]
fdb.api_version(730)
db = fdb.open()
tr = db.create_transaction()

terminal_statuses = {"done", "failed", "canceled", "cancelled"}
pending = 0

for _, value in tr.get_range_startswith(f"object/JobSchedule/{cluster_id}/".encode()):
    obj = json.loads(bytes(value))
    if obj.get("function_name") not in {"device_migration", "balancing_on_restart"}:
        continue
    status = (obj.get("status") or "").lower()
    if status not in terminal_statuses:
        pending += 1

print(pending)
PY
}

wait_for_rebalancing_complete() {
    local max_wait="${1:-$MIGRATION_WAIT_TIMEOUT_SECS}"
    local waited=0
    local stable_zero=0

    log "Waiting for rebalancing and device migration tasks to complete..."

    while [ "$waited" -lt "$max_wait" ]; do
        local pending
        if ! pending=$(get_pending_migration_count 2>>"$LOGFILE"); then
            log "ERROR: Failed to query migration tasks from FDB"
            return 1
        fi

        if [ "$pending" -eq 0 ] 2>/dev/null; then
            stable_zero=$((stable_zero + 1))
            if [ "$stable_zero" -ge 2 ]; then
                log "Rebalancing complete after ${waited}s"
                return 0
            fi
        else
            stable_zero=0
        fi

        if [ $((waited % 60)) -eq 0 ]; then
            log "  ... pending migration/rebalance tasks: ${pending} (${waited}s/${max_wait}s)"
        fi

        sleep "$MIGRATION_POLL_INTERVAL_SECS"
        waited=$((waited + MIGRATION_POLL_INTERVAL_SECS))
    done

    log "WARN: Rebalancing did not complete within ${max_wait}s"
    return 1
}

stop_fio_unit() {
    local unit="$1"
    timeout 10 systemctl stop "$unit" 2>/dev/null || true
    timeout 10 systemctl reset-failed "$unit" 2>/dev/null || true
}

cleanup_mount() {
    local mount_point="$1"
    timeout 10 umount "$mount_point" 2>/dev/null || timeout 10 umount -lf "$mount_point" 2>/dev/null || true
}

print_summary() {
    local total_secs total_hours
    total_secs=$(( $(date +%s) - START_TIME ))
    total_hours=$(awk "BEGIN{printf \"%.1f\", $total_secs/3600}")

    log ""
    log "========================================="
    log "FT=2 SOAK TEST COMPLETE"
    log "========================================="
    log "Iterations: $iteration"
    log "Passes: $passes"
    log "Failures: $failures"
    if [ "$iteration" -gt 0 ]; then
        log "Pass rate: $(awk "BEGIN{printf \"%.0f\", $passes/$iteration*100}")%"
    fi
    log "Duration: ${total_hours}h (${total_secs}s)"
    check_all_fio && log "FINAL: All fio alive" || log "FINAL: Some fio died"
    log "Node status:"
    sbctl sn list 2>&1 | grep -E 'online|offline|unavailable' | awk -F'|' '{printf "  %s %s\n", $2, $7}' | tee -a "$LOGFILE" || true
    log "Log: $LOGFILE"
}

trap print_summary EXIT

check_all_fio() {
    local count
    count=$(pgrep -cf 'fio --name=ft2' 2>/dev/null || echo 0)
    if [ "$count" -ge "$VOL_COUNT" ] 2>/dev/null; then
        log "OK: $count fio processes alive"
        # Check fio logs for freshness and IO errors
        for i in $(seq 1 "$VOL_COUNT"); do
            local logf="/tmp/fio_ft2_soak_v${i}.log"
            if [ -f "$logf" ]; then
                local age
                age=$(( $(date +%s) - $(stat -c %Y "$logf" 2>/dev/null || echo 0) ))
                if [ "$age" -gt "$FIO_STALL_TIMEOUT_SECS" ] 2>/dev/null; then
                    log "FAIL: fio vol$i log is stale (${age}s without status update): $logf"
                    tail -20 "$logf" | tee -a "$LOGFILE"
                    return 1
                fi
                local zero_lines
                zero_lines=$(grep -E '^[[:space:]]*(read|write|trim):.*IOPS=0([^0-9]|$).*BW=0' "$logf" 2>/dev/null | tail -"$FIO_ZERO_PROGRESS_LIMIT" | wc -l)
                if [ "$zero_lines" -ge "$FIO_ZERO_PROGRESS_LIMIT" ] 2>/dev/null; then
                    log "FAIL: fio vol$i shows zero progress for ${zero_lines} consecutive status intervals: $logf"
                    grep -E '^[[:space:]]*(read|write|trim):' "$logf" | tail -10 | tee -a "$LOGFILE"
                    return 1
                fi
                local err_val
                err_val=$(grep -oP 'err=\s*\K[0-9]+' "$logf" 2>/dev/null | tail -1)
                if [ -n "$err_val" ] && [ "$err_val" -gt 0 ] 2>/dev/null; then
                    log "FAIL: fio vol$i has IO errors (err=$err_val) in $logf"
                    grep -E 'error|err=' "$logf" | tail -5 | tee -a "$LOGFILE"
                    return 1
                fi
            fi
        done
        return 0
    else
        log "FAIL: Only ${count:-0} fio processes alive (expected >=$VOL_COUNT)"
        ps aux | grep 'fio --name=ft2' | grep -v grep | tee -a "$LOGFILE"
        # Check if any fio exited with errors
        for i in $(seq 1 "$VOL_COUNT"); do
            local logf="/tmp/fio_ft2_soak_v${i}.log"
            if [ -f "$logf" ]; then
                local err_val
                err_val=$(grep -oP 'err=\s*\K[0-9]+' "$logf" 2>/dev/null | tail -1)
                if [ -n "$err_val" ] && [ "$err_val" -gt 0 ] 2>/dev/null; then
                    log "  vol$i IO errors: err=$err_val"
                    grep -E 'error|err=' "$logf" | tail -3 | tee -a "$LOGFILE"
                fi
            fi
        done
        return 1
    fi
}

wait_all_online() {
    local max_wait="${1:-300}"
    local waited=0
    while [ $waited -lt $max_wait ]; do
        local online_count
        online_count=$(sbctl sn list 2>&1 | grep -c 'online' || echo 0)
        if [ "$online_count" -ge 4 ] 2>/dev/null; then
            log "All 4 nodes online (waited ${waited}s)"
            return 0
        fi
        sleep 10
        waited=$((waited + 10))
        if [ $((waited % 60)) -eq 0 ]; then
            log "  ... waiting for nodes: ${online_count}/4 online (${waited}s/${max_wait}s)"
        fi
    done
    log "WARN: Not all nodes online after ${max_wait}s"
    sbctl sn list 2>&1 | awk -F'|' '{print $2, $7}' | tee -a "$LOGFILE"
    return 1
}

shutdown_node() {
    local node_id="$1"
    log "Shutting down node $node_id"
    sbctl sn shutdown "$node_id" --force 2>&1 | tail -1 | tee -a "$LOGFILE"
}

restart_node() {
    local node_id="$1"
    log "Restarting node $node_id"
    sbctl sn restart "$node_id" 2>&1 | tail -1 | tee -a "$LOGFILE" || true
}

# Get node IDs
get_node_ids() {
    ALL_NODES=()
    while IFS= read -r line; do
        nid=$(echo "$line" | awk -F'|' '{print $2}' | tr -d ' ')
        [ -n "$nid" ] && ALL_NODES+=("$nid")
    done < <(sbctl sn list 2>&1 | grep -E 'online|offline|unavailable')
    log "Found ${#ALL_NODES[@]} nodes: ${ALL_NODES[*]}"
}

pick_random_node() {
    echo "${ALL_NODES[$((RANDOM % ${#ALL_NODES[@]}))]}"
}

pick_two_different_nodes() {
    local idx1=$((RANDOM % ${#ALL_NODES[@]}))
    local idx2=$(( (idx1 + 1 + RANDOM % (${#ALL_NODES[@]} - 1)) % ${#ALL_NODES[@]} ))
    echo "${ALL_NODES[$idx1]} ${ALL_NODES[$idx2]}"
}

test_dual_shutdown_30s_gap() {
    read -r node1 node2 <<< "$(pick_two_different_nodes)"
    log "TEST: Dual shutdown with overlap: $node1 then $node2 after ${OUTAGE_GAP_SECS}s"
    shutdown_node "$node1"
    sleep "$OUTAGE_GAP_SECS"
    check_all_fio || { restart_node "$node1"; return 1; }
    shutdown_node "$node2"
    sleep "$OVERLAP_VERIFY_SECS"
    check_all_fio || { restart_node "$node1"; restart_node "$node2"; return 1; }
    restart_node "$node1"
    restart_node "$node2"
    wait_all_online "$POST_RESTART_SETTLE_SECS"
    wait_for_rebalancing_complete "$MIGRATION_WAIT_TIMEOUT_SECS"
    check_all_fio || return 1
}

run_random_test() {
    log "=== CASE: Random dual shutdown with enforced overlap ==="
    test_dual_shutdown_30s_gap
}

# ---- SETUP ----

log "========================================="
log "FT=2 Soak Test Starting"
log "Duration: ${DURATION_SECS}s, Volumes: $VOL_COUNT"
log "Log: $LOGFILE"
log "========================================="

log "Cleaning up prior fio units, mounts and NVMe connections on this client..."
pkill -f 'fio --name=ft2_soak_v' 2>/dev/null || true
rm -f /tmp/fio_ft2_soak_v*.log
for i in $(seq 1 "$VOL_COUNT"); do
    stop_fio_unit "fio-ft2-soak-v${i}"
    cleanup_mount "/mnt/ft2_soak_v${i}"
done
timeout 20 nvme disconnect-all 2>&1 | tee -a "$LOGFILE" || true
sleep 2
log "Client cleanup complete"

get_node_ids
if [ "${#ALL_NODES[@]}" -ne "$EXPECTED_NODE_COUNT" ]; then
    log "ERROR: Need exactly ${EXPECTED_NODE_COUNT} storage nodes, found ${#ALL_NODES[@]}"
    exit 1
fi
if [ "$VOL_COUNT" -ne "$EXPECTED_NODE_COUNT" ]; then
    log "ERROR: This test expects one volume per storage node, so vol_count must be ${EXPECTED_NODE_COUNT}"
    exit 1
fi

get_cluster_id >/dev/null

# Create volumes
log "Creating $VOL_COUNT volumes..."
VOL_IDS=()
for i in $(seq 0 $((VOL_COUNT - 1))); do
    node_id="${ALL_NODES[$i]}"
    vol_name="ft2_soak_${RUN_ID}_v$((i+1))"
    create_out="$(sbctl lvol add "$vol_name" 10G "$POOL" --host-id "$node_id" 2>&1 | tee -a "$LOGFILE")"
    vid="$(printf '%s\n' "$create_out" | extract_uuid)"
    if [ -z "$vid" ]; then
        log "ERROR: Failed to create $vol_name on host-id $node_id"
        exit 1
    fi
    VOL_IDS+=("$vid")
    log "  $vol_name: $vid (host-id $node_id)"
done

# Snapshot devices before connecting so we can find only new ones
DEVS_BEFORE=($(lsblk -d -n -o NAME | grep nvme | sort || true))

# Connect all volumes
log "Connecting volumes..."
for vid in "${VOL_IDS[@]}"; do
    while IFS= read -r cmd; do
        [ -n "$cmd" ] && eval "$cmd" 2>&1 | tee -a "$LOGFILE"
    done < <(sbctl lvol connect "$vid" 2>&1 | grep "^sudo nvme connect")
done
sleep 3

log "NVMe subsystems:"
nvme list-subsys 2>&1 | tee -a "$LOGFILE"

# Find only newly-connected block devices
DEVS_AFTER=($(lsblk -d -n -o NAME | grep nvme | sort || true))
DEVS=()
for dev in "${DEVS_AFTER[@]}"; do
    found=0
    for old in "${DEVS_BEFORE[@]+"${DEVS_BEFORE[@]}"}"; do
        [ "$dev" = "$old" ] && found=1 && break
    done
    [ "$found" -eq 0 ] && DEVS+=("$dev")
done
log "Found ${#DEVS[@]} new NVMe devices: ${DEVS[*]}"

if [ ${#DEVS[@]} -lt "$VOL_COUNT" ]; then
    log "ERROR: Expected $VOL_COUNT new devices, found ${#DEVS[@]}"
    log "All devices: ${DEVS_AFTER[*]}"
    log "Pre-existing devices: ${DEVS_BEFORE[*]+"${DEVS_BEFORE[*]}"}"
    exit 1
fi

# Format, mount, start fio
for i in $(seq 0 $((VOL_COUNT - 1))); do
    dev="/dev/${DEVS[$i]}"
    mnt="/mnt/ft2_soak_v$((i+1))"
    logf="/tmp/fio_ft2_soak_v$((i+1)).log"
    log "Setting up $dev -> $mnt"
    mkfs.xfs -f "$dev" > /dev/null 2>&1
    mkdir -p "$mnt"
    mount "$dev" "$mnt"
    stop_fio_unit "fio-ft2-soak-v$((i+1))"
    systemd-run --unit="fio-ft2-soak-v$((i+1))" --remain-after-exit \
        fio --name="ft2_soak_v$((i+1))" --directory="$mnt" --direct=1 --rw=randrw \
        --bs=4K --size=2G --numjobs=4 --iodepth=16 --ioengine=libaio \
        --time_based --runtime=$FIO_RUNTIME --group_reporting \
        --status-interval="$FIO_STATUS_INTERVAL_SECS" --output="$logf"
    sleep 2
done

sleep 5
log "Verifying fio..."
check_all_fio || { log "FATAL: fio failed to start"; exit 1; }

# ---- MAIN LOOP ----

while true; do
    elapsed=$(( $(date +%s) - START_TIME ))
    if [ $elapsed -ge $DURATION_SECS ]; then
        log "Time limit reached"
        break
    fi

    remaining=$(( DURATION_SECS - elapsed ))
    iteration=$((iteration + 1))

    elapsed_h=$(awk "BEGIN{printf \"%.1f\", $elapsed/3600}")
    remaining_h=$(awk "BEGIN{printf \"%.1f\", $remaining/3600}")
    log ""
    log "========================================="
    log "ITERATION $iteration (elapsed: ${elapsed_h}h, remaining: ${remaining_h}h)"
    log "========================================="
    log "Node status:"
    sbctl sn list 2>&1 | grep -E 'online|offline|unavailable' | awk -F'|' '{printf "  %s %s\n", $2, $7}' | tee -a "$LOGFILE"
    log "Fio status: $(pgrep -cf 'fio --name=ft2' 2>/dev/null || echo 0) processes"

    # Re-read node IDs (may change after restarts)
    get_node_ids

    if ! wait_all_online "$POST_RESTART_SETTLE_SECS"; then
        log "ERROR: Cluster is not healthy before starting the next outage iteration"
        failures=$((failures + 1))
        exit 1
    fi

    if ! wait_for_rebalancing_complete "$MIGRATION_WAIT_TIMEOUT_SECS"; then
        log "ERROR: Cluster rebalancing is not complete before starting the next outage iteration"
        failures=$((failures + 1))
        exit 1
    fi

    if run_random_test; then
        passes=$((passes + 1))
        log "RESULT: PASS (total: $passes pass, $failures fail)"
    else
        failures=$((failures + 1))
        log "RESULT: FAIL (total: $passes pass, $failures fail)"
        exit 1
    fi

    log "Waiting for post-test node settle..."
    sleep "$POST_RESTART_SETTLE_SECS"

    if ! wait_for_rebalancing_complete "$MIGRATION_WAIT_TIMEOUT_SECS"; then
        log "ERROR: Cluster rebalancing did not complete during cooldown"
        failures=$((failures + 1))
        exit 1
    fi

    if ! check_all_fio; then
        log "ERROR: fio exited during cooldown"
        failures=$((failures + 1))
        exit 1
    fi
done
