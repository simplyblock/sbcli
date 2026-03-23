#!/bin/bash
# FT=2 Soak Test - 2 hour random fault tolerance testing
# 4 volumes (one per node), continuous fio, random node shutdown/restart
set -uo pipefail

SSH="python3 tests/perf/ssh_run.py"
MGMT=192.168.10.111
LOGFILE="/tmp/ft2_soak_$(date +%Y%m%d_%H%M%S).log"
DURATION_SECS=7200
START_TIME=$(date +%s)

# Node IDs
PRIMARY_115="4e03ae27-2018-4c1d-831a-27ee464d4c8c"  # .115 - primary of vol1
PRIMARY_112="c5aa6c57-6f9b-4309-8439-f0766769fa0d"  # .112 - primary of vol2
PRIMARY_113="d4339392-63d1-4fad-8871-bd20b305c698"  # .113 - primary of vol3
PRIMARY_114="08dc25cb-40d5-4ac3-a5e1-65499802963a"  # .114 - primary of vol4

ALL_NODES=("$PRIMARY_112" "$PRIMARY_113" "$PRIMARY_114" "$PRIMARY_115")
ALL_IPS=("192.168.10.112" "192.168.10.113" "192.168.10.114" "192.168.10.115")

# Volume IDs
VOL1="0e428c8f-76f9-492d-b682-967ad77eb516"  # on .115
VOL2="45848555-5155-4288-93a7-14e26f351a29"  # on .112
VOL3="31a87393-4691-4e40-a847-4e0d2ce0425f"  # on .113
VOL4="87e0453d-2938-48d1-a319-6e52e360c389"  # on .114

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg" | tee -a "$LOGFILE"
}

ssh_cmd() {
    local cmd="$1"
    local timeout="${2:-30}"
    $SSH "$cmd" $MGMT "$timeout" 2>/dev/null || true
}

check_all_fio() {
    local count
    count=$(ssh_cmd "pgrep -cf 'fio --name=ft2'" 10 2>/dev/null | tr -d '[:space:]')
    if [ "$count" -ge 4 ] 2>/dev/null; then
        log "OK: $count fio processes alive"
        return 0
    else
        log "FAIL: Only ${count:-0} fio processes alive (expected >=4)"
        ssh_cmd "ps aux | grep 'fio --name' | grep -v grep" 10 | tee -a "$LOGFILE"
        return 1
    fi
}

wait_all_online() {
    local max_wait=120
    local waited=0
    while [ $waited -lt $max_wait ]; do
        local statuses
        statuses=$(ssh_cmd "sbctl sn list 2>&1 | grep -c 'online'" 15 2>/dev/null || echo "0")
        if [ "$statuses" -ge 4 ] 2>/dev/null; then
            log "All 4 nodes online"
            return 0
        fi
        sleep 5
        waited=$((waited + 5))
    done
    log "WARN: Not all nodes online after ${max_wait}s"
    ssh_cmd "sbctl sn list 2>&1 | awk -F'|' '{print \$2, \$7}'" 15 | tee -a "$LOGFILE"
    return 1
}

shutdown_node() {
    local node_id="$1"
    log "Shutting down node $node_id"
    ssh_cmd "sbctl sn shutdown $node_id --force 2>&1 | tail -1" 30 | tee -a "$LOGFILE"
}

restart_node() {
    local node_id="$1"
    log "Restarting node $node_id"
    # May already be online (health check auto-restart), try anyway
    ssh_cmd "sbctl sn restart $node_id 2>&1 | tail -1" 60 | tee -a "$LOGFILE" || true
}

# ---- TEST CASES ----

pick_random_node() {
    echo "${ALL_NODES[$((RANDOM % 4))]}"
}

pick_two_different_nodes() {
    local idx1=$((RANDOM % 4))
    local idx2=$(( (idx1 + 1 + RANDOM % 3) % 4 ))
    echo "${ALL_NODES[$idx1]} ${ALL_NODES[$idx2]}"
}

test_single_shutdown() {
    local node=$(pick_random_node)
    log "TEST: Single node shutdown/restart: $node"
    shutdown_node "$node"
    sleep 15
    check_all_fio || return 1
    restart_node "$node"
    sleep 10
    check_all_fio || return 1
}

test_dual_shutdown_30s_gap() {
    read -r node1 node2 <<< "$(pick_two_different_nodes)"
    log "TEST: Dual shutdown (30s gap): $node1 then $node2"
    shutdown_node "$node1"
    sleep 30
    check_all_fio || return 1
    shutdown_node "$node2"
    sleep 15
    check_all_fio || return 1
    restart_node "$node1"
    restart_node "$node2"
    wait_all_online
    sleep 10
    check_all_fio || return 1
}

run_random_test() {
    local test_num=$((RANDOM % 6 + 1))
    case $test_num in
        1)
            log "=== CASE 1: Shutdown/restart random node ==="
            test_single_shutdown
            ;;
        2)
            log "=== CASE 2: Shutdown/restart random node ==="
            test_single_shutdown
            ;;
        3)
            log "=== CASE 3: Shutdown/restart random node ==="
            test_single_shutdown
            ;;
        4)
            log "=== CASE 4: Dual shutdown (30s gap) ==="
            test_dual_shutdown_30s_gap
            ;;
        5)
            log "=== CASE 5: Dual shutdown (30s gap) ==="
            test_dual_shutdown_30s_gap
            ;;
        6)
            log "=== CASE 6: Dual shutdown (30s gap) ==="
            test_dual_shutdown_30s_gap
            ;;
    esac
}

# ---- SETUP ----

log "========================================="
log "FT=2 Soak Test Starting"
log "Duration: ${DURATION_SECS}s"
log "Log: $LOGFILE"
log "========================================="

# Connect all volumes (12 paths total)
log "Connecting all volumes..."

# Vol1 (.115 primary)
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.115 --trsvcid=4430 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL1" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.112 --trsvcid=4430 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL1" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.113 --trsvcid=4430 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL1" 15

# Vol2 (.112 primary)
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.112 --trsvcid=4420 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL2" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.113 --trsvcid=4420 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL2" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.114 --trsvcid=4420 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL2" 15

# Vol3 (.113 primary)
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.113 --trsvcid=4426 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL3" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.114 --trsvcid=4426 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL3" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.115 --trsvcid=4426 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL3" 15

# Vol4 (.114 primary)
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.114 --trsvcid=4428 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL4" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.115 --trsvcid=4428 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL4" 15
ssh_cmd "sudo nvme connect --reconnect-delay=2 --ctrl-loss-tmo=3600 --nr-io-queues=3 --keep-alive-tmo=7 --transport=tcp --traddr=10.10.10.112 --trsvcid=4428 --nqn=nqn.2023-02.io.simplyblock:0fe6dbc2-1745-4f2a-8b42-09574c454cdb:lvol:$VOL4" 15

sleep 3
log "NVMe connections:"
ssh_cmd "nvme list-subsys 2>&1" 15 | tee -a "$LOGFILE"

# Find block devices
log "Discovering block devices..."
ssh_cmd "lsblk -d -n -o NAME,SIZE | grep nvme" 15 | tee -a "$LOGFILE"

DEVS=($(ssh_cmd "lsblk -d -n -o NAME | grep nvme | sort" 10))
if [ ${#DEVS[@]} -ne 4 ]; then
    log "ERROR: Expected 4 NVMe devices, found ${#DEVS[@]}"
    ssh_cmd "lsblk -d -n -o NAME,SIZE | grep nvme" 10
    exit 1
fi
log "Devices: ${DEVS[*]}"

# Format and mount each
for i in 0 1 2 3; do
    dev="/dev/${DEVS[$i]}"
    mnt="/mnt/ft2_vol$((i+1))"
    log "Formatting $dev -> $mnt"
    ssh_cmd "mkfs.xfs -f $dev 2>&1 | tail -1" 60
    ssh_cmd "mkdir -p $mnt && mount $dev $mnt" 15
done

log "Mounts:"
ssh_cmd "df -h | grep ft2" 10 | tee -a "$LOGFILE"

# Clean up old fio systemd units
for i in 0 1 2 3; do
    ssh_cmd "systemctl stop fio-ft2-vol$((i+1)) 2>/dev/null; systemctl reset-failed fio-ft2-vol$((i+1)) 2>/dev/null" 10
done

# Start fio on each volume
for i in 0 1 2 3; do
    mnt="/mnt/ft2_vol$((i+1))"
    logf="/tmp/fio_ft2_vol$((i+1)).log"
    log "Starting fio on $mnt"
    ssh_cmd "rm -f $logf; systemd-run --unit=fio-ft2-vol$((i+1)) --remain-after-exit fio --name=ft2_vol$((i+1)) --directory=$mnt --direct=1 --rw=randrw --bs=4K --size=2G --numjobs=4 --iodepth=16 --ioengine=libaio --time_based --runtime=14400 --group_reporting --output=$logf" 15
    sleep 3
    log "  fio started on $mnt"
done

sleep 5
log "Verifying all fio processes..."
check_all_fio || { log "FATAL: fio failed to start properly"; exit 1; }

# ---- MAIN LOOP ----

iteration=0
passes=0
failures=0

while true; do
    elapsed=$(( $(date +%s) - START_TIME ))
    if [ $elapsed -ge $DURATION_SECS ]; then
        log "Time limit reached ($elapsed >= $DURATION_SECS)"
        break
    fi

    remaining=$(( DURATION_SECS - elapsed ))
    iteration=$((iteration + 1))

    log ""
    log "========================================="
    log "ITERATION $iteration (elapsed: ${elapsed}s, remaining: ${remaining}s)"
    log "========================================="

    # Wait for all nodes online before test
    wait_all_online || {
        log "WARN: Nodes not all online, waiting 30s more..."
        sleep 30
        wait_all_online || { log "ERROR: Nodes still not online, aborting"; break; }
    }

    # Run random test
    if run_random_test; then
        passes=$((passes + 1))
        log "RESULT: PASS (total: $passes pass, $failures fail)"
    else
        failures=$((failures + 1))
        log "RESULT: FAIL (total: $passes pass, $failures fail)"
    fi

    # Wait 2 minutes between tests
    log "Waiting 2 minutes before next test..."
    sleep 120

    # Re-check fio after cooldown
    if ! check_all_fio; then
        log "WARN: fio died during cooldown, restarting fio instances"
        failures=$((failures + 1))
        # Restart dead fio instances
        for i in 0 1 2 3; do
            mnt="/mnt/ft2_vol$((i+1))"
            logf="/tmp/fio_ft2_vol$((i+1)).log"
            local running
            running=$(ssh_cmd "pgrep -cf 'fio --name=ft2_vol$((i+1))'" 5 2>/dev/null | tr -d '[:space:]')
            if [ "${running:-0}" -eq 0 ] 2>/dev/null; then
                log "  Restarting fio on $mnt"
                ssh_cmd "systemctl stop fio-ft2-vol$((i+1)) 2>/dev/null; systemctl reset-failed fio-ft2-vol$((i+1)) 2>/dev/null" 10
                ssh_cmd "systemd-run --unit=fio-ft2-vol$((i+1)) --remain-after-exit fio --name=ft2_vol$((i+1)) --directory=$mnt --direct=1 --rw=randrw --bs=4K --size=2G --numjobs=4 --iodepth=16 --ioengine=libaio --time_based --runtime=14400 --group_reporting --output=$logf" 15
                sleep 3
            fi
        done
    fi
done

# ---- SUMMARY ----

log ""
log "========================================="
log "FT=2 SOAK TEST COMPLETE"
log "========================================="
log "Iterations: $iteration"
log "Passes: $passes"
log "Failures: $failures"
log "Duration: $(( $(date +%s) - START_TIME ))s"
log ""

# Final fio check
check_all_fio && log "FINAL: All fio processes still alive" || log "FINAL: Some fio processes died"

# Show node status
ssh_cmd "sbctl sn list 2>&1 | awk -F'|' '{print \$2, \$7}'" 15 | tee -a "$LOGFILE"

log "Log saved to: $LOGFILE"
