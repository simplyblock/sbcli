#!/bin/bash
# FT=2 Overlapping Dual-Node Outage Test
# Run from .211 (mgmt node)
#
# 1. Creates one volume per storage node
# 2. Connects volumes from .211
# 3. Starts long-running fio on each volume
# 4. Iterates: picks 2 random nodes, overlapping outages, verifies fio alive
# 5. Waits for cluster active before next iteration
#
# Usage: bash ft2_outage_test.sh [iterations] [outage_duration_sec]

set -uo pipefail

ITERATIONS="${1:-20}"
OUTAGE_DURATION="${2:-30}"
OVERLAP_DELAY=10  # seconds between first and second outage
POOL="pool_test"
VOL_SIZE="1G"
FIO_RUNTIME=86400  # 24h

STORAGE_NODES=(192.168.10.205 192.168.10.206 192.168.10.207 192.168.10.208)
SN_PASS="3tango11"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

run_mgmt() {
    "$@" 2>&1
}

run_remote() {
    local ip="$1"; shift
    sshpass -p "$SN_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "root@$ip" "$@"
}

cleanup() {
    log "Cleaning up..."
    # Kill all fio processes
    for pid_file in /tmp/fio_*.pid; do
        if [ -f "$pid_file" ]; then
            kill "$(cat "$pid_file")" 2>/dev/null
            rm -f "$pid_file"
        fi
    done
    # Disconnect all nvme
    nvme disconnect-all 2>/dev/null
    log "Cleanup done"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Phase 1: Get cluster info
# ---------------------------------------------------------------------------
log "=== Phase 1: Cluster topology ==="
CLUSTER_ID=$(sbctl cluster list --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['UUID'])")
log "Cluster: $CLUSTER_ID"

# Get node UUIDs and their IPs
declare -A NODE_IPS
declare -A NODE_UUIDS
NODE_LIST=()

NODES_JSON=$(sbctl sn list --json 2>/dev/null)
for ip in "${STORAGE_NODES[@]}"; do
    uuid=$(echo "$NODES_JSON" | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n.get('Management IP') == '$ip':
        print(n['UUID']); break
" 2>/dev/null)
    if [ -n "$uuid" ]; then
        NODE_IPS["$uuid"]="$ip"
        NODE_UUIDS["$ip"]="$uuid"
        NODE_LIST+=("$uuid")
        log "  Node: $uuid ($ip)"
    fi
done

NUM_NODES=${#NODE_LIST[@]}
if [ "$NUM_NODES" -lt 3 ]; then
    log "ERROR: Need at least 3 primary nodes for FT=2 test, found $NUM_NODES"
    exit 1
fi

# ---------------------------------------------------------------------------
# Phase 2: Create volumes — one per node
# ---------------------------------------------------------------------------
log "=== Phase 2: Create volumes ==="
declare -A VOL_UUIDS
declare -A VOL_NQNS

for uuid in "${NODE_LIST[@]}"; do
    ip="${NODE_IPS[$uuid]}"
    vol_name="test_${ip##*.}"
    log "  Creating $vol_name on $ip..."
    vol_uuid=$(sbctl lvol add "$vol_name" "$VOL_SIZE" "$POOL" --ha-type ha 2>&1 | tail -1)
    if [[ "$vol_uuid" =~ ^[a-f0-9-]+$ ]]; then
        VOL_UUIDS["$uuid"]="$vol_uuid"
        log "    Volume: $vol_uuid"
    else
        log "    ERROR: Failed to create volume: $vol_uuid"
        exit 1
    fi
done

# ---------------------------------------------------------------------------
# Phase 3: Connect volumes from .211
# ---------------------------------------------------------------------------
log "=== Phase 3: Connect volumes ==="
HOST_NQN=$(cat /etc/nvme/hostnqn)
log "  Host NQN: $HOST_NQN"

declare -A VOL_DEVICES

for uuid in "${NODE_LIST[@]}"; do
    vol_uuid="${VOL_UUIDS[$uuid]}"
    log "  Adding host + connecting $vol_uuid..."

    # Add host NQN
    sbctl lvol add-host "$vol_uuid" "$HOST_NQN" 2>&1 | tail -3

    # Get connect commands
    CONNECT_CMDS=$(sbctl lvol connect "$vol_uuid" --host-nqn "$HOST_NQN" 2>&1)

    # Run each connect command
    echo "$CONNECT_CMDS" | grep "nvme connect" | while IFS= read -r cmd; do
        cmd="${cmd#sudo }"
        eval "$cmd" 2>&1 || true
    done

    sleep 2
done

# Wait for devices to appear
sleep 5
log "  Connected devices:"
nvme list 2>/dev/null | grep -i simplyblock || true

# Map volumes to /dev/nvme*n1 devices
NVME_DEVICES=($(nvme list 2>/dev/null | grep -i simplyblock | awk '{print $1}'))
log "  Found ${#NVME_DEVICES[@]} NVMe devices"

if [ "${#NVME_DEVICES[@]}" -lt "${#NODE_LIST[@]}" ]; then
    log "WARNING: Expected ${#NODE_LIST[@]} devices, got ${#NVME_DEVICES[@]}"
fi

# ---------------------------------------------------------------------------
# Phase 4: Start long-running fio on each device
# ---------------------------------------------------------------------------
log "=== Phase 4: Start fio ==="
FIO_PIDS=()

for i in "${!NVME_DEVICES[@]}"; do
    dev="${NVME_DEVICES[$i]}"
    log "  Starting fio on $dev..."
    fio --name="test_$i" \
        --filename="$dev" \
        --ioengine=libaio \
        --direct=1 \
        --rw=randrw \
        --rwmixread=70 \
        --bs=4k \
        --iodepth=1 \
        --numjobs=1 \
        --time_based \
        --runtime="$FIO_RUNTIME" \
        --output="/tmp/fio_${i}.log" \
        --write_iops_log="/tmp/fio_iops_${i}" \
        --log_avg_msec=1000 \
        &
    FIO_PIDS+=($!)
    echo "${FIO_PIDS[-1]}" > "/tmp/fio_${i}.pid"
    log "    PID: ${FIO_PIDS[-1]}"
done

sleep 5

# Verify all fio processes are running
for pid in "${FIO_PIDS[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
        log "ERROR: fio PID $pid not running!"
        exit 1
    fi
done
log "  All fio processes running"

# ---------------------------------------------------------------------------
# Phase 5: Outage iterations
# ---------------------------------------------------------------------------
log "=== Phase 5: Starting $ITERATIONS outage iterations ==="

outage_node() {
    local ip="$1"
    local duration="$2"
    local method="$3"
    local node_uuid="$4"

    case "$method" in
        network)
            log "    Blocking network on $ip for ${duration}s..."
            # Block all traffic, wait, then unblock.  Run via nohup so the
            # unblock still fires even if our SSH session drops.
            run_remote "$ip" "nohup bash -c 'iptables -A INPUT -j DROP; iptables -A OUTPUT -j DROP; sleep $duration; iptables -F' &>/dev/null &"
            sleep "$duration"
            # Safety: ensure iptables is flushed (the remote command may not have finished)
            for _retry in 1 2 3 4 5; do
                run_remote "$ip" "iptables -F" 2>/dev/null && break
                sleep 2
            done
            ;;
        shutdown)
            log "    Graceful shutdown $node_uuid on $ip..."
            sbctl sn shutdown "$node_uuid" --force 2>&1 | tail -1 || true
            sleep "$duration"
            log "    Restarting $node_uuid..."
            sbctl sn restart "$node_uuid" 2>&1 | tail -1 || true
            ;;
    esac
}

ensure_all_nodes_online() {
    # Ensure all nodes are online.  For any node that is not online,
    # force-shutdown then restart it.  Abort if we can't recover.
    log "  Ensuring all nodes are online..."
    for uuid in "${NODE_LIST[@]}"; do
        ip="${NODE_IPS[$uuid]}"
        for attempt in $(seq 1 30); do
            node_status=$(sbctl sn list --json 2>/dev/null | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n['UUID'] == '$uuid':
        print(n['Status']); break
" 2>/dev/null || echo "unknown")

            if [ "$node_status" = "online" ]; then
                break
            fi

            if [ "$attempt" -eq 1 ]; then
                log "    Node $ip ($uuid) status: $node_status — recovering..."
                # Flush iptables in case of leftover network block
                run_remote "$ip" "iptables -F" 2>/dev/null || true
            fi

            # If node is not online and not already restarting, force-shutdown + restart
            if [ "$node_status" != "in_restart" ] && [ "$node_status" != "in_creation" ]; then
                if [ "$node_status" != "offline" ]; then
                    sbctl sn shutdown "$uuid" --force 2>&1 | tail -1 || true
                    sleep 3
                fi
                sbctl sn restart "$uuid" 2>&1 | tail -1 || true
            fi

            sleep 10
        done

        if [ "$node_status" != "online" ]; then
            log "  FATAL: Node $ip ($uuid) stuck in $node_status after recovery attempts"
            sbctl sn list 2>&1 | head -10
            return 1
        fi
        log "    Node $ip: online"
    done
    return 0
}

wait_cluster_active() {
    log "  Waiting for cluster ACTIVE (not rebalancing)..."
    for attempt in $(seq 1 180); do
        status=$(sbctl cluster list --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['Status'])" 2>/dev/null || echo "unknown")
        if [ "$status" = "ACTIVE" ]; then
            log "  Cluster ACTIVE"
            return 0
        fi
        if [ "$((attempt % 10))" -eq 0 ]; then
            log "    Cluster status: $status (attempt $attempt)"
        fi
        sleep 5
    done
    log "  ERROR: Cluster not ACTIVE after 15 minutes: $status"
    return 1
}

for iter in $(seq 1 "$ITERATIONS"); do
    log ""
    log "=== Iteration $iter/$ITERATIONS ==="

    # Pick 2 random distinct nodes
    idx1=$((RANDOM % NUM_NODES))
    idx2=$(( (idx1 + 1 + RANDOM % (NUM_NODES - 1)) % NUM_NODES ))
    node1="${NODE_LIST[$idx1]}"
    node2="${NODE_LIST[$idx2]}"
    ip1="${NODE_IPS[$node1]}"
    ip2="${NODE_IPS[$node2]}"

    # Random outage methods
    methods=("network" "shutdown")
    method1="${methods[$((RANDOM % 2))]}"
    method2="${methods[$((RANDOM % 2))]}"

    log "  Node 1: $ip1 ($node1) — $method1"
    log "  Node 2: $ip2 ($node2) — $method2"

    # Start first outage
    outage_node "$ip1" "$OUTAGE_DURATION" "$method1" "$node1" &
    OUTAGE1_PID=$!

    # Overlap delay, then second outage
    sleep "$OVERLAP_DELAY"
    outage_node "$ip2" "$OUTAGE_DURATION" "$method2" "$node2" &
    OUTAGE2_PID=$!

    # Wait for both outages to complete
    wait "$OUTAGE1_PID" 2>/dev/null || true
    wait "$OUTAGE2_PID" 2>/dev/null || true

    log "  Both outages resolved"

    # --- Recovery: ensure all nodes come back online ---
    if ! ensure_all_nodes_online; then
        log "  FATAL: Could not recover all nodes. Aborting."
        exit 1
    fi

    # --- Check fio processes ---
    all_alive=true
    for i in "${!FIO_PIDS[@]}"; do
        pid="${FIO_PIDS[$i]}"
        if ! kill -0 "$pid" 2>/dev/null; then
            log "  ERROR: fio PID $pid (device ${NVME_DEVICES[$i]}) is DEAD!"
            all_alive=false
        fi
    done

    if $all_alive; then
        log "  All fio processes alive"
    else
        log "  FAILURE: Some fio processes died!"
        for i in "${!NVME_DEVICES[@]}"; do
            if [ -f "/tmp/fio_${i}.log" ]; then
                log "  --- fio log ${NVME_DEVICES[$i]} ---"
                tail -5 "/tmp/fio_${i}.log"
            fi
        done
        exit 1
    fi

    # --- Wait for cluster ACTIVE and not rebalancing ---
    if ! wait_cluster_active; then
        log "  FATAL: Cluster did not reach ACTIVE. Aborting."
        sbctl cluster list 2>&1 | head -5
        sbctl sn list 2>&1 | head -10
        exit 1
    fi

    sbctl sn list 2>&1 | head -10
    log "  Iteration $iter/$ITERATIONS complete"
    sleep 10
done

log ""
log "=== ALL $ITERATIONS ITERATIONS PASSED ==="
log "Stopping fio..."
for pid in "${FIO_PIDS[@]}"; do
    kill "$pid" 2>/dev/null
done
wait 2>/dev/null
log "Done"
