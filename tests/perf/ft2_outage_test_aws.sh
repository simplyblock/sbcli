#!/bin/bash
# FT=2 Overlapping Dual-Node Outage Test — AWS variant
# Run from the management (CP) node.
#
# Reads cluster_metadata.json to discover storage and client IPs.
# Uses SSH key auth (ec2-user) instead of sshpass/root.
# Fio runs on the dedicated client node, not the mgmt node.
#
# Pre-requisites on mgmt node:
#   - sbctl installed and working
#   - SSH key (~/.ssh/mtes01.pem) copied to mgmt node
#   - Client node has: nvme-cli, fio, nvme-tcp module loaded
#
# Usage: bash ft2_outage_test_aws.sh [iterations] [outage_duration_sec]
#   iterations=0 (default) means run indefinitely

set -uo pipefail

ITERATIONS="${1:-0}"
OUTAGE_DURATION="${2:-30}"
OVERLAP_DELAY=30  # seconds between first and second outage
POOL="pool01"
VOL_SIZE="1G"
FIO_RUNTIME=86400  # 24h

# --- AWS SSH config ---
SSH_KEY="${SSH_KEY:-$HOME/.ssh/mtes01.pem}"
SSH_USER="ec2-user"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -i $SSH_KEY"

# --- Read cluster_metadata.json ---
METADATA="${METADATA:-cluster_metadata.json}"
if [ ! -f "$METADATA" ]; then
    echo "ERROR: $METADATA not found. Copy it to this directory."
    exit 1
fi

# Parse storage node private IPs from metadata
mapfile -t STORAGE_NODES < <(python3 -c "
import json
with open('$METADATA') as f:
    m = json.load(f)
for sn in m['storage_nodes']:
    print(sn['private_ip'])
")

# Parse client private IP
CLIENT_IP=$(python3 -c "
import json
with open('$METADATA') as f:
    m = json.load(f)
print(m['clients'][0]['private_ip'])
")

log() { echo "[$(date '+%H:%M:%S')] $*"; }

run_remote() {
    local ip="$1"; shift
    ssh $SSH_OPTS "$SSH_USER@$ip" "$@"
}

run_client() {
    ssh $SSH_OPTS "$SSH_USER@$CLIENT_IP" "$@"
}

cleanup() {
    log "Cleaning up..."
    # Kill all fio processes on client
    run_client "sudo pkill -f fio" 2>&1 || true
    # Disconnect all nvme on client
    run_client "sudo nvme disconnect-all" 2>&1 || true
    log "Cleanup done"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Phase 1: Get cluster info
# ---------------------------------------------------------------------------
log "=== Phase 1: Cluster topology ==="
log "  Storage nodes: ${STORAGE_NODES[*]}"
log "  Client node:   $CLIENT_IP"

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
" 2>&1)
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
TIMESTAMP=$(date '+%Y%m%d_%H%M%S')

for uuid in "${NODE_LIST[@]}"; do
    ip="${NODE_IPS[$uuid]}"
    vol_name="ft2_${ip##*.}_${TIMESTAMP}"
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
# Phase 3: Connect volumes from client node, raw block devices
# ---------------------------------------------------------------------------
log "=== Phase 3: Connect volumes from client ==="
HOST_NQN=$(run_client "cat /etc/nvme/hostnqn" 2>&1)
log "  Client Host NQN: $HOST_NQN"

for uuid in "${NODE_LIST[@]}"; do
    vol_uuid="${VOL_UUIDS[$uuid]}"
    log "  Adding host + connecting $vol_uuid..."

    # Add host NQN
    sbctl -d lvol add-host "$vol_uuid" "$HOST_NQN" 2>&1

    # Get connect commands
    CONNECT_CMDS=$(sbctl -d lvol connect "$vol_uuid" --host-nqn "$HOST_NQN" 2>&1)

    # Run each connect command on the client
    echo "$CONNECT_CMDS" | grep "nvme connect" | while IFS= read -r cmd; do
        cmd="${cmd#sudo }"
        run_client "sudo $cmd" 2>&1 || true
    done

    sleep 2
done

# Wait for devices to appear on client
sleep 5
log "  Connected devices on client:"
run_client "sudo nvme list" 2>&1 || true

# Map volumes to /dev/nvme*n1 devices on client
NVME_DEVICES=()
for uuid in "${NODE_LIST[@]}"; do
    vol_uuid="${VOL_UUIDS[$uuid]}"
    dev=$(run_client "sudo nvme list" 2>&1 | grep "$vol_uuid" | awk '{print $1}')
    if [ -n "$dev" ]; then
        NVME_DEVICES+=("$dev")
    fi
done
log "  Found ${#NVME_DEVICES[@]} NVMe devices on client"

if [ "${#NVME_DEVICES[@]}" -lt "${#NODE_LIST[@]}" ]; then
    log "WARNING: Expected ${#NODE_LIST[@]} devices, got ${#NVME_DEVICES[@]}"
fi

# ---------------------------------------------------------------------------
# Phase 4: Start long-running fio on client
# ---------------------------------------------------------------------------
log "=== Phase 4: Start fio on client ==="
FIO_COUNT=${#NVME_DEVICES[@]}

for i in "${!NVME_DEVICES[@]}"; do
    dev="${NVME_DEVICES[$i]}"
    log "  Starting fio on $dev..."
    run_client "sudo fio --name=test_$i \
        --filename=$dev \
        --ioengine=libaio \
        --direct=1 \
        --rw=randrw \
        --rwmixread=70 \
        --bs=4k \
        --iodepth=4 \
        --numjobs=4 \
        --max_latency=15000000 \
        --time_based \
        --runtime=$FIO_RUNTIME \
        --output=/tmp/fio_${i}.log \
        --write_iops_log=/tmp/fio_iops_${i} \
        --log_avg_msec=1000 \
        </dev/null &>/dev/null &"
    log "    Started fio job $i"
done

sleep 5

# Verify fio processes running on client
FIO_RUNNING=$(run_client "pgrep -c fio" 2>/dev/null || echo "0")
log "  Fio processes on client: $FIO_RUNNING"
if [ "$FIO_RUNNING" -lt 1 ]; then
    log "ERROR: No fio processes running on client!"
    exit 1
fi
log "  Fio running"

# ---------------------------------------------------------------------------
# Phase 5: Outage iterations
# ---------------------------------------------------------------------------
if [ "$ITERATIONS" -gt 0 ]; then
    log "=== Phase 5: Starting $ITERATIONS outage iterations ==="
else
    log "=== Phase 5: Starting continuous outage iterations ==="
fi

outage_node() {
    local ip="$1"
    local duration="$2"
    local method="$3"
    local node_uuid="$4"

    case "$method" in
        network)
            log "    Blocking network on $ip for ${duration}s..."
            run_remote "$ip" "sudo nohup bash -c 'iptables -A INPUT -j DROP; iptables -A OUTPUT -j DROP; sleep $duration; iptables -F' &>/dev/null &"
            sleep "$duration"
            # Safety: ensure iptables is flushed
            for _retry in 1 2 3 4 5; do
                run_remote "$ip" "sudo iptables -F" 2>&1 && break
                sleep 2
            done
            ;;
        shutdown)
            log "    Graceful shutdown $node_uuid on $ip..."
            sbctl -d sn shutdown "$node_uuid" --force 2>&1
            sleep "$duration"
            log "    Restarting $node_uuid..."
            for _attempt in 1 2 3 4 5; do
                sbctl -d sn restart "$node_uuid" --force 2>&1 && break
                log "    Restart attempt $_attempt failed, retrying in 15s..."
                sleep 15
            done
            ;;
    esac
}

ensure_all_nodes_online() {
    log "  Ensuring all nodes are online..."
    for uuid in "${NODE_LIST[@]}"; do
        ip="${NODE_IPS[$uuid]}"
        for attempt in $(seq 1 30); do
            node_status=$(sbctl sn list --json 2>/dev/null | python3 -c "
import json,sys
for n in json.load(sys.stdin):
    if n['UUID'] == '$uuid':
        print(n['Status']); break
" 2>&1 || echo "unknown")

            if [ "$node_status" = "online" ]; then
                break
            fi

            if [ "$attempt" -eq 1 ]; then
                log "    Node $ip ($uuid) status: $node_status — recovering..."
            fi

            if [ "$node_status" = "offline" ]; then
                sbctl -d sn restart "$uuid" --force 2>&1 || true
            elif [ "$node_status" = "unreachable" ]; then
                run_remote "$ip" "sudo iptables -F" 2>&1 || true
                sbctl -d sn shutdown "$uuid" --force 2>&1 || true
                sleep 3
                sbctl -d sn restart "$uuid" --force 2>&1 || true
            fi

            sleep 10
        done

        if [ "$node_status" != "online" ]; then
            log "  FATAL: Node $ip ($uuid) stuck in $node_status after recovery attempts"
            sbctl -d sn list 2>&1
            return 1
        fi
        log "    Node $ip: online"
    done
    return 0
}

wait_cluster_active() {
    log "  Waiting for cluster ACTIVE and rebalancing complete..."
    local cluster_id
    cluster_id=$(sbctl cluster list --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['UUID'])")
    for attempt in $(seq 1 180); do
        status=$(sbctl cluster list --json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['Status'])" || echo "unknown")
        rebalancing=$(sbctl cluster get "$cluster_id" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('is_re_balancing', False))" || echo "True")
        migrations=$(sbctl lvol migrate-list --json 2>/dev/null | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" || echo "0")
        if [ "$status" = "ACTIVE" ] && [ "$rebalancing" = "False" ] && [ "$migrations" = "0" ]; then
            log "  Cluster ACTIVE, rebalancing complete, no active migrations"
            return 0
        fi
        if [ "$((attempt % 10))" -eq 0 ]; then
            log "    Cluster status: $status, rebalancing: $rebalancing, migrations: $migrations (attempt $attempt)"
        fi
        sleep 5
    done
    log "  ERROR: Cluster not stable after 15 minutes: status=$status, rebalancing=$rebalancing"
    return 1
}

check_fio_alive() {
    local count
    count=$(run_client "pgrep -c fio" 2>/dev/null || echo "0")
    if [ "$count" -lt 1 ]; then
        log "  ERROR: No fio processes on client!"
        for i in $(seq 0 $((FIO_COUNT - 1))); do
            run_client "tail -5 /tmp/fio_${i}.log" 2>&1 || true
        done
        return 1
    fi
    log "  Fio alive ($count processes)"
    return 0
}

iter=0
while true; do
    iter=$((iter + 1))
    if [ "$ITERATIONS" -gt 0 ] && [ "$iter" -gt "$ITERATIONS" ]; then break; fi
    log ""
    if [ "$ITERATIONS" -gt 0 ]; then
        log "=== Iteration $iter/$ITERATIONS ==="
    else
        log "=== Iteration $iter (continuous) ==="
    fi

    # Pick 2 random distinct nodes
    idx1=$((RANDOM % NUM_NODES))
    idx2=$(( (idx1 + 1 + RANDOM % (NUM_NODES - 1)) % NUM_NODES ))
    node1="${NODE_LIST[$idx1]}"
    node2="${NODE_LIST[$idx2]}"
    ip1="${NODE_IPS[$node1]}"
    ip2="${NODE_IPS[$node2]}"

    # Outage method: shutdown only
    method1="shutdown"
    method2="shutdown"

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
    wait "$OUTAGE1_PID" 2>&1 || true
    wait "$OUTAGE2_PID" 2>&1 || true

    log "  Both outages resolved"

    # --- Recovery: ensure all nodes come back online ---
    if ! ensure_all_nodes_online; then
        log "  FATAL: Could not recover all nodes. Aborting."
        exit 1
    fi

    # --- Check fio on client ---
    if ! check_fio_alive; then
        log "  FAILURE: Fio died on client!"
        exit 1
    fi

    # --- Wait for cluster ACTIVE and not rebalancing ---
    if ! wait_cluster_active; then
        log "  FATAL: Cluster did not reach ACTIVE. Aborting."
        sbctl -d cluster list 2>&1
        sbctl -d sn list 2>&1
        exit 1
    fi

    sbctl -d sn list 2>&1
    if [ "$ITERATIONS" -gt 0 ]; then
        log "  Iteration $iter/$ITERATIONS complete"
    else
        log "  Iteration $iter complete"
    fi
    sleep 10
done

log ""
log "=== ALL $iter ITERATIONS PASSED ==="
log "Stopping fio on client..."
run_client "sudo pkill -f fio" 2>&1 || true
log "Done"
