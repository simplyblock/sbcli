#!/bin/bash
# FT=2 Lab Deployment Script
# Run from .111 (mgmt node) directly
# Usage: bash deploy_ft2_lab.sh [branch]
set -uo pipefail

BRANCH="${1:-feature-lvol-migration}"
MGMT=192.168.10.111
SN1=192.168.10.112
SN2=192.168.10.113
SN3=192.168.10.114
SN4=192.168.10.115
STORAGE_NODES=("$SN1" "$SN2" "$SN3" "$SN4")

log() { echo "[$(date '+%H:%M:%S')] $*"; }

SN_PASS="3tango11"

run_remote() {
    local ip="$1"; shift
    sshpass -p "$SN_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "root@$ip" "$@"
}

log "=== FT=2 Lab Deployment ==="
log "Branch: $BRANCH"
log "Mgmt: $MGMT, Storage: ${STORAGE_NODES[*]}"

# Step 1: pip install on all nodes
log "Step 1: pip install on all nodes"
log "  Installing on $MGMT (local)..."
pip install "git+https://github.com/simplyblock-io/sbcli@$BRANCH" --upgrade --force 2>&1 | tail -1 &
for ip in "${STORAGE_NODES[@]}"; do
    log "  Installing on $ip..."
    run_remote "$ip" "pip install git+https://github.com/simplyblock-io/sbcli@$BRANCH --upgrade --force 2>&1 | tail -1" &
done
wait
log "  pip install done"

# Step 2: deploy-cleaner on all nodes
log "Step 2: deploy-cleaner on all nodes"
log "  Cleaning $MGMT (local)..."
sbctl sn deploy-cleaner 2>&1 | tail -1 &
for ip in "${STORAGE_NODES[@]}"; do
    log "  Cleaning $ip..."
    run_remote "$ip" "sbctl sn deploy-cleaner 2>&1 | tail -1" &
done
wait
log "  deploy-cleaner done"

# Step 3: Docker cleanup on mgmt
log "Step 3: Docker cleanup on mgmt"
docker rm -f $(docker ps -aq) 2>/dev/null || true
docker system prune -af --volumes 2>&1 | tail -1
log "  Docker cleanup done"

# Step 4: Configure + deploy storage nodes (parallel)
log "Step 4: Configure + deploy storage nodes (parallel with cluster create)"
for ip in "${STORAGE_NODES[@]}"; do
    log "  Configuring + deploying $ip..."
    run_remote "$ip" "sbctl sn configure --max-lvol 10 2>&1 | tail -1 && sbctl sn deploy --isolate-cores --ifname eth0 2>&1 | tail -3" &
done

# Step 5: Cluster create (parallel with step 4)
log "Step 5: Cluster create (ha, FT=2, npcs=2)"
CLUSTER_ID=$(sbctl cluster create --ha-type ha --parity-chunks-per-stripe 2 2>&1 | tail -1)
if [ -z "$CLUSTER_ID" ] || [[ "$CLUSTER_ID" == *"error"* ]]; then
    log "ERROR: Cluster create failed: $CLUSTER_ID"
    exit 1
fi
log "  Cluster ID: $CLUSTER_ID"

# Wait for storage deploys
wait
log "  All storage nodes deployed"

# Step 6: Add nodes with ha-jm-count=4
log "Step 6: Add nodes (ha-jm-count=4)"
for ip in "${STORAGE_NODES[@]}"; do
    log "  Adding $ip..."
    sbctl sn add-node "$CLUSTER_ID" "$ip:5000" eth0 --journal-partition 0 --data-nics eth1 --ha-jm-count 4 2>&1 | tail -3
done

# Step 7: Activate
log "Step 7: Activate cluster"
sbctl cluster activate "$CLUSTER_ID" 2>&1 | tail -3

# Step 8: Create pool
log "Step 8: Create pool"
sbctl pool add pool01 "$CLUSTER_ID" 2>&1 | tail -1

# Step 9: Verify
log "=== Verification ==="
sbctl cluster list
sbctl sn list
sbctl pool list 2>/dev/null || true

# Verify ha_jm_count=4
log "Checking ha_jm_count..."
for ip in "${STORAGE_NODES[@]}"; do
    JM_COUNT=$(sbctl sn list 2>/dev/null | head -1)  # placeholder
done
NODE_ID=$(sbctl sn list 2>&1 | grep online | head -1 | awk -F'|' '{print $2}' | tr -d ' ')
if [ -n "$NODE_ID" ]; then
    HA_JM=$(sbctl sn get "$NODE_ID" 2>&1 | grep ha_jm_count | awk -F: '{print $2}' | tr -d ' ,')
    log "  ha_jm_count=$HA_JM (expected 4)"
    if [ "$HA_JM" != "4" ]; then
        log "  WARNING: ha_jm_count is not 4! FT=2 dual-node failure will lose journal quorum!"
    fi
fi

log "=== Deployment Complete ==="
log "Cluster ID: $CLUSTER_ID"
