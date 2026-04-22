#!/bin/bash
# DH-CHAP / TLS Lab Deployment Script
# Run from jump host or mgmt node
# Usage: bash deploy_dhchap_lab.sh [branch]
set -uo pipefail

BRANCH="${1:-main}"
MGMT=192.168.10.211
STORAGE_NODES=(192.168.10.205 192.168.10.206 192.168.10.207 192.168.10.208)
SN_PASS="3tango11"

# Docker Hub images (avoids ECR rate limits)
SBCLI_IMAGE="simplyblock/simplyblock:main"
SPDK_IMAGE="simplyblock/spdk:main-latest"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

run_mgmt() {
    sshpass -p "$SN_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "root@$MGMT" "$@"
}

run_remote() {
    local ip="$1"; shift
    sshpass -p "$SN_PASS" ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 "root@$ip" "$@"
}

log "=== DH-CHAP Lab Deployment ==="
log "Branch: $BRANCH"
log "Mgmt: $MGMT, Storage: ${STORAGE_NODES[*]}"

# Step 1: Full cleanup on all nodes
log "Step 1: deploy-cleaner on all nodes"
run_mgmt "sbctl sn deploy-cleaner 2>&1 | tail -1" &
for ip in "${STORAGE_NODES[@]}"; do
    run_remote "$ip" "sbctl sn deploy-cleaner 2>&1 | tail -1" &
done
wait
log "  deploy-cleaner done"

# Step 2: Docker cleanup on mgmt AND all storage nodes
log "Step 2: Docker cleanup on all nodes"
run_mgmt 'docker rm -f $(docker ps -aq) 2>/dev/null; docker system prune -af --volumes 2>&1 | tail -1'
for ip in "${STORAGE_NODES[@]}"; do
    run_remote "$ip" 'docker rm -f $(docker ps -aq) 2>/dev/null; docker system prune -af --volumes 2>&1 | tail -1' &
done
wait
log "  Docker cleanup done"

# Step 3: pip install on all nodes
log "Step 3: pip install on all nodes"
run_mgmt "pip install git+https://github.com/simplyblock/sbcli@$BRANCH --upgrade --force 2>&1 | tail -1" &
for ip in "${STORAGE_NODES[@]}"; do
    run_remote "$ip" "pip install git+https://github.com/simplyblock/sbcli@$BRANCH --upgrade --force 2>&1 | tail -1" &
done
wait
log "  pip install done"

# Step 4: Patch env_var on mgmt to use Docker Hub images
log "Step 4: Patch env_var for Docker Hub images"
run_mgmt "
ENV_FILE=\$(python3 -c 'import simplyblock_core; import os; print(os.path.join(os.path.dirname(simplyblock_core.__file__), \"env_var\"))')
sed -i 's|public.ecr.aws/simply-block/simplyblock:[^ ]*|${SBCLI_IMAGE}|g' \$ENV_FILE
sed -i 's|public.ecr.aws/simply-block/ultra:[^ ]*|${SPDK_IMAGE}|g' \$ENV_FILE
cat \$ENV_FILE
"
log "  env_var patched"

# Step 5: Reboot storage nodes to rebind NVMe devices to kernel driver
log "Step 5: Reboot storage nodes"
for ip in "${STORAGE_NODES[@]}"; do
    run_remote "$ip" "reboot" 2>/dev/null &
done
wait
log "  Reboot commands sent, waiting 90s..."
sleep 90

# Verify nodes are up
for ip in "${STORAGE_NODES[@]}"; do
    while ! run_remote "$ip" "uptime" &>/dev/null; do
        log "  Waiting for $ip..."
        sleep 10
    done
    log "  $ip is up"
done

# Step 6: Wipe NVMe partitions and configure storage nodes
log "Step 6: Wipe partitions + configure storage nodes"
for ip in "${STORAGE_NODES[@]}"; do
    log "  Configuring $ip..."
    run_remote "$ip" "
        mkdir -p /etc/simplyblock
        # Wipe any existing partition tables on NVMe devices
        for dev in \$(lsblk -d -o NAME | grep nvme); do
            wipefs -af /dev/\$dev 2>/dev/null
        done
        # Write clean config file
        echo '{}' > /etc/simplyblock/sn_config_file
        sbctl sn configure --max-lvol 10 2>&1 | tail -3
    " &
done
wait
log "  All configured"

# Step 7: Deploy storage nodes
log "Step 7: Deploy storage nodes"
for ip in "${STORAGE_NODES[@]}"; do
    log "  Deploying $ip..."
    run_remote "$ip" "
        export SIMPLY_BLOCK_DOCKER_IMAGE=${SBCLI_IMAGE}
        export SIMPLY_BLOCK_SPDK_ULTRA_IMAGE=${SPDK_IMAGE}
        sbctl sn deploy --isolate-cores --ifname eth0 2>&1 | tail -3
    " &
done
wait
log "  All deployed"

# Step 8: Create dhchap config and cluster
log "Step 8: Create cluster with DH-CHAP security"
run_mgmt "
cat > /tmp/dhchap.json << 'EOFJ'
{\"dhchap_digests\": [\"sha256\", \"sha384\", \"sha512\"], \"dhchap_dhgroups\": [\"ffdhe2048\", \"ffdhe3072\", \"ffdhe4096\", \"ffdhe6144\", \"ffdhe8192\"]}
EOFJ
"
CLUSTER_ID=$(run_mgmt "sbctl cluster create --ha-type ha --parity-chunks-per-stripe 2 --host-sec /tmp/dhchap.json 2>&1 | tail -1")
if [ -z "$CLUSTER_ID" ] || [[ "$CLUSTER_ID" == *"error"* ]] || [[ "$CLUSTER_ID" == *"failed"* ]]; then
    log "ERROR: Cluster create failed: $CLUSTER_ID"
    exit 1
fi
log "  Cluster ID: $CLUSTER_ID"

# Step 9: Patch env_var again (pip install in step 3 may have overwritten it)
log "Step 9: Re-patch env_var after cluster create"
run_mgmt "
ENV_FILE=\$(python3 -c 'import simplyblock_core; import os; print(os.path.join(os.path.dirname(simplyblock_core.__file__), \"env_var\"))')
sed -i 's|public.ecr.aws/simply-block/simplyblock:[^ ]*|${SBCLI_IMAGE}|g' \$ENV_FILE
sed -i 's|public.ecr.aws/simply-block/ultra:[^ ]*|${SPDK_IMAGE}|g' \$ENV_FILE
"

# Step 10: Add storage nodes
log "Step 10: Add storage nodes"
NODES_ADDED=0
for ip in "${STORAGE_NODES[@]}"; do
    log "  Adding $ip..."
    result=$(run_mgmt "sbctl sn add-node $CLUSTER_ID $ip:5000 eth0 --data-nics eth1 --ha-jm-count 4 2>&1")
    echo "$result" | tail -3
    if echo "$result" | grep -q "Success"; then
        NODES_ADDED=$((NODES_ADDED + 1))
    else
        log "  ERROR: Failed to add $ip"
    fi
done

if [ "$NODES_ADDED" -ne "${#STORAGE_NODES[@]}" ]; then
    log "FATAL: Only $NODES_ADDED/${#STORAGE_NODES[@]} nodes added. Aborting."
    exit 1
fi

# Step 11: Activate
log "Step 11: Activate cluster"
run_mgmt "sbctl cluster activate $CLUSTER_ID 2>&1 | tail -5"

# Step 12: Create pool (no security)
log "Step 12: Create pool"
run_mgmt "sbctl pool add pool01 $CLUSTER_ID 2>&1 | tail -3"

# Step 13: Verify
log "=== Verification ==="
run_mgmt "sbctl cluster list"
run_mgmt "sbctl sn list"
run_mgmt "sbctl pool list"

log "=== Deployment Complete ==="
log "Cluster ID: $CLUSTER_ID"
