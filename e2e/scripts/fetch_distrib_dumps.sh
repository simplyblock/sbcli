#!/usr/bin/env bash
#
# Fetch distrib placement-map dumps from ALL snode-spdk pods in a K8s cluster.
#
# Usage:
#   ./fetch_distrib_dumps.sh /mnt/nfs_share/my_test_logs
#   ./fetch_distrib_dumps.sh /mnt/nfs_share/my_test_logs simplyblock
#
# Arguments:
#   $1 - Output directory (required)
#   $2 - Kubernetes namespace (optional, default: simplyblock)

set -euo pipefail

OUTDIR="${1:?Usage: $0 <output_dir> [namespace]}"
NS="${2:-simplyblock}"

mkdir -p "$OUTDIR"

echo "=== Fetching distrib dumps from all snode-spdk pods in namespace '$NS' ==="
echo "=== Output directory: $OUTDIR ==="

# Discover all snode-spdk pods
PODS=$(kubectl get pods -n "$NS" --no-headers -o custom-columns=:metadata.name | grep snode-spdk || true)

if [ -z "$PODS" ]; then
    echo "ERROR: No snode-spdk pods found in namespace '$NS'"
    exit 1
fi

echo "Found pods:"
echo "$PODS"
echo ""

for POD in $PODS; do
    echo "─── Processing pod: $POD ───"

    POD_OUTDIR="$OUTDIR/$POD/distrib_logs"
    mkdir -p "$POD_OUTDIR"

    # Find spdk.sock
    SOCK=$(kubectl exec "$POD" -c spdk-container -n "$NS" -- \
        bash -c 'find /mnt/ramdisk -name spdk.sock -maxdepth 3 2>/dev/null | head -1' 2>/dev/null || true)

    if [ -z "$SOCK" ]; then
        echo "  WARN: spdk.sock not found in $POD, skipping."
        continue
    fi
    echo "  Socket: $SOCK"

    # Get distrib bdev names
    BDEV_JSON=$(kubectl exec "$POD" -c spdk-container -n "$NS" -- \
        python spdk/scripts/rpc.py -s "$SOCK" bdev_get_bdevs 2>/dev/null || true)

    if [ -z "$BDEV_JSON" ]; then
        echo "  WARN: bdev_get_bdevs returned empty for $POD, skipping."
        continue
    fi

    DISTRIBS=$(echo "$BDEV_JSON" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for b in data:
    name = b.get('name', '')
    if name.startswith('distrib_'):
        print(name)
" 2>/dev/null | sort -u || true)

    if [ -z "$DISTRIBS" ]; then
        echo "  WARN: No distrib_* bdevs found on $POD."
        continue
    fi

    echo "  Distribs: $(echo $DISTRIBS | tr '\n' ' ')"

    for DISTRIB in $DISTRIBS; do
        echo "    Dumping $DISTRIB ..."

        STACK_FILE="/tmp/stack_${DISTRIB}.json"
        RPC_LOG="/tmp/rpc_${DISTRIB}.log"

        # Create JSON config and run rpc_sock.py inside the container
        JSON_CFG="{\"subsystems\":[{\"subsystem\":\"distr\",\"config\":[{\"method\":\"distr_debug_placement_map_dump\",\"params\":{\"name\":\"${DISTRIB}\"}}]}]}"

        kubectl exec "$POD" -c spdk-container -n "$NS" -- \
            bash -c "echo '${JSON_CFG}' > ${STACK_FILE} && python scripts/rpc_sock.py ${STACK_FILE} ${SOCK} > ${RPC_LOG} 2>&1 || true" \
            2>/dev/null

        # Copy the RPC log out
        kubectl cp -n "$NS" "${POD}:${RPC_LOG}" -c spdk-container "${POD_OUTDIR}/rpc_${DISTRIB}.log" 2>/dev/null || true

        # Copy any /tmp files matching this distrib name
        FILES=$(kubectl exec "$POD" -c spdk-container -n "$NS" -- \
            bash -c "ls /tmp/ 2>/dev/null | grep -F '${DISTRIB}' || true" 2>/dev/null || true)

        for FNAME in $FILES; do
            FNAME=$(echo "$FNAME" | tr -d '[:space:]')
            [ -z "$FNAME" ] && continue
            kubectl cp -n "$NS" "${POD}:/tmp/${FNAME}" -c spdk-container "${POD_OUTDIR}/${FNAME}" 2>/dev/null || true
            echo "      Copied /tmp/${FNAME}"
        done

        # Cleanup temp files in container
        kubectl exec "$POD" -c spdk-container -n "$NS" -- \
            bash -c "rm -f ${STACK_FILE} ${RPC_LOG}" 2>/dev/null || true
    done

    echo "  Done. Files in: $POD_OUTDIR"
    echo ""
done

echo "=== All distrib dumps saved to: $OUTDIR ==="
