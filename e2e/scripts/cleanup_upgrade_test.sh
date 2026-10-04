#!/usr/bin/env bash
# cleanup_upgrade_test.sh — Full cleanup of simplyblock K8s deployment
#
# Comprehensive 12-phase cleanup that handles all environments (Talos, OpenShift,
# generic K8s). Cleans up everything so the next test run starts completely fresh.
# Handles: Helm releases (R25 sbcli/spdk-csi + R26 operator), CRs, CRDs,
# kube-system leftovers, cert-manager, PVCs, PVs, snapshots, NVMe, hugepages,
# node labels, CSI hostpath data, and namespaces.
#
# Auto-detects Talos clusters and skips node debug operations (immutable OS).
# Auto-detects worker nodes if not provided via -w.
#
# Usage:
#   ./cleanup_upgrade_test.sh [OPTIONS]
#
# Options:
#   -n NAMESPACE       Namespace (default: simplyblock)
#   -e ENVIRONMENT     Cluster environment: local|openshift-baremetal|openshift-local|aws-openshift|gcp
#                      (default: local)
#   -w WORKER_NODES    Comma-separated worker node names
#                      (default: auto-detected from cluster)
#   -c CRD_DIR         Path to operator CRDs directory for deletion
#                      (default: searches common locations)
#   -h                 Show this help message
#
# Examples:
#   # Auto-detect everything
#   ./cleanup_upgrade_test.sh
#
#   # Custom namespace and environment
#   ./cleanup_upgrade_test.sh -n simplyblock -e local -w "node1,node2,node3"
#
#   # OpenShift cluster with CRD directory
#   ./cleanup_upgrade_test.sh -e openshift-baremetal -c /path/to/crds/

set +e  # Don't exit on errors — cleanup must be best-effort

# ── Parse arguments ──
NAMESPACE="simplyblock"
CLUSTER_ENV="local"
WORKER_NODES=""
CRD_DIR=""

while getopts "n:e:w:c:h" opt; do
  case $opt in
    n) NAMESPACE="$OPTARG" ;;
    e) CLUSTER_ENV="$OPTARG" ;;
    w) WORKER_NODES="$OPTARG" ;;
    c) CRD_DIR="$OPTARG" ;;
    h)
      head -30 "$0" | grep '^#' | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: -$opt" >&2; exit 1 ;;
  esac
done

KUBECTL_TIMEOUT="--request-timeout=120s"

# Auto-detect Talos clusters
IS_TALOS="false"
if kubectl get nodes -o jsonpath='{.items[0].status.nodeInfo.osImage}' 2>/dev/null | grep -q "Talos"; then
  IS_TALOS="true"
fi

# Auto-detect worker nodes if not provided
if [ -z "$WORKER_NODES" ]; then
  WORKER_NODES=$(kubectl get nodes --no-headers -o custom-columns=:metadata.name 2>/dev/null | tr '\n' ',')
  WORKER_NODES="${WORKER_NODES%,}"  # Remove trailing comma
fi

echo "============================================================"
echo "  SimplyBlock Full Cleanup"
echo "  Namespace:   $NAMESPACE"
echo "  Environment: $CLUSTER_ENV"
echo "  Talos:       $IS_TALOS"
echo "  Workers:     $WORKER_NODES"
echo "============================================================"
echo ""

# Helper: retry a command up to N times with backoff
retry_cmd() {
  local max_attempts=$1
  shift
  local attempt=1
  while [ $attempt -le $max_attempts ]; do
    if "$@" 2>/dev/null; then
      return 0
    fi
    echo "  Attempt $attempt/$max_attempts failed, retrying in 5s..."
    sleep 5
    attempt=$((attempt + 1))
  done
  echo "  All $max_attempts attempts failed for: $*"
  return 1
}

# ══════════════════════════════════════════════════════════════════
# Phase 1: Helm uninstall (R25 + R26 releases)
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 1: Uninstall all Helm releases (R25 + R26) ==="

# R25 charts (explicit names)
echo "  Uninstalling R25 charts (spdk-csi, sbcli)..."
helm uninstall spdk-csi -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true
helm uninstall sbcli -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true

# R26 charts / any remaining releases
echo "  Uninstalling any remaining Helm releases in $NAMESPACE..."
for rel in $(helm list -n $NAMESPACE -q 2>/dev/null); do
  echo "    Uninstalling: $rel"
  helm uninstall "$rel" -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true
done

# cert-manager
echo "  Uninstalling cert-manager..."
helm uninstall cert-manager -n cert-manager --no-hooks --timeout 60s 2>/dev/null || true

echo "  Phase 1 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 2: Delete kube-system resources
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 2: Delete simplyblock resources in kube-system ==="

# snapshot-controller and other simplyblock resources survive helm uninstall
# due to helm.sh/resource-policy: keep
for RTYPE in deployment daemonset service sa configmap; do
  for NAME in $(kubectl -n kube-system $KUBECTL_TIMEOUT get $RTYPE --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
    echo "  Deleting kube-system $RTYPE/$NAME"
    kubectl -n kube-system $KUBECTL_TIMEOUT delete $RTYPE "$NAME" --ignore-not-found 2>/dev/null || true
  done
done

# Specifically clean numa-resource-plugin resources
kubectl -n kube-system $KUBECTL_TIMEOUT delete ds simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system $KUBECTL_TIMEOUT delete sa simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system $KUBECTL_TIMEOUT delete cm simplyblock-numa-resource-plugin-config --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrole simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrolebinding simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true

echo "  Phase 2 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 3: Delete CRs (patch finalizers first)
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 3: Delete all simplyblock CRs ==="

# All known CR types (both old and new API group names)
CR_TYPES=(
  "simplyblockpool.storage.simplyblock.io"
  "simplyblocklvol.storage.simplyblock.io"
  "simplyblocktask.storage.simplyblock.io"
  "simplyblockdevices.storage.simplyblock.io"
  "simplyblockstoragenodes.storage.simplyblock.io"
  "simplyblockstoragenodesets.storage.simplyblock.io"
  "simplyblockstoragenodeops.storage.simplyblock.io"
  "simplyblockstorageclusters.storage.simplyblock.io"
  "simplyblocksnapshotreplications.storage.simplyblock.io"
  "pool.storage.simplyblock.io"
  # storagepools is the post-rename name (Pool -> StoragePool). Without it
  # pools survive every cleanup, and the next run's add_storage_pool() finds
  # and reuses a leftover pool instead of creating the one it asked for.
  "storagepools.storage.simplyblock.io"
  "storagepool.storage.simplyblock.io"
  "lvol.storage.simplyblock.io"
  "task.storage.simplyblock.io"
  "devices.storage.simplyblock.io"
  "storagenodes.storage.simplyblock.io"
  "storagenodesets.storage.simplyblock.io"
  "storagenodeops.storage.simplyblock.io"
  "storageclusters.storage.simplyblock.io"
  "snapshotreplications.storage.simplyblock.io"
  "storagebackups.storage.simplyblock.io"
  "backuprestores.storage.simplyblock.io"
  "backuppolicies.storage.simplyblock.io"
  "backupimports.storage.simplyblock.io"
)

for CR_TYPE in "${CR_TYPES[@]}"; do
  for CR_NAME in $(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get "$CR_TYPE" --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch "$CR_TYPE" "$CR_NAME" \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete "$CR_TYPE" "$CR_NAME" \
      --ignore-not-found --wait=false 2>/dev/null || true
  done
done

# Also handle FDB CRs (from R25 sbcli chart)
for FDB_TYPE in foundationdbcluster foundationdbbackup foundationdbrestore; do
  for CR_NAME in $(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get "$FDB_TYPE" --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch "$FDB_TYPE" "$CR_NAME" \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete "$FDB_TYPE" "$CR_NAME" \
      --ignore-not-found --wait=false 2>/dev/null || true
  done
done

echo "  Phase 3 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 4: Delete VolumeSnapshots, PVCs, PVs
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 4: Delete VolumeSnapshots, PVCs, PVs ==="

# VolumeSnapshots
echo "  Bulk deleting VolumeSnapshots..."
retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete volumesnapshot --all --wait=false

VS_REMAINING=$(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get volumesnapshot --no-headers 2>/dev/null | wc -l)
if [ "${VS_REMAINING:-0}" -gt 0 ]; then
  echo "  $VS_REMAINING VolumeSnapshots stuck, patching finalizers..."
  kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get volumesnapshot --no-headers \
    -o custom-columns=:metadata.name 2>/dev/null | \
    xargs -P 20 -I {} kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch volumesnapshot {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete volumesnapshot --all \
    --force --grace-period=0 --wait=false
fi

# VolumeSnapshotContents (cluster-scoped)
echo "  Bulk deleting VolumeSnapshotContents..."
retry_cmd 3 kubectl $KUBECTL_TIMEOUT delete volumesnapshotcontent --all --wait=false

VSC_REMAINING=$(kubectl $KUBECTL_TIMEOUT get volumesnapshotcontent --no-headers 2>/dev/null | wc -l)
if [ "${VSC_REMAINING:-0}" -gt 0 ]; then
  echo "  $VSC_REMAINING VolumeSnapshotContents stuck, patching finalizers..."
  kubectl $KUBECTL_TIMEOUT get volumesnapshotcontent --no-headers \
    -o custom-columns=:metadata.name 2>/dev/null | \
    xargs -P 20 -I {} kubectl $KUBECTL_TIMEOUT patch volumesnapshotcontent {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl $KUBECTL_TIMEOUT delete volumesnapshotcontent --all \
    --force --grace-period=0 --wait=false
fi

# VolumeSnapshotClasses
for VSCLASS in $(kubectl $KUBECTL_TIMEOUT get volumesnapshotclass --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete volumesnapshotclass "$VSCLASS" --ignore-not-found 2>/dev/null || true
done

# PVCs
echo "  Deleting PVCs..."
retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete pvc --all --wait=false
sleep 10

STUCK_PVCS=$(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get pvc --no-headers -o custom-columns=:metadata.name 2>/dev/null)
if [ -n "$STUCK_PVCS" ]; then
  echo "  Patching finalizers on stuck PVCs..."
  echo "$STUCK_PVCS" | xargs -P 20 -I {} \
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch pvc {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete pvc --all \
    --force --grace-period=0 --wait=false
fi

# PVs (skip vault PVs)
echo "  Deleting PVs..."
PV_LIST=$(kubectl $KUBECTL_TIMEOUT get pv --no-headers -o custom-columns=NAME:.metadata.name,CLAIM:.spec.claimRef.namespace 2>/dev/null \
  | grep -v -E '\bvault\b' 2>/dev/null | awk '{print $1}')
if [ -n "$PV_LIST" ]; then
  echo "$PV_LIST" | xargs -P 20 -I {} \
    kubectl $KUBECTL_TIMEOUT patch pv {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  echo "$PV_LIST" | xargs -P 20 -I {} \
    kubectl $KUBECTL_TIMEOUT delete pv {} --force --grace-period=0 --wait=false 2>/dev/null
fi

# Also delete simplyblock-provisioned PVs
for pv in $(kubectl get pv --no-headers 2>/dev/null | grep 'simplyblock/' | awk '{print $1}'); do
  kubectl delete pv "$pv" --ignore-not-found 2>/dev/null || true
done

echo "  Phase 4 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 5: Force delete remaining namespaced resources
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 5: Force delete remaining namespaced resources ==="

for RTYPE in pod jobs service ds statefulset deployment replicaset secret sa configmap; do
  kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete $RTYPE --all --force --grace-period=0 2>/dev/null || true
done

echo "  Phase 5 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 6: Cleanup cluster-scoped resources
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 6: Delete cluster-scoped resources ==="

# StorageClasses — select by PROVISIONER, not by name. Tests create classes
# named sc-bck-*, sc-comp-*, sc-<pvc> etc., none of which contain
# "simplyblock", so a name filter leaves them behind to accumulate (53 found
# on one cluster, oldest 5 days).
for SC in $(kubectl $KUBECTL_TIMEOUT get sc --no-headers \
      -o custom-columns=:metadata.name,:provisioner 2>/dev/null \
      | awk '$2 == "csi.simplyblock.io" { print $1 }' 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete sc "$SC" --ignore-not-found 2>/dev/null || true
done
# Catch any simplyblock-named class whose provisioner differs (e.g. hostpath)
for SC in $(kubectl $KUBECTL_TIMEOUT get sc --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete sc "$SC" --ignore-not-found 2>/dev/null || true
done

# ClusterRoles and ClusterRoleBindings
kubectl $KUBECTL_TIMEOUT delete clusterrole simplyblock-storage-node-role --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrolebinding simplyblock-storage-node-binding --ignore-not-found 2>/dev/null || true

for RES in clusterrole clusterrolebinding; do
  for NAME in $(kubectl $KUBECTL_TIMEOUT get $RES --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
    kubectl $KUBECTL_TIMEOUT delete "$RES" "$NAME" --ignore-not-found 2>/dev/null || true
  done
  # Also catch FDB-related cluster roles
  for NAME in $(kubectl $KUBECTL_TIMEOUT get $RES --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i "fdb-manager" 2>/dev/null); do
    kubectl $KUBECTL_TIMEOUT delete "$RES" "$NAME" --ignore-not-found 2>/dev/null || true
  done
done

# Webhook configurations
for WH in $(kubectl $KUBECTL_TIMEOUT get mutatingwebhookconfiguration --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete mutatingwebhookconfiguration "$WH" --ignore-not-found 2>/dev/null || true
done
for WH in $(kubectl $KUBECTL_TIMEOUT get validatingwebhookconfiguration --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete validatingwebhookconfiguration "$WH" --ignore-not-found 2>/dev/null || true
done

echo "  Phase 6 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 7: Delete CRDs
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 7: Delete CRDs ==="

# Clear finalizers on all CR instances first (so CRD deletion doesn't hang)
for crd in $(kubectl get crd -o name 2>/dev/null | grep simplyblock); do
  crd_name="${crd#customresourcedefinition.apiextensions.k8s.io/}"
  for cr_info in $(kubectl get "$crd_name" -A --no-headers -o custom-columns=NS:.metadata.namespace,NAME:.metadata.name 2>/dev/null); do
    IFS=' ' read -r ns name <<< "$cr_info"
    kubectl patch "$crd_name" "$name" -n "$ns" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  done
  kubectl patch crd "$crd_name" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done

# Delete CRDs from operator directory if available
if [ -n "$CRD_DIR" ] && [ -d "$CRD_DIR" ]; then
  kubectl delete -f "$CRD_DIR" --ignore-not-found --timeout=60s 2>/dev/null || true
fi

# Force-delete any remaining simplyblock CRDs
for crd in $(kubectl get crd -o name 2>/dev/null | grep simplyblock); do
  crd_name="${crd#customresourcedefinition.apiextensions.k8s.io/}"
  kubectl patch crd "$crd_name" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl delete crd "$crd_name" --ignore-not-found --timeout=30s 2>/dev/null || true
done

# Delete FDB CRDs
for crd in $(kubectl get crd -o name 2>/dev/null | grep foundationdb); do
  crd_name="${crd#customresourcedefinition.apiextensions.k8s.io/}"
  kubectl patch crd "$crd_name" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl delete crd "$crd_name" --ignore-not-found --timeout=30s 2>/dev/null || true
done

# Delete cert-manager CRDs
kubectl delete crd certificaterequests.cert-manager.io certificates.cert-manager.io \
  challenges.acme.cert-manager.io clusterissuers.cert-manager.io \
  issuers.cert-manager.io orders.acme.cert-manager.io \
  --ignore-not-found --force --grace-period=0 2>/dev/null || true

echo "  Phase 7 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 8: Delete + finalize namespaces
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 8: Delete namespaces ==="

kubectl delete namespace $NAMESPACE --force --grace-period=0 --wait=false 2>/dev/null || true
kubectl delete namespace cert-manager --force --grace-period=0 --wait=false 2>/dev/null || true

# Wait for namespace deletion with force-finalize
for i in $(seq 1 12); do
  REMAINING=0
  for ns in $NAMESPACE cert-manager; do
    if kubectl get namespace $ns &>/dev/null; then
      REMAINING=$((REMAINING + 1))
      echo "  Namespace $ns still terminating ($i/12), force-finalizing..."
      kubectl get namespace $ns -o json 2>/dev/null \
        | jq '.spec.finalizers = []' \
        | kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
    fi
  done
  [ "$REMAINING" -eq 0 ] && echo "  All namespaces deleted" && break
  sleep 5
done

echo "  Phase 8 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 9: Node-level cleanup (NVMe, hugepages, kubelet)
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 9: NVMe disconnect, hugepages reset, kubelet restart ==="

IFS=',' read -ra NODES <<< "$WORKER_NODES"

if [ "$IS_TALOS" = "true" ]; then
  echo "  Talos cluster detected — skipping node debug operations (immutable OS)"
else
  for NODE in "${NODES[@]}"; do
    (
      echo "  Cleaning node: $NODE"
      if [[ "$CLUSTER_ENV" == *"openshift"* ]]; then
        timeout 90 oc debug node/"$NODE" -- chroot /host bash -c \
          "nvme disconnect-all 2>/dev/null; rm -rf /etc/simplyblock 2>/dev/null; echo 0 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages && systemctl restart kubelet" 2>/dev/null || true
      else
        timeout 90 kubectl debug node/"$NODE" -q --image=busybox:latest -- chroot /host sh -c \
          "nvme disconnect-all 2>/dev/null; rm -rf /etc/simplyblock 2>/dev/null; echo 0 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages && systemctl restart kubelet" 2>/dev/null || true
      fi
      echo "  Done: $NODE"
    ) &
  done
  wait
fi

echo "  Phase 9 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 10: Remove stale node labels
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 10: Remove stale node labels ==="

# Every node, not just $WORKER_NODES. These labels are set on whichever node
# a past run happened to name, and a node that has since dropped out of the
# worker list keeps them forever. A control-plane node left carrying
# io.simplyblock.node-type=simplyblock-storage-plane silently rejoins the
# storage plane on the next deploy, because simplyblock-storage-node-ds
# selects on that label alone.
ALL_NODES_CSV=$(kubectl get nodes --no-headers -o custom-columns=:metadata.name 2>/dev/null | tr '\n' ',')
ALL_NODES_CSV="${ALL_NODES_CSV%,}"
IFS=',' read -ra LABEL_NODES <<< "$ALL_NODES_CSV"
if [ ${#LABEL_NODES[@]} -eq 0 ]; then
  LABEL_NODES=("${NODES[@]}")
fi
echo "  Stripping labels from: ${LABEL_NODES[*]}"

for NODE in "${LABEL_NODES[@]}"; do
  kubectl label node "$NODE" io.simplyblock.storagenodeset- 2>/dev/null || true
  kubectl label node "$NODE" io.simplyblock.node-type- 2>/dev/null || true
  kubectl label node "$NODE" simplyblock.io/role- 2>/dev/null || true

  # simplyblock.io/storage-node-uuid.<clusterUUID>.<idx> and
  # simplyblock.io/pool.<ns>.<cluster>.<pool> carry a cluster/pool identifier
  # in the key itself, so each deployment adds a NEW key and the old ones are
  # never overwritten. Left behind they accumulate one set per cluster
  # redeploy, and the CSI node driver — which snapshots the node's
  # simplyblock.io/* labels as its topology keys at registration — then
  # advertises topology for clusters that no longer exist. Strip every one by
  # prefix rather than by name.
  STALE_LABELS=$(kubectl get node "$NODE" -o json 2>/dev/null \
    | python3 -c "
import json,sys
try:
    labels = json.load(sys.stdin).get('metadata', {}).get('labels', {}) or {}
except Exception:
    labels = {}
for k in labels:
    if k.startswith('simplyblock.io/storage-node-uuid.') or k.startswith('simplyblock.io/pool.'):
        print(k)
" 2>/dev/null || true)

  for LBL in $STALE_LABELS; do
    kubectl label node "$NODE" "${LBL}-" 2>/dev/null || true
    echo "    stripped $LBL"
  done
  echo "  Removed labels from $NODE"
done

echo "  Phase 10 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 11: Cleanup stale CSI hostpath data
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 11: Cleanup stale CSI hostpath data ==="

if [ "$IS_TALOS" = "true" ]; then
  echo "  Talos cluster detected — skipping CSI hostpath cleanup (immutable OS)"
else
  for NODE in "${NODES[@]}"; do
    echo "  Cleaning CSI hostpath data on $NODE..."
    if [[ "$CLUSTER_ENV" == *"openshift"* ]]; then
      oc debug node/"$NODE" -- chroot /host bash -c \
        "find /var/lib/csi-hostpath-data -mindepth 1 -maxdepth 1 -type d -mtime +2 -exec rm -rf {} \;" 2>/dev/null || true
    else
      kubectl debug node/"$NODE" -q --image=busybox:latest -- chroot /host sh -c \
        "find /var/lib/csi-hostpath-data -mindepth 1 -maxdepth 1 -type d -mtime +2 -exec rm -rf {} \;" 2>/dev/null || true
    fi
  done
fi

echo "  Phase 11 complete."
echo ""

# ══════════════════════════════════════════════════════════════════
# Phase 12: Final verification
# ══════════════════════════════════════════════════════════════════
echo "=== Phase 12: Final verification ==="

echo "  Namespaces:"
kubectl get namespace $NAMESPACE 2>/dev/null && echo "  WARNING: $NAMESPACE still exists!" || echo "  $NAMESPACE: gone"
kubectl get namespace cert-manager 2>/dev/null && echo "  WARNING: cert-manager still exists!" || echo "  cert-manager: gone"

echo ""
echo "  Helm releases:"
helm list -n $NAMESPACE 2>/dev/null || echo "  None"

echo ""
echo "  SimplyBlock CRDs:"
kubectl get crd 2>/dev/null | grep -i simplyblock || echo "  None"

echo ""
echo "  FDB CRDs:"
kubectl get crd 2>/dev/null | grep -i foundationdb || echo "  None"

echo ""
echo "  kube-system simplyblock resources:"
for RTYPE in deployment daemonset service sa configmap; do
  kubectl -n kube-system get $RTYPE --no-headers 2>/dev/null | grep -i simplyblock || true
done
echo "  (empty = clean)"

echo ""
echo "============================================================"
echo "  Cleanup complete!"
echo "============================================================"
