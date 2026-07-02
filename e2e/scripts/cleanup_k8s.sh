#!/usr/bin/env bash
# cleanup_k8s.sh — Full cleanup of simplyblock K8s deployment
#
# Usage: ./cleanup_k8s.sh [NAMESPACE]
#   NAMESPACE defaults to "simplyblock"
#
# Designed to work even when etcd is overloaded (300k+ objects).
# All kubectl commands use --request-timeout to prevent indefinite hangs.

set +e

NAMESPACE="${1:-simplyblock}"
# All kubectl commands get a request timeout to survive etcd overload
KUBECTL_TIMEOUT="--request-timeout=120s"

echo "Cleaning up simplyblock deployment in namespace: $NAMESPACE"

# Helper: retry a command up to N times with backoff
retry_cmd() {
  local max_attempts=$1
  shift
  local attempt=1
  while [ $attempt -le $max_attempts ]; do
    if "$@" 2>/dev/null; then
      return 0
    fi
    echo "  Attempt $attempt/$max_attempts failed, retrying in 10s..."
    sleep 10
    attempt=$((attempt + 1))
  done
  echo "  All $max_attempts attempts failed for: $*"
  return 1
}

echo "=== Phase 1: Helm uninstall ==="
helm uninstall spdk-csi -n $NAMESPACE 2>/dev/null || true

echo "=== Phase 2: Patch finalizers and delete CRs ==="
RESOURCES=(
  "simplyblockpool.storage.simplyblock.io simplyblock-pool"
  "simplyblockpool.storage.simplyblock.io simplyblock-pool2"
  "simplyblocklvol.storage.simplyblock.io simplyblock-lvol"
  "simplyblocktask.storage.simplyblock.io simplyblock-task"
  "simplyblockdevices.storage.simplyblock.io simplyblock-devices"
  "simplyblockdevices.storage.simplyblock.io simplyblock-device-action"
  "simplyblockstoragenodes.storage.simplyblock.io simplyblock-node"
  "simplyblockstoragenodes.storage.simplyblock.io simplyblock-node2"
  "simplyblockstoragenodes.storage.simplyblock.io simplyblock-node-action"
  "simplyblockstoragenodesets.storage.simplyblock.io simplyblock-node"
  "simplyblockstoragenodesets.storage.simplyblock.io simplyblock-node2"
  "simplyblockstoragenodesets.storage.simplyblock.io simplyblock-node-action"
  "simplyblockstorageclusters.storage.simplyblock.io simplyblock-cluster"
  "simplyblockstorageclusters.storage.simplyblock.io simplyblock-cluster2"
  "simplyblockstorageclusters.storage.simplyblock.io simplyblock-cluster-activate"
  "simplyblocksnapshotreplications.storage.simplyblock.io simplyblock-snap-replication"
  "simplyblocksnapshotreplications.storage.simplyblock.io simplyblock-snap-replication-failback"
  "pool.storage.simplyblock.io simplyblock-pool"
  "pool.storage.simplyblock.io simplyblock-pool2"
  "lvol.storage.simplyblock.io simplyblock-lvol"
  "task.storage.simplyblock.io simplyblock-task"
  "devices.storage.simplyblock.io simplyblock-devices"
  "devices.storage.simplyblock.io simplyblock-device-action"
  "storagenodes.storage.simplyblock.io simplyblock-node"
  "storagenodes.storage.simplyblock.io simplyblock-node2"
  "storagenodes.storage.simplyblock.io simplyblock-node-action"
  "storagenodesets.storage.simplyblock.io simplyblock-node"
  "storagenodesets.storage.simplyblock.io simplyblock-node2"
  "storagenodesets.storage.simplyblock.io simplyblock-node-action"
  "storageclusters.storage.simplyblock.io simplyblock-cluster"
  "storageclusters.storage.simplyblock.io simplyblock-cluster2"
  "storageclusters.storage.simplyblock.io simplyblock-cluster-activate"
  "snapshotreplications.storage.simplyblock.io simplyblock-snap-replication"
  "snapshotreplications.storage.simplyblock.io simplyblock-snap-replication-failback"
)

patch_and_delete_crs() {
  echo "Removing finalizers..."
  for item in "${RESOURCES[@]}"; do
    KIND=$(echo "$item" | awk '{print $1}')
    NAME=$(echo "$item" | awk '{print $2}')
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch "$KIND" "$NAME" \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  done

  echo "Deleting resources..."
  for item in "${RESOURCES[@]}"; do
    KIND=$(echo "$item" | awk '{print $1}')
    NAME=$(echo "$item" | awk '{print $2}')
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete "$KIND" "$NAME" \
      --ignore-not-found --wait=false 2>/dev/null || true
  done
}
patch_and_delete_crs

# Dynamic catch-all: patch finalizers and delete ALL CRs of each type
# (handles resources not in the hardcoded list, e.g. encryption-pool)
echo "Cleaning up any remaining CRs..."
for CR_TYPE in \
  "simplyblockpool.storage.simplyblock.io" \
  "simplyblocklvol.storage.simplyblock.io" \
  "simplyblocktask.storage.simplyblock.io" \
  "simplyblockdevices.storage.simplyblock.io" \
  "simplyblockstoragenodes.storage.simplyblock.io" \
  "simplyblockstoragenodesets.storage.simplyblock.io" \
  "simplyblockstorageclusters.storage.simplyblock.io" \
  "simplyblocksnapshotreplications.storage.simplyblock.io" \
  "pool.storage.simplyblock.io" \
  "lvol.storage.simplyblock.io" \
  "task.storage.simplyblock.io" \
  "devices.storage.simplyblock.io" \
  "storagenodes.storage.simplyblock.io" \
  "storagenodesets.storage.simplyblock.io" \
  "storageclusters.storage.simplyblock.io" \
  "snapshotreplications.storage.simplyblock.io" \
  "storagebackups.storage.simplyblock.io" \
  "backuprestores.storage.simplyblock.io" \
  "backuppolicies.storage.simplyblock.io" \
  "backupimports.storage.simplyblock.io"; do
  for CR_NAME in $(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get "$CR_TYPE" --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch "$CR_TYPE" "$CR_NAME" \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete "$CR_TYPE" "$CR_NAME" \
      --ignore-not-found --wait=false 2>/dev/null || true
  done
done

echo "=== Phase 3a: Delete VolumeSnapshots & VolumeSnapshotContents ==="
# Bulk delete ALL snapshots first (avoid one-by-one listing which times out
# when there are 100k+ snapshot objects and etcd is overloaded).
echo "Bulk deleting all VolumeSnapshots..."
retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete volumesnapshot --all --wait=false

# If bulk delete didn't clear them (finalizers), patch and force-delete
VS_REMAINING=$(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get volumesnapshot --no-headers 2>/dev/null | wc -l)
if [ "${VS_REMAINING:-0}" -gt 0 ]; then
  echo "$VS_REMAINING VolumeSnapshots still exist, patching finalizers in parallel..."
  kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get volumesnapshot --no-headers \
    -o custom-columns=:metadata.name 2>/dev/null | \
    xargs -P 20 -I {} kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch volumesnapshot {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete volumesnapshot --all \
    --force --grace-period=0 --wait=false
fi

# Bulk delete VolumeSnapshotContents (cluster-scoped)
echo "Bulk deleting all VolumeSnapshotContents..."
retry_cmd 3 kubectl $KUBECTL_TIMEOUT delete volumesnapshotcontent --all --wait=false

VSC_REMAINING=$(kubectl $KUBECTL_TIMEOUT get volumesnapshotcontent --no-headers 2>/dev/null | wc -l)
if [ "${VSC_REMAINING:-0}" -gt 0 ]; then
  echo "$VSC_REMAINING VolumeSnapshotContents still exist, patching finalizers..."
  kubectl $KUBECTL_TIMEOUT get volumesnapshotcontent --no-headers \
    -o custom-columns=:metadata.name 2>/dev/null | \
    xargs -P 20 -I {} kubectl $KUBECTL_TIMEOUT patch volumesnapshotcontent {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl $KUBECTL_TIMEOUT delete volumesnapshotcontent --all \
    --force --grace-period=0 --wait=false
fi

# Delete VolumeSnapshotClasses
for VSCLASS in $(kubectl $KUBECTL_TIMEOUT get volumesnapshotclass --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete volumesnapshotclass "$VSCLASS" --ignore-not-found 2>/dev/null || true
done

echo "=== Phase 3b: Delete PVCs (with timeout + retry) ==="
# Bulk delete all PVCs
retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete pvc --all --wait=false

# Wait up to 120s for PVCs to go away (increased from 60s for etcd recovery)
PVC_TIMEOUT=120
while [ $PVC_TIMEOUT -gt 0 ]; do
  REMAINING=$(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get pvc --no-headers 2>/dev/null | wc -l)
  if [ "${REMAINING:-0}" -eq 0 ]; then
    echo "All PVCs deleted"
    break
  fi
  echo "Waiting for $REMAINING PVCs to delete ($PVC_TIMEOUT s remaining)..."
  sleep 10
  PVC_TIMEOUT=$((PVC_TIMEOUT - 10))
done

# If PVCs are still stuck, patch all finalizers in parallel then bulk delete
STUCK_PVCS=$(kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get pvc --no-headers -o custom-columns=:metadata.name 2>/dev/null)
if [ -n "$STUCK_PVCS" ]; then
  echo "Patching finalizers on stuck PVCs in parallel (xargs -P 20)..."
  echo "$STUCK_PVCS" | xargs -P 20 -I {} \
    kubectl -n $NAMESPACE $KUBECTL_TIMEOUT patch pvc {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  retry_cmd 3 kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete pvc --all \
    --force --grace-period=0 --wait=false
  echo "PVC force-delete issued"
fi

echo "=== Phase 3c: Delete PVs ==="
# Clean all PVs except vault ones.  Filter by CLAIM column (namespace/name)
# since PV names are pvc-<uuid> and don't contain "vault".
# Also exclude local-hostpath storageclass PVs used by vault.
PV_LIST=$(kubectl $KUBECTL_TIMEOUT get pv --no-headers -o custom-columns=NAME:.metadata.name,CLAIM:.spec.claimRef.namespace 2>/dev/null \
  | grep -v -E '\bvault\b' 2>/dev/null | awk '{print $1}')
if [ -n "$PV_LIST" ]; then
  PV_COUNT=$(echo "$PV_LIST" | wc -l)
  echo "Patching finalizers on $PV_COUNT PVs in parallel..."
  echo "$PV_LIST" | xargs -P 20 -I {} \
    kubectl $KUBECTL_TIMEOUT patch pv {} \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null
  echo "Deleting PVs in parallel..."
  echo "$PV_LIST" | xargs -P 20 -I {} \
    kubectl $KUBECTL_TIMEOUT delete pv {} --force --grace-period=0 --wait=false 2>/dev/null
  echo "PV force-delete issued"
fi

echo "=== Phase 3d: Force delete remaining namespaced resources ==="
for RTYPE in pod jobs service ds statefulset deployment replicaset secret sa configmap; do
  kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete $RTYPE --all --force --grace-period=0 2>/dev/null || true
done

echo "=== Phase 4: Cleanup cluster-scoped resources ==="
# Delete StorageClasses
for SC in $(kubectl $KUBECTL_TIMEOUT get sc --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl $KUBECTL_TIMEOUT delete sc "$SC" --ignore-not-found 2>/dev/null || true
done
kubectl $KUBECTL_TIMEOUT delete clusterrole simplyblock-storage-node-role --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrolebinding simplyblock-storage-node-binding --ignore-not-found 2>/dev/null || true

echo "=== Phase 4b: Cleanup leftover secrets, SAs, and kube-system resources ==="
kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete secret simplyblock-csi-secret-v2 --ignore-not-found 2>/dev/null || true
kubectl -n $NAMESPACE $KUBECTL_TIMEOUT delete sa simplyblock-storage-node-sa --ignore-not-found 2>/dev/null || true

# Clean up kube-system resources from previous helm installs
for RES in sa clusterrole clusterrolebinding; do
  for NAME in $(kubectl $KUBECTL_TIMEOUT get $RES -A --no-headers 2>/dev/null | grep -i simplyblock | awk '{print $1 ":" $2}' 2>/dev/null); do
    NS=$(echo "$NAME" | cut -d: -f1)
    RNAME=$(echo "$NAME" | cut -d: -f2)
    if [ "$RES" = "sa" ]; then
      kubectl -n "$NS" $KUBECTL_TIMEOUT delete sa "$RNAME" --ignore-not-found 2>/dev/null || true
    else
      kubectl $KUBECTL_TIMEOUT delete "$RES" "$RNAME" --ignore-not-found 2>/dev/null || true
    fi
  done
done
# Specifically clean numa-resource-plugin resources in kube-system
kubectl -n kube-system $KUBECTL_TIMEOUT delete ds simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system $KUBECTL_TIMEOUT delete sa simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system $KUBECTL_TIMEOUT delete cm simplyblock-numa-resource-plugin-config --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrole simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl $KUBECTL_TIMEOUT delete clusterrolebinding simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true

# Clean up snapshot-controller and any other simplyblock deployments/services in kube-system
for RTYPE in deployment service sa configmap; do
  for NAME in $(kubectl -n kube-system $KUBECTL_TIMEOUT get $RTYPE --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
    kubectl -n kube-system $KUBECTL_TIMEOUT delete $RTYPE "$NAME" --ignore-not-found 2>/dev/null || true
  done
done

echo "=== Phase 5: Verify nothing remains ==="
echo "Namespaced resources:"
kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get all 2>/dev/null || echo "No resources found"
echo ""
echo "CRDs:"
for CR_TYPE in \
  "simplyblockpool.storage.simplyblock.io" \
  "simplyblocklvol.storage.simplyblock.io" \
  "simplyblockstoragenodes.storage.simplyblock.io" \
  "simplyblockstoragenodesets.storage.simplyblock.io" \
  "simplyblockstorageclusters.storage.simplyblock.io" \
  "pool.storage.simplyblock.io" \
  "lvol.storage.simplyblock.io" \
  "storagenodes.storage.simplyblock.io" \
  "storagenodesets.storage.simplyblock.io" \
  "storageclusters.storage.simplyblock.io" \
  "storagebackups.storage.simplyblock.io" \
  "backuprestores.storage.simplyblock.io" \
  "backuppolicies.storage.simplyblock.io" \
  "backupimports.storage.simplyblock.io"; do
  kubectl -n $NAMESPACE $KUBECTL_TIMEOUT get "$CR_TYPE" 2>/dev/null || true
done

echo "=== Phase 6: Delete namespace ==="
kubectl $KUBECTL_TIMEOUT delete namespace $NAMESPACE --wait=false 2>/dev/null || true

for i in $(seq 1 36); do
  if ! kubectl $KUBECTL_TIMEOUT get namespace $NAMESPACE 2>/dev/null; then
    echo "Namespace $NAMESPACE deleted"
    break
  fi

  echo "Namespace still terminating, re-patching finalizers ($i/36)..."
  patch_and_delete_crs

  if [ "$i" -ge 6 ]; then
    echo "Force-removing namespace finalizers..."
    kubectl $KUBECTL_TIMEOUT get namespace $NAMESPACE -o json 2>/dev/null | \
      jq '.spec.finalizers = []' | \
      kubectl $KUBECTL_TIMEOUT replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f - 2>/dev/null || true
  fi

  sleep 5
done

echo "=== Cleanup complete ==="
