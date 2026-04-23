#!/usr/bin/env bash
# cleanup_k8s.sh — Full cleanup of simplyblock K8s deployment
#
# Usage: ./cleanup_k8s.sh [NAMESPACE]
#   NAMESPACE defaults to "simplyblock"

set +e

NAMESPACE="${1:-simplyblock}"
echo "Cleaning up simplyblock deployment in namespace: $NAMESPACE"

echo "=== Phase 1: Helm uninstall ==="
helm uninstall spdk-csi -n $NAMESPACE 2>/dev/null || true

echo "=== Phase 2: Patch finalizers and delete CRs ==="
RESOURCES=(
  "simplyblockpool.simplyblock.simplyblock.io simplyblock-pool"
  "simplyblockpool.simplyblock.simplyblock.io simplyblock-pool2"
  "simplyblocklvol.simplyblock.simplyblock.io simplyblock-lvol"
  "simplyblocktask.simplyblock.simplyblock.io simplyblock-task"
  "simplyblockdevices.simplyblock.simplyblock.io simplyblock-devices"
  "simplyblockdevices.simplyblock.simplyblock.io simplyblock-device-action"
  "simplyblockstoragenodes.simplyblock.simplyblock.io simplyblock-node"
  "simplyblockstoragenodes.simplyblock.simplyblock.io simplyblock-node2"
  "simplyblockstoragenodes.simplyblock.simplyblock.io simplyblock-node-action"
  "simplyblockstorageclusters.simplyblock.simplyblock.io simplyblock-cluster"
  "simplyblockstorageclusters.simplyblock.simplyblock.io simplyblock-cluster2"
  "simplyblockstorageclusters.simplyblock.simplyblock.io simplyblock-cluster-activate"
  "simplyblocksnapshotreplications.simplyblock.simplyblock.io simplyblock-snap-replication"
  "simplyblocksnapshotreplications.simplyblock.simplyblock.io simplyblock-snap-replication-failback"
  "pool.storage.simplyblock.io simplyblock-pool"
  "pool.storage.simplyblock.io simplyblock-pool2"
  "lvol.storage.simplyblock.io simplyblock-lvol"
  "task.storage.simplyblock.io simplyblock-task"
  "devices.storage.simplyblock.io simplyblock-devices"
  "devices.storage.simplyblock.io simplyblock-device-action"
  "storagenodes.storage.simplyblock.io simplyblock-node"
  "storagenodes.storage.simplyblock.io simplyblock-node2"
  "storagenodes.storage.simplyblock.io simplyblock-node-action"
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
    kubectl -n $NAMESPACE patch "$KIND" "$NAME" \
      --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  done

  echo "Deleting resources..."
  for item in "${RESOURCES[@]}"; do
    KIND=$(echo "$item" | awk '{print $1}')
    NAME=$(echo "$item" | awk '{print $2}')
    kubectl -n $NAMESPACE delete "$KIND" "$NAME" \
      --ignore-not-found --wait=false 2>/dev/null || true
  done
}
patch_and_delete_crs

echo "=== Phase 3a: Delete VolumeSnapshots & VolumeSnapshotContents ==="
# Delete snapshots first (they block PVC deletion)
for VS in $(kubectl get volumesnapshot -n $NAMESPACE --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  kubectl -n $NAMESPACE patch volumesnapshot "$VS" \
    --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl -n $NAMESPACE delete volumesnapshot "$VS" --wait=false 2>/dev/null || true
done

# Delete VolumeSnapshotContents (cluster-scoped, may block snapshot deletion)
for VSC in $(kubectl get volumesnapshotcontent --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  kubectl patch volumesnapshotcontent "$VSC" \
    --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl delete volumesnapshotcontent "$VSC" --wait=false 2>/dev/null || true
done

# Delete VolumeSnapshotClasses
for VSCLASS in $(kubectl get volumesnapshotclass --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  kubectl delete volumesnapshotclass "$VSCLASS" --ignore-not-found 2>/dev/null || true
done

echo "=== Phase 3b: Delete PVCs (with timeout + retry) ==="
# First attempt: normal delete
kubectl -n $NAMESPACE delete pvc --all --wait=false 2>/dev/null || true

# Wait up to 60s for PVCs to go away
PVC_TIMEOUT=60
while [ $PVC_TIMEOUT -gt 0 ]; do
  REMAINING=$(kubectl -n $NAMESPACE get pvc --no-headers 2>/dev/null | wc -l)
  if [ "$REMAINING" -eq 0 ]; then
    echo "All PVCs deleted"
    break
  fi
  echo "Waiting for $REMAINING PVCs to delete ($PVC_TIMEOUT s remaining)..."
  sleep 5
  PVC_TIMEOUT=$((PVC_TIMEOUT - 5))
done

# If PVCs are still stuck, patch finalizers and force delete
for PVC in $(kubectl -n $NAMESPACE get pvc --no-headers -o custom-columns=:metadata.name 2>/dev/null); do
  echo "Force-deleting stuck PVC: $PVC"
  kubectl -n $NAMESPACE patch pvc "$PVC" \
    --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl -n $NAMESPACE delete pvc "$PVC" --force --grace-period=0 2>/dev/null || true
done

echo "=== Phase 3c: Delete PVs ==="
for PV in $(kubectl get pv --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl patch pv "$PV" \
    --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  kubectl delete pv "$PV" --force --grace-period=0 2>/dev/null || true
done

echo "=== Phase 3d: Force delete remaining namespaced resources ==="
for RTYPE in pod jobs service ds statefulset deployment replicaset secret sa configmap; do
  kubectl -n $NAMESPACE delete $RTYPE --all --force --grace-period=0 2>/dev/null || true
done

echo "=== Phase 4: Cleanup cluster-scoped resources ==="
# Delete StorageClasses
for SC in $(kubectl get sc --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
  kubectl delete sc "$SC" --ignore-not-found 2>/dev/null || true
done
kubectl delete clusterrole simplyblock-storage-node-role --ignore-not-found 2>/dev/null || true
kubectl delete clusterrolebinding simplyblock-storage-node-binding --ignore-not-found 2>/dev/null || true

echo "=== Phase 4b: Cleanup leftover secrets, SAs, and kube-system resources ==="
kubectl -n $NAMESPACE delete secret simplyblock-csi-secret-v2 --ignore-not-found 2>/dev/null || true
kubectl -n $NAMESPACE delete sa simplyblock-storage-node-sa --ignore-not-found 2>/dev/null || true

# Clean up kube-system resources from previous helm installs
for RES in sa clusterrole clusterrolebinding; do
  for NAME in $(kubectl get $RES -A --no-headers 2>/dev/null | grep -i simplyblock | awk '{print $1 ":" $2}' 2>/dev/null); do
    NS=$(echo "$NAME" | cut -d: -f1)
    RNAME=$(echo "$NAME" | cut -d: -f2)
    if [ "$RES" = "sa" ]; then
      kubectl -n "$NS" delete sa "$RNAME" --ignore-not-found 2>/dev/null || true
    else
      kubectl delete "$RES" "$RNAME" --ignore-not-found 2>/dev/null || true
    fi
  done
done
# Specifically clean numa-resource-plugin resources in kube-system
kubectl -n kube-system delete ds simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system delete sa simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl -n kube-system delete cm simplyblock-numa-resource-plugin-config --ignore-not-found 2>/dev/null || true
kubectl delete clusterrole simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true
kubectl delete clusterrolebinding simplyblock-numa-resource-plugin --ignore-not-found 2>/dev/null || true

echo "=== Phase 5: Verify nothing remains ==="
echo "Namespaced resources:"
kubectl -n $NAMESPACE get all 2>/dev/null || echo "No resources found"
echo ""
echo "CRDs:"
for CR_TYPE in \
  "simplyblockpool.simplyblock.simplyblock.io" \
  "simplyblocklvol.simplyblock.simplyblock.io" \
  "simplyblockstoragenodes.simplyblock.simplyblock.io" \
  "simplyblockstorageclusters.simplyblock.simplyblock.io" \
  "pool.storage.simplyblock.io" \
  "lvol.storage.simplyblock.io" \
  "storagenodes.storage.simplyblock.io" \
  "storageclusters.storage.simplyblock.io"; do
  kubectl -n $NAMESPACE get "$CR_TYPE" 2>/dev/null || true
done

echo "=== Phase 6: Delete namespace ==="
kubectl delete namespace $NAMESPACE --wait=false 2>/dev/null || true

for i in $(seq 1 36); do
  if ! kubectl get namespace $NAMESPACE 2>/dev/null; then
    echo "Namespace $NAMESPACE deleted"
    break
  fi

  echo "Namespace still terminating, re-patching finalizers ($i/36)..."
  patch_and_delete_crs

  if [ "$i" -ge 6 ]; then
    echo "Force-removing namespace finalizers..."
    kubectl get namespace $NAMESPACE -o json 2>/dev/null | \
      jq '.spec.finalizers = []' | \
      kubectl replace --raw "/api/v1/namespaces/$NAMESPACE/finalize" -f - 2>/dev/null || true
  fi

  sleep 5
done

echo "=== Cleanup complete ==="
