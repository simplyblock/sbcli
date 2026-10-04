# Manual Steps: Clean Up + Deploy R25 Setup (for dev investigation)

These use the default parameters from the `k8s-native-upgrade.yaml` workflow:
- `ndcs=1, npcs=1, partitions=0, jm_count=3, max_lvol=30`
- Environment: `openshift-baremetal`
- Workers: `worker-0` through `worker-5`

Purpose: Reproduce the R25 setup manually so dev can observe exactly when/how FDB disappears during the upgrade uninstall step.

---

## Phase 1: Cleanup everything

```bash
NAMESPACE=simplyblock

# 1. Uninstall all Helm releases (R25 + R26)
helm uninstall spdk-csi -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true
helm uninstall sbcli -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true
for rel in $(helm list -n $NAMESPACE -q 2>/dev/null); do
  echo "Uninstalling: $rel"
  helm uninstall "$rel" -n $NAMESPACE --no-hooks --timeout 60s 2>/dev/null || true
done
helm uninstall cert-manager -n cert-manager --no-hooks --timeout 60s 2>/dev/null || true

# 2. Delete simplyblock resources in kube-system
for RTYPE in deployment service sa configmap; do
  for NAME in $(kubectl -n kube-system get $RTYPE --no-headers -o custom-columns=:metadata.name 2>/dev/null | grep -i simplyblock 2>/dev/null); do
    echo "Deleting kube-system $RTYPE/$NAME"
    kubectl -n kube-system delete $RTYPE "$NAME" --ignore-not-found 2>/dev/null || true
  done
done

# 3. Run shared cleanup scripts (if available on disk)
# bash /path/to/simplyblock-operator/helm-charts/scripts/cleanup-simplyblock.sh -f $NAMESPACE || true
# bash /path/to/e2e/scripts/cleanup_k8s.sh $NAMESPACE

# 4. Delete released PVs
for pv in $(kubectl get pv --no-headers 2>/dev/null | grep 'simplyblock/' | awk '{print $1}'); do
  kubectl delete pv "$pv" --ignore-not-found || true
done

# 5. Delete simplyblock CRDs (clear finalizers first)
for crd in $(kubectl get crd -o name 2>/dev/null | grep simplyblock); do
  crd_name="${crd#customresourcedefinition.apiextensions.k8s.io/}"
  for cr in $(kubectl get "$crd_name" -A --no-headers -o custom-columns=NS:.metadata.namespace,NAME:.metadata.name 2>/dev/null); do
    IFS=' ' read -r ns name <<< "$cr"
    kubectl patch "$crd_name" "$name" -n "$ns" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
  done
  kubectl patch crd "$crd_name" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
done
kubectl delete crd $(kubectl get crd -o name 2>/dev/null | grep simplyblock | sed 's|customresourcedefinition.apiextensions.k8s.io/||') --ignore-not-found --timeout=60s 2>/dev/null || true

# 6. Delete + finalize namespaces
kubectl delete namespace $NAMESPACE --force --grace-period=0 --wait=false 2>/dev/null || true
kubectl delete namespace cert-manager --force --grace-period=0 --wait=false 2>/dev/null || true
for ns in $NAMESPACE cert-manager; do
  kubectl get namespace $ns -o json 2>/dev/null \
    | jq '.spec.finalizers = []' \
    | kubectl replace --raw "/api/v1/namespaces/$ns/finalize" -f - 2>/dev/null || true
done

# 7. Delete cert-manager CRDs
kubectl delete crd certificaterequests.cert-manager.io certificates.cert-manager.io \
  challenges.acme.cert-manager.io clusterissuers.cert-manager.io \
  issuers.cert-manager.io orders.acme.cert-manager.io \
  --ignore-not-found --force --grace-period=0 2>/dev/null || true

# 8. Disconnect NVMe, reset hugepages, restart kubelet on each worker
for NODE in worker-0.ocp.simplyblock.ai worker-1.ocp.simplyblock.ai worker-2.ocp.simplyblock.ai worker-3.ocp.simplyblock.ai worker-4.ocp.simplyblock.ai worker-5.ocp.simplyblock.ai; do
  oc debug node/"$NODE" -- chroot /host bash -c \
    "nvme disconnect-all 2>/dev/null; echo 0 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages && systemctl restart kubelet" 2>/dev/null || true
  echo "Done: $NODE"
done

# 9. Verify namespace is gone
kubectl get namespace $NAMESPACE 2>/dev/null && echo "WARNING: namespace still exists" || echo "Namespace gone"

# 10. Remove stale storagenodeset labels
for NODE in worker-0.ocp.simplyblock.ai worker-1.ocp.simplyblock.ai worker-2.ocp.simplyblock.ai worker-3.ocp.simplyblock.ai worker-4.ocp.simplyblock.ai worker-5.ocp.simplyblock.ai; do
  kubectl label node "$NODE" io.simplyblock.storagenodeset- 2>/dev/null || true
done
```

---

## Phase 2: Install R25 sbcli control plane

```bash
# Clone sbcli R25 branch
git clone --branch remove_snode_init_container https://github.com/simplyblock-io/sbcli.git sbcli-r25

# Install sbcli chart
cd sbcli-r25/simplyblock_core/scripts/charts/
helm dependency build .
helm upgrade --install sbcli \
  --namespace simplyblock \
  --create-namespace \
  --timeout 10m \
  --set ingress-nginx.controller.admissionWebhooks.enabled=false \
  ./

# Wait for control plane pods
kubectl wait --for=condition=Ready pods --all -n simplyblock --timeout=300s \
  --field-selector=status.phase!=Succeeded || true
kubectl get pods -n simplyblock
```

---

## Phase 3: Create R25 cluster

```bash
NAMESPACE=simplyblock

# Find admin pod
ADMIN_POD=$(kubectl -n $NAMESPACE get pods --no-headers | grep -i "admin-control\|webappapi" | grep "Running" | head -1 | awk '{print $1}')
echo "Admin pod: $ADMIN_POD"

# Wait for FDB to be ready
for i in $(seq 1 30); do
  FDB_STATUS=$(kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev cluster list 2>&1) || true
  if echo "$FDB_STATUS" | grep -qiE 'Connection.*invalid|Error.*reading.*FDB|not found'; then
    echo "FDB not ready yet ($i/30): $(echo "$FDB_STATUS" | head -1)"
    sleep 10
  else
    echo "FDB appears ready"
    break
  fi
done

# Get mgmt IP from first worker
MGMT_IP=$(kubectl get node worker-0.ocp.simplyblock.ai -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}')
echo "MGMT_IP=$MGMT_IP"

# Create cluster
kubectl -n $NAMESPACE exec "$ADMIN_POD" -- \
  sbcli-dev -d --dev cluster create \
    --mgmt-ip "$MGMT_IP" \
    --mode kubernetes \
    --disable-monitoring

# Get cluster ID and secret
CLUSTER_ID=$(kubectl -n $NAMESPACE exec "$ADMIN_POD" -- \
  sbcli-dev cluster list --json 2>/dev/null | jq -r '.[0].id // .[0].uuid // empty')
CLUSTER_SECRET=$(kubectl -n $NAMESPACE exec "$ADMIN_POD" -- \
  sbcli-dev cluster get-secret "$CLUSTER_ID" 2>/dev/null | tr -d '[:space:]')

echo "CLUSTER_ID=$CLUSTER_ID"
echo "CLUSTER_SECRET=$CLUSTER_SECRET"

# Validate secret
if [ -z "$CLUSTER_SECRET" ] || [ "${#CLUSTER_SECRET}" -gt 100 ]; then
  echo "ERROR: CLUSTER_SECRET invalid (length=${#CLUSTER_SECRET})"
fi
```

---

## Phase 4: Label workers + Install R25 spdk-csi chart

```bash
NAMESPACE=simplyblock

# Label workers
for NODE in worker-0.ocp.simplyblock.ai worker-1.ocp.simplyblock.ai worker-2.ocp.simplyblock.ai worker-3.ocp.simplyblock.ai worker-4.ocp.simplyblock.ai worker-5.ocp.simplyblock.ai; do
  kubectl label node "$NODE" io.simplyblock.node-type=simplyblock-storage-plane --overwrite
done

# Clone simplyblock-operator (R25 CSI chart branch)
git clone --branch v0.2.4 https://github.com/simplyblock/simplyblock-operator.git simplyblock-operator-r25

# Find spdk-csi chart directory
if [ -d "simplyblock-operator-r25/csi-driver/charts/spdk-csi/latest/spdk-csi" ]; then
  cd simplyblock-operator-r25/csi-driver/charts/spdk-csi/latest/spdk-csi/
elif [ -d "simplyblock-operator-r25/charts/spdk-csi/latest/spdk-csi" ]; then
  cd simplyblock-operator-r25/charts/spdk-csi/latest/spdk-csi/
fi
helm dependency build . 2>/dev/null || true

# Install spdk-csi (use the CLUSTER_ID and CLUSTER_SECRET from Phase 3)
# Replace <BASE_SIMPLYBLOCK_IMAGE> with the base image tag used in the run
helm install -n simplyblock --create-namespace spdk-csi ./ \
  --set csiConfig.simplybk.uuid="${CLUSTER_ID}" \
  --set csiConfig.simplybk.ip="http://simplyblock-webappapi.simplyblock:5000" \
  --set csiSecret.simplybk.secret="${CLUSTER_SECRET}" \
  --set logicalVolume.pool_name="testing1" \
  --set image.simplyblock.tag="<BASE_SIMPLYBLOCK_IMAGE>" \
  --set image.csi.tag="v0.2.4" \
  --set logicalVolume.numDataChunks=1 \
  --set logicalVolume.numParityChunks=1 \
  --set storageclass.volumeBindingMode=Immediate \
  --set cachingnode.create=false \
  --set logicalVolume.encryption=false \
  --set storagenode.ifname=br-ex \
  --set storagenode.create=true \
  --set storagenode.numPartitions=1 \
  --set storagenode.coresPercentage=50 \
  --set image.storageNode.tag="v0.1.8"
```

---

## Phase 5: Wait for R25 cluster active

```bash
NAMESPACE=simplyblock
ADMIN_POD=$(kubectl -n $NAMESPACE get pods --no-headers | grep -i "admin-control\|webappapi" | grep "Running" | head -1 | awk '{print $1}')

# Wait for storage nodes to register
for i in $(seq 1 60); do
  SN_COUNT=$(kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev sn list --json 2>/dev/null | jq 'length' 2>/dev/null || echo "0")
  echo "Storage nodes: $SN_COUNT/6 ($i/60)"
  [ "$SN_COUNT" -ge 6 ] && break
  sleep 10
done

# Wait for all online
for i in $(seq 1 60); do
  ONLINE=$(kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev sn list --json 2>/dev/null | jq '[.[] | select(.status == "online")] | length' 2>/dev/null || echo "0")
  echo "Online: $ONLINE/6 ($i/60)"
  [ "$ONLINE" -ge 6 ] && break
  sleep 10
done

# Activate cluster (R25 needs manual activation)
kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev cluster activate "$CLUSTER_ID"

# Verify
kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev cluster list
kubectl -n $NAMESPACE exec "$ADMIN_POD" -- sbcli-dev sn list
```

---

## At this point — R25 is fully set up

Now observe and record the FDB state, then run the upgrade uninstall to see where FDB disappears.

### Key finding from dev

`kubectl annotate` on live resources does NOT protect against `helm uninstall`. Helm reads
annotations from its stored release manifest (in `sh.helm.release.v1.*` secrets), not from
the live object in etcd. The correct fix is to add `helm.sh/resource-policy: keep` directly
in the Helm chart templates and run `helm upgrade` before uninstall.

Also: `helm uninstall` does NOT remove CRDs, so CRD cascade deletion is not the issue.

### Observation checkpoint: Before uninstall

Check whether the R25 chart templates already include `helm.sh/resource-policy: keep` for FDB resources:

```bash
echo "=== FDB CRDs ==="
kubectl get crd | grep foundationdb

echo "=== FoundationDBCluster CR ==="
kubectl get foundationdbcluster -n simplyblock

echo "=== FDB pods ==="
kubectl get pods -n simplyblock | grep fdb

echo "=== FDB controller-manager deployment ==="
kubectl get deployment -n simplyblock | grep fdb

echo "=== What helm charts manage ==="
echo "--- sbcli chart manifest (FDB references) ---"
helm get manifest sbcli -n simplyblock 2>/dev/null | grep -i "kind:\|name:.*fdb" | head -30
echo "--- spdk-csi chart manifest (FDB references) ---"
helm get manifest spdk-csi -n simplyblock 2>/dev/null | grep -i "kind:\|name:.*fdb" | head -30

echo "=== Check if resource-policy: keep is in Helm's stored manifest (THIS IS THE KEY CHECK) ==="
echo "--- sbcli chart: resource-policy annotations ---"
helm get manifest sbcli -n simplyblock 2>/dev/null | grep -B10 "resource-policy" || echo "NO resource-policy annotations found in sbcli manifest"
echo "--- spdk-csi chart: resource-policy annotations ---"
helm get manifest spdk-csi -n simplyblock 2>/dev/null | grep -B10 "resource-policy" || echo "NO resource-policy annotations found in spdk-csi manifest"
```

### Test: Run helm uninstall sbcli and observe

```bash
# Uninstall sbcli chart (control plane)
helm uninstall sbcli -n simplyblock --no-hooks --timeout 60s

# Immediately check what's left
echo "=== After 'helm uninstall sbcli' ==="

echo "=== FDB CRDs ==="
kubectl get crd | grep foundationdb

echo "=== FoundationDBCluster CR ==="
kubectl get foundationdbcluster -n simplyblock

echo "=== FDB pods ==="
kubectl get pods -n simplyblock | grep fdb

echo "=== FDB controller-manager deployment ==="
kubectl get deployment -n simplyblock | grep fdb

echo "=== All remaining pods ==="
kubectl get pods -n simplyblock
```

If FDB is still present after `helm uninstall sbcli`, then also test:

```bash
# Uninstall spdk-csi chart
helm uninstall spdk-csi -n simplyblock --no-hooks --timeout 60s

# Check again
echo "=== After 'helm uninstall spdk-csi' ==="
kubectl get crd | grep foundationdb
kubectl get foundationdbcluster -n simplyblock
kubectl get pods -n simplyblock | grep fdb
kubectl get deployment -n simplyblock | grep fdb
kubectl get pods -n simplyblock
```
