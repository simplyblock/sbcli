# K8s Upgrade: R25.x (Helm) to R26+ (Operator) — End-to-End Operations Guide

This document covers the complete upgrade lifecycle from an R25.x legacy Helm-based deployment
to the R26+ operator-based architecture. It is intended for the dev team to confirm that the
setup, upgrade steps, and validation procedures are correct.

---

## Overview

| Phase | Description | Downtime? |
|-------|-------------|-----------|
| Phase 1 | Deploy R25.x cluster using legacy Helm charts | No (fresh setup) |
| Phase 2 | Pre-upgrade data setup — pool, PVCs, FIO, snapshots, clones, MD5 | No |
| Phase 3 | Maintenance window — 10-step migration from Helm to Operator | **Yes** |
| Phase 4 | Post-upgrade validation — verify old data, new provisioning, outages | No |

---

## Phase 1: Deploy R25.x Cluster (Legacy Helm Charts)

R25.x uses two Helm charts (`sbcli` + `spdk-csi`) with no operator. The cluster is created
manually via the admin pod.

> **Branch**: Use PR #1044 branch (`remove_snode_init_container`) or the relevant R25.x tag.

### 1.1 Install the `sbcli` Helm Chart

This deploys the management plane (API, admin pod, FoundationDB).

```bash
helm upgrade --install sbcli ./charts/sbcli \
  --namespace simplyblock --create-namespace \
  --set image.simplyblock.repository=<R25_REPO> \
  --set image.simplyblock.tag=<R25_TAG>
```

Wait for all pods to be ready:

```bash
kubectl wait --for=condition=Ready pods --all -n simplyblock --timeout=300s
```

### 1.2 Create the Cluster via Admin Pod

Exec into the admin pod and run `sbcli-dev cluster create`:

```bash
# Find the admin pod
ADMIN_POD=$(kubectl get pods -n simplyblock -l app=simplyblock-admin -o jsonpath='{.items[0].metadata.name}')

# Exec into it
kubectl exec -it -n simplyblock $ADMIN_POD -- bash

# Inside the pod:
sbcli-dev cluster create \
  --fabric-type tcp \
  --ndcs 1 \
  --npcs 0 \
  --single-node false
```

**Expected output**: Cluster UUID and secret are printed. Save these — they are needed for the upgrade secret in Phase 3.

```
Cluster ID:     <CLUSTER_UUID>
Cluster Secret: <CLUSTER_SECRET>
```

### 1.3 Label Worker Nodes for Storage Plane

The R25 spdk-csi chart uses the `io.simplyblock.node-type` label to discover which
worker nodes should run storage node pods. Label all workers before installing the chart:

```bash
for NODE in <worker-node-1> <worker-node-2> <worker-node-3>; do
    kubectl label node "$NODE" io.simplyblock.node-type=simplyblock-storage-plane --overwrite
done
```

### 1.4 Install the `spdk-csi` Helm Chart (Includes Storage Node Creation)

This deploys the CSI driver and creates storage nodes via `storagenode.create=true`.
Use the cluster UUID, secret, and pool name from step 1.2.

```bash
helm install -n simplyblock --create-namespace spdk-csi ./ \
  --set csiConfig.simplybk.uuid=<CLUSTER_UUID> \
  --set csiConfig.simplybk.ip=http://simplyblock-webappapi.simplyblock:5000 \
  --set csiSecret.simplybk.secret=<CLUSTER_SECRET> \
  --set logicalVolume.pool_name=testing1 \
  --set image.simplyblock.tag=remove_snode_init_container \
  --set image.csi.tag=v0.2.4 \
  --set logicalVolume.numDataChunks=1 \
  --set logicalVolume.numParityChunks=1 \
  --set storageclass.volumeBindingMode=Immediate \
  --set cachingnode.create=false \
  --set logicalVolume.encryption=false \
  --set storagenode.ifname=ens18 \
  --set storagenode.create=true \
  --set storagenode.numPartitions=0 \
  --set image.storageNode.tag=v0.1.8
```

Wait for all pods (CSI + storage nodes) to be ready:

```bash
kubectl wait --for=condition=Ready pods -l app=spdk-csi -n simplyblock --timeout=300s
```

Verify storage nodes are online:

```bash
sbcli-dev sn list
```

**Expected**: All storage nodes show `online` status.

### 1.5 Verify R25.x Cluster

```bash
# Cluster should be active
sbcli-dev cluster list

# All storage nodes online
sbcli-dev sn list

# CSI pods running
kubectl get pods -n simplyblock -l app=spdk-csi
```

---

## Phase 2: Pre-Upgrade Data Setup

Create data before the upgrade so we can verify integrity after migration.

### 2.1 Create Storage Pool

```bash
sbcli-dev pool add upgrade-test-pool
```

### 2.2 Create StorageClass and VolumeSnapshotClass

```yaml
# storageclass.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: simplyblock-csi-sc
provisioner: csi.simplyblock.io
parameters:
  cluster_id: "<CLUSTER_UUID>"
  pool_name: "upgrade-test-pool"
  ndcs: "1"
  npcs: "0"
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

```yaml
# volumesnapshotclass.yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: simplyblock-csi-snapshotclass
driver: csi.simplyblock.io
deletionPolicy: Delete
```

```bash
kubectl apply -f storageclass.yaml
kubectl apply -f volumesnapshotclass.yaml
```

### 2.3 Create PVCs (One Per Storage Node)

Create one PVC per storage node to spread data across the cluster:

```yaml
# For each storage node, create a PVC:
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: upgrade-pvc-0
  namespace: default
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
  storageClassName: simplyblock-csi-sc
```

Wait for all PVCs to be bound:

```bash
kubectl get pvc -w
```

### 2.4 Run FIO with MD5 Verification on Each PVC

Run FIO with `verify=md5` to write data and compute checksums:

```ini
[global]
name=pre-upgrade-fio
filename_format=/spdkvol/fio-data.$jobnum
rw=randrw
rwmixread=50
bs=4k
iodepth=1
direct=1
ioengine=libaio
size=1G
numjobs=1
time_based
runtime=120
group_reporting
verify=md5
verify_dump=1
verify_fatal=1
verify_backlog=4096
verify_backlog_batch=32

[job1]
```

Deploy as a K8s Job with the FIO ConfigMap mounted, one job per PVC.

**Expected**: All FIO jobs complete successfully with 0 errors. The MD5 verification
headers are written into the data files on the PVC.

### 2.5 Create Snapshots of Each PVC

After FIO completes, snapshot each PVC to preserve the verified data state:

```yaml
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata:
  name: snap-upgrade-pvc-0
spec:
  volumeSnapshotClassName: simplyblock-csi-snapshotclass
  source:
    persistentVolumeClaimName: upgrade-pvc-0
```

Wait for snapshots to be ready:

```bash
kubectl get volumesnapshot -w
```

### 2.6 Create Clones from Snapshots

Create a clone PVC from each snapshot and run FIO on the clone:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: clone-upgrade-pvc-0
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
  storageClassName: simplyblock-csi-sc
  dataSource:
    name: snap-upgrade-pvc-0
    kind: VolumeSnapshot
    apiGroup: snapshot.storage.k8s.io
```

Run FIO on clones with `verify=md5` to confirm the cloned data is intact.

**Expected**: All clone FIO jobs pass MD5 verification (the cloned data matches the original).

### 2.7 Capture Pre-Upgrade State

Record the following before starting the upgrade:

- [ ] Cluster UUID and secret
- [ ] All storage node UUIDs and their status (`sbcli-dev sn list`)
- [ ] Pool names/IDs
- [ ] PVC names and their bound PV names (`kubectl get pvc`)
- [ ] Volume snapshot names (`kubectl get volumesnapshot`)
- [ ] Clone PVC names
- [ ] FIO job results (all passed with 0 verify errors)
- [ ] `sbcli-dev lvol list` output — note all lvol IDs

---

## Phase 3: Maintenance Window Upgrade (R25 to R26)

> **WARNING**: Storage nodes are shut down during this phase. Volumes are
> unavailable to workloads. Plan for downtime and notify teams.

### Step 1 — Annotate FDB Resources with `helm.sh/resource-policy: keep`

There are 8 FDB resources that must survive `helm uninstall`:

| Kind | Name |
|------|------|
| Deployment | simplyblock-fdb-controller-manager |
| ServiceAccount | simplyblock-fdb-controller-manager |
| ClusterRole | simplyblock-fdb-manager-role |
| ClusterRole | simplyblock-fdb-manager-clusterrole |
| RoleBinding | simplyblock-fdb-manager-rolebinding |
| ClusterRoleBinding | simplyblock-fdb-manager-clusterrolebinding |
| FoundationDBCluster | simplyblock-fdb-cluster |
| ConfigMap | simplyblock-fdb-cluster-config |

> **Why the ConfigMap?** The `simplyblock-fdb-cluster-config` ConfigMap contains the FDB
> cluster connection file. Admin pods mount it as volume `fdb-cluster-file`. If this
> ConfigMap is deleted during `helm uninstall sbcli`, admin pods will be stuck in
> `ContainerCreating` and all `sbcli`/`sbctl` commands will fail.

Annotate each resource:

```bash
kubectl annotate deployment simplyblock-fdb-controller-manager -n simplyblock \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate serviceaccount simplyblock-fdb-controller-manager -n simplyblock \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate clusterrole simplyblock-fdb-manager-role \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate clusterrole simplyblock-fdb-manager-clusterrole \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate rolebinding simplyblock-fdb-manager-rolebinding -n simplyblock \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate clusterrolebinding simplyblock-fdb-manager-clusterrolebinding \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate foundationdbcluster simplyblock-fdb-cluster -n simplyblock \
  helm.sh/resource-policy=keep --overwrite
kubectl annotate configmap simplyblock-fdb-cluster-config -n simplyblock \
  helm.sh/resource-policy=keep --overwrite
```

**Verify**:

```bash
kubectl get deployment simplyblock-fdb-controller-manager -n simplyblock \
  -o jsonpath='{.metadata.annotations.helm\.sh/resource-policy}'
# Expected: keep
```

### Step 2 — Shut Down All Storage Nodes

Force-shutdown each storage node. Using `--force` combines suspend and shutdown in one
command and avoids failures when some nodes are already in a non-online state:

```bash
for NODE_ID in $(sbctl sn list | grep -v "offline" | awk '{print $2}'); do
    sbctl sn shutdown "$NODE_ID" --force
done
```

Wait for all nodes to reach `offline`:

```bash
sbctl sn list
# Expected: All nodes show "offline" status
```

### Step 3 — Uninstall the `spdk-csi` Helm Chart

```bash
helm uninstall spdk-csi --namespace simplyblock --wait
```

### Step 3.1 — Delete Orphaned Snapshot Controller

The `spdk-csi` chart deploys a `simplyblock-snapshot-controller` Deployment in
`kube-system` with `helm.sh/resource-policy: keep`. This means it survives the
`helm uninstall` above but retains stale ownership annotations pointing to the
old `spdk-csi` release. When the new `simplyblock-operator` chart tries to
install its own copy, Helm fails with:

```
rendered manifests contain a resource that already exists. Unable to continue
with install: existing resource conflict: namespace: kube-system, name:
simplyblock-snapshot-controller, existing_kind: apps/v1, Kind=Deployment,
new_kind: apps/v1, Kind=Deployment
```

**Fix**: Delete the orphaned deployment after uninstalling `spdk-csi`:

```bash
kubectl delete deployment simplyblock-snapshot-controller -n kube-system --ignore-not-found
```

### Step 4 — Uninstall the `sbcli` Helm Chart

```bash
helm uninstall sbcli --namespace simplyblock --wait
```

FDB resources survive due to the keep annotation from Step 1.

**Verify FDB still running**:

```bash
kubectl get foundationdbcluster -n simplyblock
kubectl get pods -n simplyblock -l foundationdb.org/fdb-cluster-name=simplyblock-fdb-cluster
```

### Step 4.1 — Verify FDB Cluster-Config ConfigMap

Check that the `simplyblock-fdb-cluster-config` ConfigMap survived the helm uninstall.
Admin pods mount this ConfigMap as volume `fdb-cluster-file` — without it they will be
stuck in `ContainerCreating`.

```bash
kubectl get configmap simplyblock-fdb-cluster-config -n simplyblock
```

If the ConfigMap is missing, recreate it from a running FDB pod:

```bash
# Extract the cluster file content from any FDB pod
FDB_POD=$(kubectl get pods -n simplyblock \
  -l foundationdb.org/fdb-cluster-name=simplyblock-fdb-cluster \
  -o jsonpath='{.items[0].metadata.name}')

CLUSTER_FILE=$(kubectl exec -n simplyblock "$FDB_POD" -- \
  cat /var/dynamic-conf/fdb.cluster 2>/dev/null)

# Recreate the ConfigMap
kubectl create configmap simplyblock-fdb-cluster-config \
  -n simplyblock \
  --from-literal=cluster-file="$CLUSTER_FILE"
```

**Verify**:

```bash
kubectl get configmap simplyblock-fdb-cluster-config -n simplyblock \
  -o jsonpath='{.data.cluster-file}'
# Expected: A non-empty FDB cluster connection string
```

### Step 5 — Create the Upgrade Secret

The upgrade secret tells the operator to adopt the existing cluster instead of creating a new one.

```bash
kubectl create secret generic simplyblock-<CLUSTER_CR_NAME>-upgrade \
  --namespace simplyblock \
  --from-literal=uuid=<CLUSTER_UUID> \
  --from-literal=secret=<CLUSTER_SECRET>
```

Example:

```bash
kubectl create secret generic simplyblock-simplyblock-cluster-upgrade \
  --namespace simplyblock \
  --from-literal=uuid=93cdb610-3a72-464c-b223-fe48327fc329 \
  --from-literal=secret=bdMyLkU5k4H0btBZU5H
```

> The secret name **must** match `simplyblock-<CR_NAME>-upgrade` where `CR_NAME` is the
> `metadata.name` of the StorageCluster CR you will apply in Step 7.

### Step 6 — Install the Operator Helm Chart (FDB Disabled)

> **Prerequisite — cert-manager (TLS-enabled installs only)**: If the operator chart
> enables TLS (e.g., `simplyblock-webappapi-tls` Certificate resources), `cert-manager`
> must be installed before this step. Without it, Certificate CRDs won't exist and the
> helm install will fail, or the TLS secret will never be created and admin pods will
> fail to start.
>
> ```bash
> # Check if cert-manager CRDs exist
> kubectl get crd certificates.cert-manager.io 2>/dev/null
>
> # If missing, install cert-manager
> kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
> kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=120s
> ```

Install the new operator chart with FDB creation disabled (FDB is already running):

```bash
helm upgrade --install simplyblock-operator ./charts/simplyblock-operator \
  --namespace simplyblock \
  --timeout 10m \
  --set operator.enabled=true \
  --set controlplane.foundationdb.enabled=false \
  --set image.simplyblock.repository=<TARGET_REPO> \
  --set image.simplyblock.tag=<TARGET_TAG> \
  --set image.operator.repository=simplyblock/simplyblock-operator \
  --set image.operator.tag=<OPERATOR_TAG> \
  --set controlplane.csiHostpathDriver.enabled=true \
  --set controlplane.storageclass.name=local-hostpath \
  --set csiConfig.simplybk.ip=http://simplyblock-webappapi.simplyblock:5000
```

Wait for operator pods:

```bash
kubectl wait --for=condition=Ready pods --all -n simplyblock \
  --timeout=300s --field-selector=status.phase!=Succeeded
```

### Step 6.1 — Shut Down Nodes Again (Prevent Auto-Restart)

After the operator installs, it may try to auto-restart nodes. Shut them down again
to explicitly set `auto_restart_disabled=True`:

```bash
for NODE_ID in $(sbctl sn list | grep -E "online|in_creation|reaching" | awk '{print $2}'); do
    sbctl --dev sn shutdown "$NODE_ID"
done
```

### Step 7 — Apply Custom Resources

Apply the StorageCluster, Pool, and StorageNodeSet CRs. The operator detects the upgrade
secret and adopts the existing cluster.

> **IMPORTANT — CR names must match existing backend names:**
> - The **Pool CR** `metadata.name` must match the existing pool name in the R25 cluster
>   (e.g., if the pool was created as `testing1` via `sbcli-dev pool add testing1`, the
>   Pool CR must use `name: testing1`). This allows the operator to adopt the existing pool.
> - The **StorageCluster CR** `metadata.name` must be consistent with the upgrade secret
>   name from Step 5 (`simplyblock-<CR_NAME>-upgrade`).

```yaml
# storagecluster.yaml
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: simplyblock-cluster   # Must match upgrade secret: simplyblock-<name>-upgrade
  namespace: simplyblock
spec:
  fabricType: tcp
  isSingleNode: false
  enableNodeAffinity: true
  strictNodeAntiAffinity: false
  stripe:
    dataChunks: 1
    parityChunks: 0
  warningThreshold:
    capacity: 95
    provisionedCapacity: 97
  criticalThreshold:
    capacity: 96
    provisionedCapacity: 98
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: Pool
metadata:
  name: <EXISTING_POOL_NAME>       # Must match the pool name from R25 (e.g., testing1)
  namespace: simplyblock
spec:
  clusterName: simplyblock-cluster
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageNodeSet
metadata:
  name: simplyblock-node
  namespace: simplyblock
spec:
  clusterName: simplyblock-cluster
  clusterImage: "<TARGET_REPO>:<TARGET_TAG>"
  spdkImage: "<TARGET_SPDK_IMAGE>"
  spdkProxyImage: "<TARGET_REPO>:<TARGET_TAG>"
  mgmtIfname: ens18
  dataIfname:
    - enp1s0
  maxLogicalVolumeCount: 30
  enableCpuTopology: true
  workerNodes:
    - <worker-node-1>
    - <worker-node-2>
    - <worker-node-3>
```

```bash
kubectl apply -f storagecluster.yaml -n simplyblock
```

Verify adoption (status should reflect existing UUIDs, not `in_creation`):

```bash
kubectl get storagecluster -n simplyblock -o yaml
kubectl get storagenode -n simplyblock -o yaml
kubectl get pool -n simplyblock -o yaml
```

### Step 8 — Run R25 to R26 Data Migration Script

Run the migration script inside the admin pod to update storage node fields in the database
(`lvstore_ports`, `lvol_poller_mask`, `lvstore_stack_secondary`):

```bash
ADMIN_POD=$(kubectl get pods -n simplyblock -l app=simplyblock-admin \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec -it -n simplyblock $ADMIN_POD -- bash
```

Inside the pod, run:

```python
from simplyblock_core import utils
from simplyblock_core.db_controller import DBController
db_controller = DBController()

for snode in db_controller.get_storage_nodes():
    print(f"updating storage node object: {snode.get_id()}")
    for node in db_controller.get_storage_nodes():
        if snode.get_id() == node.secondary_node_id:
            snode.lvstore_stack_secondary = node.get_id()
            break
    snode.lvstore_ports = {
        snode.lvstore: {
            "lvol_subsys_port": snode.lvol_subsys_port,
            "hublvol_port": snode.hublvol.nvmf_port
        }
    }
    if snode.lvstore_stack_secondary:
        sec = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
        snode.lvstore_ports[sec.lvstore] = {
            "lvol_subsys_port": sec.lvol_subsys_port,
            "hublvol_port": sec.hublvol.nvmf_port,
        }
    if snode.poller_cpu_cores:
        snode.lvol_poller_mask = utils.generate_mask([snode.poller_cpu_cores[-1]])
        if len(snode.poller_cpu_cores) > 1:
            snode.poller_cpu_cores = snode.poller_cpu_cores[:-1]
            snode.pollers_mask = utils.generate_mask(snode.poller_cpu_cores)

    snode.write_to_db()

print("Creating mini lvol objects")
for lvol in db_controller.get_all_lvols():
    lvol.write_to_db()

print("Creating mini Snapshots objects")
for snap in db_controller.get_snapshots():
    snap.write_to_db()

print("done")
```

**Expected**: Output ends with `done`. After running, `sbctl sn list` shows `LVS Ports`
column values populated.

### Step 9 — Patch Backend Objects with CR References

Register the K8s CR details on each backend object so the operator and backend stay in sync.

**Storage Cluster**:

```bash
export CLUSTER_UUID=<CLUSTER_UUID>
export CLUSTER_CR_NAME=simplyblock-cluster

sbctl --dev cluster set $CLUSTER_UUID cr_plural storageclusters
sbctl --dev cluster set $CLUSTER_UUID cr_namespace simplyblock
sbctl --dev cluster set $CLUSTER_UUID cr_name $CLUSTER_CR_NAME
```

**Storage Nodes** (repeat for each):

```bash
for NODE_ID in $(sbctl sn list | grep -E "offline|in_creation" | awk '{print $2}'); do
    sbctl --dev sn set "$NODE_ID" cr_plural storagenodesets
    sbctl --dev sn set "$NODE_ID" cr_namespace simplyblock
    sbctl --dev sn set "$NODE_ID" cr_name simplyblock-node
done
```

### Step 10 — Restart Storage Nodes One at a Time

Restart each storage node with the target SPDK image. Wait for the cluster to return
to `active` before restarting the next node:

```bash
export SPDK_IMAGE=<TARGET_SPDK_IMAGE>

# For each node (one at a time):
NODE_ID=<node-uuid>
sbctl -d --dev sn restart $NODE_ID --spdk-image $SPDK_IMAGE

# Wait for node online
sbctl sn list  # node should show "online"

# Wait for cluster active
sbctl cluster list  # cluster should show "active"

# Then proceed to next node
```

**Repeat for every storage node.** Do not restart the next node until the current
node is online and the cluster is active.

### Step 11 — Restart Workload Pods

Once all storage nodes are online and the cluster is active, restart application
pods to re-establish NVMe connections:

```bash
kubectl rollout restart deployment/<workload> -n <namespace>
```

Or for all deployments in a namespace:

```bash
kubectl get deployments -n <namespace> -o name | \
  xargs -I{} kubectl rollout restart {} -n <namespace>
```

---

## Phase 4: Post-Upgrade Validation

### 4.1 Verify Old Data Integrity (MD5)

Re-run FIO in verify-only mode on the original PVCs to confirm the data written
in Phase 2 is intact after migration:

```ini
[global]
name=post-upgrade-verify
filename_format=/spdkvol/fio-data.$jobnum
rw=read
bs=4k
iodepth=1
direct=1
ioengine=libaio
size=1G
numjobs=1
verify=md5
verify_only
verify_dump=1
verify_fatal=1

[job1]
```

**Expected**: All FIO verify-only jobs complete with 0 errors. The data written
pre-upgrade is intact.

### 4.2 Run FIO on Existing (Old) PVCs

Run a fresh FIO with `verify=md5` on the old PVCs to confirm read/write IO works
post-upgrade:

```ini
[global]
name=post-upgrade-io
filename_format=/spdkvol/fio-post.$jobnum
rw=randrw
rwmixread=50
bs=4k
direct=1
ioengine=libaio
size=1G
numjobs=1
time_based
runtime=120
verify=md5
verify_dump=1
verify_fatal=1

[job1]
```

**Expected**: IO completes successfully with 0 verify errors.

### 4.3 Create New Snapshots and Clones on Old PVCs

Take new snapshots of the old PVCs (post-upgrade data) and create clones from them:

- [ ] Create VolumeSnapshot for each old PVC
- [ ] Wait for snapshot to be ready
- [ ] Create clone PVC from the snapshot
- [ ] Run FIO on clone with `verify=md5`

**Expected**: Snapshots and clones work correctly on the upgraded cluster.

### 4.4 Create New PVCs (Fresh Provisioning)

Verify that new volume provisioning works end-to-end on the upgraded cluster:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: post-upgrade-new-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
  storageClassName: simplyblock-csi-sc
```

- [ ] PVC should bind successfully
- [ ] Run FIO with `verify=md5` — should complete with 0 errors

### 4.5 Snapshot and Clone New PVCs

- [ ] Create VolumeSnapshot of the new PVC
- [ ] Wait for snapshot ready
- [ ] Create clone PVC from snapshot
- [ ] Run FIO on clone with `verify=md5`

**Expected**: Full snapshot/clone lifecycle works on newly provisioned volumes.

### 4.6 Node Outage Test

Verify HA works post-upgrade by simulating node failures:

**Single node outage**:

1. Identify a storage node hosting one of the PVCs
2. Shut down that node: `sbctl sn shutdown <NODE_ID>`
3. Verify FIO continues on the PVC (HA should redirect IO)
4. Restart the node: `sbctl sn restart <NODE_ID>`
5. Wait for cluster active and node online
6. Verify all FIO jobs pass

**Multi-node outage** (if running with parity, e.g., ndcs=2 npcs=1):

1. Shut down two nodes simultaneously
2. Verify IO continues on PVCs with sufficient redundancy
3. Restart nodes one at a time
4. Wait for cluster active

**Expected**: IO continues without errors during single-node outage.
After node restart, cluster returns to active and all data is intact.

### 4.7 Final Checklist

| Check | Command | Expected |
|-------|---------|----------|
| Cluster active | `sbctl cluster list` | `active` |
| All nodes online | `sbctl sn list` | All `online` |
| LVS Ports populated | `sbctl sn list` | Non-empty values |
| Old PVCs bound | `kubectl get pvc` | All `Bound` |
| New PVCs bound | `kubectl get pvc` | All `Bound` |
| Snapshots ready | `kubectl get volumesnapshot` | All `readyToUse: true` |
| Clones bound | `kubectl get pvc` (clone PVCs) | All `Bound` |
| Operator CRs adopted | `kubectl get storagecluster -o yaml` | Shows existing UUIDs |
| FIO verify pass | FIO job logs | `0 verify errors` |
| CR refs patched | `sbctl --dev cluster get <UUID>` | `cr_name`, `cr_namespace` set |

---

## Rollback

If the upgrade fails **before Step 3** (Helm uninstall), re-install the original Helm charts.

After Step 3, rollback requires:

1. Restoring from the FDB PVC data
2. Re-installing the original `sbcli` and `spdk-csi` charts
3. Manual recovery of storage node state

> Full rollback procedures are not covered here. The recommendation is to snapshot/backup
> the FDB PVC before starting the maintenance window.

---

## Automated Test

The E2E test `K8sNativeMajorUpgrade` in
[k8s_major_upgrade.py](e2e/e2e_tests/upgrade_tests/k8s_major_upgrade.py)
automates all four phases. It is triggered by the `k8s-native-upgrade.yaml` workflow
with `UPGRADE_TYPE=r25-to-r2x`.
