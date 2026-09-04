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
| Phase 3 | Maintenance window — 11-step migration from Helm to Operator | **Yes** |
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

> **Also create an XFS StorageClass on the same pool** (`fs_type: xfs` — set via the CSI
> driver's mount options / StorageClass parameters) and use it for any PVC you plan to keep
> mounted across the upgrade. **Why**: R25's CSI container ships a newer `mkfs.ext4` that
> creates filesystems with the `FEATURE_C12` feature flag. After the upgrade, `NodeStageVolume`
> runs `e2fsck`/`tune2fs` from the **host OS**, which on RHCOS ships e2fsprogs 1.46.5 — too old
> to recognize `FEATURE_C12`. The mount then fails with `unsupported feature(s): FEATURE_C12`
> and the volume is left inaccessible (`input/output error`). XFS has no such feature-flag
> mismatch and is unaffected. This is tracked as an open product bug — see Operational Note 7.

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

### Step 1 — Ensure FDB Resources Have `helm.sh/resource-policy: keep` in Chart

There are 9 resources that must survive `helm uninstall`: 8 FDB resources, plus the old
prometheus configmap (needed in Step 6.0.1 below):

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
| ConfigMap | `<sbcli-release-name>`-simplyblock-prometheus-config |

> **Why the ConfigMap?** The `simplyblock-fdb-cluster-config` ConfigMap contains the FDB
> cluster connection file. Admin pods mount it as volume `fdb-cluster-file`. If this
> ConfigMap is deleted during `helm uninstall sbcli`, admin pods will be stuck in
> `ContainerCreating` and all `sbcli`/`sbctl` commands will fail.

> **Why the prometheus ConfigMap?** The R25 `sbcli` chart's prometheus configmap
> (e.g. `sbcli-simplyblock-prometheus-config`) holds the cluster's monitoring `basic_auth`
> credentials. The R26 operator chart creates its own fresh configmap
> (`simplyblock-prometheus-config`) with empty username/password. If the old configmap is
> deleted on `helm uninstall`, there is nowhere left to read the old credentials from, and
> Step 6.0.1 (migrate them into the new configmap) has nothing to migrate.

> **IMPORTANT: `kubectl annotate` on live resources is NOT effective for `helm uninstall`.**
> Helm reads annotations from its stored release manifest (in `sh.helm.release.v1.*`
> secrets), not from the live object in etcd. Annotating a live resource with
> `kubectl annotate` only patches etcd — Helm's copy is unchanged and it will still
> delete the resource on uninstall.
>
> **The correct approach** is to add `helm.sh/resource-policy: keep` directly in the
> Helm chart templates so it is baked into the stored manifest. This requires a chart
> change + `helm upgrade` before uninstall.

#### Option A — Chart template fix (correct way, requires chart change)

Add to each FDB template in the chart:

```yaml
metadata:
  annotations:
    "helm.sh/resource-policy": keep
```

Then run `helm upgrade` to persist the annotation into Helm's release secret before
running `helm uninstall`.

#### Option B — Patch the Helm release secret directly (workaround without chart change)

If the chart cannot be modified (e.g., upgrading from an older R25 chart), you can
patch the stored Helm release manifest directly:

```bash
# 1. Get the latest Helm release secret
SECRET_NAME=$(kubectl get secrets -n simplyblock -l owner=helm,name=sbcli \
  --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')

# 2. Decode, decompress, patch, recompress, re-encode the release data
kubectl get secret "$SECRET_NAME" -n simplyblock -o jsonpath='{.data.release}' \
  | base64 -d | base64 -d | gzip -d > /tmp/helm-release.json

# 3. Inject keep annotation into FDB resource manifests in the release
# (This is complex — use the chart fix if possible)
```

#### Option C — `kubectl annotate` live resources (limited effectiveness)

> **WARNING**: This only works if Helm happens to check live objects, which standard
> Helm does NOT do. Listed here for reference but **Option A is strongly recommended**.

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

**Verify** (check Helm's stored manifest, not just live object):

```bash
# Check live object (may not reflect what Helm sees)
kubectl get deployment simplyblock-fdb-controller-manager -n simplyblock \
  -o jsonpath='{.metadata.annotations.helm\.sh/resource-policy}'
# Expected: keep

# Check Helm's stored manifest (this is what actually matters)
helm get manifest sbcli -n simplyblock 2>/dev/null | grep -A5 "simplyblock-fdb-controller-manager" | grep resource-policy
# Expected: "helm.sh/resource-policy": keep
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

### Step 2.1 — Disable Auto-Restart on All Nodes (Safety Net)

> **Status**: The R26 operator now skips creating `node_restart` tasks for nodes
> that were already offline before the operator started. This makes Step 2.1
> optional in most cases. However, if upgrading to an older R26 build or if the
> fix regresses, this step prevents the operator's tasks-runner from creating
> stale `node_restart` tasks that block the explicit `sn restart` in Step 10
> (there is no `--force` flag for restart).

```bash
for NODE_ID in $(sbctl sn list --json | jq -r '.[].UUID'); do
    sbctl --dev sn set "$NODE_ID" auto_restart_disabled true
done
```

If stale restart tasks already exist (e.g. from a previous failed run), cancel
them before proceeding:

```bash
# List tasks
sbctl cluster list-tasks "$CLUSTER_ID" --limit 0

# Cancel any running/new node_restart tasks
for TASK_ID in $(sbctl cluster list-tasks "$CLUSTER_ID" --json --limit 0 \
  | jq -r '.[] | select(.function=="node_restart" and (.status=="running" or .status=="new")) | .id'); do
    sbctl cluster cancel-task "$CLUSTER_ID" "$TASK_ID"
done
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

FDB resources survive because the chart templates include `helm.sh/resource-policy: keep`
annotations (see Step 1). If the chart does NOT have these annotations, FDB will be
deleted by `helm uninstall` and the database will be destroyed.

**Verify FDB still running** (CRITICAL — if any of these fail, STOP and investigate):

```bash
# 1. FoundationDBCluster CR must still exist
kubectl get foundationdbcluster simplyblock-fdb-cluster -n simplyblock
# Expected: Shows the cluster resource

# 2. FDB controller-manager deployment must still exist
kubectl get deployment simplyblock-fdb-controller-manager -n simplyblock
# Expected: Shows the deployment

# 3. FDB pods must still be running
kubectl get pods -n simplyblock -l foundationdb.org/fdb-cluster-name=simplyblock-fdb-cluster
# Expected: Multiple FDB pods in Running state

# 4. FDB CRDs must still exist
kubectl get crd foundationdbclusters.apps.foundationdb.org
# Expected: Shows the CRD
```

> **If FDB resources are missing**: The chart likely does not have `helm.sh/resource-policy: keep`
> in its templates. This must be fixed in the chart (see Step 1, Option A). Note that
> `kubectl annotate` on live objects does NOT protect against `helm uninstall` — Helm reads
> from its stored release manifest, not from etcd.

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
> # If missing, install cert-manager via the Jetstack Helm chart (pinned version — the
> # automated test uses this path, not the raw upstream manifest, to avoid drift from
> # an unpinned "latest" release)
> helm repo add jetstack https://charts.jetstack.io
> helm repo update
> helm upgrade --install cert-manager jetstack/cert-manager \
>   --namespace cert-manager --create-namespace \
>   --version v1.13.0 --set installCRDs=true
> kubectl wait --for=condition=Ready pods --all -n cert-manager --timeout=120s
> ```
>
> If cert-manager was left in a broken state by a previous failed attempt, `helm uninstall
> cert-manager -n cert-manager --no-hooks --timeout 60s` and retry — a stale release can make
> the install above fail silently.

> **IMPORTANT — Apply CRDs explicitly before `helm upgrade --install` on a reused cluster.**
> Helm v3 only installs CRDs from a chart's `crds/` directory on a fresh `helm install`.
> If a `simplyblock-operator` release already exists on this cluster (e.g. a previous failed
> upgrade attempt), `helm upgrade --install` performs an **upgrade**, and Helm **silently skips
> all CRD installation** — any CRD added to the chart since the original install (e.g.
> `StoragePool`) is never registered. The operator then has nothing to reconcile, no
> `StorageNodeSet` DaemonSet is created, and every node restart in Step 10 fails waiting for an
> agent that was never scheduled. Apply the CRDs directly first, regardless of install vs. upgrade:
>
> ```bash
> kubectl apply --server-side --force-conflicts -f ./charts/simplyblock-operator/crds/
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

### Step 6.0.1 — Migrate Prometheus Credentials to the New ConfigMap

> **Why**: The new operator chart's prometheus configmap is created fresh with an empty
> `basic_auth` username/password, while the old `sbcli` chart's configmap (kept in Step 1)
> still has the real credentials. The new chart also switches prometheus to HTTPS with mTLS,
> so the configmaps can't simply be swapped — the credentials must be copied across. Skipping
> this step leaves prometheus running with empty auth post-upgrade.

```bash
OLD_CM=sbcli-simplyblock-prometheus-config   # <sbcli-release-name>-simplyblock-prometheus-config
NEW_CM=simplyblock-prometheus-config

# 1. Extract username/password from the old configmap's basic_auth block
kubectl get configmap "$OLD_CM" -n simplyblock \
  -o jsonpath='{.data.prometheus\.yml}' > /tmp/old-prometheus.yml
# Parse `basic_auth: { username: ..., password: ... }` out of /tmp/old-prometheus.yml

# 2. Inject those values into the new configmap's prometheus.yml (empty username/password
#    fields), then re-apply it
kubectl get configmap "$NEW_CM" -n simplyblock -o json > /tmp/new-cm.json
# Edit /tmp/new-cm.json: set data."prometheus.yml" username/password to the extracted values
kubectl replace -f /tmp/new-cm.json -n simplyblock

# 3. Restart prometheus to pick up the new config
kubectl delete pod simplyblock-prometheus-0 -n simplyblock --ignore-not-found
```

**Expected**: The prometheus pod restarts and scrapes successfully using the migrated
credentials — check `kubectl logs simplyblock-prometheus-0 -n simplyblock` for auth errors.

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
>
> **IMPORTANT — CR schema drift between releases.** The operator's CRD schema has changed
> shape across releases — fields get moved or dropped with strict decoding, so a CR generated
> against an older schema is rejected outright:
> - `StorageCluster`: does **not** accept `isSingleNode` or `strictNodeAntiAffinity`; **does**
>   accept `maxSubsystemCount` (moved here from `StorageNodeSet`).
> - `StorageNodeSet`: no longer accepts `maxSubsystemCount`.
>
> A field mismatch fails with `strict decoding error: unknown field "spec.xxx"` — confirm your
> CR YAML against the CRD actually installed on the target release before applying, don't reuse
> a template from a previous version's docs.
>
> **IMPORTANT — verify CR application without merging stderr into stdout.** If your automation
> checks "does this CR exist" via `kubectl get ... 2>&1` and a substring match on the CR name,
> it will get a **false positive**: a `NotFound` error message contains the CR name as a
> substring (`storageclusters.storage.simplyblock.io "simplyblock-cluster" not found`), so the
> check passes even though the CR was never created. Keep stdout and stderr separate and check
> stderr explicitly for `not found` / `could not find the requested resource`, and fail fast on
> `BadRequest` / `strict decoding error` rather than logging a warning and continuing — a CR
> that silently failed to apply here surfaces much later, as an inexplicable node-restart
> timeout in Step 10 with no obvious connection back to this step.

```yaml
# storagecluster.yaml
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: simplyblock-cluster   # Must match upgrade secret: simplyblock-<name>-upgrade
  namespace: simplyblock
spec:
  fabricType: tcp
  enableNodeAffinity: true
  stripe:
    dataChunks: 1
    parityChunks: 0
  warningThreshold:
    capacity: 95
    provisionedCapacity: 97
  criticalThreshold:
    capacity: 96
    provisionedCapacity: 98
  maxSubsystemCount: 30          # a.k.a. max_lvol — moved here from StorageNodeSet
  vcpuCount: 16                  # SPDK vCPUs to allocate per node (see note below)
---
apiVersion: storage.simplyblock.io/v1alpha1
kind: StoragePool
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
  skipKubeletConfiguration: false   # true on Talos — the operator can't edit kubelet config there
  enableCpuTopology: true           # false on Talos, for the same reason
  workerNodes:
    - <worker-node-1>
    - <worker-node-2>
    - <worker-node-3>
```

> **`vcpuCount`**: computed as a percentage of each node's CPU count (e.g. 50% on OpenShift),
> not a fixed value — pick a number the operator's CRD will accept for your node sizing rather
> than copying `16` verbatim. **`skipKubeletConfiguration` / `enableCpuTopology`**: on Talos
> nodes the operator cannot modify kubelet configuration directly, so these flip relative to a
> standard OpenShift/vanilla K8s install — detect Talos before deciding the values.

```bash
kubectl apply -f storagecluster.yaml -n simplyblock
```

Verify adoption (status should reflect existing UUIDs, not `in_creation`):

```bash
kubectl get storagecluster -n simplyblock -o yaml
kubectl get storagenode -n simplyblock -o yaml
kubectl get storagepool -n simplyblock -o yaml
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

> **Provisional**: the test code carries a TODO that `cr_plural` may need to change from
> `storagenodesets` to `storagenodes` (with `cr_name` set to the individual `StorageNode` CR
> name, not the shared `StorageNodeSet` name) pending confirmation from the operator team.
> Treat the values above as current, not final — check the operator's actual CR structure on
> the target release before relying on this in a scripted upgrade.

### Step 9.1 — Cancel Stale Restart Tasks (If Needed)

> **Status**: With the R26 operator fix (see Step 2.1), stale tasks should not
> appear. This step is a safety net for older operator builds or regressions.

If any `node_restart` tasks were created by the operator's tasks-runner between
Step 6 (operator install) and Step 10, they will block `sn restart`. Check and
cancel them:

```bash
# Check for stale node_restart tasks
sbctl cluster list-tasks "$CLUSTER_ID" --limit 0

# Cancel any that are running or new
for TASK_ID in $(sbctl cluster list-tasks "$CLUSTER_ID" --json --limit 0 \
  | jq -r '.[] | select(.function=="node_restart" and (.status=="running" or .status=="new")) | .id'); do
    echo "Cancelling stale task: $TASK_ID"
    sbctl cluster cancel-task "$CLUSTER_ID" "$TASK_ID"
done
```

### Step 10 — Restart Storage Nodes One at a Time

Restart each storage node with the new SPDK image and proxy image.

> **IMPORTANT — Maintenance upgrade**: In a maintenance upgrade all nodes start
> offline. The cluster **cannot** become `active` until every node is back online.
> Do **not** wait for cluster `active` between individual node restarts — only
> wait for each node to reach `online`, then proceed to the next. Check cluster
> `active` only after **all** nodes have been restarted.

```bash
export SPDK_IMAGE=<TARGET_SPDK_IMAGE>
export SPDK_PROXY_IMAGE=<TARGET_DOCKER_IMAGE>

for NODE_ID in $(sbctl sn list --json | jq -r '.[].UUID'); do
    echo "Restarting node: $NODE_ID"
    sbctl -d --dev sn restart "$NODE_ID" \
        --spdk-image "$SPDK_IMAGE" \
        --spdk-proxy-image "$SPDK_PROXY_IMAGE"

    # Wait for this node to come online (up to 10 minutes)
    while ! sbctl sn list --json | jq -e ".[] | select(.UUID==\"$NODE_ID\" and .Status==\"online\")" > /dev/null 2>&1; do
        sleep 5
    done
    echo "  Node $NODE_ID is online"

    sleep 10  # brief pause before next node
done
```

> **Note — Admin-control pod recycling**: During Step 10, the R26 operator may
> recycle the `simplyblock-admin-control` pods as nodes come back online
> (deployment rollout). If `kubectl exec` commands fail with `error: unable to
> upgrade connection: pod does not exist`, wait a few seconds and retry with the
> new pod name:
>
> ```bash
> ADMIN_POD=$(kubectl get pods -n simplyblock -l app=simplyblock-admin-control \
>   -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
> ```

> **Watch for — restart repeatedly using the wrong SPDK image.** A run on 2026-08-26 saw a
> node cycle `offline -> in_restart -> offline` seven times over 43 minutes here: `sn restart`
> was called with `--spdk-image <TARGET>`, but the tasks-runner's `spdk_process_start` call used
> the node's old, stored R25 image instead. The R25 SPDK is incompatible with the already-running
> R26 control plane, so the process never came up and every attempt timed out after 300s. If a
> node won't come online in this step, check `tasks-runner-restart.log` for the `spdk_image`
> value actually sent to `spdk_process_start` and confirm it matches `--spdk-image`, not the
> node's pre-upgrade value — don't assume repeated identical timeouts here are a transient
> infra issue.

### Step 10.1 — Wait for Cluster Active and Health Checks

After all nodes are restarted, wait for the cluster to become `active` and for
all node health checks to settle to `True`. The `health_check` field may
remain `None` or `False` for 30-60 seconds after a node comes online while the
monitoring loop catches up.

```bash
# Wait for cluster active
while [ "$(sbctl cluster list --json | jq -r '.[0].Status')" != "ACTIVE" ]; do
    echo "Waiting for cluster to become active..."
    sleep 10
done
echo "Cluster is active"

# Wait for all nodes to report health_check=True (up to 2 minutes)
TIMEOUT=120
while [ $TIMEOUT -gt 0 ]; do
    UNHEALTHY=$(sbctl sn list --json | jq '[.[] | select(.Health != "True")] | length')
    if [ "$UNHEALTHY" -eq 0 ]; then
        echo "All nodes are healthy"
        break
    fi
    echo "  $UNHEALTHY node(s) still settling health_check, retrying in 10s..."
    sleep 10
    TIMEOUT=$((TIMEOUT - 10))
done

if [ $TIMEOUT -le 0 ]; then
    echo "WARNING: Some nodes still have health_check != True after 120s"
    sbctl sn list
fi
```

### Step 10.2 — Activate v2 Write Protection, Then Restart Nodes Again

**Required after every R25 to R26 upgrade.** An upgraded cluster's existing
distribs stay on **v1** write protection — only freshly created clusters start
on v2. Activating it is a two-part step: switch, then restart every node once
more so the v2 generation is persisted and the nodes come back under it.

Run this only after Step 10.1 reports the cluster `ACTIVE` and every node
healthy — `switch-write-protection` sends the runtime RPC to all online nodes
and records v2 only once every one of them accepts it.

```bash
CLUSTER_ID=$(sbctl cluster list --json | jq -r '.[0].UUID')

# 1. Switch the cluster to v2 write protection
sbctl cluster switch-write-protection "$CLUSTER_ID"
sleep 30

# 2. Restart every storage node again, one at a time.
#    --force is REQUIRED here: the nodes are already online and healthy, so a
#    plain restart is refused as unnecessary.
for NODE_ID in $(sbctl sn list --json | jq -r '.[].UUID'); do
    echo "Post-switch restart of $NODE_ID"
    sbctl --dev -d sn restart "$NODE_ID" --force

    # wait for it to come back before moving to the next node
    while [ "$(sbctl sn list --json | jq -r --arg id "$NODE_ID"               '.[] | select(.UUID==$id) | .Status')" != "online" ]; do
        sleep 10
    done
    echo "  $NODE_ID back online"
    sleep 30
done

# 3. Confirm the cluster is active again
sbctl cluster list
```

If any node fails to come back online, stop and investigate before continuing —
do not proceed to Step 11 with a node down.

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

## Operational Notes (Lessons Learned from E2E Runs)

These notes capture real-world issues found during automated and manual upgrade
testing that operators should be aware of.

### 1. Do NOT wait for cluster active between node restarts (maintenance path)

In a maintenance upgrade, all storage nodes start offline. The cluster enters
`suspended` state because it has no quorum. **The cluster cannot become `active`
until all (or most) nodes are back online.** If you wait for cluster `active`
after restarting each individual node, you will hang indefinitely after the
first node.

**Correct approach**: Restart each node one at a time, wait only for that node
to reach `online` status, then immediately start the next. Only check for
cluster `active` after **all** nodes have been restarted (Step 10.1).

This does NOT apply to rolling upgrades, where only one node is down at a time
and the cluster stays active throughout.

### 2. Health check settling delay after restart

After a node restarts, its `health_check` field in the database transitions
through `None` → `False` → `True` as the monitoring loop catches up. This can
take 20-60 seconds. **Do not assert `health_check == True` immediately** after
a node comes online — poll with a timeout (120 seconds recommended).

### 3. Admin-control pod recycling during node restarts

When storage nodes come back online, the R26 operator may trigger a rollout of
the `simplyblock-admin-control` deployment. If your automation uses `kubectl exec`
to run `sbctl` commands via a cached admin pod name, the cached name may become
stale. Symptoms:

- `error: unable to upgrade connection: pod does not exist`
- `json.JSONDecodeError: Expecting value: line 1 column 1` (empty stdout)

**Mitigation**: Re-discover the admin pod name if kubectl exec fails, and retry
the command. The E2E framework handles this automatically.

### 4. Stale `node_restart` tasks blocking `sn restart`

After the R26 operator installs (Step 6), its tasks-runner may detect offline
nodes and create `node_restart` tasks. When you later run `sn restart` in
Step 10, it may fail with a conflict because the stale task is still
running/pending.

**Mitigation**: The R26 operator now skips creating restart tasks for nodes that
were already offline. For older builds, use Step 2.1 (disable auto-restart) and
Step 9.1 (cancel stale tasks) before Step 10.

### 5. StorageNodeSet CR must adopt existing nodes

The R26 operator's StorageNodeSet reconciler must detect pre-existing storage
nodes from R25 and adopt them (same ports: 8080-8085). If the operator creates
new nodes instead (ports 4420+), the old data is inaccessible. This was a known
operator bug — ensure the operator version includes the StorageNode CR adoption
fix.

### 6. Preserve resources on failure for debugging

When an upgrade test fails, avoid cleaning up PVCs, pools, and lvols in the
teardown. Use `--preserve_resources_on_failure true` in the test runner to keep
all K8s resources intact for post-mortem analysis.

### 7. ext4 `FEATURE_C12` incompatibility (open product bug)

R25's CSI container ships a newer `mkfs.ext4` (e2fsprogs >= 1.47.0) that formats volumes
with the `FEATURE_C12` flag (`orphan_file`). After the upgrade, `NodeStageVolume` runs
`e2fsck`/`tune2fs` from the **host OS** — RHCOS ships e2fsprogs 1.46.5, which doesn't
recognize `FEATURE_C12`. Every ext4 volume formatted pre-upgrade fails to remount:

```
/dev/nvme1n1 has unsupported feature(s): FEATURE_C12
e2fsck: Get a newer version of e2fsck!
tune2fs: Filesystem has unsupported read-only feature(s)
```

Confirmed across 5+ runs (Aug 15, 16, 24, 25). It affects **all** ext4 volumes, originals and
clones alike, since they were all formatted by the same CSI.

**Mitigation (test-side, in place)**: create both an ext4 and an XFS StorageClass on the same
pool (Phase 2.2) and use the XFS one for volumes that must survive the upgrade — XFS has no
equivalent feature-flag mismatch.

> **Known gap**: `_create_pvcs_with_fio()` in `k8s_major_upgrade.py` still calls
> `random.choice()` between the ext4 and XFS StorageClasses for each PVC (see the Aug 25 run,
> where PVC index 1 randomly drew ext4 and failed). For the `r25-to-r2x` maintenance-window
> path specifically, this should be forced to XFS-only rather than randomized — flagged here,
> not yet fixed as of this writing.

**Real fix (product team, still open)**: pin `mkfs.ext4` to skip `orphan_file`, bundle a
compatible `e2fsck`/`tune2fs` in the CSI container, or skip the fsck/tune2fs pass entirely
when re-staging a volume the same CSI previously formatted.

### 8. Helm v3 skips CRD installation on `helm upgrade`

`helm install` installs everything under a chart's `crds/` directory; `helm upgrade`
(including the upgrade half of `--install`) does not touch CRDs at all. On a cluster that
still has a stale `simplyblock-operator` release from a previous failed upgrade attempt,
`helm upgrade --install` takes the upgrade path and any CRD added to the chart since the
original install (e.g. `StoragePool`) is never registered. The operator then has nothing to
reconcile, no `StorageNodeSet` DaemonSet is created, and Step 10 hangs waiting for node
agents that were never scheduled — with no error anywhere pointing back at the missing CRD.

**Fix (in the test)**: apply the chart's CRDs explicitly and unconditionally before the helm
install/upgrade (see the callout in Step 6): `kubectl apply --server-side --force-conflicts
-f {crds_dir}/`.

### 9. CR-existence checks must not merge stderr into stdout

A `kubectl get <cr> ... 2>&1` followed by a substring match on the CR name will false-positive
on a `NotFound` error, because the error message itself contains the CR name:
`storageclusters.storage.simplyblock.io "simplyblock-cluster" not found`. A check written this
way logs "Verified ... exists" for a CR that was never created — see the callout in Step 7.
Keep stdout/stderr separate, and fail fast on `strict decoding error` / `NotFound` /
`BadRequest` rather than warning and continuing.

### 10. User lvol NVMe-oF listeners missing after restart (historical)

Confirmed on 2026-08-11: after the maintenance-window restart, the storage-node-controller
recreated user lvol NVMe-oF subsystems and namespaces but never called
`nvmf_subsystem_add_listener` for them — only device, JM, and hublvol subsystems got a
listener. Every pre-upgrade PVC then failed to reconnect (`connection refused` /
`invalid arguments/configuration`) even though the cluster and all nodes reported healthy.
Not reproduced in any run since (Aug 24 onward) — appears fixed upstream, but worth
re-confirming if PVC reconnect ever silently regresses after a restart step.

### 11. `sn restart --spdk-image` not honored (fixed)

Confirmed on 2026-08-26: a node cycled `offline -> in_restart -> offline` seven times because
`spdk_process_start` used the node's old, stored SPDK image instead of the `--spdk-image`
value passed to `sn restart`. Fixed — `snode.spdk_image` is now set from the passed
`--spdk-image` before `spdk_process_start` is called. If Step 10 ever times out repeatedly on
the same node again, check `tasks-runner-restart.log` for the actual `spdk_image` used first,
rather than assuming it's an infra flake.

---

## Automated Test

The E2E test `K8sNativeMajorUpgrade` in
[k8s_major_upgrade.py](e2e/e2e_tests/upgrade_tests/k8s_major_upgrade.py)
automates all four phases. It is triggered by the `k8s-native-upgrade.yaml` workflow
with `UPGRADE_TYPE=r25-to-r2x`.

### Test-side issues fixed since initial automation

- Sequential PVC verification serialized 12 FIO jobs behind a 600s timeout each, adding up
  to ~2h of dead time on any real failure — verification now runs in parallel
  (`ThreadPoolExecutor`) (Note above, run history Aug 16).
- CRDs are now applied explicitly before the operator chart install/upgrade, instead of
  relying on Helm's install-only CRD semantics (Note 8).
- CR-existence checks no longer merge stderr into stdout, and now fail fast on
  `strict decoding error` / `NotFound` / `BadRequest` instead of logging a warning and
  continuing (Note 9).
- CR YAML kept in sync with CRD schema drift (`isSingleNode`/`strictNodeAntiAffinity` removed,
  `maxSubsystemCount` moved to `StorageCluster`) (Step 7 callout).
- Worker-node NVMe/mount cleanup shell command fixed (was silently a no-op due to a nested
  quoting bug — `nvme disconnect-all` never actually ran on any worker).
- `sn restart` in Step 10 now also passes `--spdk-proxy-image` alongside `--spdk-image`, so
  the proxy is upgraded together with SPDK rather than staying on the old version.

### Known gaps / open items for dev

- **Open product bug**: ext4 `FEATURE_C12` incompatibility (Note 7) — mitigated in the test
  via an XFS StorageClass, but PVC-to-StorageClass assignment is still randomized rather than
  forced to XFS-only for this upgrade path, so ext4-formatted PVCs can still fail Phase 4.1
  intermittently.
