### chart installation
```
helm upgrade --install simplyblock-operator ./ --namespace simplyblock \
    --create-namespace \
    --set operator.enabled=true \
    --set image.operator.repository=docker.io/simplyblock/simplyblock-operator \
    --set image.operator.tag="main" \
    --set image.simplyblock.repository=docker.io/simplyblock/simplyblock \
    --set image.simplyblock.tag="main"
```

### create cluster 1

Minio instance will be `minio` namespace. 
Cluster1 will be created in `cluster1` namespace
Cluster2 will be created in `cluster2` namespace

the parameters provided below are the just for reference. Please change the parameters as per your requirements

#### pre-requisites

To test backups to S3. Make sure that minio instance exists. This will create minio instance in `minio` namespace.

```sh
kubectl create ns minio

kubectl -n minio create deployment minio \
  --image=minio/minio \
  -- /bin/sh -c "minio server /data --console-address :9001"

kubectl -n minio expose deploy/minio --port 9000

kubectl -n minio set env deploy/minio \
  MINIO_ROOT_USER=minioadmin \
  MINIO_ROOT_PASSWORD=minioadmin123
```

### Create Storage Cluster1
```
kubectl create namespace cluster1
```

The credentials to access minio are stored in a Kubernetes secret. This secret must exist in the namespace before creating `StorageCluster`

```
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: Secret
metadata:
  name: backup-credentials
  namespace: cluster1
type: Opaque
stringData:
  access_key_id: minioadmin
  secret_access_key: minioadmin123
EOF
```

create cluster
```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageCluster
metadata:
  name: simplyblock-cluster
  namespace: cluster1
spec:
  clusterName: simplyblock-cluster
  mgmtIfname: eth0
  isSingleNode: false
  enableNodeAffinity: false
  strictNodeAntiAffinity: false
  warningThreshold:
    capacity: 80
    provisionedCapacity: 120
  criticalThreshold:
    capacity: 90
    provisionedCapacity: 150
  backup:
    credentialsSecretRef:
      name: backup-credentials
    localEndpoint: http://minio.minio.svc.cluster.local:9000
    localTesting: true
    secondaryTarget: 0
    snapshotBackups: true
    withCompression: false
EOF
```

create storage nodes
```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageNode
metadata:
  name: simplyblock-node
  namespace: cluster1
spec:
  clusterName: simplyblock-cluster
  clusterImage: simplyblock/simplyblock:main
  mgmtIfname: eth0
  maxLogicalVolumeCount: 10
  partitions: 0
  corePercentage: 65
  coreIsolation: false
  skipKubeletConfiguration: false
  enableCpuTopology: false
  workerNodes:
    - manohar-storage-node-1
    - manohar-storage-node-2
    - manohar-storage-node-3
EOF
```

create storage pool
```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: Pool
metadata:
  name: pool3
  namespace: cluster1
spec:
  name: pool3
  clusterName: simplyblock-cluster
EOF
```

This will create a storage class with name: `simplyblock-simplyblock-cluster-pool3`

### Create Pod and PVC

Now use the storageclass created above to create a PVC and Pod

```
kubectl apply -f - <<'EOF'
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: spdkcsi-pvc
  namespace: cluster1
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: simplyblock-simplyblock-cluster-pool3

---
kind: Pod
apiVersion: v1
metadata:
  name: spdkcsi-test
  namespace: cluster1
spec:
  containers:
  - name: alpine
    image: alpine:3
    imagePullPolicy: "IfNotPresent"
    command: ["sleep", "365d"]
    volumeMounts:
    - mountPath: "/spdkvol"
      name: spdk-volume
  volumes:
  - name: spdk-volume
    persistentVolumeClaim:
      claimName: spdkcsi-pvc
EOF
```

### Backup

create a Backup for the above PVC: `spdkcsi-pvc`
```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: StorageBackup
metadata:
  name: spdkcsi-pvc-backup1
  namespace: cluster1
spec:
  clusterName: simplyblock-cluster
  pvcRef:
    name: spdkcsi-pvc
EOF
```

Get the status of the backup. Please note that the first backup takes some time.

```
$ kubectl -n cluster1 get storagebackup
NAME                  PHASE        PVC           BACKUPID                               SNAPSHOT                     AGE
spdkcsi-pvc-backup1   Done   spdkcsi-pvc   7fab02f8-03f6-4e76-a9ac-78b63b1ce8ef   backup-spdkcsi-pvc-backup1         3m
```

### Backup Restore to same cluster

Now, lets restore the backup into a new PVC (configurable through `spec.pvcTemplate`). We support restoring a backup to PVC with configuations:

1. To a different pool --> specify `spec.targetPool` --> Defaults to Pool associated with Source Backup's PVC.
2. To a different StorageNode --> specify `spec.targetNode` --> Defaults to the node that originally held the backup.


```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: BackupRestore
metadata:
  namespace: cluster1
  name: backuprestore-1
spec:
  clusterName: simplyblock-cluster
  targetNode: <>
  backupRef:
    name: spdkcsi-pvc-backup1
  pvcTemplate:
    metadata:
      name: backuprestore-sample-pvc2
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 10Gi
EOF
```

The state of the backuprestore can be observed by running. 
```
$ kubectl -n cluster1 get backuprestore
NAME              PHASE   BACKUP                PVC                         AGE
backuprestore-1   Done    spdkcsi-pvc-backup1   backuprestore-sample-pvc2   79s
```
The state of the `BackupRestore` object will move from `InProgress` -> `PVCBinding` --. `Done`. Once the BackupRestore is done, This will create a new PVC `backuprestore-sample-pvc2`

```
$ kubectl -n cluster1 get pvc
NAME                        STATUS   VOLUME                                         CAPACITY   ACCESS MODES   STORAGECLASS                            VOLUMEATTRIBUTESCLASS   AGE
backuprestore-sample-pvc2   Bound    restore-8274c8a1-bd1a-4511-a4e3-da4893790b9d   10Gi       RWO            simplyblock-simplyblock-cluster-pool3   <unset>                 2m26s
spdkcsi-pvc                 Bound    pvc-082c6727-ea2c-4dd6-8920-6cd4ec92819d       10Gi       RWO            simplyblock-simplyblock-cluster-pool3   <unset>                 17m
```

Now this PVC can be attached to a pod. 
```
kubectl apply -f - <<'EOF'
kind: Pod
apiVersion: v1
metadata:
  name: spdkcsi-test2
  namespace: cluster1
spec:
  containers:
  - name: alpine
    image: alpine:3
    imagePullPolicy: "IfNotPresent"
    command: ["sleep", "365d"]
    volumeMounts:
    - mountPath: "/spdkvol"
      name: spdk-volume
  volumes:
  - name: spdk-volume
    persistentVolumeClaim:
      claimName: backuprestore-sample-pvc2
EOF
```

Verify checksums match the data.

### Backup Policy

A BackupPolicy can be created and attached to an existing PVC by setting the annotation: `simplybk/backup-policy`
This will automatically create `StorageBackup` objects. as per the configured time which can be viewed by running `kubectl get storagebackup`

```
kubectl apply -f - <<'EOF'
apiVersion: storage.simplyblock.io/v1alpha1
kind: BackupPolicy
metadata:
  name: policy1
  namespace: cluster1
spec:
  clusterName: simplyblock-cluster
  maxVersions: 10
  maxAge: "7d"
  schedule: "15m,4 60m,11 24h,7"
EOF
```

Attach it to an annotation
```
kubectl annotate pvc spdkcsi-pvc -n default simplybk/backup-policy=policy1
```

Remove the annotation detaches the policy from the lvol
```
kubectl annotate pvc spdkcsi-pvc -n default simplybk/backup-policy-
```

Updating the annotation detaches the lvol from old policy and re-attaches to the new policy.
```
kubectl annotate pvc spdkcsi-pvc  -n default simplybk/backup-policy=policy2 --overwrite
```


### Known limitations

* backup restore can only restore a PVC to the same namespace
* 

