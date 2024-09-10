
# Simplyblock management cli


```bash
usage: sbcli-new [-h] [-d]
                 {caching-node,cn,cluster,lvol,mgmt,pool,snapshot} ...

positional arguments:
  {caching-node,cn,cluster,lvol,mgmt,pool,snapshot}
    caching-node (cn)   Caching client node commands
    cluster             Cluster commands
    lvol                LVol commands
    mgmt                Management node commands
    pool                Pool commands
    snapshot            Snapshot commands

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Print debug messages

```

    
## Caching client node commands


```bash
usage: sbcli-new caching-node [-h]
                              {add-node,deploy,deploy-cleaner,list,list-lvols,remove,connect,disconnect,recreate,restart,shutdown,get-io-stats,get-capacity,check}
                              ...

positional arguments:
  {add-node,deploy,deploy-cleaner,list,list-lvols,remove,connect,disconnect,recreate,restart,shutdown,get-io-stats,get-capacity,check}
    add-node            Add new Caching node to the cluster
    deploy              Deploy caching node on this machine (local exec)
    deploy-cleaner      clean local deploy (local run)
    list                List Caching nodes
    list-lvols          List connected lvols
    remove              Remove Caching node from the cluster
    connect             Connect to LVol
    disconnect          Disconnect LVol from Caching node
    recreate            recreate Caching node bdevs
    restart             restart Caching node
    shutdown            shutdown Caching node
    get-io-stats        Get node IO statistics
    get-capacity        Get Node capacity
    check               Health check node

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add new caching node to the cluster


```bash
usage: sbcli-new caching-node add-node [-h] [--cpu-mask SPDK_CPU_MASK]
                                       [--memory SPDK_MEM]
                                       [--spdk-image SPDK_IMAGE]
                                       [--namespace NAMESPACE]
                                       [--blocked-pcie BLOCKED_PCIE]
                                       [--s3-data-path S3_DATA_PATH]
                                       [--ftl-buffer-size FTL_BUFFER_SIZE]
                                       [--lvstore-cluster-size LVSTORE_CLUSTER_SIZE]
                                       [--num-md-pages-per-cluster-ratio NUM_MD_PAGES_PER_CLUSTER_RATIO]
                                       cluster_id node_ip ifname

positional arguments:
  cluster_id            UUID of the cluster to which the node will belong
  node_ip               IP of caching node to add
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --cpu-mask SPDK_CPU_MASK
                        SPDK app CPU mask, default is all cores found
  --memory SPDK_MEM     SPDK huge memory allocation, default is Max hugepages
                        available
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --namespace NAMESPACE
                        k8s namespace to deploy on
  --blocked-pcie BLOCKED_PCIE
                        block pcie from spdk
  --s3-data-path S3_DATA_PATH
                        s3 fuse mount point
  --ftl-buffer-size FTL_BUFFER_SIZE
                        FTL buffer size, default 6G
  --lvstore-cluster-size LVSTORE_CLUSTER_SIZE
                        LVS cluster size
  --num-md-pages-per-cluster-ratio NUM_MD_PAGES_PER_CLUSTER_RATIO
                        LVS md cluster ratio

```

    
### Deploy caching node on this machine (local exec)


```bash
usage: sbcli-new caching-node deploy [-h] [--ifname IFNAME]

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

    
### Clean local deploy (local run)


```bash
usage: sbcli-new caching-node deploy-cleaner [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### List caching nodes


```bash
usage: sbcli-new caching-node list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### List connected lvols


```bash
usage: sbcli-new caching-node list-lvols [-h] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove caching node from the cluster


```bash
usage: sbcli-new caching-node remove [-h] [--force] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force remove

```

    
### Connect to lvol


```bash
usage: sbcli-new caching-node connect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Disconnect lvol from caching node


```bash
usage: sbcli-new caching-node disconnect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Recreate caching node bdevs


```bash
usage: sbcli-new caching-node recreate [-h] node_id

positional arguments:
  node_id     Caching node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Restart caching node


```bash
usage: sbcli-new caching-node restart [-h] [--node-ip NODE_IP]
                                      [--blocked-pcie BLOCKED_PCIE]
                                      [--s3-data-path S3_DATA_PATH]
                                      [--ftl-buffer-size FTL_BUFFER_SIZE]
                                      id

positional arguments:
  id                    Caching node UUID

optional arguments:
  -h, --help            show this help message and exit
  --node-ip NODE_IP     Caching node new IP
  --blocked-pcie BLOCKED_PCIE
                        block pcie from spdk
  --s3-data-path S3_DATA_PATH
                        s3 fuse mount point
  --ftl-buffer-size FTL_BUFFER_SIZE
                        FTL buffer size, default 6G

```

    
### Shutdown caching node


```bash
usage: sbcli-new caching-node shutdown [-h] id

positional arguments:
  id          Caching node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get node io statistics


```bash
usage: sbcli-new caching-node get-io-stats [-h] [--history HISTORY] id

positional arguments:
  id                 node id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Get node capacity


```bash
usage: sbcli-new caching-node get-capacity [-h] [--history HISTORY] id

positional arguments:
  id                 Node id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Health check node


```bash
usage: sbcli-new caching-node check [-h] id

positional arguments:
  id          Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
## Lvol commands


```bash
usage: sbcli-new lvol [-h]
                      {add,qos-set,list,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,check}
                      ...

positional arguments:
  {add,qos-set,list,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,check}
    add                 Add a new logical volume
    qos-set             Change qos settings for an active logical volume
    list                List LVols
    get                 Get LVol details
    delete              Delete LVol
    connect             Get lvol connection strings
    resize              Resize LVol
    create-snapshot     Create snapshot from LVol
    clone               create LVol based on a snapshot
    move                Moves a full copy of the logical volume between nodes
    get-capacity        Get LVol capacity
    get-io-stats        Get LVol IO statistics
    check               Health check LVol

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add a new logical volume


```bash
usage: sbcli-new lvol add [-h] [--connection-type {nvmf,nbd}] [--pool POOL]
                          [--snapshot] [--max-size MAX_SIZE]
                          [--host-id HOST_ID] [--ha-type {single,ha,default}]
                          [--encrypt] [--crypto-key1 CRYPTO_KEY1]
                          [--crypto-key2 CRYPTO_KEY2]
                          [--max-rw-iops MAX_RW_IOPS]
                          [--max-rw-mbytes MAX_RW_MBYTES]
                          [--max-r-mbytes MAX_R_MBYTES]
                          [--max-w-mbytes MAX_W_MBYTES]
                          [--distr-vuid DISTR_VUID] [--distr-ndcs DISTR_NDCS]
                          [--distr-npcs DISTR_NPCS] [--distr-bs DISTR_BS]
                          [--distr-chunk-bs DISTR_CHUNK_BS]
                          name size

positional arguments:
  name                  LVol name or id
  size                  LVol size: 10M, 10G, 10(bytes)

optional arguments:
  -h, --help            show this help message and exit
  --connection-type {nvmf,nbd}
                        connection type, nvmf or nbd, default:nvmf
  --pool POOL           Pool UUID or name
  --snapshot, -s        Make LVol with snapshot capability, default is False
  --max-size MAX_SIZE   LVol max size
  --host-id HOST_ID     Primary storage node UUID or Hostname
  --ha-type {single,ha,default}
                        LVol HA type (single, ha), default is cluster HA type
  --encrypt             Use inline data encryption and de-cryption on the
                        logical volume
  --crypto-key1 CRYPTO_KEY1
                        the hex value of key1 to be used for lvol encryption
  --crypto-key2 CRYPTO_KEY2
                        the hex value of key2 to be used for lvol encryption
  --max-rw-iops MAX_RW_IOPS
                        Maximum Read Write IO Per Second
  --max-rw-mbytes MAX_RW_MBYTES
                        Maximum Read Write Mega Bytes Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second
  --distr-vuid DISTR_VUID
                        (Dev) set vuid manually, default: random (1-99999)
  --distr-ndcs DISTR_NDCS
                        (Dev) set ndcs manually, default: 4
  --distr-npcs DISTR_NPCS
                        (Dev) set npcs manually, default: 1
  --distr-bs DISTR_BS   (Dev) distrb bdev block size, default: 4096
  --distr-chunk-bs DISTR_CHUNK_BS
                        (Dev) distrb bdev chunk block size, default: 4096

```

    
### Change qos settings for an active logical volume


```bash
usage: sbcli-new lvol qos-set [-h] [--max-rw-iops MAX_RW_IOPS]
                              [--max-rw-mbytes MAX_RW_MBYTES]
                              [--max-r-mbytes MAX_R_MBYTES]
                              [--max-w-mbytes MAX_W_MBYTES]
                              id

positional arguments:
  id                    LVol id

optional arguments:
  -h, --help            show this help message and exit
  --max-rw-iops MAX_RW_IOPS
                        Maximum Read Write IO Per Second
  --max-rw-mbytes MAX_RW_MBYTES
                        Maximum Read Write Mega Bytes Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second

```

    
### List lvols


```bash
usage: sbcli-new lvol list [-h] [--cluster-id CLUSTER_ID] [--pool POOL]
                           [--json]

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        List LVols in particular cluster
  --pool POOL           List LVols in particular Pool ID or name
  --json                Print outputs in json format

```

    
### Get lvol details


```bash
usage: sbcli-new lvol get [-h] [--json] id

positional arguments:
  id          LVol id or name

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Delete lvol
This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted. 

```bash
usage: sbcli-new lvol delete [-h] [--force] id [id ...]

positional arguments:
  id          LVol id or ids

optional arguments:
  -h, --help  show this help message and exit
  --force     Force delete LVol from the cluster

```

    
### Get lvol connection strings
Multiple connections to the cluster are always available for multi-pathing and high-availability.

```bash
usage: sbcli-new lvol connect [-h] id

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

    
### Resize lvol
The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below the current utilization.

```bash
usage: sbcli-new lvol resize [-h] id size

positional arguments:
  id          LVol id
  size        New LVol size size: 10M, 10G, 10(bytes)

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create snapshot from lvol


```bash
usage: sbcli-new lvol create-snapshot [-h] id name

positional arguments:
  id          LVol id
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create lvol based on a snapshot


```bash
usage: sbcli-new lvol clone [-h] [--resize RESIZE] snapshot_id clone_name

positional arguments:
  snapshot_id      snapshot UUID
  clone_name       clone name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
### Moves a full copy of the logical volume between nodes


```bash
usage: sbcli-new lvol move [-h] [--force] id node_id

positional arguments:
  id          LVol UUID
  node_id     Destination Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force LVol delete from source node

```

    
### Get lvol capacity


```bash
usage: sbcli-new lvol get-capacity [-h] [--history HISTORY] id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Get lvol io statistics


```bash
usage: sbcli-new lvol get-io-stats [-h] [--history HISTORY] id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Health check lvol


```bash
usage: sbcli-new lvol check [-h] id

positional arguments:
  id          UUID of LVol

optional arguments:
  -h, --help  show this help message and exit

```

    
## Snapshot commands


```bash
usage: sbcli-new snapshot [-h] {add,list,delete,clone} ...

positional arguments:
  {add,list,delete,clone}
    add                 Create new snapshot
    list                List snapshots
    delete              Delete a snapshot
    clone               Create LVol from snapshot

optional arguments:
  -h, --help            show this help message and exit

```

    
### Create new snapshot


```bash
usage: sbcli-new snapshot add [-h] id name

positional arguments:
  id          LVol UUID
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### List snapshots


```bash
usage: sbcli-new snapshot list [-h] [--all]

optional arguments:
  -h, --help  show this help message and exit
  --all       List all snapshots

```

    
### Delete a snapshot


```bash
usage: sbcli-new snapshot delete [-h] id

positional arguments:
  id          snapshot UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create lvol from snapshot


```bash
usage: sbcli-new snapshot clone [-h] [--resize RESIZE] id lvol_name

positional arguments:
  id               snapshot UUID
  lvol_name        LVol name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
## Cluster commands


```bash
usage: sbcli-new cluster [-h]
                         {create,add,list,status,get,suspend,unsuspend,get-capacity,get-io-stats,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,cancel-task,delete}
                         ...

positional arguments:
  {create,add,list,status,get,suspend,unsuspend,get-capacity,get-io-stats,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,cancel-task,delete}
    create              Create an new cluster with this node as mgmt (local
                        run)
    add                 Add new cluster
    list                Show clusters list
    status              Show cluster status
    get                 Show cluster info
    suspend             Suspend cluster
    unsuspend           Unsuspend cluster
    get-capacity        Get cluster capacity
    get-io-stats        Get cluster IO statistics
    get-logs            Returns cluster status logs
    get-secret          Get cluster secret
    upd-secret          Updates the cluster secret
    check               Health check cluster
    update              Update cluster mgmt services
    graceful-shutdown   Graceful shutdown of storage nodes
    graceful-startup    Graceful startup of storage nodes
    list-tasks          List tasks by cluster ID
    cancel-task         Cancel task by ID
    delete              Delete Cluster

optional arguments:
  -h, --help            show this help message and exit

```

    
### Create an new cluster with this node as mgmt (local run)


```bash
usage: sbcli-new cluster create [-h] [--blk_size {512,4096}]
                                [--page_size PAGE_SIZE] [--CLI_PASS CLI_PASS]
                                [--cap-warn CAP_WARN] [--cap-crit CAP_CRIT]
                                [--prov-cap-warn PROV_CAP_WARN]
                                [--prov-cap-crit PROV_CAP_CRIT]
                                [--ifname IFNAME]
                                [--log-del-interval LOG_DEL_INTERVAL]
                                [--metrics-retention-period METRICS_RETENTION_PERIOD]
                                [--contact-point CONTACT_POINT]
                                [--grafana-endpoint GRAFANA_ENDPOINT]

optional arguments:
  -h, --help            show this help message and exit
  --blk_size {512,4096}
                        The block size in bytes
  --page_size PAGE_SIZE
                        The size of a data page in bytes
  --CLI_PASS CLI_PASS   Password for CLI SSH connection
  --cap-warn CAP_WARN   Capacity warning level in percent, default=80
  --cap-crit CAP_CRIT   Capacity critical level in percent, default=90
  --prov-cap-warn PROV_CAP_WARN
                        Capacity warning level in percent, default=180
  --prov-cap-crit PROV_CAP_CRIT
                        Capacity critical level in percent, default=190
  --ifname IFNAME       Management interface name, default: eth0
  --log-del-interval LOG_DEL_INTERVAL
                        graylog deletion interval, default: 7d
  --metrics-retention-period METRICS_RETENTION_PERIOD
                        retention period for prometheus metrics, default: 7d
  --contact-point CONTACT_POINT
                        the email or slack webhook url to be used for alerting
  --grafana-endpoint GRAFANA_ENDPOINT
                        the endpoint url for grafana

```

    
### Add new cluster


```bash
usage: sbcli-new cluster add [-h] [--blk_size {512,4096}]
                             [--page_size PAGE_SIZE] [--cap-warn CAP_WARN]
                             [--cap-crit CAP_CRIT]
                             [--prov-cap-warn PROV_CAP_WARN]
                             [--prov-cap-crit PROV_CAP_CRIT]

optional arguments:
  -h, --help            show this help message and exit
  --blk_size {512,4096}
                        The block size in bytes
  --page_size PAGE_SIZE
                        The size of a data page in bytes
  --cap-warn CAP_WARN   Capacity warning level in percent, default=80
  --cap-crit CAP_CRIT   Capacity critical level in percent, default=90
  --prov-cap-warn PROV_CAP_WARN
                        Capacity warning level in percent, default=180
  --prov-cap-crit PROV_CAP_CRIT
                        Capacity critical level in percent, default=190

```

    
### Show clusters list


```bash
usage: sbcli-new cluster list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster status


```bash
usage: sbcli-new cluster status [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster info


```bash
usage: sbcli-new cluster get [-h] id

positional arguments:
  id          the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Suspend cluster


```bash
usage: sbcli-new cluster suspend [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Unsuspend cluster


```bash
usage: sbcli-new cluster unsuspend [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get cluster capacity


```bash
usage: sbcli-new cluster get-capacity [-h] [--json] [--history HISTORY]
                                      cluster_id

positional arguments:
  cluster_id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --json             Print json output
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Get cluster io statistics


```bash
usage: sbcli-new cluster get-io-stats [-h] [--records RECORDS]
                                      [--history HISTORY]
                                      cluster_id

positional arguments:
  cluster_id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --records RECORDS  Number of records, default: 20
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Returns cluster status logs


```bash
usage: sbcli-new cluster get-logs [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get cluster secret


```bash
usage: sbcli-new cluster get-secret [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Updates the cluster secret


```bash
usage: sbcli-new cluster upd-secret [-h] cluster_id secret

positional arguments:
  cluster_id  cluster uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check cluster


```bash
usage: sbcli-new cluster check [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Update cluster mgmt services


```bash
usage: sbcli-new cluster update [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful shutdown of storage nodes


```bash
usage: sbcli-new cluster graceful-shutdown [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful startup of storage nodes


```bash
usage: sbcli-new cluster graceful-startup [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### List tasks by cluster id


```bash
usage: sbcli-new cluster list-tasks [-h] cluster_id

positional arguments:
  cluster_id  UUID of the cluster

optional arguments:
  -h, --help  show this help message and exit

```

    
### Cancel task by id


```bash
usage: sbcli-new cluster cancel-task [-h] id

positional arguments:
  id          UUID of the Task

optional arguments:
  -h, --help  show this help message and exit

```

    
### Delete cluster
This is only possible, if no storage nodes and pools are attached to the cluster

```bash
usage: sbcli-new cluster delete [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
## Management node commands


```bash
usage: sbcli-new mgmt [-h] {add,list,remove} ...

positional arguments:
  {add,list,remove}
    add              Add Management node to the cluster (local run)
    list             List Management nodes
    remove           Remove Management node

optional arguments:
  -h, --help         show this help message and exit

```

    
### Add management node to the cluster (local run)


```bash
usage: sbcli-new mgmt add [-h] cluster_ip cluster_id ifname

positional arguments:
  cluster_ip  the cluster IP address
  cluster_id  the cluster UUID
  ifname      Management interface name

optional arguments:
  -h, --help  show this help message and exit

```

    
### List management nodes


```bash
usage: sbcli-new mgmt list [-h] [--json]

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Remove management node


```bash
usage: sbcli-new mgmt remove [-h] id

positional arguments:
  id          Mgmt node uuid

optional arguments:
  -h, --help  show this help message and exit

```

    