
# Simplyblock management cli


```bash
usage: sbcli-mc [-h] [-d]
                {storage-node,sn,cluster,lvol,mgmt,pool,snapshot,caching-node,cn}
                ...

positional arguments:
  {storage-node,sn,cluster,lvol,mgmt,pool,snapshot,caching-node,cn}
    storage-node (sn)   Storage node commands
    cluster             Cluster commands
    lvol                LVol commands
    mgmt                Management node commands
    pool                Pool commands
    snapshot            Snapshot commands
    caching-node (cn)   Caching client node commands

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Print debug messages

```

    
## Storage node commands


```bash
usage: sbcli-mc storage-node [-h]
                             {deploy,deploy-cleaner,add-node,delete,remove,list,get,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,add-device,remove-device,set-failed-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,check,check-device,info,info-spdk}
                             ...

positional arguments:
  {deploy,deploy-cleaner,add-node,delete,remove,list,get,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,add-device,remove-device,set-failed-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,check,check-device,info,info-spdk}
    deploy              Deploy local services for remote ops (local run)
    deploy-cleaner      clean local deploy (local run)
    add-node            Add storage node by ip
    delete              Delete storage node obj
    remove              Remove storage node
    list                List storage nodes
    get                 Get storage node info
    restart             Restart a storage node.
    shutdown            Shutdown a storage node.
    suspend             Suspend a storage node.
    resume              Resume a storage node
    get-io-stats        Returns the current io statistics of a node
    get-capacity        Returns the size, absolute and relative utilization of
                        the node in bytes
    list-devices        List storage devices
    device-testing-mode
                        set pt testing bdev mode
    get-device          Get storage device by id
    reset-device        Reset storage device
    restart-device      Re add "removed" storage device
    add-device          Add a new storage device
    remove-device       Remove a storage device.
    set-failed-device   Set storage device to failed state.
    get-capacity-device
                        Returns the size, absolute and relative utilization of
                        the device in bytes
    get-io-stats-device
                        Returns device IO statistics
    port-list           Get Data interfaces list for a node
    port-io-stats       Get Data interfaces IO stats
    check               Health check storage node
    check-device        Health check device
    info                Get node information
    info-spdk           Get SPDK memory information

optional arguments:
  -h, --help            show this help message and exit

```

    
### Deploy local services for remote ops (local run)


```bash
usage: sbcli-mc storage-node deploy [-h] [--ifname IFNAME]

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

    
### Clean local deploy (local run)


```bash
usage: sbcli-mc storage-node deploy-cleaner [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Add storage node by ip


```bash
usage: sbcli-mc storage-node add-node [-h] [--jm-pcie JM_PCIE]
                                      [--data-nics DATA_NICS [DATA_NICS ...]]
                                      [--cpu-mask SPDK_CPU_MASK]
                                      [--memory SPDK_MEM]
                                      [--spdk-image SPDK_IMAGE] [--spdk-debug]
                                      [--iobuf_small_pool_count SMALL_POOL_COUNT]
                                      [--iobuf_large_pool_count LARGE_POOL_COUNT]
                                      [--iobuf_small_bufsize SMALL_BUFSIZE]
                                      [--iobuf_large_bufsize LARGE_BUFSIZE]
                                      cluster_id node_ip ifname

positional arguments:
  cluster_id            UUID of the cluster to which the node will belong
  node_ip               IP of storage node to add
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --jm-pcie JM_PCIE     JM device address
  --data-nics DATA_NICS [DATA_NICS ...]
                        Data interface names
  --cpu-mask SPDK_CPU_MASK
                        SPDK app CPU mask, default is all cores found
  --memory SPDK_MEM     SPDK huge memory allocation, default is 4G
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --spdk-debug          Enable spdk debug logs
  --iobuf_small_pool_count SMALL_POOL_COUNT
                        bdev_set_options param
  --iobuf_large_pool_count LARGE_POOL_COUNT
                        bdev_set_options param
  --iobuf_small_bufsize SMALL_BUFSIZE
                        bdev_set_options param
  --iobuf_large_bufsize LARGE_BUFSIZE
                        bdev_set_options param

```

    
### Delete storage node obj


```bash
usage: sbcli-mc storage-node delete [-h] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove storage node


```bash
usage: sbcli-mc storage-node remove [-h] [--force-remove] [--force-migrate]
                                    node_id

positional arguments:
  node_id          UUID of storage node

optional arguments:
  -h, --help       show this help message and exit
  --force-remove   Force remove all LVols and snapshots
  --force-migrate  Force migrate All LVols to other nodes

```

    
### List storage nodes


```bash
usage: sbcli-mc storage-node list [-h] [--cluster-id CLUSTER_ID] [--json]

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        id of the cluster for which nodes are listed
  --json                Print outputs in json format

```

    
### Get storage node info


```bash
usage: sbcli-mc storage-node get [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Restart a storage node.
All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not impact operations.

```bash
usage: All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not impact operations.

positional arguments:
  node_id               UUID of storage node

optional arguments:
  -h, --help            show this help message and exit
  --cpu-mask SPDK_CPU_MASK
                        SPDK app CPU mask, default is all cores found
  --memory SPDK_MEM     SPDK huge memory allocation, default is 4G
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --spdk-debug          Enable spdk debug logs
  --iobuf_small_pool_count SMALL_POOL_COUNT
                        bdev_set_options param
  --iobuf_large_pool_count LARGE_POOL_COUNT
                        bdev_set_options param
  --iobuf_small_bufsize SMALL_BUFSIZE
                        bdev_set_options param
  --iobuf_large_bufsize LARGE_BUFSIZE
                        bdev_set_options param

```

    
### Shutdown a storage node.
Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-availability setup, this will not impact operations.

```bash
usage: Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-availability setup, this will not impact operations.

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node shutdown

```

    
### Suspend a storage node.
The node will stop accepting new IO, but will finish processing any IO, which has been received already.

```bash
usage: The node will stop accepting new IO, but will finish processing any IO, which has been received already.

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node suspend

```

    
### Resume a storage node


```bash
usage: sbcli-mc storage-node resume [-h] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns the current io statistics of a node


```bash
usage: sbcli-mc storage-node get-io-stats [-h] [--history HISTORY] node_id

positional arguments:
  node_id            Node ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### Returns the size, absolute and relative utilization of the node in bytes


```bash
usage: sbcli-mc storage-node get-capacity [-h] [--history HISTORY] node_id

positional arguments:
  node_id            Node ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### List storage devices


```bash
usage: sbcli-mc storage-node list-devices [-h] [-s {node-seq,dev-seq,serial}]
                                          [--json]
                                          node_id

positional arguments:
  node_id               the node's UUID

optional arguments:
  -h, --help            show this help message and exit
  -s {node-seq,dev-seq,serial}, --sort {node-seq,dev-seq,serial}
                        Sort the outputs
  --json                Print outputs in json format

```

    
### Set pt testing bdev mode


```bash
usage: sbcli-mc storage-node device-testing-mode [-h]
                                                 device_id
                                                 {full_pass_through,io_error_on_read,io_error_on_write,io_error_on_unmap,io_error_on_all,discard_io_all,hotplug_removal}

positional arguments:
  device_id             Device UUID
  {full_pass_through,io_error_on_read,io_error_on_write,io_error_on_unmap,io_error_on_all,discard_io_all,hotplug_removal}
                        Testing mode

optional arguments:
  -h, --help            show this help message and exit

```

    
### Get storage device by id


```bash
usage: sbcli-mc storage-node get-device [-h] device_id

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Reset storage device
Hardware device reset. Resetting the device can return the device from an unavailable into online state, if successful

```bash
usage: Hardware device reset. Resetting the device can return the device from an unavailable into online state, if successful

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Re add "removed" storage device
a previously removed or unavailable device may be returned into online state. If the device is not physically present, accessible or healthy, it will flip back into unavailable state again.

```bash
usage: a previously removed or unavailable device may be returned into online state. If the device is not physically present, accessible or healthy, it will flip back into unavailable state again.

positional arguments:
  id          the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Add a new storage device
Adding a device will include a previously detected device (currently in "new" state) into cluster and will launch and auto-rebalancing background process in which some cluster capacity is re-distributed to this newly added device.

```bash
usage: Adding a device will include a previously detected device (currently in "new" state) into cluster and will launch and auto-rebalancing background process in which some cluster capacity is re-distributed to this newly added device.

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove a storage device.
The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection of removal did not work or if the device must be maintained otherwise while remaining inserted into the server. 

```bash
usage: The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection of removal did not work or if the device must be maintained otherwise while remaining inserted into the server. 

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force device remove

```

    
### Set storage device to failed state. 
This command can be used, if an administrator believes that the device must be changed, but its status and health state do not lead to an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices in the cluster. 

```bash
usage: This command can be used, if an administrator believes that the device must be changed, but its status and health state do not lead to an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices in the cluster. 

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns the size, absolute and relative utilization of the device in bytes


```bash
usage: sbcli-mc storage-node get-capacity-device [-h] [--history HISTORY]
                                                 device_id

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### Returns device io statistics


```bash
usage: sbcli-mc storage-node get-io-stats-device [-h] [--history HISTORY]
                                                 device_id

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### Get data interfaces list for a node


```bash
usage: sbcli-mc storage-node port-list [-h] node_id

positional arguments:
  node_id     Storage node ID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get data interfaces io stats


```bash
usage: sbcli-mc storage-node port-io-stats [-h] [--history HISTORY] port_id

positional arguments:
  port_id            Data port ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total, format: XXdYYh

```

    
### Health check storage node


```bash
usage: sbcli-mc storage-node check [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check device


```bash
usage: sbcli-mc storage-node check-device [-h] id

positional arguments:
  id          device UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get node information


```bash
usage: sbcli-mc storage-node info [-h] id

positional arguments:
  id          Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get spdk memory information


```bash
usage: sbcli-mc storage-node info-spdk [-h] id

positional arguments:
  id          Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
## Cluster commands


```bash
usage: sbcli-mc cluster [-h]
                        {create,add,list,status,get,suspend,unsuspend,get-capacity,get-io-stats,get-cli-ssh-pass,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,delete}
                        ...

positional arguments:
  {create,add,list,status,get,suspend,unsuspend,get-capacity,get-io-stats,get-cli-ssh-pass,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,delete}
    create              Create an new cluster with this node as mgmt (local
                        run)
    add                 Add new cluster
    list                Show clusters list
    status              Show cluster status
    get                 Show cluster info
    suspend             Suspend cluster
    unsuspend           Unsuspend cluster
    get-capacity        Returns the current total available capacity, utilized
                        capacity (in percent and absolute) and provisioned
                        capacity (in percent and absolute) in GB in the
                        cluster.
    get-io-stats        Returns cluster IO statistics.
    get-cli-ssh-pass    returns the ssh password for the CLI ssh connection
    get-logs            Returns cluster status logs
    get-secret          Returns auto generated, 20 characters secret.
    upd-secret          Updates the secret (replaces the existing one with a
                        new one) and returns the new one.
    check               Health check cluster
    update              Update cluster mgmt services
    graceful-shutdown   Graceful shutdown of storage nodes
    graceful-startup    Graceful startup of storage nodes
    list-tasks          List tasks by cluster ID
    delete              Delete Cluster

optional arguments:
  -h, --help            show this help message and exit

```

    
### Create an new cluster with this node as mgmt (local run)


```bash
usage: sbcli-mc cluster create [-h] [--blk_size {512,4096}]
                               [--page_size PAGE_SIZE] [--CLI_PASS CLI_PASS]
                               [--cap-warn CAP_WARN] [--cap-crit CAP_CRIT]
                               [--prov-cap-warn PROV_CAP_WARN]
                               [--prov-cap-crit PROV_CAP_CRIT]
                               [--ifname IFNAME]
                               [--log-del-interval LOG_DEL_INTERVAL]
                               [--metrics-retention-period METRICS_RETENTION_PERIOD]

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

```

    
### Add new cluster


```bash
usage: sbcli-mc cluster add [-h] [--blk_size {512,4096}]
                            [--page_size PAGE_SIZE] [--ha_type {single,ha}]
                            [--cap-warn CAP_WARN] [--cap-crit CAP_CRIT]
                            [--prov-cap-warn PROV_CAP_WARN]
                            [--prov-cap-crit PROV_CAP_CRIT]

optional arguments:
  -h, --help            show this help message and exit
  --blk_size {512,4096}
                        The block size in bytes
  --page_size PAGE_SIZE
                        The size of a data page in bytes
  --ha_type {single,ha}
                        Can be "single" for single node clusters or "HA",
                        which requires at least 3 nodes
  --cap-warn CAP_WARN   Capacity warning level in percent, default=80
  --cap-crit CAP_CRIT   Capacity critical level in percent, default=90
  --prov-cap-warn PROV_CAP_WARN
                        Capacity warning level in percent, default=180
  --prov-cap-crit PROV_CAP_CRIT
                        Capacity critical level in percent, default=190

```

    
### Show clusters list


```bash
usage: sbcli-mc cluster list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster status


```bash
usage: sbcli-mc cluster status [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster info


```bash
usage: sbcli-mc cluster get [-h] id

positional arguments:
  id          the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Suspend cluster


```bash
usage: sbcli-mc cluster suspend [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Unsuspend cluster


```bash
usage: sbcli-mc cluster unsuspend [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns the current total available capacity, utilized capacity (in percent and absolute) and provisioned capacity (in percent and absolute) in gb in the cluster.


```bash
usage: sbcli-mc cluster get-capacity [-h] [--json] [--history HISTORY]
                                     cluster_id

positional arguments:
  cluster_id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --json             Print json output
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Returns cluster io statistics.


```bash
usage: sbcli-mc cluster get-io-stats [-h] [--records RECORDS]
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

    
### Returns the ssh password for the cli ssh connection


```bash
usage: sbcli-mc cluster get-cli-ssh-pass [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns cluster status logs


```bash
usage: sbcli-mc cluster get-logs [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns auto generated, 20 characters secret.


```bash
usage: sbcli-mc cluster get-secret [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Updates the secret (replaces the existing one with a new one) and returns the new one.


```bash
usage: sbcli-mc cluster upd-secret [-h] cluster_id secret

positional arguments:
  cluster_id  cluster uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check cluster


```bash
usage: sbcli-mc cluster check [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Update cluster mgmt services


```bash
usage: sbcli-mc cluster update [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful shutdown of storage nodes


```bash
usage: sbcli-mc cluster graceful-shutdown [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful startup of storage nodes


```bash
usage: sbcli-mc cluster graceful-startup [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### List tasks by cluster id


```bash
usage: sbcli-mc cluster list-tasks [-h] cluster_id

positional arguments:
  cluster_id  UUID of the cluster

optional arguments:
  -h, --help  show this help message and exit

```

    
### Delete cluster
This is only possible, if no storage nodes and pools are attached to the cluster

```bash
usage: This is only possible, if no storage nodes and pools are attached to the cluster

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
## Lvol commands


```bash
usage: sbcli-mc lvol [-h]
                     {add,qos-set,list,list-mem,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,send-cluster-map,get-cluster-map,check}
                     ...

positional arguments:
  {add,qos-set,list,list-mem,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,send-cluster-map,get-cluster-map,check}
    add                 Add a new logical volume
    qos-set             Change qos settings for an active logical volume
    list                List LVols
    list-mem            Get the size and max_size of the lvol
    get                 Get LVol details
    delete              Delete LVol.
    connect             show connection strings to cluster.
    resize              Resize LVol.
    create-snapshot     Create snapshot from LVol
    clone               create LVol based on a snapshot
    move                Moves a full copy of the logical volume between nodes
    get-capacity        Returns the current (or historic) provisioned and
                        utilized (in percent and absolute) capacity.
    get-io-stats        Returns LVol IO statistics
    send-cluster-map    send distr cluster map
    get-cluster-map     get distr cluster map
    check               Health check LVol

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add a new logical volume


```bash
usage: sbcli-mc lvol add [-h] [--snapshot] [--max-size MAX_SIZE]
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
                         name size pool

positional arguments:
  name                  LVol name or id
  size                  LVol size: 10M, 10G, 10(bytes)
  pool                  Pool UUID or name

optional arguments:
  -h, --help            show this help message and exit
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
usage: sbcli-mc lvol qos-set [-h] [--max-rw-iops MAX_RW_IOPS]
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
usage: sbcli-mc lvol list [-h] [--cluster-id CLUSTER_ID] [--pool POOL]
                          [--json]

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        List LVols in particular cluster
  --pool POOL           List LVols in particular Pool ID or name
  --json                Print outputs in json format

```

    
### Get the size and max_size of the lvol


```bash
usage: sbcli-mc lvol list-mem [-h] [--json] [--csv]

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
  --csv       Print outputs in csv format

```

    
### Get lvol details


```bash
usage: sbcli-mc lvol get [-h] [--json] id

positional arguments:
  id          LVol id or name

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Delete lvol.
This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted. 

```bash
usage: This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted. 

positional arguments:
  id          LVol id or ids

optional arguments:
  -h, --help  show this help message and exit
  --force     Force delete LVol from the cluster

```

    
### Show connection strings to cluster.
Multiple connections to the cluster are always available for multi-pathing and high-availability.

```bash
usage: Multiple connections to the cluster are always available for multi-pathing and high-availability.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

    
### Resize lvol.
The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below the current utilization.

```bash
usage: The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below the current utilization.

positional arguments:
  id          LVol id
  size        New LVol size size: 10M, 10G, 10(bytes)

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create snapshot from lvol


```bash
usage: sbcli-mc lvol create-snapshot [-h] id name

positional arguments:
  id          LVol id
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create lvol based on a snapshot


```bash
usage: sbcli-mc lvol clone [-h] [--resize RESIZE] snapshot_id clone_name

positional arguments:
  snapshot_id      snapshot UUID
  clone_name       clone name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
### Moves a full copy of the logical volume between nodes


```bash
usage: sbcli-mc lvol move [-h] [--force] id node_id

positional arguments:
  id          LVol UUID
  node_id     Destination Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force LVol delete from source node

```

    
### Returns the current (or historic) provisioned and utilized (in percent and absolute) capacity.


```bash
usage: sbcli-mc lvol get-capacity [-h] [--history HISTORY] id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Returns lvol io statistics


```bash
usage: sbcli-mc lvol get-io-stats [-h] [--history HISTORY] id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Send distr cluster map


```bash
usage: sbcli-mc lvol send-cluster-map [-h] id

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get distr cluster map


```bash
usage: sbcli-mc lvol get-cluster-map [-h] id

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check lvol


```bash
usage: sbcli-mc lvol check [-h] id

positional arguments:
  id          UUID of LVol

optional arguments:
  -h, --help  show this help message and exit

```

    
## Management node commands


```bash
usage: sbcli-mc mgmt [-h] {add,list,remove} ...

positional arguments:
  {add,list,remove}
    add              Add Management node to the cluster
    list             List Management nodes
    remove           Remove Management node

optional arguments:
  -h, --help         show this help message and exit

```

    
### Add management node to the cluster


```bash
usage: sbcli-mc mgmt add [-h] cluster_ip cluster_id ifname

positional arguments:
  cluster_ip  the cluster IP address
  cluster_id  the cluster UUID
  ifname      Management interface name

optional arguments:
  -h, --help  show this help message and exit

```

    
### List management nodes


```bash
usage: sbcli-mc mgmt list [-h] [--json]

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Remove management node


```bash
usage: sbcli-mc mgmt remove [-h] id

positional arguments:
  id          Mgmt node uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
## Pool commands


```bash
usage: sbcli-mc pool [-h]
                     {add,set,list,get,delete,enable,disable,get-secret,upd-secret,get-capacity,get-io-stats}
                     ...

positional arguments:
  {add,set,list,get,delete,enable,disable,get-secret,upd-secret,get-capacity,get-io-stats}
    add                 Add a new Pool
    set                 Set pool attributes
    list                List pools
    get                 get pool details
    delete              Delete Pool
    enable              Set pool status to Active
    disable             Set pool status to Inactive.
    get-secret          Returns auto generated, 20 characters secret.
    upd-secret          Updates the secret (replaces the existing one with a
                        new one) and returns the new one.
    get-capacity        Return provisioned, utilized (absolute) and utilized
                        (percent) storage on the Pool.
    get-io-stats        Returns either the current or historic io statistics
                        (read-IO, write-IO, total-IO, read mbs, write mbs,
                        total mbs).

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add a new pool


```bash
usage: sbcli-mc pool add [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX]
                         [--max-rw-iops MAX_RW_IOPS]
                         [--max-rw-mbytes MAX_RW_MBYTES]
                         [--max-r-mbytes MAX_R_MBYTES]
                         [--max-w-mbytes MAX_W_MBYTES] [--has-secret]
                         name cluster_id

positional arguments:
  name                  Pool name
  cluster_id            Cluster UUID

optional arguments:
  -h, --help            show this help message and exit
  --pool-max POOL_MAX   Pool maximum size: 20M, 20G, 0(default)
  --lvol-max LVOL_MAX   LVol maximum size: 20M, 20G, 0(default)
  --max-rw-iops MAX_RW_IOPS
                        Maximum Read Write IO Per Second
  --max-rw-mbytes MAX_RW_MBYTES
                        Maximum Read Write Mega Bytes Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second
  --has-secret          Pool is created with a secret (all further API
                        interactions with the pool and logical volumes in the
                        pool require this secret)

```

    
### Set pool attributes


```bash
usage: sbcli-mc pool set [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX]
                         [--max-rw-iops MAX_RW_IOPS]
                         [--max-rw-mbytes MAX_RW_MBYTES]
                         [--max-r-mbytes MAX_R_MBYTES]
                         [--max-w-mbytes MAX_W_MBYTES]
                         id

positional arguments:
  id                    Pool UUID

optional arguments:
  -h, --help            show this help message and exit
  --pool-max POOL_MAX   Pool maximum size: 20M, 20G
  --lvol-max LVOL_MAX   LVol maximum size: 20M, 20G
  --max-rw-iops MAX_RW_IOPS
                        Maximum Read Write IO Per Second
  --max-rw-mbytes MAX_RW_MBYTES
                        Maximum Read Write Mega Bytes Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second

```

    
### List pools


```bash
usage: sbcli-mc pool list [-h] [--json] [--cluster-id CLUSTER_ID]

optional arguments:
  -h, --help            show this help message and exit
  --json                Print outputs in json format
  --cluster-id CLUSTER_ID
                        ID of the cluster

```

    
### Get pool details


```bash
usage: sbcli-mc pool get [-h] [--json] id

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Delete pool
It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).

```bash
usage: It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Set pool status to active


```bash
usage: sbcli-mc pool enable [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Set pool status to inactive.


```bash
usage: sbcli-mc pool disable [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns auto generated, 20 characters secret.


```bash
usage: sbcli-mc pool get-secret [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Updates the secret (replaces the existing one with a new one) and returns the new one.


```bash
usage: sbcli-mc pool upd-secret [-h] pool_id secret

positional arguments:
  pool_id     pool uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

    
### Return provisioned, utilized (absolute) and utilized (percent) storage on the pool.


```bash
usage: sbcli-mc pool get-capacity [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Returns either the current or historic io statistics (read-io, write-io, total-io, read mbs, write mbs, total mbs).


```bash
usage: sbcli-mc pool get-io-stats [-h] [--history HISTORY] id

positional arguments:
  id                 Pool id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
## Snapshot commands


```bash
usage: sbcli-mc snapshot [-h] {add,list,delete,clone} ...

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
usage: sbcli-mc snapshot add [-h] id name

positional arguments:
  id          LVol UUID
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### List snapshots


```bash
usage: sbcli-mc snapshot list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Delete a snapshot


```bash
usage: sbcli-mc snapshot delete [-h] id

positional arguments:
  id          snapshot UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create lvol from snapshot


```bash
usage: sbcli-mc snapshot clone [-h] [--resize RESIZE] id lvol_name

positional arguments:
  id               snapshot UUID
  lvol_name        LVol name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
## Caching client node commands


```bash
usage: sbcli-mc caching-node [-h]
                             {deploy,add-node,list,list-lvols,remove,connect,disconnect,recreate}
                             ...

positional arguments:
  {deploy,add-node,list,list-lvols,remove,connect,disconnect,recreate}
    deploy              Deploy caching node on this machine (local exec)
    add-node            Add new Caching node to the cluster
    list                List Caching nodes
    list-lvols          List connected lvols
    remove              Remove Caching node from the cluster
    connect             Connect to LVol
    disconnect          Disconnect LVol from Caching node
    recreate            recreate Caching node bdevs

optional arguments:
  -h, --help            show this help message and exit

```

    
### Deploy caching node on this machine (local exec)


```bash
usage: sbcli-mc caching-node deploy [-h] [--ifname IFNAME]

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

    
### Add new caching node to the cluster


```bash
usage: sbcli-mc caching-node add-node [-h] [--cpu-mask SPDK_CPU_MASK]
                                      [--memory SPDK_MEM]
                                      [--spdk-image SPDK_IMAGE]
                                      [--namespace NAMESPACE]
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

```

    
### List caching nodes


```bash
usage: sbcli-mc caching-node list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### List connected lvols


```bash
usage: sbcli-mc caching-node list-lvols [-h] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove caching node from the cluster


```bash
usage: sbcli-mc caching-node remove [-h] [--force] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force remove

```

    
### Connect to lvol


```bash
usage: sbcli-mc caching-node connect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Disconnect lvol from caching node


```bash
usage: sbcli-mc caching-node disconnect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Recreate caching node bdevs 


```bash
usage: sbcli-mc caching-node recreate [-h] node_id

positional arguments:
  node_id     Caching node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    