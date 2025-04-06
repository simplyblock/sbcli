
# Simplyblock management cli


```bash
usage: sbcli [-h] [-d] [--cmd CMD [CMD ...]]
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
  --cmd CMD [CMD ...]   cmd

```

    
## Storage node commands


```bash
usage: sbcli storage-node [-h]
                              {deploy,deploy-cleaner,add-node,delete,remove,list,get,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,add-device,remove-device,set-failed-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,check,check-device,info,info-spdk,remove-jm-device,restart-jm-device,send-cluster-map,get-cluster-map,make-primary,dump-lvstore}
                              ...

positional arguments:
  {deploy,deploy-cleaner,add-node,delete,remove,list,get,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,add-device,remove-device,set-failed-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,check,check-device,info,info-spdk,remove-jm-device,restart-jm-device,send-cluster-map,get-cluster-map,make-primary,dump-lvstore}
    deploy              Deploy local services for remote ops (local run)
    deploy-cleaner      clean local deploy (local run)
    add-node            Add storage node by ip
    delete              Delete storage node obj
    remove              Remove storage node
    list                List storage nodes
    get                 Get storage node info
    restart             Restart a storage node
    shutdown            Shutdown a storage node
    suspend             Suspend a storage node
    resume              Resume a storage node
    get-io-stats        Get node IO statistics
    get-capacity        Get node capacity statistics
    list-devices        List storage devices
    device-testing-mode
                        Set device testing mode
    get-device          Get storage device by id
    reset-device        Reset storage device
    restart-device      Restart storage device
    add-device          Add a new storage device
    remove-device       Remove a storage device
    set-failed-device   Set storage device to failed state
    get-capacity-device
                        Get device capacity
    get-io-stats-device
                        Get device IO statistics
    port-list           Get Data interfaces list for a node
    port-io-stats       Get Data interfaces IO stats
    check               Health check storage node
    check-device        Health check device
    info                Get node information
    info-spdk           Get SPDK memory information
    remove-jm-device    Remove JM device
    restart-jm-device   Restart JM device
    send-cluster-map    send cluster map
    get-cluster-map     get cluster map
    make-primary        In case of HA SNode, make the current node as primary
    dump-lvstore        Dump lvstore data

optional arguments:
  -h, --help            show this help message and exit

```

    
### Deploy local services for remote ops (local run)


```bash
usage: sbcli storage-node deploy [-h] [--ifname IFNAME]

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

    
### Clean local deploy (local run)


```bash
usage: sbcli storage-node deploy-cleaner [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Add storage node by ip


```bash
usage: sbcli storage-node add-node [-h] [--partitions PARTITIONS]
                                       [--jm-percent JM_PERCENT]
                                       [--data-nics DATA_NICS [DATA_NICS ...]]
                                       [--max-lvol MAX_LVOL]
                                       [--max-snap MAX_SNAP]
                                       [--max-prov MAX_PROV]
                                       [--number-of-distribs NUMBER_OF_DISTRIBS]
                                       [--number-of-devices NUMBER_OF_DEVICES]
                                       [--cpu-mask SPDK_CPU_MASK]
                                       [--spdk-image SPDK_IMAGE]
                                       [--spdk-debug]
                                       [--iobuf_small_bufsize SMALL_BUFSIZE]
                                       [--iobuf_large_bufsize LARGE_BUFSIZE]
                                       [--enable-test-device]
                                       [--disable-ha-jm] [--is-secondary-node]
                                       [--namespace NAMESPACE]
                                       cluster_id node_ip ifname

positional arguments:
  cluster_id            UUID of the cluster to which the node will belong
  node_ip               IP of storage node to add
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --partitions PARTITIONS
                        Number of partitions to create per device
  --jm-percent JM_PERCENT
                        Number in percent to use for JM from each device
  --data-nics DATA_NICS [DATA_NICS ...]
                        Data interface names
  --max-lvol MAX_LVOL   Max lvol per storage node
  --max-snap MAX_SNAP   Max snapshot per storage node
  --max-prov MAX_PROV   Maximum amount of GB to be provisioned via all storage
                        nodes
  --number-of-distribs NUMBER_OF_DISTRIBS
                        The number of distirbs to be created on the node
  --number-of-devices NUMBER_OF_DEVICES
                        Number of devices per storage node if it's not
                        supported EC2 instance
  --cpu-mask SPDK_CPU_MASK
                        SPDK app CPU mask, default is all cores found
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --spdk-debug          Enable spdk debug logs
  --iobuf_small_bufsize SMALL_BUFSIZE
                        bdev_set_options param
  --iobuf_large_bufsize LARGE_BUFSIZE
                        bdev_set_options param
  --enable-test-device  Enable creation of test device
  --disable-ha-jm       Disable HA JM for distrib creation
  --is-secondary-node   add as secondary node
  --namespace NAMESPACE
                        k8s namespace to deploy on

```

    
### Delete storage node obj


```bash
usage: sbcli storage-node delete [-h] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove storage node


```bash
usage: sbcli storage-node remove [-h] [--force-remove] node_id

positional arguments:
  node_id         UUID of storage node

optional arguments:
  -h, --help      show this help message and exit
  --force-remove  Force remove all LVols and snapshots

```

    
### List storage nodes


```bash
usage: sbcli storage-node list [-h] [--cluster-id CLUSTER_ID] [--json]

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        id of the cluster for which nodes are listed
  --json                Print outputs in json format

```

    
### Get storage node info


```bash
usage: sbcli storage-node get [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Restart a storage node
All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not impact operations

```bash
usage: sbcli storage-node restart [-h] [--max-lvol MAX_LVOL]
                                      [--max-snap MAX_SNAP]
                                      [--max-prov MAX_PROV]
                                      [--node-ip NODE_IP]
                                      [--number-of-devices NUMBER_OF_DEVICES]
                                      [--spdk-image SPDK_IMAGE] [--spdk-debug]
                                      [--iobuf_small_bufsize SMALL_BUFSIZE]
                                      [--iobuf_large_bufsize LARGE_BUFSIZE]
                                      [--force]
                                      node_id

positional arguments:
  node_id               UUID of storage node

optional arguments:
  -h, --help            show this help message and exit
  --max-lvol MAX_LVOL   Max lvol per storage node
  --max-snap MAX_SNAP   Max snapshot per storage node
  --max-prov MAX_PROV   Max provisioning size of all storage nodes
  --node-ip NODE_IP     Restart Node on new node
  --number-of-devices NUMBER_OF_DEVICES
                        Number of devices per storage node if it's not
                        supported EC2 instance
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --spdk-debug          Enable spdk debug logs
  --iobuf_small_bufsize SMALL_BUFSIZE
                        bdev_set_options param
  --iobuf_large_bufsize LARGE_BUFSIZE
                        bdev_set_options param
  --force               Force restart

```

    
### Shutdown a storage node
Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-availability setup, this will not impact operations.

```bash
usage: sbcli storage-node shutdown [-h] [--force] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node shutdown

```

    
### Suspend a storage node
The node will stop accepting new IO, but will finish processing any IO, which has been received already.

```bash
usage: sbcli storage-node suspend [-h] [--force] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node suspend

```

    
### Resume a storage node


```bash
usage: sbcli storage-node resume [-h] node_id

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get node io statistics


```bash
usage: sbcli storage-node get-io-stats [-h] [--history HISTORY]
                                           [--records RECORDS]
                                           node_id

positional arguments:
  node_id            UUID of storage node

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh
  --records RECORDS  Number of records, default: 20

```

    
### Get node capacity statistics


```bash
usage: sbcli storage-node get-capacity [-h] [--history HISTORY] node_id

positional arguments:
  node_id            UUID of storage node

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### List storage devices


```bash
usage: sbcli storage-node list-devices [-h] [-s {node-seq,dev-seq,serial}]
                                           [--json]
                                           node_id

positional arguments:
  node_id               UUID of storage node

optional arguments:
  -h, --help            show this help message and exit
  -s {node-seq,dev-seq,serial}, --sort {node-seq,dev-seq,serial}
                        Sort the outputs
  --json                Print outputs in json format

```

    
### Set device testing mode


```bash
usage: sbcli storage-node device-testing-mode [-h]
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
usage: sbcli storage-node get-device [-h] device_id

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Reset storage device
Hardware device reset. Resetting the device can return the device from an unavailable into online state, if successful

```bash
usage: sbcli storage-node reset-device [-h] device_id

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Restart storage device
a previously removed or unavailable device may be returned into online state. If the device is not physically present, accessible or healthy, it will flip back into unavailable state again.

```bash
usage: sbcli storage-node restart-device [-h] id

positional arguments:
  id          the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Add a new storage device
Adding a device will include a previously detected device (currently in "new" state) into cluster and will launch and auto-rebalancing background process in which some cluster capacity is re-distributed to this newly added device.

```bash
usage: sbcli storage-node add-device [-h] id

positional arguments:
  id          the devices's UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove a storage device
The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection of removal did not work or if the device must be maintained otherwise while remaining inserted into the server. 

```bash
usage: sbcli storage-node remove-device [-h] [--force] device_id

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force device remove

```

    
### Set storage device to failed state
This command can be used, if an administrator believes that the device must be changed, but its status and health state do not lead to an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices in the cluster. 

```bash
usage: sbcli storage-node set-failed-device [-h] id

positional arguments:
  id          Storage device ID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get device capacity


```bash
usage: sbcli storage-node get-capacity-device [-h] [--history HISTORY]
                                                  device_id

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh

```

    
### Get device io statistics


```bash
usage: sbcli storage-node get-io-stats-device [-h] [--history HISTORY]
                                                  [--records RECORDS]
                                                  device_id

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total-, format:
                     XXdYYh
  --records RECORDS  Number of records, default: 20

```

    
### Get data interfaces list for a node


```bash
usage: sbcli storage-node port-list [-h] node_id

positional arguments:
  node_id     Storage node ID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get data interfaces io stats


```bash
usage: sbcli storage-node port-io-stats [-h] [--history HISTORY] port_id

positional arguments:
  port_id            Data port ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX
                     days and YY hours -up to 10 days in total, format: XXdYYh

```

    
### Health check storage node


```bash
usage: sbcli storage-node check [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check device


```bash
usage: sbcli storage-node check-device [-h] id

positional arguments:
  id          device UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get node information


```bash
usage: sbcli storage-node info [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get spdk memory information


```bash
usage: sbcli storage-node info-spdk [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove jm device


```bash
usage: sbcli storage-node remove-jm-device [-h] [--force] jm_device_id

positional arguments:
  jm_device_id  JM device ID

optional arguments:
  -h, --help    show this help message and exit
  --force       Force device remove

```

    
### Restart jm device


```bash
usage: sbcli storage-node restart-jm-device [-h] [--force] jm_device_id

positional arguments:
  jm_device_id  JM device ID

optional arguments:
  -h, --help    show this help message and exit
  --force       Force device remove

```

    
### Send cluster map


```bash
usage: sbcli storage-node send-cluster-map [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get cluster map


```bash
usage: sbcli storage-node get-cluster-map [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### In case of ha snode, make the current node as primary


```bash
usage: sbcli storage-node make-primary [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
### Dump lvstore data


```bash
usage: sbcli storage-node dump-lvstore [-h] id

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

    
## Cluster commands


```bash
usage: sbcli cluster [-h]
                         {create,add,activate,list,status,get,get-capacity,get-io-stats,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,cancel-task,delete}
                         ...

positional arguments:
  {create,add,activate,list,status,get,get-capacity,get-io-stats,get-logs,get-secret,upd-secret,check,update,graceful-shutdown,graceful-startup,list-tasks,cancel-task,delete}
    create              Create an new cluster with this node as mgmt (local
                        run)
    add                 Add new cluster
    activate            Create distribs and raid0 bdevs on all the storage
                        node and move the cluster to active state
    list                Show clusters list
    status              Show cluster status
    get                 Show cluster info
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
usage: sbcli cluster create [-h] [--blk_size {512,4096}]
                                [--page_size PAGE_SIZE] [--CLI_PASS CLI_PASS]
                                [--cap-warn CAP_WARN] [--cap-crit CAP_CRIT]
                                [--prov-cap-warn PROV_CAP_WARN]
                                [--prov-cap-crit PROV_CAP_CRIT]
                                [--ifname IFNAME]
                                [--log-del-interval LOG_DEL_INTERVAL]
                                [--metrics-retention-period METRICS_RETENTION_PERIOD]
                                [--contact-point CONTACT_POINT]
                                [--grafana-endpoint GRAFANA_ENDPOINT]
                                [--distr-ndcs DISTR_NDCS]
                                [--distr-npcs DISTR_NPCS]
                                [--distr-bs DISTR_BS]
                                [--distr-chunk-bs DISTR_CHUNK_BS]
                                [--ha-type {single,ha}]
                                [--enable-node-affinity]
                                [--qpair-count {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127}]
                                [--max-queue-size MAX_QUEUE_SIZE]
                                [--inflight-io-threshold INFLIGHT_IO_THRESHOLD]
                                [--disable-qos] [--strict-node-anti-affinity]

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
                        graylog deletion interval, default: 2d
  --metrics-retention-period METRICS_RETENTION_PERIOD
                        retention period for prometheus metrics, default: 7d
  --contact-point CONTACT_POINT
                        the email or slack webhook url to be used for alerting
  --grafana-endpoint GRAFANA_ENDPOINT
                        the endpoint url for grafana
  --distr-ndcs DISTR_NDCS
                        (Dev) set ndcs manually, default: 1
  --distr-npcs DISTR_NPCS
                        (Dev) set npcs manually, default: 1
  --distr-bs DISTR_BS   (Dev) distrb bdev block size, default: 4096
  --distr-chunk-bs DISTR_CHUNK_BS
                        (Dev) distrb bdev chunk block size, default: 4096
  --ha-type {single,ha}
                        LVol HA type (single, ha), default is cluster single type
  --enable-node-affinity
                        Enable node affinity for storage nodes
  --qpair-count {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127}
                        tcp transport qpair count
  --max-queue-size MAX_QUEUE_SIZE
                        The max size the queue will grow
  --inflight-io-threshold INFLIGHT_IO_THRESHOLD
                        The number of inflight IOs allowed before the IO
                        queuing starts
  --disable-qos         Disable qos bdev for storage nodes
  --strict-node-anti-affinity
                        Enable strict node anti affinity for storage nodes

```

    
### Add new cluster


```bash
usage: sbcli cluster add [-h] [--blk_size {512,4096}]
                             [--page_size PAGE_SIZE] [--cap-warn CAP_WARN]
                             [--cap-crit CAP_CRIT]
                             [--prov-cap-warn PROV_CAP_WARN]
                             [--prov-cap-crit PROV_CAP_CRIT]
                             [--distr-ndcs DISTR_NDCS]
                             [--distr-npcs DISTR_NPCS] [--distr-bs DISTR_BS]
                             [--distr-chunk-bs DISTR_CHUNK_BS]
                             [--ha-type {single,ha}] [--enable-node-affinity]
                             [--qpair-count {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127}]
                             [--max-queue-size MAX_QUEUE_SIZE]
                             [--inflight-io-threshold INFLIGHT_IO_THRESHOLD]
                             [--enable-qos] [--strict-node-anti-affinity]

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
  --distr-ndcs DISTR_NDCS
                        (Dev) set ndcs manually, default: 4
  --distr-npcs DISTR_NPCS
                        (Dev) set npcs manually, default: 1
  --distr-bs DISTR_BS   (Dev) distrb bdev block size, default: 4096
  --distr-chunk-bs DISTR_CHUNK_BS
                        (Dev) distrb bdev chunk block size, default: 4096
  --ha-type {single,ha}
                        LVol HA type (single, ha), default is cluster HA type
  --enable-node-affinity
                        Enable node affinity for storage nodes
  --qpair-count {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127}
                        tcp transport qpair count
  --max-queue-size MAX_QUEUE_SIZE
                        The max size the queue will grow
  --inflight-io-threshold INFLIGHT_IO_THRESHOLD
                        The number of inflight IOs allowed before the IO
                        queuing starts
  --enable-qos          Enable qos bdev for storage nodes
  --strict-node-anti-affinity
                        Enable strict node anti affinity for storage nodes

```

    
### Create distribs and raid0 bdevs on all the storage node and move the cluster to active state


```bash
usage: sbcli cluster activate [-h] [--force] [--force-lvstore-create]
                                  cluster_id

positional arguments:
  cluster_id            the cluster UUID

optional arguments:
  -h, --help            show this help message and exit
  --force               Force recreate distr and lv stores
  --force-lvstore-create
                        Force recreate lv stores

```

    
### Show clusters list


```bash
usage: sbcli cluster list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster status


```bash
usage: sbcli cluster status [-h] cluster_id

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Show cluster info


```bash
usage: sbcli cluster get [-h] id

positional arguments:
  id          the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get cluster capacity


```bash
usage: sbcli cluster get-capacity [-h] [--json] [--history HISTORY]
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
usage: sbcli cluster get-io-stats [-h] [--records RECORDS]
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
usage: sbcli cluster get-logs [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get cluster secret


```bash
usage: sbcli cluster get-secret [-h] cluster_id

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Updates the cluster secret


```bash
usage: sbcli cluster upd-secret [-h] cluster_id secret

positional arguments:
  cluster_id  cluster uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

    
### Health check cluster


```bash
usage: sbcli cluster check [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Update cluster mgmt services


```bash
usage: sbcli cluster update [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful shutdown of storage nodes


```bash
usage: sbcli cluster graceful-shutdown [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Graceful startup of storage nodes


```bash
usage: sbcli cluster graceful-startup [-h] [--clear-data]
                                          [--spdk-image SPDK_IMAGE]
                                          id

positional arguments:
  id                    cluster UUID

optional arguments:
  -h, --help            show this help message and exit
  --clear-data          clear Alceml data
  --spdk-image SPDK_IMAGE
                        SPDK image uri

```

    
### List tasks by cluster id


```bash
usage: sbcli cluster list-tasks [-h] cluster_id

positional arguments:
  cluster_id  UUID of the cluster

optional arguments:
  -h, --help  show this help message and exit

```

    
### Cancel task by id


```bash
usage: sbcli cluster cancel-task [-h] id

positional arguments:
  id          UUID of the Task

optional arguments:
  -h, --help  show this help message and exit

```

    
### Delete cluster
This is only possible, if no storage nodes and pools are attached to the cluster

```bash
usage: sbcli cluster delete [-h] id

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
## Lvol commands


```bash
usage: sbcli lvol [-h]
                      {add,qos-set,list,list-mem,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,check,inflate}
                      ...

positional arguments:
  {add,qos-set,list,list-mem,get,delete,connect,resize,create-snapshot,clone,move,get-capacity,get-io-stats,check,inflate}
    add                 Add a new logical volume
    qos-set             Change qos settings for an active logical volume
    list                List LVols
    list-mem            Get the size and max_size of the lvol
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
    inflate             Inflate a logical volume

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add a new logical volume


```bash
usage: sbcli lvol add [-h] [--snapshot] [--max-size MAX_SIZE]
                          [--host-id HOST_ID] [--encrypt]
                          [--crypto-key1 CRYPTO_KEY1]
                          [--crypto-key2 CRYPTO_KEY2]
                          [--max-rw-iops MAX_RW_IOPS]
                          [--max-rw-mbytes MAX_RW_MBYTES]
                          [--max-r-mbytes MAX_R_MBYTES]
                          [--max-w-mbytes MAX_W_MBYTES]
                          [--distr-vuid DISTR_VUID]
                          [--ha-type {single,ha,default}]
                          [--lvol-priority-class LVOL_PRIORITY_CLASS]
                          [--namespace NAMESPACE] [--uid UID]
                          [--pvc_name PVC_NAME]
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
  --ha-type {single,ha,default}
                        LVol HA type (single, ha), default is cluster HA type
  --lvol-priority-class LVOL_PRIORITY_CLASS
                        Lvol priority class
  --namespace NAMESPACE
                        Set LVol namespace for k8s clients
  --uid UID             Set LVol UUID
  --pvc_name PVC_NAME   Set LVol PVC name for k8s clients

```

    
### Change qos settings for an active logical volume


```bash
usage: sbcli lvol qos-set [-h] [--max-rw-iops MAX_RW_IOPS]
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
usage: sbcli lvol list [-h] [--cluster-id CLUSTER_ID] [--pool POOL]
                           [--json] [--all]

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        List LVols in particular cluster
  --pool POOL           List LVols in particular Pool ID or name
  --json                Print outputs in json format
  --all                 List soft deleted LVols

```

    
### Get the size and max_size of the lvol


```bash
usage: sbcli lvol list-mem [-h] [--json] [--csv]

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
  --csv       Print outputs in csv format

```

    
### Get lvol details


```bash
usage: sbcli lvol get [-h] [--json] id

positional arguments:
  id          LVol id or name

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Delete lvol
This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted. 

```bash
usage: sbcli lvol delete [-h] [--force] id [id ...]

positional arguments:
  id          LVol id or ids

optional arguments:
  -h, --help  show this help message and exit
  --force     Force delete LVol from the cluster

```

    
### Get lvol connection strings
Multiple connections to the cluster are always available for multi-pathing and high-availability.

```bash
usage: sbcli lvol connect [-h] id

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

    
### Resize lvol
The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below the current utilization.

```bash
usage: sbcli lvol resize [-h] id size

positional arguments:
  id          LVol id
  size        New LVol size size: 10M, 10G, 10(bytes)

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create snapshot from lvol


```bash
usage: sbcli lvol create-snapshot [-h] id name

positional arguments:
  id          LVol id
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### Create lvol based on a snapshot


```bash
usage: sbcli lvol clone [-h] [--resize RESIZE] snapshot_id clone_name

positional arguments:
  snapshot_id      snapshot UUID
  clone_name       clone name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
### Moves a full copy of the logical volume between nodes


```bash
usage: sbcli lvol move [-h] [--force] id node_id

positional arguments:
  id          LVol UUID
  node_id     Destination Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force LVol delete from source node

```

    
### Get lvol capacity


```bash
usage: sbcli lvol get-capacity [-h] [--history HISTORY] id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
### Get lvol io statistics


```bash
usage: sbcli lvol get-io-stats [-h] [--history HISTORY]
                                   [--records RECORDS]
                                   id

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).
  --records RECORDS  Number of records, default: 20

```

    
### Health check lvol


```bash
usage: sbcli lvol check [-h] id

positional arguments:
  id          UUID of LVol

optional arguments:
  -h, --help  show this help message and exit

```

    
### Inflate a logical volume
All unallocated clusters are allocated and copied from the parent or zero filled if not allocated in the parent. Then all dependencies on the parent are removed.

```bash
usage: sbcli lvol inflate [-h] lvol_id

positional arguments:
  lvol_id     cloned lvol id

optional arguments:
  -h, --help  show this help message and exit

```

    
## Management node commands


```bash
usage: sbcli mgmt [-h] {add,list,remove} ...

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
usage: sbcli mgmt add [-h] cluster_ip cluster_id cluster_secret ifname

positional arguments:
  cluster_ip      the cluster IP address
  cluster_id      the cluster UUID
  cluster_secret  the cluster secret
  ifname          Management interface name

optional arguments:
  -h, --help      show this help message and exit

```

    
### List management nodes


```bash
usage: sbcli mgmt list [-h] [--json]

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Remove management node


```bash
usage: sbcli mgmt remove [-h] id

positional arguments:
  id          Mgmt node uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
## Pool commands


```bash
usage: sbcli pool [-h]
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
    get-secret          Get pool secret
    upd-secret          Updates pool secret
    get-capacity        Get pool capacity
    get-io-stats        Get pool IO statistics

optional arguments:
  -h, --help            show this help message and exit

```

    
### Add a new pool


```bash
usage: sbcli pool add [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX]
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
usage: sbcli pool set [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX]
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
usage: sbcli pool list [-h] [--json] [--cluster-id CLUSTER_ID]

optional arguments:
  -h, --help            show this help message and exit
  --json                Print outputs in json format
  --cluster-id CLUSTER_ID
                        ID of the cluster

```

    
### Get pool details


```bash
usage: sbcli pool get [-h] [--json] id

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

    
### Delete pool
It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).

```bash
usage: sbcli pool delete [-h] id

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Set pool status to active


```bash
usage: sbcli pool enable [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Set pool status to inactive.


```bash
usage: sbcli pool disable [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get pool secret


```bash
usage: sbcli pool get-secret [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Updates pool secret


```bash
usage: sbcli pool upd-secret [-h] pool_id secret

positional arguments:
  pool_id     pool uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get pool capacity


```bash
usage: sbcli pool get-capacity [-h] pool_id

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get pool io statistics


```bash
usage: sbcli pool get-io-stats [-h] [--history HISTORY]
                                   [--records RECORDS]
                                   id

positional arguments:
  id                 Pool id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).
  --records RECORDS  Number of records, default: 20

```

    
## Snapshot commands


```bash
usage: sbcli snapshot [-h] {add,list,delete,clone} ...

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
usage: sbcli snapshot add [-h] id name

positional arguments:
  id          LVol UUID
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

    
### List snapshots


```bash
usage: sbcli snapshot list [-h] [--all]

optional arguments:
  -h, --help  show this help message and exit
  --all       List soft deleted snapshots

```

    
### Delete a snapshot


```bash
usage: sbcli snapshot delete [-h] [--force] id

positional arguments:
  id          snapshot UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force remove

```

    
### Create lvol from snapshot


```bash
usage: sbcli snapshot clone [-h] [--resize RESIZE] id lvol_name

positional arguments:
  id               snapshot UUID
  lvol_name        LVol name

optional arguments:
  -h, --help       show this help message and exit
  --resize RESIZE  New LVol size: 10M, 10G, 10(bytes)

```

    
## Caching client node commands


```bash
usage: sbcli caching-node [-h]
                              {deploy,add-node,list,list-lvols,remove,connect,disconnect,recreate,get-lvol-stats}
                              ...

positional arguments:
  {deploy,add-node,list,list-lvols,remove,connect,disconnect,recreate,get-lvol-stats}
    deploy              Deploy caching node on this machine (local exec)
    add-node            Add new Caching node to the cluster
    list                List Caching nodes
    list-lvols          List connected lvols
    remove              Remove Caching node from the cluster
    connect             Connect to LVol
    disconnect          Disconnect LVol from Caching node
    recreate            recreate Caching node bdevs
    get-lvol-stats      Get LVol stats

optional arguments:
  -h, --help            show this help message and exit

```

    
### Deploy caching node on this machine (local exec)


```bash
usage: sbcli caching-node deploy [-h] [--ifname IFNAME]

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

    
### Add new caching node to the cluster


```bash
usage: sbcli caching-node add-node [-h] [--cpu-mask SPDK_CPU_MASK]
                                       [--memory SPDK_MEM]
                                       [--spdk-image SPDK_IMAGE]
                                       [--namespace NAMESPACE]
                                       [--multipathing {on,off}]
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
  --multipathing {on,off}
                        Enable multipathing for lvol connection, default: on

```

    
### List caching nodes


```bash
usage: sbcli caching-node list [-h]

optional arguments:
  -h, --help  show this help message and exit

```

    
### List connected lvols


```bash
usage: sbcli caching-node list-lvols [-h] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Remove caching node from the cluster


```bash
usage: sbcli caching-node remove [-h] [--force] id

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force remove

```

    
### Connect to lvol


```bash
usage: sbcli caching-node connect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Disconnect lvol from caching node


```bash
usage: sbcli caching-node disconnect [-h] node_id lvol_id

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Recreate caching node bdevs


```bash
usage: sbcli caching-node recreate [-h] node_id

positional arguments:
  node_id     Caching node UUID

optional arguments:
  -h, --help  show this help message and exit

```

    
### Get lvol stats


```bash
usage: sbcli caching-node get-lvol-stats [-h] [--history HISTORY] lvol_id

positional arguments:
  lvol_id            LVol UUID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes)
                     for XX days and YY hours (up to 10 days in total).

```

    
