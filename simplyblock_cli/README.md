# Simply Block CLI

## Install
```bash
pip install sbcli
```

## Command Line Interface
```bash
$ sbcli --help
usage: sbcli [-h] [-d] {storage-node,cluster,lvol,mgmt,pool} ...

Ultra management CLI

positional arguments:
  {storage-node,cluster,lvol,mgmt-node,pool}
    storage-node        Storage node commands
    cluster             Cluster commands
    lvol                lvol commands
    mgmt           Management node commands
    pool                Pool commands

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Print debug messages

```


## Cluster commands
```bash
$ sbcli cluster -h
usage: sbcli cluster [-h]
                         {create,list,status,get,suspend,unsuspend,add-dev-model,rm-dev-model,add-host-auth,rm-host-auth,get-capacity,get-io-stats,set-log-level,get-cli-ssh-pass,get-logs,get-secret,set-secret,check,update,graceful-shutdown,graceful-startup}
                         ...

Cluster commands

positional arguments:
  {create,list,status,get,suspend,unsuspend,add-dev-model,rm-dev-model,add-host-auth,rm-host-auth,get-capacity,get-io-stats,set-log-level,get-cli-ssh-pass,get-logs,get-secret,set-secret,check,update,graceful-shutdown,graceful-startup}

    create              Create an new cluster with this node as mgmt (local run)

    list                Show clusters list

    status              Show cluster status

    get                 Show cluster info

    suspend             Suspend cluster. The cluster will stop processing all IO. Attention! This will cause an "all paths down" event for nvmeof/iscsi volumes on all hosts connected to the cluster.

    unsuspend           Unsuspend cluster. The cluster will start processing IO again.

    get-capacity        Returns the current total available capacity, utilized capacity (in percent and absolute) and provisioned capacity (in percent and absolute) in GB
                        in the cluster.

    get-io-stats        Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to
                        ping):read-iops write-iops total-iops read-mbs write-mbs total-mbs

    set-log-level       Defines the detail of the log information collected and stored

    get-cli-ssh-pass    returns the ssh password for the CLI ssh connection

    get-logs            Returns distr logs

    get-secret          Returns auto generated, 20 characters secret.

    set-secret          Updates the secret (replaces the existing one with a new one) and returns the new one.

    check               Health check cluster

    update              Update cluster mgmt services

    graceful-shutdown   Graceful shutdown of storage nodes

    graceful-startup    Graceful startup of storage nodes

optional arguments:
  -h, --help            show this help message and exit
```

### Create Cluster
```bash
$ sbcli cluster create -h

usage: sbcli cluster create [-h] [--blk_size {512,4096}] [--page_size PAGE_SIZE] [--ha_type {single,ha}] [--tls {on,off}] [--auth-hosts-only {on,off}]
                                [--model_ids MODEL_IDS [MODEL_IDS ...]] [--CLI_PASS CLI_PASS] [--cap-warn CAP_WARN] [--cap-crit CAP_CRIT] [--prov-cap-warn PROV_CAP_WARN]
                                [--prov-cap-crit PROV_CAP_CRIT] [--ifname IFNAME] [--log-del-interval LOG_DEL_INTERVAL]
                                [--metrics-retention-period METRICS_RETENTION_PERIOD]

Create an new cluster with this node as mgmt (local run)

optional arguments:
  -h, --help            show this help message and exit

  --blk_size {512,4096}
                        The block size in bytes

  --page_size PAGE_SIZE
                        The size of a data page in bytes

  --ha_type {single,ha}
                        Can be "single" for single node clusters or "HA", which requires at least 3 nodes

  --tls {on,off}        TCP/IP transport security can be turned on and off. If turned on, both hosts and storage nodes must authenticate the connection via TLS certificates

  --auth-hosts-only {on,off}
                        if turned on, hosts must be explicitely added to the cluster to be able to connect to any NVMEoF subsystem in the cluster

  --model_ids MODEL_IDS [MODEL_IDS ...]
                        a list of supported NVMe device model-ids

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

### List Clusters
```bash
$ sbcli cluster list -h
usage: sbcli cluster list [-h]

Show clusters list

optional arguments:
  -h, --help  show this help message and exit

```
### Show cluster status
```bash
$ sbcli cluster status -h
usage: sbcli cluster status [-h] cluster_id

Show cluster status

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Show Cluster Info
```bash
$ sbcli cluster get -h
usage: sbcli cluster get [-h] id

Show cluster info

positional arguments:
  id          the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Suspend cluster
```bash
$ sbcli cluster suspend -h
usage: sbcli cluster suspend [-h] cluster_id

Suspend cluster. The cluster will stop processing all IO.
Attention! This will cause an "all paths down" event for nvmeof/iscsi volumes on all hosts connected to the cluster.

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Unsuspend cluster
```bash
$ sbcli cluster unsuspend -h
usage: sbcli cluster unsuspend [-h] cluster_id

Unsuspend cluster. The cluster will start processing IO again.

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Get total cluster capacity
```bash
$ sbcli cluster get-capacity -h
usage: sbcli cluster get-capacity [-h] [--json] [--history HISTORY] cluster_id

Returns the current total available capacity, utilized capacity (in percent and absolute) and provisioned capacity (in percent and absolute) in GB in the cluster.

positional arguments:
  cluster_id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --json             Print json output
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).
```

### Return IO statistics of a cluster
```bash
$ sbcli cluster get-io-stats -h
usage: sbcli cluster get-io-stats [-h] [--records RECORDS] [--history HISTORY] [--random] cluster_id

Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops write-
iops total-iops read-mbs write-mbs total-mbs

positional arguments:
  cluster_id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --records RECORDS  Number of records, default: 20
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).
  --random           Generate random data
```

### Get SSH Password for CLI SSH Connection
```bash
$ sbcli cluster get-cli-ssh-pass -h
usage: sbcli cluster get-cli-ssh-pass [-h] cluster_id

returns the ssh password for the CLI ssh connection

positional arguments:
  cluster_id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

### returns distr logs
```bash
$ sbcli cluster get-logs -h
usage: sbcli cluster get-logs [-h] cluster_id

Returns distr logs

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit
```

### Get Cluster Auto-generated Secret
```bash
$ sbcli cluster get-secret -h
usage: sbcli cluster get-secret [-h] cluster_id

Returns auto generated, 20 characters secret.

positional arguments:
  cluster_id  cluster uuid

optional arguments:
  -h, --help  show this help message and exit

```

### Set Cluster Secret
```bash
$ sbcli cluster set-secret -h
usage: sbcli cluster set-secret [-h] cluster_id secret

Updates the secret (replaces the existing one with a new one) and returns the new one.

positional arguments:
  cluster_id  cluster uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

### Health Check Cluster
```bash
$ sbcli cluster check -h
usage: sbcli cluster check [-h] id

Health check cluster

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Update Cluster Management Services
```bash
$ sbcli cluster update -h
usage: sbcli cluster update [-h] id

Update cluster mgmt services

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Cluster Graceful shutdown
```bash
sbcli cluster graceful-shutdown -h
usage: sbcli cluster graceful-shutdown [-h] id

Graceful shutdown of storage nodes

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Cluster Graceful start
```bash
sbcli cluster graceful-startup -h
usage: sbcli cluster graceful-startup [-h] id

Graceful startup of storage nodes

positional arguments:
  id          cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

## Storage node commands
```bash
$ sbcli storage-node -h
usage: sbcli storage-node [-h]
                              {deploy,deploy-cleaner,add-node,delete,remove,list,get,update,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,run-smart,add-device,remove-device,set-ro-device,set-failed-device,set-online-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,get-host-secret,get-ctrl-secret,check,check-device,info,info-spdk}


Storage node commands

positional arguments:
  {deploy,deploy-cleaner,add-node,delete,remove,list,get,update,restart,shutdown,suspend,resume,get-io-stats,get-capacity,list-devices,device-testing-mode,get-device,reset-device,restart-device,run-smart,add-device,remove-device,set-ro-device,set-failed-device,set-online-device,get-capacity-device,get-io-stats-device,port-list,port-io-stats,get-host-secret,get-ctrl-secret,check,check-device,info,info-spdk}

    deploy              Deploy local services for remote ops (local run)

    deploy-cleaner      clean local deploy (local run)

    add-node            Add storage node by ip

    delete              Delete storage node obj

    remove              Remove storage node

    list                List storage nodes

    get                 Get storage node info

    update              Update storage node db info

    restart             Restart a storage node. All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup,
                        this will not impact operations.

    shutdown            Shutdown a storage node. Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed.
                        In a high-availability setup, this will not impact operations.

    suspend             Suspend a storage node. The node will stop accepting new IO, but will finish processing any IO, which has been received already.

    resume              Resume a storage node

    get-io-stats        Returns the current io statistics of a node

    get-capacity        Returns the size, absolute and relative utilization of the node in bytes

    list-devices        List storage devices

    device-testing-mode
                        set pt testing bdev mode

    get-device          Get storage device by id

    reset-device        Reset storage device

    restart-device      Re add "removed" storage device

    run-smart           Run tests against storage device

    add-device          Add a new storage device

    remove-device       Remove a storage device. The device will become unavailable, independently if it was physically removed from the server. This function can be used
                        if auto-detection of removal did not work or if the device must be maintained otherwise while remaining inserted into the server.

    set-ro-device       Set storage device read only

    set-failed-device   Set storage device to failed state. This command can be used, if an administrator believes that the device must be changed, but its status and
                        health state do not lead to an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be
                        automatically recovered to other devices in the cluster.

    set-online-device   Set storage device to online state

    get-capacity-device
                        Returns the size, absolute and relative utilization of the device in bytes

    get-io-stats-device
                        Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to
                        ping):read-iops write-iops total-iops read-mbs write-mbs total-mbs

    port-list           Get Data interfaces list for a node

    port-io-stats       Get Data interfaces IO stats

    get-host-secret     Returns the auto-generated host secret required for the nvmeof connection between host and cluster

    get-ctrl-secret     Returns the auto-generated controller secret required for the nvmeof connection between host and cluster

    check               Health check storage node

    check-device        Health check device

    info                Get node information

    info-spdk           Get SPDK memory information

optional arguments:
  -h, --help            show this help message and exit

```

### Storage node deploy
- must be run on the storage node itself
```bash
$ sbcli storage-node deploy -h
usage: sbcli storage-node deploy [-h] [--ifname IFNAME]

Deploy local services for remote ops (local run)

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0
```

### clean local deploy
- must be run on the storage node itself
```bash
$ sbcli storage-node deploy-cleaner -h
usage: sbcli storage-node deploy-cleaner [-h]

clean local deploy (local run)

optional arguments:
  -h, --help  show this help message and exit
```

### Add new storage node
- must be run on the storage node itself
```bash
$ sbcli storage-node add-node -h
usage: sbcli storage-node add-node [-h] [--jm-pcie JM_PCIE] [--data-nics DATA_NICS [DATA_NICS ...]] [--cpu-mask SPDK_CPU_MASK] [--memory SPDK_MEM]
                                       [--dev-split DEV_SPLIT] [--spdk-image SPDK_IMAGE] [--spdk-debug] [--iobuf_small_pool_count SMALL_POOL_COUNT]
                                       [--iobuf_large_pool_count LARGE_POOL_COUNT] [--iobuf_small_bufsize SMALL_BUFSIZE] [--iobuf_large_bufsize LARGE_BUFSIZE]
                                       cluster_id node_ip ifname

Add storage node by ip

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
  --dev-split DEV_SPLIT
                        Split nvme devices by this factor, can be 2 or more
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

### delete storage node
```bash
$ sbcli storage-node delete -h
usage: sbcli storage-node delete [-h] node_id

Delete storage node obj

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
```

### Remove storage node
```bash
$ sbcli storage-node remove -h
usage: sbcli storage-node remove [-h] [--force-remove] node_id

Remove storage node

positional arguments:
  node_id          UUID of storage node

optional arguments:
  -h, --help       show this help message and exit
  --force-remove   Force remove all LVols and snapshots
```

### List storage nodes
```bash
$ sbcli storage-node remove -h
usage: sbcli storage-node remove [-h] [--force-remove] node_id

Remove storage node

positional arguments:
  node_id          UUID of storage node

optional arguments:
  -h, --help       show this help message and exit
  --force-remove   Force remove all LVols and snapshots
  --force-migrate  Force migrate All LVols to other nodes
sbcli storage-node list -h
usage: sbcli storage-node list [-h] [--cluster-id CLUSTER_ID] [--json]

List storage nodes

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        id of the cluster for which nodes are listed
  --json                Print outputs in json format

```

### Get a storage node
```bash
$ sbcli storage-node get -h
usage: sbcli storage-node get [-h] id

Get storage node info

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit

```

### Update storage node db info
```bash
$ sbcli storage-node update -h
usage: sbcli storage-node update [-h] id key value

Update storage node db info

positional arguments:
  id          UUID of storage node
  key         Key
  value       Value

optional arguments:
  -h, --help  show this help message and exit

```


### Restart storage node
- must be run on the storage node itself
```bash
$ sbcli storage-node restart -h
usage: sbcli storage-node restart [-h] [--cpu-mask SPDK_CPU_MASK] [--memory SPDK_MEM] [--spdk-image SPDK_IMAGE] [--spdk-debug]
                                      [--iobuf_small_pool_count SMALL_POOL_COUNT] [--iobuf_large_pool_count LARGE_POOL_COUNT] [--iobuf_small_bufsize SMALL_BUFSIZE]
                                      [--iobuf_large_bufsize LARGE_BUFSIZE]
                                      node_id

Restart a storage node. All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not impact
operations.

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

### Shutdown a storage node
```bash
$ sbcli storage-node shutdown -h
usage: sbcli storage-node shutdown [-h] [--force] node_id

Shutdown a storage node. Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-availability
setup, this will not impact operations.

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node shutdown

```

### Suspend a storage node
```bash
$ sbcli storage-node suspend -h
usage: sbcli storage-node suspend [-h] [--force] node_id

Suspend a storage node. The node will stop accepting new IO, but will finish processing any IO, which has been received already.

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
  --force     Force node suspend

```

### Resume a storage node
```bash
$ sbcli storage-node resume -h
usage: sbcli storage-node resume [-h] node_id

Resume a storage node

positional arguments:
  node_id     UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
```

### Returns the current io statistics of a node
```bash
$ sbcli storage-node get-io-stats -h
usage: sbcli storage-node get-io-stats [-h] [--history HISTORY] node_id

Returns the current io statistics of a node

positional arguments:
  node_id            Node ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh
```

### List storage devices
```bash
$ sbcli storage-node list-devices -h
usage: sbcli storage-node list-devices [-h] node-id [-a] [-s {node-seq,dev-seq,serial}] [--json]

List storage devices

positional arguments:
  node-id               the node's UUID

optional arguments:
  -h, --help            show this help message and exit
  -a, --all             List all devices in the cluster
  -s {node-seq,dev-seq,serial}, --sort {node-seq,dev-seq,serial}
                        Sort the outputs
  --json                Print outputs in json format

```
### Get storage node capacity
```bash
$ sbcli storage-node get-capacity -h
usage: sbcli storage-node get-capacity [-h] [--history HISTORY] node_id

Returns the size, absolute and relative utilization of the node in bytes

positional arguments:
  node_id            Node ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: X
```

### List storage devices
```bash
$ sbcli storage-node list-devices -h
usage: sbcli storage-node list-devices [-h] [-s {node-seq,dev-seq,serial}] [--json] node_id

List storage devices

positional arguments:
  node_id               the node's UUID

optional arguments:
  -h, --help            show this help message and exit
  -s {node-seq,dev-seq,serial}, --sort {node-seq,dev-seq,serial}
                        Sort the outputs
  --json                Print outputs in json format
```

### set pt testing bdev mode
```bash
$ sbcli storage-node device-testing-mode -h
usage: sbcli storage-node device-testing-mode [-h]
                                                  device_id
                                                  {full_pass_trhough,io_error_on_read,io_error_on_write,io_error_on_unmap,io_error_on_all,discard_io_all,hotplug_removal}

set pt testing bdev mode

positional arguments:
  device_id             Device UUID
  {full_pass_trhough,io_error_on_read,io_error_on_write,io_error_on_unmap,io_error_on_all,discard_io_all,hotplug_removal}
                        Testing mode

optional arguments:
  -h, --help            show this help message and exit
```


### Get storage device by ID
```bash
$ sbcli storage-node get-device -h
usage: sbcli storage-node get-device [-h] device_id

Get storage device by id

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Reset a storage device
```bash
$ sbcli storage-node reset-device -h
usage: sbcli storage-node reset-device [-h] device_id

Reset storage device

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Restart a device
```bash
$ sbcli storage-node restart-device -h
usage: sbcli storage-node restart-device [-h] id

Re add "removed" storage device

positional arguments:
  id          the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Run smart tests against a storage device
```bash
$ sbcli storage-node run-smart -h
usage: sbcli storage-node run-smart [-h] device_id

Run tests against storage device

positional arguments:
  device_id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Add a new storage device
```bash
$ sbcli storage-node add-device -h
usage: sbcli storage-node add-device [-h] name node_id cluster_id

Add a new storage device

positional arguments:
  name        Storage device name (as listed in the operating system). The device will be de-attached from the operating system and attached to the storage node
  node_id     UUID of the node
  cluster_id  UUID of the cluster

optional arguments:
  -h, --help  show this help message and exit
```

### Replace a storage node
```bash
$ sbcli storage-node replace -h
usage: sbcli storage-node replace [-h] node-id ifname [--data-nics DATA_NICS [DATA_NICS ...]]

Replace a storage node. This command is run on the new physical server, which is expected to replace the old server. Attention!!! All the nvme devices, which are part of the cluster to
which the node belongs, must be inserted into the new server before this command is run. The old node will be de-commissioned and cannot be used any more.

positional arguments:
  node-id               UUID of the node to be replaced
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --data-nics DATA_NICS [DATA_NICS ...]
                        Data interface names
```

### Remove a storage device
```bash
$ sbcli storage-node remove-device -h
usage: sbcli storage-node remove-device [-h] [--force] device_id

Remove a storage device. The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection of
removal did not work or if the device must be maintained otherwise while remaining inserted into the server.

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force device remove
```

### Set storage device to read only
```bash
$ sbcli storage-node set-ro-device -h
usage: sbcli storage-node set-ro-device [-h] device_id

Set storage device read only

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
```


### Set storage device to failed state
```bash
$ sbcli storage-node set-failed-device -h
usage: sbcli storage-node set-failed-device [-h] device_id

Set storage device to failed state. This command can be used, if an administrator believes that the device must be changed, but its status and health state do not lead to
an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices in the cluster.

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
```

### Set storage device to online state
```bash
$ sbcli storage-node set-online-device -h
usage: sbcli storage-node set-online-device [-h] device_id

Set storage device to online state

positional arguments:
  device_id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit

```

### Returns the size of a device
```bash
$ sbcli storage-node get-capacity-device -h
usage: sbcli storage-node get-capacity-device [-h] [--history HISTORY] device_id

Returns the size, absolute and relative utilization of the device in bytes

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh
```

### Returns the IO statistics of a device
```bash
$ sbcli storage-node get-io-stats-device -h
usage: sbcli storage-node get-io-stats-device [-h] [--history HISTORY] device_id

Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops write-
iops total-iops read-mbs write-mbs total-mbs

positional arguments:
  device_id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh

```

### Get data interface list for a node
```bash
$ sbcli storage-node port-list -h
usage: sbcli storage-node port-list [-h] node_id

Get Data interfaces list for a node

positional arguments:
  node_id     Storage node ID

optional arguments:
  -h, --help  show this help message and exit
```

### Get data interface IO Stats
```bash
$ sbcli storage-node port-io-stats -h
usage: sbcli storage-node port-io-stats [-h] [--history HISTORY] port_id

Get Data interfaces IO stats

positional arguments:
  port_id            Data port ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total, format: XXdYYh
```

### Host secret for nvme connection
```bash
sbcli storage-node get-host-secret -h
usage: sbcli storage-node get-host-secret [-h] id

Returns the auto-generated host secret required for the nvmeof connection between host and cluster

positional arguments:
  id          Storage node ID

optional arguments:
  -h, --help  show this help message and exit
```

### Controller secret for nvme connection
```bash
$ sbcli storage-node get-ctrl-secret -h
usage: sbcli storage-node get-ctrl-secret [-h] id

Returns the auto-generated controller secret required for the nvmeof connection between host and cluster

positional arguments:
  id          Storage node ID

optional arguments:
  -h, --help  show this help message and exit
```

### Health check storage node
```bash
$ sbcli storage-node check -h
usage: sbcli storage-node check [-h] id

Health check storage node

positional arguments:
  id          UUID of storage node

optional arguments:
  -h, --help  show this help message and exit
```

### Health check device
```bash
$ sbcli storage-node check-device -h
usage: sbcli storage-node check-device [-h] id

Health check device

positional arguments:
  id          device UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Get node information
```bash
$ sbcli storage-node info -h
usage: sbcli storage-node info [-h] id

Get node information

positional arguments:
  id          Node UUID

optional arguments:
  -h, --help  show this help message and exit
```


### Get SPDK Memory Information
```bash
$ sbcli storage-node info-spdk -h
usage: sbcli storage-node info-spdk [-h] id

Get SPDK memory information

positional arguments:
  id          Node UUID

optional arguments:
  -h, --help  show this help message and exit
```


## LVol commands
```bash
$ sbcli lvol -h
usage: sbcli lvol [-h]
                      {add,qos-set,list,list-mem,get,delete,connect,resize,set-read-only,set-read-write,suspend,unsuspend,create-snapshot,clone,move,replicate,inflate,get-capacity,get-io-stats,send-cluster-map,get-cluster-map,check}
                      ...

LVol commands

positional arguments:
  {add,qos-set,list,list-mem,get,delete,connect,resize,set-read-only,set-read-write,suspend,unsuspend,create-snapshot,clone,move,replicate,inflate,get-capacity,get-io-stats,send-cluster-map,get-cluster-map,check}
    add                 Add a new logical volume
    qos-set             Change qos settings for an active logical volume
    list                List all LVols
    list-mem            List all LVols
    get                 Get LVol details
    delete              Delete LVol. This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be
                        deleted.
    connect             show connection strings to cluster. Multiple connections to the cluster are always available for multi-pathing and high-availability.
    resize              Resize LVol. The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below
                        the current utilization.
    set-read-only       Set LVol Read-only. Current write IO in flight will still be processed, but for new IO, only read and unmap IO are possible.
    set-read-write      Set LVol Read-Write.
    suspend             Suspend LVol. IO in flight will still be processed, but new IO is not accepted. Make sure that the volume is detached from all hosts before
                        suspending it to avoid IO errors.
    unsuspend           Unsuspend LVol. IO may be resumed.
    create-snapshot     Create snapshot from LVol
    clone               create LVol based on a snapshot
    move                Moves a full copy of the logical volume between nodes
    replicate           Create a replication path between two volumes in two clusters
    inflate             Inflate a clone to "full" logical volume and disconnect it from its parent snapshot.
    get-capacity        Returns the current (or historic) provisioned and utilized (in percent and absolute) capacity.
    get-io-stats        Returns either the current or historic io statistics (read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).
    send-cluster-map    send distr cluster map
    get-cluster-map     get distr cluster map
    check               Health check LVol

optional arguments:
  -h, --help            show this help message and exit

```

### Add LVol
```bash
$ sbcli lvol add -h
usage: sbcli lvol add [-h] [--snapshot] [--max-size MAX_SIZE] [--host-id HOST_ID] [--ha-type {single,ha,default}] [--compress] [--encrypt] [--crypto-key1 CRYPTO_KEY1]
                          [--crypto-key2 CRYPTO_KEY2] [--node-ha {0,1,2}] [--dev-redundancy] [--max-rw-iops MAX_RW_IOPS] [--max-rw-mbytes MAX_RW_MBYTES]
                          [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES] [--distr-vuid DISTR_VUID] [--distr-ndcs DISTR_NDCS] [--distr-npcs DISTR_NPCS]
                          [--distr-bs DISTR_BS] [--distr-chunk-bs DISTR_CHUNK_BS]
                          name size pool

Add a new logical volume

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
  --compress            Use inline data compression and de-compression on the logical volume
  --encrypt             Use inline data encryption and de-cryption on the logical volume
  --crypto-key1 CRYPTO_KEY1
                        the hex value of key1 to be used for lvol encryption
  --crypto-key2 CRYPTO_KEY2
                        the hex value of key2 to be used for lvol encryption
  --node-ha {0,1,2}     The maximum amount of concurrent node failures accepted without interruption of operations
  --dev-redundancy      {1,2} supported minimal concurrent device failures without data loss
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

### Set QOS options for LVol
```bash
$ sbcli lvol qos-set -h
usage: sbcli lvol qos-set [-h] [--max-rw-iops MAX_RW_IOPS] [--max-rw-mbytes MAX_RW_MBYTES] [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES] id

Change qos settings for an active logical volume

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

### List LVols
```bash
$ sbcli lvol list -h
usage: sbcli lvol list [-h] [--cluster-id CLUSTER_ID] [--json]

List all LVols

optional arguments:
  -h, --help            show this help message and exit
  --cluster-id CLUSTER_ID
                        List LVols in particular cluster
  --json                Print outputs in json format
```

### Get the size and max_size of the lvol
```bash
$ sbcli lvol list-mem -h
usage: sbcli lvol list-mem [-h] [--json] [--csv]

Get the size and max_size of the lvol

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
  --csv       Print outputs in csv format

```

### Get LVol details
```bash
$ sbcli lvol get -h
usage: sbcli lvol get [-h] [--json] id

Get LVol details

positional arguments:
  id          LVol id or name

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
```

### Delete LVol
```bash
$ sbcli lvol delete -h
usage: sbcli lvol delete [-h] [--force] id [id ...]

Delete LVol. This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted.

positional arguments:
  id          LVol id or ids

optional arguments:
  -h, --help  show this help message and exit
  --force     Force delete LVol from the cluster
```

### Show nvme-cli connection commands
```bash
$ sbcli lvol connect -h
usage: sbcli lvol connect [-h] id

show connection strings to cluster. Multiple connections to the cluster are always available for multi-pathing and high-availability.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Resize
```bash
$ sbcli lvol resize -h
usage: sbcli lvol resize [-h] id size

Resize LVol. The lvol cannot be exceed the maximum size for lvols. It cannot exceed total remaining provisioned space in pool. It cannot drop below the current utilization.

positional arguments:
  id          LVol id
  size        New LVol size size: 10M, 10G, 10(bytes)

optional arguments:
  -h, --help  show this help message and exit
```

### Set read only
```bash
$ sbcli lvol set-read-only -h
usage: sbcli lvol set-read-only [-h] id

Set LVol Read-only. Current write IO in flight will still be processed, but for new IO, only read and unmap IO are possible.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Set read-write
```bash
$ sbcli lvol set-read-write -h
usage: sbcli lvol set-read-write [-h] id

Set LVol Read-Write.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Suspend
```bash
$ sbcli lvol suspend -h
usage: sbcli lvol suspend [-h] id

Suspend LVol. IO in flight will still be processed, but new IO is not accepted. Make sure that the volume is detached from all hosts before suspending it to avoid IO
errors.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Unsuspend
```bash
$ sbcli lvol unsuspend -h
usage: sbcli lvol unsuspend [-h] id

Unsuspend LVol. IO may be resumed.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```


### Create Snapshot from LVol
```bash
$ sbcli lvol create-snapshot -h
usage: sbcli lvol create-snapshot [-h] id name

Create snapshot from LVol

positional arguments:
  id          LVol id
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit
```

### Clone: Create LVol based on a snapshot
```bash
$ sbcli lvol clone -h
usage: sbcli lvol clone [-h] snapshot_id clone_name

create LVol based on a snapshot

positional arguments:
  snapshot_id  snapshot UUID
  clone_name   clone name

optional arguments:
  -h, --help   show this help message and exit
```

### Move: Moves a full copy of the logical volume between nodes
```bash
$ sbcli lvol move -h
usage: sbcli lvol move [-h] [--force] id node_id

Moves a full copy of the logical volume between nodes

positional arguments:
  id          LVol UUID
  node_id     Destination Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force LVol delete from source node
```

### Replicate: Create a replication path between two volumes in two clusters
```bash
$ sbcli lvol replicate -h
usage: sbcli lvol replicate [-h] [--asynchronous] id cluster-a cluster-b

Create a replication path between two volumes in two clusters

positional arguments:
  id              LVol id
  cluster-a       A Cluster ID
  cluster-b       B Cluster ID

optional arguments:
  -h, --help      show this help message and exit
  --asynchronous  Replication may be performed synchronously(default) and asynchronously
```

### Inflate a clone to full logical volume
```bash
$ sbcli lvol inflate -h
usage: sbcli lvol inflate [-h] id

Inflate a clone to "full" logical volume and disconnect it from its parent snapshot.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Get Capacity: Current provisioned and Utilised capacity
```bash
$ sbcli lvol get-capacity -h
usage: sbcli lvol get-capacity [-h] [--history HISTORY] id

Returns the current (or historic) provisioned and utilized (in percent and absolute) capacity.

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).
```

### Get IO Stats
```bash
$ sbcli lvol get-io-stats -h
usage: sbcli lvol get-io-stats [-h] [--history HISTORY] id

Returns either the current or historic io statistics (read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).

positional arguments:
  id                 LVol id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).
```

### send distr cluster map
```bash
$ sbcli lvol send-cluster-map -h
usage: sbcli lvol send-cluster-map [-h] id

send distr cluster map

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Get distr cluster map
```bash
$ sbcli lvol get-cluster-map -h
usage: sbcli lvol get-cluster-map [-h] id

get distr cluster map

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Health check lvol
```bash
$ sbcli lvol check -h
usage: sbcli lvol check [-h] id

Health check LVol

positional arguments:
  id          UUID of LVol

optional arguments:
  -h, --help  show this help message and exit
```

## Management node commands
```bash
$ sbcli mgmt -h
usage: sbcli mgmt [-h] {add,list,remove,show,status} ...

Management node commands

positional arguments:
  {add,list,remove,show,status}
    add                 Add Management node to the cluster
    list                List Management nodes
    remove              Remove Management node
    show                List management nodes
    status              Show management cluster status

optional arguments:
  -h, --help            show this help message and exit
```

### Add management node
```bash
$ sbcli mgmt add -h
usage: sbcli mgmt add [-h] cluster_ip cluster_id ifname

Add Management node to the cluster

positional arguments:
  cluster_ip  the cluster IP address
  cluster_id  the cluster UUID
  ifname      Management interface name

optional arguments:
  -h, --help  show this help message and exit
```

### List management nodes
```bash
$ sbcli mgmt list -h
usage: sbcli mgmt list [-h] [--json]

List Management nodes

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
```

### Remove management node
```bash
$ sbcli mgmt remove -h
usage: sbcli mgmt remove [-h] hostname

Remove Management node

positional arguments:
  hostname    hostname

optional arguments:
  -h, --help  show this help message and exit
```

### Show management nodes
```bash
$ sbcli mgmt show -h
usage: sbcli mgmt show [-h]

List management nodes

optional arguments:
  -h, --help  show this help message and exit
```

### Show management cluster status
```bash
$ sbcli mgmt status -h
usage: sbcli mgmt status [-h]

Show management cluster status

optional arguments:
  -h, --help  show this help message and exit
```


## Pool commands
```bash
$ sbcli pool -h
usage: sbcli pool [-h] {add,set,list,get,delete,enable,disable,get-secret,upd-secret,get-capacity,get-io-stats} ...

Pool commands

positional arguments:
  {add,set,list,get,delete,enable,disable,get-secret,upd-secret,get-capacity,get-io-stats}
    add                 Add a new Pool
    set                 Set pool attributes
    list                List pools
    get                 get pool details
    delete              delete pool. It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).
    enable              Set pool status to Active
    disable             Set pool status to Inactive. Attention! This will suspend all new IO to the pool! IO in flight processing will be completed.
    get-secret          Returns auto generated, 20 characters secret.
    upd-secret          Updates the secret (replaces the existing one with a new one) and returns the new one.
    get-capacity        Return provisioned, utilized (absolute) and utilized (percent) storage on the Pool.
    get-io-stats        Returns either the current or historic io statistics (read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).

optional arguments:
  -h, --help            show this help message and exit

```

### Add pool
 - QOS parameters are optional but when used in a pool, new Lvol creation will require the active qos parameter.
 - User can use both QOS parameters (--max-rw-iops, --max-rw-mbytes) and will apply which limit comes first.
```bash
$ sbcli pool add -h
usage: sbcli pool add [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX] [--max-rw-iops MAX_RW_IOPS] [--max-rw-mbytes MAX_RW_MBYTES] [--max-r-mbytes MAX_R_MBYTES]
                          [--max-w-mbytes MAX_W_MBYTES] [--has-secret]
                          name

Add a new Pool

positional arguments:
  name                  Pool name

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
  --has-secret          Pool is created with a secret (all further API interactions with the pool and logical volumes in the pool require this secret)

```

### Set pool attributes
```bash
$ sbcli pool set -h
usage: sbcli pool set [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX] [--max-rw-iops MAX_RW_IOPS] [--max-rw-mbytes MAX_RW_MBYTES] [--max-r-mbytes MAX_R_MBYTES]
                          [--max-w-mbytes MAX_W_MBYTES]
                          id

Set pool attributes

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
$ sbcli pool list -h
usage: sbcli pool list [-h] [--json] [--cluster-id]

List pools

optional arguments:
  -h, --help    show this help message and exit
  --json        Print outputs in json format
  --cluster-id  ID of the cluster

```

### Get pool details
```bash
$ sbcli pool get -h
usage: sbcli pool get [-h] [--json] id

get pool details

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

### Delete pool
```bash
$ sbcli pool delete -h
usage: sbcli pool delete [-h] id

delete pool. It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).

positional arguments:
  id          pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

### Set pool status to Active
```bash
$ sbcli pool enable -h
usage: sbcli pool enable [-h] pool_id

Set pool status to Active

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

### Set pool status to Inactive
```bash
$ sbcli pool disable -h
usage: sbcli pool disable [-h] pool_id

Set pool status to Inactive. Attention! This will suspend all new IO to the pool! IO in flight processing will be completed.

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```

### Get pool secret
```bash
$ sbcli pool get-secret -h
usage: sbcli pool get-secret [-h] pool_id

Returns auto generated, 20 characters secret.

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

```


### Update pool secret
```bash
$ sbcli pool upd-secret -h
usage: sbcli pool upd-secret [-h] pool_id secret

Updates the secret (replaces the existing one with a new one) and returns the new one.

positional arguments:
  pool_id     pool uuid
  secret      new 20 characters password

optional arguments:
  -h, --help  show this help message and exit

```

### Get Pool capacity
```bash
$ sbcli pool get-capacity -h
usage: sbcli pool get-capacity [-h] pool_id

Return provisioned, utilized (absolute) and utilized (percent) storage on the Pool.

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit
```

### Get IO Stats
```bash
$ sbcli pool get-io-stats -h
usage: sbcli pool get-io-stats [-h] [--history HISTORY] id

Returns either the current or historic io statistics (read-IO, write-IO, total-IO, read mbs, write mbs, total mbs).

positional arguments:
  id                 Pool id

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).

```


## Snapshot commands
```bash
$ sbcli snapshot -h
usage: sbcli snapshot [-h] {add,list,delete,clone} ...

Snapshot commands

positional arguments:
  {add,list,delete,clone}
    add                 Create new snapshot
    list                List snapshots
    delete              Delete a snapshot
    clone               Create LVol from snapshot

optional arguments:
  -h, --help            show this help message and exit

```

### Create snapshot
```bash
$ sbcli snapshot add -h
usage: sbcli snapshot add [-h] id name

Create new snapshot

positional arguments:
  id          LVol UUID
  name        snapshot name

optional arguments:
  -h, --help  show this help message and exit

```

### List snapshots
```bash
$ sbcli snapshot list -h
usage: sbcli snapshot list [-h]

List snapshots

optional arguments:
  -h, --help  show this help message and exit

```


### Delete snapshots
```bash
$ sbcli snapshot delete -h
usage: sbcli snapshot delete [-h] id

Delete a snapshot

positional arguments:
  id          snapshot UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Clone snapshots
```bash
$ sbcli snapshot clone -h
usage: sbcli snapshot clone [-h] id lvol_name

Create LVol from snapshot

positional arguments:
  id          snapshot UUID
  lvol_name   LVol name

optional arguments:
  -h, --help  show this help message and exit

```

## Caching client node commands
```bash
$ sbcli caching-node -h
usage: sbcli caching-node [-h] {deploy,add-node,list,list-lvols,remove,connect,disconnect,recreate} ...

Caching client node commands

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

### Deploy Caching node
```bash
$ sbcli caching-node deploy -h
usage: sbcli caching-node deploy [-h] [--ifname IFNAME]

Deploy caching node on this machine (local exec)

optional arguments:
  -h, --help       show this help message and exit
  --ifname IFNAME  Management interface name, default: eth0

```

### Add Caching Node to the Cluster
```bash
$ sbcli caching-node add-node -h
usage: sbcli caching-node add-node [-h] [--cpu-mask SPDK_CPU_MASK] [--memory SPDK_MEM] [--spdk-image SPDK_IMAGE] [--namespace NAMESPACE] cluster_id node_ip ifname

Add new Caching node to the cluster

positional arguments:
  cluster_id            UUID of the cluster to which the node will belong
  node_ip               IP of caching node to add
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --cpu-mask SPDK_CPU_MASK
                        SPDK app CPU mask, default is all cores found
  --memory SPDK_MEM     SPDK huge memory allocation, default is Max hugepages available
  --spdk-image SPDK_IMAGE
                        SPDK image uri
  --namespace NAMESPACE
                        k8s namespace to deploy on

```

### List Caching Nodes
```bash
$ sbcli caching-node list -h
usage: sbcli caching-node list [-h]

List Caching nodes

optional arguments:
  -h, --help  show this help message and exit

```

### List Connected LVols
```bash
$ sbcli caching-node list-lvols -h
usage: sbcli caching-node list-lvols [-h] id

List connected lvols

positional arguments:
  id          Caching Node UUID

optionathick                arguments:
  -h, --help  show this help message and exit

```

### Remove Caching Node from the Cluster
```bash
$ sbcli caching-node remove -h
usage: sbcli caching-node remove [-h] [--force] id

Remove Caching node from the cluster

positional arguments:
  id          Caching Node UUID

optional arguments:
  -h, --help  show this help message and exit
  --force     Force remove

```
### Connect to LVol
```bash
$ sbcli caching-node connect -h
usage: sbcli caching-node connect [-h] node_id lvol_id

Connect to LVol

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Disconnect LVol from Caching Node
```bash
$ sbcli caching-node disconnect -h
usage: sbcli caching-node disconnect [-h] node_id lvol_id

Disconnect LVol from Caching node

positional arguments:
  node_id     Caching node UUID
  lvol_id     LVol UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Recreate Caching Node BDevs
```bash
$ sbcli caching-node recreate -h
usage: sbcli caching-node recreate [-h] node_id

recreate Caching node bdevs

positional arguments:
  node_id     Caching node UUID

optional arguments:
  -h, --help  show this help message and exit

```
