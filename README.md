
# Simply Block
[![Docker Image CI](https://github.com/simplyblock-io/ultra/actions/workflows/docker-image.yml/badge.svg?branch=main)](https://github.com/simplyblock-io/ultra/actions/workflows/docker-image.yml)

## Web Api app
Please see this document 
[WebApp/README.md](../WebApp/README.md)


 
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
$ sbcli cluster  -h
usage: sbcli cluster [-h] {init,status,suspend,unsuspend,add-dev-model,rm-dev-model,add-host-auth,rm-host-auth,get-capacity,get-io-stats,set-log-level,get-event-log} ...

Cluster commands

positional arguments:
  {init,status,suspend,unsuspend,add-dev-model,rm-dev-model,add-host-auth,rm-host-auth,get-capacity,get-io-stats,set-log-level,get-event-log}
    init                Create an empty cluster
    status              Show cluster status
    suspend             Suspend cluster. The cluster will stop processing all IO. Attention! This will cause an "all paths down" event for nvmeof/iscsi volumes on all hosts connected to
                        the cluster.
    unsuspend           Unsuspend cluster. The cluster will start processing IO again.
    add-dev-model       Add a device to the white list by the device model id. When adding nodes to the cluster later on, all devices of the specified model-ids, which are present on the
                        node to be added to the cluster, are added to the storage node for the cluster. This does not apply for already added devices, but only affect devices on additional
                        nodes, which will be added to the cluster. It is always possible to also add devices present on a server to a node manually.
    rm-dev-model        Remove device from the white list by the device model id. This does not apply for already added devices, but only affect devices on additional nodes, which will be
                        added to the cluster.
    add-host-auth       If the "authorized hosts only" security feature is turned on, hosts must be explicitly added to the cluster via their nqn before they can discover the subsystem
                        initiate a connection.
    rm-host-auth        If the "authorized hosts only" security feature is turned on, this function removes hosts, which have been added to the cluster via their nqn, from the list of
                        authorized hosts. After a host has been removed, it cannot connect any longer to the subsystem and cluster.
    get-capacity        Returns the current total available capacity, utilized capacity (in percent and absolute) and provisioned capacity (in percent and absolute) in GB in the cluster.
    get-io-stats        Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops
                        write-iops total-iops read-mbs write-mbs total-mbs
    set-log-level       Defines the detail of the log information collected and stored
    get-event-log       returns cluster event log in syslog format

optional arguments:
  -h, --help            show this help message and exit

```

### Initializing new cluster
```bash
$ sbcli cluster init -h
usage: sbcli cluster init [-h] --blk_size {512,4096} --page_size_in_blocks PAGE_SIZE_IN_BLOCKS --model_ids MODEL_IDS [MODEL_IDS ...] --ha_type {single,ha} --tls {on,off} --auth-hosts-only
                          {on,off} --dhchap {off,one-way,bi-direct} [--NQN NQN] [--iSCSI]

Create an empty cluster

optional arguments:
  -h, --help            show this help message and exit
  --blk_size {512,4096}
                        The block size in bytes
  --page_size_in_blocks PAGE_SIZE_IN_BLOCKS
                        The size of a data page in logical blocks
  --model_ids MODEL_IDS [MODEL_IDS ...]
                        a list of supported NVMe device model-ids
  --ha_type {single,ha}
                        Can be "single" for single node clusters or "HA", which requires at least 3 nodes
  --tls {on,off}        TCP/IP transport security can be turned on and off. If turned on, both hosts and storage nodes must authenticate the connection via TLS certificates
  --auth-hosts-only {on,off}
                        if turned on, hosts must be explicitely added to the cluster to be able to connect to any NVMEoF subsystem in the cluster
  --dhchap {off,one-way,bi-direct}
                        if set to "one-way", hosts must present a secret whenever a connection is initiated between a host and a logical volume on a NVMEoF subsystem in the cluster. If set
                        to "bi-directional", both host and subsystem must present a secret to authenticate each other
  --NQN NQN             cluster NQN subsystem
  --iSCSI               The cluster supports iscsi LUNs in addition to nvmeof volumes

```

### Show cluster status
```bash
$ sbcli cluster status -h
usage: sbcli cluster status [-h] cluster-id

Show cluster status

positional arguments:
  cluster-id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Suspend cluster 
```bash
$ sbcli cluster suspend -h
usage: sbcli cluster suspend [-h] cluster-id

Suspend cluster. The cluster will stop processing all IO. 
Attention! This will cause an "all paths down" event for nvmeof/iscsi volumes on all hosts connected to the cluster.

positional arguments:
  cluster-id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit

```

### Unsuspend cluster 
```bash
$ sbcli cluster unsuspend -h
usage: sbcli cluster unsuspend [-h] cluster-id

Unsuspend cluster. The cluster will start processing IO again.

positional arguments:
  cluster-id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit
```


### Add device model to the NVMe devices whitelist
```bash
$ sbcli cluster add-dev-model -h
usage: sbcli cluster add-dev-model [-h] cluster-id model-ids [model-ids ...]

Add a device to the white list by the device model id. When adding nodes to the cluster later on, all devices of the specified model-ids, which are present on the node to be added to the
cluster, are added to the storage node for the cluster. This does not apply for already added devices, but only affect devices on additional nodes, which will be added to the cluster. It
is always possible to also add devices present on a server to a node manually.

positional arguments:
  cluster-id  the cluster UUID
  model-ids   a list of supported NVMe device model-ids

optional arguments:
  -h, --help  show this help message and exit
```

### Remove device model from the NVMe devices whitelist
```bash
$ sbcli cluster rm-dev-model -h
usage: sbcli cluster rm-dev-model [-h] cluster-id model-ids [model-ids ...]

Remove device from the white list by the device model id. This does not apply for already added devices, but only affect devices on additional nodes, which will be added to the cluster.

positional arguments:
  cluster-id  the cluster UUID
  model-ids   a list of NVMe device model-ids

optional arguments:
  -h, --help  show this help message and exit
```

### Add host auth
```bash
$ sbcli cluster add-host-auth -h
usage: sbcli cluster add-host-auth [-h] cluster-id host-nqn

If the "authorized hosts only" security feature is turned on, hosts must be explicitly added to the cluster via their nqn before they can discover the subsystem initiate a connection.

positional arguments:
  cluster-id  the cluster UUID
  host-nqn    NQN of the host to allow to discover and connect to teh cluster

optional arguments:
  -h, --help  show this help message and exit
```

### Remove host auth
```bash
$ sbcli cluster rm-host-auth -h
usage: sbcli cluster rm-host-auth [-h] cluster-id host-nqn

If the "authorized hosts only" security feature is turned on, this function removes hosts, which have been added to the cluster via their nqn, from the list
of authorized hosts. After a host has been removed, it cannot connect any longer to the subsystem and cluster.

positional arguments:
  cluster-id  the cluster UUID
  host-nqn    NQN of the host to remove from the allowed hosts list

optional arguments:
  -h, --help  show this help message and exit

```

### Get total cluster capacity
```bash
$ sbcli cluster get-capacity -h
usage: sbcli cluster get-capacity [-h] [--history HISTORY] cluster-id

Returns the current total available capacity, utilized capacity (in percent and absolute) and provisioned capacity (in percent and absolute) in GB in the cluster.

positional arguments:
  cluster-id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).

```

### Return io statistics of a cluster
```bash
$ sbcli cluster get-io-stats -h
usage: sbcli cluster get-io-stats [-h] [--history HISTORY] cluster-id

Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops write-iops total-iops
read-mbs write-mbs total-mbs

positional arguments:
  cluster-id         the cluster UUID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  (XXdYYh), list history records (one for every 15 minutes) for XX days and YY hours (up to 10 days in total).
```

### Set log level
```bash
$ sbcli cluster set-log-level -h
usage: sbcli cluster set-log-level [-h] cluster-id {debug,test,prod}

Defines the detail of the log information collected and stored

positional arguments:
  cluster-id         the cluster UUID
  {debug,test,prod}  Log level

optional arguments:
  -h, --help         show this help message and exit

```

### Get events log
```bash
$ sbcli cluster get-event-log -h
usage: sbcli cluster get-event-log [-h] [--from FROM] [--to TO] cluster-id

returns cluster event log in syslog format

positional arguments:
  cluster-id   the cluster UUID

optional arguments:
  -h, --help   show this help message and exit
  --from FROM  from time, format: dd-mm-yy hh:mm
  --to TO      to time, format: dd-mm-yy hh:mm

```


## Storage node commands
```bash
$ sbcli storage-node -h
usage: sbcli storage-node [-h]
                          {add,remove,list,restart,shutdown,suspend,resume,get-io-stats,list-devices,reset-device,run-smart,add-device,replace,remove-device,set-ro-device,set-failed-device,set-online-device,get-capacity-device,get-io-stats-device,get-event-log,get-log-page-device}
                          ...

Storage node commands

positional arguments:
  {add,remove,list,restart,shutdown,suspend,resume,get-io-stats,list-devices,reset-device,run-smart,add-device,replace,remove-device,set-ro-device,set-failed-device,set-online-device,get-capacity-device,get-io-stats-device,get-event-log,get-log-page-device}
    add                 Add storage node
    remove              Remove storage node
    list                List storage nodes
    restart             Restart a storage node. All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not
                        impact operations.
    shutdown            Shutdown a storage node. Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-
                        availability setup, this will not impact operations.
    suspend             Suspend a storage node. The node will stop accepting new IO, but will finish processing any IO, which has been received already.
    resume              Resume a storage node
    get-io-stats        Returns the current io statistics of a node
    list-devices        List storage devices
    reset-device        Reset storage device
    run-smart           Run tests against storage device
    add-device          Add a new storage device
    replace             Replace a storage node. This command is run on the new physical server, which is expected to replace the old server. Attention!!! All the nvme devices, which are
                        part of the cluster to which the node belongs, must be inserted into the new server before this command is run. The old node will be de-commissioned and cannot be
                        used any more.
    remove-device       Remove a storage device. The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection
                        of removal did not work or if the device must be maintained otherwise while remaining inserted into the server.
    set-ro-device       Set storage device read only
    set-failed-device   Set storage device to failed state. This command can be used, if an administrator believes that the device must be changed, but its status and health state do not
                        lead to an automatic detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices
                        in the cluster.
    set-online-device   Set storage device to online state
    get-capacity-device
                        Returns the size, absolute and relative utilization of the device in bytes
    get-io-stats-device
                        Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops
                        write-iops total-iops read-mbs write-mbs total-mbs
    get-event-log       Returns storage node event log in syslog format. This includes events from the storage node itself, the network interfaces and all devices on the node, including
                        health status information and updates.
    get-log-page-device
                        Get nvme log-page information from the device. Attention! The availability of particular log pages depends on the device model. For more information, see nvme
                        specification.

optional arguments:
  -h, --help            show this help message and exit

```

### Add new storage node
- must be run on the storage node itself
```bash
$ sbcli storage-node add -h
usage: sbcli storage-node add [-h] [--data-nics DATA_NICS [DATA_NICS ...]] [--distr] cluster-id ifname

Add storage node

positional arguments:
  cluster-id            UUID of the cluster to which the node will belong
  ifname                Management interface name

optional arguments:
  -h, --help            show this help message and exit
  --data-nics DATA_NICS [DATA_NICS ...]
                        Data interface names
  --distr               Install Disturb spdk app instead of default: ultra21
```

### Remove storage node
```bash
$ sbcli storage-node remove -h
usage: sbcli storage-node remove [-h] node-id

Remove storage node

positional arguments:
  node-id     node-id of storage node

optional arguments:
  -h, --help  show this help message and exit

```

### List storage nodes
```bash
$ sbcli storage-node list -h
usage: sbcli storage-node list [-h] [--json] cluster-id

List storage nodes

positional arguments:
  cluster-id  id of the cluster for which nodes are listed

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format

```

### Restart storage node
- must be run on the storage node itself
```bash
$ sbcli storage-node restart -h
usage: sbcli storage-node restart [-h] [-t] cluster-id

Restart a storage node. All functions and device drivers will be reset. During restart, the node does not accept IO. In a high-availability setup, this will not impact operations.

positional arguments:
  cluster-id  the cluster UUID to which the node belongs

optional arguments:
  -h, --help  show this help message and exit
  -t, --test  Run smart test on the NVMe devices

```

### Shutdown a storage node
```bash
$ sbcli storage-node shutdown -h
usage: sbcli storage-node shutdown [-h] cluster-id

Shutdown a storage node. Once the command is issued, the node will stop accepting IO,but IO, which was previously received, will still be processed. In a high-availability setup, this will
not impact operations.

positional arguments:
  cluster-id  the cluster UUID to which the node belongs

optional arguments:
  -h, --help  show this help message and exit

```

### Suspend a storage node
```bash
$ sbcli storage-node suspend -h
usage: sbcli storage-node suspend [-h] cluster-id

Suspend a storage node. The node will stop accepting new IO, but will finish processing any IO, which has been received already.

positional arguments:
  cluster-id  the cluster UUID to which the node belongs

optional arguments:
  -h, --help  show this help message and exit

```

### Resume a storage node
```bash
$ sbcli storage-node resume -h
usage: sbcli storage-node resume [-h] cluster-id

Resume a storage node

positional arguments:
  cluster-id  the cluster UUID to which the node belongs

optional arguments:
  -h, --help  show this help message and exit
```

### Returns the current io statistics of a node
```bash
$ sbcli storage-node get-io-stats -h
usage: sbcli storage-node get-io-stats [-h] cluster-id

Returns the current io statistics of a node

positional arguments:
  cluster-id  the cluster UUID

optional arguments:
  -h, --help  show this help message and exit
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

### Reset a storage device
```bash
$ sbcli storage-node reset-device -h
usage: sbcli storage-node reset-device [-h] device-id

Reset storage device

positional arguments:
  device-id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Run smart tests against a storage device
```bash
$ sbcli storage-node run-smart -h
usage: sbcli storage-node run-smart [-h] device-id

Run tests against storage device

positional arguments:
  device-id   the devices's UUID

optional arguments:
  -h, --help  show this help message and exit
```

### Add a new storage device
```bash
$ sbcli storage-node add-device -h
usage: sbcli storage-node add-device [-h] name

Add a new storage device

positional arguments:
  name        Storage device name (as listed in the operating system). The device will be de-ttached from the operating system and attached to the storage node

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
usage: sbcli storage-node remove-device [-h] device-id

Remove a storage device. The device will become unavailable, independently if it was physically removed from the server. This function can be used if auto-detection of removal did not work
or if the device must be maintained otherwise while remaining inserted into the server.

positional arguments:
  device-id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
```

### Set storage device to failed state
```bash
$ sbcli storage-node set-failed-device -h
usage: sbcli storage-node set-failed-device [-h] device-id

Set storage device to failed state. This command can be used, if an administrator believes that the device must be changed, but its status and health state do not lead to an automatic
detection of the failure state. Attention!!! The failed state is final, all data on the device will be automatically recovered to other devices in the cluster.

positional arguments:
  device-id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit
```

### Set storage device to online state
```bash
$ sbcli storage-node set-online-device -h
usage: sbcli storage-node set-online-device [-h] device-id

Set storage device to online state

positional arguments:
  device-id   Storage device ID

optional arguments:
  -h, --help  show this help message and exit

```

### Returns the size of a device
```bash
$ sbcli storage-node get-capacity-device -h
usage: sbcli storage-node get-capacity-device [-h] [--history HISTORY] device-id

Returns the size, absolute and relative utilization of the device in bytes

positional arguments:
  device-id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh
```


### Returns the io statistics of a device
```bash
$ sbcli storage-node get-io-stats-device -h
usage: sbcli storage-node get-io-stats-device [-h] [--history HISTORY] device-id

Returns the io statistics. If --history is not selected, this is a monitor, which updates current statistics records every two seconds (similar to ping):read-iops write-iops total-iops
read-mbs write-mbs total-mbs

positional arguments:
  device-id          Storage device ID

optional arguments:
  -h, --help         show this help message and exit
  --history HISTORY  list history records -one for every 15 minutes- for XX days and YY hours -up to 10 days in total-, format: XXdYYh

```

### Returns storage node event log in syslog format
```bash
$ sbcli storage-node get-event-log -h
usage: sbcli storage-node get-event-log [-h] [--from FROM] [--to TO] node-id

Returns storage node event log in syslog format. This includes events from the storage node itself, the network interfaces and all devices on the node, including health status information
and updates.

positional arguments:
  node-id      Storage node ID

optional arguments:
  -h, --help   show this help message and exit
  --from FROM  from time, format: dd-mm-yy hh:mm
  --to TO      to time, format: dd-mm-yy hh:mm
```

### Get nvme log-page information from the device
```bash
$ sbcli storage-node get-log-page-device -h
usage: sbcli storage-node get-log-page-device [-h] device-id {error,smart,telemetry,dev-self-test,endurance,persistent-event}

Get nvme log-page information from the device. Attention! The availability of particular log pages depends on the device model. For more information, see nvme specification.

positional arguments:
  device-id             Storage device ID
  {error,smart,telemetry,dev-self-test,endurance,persistent-event}
                        Can be [error , smart , telemetry , dev-self-test , endurance , persistent-event]

optional arguments:
  -h, --help            show this help message and exit
```


## LVol commands
```bash
$ sbcli lvol -h
usage: sbcli lvol [-h]
                  {add,qos-set,list,get,delete,connect,resize,set-read-only,create-snapshot,clone,get-host-secret,get-ctrl-secret,move ,replicate
                  ,inflate,get-capacity,get-io-stats} ...

LVol commands

positional arguments:
  {add,qos-set,list,get,delete,connect,resize,set-read-only,create-snapshot,clone,get-host-secret,get-ctrl-secret,move ,replicate ,inflate,get-capacity,get-io-stats}
    add                 Add LVol, if both --comp and --crypto are used, then the compress bdev will be at the top
    qos-set             Change qos settings for an active logical volume
    list                List all LVols
    get                 Get LVol details
    delete              Delete LVol
    connect             show connection string to LVol host
    resize              Resize LVol
    set-read-only       Set LVol Read-only
    create-snapshot     Create snapshot from LVol
    clone               create LVol based on a snapshot
    get-host-secret     Returns the auto-generated host secret required for the nvmeof connection between host and cluster
    get-ctrl-secret     Returns the auto-generated controller secret required for the nvmeof connection between host and cluster
    move                Moves a full copy of the logical volume between clusters
    replicate           Create a replication path between two volumes in two clusters
    inflate             Inflate a clone to "full" logical volume and disconnect it from its parent snapshot.
    get-capacity        Returns the current provisioned capacity
    get-io-stats        Returns either the current io statistics

optional arguments:
  -h, --help            show this help message and exit

```

### Add LVol
```bash
$ sbcli lvol add -h
usage:sbcli lvol add [-h] [--compress] [--encrypt] [--thick] [--iscsi] [--node-ha {0,1,2}] [--dev-redundancy] [--max-w-iops MAX_W_IOPS] [--max-r-iops MAX_R_IOPS] [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES]
                     [--distr] [--distr-ndcs DISTR_NDCS] [--distr-npcs DISTR_NPCS] [--distr-alloc_names DISTR_ALLOC_NAMES]
                     name size pool hostname

Add LVol, if both --comp and --crypto are used, then the compress bdev will be at the top

positional arguments:
  name                  LVol name or id
  size                  LVol size: 10M, 10G, 10(bytes)
  pool                  Pool UUID or name
  hostname              Storage node hostname

optional arguments:
  -h, --help            show this help message and exit
  --compress            Use inline data compression and de-compression on the logical volume
  --encrypt             Use inline data encryption and de-cryption on the logical volume
  --thick               Deactivate thin provisioning
  --iscsi               Use ISCSI fabric type instead of NVMEoF; in this case, it is required to specify the cluster hosts to which the volume will be attached
  --node-ha {0,1,2}     The maximum amount of concurrent node failures accepted without interruption of operations
  --dev-redundancy      {1,2} supported minimal concurrent device failures without data loss
  --max-w-iops MAX_W_IOPS
                        Maximum Write IO Per Second
  --max-r-iops MAX_R_IOPS
                        Maximum Read IO Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second
  --distr               Use Disturb bdev

```

### Set QOS options for LVol
```bash
$ sbcli lvol qos-set [-h]
usage: sbcli lvol qos-set [-h] [--max-w-iops MAX_W_IOPS] [--max-r-iops MAX_R_IOPS] [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES] id

Change qos settings for an active logical volume

positional arguments:
  id                    LVol id

optional arguments:
  -h, --help            show this help message and exit
  --max-w-iops MAX_W_IOPS
                        Maximum Write IO Per Second
  --max-r-iops MAX_R_IOPS
                        Maximum Read IO Per Second
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

### Get LVol details
```bash
$ sbcli lvol get -h
usage: sbcli lvol get [-h] [--json] id

Get LVol details

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
  --json      Print outputs in json format
```

### Delete LVol
```bash
$ sbcli lvol delete -h
usage: sbcli lvol delete [-h] [--force] id

Delete LVol. This is only possible, if no more snapshots and non-inflated clones of the volume exist. The volume must be suspended before it can be deleted.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
  --force     Force delete LVol from the cluster
```

### Show nvme-cli connection commands
```bash
$ sbcli lvol connect -h
usage: sbcli lvol lvol connect [-h] id

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

Suspend LVol. IO in flight will still be processed, but new IO is not accepted. Make sure that the volume is detached from all hosts before suspending it to avoid IO errors.

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

Create LVol based on a snapshot

positional arguments:
  snapshot_id          snapshot UUID
  clone_name        clone name

optional arguments:
  -h, --help  show this help message and exit
```

### Returns the host secret
```bash
$ sbcli lvol get-host-secret -h
usage: sbcli lvol get-host-secret [-h] id

Returns the auto-generated host secret required for the nvmeof connection between host and cluster

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

### Returns the controller secret
```bash
$ sbcli lvol get-ctrl-secret -h
usage: sbcli lvol get-ctrl-secret [-h] id

Returns the auto-generated controller secret required for the nvmeof connection between host and cluster

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit

```

### Moves a full copy of the logical volume between clusters
```bash
$ sbcli lvol move -h
usage: sbcli lvol move [-h] id cluster-id node-id

Moves a full copy of the logical volume between clusters

positional arguments:
  id          LVol id
  cluster-id  Destination Cluster ID
  node-id     Destination Node ID

optional arguments:
  -h, --help  show this help message and exit


```

### Create a replication path between two volumes in two clusters
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

### Inflate a clone to "full" logical volume and disconnect it from its parent snapshot.
```bash
$ sbcli lvol inflate -h
usage: sbcli lvol inflate [-h] id

Inflate a clone to "full" logical volume and disconnect it from its parent snapshot.

positional arguments:
  id          LVol id

optional arguments:
  -h, --help  show this help message and exit
```

### Returns the current LVol provisioned capacity
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

### Returns either the current io statistics
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
usage: sbcli mgmt add [-h] ip_port

Add Management node to the cluster

positional arguments:
  ip_port     Docker server IP:PORT

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
usage: sbcli pool [-h] {add,set,list,get,delete,enable,disable,get-secret,set-secret} ...

Pool commands

positional arguments:
  {add,set,list,get,delete,enable,disable,get-secret,set-secret}
    add                 Add a new Pool
    set                 Set pool attributes
    list                List pools
    get                 get pool details
    delete              delete pool. It is only possible to delete a pool if it is empty (no provisioned logical volumes contained).
    enable              Set pool status to Active
    disable             Set pool status to Inactive. Attention! This will suspend all new IO to the pool! IO in flight processing will be completed.
    get-secret          Returns auto generated, 20 characters secret.
    set-secret          Updates the secret (replaces the existing one with a new one) and returns the new one.

optional arguments:
  -h, --help            show this help message and exit

```

### Add pool
 - QOS parameters are optional but when used in a pool, new Lvol
   creation will require the active qos parameter. 
 - User can use both QOS parameters (--max-rw-iops, --max-rw-mbytes) 
   and will apply which limit comes first.
```bash
$ sbcli pool add -h
usage: sbcli pool add [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX] [--max-w-iops MAX_W_IOPS] [--max-r-iops MAX_R_IOPS] [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES]
                      [--has-secret]
                      name

Add a new Pool

positional arguments:
  name                  Pool name

optional arguments:
  -h, --help            show this help message and exit
  --pool-max POOL_MAX   Pool maximum size: 20M, 20G, 0(default)
  --lvol-max LVOL_MAX   LVol maximum size: 20M, 20G, 0(default)
  --max-w-iops MAX_W_IOPS
                        Maximum Write IO Per Second
  --max-r-iops MAX_R_IOPS
                        Maximum Read IO Per Second
  --max-r-mbytes MAX_R_MBYTES
                        Maximum Read Mega Bytes Per Second
  --max-w-mbytes MAX_W_MBYTES
                        Maximum Write Mega Bytes Per Second
  --has-secret          Pool is created with a secret (all further API interactions with the pool and logical volumes in the pool require this secret)

```

### Set pool attributes
```bash
$ sbcli pool set -h
usage: sbcli pool set [-h] [--pool-max POOL_MAX] [--lvol-max LVOL_MAX] [--max-w-iops MAX_W_IOPS] [--max-r-iops MAX_R_IOPS] [--max-r-mbytes MAX_R_MBYTES] [--max-w-mbytes MAX_W_MBYTES] id

Set pool attributes

positional arguments:
  id                    Pool UUID

optional arguments:
  -h, --help            show this help message and exit
  --pool-max POOL_MAX   Pool maximum size: 20M, 20G
  --lvol-max LVOL_MAX   LVol maximum size: 20M, 20G
  --max-w-iops MAX_W_IOPS
                        Maximum Write IO Per Second
  --max-r-iops MAX_R_IOPS
                        Maximum Read IO Per Second
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
$ sbcli pool set-secret -h
usage: sbcli pool set-secret [-h] pool_id

Updates the secret (replaces the existing one with a new one) and returns the new one.

positional arguments:
  pool_id     pool uuid

optional arguments:
  -h, --help  show this help message and exit

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