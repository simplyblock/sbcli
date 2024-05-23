# SimplyBlock API

Web app that provides API to manage the cluster

## Authentication

All requests must include Authorization header:
```
Authorization: {cluster_id} {cluster_secret}
```

Cluster secret can be obtained from sbcli

```
sbcli cluster get-secret CLUSTER_UUID
```


## Postman collection
https://red-firefly-736457.postman.co/workspace/SimplyBlock~91867c59-4e78-4ca3-97cc-af3fdb72f0a8/collection/434798-4f0ccde4-75b2-4bf2-a224-9ea62d8eb18c?action=share&creator=434798


## Usage

## Storage nodes endpoint (/storagenode)

### List Storage nodes

**GET** http://<CLUSTER_IP>/storagenode

Request

```curl
curl -X GET http://127.0.0.1/storagenode
```

Response

```json
{
  "error": "",
  "results": [
    {
      "Data NICs": "eth1",
      "Hostname": "ip-172-31-79-184",
      "LVOLs": "",
      "Management IP": "172.31.79.184",
      "NVMe Devices": "nvme_ultra_0",
      "Status": "offline",
      "Subsystem": "nqn.2023-02.io.simlpyblock:subsystem1",
      "Updated At": "17:23:28, 26/05/2023"
    }
  ],
  "status": true
}
```

### Get Storage node details

**GET** http://<CLUSTER_IP>/storagenode/<NODE_HOSTNAME/NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/ip-172-31-79-184
```

Response

```json
{
  "error": "",
  "results": [
    {
      "Data NICs": "eth1",
      "Hostname": "ip-172-31-79-184",
      "LVOLs": "",
      "Management IP": "172.31.79.184",
      "NVMe Devices": "nvme_ultra_0",
      "Status": "offline",
      "Subsystem": "nqn.2023-02.io.simlpyblock:subsystem1",
      "Updated At": "17:24:14, 26/05/2023"
    }
  ],
  "status": true
}
```

### Get Storage node Capacity

**GET** http://<CLUSTER_IP>/storagenode/capacity/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/capacity/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node io stats

**GET** http://<CLUSTER_IP>/storagenode/iostats/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/iostats/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node network ports

**GET** http://<CLUSTER_IP>/storagenode/port/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/port/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node network port statistics

**GET** http://<CLUSTER_IP>/storagenode/port-io-stats/<PORT_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/port-io-stats/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node suspend

**GET** http://<CLUSTER_IP>/storagenode/suspend/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/suspend/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node resume

**GET** http://<CLUSTER_IP>/storagenode/resume/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/resume/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node shutdown

**GET** http://<CLUSTER_IP>/storagenode/shutdown/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/shutdown/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get Storage node restart

**GET** http://<CLUSTER_IP>/storagenode/restart/<NODE_ID>

Request

```curl
curl -X GET http://127.0.0.1/storagenode/restart/31efa55b-d41f-478c-9baf-34a530242d8d
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Add Storage node

**POST** http://<CLUSTER_IP>/storagenode/add

Parameters

| Parameter             | Description                                  |
|-----------------------|----------------------------------------------|
| cluster_id (required) | Cluster UUID                                 |
| node_ip (required)    | IP of storage node to add                    |
| ifname (required)     | Management interface name                    |
| spdk_image            | SPDK image uri                               |
| spdk_debug            | Enable spdk debug logs                       |
| data_nics             | Data interface names                         |
| spdk_cpu_mask         | Maximum Write Mega Bytes Per Second          |
| spdk_mem              | SPDK huge memory allocation 4G(default)      |

Request

```curl
curl -X POST http://127.0.0.1/storagenode/add \
      -d '{"cluster_id":"cluster_id", "node_ip":"node_ip", "ifname":"eth0"}'
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

## Pool endpoint (/pool)

### List pools

**GET** http://<CLUSTER_IP>/pool

Request

```curl
curl -X GET http://127.0.0.1/pool
```

Response

```json
{
  "error": "",
  "results": [
    {
      "groups": [],
      "id": "2a991495-6008-49d5-921a-0de1a40667f4",
      "iops_max": -1,
      "lvol_max_size": -1,
      "lvols": [],
      "name": "Pool",
      "object_type": "object",
      "pool_max_size": -1,
      "pool_name": "pool2",
      "status": "active",
      "users": []
    },
    {
      "groups": [],
      "id": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "iops_max": -1,
      "lvol_max_size": -1,
      "lvols": [],
      "name": "Pool",
      "object_type": "object",
      "pool_max_size": -1,
      "pool_name": "pool1",
      "status": "active",
      "users": []
    }
  ],
  "status": true
}
```

### Get pool details

**GET** http://<CLUSTER_IP>/pool/<POOL_UUID>

Request

```curl
curl -X GET http://127.0.0.1/pool/9c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": [
    {
      "groups": [],
      "id": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "iops_max": -1,
      "lvol_max_size": -1,
      "lvols": [],
      "name": "Pool",
      "object_type": "object",
      "pool_max_size": -1,
      "pool_name": "pool1",
      "status": "active",
      "users": []
    }
  ],
  "status": true
}
```

### Create pool

**POST** http://<CLUSTER_IP>/pool/

Parameters

| Parameter       | Description                                  |
|-----------------|----------------------------------------------|
| name (required) | Pool name                                    |
| pool-max        | Pool maximum size: 20M, 20G, 0(default)      |
| lvol-max        | LVol maximum size: 20M, 20G, 0(default)      |
| max-rw-iops     | Maximum Read Write IO Per Second             |
| max-rw-mbytes   | Maximum Read Write Mega Bytes Per Second     |
| max-r-mbytes    | Maximum Read Mega Bytes Per Second           |
| max-w-mbytes    | Maximum Write Mega Bytes Per Second          |
| no_secret       | if True then pool will have no secret string |

Request

```curl
curl -X POST http://127.0.0.1/pool/
    -d '{"name":"pool1"}'
```

Response

```json
{
  "error": "",
  "results": {
    "groups": [],
    "id": "9c589be6-a7f2-4472-a02f-7613419195ef",
    "iops_max": -1,
    "lvol_max_size": -1,
    "lvols": [],
    "name": "Pool",
    "object_type": "object",
    "pool_max_size": -1,
    "pool_name": "pool1",
    "status": "active",
    "users": []
  },
  "status": true
}
```

### Update pool

**PUT** http://<CLUSTER_IP>/pool/UUID

Parameters

| Parameter     | Description                              |
|---------------|------------------------------------------|
| name          | Pool name                                |
| pool-max      | Pool maximum size: 20M, 20G, 0(default)  |
| lvol-max      | LVol maximum size: 20M, 20G, 0(default)  |
| max-rw-iops   | Maximum Read Write IO Per Second         |
| max-rw-mbytes | Maximum Read Write Mega Bytes Per Second |
| max-r-mbytes  | Maximum Read Mega Bytes Per Second       |
| max-w-mbytes  | Maximum Write Mega Bytes Per Second      |

Request

```curl
curl -X PUT http://127.0.0.1/pool/9c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"name":"pool2"}'
```

Response

```json
{
  "error": "",
  "results": {
    "groups": [],
    "id": "9c589be6-a7f2-4472-a02f-7613419195ef",
    "iops_max": -1,
    "lvol_max_size": -1,
    "lvols": [],
    "name": "Pool",
    "object_type": "object",
    "pool_max_size": -1,
    "pool_name": "pool2",
    "status": "active",
    "users": []
  },
  "status": true
}
```

### Delete pool

**DELETE** http://<CLUSTER_IP>/pool/UUID

Request

```curl
curl -X DELETE http://127.0.0.1/pool/9c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### Get pool capacity

**GET** http://<CLUSTER_IP>/pool/capacity/UUID

Request

```curl
curl -X GET http://127.0.0.1/pool/capacity/9c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```

### Get pool io stats

**GET** http://<CLUSTER_IP>/pool/iostats/UUID

Request

```curl
curl -X GET http://127.0.0.1/pool/iostats/9c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": [],
  "status": true
}
```


## Cluster endpoint (/cluster)

### Get cluster status

**GET** http://<CLUSTER_IP>/cluster/status

Request

```curl
curl -X GET http://127.0.0.1/cluster/status
```

Response

```json
{
  "error": "",
  "results": {
    "ActiveNodes": 3,
    "TotalNodes": 3,
    "Leader": "ip-172-31-75-48.ec2.internal",
    "Status": "active",
    "UpdatedAt": "16:39:25, 26/05/2023"
  },
  "status": true
}
```

### Get clusters and their details

**GET** http://<CLUSTER_IP>/cluster

Request

```curl
curl -X GET http://127.0.0.1/cluster
```

Response

```json
{
  "error": "",
  "results": {
  },
  "status": true
}
```

### Get cluster details

**GET** http://<CLUSTER_IP>/cluster/<CLUSTER_ID>

Request

```curl
curl -X GET http://127.0.0.1/cluster/10b8b609-7b28-4797-a3a1-0a64fed1fad2
```

Response

```json
{
  "error": "",
  "results": {
  },
  "status": true
}
```

### Suspend cluster

**PUT** http://<CLUSTER_IP>/cluster/suspend

Request

```curl
curl -X PUT http://127.0.0.1/cluster/suspend
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### Unsuspend cluster

**PUT** http://<CLUSTER_IP>/cluster/unsuspend

Request

```curl
curl -X PUT http://127.0.0.1/cluster/unsuspend
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```


## Management nodes endpoint (/mgmtnode)

### List management nodes

**GET** http://<CLUSTER_IP>/mgmtnode

Request

```curl
curl -X GET http://127.0.0.1/mgmtnode
```

Response

```json
{
  "error": "",
  "results": [
    {
      "Hostname": "ip-172-31-69-49",
      "IP": "172.31.69.49",
      "Status": "active",
      "UpdatedAt": "16:39:25, 26/05/2023"
    }
  ],
  "status": true
}
```

### Get Management node details

**GET** http://<CLUSTER_IP>/mgmtnode/<NODE_HOSTNAME/NODE_UUID>

Request

```curl
curl -X GET http://127.0.0.1/mgmtnode/ip-172-31-79-184
```

Response

```json
{
  "error": "",
  "results": [
    {
      "Hostname": "ip-172-31-69-49",
      "IP": "172.31.69.49",
      "Status": "active",
      "UpdatedAt": "16:39:25, 26/05/2023"
    }
  ],
  "status": true
}
```


## LVol endpoint (/lvol)

### List LVols

**GET** http://<CLUSTER_IP>/lvol

Request

```curl
curl -X GET http://127.0.0.1/lvol
```

Response

```json
{
  "error": "",
  "results": [
    {
      "id": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "name": "lvol1",
      "size": "10G",
      "pool": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "hostname": "ip-172-31-69-49",
      "type": "lvol",
      "status": "on"
    },
    {
      "id": "4c589be6-a7f2-4472-a02f-7613419195ef",
      "name": "lvol2",
      "size": "10G",
      "pool": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "hostname": "ip-172-31-69-49",
      "type": "comp",
      "status": "on"
    }
  ],
  "status": true
}
```

### Get lvol details

**GET** http://<CLUSTER_IP>/lvol/<UUID>

Request

```curl
curl -X GET http://127.0.0.1/lvol/9c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": [
    {
      "id": "4c589be6-a7f2-4472-a02f-7613419195ef",
      "name": "lvol2",
      "size": "10G",
      "pool": "9c589be6-a7f2-4472-a02f-7613419195ef",
      "hostname": "ip-172-31-69-49",
      "type": "comp",
      "status": "on"
    }
  ],
  "status": true
}
```

### Create LVol

**POST** http://<CLUSTER_IP>/lvol

- if both --comp and --crypto are used, then the compress bdev will be at the top

Parameters

| Parameter       | Description                                                                 |
|-----------------|-----------------------------------------------------------------------------|
| name (required) | LVol name or id                                                             |
| size (required) | LVol size: 10M, 10G, 10(bytes)                                              |
| pool (required) | Pool UUID or name                                                           |
| comp            | Create a new compress LVol                                                  |
| crypto          | Create a new crypto LVol                                                    |
| max_rw_iops     | Maximum Read Write IO Per Second                                            |
| max_rw_mbytes   | Maximum Read Write Mega Bytes Per Second                                    |
| max_r_mbytes    | Maximum Read Mega Bytes Per Second                                          |
| max_w_mbytes    | Maximum Write Mega Bytes Per Second                                         |
| ha_type         | LVol HA type, can be (single,ha,default=cluster's ha type), Default=default |
| distr_vuid      | Distr bdev virtual unique ID, Default=0 means random                        |
| distr_ndcs      | Distr bdev number of data chunks per stripe, Default=0 means auto set       |
| distr_npcs      | Distr bdev number of parity chunks per stripe, Default=0 means auto set     |
| distr_bs        | Distr bdev block size, Default=4096                                         |
| distr_chunk_bs  | Distr bdev chunk block size, Default=4096                                   |

Request

```curl
curl -X POST http://127.0.0.1/lvol
    -d '{"name":"lvol1", "size":"10G", "pool":"pool1"}'
```

Response

```json
{
  "error": "",
  "results": {
    "id": "4c589be6-a7f2-4472-a02f-7613419195ef",
    "name": "lvol1",
    "size": "10G",
    "pool": "9c589be6-a7f2-4472-a02f-7613419195ef",
    "hostname": "ip-172-31-69-49",
    "type": "comp",
    "status": "on"
  },
  "status": true
}
```

### Update LVol

**PUT** http://<CLUSTER_IP>/lvol/UUID

Parameters

| Parameter     | Description                              |
|---------------|------------------------------------------|
| name          | LVol name                                |
| max-rw-iops   | Maximum Read Write IO Per Second         |
| max-rw-mbytes | Maximum Read Write Mega Bytes Per Second |
| max-r-mbytes  | Maximum Read Mega Bytes Per Second       |
| max-w-mbytes  | Maximum Write Mega Bytes Per Second      |

Request

```curl
curl -X PUT http://127.0.0.1/lvol/4c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"name":"lvol2"}'
```

Response

```json
{
  "error": "",
  "results": {
    "id": "4c589be6-a7f2-4472-a02f-7613419195ef",
    "name": "lvol2",
    "size": "10G",
    "pool": "9c589be6-a7f2-4472-a02f-7613419195ef",
    "hostname": "ip-172-31-69-49",
    "type": "comp",
    "status": "on"
  },
  "status": true
}
```

### Delete LVol

**DELETE** http://<CLUSTER_IP>/lvol/UUID

Request

```curl
curl -X DELETE http://127.0.0.1/lvol/4c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### Resize LVol

**PUT** http://<CLUSTER_IP>/lvol/resize/UUID

Parameters

| Parameter | Description                    |
|-----------|--------------------------------|
| size      | LVol size: 10M, 10G, 10(bytes) |

Request

```curl
curl -X PUT http://127.0.0.1/lvol/resize/4c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"size":"20G"}'
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### Resize LVol

**PUT** http://<CLUSTER_IP>/lvol/resize/UUID

Parameters

| Parameter | Description                    |
|-----------|--------------------------------|
| size      | LVol size: 10M, 10G, 10(bytes) |

Request

```curl
curl -X PUT http://127.0.0.1/lvol/resize/4c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"size":"20G"}'
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### LVol set-read-only

**PUT** http://<CLUSTER_IP>/lvol/set-read-only/UUID

Request

```curl
curl -X PUT http://127.0.0.1/lvol/set-read-only/4c589be6-a7f2-4472-a02f-7613419195ef
```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### LVol create snapshot

**PUT** http://<CLUSTER_IP>/lvol/create-snapshot/UUID

Parameters

| Parameter | Description   |
|-----------|---------------|
| name      | snapshot name |

Request

```curl
curl -X PUT http://127.0.0.1/lvol/create-snapshot/4c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"name":"snapshot1"}'

```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

### LVol Clone

**PUT** http://<CLUSTER_IP>/lvol/clone/UUID

Create LVol based on a snapshot

Parameters

| Parameter     | Description    |
|---------------|----------------|
| snapshot_name | snapshot name  |
| clone_name    | new clone name |

Request

```curl
curl -X PUT http://127.0.0.1/lvol/clone/4c589be6-a7f2-4472-a02f-7613419195ef
    -d '{"snapshot_name":"snapshot1", "clone_name":"clone1"}'

```

Response

```json
{
  "error": "",
  "results": "Done",
  "status": true
}
```

