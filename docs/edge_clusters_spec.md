# Edge Clusters — Specification

Status: v1 draft, implemented on branch `edge-clusters` (see `simplyblock_edge/`).
Companion: `docs/edge_clusters_analysis.md` (codebase analysis, library extraction — step 1,
already merged into this branch).

## 1. Scope

Lightweight, spdk-only (non-ultra) storage for 1–2-node edge sites, kubernetes-only,
managed by the **existing centralized control plane** (same CP deployment, same FDB, same
API/security). The CP talks to an edge site over exactly two channels:

1. the edge cluster's **kubernetes API** (worker-node status, pod status, pod deployment
   from rendered yaml), and
2. **SPDK JSON-RPC** (via the spdk proxy container in the edge SPDK pod).

No snode agent, no swarm, no ultra distr/JM/hublvol machinery. Runs in 2 vCPU per node.
The edge data plane must keep serving autonomously while the uplink to the CP is down —
no CP-held lock, lease, or task gates edge IO.

Out of scope for v1 (explicitly): pools, snapshots/clones, QoS, encryption/KMS, backups,
multipath/ANA, cross-site replication, node failover (takeover of the client subsystem by
the secondary — designed for, see §5.6, not implemented), 1→2 node expansion (§10).

## 2. Tenancy and placement

- An edge cluster **is a `Cluster` record** with the new field
  `cluster_type: "hyperscale" | "edge"` (default `"hyperscale"`; old FDB records
  deserialize unchanged). This reuses, for free: cluster-secret auth, the
  `/clusters/{cluster_id}` API tenancy shape, the cluster-prefixed task/event key space,
  and CLI/DTO plumbing later.
- Per-edge kubernetes access lives on the Cluster record: `k8s_api_url`,
  `k8s_token: SecretStr`, `k8s_ca_cert` (PEM, optional), `k8s_namespace`
  (default `simplyblock`). Empty `k8s_api_url` means "the CP's own cluster"
  (in-cluster config) — used by tests and single-site deployments.
- All edge code lives in the new top-level package **`simplyblock_edge/`**. It may import
  `simplyblock_core` (models, rpc_client, db) and `simplyblock_lib` (runner/monitor
  bases), but nothing in `simplyblock_core`/`simplyblock_web` may import
  `simplyblock_edge` — except the two explicit mount points: the v2 router registration
  and the JobSchedule `FN_EDGE_*` constants (which live in core's JobSchedule like every
  other task type).

## 3. Data model (`simplyblock_edge/models.py`)

All edge records use **cluster-prefixed composite keys** (`{cluster_id}/{uuid}`) so every
read is a bounded FDB range read — no new full-table scans (analysis §1.3).

### EdgeNode (extends BaseNodeObject → shares the node status vocabulary)

| field | meaning |
|---|---|
| `cluster_id`, `uuid` | key: `{cluster_id}/{uuid}` |
| `hostname` | kubernetes node name (`nodeSelector` target, liveness join key) |
| `mgmt_ip` | node InternalIP (RPC endpoint) |
| `data_ip` | nvmf listener address (defaults to `mgmt_ip`) |
| `rpc_port` / `rpc_username` / `rpc_password` | SPDK proxy endpoint (default 8080) |
| `nvmf_port` | client-facing nvmf-tcp listener (default 4420) |
| `repl_port` | internal node-to-node replication listener (default 4430) |
| `partitions: List[EdgePartition]` | the node's contributed partitions/devices |
| `is_primary` | primary hosts the lvstore + client subsystems (first node added) |
| `status` | from BaseNodeObject: `online`, `offline`, `unreachable`, `down`, `in_creation`, `in_restart`, `removed` |
| `online_since` | for status history |

### EdgePartition (nested)

`device_path` (e.g. `/dev/nvme0n1p4`), `size`, `bdev_name` (assigned by the planner),
`status`: `online` / `failed` / `new` (added, awaiting raid grow) / `removed`.

### EdgeVolume

`cluster_id`/`uuid` key, `name` (unique per cluster, enforced at create), `size`,
`lvol_bdev` (`{lvs}/{name}`), `nqn`, `ns_id` (always 1 in v1 — one subsystem per volume),
`status`: `online` / `offline` / `in_deletion`.

DB access (`simplyblock_edge/db.py`): point reads + prefix range reads only, via the
existing `DBController.kv_store` and `BaseModel.read_from_db`.

## 4. The bdev stack

Naming uses the first uuid segment (`short = uuid.split('-')[0]`) for brevity and
determinism; every name is reconstructable from the records (idempotent reassembly).

### 4.1 Per-node local stack (Michael's rule)

Partition bdevs: `ea_{node_short}_{i}` = `bdev_aio_create(filename=device_path,
block_size=4096)`.

| partitions | local top bdev |
|---|---|
| 1 | the aio bdev itself |
| 2 | `raid1` `el_{node_short}` over the two aio bdevs |
| 3+ | `raid5f` `el_{node_short}` over all aio bdevs (strip 64 KiB) |

### 4.2 Cross-node mirror (2-node clusters only)

- **Every** node exposes its local top via an internal replication subsystem
  `"{cluster.nqn}:edge-repl:{node_uuid}"`, listener `data_ip:repl_port`, ns 1.
  (The primary exposes one too — it is unused until a takeover/failback needs it,
  and keeping the two nodes symmetric makes reassembly trivial.)
- The **primary** attaches the secondary's replication subsystem:
  `bdev_nvme_attach_controller(name="er_{peer_short}", …)` → bdev `er_{peer_short}n1`,
  and builds `raid1` `em_{cluster_short}` = `[local_top, er_{peer_short}n1]`.
- Single-node clusters skip the mirror entirely (per the sketch): the lvstore sits
  directly on the local top.

### 4.3 Lvstore and volumes

- lvstore `elvs_{cluster_short}` on the mirror (2-node) or the local top (1-node),
  `cluster_sz` 4 MiB, `clear_method=unmap`. Primary-only.
- Volume = plain SPDK lvol (thin): bdev `elvs_{cluster_short}/{volume_name}`.
- One client subsystem per volume: nqn `"{cluster.nqn}:edge-lvol:{volume_uuid}"`,
  ns 1 = the lvol bdev, listener `primary.data_ip:nvmf_port`. Clients connect with plain
  `nvme connect -t tcp` — same reconnect-tuning defaults as hyperscale
  (`ctrl-loss-tmo` etc. reused from `constants`).

### 4.4 SPDK pod (2 vCPU)

Rendered by the CP from `simplyblock_edge/templates/edge_spdk_pod.yaml.j2` and created
through the edge cluster's k8s API: `hostNetwork`, `nodeSelector` on `hostname`,
privileged (raw partition access via `/dev` hostPath), spdk container + spdk-proxy
container, 2 CPU / small hugepage allocation. Pod name `edge-spdk-{node_short}`. No init
Job, no vfio binding, no kubelet reconfiguration — partitions are consumed via AIO, so
the kernel keeps owning the devices.

## 5. Control flows (all through `simplyblock_edge/edge_cluster_ops.py`)

### 5.1 Create cluster
`create_edge_cluster(name)` → Cluster record: `cluster_type=edge`, uuid, generated
`secret`, `nqn = CLUSTER_NQN:{uuid}`, `status = unready` (flips to `active` when the
first node reaches ONLINE), `mode = kubernetes`.

### 5.2 Add node (max 2; every node needs ≥1 free partition)
1. Persist EdgeNode (`in_creation`, `is_primary` = "no primary exists yet").
2. Deploy the SPDK pod via the edge k8s API; wait for RPC liveness.
3. Build the local stack (§4.1) + replication subsystem (§4.2).
4. Second node: on the primary, attach the new node's repl subsystem and either build
   the mirror + lvstore (if the cluster had no lvstore yet, i.e. nodes were added
   before any volume existed) or fail (1→2 expansion under an existing lvstore — §10).
5. First node: create the lvstore (§4.3).
6. Node → `online`; cluster status re-derived.

### 5.3 Volume create / delete / resize / connect
- create: unique-name check (prefix scan of the cluster's volumes — bounded), lvol
  create on the primary, subsystem + ns + listener, persist EdgeVolume (`online`).
- delete: mark `in_deletion`, tear down subsystem then lvol, remove record.
- resize: `bdev_lvol_resize` + record update.
- connect info: `[{transport: tcp, ip: primary.data_ip, port: nvmf_port, nqn}]` — single
  path in v1.

### 5.4 Node statuses (monitor, §6) and admin shutdown
`shutdown_node` (admin): status → `down`; the monitor never auto-restarts a DOWN node
(that is the operator's explicit intent — same rule as hyperscale
`auto_restart_disabled`). `restart_node` (admin): enqueues FN_EDGE_NODE_RESTART.

### 5.5 Device replace / add
- `replace_device(node, old_path, new_path)`: only meaningful when the partition is a
  raid member (local raid1/raid5f) or the node participates in the mirror; enqueues
  FN_EDGE_DEVICE_REPLACE. The task: `bdev_raid_remove_base_bdev(old_aio)` (if still
  present) → `bdev_aio_delete` → `bdev_aio_create(new)` → `bdev_raid_add_base_bdev` →
  SPDK raid rebuild. Record updated (`failed` → `online`, new path).
- `add_device(node, path)`: partitions ≥3 → `bdev_raid_add_base_bdev` on the raid5f
  (**fork-capability gate**: upstream raid5f has no rebuild/grow; the call is made and a
  clear error is surfaced if the fork rejects it — see Open Questions).

### 5.6 Node returns after outage (rebuild)
The monitor detects "probe says reachable, record says offline/unreachable/in_restart"
and enqueues FN_EDGE_NODE_RESTART (deduped). The task, on the returned node:
1. Recreate aio bdevs + local stack + repl subsystem (idempotent — names are derived).
2. If the returned node is the **secondary**: on the primary, re-attach
   `er_{peer_short}` (if the controller is gone) and `bdev_raid_add_base_bdev` the
   remote leg back into `em_…` → raid1 rebuild runs inside SPDK, no CP data path.
3. If the returned node is the **primary**: rebuild local stack, re-attach the remote
   leg, recreate/examine the mirror (`bdev_examine` → lvstore loads), then recreate
   every client subsystem + ns + listener from the EdgeVolume records.
4. Node → `online`; cluster status re-derived.

Takeover (serving volumes from the secondary while the primary is dead) is deliberately
**not** in v1: the mirror keeps a full copy on the secondary, and the repl subsystem the
secondary already exposes is the mount point a future takeover flow will use.

## 6. Status model

### 6.1 Node status derivation (pure function, `simplyblock_edge/status.py`)

Probe = (k8s node Ready?, SPDK pod running?, RPC get_version ok?) via the per-cluster
k8s client + RPCClient. Decision, in order:

| condition | status |
|---|---|
| record says `down` (admin) or `removed` or `in_creation`/`in_restart` (flow-owned) | unchanged — the monitor never overrides these |
| k8s API unreachable, node object missing, or node NotReady | `unreachable` |
| pod missing / not running, or RPC dead | `offline` |
| RPC alive but record was offline/unreachable | stays as-is; a FN_EDGE_NODE_RESTART task is enqueued (reassembly decides `online`) |
| RPC alive and record `online` | `online` |

Mgmt-plane-only blips are contained the same way hyperscale learned to (analysis §2.3):
`unreachable` is a CP-view verdict — the edge data plane keeps serving; nothing about
`unreachable` triggers destructive action, and the transition back requires the
reassembly task to confirm the stack.

### 6.2 Cluster status derivation (pure, Michael's rule verbatim)

Over non-removed nodes; `down` counts as not-serving (it is a deliberate stop):

- every node offline/unreachable/down → **suspended**
- at least one online and at least one not-online → **degraded**
- all online → **active**
- no nodes yet → **unready**

Statuses reuse `Cluster.STATUS_*`; writes go through `atomic_update` (never full-object
writes — the StorageNode lost-update lessons apply unchanged).

## 7. Background services

Both are thin subclasses of the step-1 library bases and run per-CP (not per-edge):

- **`simplyblock_edge/services/edge_monitor.py`** — `PollingService` (interval 10 s,
  fast 3 s while any cluster is not active, wedge threshold 60): sweeps
  `cluster_type == edge` clusters; per cluster: probe every node (§6.1), CAS node
  status, enqueue restart tasks, derive + CAS cluster status. Per-cluster isolation:
  one unreachable edge site must not stall the sweep (probe timeouts are bounded:
  k8s 5 s, RPC 3 s).
- **`simplyblock_edge/services/tasks_runner_edge.py`** — `TaskRunner` over
  `FN_EDGE_NODE_RESTART`, `FN_EDGE_DEVICE_REPLACE`, `FN_EDGE_DEVICE_ADD` with the
  standard host lease, retry backoff (base 3 s, cap 300 s), `max_retry` 11 for restarts.

WAN posture: all edge writes are JobSchedule tasks (never API-request threads); the
monitor's probe budget per node is ≤ 8 s worst case; task retries absorb uplink flaps.

## 8. API surface (v2 only)

Mounted under the existing cluster tree (auth: same bearer schemes; the `cluster_id`
path-param coupling that authorizes per-tenant keeps working):

```
GET    /clusters/{id}/edge-nodes                    list
POST   /clusters/{id}/edge-nodes                    add node {hostname, mgmt_ip, data_ip?, partitions[]}
GET    /clusters/{id}/edge-nodes/{node_id}          detail
POST   /clusters/{id}/edge-nodes/{node_id}/shutdown admin stop (→ down)
POST   /clusters/{id}/edge-nodes/{node_id}/restart  enqueue restart task
POST   /clusters/{id}/edge-nodes/{node_id}/devices  add device {device_path}
PUT    /clusters/{id}/edge-nodes/{node_id}/devices  replace {old_path, new_path}
GET    /clusters/{id}/edge-volumes                  list
POST   /clusters/{id}/edge-volumes                  create {name, size}
GET    /clusters/{id}/edge-volumes/{vol_id}         detail
DELETE /clusters/{id}/edge-volumes/{vol_id}         delete
PUT    /clusters/{id}/edge-volumes/{vol_id}         resize {size}
GET    /clusters/{id}/edge-volumes/{vol_id}/connect connect info
```

Long-running operations (add node, restart) return 202 and run as tasks — checked via
the existing `/clusters/{id}/tasks`. Edge cluster create: `POST /clusters` gains
`cluster_type` (edge path skips the hyperscale activation machinery). DTOs are local to
the edge router module (the `_dtos.py` monolith is not extended). CLI command group:
follow-up (one `cli-reference.yaml` block, per analysis §1.2).

## 9. Deployment & security notes

- The CP reaches the edge k8s API with a ServiceAccount token provisioned at
  site-onboarding time (`k8s_token`), stored as `SecretStr` on the Cluster record like
  every other cluster secret. TokenReview-based *inbound* auth is unchanged (edge CSI
  authenticates with the cluster secret — analysis §3.1).
- The SPDK proxy is reachable from the CP at `mgmt_ip:rpc_port` with basic auth
  (`rpc_username`/`rpc_password`, generated per node). TLS via the existing `SB_TLS_*`
  scheme when enabled.
- Discovery of free partitions is the operator's input in v1 (`partitions[]` at
  node-add). The discovery-Job/CR flow (analysis §2.2) is a follow-up.

## 10. Open questions / follow-ups

1. **raid5f rebuild + grow in the fork** — device replace under raid5f and §5.5
   `add_device` both depend on it; the flows surface the SPDK error verbatim if
   unsupported. Needs a fork capability check (owner: core data-plane team).
2. **1→2 node expansion** under an existing lvstore needs raid1-insert-under or an
   offline migration; per the sketch v1 simply rejects it (`add node` fails if a
   1-node cluster already has an lvstore, i.e. volumes were created before the second
   node was added).
3. **Takeover/failback** (secondary serves while primary dead) — §5.6.
4. **CSI**: capability-aware StorageClass + connect-info caching (analysis §3).
5. Hugepages vs `--no-huge` for 2-vCPU hosts — template defaults to 1 GiB hugepages;
   revisit after perf runs.
6. Operator CRD (`EdgeCluster`) + edge-local reconciler — analysis §3.2.
