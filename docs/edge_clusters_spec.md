# Edge Clusters — Specification

Status: v2, implemented on branch `edge-clusters` (see `simplyblock_edge/`).
Companion: `docs/edge_clusters_analysis.md` (codebase analysis, library extraction — step 1,
already merged into this branch).

v2 corrections (2026-08-07): volumes are dynamic lvols over the lvstore (the lvstore sits
between the nvmf target and the first raid — §4.3); lvstore **fail-over to the secondary
and fail-back to the primary on node restart** are in scope (§5.6-5.7); **optional crypto
bdevs** between the lvol and the fabric, keyed from the external KMS exactly like
hyperscale lvols (§4.5).

## 1. Scope

Lightweight, spdk-only (non-ultra) storage for 1–2-node edge sites, kubernetes-only,
managed by the **existing centralized control plane** (same CP deployment, same FDB, same
API/security). The CP talks to an edge site over exactly two channels:

1. the edge cluster's **kubernetes API** (worker-node status, pod status, pod deployment
   from rendered yaml), and
2. **SPDK JSON-RPC** (via the spdk proxy container in the edge SPDK pod).

No snode agent, no swarm, no ultra distr/JM machinery. SPDK runs on a deploy-time
choice of 1-6 vCPUs per node (§4.5).
The edge data plane must keep serving autonomously while the uplink to the CP is down —
no CP-held lock, lease, or task gates edge IO.

Out of scope (explicitly): pools, snapshots/clones (registration hooks prepared), QoS,
backups, cross-site replication, 1→2 node expansion (§10).

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
| `is_primary` | first node added; store index 0 (per-store client ports) |
| `spdk_cpus` | deploy-time SPDK vCPU choice, 1..6 (§4.5) |
| `lvstore_base` / `leader_of` | this node's own store backing bdev; the lvs names it currently LEADS |
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

### 4.2 Active/active stores (2-node clusters — product processing)

Each node OWNS a store and runs a live SECONDARY instance of the peer's store
(the spdk-fork primary/secondary lvstore machinery — same as hyperscale):

```
partitions -> aio bdevs -> local raid -> local_top -> bdev_split(2)
    {local_top}p0 (own half)   {local_top}p1 (peer half)
repl subsystem edge-repl:{node}: ns1 = p0, ns2 = p1 (listener data_ip:4430)
er_{peer} controller: er_{peer}n1 (= peer.p0), er_{peer}n2 (= peer.p1)

store of node i:  mirror em_{i} = raid1, superblock, instantiated on BOTH nodes
    on node i (PRIMARY):   [i.p0, er_{j}n2]
    on node j (SECONDARY): [j.p1, er_{i}n1]      (the same two physical copies)
lvstore elvs_{i} on em_{i}; role via bdev_lvol_set_lvs_opts; leader = node i.
```

Single-node clusters keep the flat layout (lvstore directly on the local top,
no split/mirror; created lazily at first volume).

### 4.3 Dynamic volumes, registration, and the two ANA paths

- Volume create places on the least-loaded ONLINE store (balanced across both
  nodes) and runs on the store's LEADER; the creation is **registered on the
  pairing node's secondary instance** (`bdev_lvol_register`, snapshot/clone
  variants when those land) so the lvol bdev exists on both nodes.
- One client subsystem per volume with a namespace and listener on **both**
  nodes: ANA **optimized** on the leader's path, **non-optimized** on the
  peer's. Clients connect both entries from connect-info; kernel ANA steers.
- Client ports are per store (`nvmf_port + store_index`, 4420/4421) so a
  fail-back can fence exactly one store's IO with `nvmf_port_block`.

### 4.4 Optional encryption (crypto bdevs)

`create_volume(crypto=true)` inserts a crypto bdev `ecr_{vol_short}` between
the lvol and the fabric **on both nodes** (the registered lvol makes that
possible). AES_XTS key pairs live in the cluster's KMS via the existing
abstraction (external Vault or LocalKMS), path
`cluster/{cluster_id}/edge-volume/{volume_uuid}` — identical key handling to
hyperscale lvols. SPDK-side key registration and the crypto bdev are runtime
state, re-established from the KMS at every republish; volume delete removes
the DEKs. WAN caveat: crypto-volume *recovery publication* needs the KMS
reachable; in-flight IO never does.

### 4.5 SPDK pod and CPU layout (deploy-time choice: 1-6 vCPUs)

`spdk_cpus` is chosen per node at add time (API `spdk_cpus`, 1..6):

| vCPUs | placement |
|---|---|
| 1 | app + lvs poller + nvmf poller on core 0 |
| 2 | app + lvs poller on core 0; nvmf poller on core 1 |
| 3 | app / lvs poller / nvmf poller on cores 0/1/2 |
| 4-6 | cores 3+ add MORE nvmf poller cores |

The masks (`stack.plan_cpu_layout`) travel as pod env (`SPDK_REACTOR_MASK`,
`SPDK_APP_MASK`, `EDGE_LVS_MASK`, `EDGE_NVMF_MASK`); the lvs poller group is
placed via `bdev_lvol_create_poller_group`. The **same CPU-topology
node-preparation Job the central clusters use**
(`storage_cpu_topology.yaml.j2`: kubelet static cpu-manager policy + reserved
system cpus) runs on every edge node before the SPDK pod deploys (toggle
`SIMPLYBLOCK_EDGE_CPU_TOPOLOGY`, reserved set
`SIMPLYBLOCK_EDGE_RESERVED_SYSTEM_CPUS`). Pod: hostNetwork, nodeSelector on
hostname, privileged (raw partitions via /dev, consumed as AIO — no vfio, no
snode agent).


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

### 5.6 Fail-over (secondary lvstore promotion)

When the monitor sees a store's leader not serving (offline/unreachable/down)
while the peer is ONLINE, it enqueues FN_EDGE_FAILOVER for THAT store
(deduped, params.lvs). The survivor's secondary instance is LIVE, so the task
is exactly the product flow:

1. `bdev_lvol_update_lvstore(lvs)` — refresh the in-memory metadata of the
   secondary instance from its mirror copy (reload-then-grant).
2. `bdev_lvol_set_leader(lvs, leader=True)`.
3. Flip the survivor's listeners for the store's volumes to ANA
   **optimized** — the clients' pre-connected second path takes the IO.

No cold examine, no reconnect. If the owner recovered first, the task no-ops.

### 5.7 Node returns after outage (rebuild + fail-back)

FN_EDGE_NODE_RESTART (monitor-enqueued, deduped) on the returning node:

1. Rebuild aio bdevs + local raid + split + repl subsystem (idempotent).
2. On the surviving peer: re-add the returning node's halves into BOTH of its
   raid instances (its own store's mirror and its secondary instance of the
   returning node's store) → SPDK raid1 rebuilds.
3. On the returning node: re-instantiate both stores (examine of the
   superblocked halves; explicit create fallback), `update_lvstore` its
   secondary instance, republish all paths non-optimized.
4. **Fail-back** (peer leads the returning node's own store): wait for the
   mirror resync, then the product sequence — `nvmf_port_block` on the
   store's client port at the peer (fence), `set_leader(leader=False,
   bs_nonleadership=True)` there, `update_lvstore` + `set_leader(True)` on
   the returning node (examine already reloaded its instance), ANA flip
   (optimized home / non-optimized peer), `nvmf_port_unblock`. The fence
   bounds the handover to the block window (sub-second in hyperscale
   measurements).
   If no takeover happened (restart won the race), the returning node simply
   resumes leadership of its own store (update + set_leader + ANA).
5. Node → `online`; cluster status re-derived.

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
POST   /clusters/{id}/edge-nodes/{node_id}/devices/remove   graceful remove {device_path}
POST   /clusters/{id}/edge-nodes/{node_id}/devices/restart  bring back {device_path}
POST   /clusters/edge                               create edge cluster {name, k8s_*}
                                                    (201 returns the cluster secret)
GET    /clusters/{id}/edge-volumes                  list
POST   /clusters/{id}/edge-volumes                  create {name, size, crypto?}
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

## 10. Open questions / fork capability gates

1. **raid5f rebuild + grow in the fork** — device replace under raid5f and §5.5
   `add_device` both depend on it; the flows surface the SPDK error verbatim if
   unsupported. Needs a fork capability check (owner: core data-plane team).
2. **raid1 superblock semantics** across nodes: fail-over relies on `bdev_examine` of a
   superblocked leg assembling the mirror degraded on the OTHER node; fail-back relies
   on `bdev_raid_delete` leaving the superblock intact on the legs. Both flows carry an
   explicit-create fallback, but the fork behavior must be verified.
3. **Rebuild-progress fields** of `bdev_raid_get_bdevs` — `_wait_raid_synced` gates
   fail-back on "2 legs present, no process/rebuilding marker"; align with the fork's
   actual field names.
4. **1→2 node expansion** under an existing lvstore needs raid1-insert-under or an
   offline migration; v1 rejects it (`add node` fails if a 1-node cluster already has
   an lvstore).
5. **CSI**: capability-aware StorageClass + connect-info caching (analysis §3).
6. Hugepages sizing for 4-vCPU edge hosts (1 vCPU for SPDK) — template defaults to
   1 GiB hugepages; revisit after perf runs.
7. Operator CRD (`EdgeCluster`) + edge-local reconciler — analysis §3.2.
8. KMS DEK caching at the edge for uplink outages (crypto republish needs the KMS).
