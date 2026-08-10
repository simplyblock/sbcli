# Edge Clusters — Codebase Analysis & Refactoring Plan

Status: draft for team discussion (branch `edge-clusters`, 2026-08-06).

Scope recap: kubernetes-only, spdk-only (non-ultra) 1–2 node edge clusters, managed by the
existing **centralized** control plane (same CP, same FDB, new services). Local data path:
raid1 across two nodes (one leg local, one leg nvme-tcp to the peer), local leg = aio bdev /
raid1 / raid5 depending on device count. CP↔edge channels are exactly two: SPDK JSON-RPC and
the kubernetes API of the edge cluster. Runs in 2 vCPU. Edge storage must stay autonomous
while the uplink is down.

---

## 1. Which infrastructure to extract into libraries

### 1.1 Task runner framework — extract, highest value

Today there is **no framework**, only a convention: a shared model + claim/lease helper,
re-implemented as a hand-written `while True` loop in ~17 `services/tasks_runner_*.py`
processes with drifting retry/backoff/cancel semantics.

The genuinely generic core is small and clean (~200–300 lines, liftable almost verbatim):

- `models/job_schedule.py` — the task record (statuses, retry, owner, sub_tasks). Only
  sbcli-specific content is the hardcoded `FN_*` constants → replace with a registry.
- `controllers/tasks_controller.py:1-148` — `claim_task` (CAS via `db.atomic_update`),
  `_task_lease_is_stale`, `refresh_task_lease`, `task_lease_heartbeat`. Zero coupling to
  StorageNode/Cluster/rpc_client.
- `models/base_model.py` FDB read/write/chunked-scan (minus the `StorageNode` write-tripwire).

What the library should **add** (this is where the 17× duplication lives):

- A `TaskRunner` base class: poll loop, function_name filter, claim, heartbeat context,
  retry/backoff bookkeeping (currently the task *body* mutates `retry` and the loop infers
  failure-vs-deferral by diffing it — invert this contract), cancel/defer checks, FDB-wedge
  self-restart (exists in exactly one runner today: `tasks_runner_sync_lvol_del.py`).
- A `PollingService(interval, adaptive=...)` and `PerNodeSupervisor` base for the ~12 monitor
  services (two copy-pasted patterns: flat sweep loop, thread-per-node supervisor). Pure
  boilerplate; edge gets per-item exception isolation and thread respawn for free.
- One lock primitive. We currently have four unrelated idioms: per-task host lease,
  in-process inflight maps, FDB lock models (`restart_lock`, `lvstore_lock`,
  `ClusterAddNodeLock`, …), and restart-claim fields on the StorageNode row.

Known defects any new consumer would inherit — fix during extraction:
`get_task_by_id` scans the whole task table (no uuid index; date is baked into the FDB key);
two runners execute at module import (no `main()`); task GC is a side effect of
`storage_node_monitor`.

### 1.2 API / security infrastructure — extract the v2 stack only

- **v2 (FastAPI) is the library.** Cleanly generic already: `api/v2/_auth.py`,
  `api/v2/_dependencies.py` (hierarchical resource resolution), `api/v2/util.py` (typed
  scalars, `creation_response`), `api/v2/meta.py` (health/ready), `simplyblock_web/settings.py`,
  `simplyblock_core/settings.py` (TLS), `simplyblock_core/utils/secrets.py`, plus the
  `AccessLogMiddleware` + exception handlers currently inlined in `app.py`.
- **v1 (Flask) has essentially no reusable scaffolding** — inline hand-rolled validation.
  Edge should be v2-only; do not build a v1 surface for it.
- Prerequisites before extraction:
  - `app.py` has no router-registry seam — version gating and the legacy-redirect list are
    hardcoded. Add a plugin point so an `edge` router tree mounts without editing app.py.
  - `_dtos.py` (680 lines) and `_dependencies.py` are single-file monoliths across every
    resource — split per-resource first.
  - `simplyblock_web/utils.py` straddles three apps/frameworks — split into "v1 response
    envelope" vs "shared validation patterns".
- **Two auth gotchas that bite edge directly:**
  1. v2 authorization hinges on a route parameter literally named `cluster_id`
     (`_auth.py:135-157`). A resource tree not nested under `/clusters/{cluster_id}` is
     authenticated but **not** authorized per-tenant. Edge resources must either nest under
     the same path shape or we replace the parameter coupling with a real tenancy abstraction.
  2. Cluster-secret auth enumerates **all clusters** per request and compare-digests each
     (`_auth.py:107-132`) — O(#clusters) per API call. Fine at 10 clusters, not at 500 edge
     sites. Needs a keyed lookup (secret→cluster index or token embedding the cluster id).
- CLI: `cli.py` is 100% generated from `cli-reference.yaml`; adding an `edge-cluster` command
  group is one YAML block + methods in `clibase.py`. The generic scaffolding worth
  extracting from `clibase.py` is only ~120 lines (type factories, formatters, parser trio).
- KMS (`simplyblock_core/kms/`) is already an abstract interface (LocalKMS/Vault) — reuse as-is.
- Events: `events_controller.py` is a thin generic writer (needs only `.name` +
  `.get_clean_dict()` from its subject) — trivially extractable.

### 1.3 DB layer — reuse, don't abstract yet, but respect the scan rule

There is **no swappable seam**: `base_model.py` and `db_controller.py` both speak raw `fdb`
(transactionals, range reads, direct key indexing). Introducing a Postgres facade now would
mean rewriting the persistence layer and re-proving `atomic_update`'s CAS semantics —
agreed with the thread conclusion: stay on the shared FDB, one CP, one DB.

What edge **must** do from day one (the "no new table scans" rule):

- Composite keys prefixed by `cluster_id` (the pattern `JobSchedule`/`EventObj`/`Backup`
  already use) so all edge reads are bounded range reads.
- Name lookups via `name_index/`-style keys (the pattern exists: `lvol_name_lookup`), never
  scan-and-filter. Note as prior art: v1 `POST /lvol` still does two full-table scans per
  CSI CreateVolume despite the index existing — don't replicate that.
- ~35 existing `get_*` methods are full-table scans (list in the exploration notes); edge
  code paths must not call them in loops.

### 1.4 Proposed package shape

```
simplyblock_lib/            # new: shared, no sbcli imports
  tasks/                    # JobSchedule-equivalent, claim/lease, TaskRunner base
  monitors/                 # PollingService, PerNodeSupervisor
  api/                      # FastAPI scaffolding: auth, deps, util, meta, middleware
  events/                   # event writer
  settings/                 # TLS + web settings
  kv/                       # base_model persistence (thin; still FDB)
simplyblock_core/           # existing hyperscale logic, now importing simplyblock_lib
simplyblock_edge/           # new: edge cluster ops, edge monitors, edge task types
simplyblock_web/            # mounts core + edge router trees behind one app/auth
```

---

## 2. Kubernetes-control-plane-side limitations (beyond FDB load)

### 2.1 Cross-cluster access is new

Everything k8s-native in sbcli today assumes **in-cluster config of the CP's own cluster**
(`utils.get_k8s_*_client()` → `load_incluster_config`). Edge requires the CP to talk to N
*remote* kube-apiservers:

- Need a per-edge-cluster credential store (kubeconfig / SA token + CA) in the Cluster model,
  and a client factory keyed by cluster — touch every `patch_cr_*` / pod-management helper.
- v2 SA-token auth (`TokenReview`) validates against the **CP's** cluster only. An edge-local
  CSI driver's projected SA token is meaningless to the central CP. Edge API clients must use
  cluster-secret auth (see 1.2 gotcha #2) or the CP must run TokenReview against the *edge*
  cluster's API — feasible, but new code.

### 2.2 The snode-API gap is small for edge — because of partitions

In k8s mode today, the CP calls the node agent, and the **agent** calls the k8s API from
inside the cluster (renders `storage_deploy_spdk.yaml.j2`, creates Jobs/Pods). So the
yaml-render + Job/Pod machinery already exists — it just sits on the wrong side of the wire.
Moving it CP-side is largely a relocation.

Of the snode API surface, already replaceable with k8s API + SPDK RPC:

- SPDK pod start/kill/is-up → create/delete/list namespaced pod (code exists in
  `api/internal/storage_node/kubernetes.py`).
- Node liveness → `list_node` Ready condition; the pattern already exists in
  `mgmt_node_monitor.K8sNodeBackend` (storage-node monitoring today is ICMP + snode API +
  RPC — no k8s involvement; edge inverts that).
- Port block/unblock → `port_block.py` already prefers the SPDK RPCs
  (`nvmf_port_block/unblock/get_blocked_ports`); the iptables fallback is legacy.

The irreducible residue is (a) hardware discovery (`info()`/`scan_devices` — PCI NVMe lists,
NUMA hugepages, RoCE mapping) and (b) privileged host mutations (vfio bind, `nvme format`,
gpt partitioning over NBD, hugepage/kubelet orchestration). **The edge design mostly sidesteps
both**: nodes contribute pre-existing free *partitions* consumed as **AIO bdevs** — no PCI
driver binding, no nvme format, no partitioning by us, no NUMA topology work. What remains:

- A minimal discovery step: which partitions/devices exist and are free. One-shot privileged
  Job (or init container of the SPDK pod) publishing to a CR/ConfigMap — not a resident agent.
- Hugepages: SPDK needs some; on 2 vCPU boxes decide between a small static hugepage
  allocation in the node spec vs `--no-huge`. Init-Job pattern exists but currently loops
  back through `/snode/apply_config` — the hugepage math must move into the Job image.
- DHCHAP/PSK key files (`write_key_file`) → project a k8s Secret into the SPDK pod instead.

### 2.3 WAN/slow-uplink assumptions baked into the CP

- Monitors poll per-node every 3–30 s with LAN-tuned timeouts (`is_live` timeout 5 s,
  retry 1); runners poll the task table every 3–10 s per cluster. At hundreds of edge sites
  over slow links this needs: per-cluster-class intervals, jitter, strict timeout budgets,
  and sharding of monitor services by cluster set. The existing per-node-thread supervisor
  pattern scales to nodes, not to 500 clusters × RTT.
- Long-running API writes are fire-and-forget **threads inside the uvicorn worker** (202 +
  thread dies with the worker; no idempotency token). Acceptable on a LAN, bad over WAN —
  edge operations should be JobSchedule tasks from day one, never request-thread work.
- Status semantics: mgmt-plane unreachability must not mark storage down. The hyperscale
  monitor already learned this (`UNREACHABLE` counts only with data-plane quorum;
  `get_next_cluster_status` in `storage_node_monitor.py`). Edge needs the same separation,
  but the "peer quorum" concept degenerates at n=1/2 — the uplink being down is the *normal*
  failure mode and must map to `unreachable` (CP view) while the edge keeps serving.
  The proposed edge status derivation (all offline → suspended; one of two offline →
  degraded; else active) is a ~50-line pure function — do **not** reuse the ndcs/npcs
  arithmetic.
- Autonomy: with no CP-driven failover at the edge, everything that must survive uplink loss
  has to be SPDK-native: raid1 auto-resync on leg reappearance, nvmf reconnects, and —
  critically — **no CP-held lock or task lease may gate edge IO**.

### 2.4 FDB / API footprint (the caveat from the thread, made concrete)

- Per-request cluster enumeration in v2 auth (see 1.2) — first thing that melts with many
  edge clusters.
- `get_task_by_id` whole-table scan; monitors iterating `db.get_clusters()` every tick;
  events/tasks retention keyed to one cluster's monitor. All linear in cluster count.
- Status vocabulary is duplicated as pydantic `Literal`s in `api/v2/_dtos.py` — adding edge
  statuses (`degraded` exists for clusters; fine) or new task function names breaks v2
  serialization if the Literal isn't updated in the same change.

### 2.5 Data-path items to verify in the SPDK fork (not CP, but gating)

- raid1 rebuild on leg re-add: supported; verify behavior when the leg is an nvme-tcp bdev
  that reconnects (bdev re-registration vs new bdev name).
- **raid5f rebuild**: upstream SPDK raid5f historically lacks rebuild support. "Replace a
  partition → rebuild via raid" and "later add a device under the raid5" both depend on
  this — needs an explicit fork-capability check; raid5f grow/reshape almost certainly
  does not exist and "add device" may mean recreate-and-resync.
- 2 vCPU: single reactor + app thread; consider interrupt mode / dynamic scheduler to not
  burn a core polling on an idle edge box.

---

## 3. Operator / CSI — high-level impact

### 3.1 Topology decision (the "CSI across two clusters" question)

A CSI driver cannot span clusters: the node plugin must run where kubelet mounts volumes
(edge), and the controller plugin's sidecars (provisioner/attacher/snapshotter) watch
PVC/PV objects, which live in the **edge** cluster's kube API. So:

- **CSI deploys entirely per edge cluster** (controller + node parts), but its controller is
  just an HTTP client of the central management API — point it at the central endpoint over
  the uplink. No CSI code split across clusters.
- Consequence: CSI must tolerate uplink loss gracefully — provisioning stalls (acceptable),
  but NodeStage/NodePublish and health of already-attached volumes must not depend on the
  CP. Today's node plugin gets connect info from `GET /lvol/connect` at stage time; cache it
  (or persist it in the volume context at provision time) so remounts/reboots during an
  uplink outage still work.
- Auth: the driver authenticates with the edge cluster's secret (SA TokenReview won't work
  cross-cluster, see 2.1).

### 3.2 Operator

- Aligns with the existing plan (per the thread): CP install becomes its own CRD; SPDK pod
  management moves from sbcli into the operator. For edge, the **central** operator manages
  remote clusters → it needs the same per-edge kubeconfig plumbing as the CP (2.1), or —
  simpler — a thin edge-local operator instance that only reconciles pods/yaml while the
  central CP stays the source of truth. Recommend the latter: it keeps "edge keeps running
  standalone" true for pod restarts too (kubelet restarts pods anyway, but CR-driven changes
  queue up).
- New CRD: `EdgeCluster` (or `StorageCluster` with a profile field): nodes (1–2), per-node
  device/partition list, uplink endpoint + credential ref. CR write-back
  (`patch_cr_status/…`) must be parameterized by target cluster; today it hardcodes
  in-cluster config.
- The CR contract is currently triple-encoded (connect-entry model, v2 DTOs, camelCase CR
  patch dicts in `controllers/*_events.py`) and documented nowhere except e2e helpers —
  edge is the forcing function to write it down before adding a fourth shape.

### 3.3 Functions that don't exist at the edge (CSI/API surface diff)

Available/unchanged: create/delete/resize volume, snapshots + clones on the local lvstore,
connect info (single path or the raid1-exposing node), QoS, encryption (KMS is central —
cache/lease DEKs edge-side or crypto volumes fail closed on uplink loss — needs a decision).

Not available at edge (CSI/operator must degrade cleanly, API should reject early):

- ha_type=ha multipath fan-out, secondary/tertiary roles, hublvol/JM machinery, distr —
  the whole ultra data plane. Edge HA is the raid1 layer instead.
- Device migration tasks (`FN_DEV_MIG` family), cluster expand beyond 2 nodes, failure
  domains, cloud IMDS metadata.
- Backups to the extent they assume the ultra stack — needs a per-feature check.

Suggested mechanism: a capability field on the Cluster record (`cluster_type: hyperscale |
edge`), surfaced through the API, gating both CSI behavior (StorageClass parameters) and
CLI/API validation, instead of scattering `if edge` checks.

---

## 4. Suggested build order

1. Extract `simplyblock_lib` (tasks core + runner base, monitor bases, v2 API scaffolding,
   events, settings) — pure refactor, hyperscale behavior unchanged, immediately reduces
   the 17-runner drift.
2. Fix the two auth scaling issues (secret index, tenancy abstraction) — needed regardless.
3. Per-edge k8s client factory + credentials on the Cluster model; discovery Job + SPDK pod
   yaml (reuse/trim `storage_deploy_spdk.yaml.j2`); edge node/cluster status monitor on the
   new monitor base.
4. Edge volume ops (aio/raid1/raid5 stack via existing `rpc_client`), edge task types
   (node restart/rebuild, device replace, device add) on the new runner base.
5. CSI: capability-aware StorageClass + connect-info caching; operator: `EdgeCluster` CRD +
   edge-local reconciler.

Open questions for the team: raid5f rebuild status in the fork (§2.5); DEK caching policy
for encrypted volumes at the edge (§3.3); whether the discovery Job publishes to a CR or a
ConfigMap; interval/sharding policy for CP monitors at O(100s) of clusters (§2.3).
