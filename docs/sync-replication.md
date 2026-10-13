# Site-level synchronous replication

One simplyblock cluster (one control plane, one FoundationDB) spans **exactly two sites**. Every
lvstore (LVS) and all of its volumes are replicated synchronously to the other site. The Kubernetes
orchestrator (csi-addons / Ramen, through the simplyblock CSI driver and operator) moves volumes
between the sites with `DemoteVolume` / `PromoteVolume`. This document is the contract for the
operator / CSI developer and the requirements for whoever deploys such a cluster.

Scope of this first version (prototype):

- **Planned switchover** (both sites up) and **disaster fail-over** (one site lost). There is no live
  fail-over or fail-back: applications are stopped while their volumes switch sites.
- Promote and demote are allowed only while the replicas are in sync (for a disaster fail-over: in sync
  when the other site was lost).
- The catch-up (resync) after a desync runs automatically, in both directions.
- `EnableVolumeReplication`, `DisableVolumeReplication` and `ResyncVolume` are no-ops.

Implementation: `simplyblock_core/controllers/sync_replication_controller.py` (status, gates, demote,
promote, site return), `simplyblock_core/services/tasks_runner_sync_promote.py`,
`simplyblock_core/services/tasks_runner_sync_resync.py`, the ANA site rule in
`simplyblock_core/storage_node_ops.py` (`lvol_site_open`, `lvol_ana_state`), the REST routes in
`simplyblock_web/api/v2/cluster/storage_pool/volume/replication.py` and
`simplyblock_web/api/v2/cluster/consistency_group.py`.

---

## 1. Model

### Sites, LVS and instances

- Every storage node belongs to one site (`StorageNode.site`); all nodes of one host are on the same
  site.
- The **home site** of an LVS is the site of its primary node (the LVS owner). In that LVS's data
  layout the home site's devices are zone 0 and the other site's devices are the replica zone.
- Each LVS keeps its full local triplet on the home site (primary / secondary / tertiary, as without
  sync replication) **plus a remote triplet** on the other site (remote-primary / remote-secondary /
  remote-tertiary): **6 instances per LVS**, **5 on an FTT1 cluster** (the local roles follow the FTT:
  primary + secondary, no local tertiary; the remote triplet always has three members).
- The hublvol (IO redirect to the leader) stays within a site; there is no cross-site IO redirect.
- The journal of an LVS has copies on both sites; a write is acknowledged only with at least two local
  and two remote journal copies (or, with fewer than two working remote copies, it continues on the
  local ones and reports `remote_journal_in_sync = false`).

### Which site serves a volume

- An LVS has one **active site** at a time (`StorageNode.lvs_active_site` on the owner: empty = home
  site, `moving:<site>` while a move to that site is in flight); this document says the LVS is **led
  from** that site. It is a record, not the report of an instance: it names the triplet that may lead
  the LVS, and only a promote changes it. For a while after a promote no instance leads the LVS at all
  (see [The active and the passive triplet](#the-active-and-the-passive-triplet)). A volume is served
  only from the active site of its LVS, so **the active site moves for the whole LVS, while publication
  is per requested volume**: before a planned move every volume of the LVS must be closed on the source
  site, and after it only the volumes of the promote request are opened on the target site (the others
  stay closed and keep their assigned site until their own promote). Volumes with the same
  `storage_node_id` (the owner node in the volume DTO) share an LVS.
- Per volume the control plane records the site it is assigned to (`LVol.sync_active_site`, default the
  LVS's active site at creation) and the sites it is fenced on (`LVol.sync_demoted_sites`).
- The cluster records a lost site (`Cluster.lost_site`, `lost_site_state` = `fencing` while the site
  steps of a disaster fail-over run, `done` after).

### The active and the passive triplet

- The triplet on the active site - the **active triplet** - works exactly as the triplet of a cluster
  without sync replication, whichever site it is on:
  - one of its members leads the LVS, normally its primary; when the primary fails, its secondary takes
    the leadership over on client IO (in-site failover);
  - a restarted member is rebuilt by its position: the primary as the leader (the LVS is examined from
    disk and the leadership granted after that; a member that led meanwhile is demoted first), the
    secondary as the leader only when the primary is disconnected and as a non-leader otherwise, the
    tertiary never as the leader (see [Node restart](#6-flows-for-the-operator) for the one case in
    which its instance is not rebuilt at all).
- The triplet on the other site - the **passive triplet** - holds non-leader instances whose paths are
  closed by ANA. It never takes the leadership: not at a restart, and not when the whole active triplet
  is down. Only a promote makes it the active one.
- While a move is in flight (`moving:*`) no member of either triplet is rebuilt as a leader.
- **Between a promote and the first client IO on the new site nobody leads the LVS.** A promote hands
  the LVS off without granting the leadership (see [Promote](#promote)); the primary of the new active
  triplet takes it itself when the first IO arrives. Until then every instance reports "not leading".
  That is a normal state: it blocks no node restart, and the automatic catch-up and the scheduled
  snapshots wait for it to end (see [Known limitations](#8-known-limitations)).
- Specific to sync replication are only the promote and the demote, the passive triplet, the election
  of a leader when a lost site returns, and the catch-up runner.

### ANA rule (who publishes which path)

One site rule (`storage_node_ops.lvol_ana_state`) decides the ANA state of a sync volume's path
wherever a listener is published or an ANA state is set in the normal course (create, restart,
monitor repair, in-site failover, activation). The demote and promote themselves set the states of
one site's triplet directly (`sync_replication_controller.set_site_ana_strict`: close, or primary
`optimized` / others `non_optimized`), under their own gate and lock, and record the result so that
the site rule agrees afterwards.

- A site is **open** for a volume only if it is the volume's `sync_active_site` **and** the site its LVS
  is led from, the volume is not demoted there, the site is not the cluster's `lost_site`, and no
  leadership move of the LVS is in flight (`moving:*` closes both sites).
- On the open site: the triplet primary is `optimized`, its secondary / tertiary `non_optimized`. While
  the active triplet's primary is `OFFLINE` (not while it is `RESTARTING`), its secondary is
  `optimized` (in-site failover).
- A follower path the rule computes as `non_optimized` is published `inaccessible` instead when its
  instance reports its hublvol redirect to the leader as **known broken** (`bdev_lvol_get_lvstores`:
  not connected or no redirect); it stays closed until a re-activation or restart wires it. When the
  redirect state cannot be told (RPC failure, missing field) or the instance reports that it leads,
  the computed state is published unchanged. The `optimized` path of the in-site failover above is
  never downgraded by this check.
- Every other path is `inaccessible`, including every path of the other site. During activation every
  path stays closed until the final pass.

A volume therefore always has exactly one serving site or none (during a move, after a demote, on a
lost site before the promote).

---

## 2. Deployment requirements

### Cluster and nodes

| What | Rule |
|---|---|
| Cluster | `sbctl cluster create --sync-replication` (v2: `"sync_replication": true` in the cluster create body). Requires `--ha-type ha`, not a single-node cluster. **Deploy-time only**: an existing cluster cannot be switched to sync replication. |
| Node | `sbctl sn add-node ... --site <name>` (v2: `"site"` in the storage-node create body). Mandatory on a sync cluster, rejected (400) on any other. Site name: `[A-Za-z0-9][A-Za-z0-9._-]{0,62}`. A host must sit entirely on one site. |
| Sites | Exactly two sites; every node has a site. Checked at activation. |
| Capacity per site | At the first activation each site needs `ndcs + npcs` online nodes on at least `max(3, ha_jm_count / 2)` hosts (a full zone of every LVS, a host-disjoint triplet, its half of every journal); refused below that. A re-activation with less capacity only logs a warning. |
| Failure domains | Optional. When enabled, the first activation enforces per site: at least `npcs + 2` distinct failure domains, the same number of hosts in every domain, a host in one domain; the role rotation is built per site. |
| Journal (HA JM) | Always on (`--disable-ha-jm` is ignored). `ha_jm_count` counts both sites: default **6** (3 per site), **8** with FTT2 or failure domains (4 per site); an override must be even, at least that default and at most 8. |

### Node sizing

Each node hosts, besides its own LVS, local roles (secondary / tertiary) of LVSs homed on its site
**and** remote roles of LVSs homed on the other site: roughly twice the LVS instances, journal (JC)
contexts, memory and hublvol / subsystem state of a node in a non-sync cluster of the same size. A node
may host at most **16 JC contexts** (local + remote roles; the data plane's `jc_replace_jm` limit);
activation and cluster expansion refuse a topology that exceeds it
(`cluster_expansion/planner.py`, `JC_MAX_CONTEXTS_PER_NODE`).

### Control plane

The management plane and FoundationDB are **not** replicated by this feature. A disaster fail-over
runs on the control plane, so it must stay available when either site is lost: place the management
nodes and the FoundationDB quorum so that they survive the loss of either site on their own (for
example with a quorum member in an independent third failure domain). Splitting a majority quorum over
the two storage sites does not survive the loss of the site holding the majority. The two-site rule
applies to the storage topology only.

### Background services

Two task runners are new. Both must run on the management plane; without them a promote answers
"in progress" forever and no resync ever starts.

| Service | Task | Command (path form) | Name form |
|---|---|---|---|
| `TasksRunnerSyncPromote` | `FN_SYNC_PROMOTE` (promote / disaster fail-over) | `python3 simplyblock_core/services/tasks_runner_sync_promote.py` | `python -m simplyblock_core.services tasks-runner-sync-promote` |
| `TasksRunnerSyncResync` | `FN_SYNC_RESYNC` (catch-up per LVS) | `python3 simplyblock_core/services/tasks_runner_sync_resync.py` | `python -m simplyblock_core.services tasks-runner-sync-resync` |

- Docker Swarm: already in `simplyblock_core/scripts/docker-compose-swarm.yml` and in the service list
  that `cluster_ops` creates.
- **Kubernetes: the operator must add both to its control-plane runner table**
  (simplyblock-operator `operator/internal/controllers/controlplane/managementapi.go`).
- The existing `MainDistrEventCollector` records the sync events (zone desync, remote journal
  dropped / restored) that the status, the disaster gate and the resync depend on, and the
  `StorageNodeMonitor` runs the site return; both must run as today.

---

## 3. REST API

All routes are v2, token-authenticated as every v2 route. Prefixes used below:

- `V = /api/v2/clusters/{cluster_id}/storage-pools/{pool_id}/volumes/{volume_id}`
- `G = /api/v2/clusters/{cluster_id}/consistency-groups/{group_id}`

The sync behaviour is chosen by the cluster (`sync_replication`); on a cluster without it every route
behaves as before (async snapshot replication) and `site` is ignored.

### The `site` query parameter

`site` is the site the caller acts from - static per Kubernetes cluster. On a sync cluster it is
required on every route marked "site" below: missing or empty -> **400**; not a site of the cluster ->
**400**. `GET V/connect` is the exception: it takes no `site` and reads the led site itself (see its
row), since a node cannot know which site a volume is currently led from.

### Volume routes

| Route | csi-addons | Answer on a sync cluster |
|---|---|---|
| `POST V/replication/failover?site=S&planned=` | PromoteVolume | **200** `SyncPromoteResultDTO` once the paths of the volume on S are open; its LVS takes the leadership on the first IO. **409** while the promote runs (also the call that queued it: call again), gate failed, promote refused by the table. **412** the site the volume is served from is not online and the call is not forced. **400** bad / missing site, `generation` != 0. See [Promote](#promote). |
| `POST V/replication/demote?site=S` | DemoteVolume | **204** fenced on S, also when the volume is not served on S (no-op). **409** gate failed. **500** an ANA RPC failed (nothing recorded; retry). |
| `GET V/replication/sync-status?site=S` | GetVolumeReplicationInfo, GetReplicationStatus, GetSecondaryReadiness | **200** `SyncReplicationStatusDTO`. **400** on a cluster without sync replication. |
| `GET V/replication/status?site=S` | (existing async read) | **200** the existing `ReplicationStatusDTO`, filled from the sync status (see [DTOs](#dtos)). |
| `GET V/connect[?host_nqn=]` | NodeStage (driver) | **200** the connection entries of the site the volume's LVS is led from (`lvs_active_site`, which promote / demote set), that site's triplet only; the caller passes no site. **404** (plain text) when the entries cannot be built. |
| `PUT V/` with `replication_policy_id` | Enable / DisableVolumeReplication | **204**, the policy part is a no-op. `replication_policy_id` omitted = untouched; `null` or a UUID = no-op on sync. Other fields (name, QoS, size) apply as usual. |
| `POST V/replication/failback` (body `{}`) | ResyncVolume | **204** no-op (the resync is automatic). The body stays required. |
| `POST V/replication/{start,stop,trigger,commit,cutover-proceed}` | - | **400**: direct async replication operations are refused on a sync cluster. |
| `GET /api/v2/clusters/{cluster_id}/replication/relationships/{lvol_id}` | - | **404**: a sync volume keeps its identity on both sites and has no replication relationship. |

`planned` defaults to `false`, and **`force = not planned`**: a failover call without `planned=true`
is a forced (disaster) promote. Map csi-addons' `PromoteVolume.force` to `planned = !force`.

### Consistency-group routes

A group is handled as one unit: one gate for all members, the LVS rule checked over the union of the
members' LVSs, one promote task for all.

| Route | Answer on a sync cluster |
|---|---|
| `POST G/replication/failover?site=S&planned=` | **200** `{"members": [SyncPromoteResultDTO, ...]}` once the paths of every member on S are open (each LVS takes the leadership on the first IO); otherwise as the volume route (409 / 412 / 400). |
| `POST G/replication/demote?site=S` | **204** (an empty group too); **409** gate failed. Members are fenced in order, each recorded right after its own fence: a failure leaves the members before it demoted (a retry is a no-op for them). |
| `GET G/replication/sync-status?site=S` | **200** `SyncReplicationStatusDTO`; **400** on a cluster without sync replication. |
| `GET G/replication/status?site=S` | **200** `ConsistencyGroupReplicationStatusDTO` filled from the sync status plus `member_count`. |
| `PUT G/replication` body `{"replication_policy_id": null or UUID}` | **204** no-op. The field is required (422 without it). |
| `POST G/replication/failback` (body `{}`) | **204** no-op. |

A member that cannot be resolved to a live volume refuses the whole group request with **409** and the
ids in `volumes`, before anything is fenced or queued; on demote and promote so does a member that
belongs to another cluster than the group (the status routes do not check that). The cluster and the site are judged from the group itself, so an empty group answers like a
populated one: demote 204, promote 200 with no members.

### Errors

- The sync refusals are answered with FastAPI's `detail` object:
  `{"detail": {"message": "...", ...}}` with one extra key where it applies:
  - `problems`: the gate refusals (list of strings, per LVS / distrib / node);
  - `volumes`: the volumes that block a promote (not demoted, still served on the source site) or
    group members that were not found;
  - `task_id`: the promote task of a 409 "promote in progress", or of a failed promote reported once
    (with `volumes`).
- Other errors keep their usual shapes: a failed ANA RPC is a **500**
  `{"status": "An error occured while processing the request", "detail": "..."}`; a sync precondition
  raised outside these routes' translation is a **400** `{"error": "Preconditions are not met",
  "detail": "..."}`; unknown cluster / pool / volume / group **404**; invalid parameters or bodies
  **422**; the relationship 404 has a plain string `detail`; the connect 404 is plain text.

The status codes are chosen for csi-addons: **409 is always retryable and never a reason to force**;
**412 is the only answer on which csi-addons escalates to a forced promote.** A gate refusal is never a
412, so a desynced cluster cannot enter an escalation loop.

### DTOs

How the status is computed: every online node holding an instance of an LVS is asked
(`distr_sync_replication_status`). Per distrib the answer of the HA leader counts (the worst one if
several report leadership); without a leader (an idle volume) the worst answer of the instances that
answered counts; a distrib nobody answered for is `missing` (degraded). A silent instance alone does
not degrade the status, and the unsynced page counts come from the selected answers. This is
**not** the planned gate, which requires every answering instance to report `synced` and fails on a
silent online instance: a follower
still reporting `*_unsynced` while the leader reports `synced` gives `healthy` / `peer_ready` and a
refused (409) demote or planned promote at the same time.

`SyncReplicationStatusDTO` (`GET .../sync-status`):

| Field | Type | Meaning |
|---|---|---|
| `site` | string | The site asked from. |
| `role` | `primary` \| `secondary` | `primary` when the volume is **assigned** to this site (`sync_active_site`, default the home site). A group is `primary` only when it has members and every member is `primary` there. |
| `state` | `healthy` \| `degraded` \| `resyncing` | Cluster-wide, the worst over every LVS: `healthy` = every distrib's selected answer (see above) `synced` and no catch-up running; `resyncing` = a catch-up runs (`*_syncing`, or the LVS's resync task running) and nothing is unknown / missing; `degraded` = a zone is behind (`*_unsynced`), `unknown`, or a distrib nobody answered for. |
| `last_replicated_at` | datetime \| null | Now while healthy; else the earliest known last in-sync time of an unhealthy LVS (its first open zone-desync event, else now minus the leader's lag); null when not known. |
| `lag_seconds` | int \| null | 0 while healthy; now - `last_replicated_at`; null when that is unknown. |
| `bytes_behind` | int | Sum over every LVS of unsynced pages x page size (an upper bound). |
| `diverged` | bool | `state != healthy`. |
| `completed` | bool | Every answering instance reports every distrib in replication mode `full`; false for an LVS nobody answered for. Not "catch-up complete". |
| `degraded`, `resyncing` | bool | Some LVS is in that state. |
| `peer_ready` | bool | `completed and not degraded and not resyncing`. |

Everything but `site` and `role` is **cluster-wide**: every volume and every group gets the same
answer, never a sum over group members. The status routes may be served from a cache up to 5 seconds
old (`SYNC_STATUS_CACHE_SEC`); the gates always query live.

`ReplicationStatusDTO` (`GET V/replication/status`, the existing async DTO) on a sync cluster:
`role` = `source` where `role` above is `primary`, else `secondary`; `state` = `in_sync` when healthy,
else `degraded` (a running catch-up is `degraded` with `resyncing = true`); `last_replicated_at`,
`lag_seconds` as above; `outstanding_bytes` = `bytes_behind`; the async-only counters stay 0 / null.
`ConsistencyGroupReplicationStatusDTO` is filled the same way plus `member_count`.

`SyncPromoteResultDTO`:

```json
{"lvol_id": "<uuid>", "connection_strings": [ NvmeConnectEntry, ... ]}
```

The entries are the ones `GET V/connect` returns - the led site's triplet only, serialized with
hyphenated keys: `transport`, `ip`, `port`, `nqn`, `reconnect-delay`, `ctrl-loss-tmo`,
`fast-io-fail-tmo`, `nr-io-queues`, `keep-alive-tmo`, `host-iface`, `tls`, `connect`, `ns-id`,
`allowed-hosts`, `target-lvol-id`.

### Role is not accessibility

`role` is the **assigned** site. It does not look at demotion, `lost_site` or a leadership move: a
volume demoted on its site a moment ago still answers `role: primary` (`source`) there while every path
is `inaccessible`. Neither `role` nor `peer_ready` proves that a volume is published. The success
answer of the promote (200 with connection strings) is the API's statement that the volume was opened
on the site - it is the result of that operation, not a continuous health probe of the paths. It does
not say that an instance leads the LVS: after a move the leadership is taken on the first IO (see
[Promote](#promote)).

Not observable through the API today (the operator knows its own site statically): the site of each
node, the cluster's `sync_replication` / `lost_site` / `lost_site_state`, a volume's
`sync_active_site` / `sync_demoted_sites`, the LVS's active site, whether an instance leads the LVS,
and the effective ANA state of a path.

---

## 4. Mapping to csi-addons

| csi-addons call | simplyblock |
|---|---|
| `EnableVolumeReplication` / `DisableVolumeReplication` | no-op: no call needed, or `PUT V/` with `replication_policy_id` (204) |
| `ResyncVolume` | no-op: no call needed, or `POST V/replication/failback` (204); the catch-up is automatic |
| `DemoteVolume` | `POST V/replication/demote?site=<own site>` |
| `PromoteVolume(force)` | `POST V/replication/failover?site=<own site>&planned=<!force>`; repeat while 409 "promote in progress"; 200 = the paths are open, with the connection entries |
| `GetVolumeReplicationInfo` | `GET V/replication/sync-status?site=`: `lastSyncTime` = `last_replicated_at` (now while in sync, RPO 0); `lastSyncDuration` / `lastSyncBytes` have no source: 0 |
| `GetReplicationStatus` | `GET V/replication/sync-status?site=`: `role`, `state`, `lastReplicatedAt` = `last_replicated_at`, `lagSeconds` = `lag_seconds`, `bytesBehind` = `bytes_behind`, `diverged` |
| `GetSecondaryReadiness` | `GET V/replication/sync-status?site=`: `PeerReady` = `peer_ready`, plus `completed`, `degraded`, `resyncing` |
| volume group variants | the `G/replication/*` routes |

---

## 5. Promote and demote

### Gates

- **Planned gate** (demote, planned promote): live and cluster-wide. Every answering instance of every
  LVS must report every distrib `synced` in replication mode `full`; an LVS nobody answered for, or a
  distrib missing from an answer, fails. An `ONLINE` instance that is asked and does not answer fails
  it too (the others' `synced` is provisional while the catch-up node has not confirmed it), and so
  does a running catch-up (`FN_SYNC_RESYNC`) of the LVS; an instance that is not `ONLINE` is not
  asked and does not count. There is no remote-journal condition (with both sites up the new leader levels the journal over all
  reachable copies). A single desynced LVS anywhere in the cluster blocks every planned demote and
  promote.
- **Disaster gate** (forced promote of a lost site T): only the LVSs the request moves (those led from
  T), judged from the **persisted** events of T's nodes, never a live query: no unresolved zone desync
  and no unresolved remote-journal drop recorded by a T node before the loss. A drop of a T node ends
  only with a later "journal synced" of **the same node**, or with a live in-sync answer of the JC
  leader that sbcli recorded itself while nothing else was received: the nodes' events are collected
  independently, so another node's "synced" received later may be older than the drop (a JC
  leadership handover). After such a handover the drop stays open (fail-closed) until the status is
  queried while the new JC leader reports the journal in sync. Events emitted by the
  surviving site's instances (the consequences of the loss) never count. LVSs **led from** the
  surviving site go degraded because of the loss and do not block. The home site does not decide
  this: an LVS homed on the surviving site but moved to T earlier is judged by T's events, and an LVS
  homed on T but already led from the surviving site is not judged at all.
- The promote task re-runs the gate right before it marks the move, before each LVS's leadership
  hand-off and before it opens the volumes; the state may change after the request was queued.

### Demote(volume, site S)

- Not served on S -> 204, nothing done.
- Planned gate fails -> 409 with `problems`.
- Else every path of the volume on S's triplet of its LVS is set `inaccessible` (a node whose SPDK is
  down is skipped - its restart applies the rule), and only then S is added to `sync_demoted_sites`. A
  failed ANA RPC records nothing (500).

### Promote

The promote table, for a volume whose LVS is led from site T, promoted on S:

| Situation | Answer |
|---|---|
| a promote task for the LVS is running | 409 in progress (`task_id`) |
| the last promote of this volume to S failed (reported once per volume) | 409 with its reason, `task_id` and `volumes`; the next call judges the table again |
| the LVS's move is left unsettled (`moving:*`) by a promote that ended | reconciled first; still moving: 409 (`volumes`) with the owner and marker |
| S has no online node (a move or a disaster fail-over is needed) | 409 (never 412) |
| LVS led from S and the volume served there | 200 with the connection entries (no-op) |
| LVS led from S, the volume not open there, no member of the LVS's triplet on S `ONLINE` | 409 (as "S has no online node": no path could open) |
| LVS led from S, the volume not open there (e.g. a sibling left behind by an earlier move) | ANA-only promote queued: 409 in progress, then 200 |
| T online, the volume not demoted on T | 409 (`volumes`) |
| T online, the volume demoted, another volume of the LVS still served on T | 409 with that list (`volumes`) |
| T online, every volume of the LVS demoted on T, planned gate ok | planned move of the LVS queued: 409 in progress, then 200 |
| T not online, not forced | 412 |
| T not online, forced, no node of T may still run, disaster gate ok | disaster fail-over queued: 409 in progress, then 200 |
| forced while T is online, or a node of T may still run SPDK (ONLINE / DOWN / RESTARTING / IN_CREATION) | 409 - force never acts on a live site |
| gate fails | 409 with `problems` (never 412) |

- A site is "online" when it has an `ONLINE` node and is not the cluster's lost site. That decides the
  move and the disaster rows; the ANA-only row needs an `ONLINE` member of **this LVS's** triplet on S
  (another node of S opens none of its paths).
- Opening the volume on S sets its ANA state on every `ONLINE` member of the LVS's triplet there. An
  opening that sets no path at all (the triplet went down after the call was queued) fails the promote
  task: nothing is recorded, the failure is reported to the next call, never a 200.
- While a site is recorded as lost, **every** promote is judged by the disaster gate (the planned gate
  needs both sites).
- The promote task does the whole promote in one pass and always ends DONE, **also when it failed**;
  the task being DONE is not the success condition. A failed task is reported once to each of its
  volumes: the first POST of that volume to S after the failure answers 409 with the task's reason and
  `task_id` and queues nothing. The call after that judges the table again: 200 when the volume is
  served on S, otherwise it re-queues or answers the refusal. Keep calling while the answer is 409
  "in progress"; stop on any other 409 and fix its cause.
- **A move hands the LVS off; it grants the leadership to nobody.** Per LVS the promote task fences the
  LVS port of the other connected members while it works; demotes the old leader, if one leads; stamps
  the instances on T non-leader; gives the primary of S's triplet - the taker - the role `primary`,
  creates its hublvol and connects the other members of S's triplet to it; asks the other members once
  more whether one of them leads; then writes `lvs_active_site = S` and opens the requested volumes by
  ANA. The taker gets neither `bdev_lvol_update_lvstore` nor `bdev_lvol_set_leader`, and the task waits
  for no leader.
- Which members that reaches: in a planned move a member on T that does not answer refuses the move
  (its role could not be made non-leader), before the hand-off and after it, and so does a taker that
  does not answer; another member on S that does not answer is skipped like a disconnected peer of a
  restart. In a disaster fail-over the members of the lost site T are not asked, fenced or stamped at
  all: the site steps have fenced them, and they are rebuilt as non-leaders when the site returns.
- The taker takes the leadership **on the first client IO**: its lvstore reloads the metadata from disk
  and promotes itself, and each volume reloads its blob on its own first IO - the in-place update an
  in-site failover runs. A leadership granted by RPC instead would serve every volume with the
  metadata its blob had when the instance opened it, long before the move. So the 200 means "the paths
  on S are open", never "the LVS has a leader".
- A self-promotion that fails shows on the data path only: the client's IO fails,
  `bdev_lvol_get_lvstores` on the taker keeps answering `"lvs leadership": false`, or the taker's SPDK
  aborts (a read that fails during the update) and the node is handled as any failed node. No API
  field and no event tells "open, waiting for the first IO" from "the self-promotion failed".
- A move left unsettled (`moving:*`) by a promote that ended - a member did not answer, or a member
  other than the taker reported the leadership after the hand-off - is settled by the next promote call
  or by the storage-node monitor (every 30 s at most per LVS) once every member of both triplets
  answers (the members of a lost site whose fence has run are not asked): on the site of the member
  that leads; when nobody leads, by the same hand-off to the target site's primary once more. One case
  is not settled automatically: a member of the **target** site other than its primary leads (see
  [Known limitations](#8-known-limitations)).
- A move changes the active site of the whole LVS and opens **only the volumes of the request**. The
  other volumes of that LVS stay closed on S until their own promote (the ANA-only row).
- Volume creation and clone on an LVS that is being moved (`moving:*`) are refused: the v2 create /
  clone route answers **422** with the reason (the refusal reaches the route as a message, like every
  create refusal). Retry once the move has completed; the CSI provisioner's backoff does.

### Disaster fail-over (site T lost)

A forced promote of a volume led from T, when T is not online and none of its nodes may still run:

1. the disaster gate for the LVSs of the request - before anything changes;
2. T proven down (every T node in no running state, its SPDK not seen by the management plane and gone
   from the surviving site's data plane);
3. site steps, resumable (`lost_site = T`, `lost_site_state = fencing`): every T device `unavailable` in
   every distrib on the surviving site, the T nodes `OFFLINE`, **every other volume of every LVS led from
   T** fenced on T (`sync_demoted_sites` += T; it stays closed on S until its own promote); T proven down
   again; `lost_site_state = done`. A retry in `fencing` redoes these steps idempotently; later promotes
   skip them once `done`;
4. per LVS: the hand-off to the primary of the surviving site's triplet (nothing is granted; it takes
   the leadership on the first IO, as after a planned move), then the volumes opened on S.

A cluster with a whole site lost is not suspended (it stays at least `DEGRADED`); a site that is up but
beyond its fault tolerance still suspends it.

---

## 6. Flows for the operator

**Planned switchover A -> B** (both sites up, replicas in sync):

1. stop the applications on A;
2. demote on A (`site=A`) **every** volume of every LVS involved - the application's volumes and every
   other live volume sharing their LVSs (same `storage_node_id`); a group demote covers the group;
3. promote on B (`site=B`, `planned=true`); repeat while 409 "in progress" until 200;
4. connect on B with the returned entries (or `GET V/connect`);
5. issue client IO on at least one volume of every LVS that moved, **before any object operation on
   that LVS** (see below): mounting an existing filesystem does; for a volume without one, read from
   the block device;
6. start the applications on B.

**Disaster fail-over** (A lost): promote on B with `planned=true` answers 412; csi-addons escalates to a
forced promote (`planned=false`); repeat while 409 "in progress" until 200; connect on B and issue
client IO as in steps 4-5 above.

**First IO before object operations.** A promote that moved an LVS (planned or forced) leaves it
without a leader until a client's first IO reaches the new site. Connect a volume of that LVS and
issue IO on it before anything that needs the LVS's leader: creating, cloning or deleting a volume,
taking a snapshot of a volume or of a group. Such an operation on an LVS nobody leads does not wait
for the IO - it goes through the control plane's leaderless recovery, which grants the leadership by
RPC and may leave the volumes with stale metadata (see [Known limitations](#8-known-limitations)).

IO on **one** volume gives the whole LVS its leader; that is what keeps the operations above away from
the recovery. It reloads the metadata of that one volume only. Every other volume of the LVS reloads
its own on its own first IO: until then its used size is stale, and a resize of it works on metadata
that was not reloaded. Issue IO on a volume itself before resizing it.

**Node restart** (nothing to call):

- a member of the active triplet restarts as in a cluster without sync replication, whichever site
  that triplet is on. Its primary rebuilds the LVS as the leader: it takes the leadership back from a
  secondary that took over, and it gives a leader to an LVS that still waited for its first IO. Its
  secondary rebuilds the LVS as the leader only when the primary is disconnected, and as a non-leader
  otherwise. On an active remote triplet that is checked a second time, once the rebuild holds the
  locks of the LVS: a takeover whose primary came back while the restart waited for them is refused,
  the restart fails and the restart task retries it - then as a non-leader behind that primary. Its
  tertiary comes back as a non-leader - with one exception taken over from the restart of a cluster
  without sync replication: the tertiary of an active **home** triplet whose primary and secondary
  are both disconnected is not rebuilt at that restart; the node comes online without this instance.
  The tertiary of an active remote triplet is rebuilt in that case too;
- a member of the passive triplet, every member while the LVS is being moved, and every node of a lost
  site come back as non-leaders;
- "nobody leads" blocks no restart: a non-leader is then rebuilt behind the first connected member of
  the active triplet, its primary first. A non-leader rebuild is refused, and retried by the restart
  task, only while a connected member of the active triplet does not answer whether it leads, or more
  than one says it does.

**Site return** (A comes back, automatic - nothing to call):

- A's nodes restart as non-leaders: an LVS already moved to B is rebuilt behind the member of B's
  triplet that leads it - behind the first connected member of that triplet, its primary first, when
  nobody leads it yet (no client IO since the promote); an LVS still led from A (never promoted, not in
  sync at the loss, or empty) is rebuilt leaderless.
- When every A node is ONLINE with its zone up, the storage-node monitor elects a leader on A for each
  LVS still led from A (on its active triplet's primary, after a full verdict of both triplets and a
  local journal quorum), checks every expected volume on it and every distrib replayed, schedules the
  catch-up of every LVS and clears `lost_site`. A degraded surviving site delays the election (every
  member must answer). Events: "Site A returned: <lvs> leader elected ...", "Site A return held: ...".
- `lost_site` is cleared **before** the catch-up finishes. Volumes fenced on A stay closed there until
  promoted.

**Fail-back B -> A**: wait until `sync-status` is `healthy` / `peer_ready`, then run the planned
switchover in reverse (demote on B, promote on A). `POST .../failback` is not part of it (a no-op).

**Resync, both directions**: a zone desync event (either zone: the home site's zone 0 or the replica
zone) schedules one `FN_SYNC_RESYNC` task per LVS. It waits while a site is lost or the lagging zone's
devices are not online, then starts the catch-up on the leader of the active triplet and polls it; a
non-converging run is re-run with backoff (30 s doubling, at most 600 s) and raises one alert after 5
such runs. It also waits while nobody leads the LVS (after a promote, until the first client IO): the
runner never makes a leader. A site return schedules the catch-up of every LVS.

---

## 7. CLI

| Command | Purpose |
|---|---|
| `sbctl cluster create ... --sync-replication` / `cluster add ... --sync-replication` | create a sync-replication cluster |
| `sbctl sn add-node ... --site <name>` | add a node to a site |
| `sbctl cluster sync-status <cluster_id> [--json]` | the aggregate status and one row per LVS (state, worst status, mode, bytes behind, journal, gate problems) |
| `sbctl volume sync-status <volume_id> --site <S> [--json]` | a volume's role on S and the cluster-wide status |
| `sbctl volume sync-demote <volume_id> --site <S>` | demote on S |
| `sbctl volume sync-promote <volume_id> --site <S> [--force]` | promote on S; prints "in progress" while the task runs (run it again), then the connect commands |
| `sbctl volume connect <volume_id> [--site <S>]` | connect commands for the volume's led site (or site S's triplet when given) |

On a cluster without sync replication these commands fail with an explicit error; the async
replication commands (`volume replication-start` / `-stop` / `-trigger` / ...) fail on a sync cluster.

---

## 8. Known limitations

- A network partition in which the management plane loses site A at the same moment may pass the
  disaster gate on stale data; a full partition where the management plane and every surviving peer
  lose A while A keeps serving its own clients passes the liveness checks (research item).
- Reads after a promote go to zone 0 while the home site is alive (local-zone reads are deferred).
- Rejected on a sync cluster: lvol migration (single and batch), manual volume suspend / resume, the
  v1 `lvol/connect` (use the v2 connect).
- Only the direct async replication operations are refused; the policy paths (creating a volume with a
  replication policy, `volume replication-policy-set`, a group policy attach) still configure async
  replication on a sync cluster.
- A completed promote builds its connection entries without a host NQN, so a volume with
  `allowed_hosts` answers 409 instead of its result.
- A follower whose hublvol redirect is known broken stays closed (fail-safe) until a re-activation or
  restart wires it (nothing reconnects it in between); a transient disconnect during a reconnect closes
  a standby path for one monitor cycle. An unknown redirect state changes nothing (see the ANA rule).
- Activation of a sync cluster fails when an LVS led from its remote triplet cannot be wired; an LVS
  whose leadership is moving refuses re-activation until the move is settled.
- The API does not expose sites, `lost_site` or per-volume site records (see
  [Role is not accessibility](#role-is-not-accessibility)).

Between a promote and the first client IO (the LVS has no leader, see
[The active and the passive triplet](#the-active-and-the-passive-triplet)):

- An object operation that needs the LVS's leader (creating, cloning or deleting a volume, taking a
  snapshot of a volume or a group) goes through the leaderless recovery the sync code shares with
  every other cluster: it grants the leadership by RPC after `bdev_lvol_update_lvstore`, which does not
  reload the blobs the instance holds open, so the volumes may be served with stale metadata (a
  promoted volume can come up empty). Issue client IO first (see
  [Flows](#6-flows-for-the-operator)).
- Scheduled snapshots (per-volume interval and group) of such an LVS are deferred until it has a
  leader. A window remains: a promote that completes after the timer's check and before the
  snapshot's own leader lookup (up to the sync-delete wait inside `snapshot_controller.add`) still
  reaches that recovery.
- Three other background paths reach that recovery without an operator action and are not guarded:
  the cleanup of a volume record left `in_creation` for more than 10 minutes (`lvol_monitor`), a
  scheduled backup of a volume of the LVS (a backup policy with a schedule), and the landing volume of
  an async snapshot replication whose destination is the LVS.
- The catch-up of the LVS waits for the first IO; while an instance of it still reports a zone behind,
  the planned gate stays closed for the whole cluster.
- The first IO on one volume gives the LVS its leader, but each volume reloads its blob on its **own**
  first IO: until then its `num_allocated_clusters` (used size) is stale, and a `bdev_lvol_resize` of a
  volume that has seen no IO since the promote works on a blob that was not reloaded - also when the
  LVS has a leader already.
- Nothing in the API tells "open, waiting for the first IO" from "the self-promotion failed" (see
  [Promote](#promote)).

Leadership inside the triplets:

- A non-leader rebuild (node restart, node removal, expansion) is refused and retried while a
  connected member of the active triplet does not answer whether it leads - an RPC error, or no
  lvstore on it yet. Two non-leading members of one active triplet that restart at the same moment
  therefore refuse each other once; and while the instance of a member is still being built after a
  node removal, the restart of another member of that triplet is refused.
- A self-promotion that was armed on the previous taker by IO in flight and completes after the
  hand-off's last check leaves that member - now in the passive triplet - reporting the leadership,
  at first as the only one. Once the new taker promotes itself on its first IO, two members report it.
  A restart never asks the passive triplet, so it is not detected there; later moves of the LVS
  refuse. The way out is to restart the stale member.
- A promote whose hand-off ends with a member other than the taker reporting the leadership (such a
  self-promotion completing during the hand-off, or an old leader whose demote did not take) fails,
  and the member the LVS was handed to keeps the role `primary` without leading, until the next move
  of the LVS or its own restart. When the member that leads is on the other site, the move is settled
  there and the abandoned member's paths stay closed (passive site). When it is another member of the
  triplet the LVS was being moved **to**, the move is not settled: the LVS stays `moving:*`, both
  sites stay closed and a promote answers 409 with the owner and the marker, until an operator
  restarts the member that leads (it is rebuilt as a non-leader while the LVS is moving); the move is
  then finished on that triplet's primary by the next promote call or by the storage-node monitor.
  Settling it earlier would open the primary - not redirected to that leader - as the optimized path.
- A secondary that takes over at its restart never asks the primary it takes over from: that primary
  is not fenced, probed or demoted by the rebuild. Its connectivity is judged when the restart chooses
  the takeover - on an active remote triplet once more, by a fresh check, when the rebuild holds the
  locks of the LVS; on the home triplet only the first time, as in a cluster without sync replication.
  A primary that reconnects after that, while the rebuild of its secondary still runs, is not noticed:
  the secondary is granted the leadership next to it.
- After a network outage inside an active **remote** triplet (no node restarted) the leadership is not
  handed back to its primary: a secondary that took over by IO keeps leading until the primary or
  that secondary restarts. The port-allow failback handles the home triplet only.
- Neither the health service nor the storage-node monitor watches the client port of an LVS on a
  member of its remote triplet: a port left blocked there is neither reported nor reopened by them.
