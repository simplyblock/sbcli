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
promote, the site record and the site activation flag), `simplyblock_core/services/tasks_runner_sync_promote.py`,
`simplyblock_core/services/tasks_runner_sync_resync.py`, the site return in
`simplyblock_core/services/storage_node_monitor.py` (the site gate) and `simplyblock_core/cluster_ops.py`
(`cluster_activate(..., site=S)`, the site activation), the ANA site rule in
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
- The cluster records one site at a time as out (`Cluster.lost_site`, `lost_site_state`), in four
  states. Two are written by the storage-node monitor, with no fail-over and nothing fenced: `draining`
  (the site is beyond its fault tolerance while the other one is healthy; what is left of it is being
  shut down) and `down` (the site was judged gone as a whole, or its drain completed; it may return -
  nodes coming back do not change it). Two are written by a disaster fail-over (a forced promote):
  `fencing` while its site steps run, `done` after. Only the site activation clears a record; any
  suspension of the cluster also clears the monitor's two (see
  [Disaster fail-over](#disaster-fail-over-site-t-lost)).
- While a recorded site is being activated (see [Site return](#6-flows-for-the-operator)), the cluster
  carries an **activation flag** beside the record (`Cluster.site_activation_site`,
  `site_activation_since`, `site_activation_heartbeat`): the site, the start time - which is the
  activation's token - and a heartbeat stamped about every 60 s. It is the site's counterpart of the
  cluster status `IN_ACTIVATION`; the stored cluster status is left as it is (normally `DEGRADED`, or
  `READONLY` when set so). Only the
  activation that set it clears it, when it ends in any way; the storage-node monitor's watchdog clears
  it when its driver is gone (a heartbeat older than 300 s, or the node-scaled activation budget passed
  with no activation thread running in the monitor). A suspension never clears it: the activation stops
  at its next step and clears it itself. The flag and the record are separate: the activation clears the
  record before its last step and the flag after it.

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
- Specific to sync replication are only the promote and the demote, the passive triplet, the
  activation of one returning site (the cluster activation narrowed to it), and the catch-up runner.

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

The prototype assumes **one management node that reaches both sites**: a site the management node
cannot reach is taken as unreachable for everybody. The management plane is not replicated by this
feature. The management node must be able to reach FoundationDB; the monitoring and the disaster
fail-over need both. The two-site rule applies to the storage topology only.

The management node judges a site from two signals: its own probes (node API, SPDK RPC) are the
evidence that a node is gone, and the surviving site's data plane can only veto - a node that a peer
still reports with its journal (JM) link `enabled` is not gone. A peer without that link, a peer that
does not answer and a vote with no voter (`0/0`) veto nothing.

Limitations of the assumption:

- **Loss of the management node** (or of its FoundationDB): no monitoring, no site record, no
  disaster fail-over. The data plane keeps serving what it served.
- **No quorum of its own, no witness, no self-fencing**: a site cut off from the management node and
  from the other site does not stop serving by itself.
- **A full partition** in which the management node and the other site both lose a site that is still
  alive and serving its own clients is outside the assumption: the site looks gone to every witness, a
  forced promote passes and both sites serve.
- **A management-only partition** (the storage network intact): the nodes the management node cannot
  reach turn `UNREACHABLE` and are vetoed by their peers, so they count neither as lost nor as
  affected - on a cluster without `strict_node_anti_affinity` and otherwise healthy, a whole site cut
  off this way gets no record, and no forced promote passes. (With `strict_node_anti_affinity` the
  site's verdict also counts its `ONLINE` nodes, so the site can be recorded `draining`.) A site already
  beyond its fault tolerance for other reasons (nodes `OFFLINE`, failed devices) stays `draining` while
  such a vetoed node is part of it, until the node is reached again (it is then shut down) or the veto
  clears.
- **A management node cut off from both sites** judges nothing: every node turns `UNREACHABLE`, every
  vote is `0/0`, so the veto rule leaves no healthy site and the verdict falls back to the cluster-wide
  majority rule, in which `0/0` never counts, and no site record is proposed. On a cluster without
  `strict_node_anti_affinity`, with the devices still online, the verdict counts none of those nodes
  (it is what the devices and the nodes already `OFFLINE` give, not a frozen earlier status) and
  nothing is drained; with `strict_node_anti_affinity` the missing `ONLINE` nodes can suspend the
  cluster, and its suspend recovery drains it.

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
  `StorageNodeMonitor` judges the sites, writes the monitor's site record, drives the site drain and
  starts the site activation (in a thread of its own process); both must run as today.

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

- First placement: a planned promote first demotes T itself for the LVSs whose only blockers are volumes
  that were never connected - a volume is created with `sync_never_connected` set, and the first
  `connect` (or the connection entries a promote answers with) clears it. When every volume of an LVS
  still served on T was never connected, all of them are demoted on T (under the planned gate), as an
  unstage would have done, and the table is judged again; a volume that was ever connected (or whose
  record does not carry the flag) keeps its 409. The demote re-checks the flag under the LVS's site-rule
  lock, under which `connect` clears it before it hands out any path. An LVS a running site activation
  holds is never demoted this way: such a promote is refused with "site S is being activated; retry".
- A site is "online" when it has an `ONLINE` node and is not the cluster's lost site. That decides the
  move and the disaster rows; the ANA-only row needs an `ONLINE` member of **this LVS's** triplet on S
  (another node of S opens none of its paths).
- Opening the volume on S sets its ANA state on every `ONLINE` member of the LVS's triplet there. An
  opening that sets no path at all (the triplet went down after the call was queued) fails the promote
  task: nothing is recorded, the failure is reported to the next call, never a 200.
- While a site is recorded as lost - by a fail-over or by the storage-node monitor (`draining` /
  `down`) - **every** promote is judged by the disaster gate (the planned gate needs both sites). Only
  the forced promote of a disaster row fences the site; any other promote leaves the record as it is.
  The promote task re-reads the record before it opens a volume and refuses to open one on a site
  recorded since it was queued.
- While a recorded site T is being activated (the activation flag names T), the disaster gate of T
  fails with "site T is being activated; retry" - before the site steps, before the move, before each
  LVS's hand-off and before the opening - so no promote hands off an LVS whose leader the activation is
  building. The failed task's reason is answered by the next call, as for any failed promote; call
  again once the activation has ended (the site has returned, or the activation failed and the site is
  still recorded).
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
  all: the site steps have fenced them, and the site activation rebuilds them as non-leaders when the
  site returns.
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
2. T proven down, per node of T that is not removed: its status is not a running one (not `ONLINE` /
   `DOWN` / `RESTARTING` / `IN_CREATION`); the management plane gets no answer from its SPDK - the
   node API does not report the process up and its RPC returns no version (a call through the RPC
   proxy of a dead SPDK that merely returns is no answer); and no answering peer of the surviving site
   still reports its JM link `enabled` (a fresh vote; a peer without the link, a peer that does not
   answer and `0/0` do not block). A refusal names, per node, the status, the probe that answered or
   the count of peers still reporting the link;
3. site steps, resumable, **for a forced promote of a disaster row only**: entered from no record, from
   the monitor's `down` or from `fencing`; refused while the monitor still drains T (`draining`: "site
   T is being shut down by the monitor; retry"). `lost_site = T`, `lost_site_state = fencing`: every T
   device `unavailable` in every distrib on the surviving site, the T nodes `OFFLINE`, **every other
   volume of every LVS led from T** fenced on T (`sync_demoted_sites` += T; it stays closed on S until
   its own promote); T proven down again; `lost_site_state = done`. A retry in `fencing` redoes these
   steps idempotently; later promotes skip them once `done`;
4. per LVS: the hand-off to the primary of the surviving site's triplet (nothing is granted; it takes
   the leadership on the first IO, as after a planned move), then the volumes opened on S.

The task does not wait for the proof: a refused proof fails the task, its reason is reported by the
next call and only the call after that judges the table again (see [Promote](#promote)).

When T comes back, it returns by the same path as after a site loss without a fail-over: its nodes
restart without lvstores and the site activation rebuilds them, the `fencing` / `done` record cleared by
it (see [Site return](#6-flows-for-the-operator)). The volumes fenced on T stay closed there until they
are promoted back.

**The cluster status of a sync cluster** is judged per site:

- a site beyond its fault tolerance while the other one is healthy is shut down as a whole - the
  storage-node monitor records it `draining`, shuts down its live nodes (operator-stopped nodes are
  left alone), pauses the automatic restarts of that site only and records it `down` once nothing of it
  is pending; a site gone as a whole is recorded `down` directly. The verdict is `DEGRADED` (or
  worse by the healthy site's own verdict, never `SUSPENDED`; a cluster set `READONLY` stays
  `READONLY`), and nothing of the healthy site is touched;
- `SUSPENDED` only when no site is healthy - both sites beyond their fault tolerance, or a recorded
  site while the other one is beyond its own. The cluster-wide suspend recovery then runs as on a
  cluster without sync replication;
- a suspension clears the monitor's record (`draining` / `down`): the monitor clears it before it
  writes `SUSPENDED` itself; a suspension written elsewhere (`sbctl cluster restart`, any other) is
  cleared by the next monitor pass. The record of a fail-over (`fencing` / `done`) stays.

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
client IO as in steps 4-5 above. A forced promote soon after the loss can be refused because the site
is not proven lost yet: a peer still reports a JM link `enabled` (in the lab, with the SPDK processes
killed, that ended about 6 s after the loss; for a host that goes silent it is the keep-alive time,
not measured), a node of A is still in a running state, or the monitor still drains A. The failed
task's reason is answered by the next call; call again once the cause has cleared. Any other refusal:
stop and fix its cause, as the contract says.

**Site loss without a fail-over** (nothing to call): when a site goes beyond its fault tolerance - or
is lost as a whole - while the other one is healthy, the storage-node monitor records it, shuts down
what is left of it (`draining`; its automatic restarts paused, operator-stopped nodes left alone and
restarted only by the operator) and then records it `down`; the automatic restarts of its nodes
resume. The cluster is `DEGRADED` (or stays `READONLY` if it was set so), the healthy site is not
touched and keeps serving its volumes.
**Recording a site moves nothing**: an LVS led from the recorded site stays led from it, and its
volumes stay closed until the site activation (see Site return below) opens them or an operator fails
them over with a forced promote (which fences the site, `down -> fencing -> done`). No volume is fenced
by the record itself.

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
- a member of the passive triplet and every member while the LVS is being moved come back as
  non-leaders;
- a node of a recorded site (any record state) comes back `ONLINE` **without lvstores**, as every node
  of a cluster that serves nothing does: its devices, its own journal (JM) and its links to the peers
  are brought up, no LVS instance is built. The site activation builds them once the site is back (see
  Site return below). While the site is `draining` its automatic restarts are paused;
- while a site activation runs, no node of either site restarts: a manual restart is refused ("site A
  is being activated; retry") and the automatic restart tasks wait for it to end;
- "nobody leads" blocks no restart: a non-leader is then rebuilt behind the first connected member of
  the active triplet, its primary first. A non-leader rebuild is refused, and retried by the restart
  task, only while a connected member of the active triplet does not answer whether it leads, or more
  than one says it does.

**Site return** (A comes back, automatic - nothing to call), after a site loss without a fail-over
(`down`) or after a fail-over (`done`, or `fencing` left by a promote that no longer runs); a site still
`draining` never returns. One path for all three: the bring-up of a cluster without sync replication
(its nodes restart without lvstores, then the cluster activation builds every lvstore with every journal
copy in place), applied to site A while site B keeps serving.

1. **A's nodes restart without lvstores** (see Node restart above) and turn `ONLINE` one by one, empty
   until the site activation builds their instances. Until the record is cleared, every reader that asks
   the members of a triplet whether they lead treats such a node as absent - as if `OFFLINE` - and the
   background repairs build no volume path on it; publication follows the
   [ANA rule](#ana-rule-who-publishes-which-path), which keeps a recorded site closed. Restarting it with its LVS instances would not work: a site-aware journal counts only its local
   journal copies and needs two of them, so the first node of A to come back would be aborted by its
   own data plane.
2. **The site gate**, judged on every storage-node monitor pass (a closed gate is logged as
   `can not activate site A: <reason>`):
   - the cluster status is `ACTIVE`, `DEGRADED` or `READONLY`, and no promote task (`FN_SYNC_PROMOTE`)
     of the cluster is open;
   - no node of either site is `IN_SHUTDOWN`, `IN_CREATION` or `RESTARTING`, and no `ONLINE` node of
     either site has an active restart task (an `OFFLINE` node of B with its restart queued does not
     hold A back);
   - the activation gate of a cluster without sync replication over A's nodes: none `UNREACHABLE`,
     every one `ONLINE` for at least 30 s, except up to the cluster's fault tolerance (FTT) of nodes an
     operator shut down;
   - at least two of A's nodes `ONLINE` - the two local journal copies the data plane needs.
3. **The site activation** - the cluster activation narrowed to A, in a background thread of the
   storage-node monitor (B stays monitored meanwhile). It sets the activation flag, waits until every
   `ONLINE` node is connected to every usable device of the other `ONLINE` nodes (repairing the missing
   device links, and the journal links with them; it fails when the repair stops making progress), and
   then:
   - builds the leader of each LVS led from A on the primary of its active triplet (examined from disk,
     then granted) - only where that primary is `ONLINE` (see [Known limitations](#8-known-limitations));
   - builds A's other instances of those LVS behind that leader, and rebuilds A's instances of every
     LVS led from B behind B's serving leader, and of every LVS whose leadership is moving behind a
     member of its triplet on B, one LVS at a time, the way a node restart rebuilds a non-leader;
   - wires the hublvols of the LVS led from A, then checks and repairs the journal mesh best effort
     (an incomplete mesh is logged, the activation goes on);
   - schedules the catch-up of every LVS and clears the record - only while it still holds the flag,
     the cluster still serves, A still passes the gate's node rules and A's `ONLINE` nodes are exactly
     the ones it built on;
   - opens the paths of the LVS led from A by the [ANA rule](#ana-rule-who-publishes-which-path), emits
     the event "Site A returned" and clears the flag.

   It writes no cluster status: the stored status stays as it is while A is recorded (normally
   `DEGRADED`, or `READONLY` when set so), and the monitor's verdict moves it on its own passes once the
   record is gone.
4. **What waits while it runs** - what `IN_ACTIVATION` holds on a cluster without sync replication, as
   far as it touches A: the restart of any node (see Node restart above), node add and removal, cluster
   expansion, a cluster activation run by an operator, the device / new-device / failed-device
   migrations, the catch-up runner, the promotes judged by A's disaster gate (see [Promote](#promote)),
   and the object operations on an LVS led from A or whose leadership is moving - volume create,
   resize, clone and delete, snapshot create (of a volume or a group), clone and delete are refused
   with "site A is being activated; retry"; the backup, backup-merge, journal-compaction and
   sync-delete tasks of such an LVS wait. The same operations on an LVS led from B, B's volumes and
   the monitors go on: such an operation also reaches a node of A whose instance of that LVS the site
   activation has already rebuilt, and a group snapshot of such an LVS is refused with "retry" while
   one of its nodes on A is being rebuilt.
   A suspension during the activation (any, `sbctl cluster restart` included) stops it at its next
   step; the monitor's suspend handling - the drain and the re-activation - waits until it has stopped.
5. **When it fails** before the record step (any error, a lost flag, a suspension), the flag is cleared,
   the record kept - except that a suspension clears the monitor's record (`down`) by itself, as
   always - and the instances already built stay as a failed cluster activation leaves them; the next
   monitor pass judges the gate again. Once the record is cleared it is not restored: a cancellation at
   the following step skips the opening of the paths, and an ANA failure while opening them is logged
   and the event is still emitted. While the cluster serves, the lvol monitor repairs the eligible paths
   on its later passes; under `SUSPENDED` they reopen only with the cluster's recovery.

`lost_site` is cleared **before** the catch-up finishes. Volumes fenced on A by a fail-over stay closed
there until promoted; after `down` no volume was fenced (`sync_demoted_sites` is untouched). A volume
assigned to another site or demoted on A stays closed.

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

- Everything below the [one-management-node assumption](#control-plane) is judged by that one node:
  its loss stops monitoring and fail-over, and a full partition in which it and the surviving site lose
  a site that keeps serving its own clients passes the liveness proof and the disaster gate (the gate
  then judges stale events) - both sites serve. There is no witness and no self-fencing.
- A recorded site returns by the activation gate of a cluster without sync replication (see
  [Site return](#6-flows-for-the-operator)): every node of it `ONLINE`, except up to FTT nodes an
  operator shut down, and at least two of them `ONLINE` (the journal floor). A node that never comes
  back and was not shut down by an operator keeps the site recorded and the LVSs led from it closed.
  Ways out: remove the dead node, or shut down the returned nodes and fail the site over with a forced
  promote.
- An LVS led from the returning site whose active primary is not `ONLINE` (an operator-stopped node the
  gate allows) gets no leader from the site activation: its other instances on the site are rebuilt as
  non-leaders, and the activation's final step does not open its paths - as a cluster activation does
  not open the paths of an LVS whose owner is stopped. Restart the stopped primary: its restart rebuilds
  the LVS as the leader.
- While a site activation runs, the operations listed in [Site return](#6-flows-for-the-operator) wait
  for it: an object operation on an LVS led from the activating site is refused with "retry", a node
  restart is refused or waits, a promote of that site fails its gate. The activation of a large site
  can take minutes; its connectivity repair extends its wait while it makes progress.
- The site activation clears the record before it opens the paths: an ANA failure while opening them,
  or a suspension right after the record step, leaves volumes closed with the site no longer recorded
  - the lvol monitor reopens them on its later passes while the cluster serves (see
  [Site return](#6-flows-for-the-operator)).
- A suspended cluster with a fail-over's record (`fencing` / `done`) has no automatic way back: the
  re-activation of the suspended cluster refuses to build a leader on the recorded site (below), and the
  site activation needs a serving cluster. The monitor's own records do not outlive a suspension.
- A forced promote soon after a loss can be refused while the proof cannot pass yet (a peer still
  reports a JM link `enabled`, a node still in a running state, the site still `draining`); call again
  once the cause has cleared (see [Flows](#6-flows-for-the-operator)).
- A cluster activation run by an operator while a site is recorded refuses to build a leader on a node
  of that site, as for a fenced site: an activation of a cluster with an LVS still led from the
  recorded site fails until the site returns. Only the site activation, holding the flag, builds those
  leaders; an operator's activation is refused outright while a site activation runs.
- A `SCHEDULABLE` node of a `draining` site is waited for, not shut down (the same rule as the
  cluster-wide suspend drain): a node whose RPC recovers while its SPDK keeps running stays
  `SCHEDULABLE`, and the site stays `draining` with its restarts paused, until the node is restarted
  or shut down by hand.
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
  whose leadership is moving refuses re-activation until the move is settled (a site activation
  rebuilds its instances on the returning site as non-leaders instead).
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
