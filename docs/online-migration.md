# Online cross-cluster migration: bulk plans, failover-safe cutover, forecast

Online migration moves a volume to another cluster. It uses snapshot replication
in `migration` mode and ends with a short IO-freeze cutover
(`replication-commit`). The per-volume pipeline is on main:

- `lvol_controller.replication_commit`
- `services/tasks_runner_replication_final.py`
- `services/replication_final_step.py`

This document closes the gaps that remained in it.

| # | Gap | Before | After |
|---|-----|--------|-------|
| G1 | Bulk selection | Every volume needed its own `replication-start` and `replication-commit`. | One **migration plan** covers a cluster, a pool, or a list of volumes. |
| G2 | Node loss at cutover | The freeze ran only on the source primary recorded at commit. If the source primary was offline, its peers were not fenced at all, which left the volume writable on two sides. | The freeze runs on the source LVS **leader**. Every reachable source member is fenced. The target still has to be its leading primary, see *Target* below. |
| G3 | Forecast | None. | Each volume gets bytes remaining and an ETA. The plan gets an ETA from per-source-LVS queues. A dry run shows the forecast before anything starts. |
| G4 | Delete source | `--delete-source` per commit. | Unchanged, and also carried by the plan. |
| G5 | Target placement | A weighted-random pick by subsystem count. It ignored size and the other volumes already headed to the same node. | Deterministic least-loaded placement that counts the replicas already assigned to each node. |

## G1: migration plans

### Model

`MigrationPlan` lives in `simplyblock_core/models/migration_plan.py` and is
stored in FDB.

| Field | Meaning |
|---|---|
| `uuid`, `cluster_id` | The plan, and the source cluster |
| `target_cluster_id` | The destination cluster |
| `pool_id` | Optional source-pool filter used at selection |
| `delete_source` | Passed to every commit |
| `interval_min` | Replication cadence for volumes this plan starts |
| `status` | `running`, `done`, `partial`, `failed`, or `canceled` |
| `volumes` | One entry per volume: `lvol_id`, `name`, `size`, `used_bytes`, `target_node_id`, `state`, `task_id`, `error` |
| `created_at`, `finished_at` | Epoch seconds |

These are the per-volume states:

- `skipped`: never started. `error` holds the reason.
- `replicating`: the initial copy or catch-up is running.
- `queued`: caught up, and waiting for the source LVS cutover slot.
- `converging`: shrink rounds.
- `cutting_over`: freeze and ANA flip.
- `done`
- `failed`
- `canceled`

### Selection (`plan_volumes`)

These volumes of the source cluster are candidates, optionally restricted to
`pool_id` or an explicit `lvol_ids` list. A volume is **skipped** with a reason
in these cases:

- It is in deletion or deleted.
- It replicates in `failover` mode, or to another cluster. A plan never rewires
  a DR relationship.
- It already has a completed or running cutover.
- It belongs to a consistency group whose other members are not all selected.
  A group cuts over as a group, and the runner joins siblings to the owner's
  LVS claim.

### Start (`start_plan`)

1. Select the volumes. Order them largest first, so the big initial copies
   start first and set the critical path.
2. Volumes that do not replicate yet get
   `replication_start(mode="migration", replication_cluster_id=target)`.
   Placement follows G5.
3. Queue `replication_commit(lvol_id, delete_source, plan_id=…,
   require_baseline=True)` for every volume **at once**. The runner already
   waits until replication lag is under `REPL_CUTOVER_ENDGAME_LAG_SEC` before
   it takes any snapshot. It then serializes the endgame per source LVS, so
   queueing every volume costs nothing while they catch up.
4. Persist the plan.

`require_baseline` is new. The runner treated "no replicated snapshot yet" as
"caught up". That is right for a fail-back pair, but for a fresh volume it held
the LVS claim through the whole initial copy, and every other volume on that
LVS waited behind it. With `require_baseline` the task defers until at least
one snapshot has replicated. The flag is not counted as a retry and it moves the
deadline.

### Status, cancel, and list

- **`plan_status`** reads each volume's commit task and replication info. It
  derives the state and computes the forecast (G3). It persists the aggregate
  status once every volume is terminal.
- **`cancel_plan`** cancels commit tasks that have not prepared their target
  clone (no `tgt_lvol_composite`). A cutover that is already freezing is left
  to finish. Volumes the plan started keep replicating, so the operator can
  either stop them or commit again.

### Surfaces

CLI, in the `cluster` group:

```
sbctl cluster migrate <source-cluster-id> --target-cluster-id <id>
        [--pool <pool-id>] [--lvols <id,id,...>] [--delete-source]
        [--interval-min N] [--dry-run]
sbctl cluster migrate-status <plan-id> [--json]
sbctl cluster migrate-list <cluster-id>
sbctl cluster migrate-cancel <plan-id>
```

REST API, under `/api/v2/clusters/{cluster_id}/migration-plans`:

| Method | Path | Body or result |
|---|---|---|
| POST | `/` | Body: `{target_cluster_id, pool_id?, lvol_ids?, delete_source?, interval_min?, dry_run?}`. Returns `201` with the `MigrationPlanDTO`. A dry run returns `200` with the DTO and `id: null`. |
| GET | `/` | A list of `MigrationPlanDTO` |
| GET | `/{plan_id}` | `MigrationPlanDTO`, with live state and forecast |
| DELETE | `/{plan_id}` | Cancel. Returns `204`. |

`MigrationPlanDTO` carries:

- `id`, `status`, `target_cluster_id`, `delete_source`
- `volumes[]`, each with:
  - `lvol_id`, `name`, `state`, `target_node_id`, `error`
  - `bytes_total`, `bytes_remaining`, `eta_seconds`
- `forecast`, with:
  - `bytes_remaining`, `eta_seconds`, `completes_at`
  - `throughput_bytes_per_sec`, `throughput_measured`

## G2: node loss during the cutover

Ordinary snapshot replication already survives node loss in both directions,
see `docs/replication-node-failover.md`. It sends from any online source member
and receives on the settled target leader. This section covers the endgame.

### Source

The freeze has to run on the member that serves the volume's IO, which is the
LVS **leader**. Freezing a non-leader stops nothing. The intra-cluster
migration runner already runs its final step from a replica in this case
(`active_source_node_id`).

`_cutover_source(lvol)` checks the members in role order (primary, secondary,
tertiary). It returns the first one that is online and reports `lvs leadership`
for the volume's lvstore (`lvol_controller.is_node_leader`). If no member
qualifies, the task defers without counting a retry. The cutover is pending and
the source still serves IO, so waiting is safe.

| Step | Before | After |
|---|---|---|
| Hub attach, final step | Ran on the source primary | Run on `_cutover_source` |
| Fence | Skipped entirely when the primary was offline, which left the peers optimized and the volume writable on two sides | Fences every reachable member of the source LVS, whatever the primary's status. Peers first, the executing leader last. |
| Restore on failure | — | The executing leader becomes optimized, and the other reachable members become non-optimized. |
| Shrink snapshots | — | Unchanged. `snapshot_controller.add` already routes to the LVS leader. |

### Target

The final delta can only be received by the target **primary**, and only while
it is the settled leader of the target lvstore. If the primary is down, or
another member leads, the task defers and is not counted. The source keeps
serving meanwhile, so waiting has no client impact.

The reason is in SPDK. The delta goes through the receiving node's transfer
hublvol. SPDK demuxes hub writes by map id in the **hub's own** lvstore
(`vbdev_lvol.c`: `lvol->lvol_store->lvol_map.lvol[offset >> 48]`). A node
creates its transfer hublvol on `self.lvstore`, the lvstore that node is
primary of. A secondary or tertiary member of the target lvstore therefore
holds a hub on a *different* lvstore, and the map id would resolve in the wrong
lvstore.

A target-side fallback needs a transfer hublvol per lvstore on every member.
That is out of scope here.

> **Open issue, already on main:** snapshot replication (#1441,
> `snapshot_replication.process_snap_replicate_start`) sends to the settled
> target leader through that leader's own transfer hub. When the leader is not
> the target primary, this has the same wrong-lvstore demux. It needs a bed
> check and a fix: either gate on the primary, or add a per-lvstore hub.

A node lost **during** the freeze makes `bdev_lvol_transfer_final_step` fail,
or return something other than `Done`. The source paths are restored, so the
client resumes on the source. The next attempt resolves both sides again. The
final step can be repeated: it rewrites the delta against the same base
snapshot into the same clone.

## G3: forecast

All numbers come from data that is already recorded. Nothing is probed.

**Bytes remaining per volume:**

- A volume that has not replicated a snapshot yet still has its whole initial
  copy ahead. That is its used bytes from the latest lvol capacity record,
  falling back to the provisioned size.
- Otherwise it is `outstanding_bytes` from `get_replication_info`: the
  snapshots that are queued and not yet replicated.
- A volume in the endgame contributes 0 bytes. Its cutover time is counted
  separately, see below.

**Throughput:** measured per source LVS from the replicated snapshots of the
plan's volumes, as `sum(used_size) / sum(end_time - start_time)`. Snapshots
that shipped in under a second are left out, because their time is mostly
orchestration. Without a measurement, the forecast uses
`REPL_FORECAST_DEFAULT_BYTES_PER_SEC` (200 MB/s) and sets
`throughput_measured: false`.

**Queues:** transfers are serialized per source lvstore (the per-LVS transfer
gate), and so are cutovers (the LVS claim). For each LVS:

```
lvs_eta = sum(bytes_remaining) / throughput
        + pending_cutovers * cutover_seconds
```

`cutover_seconds` is the median measured cutover (commit task `end_time` minus
the `lvs_claim` stamp) of earlier cutovers. Without one, the forecast uses
`REPL_FORECAST_DEFAULT_CUTOVER_SEC` (60 s). The plan's ETA is the largest
`lvs_eta`, because the LVSes progress in parallel. Within one LVS a volume's
ETA is cumulative, in the queue order the runner uses (largest first).

**Limits:** the forecast does not model the client's write rate during the
initial copy. Writes show up as `outstanding_bytes` on later reads, so the ETA
is re-estimated on every status call rather than fixed at start.

## G5: target placement

`_pick_replication_node(target_cluster_id, lvol, exclude)` replaces the
weighted-random fallback in `replication_start` and `add_lvol_ha`. Sibling,
consistency-group, and chain-holder pinning still take precedence.

Among online primaries of the target cluster that are below their subsystem
limit, it picks the node with the smallest **planned load**:

- the provisioned bytes of the volumes already there, **plus**
- the provisioned bytes of every volume, in any cluster, whose
  `replication_node_id` already points at the node.

Ties go to the lower subsystem count, then to the node id, so the choice is
deterministic. Placement is no longer random, so a bulk start of N volumes
spreads them evenly.

## Tests

- **`test_migration_plan.py`:**
  - selection and the skip reasons;
  - largest-first order;
  - `require_baseline` passthrough;
  - dry run has no side effects;
  - cancel spares freezing tasks;
  - status aggregation.
- **`test_migration_forecast.py`:**
  - initial copy versus backlog bytes;
  - the measured and default throughput;
  - per-LVS queue math;
  - the plan ETA is the largest LVS ETA.
- **`test_cutover_node_failover.py`:**
  - the source leader is chosen over an offline or non-leader primary;
  - with no leader, the task defers;
  - fencing covers the peers when the primary is offline;
  - restore re-optimizes the executing leader;
  - the target is never a non-primary member, even when it leads;
  - before prepare, an offline target primary still defers;
  - the target is unsettled, so the task defers.
- **`test_replication_placement.py`:**
  - least planned load wins;
  - replicas already assigned count;
  - the choice is deterministic;
  - pinning wins over balance.
- **Runner:**
  - `require_baseline` defers while no snapshot has replicated, and does not
    count a retry.
