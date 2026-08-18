# Replication targets and policies — design

Replaces today's flat, single-target replication configuration with a
three-level hierarchy: **target → policy → volume**.

## Why

Today a source cluster has exactly **one** replication destination, stored as
three scalars on the cluster record (`models/cluster.py:187-189`):

```python
snapshot_replication_target_cluster: str = ""
snapshot_replication_target_pool: str = ""
snapshot_replication_timeout: int = 60*10
```

`cluster add-replication` overwrites them, so a second call replaces the target
instead of adding one, and there is no command to remove a target. Cadence and
mode live per volume (`replication_interval_min`, `replication_mode`) and must be
repeated on every `volume replication-start`, so two volumes that are meant to
share a schedule can silently drift. Retention of internal replication snapshots
is not configurable at all — it is the module constant
`_KEEP_REPLICATED_INTERNAL = 2`.

## Target model

### 1. ReplicationTarget

A named destination. Any number per source cluster, created at deploy time or
any time later. Structurally mirrors `BackupPolicy` (`models/backup.py`), whose
`get_id()` is already `cluster_id/uuid`.

```python
class ReplicationTarget(BaseModel):
    STATUS_ACTIVE   = 'active'
    STATUS_INACTIVE = 'inactive'

    cluster_id: str = ""          # SOURCE cluster this target belongs to
    target_name: str = ""         # unique per source cluster
    target_cluster_id: str = ""
    target_pool_uuid: str = ""    # resolved to a UUID on create, never a name
    timeout_sec: int = 60*10
    status: str = STATUS_ACTIVE

    def get_id(self):
        return "%s/%s" % (self.cluster_id, self.uuid)
```

Removal is refused while any policy references it.

### 2. ReplicationPolicy

One or more per target. Owns the **cadence** and everything else that should be
shared by a group of volumes.

```python
class ReplicationPolicy(BaseModel):
    cluster_id: str = ""          # SOURCE cluster
    policy_name: str = ""         # unique per source cluster
    target_id: str = ""           # ReplicationTarget.get_id()
    interval_min: int = 1         # cadence: internal snapshot every N minutes
    mode: str = "failover"        # failover | migration
    keep_replicated: int = 2      # internal replicated snapshots to retain
    status: str = STATUS_ACTIVE
```

`keep_replicated` finally makes retention configurable, and its **floor is 2**:
a replicated snapshot holds only its own clusters, and deleting one swap-merges
its segments into the successor CHAINED to it, so keeping fewer than a pair
leaves an arrival with nothing to chain onto (see `b34bb8d96`). The prune path
must keep verifying the chain regardless of this value.

Removal is refused while any volume references it.

### 3. Volume assignment

`LVol` gains one field:

```python
replication_policy_id: str = ""   # "" = not replicated
```

`do_replicate`, `replication_mode`, `replication_interval_min` and
`replication_node_id` remain as the **resolved effective values**, derived from
policy + target when the policy is attached. The replication service is
therefore unchanged — it keeps reading the per-volume fields it reads today.

## Operations

| Operation | CLI | Effect |
|---|---|---|
| Create target | `cluster replication-target-add <cluster> <name> <target_cluster> [--target-pool] [--timeout]` | validates both clusters and that the pool is ACTIVE; stores the pool **UUID** |
| List / remove target | `cluster replication-target-list`, `... -remove <target>` | remove refused while policies reference it |
| Create policy | `cluster replication-policy-add <cluster> <name> --target <t> --interval-min N [--mode] [--keep N]` | cadence + mode + retention |
| List / remove policy | `cluster replication-policy-list`, `... -remove <policy>` | remove refused while volumes reference it |
| Assign at create | `volume add ... --replication-policy <policy>` | sets `replication_policy_id`, derives the effective fields, selects the destination node |
| Attach later | `volume replication-policy-set <vol> <policy>` | starts replication |
| Detach | `volume replication-policy-clear <vol>` | stops replication, see below |
| Change | `volume replication-policy-set <vol> <other>` | detach then attach, atomically from the caller's view |

### Detach semantics (explicit)

Detaching must leave no replication residue:

1. Clear `replication_policy_id`, set `do_replicate = False`.
2. Cancel every non-DONE `FN_SNAPSHOT_REPLICATION` task for the volume.
3. Delete the volume's **internal** replication snapshots on the **source and
   the target**. Today `replication_stop` keeps the target copies and only
   `snapshot delete-replication-only` removes one, so this is new behaviour.

Constraints on step 3, both already established:

- **User snapshots are never touched** — only `SnapShot.TYPE_INTERNAL`.
- **Never delete a target snapshot a live volume is cloned from**
  (`_has_dependent_clone`): a failed-over volume built on it would start reading
  zeros, because the delete reaches SPDK as `bdev_lvol_delete(sync=False)` and
  frees the blocks immediately.
- If a cutover is in flight (`LVolReplication.state == cutover_pending`), detach
  must be **refused**, not raced.

### Change semantics

Change is detach-then-attach by definition, so the delta base is discarded and
the next replication to the new target is **full**. That is the intended cost;
it should be stated in the CLI help and the docs, because for a large volume it
is not a cheap operation.

## Compatibility

- On upgrade, if `snapshot_replication_target_cluster` is set, synthesise one
  `ReplicationTarget` named `default` from the three scalars, plus one
  `ReplicationPolicy` named `default` carrying the interval already in use, and
  point existing replicated volumes at it. Nothing stops replicating.
- `cluster add-replication` stays as an alias that creates/updates the `default`
  target, and is documented as deprecated.
- `volume replication-start --replication-cluster-id/--mode/--interval-min`
  stays for volumes managed without a policy; a volume with a policy rejects
  those flags rather than silently diverging from its policy.

## API

Per source cluster:

- `POST/GET/DELETE /clusters/{id}/replication-targets[/{target_id}]`
- `POST/GET/DELETE /clusters/{id}/replication-policies[/{policy_id}]`
- `PUT/DELETE /clusters/{c}/storage-pools/{p}/volumes/{v}/replication-policy`

Two REST defects to fix while touching this surface:

1. `POST .../replication_start` passes the **path** cluster as
   `replication_cluster_id`, i.e. the volume's own cluster, so it self-targets
   and never falls back to the configured destination.
2. The same route takes no body, so `mode` and `interval_min` are unreachable
   over REST. With policies this disappears: the body carries a policy id.

## Open questions

1. **One policy per volume, or several?** A single `replication_policy_id` means
   one destination per volume. Fan-out to two sites needs a per-(volume,target)
   record and a per-target `replication_node_id`, which is a deeper change to
   the service. Assumed single for now.
2. **Attach policies to pools too?** `BackupPolicyAttachment` already supports
   `pool | lvol`; a pool-level default would auto-enrol new volumes.
3. **Does the policy own cutover behaviour** (commit/failback), or only cadence,
   mode and retention?
