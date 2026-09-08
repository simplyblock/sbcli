# Recovery: JM RAID geometry mismatch after upgrade

## Symptom

After upgrading a cluster that was **suspended** before the upgrade, the
failure surfaces **on `sn restart`** (the restarting node's own LVS-recovery
examine), before `cluster activate` is even reached — and again on activate:

```
bs_super_validate: *ERROR*: unsupported version on super block
raid_bdev_read_sb_cb: *ERROR*: failed-to parse bdev raid0_<lvs> superblock
LVS <LVS> did not recover on examine on node <node>: raid=raid0_<lvs> present
  but lvstore did not recover
```

Every LVS on every node fails the same way.

The JM raid is (re)built **during `sn restart`**, not during activate: on
restart SPDK comes up fresh (`bdev_auto_examine` is off), and
`_prepare_cluster_devices_on_restart` -> `_create_jm_stack_on_raid` rebuilds the
whole JM stack from the node's NVMe partitions. That is the step that reads
`Cluster.jm_raid_layout`, so the geometry must be pinned **before** the restart.

## Cause

The per-node JM (journal) device is a RAID with **`superblock=False`** — its
geometry is not recorded on disk. Older releases built it as one **N-way RAID1
mirror across every JM partition** (a full linear copy per drive). Commit
`41dea9434` (RAID 0+1 journal layout) changed the planner to a **mirror of two
RAID0 legs**, which stripes each leg 4 KiB at a time across its drives.

On upgrade, the new code rebuilt each suspended cluster's JM raid under the new
geometry. Because the same on-disk bytes are now read through a different
layout, the alceml PBA header / journal records / distrib output come back
scrambled, and the blobstore superblock on `raid0_<lvs>` fails to parse.

## Data safety

**No data is lost and nothing was overwritten.** A fresh SPDK RAID1 create with
both legs present takes the configure→ONLINE path — it neither resyncs nor
writes — and the JM opened its alceml with `pba_init_mode=1` (use-existing,
never format) and appended zero records. The original bytes on every JM
partition are intact; they were only being *read* through the wrong geometry.
The cluster is recoverable non-destructively by reproducing the original
geometry.

## Recovery (per cluster)

Order matters. The two DB edits below only take effect once (a) the management
plane is running the fixed build that reads `jm_raid_layout`, and (b) they land
**before** the `sn restart` that rebuilds the JM stack. Do not restart any node
in the cluster until both edits are done.

### 1. Upgrade the management plane to the fixed build (this PR)

The flag is inert without the fixed code — `_create_jm_stack_on_raid` in the
running mgmt image is the only thing that reads `jm_raid_layout` and builds the
N-way path. Upgrade **mgmt only**; do **not** let the upgrade roll a restart
across the storage nodes yet (the geometry is not pinned until step 2).

### 2. Pin the geometry and clear the polluted records (before any node restart)

**a. Pin the cluster to the legacy geometry.** Authoritative — overrides any
per-JMDevice leg record the failed upgrade attempt polluted to `raid01`:

```python
# on the mgmt node, in a python shell with simplyblock_core importable
from simplyblock_core.db_controller import DBController
db = DBController()
cl = db.get_cluster_by_id("<cluster_id>")
db.atomic_update(cl, lambda c: setattr(c, "jm_raid_layout", "legacy_nway"))
```

**b. Clear the polluted per-device leg records** so nothing re-detects `raid01`
(belt-and-braces once the flag is pinned, but keeps the records honest):

```python
for n in db.get_storage_nodes_by_cluster_id("<cluster_id>"):
    jd = n.jm_device
    if jd and jd.jm_leg_bdevs:
        jd.jm_leg_bdevs = []
        jd.jm_leg_members = []
        n.write_to_db()
```

### 3. Restart the storage nodes

This is the step that actually rebuilds each JM raid — SPDK restarts, and
`_create_jm_stack_on_raid` reads `legacy_nway` and rebuilds the JM as one N-way
RAID1 across all partitions, reproducing the original geometry. The node's own
LVS-recovery examine right after now parses the superblock.

```
sbctl storage-node restart <node_id>      # one node at a time, verify each
```

### 4. Activate if still needed, and verify

```
sbctl cluster activate <cluster_id>       # if the cluster is still suspended
sbctl cluster status  <cluster_id>        # expect: active
```

Verify on each node: `raid_jm_<node>` exists with **no** `_l0`/`_l1` legs, and
there are no further `unsupported version on super block` errors in the SPDK
logs.

> Do a single node first, confirm its JM rebuilt N-way and its examine passed,
> before restarting the rest.

## Why the durable fix keeps this from recurring

`Cluster.jm_raid_layout` pins the geometry once and reproduces it forever:

* fresh clusters are pinned `raid01` at create;
* an upgrade pins the geometry from the **pre-restart** JMDevice records (any
  device with recorded RAID0+1 legs ⇒ `raid01`, else `legacy_nway`) before it
  restarts any node — while the records still reflect what is on disk;
* `_create_jm_stack_on_raid` builds the pinned geometry, and falls back to the
  per-device record only when the cluster flag is still empty.

An existing cluster can therefore never have its journals rebuilt under a
different geometry than they were written with.
