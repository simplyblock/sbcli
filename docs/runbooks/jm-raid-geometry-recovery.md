# Recovery: JM RAID geometry mismatch after upgrade

## Symptom

After upgrading a cluster that was **suspended** before the upgrade, `sbctl
cluster activate` fails on the first node with:

```
bs_super_validate: *ERROR*: unsupported version on super block
raid_bdev_read_sb_cb: *ERROR*: failed-to parse bdev raid0_<lvs> superblock
LVS <LVS> did not recover on examine on node <node>: raid=raid0_<lvs> present
  but lvstore did not recover
Error activating cluster: node <node> holds partial state for LVS <LVS> that
  examine could not recover.
```

Every LVS on every node fails the same way.

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

Requires the fix that adds `Cluster.jm_raid_layout` (this PR).

1. **Pin the cluster to the legacy geometry.** This is authoritative and
   overrides any per-JMDevice leg record that the failed upgrade attempt may
   have polluted to `raid01`:

   ```python
   # on the mgmt node, in a python shell with simplyblock_core importable
   from simplyblock_core.db_controller import DBController
   db = DBController()
   cl = db.get_cluster_by_id("<cluster_id>")
   db.atomic_update(cl, lambda c: setattr(c, "jm_raid_layout", "legacy_nway"))
   ```

2. **Clear any polluted per-device leg records** so nothing later re-detects
   `raid01` (optional once the flag is pinned, but keeps the records honest):

   ```python
   for n in db.get_storage_nodes_by_cluster_id("<cluster_id>"):
       jd = n.jm_device
       if jd and jd.jm_leg_bdevs:
           jd.jm_leg_bdevs = []
           jd.jm_leg_members = []
           n.write_to_db()
   ```

3. **Re-run activation.** Each node's JM raid is now rebuilt as N-way RAID1,
   the alceml/journal/distrib read back intact, and the LVS superblock parses:

   ```
   sbctl cluster activate <cluster_id>
   ```

4. **Verify.** `sbctl cluster status <cluster_id>` shows `active`; each node's
   `raid_jm_<node>` exists with no `_l0`/`_l1` legs; no `unsupported version`
   errors in the SPDK logs.

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
