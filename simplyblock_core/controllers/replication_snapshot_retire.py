"""Retire the replication snapshots a migrated-away volume leaves behind.

A migration with ``--delete-source`` deletes the source volume at cutover, but
not the replication snapshots its data sat on: the internal cadence and shrink
snapshots taken of it on this leg, and the landing copies of the previous leg
it was cloned from. Retention (``_prune_internal_snapshots``) only runs for a
live source when one of its snapshots finishes replicating, so once the
source is gone nothing ever deletes them again. On the 2026-10-28 back-and-
forth bed three legs left cluster A with no volumes and ~430 GiB allocated,
and cluster B went read-only at 96%.

The cutover marks that lineage ``deleted`` (the soft-delete flag the snapshot
delete path already uses for "delete once nothing depends on it"), and the
snapshot monitor deletes each marked snapshot once it is a leaf: its volume
record is gone, no volume is cloned from it, no snapshot record chains onto
it, and SPDK on the lvstore leader reports no clones. Leaf-first, a delete
never swap-merges into a neighbour, so the order of the chain does not matter.
User snapshots are never marked.
"""
import time

from simplyblock_core import utils
from simplyblock_core.controllers import replication_recovery_points
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# repl_round_: the landing copy of an endgame round that was re-queued as an
# ordinary transfer; not recognised, it was never retired and stopped the
# lineage walk, leaving every older copy below it.
_REPLICATION_NAME_PREFIXES = ("repl_internal_", "repl_commit_", "repl_round_", "demote_")


def is_replication_snapshot(snap):
    """True for a snapshot replication made: an internal snapshot, or the
    landing copy of one on the other cluster (a copy is recorded with the
    origin's name and the default ``user`` type)."""
    if snap.snap_type == SnapShot.TYPE_INTERNAL:
        return True
    return bool(snap.source_replicated_snap_uuid) and \
        (snap.snap_name or "").startswith(_REPLICATION_NAME_PREFIXES)


def source_lineage(db, src_lvol):
    """The replication snapshots *src_lvol*'s data sat on, on its own cluster:
    the ones taken of it, and the chain of landing copies it was cloned from."""
    lineage = {}
    for snap in db.get_snapshots_by_node_id(src_lvol.node_id):
        if snap.lvol and snap.lvol.get_id() == src_lvol.get_id() \
                and is_replication_snapshot(snap):
            lineage[snap.get_id()] = snap
    snap_id = src_lvol.cloned_from_snap
    while snap_id and snap_id not in lineage:
        try:
            snap = db.get_snapshot_by_id(snap_id)
        except KeyError:
            break
        if not is_replication_snapshot(snap):
            break
        lineage[snap_id] = snap
        snap_id = snap.prev_snap_uuid
    return list(lineage.values())


def retire_source_lineage(db, src_lvol):
    """Mark *src_lvol*'s replication lineage for deletion; returns the ids.

    A consistency group's newest generation is a recovery point no automatic
    path may delete, so its members are left alone."""
    lineage = source_lineage(db, src_lvol)
    keep = replication_recovery_points.group_recovery_point_ids(
        {getattr(s, "group_id", "") for s in lineage}, db=db)
    retired = []
    for snap in lineage:
        if snap.get_id() in keep or snap.deleted:
            continue
        fresh = db.atomic_update(snap, _retire)
        if fresh is None:
            continue
        _sync_mini(db, fresh)
        _unlink_from_source(db, fresh)
        retired.append(snap.get_id())
    if retired:
        logger.info("Retiring %d replication snapshot(s) of migrated-away volume %s: %s",
                    len(retired), src_lvol.get_id(), ", ".join(retired))
    return retired


# An orphaned replication copy must be at least this old before the sweep
# retires it: a landing copy is born before its transfer is finalized and
# chained, so a young one may still be about to gain a successor or a clone.
ORPHAN_COPY_MIN_AGE_SEC = 900


def retire_orphan_copies(db, cluster_id, now=None):
    """Retire the replication snapshots of *cluster_id* that no lineage walk reaches.

    retire_source_lineage walks back from a migrated-away volume's clone base,
    so it misses what lies beside that path: landing copies newer than the base
    (a cadence transfer that finished after the cutover clone was prepared),
    copies on a branch the endgame abandoned, and both halves of a pair the
    post-cutover prune skipped. Nothing else deletes them once their volume is
    gone, and under rewrite load each pins up to a volume's worth of data (bed
    bf21 2026-11-10: after three back-and-forth legs 15 such leaves on each
    cluster; cluster B reached 93-95% cannot_allocate, writes failed, fio saw
    EIO).

    A copy is retired when it is a replication snapshot whose volume record is
    gone, no volume is cloned from it, no snapshot chains onto it, it is older
    than ORPHAN_COPY_MIN_AGE_SEC, it is not a group recovery point, its source
    on the other cluster does not belong to a live volume (that pair is the
    base of the live volume's next delta), and SPDK reports no clones. The
    reaper then deletes it leaf-first like any retired snapshot. Returns the
    retired ids."""
    now = time.time() if now is None else now
    minis = db.get_mini_snapshots()
    lvols = db.get_mini_lvols()
    lvol_ids = {lv.get_id() for lv in lvols}
    cloned_from = {lv.cloned_from_snap for lv in lvols if lv.cloned_from_snap}
    chained_onto = {m.prev_snap_uuid for m in minis if m.prev_snap_uuid}
    retired = []
    for mini in minis:
        uuid = mini.get_id()
        created = mini.created_at or 0                      # 0: legacy record, old
        if mini.deleted or mini.status != SnapShot.STATUS_ONLINE \
                or uuid in cloned_from or uuid in chained_onto \
                or (created and now - created < ORPHAN_COPY_MIN_AGE_SEC):
            continue
        try:
            snap = db.get_snapshot_by_id(uuid)
        except KeyError:
            continue
        if snap.cluster_id != cluster_id or not is_replication_snapshot(snap):
            continue
        if not snap.lvol or snap.lvol.get_id() in lvol_ids:
            continue
        if _source_is_live(db, snap, lvol_ids):
            continue
        group = getattr(snap, "group_id", "") or ""
        if group and snap.get_id() in replication_recovery_points.group_recovery_point_ids(
                {group}, db=db):
            continue
        clones = _spdk_clones(db, snap)
        if clones is None or clones:
            continue
        fresh = db.atomic_update(snap, _retire)
        if fresh is None:
            continue
        _sync_mini(db, fresh)
        _unlink_from_source(db, fresh)
        retired.append(uuid)
    if retired:
        logger.info("Retiring %d orphaned replication snapshot(s) on cluster %s: %s",
                    len(retired), cluster_id, ", ".join(retired))
    return retired


def _source_is_live(db, snap, lvol_ids):
    """True when *snap* is a landing copy whose source snapshot still belongs
    to a live volume on the other cluster."""
    src_id = snap.source_replicated_snap_uuid
    if not src_id:
        return False
    try:
        src = db.get_snapshot_by_id(src_id)
    except KeyError:
        return False
    return bool(src.lvol) and src.lvol.get_id() in lvol_ids and not src.deleted


def _retire(snap):
    """Mark *snap* deleted and detach it from its replicated copies.

    After a migration each copy on the target cluster is the base the migrated
    volume is cloned from, so it has a live clone for as long as that volume
    lives. The delete path holds back a snapshot whose copy has a live clone
    (snapshot_controller._delete_locked: deleting it would hand its delete on to
    the copy); left paired, the retired lineage was held on the vacated cluster
    for good (bf6 2026-10-29: 112 retired snapshots, 0 volumes). The copy keeps
    its data and its own record; only this side's links to it go, so the
    delete stays local. A fail-over's sources are not retired here, and keep
    the hold."""
    snap.deleted = True
    snap.target_replicated_snap_uuid = ""
    snap.instances = [i for i in (snap.instances or [])
                      if _instance_cluster(i) in ("", snap.cluster_id)]


def _unlink_from_source(db, copy):
    """Drop the links that the snapshot *copy* was replicated from keeps to it.

    A retired landing copy is deleted, but its source on the other cluster
    lives on in the chain of the volume that migrated back there. Still
    pointing at the deleted copy, that source is the next return trip's chain
    predecessor, and the transfer refuses to finalize onto a copy it cannot
    resolve: bf6 2026-10-29 round 4, 56 replication tasks gave up after 8
    retries each ("Predecessor snapshot ... has remote copy ... but it cannot
    be resolved"). Unlinked, the source has no remote copy, and the next
    transfer starts the chain."""
    src_id = copy.source_replicated_snap_uuid
    if not src_id:
        return
    try:
        src = db.get_snapshot_by_id(src_id)
    except KeyError:
        return
    copy_id = copy.get_id()

    def unlink(s):
        if s.target_replicated_snap_uuid == copy_id:
            s.target_replicated_snap_uuid = ""
        s.instances = [i for i in (s.instances or []) if _instance_uuid(i) != copy_id]

    fresh = db.atomic_update(src, unlink)
    if fresh is not None:
        _sync_mini(db, fresh)


def _instance_uuid(inst):
    return (inst.get("uuid") if isinstance(inst, dict)
            else getattr(inst, "uuid", "")) or ""


def _instance_cluster(inst):
    return (inst.get("cluster_id") if isinstance(inst, dict)
            else getattr(inst, "cluster_id", "")) or ""


def _sync_mini(db, snap):
    """Carry the flag onto the snapshot's mini record, which the reaper scans.

    atomic_update writes only the full record; SnapShot.write_to_db is what
    keeps the mini index in step, so a flag set atomically stays invisible to
    every mini-table reader until some later plain write happens to refresh
    it (bf5 2026-10-28: 2 of 13 retired snapshots reaped in 20 minutes)."""
    SnapShotMini().from_snapshot(snap).write_to_db(db.kv_store)


def _spdk_clones(db, snap):
    """Clones SPDK reports for *snap* on its lvstore leader; None when unknown."""
    from simplyblock_core.controllers import lvol_controller
    node_ids = [snap.lvol.node_id] + list(getattr(snap.lvol, "nodes", None) or [])
    for node_id in dict.fromkeys(n for n in node_ids if n):
        try:
            node = db.get_storage_node_by_id(node_id)
            if node.status != StorageNode.STATUS_ONLINE or \
                    not lvol_controller.is_node_leader(node, snap.lvol.lvs_name):
                continue
            bdev = node.rpc_client().bdev_get(snap.snap_bdev)
        except Exception as e:                              # noqa: BLE001
            logger.warning("Could not read retired snapshot %s on %s: %s",
                           snap.get_id(), node_id, e)
            return None
        if bdev is None:
            return []
        return list(((bdev.get("driver_specific") or {}).get("lvol") or {}).get("clones") or [])
    return None


def reap_retired(db, cluster_id):
    """Delete the retired replication snapshots of *cluster_id* that are leaves.

    Each pass removes the current leaves; the next layer becomes a leaf once
    their deletes complete, so a chain drains over a few monitor cycles."""
    from simplyblock_core.controllers import snapshot_controller
    minis = db.get_mini_snapshots()
    candidates = [m for m in minis
                  if m.deleted and m.status == SnapShot.STATUS_ONLINE]
    if not candidates:
        return 0
    lvols = db.get_mini_lvols()
    lvol_ids = {lv.get_id() for lv in lvols}
    cloned_from = {lv.cloned_from_snap for lv in lvols if lv.cloned_from_snap}
    chained_onto = {m.prev_snap_uuid for m in minis if m.prev_snap_uuid}
    reaped = 0
    for mini in candidates:
        uuid = mini.get_id()
        if uuid in cloned_from or uuid in chained_onto:
            continue
        try:
            snap = db.get_snapshot_by_id(uuid)
        except KeyError:
            continue
        if snap.cluster_id != cluster_id or not is_replication_snapshot(snap):
            continue
        if not snap.lvol or snap.lvol.get_id() in lvol_ids:
            continue
        clones = _spdk_clones(db, snap)
        if clones is None or clones:
            continue
        logger.info("Deleting retired replication snapshot %s (%s)", uuid, snap.snap_bdev)
        if snapshot_controller.delete(uuid):
            reaped += 1
    return reaped
