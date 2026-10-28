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
from simplyblock_core import utils
from simplyblock_core.controllers import replication_recovery_points
from simplyblock_core.models.snapshot import SnapShot, SnapShotMini
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

_REPLICATION_NAME_PREFIXES = ("repl_internal_", "repl_commit_", "demote_")


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
        fresh = db.atomic_update(snap, lambda s: setattr(s, "deleted", True))
        if fresh is None:
            continue
        _sync_mini(db, fresh)
        retired.append(snap.get_id())
    if retired:
        logger.info("Retiring %d replication snapshot(s) of migrated-away volume %s: %s",
                    len(retired), src_lvol.get_id(), ", ".join(retired))
    return retired


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
