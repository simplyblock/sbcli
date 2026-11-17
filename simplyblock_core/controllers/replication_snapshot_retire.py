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

The lineage does not reach every copy the leg left on the target cluster:
the cutover clone is built early, on the newest copy that had landed then,
and a cadence snapshot still in flight lands afterwards, chained onto that
same copy beside the clone. Retention keeps it (it is the newest pair), the
retire above detaches it from its source, and from then on nothing pairs with
it, builds on it or ever deletes it -- and the clone base under it, retired on
the next leg, is never a leaf either (bed bf25 2026-11-16: 166 such copies
after 8 back-and-forth rounds, each pinning up to the whole volume; the
clusters filled up and the clients got EIO). sweep_orphaned hands those to the
reaper: a replication snapshot whose volume is gone, that no volume builds on,
and whose replication pair is broken.
"""
import time

from simplyblock_core import constants, utils
from simplyblock_core.controllers import replication_recovery_points
from simplyblock_core.models.lvol_model import LVol
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


def _live_bases(minis, lvols):
    """Ids of every snapshot a volume reads through: each volume's clone base
    and the recorded chain below it. Volumes being deleted count too -- their
    data-plane delete may not have finished."""
    by_id = {m.get_id(): m for m in minis}
    bases: set = set()
    for lv in lvols:
        snap_id = lv.cloned_from_snap
        while snap_id and snap_id not in bases:
            bases.add(snap_id)
            mini = by_id.get(snap_id)
            snap_id = mini.prev_snap_uuid if mini is not None else ""
    return bases


def _links_back(counterpart, snap_id):
    """True when *counterpart* still records *snap_id* as its replicated pair."""
    if snap_id in (counterpart.target_replicated_snap_uuid,
                   counterpart.source_replicated_snap_uuid):
        return True
    return any(_instance_uuid(i) == snap_id for i in (counterpart.instances or []))


def _pair_is_live(db, snap, moved_on=frozenset()):
    """True while *snap* belongs to a replication pair: a counterpart on the
    other cluster still exists and still links back to it. A live pair is
    retention's business (or a fail-over's hold, KI-12), never the sweep's.
    An instance recorded on another cluster counts as a pair as long as it
    exists: instance-mode replication links only that way.

    A counterpart in *moved_on* (_moved_on_orphans) does not keep the pair
    live: both halves are leftovers of a volume that migrated on."""
    for other_id in (snap.source_replicated_snap_uuid, snap.target_replicated_snap_uuid):
        if not other_id or other_id in moved_on:
            continue
        try:
            other = db.get_snapshot_by_id(other_id)
        except KeyError:
            continue
        if _links_back(other, snap.get_id()):
            return True
    for inst in snap.instances or []:
        inst_id = _instance_uuid(inst)
        if not inst_id or inst_id == snap.get_id() or inst_id in moved_on or \
                _instance_cluster(inst) in ("", snap.cluster_id):
            continue
        try:
            db.get_snapshot_by_id(inst_id)
        except KeyError:
            continue
        return True
    return False


def _orphan_candidates(minis, lvols, now, include_marked=False):
    """Mini records that may be orphaned replication snapshots: online, not
    yet marked (unless *include_marked*), past the grace age, no volume of
    their own, no volume reading through them. The full record decides the
    rest."""
    lvol_ids = {lv.get_id() for lv in lvols}
    bases = _live_bases(minis, lvols)
    grace = constants.REPL_ORPHAN_SNAPSHOT_GRACE_SEC
    out = []
    for m in minis:
        if (m.deleted and not include_marked) or m.status != SnapShot.STATUS_ONLINE:
            continue
        if m.snap_type != SnapShot.TYPE_INTERNAL and \
                not (m.snap_name or "").startswith(_REPLICATION_NAME_PREFIXES):
            continue
        if not m.created_at or now - m.created_at < grace:
            continue
        if m.get_id() in bases:
            continue
        if m.lvol is not None and m.lvol.get_id() in lvol_ids:
            continue
        out.append(m)
    return out


def _cadence_owner(snap_name):
    """The volume-id prefix a cadence snapshot is named after
    (repl_internal_<id[:8]>_<ts>, snapshot_monitor); "" for any other name."""
    if not (snap_name or "").startswith("repl_internal_"):
        return ""
    parts = snap_name[len("repl_internal_"):].split("_")
    return parts[0] if len(parts) >= 2 and len(parts[0]) == 8 else ""


def _moved_on_orphans(candidates, lvols):
    """Ids of the orphan candidates (of every cluster) whose volume migrated
    on: a cadence copy named after a volume that is still alive -- a migrated
    volume keeps its id -- but that is not its own volume and that no volume
    reads through (both already ruled out by _orphan_candidates).

    A volume that migrates back re-sends the chain under it, so both halves of
    a pair can be left like that, one per cluster, each linking back to the
    other: judged one at a time, each kept the other "live" for ever (bed
    bf26, 2026-11-17: 90 of 131 snapshots after 8 back-and-forth rounds). A
    pair whose volume is gone altogether (a deleted DR source) has no live
    namesake and stays retention's business."""
    live = {lv.get_id()[:8] for lv in lvols
            if getattr(lv, "status", "") not in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)}
    return frozenset(m.get_id() for m in candidates
                     if _cadence_owner(m.snap_name) in live)


def sweep_orphaned(db, cluster_id, now=None):
    """Mark the orphaned replication snapshots of *cluster_id* for the reaper;
    returns their ids.

    Orphaned: a replication snapshot (is_replication_snapshot) whose volume is
    gone, that no volume reads through (clone base or the chain below one),
    and whose replication pair is broken -- the counterpart is gone or no
    longer links back, as after retire_source_lineage detached it. Typically
    the copy of a cadence snapshot that landed after the cutover clone was
    built on the copy before it (see the module docstring).

    Only a leaf is marked -- SPDK on the lvstore leader reports no clones of
    it -- and reap_retired checks again before it deletes (no clone, no
    chained successor, no SPDK clones), so a snapshot something still sits on
    is never touched, whatever the records say. A consistency group's newest
    generation is never marked."""
    now = time.time() if now is None else now
    minis = db.get_mini_snapshots()
    lvols = db.get_mini_lvols()
    orphans = []
    candidates = _orphan_candidates(minis, lvols, now)
    # A half marked on an earlier sweep (of either cluster) still counts.
    moved_on = _moved_on_orphans(
        _orphan_candidates(minis, lvols, now, include_marked=True), lvols)
    for mini in candidates:
        try:
            snap = db.get_snapshot_by_id(mini.get_id())
        except KeyError:
            continue
        # A landing copy re-sent onward when its volume migrated back lost its
        # source link and reads as a plain user snapshot; a moved-on cadence
        # copy (named after a live volume that is not its own) is one anyway.
        if snap.cluster_id != cluster_id or snap.deleted or \
                snap.status != SnapShot.STATUS_ONLINE or \
                not (is_replication_snapshot(snap) or snap.get_id() in moved_on):
            continue
        if _pair_is_live(db, snap, moved_on):
            continue
        # Something still sits on it in the data plane (a landing volume
        # being chained onto it, a successor the records missed), or the
        # leader cannot be asked: not now. A chain of orphans is marked leaf
        # by leaf, one layer per sweep.
        if _spdk_clones(db, snap) != []:
            continue
        orphans.append(snap)
    if not orphans:
        return []
    keep = replication_recovery_points.group_recovery_point_ids(
        {getattr(s, "group_id", "") for s in orphans}, db=db)
    marked = []
    for snap in orphans:
        if snap.get_id() in keep:
            continue
        fresh = db.atomic_update(snap, _mark_deleted)
        if fresh is None:
            continue
        _sync_mini(db, fresh)
        marked.append(snap.get_id())
    if marked:
        logger.info("Handing %d orphaned replication snapshot(s) of cluster %s to the "
                    "reaper (volume gone, no volume reads through them, pair broken): %s",
                    len(marked), cluster_id, ", ".join(marked))
    return marked


def _mark_deleted(snap):
    snap.deleted = True


# Process-local on purpose: a rate limit only. Marking is idempotent (an
# atomic update of one flag), so two monitor processes sweeping the same
# cluster at once mark the same snapshots twice and nothing else.
_LAST_SWEEP: dict = {}


def _sweep_due(cluster_id, now):
    last = _LAST_SWEEP.get(cluster_id)
    if last is not None and now - last < constants.REPL_ORPHAN_SWEEP_INTERVAL_SEC:
        return False
    _LAST_SWEEP[cluster_id] = now
    return True


def reap_retired(db, cluster_id):
    """Delete the retired replication snapshots of *cluster_id* that are leaves.

    Each pass removes the current leaves; the next layer becomes a leaf once
    their deletes complete, so a chain drains over a few monitor cycles.
    Orphaned replication snapshots (sweep_orphaned) are marked first, at most
    every REPL_ORPHAN_SWEEP_INTERVAL_SEC."""
    from simplyblock_core.controllers import snapshot_controller
    now = time.time()
    if _sweep_due(cluster_id, now):
        try:
            sweep_orphaned(db, cluster_id, now=now)
        except Exception as e:                              # noqa: BLE001
            logger.error("Orphaned replication snapshot sweep failed for cluster %s: %s",
                         cluster_id, e)
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
        # A cadence copy re-sent onward reads as a user snapshot (no source
        # link); sweep_orphaned marked it as a moved-on half.
        if snap.cluster_id != cluster_id or not (
                is_replication_snapshot(snap) or _cadence_owner(snap.snap_name)):
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
