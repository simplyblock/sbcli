"""A migration must never fill its SOURCE cluster by itself.

Bed bf29 (2026-11-20): five volumes of one source cluster ran their cutover
endgame at the same time, under sustained client overwrite. Every shrink
round takes a snapshot of the source volume, and every snapshot pins what the
client overwrites after it; the superseded ones were deleted (async on the
primary, sync on the secondaries) more slowly than new ones were taken. The
source went 41% -> 82% -> 99% in 16 minutes (~440 MB/s of pinned data) and
ran out of space.

Three rules bound what a migration consumes on its source. Only the
migration's OWN snapshots are held back by them -- never a client write, and
never a client volume creation: the capacity thresholds (cap_warn/cap_crit)
stay alarms, and a cluster is full at 100 %.

1. One endgame per source cluster at a time
   (``REPL_CUTOVER_MAX_ENDGAMES_PER_CLUSTER``; tasks_runner_replication_final.
   _acquire_lvs_claim). The others keep replicating on their ordinary cadence
   meanwhile -- an endgame queued for the slot holds no lvstore.
2. An endgame takes its next round snapshot only once the deletions of the
   volume's earlier snapshots have completed (:func:`deletions_pending`).
3. No migration snapshot -- endgame round or ordinary cadence of a
   migration-mode volume -- while the source's free space is under the
   headroom estimate (:func:`space_hold`).
"""
import logging
import time

from simplyblock_core import constants
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger(__name__)


def source_cluster_id(db, lvol, default=""):
    """The cluster *lvol* lives on (its host node's), else *default*."""
    try:
        return db.get_storage_node_by_id(lvol.node_id).cluster_id or default
    except Exception:                                     # noqa: BLE001
        return default


def cluster_free(db, cluster_id, now=None):
    """``(free bytes, total bytes, "")`` from the cluster's latest capacity
    record, or ``(None, None, why)`` when there is no usable one: none at
    all, unreadable, no total, or older than REPL_MIGRATION_CAPACITY_STALE_SEC
    (the collector writes one every DEV_STAT_COLLECTOR_INTERVAL_SEC; a record
    much older than that says the collector is down, not that space is
    free)."""
    now = time.time() if now is None else now
    try:
        cluster = db.get_cluster_by_id(cluster_id)
        records = db.get_cluster_capacity(cluster, 1)
    except Exception as e:                                # noqa: BLE001
        return None, None, f"capacity of cluster {cluster_id} unreadable: {e}"
    if not records:
        return None, None, f"no capacity record for cluster {cluster_id}"
    rec = records[0]
    total = int(getattr(rec, "size_total", 0) or 0)
    if total <= 0:
        return None, None, f"capacity record of cluster {cluster_id} has no total"
    age = now - float(getattr(rec, "date", 0) or 0)
    if age > constants.REPL_MIGRATION_CAPACITY_STALE_SEC:
        return None, None, (f"capacity record of cluster {cluster_id} is {int(age)}s "
                            f"old (limit {constants.REPL_MIGRATION_CAPACITY_STALE_SEC}s)")
    used = int(getattr(rec, "size_used", 0) or 0)
    return max(0, total - used), total, ""


def dirty_bytes(db, lvol):
    """Bytes written to *lvol* since its newest snapshot, or None.

    The dirty bitmap of the volume's top bdev
    (``bdev_lvol_dirty_bitmap_info``). Every online member is asked and the
    largest complete count wins: only the leader's is the real one, and an
    over-estimate only makes the hold more careful."""
    from simplyblock_core.controllers.replication_delta import _members
    best = None
    try:
        members = _members(db, lvol)
    except Exception:                                     # noqa: BLE001
        return None
    for node in members:
        if getattr(node, "status", None) != StorageNode.STATUS_ONLINE:
            continue
        try:
            info = node.rpc_client(timeout=constants.REPL_FINAL_DELTA_COUNTS_TIMEOUT_SEC,
                                   retry=0).bdev_lvol_dirty_bitmap_info(lvol.top_bdev)
        except Exception:                                 # noqa: BLE001
            continue
        if isinstance(info, dict) and info.get("tracking") and info.get("complete"):
            n = int(info.get("dirty_bytes") or 0)
            best = n if best is None else max(best, n)
    return best


def headroom_floor(total):
    """The free space a migration snapshot always leaves: REPL_MIGRATION_MIN_FREE_PCT
    of the cluster."""
    return int(total * constants.REPL_MIGRATION_MIN_FREE_PCT / 100)


def required_free(total, dirty):
    """Free space a migration snapshot needs: REPL_MIGRATION_HEADROOM_FACTOR
    times what the snapshot pins at once (the bytes written since the
    volume's last snapshot), and never less than :func:`headroom_floor`.

    Why x2: the new snapshot pins those bytes the moment it is taken, and
    while its round transfers the client overwrites about as much again --
    new allocations that the previous snapshot, still pending deletion, keeps
    from being reused. The floor covers what one volume's count cannot see:
    the other volumes' writes in between, and that the capacity record is up
    to one collector interval old (at bf29's 440 MB/s, 15 s are 6.6 GB)."""
    return max(headroom_floor(total),
               int(constants.REPL_MIGRATION_HEADROOM_FACTOR * max(0, dirty or 0)))


def assess(db, lvol, cluster_id, now=None):
    """``("ok" | "low" | "unknown", detail)`` for a migration snapshot of
    *lvol* on *cluster_id*. Asks SPDK for the volume's dirty count only when
    the answer depends on it."""
    free, total, why = cluster_free(db, cluster_id, now)
    if free is None:
        return "unknown", why
    floor = headroom_floor(total)
    if free < floor:
        return "low", (f"{free >> 30} GiB free on the source cluster, under the "
                       f"{constants.REPL_MIGRATION_MIN_FREE_PCT}% floor ({floor >> 30} GiB)")
    # The dirty count cannot exceed the volume: with that much room there is
    # nothing to ask.
    size = int(getattr(lvol, "size", 0) or 0)
    if size and free >= max(floor, constants.REPL_MIGRATION_HEADROOM_FACTOR * size):
        return "ok", ""
    dirty = dirty_bytes(db, lvol)
    if dirty is None:
        # Unknown dirty count: the worst case is the whole volume.
        dirty = size
    need = required_free(total, dirty)
    if free < need:
        return "low", (f"{free >> 30} GiB free on the source cluster, under the "
                       f"{need >> 30} GiB headroom ({constants.REPL_MIGRATION_HEADROOM_FACTOR}x "
                       f"{dirty >> 20} MiB written since the last snapshot, floor "
                       f"{floor >> 30} GiB)")
    return "ok", ""


def space_hold(db, lvol, cluster_id, unknown_since=0, now=None):
    """Whether a migration snapshot of *lvol* must wait for space.

    Returns ``(reason, unknown_since)``: an empty reason means take it. A
    capacity that cannot be read holds the snapshot for at most
    REPL_MIGRATION_CAPACITY_UNKNOWN_WAIT_SEC from *unknown_since* (the caller
    keeps it and hands it back) and then lets it through, loudly: a broken
    stats collector must not stop every migration for good. Space that is
    known to be short holds for as long as it lasts -- the endgame's own
    shrink deadline bounds that wait, and a cadence snapshot just waits."""
    now = time.time() if now is None else now
    verdict, detail = assess(db, lvol, cluster_id, now)
    if verdict == "ok":
        return "", 0
    if verdict == "low":
        return f"migration snapshot of {lvol.get_id()} deferred: {detail}", 0
    since = unknown_since or now
    waited = now - since
    if waited >= constants.REPL_MIGRATION_CAPACITY_UNKNOWN_WAIT_SEC:
        logger.warning("migration snapshot of %s: source capacity unknown for %ds "
                       "(%s); proceeding without the free-space check",
                       lvol.get_id(), int(waited), detail)
        return "", since
    return (f"migration snapshot of {lvol.get_id()} deferred: {detail}; "
            f"proceeding without the check in "
            f"{int(constants.REPL_MIGRATION_CAPACITY_UNKNOWN_WAIT_SEC - waited)}s"), since


def deletions_pending(db, lvol):
    """Ids of *lvol*'s snapshots whose deletion has not completed yet
    (status in_deletion). A soft-deleted snapshot (``deleted`` but online,
    held by a clone) is not pending: nothing is deleting it."""
    try:
        snaps = db.get_snapshots_by_lvol_id(lvol.get_id())
    except Exception as e:                                # noqa: BLE001
        logger.warning("Cannot read the snapshots of %s: %s", lvol.get_id(), e)
        return []
    return [s.get_id() for s in snaps if s.status == SnapShot.STATUS_IN_DELETION]


def space_low_event(lvol, reason):
    """A warning-level cluster event: a migration snapshot waits for space."""
    try:
        from simplyblock_core.controllers import lvol_events
        lvol_events.migration_space_low(lvol, reason)
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not log the migration space event for %s: %s",
                       lvol.get_id(), e)
