# coding=utf-8
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.controllers import (
    snapshot_events, snapshot_controller, tasks_controller)
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)


# Repeatedly failing deletes must not consume a slot every cycle. Without this
# a snapshot that can never complete is retried forever, the in_deletion set
# only grows, and the cycle cost grows with it (lab 2026-08-14: 1298 records
# retried per cycle, internal-snapshot creation starved to a standstill).
_DELETE_BACKOFF: dict = {}
_DELETE_BACKOFF_GUARD = threading.Lock()
_BACKOFF_BASE_SEC = 5
_BACKOFF_MAX_SEC = 300


def delete_attempt_due(snap_uuid, now_ts):
    with _DELETE_BACKOFF_GUARD:
        _attempts, next_ts = _DELETE_BACKOFF.get(snap_uuid, (0, 0))
    return now_ts >= next_ts


def note_delete_attempt(snap_uuid, now_ts, progressed):
    """Clear the backoff when a delete advanced; otherwise back off harder."""
    with _DELETE_BACKOFF_GUARD:
        if progressed:
            _DELETE_BACKOFF.pop(snap_uuid, None)
            return
        attempts = _DELETE_BACKOFF.get(snap_uuid, (0, 0))[0] + 1
        delay = min(_BACKOFF_MAX_SEC, _BACKOFF_BASE_SEC * (2 ** min(attempts, 6)))
        _DELETE_BACKOFF[snap_uuid] = (attempts, now_ts + delay)


def forget_delete_backoff(snap_uuid):
    with _DELETE_BACKOFF_GUARD:
        _DELETE_BACKOFF.pop(snap_uuid, None)


def set_snapshot_health_check(snap, health_check_status):
    snap = db.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    snap.health_check = health_check_status
    snap.updated_at = str(datetime.now())
    snap.write_to_db()


def process_snap_delete_finish(snap, completed_node):
    """Phase-2 of the delete protocol (sync deletes + DB finalize).

    ``completed_node`` is the node where the phase-1 async delete was issued
    (``snap.deletion_status``) and has been CONFIRMED complete by a
    delete-status poll. It is authoritative regardless of where leadership
    sits NOW: phase-2 runs per-node and needs no leader. The previous
    re-detection of leadership here (and the async re-issue whenever
    ``deletion_status`` pointed elsewhere) turned every leadership move into
    another phase-1 — run 20260725: leadership flapped for an hour, the
    monitor re-fired async deletes each cycle (18k phase-1, zero phase-2)
    and raised "Failed to get leader node" 244k times while sync deletes
    that needed no leader were never sent."""
    logger.info(f"Snapshot deleted successfully, id: {snap.get_id()}")

    snode = db.get_storage_node_by_id(snap.lvol.node_id)

    # special_delete (SPDK migration_flag) is set ONLY when the SAME snapshot
    # exists on more than one node (lvol migration placed a copy elsewhere).
    # snap.instances holds those extra node-copies and is empty for a
    # home-node-only snapshot; it is NOT grown by local clones. Do not use the
    # blob open_ref>1, which local clones bump and a clone-entry leak strands.
    special_delete = len(snap.instances) > 0

    primary_node = db.get_storage_node_by_id(completed_node.get_id())

    # Every LVS member other than the phase-1 node owes a sync delete (the
    # sync pass clears the peers' lvol registrations; it is per-node and
    # needs no leadership).
    non_leaders = []
    secondary_ids = []
    if snode.secondary_node_id:
        secondary_ids.append(snode.secondary_node_id)
    if snode.tertiary_node_id:
        secondary_ids.append(snode.tertiary_node_id)
    if snode.get_id() != primary_node.get_id():
        non_leaders.append(db.get_storage_node_by_id(snode.get_id()))
    for sec_id in secondary_ids:
        if sec_id != primary_node.get_id():
            non_leaders.append(db.get_storage_node_by_id(sec_id))

    lvol_bdev_name = snap.snap_bdev
    lvs_name = snap.lvol.lvs_name
    # Mutual exclusion covers the RPC phase only, and it must use the SAME key
    # space as the creators: lvstore_op_lock is keyed "<lvs>@<node8>" when a
    # node is named, so a sync delete on a node excludes a create/delete/resize
    # of ANY other object (lvol, snapshot, clone) on that node's copy of the
    # lvstore — which is the blob-tree invariant. A whole-lvstore key would
    # take a DIFFERENT key and exclude nothing the creators hold. The DB
    # finalize below (events, unindex, remove) stays outside the lock.
    if primary_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        any_sec_down = any(
            nl.status in [StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE]
            for nl in non_leaders)
        if any_sec_down:
            primary_node.lvol_del_sync_lock()
        # The leader NEEDS its sync delete: the async delete only clears data
        # clusters — blob metadata and bdev registration survive until this
        # call (see the corrected rationale in lvol_monitor
        # process_lvol_delete_finish; leak evidence: upgrade run 20260812).
        # The "Clone entry not found" errors it produces are benign noise
        # from entries the async pass already stripped.
        with snapshot_controller.lvstore_op_lock(
                snap.cluster_id, lvs_name, node_id=primary_node.get_id()):
            ret, _ = primary_node.rpc_client().delete_lvol(snap.snap_bdev, sync=True, special_delete=special_delete)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")

    for non_leader in non_leaders:
        if non_leader.status in [StorageNode.STATUS_ONLINE]:
            logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {non_leader.get_id()}")
            with snapshot_controller.lvstore_op_lock(
                    snap.cluster_id, lvs_name, node_id=non_leader.get_id()):
                ret, err = non_leader.rpc_client().delete_lvol(lvol_bdev_name, sync=True, special_delete=special_delete)
            if not ret:
                if "code" in err and err["code"] == -19:
                    logger.error(f"Sync delete completed with error: {err}")
                else:
                    msg = f"Failed to sync delete bdev: {lvol_bdev_name} from node: {non_leader.get_id()}, adding task..."
                    logger.error(msg)
                    tasks_controller.add_lvol_sync_del_task(non_leader.cluster_id, non_leader.get_id(), lvol_bdev_name, primary_node.get_id())

        elif non_leader.status in [StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE]:
            tasks_controller.add_lvol_sync_del_task(non_leader.cluster_id, non_leader.get_id(), lvol_bdev_name, primary_node.get_id())

    if snap.instances:
        logger.info("Snapshot has instances, processing them...")
        new_main_instance = SnapShot(snap.instances[0])
        if len(snap.instances) > 1:
            new_main_instance.instances = snap.instances[1:]
        else:
            new_main_instance.instances = []
        logger.info(f"Remaining instances: {len(new_main_instance.instances)}")
        new_main_instance.status = SnapShot.STATUS_IN_DELETION
        new_main_instance.deletion_status = ""
        new_main_instance.write_to_db()
        snode = db.get_storage_node_by_id(new_main_instance.lvol.node_id)
        logger.info(f"Process Snapshot delete on node {snode.get_id()}")
        process_snap_delete(new_main_instance, snode)
    else:
        snapshot_events.snapshot_delete(snap)
        db.unindex_snapshot(snap)
        snap.remove(db.kv_store)


def process_snap_delete_try_again(snap):
    snap = db.get_snapshot_by_id(snap.get_id())
    snap.deletion_status = ""
    snap.write_to_db()


def set_snap_offline(snap):
    sn = db.get_snapshot_by_id(snap.get_id())
    sn.deletion_status = ""
    sn.status = SnapShot.STATUS_OFFLINE
    sn.write_to_db()


# Rate-limited leaderless diagnostics: lvs_name -> (last_warn_monotonic,
# suppressed_count). Run 20260725: the per-snapshot "Failed to get leader
# node" raise fired 244,072 times in one hour — one line per in-deletion
# snapshot per cycle — while phase-2 work that needed no leader sat undone.
_leaderless_warn_memo: dict = {}
_LEADERLESS_WARN_INTERVAL_SEC = 60


def _warn_leaderless(lvs_name):
    now = time.monotonic()
    last, suppressed = _leaderless_warn_memo.get(lvs_name, (0.0, 0))
    if now - last >= _LEADERLESS_WARN_INTERVAL_SEC:
        logger.warning(
            f"No confirmed leader for {lvs_name} — snapshot deletes needing "
            f"phase-1 are deferred ({suppressed} similar messages suppressed "
            f"in the last {_LEADERLESS_WARN_INTERVAL_SEC}s)")
        _leaderless_warn_memo[lvs_name] = (now, 0)
    else:
        _leaderless_warn_memo[lvs_name] = (last, suppressed + 1)


def _poll_delete_status(node, bdev_name):
    """Delete-status poll on ``node``; returns the status int or None when the
    node cannot answer (unreachable / bad state)."""
    if node is None or node.status not in [
            StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
            StorageNode.STATUS_DOWN]:
        return None
    try:
        return node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
    except Exception as e:
        logger.error(f"delete-status poll for {bdev_name} on "
                     f"{node.get_id()[:8]} failed: {e}")
        return None


def process_snap_delete(snap, snode, all_mini_lvols=None, leader_cache=None):
    # check leadership — cached per monitor cycle when the caller provides
    # ``leader_cache``: the probe costs 1-3 get_lvstores RPCs and previously
    # ran once PER SNAPSHOT, adding tens of seconds to a mass-delete cycle.
    # Leadership moves mid-cycle are handled by the poll's error codes
    # (-35/4 reset deletion_status and re-resolve next cycle).
    leader_node = None
    if leader_cache is not None and snode.get_id() in leader_cache:
        leader_node = leader_cache[snode.get_id()]
    if leader_node is None:
        if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                            StorageNode.STATUS_DOWN]:
            try:
                ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
            except Exception:
                ret = None
            if ret:
                lvs_info = ret[0]
                if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                    leader_node = snode

        if not leader_node:
            for peer_id in [snode.secondary_node_id, snode.tertiary_node_id]:
                if not peer_id:
                    continue
                try:
                    sec_node = db.get_storage_node_by_id(peer_id)
                except KeyError:
                    continue
                if sec_node.status not in [StorageNode.STATUS_ONLINE,
                                           StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                    continue
                try:
                    ret = sec_node.rpc_client().bdev_lvol_get_lvstores(sec_node.lvstore)
                except Exception:
                    continue
                if not ret:
                    continue
                lvs_info = ret[0]
                if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                    leader_node = sec_node
                    break
        if leader_node is not None and leader_cache is not None:
            leader_cache[snode.get_id()] = leader_node

    if all_mini_lvols is None:
        all_mini_lvols = db.get_mini_lvols()
    for lvol in all_mini_lvols:
        if lvol.cloned_from_snap and lvol.cloned_from_snap == snap.get_id():
            if lvol.status != SnapShot.STATUS_IN_DELETION:
                # A LIVE clone must block the snapshot's hard-delete. Only
                # in-deletion clones were treated as blockers here, so a healthy
                # clone did not stop the delete at all: a cross-cluster
                # fail-over volume (cloned from the last replicated target
                # snapshot) had its parent removed ~40 min after it was created
                # and every read then returned zeros — no filesystem, md5
                # mismatch, while every status field still said success
                # (lab runs 2026-08-10 / 2026-08-11).
                #
                # snapshot_controller._delete_locked already treats a live clone
                # as blocking (it soft-deletes and keeps the blob); this path is
                # the deferred finalisation of that same delete and has to honour
                # the same rule, or it undoes it.
                try:
                    fresh = db.get_lvol_by_id(lvol.get_id())
                except KeyError:
                    continue  # clone is gone — no longer a blocker
                if fresh.status != SnapShot.STATUS_IN_DELETION:
                    logger.warning(
                        "Not deleting snapshot %s: live clone %s still depends on it",
                        snap.get_id(), fresh.get_id())
                    return False
            if lvol.status == SnapShot.STATUS_IN_DELETION:
                # `all_mini_lvols` is a cycle-start snapshot: the clone may
                # have finished (and been removed) earlier in THIS cycle.
                # Trusting the stale record here defers the snapshot a full
                # extra cycle per chain hop (run 20260730: 7-object delete
                # chains stretched to ~50 min). Re-read the single record
                # before blocking.
                try:
                    fresh = db.get_lvol_by_id(lvol.get_id())
                except KeyError:
                    continue  # clone fully deleted — not a blocker
                if fresh.status == SnapShot.STATUS_IN_DELETION:
                    logger.error("Cannot delete snapshot while it's clone is in deletion")
                    return False

    if not leader_node:
        # Phase-2 needs NO leader: sync deletes run per-node and the
        # delete-status poll targets the recorded phase-1 node. Only phase-1
        # (async on the leader) has to wait for leadership. Run 20260725: the
        # unconditional raise here deadlocked 18k snapshot deletes for the
        # whole leadership flap even though their phase-1 had completed.
        if snap.deletion_status:
            try:
                recorded_node = db.get_storage_node_by_id(snap.deletion_status)
            except KeyError:
                recorded_node = None
            st = _poll_delete_status(recorded_node, snap.snap_bdev)
            if st in (0, 2, -2, 3):  # completed / gone / done-but-leadership-moved
                process_snap_delete_finish(snap, recorded_node)
                return True
            if st == 1:
                logger.info(f"Snap deletion in progress, id: {snap.get_id()}")
                return True
        _warn_leaderless(snap.lvol.lvs_name)
        return False

    special_delete = len(snap.instances) > 0
    if snap.deletion_status == "":
        # Phase-1: async delete on the confirmed leader.
        # See note above: special_delete only for a snapshot copied to another
        # node by lvol migration (snap.instances non-empty), never for a local
        # clone or a stranded blob open_ref.
        ret, _ = leader_node.rpc_client().delete_lvol(snap.snap_bdev, sync=False, special_delete=special_delete)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")
            return False
        snap = db.get_snapshot_by_id(snap.get_id())
        snap.deletion_status = leader_node.get_id()
        snap.write_to_db()
        # NOTE no inline sleep: this loop is serial over every in-deletion
        # snapshot — a 1s pause per phase-1 alone burned ~17 min/hour in run
        # 20260730 and stretched the cycle (and therefore every object's
        # async->sync latency) to 4-5 minutes. The status poll below simply
        # sees "in progress" (1) and retries next cycle.

    elif snap.deletion_status != leader_node.get_id():
        # Leadership moved AFTER phase-1 was issued elsewhere. Never blindly
        # re-issue on the new leader — run 20260725: leadership flapped for an
        # hour and every monitor cycle re-fired phase-1 (18k async deletes,
        # zero phase-2). The recorded node stays authoritative for its own
        # async: re-issue ONLY when it provably lost it (status 4 = no async
        # request exists there / node gone).
        try:
            recorded_node = db.get_storage_node_by_id(snap.deletion_status)
        except KeyError:
            recorded_node = None
        st = _poll_delete_status(recorded_node, snap.snap_bdev)
        if st in (0, 2, -2, 3):
            process_snap_delete_finish(snap, recorded_node)
            return True
        if st == 1:
            logger.info(f"Snap deletion in progress, id: {snap.get_id()}")
            return True
        if st == 4 or recorded_node is None:
            logger.warning(
                f"Phase-1 of {snap.snap_bdev} lost on recorded node "
                f"{snap.deletion_status[:8]} (status {st}) — re-issuing on "
                f"current leader {leader_node.get_id()[:8]}")
            ret, _ = leader_node.rpc_client().delete_lvol(snap.snap_bdev, sync=False, special_delete=special_delete)
            if not ret:
                logger.error(f"Failed to delete snap from node: {leader_node.get_id()}")
                return False
            snap = db.get_snapshot_by_id(snap.get_id())
            snap.deletion_status = leader_node.get_id()
            snap.write_to_db()
            # no inline sleep — see the phase-1 note above
        else:
            # Transient poll error on the recorded node — retry next cycle
            # rather than risking a duplicate phase-1.
            return False

    try:
        ret = leader_node.rpc_client().bdev_lvol_get_lvol_delete_status(snap.snap_bdev)
    except Exception as e:
        logger.error(e)
        # timeout detected, check other node
        return False

    if ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
        process_snap_delete_finish(snap, leader_node)

    elif ret == 1:  # Async lvol deletion is in progress or queued
        logger.info(f"Snap deletion in progress, id: {snap.get_id()}")

    elif ret == 3:  # Async deletion is done, but leadership has changed (sync deletion is now blocked)
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error(
            "Async deletion is done, but leadership has changed (sync deletion is now blocked)")

    elif ret == 4:  # No async delete request exists for this Snap
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("No async delete request exists for this snap")
        set_snap_offline(snap)

    elif ret == -1:  # Operation not permitted
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Operation not permitted")
        process_snap_delete_try_again(snap)

    elif ret == -2:  # No such file or directory
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("No such file or directory")
        process_snap_delete_finish(snap, leader_node)

    elif ret == -5:  # I/O error
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("I/O error")
        process_snap_delete_try_again(snap)

    elif ret == -11:  # Try again
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Try again")
        process_snap_delete_try_again(snap)

    elif ret == -12:  # Out of memory
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Out of memory")
        process_snap_delete_try_again(snap)

    elif ret == -16:  # Device or resource busy
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Device or resource busy")
        process_snap_delete_try_again(snap)

    elif ret == -19:  # No such device
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("No such device")
        set_snap_offline(snap)

    elif ret == -35:  # Leadership changed
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Leadership changed")
        process_snap_delete_try_again(snap)

    elif ret == -36:  # Failed to update lvol for deletion
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Failed to update snapshot for deletion")
        process_snap_delete_try_again(snap)

    else:  # Failed to update lvol for deletion
        logger.info(f"Snap deletion error, id: {snap.get_id()}, error code: {ret}")
        logger.error("Failed to update snapshot for deletion")



# get DB controller
db = db_controller.DBController()


def _due_for_internal_snapshot(lvol, all_snaps, now_ts):
    """Whether an interval-driven internal snapshot is due for *lvol*.

    Replication-enabled volumes with a positive ``replication_interval_min``
    get an automatic internal snapshot every interval; the first one is taken
    immediately (no internal snapshot exists yet). User snapshots do not reset
    the interval.
    """
    if not lvol.do_replicate or lvol.replication_interval_min <= 0:
        return False
    if lvol.status != LVol.STATUS_ONLINE:
        return False
    interval_sec = lvol.replication_interval_min * 60
    last_ts = 0
    for s in all_snaps:
        if (s.lvol.get_id() == lvol.get_id()
                and s.snap_type == SnapShot.TYPE_INTERNAL
                and s.created_at > last_ts):
            last_ts = s.created_at
    return (now_ts - last_ts) >= interval_sec


def take_due_internal_snapshots(cluster_id, now_ts):
    """Create an internal snapshot for every replicated volume whose interval
    has elapsed. The snapshot's creation auto-enqueues a replication task.

    The snapshot listing is only loaded when at least one volume actually
    replicates on an interval — and then from the mini table, which carries
    everything ``_due_for_internal_snapshot`` reads (lvol id, snap_type,
    created_at). The previous unconditional full-SnapShot scan here ran twice
    per monitor cycle and was a steady multi-second FDB load at 10k+
    snapshots (mass-snapshot run 2026-07-21) with zero replicated volumes."""
    repl_lvols = [lv for lv in db.get_lvols(cluster_id)
                  if lv.do_replicate and lv.replication_interval_min > 0]
    if not repl_lvols:
        return
    all_snaps = db.get_mini_snapshots()
    for lvol in repl_lvols:
        try:
            if not _due_for_internal_snapshot(lvol, all_snaps, now_ts):
                continue
            name = f"repl_internal_{lvol.get_id()[:8]}_{now_ts}"
            logger.info(f"Taking internal replication snapshot for lvol {lvol.get_id()}: {name}")
            _id, err = snapshot_controller.add(
                lvol.get_id(), name, snap_type=SnapShot.TYPE_INTERNAL)
            if err:
                logger.warning(f"Internal snapshot for {lvol.get_id()} failed: {err}")
        except Exception as e:
            logger.error(f"Internal snapshot scheduling failed for {lvol.get_id()}: {e}")


def main():
    logger.info("Starting LVol monitor...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        deletions_in_flight = 0
        active_clusters = [c for c in db.get_clusters()
                           if c.status not in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY,
                                               Cluster.STATUS_IN_ACTIVATION]]

        # Creation FIRST, for every cluster, before any delete work. Interval
        # snapshots drive replication, and a delete backlog must never delay
        # them: sharing one serial pass is how a stuck-delete pile stopped
        # snapshot creation entirely (lab 2026-08-14 — 5 replicated volumes
        # went a full hour with zero snapshots while 1298 deletes retried).
        for cluster in active_clusters:
            try:
                take_due_internal_snapshots(cluster.get_id(), int(time.time()))
            except Exception as e:
                logger.error(f"Internal snapshot scheduling failed for cluster {cluster.get_id()}: {e}")

        for cluster in active_clusters:
            # Only in-deletion snapshots need any processing. Find them via
            # the mini table (cheap; no embedded LVol dict) and fetch the few
            # full records individually — the previous full-SnapShot scan per
            # cycle was a steady multi-second FDB read at 10k+ snapshots.
            now_ts = int(time.time())
            in_deletion = [m for m in db.get_mini_snapshots()
                           if m.status == SnapShot.STATUS_IN_DELETION]
            if not in_deletion:
                continue
            deletions_in_flight += len(in_deletion)
            due = [m for m in in_deletion if delete_attempt_due(m.get_id(), now_ts)]
            if not due:
                continue
            snodes = {n.get_id(): n
                      for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())}
            all_mini_lvols = db.get_mini_lvols()
            leader_cache: dict = {}
            leader_cache_guard = threading.Lock()

            # Group by owning volume: a delete chain (clone -> snapshot ->
            # parent) always lives on one volume and must advance in order,
            # so each group is processed serially by a single worker while
            # different volumes run concurrently. Everything the workers do
            # is per-snapshot except the phase-2 sync deletes, which take the
            # per-lvstore mutex in process_snap_delete_finish.
            groups: dict = {}
            for mini in due:
                try:
                    key = mini.lvol.get_id()
                except Exception:
                    key = mini.get_id()
                groups.setdefault(key, []).append(mini)

            def _process_group(minis):
                for mini in minis:
                    try:
                        snap = db.get_snapshot_by_id(mini.get_id())
                    except KeyError:
                        forget_delete_backoff(mini.get_id())
                        continue
                    # Re-check on the authoritative record; also skip snapshots
                    # of other clusters (the mini table is not cluster-scoped).
                    if snap.status != SnapShot.STATUS_IN_DELETION:
                        forget_delete_backoff(mini.get_id())
                        continue
                    if snap.cluster_id and snap.cluster_id != cluster.get_id():
                        continue
                    snode = snodes.get(snap.lvol.node_id)
                    if snode is None:
                        continue
                    progressed = False
                    try:
                        with leader_cache_guard:
                            local_cache = dict(leader_cache)
                        process_snap_delete(snap, snode, all_mini_lvols, local_cache)
                        with leader_cache_guard:
                            leader_cache.update(local_cache)
                        # "Progress" = the record left in_deletion (finished, or
                        # handed to the next hop). Anything else backs off.
                        try:
                            progressed = (db.get_snapshot_by_id(snap.get_id()).status
                                          != SnapShot.STATUS_IN_DELETION)
                        except KeyError:
                            progressed = True
                    except Exception as e:
                        logger.error(e)
                    note_delete_attempt(mini.get_id(), now_ts, progressed)

            workers = max(1, min(constants.SNAP_DELETE_WORKERS, len(groups)))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(_process_group, groups.values()))

        # Adaptive cadence: chained deletes (clone -> snapshot -> parent)
        # advance one hop per cycle, so the idle 30s interval alone adds
        # minutes per chain (run 20260730). Re-scan quickly while snapshots
        # are draining; keep the low-load 30s when idle.
        if deletions_in_flight > 0:
            time.sleep(constants.LVOL_MONITOR_DELETION_INTERVAL_SEC)
        else:
            time.sleep(constants.LVOL_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
