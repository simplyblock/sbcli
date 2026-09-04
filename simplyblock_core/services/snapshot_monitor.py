import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.controllers import (
    snapshot_events, snapshot_controller)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

utils.init_sentry_sdk(__name__)


def set_snapshot_health_check(snap, health_check_status):
    snap = db.get_snapshot_by_id(snap.get_id())
    if snap.health_check == health_check_status:
        return
    snap.health_check = health_check_status
    snap.updated_at = str(datetime.now())
    snap.write_to_db()


def _await_delete_completion(node, bdev_name, wait_sec):
    """Poll the async-delete status until it leaves "in progress" (1) or the
    budget runs out. Returns the last status seen."""
    deadline = time.time() + max(0, wait_sec)
    ret = node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
    while ret == 1 and time.time() < deadline:
        time.sleep(0.2)
        ret = node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
    return ret


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
        # Inner lock: synchronous single-node operations are mutually exclusive
        # per node (same key space as the creators, "<lvs>@<node8>"). The chain
        # lock is already held by the caller — outer chain, inner node, always
        # in that order.
        with snapshot_controller.lvstore_op_lock(
                snap.cluster_id, snap.lvol.lvs_name, node_id=primary_node.get_id()):
            ret = snapshot_controller.delete_bdev_absent_ok(
                primary_node, snap.snap_bdev, sync=True, special_delete=special_delete)
        if not ret:
            logger.error(f"Failed to delete snap from node: {snode.get_id()}")

    lvol_bdev_name = snap.snap_bdev
    for non_leader in non_leaders:
        # Attempt first, classify afterwards: a peer that is merely suspended
        # is still up and can clear its registration, and one that is gone owes
        # nothing at all. Only a failure on a live peer earns a retry task.
        logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {non_leader.get_id()}")
        with snapshot_controller.lvstore_op_lock(
                snap.cluster_id, snap.lvol.lvs_name, node_id=non_leader.get_id()):
            snapshot_controller.sync_delete_on_peer(
                non_leader, lvol_bdev_name, primary_node.get_id(),
                special_delete=special_delete)

    # Release the primary's del-sync gate. It is set above whenever a peer
    # looked down, it BLOCKS snapshot/lvol creation on this node, and it is
    # cleared only by the sync-del task runner — so once a peer is handled
    # inline (or owes nothing at all) nothing would ever clear it and creation
    # on this node stops for good. Reset re-checks for pending sync-del tasks
    # and keeps the gate only while some remain.
    primary_node.lvol_del_sync_lock_reset()

    if snap.instances:
        # Hand the delete on to the next copy. The instance is a DIFFERENT
        # record (its own uuid, node and bdev) and it inherits the rest of the
        # chain, so once it is written this record has nothing left to do and
        # must be retired -- exactly like the no-instances case below.
        #
        # Leaving it behind made the delete non-terminating: the record stayed
        # in_deletion with its instances list intact, so every monitor cycle
        # re-ran phase-2 for it, logged "Snapshot deleted successfully", and
        # rewrote the instance record to in_deletion again -- resurrecting a
        # copy that had already been deleted. Lab 2026-08-20: 104 snapshots
        # frozen for 40+ minutes with no errors at all, 869 "Snapshot has
        # instances" per 2 minutes; the 104 WITHOUT instances drained fine.
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
        # Retire this record only after the successor is durable, so a crash in
        # between loses nothing: worst case the hand-off is repeated. No delete
        # event here -- the snapshot is not gone until its last copy is, which
        # is the branch below.
        db.unindex_snapshot(snap)
        snap.remove(db.kv_store)
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
        ret = snapshot_controller.delete_bdev_absent_ok(
            leader_node, snap.snap_bdev, sync=False, special_delete=special_delete)
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
            ret = snapshot_controller.delete_bdev_absent_ok(
                leader_node, snap.snap_bdev, sync=False, special_delete=special_delete)
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
        # Bounded wait so phase-1 (async) and phase-2 (the 2-3 sync deletes)
        # complete inside ONE chain-lock hold: the whole delete of a chain
        # member must be atomic against any other operation in that chain,
        # because a delete swap-merges segments into the neighbouring snapshot
        # and re-links parents. Returning here on "in progress" would release
        # the chain and let a create/clone/delete interleave between the async
        # and the syncs. If the async outlives the budget we release and retry
        # on a later cycle — still one uninterrupted attempt each time.
        ret = _await_delete_completion(
            leader_node, snap.snap_bdev,
            constants.SNAP_DELETE_COMPLETION_WAIT_SEC)
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


def _outstanding_internal_snapshot(lvol, all_snaps):
    """The newest internal snapshot of *lvol* that has NOT reached the remote
    side yet, or None when the last one completed.

    A transfer that has not finished must not be followed by another. The
    interval is a cadence for a pipeline that keeps up, not a licence to queue
    work without bound: whenever transfers stall — a full initial sync into a
    second destination, a slow link, a wedged gateway — a purely time-driven
    cadence mints another snapshot, and with it another REP_* landing volume on
    the receiving node, every interval.

    Lab 2026-08-20 (case 4): 5 volumes at a 1-minute cadence put 75 REP_*
    volumes on one receiving node in 20 minutes and pinned it at its subsystem
    cap. Retention could not reclaim any of them, because it only prunes
    snapshots that DID replicate (they carry target_replicated_snap_uuid), so
    the pile-up was self-sustaining: the more it grew, the less chance any
    single transfer had of finishing.

    The mini table does not carry the replicated-counterpart uuids, so the full
    record is read — but only for a volume that is already due, i.e. at most
    once per volume per interval, never per monitor cycle.
    """
    newest = None
    for s in all_snaps:
        if (s.lvol.get_id() == lvol.get_id()
                and s.snap_type == SnapShot.TYPE_INTERNAL
                and s.status != SnapShot.STATUS_IN_DELETION
                and (newest is None or s.created_at > newest.created_at)):
            newest = s
    if newest is None:
        return None                       # first internal snapshot of this volume
    try:
        full = db.get_snapshot_by_id(newest.get_id())
    except KeyError:
        return None                       # vanished under us: nothing outstanding
    # Either direction counts as delivered: to-target replication records the
    # target copy, fail-back records the source copy.
    if full.target_replicated_snap_uuid or full.source_replicated_snap_uuid:
        return None
    # Hold back only while the transfer is still LIVE. "Never start the next
    # one until this one finishes" is right for a transfer in progress, but
    # conditioning it on a marker that may never arrive turns one stuck
    # transfer into a permanent stop: the volume takes no further snapshots,
    # nothing is queued, nothing is in flight, and no error is raised.
    #
    # That is what a chaining bug did on 2026-08-20 -- transfers refused to
    # finalize, so the marker never came and the cadence froze silently for the
    # rest of the run (case 4: outstanding=0 and not one new snapshot in 20
    # minutes). The chaining bug is fixed, but the guard must not be able to
    # convert ANY future terminal failure into a silent halt.
    if not _replication_task_is_live(full):
        logger.error(
            "Internal snapshot %s of lvol %s never replicated and has no live "
            "replication task; resuming the cadence. Replication for this "
            "volume is NOT making progress -- investigate the transfer.",
            full.get_id(), lvol.get_id())
        return None
    return full


def _replication_task_is_live(snapshot):
    """Whether a replication task for *snapshot* can still make progress.

    A task that is DONE (successfully or having given up) or cancelled will
    never set the replicated marker, so waiting on it is waiting for ever.
    When the tasks cannot be read at all, assume live: back-pressure staying on
    is the conservative side of that guess.
    """
    try:
        node = db.get_storage_node_by_id(snapshot.lvol.node_id)
        tasks = db.get_job_tasks(node.cluster_id)
    except Exception as e:
        logger.warning("Cannot read replication tasks for %s (%s); keeping "
                       "back-pressure on", snapshot.get_id(), e)
        return True
    for task in tasks:
        if task.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
            continue
        if task.function_params.get("snapshot_id") != snapshot.get_id():
            continue
        if task.status != JobSchedule.STATUS_DONE and not task.canceled:
            return True
    return False


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
            outstanding = _outstanding_internal_snapshot(lvol, all_snaps)
            if outstanding is not None:
                logger.warning(
                    "Skipping internal replication snapshot for lvol %s: the "
                    "previous one (%s, taken %ss ago) has not replicated yet",
                    lvol.get_id(), outstanding.get_id(),
                    max(0, now_ts - outstanding.created_at))
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
        for cluster in db.get_clusters():

            if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
                logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
                continue

            try:
                take_due_internal_snapshots(cluster.get_id(), int(time.time()))
            except Exception as e:
                logger.error(f"Internal snapshot scheduling failed for cluster {cluster.get_id()}: {e}")

            # Only in-deletion snapshots need any processing. Find them via
            # the mini table (cheap; no embedded LVol dict) and fetch the few
            # full records individually — the previous full-SnapShot scan per
            # cycle was a steady multi-second FDB read at 10k+ snapshots.
            in_deletion = [m for m in db.get_mini_snapshots()
                           if m.status == SnapShot.STATUS_IN_DELETION]
            if not in_deletion:
                continue
            deletions_in_flight += len(in_deletion)
            snodes = {n.get_id(): n
                      for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())}
            all_mini_lvols = db.get_mini_lvols()
            leader_cache: dict = {}
            leader_cache_guard = threading.Lock()

            # Chains are independent: no blob link crosses them, so they delete
            # in parallel. Members of ONE chain must not run concurrently (a
            # delete swap-merges into its neighbour), so each chain's records
            # go to a single worker AND every delete takes the chain lock,
            # which also excludes creates/clones in that chain running in other
            # CP processes.
            chains: dict = {}
            for mini in in_deletion:
                try:
                    root, _lvs = snapshot_controller.resolve_chain_root(mini.get_id())
                except Exception:
                    root = mini.get_id()
                chains.setdefault(root, []).append(mini)

            def _process_chain(minis, cluster=cluster, snodes=snodes,
                               all_mini_lvols=all_mini_lvols, leader_cache=leader_cache,
                               leader_cache_guard=leader_cache_guard):
                for mini in minis:
                    try:
                        snap = db.get_snapshot_by_id(mini.get_id())
                    except KeyError:
                        continue
                    # Re-check on the authoritative record; also skip snapshots
                    # of other clusters (the mini table is not cluster-scoped).
                    if snap.status != SnapShot.STATUS_IN_DELETION:
                        continue
                    if snap.cluster_id and snap.cluster_id != cluster.get_id():
                        continue
                    snode = snodes.get(snap.lvol.node_id)
                    if snode is None:
                        continue
                    try:
                        with leader_cache_guard:
                            local_cache = dict(leader_cache)
                        # The whole delete — async, then the sync deletes in
                        # process_snap_delete_finish — runs inside this lock.
                        # The inner recursion (next instance of the same
                        # snapshot) is already under it and must NOT re-acquire.
                        with snapshot_controller.object_mutation_lock(
                                snap.cluster_id or cluster.get_id(), snap.get_id()):
                            process_snap_delete(snap, snode, all_mini_lvols, local_cache)
                        with leader_cache_guard:
                            leader_cache.update(local_cache)
                    except Exception as e:
                        logger.error(e)

            workers = max(1, min(constants.CHAIN_DELETE_WORKERS, len(chains)))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(_process_chain, chains.values()))

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
