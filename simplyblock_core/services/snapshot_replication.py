import logging
import time
import uuid

from simplyblock_core import (constants, db_controller, snapshot_retention,
                              utils, xfer_timing)
from simplyblock_core.controllers import lvol_controller, snapshot_events, snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)
# get DB controller
db = db_controller.DBController()


def _policy_target_pool(lvol, destination_cluster_id):
    """The pool the volume's replication target names on *destination_cluster_id*."""
    policy = db.get_replication_policy_for_lvol(lvol)
    if policy is None:
        return None
    try:
        target = db.get_replication_target_by_id(policy.target_id)
    except KeyError:
        return None
    if target.target_cluster_id != destination_cluster_id:
        return None
    return target.target_pool_uuid or None


def _destination_pool_uuid(remote_node, lvol=None, source_cluster_id=None):
    """The pool a replicated copy should be created in on *remote_node*.

    Most specific wins:

      1. the pool named by the replication TARGET the volume's policy points at,
         when that target IS this destination — it was chosen for this pair;
      2. the SOURCE cluster's snapshot_replication_target_pool, but only when
         that cluster's configured destination is this one;
      3. the first ACTIVE pool on the destination cluster.

    Step 2 used to be unconditional and was read off the DESTINATION cluster.
    That field is outgoing config -- "the pool I replicate into on my target" --
    so any cluster that is itself a source handed out a pool belonging to a
    third cluster as soon as data came back the other way. Lab 2026-08-19: a
    fail-back into the src cluster placed its REP_* volumes in the tgt cluster's
    pool, and 13 of them ended up stuck in_deletion.
    """
    if lvol is not None:
        pool_uuid = _policy_target_pool(lvol, remote_node.cluster_id)
        if pool_uuid:
            return pool_uuid
    if source_cluster_id:
        source_cluster = db.get_cluster_by_id(source_cluster_id)
        if (source_cluster.snapshot_replication_target_cluster == remote_node.cluster_id
                and source_cluster.snapshot_replication_target_pool):
            return source_cluster.snapshot_replication_target_pool
    for pool in db.get_pools(remote_node.cluster_id):
        if pool.status == Pool.STATUS_ACTIVE:
            return pool.uuid
    return None


def _unreplicated_local_ancestor(snode, snapshot, replicate_to_source):
    """The deepest chain ancestor of *snapshot* that must replicate FIRST.

    bdev_lvol_transfer sends a blob's OWN cluster map and nothing else
    (prepare_s3_clusters copies blob->active.clusters; inherited clusters are 0
    there and are skipped). The remote image is therefore complete only if
    every blob between the remote chain base and this snapshot is transferred
    too, bottom-up. Two things put such blobs in the chain:

      * a fail-over volume is a CLONE — its whole pre-fail-over history lives
        in base snapshots (lab 2026-08-20 case 4: XFS AG3, written once at
        mkfs, was zeros on the fresh cluster; everything fio rewrote after the
        fail-over arrived fine);
      * a USER snapshot between two internal cadence snapshots absorbs the
        writes made before it — the next internal snapshot's own map no longer
        contains them.

    Returns ``(verdict, record, why)``: ``("ok", None, "")`` when the chain
    base below is already replicated (or the snapshot is a self-contained
    root); ``("pending", rec, "")`` naming the DEEPEST unreplicated ancestor —
    replicate that one first and re-check (bottom-up order falls out of
    retrying); ``("blocked", rec_or_None, why)`` when the chain cannot be made
    complete (an ancestor is mid-deletion, or a blob has no snapshot record).

    Races: the walk is repeated on every attempt, so a concurrent user delete
    of an ancestor is harmless — its segments swap-merge into the successor,
    and the next walk sees the new chain. Deleting an ancestor of a snapshot
    that is TRANSFERRING is refused in snapshot_controller.delete (the merge
    would mutate the map mid-transfer).
    """
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    lvs = snapshot.snap_bdev.split("/")[0]
    by_bdev = {}
    for s in db.get_snapshots_by_node_id(snapshot.lvol.node_id):
        by_bdev[s.snap_bdev] = s
    rpc = snode.rpc_client()
    cur = snapshot.snap_bdev
    deepest = None
    for _ in range(64):
        ret = rpc.get_bdevs(cur)
        if not ret:
            return ("blocked", None,
                    f"chain bdev {cur} not readable on {snode.get_id()}")
        base = ((ret[0].get("driver_specific") or {}).get("lvol") or {}).get("base_snapshot")
        if not base:
            # Chain root: a self-contained blob, transferable in full.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        cur = f"{lvs}/{base}"
        rec = by_bdev.get(cur)
        if rec is None:
            return ("blocked", None,
                    f"chain blob {cur} has no snapshot record; its data cannot "
                    f"be replicated and the copy would have holes")
        if getattr(rec, attr, ""):
            # Everything below this point already exists on the remote side.
            return ("pending", deepest, "") if deepest else ("ok", None, "")
        if rec.status == SnapShot.STATUS_IN_DELETION:
            return ("blocked", rec,
                    f"chain ancestor {rec.get_id()} is mid-deletion")
        deepest = rec
    return ("blocked", None, "chain deeper than 64 blobs")


def _group_id_for_lvol(lvol):
    """The consistency group *lvol* belongs to, or "".

    A group is owned by a replication policy and pinned to one node/LVS.
    """
    policy_id = getattr(lvol, "replication_policy_id", "")
    if not policy_id:
        return ""
    try:
        group = db.get_consistency_group_for_policy(policy_id)
    except Exception as e:                              # noqa: BLE001
        logger.warning("Could not resolve the consistency group of %s: %s",
                       lvol.get_id(), e)
        return ""
    return group.get_id() if group else ""


def _lvs_transfer_hold(task, snapshot):
    """Why this transfer must wait, or "" when it may start now.

    Two priorities share one lvstore's bandwidth:

    1. A volume in its FINAL CUTOVER owns the lvstore. Its convergence rounds
       decide how long client IO freezes -- every second another volume steals
       from a round is a second of writes the frozen final step must copy --
       so nothing else on that lvstore transfers meanwhile. Members of the same
       consistency group are exempt: the group cuts over together.

    2. Consistency groups outrank loose volumes. A group's members transfer in
       PARALLEL with each other (their snapshots belong to one generation and
       are only useful together), and everything else on the lvstore waits, so
       groups are effectively serialized against each other rather than
       interleaved.
    """
    own_lvol = getattr(snapshot, "lvol", None)
    lvs_name = getattr(own_lvol, "lvs_name", "") if own_lvol else ""
    if own_lvol is None or not lvs_name:
        return ""
    own_id = own_lvol.get_id()
    own_group = _group_id_for_lvol(own_lvol)

    tasks = db.get_job_tasks(task.cluster_id)

    # --- priority 1: a cutover in progress on this lvstore -----------------
    for t in tasks:
        if t.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if t.status == JobSchedule.STATUS_DONE or t.canceled:
            continue
        params = t.function_params or {}
        if params.get("cutover_lvs") != lvs_name:
            continue
        holder = params.get("lvol_id")
        if not holder or holder == own_id:
            return ""                       # our own cutover: keep moving
        holder_group = params.get("cutover_group") or ""
        if holder_group and holder_group == own_group:
            return ""                       # same group: cut over together
        return (f"lvol {holder[:8]} is in final cutover on lvstore {lvs_name}")

    # --- priority 2: a consistency group is transferring on this lvstore ---
    for t in tasks:
        if t.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
            continue
        if t.status != JobSchedule.STATUS_RUNNING or t.get_id() == task.get_id():
            continue
        other_snap_id = (t.function_params or {}).get("snapshot_id")
        if not other_snap_id:
            continue
        try:
            other_lvol = db.get_snapshot_by_id(other_snap_id).lvol
        except KeyError:
            continue
        if getattr(other_lvol, "lvs_name", "") != lvs_name:
            continue
        other_group = _group_id_for_lvol(other_lvol)
        if other_group and other_group != own_group:
            return (f"consistency group {other_group.split('/')[-1][:8]} is "
                    f"transferring on lvstore {lvs_name}")

    return ""


def _finish_completed_transfer(task, snapshot, offset):
    """Chain, convert and mark the snapshot replicated. Returns True.

    This is what sets target_replicated_snap_uuid, which is the signal a
    cutover's convergence loop waits on -- so it must run as soon as the
    transfer is known to be finished, not on some later pass.
    """
    # The data is on the landing volume now. Record it BEFORE finishing: the
    # finish converts the landing volume into a snapshot node by node, and a
    # retry after a partial finish must resume the finish, never re-send into
    # a volume that is already a snapshot on some members (see
    # _resume_finish).
    if not task.function_params.get("transfer_done"):
        task.function_params["transfer_done"] = True
        task.write_to_db()
    # submit -> Done, measured. NOT a throughput: see fix_xfer_latency notes.
    xfer_timing.gap("transfer_complete",
                    task.function_params.get("xfer_submit_t"),
                    snap=snapshot.get_id(), lvol=snapshot.lvol.get_id(),
                    bytes=offset)
    with xfer_timing.phase("replicate_finish", snap=snapshot.get_id(),
                           lvol=snapshot.lvol.get_id()):
        new_snapshot_uuid = process_snap_replicate_finish(task, snapshot)
    if new_snapshot_uuid:
        task.function_result = new_snapshot_uuid
        task.status = JobSchedule.STATUS_DONE
        task.function_params["end_time"] = int(time.time())
        task.write_to_db()
    else:
        _suspend_for_retry(task, "complete repl failed, retrying", backoff=True)
    return True


def _backoff_seconds(retry):
    """Delay before the next attempt after a failure that touched the target."""
    return min(constants.REPL_RETRY_BACKOFF_BASE_SEC * (2 ** min(retry, 6)),
               constants.REPL_RETRY_BACKOFF_MAX_SEC)


def _suspend_for_retry(task, msg, backoff=False, count=True, level=logging.WARNING):
    """Suspend *task* for another attempt, and say why.

    Every retry is logged: several of these paths used to count a retry
    silently, so a volume could run all its tasks into max retry without one
    line in the log naming the reason (2026-09-29: LVS_1 on the source kept
    losing its leader; vm-a's tasks gave up unseen, and a fail-over later
    found nothing on the target).

    ``backoff`` delays the next attempt after a failure that involved the
    target (a failed transfer or finish): an immediate retry repeats whatever
    the target did, and on 2026-09-29 each retry into a half-converted landing
    volume fenced the target LVS again, 14 times in 11 minutes.

    ``count`` is False for waiting conditions that are not a fault of the
    transfer (no leader on the source right now): those do not spend retries,
    or a leaderless window of half a minute is enough to kill every task.
    """
    logger.log(level, "Replication task %s (snapshot %s): %s",
               task.uuid, task.function_params.get("snapshot_id"), msg)
    task.function_result = msg
    task.function_params["last_error"] = msg
    task.status = JobSchedule.STATUS_SUSPENDED
    if count:
        task.retry += 1
    if backoff:
        task.function_params["not_before"] = int(time.time()) + _backoff_seconds(task.retry)
    task.write_to_db()


def _resume_finish(task, snapshot):
    """Finish a transfer that already completed; True if this handled the task.

    A finish that failed half-way (converted on the primary, failed on the
    secondary) leaves the landing volume a snapshot on some members. Starting
    over re-sent the transfer into it: every write failed, and the target LVS
    dropped its leadership and fenced its ports on each attempt (2026-09-29,
    LVS_1 on site A, 14 times). The data is complete, so resume the finish
    instead: chain and convert where it has not happened yet.
    """
    if not task.function_params.get("transfer_done"):
        return False
    remote_lv_id = task.function_params.get("remote_lvol_id")
    remote_lv = None
    if remote_lv_id:
        try:
            remote_lv = db.get_lvol_by_id(remote_lv_id)
        except KeyError:
            remote_lv = None
    if remote_lv is None or remote_lv.status == LVol.STATUS_IN_DELETION:
        # The landing volume is gone: nothing to resume, start over.
        logger.warning("Replication task %s: landing volume %s is gone; "
                       "starting the transfer over", task.uuid, remote_lv_id)
        for key in ("transfer_done", "converted_nodes", "remote_lvol_id", "offset"):
            task.function_params.pop(key, None)
        task.write_to_db()
        return False
    logger.info("Replication task %s: transfer of %s already completed; "
                "resuming the finish (converted on: %s)", task.uuid,
                snapshot.get_id(), task.function_params.get("converted_nodes") or "none")
    _finish_completed_transfer(task, snapshot, task.function_params.get("offset"))
    return True


def _cutover_owns(lvol_id, cluster_id):
    """True while a final cutover is running for this volume.

    Such a volume already holds its lvstore and every other transfer on it is
    held, so waiting inline for its transfer starves nothing -- and this is the
    window the client's IO freeze is paying for.
    """
    if not lvol_id:
        return False
    try:
        tasks = db.get_job_tasks(cluster_id)
    except Exception:                                     # noqa: BLE001
        return False
    for other in tasks:
        if other.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if other.status == JobSchedule.STATUS_DONE or other.canceled:
            continue
        if (other.function_params or {}).get("lvol_id") == lvol_id:
            return True
    return False


def _await_transfer_completion(task, snapshot, snode):
    """Poll the just-submitted transfer at 100ms and finish it in this pass.

    Returns True when the transfer completed and was finished here; False to
    leave it for the pass-based path (still in flight, or the budget ran out).

    Before this, submit and the Done check happened on DIFFERENT passes of the
    runner loop, so a transfer that finished in milliseconds was not acted on
    for a median of 81 SECONDS (run 20260828_115307: every poll found state
    already 'Done', never once 'In progress').
    """
    budget = (constants.REPL_XFER_INLINE_WAIT_CUTOVER_SEC
              if _cutover_owns(snapshot.lvol.get_id(), task.cluster_id)
              else constants.REPL_XFER_INLINE_WAIT_SEC)
    deadline = time.time() + budget
    rpc = snode.rpc_client()
    while time.time() < deadline:
        try:
            ret = rpc.bdev_lvol_transfer_stat(snapshot.snap_bdev)
        except Exception as e:                            # noqa: BLE001
            logger.warning("transfer_stat for %s raised while waiting inline "
                           "(%s); leaving it to the next pass",
                           snapshot.get_id(), e)
            return False
        if not ret:
            return False
        state = ret.get("transfer_state")
        if state == "Done":
            return _finish_completed_transfer(task, snapshot, ret.get("offset"))
        if state == "Failed":
            return False              # the pass-based path records the retry
        if state == "No process":
            return False              # transfer never started; pass-based path retries
        time.sleep(constants.REPL_XFER_POLL_INTERVAL_SEC)
    xfer_timing.stamp("inline_wait_expired", snap=snapshot.get_id(),
                      lvol=snapshot.lvol.get_id(), budget=budget)
    return False


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    _t_landing = None          # set only if we create the landing volume below

    hold = _lvs_transfer_hold(task, snapshot)
    if hold:
        # Not a failure and not a retry: come back when the lvstore frees up.
        task.function_result = f"held: {hold}"
        task.write_to_db()
        logger.info("Holding replication of %s: %s", snapshot.get_id(), hold)
        return False
    # Drive the transfer from whichever member of the SOURCE lvstore leads it
    # now — the snapshot exists on every member, so an outage of the recorded
    # primary must not stop replication (see _source_leader_node).
    snode = _source_leader_node(snapshot) or db.get_storage_node_by_id(snapshot.lvol.node_id)
    replicate_to_source = task.function_params["replicate_to_source"]

    # Once ONLY: snapshots form a TREE through clones, so several descendants
    # share ancestors, and each of their gates enqueues the shared ancestor.
    # _add_task dedupes the ACTIVE task; this guard covers the rest — a task
    # for a snapshot that already has its copy on the remote side (a second
    # enqueue that raced the first one's completion, a stale queue entry) must
    # recognize the copy as existent, not build a second one.
    already = getattr(snapshot,
                      "source_replicated_snap_uuid" if replicate_to_source
                      else "target_replicated_snap_uuid", "")
    if already:
        msg = (f"Snapshot {snapshot.get_id()} is already replicated "
               f"(remote copy {already}); nothing to transfer")
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db()
        return

    # The chain below this snapshot must be on the remote side FIRST, or the
    # copy has holes (see _unreplicated_local_ancestor). Checked on every
    # attempt so chain changes (user deletes swap-merging blobs) re-resolve.
    verdict, ancestor, why = _unreplicated_local_ancestor(
        snode, snapshot, replicate_to_source)
    if verdict == "pending":
        from simplyblock_core.controllers import tasks_controller
        dest_lvol_id = (task.function_params.get("dest_lvol_id")
                        or snapshot.lvol.get_id())
        tasks_controller.add_snapshot_replication_task(
            snapshot.cluster_id, task.node_id, ancestor.get_id(),
            replicate_to_source=replicate_to_source, dest_lvol_id=dest_lvol_id)
        msg = (f"waiting for chain ancestor {ancestor.get_id()} to replicate "
               f"first (a copy without it would have holes)")
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return
    if verdict == "blocked":
        msg = f"replication chain of {snapshot.get_id()} is incomplete: {why}; retrying"
        logger.error(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        if replicate_to_source:
            try:
                remote_node_uuid = db.get_storage_node_by_id(task.node_id)
            except KeyError:
                msg = f"Unable to find node: {task.node_id}, stopping task"
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            # A snapshot only has a counterpart on the destination when it was
            # replicated FROM there. Anything created after the fail-over — and
            # everything at all when failing back to a freshly installed cluster
            # — has source_replicated_snap_uuid empty, and looking that up used
            # to hand an empty id to get_snapshot_by_id, which degenerates into a
            # whole-table scan and dies with "Multiple values present" (348 such
            # failures in labs 2026-08-17/18: every fail-back task died here on
            # its first step, so nothing ever replicated back). Reuse the
            # counterpart's pool when there is one, otherwise resolve the pool on
            # the destination cluster the same way the forward direction does.
            remote_pool_uuid = None
            if snapshot.source_replicated_snap_uuid:
                try:
                    remote_pool_uuid = db.get_snapshot_by_id(
                        snapshot.source_replicated_snap_uuid).lvol.pool_uuid
                except KeyError:
                    logger.warning(
                        "Counterpart snapshot %s of %s is gone; resolving the "
                        "destination pool from the cluster instead",
                        snapshot.source_replicated_snap_uuid, snapshot.get_id())
            if not remote_pool_uuid:
                remote_pool_uuid = _destination_pool_uuid(
                    remote_node_uuid, lvol=snapshot.lvol,
                    source_cluster_id=snode.cluster_id)
            if not remote_pool_uuid:
                logger.error("Unable to find pool on remote cluster: %s",
                             remote_node_uuid.cluster_id)
                return
        else:  # replicate to target
            # A task can outlive the configuration that created it (or be queued
            # for a volume that never had a destination, e.g. a REP_* receiving
            # volume). Retrying it for ever burns the runner's cycles and blocks
            # every delete waiting behind its snapshot, so end it here.
            # A chain-ancestor task replicates a snapshot whose own lvol may be
            # long gone or never configured (a fail-over base chain): the
            # destination then comes from the policy-managed DESCENDANT volume,
            # carried on the task as dest_lvol_id by the ancestor gate.
            dest_lvol = snapshot.lvol
            if not dest_lvol.replication_node_id and task.function_params.get("dest_lvol_id"):
                try:
                    dest_lvol = db.get_lvol_by_id(task.function_params["dest_lvol_id"])
                except KeyError:
                    pass
            if not dest_lvol.replication_node_id:
                msg = (f"LVol {snapshot.lvol.get_id()} has no replication destination; "
                       f"dropping replication task for snapshot {snapshot.get_id()}")
                logger.error(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db()
                return
            remote_node_uuid = db.get_storage_node_by_id(dest_lvol.replication_node_id)
            remote_pool_uuid = _destination_pool_uuid(
                remote_node_uuid, lvol=dest_lvol, source_cluster_id=snode.cluster_id)
            if not remote_pool_uuid:
                logger.error(f"Unable to find pool on remote cluster: {remote_node_uuid.cluster_id}")
                return

        # The destination may already hold this snapshot: after a relocate,
        # the chain of the volume now on this side came FROM the destination,
        # and its originals are still there (same data_uuid). Link to the one
        # on the destination's lvstore instead of shipping it back in full --
        # the full copy also collided with the original's name on 2026-09-29,
        # which broke the finish after the convert and led to writes into the
        # converted landing volume.
        counterpart = _counterpart_on_destination(snapshot, remote_node_uuid)
        if counterpart is not None:
            if replicate_to_source:
                snapshot.source_replicated_snap_uuid = counterpart.get_id()
            else:
                snapshot.target_replicated_snap_uuid = counterpart.get_id()
            snapshot.write_to_db()
            msg = (f"Snapshot {snapshot.get_id()} is already replicated "
                   f"(remote copy {counterpart.get_id()} on {counterpart.lvol.lvs_name}, "
                   f"same data); linked, nothing to transfer")
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return

        # An earlier attempt of THIS task may have created the landing volume
        # and died before storing its id (a node outage mid-create): add_lvol_ha
        # then fails "LVol name must be unique" on EVERY retry and the task
        # loops forever, stalling the volume's whole chain behind it (case 6,
        # run 20260824_144226: three volumes stuck on their first cadence
        # snapshot, retrying every ~31s for the rest of the run). The name is
        # derived from the snapshot, so a record wearing it IS this transfer's
        # landing volume: adopt it when it is usable, clear it when it is not.
        rep_name = f"REP_{snapshot.snap_name}"
        existing = None
        try:
            existing = db.get_lvol_by_name(rep_name, include_deleted=True)
        except KeyError:
            pass
        if existing is not None:
            if existing.status == LVol.STATUS_ONLINE:
                logger.info(f"Adopting landing volume {existing.get_id()} "
                            f"({rep_name}) left by an interrupted attempt")
                task.function_params["remote_lvol_id"] = existing.get_id()
                task.write_to_db()
            elif existing.status == LVol.STATUS_IN_DELETION:
                _suspend_for_retry(task, f"stale landing volume {rep_name} still deleting, retrying")
                return
            else:
                logger.warning(f"Deleting half-created landing volume "
                               f"{existing.get_id()} ({rep_name}, status "
                               f"{existing.status}) from an interrupted attempt")
                try:
                    lvol_controller.delete_lvol(existing, force_delete=True)
                except Exception as e:
                    logger.error(f"Failed to clear stale landing volume {rep_name}: {e}")
                _suspend_for_retry(task, f"cleared stale landing volume {rep_name}, retrying")
                return

    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"]:
        # internal=True: this REP_* volume is the landing copy for a transfer,
        # created by the system and never handed to a client. The per-node
        # subsystem cap is a user-admission limit; enforcing it here only stops
        # replication on a node that is already full, which is precisely when
        # the transfers that would let retention free those slots are needed.
        _t_landing = xfer_timing.now()
        # Carry the source volume's subsystem-packing capacity onto the landing
        # copy. Without it the copy defaults to a one-namespace subsystem, which
        # can never be joined by later namespaced volumes -- so every replicated
        # copy, and every clone taken from it (test-failover, fail-over), lands in
        # its own subsystem at NSID 1, ignoring the source's
        # max_namespace_per_subsys. namespaced is not a persisted field; a
        # max_namespace_per_subsys > 1 IS the "shareable subsystem" signal.
        src_max_ns = snapshot.lvol.max_namespace_per_subsys
        lv_id, err = lvol_controller.add_lvol_ha(
            f"REP_{snapshot.snap_name}", snapshot.size, remote_node_uuid.get_id(), snapshot.lvol.ha_type,
            remote_pool_uuid, internal=True,
            namespaced=src_max_ns > 1,
            max_namespace_per_subsys=src_max_ns)
        if lv_id:
            task.function_params["remote_lvol_id"] = lv_id
            task.write_to_db()
        else:
            logger.error(err)
            task.function_result = "Error creating remote lvol"
            task.write_to_db()
            return

    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    # Send to whichever member of the target lvstore currently leads it — the
    # hub only accepts receive IO on the leader, and leadership does not
    # return to the recorded node on its own after an outage.
    remote_lv_node = _receiving_leader_node(remote_lv)
    if remote_lv_node is None:
        # Leadership does not come back on its own: SPDK drops it on a
        # failed write and the control plane grants it only on restart,
        # activation or its leaderless-LVS recovery -- which nothing ran
        # while this task waited (2026-10-02, LVS_1 on site A leaderless
        # for 20 minutes, every convert refused). Run that recovery now.
        remote_lv_node = _recover_target_leader(remote_lv)
    if remote_lv_node is None:
        # Waiting, not failing: the target's leadership is not this transfer's
        # fault, and counting it let a leaderless window kill the task.
        _suspend_for_retry(task, f"No online LVS leader on the target "
                                 f"({remote_lv.lvs_name}), retrying",
                           backoff=True, count=False)
        return

    # 2 attach the TARGET NODE'S TRANSFER HUBLVOL on the source. Transfers must
    # go over a hublvol: the fork demuxes each write by the map id carried in
    # the top 16 bits of the LBA (lvol_map.lvol[offset >> 48]) and that demux
    # only exists on a hublvol namespace. The receiving volume's own namespace
    # is not a valid transfer gateway. This mirrors the (working) migration
    # runner, which has always sent bulk transfers hub+map_id.
    from simplyblock_core.services.replication_final_step import ensure_hub_attached
    xfer_timing.gap("landing_volume_create", _t_landing,
                    snap=snapshot.get_id(), lvol=snapshot.lvol.get_id())
    with xfer_timing.phase("hub_attach", snap=snapshot.get_id(),
                           lvol=snapshot.lvol.get_id(),
                           tgt=remote_lv_node.get_id()):
        _hub_ctrl, hub_bdev, hub_err = ensure_hub_attached(snode.rpc_client(), remote_lv_node)
    if hub_err:
        logger.error(f"Transfer hub attach failed: {hub_err}")
        _suspend_for_retry(task, f"transfer hub attach failed: {hub_err}, retrying",
                           backoff=True)
        return

    # The receiving volume's map id rides in every write's LBA (see above); the
    # hub uses it to route the data into the receiving volume. Without it the
    # transfer cannot land.
    ret = remote_lv_node.rpc_client().get_bdevs(remote_lv.top_bdev)
    try:
        remote_map_id = ret[0]["driver_specific"]["lvol"]["map_id"]
    except (TypeError, KeyError, IndexError):
        remote_map_id = None
    if not remote_map_id:
        logger.error(f"map_id of receiving lvol {remote_lv.top_bdev} not found on "
                     f"{remote_lv.node_id}; not starting a transfer that cannot land")
        _suspend_for_retry(task, "receiving lvol map_id unavailable, retrying")
        return

    # Never transfer into a snapshot. The landing volume is written ONLY
    # before it is converted; after the convert it is part of the target's
    # snapshot chain, and a write into it fails -- on 2026-09-29 every such
    # write made the target LVS drop its leadership and fence its ports
    # (14 times in 11 minutes). A landing volume that is already a snapshot on
    # any member here, without this task knowing its transfer completed (that
    # case resumes the finish, see _resume_finish), holds data of unknown
    # completeness: discard it and start over on a fresh one.
    converted_on = _landing_volume_snapshot_members(remote_lv)
    if converted_on:
        logger.error("Replication task %s: landing volume %s is already a snapshot on %s; "
                     "refusing to transfer into it, discarding it and starting over",
                     task.uuid, remote_lv.top_bdev, ", ".join(converted_on))
        try:
            lvol_controller.delete_lvol(remote_lv, force_delete=True)
        except Exception as e:                            # noqa: BLE001
            logger.error("Failed to discard landing volume %s: %s", remote_lv.get_id(), e)
        for key in ("remote_lvol_id", "converted_nodes", "offset", "transfer_done"):
            task.function_params.pop(key, None)
        _suspend_for_retry(task, f"landing volume {remote_lv.top_bdev} was already a snapshot "
                                 f"on {', '.join(converted_on)}; discarded, retrying",
                           backoff=True)
        return

    # NOTE deliberately NO bdev_lvol_set_migration_flag here: the flag drives the
    # distrib-level special_io machinery of INTRA-cluster migration; it has no
    # place in a cross-cluster receive (the source cluster's map/COW context does
    # not exist on the target cluster).
    # The hub rejects receive IO on a non-leader ("receive io for hublvol in
    # nonleader mode"); do not start a transfer that cannot land.
    if not _require_lvs_leader(remote_lv_node, remote_lv.lvs_name, "transfer receive"):
        _suspend_for_retry(task, f"target node {remote_lv_node.get_id()} not LVS "
                                 f"leader of {remote_lv.lvs_name}, retrying",
                           backoff=True, count=False)
        return

    allow_partial = _partial_transfer_decision(
        task, snapshot, replicate_to_source, remote_lv, remote_lv_node)
    logger.info("Transfer of %s into %s: %s", snapshot.get_id(),
                remote_lv.top_bdev,
                "PARTIAL allowed (landing volume is a clone of the previous "
                "replicated snapshot on every online member)" if allow_partial
                else "FULL (no delta basis on every online member; see above "
                     "for which check declined)")

    offset = 0
    if task.function_params.get("offset"):
        offset = task.function_params["offset"]

    # Flip to IN_REPLICATION under the CHAIN lock, BEFORE the transfer starts.
    # The delete path refuses to delete a snapshot in this state AND refuses to
    # delete its predecessor (whose swap-merge would mutate the cluster map the
    # transfer is reading) under the same chain-root-keyed lock — setting the
    # status after starting the transfer left a window in which a delete could
    # slip between the check and the merge.
    with snapshot_controller.object_mutation_lock(snapshot.cluster_id, snapshot.get_id()):
        try:
            fresh = db.get_snapshot_by_id(snapshot.get_id())
        except KeyError:
            msg = f"Snapshot {snapshot.get_id()} vanished before transfer start"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        if fresh.status == SnapShot.STATUS_IN_DELETION:
            msg = f"Snapshot {snapshot.get_id()} is being deleted; not starting a transfer"
            logger.error(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db()
            return
        if fresh.status != SnapShot.STATUS_IN_REPLICATION:
            fresh.status = SnapShot.STATUS_IN_REPLICATION
            fresh.write_to_db()

    # 3 start replication
    xfer_timing.stamp("transfer_submit", snap=snapshot.get_id(),
                      lvol=snapshot.lvol.get_id(), size=snapshot.size)
    task.function_params["xfer_submit_t"] = xfer_timing.now()
    task.write_to_db()
    snode.rpc_client().bdev_lvol_transfer(
        name=snapshot.snap_bdev,
        offset=offset,
        # 16 in-flight clusters (32 MiB window). With the dispatch fix keeping
        # the window genuinely full AND reads fragmented like writes (32x64KiB
        # per cluster), 16 clusters already put up to 512 concurrent 64 KiB
        # IOs per phase on the wire -- a 64-window quadruples the DMA buffer
        # (128 MiB per transfer task) for little more overlap.
        batch_size=16,
        bdev_name=hub_bdev,
        operation="replicate",
        lvol_id=remote_map_id,
        # Safe only because the landing volume was chained onto the previous
        # replicated snapshot above; the fork independently refuses the delta
        # path unless the snapshot's dirty generation is complete.
        allow_partial=allow_partial,
    )
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params["start_time"] = int(time.time())
    task.write_to_db()

    # Do not hand the transfer back to the pass loop and forget about it: wait
    # for it here, polling every 100ms, and finish it in this same pass. Before
    # this, submit and the Done check happened on different passes and a
    # transfer that finished in milliseconds went unnoticed for a median of 81
    # SECONDS (run 20260828_115307). Returns False if it is still running when
    # the budget expires, in which case the pass-based path picks it up as
    # before.
    _await_transfer_completion(task, snapshot, snode)


def _counterpart_on_destination(snapshot, remote_node):
    """The destination's copy of *snapshot* on *remote_node*'s lvstore, or None.

    Copies share data_uuid (the finish copies it onto every replicated
    snapshot). Only a copy on the destination's own lvstore counts: a chain
    can only be built on a snapshot in the same lvstore.
    """
    lvstore = getattr(remote_node, "lvstore", "")
    if not snapshot.data_uuid or not lvstore:
        return None
    for cand in db.get_snapshots(remote_node.cluster_id):
        if (cand.get_id() != snapshot.get_id()
                and cand.data_uuid == snapshot.data_uuid
                and cand.status != SnapShot.STATUS_IN_DELETION
                and cand.lvol and cand.lvol.lvs_name == lvstore):
            return cand
    return None


def _landing_volume_snapshot_members(remote_lv):
    """Ids of the online target members on which *remote_lv* is a snapshot.

    Reads SPDK's own view (bdev driver_specific.lvol.snapshot) on every online
    member of the target lvstore: the control plane's record cannot say
    whether an earlier attempt converted the volume on some members only. A
    member that cannot be asked is skipped; the transfer's own leader probe
    decides about it.
    """
    members = []
    for node_id in (getattr(remote_lv, "nodes", None) or [remote_lv.node_id]):
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            ret = node.rpc_client().get_bdevs(remote_lv.top_bdev)
            if ret and ret[0].get("driver_specific", {}).get("lvol", {}).get("snapshot"):
                members.append(node.get_id())
        except Exception as e:                            # noqa: BLE001
            logger.warning("Could not read landing volume %s on %s: %s",
                           remote_lv.top_bdev, node.get_id(), e)
    return members


def _receiving_leader_node(remote_lv):
    """The node that currently leads *remote_lv*'s lvstore, or None.

    The receiving lvol is HA — it exists on every member of the target
    lvstore — but only the LEADER can accept hub receive IO or persist a
    convert. ``remote_lv.node_id`` records where the lvol was created, which
    is not where leadership sits after the target node has been down: case 5
    (target node offline mid-replication) parked leadership on the peer, the
    pinned node kept failing the leadership gate, and the volume retried
    forever without ever converging — lag grew one snapshot per minute while
    the other four volumes replicated normally. Follow leadership instead of
    the recorded node; nothing moves leadership back on its own.
    """
    return _lvs_leader_among(remote_lv.nodes, remote_lv.node_id, remote_lv.lvs_name)


def _recover_target_leader(remote_lv):
    """The control plane's leaderless-LVS recovery for the target lvstore;
    the node that leads afterwards, or None."""
    from simplyblock_core import storage_node_ops
    nodes = []
    for node_id in (getattr(remote_lv, "nodes", None) or [remote_lv.node_id]):
        try:
            nodes.append(db.get_storage_node_by_id(node_id))
        except KeyError:
            continue
    try:
        leader = storage_node_ops.find_leader_with_failover(nodes, remote_lv.lvs_name)
    except Exception as e:  # noqa: BLE001
        logger.warning("Leaderless-LVS recovery of %s failed: %s", remote_lv.lvs_name, e)
        return None
    if isinstance(leader, tuple):
        leader = leader[0]
    if not leader:
        return None
    logger.info("Leadership of %s recovered on %s for the transfer", remote_lv.lvs_name,
                leader.get_id())
    return _receiving_leader_node(remote_lv)


def _secondary_lacks_bdev(node, bdev_name):
    """True when *node* has no bdev *bdev_name* (the add_clone's -19)."""
    try:
        return not node.rpc_client(timeout=10).get_bdevs(bdev_name)
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not probe %s on %s: %s", bdev_name, node.get_id(), e)
        return False


def _lvs_leader_among(nodes_ids, preferred_id, lvs_name):
    """The online node among *nodes_ids* that currently leads *lvs_name*."""
    from simplyblock_core.controllers import lvol_controller
    candidates = []
    for node_id in (nodes_ids or ([preferred_id] if preferred_id else [])):
        try:
            candidates.append(db.get_storage_node_by_id(node_id))
        except KeyError:
            continue
    # Prefer the recorded node while it still leads: keeps a stable home.
    candidates.sort(key=lambda n: n.get_id() != preferred_id)
    for node in candidates:
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            if lvol_controller.is_node_leader(node, lvs_name):
                return node
        except Exception as e:
            logger.warning("Leadership probe failed on %s: %s", node.get_id(), e)
    return None


def _source_leader_node(snapshot):
    """The node that currently leads the SOURCE lvstore, or None.

    A snapshot is registered on every member of its lvstore, so the transfer
    can be driven from whichever member holds leadership — which is the point
    of HA. Pinning to ``snapshot.lvol.node_id`` means an outage of that one
    node stops replication entirely even though the promoted peer serves the
    volume and holds the same snapshot: case 6 (source primary offline,
    secondary survives) saw zero replications during the whole outage, every
    task parked on "node is not online, retrying".
    """
    lv = snapshot.lvol
    return _lvs_leader_among(getattr(lv, "nodes", None), lv.node_id, lv.lvs_name)


def _require_lvs_leader(node, lvs_name, what):
    """True when *node* currently holds LVS leadership for *lvs_name*.

    Transfers into a hub on a non-leader fail loudly, but bdev_lvol_convert on a
    non-leader DEGRADES SILENTLY: the fork's non-leader branch marks the blob
    CLEAN and replies success without persisting anything — the "snapshot"
    looks converted while its metadata never reached the journal. Leadership
    must therefore be verified BEFORE the operation; on False the caller
    suspends and retries rather than proceeding.
    """
    from simplyblock_core.controllers import lvol_controller
    if lvol_controller.is_node_leader(node, lvs_name):
        return True
    logger.error("Node %s is not LVS leader of %s — refusing %s (retry)",
                 node.get_id(), lvs_name, what)
    return False


def _other_active_transfers_to_node(current_task, target_node_id):
    """True when another RUNNING snapshot-replication task is transferring into
    *target_node_id* — its writes ride the same shared hub session, so the hub
    must not be detached under it."""
    for t in db.get_job_tasks(current_task.cluster_id):
        if (t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION
                and t.get_id() != current_task.get_id()
                and t.status == JobSchedule.STATUS_RUNNING):
            rid = t.function_params.get("remote_lvol_id")
            if not rid:
                continue
            try:
                if db.get_lvol_by_id(rid).node_id == target_node_id:
                    return True
            except KeyError:
                continue
    return False


def _has_dependent_clone(snapshot_uuid):
    """True when any live volume is cloned from *snapshot_uuid*.

    A failed-over volume is a clone of the last replicated target snapshot, so
    that snapshot must outlive it. Uses the mini index (same source the snapshot
    delete path consults) and ignores volumes that are themselves going away.
    """
    for lvol in db.get_mini_lvols():
        if lvol.cloned_from_snap != snapshot_uuid:
            continue
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        return True
    return False


def _successor_is_chained_to(successor, predecessor_target_uuid):
    """True when *successor*'s remote copy is chained onto *predecessor_target_uuid*.

    Retention deletes a predecessor expecting SPDK to swap-merge its segments
    into the successor that is CHAINED to it. If the chain link was never
    established, that delete does not merge — it drops the segments, and the
    target is left holding the newest delta over holes (all-zeros DR fail-over,
    labs 2026-08-10..17). Keeping N snapshots only widens the race; it never
    establishes the precondition, so verify it explicitly before pruning.

    The DB link is authoritative when present: ``prev_snap_uuid`` is only
    written after bdev_lvol_add_clone and bdev_lvol_convert both succeeded, on
    the primary and on an online secondary (every failure returns before the
    record is written). The converse does not hold — the link write is
    best-effort — so when the link is absent we ask SPDK on the target node,
    which is the real authority, rather than blocking the prune forever.
    """
    successor_target_uuid = successor.target_replicated_snap_uuid
    if not successor_target_uuid:
        return False
    try:
        successor_copy = db.get_snapshot_by_id(successor_target_uuid)
    except KeyError:
        return False

    if successor_copy.prev_snap_uuid == predecessor_target_uuid:
        return True

    # No DB link. Ask the target node whether the blob is actually chained, so a
    # missing link (best-effort write, or a snapshot replicated before chaining
    # was implemented) cannot make the pair unprunable for ever.
    try:
        predecessor_copy = db.get_snapshot_by_id(predecessor_target_uuid)
        remote_snode = db.get_storage_node_by_id(successor_copy.lvol.node_id)
        if remote_snode.status != StorageNode.STATUS_ONLINE:
            return False
        for bdev in (remote_snode.rpc_client().get_bdevs(successor_copy.snap_bdev) or []):
            driver = (bdev.get("driver_specific") or {}).get("lvol") or {}
            if not driver.get("clone"):
                continue
            if driver.get("base_snapshot") in (predecessor_copy.snap_bdev,
                                               predecessor_copy.snap_uuid):
                logger.info("Snapshot %s is chained onto %s in SPDK but the DB link is "
                            "missing; pruning on the SPDK verdict",
                            successor_copy.get_id(), predecessor_target_uuid)
                return True
    except Exception as e:
        logger.warning("Could not verify the chain of %s onto %s: %s",
                       successor_target_uuid, predecessor_target_uuid, e)
    return False


_KEEP_REPLICATED_INTERNAL = 2



def _keep_replicated_for(source_lvol):
    """How many replicated internal snapshots to retain for *source_lvol*.

    A volume under a replication policy uses the policy's ``keep_replicated``
    (never below its floor, since fewer than a pair leaves an arrival with
    nothing to chain onto); otherwise the module default applies.
    """
    try:
        policy = db.get_replication_policy_for_lvol(source_lvol)
    except Exception:
        policy = None
    if policy is None:
        return _KEEP_REPLICATED_INTERNAL
    from simplyblock_core.models.replication import ReplicationPolicy
    return max(policy.keep_replicated, ReplicationPolicy.MIN_KEEP_REPLICATED)

def _retention_schedule_for(source_lvol):
    """Parsed retention tiers from the volume's policy, or [] when it has none.

    A malformed schedule must not silently disable retention or crash the
    replication runner: it is reported and treated as "no schedule", which
    falls back to the flat keep-count.
    """
    try:
        policy = db.get_replication_policy_for_lvol(source_lvol)
    except KeyError:
        return []
    if (policy is None) or (spec := getattr(policy, "retention_schedule", None)) is None:
        return []
    try:
        return snapshot_retention.parse_schedule(spec)
    except snapshot_retention.RetentionScheduleError as e:
        logger.error("Ignoring invalid retention_schedule %r on policy %s: %s",
                     spec, policy.get_id(), e)
        return []


def _prune_internal_snapshots(source_lvol):
    """Retention for replication-driven internal snapshots.

    Internal snapshots are transient checkpoints taken at a fixed interval
    purely to drive replication. Once a newer internal snapshot has been
    successfully replicated, the older internal snapshots are redundant: they
    are removed on BOTH the target (the explicit requirement — only the last
    replicated internal snapshot persists there) and the source (so the source
    snapshot chain stays bounded). User snapshots are never auto-deleted, on
    either side.

    Only snapshots strictly older than the most-recent replicated internal
    snapshot are pruned, so the newest internal snapshot — which serves as the
    base for the next delta transfer — always remains.
    """
    replicated_internal = [
        s for s in db.get_snapshots_by_node_id(source_lvol.node_id)
        if s.lvol.get_id() == source_lvol.get_id()
        and s.snap_type == SnapShot.TYPE_INTERNAL
        and s.status == SnapShot.STATUS_ONLINE
        and s.target_replicated_snap_uuid
    ]
    keep = _keep_replicated_for(source_lvol)
    replicated_internal.sort(key=lambda s: s.created_at)

    # A retention SCHEDULE, when the policy defines one, decides which older
    # snapshots survive; without it retention stays the flat "newest N".
    # Either way the newest `keep` are protected, because deleting a snapshot
    # swap-merges its segments into the successor chained to it.
    schedule = _retention_schedule_for(source_lvol)
    # Say which retention is in force, every time it prunes. Soak run
    # 20260827_224741 ended with 2 snapshots at consecutive cadence ticks after
    # 124 minutes under `5m:15m,7m:30m,10m:1h` -- which is exactly what the FLAT
    # keep-N path produces, and nothing in the log said which path ran. The
    # ladder itself is provably correct (test_case11_retention_ladder), so the
    # open question is whether the schedule reaches this function at all.
    logger.info(
        "Retention for lvol %s: %s (replicated internal snapshots: %d, keep=%d)",
        source_lvol.get_id(),
        ("schedule %s" % snapshot_retention.describe(schedule)) if schedule
        else "FLAT keep-newest (no schedule on the policy)",
        len(replicated_internal), keep)
    if schedule:
        retained_ts = snapshot_retention.select_retained(
            [s.created_at for s in replicated_internal], schedule,
            now=time.time(), always_keep_newest=keep)
        candidates = [(i, s) for i, s in enumerate(replicated_internal)
                      if s.created_at not in retained_ts]
        if not candidates:
            return
    else:
        if len(replicated_internal) <= keep:
            return
        candidates = list(enumerate(replicated_internal))[:-keep]
    # Keep the newest TWO replicated internal snapshots, not just one.
    #
    # A replicated snapshot holds only its own clusters; the rest of the data
    # lives in the chain below it, and deleting a snapshot swap-merges its
    # segments into the successor that is CHAINED to it. Keeping only the
    # newest meant the predecessor was pruned the instant a replication
    # finished, so the NEXT arrival had nothing to chain onto and kept just
    # its delta — the target then holds the last delta over holes. Whether it
    # broke was pure timing, which is why the same fail-over case passed twice
    # and then failed (labs run 15 vs 19).
    #
    # Two kept widens the window in which the successor gets chained, but a
    # count can never establish the precondition: if chaining lagged or failed
    # for one snapshot while newer ones kept arriving, the predecessor was still
    # pruned and its segments were dropped instead of merged. So the chain is
    # verified per candidate below, and an unchained successor defers the prune.
    for index, snap in candidates:
        target_uuid = snap.target_replicated_snap_uuid
        try:
            db.get_snapshot_by_id(target_uuid)
        except KeyError:
            target_uuid = ""  # already gone — fall through to source cleanup
        if target_uuid and not _successor_is_chained_to(
                replicated_internal[index + 1], target_uuid):
            # The successor does not (yet) sit on top of this snapshot. Deleting
            # it now would drop its segments rather than swap-merge them, which
            # is exactly how a fail-over clone ends up reading zeros. Leave both
            # copies and retry next cycle: replication is still converging, or
            # chaining failed and its own retry has to land first.
            logger.warning("Deferring prune of replicated internal snapshot %s: its "
                           "successor %s is not chained onto target copy %s",
                           snap.get_id(), replicated_internal[index + 1].get_id(),
                           target_uuid)
            continue
        if target_uuid and _has_dependent_clone(target_uuid):
            # Never prune a target snapshot a volume is cloned from. The delete
            # reaches SPDK as bdev_lvol_delete(sync=False) and frees the blocks
            # there and then, so no downstream DB-level guard can save the clone:
            # a failed-over volume built on this snapshot would silently start
            # reading zeros. Keep both copies; the pair is released once the
            # dependent volume is gone.
            logger.info("Keeping replicated internal snapshot %s on source and "
                        "%s on target: a volume is cloned from the target copy",
                        snap.get_id(), target_uuid)
            continue
        if target_uuid:
            logger.info("Pruning replicated internal snapshot on target: %s", target_uuid)
            if not snapshot_controller.delete(target_uuid):
                logger.warning("Failed to delete target internal snapshot %s, will retry", target_uuid)
                continue
        logger.info("Pruning internal snapshot on source: %s", snap.get_id())
        if not snapshot_controller.delete(snap.get_id()):
            logger.warning("Failed to delete source internal snapshot %s, will retry", snap.get_id())


def _previous_replicated_snapshot(snapshot, replicate_to_source):
    """The newest older snapshot of the same lvol whose copy already exists on
    the remote cluster — the chain target for the snapshot being finalized.

    ``snap_ref_id`` wins when populated, but internal replication snapshots
    are created without it, so fall back to age ordering. Returns None only
    when the snapshot genuinely has no replicated predecessor (first snapshot
    of the volume).

    REPLICATED is the whole precondition, and it applies to the snap_ref_id
    shortcut too. Returning a referenced predecessor that had never been
    replicated handed _resolve_chain_target a BLANK remote-copy id, which it
    read as "there is a remote copy, but I cannot resolve it" and refused to
    finalize on. The transfer then retried for ever and the volume's snapshot
    never got its replicated marker (lab 2026-08-20: 299 "Predecessor snapshot
    ... has remote copy  but it cannot be resolved ('Snapshot lookup with a
    blank id')" in 75 minutes, and case 4 never produced a post-baseline
    replicated point)."""
    attr = ("source_replicated_snap_uuid" if replicate_to_source
            else "target_replicated_snap_uuid")
    # The newest older replicated SIBLING is the predecessor, and it must be
    # looked for FIRST: snapshot_controller.add stamps snap_ref_id on EVERY
    # snapshot of a cloned volume, naming the clone lineage's ORIGIN, not the
    # snapshot before this one. Honouring that reference ahead of the siblings
    # chained every delta of a failed-over volume onto the fail-over point
    # instead of onto the previous copy -- a star, not a chain. A partial
    # transfer carries only the delta against the predecessor on the source,
    # so each copy on the destination held "fail-over point + one 5-minute
    # delta" and nothing in between; the next relocate cloned from such a copy
    # and the guest found a file system with holes (2026-10-01, wp-db on the
    # real test bed: XFS metadata CRC errors, MariaDB would not start).
    prev = None
    for s in db.get_snapshots_by_node_id(snapshot.lvol.node_id):
        if (s.lvol.get_id() == snapshot.lvol.get_id()
                and s.get_id() != snapshot.get_id()
                and getattr(s, attr, "")
                and s.status != SnapShot.STATUS_IN_DELETION
                and s.created_at < snapshot.created_at
                and (prev is None or s.created_at > prev.created_at)):
            prev = s
    if prev is not None:
        return prev

    # No older SIBLING — but a fail-over volume is a CLONE, and its first
    # snapshot's chain parent is the snapshot it was cloned from, not a
    # sibling. On fail-back that parent is the replicated copy of the
    # fail-over point (n(1)' on the target), whose counterpart on the original
    # source is n(1) — exactly the snapshot the delta must be chained onto.
    # Without this the first fail-back delta lands as a standalone blob and
    # reads its own clusters plus zeros, the same failure as the unchained
    # forward replication (case 2).
    lvol = snapshot.lvol
    try:
        lvol = db.get_lvol_by_id(snapshot.lvol.get_id())
    except (KeyError, AttributeError):
        pass
    parent_uuid = getattr(lvol, "cloned_from_snap", "")
    parent = None
    if parent_uuid:
        try:
            parent = db.get_snapshot_by_id(parent_uuid)
        except KeyError as e:
            logger.error("clone parent %s unresolvable: %s", parent_uuid, e)
            return None
    if parent is not None and getattr(parent, attr, ""):
        logger.info("Chain parent for %s is the clone's origin snapshot %s",
                    snapshot.get_id(), parent.get_id())
        return parent
    # Last: a referenced snapshot (snap_ref_id). For a clone it names the
    # lineage's origin, which the clone parent above already covers; it is
    # consulted only when nothing else resolved, and only when replicated.
    if snapshot.snap_ref_id and snapshot.snap_ref_id != parent_uuid:
        try:
            referenced = db.get_snapshot_by_id(snapshot.snap_ref_id)
        except KeyError as e:
            logger.error("snap_ref_id %s unresolvable: %s", snapshot.snap_ref_id, e)
            return None
        if getattr(referenced, attr, ""):
            logger.info("Chain parent for %s is its referenced snapshot %s",
                        snapshot.get_id(), referenced.get_id())
            return referenced
    return None


def _resolve_chain_target(snapshot, replicate_to_source, remote_snode):
    """Resolve the remote-cluster snapshot the new copy must be chained to.

    Returns ``(target_prev_snap, prev_snap_for_db, ok)``. ``ok`` is False when
    a replicated predecessor exists but its remote copy cannot be used — the
    caller must fail-and-retry rather than finalize an unchained snapshot: an
    unchained copy reads only its own delta (zeros elsewhere) and retention's
    delete cannot swap-merge segments into a successor."""
    prev_snap = _previous_replicated_snapshot(snapshot, replicate_to_source)
    if not prev_snap:
        return None, None, True
    remote_copy_uuid = (prev_snap.source_replicated_snap_uuid
                        if replicate_to_source
                        else prev_snap.target_replicated_snap_uuid)
    if not remote_copy_uuid:
        # A blank id is not an unresolvable copy, it is NO copy: this
        # predecessor was never replicated, so there is nothing on the remote
        # side to chain onto and this snapshot starts the chain. Treating the
        # blank as a broken reference made the task refuse to finalize and
        # retry for ever (see _previous_replicated_snapshot).
        logger.info("Predecessor %s has no copy on the remote side; the new "
                    "snapshot starts the chain", prev_snap.get_id())
        return None, None, True
    try:
        _snap_obj = db.get_snapshot_by_id(remote_copy_uuid)
    except KeyError as e:
        logger.error(
            "Predecessor snapshot %s has remote copy %s but it cannot be "
            "resolved (%s); refusing to finalize an unchained snapshot",
            prev_snap.get_id(), remote_copy_uuid, e)
        return None, None, False
    if _snap_obj.lvol.node_id != remote_snode.get_id():
        logger.error(
            "Predecessor remote copy %s lives on node %s but the new snapshot "
            "is on %s; cannot chain across lvstores",
            remote_copy_uuid, _snap_obj.lvol.node_id, remote_snode.get_id())
        return None, None, False
    return {"snap_bdev": _snap_obj.snap_bdev}, _snap_obj, True


def _prechain_landing_volume(task, snapshot, replicate_to_source, remote_lv,
                             remote_lv_node):
    """Chain the landing volume onto the destination's copy of the PREVIOUS
    snapshot, before the transfer runs, so only the delta has to be sent.

    Returns True when the landing volume (and every online member of its HA
    pair) is chained, which is the precondition for asking for a PARTIAL
    transfer. Returns False to mean "send a full transfer", which is always
    correct and is what this pipeline did unconditionally until now.

    Why chaining first is what makes a partial transfer correct
    -----------------------------------------------------------
    ``bdev_lvol_transfer`` sends a blob's OWN cluster map and nothing else, so a
    partial transfer ships only the ranges written since the previous snapshot.
    Everything OUTSIDE that delta therefore has to be on the destination
    already, and a fresh empty landing volume does not have it -- that is
    exactly why the delta path has never been switched on. Chaining the landing
    volume onto the predecessor's remote copy supplies it: a cluster the new
    snapshot does not own stays unallocated and reads through to the parent,
    and the first write into a cluster the delta DOES touch makes the blobstore
    copy-on-write the whole cluster from that same parent before applying the
    incoming range. Both halves of the destination image thus come from the
    same predecessor the source computed its delta against.

    This is the very ``bdev_lvol_add_clone`` the finish step used to issue AFTER
    the transfer; issuing it BEFORE is what turns the landing volume into a
    valid delta target. Nodes chained here are recorded on the task so the
    finish step does not add the same clone entry twice.

    The bar for returning True is deliberately high:
      * a predecessor copy must resolve cleanly on this node (``ok`` and a
        non-empty ``target_prev_snap`` from :func:`_resolve_chain_target`);
      * the LVS leader must accept the chain;
      * the secondary must be ONLINE and accept it too. The transfer lands on
        the leader and the lvstore mirrors it to the secondary; an unchained
        secondary would read zeros wherever the delta did not write, so a
        degraded pair gets a full transfer rather than a delta.
    Anything short of that logs why and falls back to a full transfer.
    """
    target_prev_snap, _prev_snap_for_db, ok = _resolve_chain_target(
        snapshot, replicate_to_source, remote_lv_node)
    if not ok:
        logger.info(
            "Landing volume %s stays a full-transfer target: the predecessor's "
            "copy on the destination could not be resolved", remote_lv.top_bdev)
        return False
    if not target_prev_snap:
        logger.info(
            "Landing volume %s stays a full-transfer target: %s starts the "
            "chain, so there is no predecessor to clone from",
            remote_lv.top_bdev, snapshot.get_id())
        return False

    sec_node = None
    if remote_lv_node.secondary_node_id:
        try:
            sec_node = db.get_storage_node_by_id(remote_lv_node.secondary_node_id)
        except KeyError:
            sec_node = None
    if sec_node is None or sec_node.status != StorageNode.STATUS_ONLINE:
        logger.info(
            "Landing volume %s stays a full-transfer target: the secondary of "
            "%s is not online, and a delta would leave holes on it",
            remote_lv.top_bdev, remote_lv_node.get_id())
        return False

    prechained = list(task.function_params.get("prechained_node_ids") or [])

    def _chain_on(node, role):
        if node.get_id() in prechained:
            return True
        logger.info("Pre-chaining landing volume %s onto %s on %s (%s) so the "
                    "transfer can ship only the delta",
                    remote_lv.top_bdev, target_prev_snap['snap_bdev'],
                    node.get_id(), role)
        try:
            ret = node.rpc_client().bdev_lvol_add_clone(
                remote_lv.top_bdev, target_prev_snap['snap_bdev'])
        except Exception as e:
            logger.warning("Pre-chain of %s on %s (%s) raised %s; falling back "
                           "to a full transfer", remote_lv.top_bdev,
                           node.get_id(), role, e)
            return False
        if not ret:
            logger.warning("Pre-chain of %s onto %s failed on %s (%s); falling "
                           "back to a full transfer", remote_lv.top_bdev,
                           target_prev_snap['snap_bdev'], node.get_id(), role)
            return False
        prechained.append(node.get_id())
        return True

    primary_ok = _chain_on(remote_lv_node, "primary")
    # Record whatever actually landed even on the failure path: the entry exists
    # on that node now, and the finish step must not add it a second time.
    secondary_ok = _chain_on(sec_node, "secondary") if primary_ok else False
    if prechained != (task.function_params.get("prechained_node_ids") or []):
        task.function_params["prechained_node_ids"] = prechained
        task.write_to_db()

    if not (primary_ok and secondary_ok):
        # A full transfer into a partially chained volume is still correct: it
        # writes every cluster the snapshot owns, and the chained node reads the
        # remainder from the parent exactly as it should.
        return False
    return True


def _partial_transfer_decision(task, snapshot, replicate_to_source, remote_lv,
                               remote_lv_node):
    """Whether this transfer may ship only the delta, chaining the landing
    volume first if that has not happened yet.

    The verdict is cached on the task so a resumed transfer (offset > 0) keeps
    the mode its first attempt used -- but it is KEYED to the landing volume it
    was made about. An interrupted attempt can delete a half-created landing
    volume and build a fresh, unchained one (see the adoption block in
    process_snap_replicate_start); carrying "partial is fine" onto that volume
    would ship a delta into something that holds nothing, which is precisely
    the silent data loss this design exists to avoid. A key mismatch throws
    away the old verdict AND the old chain record and re-derives both.
    """
    landing_key = remote_lv.get_id()
    if task.function_params.get("allow_partial_landing") == landing_key:
        return bool(task.function_params.get("allow_partial"))

    # Chain state recorded against a previous landing volume says nothing about
    # this one.
    task.function_params["prechained_node_ids"] = []
    allow_partial = _prechain_landing_volume(
        task, snapshot, replicate_to_source, remote_lv, remote_lv_node)
    task.function_params["allow_partial"] = allow_partial
    task.function_params["allow_partial_landing"] = landing_key
    task.write_to_db()
    return allow_partial


def _prechained_nodes_for(task, remote_lv):
    """Nodes already carrying the landing volume's clone entry.

    Empty unless the record was made about THIS landing volume: adding the same
    clone entry twice is not idempotent on the SPDK side, and trusting a stale
    record would skip a chain the volume actually needs.
    """
    if task.function_params.get("allow_partial_landing") != remote_lv.get_id():
        return set()
    return set(task.function_params.get("prechained_node_ids") or [])


def process_snap_replicate_finish(task, snapshot):

    # Close the transfer session — but ONLY when this was the last active
    # transfer into that target node. The hub is ONE shared session per target
    # node: a naive per-cycle detach rips the qpair out from under the other
    # volumes' in-flight transfers, mass-failing their IO on the hub and
    # churning LVS leadership on the target ("receive io for hublvol in
    # nonleader mode" storms, observed live 2026-08-13). This is the refcount
    # discipline the migration runner's hub_manager exists for.
    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    # add_clone/convert must run on the leader too — a convert on a non-leader
    # reports success and persists nothing. Follow leadership, not the node the
    # receiving lvol was created on (see _receiving_leader_node).
    remote_snode = (_receiving_leader_node(remote_lv)
                    or db.get_storage_node_by_id(remote_lv.node_id))
    _src_node = (_source_leader_node(snapshot)
                 or db.get_storage_node_by_id(snapshot.lvol.node_id))
    if remote_snode.transfer_hublvol and remote_snode.transfer_hublvol.bdev_name:
        if not _other_active_transfers_to_node(task, remote_snode.get_id()):
            xfer_timing.stamp("hub_detach", snap=snapshot.get_id(),
                              lvol=snapshot.lvol.get_id())
            # Non-fatal: a resumed finish (see _resume_finish) finds the hub
            # already detached by the first attempt.
            try:
                _src_node.rpc_client().bdev_nvme_detach_controller(
                    remote_snode.transfer_hublvol.bdev_name)
            except Exception as e:                        # noqa: BLE001
                logger.warning("Transfer hub detach on %s failed (non-fatal): %s",
                               _src_node.get_id(), e)
    replicate_to_source = task.function_params["replicate_to_source"]
    if "replicate_as_snap_instance" in task.function_params:
        replicate_as_snap_instance = task.function_params["replicate_as_snap_instance"]
    else:
        replicate_as_snap_instance = False
    # Resolve the predecessor's copy on the REMOTE cluster and chain the new
    # snapshot to it. Without this link every replicated snapshot is a
    # standalone blob: a fail-over clone reads only the last delta and zeros
    # elsewhere, and retention's delete cannot swap-merge segments into a
    # successor (all-zeros DR fail-over, labs 2026-08-10..14; chain_attempts=0
    # in every run because snap_ref_id is never populated on internal
    # snapshots). Resolve the predecessor by lvol + age instead, and if one
    # exists but its remote copy cannot be resolved, fail and retry rather
    # than silently building an unchained snapshot.
    target_prev_snap, _prev_snap_for_db, ok = _resolve_chain_target(
        snapshot, replicate_to_source, remote_snode)
    if not ok:
        return False

    # Leadership gate BEFORE chain/convert on the primary: a convert on a
    # non-leader returns success without persisting (silent conversion error).
    if not _require_lvs_leader(remote_snode, remote_lv.lvs_name, "add_clone/convert"):
        return False

    # Nodes whose landing volume was already chained BEFORE the transfer, so it
    # could receive a delta (see _prechain_landing_volume). Those must be
    # skipped here: adding the same clone entry twice is not idempotent.
    _prechained = _prechained_nodes_for(task, remote_lv)

    # Nodes on which an earlier attempt of this finish already converted the
    # landing volume (see _resume_finish): chaining or converting them again
    # is not idempotent, so skip them and carry on with the rest.
    converted = list(task.function_params.get("converted_nodes") or [])

    def _mark_converted(node):
        converted.append(node.get_id())
        task.function_params["converted_nodes"] = converted
        task.write_to_db()

    # chain snaps on primary
    if remote_snode.get_id() in converted:
        logger.info("Landing volume %s is already converted on %s; skipping "
                    "its chain and convert", remote_lv.top_bdev, remote_snode.get_id())
    elif target_prev_snap and remote_snode.get_id() not in _prechained:
        logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {target_prev_snap['snap_bdev']}")
        with xfer_timing.phase("chain_add_clone", snap=snapshot.get_id(),
                               lvol=snapshot.lvol.get_id(), node="primary"):
            ret = remote_snode.rpc_client().bdev_lvol_add_clone( remote_lv.top_bdev, target_prev_snap['snap_bdev'])
        if not ret:
            logger.error("Failed to chain replicated snapshot on primary node")
            return False
    elif target_prev_snap:
        logger.info("Landing volume %s was already chained to %s on %s before "
                    "the transfer; skipping the redundant add_clone",
                    remote_lv.top_bdev, target_prev_snap['snap_bdev'],
                    remote_snode.get_id())

    # convert to snapshot on primary
    if remote_snode.get_id() not in converted:
        with xfer_timing.phase("chain_convert", snap=snapshot.get_id(),
                               lvol=snapshot.lvol.get_id(), node="primary"):
            ret = remote_snode.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)
        if not ret:
            logger.error("Failed to convert to snapshot on primary node")
            return False
        _mark_converted(remote_snode)

    # chain snaps on secondary
    sec_node = db.get_storage_node_by_id(remote_snode.secondary_node_id)
    if sec_node.status == StorageNode.STATUS_ONLINE and sec_node.get_id() in converted:
        logger.info("Landing volume %s is already converted on %s; skipping "
                    "its chain and convert", remote_lv.top_bdev, sec_node.get_id())
    elif sec_node.status == StorageNode.STATUS_ONLINE:
        if target_prev_snap and sec_node.get_id() not in _prechained:
            logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {target_prev_snap['snap_bdev']}")
            with xfer_timing.phase("chain_add_clone", snap=snapshot.get_id(),
                                   lvol=snapshot.lvol.get_id(), node="secondary"):
                ret = sec_node.rpc_client().bdev_lvol_add_clone(remote_lv.top_bdev, target_prev_snap['snap_bdev'])
            if not ret:
                if _secondary_lacks_bdev(sec_node, target_prev_snap['snap_bdev']) or                         _secondary_lacks_bdev(sec_node, remote_lv.top_bdev):
                    # The secondary does not hold the base (-19 No such
                    # device): its view of the chain is already behind, and
                    # failing here only re-runs the finish until the task
                    # gives up, then re-transfers into a converted landing
                    # (2026-10-02, LVS_1 site A: 8 retries, a duplicate
                    # landing, a write into a snapshot). The primary's
                    # convert made the copy; the secondary is repaired by
                    # the lvstore sync, not by this task.
                    logger.warning("Secondary %s does not hold %s or %s; the chain is "
                                   "not repeated there", sec_node.get_id(),
                                   target_prev_snap['snap_bdev'], remote_lv.top_bdev)
                else:
                    logger.error("Failed to chain replicated snapshot on secondary node")
                    return False
        elif target_prev_snap:
            logger.info("Landing volume %s was already chained to %s on %s "
                        "before the transfer; skipping the redundant add_clone",
                        remote_lv.top_bdev, target_prev_snap['snap_bdev'],
                        sec_node.get_id())

        # convert to snapshot on secondary
        with xfer_timing.phase("chain_convert", snap=snapshot.get_id(),
                               lvol=snapshot.lvol.get_id(), node="secondary"):
            ret = sec_node.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)
        if not ret:
            logger.error("Failed to convert to snapshot on secondary node")
            return False
        _mark_converted(sec_node)

    new_snapshot_uuid = str(uuid.uuid4())

    new_snapshot = SnapShot()
    new_snapshot.uuid = new_snapshot_uuid
    new_snapshot.data_uuid = snapshot.data_uuid
    new_snapshot.cluster_id = remote_snode.cluster_id
    new_snapshot.lvol = remote_lv
    new_snapshot.pool_uuid = remote_lv.pool_uuid
    new_snapshot.snap_bdev = remote_lv.top_bdev
    new_snapshot.snap_uuid = remote_lv.lvol_uuid
    new_snapshot.size = snapshot.size
    new_snapshot.used_size = snapshot.used_size
    new_snapshot.snap_name = snapshot.snap_name
    # Snapshot names are unique per cluster. The destination can hold a
    # snapshot of this name already (the original of a chain coming back
    # after a relocate, on another lvstore than this copy): the record must
    # not fail AFTER the convert, which leaves a converted landing volume the
    # control plane knows nothing about.
    if any(s.snap_name == new_snapshot.snap_name
           for s in db.get_snapshots(remote_snode.cluster_id)):
        new_snapshot.snap_name = f"{snapshot.snap_name}-{new_snapshot_uuid[:8]}"
        logger.warning("Snapshot name %s is taken on cluster %s; recording the copy as %s",
                       snapshot.snap_name, remote_snode.cluster_id, new_snapshot.snap_name)
    new_snapshot.blobid = remote_lv.blobid
    new_snapshot.created_at = int(time.time())
    new_snapshot.status = SnapShot.STATUS_ONLINE
    # Consistency-group provenance travels with the copy: the fail-over
    # generation selector returns the TARGET record, and requirement 4's
    # membership warnings are computed from (group_id, group_seq) on it.
    new_snapshot.group_id = getattr(snapshot, "group_id", "")
    new_snapshot.group_seq = getattr(snapshot, "group_seq", 0)
    snapshot.instances.append(new_snapshot)
    if not replicate_as_snap_instance:
        if replicate_to_source:
            new_snapshot.target_replicated_snap_uuid = snapshot.uuid
            snapshot.source_replicated_snap_uuid = new_snapshot_uuid
        else:
            snapshot.target_replicated_snap_uuid = new_snapshot_uuid
            new_snapshot.source_replicated_snap_uuid = snapshot.uuid

        if _prev_snap_for_db:
            # The chain link is what lets retention delete this snapshot's
            # predecessor safely: the prune path refuses to drop a predecessor
            # until it can see the successor sitting on top of it. Swallowing a
            # failure here used to leave SPDK chained but the record unlinked,
            # so record it before the snapshot is published, and fail the task
            # (it retries) rather than publishing a snapshot that looks
            # unchained to retention.
            new_snapshot.prev_snap_uuid = _prev_snap_for_db.get_id()
            _prev_snap_for_db.next_snap_uuid = new_snapshot_uuid
            try:
                _prev_snap_for_db.write_to_db()
            except Exception as e:
                logger.error("Failed to record the chain back-link on %s: %s",
                             _prev_snap_for_db.get_id(), e)
                return False

    new_snapshot.write_to_db()

    if snapshot.status == SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_ONLINE

    snapshot.write_to_db()

    # Tear down the landing volume's plumbing (subsystem/namespace); its BLOB
    # deliberately lives on -- it was just converted into the chained snapshot.
    # The record removal must not depend on the teardown succeeding: delete_lvol
    # can raise (SPDK refuses to delete a bdev that is now a cloned snapshot),
    # and a record left in_deletion never converges -- the monitor re-issues
    # its delete forever (297 warnings/10min, run 20260821_205111) and every
    # later cleanup that waits for lvols to drain times out on it.
    remote_lv.bdev_stack = []
    remote_lv.write_to_db()
    # Tear the subsystem/namespace down DIRECTLY, not via delete_lvol:
    # delete_lvol flips the record to in_deletion and hands it to the
    # monitor's async machinery, so an interruption anywhere before the
    # remove() below stranded a record the monitor can never finish (empty
    # stack -> nothing to issue -> status poll 4 forever; runs 20260824 and
    # 20260825_125156). With the stack already emptied there is no blob work
    # to do -- only nvmf plumbing on the volume's nodes.
    for _node_id in remote_lv.nodes:
        try:
            _node = db.get_storage_node_by_id(_node_id)
            if _node.status == StorageNode.STATUS_ONLINE:
                lvol_controller.delete_lvol_from_node(
                    remote_lv.get_id(), _node_id, force=True)
        except Exception as e:
            logger.error(f"Landing volume {remote_lv.get_id()} teardown on "
                         f"{_node_id[:8]} raised: {e}; retiring the record "
                         f"anyway (its bdev lives on as the converted snapshot)")
    remote_lv.remove(db.kv_store)
    snapshot_events.replication_task_finished(snapshot)
    _prune_internal_snapshots(snapshot.lvol)
    return new_snapshot_uuid


def task_runner(task: JobSchedule):
    # get_snapshot_by_id raises for a snapshot that is gone; it never returns
    # None. Without the catch the runner failed on every attempt and the task
    # stayed open for good, where replication_stop tripped over it.
    try:
        snapshot = db.get_snapshot_by_id(task.function_params["snapshot_id"])
    except KeyError:
        snapshot = None
    if not snapshot:
        task.function_result = "snapshot not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if (task.status == JobSchedule.STATUS_SUSPENDED and not task.canceled
            and int(time.time()) < int(task.function_params.get("not_before") or 0)):
        return False

    try:
        db.get_storage_node_by_id(snapshot.lvol.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    # Any online member of the source lvstore that holds leadership can drive
    # this; waiting for the recorded primary stalls replication for the whole
    # duration of its outage even though the promoted peer holds the snapshot.
    snode = _source_leader_node(snapshot)
    if snode is None:
        # Waiting, not failing (see _suspend_for_retry): on 2026-09-29 this
        # path spent every retry of vm-a's tasks within seconds while LVS_1 on
        # the source flapped, silently, and the volume stopped replicating.
        _suspend_for_retry(task, f"no online source LVS leader for "
                                 f"{snapshot.lvol.lvs_name}, retrying",
                           backoff=True, count=False)
        return False

    if task.retry >= task.max_retry or task.canceled is True:
        # Carry the last real reason: "max retry reached" alone names none.
        last = task.function_params.get("last_error")
        task.function_result = (f"max retry reached ({task.retry}/{task.max_retry}) after: {last}"
                                if last else "max retry reached")
        if task.canceled is True:
            task.function_result = "task cancelled"
        logger.error("Replication task %s (snapshot %s) gave up: %s",
                     task.uuid, snapshot.get_id(), task.function_result)

        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        if snapshot.status != SnapShot.STATUS_ONLINE:
            snapshot.status = SnapShot.STATUS_ONLINE
            snapshot.write_to_db()

        # A task can reach max retry BEFORE it ever created a receiving lvol
        # (e.g. every attempt failed at the leadership gate). Reading the param
        # unconditionally raised KeyError out of main() and killed the whole
        # replication runner — one unlucky task stopped replication for every
        # volume in the cluster (lab run 19: the service crash-looped, so no
        # snapshot was ever chained or pruned).
        remote_lv_id = task.function_params.get("remote_lvol_id")
        if not remote_lv_id:
            return True
        try:
            remote_lv = db.get_lvol_by_id(remote_lv_id)
        except KeyError:
            return True
        # abort path: close the transfer session here too (last user only)
        try:
            _rl_node = db.get_storage_node_by_id(remote_lv.node_id)
            if (_rl_node.transfer_hublvol and _rl_node.transfer_hublvol.bdev_name
                    and not _other_active_transfers_to_node(task, _rl_node.get_id())):
                snode.rpc_client().bdev_nvme_detach_controller(
                    _rl_node.transfer_hublvol.bdev_name)
        except Exception as e:
            logger.warning("Abort-path hub detach failed (non-fatal): %s", e)
        try:
            lvol_controller.delete_lvol(remote_lv, force_delete=True)
        except Exception as e:
            logger.warning("Abort-path cleanup of %s failed (non-fatal): %s",
                           remote_lv_id, e)

        return True


    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        if not _resume_finish(task, snapshot):
            process_snap_replicate_start(task, snapshot)

    elif task.status == JobSchedule.STATUS_RUNNING:
        snode = _source_leader_node(snapshot) or db.get_storage_node_by_id(snapshot.lvol.node_id)
        ret = snode.rpc_client().bdev_lvol_transfer_stat(snapshot.snap_bdev)
        if ret:
            # offset is the bytes moved so far: the ONLY direct read on actual
            # transfer throughput, as distinct from round duration.
            xfer_timing.stamp("transfer_running", snap=snapshot.get_id(),
                              lvol=snapshot.lvol.get_id(),
                              state=str(ret.get("transfer_state")).replace(" ", "_"),
                              offset=ret.get("offset"))
        if not ret:
            logger.error("Failed to get transfer stat")
            return False
        status = ret["transfer_state"]
        offset = ret["offset"]
        if status == "No process":
            _suspend_for_retry(task, f"Status: {status}, offset:{offset}, retrying")
            return False
        if status == "In progress":
            task.function_result = f"Status: {status}, offset:{offset}"
            task.function_params["offset"] = offset
            task.write_to_db()
            return True
        if status == "Failed":
            _suspend_for_retry(task, f"transfer failed at offset {offset}, retrying",
                               backoff=True)
            return False
        if status == "Done":
            return _finish_completed_transfer(task, snapshot, offset)


def main():
    logger.info("Starting Tasks runner...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
                        if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                            active_task = False
                            for t in db.get_job_tasks(task.cluster_id):
                                if t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and t.function_params["snapshot_id"] ==  task.function_params['snapshot_id']:
                                    if t.status == JobSchedule.STATUS_RUNNING and t.canceled is False:
                                        active_task = True
                                        break
                            if active_task:
                                logger.info("replication task found for same snapshot, retry")
                                continue
                        if task.status != JobSchedule.STATUS_DONE:
                            # Re-read the task in case cancel changed it. If it has
                            # since vanished -- retention, a concurrent cleanup, or a
                            # stale index entry a repair has yet to clear -- skip it.
                            # One missing task must never take the runner down with
                            # it: this KeyError used to propagate out of main() and
                            # stop replication for every cluster, then crash the
                            # restarted container on the same entry (live 2026-09-28).
                            try:
                                task = db.get_task_by_id(task.uuid)
                            except KeyError:
                                logger.warning("Replication task %s vanished before "
                                               "dispatch; skipping", task.uuid)
                                continue
                            # One task must never take the runner down with it:
                            # an RPC to a node that just went offline, or a
                            # malformed param, used to propagate out of main()
                            # and stop replication for the whole cluster until
                            # the container restarted (and then again).
                            try:
                                res = task_runner(task)
                            except Exception as e:
                                logger.error("Replication task %s failed: %s",
                                             task.get_id(), e)
                                res = False
                            if not res:
                                time.sleep(3)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
