"""Task runner for cross-cluster replication cutover (FN_REPLICATION_FINAL).

Consumes the task enqueued by migration-commit (D7) and fail-back (D8) and
drives the whole cutover:

  1. SHRINK phase (>=2 rounds): the controller took shrink snapshot #1 at
     commit time; this runner waits until it is replicated AND converted on the
     target (target_replicated_snap_uuid set happens after convert), then
     IMMEDIATELY takes the next shrink snapshot — whose delta covers only the
     wait window — and waits again. Each round shrinks the freeze residual.
  2. CUTOVER phase, immediately after the last round: build the writable
     target clone on the LAST replicated snapshot (guaranteed fresh by the
     shrink), resolve its map_id, record the LVolReplication, and run the
     IO-freeze final step + ANA flip via :mod:`replication_final_step`.

task.function_params:
    lvol_id, src_node_id, tgt_node_id, operation, final_state,
    shrink_round, shrink_snap_id, shrink_deadline
    (+ tgt_lvol_composite/tgt_map_id/tgt_snap_composite/replication_id once
     the cutover phase has prepared them)
"""
import time
import uuid as uuid_lib
from datetime import datetime

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)

db = db_controller.DBController()


def _finalize(task, ok, err):
    if ok:
        replication_id = task.function_params.get("replication_id")
        final_state = task.function_params.get("final_state", LVolReplication.STATE_CUTOVER_DONE)
        if replication_id:
            try:
                rep = db.get_lvol_replication_by_id(replication_id)
                rep.state = final_state
                rep.write_to_db(db.kv_store)
            except Exception as e:
                logger.error(f"Failed to update replication state: {e}")
        task.function_result = "cutover done"
        task.status = JobSchedule.STATUS_DONE
        task.function_params["end_time"] = int(time.time())
        task.write_to_db(db.kv_store)

        # The hand-off is complete: the target serves the data from here on,
        # so the SOURCE must stop replicating. Nothing else clears its cadence
        # config, and a retired source otherwise keeps taking internal
        # snapshots and shipping them to the very target it handed off to
        # (observed 2026-08-21: replication_final done "cutover done" while
        # the source volumes kept replicating). In-flight transfers drain
        # naturally; this stops NEW cadence snapshots at the gate the monitor
        # reads (do_replicate / replication_interval_min).
        try:
            src_lvol = db.get_lvol_by_id(task.function_params.get("lvol_id"))
            if src_lvol.do_replicate:
                src_lvol.do_replicate = False
                src_lvol.replication_interval_min = 0
                src_lvol.replication_policy_id = ""
                src_lvol.write_to_db()
                logger.info(f"Cutover done: stopped replication on source "
                            f"volume {src_lvol.get_id()}")
        except KeyError:
            pass          # source already gone (e.g. deleted out of band)
        except Exception as e:
            logger.error(f"Could not stop replication on the source after "
                         f"cutover: {e}")

        # Optional migration semantics: the source volume has served its
        # purpose once the client runs on the target, so `replication-commit
        # --delete-source` retires it here — strictly AFTER the cutover state
        # is durable, so a crash in between leaves a completed cutover with
        # the source still present (retryable by hand), never a deleted
        # source with an uncommitted cutover. The relationship record is what
        # later look-ups (target-by-source, active side) resolve through, and
        # it survives the volume's deletion.
        if task.function_params.get("delete_source"):
            src_lvol_id = task.function_params.get("lvol_id")
            try:
                from simplyblock_core.controllers import lvol_controller
                src_lvol = db.get_lvol_by_id(src_lvol_id)
                logger.info(f"Cutover committed with --delete-source: deleting "
                            f"source volume {src_lvol_id}")
                lvol_controller.delete_lvol(src_lvol)
            except Exception as e:
                # The cutover itself succeeded; a failed source delete is
                # reported loudly but does not un-succeed the task.
                logger.error(f"Source volume {src_lvol_id} could not be "
                             f"deleted after the cutover: {e}")
        return True

    task.function_result = err or "cutover failed, retrying"
    task.status = JobSchedule.STATUS_SUSPENDED
    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


def task_runner(task: JobSchedule):
    params = task.function_params
    lvol_id = params.get("lvol_id")
    if not lvol_id:
        return _finalize(task, False, "missing lvol_id in task params")

    if task.retry >= task.max_retry or task.canceled is True:
        task.function_result = "task cancelled" if task.canceled else "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    try:
        lvol = db.get_lvol_by_id(lvol_id)
        tgt_node = db.get_storage_node_by_id(params["tgt_node_id"])
    except KeyError as e:
        return _finalize(task, False, f"object not found: {e}")

    # The source may be gone entirely (fail-over after cluster loss); cutover
    # proceeds with a best-effort ANA flip in that case.
    src_node = None
    try:
        src_node = db.get_storage_node_by_id(params["src_node_id"])
    except KeyError:
        pass

    if tgt_node.status != StorageNode.STATUS_ONLINE:
        task.function_result = "target node not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if src_node is None:
        return _finalize(task, False, "source node not found for cutover")

    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED, JobSchedule.STATUS_RUNNING]:
        task.status = JobSchedule.STATUS_RUNNING
        task.function_params.setdefault("start_time", int(time.time()))
        task.write_to_db(db.kv_store)

        # ---- SHRINK PHASE ----------------------------------------------- #
        if "shrink_snap_id" in params and params.get("shrink_round", 0) > 0:
            done, err = _shrink_step(task, lvol)
            if err:
                return _finalize(task, False, err)
            if not done:
                # waiting on replication of the current shrink snapshot; come
                # back next pass WITHOUT burning a retry (bounded by deadline)
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return False

        # ---- CUTOVER PHASE (immediately after the last shrink round) ---- #
        if "tgt_lvol_composite" not in params:
            err = _prepare_cutover(task, lvol, src_node, tgt_node)
            if err:
                return _finalize(task, False, err)
            params = task.function_params

        try:
            ok, err = replication_final_step.run_cutover(
                src_node, tgt_node, lvol,
                params["tgt_lvol_composite"], params["tgt_map_id"],
                params["tgt_snap_composite"], operation=params.get("operation", "replicate"))
        except Exception as e:
            logger.error(f"Cutover raised: {e}", exc_info=True)
            return _finalize(task, False, str(e))
        return _finalize(task, ok, err)
    return True


SHRINK_ROUNDS = 2


def _shrink_step(task, lvol):
    """Advance the delta-shrink state machine one step.

    Returns (done, error): done=True when all rounds are replicated and the
    cutover may start IMMEDIATELY; error aborts the task.
    """
    params = task.function_params
    if int(time.time()) > params.get("shrink_deadline", 0):
        return False, "shrink phase timed out waiting for replication"

    snap_id = params["shrink_snap_id"]
    try:
        snap = db.get_snapshot_by_id(snap_id)
    except KeyError:
        return False, f"shrink snapshot {snap_id} disappeared"

    # target_replicated_snap_uuid is set at replicate-finish AFTER the target
    # copy is chained and converted — replicated AND converted in one signal.
    if not snap.target_replicated_snap_uuid:
        task.function_result = (f"shrink round {params['shrink_round']}: waiting "
                                f"for {snap_id[:8]} to replicate")
        return False, None

    if params["shrink_round"] >= SHRINK_ROUNDS:
        return True, None

    # Round replicated — IMMEDIATELY take the next snapshot: its delta covers
    # only the wait window of the previous round.
    from simplyblock_core.controllers import snapshot_controller
    new_snap, err = snapshot_controller.add(
        lvol.get_id(), f"repl_commit_{uuid_lib.uuid4()}",
        snap_type=SnapShot.TYPE_INTERNAL)
    if err:
        return False, f"shrink round {params['shrink_round'] + 1} snapshot failed: {err}"
    params["shrink_round"] += 1
    params["shrink_snap_id"] = new_snap
    task.function_result = f"shrink round {params['shrink_round']}: snapshot taken"
    task.write_to_db(db.kv_store)
    return False, None


def _prepare_cutover(task, lvol, src_node, tgt_node):
    """Build the target clone on the last replicated snapshot and record the
    relationship. Runs once, immediately after the shrink phase, so the base is
    guaranteed to be the freshly replicated shrink snapshot."""
    from simplyblock_core.controllers import lvol_controller

    # Same resolution as fail-over: the destination is the cluster the volume
    # replicates INTO, and the pool comes from its policy's target. The source
    # cluster's outgoing config is only a fallback -- on a fail-back cutover it
    # describes the wrong direction, or is not set at all for a policy-driven
    # volume.
    target_cluster, target_pool_uuid = lvol_controller.resolve_replication_destination(
        db, lvol, tgt_node, src_node)

    new_lvol, snapshot, error = lvol_controller._clone_from_last_replicated(
        db, lvol.get_id(), lvol, tgt_node, target_pool_uuid, src_node.cluster_id)
    if error:
        return f"cutover clone failed: {error}"

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.write_to_db(db.kv_store)
    # Expose inaccessible until the final step flips ANA.
    lvol_controller.suspend_lvol(new_lvol.get_id())

    tgt_map_id = lvol_controller._resolve_target_map_id(tgt_node, new_lvol.lvol_bdev)
    if tgt_map_id is None:
        lvol_controller.delete_lvol_from_node(new_lvol, tgt_node)
        db.release_lvol_ns_slot(new_lvol)
        return "could not resolve target map_id"

    rep = LVolReplication()
    rep.uuid = str(uuid_lib.uuid4())
    rep.create_dt = str(datetime.now())
    rep.source_lvol = lvol
    rep.target_lvol = new_lvol
    rep.source_cluster_id = src_node.cluster_id
    rep.target_cluster_id = target_cluster.get_id()
    rep.mode = lvol.replication_mode
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.direction = LVolReplication.DIRECTION_TO_TARGET
    rep.target_nqn = new_lvol.nqn
    rep.target_ns_id = new_lvol.ns_id
    rep.write_to_db(db.kv_store)

    task.function_params.update({
        "tgt_lvol_composite": new_lvol.top_bdev,
        "tgt_map_id": tgt_map_id,
        "tgt_snap_composite": snapshot.snap_bdev,
        "replication_id": rep.get_id(),
    })
    task.write_to_db(db.kv_store)
    return None


def main():
    logger.info("Starting replication-final tasks runner...")
    while True:
        try:
            clusters = db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        for cl in clusters:
            for task in db.get_job_tasks(cl.get_id(), reverse=False):
                if task.function_name != JobSchedule.FN_REPLICATION_FINAL:
                    continue
                if task.status == JobSchedule.STATUS_DONE:
                    continue
                task = db.get_task_by_id(task.uuid)
                try:
                    res = task_runner(task)
                except Exception as e:
                    logger.error(f"replication-final task {task.uuid} failed: {e}", exc_info=True)
                    res = False
                if not res:
                    time.sleep(3)
        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
