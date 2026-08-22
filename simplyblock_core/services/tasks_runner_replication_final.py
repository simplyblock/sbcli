# coding=utf-8
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

from simplyblock_core import db_controller, utils
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step
from simplyblock_core.services.task_runner_base import RunnerSpec, TaskDefer, TaskRetry, serve

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)

db = db_controller.DBController()


def _record_cutover_done(task):
    replication_id = task.function_params.get("replication_id")
    if not replication_id:
        return

    final_state = task.function_params.get("final_state", LVolReplication.STATE_CUTOVER_DONE)
    try:
        rep = db.get_lvol_replication_by_id(replication_id)
        rep.state = final_state
        rep.write_to_db(db.kv_store)
    except Exception as e:
        logger.error(f"Failed to update replication state: {e}")


def _stop_source_replication(task):
    """The hand-off is complete: the target serves the data from here on, so the
    SOURCE must stop replicating. Nothing else clears its cadence config, and a
    retired source otherwise keeps taking internal snapshots and shipping them
    to the very target it handed off to (observed 2026-08-21: replication_final
    done "cutover done" while the source volumes kept replicating). In-flight
    transfers drain naturally; this stops NEW cadence snapshots at the gate the
    monitor reads (do_replicate / replication_interval_min).
    """
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


def _delete_source_if_requested(task):
    """Optional migration semantics: the source volume has served its purpose
    once the client runs on the target, so `replication-commit --delete-source`
    retires it here — strictly AFTER the cutover state is durable, so a crash in
    between leaves a completed cutover with the source still present (retryable
    by hand), never a deleted source with an uncommitted cutover. The
    relationship record is what later look-ups (target-by-source, active side)
    resolve through, and it survives the volume's deletion.
    """
    if not task.function_params.get("delete_source"):
        return

    src_lvol_id = task.function_params.get("lvol_id")
    try:
        from simplyblock_core.controllers import lvol_controller
        src_lvol = db.get_lvol_by_id(src_lvol_id)
        logger.info(f"Cutover committed with --delete-source: deleting "
                    f"source volume {src_lvol_id}")
        lvol_controller.delete_lvol(src_lvol)
    except Exception as e:
        # The cutover itself succeeded; a failed source delete is reported
        # loudly but does not un-succeed the task.
        logger.error(f"Source volume {src_lvol_id} could not be "
                     f"deleted after the cutover: {e}")


def task_runner(task: JobSchedule):
    params = task.function_params
    lvol_id = params.get("lvol_id")
    if not lvol_id:
        raise TaskRetry("missing lvol_id in task params")

    try:
        lvol = db.get_lvol_by_id(lvol_id)
        tgt_node = db.get_storage_node_by_id(params["tgt_node_id"])
    except KeyError as e:
        raise TaskRetry(f"object not found: {e}")

    if tgt_node.status != StorageNode.STATUS_ONLINE:
        raise TaskRetry("target node not online, retrying")

    try:
        src_node = db.get_storage_node_by_id(params["src_node_id"])
    except KeyError:
        raise TaskRetry("source node not found for cutover")

    params.setdefault("start_time", int(time.time()))

    # ---- SHRINK PHASE --------------------------------------------------- #
    if "shrink_snap_id" in params and params.get("shrink_round", 0) > 0:
        _shrink_step(task, lvol)

    # ---- CUTOVER PHASE (immediately after the last shrink round) -------- #
    if "tgt_lvol_composite" not in params:
        _prepare_cutover(task, lvol, src_node, tgt_node)

    ok, err = replication_final_step.run_cutover(
        src_node, tgt_node, lvol,
        params["tgt_lvol_composite"], params["tgt_map_id"],
        params["tgt_snap_composite"], operation=params.get("operation", "replicate"))
    if not ok:
        raise TaskRetry(err or "cutover failed, retrying")

    _record_cutover_done(task)
    params["end_time"] = int(time.time())
    task.function_result = "cutover done"

    _stop_source_replication(task)
    _delete_source_if_requested(task)


SPEC = RunnerSpec(
    name="tasks-runner-replication-final",
    function_names=[JobSchedule.FN_REPLICATION_FINAL],
    handler=task_runner,
)


SHRINK_ROUNDS = 2


def _shrink_step(task, lvol):
    """Advance the delta-shrink state machine one step.

    Returns once all rounds are replicated and the cutover may start
    IMMEDIATELY. Raises :class:`TaskDefer` while a round is still replicating —
    waiting for the target is not a failure, so it must not consume a retry; the
    wait is bounded by ``shrink_deadline`` instead.
    """
    params = task.function_params
    if int(time.time()) > params.get("shrink_deadline", 0):
        raise TaskRetry("shrink phase timed out waiting for replication")

    snap_id = params["shrink_snap_id"]
    try:
        snap = db.get_snapshot_by_id(snap_id)
    except KeyError:
        raise TaskRetry(f"shrink snapshot {snap_id} disappeared")

    # target_replicated_snap_uuid is set at replicate-finish AFTER the target
    # copy is chained and converted — replicated AND converted in one signal.
    if not snap.target_replicated_snap_uuid:
        raise TaskDefer(f"shrink round {params['shrink_round']}: waiting "
                        f"for {snap_id[:8]} to replicate")

    if params["shrink_round"] >= SHRINK_ROUNDS:
        return

    # Round replicated — IMMEDIATELY take the next snapshot: its delta covers
    # only the wait window of the previous round.
    from simplyblock_core.controllers import snapshot_controller
    new_snap, err = snapshot_controller.add(
        lvol.get_id(), f"repl_commit_{uuid_lib.uuid4()}",
        snap_type=SnapShot.TYPE_INTERNAL)
    if err:
        raise TaskRetry(f"shrink round {params['shrink_round'] + 1} snapshot failed: {err}")
    params["shrink_round"] += 1
    params["shrink_snap_id"] = new_snap
    raise TaskDefer(f"shrink round {params['shrink_round']}: snapshot taken")


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
        raise TaskRetry(f"cutover clone failed: {error}")

    new_lvol.status = LVol.STATUS_ONLINE
    new_lvol.write_to_db(db.kv_store)
    # Expose inaccessible until the final step flips ANA.
    lvol_controller.suspend_lvol(new_lvol.get_id())

    tgt_map_id = lvol_controller._resolve_target_map_id(tgt_node, new_lvol.lvol_bdev)
    if tgt_map_id is None:
        lvol_controller.delete_lvol_from_node(new_lvol, tgt_node)
        db.release_lvol_ns_slot(new_lvol)
        raise TaskRetry("could not resolve target map_id")

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
    # Checkpoint the task itself, against the driver's usual ownership of task
    # writes: the clone and the LVolReplication record above already exist, so a
    # crash before the cutover completes must not let the next attempt build a
    # second clone.
    task.write_to_db(db.kv_store)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
