# coding=utf-8
"""Task runner for cross-cluster replication cutover (FN_REPLICATION_FINAL).

Consumes the task enqueued by migration-commit (D7) and fail-back (D8). The
setup work — creating the writable target lvol on top of the last replicated
snapshot and resolving its map_id / snapshot composite — is done by the
controller before the task is queued; this runner performs the IO-freeze cutover
via the shared :mod:`replication_final_step` module and finalizes the
LVolReplication state.

task.function_params:
    lvol_id, src_node_id, tgt_node_id, tgt_lvol_composite, tgt_map_id,
    tgt_snap_composite, operation, replication_id, final_state
"""
import time

from simplyblock_core import db_controller, utils
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVolReplication
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import replication_final_step
from simplyblock_core.services.task_runner_base import RunnerSpec, TaskRetry, serve

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
    ok, err = replication_final_step.run_cutover(
        src_node, tgt_node, lvol,
        params["tgt_lvol_composite"], params["tgt_map_id"],
        params["tgt_snap_composite"], operation=params.get("operation", "replicate"))
    if not ok:
        raise TaskRetry(err or "cutover failed, retrying")

    _record_cutover_done(task)
    params["end_time"] = int(time.time())
    task.function_result = "cutover done"


SPEC = RunnerSpec(
    name="tasks-runner-replication-final",
    function_names=[JobSchedule.FN_REPLICATION_FINAL],
    handler=task_runner,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
