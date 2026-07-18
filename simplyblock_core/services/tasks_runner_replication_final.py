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

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVolReplication
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

    if (0 <= task.max_retry <= task.retry) or task.canceled is True:
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


def main():
    logger.info("Starting replication-final tasks runner...")
    while True:
        clusters = db.get_clusters()
        for cl in clusters:
            for task in db.get_job_tasks(cl.get_id(), reverse=False):
                if task.function_name != JobSchedule.FN_REPLICATION_FINAL:
                    continue
                if task.status == JobSchedule.STATUS_DONE:
                    continue
                task = db.get_task_by_id(task.uuid)
                if not tasks_controller.claim_task(task):
                    logger.info(f"Replication-final task {task.uuid} owned by another runner host; skipping")
                    continue
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
