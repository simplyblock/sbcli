# coding=utf-8
import time


from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.controllers.cluster_expansion.executor import (
    integrate_new_node_into_cluster,
)
from simplyblock_core.controllers.cluster_expansion.planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    expand_state_rearm,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def task_runner(task):
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return False

    # max_retry < 0 means never give up (the plan must complete — an
    # abandoned half-moved topology strands stale sec/tert pointers);
    # cancel the task to stop it deliberately.
    if 0 <= task.max_retry <= task.retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    new_node_id = task.function_params.get("new_node_id")
    if not new_node_id:
        task.function_result = "missing new_node_id in function_params"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    try:
        cluster = db.get_cluster_by_id(task.cluster_id)
        new_snode = db.get_storage_node_by_id(new_node_id)

        # Retry-by-resume: a prior attempt that aborted left expand_state at
        # the failed move's cursor. Flip it back to in_progress so the
        # orchestrator re-attempts that move instead of recomputing a fresh
        # diff against a topology the aborted run may have partially mutated.
        if (cluster.expand_state or {}).get("phase") == EXPAND_PHASE_ABORTED:
            cluster.expand_state = expand_state_rearm(cluster.expand_state)
            cluster.write_to_db()

        integrate_new_node_into_cluster(
            cluster, new_snode, db_controller=db,
            manage_cluster_status=True)

        # integrate_new_node_into_cluster returns only on success; the
        # orchestrator marks expand_state completed. Re-read to confirm.
        cluster = db.get_cluster_by_id(task.cluster_id)
        phase = (cluster.expand_state or {}).get("phase")
        if phase == EXPAND_PHASE_COMPLETED:
            # Queue new-device migration now that the rotation has landed and
            # the cluster is back to ACTIVE — tasks are created against the
            # post-rotation lvstore_stack (which includes the newcomer's
            # primary distr). Mirrors the trigger the non-expansion add path
            # runs inside add_node.
            new_snode = db.get_storage_node_by_id(new_node_id)
            for dev in new_snode.nvme_devices:
                if dev.status == NVMeDevice.STATUS_ONLINE:
                    tasks_controller.add_new_device_mig_task(dev.get_id())
            task.function_result = f"expansion complete: {new_node_id}"
            task.status = JobSchedule.STATUS_DONE
        else:
            # Shouldn't happen (no exception but not completed) — suspend and
            # retry rather than silently mark done.
            task.function_result = f"unexpected phase after run: {phase!r}"
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return True
    except Exception as e:
        logger.error(e)
        task.function_result = f"attempt {task.retry + 1} failed: {e}"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False


def main():
    logger.info("Starting Tasks runner cluster expand...")
    while True:
        try:
            clusters = db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                    if task.function_name != JobSchedule.FN_CLUSTER_EXPAND:
                        continue
                    # Per-task isolation: a crash in task_runner must not
                    # escape to the outer loop and kill the runner.
                    try:
                        while task.status != JobSchedule.STATUS_DONE:
                            # Re-fetch: the task may have been cancelled.
                            task = db.get_task_by_id(task.uuid)
                            # Lease gate: skip a task another live runner owns.
                            if not tasks_controller.claim_task(task):
                                logger.info(
                                    f"Cluster-expand task {task.uuid} owned by "
                                    f"another runner host; skipping")
                                break
                            res = task_runner(task)
                            if res:
                                if task.status == JobSchedule.STATUS_DONE:
                                    break
                            else:
                                # Cap the exponential backoff so a permanently
                                # failing expansion can't grow the sleep without
                                # bound.
                                delay_seconds = min(
                                    delay_seconds * 2,
                                    constants.RESTART_TASK_EXEC_INTERVAL_MAX_SEC,
                                )
                            time.sleep(delay_seconds)
                    except Exception as e:
                        logger.error(
                            f"Cluster-expand task {task.uuid} processing "
                            f"crashed: {e}")
                        logger.exception(e)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
