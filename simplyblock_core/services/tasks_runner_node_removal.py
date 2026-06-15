# coding=utf-8
import time


from simplyblock_core import db_controller, storage_node_ops, utils, constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_task(task):
    """Advance one node-removal task by one orchestration pass.

    node_removal_orchestrate is idempotent and resumable: it returns True only
    when the node is fully REMOVED, and False to mean "incomplete, retry later"
    (most commonly: device failure-migration still in progress). On False we
    suspend the task so the outer loop revisits it on the next tick instead of
    busy-spinning here for what can be hours.
    """
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    cluster = db.get_cluster_by_id(task.cluster_id)
    if cluster.status == Cluster.STATUS_IN_ACTIVATION:
        task.function_result = "cluster is in_activation, waiting"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    force_remove = bool(task.function_params.get("force_remove", False))
    try:
        done = storage_node_ops.node_removal_orchestrate(task.node_id, force_remove=force_remove)
    except Exception as e:
        logger.error(f"Node-removal task {task.uuid} raised: {e}")
        logger.exception(e)
        task.function_result = f"error: {e}"
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    if done:
        task.function_result = "Node removed"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    # Incomplete: a phase asked us to retry (typically waiting on migration).
    task.function_result = "removal in progress, retrying"
    task.retry += 1
    task.status = JobSchedule.STATUS_SUSPENDED
    task.write_to_db(db.kv_store)
    return False


logger.info("Starting Tasks runner node removal...")

while True:
    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
    try:
        clusters = db.get_clusters()
    except Exception as e:
        logger.error(f"Failed to get clusters: {e}")
        continue
    if not clusters:
        logger.error("No clusters found!")
        continue
    for cl in clusters:
        tasks = db.get_job_tasks(cl.get_id(), reverse=False)
        for task in tasks:
            if task.function_name != JobSchedule.FN_NODE_REMOVAL:
                continue
            if task.status == JobSchedule.STATUS_DONE:
                continue
            # get a fresh object: cancel/other writers may have changed it
            task = db.get_task_by_id(task.uuid)
            # Lease gate: skip a task another live runner host owns.
            if not tasks_controller.claim_task(task):
                logger.info(f"Node-removal task {task.uuid} owned by another runner host; skipping")
                continue
            try:
                process_task(task)
            except Exception as e:
                logger.error(f"Node-removal task {task.uuid} processing crashed: {e}")
                logger.exception(e)
