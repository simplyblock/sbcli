# coding=utf-8
import time


from simplyblock_core import db_controller, storage_node_ops, utils, constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_task(task):
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return False

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if db.get_cluster_by_id(cl.get_id()).status == Cluster.STATUS_IN_ACTIVATION:
        task.function_result = "Cluster is in_activation, waiting"
        task.status = JobSchedule.STATUS_NEW
        task.write_to_db(db.kv_store)
        return False

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    try:
        res = storage_node_ops.add_node(**task.function_params)
        msg = f"Node add result: {res}"
        logger.info(msg)
        task.function_result = msg
        if res:
            task.status = JobSchedule.STATUS_DONE
        else:
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return True
    except Exception as e:
        logger.error(e)
        return False


logger.info("Starting Tasks runner node add...")

while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                if task.function_name == JobSchedule.FN_NODE_ADD:
                    while task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = process_task(task)
                        if res:
                            if task.status == JobSchedule.STATUS_DONE:
                                break
                        else:
                            delay_seconds *= 2
                        time.sleep(delay_seconds)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
