# coding=utf-8
import time


from simplyblock_core import db_controller, storage_node_ops, utils
from simplyblock_core.controllers import tasks_events
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db_controller = db_controller.DBController()


logger.info("Starting Tasks runner...")
while True:

    clusters = db_controller.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            tasks = db_controller.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:

                if task.function_name == JobSchedule.FN_NODE_ADD:
                    if task.status != JobSchedule.STATUS_DONE:

                        # get new task object because it could be changed from cancel task
                        task = db_controller.get_task_by_id(task.uuid)

                        if task.canceled:
                            task.function_result = "canceled"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db_controller.kv_store)
                            continue

                        if db_controller.get_cluster_by_id(cl.get_id()).status == Cluster.STATUS_IN_ACTIVATION:
                            task.function_result = "Cluster is in_activation, waiting"
                            task.status = JobSchedule.STATUS_NEW
                            task.write_to_db(db_controller.kv_store)
                            continue

                        if task.status != JobSchedule.STATUS_RUNNING:
                            task.status = JobSchedule.STATUS_RUNNING
                            task.write_to_db(db_controller.kv_store)

                        res = storage_node_ops.add_node(**task.function_params)
                        logger.info(f"Node add result: {res}")
                        task.function_result = str(res)
                        task.status = JobSchedule.STATUS_DONE
                        task.write_to_db(db_controller.kv_store)
                        tasks_events.task_updated(task)

    time.sleep(5)
