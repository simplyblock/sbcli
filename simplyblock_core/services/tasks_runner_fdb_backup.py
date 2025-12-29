# coding=utf-8
import time


from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import backup_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

logger.info("Starting Tasks runner fdb backup...")

while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            tasks = tasks_controller.get_backup_tasks(cl.get_id())
            for task in tasks:
                if task.status != JobSchedule.STATUS_DONE:
                    task = db.get_task_by_id(task.uuid)
                    if task.canceled:
                        task.function_result = "canceled"
                        task.status = JobSchedule.STATUS_DONE
                        task.write_to_db(db.kv_store)
                        continue

                    if task.retry >= task.max_retry:
                        task.function_result = "max retry reached, stopping task"
                        task.status = JobSchedule.STATUS_DONE
                        task.write_to_db(db.kv_store)
                        continue

                    if task.status != JobSchedule.STATUS_RUNNING:
                        task.status = JobSchedule.STATUS_RUNNING
                        task.write_to_db(db.kv_store)

                    ret = backup_controller.create_backup()
                    if ret:
                        task.function_result = "Backup created"
                        task.status = JobSchedule.STATUS_DONE
                        task.write_to_db(db.kv_store)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
