# coding=utf-8
import time


from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def task_runner(task):
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if 0 <= task.max_retry <= task.retry:
        task.function_result = "max retry reached, stopping task"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    ret = fdb_backup_controller.create_backup(task.cluster_id)
    if ret:
        task.function_result = "Backup created"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)


def main():
    logger.info("Starting Tasks runner fdb backup...")

    while True:
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                if cl.status == Cluster.STATUS_IN_ACTIVATION:
                    continue

                tasks = db.get_job_tasks(cl.get_id())
                for task in tasks:
                    if task.status != JobSchedule.STATUS_DONE:
                        if task.function_name == JobSchedule.FN_FDB_BACKUP:
                            task_runner(task)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
