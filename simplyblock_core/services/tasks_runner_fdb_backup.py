import time
from datetime import datetime

from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.controllers import fdb_backup_events
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def prune_old_backups(cluster_id):
    ret = fdb_backup_controller.list_backups(cluster_id)
    if not ret:
        return

    if not isinstance(ret, (list, tuple)):
        logger.error("Unexpected backup list response type: %s", type(ret).__name__)
        return

    cl = db.get_cluster_by_id(cluster_id)
    logger.info("Pruning old backups")
    date_to_delete_before = datetime.now()
    date_to_delete_before = date_to_delete_before.replace(day=date_to_delete_before.day - cl.backup_retention_days)
    for item in ret:
        # date = datetime.datetime.strptime(param[1:-1], "%Y/%m/%d.%H:%M:%S+0000").strftime("%Y-%m-%d %H:%M:%S")
        try:
            db_date = datetime.strptime(item.get("Date"), "%Y-%m-%d %H:%M:%S")
            if db_date < date_to_delete_before:
                logger.info("Deleting backup: %s", item.get("Name"))
                fdb_backup_controller.backup_delete(cl.get_backup_path(item.get("Name")))
        except Exception as e:
            logger.error("Failed to parse date from backup item: %s, %s", item, e)


def process_fdb_backup_task(task):
    task = db.get_task_by_id(task.uuid)
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached, stopping task"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        fdb_backup_events.fdb_backup_failed(task.cluster_id, task)
        return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    ret = fdb_backup_controller.create_backup(task.cluster_id)
    if ret:
        task.function_result = "Backup created"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        prune_old_backups(task.cluster_id)
    else:
        task.retry += 1
        task.status = JobSchedule.STATUS_SUSPENDED
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
                            process_fdb_backup_task(task)

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
