from datetime import datetime

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services.task_runner_base import RunnerSpec, TaskRetry, serve

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
        try:
            db_date = datetime.strptime(item.get("Date"), "%Y-%m-%d %H:%M:%S")
            if db_date < date_to_delete_before:
                logger.info("Deleting backup: %s", item.get("Name"))
                fdb_backup_controller.backup_delete(cl.get_backup_path(item.get("Name")))
        except Exception as e:
            logger.error("Failed to parse date from backup item: %s, %s", item, e)


def process_fdb_backup_task(task):
    if not fdb_backup_controller.create_backup(task.cluster_id):
        raise TaskRetry("failed to create backup")

    task.function_result = "Backup created"
    prune_old_backups(task.cluster_id)


SPEC = RunnerSpec(
    name="tasks-runner-fdb-backup",
    function_names=[JobSchedule.FN_FDB_BACKUP],
    handler=process_fdb_backup_task,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
