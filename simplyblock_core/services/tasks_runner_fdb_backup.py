# coding=utf-8
from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_lib.tasks import TaskResult, TaskRunner

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


class FDBBackupRunner(TaskRunner):

    function_names = (JobSchedule.FN_FDB_BACKUP,)

    def execute(self, task):
        if fdb_backup_controller.create_backup(task.cluster_id):
            return TaskResult.done("Backup created")
        # Backup failed: leave the task untouched and re-attempt on the next
        # cycle (no retry consumed) — pre-refactor behavior.
        return None


def main():
    FDBBackupRunner(
        db,
        interval_sec=constants.TASK_EXEC_INTERVAL_SEC,
        cluster_filter=lambda cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
        logger=logger,
    ).run_forever()


if __name__ == "__main__":
    main()
