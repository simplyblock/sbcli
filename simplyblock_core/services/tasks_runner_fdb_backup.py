# coding=utf-8
from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import fdb_backup_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services.task_runner_base import RunnerSpec, TaskRetry, serve

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_fdb_backup_task(task):
    if not fdb_backup_controller.create_backup(task.cluster_id):
        raise TaskRetry("failed to create backup")

    task.function_result = "Backup created"


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
