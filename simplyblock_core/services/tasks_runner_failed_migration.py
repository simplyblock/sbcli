# coding=utf-8
import time

from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import tasks_controller, device_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import migration_task_common as mig
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskRetry,
    serve,
)

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def task_runner(task):
    snode = mig.require_node(task)
    mig.require_active_cluster(task)

    if not mig.migration_started(task):
        if not mig.nodes_settled(task.cluster_id):
            raise TaskRetry("node is online < 1 min, retrying")

    if snode.status != StorageNode.STATUS_ONLINE:
        raise TaskRetry("node is not online, retrying")

    if tasks_controller.get_new_device_mig_task_for_device(task.cluster_id):
        raise TaskRetry("dev expansion task found, retry")

    rpc_client = snode.rpc_client(timeout=5, retry=2)

    if not mig.migration_started(task):
        try:
            device = db.get_storage_device_by_id(task.device_id)
        except KeyError:
            raise TaskAbort("Device not found")

        started = mig.start_migration(task, lambda: rpc_client.distr_migration_failure_start(
            task.function_params["distr_name"], device.cluster_device_order,
            mig.qos_high_priority(snode.cluster_id),
            job_size=constants.MIG_JOB_SIZE, jobs=constants.MIG_PARALLEL_JOBS))
        if started is None:
            raise TaskAbort("canceled while starting migration")
        task = started
        time.sleep(3)

    mig.poll_migration(task, rpc_client)


def tag_device_migrated(task):
    """The device's data now lives elsewhere, so record that on the device.

    Runs from the driver's on_finish rather than inline: the check below asks
    whether any failed-migration task for this device is still open, and this
    task only stops counting as open once it has been written DONE. Inline it
    would always find itself and never tag anything — which the old code got
    away with only because its handle_task_result wrote DONE first.
    """
    if not mig.migration_started(task):
        return  # never got as far as moving any data
    if tasks_controller.get_failed_device_mig_task(task.cluster_id, task.device_id):
        return
    device_controller.device_set_failed_and_migrated(task.device_id)


SPEC = RunnerSpec(
    name="tasks-runner-failed-migration",
    function_names=[JobSchedule.FN_FAILED_DEV_MIG],
    handler=task_runner,
    on_finish=tag_device_migrated,
    is_eligible=mig.sibling_eligibility,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
