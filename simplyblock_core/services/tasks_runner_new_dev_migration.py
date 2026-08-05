# coding=utf-8
import time

from simplyblock_core import db_controller, utils, constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import migration_task_common as mig
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskRetry,
    serve,
)

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def _recovery_migration_open(cluster_id):
    """Recovery-before-expansion priority: the expansion migration (this task
    family, queued when the role rebalance completes) must not run while any
    outage-recovery data migration is open — an unexpected node outage during
    the expansion queues those, and the required order is: expansion completes
    -> outage device migration drains -> expansion migration runs."""
    for task in db.get_job_tasks(cluster_id):
        if (task.function_name in (JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG)
                and task.status != JobSchedule.STATUS_DONE
                and task.canceled is False):
            return task
    return None


def task_runner(task):
    snode = mig.require_node(task)

    if snode.status != StorageNode.STATUS_ONLINE:
        raise TaskRetry("node is not online, retrying")

    mig.require_active_cluster(task)

    open_recovery = _recovery_migration_open(task.cluster_id)
    if open_recovery is not None:
        # Deferral, not failure: no retry consumed.
        raise TaskDefer(f"deferring: recovery migration {open_recovery.uuid} "
                        f"({open_recovery.function_name}) is open")

    if not mig.migration_started(task):
        if not mig.nodes_settled(task.cluster_id, primaries_only=True):
            raise TaskRetry("node is online < 1 min, retrying")

    rpc_client = snode.rpc_client(timeout=5, retry=2)

    if not mig.migration_started(task):
        if not _all_devices_online_or_written_off(task.cluster_id):
            raise TaskRetry("Some devs are offline, retrying")

        try:
            db.get_storage_device_by_id(task.device_id)
        except KeyError:
            raise TaskAbort("Device not found")

        started = mig.start_migration(task, lambda: rpc_client.distr_migration_expansion_start(
            task.function_params["distr_name"], mig.qos_high_priority(snode.cluster_id),
            job_size=constants.MIG_JOB_SIZE, jobs=constants.MIG_PARALLEL_JOBS))
        if started is None:
            raise TaskAbort("canceled while starting migration")
        task = started
        time.sleep(3)

    mig.poll_migration(task, rpc_client, allow_all_errors=mig.allow_all_migration_errors(
        task.cluster_id, [NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]))


def _all_devices_online_or_written_off(cluster_id):
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            if dev.status not in [NVMeDevice.STATUS_ONLINE,
                                  NVMeDevice.STATUS_FAILED_AND_MIGRATED,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                return False
    return True


SPEC = RunnerSpec(
    name="tasks-runner-new-dev-migration",
    function_names=[JobSchedule.FN_NEW_DEV_MIG],
    handler=task_runner,
    is_eligible=mig.sibling_eligibility,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
