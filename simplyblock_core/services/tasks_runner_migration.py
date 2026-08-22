# coding=utf-8
from simplyblock_core import db_controller, utils, constants
from simplyblock_core.controllers import tasks_events, tasks_controller, lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import jc_compression_upgrade
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

MIGRATION_WAIT_UNAVAILABLE_KEY = mig.MIGRATION_WAIT_UNAVAILABLE_KEY


def _online_device_count(cluster_id, primaries_only=False):
    online = 0
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if primaries_only and node.is_secondary_node:
            continue
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online += 1
    return online


def _wait_for_cluster_recovery(task):
    """Gate a not-yet-started migration on the cluster being whole enough.

    Whether waiting costs a retry depends on why: with nothing unavailable the
    hold-up is transient and counts against the budget, but while nodes or
    devices are down it is the recovery we are waiting on, and burning retries
    against it would terminate the migration before the cluster came back.
    """
    unavailable = mig.cluster_unavailable_state(task.cluster_id)
    if not unavailable:
        raise TaskRetry(task.function_result or "waiting to start migration, retrying")
    mig.require_recovery_progress(task, unavailable)


def task_runner(task):
    snode = mig.require_node(task)

    if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
        task.function_result = "node is not online, retrying"
        _wait_for_cluster_recovery(task)
        raise TaskDefer("node is not online, retrying")

    mig.require_active_cluster(task)

    if tasks_controller.get_active_lvol_migration(task.node_id):
        raise TaskRetry("LVol migration tasks found, retrying")

    if not mig.migration_started(task):
        if not mig.nodes_settled(task.cluster_id, primaries_only=True):
            raise TaskDefer("node is online < 1 min, retrying")

        online_devices = _online_device_count(task.cluster_id, primaries_only=True)
        wanted = task.function_params.get("migration_devices", 0)
        if online_devices < wanted:
            task.function_result = (f"only {online_devices} devices online, waiting for "
                                    f"more devices to be online")
            _wait_for_cluster_recovery(task)
            raise TaskDefer(task.function_result)

        mig.require_recovery_progress(task, mig.cluster_unavailable_state(task.cluster_id))

    rpc_client = snode.rpc_client(timeout=5, retry=2)

    # Only start migration on a node that is the leader for its primary LVS.
    # Migration IO triggers auto-leader promotion in the data plane, so
    # starting migration on a non-leader causes a split-brain write conflict.
    if not snode.is_secondary_node and not lvol_controller.is_node_leader(snode, snode.lvstore):
        raise TaskRetry(f"Node {snode.get_id()} is not the leader for {snode.lvstore}, "
                        f"deferring migration")

    if not mig.migration_started(task):
        # Recorded alongside the start so a later poll can tell how much of the
        # cluster the migration was sized against.
        task.function_params["migration_devices"] = _online_device_count(task.cluster_id)
        started = mig.start_migration(task, lambda: rpc_client.distr_migration_expansion_start(
            task.function_params["distr_name"], mig.qos_high_priority(snode.cluster_id),
            job_size=constants.MIG_JOB_SIZE, jobs=constants.MIG_PARALLEL_JOBS))
        if started is None:
            raise TaskAbort("canceled while starting migration")
        task = started

    mig.poll_migration(task, rpc_client, allow_all_errors=mig.allow_all_migration_errors(
        task.cluster_id, [NVMeDevice.STATUS_READONLY,
                          NVMeDevice.STATUS_CANNOT_ALLOCATE,
                          NVMeDevice.STATUS_FAILED]))


def _is_eligible(task, cluster):
    """One migration at a time per node and distr.

    Beyond the shared sibling rule, a device migration also waits behind a
    SUSPENDED new-device migration for the same distr: that one is mid-flight
    on the data plane even while its task is parked, and starting a second
    migration over the same distr would collide with it.
    """
    if mig.migration_started(task):
        return True

    distr_name = task.function_params.get("distr_name")
    for sibling in db.get_job_tasks(task.cluster_id):
        if sibling.function_name not in [JobSchedule.FN_FAILED_DEV_MIG,
                                         JobSchedule.FN_DEV_MIG,
                                         JobSchedule.FN_NEW_DEV_MIG]:
            continue
        if sibling.node_id != task.node_id or sibling.canceled:
            continue
        if sibling.function_params.get("distr_name") != distr_name:
            continue
        if sibling.status == JobSchedule.STATUS_RUNNING:
            return False
        if (sibling.status == JobSchedule.STATUS_SUSPENDED
                and sibling.function_name == JobSchedule.FN_NEW_DEV_MIG):
            return False
    return True


def update_master_tasks(cluster_id):
    """Roll every master task's sub-task statuses up into it.

    Per cycle rather than per sub-task attempt: the roll-up reads all of a
    master's sub-tasks anyway, so running it once a cycle both covers every
    status change (not only the attempts that finish a sub-task) and drops the
    redundant re-computation each sibling used to trigger.
    """
    tasks = {t.uuid: t for t in db.get_job_tasks(cluster_id, reverse=False)}
    for master_task in list(tasks.values()):
        if master_task.sub_tasks:
            _roll_up(master_task, tasks)


def _roll_up(master_task, tasks):
    status_map = {
        JobSchedule.STATUS_DONE: 0,
        JobSchedule.STATUS_NEW: 0,
        JobSchedule.STATUS_SUSPENDED: 0,
        JobSchedule.STATUS_RUNNING: 0,
    }
    for sub_task_id in master_task.sub_tasks:
        sub_task = tasks.get(sub_task_id)
        if sub_task is None:
            return
        status_map[sub_task.status] = status_map.get(sub_task.status, 0) + 1

    total = len(master_task.sub_tasks)
    for status in (JobSchedule.STATUS_DONE, JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED):
        if status_map[status] == total:
            rolled_up = status
            break
    else:
        rolled_up = JobSchedule.STATUS_RUNNING

    if master_task.status == rolled_up:
        return
    logger.info(f"_set_master_task_status: {rolled_up}")
    master_task.status = rolled_up
    master_task.function_result = rolled_up
    master_task.write_to_db(db.kv_store)
    tasks_events.task_updated(master_task)


def resume_jc_compression(task):
    """A finished migration frees the node for JC compression again."""
    if tasks_controller.get_active_node_tasks(task.cluster_id, task.node_id):
        return

    # Release-upgrade guard (remove with the jc_compression_upgrade plugin):
    # resumes are held until `cluster upgrade-complete`.
    if jc_compression_upgrade.resume_is_held(db.get_cluster_by_id(task.cluster_id)):
        logger.info("JC compression resume held: cluster upgrade in progress")
        return

    logger.info("no task found on same node, resuming compression")
    node = db.get_storage_node_by_id(task.node_id)
    for peer in db.get_storage_nodes_by_cluster_id(node.cluster_id):
        if peer.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            logger.warning("Not all nodes are online, can not resume JC compression")
    try:
        _, err = node.rpc_client(timeout=5, retry=2).jc_suspend_compression(
            jm_vuid=node.jm_vuid, suspend=False)
        if err:
            logger.info("Failed to resume JC compression adding task...")
            tasks_controller.add_jc_comp_resume_task(task.cluster_id, task.node_id, node.jm_vuid)
    except Exception as e:
        logger.error(e)


SPEC = RunnerSpec(
    name="tasks-runner-migration",
    function_names=[JobSchedule.FN_DEV_MIG],
    handler=task_runner,
    on_finish=resume_jc_compression,
    on_cycle=lambda cluster: update_master_tasks(cluster.get_id()),
    is_eligible=_is_eligible,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
