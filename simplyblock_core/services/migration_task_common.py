# coding=utf-8
"""Shared pieces of the three device-migration runners.

``tasks_runner_migration`` (FN_DEV_MIG), ``tasks_runner_new_dev_migration``
(FN_NEW_DEV_MIG) and ``tasks_runner_failed_migration`` (FN_FAILED_DEV_MIG) all
drive the same data-plane operation — start a distr migration, then poll
``distr_migration_status`` until it settles — and differ only in what they
migrate and when they are allowed to start.
"""
from datetime import datetime, timezone

from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services.task_runner_base import (
    TaskAbort,
    TaskDefer,
    TaskProgress,
    TaskRetry,
    checkpoint,
)

logger = utils.get_logger(__name__)

db = db_controller.DBController()

MIGRATION_WAIT_UNAVAILABLE_KEY = "wait_unavailable_before_retry"

# A migration may only start once the cluster has been serving for a moment:
# a node that just came back is still settling its lvstore.
NODE_SETTLE_SEC = 60


def migration_started(task):
    """Whether this task has already issued its data-plane migration.

    The runners used to read this off ``task.status`` being RUNNING. That no
    longer distinguishes anything — the driver moves a task to RUNNING before
    calling the handler — so the marker the start step writes is used directly,
    which is what the status was standing in for anyway.
    """
    return "migration" in task.function_params


def require_active_cluster(task):
    # IN_SHRINK is operable: draining data off the departing node is the node
    # removal's own work, and removal blocks until every data device reaches
    # FAILED_AND_MIGRATED — which only this family sets. Refusing here would
    # deadlock the removal against the status it set itself.
    cluster = db.get_cluster_by_id(task.cluster_id)
    if not cluster.allows_operation():
        raise TaskRetry("cluster is not active, retrying")

    # Expansion-first ordering: no data migration runs while a cluster
    # expansion is open — even between the expand task's retries, when the
    # cluster status is momentarily ACTIVE. Without this, an outage's recovery
    # migrations would start in that window and then block the expansion
    # resume, inverting the required order (expansion completes FIRST, then
    # outage device migration, then expansion migration). A deferral, not a
    # failure: no retry consumed.
    if tasks_controller.get_active_cluster_expand_task(task.cluster_id):
        raise TaskDefer("cluster expansion in progress, deferring")


def require_node(task):
    try:
        return db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort(f"Node not found: {task.node_id}")


def nodes_settled(cluster_id, primaries_only=False):
    """False while any node has been back for less than NODE_SETTLE_SEC.

    The three runners disagree on whether waiting for this costs a retry, so
    the verdict is returned rather than raised.
    """
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if primaries_only and node.is_secondary_node:
            continue
        if not node.online_since:
            continue
        try:
            settled_for = datetime.now(timezone.utc) - datetime.fromisoformat(node.online_since)
        except Exception as e:
            logger.error(f"Failed to get online since: {e}")
            continue
        if settled_for.total_seconds() < NODE_SETTLE_SEC:
            return False
    return True


def cluster_unavailable_state(cluster_id):
    """Nodes and devices that are neither serving nor written off, as stable
    ids — the set a migration waits on before (re)starting."""
    unavailable = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]:
            continue
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            unavailable.append(f"node:{node.get_id()}")
        for dev in node.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                continue
            if dev.status != NVMeDevice.STATUS_ONLINE:
                unavailable.append(f"dev:{dev.get_id()}")
    return sorted(unavailable)


def require_recovery_progress(task, unavailable):
    """Hold a migration back while the cluster is degraded, releasing it only
    when something actually recovers.

    Retrying against an unchanged set of unavailable nodes/devices just burns
    the budget, so the set is recorded on the task and compared: an unchanged
    (or grown) set defers, while any member coming back is the recovery event
    that lets the migration proceed.
    """
    previous = sorted(task.function_params.get(MIGRATION_WAIT_UNAVAILABLE_KEY, []))

    if not unavailable:
        if previous:
            task.function_params.pop(MIGRATION_WAIT_UNAVAILABLE_KEY, None)
        return

    recovered = set(previous) - set(unavailable)
    task.function_params[MIGRATION_WAIT_UNAVAILABLE_KEY] = unavailable
    if previous and recovered:
        logger.info("Migration retry allowed after recovery event for task %s: %s",
                    task.uuid, sorted(recovered))
        return

    raise TaskDefer("waiting for unavailable nodes/devices to recover before "
                    f"restarting migration: {unavailable}")


def start_migration(task, start):
    """Issue the data-plane migration and record that it was issued.

    ``start`` is the runner's own RPC call, returning falsy on failure. The
    marker is checkpointed immediately: a crash between the RPC and the end of
    the handler would otherwise lose it and start a second migration.
    """
    try:
        started = start()
    except Exception as e:
        logger.error(e)
        started = False
    if not started:
        raise TaskRetry("Failed to start device migration task, retry later")

    return checkpoint(task, migration={"name": task.function_params["distr_name"]})


def report_migration_status(task, res, allow_all_errors=False, allowed_error_codes=None):
    """Translate a ``distr_migration_status`` poll into the task's outcome."""
    if not res:
        raise TaskRetry("Failed to get mig status")

    allowed_error_codes = allowed_error_codes or [0]
    res_data = res[0]
    migration_status = res_data.get("status")
    error_code = res_data.get("error", -1)
    progress = res_data.get("progress", -1)

    if migration_status == "completed":
        if error_code == 0:
            task.function_result = "Done"
            return
        if error_code in allowed_error_codes or allow_all_errors:
            task.function_result = f"mig completed with status: {error_code}"
            return
        # Drop the marker so the next attempt starts a fresh migration rather
        # than polling the one that just errored.
        del task.function_params['migration']
        raise TaskRetry(f"mig error: {error_code}, retrying")

    if migration_status == "failed":
        raise TaskAbort(migration_status)

    if migration_status == "none":
        del task.function_params['migration']
        raise TaskRetry("mig retry after restart")

    raise TaskProgress(f"Status: {migration_status}, progress:{progress}")


def no_sibling_migration(task):
    """Eligibility: one migration at a time per node.

    Only gates a task that has not started yet — once its own migration is
    running it is itself the sibling everything else waits on.
    """
    if migration_started(task):
        return True
    return not tasks_controller.get_active_node_mig_task(
        task.cluster_id, task.node_id, task.function_params.get("distr_name"))


def allow_all_migration_errors(cluster_id, statuses):
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            if dev.status in statuses:
                return True
    return False


def poll_migration(task, rpc_client, allow_all_errors=False):
    try:
        res = rpc_client.distr_migration_status(**task.function_params["migration"])
    except (TaskAbort, TaskDefer, TaskProgress, TaskRetry):
        raise
    except Exception as e:
        logger.error("Failed to get migration task status")
        logger.exception(e)
        raise TaskRetry("Failed to get migration status")

    report_migration_status(task, res, allow_all_errors=allow_all_errors)


def qos_high_priority(cluster_id):
    return db.get_cluster_by_id(cluster_id).is_qos_set()


def sibling_eligibility(task, cluster):
    """Spec-shaped wrapper of :func:`no_sibling_migration`."""
    return no_sibling_migration(task)
