# coding=utf-8
import time
from typing import Optional

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import events_controller, snapshot_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

def get_primary_node(task) -> Optional[StorageNode]:
    if "primary_node" in task.function_params:
        return db.get_storage_node_by_id(task.function_params["primary_node"])

    nodes = db.get_primary_storage_nodes_by_secondary_node_id(task.node_id)
    if nodes:
        return nodes[0]
    return None


def _count_failure_and_maybe_alert(task, node, what, msg):
    """Count one genuine failure of this leg and escalate past the threshold.

    task.retry was never incremented by this runner (max_retry was declared
    and never enforced), so it is free to use as what it actually is here: a
    failure counter. It does not terminate anything -- these tasks are
    unbounded by design, because giving up leaks the volume -- it only
    decides when to raise an alert.

    The event fires once on crossing the threshold and again whenever the
    error CHANGES, so a node that stays broken does not write an event every
    3 seconds.
    """
    previous = task.function_result
    task.retry = (task.retry or 0) + 1
    if task.retry <= constants.TASK_FAILURE_ALERT_THRESHOLD:
        return
    if task.retry > constants.TASK_FAILURE_ALERT_THRESHOLD + 1 and previous == msg:
        return
    try:
        events_controller.log_event_cluster(
            cluster_id=node.cluster_id,
            domain=events_controller.DOMAIN_STORAGE,
            event="LVOL_TASK_FAILING_REPEATEDLY",
            db_object=task,
            caused_by=events_controller.CAUSED_BY_MONITOR,
            message=(f"{what} has failed {task.retry} times on node "
                     f"{node.get_id()}; it will keep retrying, but this is "
                     f"not a transient condition and needs attention. "
                     f"Last error: {msg}"),
            node_id=node.get_id(),
            event_level="Critical")
    except Exception as event_error:
        logger.warning(f"Could not log repeated-failure event: {event_error}")


def _log_sync_delete_failure(task, node, lvol_bdev_name, msg):
    """Record a failed sync delete in the cluster event log.

    This is the case an operator cannot see from the volume list alone: the
    async delete already SUCCEEDED, so the data is going away, but a node still
    holds its replica bdev and the volume is pinned in_deletion until this
    task drains. Emitted only when the failure message CHANGES (first failure,
    or a different error), not on every 3s retry -- a node that stays down
    would otherwise write thousands of identical events.
    """
    if task.function_result == msg:
        return
    try:
        events_controller.log_event_cluster(
            cluster_id=node.cluster_id,
            domain=events_controller.DOMAIN_STORAGE,
            event="SYNC_DELETE_FAILED",
            db_object=task,
            caused_by=events_controller.CAUSED_BY_MONITOR,
            message=(f"Sync delete of {lvol_bdev_name} failed on node "
                     f"{node.get_id()} after a successful async delete; the "
                     f"volume stays in_deletion until this drains. {msg}"),
            node_id=node.get_id(),
            event_level="Error")
    except Exception as event_error:
        logger.warning(f"Could not log sync-delete failure event: {event_error}")


def main():
    logger.info("Starting Tasks runner...")

    while True:
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                if cl.status == Cluster.STATUS_IN_ACTIVATION:
                    continue

                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    if task.function_name == JobSchedule.FN_LVOL_SYNC_OP:
                        if task.status != JobSchedule.STATUS_DONE:
                            if not tasks_controller.claim_task(task):
                                logger.info(f"LVol sync task {task.uuid} owned by another runner host; skipping")
                                continue
                            # Re-read (it may have been canceled concurrently).
                            task = db.get_task_by_id(task.uuid)
                            if task.status == JobSchedule.STATUS_DONE:
                                continue
                            try:
                                tasks_controller.run_lvol_sync_op_task(task)
                            except Exception as e:
                                logger.error(f"lvol sync-op task {task.uuid} crashed: {e}")
                        continue

                    if task.function_name == JobSchedule.FN_LVOL_SYNC_DEL:
                        if task.status != JobSchedule.STATUS_DONE:
                            if not tasks_controller.claim_task(task):
                                logger.info(f"LVol sync task {task.uuid} owned by another runner host; skipping")
                                continue

                            # get new task object because it could be changed from cancel task
                            task = db.get_task_by_id(task.uuid)

                            if task.canceled:
                                task.function_result = "canceled"
                                task.status = JobSchedule.STATUS_DONE
                                task.write_to_db(db.kv_store)
                                primary_node = get_primary_node(task)
                                if primary_node:
                                    primary_node.lvol_del_sync_lock_reset()
                                continue

                            node = db.get_storage_node_by_id(task.node_id)

                            if not node:
                                task.function_result = "node not found"
                                task.status = JobSchedule.STATUS_DONE
                                task.write_to_db(db.kv_store)
                                primary_node = db.get_storage_node_by_id(task.function_params["primary_node"])
                                primary_node.lvol_del_sync_lock_reset()
                                continue

                            # The node must be ONLINE, not merely "not in a
                            # transitional state". DOWN was accepted here, so
                            # the runner fired the RPC at a node that cannot
                            # answer; it failed and suspended, which works but
                            # is pure noise. register (run_lvol_sync_op_task)
                            # already requires ONLINE.
                            if node.status != StorageNode.STATUS_ONLINE:
                                msg = f"Node is {node.status}, retry task"
                                logger.info(msg)
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)
                                continue

                            # Re-check the restart phase. This leg was queued
                            # BECAUSE a restart owned the LVS state
                            # (check_non_leader_for_operation returns "queue"
                            # for PRE_BLOCK/BLOCKED/POST_UNBLOCK); draining it
                            # without re-testing that condition undoes the
                            # deferral the inline path just made. register
                            # already guards on this.
                            # Imported here, not at module scope:
                            # storage_node_ops pulls in the controllers that
                            # import this module (tasks_controller does the
                            # same for the identical reason).
                            from simplyblock_core import storage_node_ops
                            lvs_name_of_task = task.function_params[
                                "lvol_bdev_name"].split("/")[0]
                            if storage_node_ops.get_restart_phase(
                                    node.get_id(), lvs_name_of_task):
                                msg = "LVS owned by a restart, retry task"
                                logger.info(msg)
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)
                                continue

                            if task.status != JobSchedule.STATUS_RUNNING:
                                task.status = JobSchedule.STATUS_RUNNING
                                task.write_to_db(db.kv_store)

                            lvol_bdev_name = task.function_params["lvol_bdev_name"]

                            logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
                            try:
                                # Per-node lvstore lock: the sync delete mutates
                                # the replica blob tree and must not interleave
                                # with a create/register of another object on
                                # this node. The try also keeps a dead node from
                                # killing the runner: on 2026-07-16 an unhandled
                                # RPCException ('connection error') here took the
                                # whole service down and no deferred sync delete
                                # ever ran again.
                                with snapshot_controller.lvstore_op_lock(
                                        node.cluster_id,
                                        lvol_bdev_name.split("/")[0],
                                        node_id=node.get_id()):
                                    ret, err = node.rpc_client().delete_lvol(lvol_bdev_name, sync=True)
                            except Exception as e:
                                msg = (f"Sync delete of {lvol_bdev_name} on {node.get_id()} "
                                       f"failed: {e}; will retry")
                                logger.error(msg)
                                _log_sync_delete_failure(task, node, lvol_bdev_name, msg)
                                _count_failure_and_maybe_alert(
                                    task, node,
                                    f"Sync delete of {lvol_bdev_name}", msg)
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)
                                continue
                            if not ret:
                                if "code" in err and err["code"] == -19:
                                    logger.error(f"Sync delete completed with error: {err}")
                                else:
                                    msg =  f"Failed to sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}"
                                    logger.error(msg)
                                    _log_sync_delete_failure(task, node, lvol_bdev_name, msg)
                                    # -19 never reaches here: it is handled
                                    # above as success (the peer is already
                                    # clean), so it is never counted.
                                    _count_failure_and_maybe_alert(
                                        task, node,
                                        f"Sync delete of {lvol_bdev_name}", msg)
                                    task.function_result = msg
                                    task.status = JobSchedule.STATUS_SUSPENDED
                                    task.write_to_db(db.kv_store)
                                    continue

                            task.function_result = f"bdev {lvol_bdev_name} deleted"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            primary_node = get_primary_node(task)
                            if primary_node:
                                primary_node.lvol_del_sync_lock_reset()

        time.sleep(3)


if __name__ == "__main__":
    main()
