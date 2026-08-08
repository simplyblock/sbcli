# coding=utf-8
"""Task runner for the deferred per-node lvol operations.

Two task families share this runner, both of them work that could not be done
inline on the owning node at the time it was requested:

- ``FN_LVOL_SYNC_OP`` — re-apply a create-registration or a resize on a
  non-leader node (incident 2026-07-10: an in-memory deferral queue was never
  drained, so a volume's tertiary subsystem was never created).
- ``FN_LVOL_SYNC_DEL`` — delete a replica bdev on a secondary node, holding the
  primary's del-sync lock until the task ends.
"""
from typing import Optional

from simplyblock_core import db_controller, storage_node_ops, utils
from simplyblock_core.controllers import snapshot_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
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


def get_primary_node(task) -> Optional[StorageNode]:
    if "primary_node" in task.function_params:
        return db.get_storage_node_by_id(task.function_params["primary_node"])

    nodes = db.get_primary_storage_nodes_by_secondary_node_id(task.node_id)
    if nodes:
        return nodes[0]
    return None


def _run_sync_op(task):
    lvol_id = task.function_params.get("lvol_id")
    op = task.function_params.get("op")

    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        raise TaskAbort("lvol no longer exists")

    if lvol.status == LVol.STATUS_IN_DELETION:
        raise TaskAbort("lvol is being deleted")
    if lvol.status != LVol.STATUS_ONLINE:
        raise TaskDefer(f"lvol status is {lvol.status}, retrying")

    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node no longer exists")

    if node.get_id() not in lvol.nodes:
        raise TaskAbort("node no longer hosts this lvol (topology moved)")
    if node.status != StorageNode.STATUS_ONLINE:
        raise TaskDefer(f"node is {node.status}, retrying")
    if storage_node_ops.get_restart_phase(task.node_id, lvol.lvs_name):
        # The owning flow (restart/activation/expansion) re-registers
        # lvols itself; re-check once it has released the LVS.
        raise TaskDefer("LVS owned by a restart/activation/expansion, retrying")

    if op == "register":
        ok, err = storage_node_ops.repair_lvol_registration_on_non_leader(
            lvol, node, task.function_params.get("secondary_index", 0))
        if not ok:
            raise TaskDefer(f"registration failed: {err}")
        task.function_result = f"registered lvol {lvol_id} on {task.node_id}"
    elif op == "resize":
        # Converge to the CURRENT DB size — resize_lvol persists the new
        # size after the fan-out, so this always applies the latest
        # target even if the lvol was resized again meanwhile.
        size_in_mib = utils.convert_size(lvol.size, 'MiB')
        if not node.rpc_client(timeout=10, retry=2).bdev_lvol_resize(
                f"{lvol.lvs_name}/{lvol.lvol_bdev}", size_in_mib):
            raise TaskDefer("resize RPC failed, retrying")
        task.function_result = f"resized lvol {lvol_id} on {task.node_id} to {size_in_mib} MiB"
    else:
        raise TaskAbort(f"unknown op {op!r}")


def _run_sync_del(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node not found")

    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
        raise TaskDefer(f"Node is {node.status}, retry task")

    lvol_bdev_name = task.function_params["lvol_bdev_name"]
    logger.info(f"Sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
    try:
        # Per-node lvstore lock: the sync delete mutates the replica blob tree
        # and must not interleave with a create/register of another object on
        # this node.
        with snapshot_controller.lvstore_op_lock(
                node.cluster_id,
                lvol_bdev_name.split("/")[0],
                node_id=node.get_id()):
            ret, err = node.rpc_client().delete_lvol(lvol_bdev_name, sync=True)
    except Exception as e:
        raise TaskRetry(f"Sync delete of {lvol_bdev_name} on {node.get_id()} failed: {e}; will retry")

    if not ret:
        if "code" not in err or err["code"] != -19:
            raise TaskRetry(f"Failed to sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
        logger.error(f"Sync delete completed with error: {err}")

    task.function_result = f"bdev {lvol_bdev_name} deleted"


def process_task(task):
    if task.function_name == JobSchedule.FN_LVOL_SYNC_OP:
        _run_sync_op(task)
    else:
        _run_sync_del(task)


def release_del_sync_lock(task):
    """Free the primary's del-sync lock once the delete task is over, however
    it ended — the lock is keyed on there being no active task left."""
    if task.function_name != JobSchedule.FN_LVOL_SYNC_DEL:
        return

    primary_node = get_primary_node(task)
    if primary_node:
        primary_node.lvol_del_sync_lock_reset()


SPEC = RunnerSpec(
    name="tasks-runner-sync-lvol-del",
    function_names=[JobSchedule.FN_LVOL_SYNC_OP, JobSchedule.FN_LVOL_SYNC_DEL],
    handler=process_task,
    on_finish=release_del_sync_lock,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
