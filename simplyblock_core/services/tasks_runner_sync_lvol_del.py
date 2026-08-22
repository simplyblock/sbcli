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
import time
from typing import NoReturn, Optional

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import events_controller, snapshot_controller
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


def _is_already_gone(err):
    """True when an RPC error means "the thing is not there" (-19/ENODEV).

    For a delete that is success; for a register it is still a failure, but
    it is not an actionable one to alert on repeatedly -- the object the
    caller wanted registered has gone away underneath it.
    """
    if err is None:
        return False
    if isinstance(err, dict):
        return err.get("code") == -19
    return "-19" in str(err)


# Last threshold-alert message reported per task. The driver clears
# ``function_result`` before every attempt, so the previous attempt's message
# cannot be read off the task; without this a node that stays broken would
# write one identical event per poll. Unlike the SYNC_DELETE_FAILED event,
# which dedupes through the driver's on_failure, this alert must also fire
# once on crossing the threshold with an UNCHANGED message, so it keeps its
# own memo.
_last_alerted_failure: dict = {}


def _alert_repeated_failure(task, node_id, what, msg):
    """Escalate a failure that keeps repeating to the cluster event log.

    Both task families here are unbounded by design (``max_retry=-1``) --
    giving up leaks the volume -- so nothing else ever surfaces a leg that
    fails permanently. Fires once on crossing the threshold and again only
    when the error CHANGES.
    """
    # The driver consumes this attempt's retry only after the handler returns.
    attempts = (task.retry or 0) + 1
    threshold = constants.TASK_FAILURE_ALERT_THRESHOLD
    if attempts <= threshold:
        return
    previous = _last_alerted_failure.get(task.uuid)
    if attempts > threshold + 1 and previous == msg:
        return
    _last_alerted_failure[task.uuid] = msg

    try:
        events_controller.log_event_cluster(
            cluster_id=task.cluster_id,
            domain=events_controller.DOMAIN_STORAGE,
            event="LVOL_TASK_FAILING_REPEATEDLY",
            db_object=task,
            caused_by=events_controller.CAUSED_BY_MONITOR,
            message=(f"{what} has failed {attempts} times on node {node_id}; "
                     f"it will keep retrying, but this is not a transient "
                     f"condition and needs attention. Last error: {msg}"),
            node_id=node_id,
            event_level="Critical")
    except Exception as event_error:
        logger.warning(f"Could not log repeated-failure event: {event_error}")


def _run_sync_op(task):
    lvol_id = task.function_params.get("lvol_id")
    op = task.function_params.get("op")

    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        # A missing record is normally obsolescence (the lvol was deleted) and
        # completes the task. But add_lvol_ha queues these register tasks in
        # its pre-check, BEFORE it writes the lvol record at the end of the
        # create -- so this runner can see the task while the create is still
        # running and permanently DROP the registration, leaving the replica
        # missing and every later snapshot on that node failing (the LVOL_109
        # class). Inside the grace window, defer instead: once the record
        # appears, the status check below takes over.
        try:
            age = time.time() - float(task.date or 0)
        except (TypeError, ValueError):
            # Unknown age: fall back to treating the record as genuinely gone,
            # rather than deferring forever.
            age = float("inf")
        if age < constants.LVOL_SYNC_OP_RECORD_GRACE_SEC:
            raise TaskDefer("lvol record not written yet, retrying")
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
            msg = f"registration failed: {err}"
            if _is_already_gone(err):
                # -19 / ENODEV: nothing to act on, do not count it.
                raise TaskDefer(msg)
            _alert_repeated_failure(
                task, task.node_id, f"Deferred lvol {op} for {lvol_id}", msg)
            raise TaskRetry(msg)
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


def alert_sync_delete_failure(task, reason):
    """Report a failed sync delete in the cluster event log.

    This is the case an operator cannot see from the volume list alone: the
    async delete already SUCCEEDED, so the data is going away, but a node still
    holds its replica bdev and the volume is pinned in_deletion until this task
    drains.
    """
    if task.function_name != JobSchedule.FN_LVOL_SYNC_DEL:
        return

    events_controller.log_event_cluster(
        cluster_id=task.cluster_id,
        domain=events_controller.DOMAIN_STORAGE,
        event="SYNC_DELETE_FAILED",
        db_object=task,
        caused_by=events_controller.CAUSED_BY_MONITOR,
        message=(f"Sync delete of {task.function_params['lvol_bdev_name']} failed on "
                 f"node {task.node_id} after a successful async delete; the volume "
                 f"stays in_deletion until this drains. {reason}"),
        node_id=task.node_id,
        event_level="Error")


def _fail_sync_del(task, node, lvol_bdev_name, msg) -> NoReturn:
    _alert_repeated_failure(
        task, node.get_id(), f"Sync delete of {lvol_bdev_name}", msg)
    raise TaskRetry(msg)


def _run_sync_del(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node not found")

    # The node must be ONLINE, not merely "not in a transitional state". DOWN
    # was accepted here, so the runner fired the RPC at a node that cannot
    # answer; it failed and suspended, which works but is pure noise. register
    # (_run_sync_op) already requires ONLINE.
    if node.status != StorageNode.STATUS_ONLINE:
        raise TaskDefer(f"Node is {node.status}, retry task")

    lvol_bdev_name = task.function_params["lvol_bdev_name"]

    # Re-check the restart phase. This leg was queued BECAUSE a restart owned
    # the LVS state (check_non_leader_for_operation returns "queue" for
    # PRE_BLOCK/BLOCKED/POST_UNBLOCK); draining it without re-testing that
    # condition undoes the deferral the inline path just made. register already
    # guards on this.
    if storage_node_ops.get_restart_phase(
            node.get_id(), lvol_bdev_name.split("/")[0]):
        raise TaskDefer("LVS owned by a restart, retry task")

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
        _fail_sync_del(
            task, node, lvol_bdev_name,
            f"Sync delete of {lvol_bdev_name} on {node.get_id()} failed: {e}; will retry")

    if not ret:
        if "code" not in err or err["code"] != -19:
            # -19 never reaches here: it is handled below as success (the peer
            # is already clean), so it is never counted.
            _fail_sync_del(
                task, node, lvol_bdev_name,
                f"Failed to sync delete bdev: {lvol_bdev_name} from node: {node.get_id()}")
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
    _last_alerted_failure.pop(task.uuid, None)

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
    on_failure=alert_sync_delete_failure,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
    interval=3,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
