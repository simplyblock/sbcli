import copy
import time

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskProgress,
    checkpoint,
    serve,
    set_result,
)

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

# function_params key: when this removal's current run of unfinished passes
# began. Bounds the wait below.
INCOMPLETE_SINCE_KEY = "incomplete_since"


def _cursor_persister(task):
    """Persist the removal cursor through the driver's checkpoint.

    The handler is given a frozen task, so the cursor cannot write its position
    into function_params itself. Unchanged positions are not re-written: the
    orchestrator replays every step on every pass, and a commit per replayed
    step would be a write per step per tick for nothing.
    """
    last: dict = {}

    def persist(**params):
        if params == last:
            return
        if checkpoint(task, **params) is None:
            # Canceled or finished underneath us. The driver sees that on its
            # next pass and finishes the task; nothing to record here.
            logger.info(f"Node-removal task {task.uuid}: not recording the step, "
                        f"the task was canceled or finished")
            return
        last.clear()
        last.update(copy.deepcopy(params))

    return persist


def _give_up(task, msg):
    """End the removal for good: REMOVED_FAILED, which an operator can see and
    re-drive. The node keeps whatever data could not be migrated off it, so it
    is not REMOVED."""
    logger.error(f"Node-removal task {task.uuid}: {msg}")
    storage_node_ops.set_node_status(
        task.node_id, StorageNode.STATUS_REMOVED_FAILED, caused_by="remove")
    raise TaskAbort(msg)


def process_task(task):
    """Advance one node-removal task by one orchestration pass.

    node_removal_orchestrate is idempotent and resumable: it returns True only
    when the node is fully REMOVED, and False to mean "incomplete, retry later"
    (most commonly: device failure-migration still in progress, which can take
    hours). Incomplete is progress, not failure — it consumes no retry, and the
    task stays RUNNING so the next tick picks it straight back up.

    It is still bounded. A removal that keeps coming back incomplete for
    NODE_REMOVAL_MAX_WAIT_SEC gives up as REMOVED_FAILED instead of retrying for
    ever: one task was once seen retrying 68 times with nothing surfacing why,
    and the next removal was refused with "Task found" while it lingered.
    """
    cluster = db.get_cluster_by_id(task.cluster_id)
    if cluster.status == Cluster.STATUS_IN_ACTIVATION:
        # Checked here rather than through is_eligible: the wait also has to
        # park the node in PENDING_REMOVAL, and eligibility must stay pure.
        # Forward only: a node past pending_removal keeps its place while the
        # activation runs, instead of being rewound to the start.
        storage_node_ops.advance_removal_status(
            task.node_id, StorageNode.STATUS_PENDING_REMOVAL, caused_by="remove", db_controller=db)
        raise TaskDefer("cluster is in_activation, waiting")

    force_remove = bool(task.function_params.get("force_remove", False))
    # The cursor reads its position out of the task and writes it back as the
    # orchestration advances, so a removal that is taking hours can be asked
    # which step it is on instead of only when it started.
    cursor = storage_node_ops.RemovalCursor(task, persist=_cursor_persister(task))
    try:
        done = storage_node_ops.node_removal_orchestrate(
            task.node_id, force_remove=force_remove, cursor=cursor)
    except storage_node_ops.RemovalGaveUp as e:
        # A step reported that retrying cannot help -- every drain target
        # exhausted, say. Terminal, and distinct from the wait ceiling, which
        # only catches waits that never end on their own.
        _give_up(task, f"removal failed at step '{cursor.step or 'unknown'}': {e}")

    if done:
        set_result(task, "Node removed")
        return

    since = task.function_params.get(INCOMPLETE_SINCE_KEY)
    if not since:
        checkpoint(task, **{INCOMPLETE_SINCE_KEY: time.time()})
    elif time.time() - float(since) >= constants.NODE_REMOVAL_MAX_WAIT_SEC:
        _give_up(task, f"removal gave up after "
                       f"~{constants.NODE_REMOVAL_MAX_WAIT_SEC // 3600}h "
                       f"at step '{cursor.step or 'unknown'}'")
    raise TaskProgress("removal in progress, retrying")


def _on_finish(task):
    """A removal task that ended without removing its node -- retry ceiling,
    cancel -- leaves the node REMOVED_FAILED, so it can be re-driven.

    Otherwise the node stays wherever the removal stopped (often with its SPDK
    already shut down) and nothing drives it on: the removal statuses only move
    forward, so no other path brings it back.
    """
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        return
    if node.status in StorageNode.REMOVAL_IN_PROGRESS_STATUSES:
        logger.error(f"Node-removal task {task.uuid} ended ({task.function_result}) "
                     f"with node {task.node_id} at {node.status}; marking it removed_failed")
        storage_node_ops.set_node_status(
            task.node_id, StorageNode.STATUS_REMOVED_FAILED, caused_by="remove")


SPEC = RunnerSpec(
    name="tasks-runner-node-removal",
    function_names=[JobSchedule.FN_NODE_REMOVAL],
    handler=process_task,
    interval=constants.TASK_EXEC_INTERVAL_SEC,
    on_finish=_on_finish,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
