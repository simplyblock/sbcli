# coding=utf-8
from simplyblock_core import db_controller, storage_node_ops, utils, constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskProgress,
    serve,
)


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_task(task):
    """Advance one node-removal task by one orchestration pass.

    node_removal_orchestrate is idempotent and resumable: it returns True only
    when the node is fully REMOVED, and False to mean "incomplete, retry later"
    (most commonly: device failure-migration still in progress, which can take
    hours). Incomplete is progress, not failure — it consumes no retry, and the
    task stays RUNNING so the next tick picks it straight back up.
    """
    force_remove = bool(task.function_params.get("force_remove", False))
    if not storage_node_ops.node_removal_orchestrate(task.node_id, force_remove=force_remove):
        raise TaskProgress("removal in progress, retrying")

    task.function_result = "Node removed"


SPEC = RunnerSpec(
    name="tasks-runner-node-removal",
    function_names=[JobSchedule.FN_NODE_REMOVAL],
    handler=process_task,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
    interval=constants.TASK_EXEC_INTERVAL_SEC,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
