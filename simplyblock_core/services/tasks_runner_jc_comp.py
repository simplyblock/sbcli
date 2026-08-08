# coding=utf-8
from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
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


def process_task(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        raise TaskAbort("node not found")

    if node.status != StorageNode.STATUS_ONLINE:
        raise TaskDefer(f"Node is {node.status}, retry task")

    if tasks_controller.get_active_node_tasks(task.cluster_id, task.node_id):
        raise TaskDefer("Task found on same node")

    if any(n.status != StorageNode.STATUS_ONLINE
           for n in db.get_storage_nodes_by_cluster_id(node.cluster_id)):
        raise TaskDefer("Not all nodes are online, can not resume JC compression")

    logger.info("no task found on same node, resuming compression")
    jm_vuid = task.function_params.get("jm_vuid", node.jm_vuid)
    ret, err = node.rpc_client(timeout=5, retry=2).jc_suspend_compression(
        jm_vuid=jm_vuid, suspend=False)

    if not ret:
        if err:
            raise TaskAbort(f"JC {node.jm_vuid} compression not needed")
        raise TaskRetry("JC comp resume failed, retry task")

    task.function_result = f"JC {node.jm_vuid} compression resumed on node"


SPEC = RunnerSpec(
    name="tasks-runner-jc-comp",
    function_names=[JobSchedule.FN_JC_COMP_RESUME],
    handler=process_task,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
    interval=60,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
