from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import activation_gate, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import jc_compression_upgrade
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskDefer,
    TaskRetry,
    serve,
    set_result,
)

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def _site_activation_holds(cl, task) -> str:
    """activation_gate.site_activation_blocks for the LVS whose JM group the
    task resumes - ``function_params["jm_vuid"]``, else the JM of its node, as
    the resume reads it. That LVS is owned by the node of the cluster whose
    lvstore carries the jm_vuid; the task's node is only where the RPC goes. An
    owner that is not found is held like a node-level operation."""
    if not activation_gate.site_activation_blocks(cl):
        return ""
    if "jm_vuid" in task.function_params:
        jm_vuid = task.function_params["jm_vuid"]
    else:
        try:
            jm_vuid = db.get_storage_node_by_id(task.node_id).jm_vuid
        except KeyError:
            return activation_gate.site_activation_blocks(cl)
    for node in db.get_storage_nodes_by_cluster_id(cl.get_id()):
        if node.lvstore and node.jm_vuid == jm_vuid:
            return activation_gate.site_activation_blocks(cl, node)
    return activation_gate.site_activation_blocks(cl)


def _is_eligible(task, cluster):
    """Left untouched while the cluster is in_activation, or while a site
    activation holds the LVS the task resumes."""
    return (cluster.status != Cluster.STATUS_IN_ACTIVATION
            and not _site_activation_holds(cluster, task))


def process_task(task):
    # Release-upgrade guard (remove with the jc_compression_upgrade plugin):
    # resumes are held until `cluster upgrade-complete`.
    if jc_compression_upgrade.resume_is_held(db.get_cluster_by_id(task.cluster_id)):
        raise TaskDefer("JC compression resume held: cluster upgrade in progress")

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

    set_result(task, f"JC {node.jm_vuid} compression resumed on node")


SPEC = RunnerSpec(
    name="tasks-runner-jc-comp",
    function_names=[JobSchedule.FN_JC_COMP_RESUME],
    handler=process_task,
    is_eligible=_is_eligible,
    interval=60,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
