# coding=utf-8
from simplyblock_core import db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.controllers.cluster_expansion.executor import (
    integrate_new_node_into_cluster,
)
from simplyblock_core.controllers.cluster_expansion.planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    expand_state_rearm,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskAbort,
    TaskRetry,
    serve,
)


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


def process_task(task):
    new_node_id = task.function_params.get("new_node_id")
    if not new_node_id:
        raise TaskAbort("missing new_node_id in function_params")

    cluster = db.get_cluster_by_id(task.cluster_id)
    new_snode = db.get_storage_node_by_id(new_node_id)

    # Retry-by-resume: a prior attempt that aborted left expand_state at
    # the failed move's cursor. Flip it back to in_progress so the
    # orchestrator re-attempts that move instead of recomputing a fresh
    # diff against a topology the aborted run may have partially mutated.
    if (cluster.expand_state or {}).get("phase") == EXPAND_PHASE_ABORTED:
        cluster.expand_state = expand_state_rearm(cluster.expand_state)
        cluster.write_to_db()

    integrate_new_node_into_cluster(
        cluster, new_snode, db_controller=db,
        manage_cluster_status=True)

    # integrate_new_node_into_cluster returns only on success; the
    # orchestrator marks expand_state completed. Re-read to confirm.
    cluster = db.get_cluster_by_id(task.cluster_id)
    phase = (cluster.expand_state or {}).get("phase")
    if phase != EXPAND_PHASE_COMPLETED:
        raise TaskRetry(f"unexpected phase after run: {phase!r}")

    # Queue new-device migration now that the rotation has landed and
    # the cluster is back to ACTIVE — tasks are created against the
    # post-rotation lvstore_stack (which includes the newcomer's
    # primary distr). Mirrors the trigger the non-expansion add path
    # runs inside add_node.
    new_snode = db.get_storage_node_by_id(new_node_id)
    for dev in new_snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_ONLINE:
            tasks_controller.add_new_device_mig_task(dev.get_id())

    task.function_result = f"expansion complete: {new_node_id}"


SPEC = RunnerSpec(
    name="tasks-runner-cluster-expand",
    function_names=[JobSchedule.FN_CLUSTER_EXPAND],
    handler=process_task,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
