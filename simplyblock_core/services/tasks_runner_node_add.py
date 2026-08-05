# coding=utf-8
import socket
import time


from simplyblock_core import db_controller, storage_node_ops, utils, constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.services.task_runner_base import (
    RunnerSpec,
    TaskDefer,
    TaskRetry,
    serve,
)


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

# Node-add tasks for different nodes are processed concurrently. The slow part
# of add_node (SPDK boot, local device/alceml prep) is node-local with no
# cross-node shared state; the only part that must be serialized — wiring the
# node into the cluster mesh — is guarded per cluster by ClusterAddNodeLock
# inside storage_node_ops.add_node. We cap concurrency so a large
# cluster-create / expansion fan-out can't exhaust the runner host.
MAX_CONCURRENT_NODE_ADDS = constants.NODE_ADD_MAX_PARALLEL

# Applying the CPU topology during add_node makes the node reboot
# (kubeletconfig / MCP update). The in-flight attempt then fails, but the
# right reaction is neither a quick blind retry (the node is down for
# 5-8 minutes; each attempt burns one of max_retry) nor exponential backoff
# (which keeps sleeping long after the node is back). Bound how long we are
# willing to wait for the node's agent to answer again — matches the
# topology job's own reboot budget (sleep 900).
NODE_REBOOT_WAIT_MAX_SEC = 900
NODE_REBOOT_POLL_SEC = 15


def _node_addr(task):
    """The target host of an add. Two task records must never drive the same
    one concurrently: the concurrency model above ("different tasks target
    different nodes, no shared state") breaks if they do — two add_node() calls
    race the same host's config-slot classify-then-create logic milliseconds
    apart (2026-07-23: two threads, 4ms apart, produced 6 node records for a
    4-slot host). tasks_controller._validate_new_task_node_add should stop such
    a task from being created at all; this is the backstop for any that already
    exists, however it got there."""
    return (task.function_params or {}).get("node_addr")


def _node_api_reachable(task, timeout=5):
    """TCP-level reachability of the node agent (host:port from the task's
    node_addr). During add the StorageNode record may not exist yet, so this
    intentionally checks the address, not the DB object."""
    addr = _node_addr(task) or ""
    if ":" not in addr:
        return True  # can't tell — let the normal retry path decide
    host, _, port = addr.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _wait_node_reachable(task):
    """After a failed attempt, if the node is unreachable (rebooting for the
    CPU-topology change), wait for it to answer again — up to
    NODE_REBOOT_WAIT_MAX_SEC — instead of consuming retries against a node
    that cannot possibly respond. Returns True if a wait took place."""
    if _node_api_reachable(task):
        return False
    logger.info(
        f"Node-add task {task.uuid}: node agent unreachable (rebooting for "
        f"CPU topology?); waiting up to {NODE_REBOOT_WAIT_MAX_SEC}s for it to return")
    deadline = time.time() + NODE_REBOOT_WAIT_MAX_SEC
    while time.time() < deadline:
        time.sleep(NODE_REBOOT_POLL_SEC)
        if _node_api_reachable(task):
            logger.info(f"Node-add task {task.uuid}: node agent reachable again; retrying add")
            return True
    logger.warning(
        f"Node-add task {task.uuid}: node agent still unreachable after "
        f"{NODE_REBOOT_WAIT_MAX_SEC}s; resuming normal retry schedule")
    return True


def process_task(task):
    try:
        res = storage_node_ops.add_node(**task.function_params)
        msg = f"Node add result: {res}"
        logger.info(msg)
    except Exception as e:
        logger.error(e)
        res, msg = False, f"Node add raised: {e}"

    if res:
        task.function_result = msg
        return

    # The one guaranteed topology reboot per node must not eat the retry
    # budget: add_node catches the interrupted spdk_process_start and reports
    # failure, so wait for the agent to answer and re-attempt on a fresh
    # schedule instead. The re-run is idempotent — add_node cleans up its own
    # stale IN_CREATION record on re-entry.
    if _wait_node_reachable(task):
        raise TaskDefer(f"{msg} (node agent was rebooting)")

    raise TaskRetry(msg)


SPEC = RunnerSpec(
    name="tasks-runner-node-add",
    function_names=[JobSchedule.FN_NODE_ADD],
    handler=process_task,
    is_eligible=lambda task, cluster: cluster.status != Cluster.STATUS_IN_ACTIVATION,
    concurrency=MAX_CONCURRENT_NODE_ADDS,
    exclusion_key=_node_addr,
)


def main():
    serve(SPEC)


if __name__ == "__main__":
    main()
