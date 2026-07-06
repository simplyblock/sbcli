# coding=utf-8
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor


from simplyblock_core import db_controller, storage_node_ops, utils, constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

# Node-add tasks for different nodes are processed concurrently. The slow part
# of add_node (SPDK boot, local device/alceml prep) is node-local with no
# cross-node shared state; the only part that must be serialized — wiring the
# node into the cluster mesh — is guarded per cluster by ClusterAddNodeLock
# inside storage_node_ops.add_node. We cap concurrency so a large
# cluster-create / expansion fan-out can't exhaust the runner host.
MAX_CONCURRENT_NODE_ADDS = 8

# uuids of node-add tasks a worker on THIS host is currently driving, so the
# dispatch loop never hands the same task to two workers. (Cross-host
# duplicate execution is separately prevented by the per-task lease in
# tasks_controller.claim_task.)
_inflight = set()
_inflight_lock = threading.Lock()


def process_task(task, cl):
    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return False

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if db.get_cluster_by_id(cl.get_id()).status == Cluster.STATUS_IN_ACTIVATION:
        task.function_result = "Cluster is in_activation, waiting"
        task.status = JobSchedule.STATUS_NEW
        task.write_to_db(db.kv_store)
        return False

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    try:
        res = storage_node_ops.add_node(**task.function_params)
        msg = f"Node add result: {res}"
        logger.info(msg)
        task.function_result = msg
        if res:
            task.status = JobSchedule.STATUS_DONE
        else:
            task.retry += 1
            task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return True
    except Exception as e:
        logger.error(e)
        return False


# Applying the CPU topology during add_node makes the node reboot
# (kubeletconfig / MCP update). The in-flight attempt then fails, but the
# right reaction is neither a quick blind retry (the node is down for
# 5-8 minutes; each attempt burns one of max_retry) nor exponential backoff
# (which keeps sleeping long after the node is back). Bound how long we are
# willing to wait for the node's agent to answer again — matches the
# topology job's own reboot budget (sleep 900).
NODE_REBOOT_WAIT_MAX_SEC = 900
NODE_REBOOT_POLL_SEC = 15


def _node_api_reachable(task, timeout=5):
    """TCP-level reachability of the node agent (host:port from the task's
    node_addr). During add the StorageNode record may not exist yet, so this
    intentionally checks the address, not the DB object."""
    addr = (task.function_params or {}).get("node_addr", "")
    if ":" not in addr:
        return True  # can't tell — let the normal retry path decide
    host, _, port = addr.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _wait_node_reachable(task, task_uuid):
    """After a failed attempt, if the node is unreachable (rebooting for the
    CPU-topology change), wait for it to answer again — up to
    NODE_REBOOT_WAIT_MAX_SEC — instead of consuming retries against a node
    that cannot possibly respond. Returns True if a wait took place."""
    if _node_api_reachable(task):
        return False
    logger.info(
        f"Node-add task {task_uuid}: node agent unreachable (rebooting for "
        f"CPU topology?); waiting up to {NODE_REBOOT_WAIT_MAX_SEC}s for it to return")
    deadline = time.time() + NODE_REBOOT_WAIT_MAX_SEC
    while time.time() < deadline:
        time.sleep(NODE_REBOOT_POLL_SEC)
        if _node_api_reachable(task):
            logger.info(f"Node-add task {task_uuid}: node agent reachable again; retrying add")
            return True
    logger.warning(
        f"Node-add task {task_uuid}: node agent still unreachable after "
        f"{NODE_REBOOT_WAIT_MAX_SEC}s; resuming normal retry schedule")
    return True


def _run_task(task_uuid, cluster_id):
    """Worker thread: drive one node-add task to completion (or suspension),
    then drop it from the in-flight set so a later cycle can retry it.

    Guarded against BaseException — add_node -> write_to_db calls exit(1) on a
    DB write failure, which surfaces here as SystemExit; it must be logged and
    contained to this worker, never allowed to kill the service loop or leave
    the task stuck in the in-flight set.
    """
    delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
    try:
        while True:
            # Re-fetch for fresh FDB state (the task may have been canceled).
            task = db.get_task_by_id(task_uuid)
            if task is None or task.status == JobSchedule.STATUS_DONE:
                break
            cl = db.get_cluster_by_id(cluster_id)
            # Lease gate: skip a task another live runner host owns.
            if not tasks_controller.claim_task(task):
                logger.info(f"Node-add task {task_uuid} owned by another runner host; skipping")
                break
            # add_node blocks for many minutes with no task writes; heartbeat
            # the lease so another runner host never sees it stale mid-add.
            with tasks_controller.task_lease_heartbeat(task):
                res = process_task(task, cl)
            if res:
                if task.status == JobSchedule.STATUS_DONE:
                    break
            # Reboot-aware wait: an attempt that failed because the node went
            # down (topology reboot) neither burns the backoff nor retries
            # blindly — wait for the agent to answer, then retry promptly on
            # a fresh schedule.
            if _wait_node_reachable(task, task_uuid):
                delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                continue
            if not res:
                # Cap the exponential backoff so a permanently failing node-add
                # can't grow the sleep without bound.
                delay_seconds = min(
                    delay_seconds * 2,
                    constants.RESTART_TASK_EXEC_INTERVAL_MAX_SEC,
                )
            time.sleep(delay_seconds)
    except BaseException as e:
        logger.error(f"Node-add task {task_uuid} processing crashed: {e}")
        logger.exception(e)
    finally:
        with _inflight_lock:
            _inflight.discard(task_uuid)


def main():
    logger.info("Starting Tasks runner node add...")

    executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_NODE_ADDS)

    while True:
        try:
            clusters = db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                # An unhandled FDBError here (1031 transaction timeout) killed
                # this runner at cluster start on 2026-07-16 — no auto-restart
                # ran for the rest of the run. Log and retry next tick instead.
                try:
                    tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                except Exception as e:
                    logger.error(f"Failed to read tasks for cluster {cl.get_id()}: {e}")
                    continue
                for task in tasks:
                    if task.function_name != JobSchedule.FN_NODE_ADD:
                        continue
                    if task.status == JobSchedule.STATUS_DONE:
                        continue
                    # Dispatch to a worker once; skip if a worker on this host is
                    # already driving it. Excess tasks queue in the executor and
                    # run as workers free up.
                    with _inflight_lock:
                        if task.uuid in _inflight:
                            continue
                        _inflight.add(task.uuid)
                    executor.submit(_run_task, task.uuid, cl.get_id())

        time.sleep(constants.TASK_EXEC_INTERVAL_SEC)


if __name__ == "__main__":
    main()
