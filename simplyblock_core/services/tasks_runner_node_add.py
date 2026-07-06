# coding=utf-8
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
            else:
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
