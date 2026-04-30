# coding=utf-8
import time

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import device_controller, health_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClientException


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

utils.init_sentry_sdk()


def _get_node_unavailable_devices_count(node_id):
    node = db.get_storage_node_by_id(node_id)
    devices = []
    for dev in node.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            devices.append(dev)
    return len(devices)


def _get_device(task):
    node = db.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.get_id() == task.device_id:
            return dev


def _validate_no_task_node_restart(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _ensure_spdk_killed(node):
    """Best-effort kill of the SPDK process on the node before we mark it
    OFFLINE. Without this, flipping the status to OFFLINE while SPDK is still
    running produces a DB-vs-data-plane split: the DB says the node is not
    serving, but SPDK is actually still serving IO — and a subsequent
    restart_storage_node would spin up a second SPDK on top.

    Returns True if we are confident the data plane is not serving (SPDK
    killed successfully, or the node API is unreachable which implies the
    process is also unreachable). Returns False only when the node API is
    reachable but spdk_process_kill raised — in that narrow case we don't
    know for sure whether SPDK is gone, so the caller should leave the DB
    state as-is and let a later attempt retry.
    """
    if not health_controller._check_node_api(node):
        # Node API is down; the SPDK process on the same host is not reachable
        # to serve IO either. Safe to proceed.
        logger.info(
            f"Node {node.get_id()} API unreachable at {node.mgmt_ip}:5000; "
            f"assuming SPDK is not serving"
        )
        return True

    # Short-circuit when the SPDK container is already gone (common after a
    # `docker kill spdk_*`: by the time this task body runs, SNodeAPI reports
    # the container in `exited` state).  Skipping the kill RPC avoids a ~30 s
    # retry-then-timeout cycle on an already-dead container.
    try:
        client = node.client(timeout=5, retry=2)
        is_up, _ = client.spdk_process_is_up(node.rpc_port, node.cluster_id)
        if not is_up:
            logger.info(
                f"SPDK on {node.get_id()} already not running; skipping kill"
            )
            return True
    except Exception as exc:
        # If the probe itself fails, fall through and try the kill — it's
        # the conservative path (better to over-kill than leave SPDK serving).
        logger.warning(
            f"spdk_process_is_up probe failed on {node.get_id()}: {exc}; "
            f"proceeding with kill"
        )

    try:
        logger.info(f"Killing SPDK on node {node.get_id()} (rpc_port={node.rpc_port})")
        node.client(timeout=10, retry=5).spdk_process_kill(node.rpc_port, node.cluster_id)
        return True
    except SNodeClientException as exc:
        logger.error(
            f"Failed to kill SPDK on {node.get_id()}: {exc}; "
            f"leaving DB state unchanged to avoid split-brain"
        )
        return False
    except Exception as exc:
        # Other transport errors — treat as unreachable (process also unreachable).
        logger.warning(
            f"spdk_process_kill transport error on {node.get_id()}: {exc}; "
            f"assuming SPDK is not serving"
        )
        return True


def _reset_if_transient(node_id):
    """Roll the node back to STATUS_OFFLINE if a partial shutdown/restart
    left it stuck in an intermediate CP state. Without this, a failed
    attempt leaves the node pinned in STATUS_IN_SHUTDOWN or STATUS_RESTARTING,
    which (a) blocks future restart attempts via the mutual-exclusion guard,
    and (b) causes peers' cluster_map health checks to fail cluster-wide.

    Before flipping to OFFLINE we confirm the SPDK process is not running
    on the node's host — otherwise we'd risk a split-brain where the DB
    says OFFLINE but SPDK is still serving IO.
    """
    try:
        node = db.get_storage_node_by_id(node_id)
    except KeyError:
        return
    if node.status not in (StorageNode.STATUS_IN_SHUTDOWN, StorageNode.STATUS_RESTARTING):
        return
    logger.warning(
        f"Node {node_id} left in {node.status} after failed restart attempt; "
        f"verifying SPDK is not serving before resetting to OFFLINE"
    )
    if not _ensure_spdk_killed(node):
        logger.error(
            f"Could not confirm SPDK is down on {node_id}; refusing to flip to "
            f"OFFLINE to avoid split-brain. Next retry will attempt again."
        )
        return
    try:
        storage_node_ops.set_node_status(node_id, StorageNode.STATUS_OFFLINE)
        logger.info(f"Node {node_id} reset to OFFLINE (SPDK confirmed down)")
    except Exception as exc:
        logger.error(f"Failed to reset node {node_id} to OFFLINE: {exc}")


def task_runner(task):
    if task.function_name == JobSchedule.FN_DEV_RESTART:
        return task_runner_device(task)
    if task.function_name == JobSchedule.FN_NODE_RESTART:
        return task_runner_node(task)


def task_runner_device(task):
    device = _get_device(task)

    if task.retry >= constants.TASK_EXEC_RETRY_COUNT:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    if not _validate_no_task_node_restart(task.cluster_id, task.node_id):
        task.function_result = "canceled: node restart found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_unavailable(device.get_id())
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        device_controller.device_set_retries_exhausted(device.get_id(), True)
        return True

    node = db.get_storage_node_by_id(task.node_id)
    if node.status != StorageNode.STATUS_ONLINE:
        logger.error(f"Node is not online: {node.get_id()}, retry")
        task.function_result = "Node is offline"
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False

    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "Device is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Device is not unavailable: {device.get_id()}, {device.status} , stopping task")
        task.function_result = f"stopped because dev is {device.status}"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    # set device online for the first 3 retries
    if task.retry < 3:
        logger.info(f"Set device online {device.get_id()}")
        device_controller.device_set_io_error(device.get_id(), False)
        device_controller.device_set_state(device.get_id(), NVMeDevice.STATUS_ONLINE)
    else:
        logger.info(f"Restarting device {device.get_id()}")
        device_controller.restart_device(device.get_id(), force=True)

    # check device status
    time.sleep(5)
    device = _get_device(task)
    if device.status == NVMeDevice.STATUS_ONLINE and device.io_error is False:
        logger.info(f"Device is online: {device.get_id()}")
        task.function_result = "done"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        tasks_controller.add_device_mig_task_for_node(task.node_id)

        return True

    task.retry += 1
    task.write_to_db(db.kv_store)
    return False


def task_runner_node(task):
    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.retry >= task.max_retry:
        task.function_result = "max retry reached"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        storage_node_ops.set_node_status(task.node_id, StorageNode.STATUS_OFFLINE)
        return True

    if node.status in [StorageNode.STATUS_REMOVED, StorageNode.STATUS_SCHEDULABLE]:
        logger.info(f"Node is {node.status}, stopping task")
        task.function_result = f"Node is {node.status}, stopping"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True
    # DOWN used to short-circuit here too. After removing the monitor's
    # set_node_online (which previously did DOWN -> ONLINE on health-check
    # pass), DOWN must be handled by this runner: shutdown + restart drives
    # the node through IN_RESTART -> ONLINE, which is the only legal path.

    # The node-restart task is meant to fix the NODE, not individual devices.
    # Previously this short-circuit also required `unavailable_devices_count
    # == 0`, which meant a node that was ONLINE but still had any residual
    # UNAVAILABLE device (a routine transient right after an outage — peer
    # nodes call device_set_unavailable on the target's remote-device records
    # and clearing those is decoupled from the target node's own restart
    # completion) would be treated as "still broken", and the runner would
    # slam through another shutdown + restart cycle even though the node was
    # serving IO just fine. That produced visible online → in_shutdown →
    # offline → in_restart cycles.
    #
    # Device-level recovery has its own task type (add_device_to_auto_restart
    # / FN_DEV_RESTART); this one only needs the NODE to be healthy.
    if node.status == StorageNode.STATUS_ONLINE and getattr(node, "health_check", True):
        logger.info(f"Node is online and healthy: {node.get_id()}")
        task.function_result = "Node is online"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if task.status != JobSchedule.STATUS_RUNNING:
        if node.status == StorageNode.STATUS_RESTARTING:
            logger.info("Node is restarting, stopping task")
            task.function_result = "Node is restarting"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return True
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    # is node reachable?
    ping_check = health_controller._check_node_ping(node.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
    node_api_check = health_controller._check_node_api(node)
    logger.info(f"Check: node API {node.mgmt_ip}:5000 ... {node_api_check}")
    node_data_nic_ping_check = False
    for data_nic in node.data_nics:
        if data_nic.ip4_address:
            data_ping_check = health_controller._check_ping_from_node(data_nic.ip4_address, ifname=data_nic.if_name, node=node)
            logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping_check}")
            node_data_nic_ping_check |= data_ping_check
    if not ping_check or not node_api_check or not node_data_nic_ping_check:
        # node is unreachable, retry
        logger.info(f"Node is not reachable: {task.node_id}, retry")
        task.function_result = "Node is unreachable, retry"
        task.retry += 1
        task.write_to_db(db.kv_store)
        return False


    shutdown_succeeded = False
    try:
        try:
            # shutting down node
            logger.info(f"Shutdown node {node.get_id()}")
            ret = storage_node_ops.shutdown_storage_node(node.get_id(), force=True)
            if ret:
                logger.info("Node shutdown succeeded")
                shutdown_succeeded = True
            else:
                logger.error("Node shutdown returned False; will retry after reset")
            time.sleep(3)
        except Exception as e:
            logger.error(e)
            return False

        # Skip the restart step if shutdown did not succeed — restarting on top
        # of a half-shutdown node produced the in_restart hang we're guarding
        # against. Let the outer retry reattempt the whole cycle.
        if not shutdown_succeeded:
            task.retry += 1
            task.write_to_db(db.kv_store)
            return False

        try:
            # resetting node
            logger.info(f"Restart node {node.get_id()}")
            ret = storage_node_ops.restart_storage_node(node.get_id(), force=True)
            if ret:
                logger.info("Node restart succeeded")
        except Exception as e:
            logger.error(e)
            return False

        time.sleep(3)
        node = db.get_storage_node_by_id(task.node_id)
        # Same relaxation as the task-entry short-circuit above: success of
        # THIS task is "node is online and healthy". Residual per-device
        # UNAVAILABLE flags are the responsibility of FN_DEV_RESTART, not
        # this runner — accepting them here prevents an endless shutdown/
        # restart cycle when a device stays UNAVAILABLE for reasons outside
        # this node's control (peer-side records, timing, etc.).
        if node.status == StorageNode.STATUS_ONLINE and getattr(node, "health_check", True):
            logger.info(f"Node is online and healthy: {node.get_id()}")
            task.function_result = "done"
            task.status = JobSchedule.STATUS_DONE
            task.write_to_db(db.kv_store)
            return True

        task.retry += 1
        task.write_to_db(db.kv_store)
        return False
    finally:
        # On any non-success exit from the shutdown/restart sequence, make sure
        # we don't leave the node pinned in STATUS_IN_SHUTDOWN or
        # STATUS_RESTARTING — both are terminal traps if the task doesn't
        # reach STATUS_ONLINE.
        try:
            post_node = db.get_storage_node_by_id(task.node_id)
            if post_node.status != StorageNode.STATUS_ONLINE:
                _reset_if_transient(task.node_id)
        except KeyError:
            pass
        except Exception as exc:
            logger.error(f"Post-task status reset check failed: {exc}")


logger.info("Starting Tasks runner...")
while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                if task.function_name in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_NODE_RESTART]:
                    # Restart tasks start at a short cadence and cap the
                    # exponential backoff at RESTART_TASK_EXEC_INTERVAL_MAX_SEC
                    # so recovery time is bounded even after several retries.
                    delay_seconds = constants.RESTART_TASK_EXEC_INTERVAL_SEC
                    while task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            if task.status == JobSchedule.STATUS_DONE:
                                break
                        else:
                            delay_seconds = min(
                                delay_seconds * 2,
                                constants.RESTART_TASK_EXEC_INTERVAL_MAX_SEC,
                            )
                        time.sleep(delay_seconds)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
