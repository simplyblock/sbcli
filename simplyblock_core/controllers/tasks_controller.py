# coding=utf-8
import datetime
import json
import logging
import time
import uuid

from simplyblock_core import db_controller, constants, utils
from simplyblock_core.controllers import tasks_events, device_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()
db = db_controller.DBController()


def _validate_new_task_dev_restart(cluster_id, node_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_RESTART and task.device_id == device_id and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
        elif task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                logger.info(f"Task found, skip adding new task: {task.get_id()}")
                return False
    return True


def _validate_new_task_node_restart(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.get_id()
    return False


def _add_task(function_name, cluster_id, node_id, device_id,
              max_retry=constants.TASK_EXEC_RETRY_COUNT, function_params=None, send_to_cluster_log=True):

    if function_name in [JobSchedule.FN_DEV_RESTART, JobSchedule.FN_FAILED_DEV_MIG]:
        if not _validate_new_task_dev_restart(cluster_id, node_id, device_id):
            return False

    if function_name == JobSchedule.FN_NODE_RESTART:
        task_id = _validate_new_task_node_restart(cluster_id, node_id)
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_NEW_DEV_MIG:
        task_id = get_new_device_mig_task(cluster_id, node_id, function_params['distr_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_DEV_MIG:
        task_id = get_device_mig_task(cluster_id, node_id, device_id, function_params['distr_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_JC_COMP_RESUME:
        task_id = get_jc_comp_task(cluster_id, node_id, function_params['jm_vuid'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_PORT_ALLOW:
        task_id = get_port_allow_tasks(cluster_id, node_id, function_params['port_number'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_LVOL_SYNC_DEL:
        task_id = get_lvol_sync_del_task(cluster_id, node_id, function_params['lvol_bdev_name'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False
    elif function_name == JobSchedule.FN_LVOL_MIG:
        task_id = get_active_lvol_mig_task(cluster_id, function_params.get("lvol_id"))
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    elif function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
        task_id = get_snapshot_replication_task(
            cluster_id, function_params['snapshot_id'], function_params['replicate_to_source'])
        if task_id:
            logger.info(f"Task found, skip adding new task: {task_id}")
            return False

    task_obj = JobSchedule()
    task_obj.uuid = str(uuid.uuid4())
    task_obj.cluster_id = cluster_id
    task_obj.node_id = node_id
    task_obj.device_id = device_id
    task_obj.date = int(time.time())
    task_obj.function_name = function_name
    if function_params and type(function_params) is dict:
        task_obj.function_params = function_params
    task_obj.max_retry = max_retry
    task_obj.status = JobSchedule.STATUS_NEW
    task_obj.write_to_db(db.kv_store)
    if send_to_cluster_log:
        tasks_events.task_create(task_obj)
    return task_obj.uuid


def add_device_mig_task_for_node(node_id):
    sub_tasks = []
    node = db.get_storage_node_by_id(node_id)
    cluster_id = node.cluster_id
    master_task = None
    for task in  db.get_job_tasks(cluster_id):
        if task.function_name == JobSchedule.FN_BALANCING_AFTER_NODE_RESTART :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                logger.info("Master task found, skip adding new master task")
                master_task = task
                break

    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue

        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                task_id = _add_task(JobSchedule.FN_DEV_MIG, cluster_id, node.get_id(), bdev['name'],
                          max_retry=-1, function_params={'distr_name': bdev['name']}, send_to_cluster_log=False)
                if task_id:
                    sub_tasks.append(task_id)
    if sub_tasks:
        if master_task:
            master_task.sub_tasks.extend(sub_tasks)
            master_task.write_to_db()
        else:
            task_obj = JobSchedule()
            task_obj.uuid = str(uuid.uuid4())
            task_obj.cluster_id = cluster_id
            task_obj.date = int(time.time())
            task_obj.function_name = JobSchedule.FN_BALANCING_AFTER_NODE_RESTART
            task_obj.sub_tasks = sub_tasks
            task_obj.status = JobSchedule.STATUS_NEW
            task_obj.write_to_db(db.kv_store)
            tasks_events.task_create(task_obj)
        return True


def add_device_to_auto_restart(device):
    return _add_task(JobSchedule.FN_DEV_RESTART, device.cluster_id, device.node_id, device.get_id())


def add_node_to_auto_restart(node):
    # Auto-restart kills SPDK and runs the full recreate path. The
    # legitimate triggers are:
    #   - OFFLINE: SPDK is gone (set_node_offline pairs the OFFLINE
    #     flip with this call).
    #   - UNREACHABLE: peer JM connections and remote-device records
    #     on other nodes have been torn down on their side; passively
    #     clearing the flag would leave the data-plane view
    #     inconsistent. The full restart impl is the path that
    #     re-establishes peer relationships and commits ONLINE.
    #   - SCHEDULABLE: SPDK RPC was unresponsive; restart drives a
    #     coordinated recreate.
    #
    # DOWN intentionally does NOT trigger auto-restart: SPDK is still
    # up and cluster-internal traffic works; only the client-facing
    # port is blocked. Recovery is port-unblock, not a destructive
    # kill-and-replay.
    _AUTO_RESTART_OK = (
        StorageNode.STATUS_OFFLINE,
        StorageNode.STATUS_UNREACHABLE,
        StorageNode.STATUS_SCHEDULABLE,
    )
    if node.status not in _AUTO_RESTART_OK:
        logger.warning(
            "Refusing to queue auto-restart for node %s in status %s "
            "(only OFFLINE / UNREACHABLE / SCHEDULABLE are valid triggers)",
            node.get_id(), node.status,
        )
        return False

    cluster = db.get_cluster_by_id(node.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
                              Cluster.STATUS_READONLY, Cluster.STATUS_UNREADY, Cluster.STATUS_SUSPENDED]:
        logger.warning(f"Cluster is not active, skip node auto restart, status: {cluster.status}")
        return False
    offline_nodes = 0
    for sn in db.get_storage_nodes_by_cluster_id(node.cluster_id):
        if node.get_id() != sn.get_id() and sn.status != StorageNode.STATUS_ONLINE and node.mgmt_ip != sn.mgmt_ip:
            offline_nodes += 1
    if offline_nodes > cluster.distr_npcs and cluster.status != Cluster.STATUS_SUSPENDED:
        logger.info("Node found that is not online, skip node auto restart")
        return False
    return _add_task(JobSchedule.FN_NODE_RESTART, node.cluster_id, node.get_id(), "", max_retry=11)


def cancel_pending_node_restart_tasks(cluster_id, node_id):
    # Called from set_node_status the moment a node transitions to ONLINE.
    # Without this, an obsolete FN_NODE_RESTART row left over from the
    # outage stays in `new`/`running` and blocks every subsequent restart
    # via the dedup guard in `_validate_new_task_node_restart` until the
    # task runner happens to pick it up — observed as a 5-minute window
    # of failing manual restarts after the node was already back online.
    canceled = 0
    for task in db.get_job_tasks(cluster_id):
        if (task.function_name == JobSchedule.FN_NODE_RESTART
                and task.node_id == node_id
                and task.status != JobSchedule.STATUS_DONE
                and not task.canceled):
            task.canceled = True
            task.status = JobSchedule.STATUS_DONE
            task.function_result = "canceled: node back online"
            task.write_to_db(db.kv_store)
            canceled += 1
            logger.info(
                f"Canceled obsolete node_restart task {task.get_id()} (node {node_id} back online)")
    return canceled


def list_tasks(cluster_id, is_json=False, limit=50, **kwargs):
    try:
        db.get_cluster_by_id(cluster_id)
    except KeyError:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    data = []
    tasks = db.get_job_tasks(cluster_id, reverse=True)
    tasks.reverse()
    if is_json is True:
        for t in tasks:
            if t.function_name == JobSchedule.FN_DEV_MIG:
                continue
            data.append(t.get_clean_dict())
            if len(data)+1 > limit > 0:
                return json.dumps(data, indent=2)
        return json.dumps(data, indent=2)

    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_MIG:
            continue
        logger.debug(task)
        if task.max_retry > 0:
            retry = f"{task.retry}/{task.max_retry}"
        else:
            retry = f"{task.retry}"
        logger.debug(task)
        upd = task.updated_at
        if upd:
            try:
                parsed = datetime.datetime.fromisoformat(upd)
                upd = parsed.strftime("%H:%M:%S, %d/%m/%Y")
            except Exception as e:
                logger.error(e)

        if task.sub_tasks:
            target_id = f"Master task for {len(task.sub_tasks)} subtasks"

        else:
            target_id = f"NodeID:{task.node_id}"
            if task.device_id:
                target_id += f"\nDeviceID:{task.device_id}"

        data.append({
            "Task ID": task.uuid,
            "Target ID": target_id,
            "Function": task.function_name,
            "Retry": retry,
            "Status": task.status,
            "Result": task.function_result,
            "Updated At": upd or "",
        })
        if len(data)+1 > limit > 0:
            return utils.print_table(data)
    return utils.print_table(data)


def cancel_task(task_id):
    try:
        task = db.get_task_by_id(task_id)
    except KeyError as e:
        logger.error(e)
        return False

    if task.sub_tasks:
        logger.error("Can not cancel master task")
        return False

    if task.device_id:
        device_controller.device_set_retries_exhausted(task.device_id, True)

    task.canceled = True
    task.write_to_db(db.kv_store)
    tasks_events.task_canceled(task)
    return True


def get_subtasks(master_task_id):
    master_task = db.get_task_by_id(master_task_id)
    data = []
    tasks = {t.uuid: t for t in db.get_job_tasks(master_task.cluster_id)}
    for sub_task_id in master_task.sub_tasks:
        sub_task = tasks[sub_task_id]
        if sub_task.max_retry > 0:
            retry = f"{sub_task.retry}/{sub_task.max_retry}"
        else:
            retry = f"{sub_task.retry}"

        upd = sub_task.updated_at
        if upd:
            try:
                parsed = datetime.datetime.fromisoformat(upd)
                upd = parsed.strftime("%H:%M:%S, %d/%m/%Y")
            except Exception as e:
                logger.error(e)

        logger.debug(sub_task)
        data.append({
            "Task ID": sub_task.uuid,
            "Node ID": sub_task.node_id,
            "Distrib": sub_task.device_id,
            "Function": sub_task.function_name,
            "Retry": retry,
            "Status": sub_task.status,
            "Result": sub_task.function_result,
            "Updated At": upd or "",
        })
    return utils.print_table(data)


def get_active_node_restart_task(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NODE_RESTART and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def get_active_dev_restart_task(cluster_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_RESTART and task.device_id == device_id:
            if task.status == JobSchedule.STATUS_RUNNING and task.canceled is False:
                return task.uuid
    return False


def get_active_node_mig_task(cluster_id, node_id, distr_name=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name in [JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_DEV_MIG,
                                  JobSchedule.FN_NEW_DEV_MIG] and task.node_id == node_id:
            if task.status == JobSchedule.STATUS_RUNNING and task.canceled is False:
                if distr_name:
                    if "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                        return task.uuid
                else:
                    return task.uuid
    return False


def add_device_failed_mig_task(device_id):
    device = db.get_storage_device_by_id(device_id)
    for node in db.get_storage_nodes_by_cluster_id(device.cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                _add_task(JobSchedule.FN_FAILED_DEV_MIG, device.cluster_id, node.get_id(), device.get_id(),
                          max_retry=-1, function_params={'distr_name': bdev['name']})
    return True


def add_new_device_mig_task(device_id):
    device = db.get_storage_device_by_id(device_id)
    for node in db.get_storage_nodes_by_cluster_id(device.cluster_id):
        if node.status == StorageNode.STATUS_REMOVED:
            continue
        for bdev in node.lvstore_stack:
            if bdev['type'] == "bdev_distr":
                _add_task(JobSchedule.FN_NEW_DEV_MIG, device.cluster_id, node.get_id(), device.get_id(),
                          max_retry=-1, function_params={'distr_name': bdev['name']})
    return True


def add_node_add_task(cluster_id, function_params):
    return _add_task(JobSchedule.FN_NODE_ADD, cluster_id, "", "",
                     function_params=function_params, max_retry=11)


def get_active_node_tasks(cluster_id, node_id):
    tasks = db.get_job_tasks(cluster_id)
    out = []
    for task in tasks:
        if task.function_name in [JobSchedule.FN_PORT_ALLOW, JobSchedule.FN_JC_COMP_RESUME]:
            continue
        if task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                out.append(task)
    return out


def get_new_device_mig_task(cluster_id, node_id, distr_name, dev_id=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NEW_DEV_MIG and task.node_id == node_id:
            if dev_id:
                if task.device_id != dev_id:
                    continue
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False \
                    and "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                return task.uuid
    return False


def get_device_mig_task(cluster_id, node_id, device_id, distr_name):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_DEV_MIG and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False \
                    and "distr_name" in task.function_params and task.function_params["distr_name"] == distr_name:
                return task.uuid
    return False


def get_new_device_mig_task_for_device(cluster_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_NEW_DEV_MIG:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def get_failed_device_mig_task(cluster_id, device_id):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_FAILED_DEV_MIG and task.device_id == device_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def add_port_allow_task(cluster_id, node_id, port_number):
    return _add_task(JobSchedule.FN_PORT_ALLOW, cluster_id, node_id, "", function_params={"port_number": port_number})


def get_port_allow_tasks(cluster_id, node_id, port_number):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_PORT_ALLOW and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if "port_number" in task.function_params and task.function_params["port_number"] == port_number:
                    return task.uuid
    return False


def add_jc_comp_resume_task(cluster_id, node_id, jm_vuid):
    return _add_task(JobSchedule.FN_JC_COMP_RESUME, cluster_id, node_id, "",
                     function_params={"jm_vuid": jm_vuid}, max_retry=10)


def get_jc_comp_task(cluster_id, node_id, jm_vuid=0):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_JC_COMP_RESUME and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if jm_vuid and "jm_vuid" in task.function_params and task.function_params["jm_vuid"] == jm_vuid:
                    return task.uuid
    return False


def get_active_lvol_mig_task(cluster_id, lvol_id):
    """Return the UUID of an active (non-done, non-cancelled) lvol migration task."""
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_MIG and task.canceled is False:
            if task.status != JobSchedule.STATUS_DONE:
                if task.function_params.get("lvol_id") == lvol_id:
                    return task.uuid
    return False


def get_active_lvol_mig_task_on_node(cluster_id, node_id):
    """Return the UUID of an active lvol migration task on a given source node."""
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_MIG and task.node_id == node_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                return task.uuid
    return False


def add_lvol_mig_task(migration):
    """Create the JobSchedule task that drives a live volume migration."""
    return _add_task(
        JobSchedule.FN_LVOL_MIG,
        migration.cluster_id,
        migration.source_node_id,
        "",
        max_retry=migration.max_retries,
        function_params={
            "migration_id": migration.uuid,
            "lvol_id": migration.lvol_id,
            "target_node_id": migration.target_node_id,
        },
    )


def add_lvol_sync_del_task(cluster_id, node_id, lvol_bdev_name, primary_node):
    return _add_task(JobSchedule.FN_LVOL_SYNC_DEL, cluster_id, node_id, "",
                     function_params={"lvol_bdev_name": lvol_bdev_name, "primary_node": primary_node}, max_retry=10)

def get_lvol_sync_del_task(cluster_id, node_id, lvol_bdev_name=None):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_LVOL_SYNC_DEL and task.node_id == node_id :
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if lvol_bdev_name:
                    if "lvol_bdev_name" in task.function_params and task.function_params["lvol_bdev_name"] == lvol_bdev_name:
                        return task.uuid
                else:
                    return task.uuid
    return False

def get_snapshot_replication_task(cluster_id, snapshot_id, replicate_to_source):
    tasks = db.get_job_tasks(cluster_id)
    for task in tasks:
        if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and task.function_params["snapshot_id"] == snapshot_id:
            if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                if task.function_params["replicate_to_source"] == replicate_to_source:
                    return task.uuid
    return False


def add_backup_task(backup):
    """Create the task that performs an S3 backup."""
    return _add_task(
        JobSchedule.FN_BACKUP,
        backup.cluster_id,
        backup.node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "backup_id": backup.uuid,
        },
    )


def add_backup_restore_task(cluster_id, node_id, backup_id, lvol_name, chain_ids, lvol_id=""):
    """Create the task that restores an S3 backup chain into a new lvol."""
    return _add_task(
        JobSchedule.FN_BACKUP_RESTORE,
        cluster_id,
        node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "backup_id": backup_id,
            "lvol_name": lvol_name,
            "lvol_id": lvol_id,
            "chain_ids": chain_ids,
        },
    )


def add_backup_merge_task(cluster_id, node_id, keep_backup_id, old_backup_id):
    """Create the task that merges two S3 backups."""
    return _add_task(
        JobSchedule.FN_BACKUP_MERGE,
        cluster_id,
        node_id,
        "",
        max_retry=constants.BACKUP_MAX_RETRIES,
        function_params={
            "keep_backup_id": keep_backup_id,
            "old_backup_id": old_backup_id,
        },
    )


def _check_snap_instance_on_node(snapshot_id: str , node_id: str):
    snapshot = db.get_snapshot_by_id(snapshot_id)
    for sn_inst in snapshot.instances:
        if sn_inst["lvol"]["node_id"] == node_id:
            logger.info("Snapshot instance found on node, skip adding replication task")
            return

    if snapshot.snap_ref_id:
        prev_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
        _check_snap_instance_on_node(prev_snap.get_id(), node_id)

    _add_task(JobSchedule.FN_SNAPSHOT_REPLICATION, snapshot.cluster_id, node_id, "",
              function_params={"snapshot_id": snapshot.get_id(), "replicate_to_source": False,
                               "replicate_as_snap_instance": True},
              send_to_cluster_log=False)


def add_snapshot_replication_task(cluster_id, node_id, snapshot_id, replicate_to_source=False):
    if not replicate_to_source:
        snapshot = db.get_snapshot_by_id(snapshot_id)
        if snapshot.snap_ref_id:
            prev_snap = db.get_snapshot_by_id(snapshot.snap_ref_id)
            _check_snap_instance_on_node(prev_snap.get_id(), node_id)

    return _add_task(JobSchedule.FN_SNAPSHOT_REPLICATION, cluster_id, node_id, "",
                     function_params={"snapshot_id": snapshot_id, "replicate_to_source": replicate_to_source},
                     send_to_cluster_log=False)
