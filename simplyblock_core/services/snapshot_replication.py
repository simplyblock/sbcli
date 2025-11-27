# coding=utf-8
import time
import uuid

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import lvol_controller, snapshot_events
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)
# get DB controller
db = db_controller.DBController()


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    if "remote_lvol_id" not in task.function_params or not task.function_params["remote_lvol_id"] :
        remote_node_uuid = db.get_storage_node_by_id(snapshot.lvol.replication_node_id)
        cluster = db.get_cluster_by_id(remote_node_uuid.cluster_id)
        remote_pool_uuid = None
        if cluster.snapshot_replication_target_pool:
            remote_pool_uuid = cluster.snapshot_replication_target_pool
        else:
            for bool in db.get_pools(remote_node_uuid.cluster_id):
                if bool.status == Pool.STATUS_ACTIVE:
                    remote_pool_uuid = bool.uuid
                    break
        if not remote_pool_uuid:
            logger.error(f"Unable to find pool on remote cluster: {remote_node_uuid.cluster_id}")
            return

        lv_id, err = lvol_controller.add_lvol_ha(
            f"REP_{snapshot.snap_name}", snapshot.size, snapshot.lvol.replication_node_id, snapshot.lvol.ha_type,
            remote_pool_uuid)
        if lv_id:
            task.function_params["remote_lvol_id"] = lv_id
            task.write_to_db()
        else:
            logger.error(err)
            task.function_result = "Error creating remote lvol"
            task.write_to_db()
            return

    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    remote_lv_node = db.get_storage_node_by_id(remote_lv.node_id)
    if remote_lv_node.status != StorageNode.STATUS_ONLINE:
        task.function_result = "Target node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.retry += 1
        task.write_to_db()
        return

    # 2 connect to it
    ret = snode.rpc_client().bdev_nvme_controller_list(remote_lv.top_bdev)
    if not ret:
        remote_snode = db.get_storage_node_by_id(remote_lv.node_id)
        for nic in remote_snode.data_nics:
            ip = nic.ip4_address
            ret = snode.rpc_client().bdev_nvme_attach_controller(
                remote_lv.top_bdev, remote_lv.nqn, ip, remote_lv.subsys_port, nic.trtype)
            if not ret:
                msg = "controller attach failed"
                logger.error(msg)
                raise RuntimeError(msg)
            bdev_name = ret[0]
            if not bdev_name:
                msg = "Bdev name not returned from controller attach"
                logger.error(msg)
                raise RuntimeError(msg)
            bdev_found = False
            for i in range(5):
                ret = snode.rpc_client().get_bdevs(bdev_name)
                if ret:
                    bdev_found = True
                    break
                else:
                    time.sleep(1)

            if not bdev_found:
                logger.error("lvol Bdev not found after 5 attempts")
                raise RuntimeError(f"Failed to connect to lvol: {remote_lv.get_id()}")

    offset = 0
    if "offset" in task.function_params and task.function_params["offset"]:
        offset = task.function_params["offset"]
    # 3 start replication
    snode.rpc_client().bdev_lvol_transfer(
        lvol_name=snapshot.snap_bdev,
        offset=offset,
        cluster_batch=16,
        gateway=f"{remote_lv.top_bdev}n1",
        operation="replicate"
    )
    task.status = JobSchedule.STATUS_RUNNING
    task.function_params["start_time"] = int(time.time())
    task.write_to_db()

    if snapshot.status != SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_IN_REPLICATION
        snapshot.write_to_db()


def process_snap_replicate_finish(task, snapshot):

    task.function_result = "Done"
    task.status = JobSchedule.STATUS_DONE
    task.function_params["end_time"] = int(time.time())
    task.write_to_db()

    # detach remote lvol
    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    snode.rpc_client().bdev_nvme_detach_controller(remote_lv.top_bdev)
    remote_snode = db.get_storage_node_by_id(remote_lv.node_id)

    # chain snaps on primary
    snaps = db.get_snapshots(remote_snode.cluster_id)
    for sn in snaps:
        if sn.lvol.get_id() == snapshot.lvol.get_id():
            try:
                target_prev_snap = db.get_snapshot_by_id(sn.target_replicated_snap_uuid)
                logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {sn.snap_bdev}")
                remote_snode.rpc_client().bdev_lvol_add_clone(target_prev_snap.snap_bdev, remote_lv.top_bdev)
                break
            except KeyError:
                logger.info(f"Snapshot {sn.target_replicated_snap_uuid} not found")

    # convert to snapshot on primary
    remote_snode.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)

    sec_node = db.get_storage_node_by_id(remote_snode.secondary_node_id)
    # chain snaps on secondary
    if sec_node.status == StorageNode.STATUS_ONLINE:
        for sn in snaps:
            if sn.lvol.get_id() == snapshot.lvol.get_id():
                try:
                    target_prev_snap = db.get_snapshot_by_id(sn.target_replicated_snap_uuid)
                    logger.info(f"Chaining replicated lvol: {remote_lv.top_bdev} to snap: {sn.snap_bdev}")
                    sec_node.rpc_client().bdev_lvol_add_clone(target_prev_snap.snap_bdev, remote_lv.top_bdev)
                    break
                except KeyError:
                    logger.info(f"Snapshot {sn.target_replicated_snap_uuid} not found")

        # convert to snapshot on secondary
        sec_node.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)

    new_snapshot_uuid = str(uuid.uuid4())

    if snapshot.status == SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_ONLINE
        snapshot.target_replicated_snap_uuid = new_snapshot_uuid
        snapshot.write_to_db()

    new_snapshot = SnapShot()
    new_snapshot.uuid = new_snapshot_uuid
    new_snapshot.cluster_id = remote_snode.cluster_id
    new_snapshot.lvol = snapshot.lvol
    new_snapshot.pool_uuid = remote_lv.pool_uuid
    new_snapshot.snap_bdev = remote_lv.top_bdev
    new_snapshot.snap_uuid = remote_lv.lvol_uuid
    new_snapshot.size = snapshot.size
    new_snapshot.used_size = snapshot.used_size
    new_snapshot.snap_name = snapshot.snap_name
    new_snapshot.blobid = remote_lv.blobid
    new_snapshot.created_at = int(time.time())
    new_snapshot.source_replicated_snap_uuid = snapshot.uuid
    new_snapshot.status = SnapShot.STATUS_ONLINE
    new_snapshot.write_to_db()


    # delete lvol object
    remote_lv.bdev_stack = []
    remote_lv.write_to_db()
    lvol_controller.delete_lvol(remote_lv.get_id(), True)
    remote_lv.remove(db.kv_store)
    snapshot_events.replication_task_finished(snapshot)

    return True


def task_runner(task: JobSchedule):
    snapshot = db.get_snapshot_by_id(task.function_params["snapshot_id"])
    if not snapshot:
        task.function_result = "snapshot not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)

    if not snode:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return True

    if snode.status != StorageNode.STATUS_ONLINE:
        task.function_result = "node is not online, retrying"
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return False

    if task.retry >= task.max_retry or task.canceled is True:
        task.function_result = "max retry reached"
        if task.canceled is True:
            task.function_result = "task cancelled"

        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)

        if snapshot.status != SnapShot.STATUS_ONLINE:
            snapshot.status = SnapShot.STATUS_ONLINE
            snapshot.write_to_db()

        remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
        snode.rpc_client().bdev_nvme_detach_controller(remote_lv.top_bdev)
        lvol_controller.delete_lvol(remote_lv.get_id(), True)

        return True


    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
        process_snap_replicate_start(task, snapshot)

    elif task.status == JobSchedule.STATUS_RUNNING:
        snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
        ret = snode.rpc_client().bdev_lvol_transfer_stat(snapshot.snap_bdev)
        if not ret:
            logger.error("Failed to get transfer stat")
            return False
        status = ret["transfer_state"]
        offset = ret["offset"]
        if status == "No process":
            task.function_result = f"Status: {status}, offset:{offset}, retrying"
            task.status = JobSchedule.STATUS_NEW
            task.retry += 1
            task.write_to_db()
            return False
        if status == "In progress":
            task.function_result = f"Status: {status}, offset:{offset}"
            task.function_params["offset"] = offset
            task.write_to_db()
            return True
        if status == "Failed":
            task.function_result = f"Status: {status}, offset:{offset}, retrying"
            task.status = JobSchedule.STATUS_SUSPENDED
            task.retry += 1
            task.write_to_db()
            return False
        if status == "Done":
            process_snap_replicate_finish(task, snapshot)
            return True


logger.info("Starting Tasks runner...")
while True:
    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:
                delay_seconds = constants.TASK_EXEC_INTERVAL_SEC
                if task.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION:
                    if task.status in [JobSchedule.STATUS_NEW, JobSchedule.STATUS_SUSPENDED]:
                        active_task = False
                        for t in db.get_job_tasks(task.cluster_id):
                            if t.function_name == JobSchedule.FN_SNAPSHOT_REPLICATION and t.function_params["snapshot_id"] ==  task.function_params['snapshot_id']:
                                if t.status == JobSchedule.STATUS_RUNNING and t.canceled is False:
                                    active_task = True
                                    break
                        if active_task:
                            logger.info("replication task found for same snapshot, retry")
                            continue
                    if task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if not res:
                            time.sleep(3)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
