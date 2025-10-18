# coding=utf-8
import time
import uuid

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.snode_client import SNodeClient


logger = utils.get_logger(__name__)
utils.init_sentry_sdk(__name__)
# get DB controller
db = db_controller.DBController()


def process_snap_replicate_start(task, snapshot):
    # 1 create lvol on remote node
    logger.info("Starting snapshot replication task")
    lv_id, err = lvol_controller.add_lvol_ha(
        f"REP_{snapshot.name}", snapshot.size, snapshot.lvol.replication_node_id, snapshot.lvol.ha_type,
        snapshot.lvol.pool_uuid, use_comp=False, use_crypto=False, distr_vuid=0, max_rw_iops=0, max_rw_mbytes=0,
        max_r_mbytes=0, max_w_mbytes=0)
    remote_lv = db.get_lvol_by_id(lv_id)
    # 2 connect to it
    remote_snode = db.get_storage_node_by_id(remote_lv.node_id)
    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    for nic in remote_snode.data_nics:
        ip = nic.ip4_address
        ret = snode.rpc_client().bdev_nvme_attach_controller(
            remote_lv.top_bdev, remote_lv.nqn, ip, remote_lv.subsys_port, nic.trtype)


    # 3 start replication
    snode.rpc_client().bdev_lvol_transfer(
        lvol_name=snapshot.snap_bdev,
        offset=0,
        cluster_batch=16,
        gateway=f"{remote_lv.top_bdev}n1",
        operation="replicate"
    )
    task.function_params["remote_lvol_id"] = lv_id
    task.status = JobSchedule.STATUS_RUNNING
    task.write_to_db()

    if snapshot.status != SnapShot.STATUS_IN_REPLICATION:
        snapshot.status = SnapShot.STATUS_IN_REPLICATION
        snapshot.write_to_db()


def process_snap_replicate_finish(task, snapshot):

    task.function_result = "Done"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db()
    if snapshot.status != SnapShot.STATUS_ONLINE:
        snapshot.status = SnapShot.STATUS_ONLINE
        snapshot.write_to_db()

    remote_lv = db.get_lvol_by_id(task.function_params["remote_lvol_id"])
    snode = db.get_storage_node_by_id(snapshot.lvol.node_id)
    snode.rpc_client().bdev_nvme_detach_controller(remote_lv.top_bdev)

    remote_snode = db.get_storage_node_by_id(remote_lv.node_id)
    remote_snode.rpc_client().bdev_lvol_convert(remote_lv.top_bdev)

    new_snapshot = snapshot
    new_snapshot.uuid = str(uuid.uuid4())
    new_snapshot.cluster_id = snode.cluster_id
    new_snapshot.lvol = remote_lv
    new_snapshot.snap_bdev = remote_lv.top_bdev
    new_snapshot.write_to_db()
    lvol_controller.delete_lvol(remote_lv.get_id(), True)

    return True


def task_runner(task: JobSchedule):

    snapshot = db.get_snapshot_by_id(task.function_params["snapshot_id"])

    if task.status == JobSchedule.STATUS_NEW:
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
            task.write_to_db()
            return False
        if status == "In progress":
            task.function_result = f"Status: {status}, offset:{offset}"
            task.write_to_db()
            return True
        if status == "Failed":
            task.function_result = f"Status: {status}, offset:{offset}, retrying"
            task.status = JobSchedule.STATUS_NEW
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
                    while task.status != JobSchedule.STATUS_DONE:
                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)
                        res = task_runner(task)
                        if res:
                            if task.status == JobSchedule.STATUS_DONE:
                                break
                        else:
                            if task.retry <= 3:
                                delay_seconds *= 1
                            else:
                                delay_seconds *= 2
                        time.sleep(delay_seconds)

    time.sleep(constants.TASK_EXEC_INTERVAL_SEC)
