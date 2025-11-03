# coding=utf-8
import time


from simplyblock_core import db_controller, utils, storage_node_ops, distr_controller
from simplyblock_core.controllers import tcp_ports_events, health_controller
from simplyblock_core.fw_api_client import FirewallClient
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


logger.info("Starting Tasks runner...")
while True:

    clusters = db.get_clusters()
    if not clusters:
        logger.error("No clusters found!")
    else:
        for cl in clusters:
            if cl.status == Cluster.STATUS_IN_ACTIVATION:
                continue

            tasks = db.get_job_tasks(cl.get_id(), reverse=False)
            for task in tasks:

                if task.function_name == JobSchedule.FN_PORT_ALLOW:
                    if task.status != JobSchedule.STATUS_DONE:

                        # get new task object because it could be changed from cancel task
                        task = db.get_task_by_id(task.uuid)

                        if task.canceled:
                            task.function_result = "canceled"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        node = db.get_storage_node_by_id(task.node_id)

                        if not node:
                            task.function_result = "node not found"
                            task.status = JobSchedule.STATUS_DONE
                            task.write_to_db(db.kv_store)
                            continue

                        if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
                            msg = f"Node is {node.status}, retry task"
                            logger.info(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        # check node ping
                        ping_check = health_controller._check_node_ping(node.mgmt_ip)
                        logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
                        if not ping_check:
                            time.sleep(1)
                            ping_check = health_controller._check_node_ping(node.mgmt_ip)
                            logger.info(f"Check 2: ping mgmt ip {node.mgmt_ip} ... {ping_check}")

                        if not ping_check:
                            msg = "Node ping is false, retry task"
                            logger.info(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        # check node ping
                        logger.info("connect to remote devices")
                        nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
                        # connect to remote devs
                        try:
                            node_bdevs = node.rpc_client().get_bdevs()
                            logger.debug(node_bdevs)
                            if node_bdevs:
                                node_bdev_names = {}
                                for b in node_bdevs:
                                    node_bdev_names[b['name']] = b
                                    for al in b['aliases']:
                                        node_bdev_names[al] = b
                            else:
                                node_bdev_names = {}
                            remote_devices = []
                            for nd in nodes:
                                if nd.get_id() == node.get_id() or nd.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
                                    continue
                                logger.info(f"Connecting to node {nd.get_id()}")
                                for index, dev in enumerate(nd.nvme_devices):

                                    if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                                          NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                                        logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                                        continue

                                    if not dev.alceml_bdev:
                                        raise ValueError(f"device alceml bdev not found!, {dev.get_id()}")

                                    dev.remote_bdev = storage_node_ops.connect_device(
                                        f"remote_{dev.alceml_bdev}", dev, node.rpc_client(),
                                        bdev_names=list(node_bdev_names), reattach=False)

                                    remote_devices.append(dev)
                            if not remote_devices:
                                msg = "Node unable to connect to remote devs, retry task"
                                logger.info(msg)
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)
                                continue
                            else:
                                node = db.get_storage_node_by_id(task.node_id)
                                node.remote_devices = remote_devices
                                node.write_to_db()

                            logger.info("connect to remote JM devices")
                            remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(node)
                            if not remote_jm_devices or len(remote_jm_devices) < 2:
                                msg = "Node unable to connect to remote JMs, retry task"
                                logger.info(msg)
                                task.function_result = msg
                                task.status = JobSchedule.STATUS_SUSPENDED
                                task.write_to_db(db.kv_store)
                                continue
                            else:
                                node = db.get_storage_node_by_id(task.node_id)
                                node.remote_jm_devices = remote_jm_devices
                                node.write_to_db()


                        except Exception as e:
                            logger.error(e)
                            msg = "Error when connect to remote devs, retry task"
                            logger.info(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        logger.info("Sending device status event")
                        for db_dev in node.nvme_devices:
                            distr_controller.send_dev_status_event(db_dev, db_dev.status)

                        logger.info("Finished sending device status and now waiting 5s for JMs to connect")
                        time.sleep(5)

                        sec_node = db.get_storage_node_by_id(node.secondary_node_id)
                        snode = db.get_storage_node_by_id(node.get_id())
                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                            if ret:
                                lvs_info = ret[0]
                                if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                                    # is_sec_node_leader = True
                                    # check jc_compression status
                                    jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(snode.jm_vuid)
                                    retries = 10
                                    while jc_compression_is_active:
                                        if retries <= 0:
                                            logger.warning("Timeout waiting for JC compression task to finish")
                                            break
                                        retries -= 1
                                        logger.info(
                                            f"JC compression task found on node: {sec_node.get_id()}, retrying in 60 seconds")
                                        time.sleep(60)
                                        jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(
                                            snode.jm_vuid)

                        lvstore_check = True
                        if node.lvstore_status == "ready":
                            lvstore_check &= health_controller._check_node_lvstore(node.lvstore_stack, node, auto_fix=True)
                            if node.secondary_node_id:
                                lvstore_check &= health_controller._check_node_hublvol(node)
                                sec_node = db.get_storage_node_by_id(node.secondary_node_id)
                                if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                                    lvstore_check &= health_controller._check_sec_node_hublvol(sec_node, auto_fix=True)

                        if lvstore_check is False:
                            msg = "Node LVolStore check fail, retry later"
                            logger.warning(msg)
                            task.function_result = msg
                            task.status = JobSchedule.STATUS_SUSPENDED
                            task.write_to_db(db.kv_store)
                            continue

                        if task.status != JobSchedule.STATUS_RUNNING:
                            task.status = JobSchedule.STATUS_RUNNING
                            task.write_to_db(db.kv_store)

                        not_deleted = []
                        for bdev_name in snode.lvol_sync_del_queue:
                            logger.info(f"Sync delete bdev: {bdev_name} from node: {snode.get_id()}")
                            ret, err = snode.rpc_client().delete_lvol(bdev_name, del_async=True)
                            if not ret:
                                if "code" in err and err["code"] == -19:
                                    logger.error(f"Sync delete completed with error: {err}")
                                else:
                                    logger.error(
                                        f"Failed to sync delete bdev: {bdev_name} from node: {snode.get_id()}")
                                    not_deleted.append(bdev_name)
                        snode.lvol_sync_del_queue = not_deleted
                        snode.write_to_db()

                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            sec_rpc_client = sec_node.rpc_client()
                            sec_rpc_client.bdev_lvol_set_leader(node.lvstore, leader=False, bs_nonleadership=True)

                        port_number = task.function_params["port_number"]
                        snode_api = SNodeClient(f"{node.mgmt_ip}:5000", timeout=3, retry=2)

                        logger.info(f"Allow port {port_number} on node {node.get_id()}")

                        fw_api = FirewallClient(snode, timeout=5, retry=2)
                        port_type = "tcp"
                        if node.active_rdma:
                            port_type = "udp"
                        fw_api.firewall_set_port(port_number, port_type, "allow", node.rpc_port)
                        tcp_ports_events.port_allowed(node, port_number)

                        task.function_result = f"Port {port_number} allowed on node"
                        task.status = JobSchedule.STATUS_DONE
                        task.write_to_db(db.kv_store)

    time.sleep(5)
