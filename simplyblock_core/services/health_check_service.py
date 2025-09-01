# coding=utf-8
import time
from datetime import datetime

from simplyblock_core import utils
from simplyblock_core.controllers import health_controller, storage_events, device_events, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core import constants, db_controller, distr_controller, storage_node_ops

logger = utils.get_logger(__name__)


utils.init_sentry_sdk()

def set_node_health_check(snode, health_check_status):
    snode = db.get_storage_node_by_id(snode.get_id())
    if snode.health_check == health_check_status:
        return
    old_status = snode.health_check
    snode.health_check = health_check_status
    snode.updated_at = str(datetime.now())
    snode.write_to_db()
    storage_events.snode_health_check_change(snode, snode.health_check, old_status, caused_by="monitor")


def set_device_health_check(cluster_id, device, health_check_status):
    if device.health_check == health_check_status:
        return
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    for node in nodes:
        if node.nvme_devices:
            for dev in node.nvme_devices:
                if dev.get_id() == device.get_id():
                    old_status = dev.health_check
                    dev.health_check = health_check_status
                    node.write_to_db()
                    device_events.device_health_check_change(
                        dev, dev.health_check, old_status, caused_by="monitor")
                    return


# get DB controller
db = db_controller.DBController()

logger.info("Starting health check service")
while True:
    clusters = db.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()
        snodes = db.get_storage_nodes_by_cluster_id(cluster_id)
        if not snodes:
            logger.warning("storage nodes list is empty")

        for snode in snodes:
            logger.info("Node: %s, status %s", snode.get_id(), snode.status)

            if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                                    StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                logger.info(f"Node status is: {snode.status}, skipping")
                set_node_health_check(snode, False)
                for device in snode.nvme_devices:
                    set_device_health_check(cluster_id, device, False)
                continue

            # 1- check node ping
            ping_check = health_controller._check_node_ping(snode.mgmt_ip)
            logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

            # 2- check node API
            node_api_check = health_controller._check_node_api(snode.mgmt_ip)
            logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

            # 3- check node RPC
            node_rpc_check = health_controller._check_node_rpc(
                snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
            logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

            is_node_online = ping_check and node_api_check and node_rpc_check

            health_check_status = is_node_online
            if node_rpc_check:
                logger.info(f"Node device count: {len(snode.nvme_devices)}")
                node_devices_check = True
                node_remote_devices_check = True

                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password,
                    timeout=3, retry=2)
                connected_devices = []

                node_bdevs = rpc_client.get_bdevs()
                if node_bdevs:
                    # node_bdev_names = [b['name'] for b in node_bdevs]
                    node_bdev_names = {}
                    for b in node_bdevs:
                        node_bdev_names[b['name']] = b
                        for al in b['aliases']:
                            node_bdev_names[al] = b
                else:
                    node_bdev_names = {}

                subsystem_list = rpc_client.subsystem_list() or []
                subsystems = {
                        subsystem['nqn']: subsystem
                        for subsystem
                        in subsystem_list
                }

                for device in snode.nvme_devices:
                    passed = True

                    if device.io_error:
                        logger.info(f"Device io_error {device.get_id()}")
                        passed = False

                    if device.status != NVMeDevice.STATUS_ONLINE:
                        logger.info(f"Device status {device.status}")
                        passed = False

                    if snode.enable_test_device:
                        bdevs_stack = [device.nvme_bdev, device.testing_bdev, device.alceml_bdev, device.pt_bdev]
                    else:
                        bdevs_stack = [device.nvme_bdev, device.alceml_bdev, device.pt_bdev]

                    logger.info(f"Checking Device: {device.get_id()}, status:{device.status}")
                    problems = 0
                    for bdev in bdevs_stack:
                        if not bdev:
                            continue

                        if not health_controller.check_bdev(bdev, bdev_names=node_bdev_names):
                            problems += 1
                            passed = False

                    logger.info(f"Checking Device's BDevs ... ({(len(bdevs_stack) - problems)}/{len(bdevs_stack)})")

                    passed &= health_controller.check_subsystem(device.nvmf_nqn, nqns=subsystems)

                    set_device_health_check(cluster_id, device, passed)
                    if device.status == NVMeDevice.STATUS_ONLINE:
                        node_devices_check &= passed

                logger.info(f"Node remote device: {len(snode.remote_devices)}")

                for remote_device in snode.remote_devices:
                    org_dev = db.get_storage_device_by_id(remote_device.get_id())
                    org_node =  db.get_storage_node_by_id(remote_device.node_id)
                    if org_dev.status == NVMeDevice.STATUS_ONLINE and org_node.status == StorageNode.STATUS_ONLINE:
                        if health_controller.check_bdev(remote_device.remote_bdev, bdev_names=node_bdev_names):
                            connected_devices.append(remote_device.get_id())
                            continue

                        if not org_dev.alceml_bdev:
                            logger.error(f"device alceml bdev not found!, {org_dev.get_id()}")
                            continue

                        try:
                            storage_node_ops.connect_device(
                                    f"remote_{org_dev.alceml_bdev}", org_dev, rpc_client,
                                    bdev_names=list(node_bdev_names), reattach=False,
                            )
                            connected_devices.append(org_dev.get_id())
                            sn = db.get_storage_node_by_id(snode.get_id())
                            for d in sn.remote_devices:
                                if d.get_id() == remote_device.get_id():
                                    d.status = NVMeDevice.STATUS_ONLINE
                                    sn.write_to_db()
                                    break
                            distr_controller.send_dev_status_event(org_dev, NVMeDevice.STATUS_ONLINE, snode)
                        except RuntimeError:
                            logger.error(f"Failed to connect to device: {org_dev.get_id()}")
                            node_remote_devices_check = False

                connected_jms = []
                if snode.jm_device and snode.jm_device.get_id():
                    jm_device = snode.jm_device
                    logger.info(f"Node JM: {jm_device.get_id()}")
                    if jm_device.jm_bdev in node_bdev_names:
                        logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... ok")
                        connected_jms.append(jm_device.get_id())
                    else:
                        logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... not found")

                if snode.enable_ha_jm:
                    logger.info(f"Node remote JMs: {len(snode.remote_jm_devices)}")
                    for remote_device in snode.remote_jm_devices:
                        if remote_device.remote_bdev:
                            check = health_controller.check_bdev(remote_device.remote_bdev, bdev_names=node_bdev_names)
                            if check:
                                connected_jms.append(remote_device.get_id())
                            else:
                                node_remote_devices_check = False

                    for jm_id in snode.jm_ids:
                        if jm_id and jm_id not in connected_jms:
                            for nd in db.get_storage_nodes():
                                if nd.jm_device and nd.jm_device.get_id() == jm_id:
                                    if nd.status == StorageNode.STATUS_ONLINE:
                                        node_remote_devices_check = False
                                    break

                    if not node_remote_devices_check and cluster.status in [
                        Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
                        snode = db.get_storage_node_by_id(snode.get_id())
                        snode.remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(snode)
                        snode.write_to_db()

                lvstore_check = True
                snode = db.get_storage_node_by_id(snode.get_id())
                if snode.lvstore_status == "ready" or snode.status == StorageNode.STATUS_ONLINE or \
                        snode.lvstore_status == "failed" :

                    lvstore_stack = snode.lvstore_stack
                    lvstore_check &= health_controller._check_node_lvstore(
                        lvstore_stack, snode, auto_fix=True, node_bdev_names=node_bdev_names)

                    if snode.secondary_node_id:

                        lvstore_check &= health_controller._check_node_hublvol(
                            snode, node_bdev_names=node_bdev_names, node_lvols_nqns=subsystems)

                        second_node_1 = db.get_storage_node_by_id(snode.secondary_node_id)
                        if second_node_1 and second_node_1.status == StorageNode.STATUS_ONLINE:
                            lvstore_check &= health_controller._check_node_lvstore(
                                lvstore_stack, second_node_1, auto_fix=True, stack_src_node=snode)
                            lvstore_check &= health_controller._check_sec_node_hublvol(second_node_1, auto_fix=True)

                    lvol_port_check = False
                    # if node_api_check:
                    ports = [snode.lvol_subsys_port]

                    if snode.lvstore_stack_secondary_1:
                        second_node_1 = db.get_storage_node_by_id(snode.lvstore_stack_secondary_1)
                        if second_node_1 and second_node_1.status == StorageNode.STATUS_ONLINE:
                            ports.append(second_node_1.lvol_subsys_port)

                    for port in ports:
                        lvol_port_check = health_controller._check_port_on_node(snode, port)
                        logger.info(
                            f"Check: node {snode.mgmt_ip}, port: {port} ... {lvol_port_check}")
                        if not lvol_port_check:
                            tasks_controller.add_port_allow_task(snode.cluster_id, snode.get_id(), port)

                health_check_status = is_node_online and node_devices_check and node_remote_devices_check and lvstore_check
            set_node_health_check(snode, bool(health_check_status))

    time.sleep(constants.HEALTH_CHECK_INTERVAL_SEC)

