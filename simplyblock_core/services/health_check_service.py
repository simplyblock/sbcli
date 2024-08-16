# coding=utf-8
import time
from datetime import datetime


from simplyblock_core.controllers import health_controller, storage_events, device_events
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core import constants, kv_store, utils


logger = utils.get_logger(__name__)


def set_node_health_check(snode, health_check_status):
    snode = db_controller.get_storage_node_by_id(snode.get_id())
    if snode.health_check == health_check_status:
        return
    old_status = snode.health_check
    snode.health_check = health_check_status
    snode.updated_at = str(datetime.now())
    snode.write_to_db(db_store)
    storage_events.snode_health_check_change(snode, snode.health_check, old_status, caused_by="monitor")


def set_device_health_check(cluster_id, device, health_check_status):
    if device.health_check == health_check_status:
        return
    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    for node in nodes:
        if node.nvme_devices:
            for dev in node.nvme_devices:
                if dev.get_id() == device.get_id():
                    old_status = dev.health_check
                    dev.health_check = health_check_status
                    node.write_to_db(db_store)
                    device_events.device_health_check_change(
                        dev, dev.health_check, old_status, caused_by="monitor")


# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()

logger.info("Starting health check service")
while True:
    clusters = db_controller.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        if not snodes:
            logger.error("storage nodes list is empty")

        for snode in snodes:
            logger.info("Node: %s, status %s", snode.get_id(), snode.status)

            if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE]:
                logger.info(f"Node status is: {snode.status}, skipping")
                continue

            # 1- check node ping
            ping_check = health_controller._check_node_ping(snode.mgmt_ip)
            logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

            # 2- check node API
            node_api_check = health_controller._check_node_api(snode.mgmt_ip)
            logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

            if snode.status == StorageNode.STATUS_OFFLINE:
                set_node_health_check(snode, ping_check & node_api_check)
                continue

            # 3- check node RPC
            node_rpc_check = health_controller._check_node_rpc(
                snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
            logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

            # 4- docker API
            node_docker_check = health_controller._check_node_docker_api(snode.mgmt_ip)
            logger.info(f"Check: node docker API {snode.mgmt_ip}:2375 ... {node_docker_check}")

            is_node_online = ping_check and node_api_check and node_rpc_check and node_docker_check

            health_check_status = is_node_online
            if not node_rpc_check:
                logger.info("Putting all devices to unavailable state because RPC check failed")
                for dev in snode.nvme_devices:
                    if dev.io_error:
                        logger.debug(f"Skipping Device action because of io_error {dev.get_id()}")
                        continue
                    set_device_health_check(cluster_id, dev, False)
            else:
                logger.info(f"Node device count: {len(snode.nvme_devices)}")
                node_devices_check = True
                node_remote_devices_check = True

                for dev in snode.nvme_devices:
                    if dev.io_error:
                        logger.debug(f"Skipping Device check because of io_error {dev.get_id()}")
                        continue
                    ret = health_controller.check_device(dev.get_id())
                    set_device_health_check(cluster_id, dev, ret)
                    if dev.status == dev.STATUS_ONLINE:
                        node_devices_check &= ret

                logger.info(f"Node remote device: {len(snode.remote_devices)}")
                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password,
                    timeout=10, retry=1)
                for remote_device in snode.remote_devices:
                    ret = rpc_client.get_bdevs(remote_device.remote_bdev)
                    if ret:
                        logger.info(f"Checking bdev: {remote_device.remote_bdev} ... ok")
                    else:
                        logger.info(f"Checking bdev: {remote_device.remote_bdev} ... not found")
                    node_remote_devices_check &= bool(ret)

                health_check_status = is_node_online and node_devices_check and node_remote_devices_check
            set_node_health_check(snode, health_check_status)

    time.sleep(constants.HEALTH_CHECK_INTERVAL_SEC)

