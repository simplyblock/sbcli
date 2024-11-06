# coding=utf-8
import time


from simplyblock_core import constants, kv_store, cluster_ops, storage_node_ops, utils
from simplyblock_core.controllers import health_controller, device_controller, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController(kv_store=db_store)


def get_cluster_target_status(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if cluster.status == cluster.STATUS_UNREADY:
        return Cluster.STATUS_UNREADY
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)

    online_nodes = 0
    offline_nodes = 0
    affected_nodes = 0
    online_devices = 0
    offline_devices = 0

    for node in snodes:
        if node.status == StorageNode.STATUS_ONLINE:
            online_nodes += 1
            node_online_devices = 0
            node_offline_devices = 0
            for dev in node.nvme_devices:
                if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_JM, NVMeDevice.STATUS_READONLY]:
                    node_online_devices += 1
                else:
                    node_offline_devices += 1

            if node_offline_devices > 0 or node_online_devices == 0:
                affected_nodes += 1

            online_devices += node_online_devices
            offline_devices += node_offline_devices

        else:
            offline_nodes += 1

    logger.debug(f"online_nodes: {online_nodes}")
    logger.debug(f"offline_nodes: {offline_nodes}")
    logger.debug(f"affected_nodes: {affected_nodes}")
    logger.debug(f"online_devices: {online_devices}")
    logger.debug(f"offline_devices: {offline_devices}")

    # if more than two affected nodes then cluster is suspended
    if affected_nodes > 2 or offline_nodes > 2:
        return Cluster.STATUS_SUSPENDED

    # if any device goes offline then cluster is degraded
    if offline_devices > 0:
        return Cluster.STATUS_DEGRADED

    # if any node goes offline then cluster is degraded
    if offline_nodes > 0:
        return Cluster.STATUS_DEGRADED

    return Cluster.STATUS_ACTIVE


def update_cluster_status(cluster_id):
    cluster = db_controller.get_cluster_by_id(cluster_id)

    if cluster.ha_type == "ha":

        if cluster.status == Cluster.STATUS_READONLY:
            return
        if cluster.status == Cluster.STATUS_UNREADY:
            return

        cluster_target_status = get_cluster_target_status(cluster_id)
        logger.info(f"Target cluster status {cluster_target_status}, current status: {cluster.status}")
        if cluster.status == cluster_target_status:
            return

        if cluster_target_status == Cluster.STATUS_ACTIVE:
            logger.info(f"Resuming cluster: {cluster_id}")
            cluster_ops.unsuspend_cluster(cluster_id)

        elif cluster_target_status == Cluster.STATUS_SUSPENDED:
            logger.warning(f"Suspending cluster: {cluster_id}")
            cluster_ops.suspend_cluster(cluster_id)

        elif cluster_target_status == Cluster.STATUS_DEGRADED:
            logger.warning(f"Degrading cluster: {cluster_id}")
            cluster_ops.degrade_cluster(cluster_id)


def set_node_online(node):
    if node.status != StorageNode.STATUS_ONLINE:
        # set devices online
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
                device_controller.device_set_online(dev.get_id())

        # set jm dev online
        if node.jm_device.status == JMDevice.STATUS_UNAVAILABLE:
            device_controller.set_jm_device_state(node.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # set node online
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_ONLINE)


def set_node_offline(node):
    if node.status != StorageNode.STATUS_UNREACHABLE:
        # set node unavailable
        storage_node_ops.set_node_status(node.get_id(), StorageNode.STATUS_UNREACHABLE)

        # set devices unavailable
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                device_controller.device_set_unavailable(dev.get_id())

        # set jm dev offline
        if node.jm_device.status != JMDevice.STATUS_UNAVAILABLE:
            device_controller.set_jm_device_state(node.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)


logger.info("Starting node monitor")
while True:
    clusters = db_controller.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()
        if cluster.status == Cluster.STATUS_IN_ACTIVATION:
            logger.info(f"Cluster status is: {cluster.status}, skipping monitoring")
            continue

        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for snode in nodes:
            if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE]:
                logger.info(f"Node status is: {snode.status}, skipping")
                continue

            logger.info(f"Checking node {snode.hostname}")

            # 1- check node ping
            ping_check = health_controller._check_node_ping(snode.mgmt_ip)
            logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")
            if not ping_check:
                time.sleep(1)
                ping_check = health_controller._check_node_ping(snode.mgmt_ip)
                logger.info(f"Check 2: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

            # 2- check node API
            node_api_check = health_controller._check_node_api(snode.mgmt_ip)
            logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

            # 3- check spdk_process
            spdk_process = health_controller._check_spdk_process_up(snode.mgmt_ip)
            logger.info(f"Check: spdk process {snode.mgmt_ip}:5000 ... {spdk_process}")

            # 4- check rpc
            node_rpc_check = health_controller._check_node_rpc(
                snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
            logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

            is_node_online = ping_check and node_api_check and spdk_process and node_rpc_check
            if is_node_online:
                set_node_online(snode)

                # check JM device
                if snode.jm_device:
                    if snode.jm_device.status in [JMDevice.STATUS_ONLINE, JMDevice.STATUS_UNAVAILABLE]:
                        ret = health_controller.check_jm_device(snode.jm_device.get_id())
                        if ret:
                            logger.info(f"JM bdev is online: {snode.jm_device.get_id()}")
                            if snode.jm_device.status != JMDevice.STATUS_ONLINE:
                                device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)
                        else:
                            logger.error(f"JM bdev is offline: {snode.jm_device.get_id()}")
                            if snode.jm_device.status != JMDevice.STATUS_UNAVAILABLE:
                                device_controller.set_jm_device_state(snode.jm_device.get_id(),
                                                                      JMDevice.STATUS_UNAVAILABLE)

            else:
                set_node_offline(snode)

                if not ping_check and not node_api_check and not spdk_process:
                    # restart on new node
                    storage_node_ops.set_node_status(snode.get_id(), StorageNode.STATUS_SCHEDULABLE)

                elif ping_check and node_api_check and not spdk_process:
                    # add node to auto restart
                    if cluster.status == Cluster.STATUS_ACTIVE:
                        tasks_controller.add_node_to_auto_restart(snode)

        update_cluster_status(cluster_id)

    logger.info(f"Sleeping for {constants.NODE_MONITOR_INTERVAL_SEC} seconds")
    time.sleep(constants.NODE_MONITOR_INTERVAL_SEC)
