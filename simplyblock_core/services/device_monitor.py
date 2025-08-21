# coding=utf-8
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller, device_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()


logger.info("Starting Device monitor...")
while True:
    for cluster in db.get_clusters():
        for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
            auto_restart_devices = []

            if node.status != StorageNode.STATUS_ONLINE:
                logger.warning(f"Node status is not online, id: {node.get_id()}, status: {node.status}")
                continue
            for dev in node.nvme_devices:
                if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                                      NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    logger.warning(f"Device status is not recognised, id: {dev.get_id()}, status: {dev.status}")
                    continue
                if cluster.status == Cluster.STATUS_ACTIVE:
                    if dev.status in [NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                        dev_stat = db.get_device_stats(dev, 1)
                        if dev_stat and dev_stat[0].size_util < cluster.cap_crit:
                            device_controller.device_set_online(dev.get_id())

                elif dev.io_error and dev.status == NVMeDevice.STATUS_UNAVAILABLE and not dev.retries_exhausted:
                    logger.info("Adding device to auto restart")
                    auto_restart_devices.append(dev)

            if len(auto_restart_devices) >= 2:
                tasks_controller.add_node_to_auto_restart(node)
            elif len(auto_restart_devices) == 1:
                tasks_controller.add_device_to_auto_restart(auto_restart_devices[0])

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
