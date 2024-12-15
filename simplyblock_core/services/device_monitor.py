# coding=utf-8
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


logger = utils.get_logger(__name__)


# get DB controller
db_controller = db_controller.DBController()


logger.info("Starting Device monitor...")
while True:
    nodes = db_controller.get_storage_nodes()
    for node in nodes:
        auto_restart_devices = []
        online_devices = []
        if node.status != StorageNode.STATUS_ONLINE:
            logger.warning(f"Node status is not online, id: {node.get_id()}, status: {node.status}")
            continue
        for dev in node.nvme_devices:
            if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                logger.warning(f"Device status is not online or unavailable, id: {dev.get_id()}, status: {dev.status}")
                continue

            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices.append(dev)

            if dev.io_error and dev.status == NVMeDevice.STATUS_UNAVAILABLE and not dev.retries_exhausted:
                logger.info("Adding device to auto restart")
                auto_restart_devices.append(dev)

        if len(auto_restart_devices) >= 2:
            tasks_controller.add_node_to_auto_restart(node)
        elif len(auto_restart_devices) == 1:
            tasks_controller.add_device_to_auto_restart(auto_restart_devices[0])

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
