# coding=utf-8
import logging
import os

import time
import sys


from simplyblock_core import constants, kv_store
from simplyblock_core.controllers import health_controller, storage_events
from simplyblock_core.models.nvme_device import NVMeDevice


def set_dev_status(device, status):
    if device.status != status:
        nodes = db_controller.get_storage_nodes()
        for node in nodes:
            if node.nvme_devices:
                for dev in node.nvme_devices:
                    if dev.get_id() == device.get_id():
                        old_status = dev.status
                        dev.status = status
                        node.write_to_db(db_store)
                        storage_events.device_status_change(dev.cluster_id, dev,  dev.status, old_status)
                        return


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()


logger.info("Starting Device monitor...")
while True:
    devices = db_controller.get_storage_devices()
    if not devices:
        logger.error("devices list is empty")

    for dev in devices:
        if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
            logger.warning(f"Device status is not online or unavailable, id: {dev.get_id()}, status: {dev.status}")
            continue

        ret = health_controller.check_device(dev.get_id())
        logger.info(f"Device: {dev.get_id()}, is healthy: {ret}")
        if ret:
            set_dev_status(dev, NVMeDevice.STATUS_ONLINE)
        else:
            set_dev_status(dev, NVMeDevice.STATUS_UNAVAILABLE)

    time.sleep(constants.DEV_MONITOR_INTERVAL_SEC)
