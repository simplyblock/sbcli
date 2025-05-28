# coding=utf-8
import time

from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

from simplyblock_core.snode_client import SNodeClient

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


logger.info("Starting new device discovery service...")
while True:
    nodes = db.get_storage_nodes()
    for node in nodes:
        time.sleep(constants.DEV_DISCOVERY_INTERVAL_SEC)
        break
        auto_restart_devices = []
        online_devices = []
        if node.status != StorageNode.STATUS_ONLINE or node.is_secondary_node:  # pass
            logger.warning(f"Skipping node, id: {node.get_id()}, status: {node.status}")
            continue

        known_sn = [dev.serial_number for dev in node.nvme_devices]
        if node.jm_device and 'serial_number' in node.jm_device.device_data_dict:
            known_sn.append(node.jm_device.device_data_dict['serial_number'])

        snode_api = SNodeClient(node.api_endpoint)
        node_info, _ = snode_api.info()


        # check for unused nvme devices
        if "lsblk" in node_info:
            for dev in node_info['nvme_devices']:
                if dev['serial_number'] in known_sn:
                    continue
                for block_dev in node_info['lsblk']['blockdevices']:
                    if block_dev['name'] == dev['device_name']:
                        if 'children' not in block_dev:
                            logger.info(f"Unused device found: {dev['address']}")
                            # try mount to spdk driver
                            snode_api.bind_device_to_spdk(dev['address'])
                            time.sleep(3)
                            devs = storage_node_ops.addNvmeDevices(node, [dev['address']])
                            if devs:
                                logger.info(f"New ssd found: {dev['address']}")
                                new_dev = devs[0]
                                new_dev.status = NVMeDevice.STATUS_NEW
                                node.nvme_devices.append(new_dev)
                                node.write_to_db(db.kv_store)

    time.sleep(constants.DEV_DISCOVERY_INTERVAL_SEC)
