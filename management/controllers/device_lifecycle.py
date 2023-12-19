# coding=utf-8
import logging
import time

from management import constants
from management import utils
from management.kv_store import DBController
from management.models.compute_node import ComputeNode
from management.models.nvme_device import NVMeDevice
from management.models.storage_node import StorageNode
from management import services
from management import spdk_installer
from management.pci_utils import  bind_spdk_driver
from management.rpc_client import RPCClient
from management.storage_node_ops import _get_nvme_list, _run_nvme_smart_log, _run_nvme_smart_log_add

logger = logging.getLogger()




def run_test_storage_device(kv_store, dev_name):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    nvme_device = None
    for node_nvme_device in snode.nvme_devices:
        if node_nvme_device.device_name == dev_name:
            nvme_device = node_nvme_device
            break

    if nvme_device is None:
        logging.error("Device not found")
        exit(1)

    global_settings = db_controller.get_global_settings()
    logger.debug("Running smart-log on device: %s", dev_name)
    smart_log_data = _run_nvme_smart_log(dev_name)
    if "critical_warning" in smart_log_data:
        critical_warnings = smart_log_data["critical_warning"]
        if critical_warnings > 0:
            logger.info("Critical warnings found: %s on device: %s, setting drive to failed state" %
                        (critical_warnings, dev_name))
            nvme_device.status = NVMeDevice.STATUS_FAILED
    logger.debug("Running smart-log-add on device: %s", dev_name)
    additional_smart_log = _run_nvme_smart_log_add(dev_name)
    program_fail_count = additional_smart_log['Device stats']['program_fail_count']['normalized']
    erase_fail_count = additional_smart_log['Device stats']['erase_fail_count']['normalized']
    crc_error_count = additional_smart_log['Device stats']['crc_error_count']['normalized']
    if program_fail_count < global_settings.NVME_PROGRAM_FAIL_COUNT:
        nvme_device.status = NVMeDevice.STATUS_FAILED
        logger.info("program_fail_count: %s is below %s on drive: %s, setting drive to failed state",
                    program_fail_count, global_settings.NVME_PROGRAM_FAIL_COUNT, dev_name)
    if erase_fail_count < global_settings.NVME_ERASE_FAIL_COUNT:
        nvme_device.status = NVMeDevice.STATUS_FAILED
        logger.info("erase_fail_count: %s is below %s on drive: %s, setting drive to failed state",
                    erase_fail_count, global_settings.NVME_ERASE_FAIL_COUNT, dev_name)
    if crc_error_count < global_settings.NVME_CRC_ERROR_COUNT:
        nvme_device.status = NVMeDevice.STATUS_FAILED
        logger.info("crc_error_count: %s is below %s on drive: %s, setting drive to failed state",
                    crc_error_count, global_settings.NVME_CRC_ERROR_COUNT, dev_name)

    snode.write_to_db(kv_store)
    logger.info("Done")


def add_storage_device(kv_store, dev_name):
    db_controller = DBController(kv_store)
    global_settings = db_controller.get_global_settings()
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    for node_nvme_device in snode.nvme_devices:
        if node_nvme_device.device_name == dev_name:
            logging.error("Device already added to the cluster")
            exit(1)

    nvme_devs = _get_nvme_list(global_settings)
    for device in nvme_devs:
        if device.device_name == dev_name:
            nvme_device = device
            break
    else:
        logging.error("Device not found: %s", dev_name)
        exit(1)

    # running smart tests
    logger.debug("Running smart-log on device: %s", dev_name)
    smart_log_data = _run_nvme_smart_log(dev_name)
    if "critical_warning" in smart_log_data:
        critical_warnings = smart_log_data["critical_warning"]
        if critical_warnings > 0:
            logger.info("Critical warnings found: %s on device: %s" % (critical_warnings, dev_name))
            exit(1)

    logger.debug("Running smart-log-add on device: %s", dev_name)
    additional_smart_log = _run_nvme_smart_log_add(dev_name)
    program_fail_count = additional_smart_log['Device stats']['program_fail_count']['normalized']
    erase_fail_count = additional_smart_log['Device stats']['erase_fail_count']['normalized']
    crc_error_count = additional_smart_log['Device stats']['crc_error_count']['normalized']
    if program_fail_count < global_settings.NVME_PROGRAM_FAIL_COUNT:
        logger.info("program_fail_count: %s is below %s on drive: %s",
                    program_fail_count, global_settings.NVME_PROGRAM_FAIL_COUNT, dev_name)
        exit(1)
    if erase_fail_count < global_settings.NVME_ERASE_FAIL_COUNT:
        logger.info("erase_fail_count: %s is below %s on drive: %s",
                    erase_fail_count, global_settings.NVME_ERASE_FAIL_COUNT, dev_name)
        exit(1)
    if crc_error_count < global_settings.NVME_CRC_ERROR_COUNT:
        logger.info("crc_error_count: %s is below %s on drive: %s",
                    crc_error_count, global_settings.NVME_CRC_ERROR_COUNT, dev_name)
        exit(1)

    logger.info("binding spdk drivers")
    bind_spdk_driver(nvme_device.pcie_address)
    time.sleep(1)

    logger.info("init device in spdk")
    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # attach bdev controllers
    logger.info("adding controller")
    ret = rpc_client.bdev_nvme_controller_attach("nvme_ultr21a_%s" % nvme_device.sequential_number, nvme_device.pcie_address)
    logger.debug(ret)

    logger.debug("controllers list")
    ret = rpc_client.bdev_nvme_controller_list()
    logger.debug(ret)

    # # create nvme partitions
    # device_to_partition, status_ns = create_partitions_arrays(global_settings, nvme_devs)
    # out_data = {
    #     'device_to_partition': device_to_partition,
    #     'status_ns': status_ns,
    #     'NS_LB_SIZE': global_settings.NS_LB_SIZE,
    #     'NS_SIZE_IN_LBS': global_settings.NS_SIZE_IN_LBS}
    # rpc_client.create_nvme_partitions(out_data)

    # allocate bdevs
    logger.info("Allocating bdevs")
    ret = rpc_client.allocate_bdev(nvme_device.device_name, nvme_device.sequential_number)
    logger.debug(ret)

    # creating namespaces
    logger.info("Creating namespaces")
    ret = rpc_client.nvmf_subsystem_add_ns(snode.subsystem, nvme_device.device_name)
    logger.debug(ret)

    # set device new sequential number, increase node device count
    nvme_device.sequential_number = snode.sequential_number
    snode.sequential_number += nvme_device.partitions_count
    snode.partitions_count += nvme_device.partitions_count
    snode.nvme_devices.append(nvme_device)
    snode.write_to_db(kv_store)

    # create or update cluster map
    logger.info("Updating cluster map")
    cmap = db_controller.get_cluster_map()
    cmap.recalculate_partitions()
    logger.debug(cmap)
    cmap.write_to_db(kv_store)

    logger.info("Done")
    return True


def reset_storage_device(kv_store, dev_name):
    db_controller = DBController(kv_store)
    baseboard_sn = utils.get_baseboard_sn()
    snode = db_controller.get_storage_node_by_id(baseboard_sn)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        exit(1)

    nvme_device = None
    for node_nvme_device in snode.nvme_devices:
        if node_nvme_device.device_name == dev_name:
            nvme_device = node_nvme_device
            break

    if nvme_device is None:
        logging.error("Device not found")
        exit(1)

    logging.info("Resetting device")

    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    # make suspend request
    response = rpc_client.reset_device(nvme_device.device_name)
    if 'result' in response and response['result']:
        logging.info("Setting device status to resetting")
        nvme_device.status = NVMeDevice.STATUS_RESETTING
        snode.write_to_db(kv_store)
        logger.info("Done")
        return True
    else:
        logger.error("Error resetting device")
        logger.debug(response)
        exit(1)
