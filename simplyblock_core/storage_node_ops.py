# coding=utf-8
import datetime
import json
import logging as log
import math
import os

import pprint

import time
import uuid

import docker

from simplyblock_core import constants, scripts, distr_controller
from simplyblock_core import utils
from simplyblock_core.controllers import lvol_controller, storage_events, snapshot_controller, device_events, \
    device_controller, tasks_controller, health_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core import shell_utils
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.pci_utils import bind_spdk_driver
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient

logger = log.getLogger()


class StorageOpsException(Exception):
    def __init__(self, message):
        self.message = message


def _get_data_nics(data_nics):
    if not data_nics:
        return
    out, _, _ = shell_utils.run_command("ip -j address show")
    data = json.loads(out)
    logger.debug("ifaces")
    logger.debug(pprint.pformat(data))

    def _get_ip4_address(list_of_addr):
        if list_of_addr:
            for data in list_of_addr:
                if data['family'] == 'inet':
                    return data['local']
        return ""

    devices = {i["ifname"]: i for i in data}
    iface_list = []
    for nic in data_nics:
        if nic not in devices:
            continue
        device = devices[nic]
        iface = IFace({
            'uuid': str(uuid.uuid4()),
            'if_name': device['ifname'],
            'ip4_address': _get_ip4_address(device['addr_info']),
            'port_number': 1,  # TODO: check this value
            'status': device['operstate'],
            'net_type': device['link_type']})
        iface_list.append(iface)

    return iface_list


def _get_if_ip_address(ifname):
    out, _, _ = shell_utils.run_command("ip -j address show %s" % ifname)
    data = json.loads(out)
    logger.debug(pprint.pformat(data))
    if data:
        data = data[0]
        if 'addr_info' in data and data['addr_info']:
            address_info = data['addr_info']
            for adr in address_info:
                if adr['family'] == 'inet':
                    return adr['local']
    logger.error("IP not found for interface %s", ifname)
    exit(1)


def addNvmeDevices(snode, devs):
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=60, retry=10)

    devices = []
    ret = rpc_client.bdev_nvme_controller_list()
    ctr_map = {}
    try:
        if ret:
            ctr_map = {i["ctrlrs"][0]['trid']['traddr']: i["name"] for i in ret}
    except:
        pass

    next_physical_label = get_next_physical_device_order()
    for pcie in devs:

        if pcie in ctr_map:
            nvme_controller = ctr_map[pcie]
            nvme_bdevs = []
            for bdev in rpc_client.get_bdevs():
                if bdev['name'].startswith(nvme_controller):
                    nvme_bdevs.append(bdev['name'])
        else:
            pci_st = str(pcie).replace("0", "").replace(":", "").replace(".", "")
            nvme_controller = "nvme_%s" % pci_st
            nvme_bdevs, err = rpc_client.bdev_nvme_controller_attach(nvme_controller, pcie)
            # time.sleep(1)

        if not nvme_bdevs:
            continue

        for nvme_bdev in nvme_bdevs:
            rpc_client.bdev_examine(nvme_bdev)
            rpc_client.bdev_wait_for_examine()

            ret = rpc_client.get_bdevs(nvme_bdev)
            nvme_dict = ret[0]
            nvme_driver_data = nvme_dict['driver_specific']['nvme'][0]
            model_number = nvme_driver_data['ctrlr_data']['model_number']
            total_size = nvme_dict['block_size'] * nvme_dict['num_blocks']

            devices.append(
                NVMeDevice({
                    'uuid': str(uuid.uuid4()),
                    'device_name': nvme_dict['name'],
                    'size': total_size,
                    'physical_label': next_physical_label,
                    'pcie_address': nvme_driver_data['pci_address'],
                    'model_id': model_number,
                    'serial_number': nvme_driver_data['ctrlr_data']['serial_number'],
                    'nvme_bdev': nvme_bdev,
                    'nvme_controller': nvme_controller,
                    'node_id': snode.get_id(),
                    'cluster_id': snode.cluster_id,
                    'status': NVMeDevice.STATUS_ONLINE
            }))
        next_physical_label += 1
    return devices


def _get_nvme_list(cluster):
    out, err, _ = shell_utils.run_command("sudo nvme list -v -o json")
    data = json.loads(out)
    logger.debug("nvme list:")
    logger.debug(pprint.pformat(data))

    def _get_pcie_controller(ctrl_list):
        if ctrl_list:
            for item in ctrl_list:
                if 'Transport' in item and item['Transport'] == 'pcie':
                    return item

    def _get_size_from_namespaces(namespaces):
        size = 0
        if namespaces:
            for ns in namespaces:
                size += ns['PhysicalSize']
        return size

    sequential_number = 0
    devices = []
    if data and 'Devices' in data:
        for dev in data['Devices'][0]['Subsystems']:
            controller = _get_pcie_controller(dev['Controllers'])
            if not controller:
                continue

            if controller['ModelNumber'] not in cluster.model_ids:
                logger.info("Device model ID is not recognized: %s, skipping device: %s",
                            controller['ModelNumber'], controller['Controller'])
                continue

            size = _get_size_from_namespaces(controller['Namespaces'])
            device_partitions_count = int(size / (cluster.blk_size * cluster.page_size_in_blocks))
            devices.append(
                NVMeDevice({
                    'device_name': controller['Controller'],
                    'sequential_number': sequential_number,
                    'partitions_count': device_partitions_count,
                    'capacity': size,
                    'size': size,
                    'pcie_address': controller['Address'],
                    'model_id': controller['ModelNumber'],
                    'serial_number': controller['SerialNumber'],
                    # 'status': controller['State']
                }))
            sequential_number += device_partitions_count
    return devices


def _run_nvme_smart_log(dev_name):
    out, _, _ = shell_utils.run_command("sudo nvme smart-log /dev/%s -o json" % dev_name)
    data = json.loads(out)
    logger.debug(out)
    return data


def _run_nvme_smart_log_add(dev_name):
    out, _, _ = shell_utils.run_command("sudo nvme intel smart-log-add /dev/%s --json" % dev_name)
    data = json.loads(out)
    logger.debug(out)
    return data


def get_next_cluster_device_order(db_controller, cluster_id):
    max_order = 0
    found = False
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            found = True
            max_order = max(max_order, dev.cluster_device_order)
    if found:
        return max_order + 1
    return 0


def get_next_physical_device_order():
    db_controller = DBController()
    max_order = 0
    found = False
    for node in db_controller.get_storage_nodes():
        for dev in node.nvme_devices:
            found = True
            max_order = max(max_order, dev.physical_label)
    if found:
        return max_order + 1
    return 0


def _search_for_partitions(rpc_client, nvme_device):
    partitioned_devices = []
    for bdev in rpc_client.get_bdevs():
        name = bdev['name']
        if name.startswith(f"{nvme_device.nvme_bdev}p"):
            new_dev = NVMeDevice(nvme_device.to_dict())
            new_dev.uuid = str(uuid.uuid4())
            new_dev.device_name = name
            new_dev.nvme_bdev = name
            new_dev.is_partition = True
            new_dev.size = bdev['block_size'] * bdev['num_blocks']
            partitioned_devices.append(new_dev)
    return partitioned_devices


def _create_jm_stack_on_raid(rpc_client, jm_nvme_bdevs, snode, after_restart):
    raid_bdev = f"raid_jm_{snode.get_id()}"
    if len(jm_nvme_bdevs) > 1:
        raid_level = "1"
        ret = rpc_client.bdev_raid_create(raid_bdev, jm_nvme_bdevs, raid_level)
        if not ret:
            logger.error(f"Failed to create raid_jm_{snode.get_id()}")
            return False
    else:
        raid_bdev = jm_nvme_bdevs[0]

    alceml_name = f"alceml_jm_{snode.get_id()}"

    nvme_bdev = raid_bdev
    test_name = ""
    # if snode.enable_test_device:
    #     test_name = f"{raid_bdev}_test"
    #     ret = rpc_client.bdev_passtest_create(test_name, raid_bdev)
    #     if not ret:
    #         logger.error(f"Failed to create passtest bdev {test_name}")
    #         return False
    #     nvme_bdev = test_name
    pba_init_mode = 3
    if after_restart:
        pba_init_mode = 1
    alceml_cpu_mask = ""
    alceml_worker_cpu_mask = ""
    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    if snode.alceml_worker_cpu_cores:
        alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_worker_cpu_cores[snode.alceml_worker_cpu_index])
        snode.alceml_worker_cpu_index = (snode.alceml_worker_cpu_index + 1) % len(snode.alceml_worker_cpu_cores)

    ret = rpc_client.bdev_alceml_create(alceml_name, nvme_bdev, str(uuid.uuid4()), pba_init_mode=pba_init_mode,
                                        alceml_cpu_mask=alceml_cpu_mask, alceml_worker_cpu_mask=alceml_worker_cpu_mask)
    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    alceml_id = str(uuid.uuid4())
    pt_name = ""
    subsystem_nqn = ""
    IP = ""
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False

        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        IP = None
        for iface in snode.data_nics:
            if iface.ip4_address:
                tr_type = iface.get_transport_type()
                logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
                IP = iface.ip4_address
                break

    ret = rpc_client.get_bdevs(raid_bdev)

    return JMDevice({
        'uuid': snode.get_id(),
        'device_name': jm_bdev,
        'size': ret[0]["block_size"] * ret[0]["num_blocks"],
        'status': JMDevice.STATUS_ONLINE,
        'jm_nvme_bdev_list': jm_nvme_bdevs,
        'raid_bdev': raid_bdev,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'testing_bdev': test_name,
        'jm_bdev': jm_bdev,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': 4420,
    })


def _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart):
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    logger.info(f"adding {alceml_name}")

    nvme_bdev = nvme.nvme_bdev
    test_name = ""
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme.nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return False
        nvme_bdev = test_name
    pba_init_mode = 3
    if after_restart:
        pba_init_mode = 1
    alceml_cpu_mask = ""
    alceml_worker_cpu_mask = ""
    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    if snode.alceml_worker_cpu_cores:
        alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_worker_cpu_cores[snode.alceml_worker_cpu_index])
        snode.alceml_worker_cpu_index = (snode.alceml_worker_cpu_index + 1) % len(snode.alceml_worker_cpu_cores)

    ret = rpc_client.bdev_alceml_create(alceml_name, nvme_bdev, alceml_id, pba_init_mode=pba_init_mode,
                                            alceml_cpu_mask=alceml_cpu_mask, alceml_worker_cpu_mask=alceml_worker_cpu_mask)

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    pt_name = ""
    subsystem_nqn = ""
    IP = ""
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False

        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        IP = None
        for iface in snode.data_nics:
            if iface.ip4_address:
                tr_type = iface.get_transport_type()
                logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
                IP = iface.ip4_address
                break

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': nvme.size,
        'status': JMDevice.STATUS_ONLINE,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'nvme_bdev': nvme.nvme_bdev,
        "serial_number": nvme.serial_number,
        "device_data_dict": nvme.to_dict(),
        'jm_bdev': jm_bdev,
        'testing_bdev': test_name,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': 4420,
    })


def _create_storage_device_stack(rpc_client, nvme, snode, after_restart):
    db_controller = DBController()
    nvme_bdev = nvme.nvme_bdev
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return False
        nvme_bdev = test_name
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    logger.info(f"adding {alceml_name}")
    pba_init_mode = 3
    if after_restart and nvme.status != NVMeDevice.STATUS_NEW:
        pba_init_mode = 1
    alceml_cpu_mask = ""
    alceml_worker_cpu_mask = ""

    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    if snode.alceml_worker_cpu_cores:
        alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_worker_cpu_cores[snode.alceml_worker_cpu_index])
        snode.alceml_worker_cpu_index = (snode.alceml_worker_cpu_index + 1) % len(snode.alceml_worker_cpu_cores)

    ret = rpc_client.bdev_alceml_create(alceml_name, nvme_bdev, alceml_id, pba_init_mode=pba_init_mode,
                                        alceml_cpu_mask=alceml_cpu_mask, alceml_worker_cpu_mask=alceml_worker_cpu_mask)
    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False
    alceml_bdev = alceml_name
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    qos_bdev = ""
    # Add qos bdev device
    if cluster.enable_qos:
        max_queue_size = cluster.max_queue_size
        inflight_io_threshold = cluster.inflight_io_threshold
        qos_bdev = f"{alceml_name}_qos"
        ret = rpc_client.qos_vbdev_create(qos_bdev, alceml_name, nvme.nvme_controller, max_queue_size, inflight_io_threshold)
        if not ret:
            logger.error(f"Failed to create qos bdev: {qos_bdev}")
            return False
        alceml_bdev = qos_bdev

    # add pass through
    pt_name = f"{alceml_name}_PT"
    ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_bdev)
    if not ret:
        logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
        return False

    subsystem_nqn = snode.subsystem + ":dev:" + alceml_id
    logger.info("creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', alceml_id)
    IP = None
    for iface in snode.data_nics:
        if iface.ip4_address:
            tr_type = iface.get_transport_type()
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
            IP = iface.ip4_address
            break
    logger.info(f"add {pt_name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
    if not ret:
        logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
        return False
    if snode.enable_test_device:
        nvme.testing_bdev = test_name
    nvme.alceml_bdev = alceml_bdev
    nvme.pt_bdev = pt_name
    nvme.qos_bdev = qos_bdev
    nvme.alceml_name = alceml_name
    nvme.nvmf_nqn = subsystem_nqn
    nvme.nvmf_ip = IP
    nvme.nvmf_port = 4420
    nvme.io_error = False
    # if nvme.status != NVMeDevice.STATUS_NEW:
    #     nvme.status = NVMeDevice.STATUS_ONLINE
    return nvme


def _create_device_partitions(rpc_client, nvme, snode, num_partitions_per_dev, jm_percent):
    nbd_device = rpc_client.nbd_start_disk(nvme.nvme_bdev)
    # time.sleep(3)
    if not nbd_device:
        logger.error(f"Failed to start nbd dev")
        return False
    snode_api = SNodeClient(snode.api_endpoint)
    result, error = snode_api.make_gpt_partitions(nbd_device, jm_percent, num_partitions_per_dev)
    if error:
        logger.error(f"Failed to make partitions")
        logger.error(error)
        return False
    time.sleep(3)
    rpc_client.nbd_stop_disk(nbd_device)
    time.sleep(1)
    rpc_client.bdev_nvme_detach_controller(nvme.nvme_controller)
    time.sleep(1)
    rpc_client.bdev_nvme_controller_attach(nvme.nvme_controller, nvme.pcie_address)
    time.sleep(1)
    rpc_client.bdev_examine(nvme.nvme_bdev)
    time.sleep(1)
    return True


def _prepare_cluster_devices_partitions(snode, devices):
    db_controller = DBController()
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    new_devices = []
    jm_devices = []
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    bdevs_names = [d['name'] for d in rpc_client.get_bdevs()]
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue

        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            new_devices.append(nvme)
            continue

        if nvme.is_partition:
            dev_part = f"{nvme.nvme_bdev[:-2]}p1"
            if dev_part in bdevs_names:
                if dev_part not in jm_devices:
                    jm_devices.append(dev_part)

            new_device = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=True)
            if not new_device:
                logger.error("failed to create dev stack")
                return False
            new_devices.append(new_device)

        else:
            # look for partitions
            partitioned_devices = _search_for_partitions(rpc_client, nvme)
            logger.debug("partitioned_devices")
            logger.debug(partitioned_devices)
            if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
                logger.info("Partitioned devices found")
            else:
                logger.info(f"Creating partitions for {nvme.nvme_bdev}")
                _create_device_partitions(rpc_client, nvme, snode, snode.num_partitions_per_dev, snode.jm_percent)
                partitioned_devices = _search_for_partitions(rpc_client, nvme)
                if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
                    logger.info("Device partitions created")
                else:
                    logger.error("Failed to create partitions")
                    return False

            jm_devices.append(partitioned_devices.pop(0).nvme_bdev)

            for dev in partitioned_devices:
                new_device = _create_storage_device_stack(rpc_client, dev, snode, after_restart=False)
                if not new_device:
                    logger.error("failed to create dev stack")
                    return False
                if nvme.status == NVMeDevice.STATUS_NEW:
                    new_device.status = NVMeDevice.STATUS_NEW
                else:
                    new_device.status = NVMeDevice.STATUS_ONLINE
                    new_device.cluster_device_order = dev_order
                    dev_order += 1
                    device_events.device_create(new_device)
                new_devices.append(new_device)

    snode.nvme_devices = new_devices

    if jm_devices:
        jm_device = _create_jm_stack_on_raid(rpc_client, jm_devices, snode, after_restart=False)
        if not jm_device:
            logger.error(f"Failed to create JM device")
            return False

        snode.jm_device = jm_device

    return True


def _prepare_cluster_devices_jm_on_dev(snode, devices):
    db_controller = DBController()
    if not devices:
        return True

    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    new_devices = []
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue

        if nvme.status == NVMeDevice.STATUS_JM:
            jm_device = _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart=False)
            if not jm_device:
                logger.error(f"Failed to create JM device")
                return False
            snode.jm_device = jm_device
            continue

        new_devices.append(nvme)
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            logger.debug(f"Device is not online : {nvme.get_id()}, status: {nvme.status}")
        else:
            nvme = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=False)
            if not nvme:
                logger.error("failed to create dev stack")
                return False
            if nvme.status == NVMeDevice.STATUS_ONLINE:
                if nvme.cluster_device_order <= 0 :
                    nvme.cluster_device_order = dev_order
                    dev_order += 1
                device_events.device_create(nvme)

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_on_restart(snode, clear_data=False):
    db_controller = DBController()

    new_devices = []

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=5*60)

    for index, nvme in enumerate(snode.nvme_devices):
        if nvme.status == NVMeDevice.STATUS_JM:
            continue

        new_devices.append(nvme)

        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                               NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_NEW]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            continue
        dev = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=not clear_data)
        if not dev:
            logger.error(f"Failed to create dev stack {nvme.get_id()}")
            return False
        # if nvme.status == NVMeDevice.STATUS_ONLINE:
        #     device_events.device_restarted(dev)

    snode.nvme_devices = new_devices
    snode.write_to_db()

    # prepare JM device
    jm_device = snode.jm_device
    if jm_device is None or jm_device.status == JMDevice.STATUS_REMOVED:
        return True

    if not jm_device or not jm_device.uuid:
        return True

    jm_device.status = JMDevice.STATUS_UNAVAILABLE

    if jm_device.jm_nvme_bdev_list:
        all_bdevs_found = True
        for bdev_name in jm_device.jm_nvme_bdev_list:
            ret = rpc_client.get_bdevs(bdev_name)
            if not ret:
                logger.error(f"BDev not found: {bdev_name}")
                all_bdevs_found = False
                break

        if all_bdevs_found:
            ret = _create_jm_stack_on_raid(rpc_client, jm_device.jm_nvme_bdev_list, snode, after_restart=not clear_data)
            if not ret:
                logger.error(f"Failed to create JM device")
                return False


    else:
        nvme_bdev = jm_device.nvme_bdev
        if snode.enable_test_device:
            ret = rpc_client.bdev_passtest_create(jm_device.testing_bdev, jm_device.nvme_bdev)
            if not ret:
                logger.error(f"Failed to create passtest bdev {jm_device.testing_bdev}")
                return False
            nvme_bdev = jm_device.testing_bdev
        alceml_cpu_mask = ""
        alceml_worker_cpu_mask = ""
        if snode.alceml_cpu_cores:
            alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
            snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)

        if snode.alceml_worker_cpu_cores:
            alceml_worker_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_worker_cpu_cores[snode.alceml_worker_cpu_index])
            snode.alceml_worker_cpu_index = (snode.alceml_worker_cpu_index + 1) % len(snode.alceml_worker_cpu_cores)

        pba_init_mode = 3
        if not clear_data:
            pba_init_mode = 1
        ret = rpc_client.bdev_alceml_create(jm_device.alceml_bdev, nvme_bdev, jm_device.get_id(),
                                                pba_init_mode=pba_init_mode, alceml_cpu_mask=alceml_cpu_mask, alceml_worker_cpu_mask=alceml_worker_cpu_mask)

        if not ret:
            logger.error(f"Failed to create alceml bdev: {jm_device.alceml_bdev}")
            return False

        jm_bdev = f"jm_{snode.get_id()}"
        ret = rpc_client.bdev_jm_create(jm_bdev, jm_device.alceml_bdev, jm_cpu_mask=snode.jm_cpu_mask)
        if not ret:
            logger.error(f"Failed to create {jm_bdev}")
            return False

        if snode.enable_ha_jm:
            # add pass through
            pt_name = f"{jm_bdev}_PT"
            ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
            if not ret:
                logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
                return False

            cluster = db_controller.get_cluster_by_id(snode.cluster_id)
            subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
            logger.info("creating subsystem %s", subsystem_nqn)
            ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
            logger.info(f"add {pt_name} to subsystem")
            ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
            if not ret:
                logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
                return False

            for iface in snode.data_nics:
                if iface.ip4_address:
                    tr_type = iface.get_transport_type()
                    logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                    ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
                    break


    return True


def _connect_to_remote_devs(this_node, force_conect_restarting_nodes=False):
    db_controller = DBController()

    rpc_client = RPCClient(
        this_node.mgmt_ip, this_node.rpc_port,
        this_node.rpc_username, this_node.rpc_password, timeout=5, retry=2)

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []

    remote_devices = []

    if force_conect_restarting_nodes:
        allowed_node_statuses = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_RESTARTING]
        allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]
    else:
        allowed_node_statuses = [StorageNode.STATUS_ONLINE]
        allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE]


    nodes = db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id)
    # connect to remote devs
    for node_index, node in enumerate(nodes):
        if node.get_id() == this_node.get_id() or node.status not in allowed_node_statuses:
            continue
        logger.info(f"Connecting to node {node.get_id()}")
        for index, dev in enumerate(node.nvme_devices):

            if dev.status not in allowed_dev_statuses:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue

            if not dev.alceml_bdev:
                logger.error(f"device alceml bdev not found!, {dev.get_id()}")
                continue
            name = f"remote_{dev.alceml_bdev}"
            bdev_name = f"{name}n1"
            if bdev_name in node_bdev_names:
                logger.info(f"bdev found {bdev_name}")
            else:
                if rpc_client.bdev_nvme_controller_list(name):
                    logger.info(f"detaching {name} from {this_node.get_id()}")
                    rpc_client.bdev_nvme_detach_controller(name)
                    time.sleep(1)

                logger.info(f"Connecting {name} to {this_node.get_id()}")
                ret = rpc_client.bdev_nvme_attach_controller_tcp(name, dev.nvmf_nqn, dev.nvmf_ip, dev.nvmf_port)
                if not ret:
                    logger.error(f"Failed to connect to device: {dev.get_id()}")
                    continue
            dev.remote_bdev = bdev_name
            remote_devices.append(dev)
            # distr_controller.send_dev_status_event(dev, dev.status, this_node)
    return remote_devices


def _connect_to_remote_jm_devs(this_node, jm_ids=[]):
    db_controller = DBController()

    rpc_client = RPCClient(
        this_node.mgmt_ip, this_node.rpc_port,
        this_node.rpc_username, this_node.rpc_password, timeout=5, retry=2)

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []
    remote_devices = []
    if this_node.is_secondary_node:
        for node in db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id):
            if node.get_id() == this_node.get_id() or node.is_secondary_node:
                continue
            if node.jm_device and node.jm_device.status in [JMDevice.STATUS_ONLINE, JMDevice.STATUS_UNAVAILABLE]:
                remote_devices.append(node.jm_device)
    else:
        if jm_ids:
            for jm_id in jm_ids:
                jm_dev = db_controller.get_jm_device_by_id(jm_id)
                if jm_dev:
                    remote_devices.append(jm_dev)
        elif len(this_node.remote_jm_devices) > 0:
            remote_devices = this_node.remote_jm_devices
        else:
            for node in db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id):
                if node.get_id() == this_node.get_id() or node.is_secondary_node:
                    continue
                if node.jm_device and node.jm_device.status == JMDevice.STATUS_ONLINE:
                    remote_devices.append(node.jm_device)
                    if len(remote_devices) >= 2 :
                        break

    new_devs = []
    for jm_dev in remote_devices:
        if not jm_dev.jm_bdev:
            continue

        org_dev = None
        org_dev_node = None
        for node in db_controller.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == jm_dev.get_id():
                org_dev = node.jm_device
                org_dev_node = node
                break

        if not org_dev:
            continue

        name = f"remote_{org_dev.jm_bdev}"
        bdev_name = f"{name}n1"
        org_dev.remote_bdev = bdev_name

        if org_dev.status == NVMeDevice.STATUS_ONLINE:

            if bdev_name in node_bdev_names:
                logger.debug(f"bdev found {bdev_name}")
                org_dev.status = JMDevice.STATUS_ONLINE
                new_devs.append(org_dev)
            else:
                if rpc_client.bdev_nvme_controller_list(name):
                    logger.info(f"detaching {name} from {this_node.get_id()}")
                    rpc_client.bdev_nvme_detach_controller(name)
                    time.sleep(1)

                logger.info(f"Connecting {name} to {this_node.get_id()}")
                ret = rpc_client.bdev_nvme_attach_controller_tcp(
                    name, org_dev.nvmf_nqn, org_dev.nvmf_ip, org_dev.nvmf_port)
                if ret:
                    org_dev.status = JMDevice.STATUS_ONLINE
                else:
                    logger.error(f"failed to connect to remote JM {name}")
                    org_dev.status = JMDevice.STATUS_UNAVAILABLE
                new_devs.append(org_dev)

        else:
            # if bdev_name in node_bdev_names:
            #     logger.debug(f"bdev found {bdev_name}")
            #     rpc_client.bdev_nvme_detach_controller(name)

            org_dev.status = JMDevice.STATUS_UNAVAILABLE
            new_devs.append(org_dev)

    return new_devs


def add_node(cluster_id, node_ip, iface_name, data_nics_list,
             max_lvol, max_snap, max_prov, spdk_image=None, spdk_debug=False,
             small_bufsize=0, large_bufsize=0, spdk_cpu_mask=None,
             num_partitions_per_dev=0, jm_percent=0, number_of_devices=0, enable_test_device=False,
             namespace=None, number_of_distribs=2, enable_ha_jm=False, is_secondary_node=False):

    db_controller = DBController()
    kv_store = db_controller.kv_store

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    if is_secondary_node is True and cluster.ha_type != "ha":
        logger.error("Secondary nodes are allowed to be added to HA cluster only")
        return False

    logger.info(f"Adding Storage node: {node_ip}")
    timeout = 60
    if spdk_image:
        timeout = 5 * 60
    snode_api = SNodeClient(node_ip, timeout=timeout, retry=10)
    node_info, _ = snode_api.info()
    if not node_info:
        logger.error("SNode API is not reachable")
        return False
    logger.info(f"Node found: {node_info['hostname']}")
    # if "cluster_id" in node_info and node_info['cluster_id']:
    #     if node_info['cluster_id'] != cluster_id:
    #         logger.error(f"This node is part of another cluster: {node_info['cluster_id']}")
    #         return False

    cloud_instance = node_info['cloud_instance']
    if not cloud_instance:
        # Create a static cloud instance from node info
        cloud_instance = {"id": node_info['system_id'], "type": "None", "cloud": "None",
                          "ip": node_info['network_interface'][iface_name]["ip"],
                          "public_ip": node_info['network_interface'][iface_name]["ip"]}
    """"
     "cloud_instance": {
          "id": "565979732541",
          "type": "m6id.large",
          "cloud": "google",
          "ip": "10.10.10.10",
          "public_ip": "20.20.20.20",
    }
    """""
    logger.debug(json.dumps(cloud_instance, indent=2))
    logger.info(f"Instance id: {cloud_instance['id']}")
    logger.info(f"Instance cloud: {cloud_instance['cloud']}")
    logger.info(f"Instance type: {cloud_instance['type']}")
    logger.info(f"Instance privateIp: {cloud_instance['ip']}")
    logger.info(f"Instance public_ip: {cloud_instance['public_ip']}")

    for node in db_controller.get_storage_nodes():
        if node.cloud_instance_id and node.cloud_instance_id == cloud_instance['id']:
            logger.error(f"Node already exists, try remove it first: {cloud_instance['id']}")
            return False

    # Tune cpu maks parameters
    cpu_count = node_info["cpu_count"]
    pollers_mask = ""
    app_thread_mask = ""
    jm_cpu_mask = ""
    alceml_cpu_cores = []
    distrib_cpu_cores = []
    alceml_worker_cpu_cores = []

    alceml_cpu_index = 0
    alceml_worker_cpu_index = 0
    distrib_cpu_index = 0
    jc_singleton_mask = ""


    poller_cpu_cores = []

    if not spdk_cpu_mask:
        spdk_cpu_mask = hex(int(math.pow(2, cpu_count))-2)

    spdk_cores = utils.hexa_to_cpu_list(spdk_cpu_mask)
    if cpu_count < spdk_cores[-1]:
        print(f"ERROR: The cpu mask {spdk_cpu_mask} is greater than the total cpus on the system {cpu_count}")
        return False
    if spdk_cores[-1] >= 64:
        print(f"ERROR: The provided cpu mask {spdk_cpu_mask} has values greater than 63, which is not allowed")
        return False
    if len(spdk_cores) >= 4:
        app_thread_core, jm_cpu_core, poller_cpu_cores, alceml_cpu_cores, alceml_worker_cpu_cores, distrib_cpu_cores, jc_singleton_core = utils.calculate_core_allocation(
            spdk_cores)

        if is_secondary_node:
            distrib_cpu_cores = distrib_cpu_cores+alceml_cpu_cores

        pollers_mask = utils.generate_mask(poller_cpu_cores)
        app_thread_mask = utils.generate_mask(app_thread_core)
        if jc_singleton_core:
            jc_singleton_mask = utils.decimal_to_hex_power_of_2(jc_singleton_core[0])
        #spdk_cpu_mask = utils.generate_mask(spdk_cores)
        jm_cpu_mask = utils.generate_mask(jm_cpu_core)
        #distrib_cpu_mask = utils.generate_mask(distrib_cpu_cores)

    # Calculate pool count
    if cloud_instance['type']:
        ins_type = cloud_instance['type']
        supported_type, storage_devices, device_size = utils.get_total_size_per_instance_type(ins_type)
        if not supported_type:
            logger.warning(f"Unsupported instance-type {ins_type} for deployment")
            if not number_of_devices:
                logger.error(f"Unsupported instance-type {ins_type} "
                             "for deployment, please specify --number-of-devices")
                return False
        else:
            number_of_devices = storage_devices
    else:
        logger.warning("Can not get instance type for this instance.")
        if not number_of_devices:
            logger.error("Unsupported instance type please specify --number-of-devices.")
            return False
    try:
        max_prov = int(max_prov)
        max_prov = f"{max_prov}g"
    except Exception:
        pass
    max_prov = int(utils.parse_size(max_prov))
    if max_prov <= 0:
        logger.error(f"Incorrect max-prov value {max_prov}")
        return False
    number_of_split = num_partitions_per_dev if num_partitions_per_dev else 1
    number_of_alceml_devices = number_of_devices * number_of_split
    # for jm
    number_of_alceml_devices += 1
    if is_secondary_node:
        number_of_distribs *= 5
    small_pool_count, large_pool_count = utils.calculate_pool_count(
        number_of_alceml_devices, number_of_distribs, cpu_count, len(poller_cpu_cores) or cpu_count)

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(small_pool_count, large_pool_count, max_lvol, max_prov,
                                                          cpu_count)

    # Calculate minimum sys memory
    minimum_sys_memory = utils.calculate_minimum_sys_memory(max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
        logger.info(f"Minimum required huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
    else:
        logger.error(f"Cannot get memory info from the instance.. Exiting")
        return False

    satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
                                                      minimum_sys_memory,
                                                      int(memory_details['free']),
                                                      int(memory_details['huge_total']))
    if not satisfied:
        logger.error(
            f"Not enough memory for the provided max_lvo: {max_lvol}, max_snap: {max_snap}, max_prov: {max_prov}.. Exiting")
        return False

    logger.info("Joining docker swarm...")
    cluster_docker = utils.get_docker_client(cluster_id)
    cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
    fdb_connection = cluster.db_connection
    results, err = snode_api.join_swarm(
        cluster_ip=cluster_ip,
        join_token=cluster_docker.swarm.attrs['JoinTokens']['Worker'],
        db_connection=cluster.db_connection,
        cluster_id=cluster_id)

    if not results:
        logger.error(f"Failed to Join docker swarm: {err}")
        return False

    rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()
    mgmt_ip = node_info['network_interface'][iface_name]['ip']
    if not spdk_image:
        spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE

    logger.info("Deploying SPDK")
    results = None
    try:
        results, err = snode_api.spdk_process_start(
            spdk_cpu_mask, spdk_mem, spdk_image, spdk_debug, cluster_ip, fdb_connection,
            namespace, mgmt_ip, constants.RPC_HTTP_PROXY_PORT, rpc_user, rpc_pass,
            multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED, timeout=constants.SPDK_PROXY_TIMEOUT)
    except Exception as e:
        logger.error(e)
        return False

    # time.sleep(5)
    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False

    data_nics = []
    names = data_nics_list or [iface_name]
    for nic in names:
        device = node_info['network_interface'][nic]
        data_nics.append(
            IFace({
                'uuid': str(uuid.uuid4()),
                'if_name': device['name'],
                'ip4_address': device['ip'],
                'status': device['status'],
                'net_type': device['net_type']}))

    hostname = node_info['hostname']
    BASE_NQN = cluster.nqn.split(":")[0]
    subsystem_nqn = f"{BASE_NQN}:{hostname}"
    # creating storage node object
    snode = StorageNode()
    snode.uuid = str(uuid.uuid4())
    snode.status = StorageNode.STATUS_IN_CREATION
    snode.baseboard_sn = node_info['system_id']
    snode.system_uuid = node_info['system_id']
    snode.create_dt = str(datetime.datetime.now())

    snode.cloud_instance_id = cloud_instance['id']
    snode.cloud_instance_type = cloud_instance['type']
    snode.cloud_instance_public_ip = cloud_instance['public_ip']

    snode.namespace = namespace
    snode.hostname = hostname
    snode.host_nqn = subsystem_nqn
    snode.subsystem = subsystem_nqn
    snode.data_nics = data_nics
    snode.mgmt_ip = mgmt_ip
    snode.primary_ip = mgmt_ip
    snode.rpc_port = constants.RPC_HTTP_PROXY_PORT
    snode.rpc_username = rpc_user
    snode.rpc_password = rpc_pass
    snode.cluster_id = cluster_id
    snode.api_endpoint = node_ip
    snode.host_secret = utils.generate_string(20)
    snode.ctrl_secret = utils.generate_string(20)
    snode.number_of_distribs = number_of_distribs
    snode.enable_ha_jm = enable_ha_jm
    snode.is_secondary_node = is_secondary_node

    if 'cpu_count' in node_info:
        snode.cpu = node_info['cpu_count']
    if 'cpu_hz' in node_info:
        snode.cpu_hz = node_info['cpu_hz']
    if 'memory' in node_info:
        snode.memory = node_info['memory']
    if 'hugepages' in node_info:
        snode.hugepages = node_info['hugepages']

    snode.spdk_cpu_mask = spdk_cpu_mask or ""
    snode.spdk_mem = spdk_mem
    snode.max_lvol = max_lvol
    snode.max_snap = max_snap
    snode.max_prov = max_prov
    snode.number_of_devices = number_of_devices
    snode.spdk_image = spdk_image or ""
    snode.spdk_debug = spdk_debug or 0
    snode.write_to_db(kv_store)
    snode.app_thread_mask = app_thread_mask or ""
    snode.pollers_mask = pollers_mask or ""
    snode.jm_cpu_mask = jm_cpu_mask
    snode.alceml_cpu_index = alceml_cpu_index
    snode.alceml_worker_cpu_index = alceml_worker_cpu_index
    snode.distrib_cpu_index = distrib_cpu_index
    snode.alceml_cpu_cores = alceml_cpu_cores
    snode.alceml_worker_cpu_cores = alceml_worker_cpu_cores
    snode.distrib_cpu_cores = distrib_cpu_cores
    snode.jc_singleton_mask = jc_singleton_mask or ""

    snode.poller_cpu_cores = poller_cpu_cores or []

    snode.iobuf_small_pool_count = small_pool_count or 0
    snode.iobuf_large_pool_count = large_pool_count or 0
    snode.iobuf_small_bufsize = small_bufsize or 0
    snode.iobuf_large_bufsize = large_bufsize or 0
    snode.enable_test_device = enable_test_device

    snode.num_partitions_per_dev = num_partitions_per_dev
    snode.jm_percent = jm_percent

    snode.write_to_db(kv_store)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=10, retry=100)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False
    rpc_client.bdev_set_options(0, 0, 0, 0)
    rpc_client.accel_set_options()

    ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
    if not ret:
        logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")

    # 2- set socket implementation options
    ret = rpc_client.sock_impl_set_options()
    if not ret:
        logger.error("Failed socket implement set options")
        return False

    # 3- set nvme config
    if snode.pollers_mask:
        ret = rpc_client.nvmf_set_config(snode.pollers_mask)
        if not ret:
            logger.error("Failed to set pollers mask")
            return False

    # 4- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    rpc_client.log_set_print_level("DEBUG")

    # 5- set app_thread cpu mask
    if snode.app_thread_mask:
        ret = rpc_client.thread_get_stats()
        app_thread_process_id = 0
        if ret.get("threads"):
            for entry in ret["threads"]:
                if entry['name'] == 'app_thread':
                    app_thread_process_id = entry['id']
                    break

        ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
        if not ret:
            logger.error("Failed to set app thread mask")
            return False

    # 6- set nvme bdev options
    ret = rpc_client.bdev_nvme_set_options()
    if not ret:
        logger.error("Failed to set nvme options")
        return False

    qpair = cluster.qpair_count
    ret = rpc_client.transport_create("TCP", qpair)
    if not ret:
        logger.error(f"Failed to create transport TCP with qpair: {qpair}")
        return False

    # 7- set jc singleton mask
    if snode.jc_singleton_mask:
        ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
        if not ret:
            logger.error("Failed to set jc singleton mask")
            return False

    # get new node info after starting spdk
    node_info, _ = snode_api.info()

    # discover devices
    nvme_devs = addNvmeDevices(snode, node_info['spdk_pcie_list'])
    if nvme_devs:

        if not is_secondary_node:

            for nvme in nvme_devs:
                nvme.status = NVMeDevice.STATUS_ONLINE

            # prepare devices
            if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

                jm_device = nvme_devs[0]
                for index, nvme in enumerate(nvme_devs):
                    if nvme.size < jm_device.size:
                        jm_device = nvme
                jm_device.status = NVMeDevice.STATUS_JM

                ret = _prepare_cluster_devices_jm_on_dev(snode, nvme_devs)
            else:
                ret = _prepare_cluster_devices_partitions(snode, nvme_devs)
            if not ret:
                logger.error("Failed to prepare cluster devices")
                return False

    logger.info("Connecting to remote devices")
    remote_devices = _connect_to_remote_devs(snode)
    snode.remote_devices = remote_devices

    if snode.enable_ha_jm:
        logger.info("Connecting to remote JMs")
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)

    snode.write_to_db(kv_store)

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the new devices")
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Connecting to node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        for dev in snode.nvme_devices:
            if dev.status != NVMeDevice.STATUS_ONLINE:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue
            name = f"remote_{dev.alceml_bdev}"
            ret = rpc_client.bdev_nvme_attach_controller_tcp(name, dev.nvmf_nqn, dev.nvmf_ip, dev.nvmf_port)
            if not ret:
                logger.warning(f"Failed to connect to device: {name}")
                continue

            dev.remote_bdev = f"{name}n1"
            idx = -1
            for i, d in enumerate(node.remote_devices):
                if d.get_id() == dev.get_id():
                    idx = i
                    break
            if idx >= 0:
                node.remote_devices[idx] = dev
            else:
                node.remote_devices.append(dev)

        if node.enable_ha_jm:
            node.remote_jm_devices = _connect_to_remote_jm_devs(node)
        node.write_to_db(kv_store)
        logger.info(f"connected to devices count: {len(node.remote_devices)}")
        # time.sleep(3)

    logger.info("Setting node status to Active")
    set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE)

    snode = db_controller.get_storage_node_by_id(snode.get_id())

    if cluster.ha_type == "ha":
        secondary_nodes = get_secondary_nodes(snode)
        if secondary_nodes:
            snode.secondary_node_id = secondary_nodes[0]
            snode.write_to_db()

    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        logger.warning(f"The cluster status is not active ({cluster.status}), adding the node without distribs and lvstore")
        logger.info("Done")
        return "Success"

    logger.info("Sending cluster map")
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    for node_index, node in enumerate(snodes):
        if  node.status != StorageNode.STATUS_ONLINE or node.get_id() == snode.get_id():
            continue
        ret = distr_controller.send_cluster_map_add_node(snode, node)

    # time.sleep(3)

    for dev in snode.nvme_devices:
        # distr_controller.send_dev_status_event(dev, NVMeDevice.STATUS_ONLINE)
        tasks_controller.add_new_device_mig_task(dev.get_id())

    # time.sleep(3)

    nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    # Create distribs
    max_size = cluster.cluster_max_size
    ret = create_lvstore(snode, cluster.distr_ndcs, cluster.distr_npcs, cluster.distr_bs,
                         cluster.distr_chunk_bs, cluster.page_size_in_blocks, max_size, nodes)
    if not ret:
        logger.error("Failed to create lvstore")
        return False

    storage_events.snode_add(snode)
    logger.info("Done")
    return "Success"


def get_number_of_online_devices(cluster_id):
    dev_count = 0
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    online_nodes = []
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1


def delete_storage_node(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if snode.status != StorageNode.STATUS_REMOVED:
        logger.error(f"Node must be in removed status")
        return False

    task_id = tasks_controller.get_active_node_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Task found: {task_id}, can not delete storage node")
        return False

    snode.remove(db_controller.kv_store)

    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Sending cluster map to node: {node.get_id()}")
        send_cluster_map(node.get_id())

    storage_events.snode_delete(snode)
    logger.info("done")


def remove_storage_node(node_id, force_remove=False, force_migrate=False):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_ONLINE:
        logger.error(f"Can not remove online node: {node_id}")
        return False

    task_id = tasks_controller.get_active_node_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Task found: {task_id}, can not remove storage node")
        if force_remove is False:
            return False

        tasks = db_controller.get_job_tasks(snode.cluster_id)
        for task in tasks:
            if task.node_id == node_id:
                if task.status != JobSchedule.STATUS_DONE and task.canceled is False:
                    tasks_controller.cancel_task(task.get_id())

    if snode.lvols:
        if force_migrate:
            for lvol in db_controller.get_lvols_by_node_id(node_id):
                pass
                # lvol_controller.migrate(lvol_id)
        elif force_remove:
            for lvol in db_controller.get_lvols_by_node_id(node_id):
                lvol_controller.delete_lvol(lvol.get_id(), True)
        else:
            logger.error("LVols found on the storage node, use --force-remove or --force-migrate")
            return False

    snaps = db_controller.get_snapshots()
    node_snaps = []
    for sn in snaps:
        if sn.lvol.node_id == node_id and sn.deleted is False:
            node_snaps.append(sn)

    if node_snaps:
        if force_migrate:
            logger.error("Not implemented!")
            return False
        elif force_remove:
            for sn in node_snaps:
                snapshot_controller.delete(sn.get_id())
        else:
            logger.error("Snapshots found on the storage node, use --force-remove or --force-migrate")
            return False

    if snode.nvme_devices:
        for dev in snode.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                distr_controller.disconnect_device(dev)

    if snode.jm_device and snode.jm_device.get_id() and snode.jm_device.status in [JMDevice.STATUS_ONLINE, JMDevice.STATUS_UNAVAILABLE]:
        logger.info("Removing JM")
        device_controller.remove_jm_device(snode.jm_device.get_id(), force=True)

    logger.debug("Leaving swarm...")
    try:
        node_docker = docker.DockerClient(base_url=f"tcp://{snode.mgmt_ip}:2375", version="auto")
        cluster_docker = utils.get_docker_client(snode.cluster_id)
        cluster_docker.nodes.get(node_docker.info()["Swarm"]["NodeID"]).remove(force=True)
    except:
        pass

    try:
        if health_controller._check_node_api(snode.mgmt_ip):
            snode_api = SNodeClient(snode.api_endpoint, timeout=20)
            snode_api.spdk_process_kill()
            snode_api.leave_swarm()
            pci_address = []
            for dev in snode.nvme_devices:
                if dev.pcie_address not in pci_address:
                    ret = snode_api.delete_dev_gpt_partitions(dev.pcie_address)
                    logger.debug(ret)
                    pci_address.append(dev.pcie_address)
    except Exception as e:
        logger.exception(e)

    set_node_status(node_id, StorageNode.STATUS_REMOVED)

    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_JM, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
            continue
        device_controller.device_set_failed(dev.get_id())

    logger.info("done")


def restart_storage_node(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None, set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0, number_of_devices=0,
        force=False, node_ip=None, clear_data=False):

    db_controller = DBController()
    kv_store = db_controller.kv_store

    db_controller = DBController()
    logger.info("Restarting storage node")
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_ONLINE:
        logger.error(f"Can not restart online node: {node_id}")
        if force is False:
            return False

    if snode.status == StorageNode.STATUS_REMOVED:
        logger.error(f"Can not restart removed node: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_RESTARTING:
        logger.error(f"Node is in restart: {node_id}")
        if force is False:
            return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not restart storage node")
        if force is False:
            return False

    logger.info("Setting node state to restarting")
    set_node_status(node_id, StorageNode.STATUS_RESTARTING)
    snode = db_controller.get_storage_node_by_id(node_id)

    if node_ip:
        if node_ip != snode.api_endpoint:
            logger.info(f"Restarting on new node with ip: {node_ip}")
            snode_api = SNodeClient(node_ip, timeout=5*60, retry=3)
            node_info, _ = snode_api.info()
            if not node_info:
                logger.error("Failed to get node info!")
                return False
            snode.api_endpoint = node_ip
            snode.mgmt_ip = node_ip.split(":")[0]
            data_nics = []
            for nic in snode.data_nics:
                device = node_info['network_interface'][nic.if_name]
                data_nics.append(
                    IFace({
                        'uuid': str(uuid.uuid4()),
                        'if_name': device['name'],
                        'ip4_address': device['ip'],
                        'status': device['status'],
                        'net_type': device['net_type']}))
            snode.data_nics = data_nics
            snode.hostname = node_info['hostname']
        else:
            node_ip = None

    logger.info(f"Restarting Storage node: {snode.mgmt_ip}")
    snode_api = SNodeClient(snode.api_endpoint, timeout=5*60, retry=3)
    node_info, _ = snode_api.info()
    logger.debug(f"Node info: {node_info}")

    logger.info("Restarting SPDK")

    if max_lvol:
        snode.max_lvol = max_lvol
    if max_snap:
        snode.max_snap = max_snap
    if max_prov:
        try:
            max_prov = int(max_prov)
            max_prov = f"{max_prov}g"
        except Exception:
            pass
        snode.max_prov = int(utils.parse_size(max_prov))
    if snode.max_prov <= 0:
        logger.error(f"Incorrect max-prov value {max_prov}")
        return False
    if spdk_image:
        snode.spdk_image = spdk_image

    # Calculate pool count
    if snode.cloud_instance_type:
        supported_type, storage_devices, device_size = utils.get_total_size_per_instance_type(snode.cloud_instance_type)
        if not supported_type:
            logger.warning(f"Unsupported instance-type {snode.cloud_instance_type} for deployment")
            if not number_of_devices:
                if not snode.number_of_devices:
                    logger.error(f"Unsupported instance-type {snode.cloud_instance_type} "
                                 "for deployment, please specify --number-of-devices")
                    return False
                number_of_devices = snode.number_of_devices
        else:
            number_of_devices = storage_devices
    else:
        logger.warning("Can not get instance type for this instance..")
        if not number_of_devices:
            if snode.number_of_devices:
                number_of_devices = snode.number_of_devices
            else:
                logger.error("Unsupported instance type please specify --number-of-devices")
                return False
    snode.number_of_devices = number_of_devices

    number_of_split = snode.num_partitions_per_dev if snode.num_partitions_per_dev else snode.num_partitions_per_dev + 1
    number_of_alceml_devices = number_of_devices * number_of_split
    small_pool_count, large_pool_count = utils.calculate_pool_count(
        number_of_alceml_devices, snode.number_of_distribs, snode.cpu, len(snode.poller_cpu_cores) or snode.cpu)

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(small_pool_count, large_pool_count, snode.max_lvol, snode.max_prov,
                                                          snode.cpu)

    # Calculate minimum sys memory
    minimum_sys_memory = utils.calculate_minimum_sys_memory(snode.max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
        logger.info(f"Minimum required huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
    else:
        logger.error(f"Cannot get memory info from the instance.. Exiting")
        return False

    satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
                                                      minimum_sys_memory,
                                                      int(memory_details['free']),
                                                      int(memory_details['huge_total']))
    if not satisfied:
        logger.error(
            f"Not enough memory for the provided max_lvo: {snode.max_lvol}, max_snap: {snode.max_snap}, max_prov: {utils.humanbytes(snode.max_prov)}.. Exiting")

    spdk_debug = snode.spdk_debug
    if set_spdk_debug:
        spdk_debug = spdk_debug
        snode.spdk_debug = spdk_debug

    cluster_docker = utils.get_docker_client(snode.cluster_id)
    cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    results = None
    try:
        fdb_connection = cluster.db_connection
        results, err = snode_api.spdk_process_start(
            snode.spdk_cpu_mask, spdk_mem, snode.spdk_image, spdk_debug, cluster_ip, fdb_connection,
            snode.namespace, snode.mgmt_ip, constants.RPC_HTTP_PROXY_PORT, snode.rpc_username, snode.rpc_password,
            multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED, timeout=constants.SPDK_PROXY_TIMEOUT)
    except Exception as e:
        logger.error(e)
        return False

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False
    # time.sleep(3)

    if small_bufsize:
        snode.iobuf_small_bufsize = small_bufsize
    if large_bufsize:
        snode.iobuf_large_bufsize = large_bufsize

    snode.write_to_db(db_controller.kv_store)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=10 * 60, retry=10)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False
    rpc_client.bdev_set_options(0, 0, 0, 0)
    rpc_client.accel_set_options()

    # 2- set socket implementation options
    ret = rpc_client.sock_impl_set_options()
    if not ret:
        logger.error("Failed socket implement set options")
        return False

    ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
    if not ret:
        logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")


    # 3- set nvme config
    if snode.pollers_mask:
        ret = rpc_client.nvmf_set_config(snode.pollers_mask)
        if not ret:
            logger.error("Failed to set pollers mask")
            return False

    # 4- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    rpc_client.log_set_print_level("DEBUG")

    # 5- set app_thread cpu mask
    if snode.app_thread_mask:
        ret = rpc_client.thread_get_stats()
        app_thread_process_id = 0
        if ret.get("threads"):
            for entry in ret["threads"]:
                if entry['name'] == 'app_thread':
                    app_thread_process_id = entry['id']
                    break

        ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
        if not ret:
            logger.error("Failed to set app thread mask")
            return False

    # 6- set nvme bdev options
    ret = rpc_client.bdev_nvme_set_options()
    if not ret:
        logger.error("Failed to set nvme options")
        return False

    qpair = cluster.qpair_count
    ret = rpc_client.transport_create("TCP", qpair)
    if not ret:
        logger.error(f"Failed to create transport TCP with qpair: {qpair}")
        return False

    # 7- set jc singleton mask
    if snode.jc_singleton_mask:
        ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
        if not ret:
            logger.error("Failed to set jc singleton mask")
            return False

    node_info, _ = snode_api.info()

    if snode.is_secondary_node:
        pass
        # ret = _prepare_cluster_devices_on_restart(snode)
        # if not ret:
        #     logger.error("Failed to prepare cluster devices")
        #     return False

    else:

        nvme_devs = addNvmeDevices(snode, node_info['spdk_pcie_list'])
        if not nvme_devs:
            logger.error("No NVMe devices was found!")
            return False

        logger.info(f"Devices found: {len(nvme_devs)}")
        logger.debug(nvme_devs)

        logger.info(f"Devices in db: {len(snode.nvme_devices)}")
        logger.debug(snode.nvme_devices)

        new_devices = []
        active_devices = []
        removed_devices = []
        known_devices_sn = []
        devices_sn = [d.serial_number for d in nvme_devs]
        for db_dev in snode.nvme_devices:
            known_devices_sn.append(db_dev.serial_number)
            if db_dev.status in [NVMeDevice.STATUS_FAILED_AND_MIGRATED, NVMeDevice.STATUS_FAILED]:
                removed_devices.append(db_dev)
                continue
            if db_dev.serial_number in devices_sn:
                logger.info(f"Device found: {db_dev.get_id()}, status {db_dev.status}")
                # if db_dev.status not in [NVMeDevice.STATUS_JM, NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_NEW]:
                #     db_dev.status = NVMeDevice.STATUS_ONLINE
                active_devices.append(db_dev)
            else:
                logger.info(f"Device not found: {db_dev.get_id()}")
                db_dev.status = NVMeDevice.STATUS_REMOVED
                removed_devices.append(db_dev)
                distr_controller.send_dev_status_event(db_dev, db_dev.status)

        if snode.jm_device and "serial_number" in snode.jm_device.device_data_dict:
            known_devices_sn.append(snode.jm_device.device_data_dict['serial_number'])

        for dev in nvme_devs:
            if dev.serial_number not in known_devices_sn:
                logger.info(f"New device found: {dev.get_id()}")
                dev.status = NVMeDevice.STATUS_NEW
                new_devices.append(dev)
                snode.nvme_devices.append(dev)

        snode.write_to_db(db_controller.kv_store)
        if node_ip:
            # prepare devices on new node
            if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

                jm_device = nvme_devs[0]
                for index, nvme in enumerate(nvme_devs):
                    if nvme.size < jm_device.size:
                        jm_device = nvme
                jm_device.status = NVMeDevice.STATUS_JM

                if snode.jm_device and snode.jm_device.get_id():
                    jm_device.uuid = snode.jm_device.get_id()

                ret = _prepare_cluster_devices_jm_on_dev(snode, nvme_devs)
            else:
                ret = _prepare_cluster_devices_partitions(snode, nvme_devs)
            if not ret:
                logger.error("Failed to prepare cluster devices")
                # return False
            snode.nvme_devices.extend(removed_devices)
        else:
            ret = _prepare_cluster_devices_on_restart(snode, clear_data=clear_data)
            if not ret:
                logger.error("Failed to prepare cluster devices")
                return False

    snode.write_to_db()

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the node devices")
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    for node in snodes:
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        node.remote_devices = _connect_to_remote_devs(node, force_conect_restarting_nodes=True)
        node.write_to_db(kv_store)

    snode = db_controller.get_storage_node_by_id(snode.get_id())
    for db_dev in snode.nvme_devices:
        if db_dev.status in [NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_READONLY]:
            db_dev.status = NVMeDevice.STATUS_ONLINE
            device_events.device_restarted(db_dev)
    snode.write_to_db(db_controller.kv_store)


    logger.info("Connecting to remote devices")
    snode.remote_devices = _connect_to_remote_devs(snode)
    if snode.enable_ha_jm:
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
    snode.health_check = True
    snode.write_to_db(db_controller.kv_store)

    if cluster.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
        if snode.lvstore_stack or snode.is_secondary_node:
            ret = recreate_lvstore(snode)
            if not ret:
                logger.error("Failed to recreate lvstore")
                return False

    logger.info("Setting node status to Online")
    set_node_status(node_id, StorageNode.STATUS_ONLINE)


    if snode.jm_device and snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)


    logger.info(f"Sending device status event")
    for dev in snode.nvme_devices:
        distr_controller.send_dev_status_event(dev, dev.status)
        if dev.status != NVMeDevice.STATUS_ONLINE:
            logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
            continue
        logger.info(f"Starting migration task for device {dev.get_id()}")
        tasks_controller.add_device_mig_task(dev.get_id())

    logger.info("Done")
    return "Success"


def list_storage_nodes(is_json, cluster_id=None):
    db_controller = DBController()
    if cluster_id:
        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    else:
        nodes = db_controller.get_storage_nodes()
    data = []
    output = ""
    now = datetime.datetime.now()

    for node in nodes:
        logger.debug(node)
        logger.debug("*" * 20)
        total_devices = len(node.nvme_devices)
        online_devices = 0
        uptime = ""
        if node.online_since and node.status == StorageNode.STATUS_ONLINE:
            try:
                uptime = utils.strfdelta((now - datetime.datetime.fromisoformat(node.online_since)))
            except:
                pass

        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1
        data.append({
            "UUID": node.uuid,
            "Hostname": node.hostname,
            "Management IP": node.mgmt_ip,
            "Devices": f"{total_devices}/{online_devices}",
            "LVols": f"{node.lvols}",
            "Status": node.status,
            "Health": node.health_check,
            "Up time": uptime,
            "Cloud ID": node.cloud_instance_id,
            "Cloud Type": node.cloud_instance_type,
            "Ext IP": node.cloud_instance_public_ip,

        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def list_storage_devices(node_id, sort, is_json):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        return False

    storage_devices = []
    jm_devices = []
    remote_devices = []
    for device in snode.nvme_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        storage_devices.append({
            "UUID": device.uuid,
            "Name": device.device_name,
            "Size": utils.humanbytes(device.size),
            "Serial Number": device.serial_number,
            "PCIe": device.pcie_address,
            "Status": device.status,
            "IO Err": device.io_error,
            "Health": device.health_check
        })

    if snode.jm_device:
        jm_devices.append({
            "UUID": snode.jm_device.uuid,
            "Name": snode.jm_device.device_name,
            "Size": utils.humanbytes(snode.jm_device.size),
            "Status": snode.jm_device.status,
            "IO Err": snode.jm_device.io_error,
            "Health": snode.jm_device.health_check
        })

    for device in snode.remote_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        remote_devices.append({
            "UUID": device.uuid,
            "Name": device.device_name,
            "Size": utils.humanbytes(device.size),
            "Serial Number": device.serial_number,
            "Node ID": device.node_id,
        })

    for device in snode.remote_jm_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        remote_devices.append({
            "UUID": device.uuid,
            "Name": device.remote_bdev,
            "Size": utils.humanbytes(device.size),
            "Serial Number": "",
            "Node ID": "",
        })

    if sort and sort in ['node-seq', 'dev-seq', 'serial']:
        if sort == 'serial':
            sort_key = "Serial Number"
        elif sort == 'dev-seq':
            sort_key = "Sequential Number"
        elif sort == 'node-seq':
            # TODO: check this key
            sort_key = "Sequential Number"
        storage_devices = sorted(storage_devices, key=lambda d: d[sort_key])

    data = {
        "Storage Devices": storage_devices,
        "JM Devices": jm_devices,
        "Remote Devices": remote_devices,
    }
    if is_json:
        return json.dumps(data, indent=2)
    else:
        out = ""
        for d in data:
            out += f"{d}\n{utils.print_table(data[d])}\n\n"
        return out


def shutdown_storage_node(node_id, force=False):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_SUSPENDED:
        logger.error("Node is not in suspended state")
        if force is False:
            return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not shutdown storage node")
        if force is False:
            return False

    logger.info("Shutting down node")
    set_node_status(node_id, StorageNode.STATUS_IN_SHUTDOWN)


    if snode.jm_device and snode.jm_device.status != JMDevice.STATUS_REMOVED:
        logger.info("Setting JM unavailable")
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)

    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
            device_controller.device_set_unavailable(dev.get_id())

    # # make other nodes disconnect from this node
    # logger.info("disconnect all other nodes connections to this node")
    # for dev in snode.nvme_devices:
    #     distr_controller.disconnect_device(dev)

    logger.info("Stopping SPDK")
    if health_controller._check_node_api(snode.mgmt_ip):
        snode_api = SNodeClient(snode.api_endpoint)
        results, err = snode_api.spdk_process_kill()

    logger.info("Setting node status to offline")
    set_node_status(node_id, StorageNode.STATUS_OFFLINE)

    tasks = db_controller.get_job_tasks(snode.cluster_id)
    for task in tasks:
        if  task.node_id == node_id and task.status != JobSchedule.STATUS_DONE:
            if task.function_name in [JobSchedule.FN_DEV_MIG, JobSchedule.FN_FAILED_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG]:
                task.canceled = True
                task.write_to_db(db_controller.kv_store)

    logger.info("Done")
    return True


def suspend_storage_node(node_id, force=False):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Node is not in online state")
        if force is False:
            return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not suspend storage node")
        if force is False:
            return False

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    online_nodes = 0
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes += 1

    if cluster.ha_type == "ha":
        if online_nodes <= 3 and cluster.status == cluster.STATUS_ACTIVE:
            logger.warning(f"Cluster mode is HA but online storage nodes are less than 3")
            if force is False:
                return False

        if cluster.status == cluster.STATUS_DEGRADED and force is False:
            logger.warning(f"Cluster status is degraded, use --force but this will suspend the cluster")
            return False

    logger.info("Suspending node")
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_ONLINE:
            device_controller.device_set_unavailable(dev.get_id())

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=5, retry=1)

    nodes = []
    if snode.is_secondary_node:
        ret = db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id)
        if ret:
            nodes.extend(ret)
    elif snode.lvols > 0:
        nodes.append(snode)


    for node in nodes:
        if node.lvols==0:
            continue

        for lvol in db_controller.get_lvols_by_node_id(node.get_id()):
            for iface in snode.data_nics:
                if iface.ip4_address:
                    ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                        lvol.nqn, iface.ip4_address, "4420", False, ana="inaccessible")

        rpc_client.bdev_lvol_set_leader(False, lvs_name=node.lvstore)
        rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)



    if snode.jm_device and snode.jm_device.status != JMDevice.STATUS_REMOVED:
        logger.info("Setting JM unavailable")
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)

    logger.info("Setting node status to suspended")
    set_node_status(snode.get_id(), StorageNode.STATUS_SUSPENDED)
    logger.info("Done")
    return True


def resume_storage_node(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_SUSPENDED:
        logger.error("Node is not in suspended state")
        return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not resume storage node")
        return False

    logger.info("Resuming node")
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            device_controller.device_set_online(dev.get_id())

    logger.info("Set JM Online")
    if snode.jm_device and snode.jm_device.get_id():
        device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

    logger.info("Connecting to remote devices")
    snode = db_controller.get_storage_node_by_id(node_id)
    snode.remote_devices = _connect_to_remote_devs(snode)
    if snode.enable_ha_jm:
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)

    snode.write_to_db(db_controller.kv_store)

    logger.debug("Setting LVols to online")

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)


    nodes = []
    if snode.is_secondary_node:
        ret = db_controller.get_primary_storage_nodes_by_secondary_node_id(node_id)
        if ret:
            nodes.extend(ret)
    elif snode.lvols > 0:
        nodes.append(snode)

    for node in nodes:
        if node.lvols==0 or not node.lvstore:
            continue

        if node.get_id() != snode.get_id():
            for lvol in db_controller.get_lvols_by_node_id(node.get_id()):
                if lvol:
                    for iface in snode.data_nics:
                        if iface.ip4_address:
                            ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                                lvol.nqn, iface.ip4_address, "4420", False)

    for node in nodes:
        if node.lvols == 0 or node.get_id() != snode.get_id():
            continue

        if node.secondary_node_id:
            sec_node = db_controller.get_storage_node_by_id(node.secondary_node_id)
            if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                remote_rpc_client = RPCClient(sec_node.mgmt_ip, sec_node.rpc_port, sec_node.rpc_username, sec_node.rpc_password)

                for lvol in db_controller.get_lvols_by_node_id(node.get_id()):
                    for iface in sec_node.data_nics:
                        if iface.ip4_address:
                            ret = remote_rpc_client.nvmf_subsystem_listener_set_ana_state(
                                    lvol.nqn, iface.ip4_address, "4420", False, ana="inaccessible")

                remote_rpc_client.bdev_lvol_set_leader(False, lvs_name=node.lvstore)
                remote_rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)
                time.sleep(1)

        if node.jm_vuid:
            ret = rpc_client.jc_explicit_synchronization(node.jm_vuid)
            logger.info(f"JM Sync res: {ret}")
            time.sleep(1)

        for lvol in db_controller.get_lvols_by_node_id(node.get_id()):
            for iface in node.data_nics:
                if iface.ip4_address:
                    ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                            lvol.nqn, iface.ip4_address, "4420", True)

        time.sleep(2)

        if node.secondary_node_id:
            sec_node = db_controller.get_storage_node_by_id(node.secondary_node_id)
            if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                remote_rpc_client = RPCClient(sec_node.mgmt_ip, sec_node.rpc_port, sec_node.rpc_username,
                                              sec_node.rpc_password)

                for lvol in db_controller.get_lvols_by_node_id(node.get_id()):
                    for iface in sec_node.data_nics:
                        if iface.ip4_address:
                            ret = remote_rpc_client.nvmf_subsystem_listener_set_ana_state(
                                lvol.nqn, iface.ip4_address, "4420", False)

    logger.info("Setting node status to online")
    set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE)
    logger.info("Done")
    return True


def get_node_capacity(node_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    this_node = db_controller.get_storage_node_by_id(node_id)
    if not this_node:
        logger.error("Storage node Not found")
        return

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_node_capacity(this_node, records_number)
    new_records = utils.process_records(records, records_count)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Absolut": utils.humanbytes(record['size_total']),
            "Provisioned": utils.humanbytes(record['size_prov']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
            "Prov Util %": f"{record['size_prov_util']}%",
        })
    return out


def get_node_iostats_history(node_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    node = db_controller.get_storage_node_by_id(node_id)
    if not node:
        logger.error("node not found")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_node_stats(node, records_number)
    new_records = utils.process_records(records, records_count)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record["read_io_ps"],
            "Read lat": record["read_latency_ps"],
            "Write speed": utils.humanbytes(record["write_bytes_ps"]),
            "Write IOPS": record["write_io_ps"],
            "Write lat": record["write_latency_ps"],
        })
    return out


def get_node_ports(node_id):
    db_controller = DBController()
    node = db_controller.get_storage_node_by_id(node_id)
    if not node:
        logger.error("node not found")
        return False

    out = []
    for nic in node.data_nics:
        out.append({
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.get_transport_type(),
            "Status": nic.status,
        })
    return utils.print_table(out)


def get_node_port_iostats(port_id, history=None, records_count=20):
    db_controller = DBController()
    nodes = db_controller.get_storage_nodes()
    nd = None
    port = None
    for node in nodes:
        for nic in node.data_nics:
            if nic.get_id() == port_id:
                port = nic
                nd = node
                break

    if not port:
        logger.error("Port not found")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_port_stats(nd.get_id(), port.get_id(), limit=records_number)
    new_records = utils.process_records(records, records_count)

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(record['date'])),
            "out_speed": utils.humanbytes(record['out_speed']),
            "in_speed": utils.humanbytes(record['in_speed']),
            "bytes_sent": utils.humanbytes(record['bytes_sent']),
            "bytes_received": utils.humanbytes(record['bytes_received']),
        })
    return utils.print_table(out)


def deploy(ifname):
    if not ifname:
        ifname = "eth0"

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        logger.error(f"Error getting interface ip: {ifname}")
        return False

    logger.info("NVMe SSD devices found on node:")
    stream = os.popen("lspci -Dnn | grep -i nvme")
    for l in stream.readlines():
        logger.info(l.strip())

    logger.info("Installing dependencies...")
    ret = scripts.install_deps()

    logger.info(f"Node IP: {dev_ip}")
    ret = scripts.configure_docker(dev_ip)

    start_storage_node_api_container(dev_ip)
    return f"{dev_ip}:5000"

def start_storage_node_api_container(node_ip):
    node_docker = docker.DockerClient(base_url=f"tcp://{node_ip}:2375", version="auto", timeout=60 * 5)
    node_docker.images.pull(constants.SIMPLY_BLOCK_DOCKER_IMAGE)

    # create the api container
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] == "/SNodeAPI":
            logger.info("SNodeAPI container found, removing...")
            node.stop(timeout=1)
            node.remove(force=True)
            # time.sleep(1)

    logger.info("Creating SNodeAPI container")
    container = node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_web/node_webapp.py storage_node",
        detach=True,
        privileged=True,
        name="SNodeAPI",
        network_mode="host",
        volumes=[
            '/etc/simplyblock:/etc/simplyblock',
            '/etc/foundationdb:/etc/foundationdb',
            '/var/tmp:/var/tmp',
            '/var/run:/var/run',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys'],
        restart_policy={"Name": "always"},
        environment=[
            f"DOCKER_IP={node_ip}"
        ]
    )
    logger.info("Pulling SPDK images")
    logger.debug(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    node_docker.images.pull(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)


def deploy_cleaner():
    scripts.deploy_cleaner()
    return True


def get_host_secret(node_id):
    db_controller = DBController()
    node = db_controller.get_storage_node_by_id(node_id)
    if not node:
        logger.error("node not found")
        return False

    return node.host_secret


def get_ctrl_secret(node_id):
    db_controller = DBController()
    node = db_controller.get_storage_node_by_id(node_id)
    if not node:
        logger.error("node not found")
        return False

    return node.ctrl_secret


def health_check(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("node not found")
        return False

    try:

        res = utils.ping_host(snode.mgmt_ip)
        if res:
            logger.info(f"Ping host: {snode.mgmt_ip}... OK")
        else:
            logger.error(f"Ping host: {snode.mgmt_ip}... Failed")

        node_docker = docker.DockerClient(base_url=f"tcp://{snode.mgmt_ip}:2375", version="auto")
        containers_list = node_docker.containers.list(all=True)
        for cont in containers_list:
            name = cont.attrs['Name']
            state = cont.attrs['State']

            if name in ['/spdk', '/spdk_proxy', '/SNodeAPI'] or name.startswith("/app_"):
                logger.debug(state)
                since = ""
                try:
                    start = datetime.datetime.fromisoformat(state['StartedAt'].split('.')[0])
                    since = str(datetime.datetime.now() - start).split('.')[0]
                except:
                    pass
                clean_name = name.split(".")[0].replace("/", "")
                logger.info(f"Container: {clean_name}, Status: {state['Status']}, Since: {since}")

    except Exception as e:
        logger.error(f"Failed to connect to node's docker: {e}")

    try:
        logger.info("Connecting to node's SPDK")
        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password,
            timeout=3, retry=1)

        ret = rpc_client.get_version()
        logger.info(f"SPDK version: {ret['version']}")

        ret = rpc_client.get_bdevs()
        logger.info(f"SPDK BDevs count: {len(ret)}")
        for bdev in ret:
            name = bdev['name']
            product_name = bdev['product_name']
            driver = ""
            for d in bdev['driver_specific']:
                driver = d
                break
            # logger.info(f"name: {name}, product_name: {product_name}, driver: {driver}")

        logger.info(f"getting device bdevs")
        for dev in snode.nvme_devices:
            nvme_bdev = rpc_client.get_bdevs(dev.nvme_bdev)
            if snode.enable_test_device:
                testing_bdev = rpc_client.get_bdevs(dev.testing_bdev)
            alceml_bdev = rpc_client.get_bdevs(dev.alceml_bdev)
            pt_bdev = rpc_client.get_bdevs(dev.pt_bdev)

            subsystem = rpc_client.subsystem_list(dev.nvmf_nqn)

            # dev.testing_bdev = test_name
            # dev.alceml_bdev = alceml_name
            # dev.pt_bdev = pt_name
            # # nvme.nvmf_nqn = subsystem_nqn
            # # nvme.nvmf_ip = IP
            # # nvme.nvmf_port = 4420

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")

    try:
        logger.info("Connecting to node's API")
        snode_api = SNodeClient(f"{snode.mgmt_ip}:5000")
        node_info, _ = snode_api.info()
        logger.info(f"Node info: {node_info['hostname']}")

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")


def get_info(node_id):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    snode_api = SNodeClient(f"{snode.mgmt_ip}:5000")
    node_info, _ = snode_api.info()
    return json.dumps(node_info, indent=2)


def get_spdk_info(node_id):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    ret = rpc_client.ultra21_util_get_malloc_stats()
    if not ret:
        logger.error(f"Failed to get SPDK info for node {node_id}")
        return False
    data = []
    for key in ret.keys():
        data.append({
            "Key": key,
            "Value": ret[key],
            "Parsed": utils.humanbytes(ret[key])
        })
    return utils.print_table(data)


def get(node_id):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    data = snode.get_clean_dict()
    return json.dumps(data, indent=2, sort_keys=True)


def set_node_status(node_id, status):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if snode.status != status:
        old_status = snode.status
        snode.status = status
        snode.updated_at = str(datetime.datetime.now())
        if status == StorageNode.STATUS_ONLINE:
            snode.online_since = str(datetime.datetime.now())
        else:
            snode.online_since = ""
        snode.write_to_db(db_controller.kv_store)
        storage_events.snode_status_change(snode, snode.status, old_status, caused_by="monitor")
        distr_controller.send_node_status_event(snode, status)

    if snode.status == StorageNode.STATUS_ONLINE:
        logger.info("Connecting to remote devices")
        snode.remote_devices = _connect_to_remote_devs(snode)
        if snode.enable_ha_jm:
            snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
        snode.health_check = True
        snode.write_to_db(db_controller.kv_store)

    return True


def recreate_lvstore_on_sec(snode):
    db_controller = DBController()
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    nodes = db_controller.get_primary_storage_nodes_by_secondary_node_id(snode.get_id())

    for node in nodes:
        remote_rpc_client = RPCClient(
            node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password,
            timeout=3, retry=2)

        lvol_list = db_controller.get_lvols_by_node_id(node.get_id())

        ret, err = _create_bdev_stack(snode, node.lvstore_stack, primary_node=node)
        ret = rpc_client.bdev_examine(node.raid)
        ret = rpc_client.bdev_wait_for_examine()

        # time.sleep(2)

        if node.status == StorageNode.STATUS_ONLINE:
            for lvol in lvol_list:
                for iface in node.data_nics:
                    if iface.ip4_address:
                        ret = remote_rpc_client.nvmf_subsystem_listener_set_ana_state(
                            lvol.nqn, iface.ip4_address, "4420", False, "inaccessible")

            remote_rpc_client.bdev_lvol_set_leader(False, lvs_name=node.lvstore)
            remote_rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)
            time.sleep(2)

        if node.jm_vuid:
            ret = rpc_client.jc_explicit_synchronization(node.jm_vuid)
            logger.info(f"JM Sync res: {ret}")
            time.sleep(1)

        for lvol in lvol_list:
            is_created, error = lvol_controller.recreate_lvol_on_node(
                lvol, snode, 1, ana_state="inaccessible")
            if error:
                logger.error(f"Failed to recreate LVol: {lvol.get_id()} on node: {snode.get_id()}")
                lvol.status = LVol.STATUS_OFFLINE
            else:
                lvol.status = LVol.STATUS_ONLINE
                lvol.io_error = False
                lvol.health_check = True
            lvol.write_to_db(db_controller.kv_store)

        rpc_client.bdev_lvol_set_leader(False, lvs_name=node.lvstore)
        rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)

        if node.status == StorageNode.STATUS_ONLINE:
            for lvol in lvol_list:
                for iface in node.data_nics:
                    if iface.ip4_address:
                        ret = remote_rpc_client.nvmf_subsystem_listener_set_ana_state(
                            lvol.nqn, iface.ip4_address, "4420", True)

            # remote_rpc_client.bdev_lvol_set_leader(False, lvs_name=node.lvstore)
            # remote_rpc_client.bdev_distrib_force_to_non_leader(node.jm_vuid)
            # time.sleep(2)
            time.sleep(5)

        for lvol in lvol_list:
            for iface in snode.data_nics:
                if iface.ip4_address:
                    ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                        lvol.nqn, iface.ip4_address, "4420", False)

    return True


def recreate_lvstore(snode):
    db_controller = DBController()

    if snode.is_secondary_node:
        return recreate_lvstore_on_sec(snode)

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    ret, err = _create_bdev_stack(snode, [], primary_node=snode)

    if err:
        logger.error(f"Failed to recreate lvstore on node {snode.get_id()}")
        logger.error(err)
        # return False

    ret = rpc_client.bdev_examine(snode.raid)
    ret = rpc_client.bdev_wait_for_examine()


    # time.sleep(1)
    # if snode.jm_vuid:
    #     ret = rpc_client.jc_explicit_synchronization(snode.jm_vuid)
    #     logger.info(f"JM Sync res: {ret}")
    #     time.sleep(1)
    rpc_client.bdev_distrib_force_to_non_leader(snode.jm_vuid)
    time.sleep(5)
    sec_node = None
    lvol_list = db_controller.get_lvols_by_node_id(snode.get_id())
    if snode.secondary_node_id:
        sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if sec_node.status == StorageNode.STATUS_ONLINE:
            sec_rpc_client = RPCClient(sec_node.mgmt_ip, sec_node.rpc_port, sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)

            for lvol in lvol_list:
                if lvol.ha_type == "ha":
                    for iface in sec_node.data_nics:
                        if iface.ip4_address:
                            ret = sec_rpc_client.nvmf_subsystem_listener_set_ana_state(
                                lvol.nqn, iface.ip4_address, "4420", False, "inaccessible")

            time.sleep(1)
            sec_rpc_client.bdev_lvol_set_leader(False, lvs_name=snode.lvstore)
            sec_rpc_client.bdev_distrib_force_to_non_leader(snode.jm_vuid)
            time.sleep(2)


    # rpc_client.bdev_lvol_set_leader(False, lvs_name=snode.lvstore)
    # rpc_client.bdev_distrib_force_to_non_leader(snode.jm_vuid)
    # time.sleep(1)


    # time.sleep(1)
    #
    # ret, err = _create_bdev_stack(snode, [], primary_node=snode)
    #
    # if err:
    #     logger.error(f"Failed to recreate lvstore on node {snode.get_id()}")
    #     logger.error(err)
    #     # return False
    #
    # ret = rpc_client.bdev_examine(snode.raid)
    # ret = rpc_client.bdev_wait_for_examine()


    if snode.jm_vuid:
        ret = rpc_client.jc_explicit_synchronization(snode.jm_vuid)
        logger.info(f"JM Sync res: {ret}")
        time.sleep(2)

    # ret = rpc_client.bdev_wait_for_examine()

    for lvol in lvol_list:
        lvol_obj = db_controller.get_lvol_by_id(lvol.get_id())
        is_created, error = lvol_controller.recreate_lvol_on_node(lvol_obj, snode)
        if error:
            logger.error(f"Failed to recreate LVol: {lvol_obj.get_id()} on node: {snode.get_id()}")
            lvol_obj.status = LVol.STATUS_OFFLINE
        else:
            lvol_obj.status = LVol.STATUS_ONLINE
            lvol_obj.io_error = False
            lvol_obj.health_check = True
        lvol_obj.write_to_db()

    if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
        time.sleep(10)
        sec_rpc_client = RPCClient(sec_node.mgmt_ip, sec_node.rpc_port, sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)
        for lvol in lvol_list:
            if lvol.ha_type == "ha":
                for iface in sec_node.data_nics:
                    if iface.ip4_address:
                        ret = sec_rpc_client.nvmf_subsystem_listener_set_ana_state(
                            lvol.nqn, iface.ip4_address, "4420", False)
    return True


def get_next_ha_jms(current_node):
    db_controller = DBController()
    jm_count = {}
    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if (node.get_id() == current_node.get_id() or node.status != StorageNode.STATUS_ONLINE  or
                node.is_secondary_node):
            continue
        if node.jm_device and node.jm_device.status == JMDevice.STATUS_ONLINE:
            jm_count[node.jm_device.get_id()] = 1 + jm_count.get(node.jm_device.get_id(), 0)
        for rem_jm_device in node.remote_jm_devices:
            if rem_jm_device.get_id() != current_node.jm_device.get_id():
                try:
                    if db_controller.get_jm_device_by_id(rem_jm_device.get_id()).status == JMDevice.STATUS_ONLINE:
                        jm_count[rem_jm_device.get_id()] = 1 + jm_count.get(rem_jm_device.get_id(), 0)
                except :
                    pass
    jm_count = dict(sorted(jm_count.items(), key=lambda x: x[1]))
    return list(jm_count.keys())[:2]


def get_node_jm_names(current_node):
    db_controller = DBController()
    jm_list = []
    if current_node.jm_device:
        jm_list.append(current_node.jm_device.jm_bdev)
    else:
        jm_list.append("JM_LOCAL")

    if current_node.enable_ha_jm:
        for jm_dev in current_node.remote_jm_devices[:2]:
            jm_list.append(jm_dev.remote_bdev)
    return jm_list


def get_secondary_nodes(current_node):
    db_controller = DBController()
    nodes = []
    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if node.get_id() != current_node.get_id() and node.is_secondary_node:
            nodes.append(node.get_id())
    return nodes


def create_lvstore(snode, ndcs, npcs, distr_bs, distr_chunk_bs, page_size_in_blocks, max_size, nodes):
    db_controller = DBController()
    lvstore_stack = []
    distrib_list = []
    distrib_vuids = []
    size = max_size // snode.number_of_distribs
    distr_page_size = (ndcs + npcs) * page_size_in_blocks
    cluster_sz = ndcs * page_size_in_blocks
    strip_size_kb = int((ndcs + npcs) * 2048)
    strip_size_kb = utils.nearest_upper_power_of_2(strip_size_kb)
    jm_vuid = 0
    jm_ids = []
    if snode.enable_ha_jm:
        jm_vuid = utils.get_random_vuid()
        jm_ids = get_next_ha_jms(snode)
        logger.debug(f"online_jms: {str(jm_ids)}")
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode, jm_ids)
        snode.jm_vuid = jm_vuid
        snode.write_to_db()

    for _ in range(snode.number_of_distribs):
        distrib_vuid = utils.get_random_vuid()
        while distrib_vuid in distrib_list:
            distrib_vuid = utils.get_random_vuid()

        distrib_name = f"distrib_{distrib_vuid}"
        lvs_name = f"LVS_{distrib_vuid}"
        lvstore_stack.extend(
            [
                {
                    "type": "bdev_distr",
                    "name": distrib_name,
                    "params": {
                        "name": distrib_name,
                        "jm_vuid": jm_vuid,
                        "vuid": distrib_vuid,
                        "ndcs": ndcs,
                        "npcs": npcs,
                        "num_blocks": size // distr_bs,
                        "block_size": distr_bs,
                        "chunk_size": distr_chunk_bs,
                        "pba_page_size": distr_page_size,
                    }
                }
            ]
        )
        distrib_list.append(distrib_name)
        distrib_vuids.append(distrib_vuid)


    if len(distrib_list) == 1:
        raid_device = distrib_list[0]
    else:
        raid_device = f"raid0_{jm_vuid}"
        lvstore_stack.append(
            {
                "type": "bdev_raid",
                "name": raid_device,
                "params": {
                    "name": raid_device,
                    "raid_level": "0",
                    "base_bdevs": distrib_list,
                    "strip_size_kb": strip_size_kb
                },
                "distribs_list": distrib_list,
                "jm_ids": jm_ids,
                "jm_vuid": jm_vuid
            }
        )

    lvs_name = f"LVS_{jm_vuid}"
    lvstore_stack.append(
        {
            "type": "bdev_lvstore",
            "name": lvs_name,
            "params": {
                "name": lvs_name,
                "bdev_name": raid_device,
                "cluster_sz": cluster_sz,
                "clear_method": "unmap",
                "num_md_pages_per_cluster_ratio": 1,
            }
        }
    )

    ret, err = _create_bdev_stack(snode, lvstore_stack)
    if err:
        logger.error(f"Failed to create lvstore on node {snode.get_id()}")
        logger.error(err)
        return False

    snode.lvstore = lvs_name
    snode.lvstore_stack = lvstore_stack
    snode.raid = raid_device
    snode.write_to_db()

    # time.sleep(1)

    if snode.secondary_node_id:
        # creating lvstore on secondary
        sec_node_1 = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        ret, err = _create_bdev_stack(sec_node_1, lvstore_stack, primary_node=snode)
        if err:
            logger.error(f"Failed to create lvstore on node {sec_node_1.get_id()}")
            logger.error(err)
            return False

        temp_rpc_client = RPCClient(
                sec_node_1.mgmt_ip, sec_node_1.rpc_port,
                sec_node_1.rpc_username, sec_node_1.rpc_password)
        # time.sleep(5)
        ret = temp_rpc_client.bdev_examine(snode.raid)
        # time.sleep(1)
        ret = temp_rpc_client.bdev_wait_for_examine()
        sec_node_1.write_to_db()

    return True


def _create_bdev_stack(snode, lvstore_stack=None, primary_node=False):
    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    created_bdevs = []
    if not lvstore_stack:
        # Restart case
        stack = snode.lvstore_stack
    else:
        stack = lvstore_stack

    node_bdevs = rpc_client.get_bdevs()
    if node_bdevs:
        node_bdev_names = [b['name'] for b in node_bdevs]
    else:
        node_bdev_names = []

    for bdev in stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']

        if name in node_bdev_names:
            continue

        elif type == "bdev_distr":
            if snode.is_secondary_node and primary_node:
                jm_list = []
                if primary_node.jm_device and primary_node.jm_device.status == JMDevice.STATUS_ONLINE:
                    bdev_name = f"remote_{primary_node.jm_device.jm_bdev}n1"
                    jm_list.append(bdev_name)
                else:
                    jm_list.append("JM_LOCAL")
                for jm_dev in primary_node.remote_jm_devices[:2]:
                    jm_list.append(jm_dev.remote_bdev)
                params['jm_names'] = jm_list
            else:
                params['jm_names'] = get_node_jm_names(snode)

            if snode.distrib_cpu_cores:
                distrib_cpu_mask = utils.decimal_to_hex_power_of_2(snode.distrib_cpu_cores[snode.distrib_cpu_index])
                params['distrib_cpu_mask'] = distrib_cpu_mask
                snode.distrib_cpu_index = (snode.distrib_cpu_index + 1) % len(snode.distrib_cpu_cores)
            ret = rpc_client.bdev_distrib_create(**params)
            if ret:
                ret = distr_controller.send_cluster_map_to_distr(snode, name)
                if not ret:
                    return False, "Failed to send cluster map"
                # time.sleep(1)

        elif type == "bdev_lvstore" and lvstore_stack and not snode.is_secondary_node:
            ret = rpc_client.create_lvstore(**params)

        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_create(**params)

        elif type == "bdev_raid":
            #
            # # sync jm
            # if snode.jm_vuid:
            #     ret = rpc_client.jc_explicit_synchronization(snode.jm_vuid)
            #     logger.info(f"JM Sync res: {ret}")
            #     # time.sleep(3)

            distribs_list = bdev["distribs_list"]
            strip_size_kb = params["strip_size_kb"]
            ret = rpc_client.bdev_raid_create(name, distribs_list, strip_size_kb=strip_size_kb)

        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if ret:
            bdev['status'] = "created"
            created_bdevs.insert(0, bdev)
        else:
            if created_bdevs:
                # rollback
                _remove_bdev_stack(created_bdevs[::-1], rpc_client)
            return False, f"Failed to create BDev: {name}"

    return True, None


def _remove_bdev_stack(bdev_stack, rpc_client, remove_distr_only=False):
    for bdev in reversed(bdev_stack):
        if 'status' in bdev and bdev['status'] == 'deleted':
            continue
        type = bdev['type']
        name = bdev['name']
        if type == "bdev_distr":
            ret = rpc_client.bdev_distrib_delete(name)
        elif type == "bdev_raid":
            ret = rpc_client.bdev_raid_delete(name)
        elif type == "bdev_lvstore" and not remove_distr_only:
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_delete(name)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue
        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
        # time.sleep(1)


def send_cluster_map(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"snode not found: {node_id}")
        return False

    logger.info("Sending cluster map")
    return distr_controller.send_cluster_map_to_node(snode)


def get_cluster_map(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"snode not found: {node_id}")
        return False

    distribs_list = []
    nodes = [snode]
    if snode.is_secondary_node:
        for node in db_controller.get_storage_nodes():
            if node.secondary_node_id == snode.get_id():
                for bdev in node.lvstore_stack:
                    type = bdev['type']
                    if type == "bdev_raid":
                        distribs_list.extend(bdev["distribs_list"])

    else:
        if snode.secondary_node_id:
            sec =  db_controller.get_storage_node_by_id(snode.secondary_node_id)
            if sec:
                nodes.append(sec)

        for bdev in snode.lvstore_stack:
            type = bdev['type']
            if type == "bdev_raid":
                distribs_list.extend(bdev["distribs_list"])

    for node in nodes:
        logger.info(f"getting cluster map from node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        for distr in distribs_list:
            ret = rpc_client.distr_get_cluster_map(distr)
            if not ret:
                logger.error(f"Failed to get distr cluster map: {distr}")
                return False
            logger.debug(ret)
            print("*"*100)
            print(distr)
            results, is_passed = distr_controller.parse_distr_cluster_map(ret)
            print(utils.print_table(results))
    return True


def make_sec_new_primary(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"snode not found: {node_id}")
        return False

    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_NEW:
            device_controller.add_device(dev.get_id())

    time.sleep(5)
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_REMOVED:
            device_controller.device_set_failed(dev.get_id())

    snode = db_controller.get_storage_node_by_id(node_id)
    snode.primary_ip = snode.mgmt_ip
    snode.write_to_db(db_controller.kv_store)
    return True


def dump_lvstore(node_id):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if not snode.lvstore:
        logger.error("Storage node does not have lvstore")
        return False

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password, timeout=180)
    logger.info(f"Dumping lvstore data on node: {snode.get_id()}")
    file_name = f"LVS_dump_{snode.hostname}_{snode.lvstore}_{str(datetime.datetime.now().isoformat())}.txt"
    file_path = f"/etc/simplyblock/{file_name}"
    ret = rpc_client.bdev_lvs_dump(snode.lvstore, file_path)
    if not ret:
        logger.error("faild to dump lvstore data")
        return False

    logger.info(f"LVS dump file path: {file_name}")
    snode_api = SNodeClient(f"{snode.mgmt_ip}:5000")
    file_content, _ = snode_api.get_file_content(file_name)

    return file_content
