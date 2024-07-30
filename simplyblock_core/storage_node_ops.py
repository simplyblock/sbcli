# coding=utf-8
import datetime
import json
import logging as log
import os

import pprint

import time
import uuid

import docker

from simplyblock_core import constants, scripts, distr_controller
from simplyblock_core import utils
from simplyblock_core.controllers import lvol_controller, storage_events, snapshot_controller, device_events, \
    device_controller, tasks_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core import shell_utils
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.pci_utils import get_nvme_devices, bind_spdk_driver
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


def addNvmeDevices(cluster, rpc_client, devs, snode):
    devices = []
    ret = rpc_client.bdev_nvme_controller_list()
    ctr_map = {}
    try:
        if ret:
            ctr_map = {i["ctrlrs"][0]['trid']['traddr']: i["name"] for i in ret}
    except:
        pass

    next_physical_label = get_next_physical_device_order()
    for index, pcie in enumerate(devs):

        if pcie in ctr_map:
            nvme_controller = ctr_map[pcie]
        else:
            nvme_controller = "nvme_%s" % index
            ret, err = rpc_client.bdev_nvme_controller_attach(nvme_controller, pcie)
            time.sleep(2)

        nvme_bdev = f"{nvme_controller}n1"
        rpc_client.bdev_examine(nvme_bdev)
        time.sleep(5)
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
            new_dev.size = bdev['block_size'] * bdev['num_blocks']
            partitioned_devices.append(new_dev)
    return partitioned_devices


def _create_jm_stack_on_raid(rpc_client, jm_nvme_bdevs, snode, after_restart):
    raid_bdev = f"raid_jm_{snode.get_id()}"
    ret = rpc_client.bdev_raid_create(raid_bdev, jm_nvme_bdevs)
    if not ret:
        logger.error(f"Failed to create raid_jm_{snode.get_id()}")
        return False
    alceml_name = f"alceml_jm_{snode.get_id()}"
    pba_init_mode = 3
    if after_restart:
        pba_init_mode = 2
    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        ret = rpc_client.bdev_alceml_create(alceml_name, raid_bdev, str(uuid.uuid4()), pba_init_mode=pba_init_mode,
                                            alceml_cpu_mask=alceml_cpu_mask)
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    else:
        ret = rpc_client.bdev_alceml_create(alceml_name, raid_bdev, str(uuid.uuid4()), pba_init_mode=pba_init_mode)
    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False
    ret = rpc_client.get_bdevs(raid_bdev)

    return JMDevice({
        'uuid': str(uuid.uuid4()),
        'device_name': jm_bdev,
        'size': ret[0]["block_size"] * ret[0]["num_blocks"],
        'status': JMDevice.STATUS_ONLINE,
        'jm_nvme_bdev_list': jm_nvme_bdevs,
        'raid_bdev': raid_bdev,
        'alceml_bdev': alceml_name,
        'jm_bdev': jm_bdev
    })


def _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart):

    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    logger.info(f"adding {alceml_name}")

    pba_init_mode = 3
    if after_restart:
        pba_init_mode = 2
    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        ret = rpc_client.bdev_alceml_create(alceml_name, nvme.nvme_bdev, alceml_id, pba_init_mode=pba_init_mode,
                                            alceml_cpu_mask=alceml_cpu_mask)
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    else:
        ret = rpc_client.bdev_alceml_create(alceml_name, nvme.nvme_bdev, alceml_id, pba_init_mode=pba_init_mode)

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': nvme.size,
        'status': JMDevice.STATUS_ONLINE,
        'alceml_bdev': alceml_name,
        'nvme_bdev': nvme.nvme_bdev,
        'jm_bdev': jm_bdev
    })


def _create_storage_device_stack(rpc_client, nvme, snode, after_restart):
    test_name = f"{nvme.nvme_bdev}_test"
    ret = rpc_client.bdev_passtest_create(test_name, nvme.nvme_bdev)
    if not ret:
        logger.error(f"Failed to create passtest bdev {test_name}")
        return False
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    logger.info(f"adding {alceml_name}")
    pba_init_mode = 3
    if after_restart:
        pba_init_mode = 2

    if snode.alceml_cpu_cores:
        alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
        ret = rpc_client.bdev_alceml_create(alceml_name, test_name, alceml_id, pba_init_mode=pba_init_mode,
                                            alceml_cpu_mask=alceml_cpu_mask)
        snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
    else:
        ret = rpc_client.bdev_alceml_create(alceml_name, test_name, alceml_id, pba_init_mode=pba_init_mode)


    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    # add pass through
    pt_name = f"{alceml_name}_PT"
    ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_name)
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
            ret = rpc_client.transport_list()
            found = False
            if ret:
                for ty in ret:
                    if ty['trtype'] == tr_type:
                        found = True
            if found is False:
                ret = rpc_client.transport_create(tr_type)
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, tr_type, iface.ip4_address, "4420")
            IP = iface.ip4_address
            break
    logger.info(f"add {pt_name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name)
    if not ret:
        logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
        return False

    nvme.testing_bdev = test_name
    nvme.alceml_bdev = alceml_name
    nvme.pt_bdev = pt_name
    nvme.nvmf_nqn = subsystem_nqn
    nvme.nvmf_ip = IP
    nvme.nvmf_port = 4420
    nvme.io_error = False
    nvme.status = NVMeDevice.STATUS_ONLINE
    return nvme


def _create_device_partitions(rpc_client, nvme, snode):
    nbd_device = rpc_client.nbd_start_disk(nvme.nvme_bdev)
    time.sleep(3)
    if not nbd_device:
        logger.error(f"Failed to start nbd dev")
        return False
    snode_api = SNodeClient(snode.api_endpoint)
    result, error = snode_api.make_gpt_partitions(
        nbd_device, snode.jm_percent, snode.num_partitions_per_dev)
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
    for index, nvme in enumerate(devices):
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            continue

        # look for partitions
        partitioned_devices = _search_for_partitions(rpc_client, nvme)
        logger.debug("partitioned_devices")
        logger.debug(partitioned_devices)
        if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
            logger.info("Partitioned devices found")
        else:
            logger.info(f"Creating partitions for {nvme.nvme_bdev}")
            _create_device_partitions(rpc_client, nvme, snode)
            partitioned_devices = _search_for_partitions(rpc_client, nvme)
            if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
                logger.info("Device partitions created")
            else:
                logger.error("Failed to create partitions")
                return False

        jm_devices.append(partitioned_devices.pop(0))

        for dev in partitioned_devices:
            new_device = _create_storage_device_stack(rpc_client, dev, snode, after_restart=False)
            if not new_device:
                logger.error("failed to create dev stack")
                return False
            new_device.cluster_device_order = dev_order
            dev_order += 1
            new_devices.append(new_device)
            device_events.device_create(new_device)

    snode.nvme_devices = new_devices

    if jm_devices:
        jm_nvme_bdevs = [dev.nvme_bdev for dev in jm_devices]
        jm_device = _create_jm_stack_on_raid(rpc_client, jm_nvme_bdevs, snode, after_restart=False)
        if not jm_device:
            logger.error(f"Failed to create JM device")
            return False
        snode.jm_device = jm_device

    return True


def _prepare_cluster_devices_jm_on_dev(snode, devices):
    db_controller = DBController()

    jm_device = devices[0]
    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    for index, nvme in enumerate(devices):
        nvme.cluster_device_order = dev_order
        dev_order += 1
        if nvme.size < jm_device.size:
            jm_device = nvme
        device_events.device_create(nvme)
    jm_device.status = NVMeDevice.STATUS_JM

    rpc_client = RPCClient(snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)

    new_devices = []
    for index, nvme in enumerate(devices):
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                               NVMeDevice.STATUS_JM, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is not online or unavailable: {nvme.get_id()}, status: {nvme.status}")
            continue

        if nvme.status == NVMeDevice.STATUS_JM:
            jm_device = _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart=False)
            if not jm_device:
                logger.error(f"Failed to create JM device")
                return False
            snode.jm_device = jm_device
        else:
            new_device = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=False)
            if not new_device:
                logger.error("failed to create dev stack")
                return False
            new_device.cluster_device_order = dev_order
            dev_order += 1
            new_devices.append(new_device)
            device_events.device_create(new_device)

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_on_restart(snode):
    db_controller = DBController()

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    for index, nvme in enumerate(snode.nvme_devices):
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            continue

        dev = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=True)
        if not dev:
            logger.error(f"Failed to create dev stack {nvme.get_id()}")
            return False
        device_events.device_restarted(dev)

    # prepare JM device
    jm_device = snode.jm_device
    if jm_device.jm_nvme_bdev_list:
        ret = _create_jm_stack_on_raid(rpc_client, jm_device.jm_nvme_bdev_list, snode, after_restart=False)
        if not ret:
            logger.error(f"Failed to create JM device")
            return False
    else:

        if snode.alceml_cpu_cores:
            alceml_cpu_mask = utils.decimal_to_hex_power_of_2(snode.alceml_cpu_cores[snode.alceml_cpu_index])
            ret = rpc_client.bdev_alceml_create(jm_device.alceml_bdev, jm_device.nvme_bdev, jm_device.get_id(),
                                            pba_init_mode=2, alceml_cpu_mask=alceml_cpu_mask)
            snode.alceml_cpu_index = (snode.alceml_cpu_index + 1) % len(snode.alceml_cpu_cores)
        else:
            ret = rpc_client.bdev_alceml_create(jm_device.alceml_bdev, jm_device.nvme_bdev, jm_device.get_id(),
                                            pba_init_mode=2)
        if not ret:
            logger.error(f"Failed to create alceml bdev: {jm_device.alceml_bdev}")
            return False

        jm_bdev = f"jm_{snode.get_id()}"
        ret = rpc_client.bdev_jm_create(jm_bdev, jm_device.alceml_bdev, jm_cpu_mask=snode.jm_cpu_mask)
        if not ret:
            logger.error(f"Failed to create {jm_bdev}")
            return False

    return True


def _connect_to_remote_devs(this_node):
    db_controller = DBController()

    rpc_client = RPCClient(
        this_node.mgmt_ip, this_node.rpc_port,
        this_node.rpc_username, this_node.rpc_password)

    remote_devices = []
    # connect to remote devs
    snodes = db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id)
    for node_index, node in enumerate(snodes):
        if node.get_id() == this_node.get_id() or node.status == node.STATUS_OFFLINE:
            continue
        for index, dev in enumerate(node.nvme_devices):
            if dev.status != NVMeDevice.STATUS_ONLINE:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue
            name = f"remote_{dev.alceml_bdev}"
            logger.info(f"Connecting to {name}")
            ret = rpc_client.bdev_nvme_attach_controller_tcp(name, dev.nvmf_nqn, dev.nvmf_ip, dev.nvmf_port)
            if not ret:
                logger.error(f"Failed to connect to device: {dev.get_id()}")
                continue
            dev.remote_bdev = f"{name}n1"
            remote_devices.append(dev)
    return remote_devices


def add_node(cluster_id, node_ip, iface_name, data_nics_list,
             max_lvol, max_snap, max_prov, spdk_image=None, spdk_debug=False,
             small_bufsize=0, large_bufsize=0,
             num_partitions_per_dev=0, jm_percent=0, number_of_devices=0):
    db_controller = DBController()
    kv_store = db_controller.kv_store

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    logger.info(f"Adding Storage node: {node_ip}")
    timeout = 60
    if spdk_image:
        timeout = 5*60
    snode_api = SNodeClient(node_ip, timeout=timeout)
    node_info, _ = snode_api.info()
    logger.info(f"Node found: {node_info['hostname']}")
    if "cluster_id" in node_info and node_info['cluster_id']:
        if node_info['cluster_id'] != cluster_id:
            logger.error(f"This node is part of another cluster: {node_info['cluster_id']}")
            return False

    cloud_instance = node_info['cloud_instance']
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
    alceml_cpu_index = 0
    distrib_cpu_mask = ""

    poller_cpu_cores = []
    if cpu_count < 8:
        mask = (1 << (cpu_count - 1)) - 1
        mask <<= 1
        spdk_cpu_mask = f'0x{mask:X}'
    else:
        app_thread_core, jm_cpu_core, poller_cpu_cores, alceml_cpu_cores, distrib_cpu_cores = \
            utils.calculate_core_allocation(cpu_count)
        spdk_cores = app_thread_core + jm_cpu_core + poller_cpu_cores + alceml_cpu_cores + distrib_cpu_cores

        pollers_mask = utils.generate_mask(poller_cpu_cores)
        app_thread_mask = utils.generate_mask(app_thread_core)
        spdk_cpu_mask = utils.generate_mask(spdk_cores)
        jm_cpu_mask = utils.generate_mask(jm_cpu_core)
        distrib_cpu_mask = utils.generate_mask(distrib_cpu_cores)

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

    number_of_split = num_partitions_per_dev if num_partitions_per_dev else num_partitions_per_dev + 1
    number_of_alceml_devices = number_of_devices * number_of_split
    small_pool_count, large_pool_count = utils.calculate_pool_count(
        number_of_alceml_devices, max_lvol, max_snap, cpu_count, len(poller_cpu_cores) or cpu_count)

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(small_pool_count, large_pool_count, max_lvol, max_snap, cpu_count)

    # Calculate minimum sys memory
    minimum_sys_memory = utils.calculate_minimum_sys_memory(max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
    else:
        logger.error(f"Cannot get memory info from the instance.. Exiting")
        return False

    satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
                                                      minimum_sys_memory,
                                                      int(memory_details['free']),
                                                      int(memory_details['huge_total']))
    if not satisfied:
        logger.error(f"Not enough memory for the provided max_lvo: {max_lvol}, max_snap: {max_snap}, max_prov: {max_prov}.. Exiting")
        return False

    logger.info("Joining docker swarm...")
    cluster_docker = utils.get_docker_client(cluster_id)
    cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
    results, err = snode_api.join_swarm(
        cluster_ip=cluster_ip,
        join_token=cluster_docker.swarm.attrs['JoinTokens']['Worker'],
        db_connection=cluster.db_connection,
        cluster_id=cluster_id)

    if not results:
        logger.error(f"Failed to Join docker swarm: {err}")
        return False

    logger.info("Deploying SPDK")
    results, err = snode_api.spdk_process_start(spdk_cpu_mask, spdk_mem, spdk_image, spdk_debug, cluster_ip)
    time.sleep(10)
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
    rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()
    BASE_NQN = cluster.nqn.split(":")[0]
    subsystem_nqn = f"{BASE_NQN}:{hostname}"
    # creating storage node object
    snode = StorageNode()
    snode.uuid = str(uuid.uuid4())
    snode.status = StorageNode.STATUS_IN_CREATION
    snode.baseboard_sn = node_info['system_id']
    snode.system_uuid = node_info['system_id']

    snode.cloud_instance_id = cloud_instance['id']
    snode.cloud_instance_type = cloud_instance['type']
    snode.cloud_instance_public_ip = cloud_instance['public_ip']

    snode.hostname = hostname
    snode.host_nqn = subsystem_nqn
    snode.subsystem = subsystem_nqn
    snode.data_nics = data_nics
    snode.mgmt_ip = node_info['network_interface'][iface_name]['ip']
    snode.rpc_port = constants.RPC_HTTP_PROXY_PORT
    snode.rpc_username = rpc_user
    snode.rpc_password = rpc_pass
    snode.cluster_id = cluster_id
    snode.api_endpoint = node_ip
    snode.host_secret = utils.generate_string(20)
    snode.ctrl_secret = utils.generate_string(20)

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
    snode.alceml_cpu_cores = alceml_cpu_cores
    snode.distrib_cpu_mask = distrib_cpu_mask
    snode.poller_cpu_cores = poller_cpu_cores or []

    snode.iobuf_small_pool_count = small_pool_count or 0
    snode.iobuf_large_pool_count = large_pool_count or 0
    snode.iobuf_small_bufsize = small_bufsize or 0
    snode.iobuf_large_bufsize = large_bufsize or 0

    snode.num_partitions_per_dev = num_partitions_per_dev
    snode.jm_percent = jm_percent

    snode.write_to_db(kv_store)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False

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

    # get new node info after starting spdk
    node_info, _ = snode_api.info()

    # discover devices
    nvme_devs = addNvmeDevices(cluster, rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    # prepare devices
    if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:
        ret = _prepare_cluster_devices_jm_on_dev(snode, nvme_devs)
    else:
        ret = _prepare_cluster_devices_partitions(snode, nvme_devs)
    if not ret:
        logger.error("Failed to prepare cluster devices")
        return False

    logger.info("Connecting to remote devices")
    remote_devices = _connect_to_remote_devs(snode)
    snode.remote_devices = remote_devices

    logger.info("Setting node status to Active")
    snode.status = StorageNode.STATUS_ONLINE
    snode.write_to_db(kv_store)

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the new devices")
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Connecting to node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        count = 0
        for dev in snode.nvme_devices:
            if dev.status != NVMeDevice.STATUS_ONLINE:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue
            name = f"remote_{dev.alceml_bdev}"
            ret = rpc_client.bdev_nvme_attach_controller_tcp(name, dev.nvmf_nqn, dev.nvmf_ip, dev.nvmf_port)
            if not ret:
                logger.error(f"Failed to connect to device: {name}")
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
            count += 1
        node.write_to_db(kv_store)
        logger.info(f"connected to devices count: {count}")
        time.sleep(3)

    logger.info("Sending cluster map")
    ret = distr_controller.send_cluster_map_to_node(snode)
    if not ret:
        return False, "Failed to send cluster map"
    ret = distr_controller.send_cluster_map_add_node(snode)
    if not ret:
        return False, "Failed to send cluster map add node"
    time.sleep(3)

    logger.info("Sending cluster event updates")
    distr_controller.send_node_status_event(snode, StorageNode.STATUS_ONLINE)

    for dev in snode.nvme_devices:
        distr_controller.send_dev_status_event(dev, NVMeDevice.STATUS_ONLINE)

    storage_events.snode_add(snode)
    logger.info("Done")
    return "Success"


def delete_storage_node(node_id):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if snode.status != StorageNode.STATUS_REMOVED:
        logger.error(f"Node must be in removed status")
        return False

    snode.remove(db_controller.kv_store)

    for lvol in db_controller.get_lvols(snode.cluster_id):
        logger.info(f"Sending cluster map to LVol: {lvol.get_id()}")
        lvol_controller.send_cluster_map(lvol.get_id())

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

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not remove storage node")
        if force_remove is False:
            return False

    if snode.lvols:
        if force_migrate:
            for lvol_id in snode.lvols:
                lvol_controller.migrate(lvol_id)
        elif force_remove:
            for lvol_id in snode.lvols:
                lvol_controller.delete_lvol(lvol_id, True)
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
            if dev.status == NVMeDevice.STATUS_JM:
                continue
            if dev.status == 'online':
                distr_controller.disconnect_device(dev)
            old_status = dev.status
            dev.status = NVMeDevice.STATUS_FAILED
            distr_controller.send_dev_status_event(dev, NVMeDevice.STATUS_FAILED)
            device_events.device_status_change(dev, NVMeDevice.STATUS_FAILED, old_status)

    logger.info("Removing storage node")

    logger.debug("Leaving swarm...")
    try:
        node_docker = docker.DockerClient(base_url=f"tcp://{snode.mgmt_ip}:2375", version="auto")
        cluster_docker = utils.get_docker_client(snode.cluster_id)
        cluster_docker.nodes.get(node_docker.info()["Swarm"]["NodeID"]).remove(force=True)
    except:
        pass

    try:
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

    old_status = snode.status
    snode.status = StorageNode.STATUS_REMOVED
    snode.write_to_db(db_controller.kv_store)
    logger.info("Sending node event update")
    distr_controller.send_node_status_event(snode, snode.status)
    storage_events.snode_status_change(snode, StorageNode.STATUS_REMOVED, old_status)
    logger.info("done")


def restart_storage_node(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None,
        set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0, number_of_devices=0, force=False):

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
        return False

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id:
        logger.error(f"Restart task found: {task_id}, can not restart storage node")
        if force is False:
            return False

    logger.info("Setting node state to restarting")
    old_status = snode.status
    snode.status = StorageNode.STATUS_RESTARTING
    snode.write_to_db(kv_store)
    logger.info("Sending node event update")
    distr_controller.send_node_status_event(snode, snode.status)
    storage_events.snode_status_change(snode, snode.status, old_status)

    logger.info(f"Restarting Storage node: {snode.mgmt_ip}")

    snode_api = SNodeClient(snode.api_endpoint)
    node_info, _ = snode_api.info()
    logger.debug(f"Node info: {node_info}")

    logger.info("Restarting SPDK")

    img = snode.spdk_image
    if max_lvol:
        snode.max_lvol = max_lvol
    if max_snap:
        snode.max_snap = max_snap
    if max_prov:
        snode.max_prov = max_prov
    if spdk_image:
        img = spdk_image
        snode.spdk_image = img

    # Calculate pool count
    if snode.node.cloud_instance_type:
        supported_type, storage_devices, device_size = utils.get_total_size_per_instance_type(snode.node.cloud_instance_type)
        if not supported_type:
            logger.warning(f"Unsupported instance-type {snode.node.cloud_instance_type} for deployment")
            if not number_of_devices:
                if not snode.number_of_devices:
                    logger.error(f"Unsupported instance-type {snode.node.cloud_instance_type} "
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
        number_of_alceml_devices, snode.max_lvol, snode.max_snap, snode.cpu, len(snode.poller_cpu_cores) or snode.cpu)

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(small_pool_count, large_pool_count, snode.max_lvol, snode.max_snap, snode.cpu)

    # Calculate minimum sys memory
    minimum_sys_memory = utils.calculate_minimum_sys_memory(snode.max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
    else:
        logger.error(f"Cannot get memory info from the instance.. Exiting")

    satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
                                                      minimum_sys_memory,
                                                      int(memory_details['free']),
                                                      int(memory_details['huge_total']))
    if not satisfied:
        logger.error(f"Not enough memory for the provided max_lvo: {snode.max_lvol}, max_snap: {snode.max_snap}, max_prov: {utils.humanbytes(snode.max_prov)}.. Exiting")


    spdk_debug = snode.spdk_debug
    if set_spdk_debug:
        spdk_debug = spdk_debug
        snode.spdk_debug = spdk_debug

    cluster_docker = utils.get_docker_client(snode.cluster_id)
    cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
    results, err = snode_api.spdk_process_start(snode.spdk_cpu_mask, spdk_mem, img, spdk_debug, cluster_ip)

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False
    time.sleep(3)


    if small_bufsize:
        snode.iobuf_small_bufsize = small_bufsize
    if large_bufsize:
        snode.iobuf_large_bufsize = large_bufsize

    snode.write_to_db(db_controller.kv_store)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password,
        timeout=10 * 60, retry=5)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False

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

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    node_info, _ = snode_api.info()
    nvme_devs = addNvmeDevices(cluster, rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    logger.info(f"Devices found: {len(nvme_devs)}")
    logger.debug(nvme_devs)

    logger.info(f"Devices in db: {len(snode.nvme_devices)}")
    logger.debug(snode.nvme_devices)

    new_devices = []
    active_devices = []
    known_devices_sn = []
    devices_sn = [d.serial_number for d in nvme_devs]
    for db_dev in snode.nvme_devices:
        known_devices_sn.append(db_dev.serial_number)
        if db_dev.serial_number in devices_sn:
            logger.info(f"Device found: {db_dev.get_id()}, status {db_dev.status}")
            if db_dev.status != NVMeDevice.STATUS_JM:
                db_dev.status = NVMeDevice.STATUS_ONLINE
            active_devices.append(db_dev)
        else:
            logger.info(f"Device not found: {db_dev.get_id()}")
            db_dev.status = NVMeDevice.STATUS_REMOVED
            distr_controller.send_dev_status_event(db_dev, db_dev.status)

    # todo: handle new devices
    # for dev in nvme_devs:
    #     if dev.serial_number not in known_devices_sn:
    #         logger.info(f"New device found: {dev.get_id()}")
    #         dev.status = NVMeDevice.STATUS_NEW
    #         new_devices.append(dev)
    #         snode.nvme_devices.append(dev)

    # dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    # for index, nvme in enumerate(new_devices):
    #     nvme.cluster_device_order = dev_order
    #     dev_order += 1

    # prepare devices
    ret = _prepare_cluster_devices_on_restart(snode)
    if not ret:
        logger.error("Failed to prepare cluster devices")
        return False

    logger.info("Connecting to remote devices")
    remote_devices = _connect_to_remote_devs(snode)
    snode.remote_devices = remote_devices

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the node devices")
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Connecting to node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        count = 0
        for dev in snode.nvme_devices:
            if dev.status != 'online':
                continue
            name = f"remote_{dev.alceml_bdev}"
            ret = rpc_client.bdev_nvme_controller_list(name)
            if ret:
                logger.debug(f"controller found, removing")
                rpc_client.bdev_nvme_detach_controller(name)
                time.sleep(1)
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
            count += 1
        node.write_to_db(kv_store)
        logger.info(f"connected to devices count: {count}")
        time.sleep(3)

    logger.info("Setting node status to Online")
    old_status = snode.status
    snode.status = StorageNode.STATUS_ONLINE
    snode.write_to_db(kv_store)
    storage_events.snode_status_change(snode, snode.status, old_status)

    logger.info("Sending node event update")
    distr_controller.send_node_status_event(snode, StorageNode.STATUS_ONLINE)

    logger.info("Sending devices event updates")
    logger.info("Starting migration tasks")
    for dev in snode.nvme_devices:
        if dev.status != NVMeDevice.STATUS_ONLINE:
            logger.info(f"Device is not online: {dev.get_id()}, status: {dev.status}")
            continue

        distr_controller.send_dev_status_event(dev, NVMeDevice.STATUS_ONLINE)
        tasks_controller.add_device_mig_task(dev.get_id())

    # logger.info("Sending cluster map to current node")
    # ret = distr_controller.send_cluster_map_to_node(snode)
    # if not ret:
    #     return False, "Failed to send cluster map"
    # time.sleep(3)

    for lvol_id in snode.lvols:
        lvol = lvol_controller.recreate_lvol(lvol_id, snode)
        if not lvol:
            logger.error(f"Failed to create LVol: {lvol_id}")
            return False
        lvol.status = lvol.STATUS_ONLINE
        lvol.io_error = False
        lvol.write_to_db(db_controller.kv_store)

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

    for node in nodes:
        logger.debug(node)
        logger.debug("*" * 20)
        total_devices = len(node.nvme_devices)
        online_devices = 0
        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1
        data.append({
            "UUID": node.uuid,
            "Hostname": node.hostname,
            "Management IP": node.mgmt_ip,
            "Devices": f"{total_devices}/{online_devices}",
            "LVols": f"{len(node.lvols)}",
            "Status": node.status,
            "Health": node.health_check,

            "Cloud ID": node.cloud_instance_id,
            "Cloud Type": node.snode.node.cloud_instance_type,
            "Ext IP": node.cloud_instance_public_ip,

        })

    if not data:
        return output

    if is_json:
        output = json.dumps(data, indent=2)
    else:
        output = utils.print_table(data)
    return output


def list_storage_devices(kv_store, node_id, sort, is_json):
    db_controller = DBController(kv_store)
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
    old_status = snode.status
    snode.status = StorageNode.STATUS_IN_SHUTDOWN
    snode.write_to_db(db_controller.kv_store)
    storage_events.snode_status_change(snode, snode.status, old_status)

    logger.debug("Removing LVols")
    for lvol_id in snode.lvols:
        logger.debug(lvol_id)
        lvol_controller.delete_lvol_from_node(lvol_id, snode.get_id(), clear_data=False)

    for dev in snode.nvme_devices:
        if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY]:
            device_controller.device_set_unavailable(dev.get_id())
    distr_controller.send_node_status_event(snode, StorageNode.STATUS_IN_SHUTDOWN)

    # shutdown node
    # make other nodes disconnect from this node
    logger.info("disconnect all other nodes connections to this node")
    for dev in snode.nvme_devices:
        distr_controller.disconnect_device(dev)

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    # delete jm
    logger.info("Removing JM")
    rpc_client.bdev_jm_delete(f"jm_{snode.get_id()}")

    logger.info("Stopping SPDK")
    snode_api = SNodeClient(snode.api_endpoint)
    results, err = snode_api.spdk_process_kill()

    distr_controller.send_node_status_event(snode, StorageNode.STATUS_OFFLINE)

    logger.info("Setting node status to offline")
    snode = db_controller.get_storage_node_by_id(node_id)
    old_status = snode.status
    snode.status = StorageNode.STATUS_OFFLINE
    snode.write_to_db(db_controller.kv_store)

    # send event log
    storage_events.snode_status_change(snode, snode.status, old_status)
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
    distr_controller.send_node_status_event(snode, StorageNode.STATUS_SUSPENDED)
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_ONLINE:
            device_controller.device_set_unavailable(dev.get_id())

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    logger.debug("Setting LVols to offline")
    for lvol_id in snode.lvols:
        logger.debug(lvol_id)
        lvol = db_controller.get_lvol_by_id(lvol_id)
        if lvol:
            ret = rpc_client.nvmf_subsystem_remove_ns(lvol.nqn, 1)
            lvol.status = lvol.STATUS_OFFLINE
            lvol.write_to_db(db_controller.kv_store)

    logger.info("Setting node status to suspended")
    snode = db_controller.get_storage_node_by_id(node_id)
    old_status = snode.status
    snode.status = StorageNode.STATUS_SUSPENDED
    snode.write_to_db(db_controller.kv_store)

    storage_events.snode_status_change(snode, snode.status, old_status)
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

    logger.info("Sending cluster event updates")
    distr_controller.send_node_status_event(snode, StorageNode.STATUS_ONLINE)

    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_UNAVAILABLE:
            device_controller.device_set_online(dev.get_id())

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    logger.debug("Setting LVols to online")
    for lvol_id in snode.lvols:
        logger.debug(lvol_id)
        lvol = db_controller.get_lvol_by_id(lvol_id)
        if lvol:
            ret = rpc_client.nvmf_subsystem_add_ns(lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid)
            lvol.status = lvol.STATUS_ONLINE
            lvol.write_to_db(db_controller.kv_store)

    logger.info("Setting node status to online")
    snode = db_controller.get_storage_node_by_id(node_id)
    old_status = snode.status
    snode.status = StorageNode.STATUS_ONLINE
    snode.write_to_db(db_controller.kv_store)

    storage_events.snode_status_change(snode, snode.status, old_status)
    logger.info("Done")
    return True


# not used in AWS, must run on bare-metal servers
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
        logger.error("Device not found")
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


# Deprecated
def add_storage_device(dev_name, node_id, cluster_id):
    db_controller = DBController()
    kv_store = db_controller.kv_store
    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("Node is not part of the cluster: %s", node_id)
        exit(1)

    for node_nvme_device in snode.nvme_devices:
        if node_nvme_device.device_name == dev_name:
            logger.error("Device already added to the cluster")
            exit(1)

    nvme_devs = _get_nvme_list(cluster)
    for device in nvme_devs:
        if device.device_name == dev_name:
            nvme_device = device
            break
    else:
        logger.error("Device not found: %s", dev_name)
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
    if program_fail_count < constants.NVME_PROGRAM_FAIL_COUNT:
        logger.info("program_fail_count: %s is below %s on drive: %s",
                    program_fail_count, constants.NVME_PROGRAM_FAIL_COUNT, dev_name)
        exit(1)
    if erase_fail_count < constants.NVME_ERASE_FAIL_COUNT:
        logger.info("erase_fail_count: %s is below %s on drive: %s",
                    erase_fail_count, constants.NVME_ERASE_FAIL_COUNT, dev_name)
        exit(1)
    if crc_error_count < constants.NVME_CRC_ERROR_COUNT:
        logger.info("crc_error_count: %s is below %s on drive: %s",
                    crc_error_count, constants.NVME_CRC_ERROR_COUNT, dev_name)
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
    ret = rpc_client.bdev_nvme_controller_attach("nvme_ultr21a_%s" % nvme_device.sequential_number,
                                                 nvme_device.pcie_address)
    logger.debug(ret)

    logger.debug("controllers list")
    ret = rpc_client.bdev_nvme_controller_list()
    logger.debug(ret)

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

    node_docker = docker.DockerClient(base_url=f"tcp://{dev_ip}:2375", version="auto", timeout=60 * 5)
    # create the api container
    nodes = node_docker.containers.list(all=True)
    for node in nodes:
        if node.attrs["Name"] == "/SNodeAPI":
            logger.info("SNodeAPI container found, removing...")
            node.stop()
            node.remove(force=True)
            time.sleep(2)

    logger.info("Creating SNodeAPI container")
    container = node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "python simplyblock_web/node_webapp.py storage_node",
        detach=True,
        privileged=True,
        name="SNodeAPI",
        network_mode="host",
        volumes=[
            '/etc/foundationdb:/etc/foundationdb',
            '/var/tmp:/var/tmp',
            '/var/run:/var/run',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys'],
        restart_policy={"Name": "always"},
        environment=[
            f"DOCKER_IP={dev_ip}"
        ]
    )
    logger.info("Pulling SPDK images")
    logger.debug(constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE)
    logger.debug(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    node_docker.images.pull(constants.SIMPLY_BLOCK_SPDK_CORE_IMAGE)
    node_docker.images.pull(constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    return f"{dev_ip}:5000"


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
                    since = str(datetime.datetime.now()-start).split('.')[0]
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
    return json.dumps(data, indent=2)


def set_node_status(node_id, status):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if snode.status != status:
        old_status = snode.status
        snode.status = status
        snode.updated_at = str(datetime.datetime.now())
        snode.write_to_db(db_controller.kv_store)
        storage_events.snode_status_change(snode, snode.status, old_status, caused_by="monitor")
        distr_controller.send_node_status_event(snode, status)

    if snode.status == StorageNode.STATUS_ONLINE:
        logger.info("Connecting to remote devices")
        _connect_to_remote_devs(snode)

    return True
