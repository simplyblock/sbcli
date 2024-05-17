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
    device_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core import shell_utils
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice
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
    sequential_number = 0
    devices = []
    ret = rpc_client.bdev_nvme_controller_list()
    if ret:
        ctr_map = {i["ctrlrs"][0]['trid']['traddr']: i["name"] for i in ret}
    else:
        ctr_map = {}

    for index, pcie in enumerate(devs):

        if pcie in ctr_map:
            nvme_bdev = ctr_map[pcie] + "n1"
        else:
            name = "nvme_%s" % index
            ret, err = rpc_client.bdev_nvme_controller_attach(name, pcie)
            time.sleep(2)
            nvme_bdev = f"{name}n1"

        ret = rpc_client.get_bdevs(nvme_bdev)
        if ret:
            nvme_dict = ret[0]
            nvme_driver_data = nvme_dict['driver_specific']['nvme'][0]
            model_number = nvme_driver_data['ctrlr_data']['model_number']

            size = nvme_dict['block_size'] * nvme_dict['num_blocks']
            device_partitions_count = int(size / (cluster.blk_size * cluster.page_size_in_blocks))
            devices.append(
                NVMeDevice({
                    'uuid': str(uuid.uuid4()),
                    'device_name': nvme_dict['name'],
                    'sequential_number': sequential_number,
                    'partitions_count': device_partitions_count,
                    'capacity': size,
                    'size': size,
                    'pcie_address': nvme_driver_data['pci_address'],
                    'model_id': model_number,
                    'serial_number': nvme_driver_data['ctrlr_data']['serial_number'],
                    'nvme_bdev': nvme_bdev,
                    'alloc_bdev': nvme_bdev,
                    'node_id': snode.get_id(),
                    'cluster_id': snode.cluster_id,

                    # 'nvmf_nqn': subsystem_nqn,
                    # 'nvmf_ip': IP,
                    # 'nvmf_port': 4420,

                    'status': 'online'
                }))
            sequential_number += device_partitions_count
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


def get_next_cluster_device_order(db_controller):
    max_order = 0
    found = False
    for node in db_controller.get_storage_nodes():
        for dev in node.nvme_devices:
            found = True
            max_order = max(max_order, dev.cluster_device_order)
    if found:
        return max_order + 1
    return 0


def _prepare_cluster_devices(snode, after_restart=False):
    db_controller = DBController()

    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password)

    for index, nvme in enumerate(snode.nvme_devices):
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                               NVMeDevice.STATUS_JM, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is not online or unavailable: {nvme.get_id()}, status: {nvme.status}")
            continue

        test_name = f"{nvme.nvme_bdev}_test"
        # create testing bdev
        ret = rpc_client.bdev_passtest_create(test_name, nvme.nvme_bdev)
        if not ret:
            logger.error(f"Failed to create bdev: {test_name}")
            return False
        alceml_id = nvme.get_id()
        alceml_name = device_controller.get_alceml_name(alceml_id)
        logger.info(f"adding {alceml_name}")
        pba_init_mode = 3
        if after_restart:
            pba_init_mode = 2
        ret = rpc_client.bdev_alceml_create(alceml_name, test_name, alceml_id, pba_init_mode=pba_init_mode)
        if not ret:
            logger.error(f"Failed to create alceml bdev: {alceml_name}")
            return False

        # create jm
        if nvme.jm_bdev:
            ret = rpc_client.bdev_jm_create(nvme.jm_bdev, alceml_name)
            if not ret:
                logger.error(f"Failed to create JM bdev: {nvme.jm_bdev}")
                return False
            nvme.testing_bdev = test_name
            nvme.alceml_bdev = alceml_name
            nvme.io_error = True
            nvme.status = NVMeDevice.STATUS_JM
            continue

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
        old_status = nvme.status
        nvme.status = NVMeDevice.STATUS_ONLINE
        device_events.device_status_change(nvme, nvme.status, old_status)
        snode.write_to_db(db_controller.kv_store)

    return True


def _connect_to_remote_devs(this_node):
    db_controller = DBController()

    rpc_client = RPCClient(
        this_node.mgmt_ip, this_node.rpc_port,
        this_node.rpc_username, this_node.rpc_password)

    remote_devices = []
    # connect to remote devs
    snodes = db_controller.get_storage_nodes()
    for node_index, node in enumerate(snodes):
        if node.get_id() == this_node.get_id() or node.status == node.STATUS_OFFLINE:
            continue
        for index, dev in enumerate(node.nvme_devices):
            if dev.status != 'online':
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


def add_node(cluster_id, node_ip, iface_name, data_nics_list, spdk_cpu_mask,
             spdk_mem, dev_split=1, spdk_image=None, spdk_debug=False,
             small_pool_count=0, large_pool_count=0, small_bufsize=0, large_bufsize=0, jm_device_pcie=None):
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

    ec2_metadata = None
    if "ec2_metadata" in node_info and node_info['ec2_metadata']:
        ec2_metadata = node_info['ec2_metadata']
        """"
         "ec2_metadata": {
              "accountId": "565979732541",
              "architecture": "x86_64",
              "availabilityZone": "eu-west-1a",
              "billingProducts": [
                "bp-6fa54006"
              ],
              "devpayProductCodes": null,
              "imageId": "ami-08e592fbb0f535224",
              "instanceId": "i-0ba9e766df57bc62c",
              "instanceType": "m6id.large",
              "kernelId": null,
              "marketplaceProductCodes": null,
              "pendingTime": "2024-03-24T19:39:14Z",
              "privateIp": "172.31.23.236",
              "ramdiskId": null,
              "region": "eu-west-1",
              "version": "2017-09-30"
        }
        """""
        logger.debug(json.dumps(ec2_metadata,indent=2))
        logger.info(f"EC2 Instance found: {ec2_metadata['instanceId']}")
        logger.info(f"EC2 Instance type: {ec2_metadata['instanceType']}")
        logger.info(f"EC2 Instance privateIp: {ec2_metadata['privateIp']}")
        logger.info(f"EC2 Instance region: {ec2_metadata['region']}")

        for node in db_controller.get_storage_nodes():
            if node.ec2_instance_id and node.ec2_instance_id == ec2_metadata['instanceId']:
                logger.error(f"Node already exists, try remove it first: {ec2_metadata['instanceId']}")
                return False

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
        logger.info(f"Hugepages Total: {utils.humanbytes(memory_details['huge_total'])}")
        huge_free = memory_details['huge_free']
        logger.info(f"Hugepages Free: {utils.humanbytes(huge_free)}")
        if huge_free < 1 * 1024 * 1024:
            logger.warning(f"Free hugepages are less than 1G: {utils.humanbytes(huge_free)}")
        if not spdk_mem:
            spdk_mem = huge_free
            logger.info(f"Using the free hugepages for spdk memory: {utils.humanbytes(huge_free)}")

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

    if ec2_metadata:
        snode.ec2_metadata = ec2_metadata
        snode.ec2_instance_id = ec2_metadata['instanceId']

    if "ec2_public_ip" in node_info and node_info['ec2_public_ip']:
        snode.ec2_public_ip = node_info['ec2_public_ip']

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
    snode.spdk_mem = spdk_mem or 0
    snode.spdk_image = spdk_image or ""
    snode.spdk_debug = spdk_debug or 0
    snode.write_to_db(kv_store)

    snode.iobuf_small_pool_count = small_pool_count or 0
    snode.iobuf_large_pool_count = large_pool_count or 0
    snode.iobuf_small_bufsize = small_bufsize or 0
    snode.iobuf_large_bufsize = large_bufsize or 0

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

    # 2- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    # 3- set nvme bdev options
    ret = rpc_client.bdev_nvme_set_options()
    if not ret:
        logger.error("Failed to set nvme options")
        return False

    # get new node info after starting spdk
    node_info, _ = snode_api.info()
    # adding devices
    nvme_devs = addNvmeDevices(cluster, rpc_client, node_info['spdk_pcie_list'], snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    if dev_split > 1:
        # split devices
        new_devices = []
        for dev in nvme_devs:
            ret = rpc_client.bdev_split(dev.nvme_bdev, dev_split)
            for pt in ret:
                dev_dict = dev.get_clean_dict()
                dev_dict['uuid'] = str(uuid.uuid4())
                dev_dict['device_name'] = pt
                dev_dict['nvme_bdev'] = pt
                dev_dict['size'] = int(dev.size / dev_split)
                new_devices.append(NVMeDevice(dev_dict))
        snode.nvme_devices = new_devices
    else:
        snode.nvme_devices = nvme_devs

    jm_device = snode.nvme_devices[0]
    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller)
    for index, nvme in enumerate(snode.nvme_devices):
        nvme.cluster_device_order = dev_order
        dev_order += 1
        if jm_device_pcie:
            if nvme.pcie_address == jm_device_pcie:
                jm_device = nvme
        elif nvme.size < jm_device.size:
            jm_device = nvme
        device_events.device_create(nvme)

    # create jm
    logger.info(f"Using device for JM: {jm_device.get_id()}")
    jm_device.jm_bdev = f"jm_{snode.get_id()}"

    # save object
    snode.write_to_db(db_controller.kv_store)

    # prepare devices
    ret = _prepare_cluster_devices(snode)
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
    snodes = db_controller.get_storage_nodes()
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Connecting to node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        count = 0
        for dev in snode.nvme_devices:
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
    distr_controller.send_node_status_event(snode.get_id(), "online")

    for dev in snode.nvme_devices:
        distr_controller.send_dev_status_event(dev.cluster_device_order, "online")

    storage_events.snode_add(snode)
    logger.info("Done")
    return "Success"


# Deprecated
def add_storage_node(cluster_id, iface_name, data_nics):
    db_controller = DBController()
    kv_store = db_controller.kv_store

    cluster = db_controller.get_cluster_by_id(cluster_id)
    if not cluster:
        logger.error("Cluster not found: %s", cluster_id)
        return False

    logger.info("Add Storage node")

    hostname = utils.get_hostname()
    snode = db_controller.get_storage_node_by_hostname(hostname)
    if snode:
        logger.error("Node already exists, try remove it first.")
        exit(1)
    else:
        snode = StorageNode()
        snode.uuid = str(uuid.uuid4())

    mgmt_ip = _get_if_ip_address(iface_name)
    system_id = utils.get_system_id()

    BASE_NQN = cluster.nqn.split(":")[0]
    subsystem_nqn = f"{BASE_NQN}:{hostname}"

    if data_nics:
        data_nics = _get_data_nics(data_nics)
    else:
        data_nics = _get_data_nics([iface_name])

    rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()

    # creating storage node object
    snode.status = StorageNode.STATUS_IN_CREATION
    snode.baseboard_sn = utils.get_baseboard_sn()
    snode.system_uuid = system_id
    snode.hostname = hostname
    snode.host_nqn = subsystem_nqn
    snode.subsystem = subsystem_nqn
    snode.data_nics = data_nics
    snode.mgmt_ip = mgmt_ip
    snode.rpc_port = constants.RPC_HTTP_PROXY_PORT
    snode.rpc_username = rpc_user
    snode.rpc_password = rpc_pass
    snode.cluster_id = cluster_id
    snode.write_to_db(kv_store)

    # creating RPCClient instance
    rpc_client = RPCClient(
        snode.mgmt_ip,
        snode.rpc_port,
        snode.rpc_username,
        snode.rpc_password)

    logger.info("Getting nvme devices")
    devs = get_nvme_devices()
    logger.debug(devs)
    pcies = [d[0] for d in devs]
    nvme_devs = addNvmeDevices(cluster, rpc_client, pcies, snode)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")

    logger.debug(nvme_devs)
    snode.nvme_devices = nvme_devs

    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller)
    for index, nvme in enumerate(snode.nvme_devices):
        nvme.cluster_device_order = dev_order
        dev_order += 1
    snode.write_to_db(db_controller.kv_store)

    # prepare devices
    _prepare_cluster_devices(snode)

    logger.info("Connecting to remote devices")
    remote_devices = _connect_to_remote_devs(snode)
    snode.remote_devices = remote_devices

    logger.info("Setting node status to Active")
    snode.status = StorageNode.STATUS_ONLINE
    snode.write_to_db(kv_store)

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the new devices")
    snodes = db_controller.get_storage_nodes()
    for node_index, node in enumerate(snodes):
        if node.get_id() == snode.get_id():
            continue
        logger.info(f"Connecting to node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
        count = 0
        for dev in snode.nvme_devices:
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

    logger.info("Sending cluster map")
    ret = distr_controller.send_cluster_map_to_node(snode)
    if not ret:
        return False, "Failed to send cluster map"
    ret = distr_controller.send_cluster_map_add_node(snode)
    if not ret:
        return False, "Failed to send cluster map add node"
    time.sleep(3)

    logger.info("Sending cluster event updates")
    distr_controller.send_node_status_event(snode.get_id(), "online")

    for dev in snode.nvme_devices:
        distr_controller.send_dev_status_event(dev.cluster_device_order, "online")

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

    for lvol in db_controller.get_lvols():
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
            distr_controller.send_dev_status_event(dev.cluster_device_order, NVMeDevice.STATUS_FAILED)
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
        snode_api = SNodeClient(snode.api_endpoint)
        snode_api.spdk_process_kill()
        snode_api.leave_swarm()
    except Exception as e:
        logger.warning(f"Failed to remove SPDK process: {e}")

    old_status = snode.status
    snode.status = StorageNode.STATUS_REMOVED
    snode.write_to_db(db_controller.kv_store)
    logger.info("Sending node event update")
    distr_controller.send_node_status_event(snode.get_id(), snode.status)
    storage_events.snode_status_change(snode, StorageNode.STATUS_REMOVED, old_status)
    logger.info("done")


def restart_storage_node(
        node_id,
        spdk_cpu_mask=None,
        spdk_mem=None,
        spdk_image=None,
        set_spdk_debug=None,
        small_pool_count=0, large_pool_count=0,
        small_bufsize=0, large_bufsize=0):

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

    logger.info("Setting node state to restarting")
    old_status = snode.status
    snode.status = StorageNode.STATUS_RESTARTING
    snode.write_to_db(kv_store)
    logger.info("Sending node event update")
    distr_controller.send_node_status_event(snode.get_id(), snode.status)
    storage_events.snode_status_change(snode, snode.status, old_status)

    logger.info(f"Restarting Storage node: {snode.mgmt_ip}")

    snode_api = SNodeClient(snode.api_endpoint)
    node_info, _ = snode_api.info()
    logger.info(f"Node info: {node_info}")

    logger.info("Restarting SPDK")
    cpu = snode.spdk_cpu_mask
    if spdk_cpu_mask:
        cpu = spdk_cpu_mask
        snode.spdk_cpu_mask = cpu
    mem = snode.spdk_mem
    if spdk_mem:
        mem = spdk_mem
        snode.spdk_mem = mem
    img = snode.spdk_image
    if spdk_image:
        img = spdk_image
        snode.spdk_image = img
    spdk_debug = snode.spdk_debug
    if set_spdk_debug:
        spdk_debug = spdk_debug
        snode.spdk_debug = spdk_debug

    cluster_docker = utils.get_docker_client(snode.cluster_id)
    cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
    results, err = snode_api.spdk_process_start(cpu, mem, img, spdk_debug, cluster_ip)

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False
    time.sleep(3)

    if small_pool_count:
        snode.iobuf_small_pool_count = small_pool_count
    if large_pool_count:
        snode.iobuf_large_pool_count = large_pool_count
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

    # 2- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    # 3- set nvme bdev options
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
            distr_controller.send_dev_status_event(db_dev.cluster_device_order, "offline")

    for dev in nvme_devs:
        if dev.serial_number not in known_devices_sn:
            logger.info(f"New device found: {dev.get_id()}")
            dev.status = 'new'
            new_devices.append(dev)
            snode.nvme_devices.append(dev)

    dev_order = get_next_cluster_device_order(db_controller)
    for index, nvme in enumerate(new_devices):
        nvme.cluster_device_order = dev_order
        dev_order += 1

    # prepare devices
    ret = _prepare_cluster_devices(snode, after_restart=True)
    if not ret:
        logger.error("Failed to prepare cluster devices")
        return False

    logger.info("Connecting to remote devices")
    remote_devices = _connect_to_remote_devs(snode)
    snode.remote_devices = remote_devices

    # make other nodes connect to the new devices
    logger.info("Make other nodes connect to the node devices")
    snodes = db_controller.get_storage_nodes()
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
    distr_controller.send_node_status_event(snode.get_id(), NVMeDevice.STATUS_ONLINE)

    logger.info("Sending devices event updates")
    for dev in snode.nvme_devices:
        if dev.status != NVMeDevice.STATUS_ONLINE:
            logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
            continue
        distr_controller.send_dev_status_event(dev.cluster_device_order, NVMeDevice.STATUS_ONLINE)

    logger.info("Sending cluster map to current node")
    ret = distr_controller.send_cluster_map_to_node(snode)
    if not ret:
        return False, "Failed to send cluster map"
    time.sleep(3)

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


def list_storage_nodes(kv_store, is_json):
    db_controller = DBController(kv_store)
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
            # "Data NICs": "\n".join([d.if_name for d in node.data_nics]),
            "Status": node.status,
            "Health": node.health_check,

            "EC2 ID": node.ec2_instance_id,
            "EC2 Type": node.ec2_metadata['instanceType'] if node.ec2_metadata else "",
            "EC2 Ext IP": node.ec2_public_ip,

            # "Updated At": datetime.datetime.strptime(node.updated_at, "%Y-%m-%d %H:%M:%S.%f").strftime(
            #     "%H:%M:%S, %d/%m/%Y"),
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

    data = []
    for device in snode.nvme_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        data.append({
            "UUID": device.uuid,
            "Name": device.device_name,
            "Hostname": snode.hostname,
            "Size": utils.humanbytes(device.size),
            # "Sequential Number": device.sequential_number,
            # "Partitions Count": device.partitions_count,
            # "Model ID": device.model_id,
            "Serial Number": device.serial_number,
            "PCIe": device.pcie_address,
            "Status": device.status,
            "IO Err": device.io_error,
            "Health": device.health_check,

        })

    if sort and sort in ['node-seq', 'dev-seq', 'serial']:
        if sort == 'serial':
            sort_key = "Serial Number"
        elif sort == 'dev-seq':
            sort_key = "Sequential Number"
        elif sort == 'node-seq':
            # TODO: check this key
            sort_key = "Sequential Number"
        sorted_data = sorted(data, key=lambda d: d[sort_key])
        data = sorted_data

    if is_json:
        return json.dumps(data, indent=2)
    else:
        return utils.print_table(data)


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

    # cls = db_controller.get_clusters(id=snode.cluster_id)
    # snodes = db_controller.get_storage_nodes()
    # online_nodes = 0
    # for node in snodes:
    #     if node.status == node.STATUS_ONLINE:
    #         online_nodes += 1
    # if cls[0].ha_type == "ha" and online_nodes <= 3:
    #     logger.warning(f"Cluster mode is HA but online storage nodes are less than 3")
    #     if force is False:
    #         return False

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
    distr_controller.send_node_status_event(snode.get_id(), "in_shutdown")

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

    distr_controller.send_node_status_event(snode.get_id(), StorageNode.STATUS_OFFLINE)

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
        return False

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    snodes = db_controller.get_storage_nodes()
    online_nodes = 0
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes += 1
    if cluster.ha_type == "ha" and online_nodes <= 3 and cluster.status == cluster.STATUS_ACTIVE:
        logger.warning(f"Cluster mode is HA but online storage nodes are less than 3")
        if force is False:
            return False

    if cluster.ha_type == "ha" and cluster.status == cluster.STATUS_DEGRADED and force is False:
        logger.warning(f"Cluster status is degraded, use --force but this will suspend the cluster")
        return False

    logger.info("Suspending node")
    distr_controller.send_node_status_event(snode.get_id(), "suspended")
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

    logger.info("Resuming node")

    logger.info("Sending cluster event updates")
    distr_controller.send_node_status_event(snode.get_id(), "online")

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


def replace_node(kv_store, old_node_name, iface_name):
    return "Not implemented!"



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


def get_node_port_iostats(port_id, history=None):
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

    limit = 20
    if history and history > 1:
        limit = history
    data = db_controller.get_port_stats(nd.get_id(), port.get_id(), limit=limit)
    out = []

    for record in data:
        out.append({
            "Date": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(record.date)),
            "out_speed": utils.humanbytes(record.out_speed),
            "in_speed": utils.humanbytes(record.in_speed),
            "bytes_sent": utils.humanbytes(record.bytes_sent),
            "bytes_received": utils.humanbytes(record.bytes_received),
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
        restart_policy={"Name": "always"}
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


def update(node_id, key, value):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    if key in snode._attribute_map:
        setattr(snode, key, value)
        snode.write_to_db(db_controller.kv_store)
        return True
    else:
        logger.error("Key not found")
        return False


def get(node_id):
    db_controller = DBController()

    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error(f"Can not find storage node: {node_id}")
        return False

    data = snode.get_clean_dict()
    return json.dumps(data, indent=2)
