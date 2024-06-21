# coding=utf-8
import datetime
import logging as log

import docker

from simplyblock_core import utils, distr_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient

logger = log.getLogger()


def check_cluster(cluster_id):
    db_controller = DBController()
    st = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    data = []
    result = True
    for node in st:
        # check if node is online, unavailable, restarting
        ret = check_node(node.get_id(), with_devices=False)
        result &= ret
        print("*"*100)
        data.append({
            "Kind": "Node",
            "UUID": node.get_id(),
            "Status": "ok" if ret else "failed"
        })

        for device in node.nvme_devices:
            ret = check_device(device.get_id())
            result &= ret
            print("*" * 100)
            data.append({
                "Kind": "Device",
                "UUID": device.get_id(),
                "Status": "ok" if ret else "failed"
            })

        for lvol in db_controller.get_lvols(cluster_id):
            ret = check_lvol(lvol.get_id())
            result &= ret
            print("*" * 100)
            data.append({
                "Kind": "LVol",
                "UUID": lvol.get_id(),
                "Status": "ok" if ret else "failed"
            })
    print(utils.print_table(data))
    return result


def _check_node_docker_api(ip):
    try:
        node_docker = docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=3)
        ret = node_docker.info()
        if ret:
            logger.debug(ret)
            return True
    except Exception as e:
        logger.error(f"Failed to connect to node's docker: {e}")
    return False


def _check_node_rpc(rpc_ip, rpc_port, rpc_username, rpc_password):
    try:
        rpc_client = RPCClient(
            rpc_ip, rpc_port, rpc_username, rpc_password,
            timeout=10, retry=1)
        ret = rpc_client.get_version()
        if ret:
            logger.debug(f"SPDK version: {ret['version']}")
            return True
    except Exception as e:
        logger.debug(e)
    return False


def _check_node_api(ip):
    try:
        snode_api = SNodeClient(f"{ip}:5000", timeout=3, retry=1)
        logger.debug(f"Node API={ip}:5000")
        node_info, _ = snode_api.info()
        if node_info:
            logger.debug(node_info)
            return True
    except Exception as e:
        logger.debug(e)
    return False


def _check_node_ping(ip):
    res = utils.ping_host(ip)
    if res:
        return True
    else:
        return False


def check_node(node_id, with_devices=True):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if not snode:
        logger.error("node not found")
        return False

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    logger.info(f"Checking node {node_id}, status: {snode.status}")

    print("*" * 100)

    # passed = True

    # 1- check node ping
    ping_check = _check_node_ping(snode.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

    # 2- check node API
    node_api_check = _check_node_api(snode.mgmt_ip)
    logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

    # 3- check node RPC
    node_rpc_check = _check_node_rpc(
        snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password)
    logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

    # 4- docker API
    node_docker_check = _check_node_docker_api(snode.mgmt_ip)
    logger.info(f"Check: node docker API {snode.mgmt_ip}:2375 ... {node_docker_check}")

    is_node_online = ping_check and node_api_check and node_rpc_check and node_docker_check

    logger.info(f"Results : {is_node_online}")
    print("*" * 100)

    node_devices_check = True
    node_remote_devices_check = True

    if not node_rpc_check:
        logger.info("Skipping devices checks because RPC check failed")
    else:
        logger.info(f"Node device count: {len(snode.nvme_devices)}")
        for dev in snode.nvme_devices:
            ret = check_device(dev.get_id())
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                node_devices_check &= ret
            print("*" * 100)

        logger.info(f"Node remote device: {len(snode.remote_devices)}")

        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password,
            timeout=3, retry=1)
        for remote_device in snode.remote_devices:
            ret = rpc_client.get_bdevs(remote_device.remote_bdev)
            if ret:
                logger.info(f"Checking bdev: {remote_device.remote_bdev} ... ok")
            else:
                logger.info(f"Checking bdev: {remote_device.remote_bdev} ... not found")
            node_remote_devices_check &= bool(ret)

    return is_node_online and node_devices_check and node_remote_devices_check


def check_device(device_id):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error("node not found")
        return False

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Skipping ,device status is {device.status}")
        return True

    passed = True
    try:
        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password)

        bdevs_stack = [device.nvme_bdev, device.testing_bdev, device.alceml_bdev, device.pt_bdev]
        # if device.jm_bdev:
        #     bdevs_stack.append(device.jm_bdev)
        logger.info(f"Checking Device: {device_id}, status:{device.status}")
        problems = 0
        for bdev in bdevs_stack:
            if not bdev:
                continue
            ret = rpc_client.get_bdevs(bdev)
            if ret:
                logger.debug(f"Checking bdev: {bdev} ... ok")
            else:
                logger.error(f"Checking bdev: {bdev} ... not found")
                problems += 1
                passed = False
                # return False
        logger.info(f"Checking Device's BDevs ... ({(len(bdevs_stack)-problems)}/{len(bdevs_stack)})")

        ret = rpc_client.subsystem_list(device.nvmf_nqn)
        logger.debug(f"Checking subsystem: {device.nvmf_nqn}")
        if ret:
            logger.info(f"Checking subsystem ... ok")
        else:
            logger.info(f"Checking subsystem: ... not found")
            passed = False

        if device.status == NVMeDevice.STATUS_ONLINE:
            logger.info("Checking other node's connection to this device...")
            ret = check_remote_device(device_id)
            passed &= ret

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")
        passed = False

    return passed


def check_remote_device(device_id):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        logger.error("device not found")
        return False
    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error("node not found")
        return False

    result = True
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status == StorageNode.STATUS_ONLINE:
            if node.get_id() == snode.get_id():
                continue
            logger.info(f"Connecting to node: {node.get_id()}")
            rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password)
            name = f"remote_{device.alceml_bdev}n1"
            ret = rpc_client.get_bdevs(name)
            if ret:
                logger.info(f"Checking bdev: {device.alceml_bdev} ... ok")
            else:
                logger.info(f"Checking bdev: {device.alceml_bdev} ... not found")
                result = False

    return result


def check_lvol_on_node(lvol_id, node_id):

    db_controller = DBController()
    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    snode = db_controller.get_storage_node_by_id(node_id)
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=5, retry=1)

    passed = True
    try:
        for bdev_info in lvol.bdev_stack:
            bdev_name = bdev_info['name']
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                logger.info(f"Checking bdev: {bdev_name} ... ok")
            else:
                logger.error(f"Checking LVol: {bdev_name} ... failed")
                passed = False

        ret = rpc_client.subsystem_list(lvol.nqn)
        if ret:
            logger.info(f"Checking subsystem ... ok")
        else:
            logger.info(f"Checking subsystem ... not found")
            passed = False

        logger.info("Checking Distr map ...")
        ret = rpc_client.distr_get_cluster_map(lvol.base_bdev)
        if not ret:
            logger.error("Failed to get cluster map")
            passed = False
        else:
            results, is_passed = distr_controller.parse_distr_cluster_map(ret)
            if results:
                logger.info(utils.print_table(results))
                logger.info(f"Checking Distr map ... {is_passed}")
            else:
                logger.error("Failed to parse distr cluster map")
            passed &= is_passed
    except Exception as e:
        logger.exception(e)
        return False

    return passed


def check_lvol(lvol_id):
    db_controller = DBController()

    lvol = db_controller.get_lvol_by_id(lvol_id)
    if not lvol:
        logger.error(f"lvol not found: {lvol_id}")
        return False

    if lvol.ha_type == 'single':
        ret = check_lvol_on_node(lvol_id, lvol.node_id)
        return ret

    elif lvol.ha_type == "ha":
        passed = True
        for nodes_id in lvol.nodes:
            ret = check_lvol_on_node(lvol_id, nodes_id)
            if not ret:
                passed = False
        return passed
