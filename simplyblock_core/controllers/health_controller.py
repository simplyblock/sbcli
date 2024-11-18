# coding=utf-8
import datetime
import logging as log

import docker

from simplyblock_core import utils, distr_controller
from simplyblock_core.kv_store import DBController
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
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
    return True
    # try:
    #     node_docker = docker.DockerClient(base_url=f"tcp://{ip}:2375", version="auto", timeout=3)
    #     ret = node_docker.info()
    #     if ret:
    #         logger.debug(ret)
    #         return True
    # except Exception as e:
    #     logger.error(f"Failed to connect to node's docker: {e}")
    # return False


def _check_node_rpc(rpc_ip, rpc_port, rpc_username, rpc_password, timeout=3, retry=2):
    try:
        rpc_client = RPCClient(
            rpc_ip, rpc_port, rpc_username, rpc_password,
            timeout=timeout, retry=retry)
        ret = rpc_client.get_version()
        if ret:
            logger.debug(f"SPDK version: {ret['version']}")
            return True
    except Exception as e:
        logger.debug(e)
    return False


def _check_node_api(ip):
    try:
        snode_api = SNodeClient(f"{ip}:5000", timeout=3, retry=2)
        logger.debug(f"Node API={ip}:5000")
        info, _ = snode_api.info()
        if info:
            logger.debug(f"Hostname: {info['hostname']}")
            return True
    except Exception as e:
        logger.debug(e)
    return False


def _check_spdk_process_up(ip):
    try:
        snode_api = SNodeClient(f"{ip}:5000", timeout=3, retry=2)
        logger.debug(f"Node API={ip}:5000")
        is_up, _ = snode_api.spdk_process_is_up()
        logger.debug(f"SPDK is {is_up}")
        return is_up
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
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                ret = check_device(dev.get_id())
                node_devices_check &= ret
            else:
                logger.info(f"Device skipped: {dev.get_id()} status: {dev.status}")
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

        if snode.jm_device:
            jm_device = snode.jm_device
            logger.info(f"Node JM: {jm_device.get_id()}")
            ret = check_jm_device(jm_device.get_id())
            if ret:
                logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... ok")
            else:
                logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... not found")
            node_devices_check &= ret

        if snode.enable_ha_jm:
            logger.info(f"Node remote JMs: {len(snode.remote_jm_devices)}")
            for remote_device in snode.remote_jm_devices:
                ret = rpc_client.get_bdevs(remote_device.remote_bdev)
                if ret:
                    logger.info(f"Checking bdev: {remote_device.remote_bdev} ... ok")
                else:
                    logger.info(f"Checking bdev: {remote_device.remote_bdev} ... not found")
                node_remote_devices_check &= bool(ret)

        lvstore_check = True
        if snode.lvstore and snode.lvstore_stack:
            distribs_list = []
            for bdev in snode.lvstore_stack:
                type = bdev['type']
                if type == "bdev_raid":
                    distribs_list = bdev["distribs_list"]

            for distr in distribs_list:
                ret = rpc_client.get_bdevs(distr)
                if ret:
                    logger.info(f"Checking distr bdev : {distr} ... ok")
                    logger.info("Checking Distr map ...")
                    ret = rpc_client.distr_get_cluster_map(distr)
                    if not ret:
                        logger.error("Failed to get cluster map")
                        lvstore_check = False
                    else:
                        results, is_passed = distr_controller.parse_distr_cluster_map(ret)
                        if results:
                            logger.info(utils.print_table(results))
                            logger.info(f"Checking Distr map ... {is_passed}")
                        else:
                            logger.error("Failed to parse distr cluster map")
                        lvstore_check &= is_passed
                else:
                    logger.info(f"Checking distr bdev : {distr} ... not found")
                    lvstore_check = False
            ret = rpc_client.get_bdevs(snode.raid)
            if ret:
                logger.info(f"Checking raid bdev: {snode.raid} ... ok")
            else:
                logger.info(f"Checking raid bdev: {snode.raid} ... not found")
                lvstore_check = False
            ret = rpc_client.bdev_lvol_get_lvstores(snode.lvstore)
            if ret:
                logger.info(f"Checking lvstore: {snode.lvstore} ... ok")
            else:
                logger.info(f"Checking lvstore: {snode.lvstore} ... not found")
                lvstore_check = False

    return is_node_online and node_devices_check and node_remote_devices_check and lvstore_check


def check_device(device_id):
    db_controller = DBController()
    device = db_controller.get_storage_device_by_id(device_id)
    if not device:
        # is jm device ?
        for node in db_controller.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == device_id:
                return check_jm_device(node.jm_device.get_id())

        logger.error("device not found")
        return False

    snode = db_controller.get_storage_node_by_id(device.node_id)
    if not snode:
        logger.error("node not found")
        return False

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
        logger.info(f"Skipping ,device status is {device.status}")
        return True

    passed = True
    try:
        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password)

        if snode.enable_test_device:
            bdevs_stack = [device.nvme_bdev, device.testing_bdev, device.alceml_bdev, device.pt_bdev]
        else:
            bdevs_stack = [device.nvme_bdev, device.alceml_bdev, device.pt_bdev]

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
            rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=5, retry=1)
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
            if bdev_info['type'] == "bdev_lvol":
                bdev_name = bdev_info['params']["lvs_name"] + "/" + bdev_info['params']["name"]
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                logger.info(f"Checking bdev: {bdev_name} ... ok")
            else:
                logger.error(f"Checking bdev: {bdev_name} ... failed")
                passed = False

        ret = rpc_client.subsystem_list(lvol.nqn)
        if ret:
            logger.info(f"Checking subsystem ... ok")
        else:
            logger.info(f"Checking subsystem ... not found")
            passed = False

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


def check_snap(snap_id):
    db_controller = DBController()
    snap = db_controller.get_snapshot_by_id(snap_id)
    if not snap:
        logger.error(f"snap not found: {snap_id}")
        return False

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    rpc_client = RPCClient(
        snode.mgmt_ip, snode.rpc_port,
        snode.rpc_username, snode.rpc_password, timeout=5, retry=1)

    ret = rpc_client.get_bdevs(snap.snap_bdev)
    return ret


def check_jm_device(device_id):
    db_controller = DBController()
    jm_device = None
    snode = None
    for node in db_controller.get_storage_nodes():
        if node.jm_device.get_id() == device_id:
            jm_device = node.jm_device
            snode = node
            break
    if not jm_device:
        logger.error("device not found")
        return False

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    if jm_device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Skipping ,device status is {jm_device.status}")
        return True

    if snode.primary_ip != snode.mgmt_ip and jm_device.status == JMDevice.STATUS_UNAVAILABLE:
        return True

    passed = True
    try:
        rpc_client = RPCClient(
            snode.mgmt_ip, snode.rpc_port,
            snode.rpc_username, snode.rpc_password, timeout=3, retry=2)

        ret = rpc_client.get_bdevs(jm_device.jm_bdev)
        if ret:
            logger.debug(f"Checking bdev: {jm_device.jm_bdev} ... ok")
        else:
            logger.debug(f"Checking bdev: {jm_device.jm_bdev} ... not found")
            passed = False

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")
        passed = False

    return passed
