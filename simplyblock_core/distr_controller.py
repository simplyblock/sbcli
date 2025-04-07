# coding=utf-8
import datetime
import logging
import re

from simplyblock_core import utils
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.db_controller import DBController

logger = logging.getLogger()


def send_node_status_event(node, node_status, target_node=None):
    db_controller = DBController()
    node_id = node.get_id()
    if node_status == StorageNode.STATUS_SCHEDULABLE:
        node_status = StorageNode.STATUS_UNREACHABLE
    logging.info(f"Sending event updates, node: {node_id}, status: {node_status}")
    node_status_event = {
        "timestamp": datetime.datetime.now().isoformat("T", "seconds") + 'Z',
        "event_type": "node_status",
        "UUID_node": node_id,
        "status": node_status}
    events = {"events": [node_status_event]}
    logger.debug(node_status_event)
    if target_node:
        snodes = [target_node]
    else:
        snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
    for node in snodes:
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            continue
        logger.info(f"Sending to: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)
        ret = rpc_client.distr_status_events_update(events)


def send_dev_status_event(device, status, target_node=None):
    if status == NVMeDevice.STATUS_NEW:
        return
    db_controller = DBController()
    storage_ID = device.cluster_device_order
    if target_node:
        snodes = [db_controller.get_storage_node_by_id(target_node.get_id())]
    else:
        snodes = db_controller.get_storage_nodes_by_cluster_id(device.cluster_id)
    for node in snodes:
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            continue

        dev_status = status

        if status == NVMeDevice.STATUS_ONLINE and node.get_id() != device.node_id:
            rem_dev = None
            for dev2 in node.remote_devices:
                if dev2.get_id() == device.get_id() :
                    rem_dev = dev2
                    break

            if not rem_dev or rem_dev.status != NVMeDevice.STATUS_ONLINE:
                dev_status = NVMeDevice.STATUS_UNAVAILABLE
                logger.warning(f"Device is not connected to node, dev: {device.get_id()}, node: {node.get_id()}")

        events = {"events": [{
            "timestamp": datetime.datetime.now().isoformat("T", "seconds") + 'Z',
            "event_type": "device_status",
            "storage_ID": storage_ID,
            "status": dev_status}]}
        logging.debug(f"Sending event updates, device: {storage_ID}, status: {dev_status}, node: {node.get_id()}")
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)
        ret = rpc_client.distr_status_events_update(events)
        if not ret:
            logger.warning("Failed to send event update")


def disconnect_device(device):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(device.cluster_id)
    for node in snodes:
        if node.status != node.STATUS_ONLINE:
            continue
        new_remote_devices = []
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=5, retry=2)
        for rem_dev in node.remote_devices:
            if rem_dev.get_id() == device.get_id():
                ctrl_name = rem_dev.remote_bdev[:-2]
                rpc_client.bdev_nvme_detach_controller(ctrl_name)
            else:
                new_remote_devices.append(rem_dev)
        node.remote_devices = new_remote_devices
        node.write_to_db(db_controller.kv_store)


def get_distr_cluster_map(snodes, target_node, distr_name=""):
    map_cluster = {}
    map_prob = {}
    local_node_index = 0
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(target_node.cluster_id)
    for index, snode in enumerate(snodes):
        if snode.is_secondary_node:  # pass
            continue
        dev_map = {}
        dev_w_map = {}
        node_w = 0
        for i, dev in enumerate(snode.nvme_devices):
            if dev.status in [NVMeDevice.STATUS_JM, NVMeDevice.STATUS_NEW]:
                continue
            dev_w_gib = utils.convert_size(dev.size, 'GiB') or 1
            name = None
            dev_status = dev.status
            if snode.get_id() == target_node.get_id():
                name = dev.alceml_bdev
                local_node_index = index
            else:
                for dev2 in target_node.remote_devices:
                    if dev2.get_id() == dev.get_id():
                        name = dev2.remote_bdev
                        dev_status = dev.status
                        break
            if not name:
                name = f"remote_{dev.alceml_bdev}n1"
                if dev_status == NVMeDevice.STATUS_ONLINE:
                    dev_status = NVMeDevice.STATUS_UNAVAILABLE
            logger.debug(f"Device: {dev.get_id()}, status: {dev_status}, bdev_name: {name}")
            dev_map[dev.cluster_device_order] = {
                "UUID": dev.get_id(),
                "bdev_name": name,
                "status": dev_status,
                "physical_label": dev.physical_label
            }
            if dev.status in [NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                dev_w_map[dev.cluster_device_order] = {"weight": dev_w_gib, "id": -1}
            else:
                dev_w_map[dev.cluster_device_order] = {"weight": dev_w_gib, "id": dev.cluster_device_order}
                node_w += dev_w_gib

        node_status = snode.status
        if node_status == StorageNode.STATUS_SCHEDULABLE:
            node_status = StorageNode.STATUS_UNREACHABLE
        map_cluster[snode.get_id()] = {
            "status": node_status,
            "devices": dev_map}
        map_prob[snode.get_id()] = {
            "weight": node_w,
            "items": [d for k, d in dev_w_map.items()]}
    cl_map = {
        "name": distr_name,
        "UUID_node_target": target_node.get_id(),
        "timestamp": datetime.datetime.now().isoformat("T", "seconds")+'Z',
        "map_cluster": map_cluster,
        "map_prob": [d for k, d in map_prob.items()]
    }
    if cluster.enable_node_affinity:
        # if target_node.is_secondary_node and distr_name:
        #     for index, snode in enumerate(snodes):
        #         for bdev in snode.lvstore_stack:
        #             if bdev['type'] == "bdev_distr" and bdev['name'] == distr_name:
        #                 local_node_index = index
        #                 break
        cl_map['ppln1'] = local_node_index
    return cl_map


def parse_distr_cluster_map(map_string):
    db_controller = DBController()
    node_pattern = re.compile(r".*uuid_node=(.*)  status=(.*)$", re.IGNORECASE)
    device_pattern = re.compile(
        r".*storage_ID=(.*)  status=(.*)  uuid_device=(.*)  storage_bdev_name=(.*)$", re.IGNORECASE)

    results = []
    passed = True
    for line in map_string.split('\n'):
        line = line.strip()
        m = node_pattern.match(line)
        if m:
            node_id, status = m.groups()
            data = {
                "Kind": "Node",
                "UUID": node_id,
                "Reported Status": status,
                "Actual Status": "",
                "Results": "",
            }
            nd = db_controller.get_storage_node_by_id(node_id)
            if nd:
                node_status = nd.status
                if node_status == StorageNode.STATUS_SCHEDULABLE:
                    node_status = StorageNode.STATUS_UNREACHABLE
                data["Actual Status"] = node_status
                if node_status == status:
                    data["Results"] = "ok"
                else:
                    data["Results"] = "failed"
                    passed = False
            else:
                data["Results"] = "not found"
                passed = False
            results.append(data)
        m = device_pattern.match(line)
        if m:
            storage_id, status, device_id, bdev_name = m.groups()
            data = {
                "Kind": "Device",
                "UUID": device_id,
                "Reported Status": status,
                "Actual Status": "",
                "Results": "",
            }
            sd = db_controller.get_storage_device_by_id(device_id)
            if sd:
                data["Actual Status"] = sd.status
                if sd.status == status:
                    data["Results"] = "ok"
                else:
                    data["Results"] = "failed"
                    passed = False
            else:
                data["Results"] = "not found"
                passed = False
            results.append(data)
    return results, passed


def send_cluster_map_to_node(node):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
    rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=10)

    # if node.lvstore_stack_secondary_1:
    #     for snode in db_controller.get_primary_storage_nodes_by_secondary_node_id(node.get_id()):
    #         for bdev in snode.lvstore_stack:
    #             if bdev['type'] == "bdev_distr":
    #                 cluster_map_data = get_distr_cluster_map(snodes, node, bdev["name"])
    #                 ret = rpc_client.distr_send_cluster_map(cluster_map_data)
    #                 if not ret:
    #                     logger.error("Failed to send cluster map")
    #                     return False
    #     return True
    # else:
    cluster_map_data = get_distr_cluster_map(snodes, node)
    ret = rpc_client.distr_send_cluster_map(cluster_map_data)
    if not ret:
        logger.error("Failed to send cluster map")
        logger.info(cluster_map_data)
        return False
    return True


def send_cluster_map_to_distr(node, distr_name):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
    rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=10)
    cluster_map_data = get_distr_cluster_map(snodes, node, distr_name)
    ret = rpc_client.distr_send_cluster_map(cluster_map_data)
    if not ret:
        logger.error("Failed to send cluster map")
        logger.info(cluster_map_data)
        return False
    return True


def send_cluster_map_add_node(snode, target_node):
    if target_node.status != StorageNode.STATUS_ONLINE:
        return False
    logger.info(f"Sending to: {target_node.get_id()}")
    rpc_client = RPCClient(target_node.mgmt_ip, target_node.rpc_port, target_node.rpc_username, target_node.rpc_password, timeout=5)

    cluster_map_data = get_distr_cluster_map([snode], target_node)
    cl_map = {
        "map_cluster": cluster_map_data['map_cluster'],
        "map_prob": cluster_map_data['map_prob']}
    ret = rpc_client.distr_add_nodes(cl_map)
    if not ret:
        logger.error("Failed to send cluster map")
        return False
    return True


"""

{
	"UUID_node" : "2373f2e5-609d-471c-8756-ba71c4e45069",
        "devices": {
            "4": {
                "physical_label": "1",
                "UUID": "67eadedc-94e6-4a47-a74a-10dbe847f3f9",
                "bdev_name": "alloc0004",
                "status": "online",
                "weight": 1000
            },
            "5": {
                "physical_label": "3",
                "UUID": "6c304117-66b3-4508-b9fc-84d2dbd482ff",
                "bdev_name": "alloc0005",
                "status": "online",
                "weight": 1000
            }
        }
}
"""
def send_cluster_map_add_device(device: NVMeDevice, target_node):
    db_controller = DBController()
    dnode = db_controller.get_storage_node_by_id(device.node_id)
    dev_w_gib = utils.convert_size(device.size, 'GiB') or 1
    if target_node.status == StorageNode.STATUS_ONLINE:
        rpc_client = RPCClient(
            target_node.mgmt_ip, target_node.rpc_port, target_node.rpc_username, target_node.rpc_password, timeout=3)

        if target_node.get_id() == dnode.get_id():
            name = device.alceml_bdev
        else:
            name = f"remote_{device.alceml_bdev}n1"

        cl_map = {
            "UUID_node": dnode.get_id(),
            "devices" : {device.cluster_device_order: {
                "UUID": device.get_id(),
                "bdev_name": name,
                "status": device.status,
                "weight": dev_w_gib,
            }}
        }
        ret = rpc_client.distr_add_devices(cl_map)
        if not ret:
            logger.error("Failed to send cluster map")
            return False
    return True
