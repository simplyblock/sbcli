# coding=utf-8
import datetime
import logging
import re
import threading

from simplyblock_core import utils
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.db_controller import DBController

logger = logging.getLogger()


def _remote_device_from_device(device, status, remote_bdev=None):
    remote_device = RemoteDevice()
    remote_device.uuid = device.uuid
    remote_device.alceml_name = device.alceml_name
    remote_device.node_id = device.node_id
    remote_device.size = device.size
    remote_device.status = status
    remote_device.nvmf_multipath = device.nvmf_multipath
    remote_device.remote_bdev = remote_bdev or f"remote_{device.alceml_bdev}n1"
    return remote_device


def _persist_target_device_event(device, status, target_node):
    db_controller = DBController()
    node = db_controller.get_storage_node_by_id(target_node.get_id())
    if node.get_id() == device.node_id:
        for dev in node.nvme_devices:
            if dev.get_id() == device.get_id():
                dev.status = status
                break
    else:
        new_remote_devices = []
        found = False
        for rem_dev in node.remote_devices:
            if rem_dev.get_id() == device.get_id():
                rem_dev.status = status
                if not rem_dev.remote_bdev and status == NVMeDevice.STATUS_ONLINE:
                    rem_dev.remote_bdev = f"remote_{device.alceml_bdev}n1"
                found = True
            new_remote_devices.append(rem_dev)
        if not found and status == NVMeDevice.STATUS_ONLINE:
            new_remote_devices.append(_remote_device_from_device(device, status))
        node.remote_devices = new_remote_devices
    node.write_to_db(db_controller.kv_store)


def send_node_status_event(node, node_status, target_node=None):
    db_controller = DBController()
    node_id = node.get_id()
    if node_status == StorageNode.STATUS_SCHEDULABLE:
        node_status = StorageNode.STATUS_UNREACHABLE
    logger.info(f"Sending event updates, node: {node_id}, status: {node_status}")
    node_status_event = {
        "timestamp": datetime.datetime.now().isoformat("T", "seconds") + 'Z',
        "event_type": "node_status",
        "UUID_node": node_id,
        "status": node_status}
    events = {"events": [node_status_event]}
    logger.debug(node_status_event)
    skipped_nodes = []
    connect_threads = []
    if target_node:
        snodes = [target_node]
    else:
        snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
        for node in snodes:
            if node.status == StorageNode.STATUS_SCHEDULABLE:
                skipped_nodes.append(node)

    for node in snodes:
        if node.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,  StorageNode.STATUS_DOWN]:
            continue
        node_found_same_host = False
        for n in skipped_nodes:
            if node.mgmt_ip == n.mgmt_ip:
                node_found_same_host = True
                break
        if node_found_same_host:
            continue
        logger.info(f"Sending to: {node.get_id()}")
        t = threading.Thread(
            target=_send_event_to_node,
            args=(node, events,))
        connect_threads.append(t)
        t.start()

    for t in connect_threads:
        t.join()


def send_dev_status_event(device, status, target_node=None):
    if status == NVMeDevice.STATUS_NEW:
        return
    db_controller = DBController()
    storage_ID = device.cluster_device_order
    skipped_nodes = []
    connect_threads = []
    if target_node:
        snodes = [db_controller.get_storage_node_by_id(target_node.get_id())]
    else:
        snodes = db_controller.get_storage_nodes_by_cluster_id(device.cluster_id)
        for node in snodes:
            if node.status == StorageNode.STATUS_SCHEDULABLE:
                skipped_nodes.append(node)

    results = []
    for node in snodes:
        if node.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
            logger.info(f"skipping node: {node.get_id()} with status: {node.status}")
            continue
        node_found_same_host = False
        for n in skipped_nodes:
            if node.mgmt_ip == n.mgmt_ip:
                node_found_same_host = True
                break
        if node_found_same_host:
            continue
        dev_status = status

        if status == NVMeDevice.STATUS_ONLINE and node.get_id() != device.node_id:
            dev_node = db_controller.get_storage_node_by_id(device.node_id)
            rem_dev = None
            for dev2 in node.remote_devices:
                if dev2.get_id() == device.get_id() :
                    rem_dev = dev2
                    break

            if not rem_dev or rem_dev.status != NVMeDevice.STATUS_ONLINE:
                # Don't downgrade to unavailable if the device's node is just
                # temporarily down — the device is still running and may be
                # reachable via other paths.
                if dev_node and dev_node.status == StorageNode.STATUS_DOWN:
                    logger.info(f"Device not connected to node but device node is down (transient), keeping online status, dev: {device.get_id()}, node: {node.get_id()}")
                else:
                    dev_status = NVMeDevice.STATUS_UNAVAILABLE
                    logger.warning(f"Device is not connected to node, dev: {device.get_id()}, node: {node.get_id()}")

        events = {"events": [{
            "timestamp": datetime.datetime.now().isoformat("T", "seconds") + 'Z',
            "event_type": "device_status",
            "storage_ID": storage_ID,
            "status": dev_status}]}
        logger.debug(f"Sending event updates, device: {storage_ID}, status: {dev_status}, node: {node.get_id()}")
        if target_node:
            sent = _send_event_to_node(node, events)
            results.append(sent)
            if sent:
                _persist_target_device_event(device, dev_status, node)
        else:
            result = {"sent": False, "node": node, "status": dev_status}
            t = threading.Thread(
                target=_send_event_to_node,
                args=(node, events, result))
            connect_threads.append((t, result))
            t.start()

    for t, result in connect_threads:
        t.join()
        results.append(result["sent"])
        if result["sent"]:
            _persist_target_device_event(device, result["status"], result["node"])

    return all(results) if results else False


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
            }
            if (dev.physical_label>0):
                dev_map[dev.cluster_device_order].update({"physical_label": dev.physical_label})

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


def parse_distr_cluster_map(map_string, nodes=None, devices=None):
    db_controller = DBController()
    node_pattern = re.compile(r".*uuid_node=(.*)  status=(.*)$", re.IGNORECASE)
    device_pattern = re.compile(
        r".*storage_ID=(.*)  status=(.*)  uuid_device=(.*)  storage_bdev_name=(.*)$", re.IGNORECASE)

    if not nodes or not devices:
        nodes = {}
        devices = {}
        for n in db_controller.get_storage_nodes():
            nodes[n.get_id()] = n
            for dev in n.nvme_devices:
                devices[dev.get_id()] = dev

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
                "Found Status": status,
                "Desired Status": "",
                "Results": "",
            }
            try:
                node_status = nodes[node_id].status
                # Canonicalise CP states whose data-plane representation is
                # "node not serving" — SPDK cluster maps reflect the last
                # reachability event, which is offline/unreachable during
                # CP-side restart or shutdown transitions. Treating these as
                # strict mismatches caused peers' health checks to flip
                # Health=False cluster-wide while one node was stuck in a
                # transient state.
                if node_status in (
                    StorageNode.STATUS_SCHEDULABLE,
                    StorageNode.STATUS_RESTARTING,
                    StorageNode.STATUS_IN_SHUTDOWN,
                ):
                    node_status = StorageNode.STATUS_UNREACHABLE
                data["Desired Status"] = node_status
                if node_status == status:
                    data["Results"] = "ok"
                else:
                    data["Results"] = "failed"
                    passed = False
            except KeyError:
                data["Results"] = "not found"
                passed = False
            results.append(data)
        m = device_pattern.match(line)
        if m:
            storage_id, status, device_id, bdev_name = m.groups()
            data = {
                "Kind": "Device",
                "UUID": device_id,
                "Found Status": status,
                "Desired Status": "",
                "Results": "",
            }
            try:
                sd =  devices[device_id]
                data["Desired Status"] = sd.status
                if sd.status == status:
                    data["Results"] = "ok"
                else:
                    data["Results"] = "failed"
                    passed = False
            except KeyError:
                data["Results"] = "not found"
                passed = False
            results.append(data)
    return results, passed


def send_cluster_map_to_node(node: StorageNode):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
    cluster_map_data = get_distr_cluster_map(snodes, node)
    try:
        node.rpc_client(timeout=10).distr_send_cluster_map(cluster_map_data)
    except Exception:
        logger.error("Failed to send cluster map")
        logger.info(cluster_map_data)
        return False
    return True


def send_cluster_map_to_distr(node: StorageNode, distr_name: str):
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(node.cluster_id)
    cluster_map_data = get_distr_cluster_map(snodes, node, distr_name)
    try:
        node.rpc_client(timeout=10).distr_send_cluster_map(cluster_map_data)
    except Exception:
        logger.error("Failed to send cluster map")
        logger.info(cluster_map_data)
        return False
    return True


def send_cluster_map_add_node(snode, target_node):
    if target_node.status != StorageNode.STATUS_ONLINE:
        return False
    logger.info(f"Sending to: {target_node.get_id()}")
    cluster_map_data = get_distr_cluster_map([snode], target_node)
    cl_map = {
        "map_cluster": cluster_map_data['map_cluster'],
        "map_prob": cluster_map_data['map_prob']}
    try:
        target_node.rpc_client(timeout=10).distr_add_nodes(cl_map)
    except Exception:
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
def send_cluster_map_add_device(device: NVMeDevice, target_node: StorageNode):
    db_controller = DBController()
    try:
        dnode = db_controller.get_storage_node_by_id(device.node_id)
    except KeyError:
        logger.exception("Node not found")
        return False
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
                "physical_label":  device.physical_label if device.physical_label > 0 else -1,
            }}
        }
        try:
            rpc_client.distr_add_devices(cl_map)
        except Exception:
            logger.error("Failed to send cluster map")
            return False
    return True


def _send_event_to_node(node, events, result=None):
    try:
        node.rpc_client(timeout=1, retry=0).distr_status_events_update(events)
        if result is not None:
            result["sent"] = True
        return True
    except Exception as e:
        # Best-effort broadcast. Peer may be restarting / port-blocked /
        # momentarily unreachable; periodic device-status resync will
        # converge it. Demoted to debug so the restart log isn't noisy
        # with non-fatal delivery misses.
        logger.debug("Failed to send event update to %s: %s", node.get_id(), e)
        if result is not None:
            result["sent"] = False
        return False
