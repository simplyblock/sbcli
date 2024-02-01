# coding=utf-8
import logging
import os

import time
import sys
import uuid

from simplyblock_core import constants, kv_store
from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.models.stats import CapacityStat


def add_cluster_stats(cl, records):

    if not records:
        return False

    size_total = 0
    size_used = 0
    size_prov = 0

    for rec in records:
        size_total += rec.size_total
        size_used += rec.size_used
        size_prov += rec.size_prov

    size_free = size_total - size_used
    size_util = 0
    size_prov_util = 0
    if size_total > 0:
        size_util = int((size_used / size_total) * 100)
        size_prov_util = int((size_prov / size_total) * 100)

    data = {
        "uuid": str(uuid.uuid4()),
        "node_id": cl.get_id(),
        "device_id": cl.get_id(),
        "date": int(time.time()),

        "size_total": size_total,
        "size_used": size_used,
        "size_free": size_free,
        "size_util": size_util,
        "size_prov": size_prov,
        "size_prov_util": size_prov_util,
    }

    stat_obj = CapacityStat(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
    return stat_obj


def add_node_stats(node, records):

    if not records:
        return False

    size_total = 0
    size_used = 0

    for rec in records:
        size_total += rec.size_total
        size_used += rec.size_used

    size_free = size_total - size_used
    size_util = 0
    size_prov_util = 0
    size_prov = 0
    for lvol_id in node.lvols:
        lvol = db_controller.get_lvol_by_id(lvol_id)
        if lvol:
            size_prov += lvol.size

    if size_total > 0:
        size_util = int((size_used / size_total) * 100)
        size_prov_util = int((size_prov / size_total) * 100)

    data = {
        "uuid": str(uuid.uuid4()),
        "node_id": node.get_id(),
        "device_id": node.get_id(),
        "date": int(time.time()),

        "size_total": size_total,
        "size_used": size_used,
        "size_free": size_free,
        "size_util": size_util,
        "size_prov": size_prov,
        "size_prov_util": size_prov_util,
    }

    stat_obj = CapacityStat(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
    return stat_obj


def add_device_stats(device, capacity_dict):

    if capacity_dict['res'] != 1:
        logger.error(f"Error getting Alceml capacity, response={capacity_dict}")
        return False

    size_total = int(capacity_dict['npages_nmax']*capacity_dict['pba_page_size'])
    size_used = int(capacity_dict['npages_used']*capacity_dict['pba_page_size'])
    size_free = size_total - size_used
    size_util = 0
    if size_total > 0:
        size_util = int((size_used / size_total) * 100)

    data = {
        "uuid": str(uuid.uuid4()),
        "node_id": device.node_id,
        "device_id": device.get_id(),
        "date": int(time.time()),

        "size_total": size_total,
        "size_used": size_used,
        "size_free": size_free,
        "size_util": size_util,

        "stats_dict": capacity_dict
    }

    stat_obj = CapacityStat(data=data)
    stat_obj.write_to_db(db_controller.kv_store)

    return stat_obj


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_controller = kv_store.DBController()

logger.info("Starting device capacity collector...")
while True:

    clusters = db_controller.get_clusters()
    for cl in clusters:
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        if not snodes:
            logger.error(f"Cluster has no storage nodes: {cl.get_id()}")

        node_records = []
        for node in snodes:
            logger.info("Node: %s", node.get_id())
            if node.status != 'online':
                logger.info("Node is not online, skipping")
                continue
            if not node.nvme_devices:
                logger.error("No devices found in node: %s", node.get_id())
                continue

            rpc_client = RPCClient(
                node.mgmt_ip, node.rpc_port,
                node.rpc_username, node.rpc_password,
                timeout=3, retry=2)
            devices_records = []
            for device in node.nvme_devices:
                logger.info("Getting device stats: %s", device.uuid)
                if device.status != 'online':
                    logger.info("Device is not online, skipping")
                    continue
                capacity_dict = rpc_client.alceml_get_capacity(device.alceml_bdev)
                if not capacity_dict:
                    logger.error("Error getting device stats: %s", device.get_id())
                    continue
                record = add_device_stats(device, capacity_dict)
                if record:
                    devices_records.append(record)

            node_record = add_node_stats(node, devices_records)
            if node_record:
                node_records.append(node_record)

        if node_records:
            add_cluster_stats(cl, node_records)

    time.sleep(constants.DEV_STAT_COLLECTOR_INTERVAL_SEC)
