# coding=utf-8
import logging
import os

import time
import sys

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(SCRIPT_PATH, "../.."))

from management import constants, kv_store
from management.rpc_client import RPCClient
from management.models.device_stat import DeviceStat


def update_device_stats(node, device, stats, capacity_dict):
    now = int(time.time())
    data = {
        "uuid": device.uuid,
        "node_id": node.get_id(),
        "date": now,
        "read_latency_ticks": stats['read_latency_ticks'],
        "write_latency_ticks": stats['write_latency_ticks'],
        "stats": stats
    }
    if capacity_dict:
        if capacity_dict['res'] == 1:
            data.update(capacity_dict)
        else:
            logger.warning(f"Error getting Alceml capacity, response={capacity_dict['res']}")


    last_stat = DeviceStat(data={"uuid": device.get_id(), "node_id": node.get_id()}).get_last(db_store)
    if last_stat:
        time_diff = (now - last_stat.date)
        if time_diff > 0:
            data.update({
                "read_bytes_per_sec": int(
                    (stats['bytes_read'] - last_stat.stats['bytes_read']) / time_diff),
                "read_iops": int((stats['num_read_ops'] - last_stat.stats['num_read_ops']) / time_diff),
                "write_bytes_per_sec": int(
                    (stats['bytes_written'] - last_stat.stats['bytes_written']) / time_diff),
                "write_iops": int((stats['num_write_ops'] - last_stat.stats['num_write_ops']) / time_diff),
                "unmapped_bytes_per_sec": int(
                    (stats['bytes_unmapped'] - last_stat.stats['bytes_unmapped']) / time_diff),
                "read_latency_ticks": int(
                    (stats['read_latency_ticks'] - last_stat.stats['read_latency_ticks']) / time_diff),
                "write_latency_ticks": int(
                    (stats['write_latency_ticks'] - last_stat.stats['write_latency_ticks']) / time_diff),
            })

    else:
        logger.warning("last record not found")

    stat_obj = DeviceStat(data=data)
    stat_obj.write_to_db(db_store)
    return


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()

logger.info("Starting device stats collector...")
while True:
    # get node object
    snodes = db_controller.get_storage_nodes()
    if not snodes:
        logger.error("storage nodes list is empty")

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

        for device in node.nvme_devices:
            # getting device stats
            logger.info("Getting device stats: %s", device.uuid)
            if device.status != 'online':
                logger.info("Device is not online, skipping")
                continue
            # capacity_dict = rpc_client.alceml_get_capacity(device.alceml_bdev)
            stats_dict = rpc_client.get_device_stats(device.alloc_bdev)
            if stats_dict and stats_dict['bdevs']:
                stats = stats_dict['bdevs'][0]
                update_device_stats(node, device, stats, None)
            else:
                logger.error("Error getting device stats: %s", device.uuid)

    time.sleep(constants.DEV_STAT_COLLECTOR_INTERVAL_SEC)
