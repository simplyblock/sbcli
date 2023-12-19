# coding=utf-8
import logging
import os
import numpy

import time
import sys

SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(SCRIPT_PATH, "../.."))

from management import constants, kv_store
from management.rpc_client import RPCClient
from management.models.device_stat import LVolStat


def calculate_mean_and_stdev_for_all_devices(devices_list):
    capacity_list = []
    for device in devices_list:
        capacity_list.append(device.get_capacity_percentage())
    n_array = numpy.array(capacity_list)
    mean_value = int(numpy.mean(n_array))
    st_dev = int(numpy.std(n_array))
    return mean_value, st_dev


def update_lvol_stats(node, lvol, stats):
    now = int(time.time())
    data = {
        "uuid": lvol.uuid,
        "node_id": node.get_id(),
        "date": now,
        "read_latency_ticks": stats['read_latency_ticks'],
        "write_latency_ticks": stats['write_latency_ticks'],
        "stats": stats
    }
    last_stat = LVolStat(data={"uuid": lvol.get_id(), "node_id": node.get_id()}).get_last(db_store)
    if last_stat:
        data.update({
            "read_bytes_per_sec": int((stats['bytes_read'] - last_stat.stats['bytes_read']) / (now - last_stat.date)),
            "read_iops": int((stats['num_read_ops'] - last_stat.stats['num_read_ops']) / (now - last_stat.date)),
            "write_bytes_per_sec": int(
                (stats['bytes_written'] - last_stat.stats['bytes_written']) / (now - last_stat.date)),
            "write_iops": int((stats['num_write_ops'] - last_stat.stats['num_write_ops']) / (now - last_stat.date)),
            "unmapped_bytes_per_sec": int(
                (stats['bytes_unmapped'] - last_stat.stats['bytes_unmapped']) / (now - last_stat.date)),
        })
    else:
        logger.warning("last record not found")

    stat_obj = LVolStat(data=data)
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


logger.info("Starting stats collector...")
while True:
    lvols = db_controller.get_lvols()
    if not lvols:
        logger.error("LVols list is empty")

    for lvol in lvols:
        snode = db_controller.get_storage_node_by_hostname(lvol.hostname)
        rpc_client = RPCClient(
            snode.mgmt_ip,
            snode.rpc_port,
            snode.rpc_username,
            snode.rpc_password,
            timeout=3, retry=2
        )

        # getting device stats
        logger.info("Getting lVol stats: %s", lvol.uuid)
        stats_dict = rpc_client.get_lvol_stats(lvol.top_bdev)
        if stats_dict and stats_dict['bdevs']:
            stats = stats_dict['bdevs'][0]
            update_lvol_stats(snode, lvol, stats)
        else:
            logger.error("Error getting LVol stats: %s", lvol.uuid)

    time.sleep(constants.LVOL_STAT_COLLECTOR_INTERVAL_SEC)
