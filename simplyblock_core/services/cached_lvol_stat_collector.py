# coding=utf-8
import time

from simplyblock_core import constants, kv_store, utils
from simplyblock_core.models.stats import CachedLVolStatObject
from simplyblock_core.rpc_client import RPCClient


logger = utils.get_logger(__name__)

last_object_record = {}


def add_lvol_stats(lvol, stats_dict):
    now = int(time.time())
    data = {
        "cluster_id": lvol.get_id(),
        "uuid": lvol.get_id(),
        "date": now}

    if stats_dict and stats_dict['bdevs']:
        stats = stats_dict['bdevs'][0]
        data.update({
            "read_bytes": stats['bytes_read'],
            "read_io": stats['num_read_ops'],
            "read_latency_ticks": stats['read_latency_ticks'],

            "write_bytes": stats['bytes_written'],
            "write_io": stats['num_write_ops'],
            "write_latency_ticks": stats['write_latency_ticks'],

            "unmap_bytes": stats['bytes_unmapped'],
            "unmap_io": stats['num_unmap_ops'],
            "unmap_latency_ticks": stats['unmap_latency_ticks'],
        })

        last_record = None
        if lvol.get_id() in last_object_record:
            last_record = last_object_record[lvol.get_id()]
        else:
            records = db_controller.get_cached_lvol_stats(lvol.get_id(), limit=1)
            if records:
                last_record = records[0]

        if last_record:
            time_diff = (now - last_record.date)
            if time_diff > 0:
                data['record_duration'] = time_diff
                data['read_bytes_ps'] = int((data['read_bytes'] - last_record['read_bytes']) / time_diff)
                data['read_io_ps'] = int((data['read_io'] - last_record['read_io']) / time_diff)
                data['read_latency_ps'] = int(
                    (data['read_latency_ticks'] - last_record['read_latency_ticks']) / time_diff)

                data['write_bytes_ps'] = int((data['write_bytes'] - last_record['write_bytes']) / time_diff)
                data['write_io_ps'] = int((data['write_io'] - last_record['write_io']) / time_diff)
                data['write_latency_ps'] = int(
                    (data['write_latency_ticks'] - last_record['write_latency_ticks']) / time_diff)

                data['unmap_bytes_ps'] = int((data['unmap_bytes'] - last_record['unmap_bytes']) / time_diff)
                data['unmap_io_ps'] = int((data['unmap_io'] - last_record['unmap_io']) / time_diff)
                data['unmap_latency_ps'] = int(
                    (data['unmap_latency_ticks'] - last_record['unmap_latency_ticks']) / time_diff)

        else:
            logger.warning("last record not found")
    else:
        logger.error("Error getting stats")

    stat_obj = CachedLVolStatObject(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
    last_object_record[lvol.get_id()] = stat_obj
    return stat_obj


db_store = kv_store.KVStore()
db_controller = kv_store.DBController()

logger.info("Starting stats collector...")
while True:
    cnodes = db_controller.get_caching_nodes()
    for node in cnodes:
        rpc_client = RPCClient(node.mgmt_ip, node.rpc_port, node.rpc_username, node.rpc_password, timeout=3, retry=1)
        for cached_lvol in node.lvols:
            logger.info("Getting lVol stats: %s", cached_lvol.uuid)
            stats_dict = rpc_client.get_lvol_stats(cached_lvol.ocf_bdev)
            add_lvol_stats(cached_lvol.lvol, stats_dict)

    logger.info(f"Sleeping for {constants.CACHED_LVOL_STAT_COLLECTOR_INTERVAL_SEC} seconds")
    time.sleep(constants.CACHED_LVOL_STAT_COLLECTOR_INTERVAL_SEC)
