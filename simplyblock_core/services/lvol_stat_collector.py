# coding=utf-8
import time


from simplyblock_core import constants, kv_store, utils
from simplyblock_core.controllers import lvol_events
from simplyblock_core.models.stats import LVolStatObject, PoolStatObject
from simplyblock_core.rpc_client import RPCClient


logger = utils.get_logger(__name__)


last_object_record = {}


def add_lvol_stats(pool, lvol, stats_dict):
    now = int(time.time())
    data = {
        "pool_id": pool.get_id(),
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

        if lvol.get_id() in last_object_record:
            last_record = last_object_record[lvol.get_id()]
        else:
            last_record = LVolStatObject(
                data={"uuid": lvol.get_id(), "pool_id": pool.get_id()}
            ).get_last(db_controller.kv_store)
        if last_record:
            time_diff = (now - last_record.date)
            if time_diff > 0:
                data['read_bytes_ps'] = int((data['read_bytes'] - last_record['read_bytes']) / time_diff)
                data['read_io_ps'] = int((data['read_io'] - last_record['read_io']) / time_diff)
                data['read_latency_ps'] = int((data['read_latency_ticks'] - last_record['read_latency_ticks']) / time_diff)

                data['write_bytes_ps'] = int((data['write_bytes'] - last_record['write_bytes']) / time_diff)
                data['write_io_ps'] = int((data['write_io'] - last_record['write_io']) / time_diff)
                data['write_latency_ps'] = int((data['write_latency_ticks'] - last_record['write_latency_ticks']) / time_diff)

                data['unmap_bytes_ps'] = int((data['unmap_bytes'] - last_record['unmap_bytes']) / time_diff)
                data['unmap_io_ps'] = int((data['unmap_io'] - last_record['unmap_io']) / time_diff)
                data['unmap_latency_ps'] = int((data['unmap_latency_ticks'] - last_record['unmap_latency_ticks']) / time_diff)

                if data['read_io_ps'] > 0 and data['write_io_ps'] > 0 and lvol.io_error:
                    # set lvol io error to false
                    lvol = db_controller.get_lvol_by_id(lvol.get_id())
                    lvol.io_error = False
                    lvol.write_to_db(db_store)
                    lvol_events.lvol_io_error_change(lvol, False, True, caused_by="monitor")

        else:
            logger.warning("last record not found")
    else:
        logger.error("Error getting stats")

    stat_obj = LVolStatObject(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
    last_object_record[lvol.get_id()] = stat_obj
    return stat_obj


def add_pool_stats(pool, records):

    if not records:
        return False

    records_sum = utils.sum_records(records)

    data = records_sum.get_clean_dict()
    data.update({
        "pool_id": pool.get_id(),
        "uuid": pool.get_id(),
        "date": int(time.time())
    })

    stat_obj = PoolStatObject(data=data)
    stat_obj.write_to_db(db_controller.kv_store)
    return stat_obj


# get DB controller
db_store = kv_store.KVStore()
db_controller = kv_store.DBController()

logger.info("Starting stats collector...")
while True:

    pools = db_controller.get_pools()
    all_lvols = db_controller.get_lvols()  # pass
    for pool in pools:
        lvols = []
        for lvol in all_lvols:
            if lvol.pool_uuid == pool.get_id():
                lvols.append(lvol)

        if not lvols:
            logger.error("LVols list is empty")

        stat_records = []
        for lvol in lvols:
            if lvol.status == lvol.STATUS_IN_DELETION:
                logger.warning(f"LVol in deletion, id: {lvol.get_id()}, status: {lvol.status}.. skipping")
                continue
            snode = db_controller.get_storage_node_by_hostname(lvol.hostname)
            rpc_client = RPCClient(
                snode.mgmt_ip, snode.rpc_port,
                snode.rpc_username, snode.rpc_password,
                timeout=3, retry=2)

            logger.info("Getting lVol stats: %s", lvol.uuid)
            stats_dict = rpc_client.get_lvol_stats(lvol.top_bdev)
            record = add_lvol_stats(pool, lvol, stats_dict)
            stat_records.append(record)
        if stat_records:
            add_pool_stats(pool, stat_records)

    time.sleep(constants.LVOL_STAT_COLLECTOR_INTERVAL_SEC)
