# coding=utf-8
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import lvol_events
from simplyblock_core.models.stats import LVolStatObject, PoolStatObject
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCClient


logger = utils.get_logger(__name__)


last_object_record = {}


def sum_stats(stats_list):
    if not stats_list or len(stats_list) == 0:
        return None
    if len(stats_list) == 1:
        return stats_list[0]

    ret = {}
    for key in stats_list[0].keys():
        for stat_dict in stats_list:
            value = stat_dict[key]
            try:
                v_int = int(value)
                if key in ret:
                    ret[key] += v_int
                else:
                    ret[key] = v_int
            except:
                pass
    return ret


def add_lvol_stats(cluster, pool, lvol, stats_list, capacity_dict=None, connected_clients=0):
    now = int(time.time())
    data = {
        "pool_id": pool.get_id(),
        "uuid": lvol.get_id(),
        "date": now}

    if connected_clients:
        data['connected_clients'] = connected_clients

    if capacity_dict:
        size_used = 0
        lvol_dict = capacity_dict[0]
        size_total = int(lvol_dict['num_blocks']*lvol_dict['block_size'])
        cluster_size = cluster.distr_ndcs * cluster.page_size_in_blocks
        if "driver_specific" in lvol_dict and "lvol" in lvol_dict["driver_specific"]:
            num_allocated_clusters = lvol_dict["driver_specific"]["lvol"]["num_allocated_clusters"]
            size_used = int(num_allocated_clusters*cluster_size)

        size_free = size_total - size_used
        size_util = 0
        if size_total > 0:
            size_util = int((size_used / size_total) * 100)

        data.update({
            "size_total": size_total,
            "size_used": size_used,
            "size_free": size_free,
            "size_util": size_util,
            # "capacity_dict": capacity_dict
        })
    else:
        logger.error(f"Error getting Alceml capacity, response={capacity_dict}")

    if stats_list:

        stats = sum_stats(stats_list)

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
                if data['read_bytes'] >= last_record['read_bytes']:
                    data['read_bytes_ps'] = int((data['read_bytes'] - last_record['read_bytes']) / time_diff)
                else:
                    data['read_bytes_ps'] = int(data['read_bytes'] / time_diff)

                if data['read_io'] >= last_record['read_io']:
                    data['read_io_ps'] = int((data['read_io'] - last_record['read_io']) / time_diff)
                else:
                    data['read_io_ps'] = int(data['read_io'] / time_diff)

                if data['read_latency_ticks'] >= last_record['read_latency_ticks']:
                    data['read_latency_ps'] = int((data['read_latency_ticks'] - last_record['read_latency_ticks']) / time_diff)
                else:
                    data['read_latency_ps'] = int(data['read_latency_ticks'] / time_diff)

                if data['write_bytes'] >= last_record['write_bytes']:
                    data['write_bytes_ps'] = int((data['write_bytes'] - last_record['write_bytes']) / time_diff)
                else:
                    data['write_bytes_ps'] = int(data['write_bytes'] / time_diff)

                if data['write_io'] >= last_record['write_io']:
                    data['write_io_ps'] = int((data['write_io'] - last_record['write_io']) / time_diff)
                else:
                    data['write_io_ps'] = int(data['write_io'] / time_diff)

                if data['write_latency_ticks'] >= last_record['write_latency_ticks']:
                    data['write_latency_ps'] = int((data['write_latency_ticks'] - last_record['write_latency_ticks']) / time_diff)
                else:
                    data['write_latency_ps'] = int(data['write_latency_ticks'] / time_diff)

                if data['unmap_bytes'] >= last_record['unmap_bytes']:
                    data['unmap_bytes_ps'] = int((data['unmap_bytes'] - last_record['unmap_bytes']) / time_diff)
                else:
                    data['unmap_bytes_ps'] = int(data['unmap_bytes'] / time_diff)

                if data['unmap_io'] >= last_record['unmap_io']:
                    data['unmap_io_ps'] = int((data['unmap_io'] - last_record['unmap_io']) / time_diff)
                else:
                    data['unmap_io_ps'] = int(data['unmap_io'] / time_diff)

                if data['unmap_latency_ticks'] >= last_record['unmap_latency_ticks']:
                    data['unmap_latency_ps'] = int((data['unmap_latency_ticks'] - last_record['unmap_latency_ticks']) / time_diff)
                else:
                    data['unmap_latency_ps'] = int(data['unmap_latency_ticks'] / time_diff)

                if data['read_io_ps'] > 0 and data['write_io_ps'] > 0 and lvol.io_error:
                    # set lvol io error to false
                    lvol = db_controller.get_lvol_by_id(lvol.get_id())
                    lvol.io_error = False
                    lvol.write_to_db()
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
db_controller = db_controller.DBController()

logger.info("Starting stats collector...")
while True:

    pools = db_controller.get_pools()
    all_lvols = db_controller.get_lvols()  # pass
    for pool in pools:
        cluster = db_controller.get_cluster_by_id(pool.cluster_id)
        lvols = db_controller.get_lvols_by_pool_id(pool.get_id())
        if not lvols:
            logger.error("LVols list is empty")

        stat_records = []
        for lvol in lvols:
            if lvol.status == lvol.STATUS_IN_DELETION:
                logger.warning(f"LVol in deletion, id: {lvol.get_id()}, status: {lvol.status}.. skipping")
                continue
            hosts = []
            stats = []
            capacity_dict = None
            connected_clients = 0
            if lvol.ha_type == "ha":
                hosts = lvol.nodes
            else:
                hosts=[lvol.node_id]
            for host in hosts:
                snode = db_controller.get_storage_node_by_id(host)
                if snode.status != StorageNode.STATUS_ONLINE:
                    continue
                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password,
                    timeout=1, retry=2)
                logger.info("Getting lVol stats: %s from node: %s", lvol.uuid, host)
                stats_dict = rpc_client.get_lvol_stats(lvol.top_bdev)
                if stats_dict and stats_dict['bdevs']:
                    stats.append( stats_dict['bdevs'][0])

                if not capacity_dict:
                    capacity_dict = rpc_client.get_bdevs(lvol.base_bdev)

                ret = rpc_client.nvmf_subsystem_get_controllers(lvol.nqn)
                if ret:
                    connected_clients = max(len(ret), connected_clients)

            record = add_lvol_stats(cluster, pool, lvol, stats, capacity_dict, connected_clients)
            stat_records.append(record)

        if stat_records:
            add_pool_stats(pool, stat_records)

    time.sleep(constants.LVOL_STAT_COLLECTOR_INTERVAL_SEC)
