# coding=utf-8
import time
import os

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import lvol_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
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


def add_lvol_stats(cluster, lvol, stats_list, capacity_dict=None):
    now = int(time.time())
    data = {
        "pool_id": lvol.pool_uuid,
        "uuid": lvol.get_id(),
        "date": now}

    if capacity_dict:
        size_used = 0
        lvol_dict = capacity_dict
        size_total = int(lvol_dict['num_blocks']*lvol_dict['block_size'])
        cluster_size = cluster.page_size_in_blocks
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
                data={"uuid": lvol.get_id(), "pool_id": lvol.pool_uuid},
            ).get_last(db.kv_store)
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
                    lvol = db.get_lvol_by_id(lvol.get_id())
                    lvol.io_error = False
                    lvol.write_to_db()
                    lvol_events.lvol_io_error_change(lvol, False, True, caused_by="monitor")

        else:
            logger.warning("last record not found")
    else:
        logger.error("Error getting stats")

    stat_obj = LVolStatObject(data=data)
    stat_obj.write_to_db(db.kv_store)
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
    stat_obj.write_to_db(db.kv_store)
    return stat_obj


# get DB controller
db = db_controller.DBController()

logger.info("Starting stats collector...")
while True:

    for cluster in db.get_clusters():

        if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
            logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
            continue

        all_node_bdev_names = {}
        all_node_lvols_nqns = {}
        all_node_lvols_stats = {}

        pools_lvols_stats = {}
        for snode in db.get_storage_nodes_by_cluster_id(cluster.get_id()):

            if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:

                lvol_list = db.get_lvols_by_node_id(snode.get_id())

                if not lvol_list:
                    continue

                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=3, retry=2)

                if snode.get_id() in all_node_bdev_names and all_node_bdev_names[snode.get_id()]:
                    node_bdev_names = all_node_bdev_names[snode.get_id()]
                else:
                    node_bdevs = rpc_client.get_bdevs()
                    if node_bdevs:
                        node_bdev_names = {b['name']: b for b in node_bdevs}
                        all_node_bdev_names[snode.get_id()] = node_bdev_names

                if snode.get_id() in all_node_lvols_nqns and all_node_lvols_nqns[snode.get_id()]:
                    node_lvols_nqns = all_node_lvols_nqns[snode.get_id()]
                else:
                    ret = rpc_client.subsystem_list()
                    if ret:
                        node_lvols_nqns = {}
                        for sub in ret:
                            node_lvols_nqns[sub['nqn']] = sub
                        all_node_lvols_nqns[snode.get_id()] = node_lvols_nqns

                if snode.get_id() in all_node_lvols_stats and all_node_lvols_stats[snode.get_id()]:
                    node_lvols_stats = all_node_lvols_stats[snode.get_id()]
                else:
                    ret = rpc_client.get_lvol_stats()
                    if ret:
                        node_lvols_stats = {}
                        for st in ret['bdevs']:
                            node_lvols_stats[st['name']] = st
                        all_node_lvols_stats[snode.get_id()] = node_lvols_stats

                if snode.secondary_node_id:
                    sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
                    if sec_node and sec_node.status==StorageNode.STATUS_ONLINE:
                        sec_rpc_client = RPCClient(
                            sec_node.mgmt_ip, sec_node.rpc_port,
                            sec_node.rpc_username, sec_node.rpc_password, timeout=3, retry=2)

                        if sec_node.get_id() not in all_node_bdev_names or not all_node_bdev_names[sec_node.get_id()]:
                            ret = sec_rpc_client.get_bdevs()
                            if ret:
                                # node_bdev_names = {}
                                node_bdev_names = {b['name']: b for b in ret}
                                all_node_bdev_names[sec_node.get_id()] = node_bdev_names

                        if sec_node.get_id() not in all_node_lvols_nqns or not all_node_lvols_nqns[sec_node.get_id()]:
                            ret = sec_rpc_client.subsystem_list()
                            if ret:
                                node_lvols_nqns = {}
                                for sub in ret:
                                    node_lvols_nqns[sub['nqn']] = sub
                                all_node_lvols_nqns[sec_node.get_id()] = node_lvols_nqns

                        if sec_node.get_id() not in all_node_lvols_stats or not all_node_lvols_stats[sec_node.get_id()]:
                            ret = sec_rpc_client.get_lvol_stats()
                            if ret:
                                sec_node_lvols_stats = {}
                                for st in ret['bdevs']:
                                    sec_node_lvols_stats[st['name']] = st
                                all_node_lvols_stats[sec_node.get_id()] = sec_node_lvols_stats

                for lvol in lvol_list:
                    if lvol.status in [LVol.STATUS_IN_CREATION, LVol.STATUS_IN_DELETION]:
                        continue

                    capacity_dict = {}
                    stats = []
                    logger.info("Getting lVol stats: %s from node: %s", lvol.uuid, snode.get_id())
                    if snode.get_id() in all_node_lvols_stats and lvol.lvol_uuid in all_node_lvols_stats[snode.get_id()]:
                        stats.append(all_node_lvols_stats[snode.get_id()][lvol.lvol_uuid])

                    if snode.get_id() in all_node_bdev_names and lvol.lvol_uuid in all_node_bdev_names[snode.get_id()]:
                        capacity_dict = all_node_bdev_names[snode.get_id()][lvol.lvol_uuid]

                    if lvol.ha_type == "ha":
                        sec_node = db.get_storage_node_by_id(snode.secondary_node_id)
                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            logger.info("Getting lVol stats: %s from node: %s", lvol.uuid, sec_node.get_id())
                            if lvol.lvol_uuid in all_node_lvols_stats[sec_node.get_id()]:
                                stats.append(all_node_lvols_stats[sec_node.get_id()][lvol.lvol_uuid])

                    record = add_lvol_stats(cluster, lvol, stats, capacity_dict)
                    if record:
                        if lvol.pool_uuid in pools_lvols_stats and pools_lvols_stats[lvol.pool_uuid]:
                            pools_lvols_stats[lvol.pool_uuid].append(record)
                        else:
                            pools_lvols_stats[lvol.pool_uuid] = [record]

        for pool in db.get_pools(cluster_id=cluster.get_id()):

            if pool.get_id() in pools_lvols_stats:
                stat_records = pools_lvols_stats[pool.get_id()]
                if stat_records:
                    add_pool_stats(pool, stat_records)

    time.sleep(constants.LVOL_STAT_COLLECTOR_INTERVAL_SEC)
