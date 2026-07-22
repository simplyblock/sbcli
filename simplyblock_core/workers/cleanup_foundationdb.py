# coding=utf-8
import time
import os

import fdb

from simplyblock_core import constants, utils
from simplyblock_core.db_controller import DBController


logger = utils.get_logger(__name__)

logger.info("Starting FDB cleanup script...")

db_controller = DBController()
logger.debug("Database controller initialized.")

deletion_interval = os.getenv('LOG_DELETION_INTERVAL', '7d')

# Ranges cleared per FDB transaction. Clears are cheap mutations; batching
# them replaces the previous one-transaction-per-range pattern that committed
# ~2 ranges per lvol per cycle (mass-snapshot run 2026-07-21: 233k commits
# over the run, a steady ~100 commits/s of pure no-op load on FDB while the
# API was starving on transaction timeouts).
CLEAR_BATCH_SIZE = 500


@fdb.transactional
def _clear_ranges_tx(tr, ranges):
    for start, end in ranges:
        tr.clear_range(start, end)


def clear_ranges(ranges, label):
    """Clear all (start, end) byte ranges in batched transactions and log a
    single summary line per object type instead of one INFO per range (the
    per-range logging alone produced a 134 MB log in one 5h run)."""
    cleared = 0
    failed = 0
    for i in range(0, len(ranges), CLEAR_BATCH_SIZE):
        batch = ranges[i:i + CLEAR_BATCH_SIZE]
        try:
            _clear_ranges_tx(db_controller.kv_store, batch)
            cleared += len(batch)
        except Exception as e:
            failed += len(batch)
            logger.error(f"Failed to clear a batch of {label} ranges: {e}")
    logger.info(f"Cleared {cleared} {label} ranges"
                + (f" ({failed} failed)" if failed else ""))


def PoolStatObject(lvols, st_date, end_date):
    ranges = []
    # One range per pool — the old per-lvol loop cleared the identical pool
    # range once for every lvol in it.
    for pool_uuid in sorted({lvol.pool_uuid for lvol in lvols}):
        index = "object/PoolStatObject/%s/%s/" % (pool_uuid, pool_uuid)
        ranges.append(((index + str(st_date)).encode('utf-8'),
                       (index + str(end_date)).encode('utf-8')))
    clear_ranges(ranges, "PoolStatObject")


def LVolStatObject(lvols, st_date, end_date):
    ranges = []
    for lvol in lvols:
        index = "object/LVolStatObject/%s/%s/" % (lvol.pool_uuid, lvol.uuid)
        ranges.append(((index + str(st_date)).encode('utf-8'),
                       (index + str(end_date)).encode('utf-8')))
    clear_ranges(ranges, "LVolStatObject")


def DeviceStatObject(clusters, st_date, end_date):
    ranges = []
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for node in snodes:
            for device in node.nvme_devices:
                device_id = device.get_id()
                index = "object/DeviceStatObject/%s/%s/" % (cluster_id, device_id)
                ranges.append(((index + str(st_date)).encode('utf-8'),
                               (index + str(end_date)).encode('utf-8')))
    clear_ranges(ranges, "DeviceStatObject")


def NodeStatObject(clusters, st_date, end_date):
    ranges = []
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for node in snodes:
            node_id = node.get_id()
            index = "object/NodeStatObject/%s/%s/" % (cluster_id, node_id)
            ranges.append(((index + str(st_date)).encode('utf-8'),
                           (index + str(end_date)).encode('utf-8')))
    clear_ranges(ranges, "NodeStatObject")


def ClusterStatObject(clusters, st_date, end_date):
    ranges = []
    for cl in clusters:
        cluster_id = cl.get_id()
        index = "object/ClusterStatObject/%s/%s/" % (cluster_id, cluster_id)
        ranges.append(((index + str(st_date)).encode('utf-8'),
                       (index + str(end_date)).encode('utf-8')))
    clear_ranges(ranges, "ClusterStatObject")


def convert_to_seconds(time_string):
    num = int(''.join(filter(str.isdigit, time_string)))
    unit = ''.join(filter(str.isalpha, time_string))

    if unit == 'h':
        return num * 3600  # hours to seconds
    elif unit == 'm':
        return num * 60    # minutes to seconds
    elif unit == 'd':
        return num * 86400 # days to seconds
    else:
        raise ValueError("Unsupported time unit")

while True:
    try:
        clusters = db_controller.get_clusters()
        if db_controller.kv_store is None:
            raise RuntimeError('Database not initialized')

        # Minis carry pool_uuid + uuid — all the range keys need. The full
        # LVol scan here re-read every 72-field record each cycle.
        lvols = db_controller.get_mini_lvols()
        logger.info("Clusters and logical volumes successfully retrieved for cleanup.")

        st_date = "" # seconds
        end_date = int(time.time()) - convert_to_seconds(deletion_interval)

        LVolStatObject(lvols, st_date, end_date)
        PoolStatObject(lvols, st_date, end_date)
        DeviceStatObject(clusters, st_date, end_date)
        NodeStatObject(clusters, st_date, end_date)
        ClusterStatObject(clusters, st_date, end_date)

        logger.info("Completed a cleaning cycle. Sleeping until next interval.")
        time.sleep(constants.FDB_CLEANUP_INTERVAL_SEC)

    except Exception as e:
        logger.error(f"An error occurred in the main loop: {e}")
        time.sleep(10)
