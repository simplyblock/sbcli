import time
import datetime
import logging
import sys

from simplyblock_core import cluster_ops, constants
from simplyblock_core.kv_store import KVStore, DBController

from graypy import GELFUDPHandler

# Configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
gelf_handler = GELFUDPHandler('0.0.0.0', constants.GELF_PORT)
logger = logging.getLogger()
logger.addHandler(gelf_handler)
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

logger.info("Starting FDB cleanup script...")

db_controller = DBController()
logger.debug("Database controller initialized.")

def PoolStatObject(lvols, st_date, end_date):
    for lvol in lvols:
        index = "object/PoolStatObject/%s/%s/" % (lvol.pool_uuid, lvol.pool_uuid)
        start = index + st_date
        end = index + end_date
        try:
            fdb = KVStore()
            fdb.db.clear_range(start, end)
            logger.info(f"Cleared PoolStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear PoolStatObject for {lvol.pool_uuid}: {e}")

def LVolStatObject(lvols, st_date, end_date):
    for lvol in lvols:
        index = "object/LVolStatObject/%s/%s/" % (lvol.pool_uuid, lvol.uuid)
        start = index + st_date
        end = index + end_date
        try:
            fdb = KVStore()
            fdb.db.clear_range(start, end)
            logger.info(f"Cleared LVolStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear LVolStatObject for {lvol.uuid}: {e}")

def DeviceStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            for device in node.nvme_devices:
                device_id = device.get_id()
                index = "object/DeviceStatObject/%s/%s/" % (cluster_id, device_id)
                start = index + st_date
                end = index + end_date
                try:
                    fdb = KVStore()
                    fdb.db.clear_range(start, end)
                    logger.info(f"Cleared DeviceStatObject data from {start} to {end}")
                except Exception as e:
                    logger.error(f"Failed to clear DeviceStatObject for {device_id}: {e}")

def NodeStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            node_id = node.get_id()
            index = "object/NodeStatObject/%s/%s/" % (cluster_id, node_id)
            start = index + st_date
            end = index + end_date
            try:
                fdb = KVStore()
                fdb.db.clear_range(start, end)
                logger.info(f"Cleared NodeStatObject data from {start} to {end}")
            except Exception as e:
                logger.error(f"Failed to clear NodeStatObject for {node_id}: {e}")

def ClusterStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        index = "object/ClusterStatObject/%s/%s/" % (cluster_id, cluster_id)
        start = index + st_date
        end = index + end_date
        try:
            fdb = KVStore()
            fdb.db.clear_range(start, end)
            logger.info(f"Cleared ClusterStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear ClusterStatObject for {cluster_id}: {e}")

days_back = 3

while True:
    try:
        clusters = db_controller.get_clusters()
        lvols = db_controller.get_lvols()
        logger.info("Clusters and logical volumes successfully retrieved for cleanup.")
        
        st_date = int(time.time())  # seconds
        end_date = int(st_date - (days_back * 86400))
        
        LVolStatObject(lvols, st_date, end_date)
        PoolStatObject(lvols, st_date, end_date)  
        DeviceStatObject(clusters, st_date, end_date)
        NodeStatObject(clusters, st_date, end_date)
        ClusterStatObject(clusters, st_date, end_date)
        
        logger.info("Completed a cleaning cycle. Sleeping until next interval.")
        time.sleep(constants.FDB_CHECK_INTERVAL_SEC)

    except Exception as e:
        logger.error(f"An error occurred in the main loop: {e}")
        time.sleep(10)
