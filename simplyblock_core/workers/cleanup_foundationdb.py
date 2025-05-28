# coding=utf-8
import time
import os

from simplyblock_core import constants, utils
from simplyblock_core.db_controller import DBController


logger = utils.get_logger(__name__)

logger.info("Starting FDB cleanup script...")

db_controller = DBController()
logger.debug("Database controller initialized.")

deletion_interval = os.getenv('LOG_DELETION_INTERVAL', '7d')

def PoolStatObject(lvols, st_date, end_date):
    for lvol in lvols:
        index = "object/PoolStatObject/%s/%s/" % (lvol.pool_uuid, lvol.pool_uuid)
        start = index + str(st_date)
        end = index + str(end_date)
        try:
            db_controller.kv_store.clear_range(start.encode('utf-8'), end.encode('utf-8'))
            logger.info(f"Cleared PoolStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear PoolStatObject for {lvol.pool_uuid}: {e}")

def LVolStatObject(lvols, st_date, end_date):
    for lvol in lvols:
        index = "object/LVolStatObject/%s/%s/" % (lvol.pool_uuid, lvol.uuid)
        start = index + str(st_date)
        end = index + str(end_date)
        try:
            db_controller.kv_store.clear_range(start.encode('utf-8'), end.encode('utf-8'))
            logger.info(f"Cleared LVolStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear LVolStatObject for {lvol.uuid}: {e}")

def DeviceStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for node in snodes:
            for device in node.nvme_devices:
                device_id = device.get_id()
                index = "object/DeviceStatObject/%s/%s/" % (cluster_id, device_id)
                start = index + str(st_date)
                end = index + str(end_date)
                try:
                    db_controller.kv_store.clear_range(start.encode('utf-8'), end.encode('utf-8'))
                    logger.info(f"Cleared DeviceStatObject data from {start} to {end}")
                except Exception as e:
                    logger.error(f"Failed to clear DeviceStatObject for {device_id}: {e}")

def NodeStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for node in snodes:
            node_id = node.get_id()
            index = "object/NodeStatObject/%s/%s/" % (cluster_id, node_id)
            start = index + str(st_date)
            end = index + str(end_date)
            try:
                db_controller.kv_store.clear_range(start.encode('utf-8'), end.encode('utf-8'))
                logger.info(f"Cleared NodeStatObject data from {start} to {end}")
            except Exception as e:
                logger.error(f"Failed to clear NodeStatObject for {node_id}: {e}")

def ClusterStatObject(clusters, st_date, end_date):
    for cl in clusters:
        cluster_id = cl.get_id()
        index = "object/ClusterStatObject/%s/%s/" % (cluster_id, cluster_id)
        start = index + str(st_date)
        end = index + str(end_date)
        try:
            db_controller.kv_store.clear_range(start.encode('utf-8'), end.encode('utf-8'))
            logger.info(f"Cleared ClusterStatObject data from {start} to {end}")
        except Exception as e:
            logger.error(f"Failed to clear ClusterStatObject for {cluster_id}: {e}")

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
        lvols = db_controller.get_lvols()  # pass
        logger.info("Clusters and logical volumes successfully retrieved for cleanup.")
        
        st_date = "" # seconds
        end_date = int(time.time()) - convert_to_seconds(deletion_interval)
        
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
