# coding=utf-8
"""
Filename: lvol_scheduler.py
Author: Hamdy Khader
Email: hamdy@simplyblock.io
Description:
LVol scheduler service will collect, calculate and store the required metric parameters for lvol scheduler algorithm
to make the following decisions:
    - Which node should be used for hosting the next lvol?
    - Is lvol transfer required because of high RAM consumption ? if so then which lvol/s to which node?

Here we have the metric parameters used (per node):
    ram_utilization
        a timely reading of random memory utilization from file /etc/meminfo of the node
    lvol_utilization
        sum of the consumed data size (bytes) for all lvol on the node
    lvol_count
        count of lvols on the node
"""
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)


# get DB controller
db = db_controller.DBController()

logger.info("Starting stats collector...")
while True:

    for cluster in db.get_clusters():

        if cluster.status in [Cluster.STATUS_INACTIVE, Cluster.STATUS_UNREADY, Cluster.STATUS_IN_ACTIVATION]:
            logger.warning(f"Cluster {cluster.get_id()} is in {cluster.status} state, skipping")
            continue

        for snode in db.get_storage_nodes_by_cluster_id(cluster.get_id()):

            if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
                continue

            node_info = snode.snode_api().info()
            ram_utilization= 0
            lvol_utilization = 0
            lvol_count = 0

            # check for memory
            if "memory_details" in node_info and node_info['memory_details']:
                memory_details = node_info['memory_details']
                logger.info("Node Memory info")
                logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
                logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
                ram_utilization = int(memory_details['free']/memory_details['total']*100)

            lvol_list = db.get_lvols_by_node_id(snode.get_id())
            lvol_count = len(lvol_list)
            for lvol in lvol_list:
                records = db.get_lvol_stats(lvol, 1)
                if records:
                    lvol_utilization += records[0].size_used

    time.sleep(constants.LVOL_SCHEDULER_INTERVAL_SEC)
