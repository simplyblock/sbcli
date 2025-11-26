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
from simplyblock_core.rpc_client import RPCClient

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

            lvol_list = db.get_lvols_by_node_id(snode.get_id())

            if not lvol_list:
                continue

            if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:

                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=3, retry=2)

    time.sleep(constants.LVOL_SCHEDULER_INTERVAL_SEC)
