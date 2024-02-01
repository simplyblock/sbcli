# coding=utf-8
import logging
import os

import time
import sys
import uuid



from simplyblock_core import kv_store
from simplyblock_core.controllers import cluster_events


# configure logging
logger_handler = logging.StreamHandler(stream=sys.stdout)
logger_handler.setFormatter(logging.Formatter('%(asctime)s: %(levelname)s: %(message)s'))
logger = logging.getLogger()
logger.addHandler(logger_handler)
logger.setLevel(logging.DEBUG)

# get DB controller
db_controller = kv_store.DBController()

logger.info("Starting capacity monitoring service...")
while True:
    clusters = db_controller.get_clusters()
    for cl in clusters:
        logger.info(f"Checking cluster: {cl.get_id()}")
        records = db_controller.get_cluster_capacity(cl, 1)
        if not records:
            logger.error("Cluster capacity record not found!")
            continue

        size_util = records[0].size_util
        size_prov = records[0].size_prov_util
        logger.debug(f"cluster abs util: {size_util}, prov util: {size_prov}")

        if cl.cap_crit and cl.cap_crit < size_util:
            logger.warning(f"Cluster absolute cap critical, util: {size_util}% of cluster util: {cl.cap_crit}")
            cluster_events.cluster_cap_crit(cl, size_util)
        elif cl.cap_warn and cl.cap_warn < size_util:
            logger.warning(f"Cluster absolute cap warning, util: {size_util}% of cluster util: {cl.cap_warn}")
            cluster_events.cluster_cap_warn(cl, size_util)

        if cl.prov_cap_crit and cl.prov_cap_crit < size_prov:
            logger.warning(f"Cluster provisioned cap critical, util: {size_prov}% of cluster util: {cl.prov_cap_crit}")
            cluster_events.cluster_prov_cap_crit(cl, size_prov)
        elif cl.prov_cap_warn and cl.prov_cap_warn < size_prov:
            logger.warning(f"Cluster provisioned cap warning, util: {size_prov}% of cluster util: {cl.prov_cap_warn}")
            cluster_events.cluster_prov_cap_warn(cl, size_prov)

    # time.sleep(constants.DEV_STAT_COLLECTOR_INTERVAL_SEC)
    time.sleep(60)
