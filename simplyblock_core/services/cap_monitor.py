# coding=utf-8

import time
from datetime import datetime, timezone

from simplyblock_core import db_controller, constants, cluster_ops, utils
from simplyblock_core.controllers import cluster_events
from simplyblock_core.models.cluster import Cluster


logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()
last_event = {}

logger.info("Starting capacity monitoring service...")
while True:
    clusters = db.get_clusters()
    for cl in clusters:
        logger.info(f"Checking cluster: {cl.get_id()}")
        records = db.get_cluster_capacity(cl, 1)
        if not records:
            logger.error("Cluster capacity record not found!")
            continue

        size_util = records[0].size_util
        size_prov = records[0].size_prov_util
        logger.debug(f"cluster abs util: {size_util}, prov util: {size_prov}")
        if cl.cap_crit:
            if cl.cap_crit <= size_util:
                logger.warning(f"Cluster absolute cap critical, util: {size_util}% of cluster util: {cl.cap_crit}, "
                               f"putting the cluster in read_only mode")
                if cl.id in last_event:
                    diff = datetime.now(timezone.utc) - datetime.fromtimestamp(last_event[cl.id]["date"]/1000, timezone.utc)
                    if diff and diff.total_seconds() > 60 * 15:
                        ev = cluster_events.cluster_cap_crit(cl, size_util)
                        if ev:
                            last_event[cl.id] = ev
                else:
                    ev = cluster_events.cluster_cap_crit(cl, size_util)
                    if ev:
                        last_event[cl.id] = ev
                if cl.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
                    cluster_ops.cluster_set_read_only(cl.get_id())
            else:
                if cl.status == Cluster.STATUS_READONLY:
                    cluster_ops.cluster_set_active(cl.get_id())

        if cl.cap_warn:
            if cl.cap_warn < size_util < cl.cap_crit:
                logger.warning(f"Cluster absolute cap warning, util: {size_util}% of cluster util: {cl.cap_warn}")
                cluster_events.cluster_cap_warn(cl, size_util)

        if cl.prov_cap_crit:
            if cl.prov_cap_crit < size_prov:
                logger.warning(f"Cluster provisioned cap critical, util: {size_prov}% of cluster util: {cl.prov_cap_crit}")
                cluster_events.cluster_prov_cap_crit(cl, size_prov)

        if cl.prov_cap_warn:
            if cl.prov_cap_warn < size_prov < cl.prov_cap_crit:
                logger.warning(f"Cluster provisioned cap warning, util: {size_prov}% of cluster util: {cl.prov_cap_warn}")
                cluster_events.cluster_prov_cap_warn(cl, size_prov)

    time.sleep(constants.CAP_MONITOR_INTERVAL_SEC)
