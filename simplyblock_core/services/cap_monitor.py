# coding=utf-8

import time
from datetime import datetime, UTC

from simplyblock_core import db_controller, constants, cluster_ops, utils
from simplyblock_core.controllers import cluster_events, fdb_backup_controller, mgmt_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.prom_client import PromClient

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()
last_event: dict[str, dict] = {}

def create_fdb_backup_if_needed(cluster):
    tasks = fdb_backup_controller.get_backup_tasks(cluster.get_id())
    if not tasks:
        fdb_backup_controller.add_backup_task(cluster.get_id())
        return
    tasks.reverse()
    last_backup_task = tasks[0]
    if last_backup_task and last_backup_task.status == JobSchedule.STATUS_DONE:
        if last_backup_task.date + cluster.backup_frequency_seconds < time.time():
            fdb_backup_controller.add_backup_task(cluster.get_id())

def check_mgmt_disk_util_docker(cluster):
    prom_client = PromClient(cluster.get_id())
    nodes_stats = prom_client.get_node_filesystem_metrics(history="5m")
    if nodes_stats:
        for node_name in nodes_stats:
            avail_bytes = nodes_stats[node_name].get("avail_bytes")[0]
            size_bytes = nodes_stats[node_name].get("size_bytes")[0]
            dist_util = int( 100 - ((avail_bytes * 100) / size_bytes))
            if dist_util > 90:
                logger.warning(f"Node {node_name} disk util: {dist_util}%")
                mgmt_events.dist_usage_warning(node_name, dist_util)

def check_api_metrics(cluster):
    prom_client = PromClient(cluster.get_id())
    api_stats = prom_client.get_api_metrics(history="5m")
    data = []
    if api_stats:
        for api_stat in api_stats:
            for i in range(len(api_stat.get("seconds_count"))):
                http_request_duration_seconds_count = api_stat.get("seconds_count")[i]
                http_request_duration_seconds_sum = api_stats.get("seconds_sum")[i]
                data.append(http_request_duration_seconds_sum/http_request_duration_seconds_count)

        avg_api_req_duration = sum(data)/len(data)
        logger.debug(f"avg_api_req_duration: {avg_api_req_duration}")
        if avg_api_req_duration > 15:
            logger.warning(f"API request duration is too high: {avg_api_req_duration}s")
            mgmt_events.api_latency_warning(cluster.get_id(), avg_api_req_duration)


def main():
    logger.info("Starting capacity monitoring service...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cl in clusters:
            create_fdb_backup_if_needed(cl)
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
                        diff = datetime.now(UTC) - datetime.fromtimestamp(last_event[cl.id]["date"]/1000, UTC)
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

            if cl.mode == "docker":
                check_mgmt_disk_util_docker(cl)

            check_api_metrics(cl)

        time.sleep(constants.CAP_MONITOR_INTERVAL_SEC)


if __name__ == "__main__":
    main()
