
import time
from datetime import UTC, datetime

from simplyblock_core import (
    capacity_guard,
    cluster_ops,
    constants,
    db_controller,
    utils,
)
from simplyblock_core.controllers import (
    cluster_events,
    device_controller,
    fdb_backup_controller,
    mgmt_events,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
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
            avail_bytes = int(nodes_stats[node_name].get("avail_bytes")[0])
            size_bytes = int(nodes_stats[node_name].get("size_bytes")[0])
            dist_util = int( 100 - ((avail_bytes * 100) / size_bytes))
            if dist_util > 90:
                logger.warning(f"Node {node_name} disk util: {dist_util}%")
                mgmt_events.dist_usage_warning(node_name, dist_util)

def check_api_metrics(cluster):
    prom_client = PromClient(cluster.get_id())
    api_stats = prom_client.get_api_metrics(history="5m")
    data = []
    if api_stats:
        for node in api_stats:
            api_stat = api_stats[node]
            for i in range(len(api_stat.get("seconds_count"))):
                http_request_duration_seconds_count = float(api_stat.get("seconds_count")[i])
                http_request_duration_seconds_sum = float(api_stat.get("seconds_sum")[i])
                data.append(http_request_duration_seconds_sum/http_request_duration_seconds_count)

        avg_api_req_duration = sum(data)/len(data)
        logger.debug(f"avg_api_req_duration: {avg_api_req_duration}")
        if avg_api_req_duration > 15:
            logger.warning(f"API request duration is too high: {avg_api_req_duration}s")
            mgmt_events.api_latency_warning(cluster.get_id(), avg_api_req_duration)


def _stripe_width(cl):
    return int(getattr(cl, "distr_ndcs", 0) or 0) + int(getattr(cl, "distr_npcs", 0) or 0)


def _devices_full(cl):
    """Why *cl* must go read_only although its average is below cap_crit, or "".

    A new stripe needs ndcs + npcs distinct devices with room (the distrib
    never puts two chunks of one stripe on one device). When fewer devices
    than that are below cap_crit, the next writes that need a new location
    fail. They fail whatever the cluster average is: bed bf24 (2026-11-14,
    5 x 1 device, 1+2) had three devices full and two at about 70% at an 88%
    average, below the 90% threshold, and the clients got EIO. Devices without
    a capacity reading are left out, and so is a cluster that has fewer
    counted devices than a stripe needs: no reading is not a full device.
    """
    width = _stripe_width(cl)
    if not cl.cap_crit or width < 1:
        return ""
    try:
        devices = capacity_guard.device_utilizations(db, cl.get_id())
    except Exception as e:                                # noqa: BLE001
        logger.warning(f"Device capacity of cluster {cl.get_id()} unreadable: {e}")
        return ""
    if len(devices) < width:
        return ""
    room = [d for d in devices if d[2] < cl.cap_crit]
    if len(room) >= width:
        return ""
    full = sorted(f"{dev.get_id()}={util}%" for _n, dev, util in devices if util >= cl.cap_crit)
    return (f"only {len(room)} device(s) below {cl.cap_crit}% where a stripe needs {width} "
            f"(full: {', '.join(full)})")


def _stop_allocating_on_full_devices(cl):
    """Mark an online device at or above cap_crit cannot_allocate.

    The distrib then places new data on the other devices, while the device
    still has room for the writes it already holds data for. Before this, a
    device was marked only when it was full already (error_write_cannot_allocate
    from the data plane), and the writes in flight that needed space on it
    failed. Only while enough other devices have room for a whole stripe
    (_devices_full decides otherwise). device_monitor brings the device back
    online once it is below cap_crit again.
    """
    try:
        devices = capacity_guard.device_utilizations(db, cl.get_id())
    except Exception as e:                                # noqa: BLE001
        logger.warning(f"Device capacity of cluster {cl.get_id()} unreadable: {e}")
        return
    for node, dev, util in devices:
        if util < cl.cap_crit or dev.status != NVMeDevice.STATUS_ONLINE:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.warning(f"Device {dev.get_id()} on node {node.get_id()} is at {util}% capacity, "
                       f"at or above the cluster's critical threshold of {cl.cap_crit}%: "
                       f"no new allocations on it")
        try:
            device_controller.device_set_state(dev.get_id(), NVMeDevice.STATUS_CANNOT_ALLOCATE)
        except Exception as e:                            # noqa: BLE001
            logger.error(f"Could not set device {dev.get_id()} cannot_allocate: {e}")


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
            devices_full = _devices_full(cl)
            if cl.cap_crit:
                if cl.cap_crit <= size_util or devices_full:
                    logger.warning(f"Cluster absolute cap critical, util: {size_util}% of cluster util: {cl.cap_crit}, "
                                   f"{devices_full + ', ' if devices_full else ''}"
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
                    elif cl.status in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED]:
                        _stop_allocating_on_full_devices(cl)

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
