import logging

from flask import Blueprint, Response
from prometheus_client import CollectorRegistry, Gauge, generate_latest

from simplyblock_core import db_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger(__name__)

bp = Blueprint("metrics", __name__)

db = db_controller.DBController()

io_stats_keys = [
    "date",
    "read_bytes",
    "read_bytes_ps",
    "read_io_ps",
    "read_io",
    "read_latency_ps",
    "write_bytes",
    "write_bytes_ps",
    "write_io",
    "write_io_ps",
    "write_latency_ps",
    "size_total",
    "size_prov",
    "size_used",
    "size_free",
    "size_util",
    "size_prov_util",
    "read_latency_ticks",
    "record_duration",
    "record_end_time",
    "record_start_time",
    "unmap_bytes",
    "unmap_bytes_ps",
    "unmap_io",
    "unmap_io_ps",
    "unmap_latency_ps",
    "unmap_latency_ticks",
    "write_latency_ticks",
]


# Every gauge family below is constructed against a registry that lives only
# for the duration of one request (see get_data). They must NOT be cached at
# module scope: a Gauge keeps a child object per label set it has ever been
# given, and these label sets carry entity identity (lvol/pvc_name/device/...),
# so a process-lifetime gauge keeps reporting volumes that were deleted days
# ago. Constructing them per request is what bounds the exposition to the
# entities that currently exist.

def get_device_metrics(registry):
    labels = ['cluster', "cluster_name", "snode", "device"]
    return {
        "device_" + k: Gauge("device_" + k, "device_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "health_check"]
    }


def get_snode_metrics(registry):
    labels = ['cluster', "cluster_name", "snode", "hostname"]
    gauges = {
        "snode_" + k: Gauge("snode_" + k, "snode_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "health_check"]
    }
    # Additional SPDK-specific metrics
    gauges["snode_cpu_busy_percentage"] = Gauge(
        "snode_cpu_busy_percentage",
        "Per-thread CPU Busy %",
        labelnames=['cluster', "cluster_name", 'snode', 'hostname', 'thread_name'],
        registry=registry
    )
    gauges["snode_cpu_core_utilization"] = Gauge(
        "snode_cpu_core_utilization",
        "Per-core CPU Utilization %",
        labelnames=['cluster', "cluster_name", 'snode', 'hostname', 'core_id', 'thread_names'],
        registry=registry
    )
    return gauges


def get_cluster_metrics(registry):
    labels = ['cluster', "cluster_name"]
    return {
        "cluster_" + k: Gauge("cluster_" + k, "cluster_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "prov_cap_crit", "cap_crit"]
    }


def get_lvol_metrics(registry):
    labels = ['cluster', "cluster_name", "pool", "lvol", "lvol_name", "pvc_name"]
    return {
        "lvol_" + k: Gauge("lvol_" + k, "lvol_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "health_check"]
    }


def get_pool_metrics(registry):
    labels = ['cluster', "cluster_name", "pool", "name"]
    return {
        "pool_" + k: Gauge("pool_" + k, "pool_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code"]
    }


def _gauge_value(value):
    """health_check is Optional[bool]: None means "not applicable" rather than
    "unhealthy". Gauge.set(None) raises TypeError and would fail the whole
    scrape, so report NaN, which Prometheus reads as "no sample" instead of 0.
    """
    return float('nan') if value is None else float(value)


@bp.route('/cluster/metrics', methods=['GET'])
def get_data():
    # One registry per request, and every gauge family built exactly once here,
    # before the loops below.
    #
    # Both halves matter. A registry that outlives the request accumulates a
    # child series per label set forever, so deleted volumes keep being
    # exported (observed on a customer cluster: 94 live volumes but ~350k
    # series / 83 MB / 5 s per scrape, growing ~7 MB/day at ~1.2k volume
    # deletions/day, until Prometheus could no longer usefully ingest it).
    # And a Gauge registers its name with the registry on construction, so
    # building a family inside the per-entity loops instead of here raises
    # DuplicateTimeseries on the second node/device/pool/volume.
    registry = CollectorRegistry()
    cluster_gauges = get_cluster_metrics(registry)
    snode_gauges = get_snode_metrics(registry)
    device_gauges = get_device_metrics(registry)
    pool_gauges = get_pool_metrics(registry)
    lvol_gauges = get_lvol_metrics(registry)

    clusters = db.get_clusters()
    for cl in clusters:

        records = db.get_cluster_stats(cl, 1)
        if records:
            data = records[0].get_clean_dict()
            object_data = cl.get_clean_dict()

            for g in cluster_gauges:
                v = g.replace("cluster_", "")
                # Resolve the value first and skip if there isn't one: calling
                # .labels() creates the child series, and an un-set child is
                # exported as 0. A metric with no source value must stay absent
                # rather than read as a real zero.
                if v in data:
                    value = data[v]
                elif v == "status_code":
                    value = cl.get_status_code()
                elif v in ("prov_cap_crit", "cap_crit"):
                    value = object_data[v]
                else:
                    continue
                cluster_gauges[g].labels(
                    cluster=cl.get_id(), cluster_name=cl.cluster_name).set(value)

        snodes = db.get_storage_nodes_by_cluster_id(cl.get_id())
        for node in snodes:
            logger.info("Node: %s", node.get_id())
            if node.status != StorageNode.STATUS_ONLINE:
                logger.info("Node is not online, skipping")
                continue

            if not node.nvme_devices:
                logger.error("No devices found in node: %s", node.get_id())
                continue

            rpc_client = node.rpc_client(timeout=3*60, retry=10)

            reactor_data = rpc_client.framework_get_reactors()
            thread_data = rpc_client.thread_get_stats()

            thread_busy_map = {t["id"]: t["busy"] for t in thread_data.get("threads", [])}

            node_records = db.get_node_stats(node, 1)
            if node_records:
                data = node_records[0].get_clean_dict()
                for g in snode_gauges:
                    v = g.replace("snode_", "")
                    if v in data:
                        value = data[v]
                    elif v == "status_code":
                        value = node.get_status_code()
                    elif v == "health_check":
                        value = _gauge_value(node.health_check)
                    else:
                        continue  # incl. cpu_*, which come from reactor_data below
                    snode_gauges[g].labels(
                        cluster=cl.get_id(), cluster_name=cl.cluster_name,
                        snode=node.get_id(), hostname=node.hostname).set(value)

                # Walked once per node. This used to sit inside the loop above,
                # so every reactor was re-walked once per gauge name (~33x per
                # node) while only two of those names ever consumed the result.
                for reactor in (reactor_data or {}).get("reactors", []):
                    lcore = reactor.get("lcore")
                    core_idle = reactor.get("idle", 0)
                    core_busy = reactor.get("busy", 0)
                    irq = reactor.get("irq", 0)
                    sys = reactor.get("sys", 0)

                    thread_names = ", ".join(thread["name"] for thread in reactor.get("lw_threads", []))

                    for thread in reactor.get("lw_threads", []):
                        thread_name = thread.get("name")
                        thread_id = thread.get("id")
                        thread_busy = thread_busy_map.get(thread_id, 0)

                        total_core_cycles = core_busy + core_idle
                        cpu_usage_percent = (thread_busy / total_core_cycles) * 100 if total_core_cycles > 0 else 0

                        snode_gauges["snode_cpu_busy_percentage"].labels(
                            cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(),
                            hostname=node.hostname, thread_name=thread_name).set(cpu_usage_percent)

                    total_cycle = core_busy + irq + sys
                    total_with_idle = total_cycle + core_idle
                    core_utilization = (total_cycle / total_with_idle) * 100 if total_with_idle > 0 else 0
                    snode_gauges["snode_cpu_core_utilization"].labels(
                        cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(),
                        hostname=node.hostname, core_id=str(lcore), thread_names=thread_names).set(core_utilization)

            for device in node.nvme_devices:

                logger.info("Getting device stats: %s", device.uuid)
                if device.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    logger.info(f"Device is skipped: {device.get_id()} status: {device.status}")
                    continue

                device_records = db.get_device_stats(device, 1)
                if device_records:
                    data = device_records[0].get_clean_dict()
                    for g in device_gauges:
                        v = g.replace("device_", "")
                        if v in data:
                            value = data[v]
                        elif v == "status_code":
                            value = device.get_status_code()
                        elif v == "health_check":
                            value = _gauge_value(device.health_check)
                        else:
                            continue
                        device_gauges[g].labels(
                            cluster=cl.get_id(), cluster_name=cl.cluster_name,
                            snode=node.get_id(), device=device.get_id()).set(value)

        for pool in db.get_pools():

            pool_records = db.get_pool_stats(pool, 1)
            if pool_records:
                data = pool_records[0].get_clean_dict()
                for g in pool_gauges:
                    v = g.replace("pool_", "")
                    if v in data:
                        value = data[v]
                    elif v == "status_code":
                        value = pool.get_status_code()
                    else:
                        continue
                    pool_gauges[g].labels(
                        cluster=cl.get_id(), cluster_name=cl.cluster_name,
                        name=pool.pool_name, pool=pool.get_id()).set(value)

        for lvol in db.get_lvols(cl.get_id()):
            lvol_records = db.get_lvol_stats(lvol, limit=1)
            if lvol_records:
                data = lvol_records[0].get_clean_dict()
                for g in lvol_gauges:
                    v = g.replace("lvol_", "")
                    if v in data:
                        value = data[v]
                    elif v == "status_code":
                        value = lvol.get_status_code()
                    elif v == "health_check":
                        value = _gauge_value(lvol.health_check)
                    else:
                        continue
                    lvol_gauges[g].labels(
                        cluster=cl.get_id(), cluster_name=cl.cluster_name, lvol=lvol.get_id(),
                        lvol_name=lvol.lvol_name, pvc_name=lvol.pvc_name, pool=lvol.pool_name).set(value)

    return Response(generate_latest(registry), mimetype='text/plain; version=0.0.4; charset=utf-8')
