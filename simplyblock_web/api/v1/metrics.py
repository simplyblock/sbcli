import logging

from flask import Blueprint
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import db_controller


from prometheus_client import generate_latest
from flask import Response
from prometheus_client import Gauge, CollectorRegistry


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

def get_device_metrics(registry: CollectorRegistry):
    labels = ['cluster', "cluster_name", "snode", "device"]
    return {
        "device_" + k: Gauge("device_" + k, "device_" + k, labelnames=labels, registry=registry)
        for k in
        io_stats_keys + ["status_code", "health_check"]
    }


def get_snode_metrics(registry: CollectorRegistry):
    labels = ['cluster', "cluster_name", "snode", "hostname"]
    return {
        **{
            "snode_" + k: Gauge("snode_" + k, "snode_" + k, labelnames=labels, registry=registry)
            for k in
            io_stats_keys + ["status_code", "health_check"]
        },
        "snode_cpu_busy_percentage": Gauge(
            "snode_cpu_busy_percentage",
            "Per-thread CPU Busy %",
            labelnames=['cluster', "cluster_name", 'snode', 'hostname', 'thread_name'],
            registry=registry
        ),
        "snode_cpu_core_utilization": Gauge(
            "snode_cpu_core_utilization",
            "Per-core CPU Utilization %",
            labelnames=['cluster', "cluster_name", 'snode', 'hostname', 'core_id', 'thread_names'],
            registry=registry
        ),
    }


def get_cluster_metrics(registry: CollectorRegistry):
    labels = ['cluster', "cluster_name"]
    return {
        "cluster_" + k: Gauge("cluster_" + k, "cluster_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "prov_cap_crit", "cap_crit"]
    }


def get_lvol_metrics(registry: CollectorRegistry):
    labels = ['cluster', "cluster_name", "pool", "lvol", "lvol_name", "pvc_name"]
    return {
        "lvol_" + k: Gauge("lvol_" + k, "lvol_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code", "health_check"]
    }


def get_pool_metrics(registry: CollectorRegistry):
    labels = ['cluster', "cluster_name", "pool", "name"]
    return {
        "pool_" + k: Gauge("pool_" + k, "pool_" + k, labelnames=labels, registry=registry)
        for k in io_stats_keys + ["status_code"]
    }


@bp.route('/cluster/metrics', methods=['GET'])
def get_data():

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
            object_data =  cl.get_clean_dict()

            ng = cluster_gauges
            for g in ng:
                v = g.replace("cluster_", "")
                if v in data:
                    ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name).set(data[v])
                elif v == "status_code":
                    ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name).set(cl.get_status_code())
                elif v == "prov_cap_crit":
                    ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name).set(object_data[v])
                elif v == "cap_crit":
                    ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name).set(object_data[v])

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
                ng = snode_gauges
                for g in ng:
                    v = g.replace("snode_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), hostname=node.hostname).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), hostname=node.hostname).set(node.get_status_code())
                    elif v == "health_check":
                        # health_check is None when not applicable (node not
                        # ONLINE/DOWN); report NaN so it isn't read as unhealthy.
                        hc = float('nan') if node.health_check is None else float(node.health_check)
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), hostname=node.hostname).set(hc)
                    if reactor_data and "reactors" in reactor_data:
                        for reactor in reactor_data.get("reactors", []):
                            lcore = reactor.get("lcore")
                            core_idle = reactor.get("idle", 0)
                            core_busy = reactor.get("busy", 0)
                            irq = reactor.get("irq", 0)
                            sys = reactor.get("sys", 0)

                            thread_names = ", ".join(thread["name"] for thread in reactor.get("lw_threads", []))
                            if v == "cpu_busy_percentage":
                                for thread in reactor.get("lw_threads", []):
                                    thread_name = thread.get("name")
                                    thread_id = thread.get("id")
                                    thread_busy = thread_busy_map.get(thread_id, 0)

                                    total_core_cycles = core_busy + core_idle
                                    cpu_usage_percent = (thread_busy / total_core_cycles) * 100 if total_core_cycles > 0 else 0

                                    ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), hostname=node.hostname, thread_name=thread_name).set(cpu_usage_percent)

                            elif v == "cpu_core_utilization":

                                total_cycle = core_busy + irq + sys
                                total_with_idle = total_cycle + core_idle
                                core_utilization = (total_cycle / total_with_idle) * 100 if total_with_idle > 0 else 0
                                ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), hostname=node.hostname, core_id=str(lcore), thread_names=thread_names).set(core_utilization)


            for device in node.nvme_devices:

                logger.info("Getting device stats: %s", device.uuid)
                if device.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                    logger.info(f"Device is skipped: {device.get_id()} status: {device.status}")
                    continue

                device_records = db.get_device_stats(device, 1)
                if device_records:
                    data = device_records[0].get_clean_dict()
                    ng = device_gauges
                    for g in ng:
                        v = g.replace("device_", "")
                        if v in data:
                            ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), device=device.get_id()).set(data[v])
                        elif v == "status_code":
                            ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), device=device.get_id()).set(
                                device.get_status_code())
                        elif v == "health_check":
                            hc = float('nan') if device.health_check is None else float(device.health_check)
                            ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, snode=node.get_id(), device=device.get_id()).set(
                                hc)


        for pool in db.get_pools():

            pool_records = db.get_pool_stats(pool, 1)
            if pool_records:
                data = pool_records[0].get_clean_dict()
                ng = pool_gauges
                for g in ng:
                    v = g.replace("pool_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, name=pool.pool_name, pool=pool.get_id()).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, name=pool.pool_name, pool=pool.get_id()).set(
                            pool.get_status_code())

        for lvol in db.get_lvols(cl.get_id()):
            lvol_records = db.get_lvol_stats(lvol, limit=1)
            if lvol_records:
                data = lvol_records[0].get_clean_dict()
                ng = lvol_gauges
                for g in ng:
                    v = g.replace("lvol_", "")
                    if v in data:
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, lvol=lvol.get_id(), lvol_name=lvol.lvol_name, pvc_name=lvol.pvc_name, pool=lvol.pool_name).set(data[v])
                    elif v == "status_code":
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, lvol=lvol.get_id(), lvol_name=lvol.lvol_name, pvc_name=lvol.pvc_name, pool=lvol.pool_name).set(
                            lvol.get_status_code())
                    elif v == "health_check":
                        ng[g].labels(cluster=cl.get_id(), cluster_name=cl.cluster_name, lvol=lvol.get_id(), lvol_name=lvol.lvol_name, pvc_name=lvol.pvc_name, pool=lvol.pool_name).set(
                            lvol.health_check)


    return Response(generate_latest(registry), mimetype='text/plain; version=0.0.4; charset=utf-8')
