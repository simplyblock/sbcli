from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
from simplyblock_core.services.spdk import client as spdk_client
from simplyblock_core import constants, db_controller, utils
from simplyblock_core.rpc_client import RPCClient

logger = utils.get_logger(__name__)

PUSHGATEWAY_URL = "http://pushgateway:9091"

db_controller = db_controller.DBController()

def push_metrics(reactor_data, thread_data, cluster_id, snode):
    """Formats and pushes SPDK metrics to Prometheus Pushgateway."""
    registry = CollectorRegistry()
    tick_rate_gauge = Gauge('tick_rate', 'SPDK Tick Rate', ['cluster', 'snode', 'node_ip'], registry=registry)
    cpu_busy_gauge = Gauge('cpu_busy_percentage', 'Per-thread CPU Busy Percentage', ['cluster', 'snode', 'node_ip', 'thread_name'], registry=registry)
    cpu_utilization_gauge = Gauge('cpu_core_utilization', 'Per-core CPU Utilization', ['cluster', 'snode', 'node_ip', 'core_id', 'thread_names'], registry=registry)

    snode_id = snode.id
    snode_ip = snode.mgmt_ip
    tick_rate = reactor_data.get("tick_rate")
    if tick_rate is not None:
        tick_rate_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=snode_ip).set(tick_rate)

    thread_busy_map = {t["id"]: t["busy"] for t in thread_data.get("threads", [])}

    for reactor in reactor_data.get("reactors", []):
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
            
            cpu_usage_percent = (thread_busy / (core_busy + core_idle)) * 100 if (core_busy + core_idle) > 0 else 0

            cpu_busy_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=snode_ip, thread_name=thread_name).set(cpu_usage_percent)

        total_cycle = core_busy + irq + sys
        core_utilization_percent = (total_cycle / (total_cycle + core_idle)) * 100 if (total_cycle + core_idle)  > 0 else 0

        cpu_utilization_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=snode_ip, core_id=str(lcore), thread_names=thread_names).set(core_utilization_percent)

    push_to_gateway(PUSHGATEWAY_URL, job='metricsgateway', grouping_key={'cluster': str(cluster_id), 'snode': str(snode_id)}, registry=registry)
    logger.info("Metrics pushed successfully")

logger.info("Starting spdk stats collector...")
while True:
    clusters = db_controller.get_clusters()
    for cluster in clusters:
        cluster_id = cluster.get_id()
        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        for snode in nodes:
            if len(snode.nvme_devices) > 0:
                rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=3*60, retry=10)

                reactor_data = rpc_client.framework_get_reactors()
                thread_data = rpc_client.thread_get_stats()  
                if reactor_data and "reactors" in reactor_data and "threads" in thread_data:
                    push_metrics(reactor_data, thread_data, cluster_id, snode)
            else:
                logger.info(f"Skipping snode {snode.mgmt_ip} with NO NVMe devices.")
    
    time.sleep(constants.SPDK_STAT_COLLECTOR_INTERVAL_SEC)
