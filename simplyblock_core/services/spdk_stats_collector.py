from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
from simplyblock_core.services.spdk import client as spdk_client
from simplyblock_core import db_controller
from simplyblock_core.rpc_client import RPCClient
import socket
import fcntl
import struct
import subprocess


PUSHGATEWAY_URL = "http://pushgateway:9091"
SPDK_SOCK_PATH = "/var/tmp/spdk.sock"
cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

db_controller = db_controller.DBController()

def get_node_ip(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,
        struct.pack('256s', ifname[:15].encode('utf-8'))
    )[20:24])

NODE_IP = get_node_ip('eth0')

def run_command(cmd):
    try:
        process = subprocess.Popen(
            cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()
        return stdout.strip().decode("utf-8")
    except Exception as e:
        return str(e)

def push_metrics(ret,cluster_id,snode_id):
    """Formats and pushes SPDK metrics to Prometheus Pushgateway."""
    registry = CollectorRegistry()
    tick_rate_gauge = Gauge('tick_rate', 'SPDK Tick Rate', ['cluster', 'snode', 'node_ip'], registry=registry)
    cpu_busy_gauge = Gauge('cpu_busy_percentage', 'CPU Busy Percentage', ['cluster', 'snode', 'node_ip', 'thread_name'], registry=registry)
    pollers_count_gauge = Gauge('pollers_count', 'Number of pollers', ['cluster', 'snode', 'node_ip', 'poller_type', 'thread_name'], registry=registry)

    tick_rate = ret.get("tick_rate")
    if tick_rate is not None:
        tick_rate_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=NODE_IP).set(tick_rate)
    for thread in ret.get("threads", []):
        thread_name = thread.get("name")
        busy = thread.get("busy", 0)
        idle = thread.get("idle", 0)

        total_cycles = busy + idle
        cpu_usage_percent = (busy / total_cycles) * 100 if total_cycles > 0 else 0
        cpu_busy_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=NODE_IP, thread_name=thread_name).set(cpu_usage_percent)

        pollers_count_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=NODE_IP, poller_type="active", thread_name=thread_name).set(thread.get("active_pollers_count", 0))
        pollers_count_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=NODE_IP, poller_type="timed", thread_name=thread_name).set(thread.get("timed_pollers_count", 0))
        pollers_count_gauge.labels(cluster=cluster_id, snode=snode_id, node_ip=NODE_IP, poller_type="paused", thread_name=thread_name).set(thread.get("paused_pollers_count", 0))
    
    push_to_gateway(PUSHGATEWAY_URL, job='metricsgateway', registry=registry)
    print("Metrics pushed successfully")

if __name__ == "__main__":
    while True:
        try:
            clusters = db_controller.get_clusters()
            for cluster in clusters:
                cluster_id = cluster.get_id()
                nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
                for snode in nodes:
                    rpc_client = RPCClient(
                    snode.mgmt_ip, snode.rpc_port,
                    snode.rpc_username, snode.rpc_password, timeout=3*60, retry=10)
                    ret = rpc_client.thread_get_stats()
                    print(f"spdk thread_get_stats return: {ret}")
                    if ret and "threads" in ret:
                        push_metrics(ret, cluster_id, snode.id)
        except Exception as e:
            print(f"SPDK query failed: {e}")
        
        time.sleep(10)
