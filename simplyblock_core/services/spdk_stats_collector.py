from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
from simplyblock_core.services.spdk import client as spdk_client
import socket
import fcntl
import struct
import subprocess


PUSHGATEWAY_URL = "http://pushgateway:9091"
SPDK_SOCK_PATH = "/var/tmp/spdk.sock"
cluster_id_file = "/etc/foundationdb/sbcli_cluster_id"

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

def get_cluster_id():
    out = run_command(f"cat {cluster_id_file}")
    return out

def push_metrics(ret):
    """Formats and pushes SPDK metrics to Prometheus Pushgateway."""
    registry = CollectorRegistry()
    tick_rate_gauge = Gauge('tick_rate', 'SPDK Tick Rate', ['cluster_id','node_ip'], registry=registry)
    cpu_busy_gauge = Gauge('cpu_busy_percentage', 'CPU Busy Percentage', ['cluster_id', 'node_ip', 'thread_name'], registry=registry)
    pollers_count_gauge = Gauge('pollers_count', 'Number of pollers', ['cluster_id', 'node_ip', 'poller_type', 'thread_name'], registry=registry)

    cluster_id = get_cluster_id()

    tick_rate = ret.get("tick_rate")
    if tick_rate is not None:
        tick_rate_gauge.labels(cluster_id=cluster_id, node_ip=NODE_IP).set(tick_rate)
    for thread in ret.get("threads", []):
        thread_name = thread.get("name")
        busy = thread.get("busy", 0)
        idle = thread.get("idle", 0)

        total_cycles = busy + idle
        cpu_usage_percent = (busy / total_cycles) * 100 if total_cycles > 0 else 0
        cpu_busy_gauge.labels(cluster_id=cluster_id, node_ip=NODE_IP, thread_name=thread_name).set(cpu_usage_percent)

        pollers_count_gauge.labels(cluster_id=cluster_id, node_ip=NODE_IP, poller_type="active", thread_name=thread_name).set(thread.get("active_pollers_count", 0))
        pollers_count_gauge.labels(cluster_id=cluster_id, node_ip=NODE_IP, poller_type="timed", thread_name=thread_name).set(thread.get("timed_pollers_count", 0))
        pollers_count_gauge.labels(cluster_id=cluster_id, node_ip=NODE_IP, poller_type="paused", thread_name=thread_name).set(thread.get("paused_pollers_count", 0))
    
    push_to_gateway(PUSHGATEWAY_URL, job='metricsgateway', registry=registry)
    print("Metrics pushed successfully")

if __name__ == "__main__":
    client = spdk_client.JSONRPCClient(SPDK_SOCK_PATH, 5260)
    while True:
        try:
            ret = client.call("thread_get_stats")
            if ret and "threads" in ret:
                push_metrics(ret)
        except Exception as e:
            print(f"SPDK query failed: {e}")
        
        time.sleep(10)
