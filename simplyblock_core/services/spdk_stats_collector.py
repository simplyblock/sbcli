from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
from simplyblock_core.services.spdk import client as spdk_client
import socket

PUSHGATEWAY_URL = "http://pushgateway:9091"
SPDK_SOCK_PATH = "/var/tmp/spdk.sock"

def get_mgmt_ip():
    return socket.gethostbyname(socket.gethostname())

MGMT_IP = get_mgmt_ip()
client = spdk_client.JSONRPCClient(SPDK_SOCK_PATH, 5260)

def push_metrics(ret):
    """Formats and pushes SPDK metrics to Prometheus Pushgateway."""
    registry = CollectorRegistry()
    tick_rate_gauge = Gauge('tick_rate', 'SPDK Tick Rate', ['mgmt_ip'], registry=registry)
    cpu_busy_gauge = Gauge('cpu_busy_percentage', 'CPU Busy Percentage', ['mgmt_ip', 'thread_name'], registry=registry)
    
    tick_rate = ret.get("tick_rate")
    if tick_rate is not None:
        tick_rate_gauge.labels(mgmt_ip=MGMT_IP).set(tick_rate)
    
    for thread in ret.get("threads", []):
        thread_name = thread.get("name")
        cpu_busy = thread.get("busy")
        if cpu_busy is not None:
            cpu_busy_gauge.labels(mgmt_ip=MGMT_IP, thread_name=thread_name).set(cpu_busy)
    
    push_to_gateway(PUSHGATEWAY_URL, job='spdk_metrics', registry=registry)
    print("Metrics pushed successfully")

if __name__ == "__main__":
    while True:
        try:
            ret = client.call("thread_get_stats")
            if ret:
                push_metrics(ret)
        except Exception as e:
            print(f"SPDK query failed: {e}")
        
        time.sleep(10)
