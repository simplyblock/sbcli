# coding=utf-8
"""Shared plumbing for the edge e2e suite: state access, SSH, the v2 API
client, AWS fault injection, and status polling."""
import json
import pathlib
import subprocess
import time

import boto3
import requests

STATE_FILE = pathlib.Path(__file__).parent / "state.json"
SSH_USER = "ubuntu"
SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=ERROR", "-o", "ConnectTimeout=10"]


def load_state() -> dict:
    return json.loads(STATE_FILE.read_text())


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def instance(state, name) -> dict:
    return state["instances"][name]


# --------------------------------------------------------------------- SSH

def ssh(state, name, command, key_path=None, check=True, timeout=300) -> str:
    """Run a command on an instance (by Name tag) via its public IP."""
    host = instance(state, name)["public_ip"]
    key = key_path or state.get("key_path", f"~/.ssh/{state['key_name']}.pem")
    argv = ["ssh", "-i", key, *SSH_OPTS, f"{SSH_USER}@{host}", command]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise RuntimeError(f"ssh {name}: {command!r} -> rc={result.returncode}\n"
                           f"{result.stdout}\n{result.stderr}")
    return result.stdout


def kubectl(state, cluster_server_name, command, **kwargs) -> str:
    return ssh(state, cluster_server_name, f"sudo kubectl {command}", **kwargs)


# --------------------------------------------------------------- API client

class EdgeApi:
    """Minimal v2 API client for the central control plane."""

    def __init__(self, base_url, cluster_id, secret):
        self.base = base_url.rstrip('/')
        self.cluster_id = cluster_id
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {secret}"
        self.session.verify = False

    def _url(self, path):
        return f"{self.base}/api/v2/clusters/{self.cluster_id}{path}"

    def request(self, method, path, **kwargs):
        response = self.session.request(method, self._url(path), timeout=30, **kwargs)
        if response.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {response.status_code}: {response.text}")
        return response

    def cluster_status(self) -> str:
        return self.request("GET", "/").json()["status"]

    def nodes(self) -> list:
        return self.request("GET", "/edge-nodes/").json()

    def node(self, node_id) -> dict:
        return self.request("GET", f"/edge-nodes/{node_id}").json()

    def add_node(self, hostname, mgmt_ip, partitions, data_ip=None, spdk_cpus=1):
        return self.request("POST", "/edge-nodes/", json={
            "hostname": hostname, "mgmt_ip": mgmt_ip, "data_ip": data_ip,
            "partitions": partitions, "spdk_cpus": spdk_cpus})

    def create_volume(self, name, size) -> dict:
        return self.request("POST", "/edge-volumes/",
                            json={"name": name, "size": size}).json()

    def volumes(self) -> list:
        return self.request("GET", "/edge-volumes/").json()

    def connect_info(self, volume_id) -> list:
        return self.request("GET", f"/edge-volumes/{volume_id}/connect").json()

    def remove_device(self, node_id, device_path):
        self.request("POST", f"/edge-nodes/{node_id}/devices/remove",
                     json={"device_path": device_path})

    def restart_device(self, node_id, device_path):
        self.request("POST", f"/edge-nodes/{node_id}/devices/restart",
                     json={"device_path": device_path})

    def replace_device(self, node_id, old_path, new_path) -> dict:
        return self.request("PUT", f"/edge-nodes/{node_id}/devices",
                            json={"old_path": old_path, "new_path": new_path}).json()

    def node_by_hostname(self, hostname) -> dict:
        node = next((n for n in self.nodes() if n["hostname"] == hostname), None)
        if node is None:
            raise RuntimeError(f"edge node {hostname} not found")
        return node


# ------------------------------------------------------------ AWS actions

def ec2(state):
    return boto3.session.Session(region_name=state["region"]).client("ec2")


def reboot_instance(state, name):
    ec2(state).reboot_instances(InstanceIds=[instance(state, name)["instance_id"]])


def force_detach_volume(state, volume_id):
    ec2(state).detach_volume(VolumeId=volume_id, Force=True)
    _wait_volume(state, volume_id, "available")


def attach_volume(state, volume_id, instance_name, device="/dev/sdf"):
    ec2(state).attach_volume(VolumeId=volume_id, Device=device,
                             InstanceId=instance(state, instance_name)["instance_id"])
    _wait_volume(state, volume_id, "in-use")


def create_and_attach_volume(state, instance_name, size_gb, device) -> str:
    client = ec2(state)
    az = client.describe_instances(
        InstanceIds=[instance(state, instance_name)["instance_id"]])[
        "Reservations"][0]["Instances"][0]["Placement"]["AvailabilityZone"]
    volume = client.create_volume(AvailabilityZone=az, Size=size_gb, VolumeType="gp3",
                                  TagSpecifications=[{"ResourceType": "volume",
                                                      "Tags": [{"Key": "Name",
                                                                "Value": f"{instance_name}-replacement"}]}])
    _wait_volume(state, volume["VolumeId"], "available")
    attach_volume(state, volume["VolumeId"], instance_name, device)
    return volume["VolumeId"]


def _wait_volume(state, volume_id, target, timeout=180):
    client = ec2(state)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        volume = client.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]
        if volume["State"] == target:
            return
        time.sleep(5)
    raise TimeoutError(f"volume {volume_id} did not reach {target}")


# --------------------------------------------------- network fault injection

CENTRAL_CIDR = "10.90.1.0/24"


def break_connection(state, edge_node_name, central_ips):
    """Hard partition: drop all traffic between this edge node and the central
    nodes. Local (on-cluster) IO is untouched — the client pod and the target
    live on the same host/subnet path."""
    rules = "; ".join(
        f"sudo iptables -I INPUT -s {ip} -j DROP; sudo iptables -I OUTPUT -d {ip} -j DROP"
        for ip in central_ips)
    ssh(state, edge_node_name, rules)


def make_connection_flaky(state, edge_node_name, loss_pct=35, delay_ms=400):
    """Flaky uplink: netem loss+delay on the primary interface. Client IO on
    the edge cluster itself does not cross this qdisc (local path)."""
    ssh(state, edge_node_name,
        f"IF=$(ip route show default | awk '{{print $5; exit}}'); "
        f"sudo tc qdisc add dev $IF root netem loss {loss_pct}% delay {delay_ms}ms")


def heal_connection(state, edge_node_name):
    ssh(state, edge_node_name,
        "IF=$(ip route show default | awk '{print $5; exit}'); "
        "sudo tc qdisc del dev $IF root 2>/dev/null; "
        "sudo iptables -F INPUT; sudo iptables -F OUTPUT", check=False)


# ------------------------------------------------------------------ polling

def wait_for(description, predicate, timeout=600, interval=10):
    """Poll until predicate() is truthy; raise with the description on timeout."""
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as e:  # API may be transiently unreachable mid-fault
            last_error = e
        time.sleep(interval)
    raise TimeoutError(f"Timed out waiting for: {description} (last error: {last_error})")


def wait_node_status(api, hostname, status, timeout=600):
    return wait_for(f"node {hostname} -> {status}",
                    lambda: api.node_by_hostname(hostname)["status"] == status,
                    timeout=timeout)


def wait_cluster_status(api, status, timeout=600):
    return wait_for(f"cluster -> {status}",
                    lambda: api.cluster_status() == status, timeout=timeout)


def observe_node_transitions(api, hostname, expected_sequence, timeout=900,
                             interval=5) -> list:
    """Watch a node until every status in expected_sequence has been seen in
    order (intermediate repeats allowed); returns the observed trace."""
    trace = []
    remaining = list(expected_sequence)
    deadline = time.monotonic() + timeout
    while remaining and time.monotonic() < deadline:
        try:
            status = api.node_by_hostname(hostname)["status"]
        except Exception:
            status = None
        if status is not None and (not trace or trace[-1] != status):
            trace.append(status)
        while remaining and remaining[0] in trace:
            trace_index = trace.index(remaining[0])
            trace = trace[trace_index:]
            remaining.pop(0)
        time.sleep(interval)
    if remaining:
        raise TimeoutError(
            f"node {hostname}: never observed {remaining} (trace so far: {trace})")
    return trace
