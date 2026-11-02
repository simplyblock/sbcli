"""Unit tests for the node agent on the pod network (design-superclusters.md
P0-3): what the agent reads and does about the host's network, its
interfaces, the NVMe-oF connections it opens, and the pings it sends, runs
in the host's network namespace when the agent is not in it, and the node it
reports is the Kubernetes node rather than its pod."""

import json
from unittest.mock import patch

from flask import Flask

from simplyblock_core.utils import host_network

HOST_NS = "net:[4026531840]"
POD_NS = "net:[4026532999]"


def _namespaces(own):
    def readlink(path):
        return own if path == "/proc/self/ns/net" else HOST_NS
    return readlink


def test_a_command_in_the_host_network_namespace_runs_as_it_is():
    with patch("os.readlink", _namespaces(HOST_NS)):
        assert host_network.host_command("ip -j address show") == "ip -j address show"


def test_a_command_from_a_pod_network_namespace_enters_the_hosts():
    with patch("os.readlink", _namespaces(POD_NS)):
        assert host_network.host_command("ip -j address show") == \
            "nsenter --net=/proc/1/ns/net ip -j address show"


def test_the_host_namespace_is_assumed_when_it_cannot_be_read():
    def unreadable(path):
        raise OSError("no such file")
    with patch("os.readlink", unreadable):
        assert host_network.host_command("ping 10.0.0.1") == "ping 10.0.0.1"


def test_the_nic_list_is_the_hosts():
    from simplyblock_core import utils
    seen = []

    def run(cmd):
        seen.append(cmd)
        return json.dumps([{"ifname": "ens16np0", "operstate": "UP", "link_type": "ether",
                            "addr_info": [{"family": "inet", "local": "10.10.10.245"}]}]), "", 0

    with patch("os.readlink", _namespaces(POD_NS)), patch.object(utils.shell_utils, "run_command", run):
        nics = utils.get_nics_data()
    assert seen == ["nsenter --net=/proc/1/ns/net ip -j address show"]
    assert nics["ens16np0"]["ip"] == "10.10.10.245"


LINKS = [
    {"ifname": "eth0", "master": "bond0"},
    {"ifname": "eth1", "master": "bond0"},
    {"ifname": "bond0", "master": "br-ex"},
    {"ifname": "br-ex"},
    {"ifname": "ens16np0"},
    {"ifname": "ens16np0.100", "link": "ens16np0"},
]


def _links(cmd):
    assert cmd == "ip -j -d link show"
    return json.dumps(LINKS), "", 0


def test_a_bridge_over_a_bond_resolves_to_the_bonds_ports():
    with patch("os.readlink", _namespaces(HOST_NS)), patch.object(host_network, "run_command", _links):
        assert sorted(host_network.underlying_interfaces("br-ex")) == ["eth0", "eth1"]


def test_a_vlan_resolves_to_its_lower_interface():
    with patch("os.readlink", _namespaces(HOST_NS)), patch.object(host_network, "run_command", _links):
        assert host_network.underlying_interfaces("ens16np0.100") == ["ens16np0"]


def test_an_interface_the_host_does_not_have_resolves_to_nothing():
    with patch("os.readlink", _namespaces(HOST_NS)), patch.object(host_network, "run_command", _links):
        assert host_network.underlying_interfaces("missing0") == []


def test_carrier_is_read_from_the_hosts_link_flags():
    def run(cmd):
        assert cmd == "nsenter --net=/proc/1/ns/net ip -j link show dev ens16np0"
        return json.dumps([{"ifname": "ens16np0", "flags": ["BROADCAST", "UP", "LOWER_UP"]}]), "", 0
    with patch("os.readlink", _namespaces(POD_NS)), patch.object(host_network, "run_command", run):
        assert host_network.has_carrier("ens16np0")


def test_ping_and_nvme_connections_leave_from_the_host():
    from simplyblock_web.api.internal.storage_node import docker
    seen = []

    def run(cmd):
        seen.append(cmd)
        if cmd.endswith("ip -j link show dev ens16np0"):
            return json.dumps([{"ifname": "ens16np0", "flags": ["UP", "LOWER_UP"]}]), "", 0
        return "", "", 0

    with Flask(__name__).app_context(), \
            patch("os.readlink", _namespaces(POD_NS)), \
            patch.object(host_network, "run_command", run), \
            patch.object(docker.shell_utils, "run_command", run):
        docker.ping_ip(docker.PingQuery(ip="10.10.10.246", ifname="ens16np0"))
        docker.connect_to_nvme(docker.utils.NVMEConnectParams(
            ip="10.10.10.246", port=4420, nqn="nqn.2023-02.io.simplyblock:x"))
        docker.disconnect_nqn(docker.utils.DisconnectParams(nqn="nqn.2023-02.io.simplyblock:x", device_path=None, all=None))
    assert "nsenter --net=/proc/1/ns/net ping -c 3 -W 1 10.10.10.246" in seen
    assert any(c.startswith("nsenter --net=/proc/1/ns/net nvme connect ") for c in seen)
    assert "nsenter --net=/proc/1/ns/net nvme disconnect --nqn=nqn.2023-02.io.simplyblock:x" in seen


def test_the_reported_hostname_is_the_kubernetes_node():
    with patch.dict("os.environ", {"HOSTNAME": "worker-2"}), \
            patch("socket.gethostname", return_value="snode-api-xk2p9"):
        assert host_network.node_hostname() == "worker-2"


class _Node:
    """A Node object as the Kubernetes client returns it."""

    def __init__(self, provider_id, labels, addresses, annotations=None):
        from types import SimpleNamespace
        self.spec = SimpleNamespace(provider_id=provider_id)
        self.metadata = SimpleNamespace(labels=labels, annotations=annotations or {})
        self.status = SimpleNamespace(addresses=[
            SimpleNamespace(type=kind, address=address) for kind, address in addresses])


def test_cloud_info_falls_back_to_the_node_when_metadata_is_unreachable():
    from simplyblock_web.api.internal import _node_info
    node = _Node("aws:///eu-west-1a/i-0abc123", {"node.kubernetes.io/instance-type": "i4i.2xlarge"},
                 [("InternalIP", "10.0.1.5"), ("ExternalIP", "54.1.2.3")])

    class Core:
        def read_node(self, name, **_kwargs):
            assert name == "worker-2"
            return node

    with patch.dict("os.environ", {"HOSTNAME": "worker-2"}), \
            patch.object(_node_info, "_metadata_cloud_info", return_value=None), \
            patch.object(_node_info, "get_k8s_core_client", return_value=Core()):
        info = _node_info.get_cloud_info()
    assert info == {"id": "i-0abc123", "type": "i4i.2xlarge", "cloud": "amazon",
                    "ip": "10.0.1.5", "public_ip": "54.1.2.3"}


def test_a_node_without_a_provider_carries_no_cloud_info():
    from simplyblock_web.api.internal import _node_info

    class Core:
        def read_node(self, name, **_kwargs):
            return _Node("", {}, [("InternalIP", "10.0.1.5")])

    with patch.dict("os.environ", {"HOSTNAME": "worker-2"}), \
            patch.object(_node_info, "_metadata_cloud_info", return_value=None), \
            patch.object(_node_info, "get_k8s_core_client", return_value=Core()):
        assert _node_info.get_cloud_info() is None


def _fallback(node, **patches):
    """Cloud info read from the node alone, the metadata service unreachable."""
    from simplyblock_web.api.internal import _node_info

    class Core:
        def __init__(self):
            self.kwargs = {}

        def read_node(self, name, **kwargs):
            self.kwargs = kwargs
            if isinstance(node, Exception):
                raise node
            return node

    core = Core()
    with patch.dict("os.environ", {"HOSTNAME": "worker-2"}), \
            patch.object(_node_info, "_metadata_cloud_info", return_value=None), \
            patch.object(_node_info, "get_k8s_core_client", return_value=core):
        return _node_info.get_cloud_info(), core


AZURE = ("azure:///subscriptions/s1/resourceGroups/rg/providers/Microsoft.Compute/"
         "virtualMachineScaleSets/{}/virtualMachines/0")


def test_two_azure_scale_sets_with_the_same_ordinal_are_distinct_instances():
    a, _ = _fallback(_Node(AZURE.format("pool-a"), {}, []))
    b, _ = _fallback(_Node(AZURE.format("pool-b"), {}, []))
    assert a["id"] != b["id"]
    assert a["id"].endswith("virtualMachineScaleSets/pool-a/virtualMachines/0")


def test_a_gce_instance_is_identified_by_its_numeric_id_as_the_metadata_service_reports_it():
    info, _ = _fallback(_Node("gce://project/europe-west1-b/gke-node-1", {}, [],
                              annotations={"container.googleapis.com/instance_id": "4515203398452717513"}))
    assert info["id"] == "4515203398452717513"


def test_a_gce_instance_without_its_numeric_id_carries_no_cloud_info():
    """The instance name is not the id the metadata service reports; a node
    that would report one or the other depending on reachability has no
    stable identity, so none is reported."""
    info, _ = _fallback(_Node("gce://project/europe-west1-b/gke-node-1", {}, []))
    assert info is None


def test_the_node_read_is_bounded_and_a_timeout_reports_nothing():
    info, core = _fallback(TimeoutError("read timed out"))
    assert info is None
    assert core.kwargs.get("_request_timeout"), "the Node read has no timeout"
    _, core = _fallback(_Node("aws:///eu-west-1a/i-0abc123", {}, []))
    assert core.kwargs.get("_request_timeout")


def test_a_failed_ping_reports_its_diagnostic():
    from simplyblock_web.api.internal.storage_node import docker

    def run(cmd):
        return "", b"connect: Network is unreachable", 2

    with Flask(__name__).app_context(), \
            patch("os.readlink", _namespaces(POD_NS)), \
            patch.object(docker.shell_utils, "run_command", run):
        response = docker.ping_ip(docker.PingQuery(ip="10.10.10.246", ifname="ens16np0"))
    assert "Network is unreachable" in json.dumps(response.get_json())
