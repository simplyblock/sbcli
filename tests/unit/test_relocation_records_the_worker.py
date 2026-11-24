"""A relocated node is recorded the way an added one is.

StorageNodeOps Migrate restarts a node with another host's address. The
record then has to describe that host the way the add described the old one:
``mgmt_ip`` is the worker's address and ``hostname`` the worker's name with
the RPC port suffix. On the pod network the agent's address is its pod's, not
the worker's, and the agent reports the worker's address itself.

Talos lab, 2026-11-12, after a Migrate to worker-4 on the pod network: the
node listed as "worker-4" at 10.244.5.242 (its agent pod) beside
"worker-2_4422" at 192.168.10.143 and "worker-3_4424" at 192.168.10.144.
Three seconds after the Migrate succeeded the operator rolled that pod, the
monitor pinged the recorded address, got nothing, and flipped the healthy
node to unreachable.
"""
import inspect

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_network import POD_NET_TYPE

TARGET = "worker-4.sb-b4-node-api.simplyblock.svc:5000"
WORKER_IP = "192.168.10.145"
POD_IP = "10.244.5.242"


def _node(pod_network=False, networks=()):
    snode = StorageNode()
    snode.uuid = "65c468bd-73a5-4f15-a020-853532e3db31"
    snode.rpc_port = 4420
    snode.api_endpoint = "worker-1.sb-b4-node-api.simplyblock.svc:5000"
    snode.mgmt_ip = "192.168.10.142"
    snode.hostname = "worker-1_4420"
    snode.k8s_node_name = "worker-1"
    snode.networks = list(networks)
    nic = IFace()
    nic.if_name = "eth0"
    nic.net_type = POD_NET_TYPE if pod_network else ""
    snode.data_nics = [nic]
    return snode


def _info(host_ip=WORKER_IP):
    info = {"hostname": "worker-4", "network_interface": {}}
    if host_ip is not None:
        info["host_ip"] = host_ip
    return info


@pytest.fixture
def resolves_to_pod(monkeypatch):
    """The target's name answers with the agent pod's address, as a Service
    name does on the pod network."""
    monkeypatch.setattr(storage_node_ops.utils, "resolve_address", lambda _: POD_IP)


class TestOnThePodNetwork:
    def test_the_management_address_is_the_workers_not_the_pods(self, resolves_to_pod):
        snode = _node(pod_network=True)
        storage_node_ops.record_host(snode, TARGET, _info())
        assert snode.mgmt_ip == WORKER_IP

    def test_the_api_endpoint_is_the_targets_name(self, resolves_to_pod):
        snode = _node(pod_network=True)
        storage_node_ops.record_host(snode, TARGET, _info())
        assert snode.api_endpoint == TARGET

    def test_an_agent_without_a_host_address_is_refused(self, resolves_to_pod):
        """Rather than fall back to the pod's address and store the wrong kind
        of value again."""
        snode = _node(pod_network=True)
        with pytest.raises(ValueError, match="reports no host address"):
            storage_node_ops.record_host(snode, TARGET, _info(host_ip=None))
        assert snode.mgmt_ip == "192.168.10.142"


class TestOnAttachedNetworks:
    def test_the_management_address_is_the_workers(self, resolves_to_pod):
        snode = _node(networks=["storage-vf"])
        storage_node_ops.record_host(snode, TARGET, _info())
        assert snode.mgmt_ip == WORKER_IP


class TestOnTheHostNetwork:
    def test_the_targets_address_is_the_workers(self, monkeypatch):
        monkeypatch.setattr(storage_node_ops.utils, "resolve_address", lambda _: WORKER_IP)
        snode = _node()
        storage_node_ops.record_host(snode, "192.168.10.145:5000", _info(host_ip=None))
        assert snode.mgmt_ip == WORKER_IP


class TestTheHostname:
    def test_carries_the_rpc_port_suffix_like_an_added_node(self, resolves_to_pod):
        snode = _node(pod_network=True)
        storage_node_ops.record_host(snode, TARGET, _info())
        assert snode.hostname == "worker-4_4420"

    def test_the_kubernetes_node_name_is_the_workers(self, resolves_to_pod):
        snode = _node(pod_network=True)
        storage_node_ops.record_host(snode, TARGET, _info())
        assert snode.k8s_node_name == "worker-4"


class TestTheRestartUsesIt:
    def test_the_relocation_branch_records_the_host_this_way(self):
        src = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "record_host(snode, node_address, node_info)" in src
        assert "snode.mgmt_ip = utils.resolve_address(node_address)" not in src
