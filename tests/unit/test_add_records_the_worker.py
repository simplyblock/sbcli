"""An added node is recorded through the same place a relocated one is.

The add and the relocation restart each assembled the record's host identity
by hand and drifted: the relocation stored the bare hostname from April 2026
and, on the pod network, the agent pod's address. #1642 gave the restart
``record_host``; the add now goes through it too, with the management
interface it was given deciding the worker's address on the host network.
"""
import inspect

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_network import POD_NET_TYPE

NODE_ADDR = "192.168.10.142:5000"


def _node(pod_network=False):
    snode = StorageNode()
    snode.uuid = "d734922a-3ced-4874-b374-38e3a1ee7aeb"
    snode.rpc_port = 4420
    nic = IFace()
    nic.if_name = "eth0"
    nic.net_type = POD_NET_TYPE if pod_network else ""
    snode.data_nics = [nic]
    return snode


def _info(host_ip=None):
    info = {
        "hostname": "worker-1",
        "network_interface": {
            "eth0": {"ip": "192.168.10.142"},
            "eno1": {"ip": "10.0.0.142"},
            "eno2": {"ip": ""},
        },
    }
    if host_ip is not None:
        info["host_ip"] = host_ip
    return info


@pytest.fixture
def never_resolves(monkeypatch):
    """The add knows the interface; it must not fall back to resolving."""
    def boom(_):
        raise AssertionError("resolve_address was consulted")
    monkeypatch.setattr(storage_node_ops.utils, "resolve_address", boom)


class TestOnTheHostNetworkTheInterfaceDecides:
    def test_the_named_interfaces_address_is_the_workers(self, never_resolves):
        snode = _node()
        storage_node_ops.record_host(snode, NODE_ADDR, _info(), iface_names="eno1")
        assert snode.mgmt_ip == "10.0.0.142"

    def test_the_first_interface_with_an_address_wins(self, never_resolves):
        snode = _node()
        storage_node_ops.record_host(snode, NODE_ADDR, _info(), iface_names=["eno2", "eno1"])
        assert snode.mgmt_ip == "10.0.0.142"

    def test_an_interface_without_an_address_is_refused(self, never_resolves):
        snode = _node()
        with pytest.raises(ValueError, match="eno2"):
            storage_node_ops.record_host(snode, NODE_ADDR, _info(), iface_names="eno2")


class TestOnThePodNetworkTheAgentDecides:
    def test_the_reported_host_address_wins_over_any_interface(self, never_resolves):
        snode = _node(pod_network=True)
        storage_node_ops.record_host(snode, NODE_ADDR, _info(host_ip="192.168.10.142"),
                                     iface_names="eno1")
        assert snode.mgmt_ip == "192.168.10.142"


class TestTheRecord:
    def test_hostname_and_node_name_as_before(self, never_resolves):
        snode = _node()
        storage_node_ops.record_host(snode, NODE_ADDR, _info(), iface_names="eth0")
        assert (snode.hostname, snode.k8s_node_name, snode.api_endpoint) == (
            "worker-1_4420", "worker-1", NODE_ADDR)


class TestTheAddUsesIt:
    def test_add_node_records_the_host_this_way(self):
        src = inspect.getsource(storage_node_ops.add_node)
        assert "record_host(snode, node_addr, node_info, iface_names=iface_name)" in src
        for by_hand in ("snode.hostname = hostname", "snode.mgmt_ip = mgmt_ip",
                        "snode.api_endpoint = node_addr", "snode.k8s_node_name = node_info"):
            assert by_hand not in src, by_hand

    def test_the_subsystem_nqn_is_built_from_the_recorded_hostname(self):
        src = inspect.getsource(storage_node_ops.add_node)
        assert 'f"{BASE_NQN}:{snode.hostname}"' in src
