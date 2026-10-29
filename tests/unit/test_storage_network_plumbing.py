"""The networks a node is added with reach the node agent and are kept for the
restart (operator design-storage-network.md §5, P0-1): v2 node-add parameters,
the node agent request body, and the StorageNode record."""

import json
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.snode_client import SNodeClient

NETWORKS = [{"name": "fabric", "namespace": "infra", "interface": "data0", "ips": ["10.10.10.101/24"]}]


@pytest.fixture
def snode_client():
    with patch("simplyblock_core.snode_client.requests.session") as session_factory:
        session = MagicMock()
        session_factory.return_value = session
        session.headers = {}
        session.request.return_value = MagicMock(status_code=200, json=lambda: {"results": True, "status": True})
        client = SNodeClient("snode.host")
        client._fake_session = session
        yield client


def _posted(client):
    return json.loads(client._fake_session.request.call_args.kwargs["data"])


def test_the_start_request_carries_the_networks(snode_client):
    snode_client.spdk_process_start("0-1", 1024, networks=NETWORKS)

    assert _posted(snode_client)["networks"] == NETWORKS


@pytest.mark.parametrize("networks", [None, []])
def test_the_start_request_without_networks_has_no_networks_key(snode_client, networks):
    snode_client.spdk_process_start("0-1", 1024, networks=networks)

    assert "networks" not in _posted(snode_client)


def test_the_v2_node_add_hands_the_networks_to_the_task():
    from simplyblock_web.api.v2.cluster import storage_node as v2
    from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

    cluster = MagicMock()
    cluster.get_id.return_value = "00000000-0000-0000-0000-0000000000c1"
    parameters = StorageNodeParams(node_address="10.0.0.1:5000", interface_name="eth0", networks=NETWORKS)

    with patch.object(v2.tasks_controller, "add_node_add_task", return_value="00000000-0000-0000-0000-000000000001") as add, \
            patch.object(v2.util, "creation_response"):
        v2.add(MagicMock(), cluster, parameters)

    assert add.call_args.args[1]["networks"] == NETWORKS


def test_the_v2_node_add_without_networks_leaves_the_task_parameters_as_before():
    from simplyblock_web.api.v2.cluster import storage_node as v2
    from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

    cluster = MagicMock()
    cluster.get_id.return_value = "00000000-0000-0000-0000-0000000000c1"

    with patch.object(v2.tasks_controller, "add_node_add_task", return_value="00000000-0000-0000-0000-000000000001") as add, \
            patch.object(v2.util, "creation_response"):
        v2.add(MagicMock(), cluster, StorageNodeParams(node_address="a:1", interface_name="eth0"))

    assert "networks" not in add.call_args.args[1]


REPORT = {
    "pod_ip": "10.42.1.5",
    "interfaces": [
        {"name": "mgmt0", "ips": ["10.0.0.5"]},
        {"name": "data0", "ips": ["10.10.10.101"]},
        {"name": "data1", "ips": ["10.20.0.5"]},
    ],
}


class TestWhatThePodReportsBecomesTheNode:
    def test_the_data_nics_are_the_data_interfaces(self):
        from simplyblock_core import storage_network

        nics = storage_network.from_report(REPORT)

        assert [(n.if_name, n.ip4_address) for n in nics] == [("data0", "10.10.10.101"), ("data1", "10.20.0.5")]
        assert all(n.trtype == "TCP" and n.net_type == "attached" and n.uuid for n in nics)

    def test_without_data_interfaces_the_management_interface_carries_the_data(self):
        from simplyblock_core import storage_network

        nics = storage_network.from_report({"pod_ip": "10.42.1.5", "interfaces": [REPORT["interfaces"][0]]})

        assert [n.if_name for n in nics] == ["mgmt0"]

    def test_a_report_with_no_interfaces_is_refused(self):
        from simplyblock_core import storage_network

        with pytest.raises(ValueError, match="no interfaces"):
            storage_network.from_report({"pod_ip": "10.42.1.5", "interfaces": []})


class TestTheControlPlaneDialsTheSpdkProxyByName:
    def _node(self, **fields):
        from simplyblock_core.models.storage_node import StorageNode

        return StorageNode({"mgmt_ip": "192.168.1.9", "rpc_port": 8080, "rpc_username": "u",
                            "rpc_password": "p", "hostname": "w1_8080", "cr_namespace": "storage",
                            "namespace": "simplyblock", "cluster_id": "15260308-01ab", **fields})

    def _dialed(self, node, tls="disabled"):
        with patch("simplyblock_core.models.storage_node.RPCClient") as client, \
                patch("simplyblock_core.models.storage_node.Settings") as settings:
            settings.return_value.tls_connect = tls
            node.rpc_client()
        return client.call_args.args[0]

    def test_a_node_on_attached_networks_is_dialed_by_its_name_in_the_proxy_service(self):
        node = self._node(networks=NETWORKS)

        assert self._dialed(node) == "w1.simplyblock-spdk-proxy.storage.svc"
        assert node.mgmt_ip == "192.168.1.9"

    def test_a_node_on_the_pod_network_is_dialed_by_its_own_service(self):
        node = self._node(data_nics=[{"if_name": "eth0", "ip4_address": "10.103.252.64", "net_type": "pod"}])

        assert self._dialed(node) == "snode-spdk-svc-8080-152603.simplyblock.svc"

    def test_tls_dials_the_proxy_service_name_on_every_network(self):
        assert self._dialed(self._node(), tls="enabled") == "w1.simplyblock-spdk-proxy.storage.svc"

    def test_a_node_on_the_host_network_is_dialed_at_its_management_address_as_before(self):
        assert self._dialed(self._node()) == "192.168.1.9"

    def test_the_k8s_node_name_field_decides_the_service_name(self):
        node = self._node(hostname="something-else_8080", k8s_node_name="w1", networks=NETWORKS)

        assert self._dialed(node) == "w1.simplyblock-spdk-proxy.storage.svc"

    def test_a_record_without_the_field_falls_back_to_the_hostname(self):
        node = self._node(hostname="w7_8080", networks=NETWORKS)

        assert node.k8s_node_name == "" and self._dialed(node).startswith("w7.")

    def test_a_node_has_no_spdk_ip(self):
        from simplyblock_core.models.storage_node import StorageNode

        assert not hasattr(StorageNode(), "spdk_ip")

    def test_a_record_an_earlier_build_wrote_with_spdk_ip_still_loads(self):
        from simplyblock_core.models.storage_node import StorageNode

        node = StorageNode().from_dict({"uuid": "n1", "spdk_ip": "10.0.0.5", "hostname": "w1_8080", "rpc_port": 8080})

        assert node.uuid == "n1" and node._k8s_node_label() == "w1"


class TestARestartRefreshesTheNodeFromTheReport:
    def test_the_addresses_follow_the_report_and_each_nic_keeps_its_identity(self):
        from simplyblock_core import storage_network
        from simplyblock_core.models.iface import IFace

        known = [IFace({"uuid": "keep-me", "if_name": "data0", "ip4_address": "10.10.10.99",
                        "status": "Up", "net_type": "attached", "trtype": "TCP"})]
        nics = storage_network.from_report(REPORT, known)

        by_name = {n.if_name: n for n in nics}
        assert by_name["data0"].uuid == "keep-me" and by_name["data0"].ip4_address == "10.10.10.101"
        assert by_name["data1"].uuid and by_name["data1"].uuid != "keep-me"


class TestANodeOnTheHostNetworkIsUnchanged:
    def test_it_has_no_pod_network_and_its_rpc_host_is_the_worker(self):
        from simplyblock_core.models.storage_node import StorageNode

        with patch("simplyblock_core.models.storage_node.Settings") as settings:
            settings.return_value.tls_connect = "disabled"
            node = StorageNode({"mgmt_ip": "192.168.1.9"})

            assert not node.on_pod_network and node.rpc_host == "192.168.1.9"

    def test_with_networks_it_is_on_the_pod_network(self):
        from simplyblock_core.models.storage_node import StorageNode

        assert StorageNode({"mgmt_ip": "192.168.1.9", "networks": NETWORKS}).on_pod_network


class TestTheWorkersAddressOfANodeOnAttachedNetworks:
    INFO: ClassVar[dict] = {"hostname": "w1", "host_ip": "192.168.10.62", "network_interface": {"eth0": {"ip": "10.42.1.9"}}}

    def test_a_named_host_interface_still_decides_it(self):
        from simplyblock_core import utils

        assert utils.get_mgmt_ip(self.INFO, "eth0", []) == ("10.42.1.9", "eth0")

    def test_with_networks_and_no_interface_the_agent_reports_its_hosts_address(self):
        from simplyblock_core import utils

        assert utils.get_mgmt_ip(self.INFO, "", NETWORKS) == ("192.168.10.62", "")

    def test_with_networks_the_hosts_address_wins_over_the_pods_own_interface(self):
        from simplyblock_core import utils

        assert utils.get_mgmt_ip(self.INFO, "eth0", NETWORKS)[0] == "192.168.10.62"

    def test_an_agent_that_reports_no_host_address_gives_none_for_a_networked_node(self):
        from simplyblock_core import utils

        assert utils.get_mgmt_ip({"network_interface": {}}, "", NETWORKS) is None


DEVICE_NETWORKS = [{"name": "vf", "namespace": "infra", "interface": "data0", "ips": ["10.10.10.101/24"],
                    "resource": "intel.com/sriov_storage"}]


def test_the_v2_node_add_keeps_the_device_resource_of_a_network():
    """The device resource travels with its network into the task, so the node record
    keeps it and a restart requests the device again."""
    from simplyblock_web.api.v2.cluster import storage_node as v2
    from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

    cluster = MagicMock()
    cluster.get_id.return_value = "00000000-0000-0000-0000-0000000000c1"
    parameters = StorageNodeParams(node_address="10.0.0.1:5000", interface_name="eth0", networks=DEVICE_NETWORKS)

    with patch.object(v2.tasks_controller, "add_node_add_task", return_value="00000000-0000-0000-0000-000000000001") as add, \
            patch.object(v2.util, "creation_response"):
        v2.add(MagicMock(), cluster, parameters)

    assert add.call_args.args[1]["networks"] == DEVICE_NETWORKS
