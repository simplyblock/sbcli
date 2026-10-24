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


def test_the_v2_node_add_parameters_accept_networks():
    from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

    parameters = StorageNodeParams(node_address="10.0.0.1:5000", interface_name="eth0", networks=NETWORKS)

    assert parameters.networks[0].interface == "data0"
    assert StorageNodeParams(node_address="a", interface_name="b").networks == []


def test_the_v2_node_add_hands_the_networks_to_the_task():
    from simplyblock_web.api.v2.cluster import storage_node as v2
    from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

    cluster = MagicMock()
    cluster.get_id.return_value = "00000000-0000-0000-0000-0000000000c1"
    parameters = StorageNodeParams(node_address="10.0.0.1:5000", interface_name="eth0", networks=NETWORKS)

    with patch.object(v2.tasks_controller, "add_node_add_task", return_value="00000000-0000-0000-0000-000000000001") as add, \
            patch.object(v2.util, "creation_response"):
        v2.add(MagicMock(), cluster, parameters)

    assert add.call_args.args[1]["networks"] == [{**n, "resource": ""} for n in NETWORKS]


def test_a_storage_node_keeps_its_networks():
    from simplyblock_core.models.storage_node import StorageNode

    assert StorageNode().networks == []
    assert StorageNode({'networks': NETWORKS}).networks == NETWORKS


class TestAddNodeAndRestartKeepTheNetworks:
    """Structural, in the manner of test_add_node_pod_cleanup: add_node and
    restart need too much of the world mocked to be driven here. The live run
    on a cluster with Multus covers the behavior itself."""

    def test_add_node_takes_the_networks_and_starts_spdk_with_them(self):
        import inspect

        from simplyblock_core import storage_node_ops

        assert "networks" in inspect.signature(storage_node_ops.add_node).parameters
        source = inspect.getsource(storage_node_ops.add_node)
        start = source[source.index("snode_api.spdk_process_start("):]
        assert "networks=networks" in start[:start.index("except Exception")]

    def test_add_node_records_the_networks_on_the_node(self):
        import inspect

        from simplyblock_core import storage_node_ops

        assert "snode.networks = " in inspect.getsource(storage_node_ops.add_node)

    def test_restart_starts_spdk_with_the_recorded_networks(self):
        import inspect

        from simplyblock_core import storage_node_ops

        source = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        start = source[source.index("snode_api.spdk_process_start("):]
        assert "networks=snode.networks" in start[:start.index("except Exception")]

    def test_the_task_runner_hands_the_networks_to_add_node(self):
        import inspect

        from simplyblock_core import storage_node_ops
        from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

        # every key the v2 handler puts in function_params is an add_node parameter
        add_node_parameters = set(inspect.signature(storage_node_ops.add_node).parameters)
        assert "networks" in add_node_parameters
        assert "networks" in StorageNodeParams.model_fields


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
        {"name": "mgmt0", "ips": ["10.0.0.5"], "mac": "bb:bb"},
        {"name": "data0", "ips": ["10.10.10.101"], "mac": "cc:cc"},
        {"name": "data1", "ips": ["10.20.0.5"], "mac": "dd:dd"},
    ],
}


class TestWhatThePodReportsBecomesTheNode:
    def test_the_rpc_address_is_the_management_interface_and_the_data_nics_are_the_data_interfaces(self):
        from simplyblock_core import storage_network

        spdk_ip, nics = storage_network.from_report(REPORT)

        assert spdk_ip == "10.0.0.5"
        assert [(n["if_name"], n["ip4_address"]) for n in nics] == [("data0", "10.10.10.101"), ("data1", "10.20.0.5")]
        assert all(n["trtype"] == "TCP" and n["net_type"] == "attached" and n["uuid"] for n in nics)

    def test_without_data_interfaces_the_management_interface_carries_the_data(self):
        from simplyblock_core import storage_network

        spdk_ip, nics = storage_network.from_report({"pod_ip": "10.42.1.5", "interfaces": [REPORT["interfaces"][0]]})

        assert spdk_ip == "10.0.0.5"
        assert [n["if_name"] for n in nics] == ["mgmt0"]

    def test_without_a_management_interface_the_rpc_address_is_the_pods(self):
        from simplyblock_core import storage_network

        spdk_ip, nics = storage_network.from_report({"pod_ip": "10.42.1.5", "interfaces": [REPORT["interfaces"][1]]})

        assert spdk_ip == "10.42.1.5"
        assert [n["if_name"] for n in nics] == ["data0"]

    def test_a_report_with_no_interfaces_is_refused(self):
        from simplyblock_core import storage_network

        with pytest.raises(ValueError, match="no interfaces"):
            storage_network.from_report({"pod_ip": "10.42.1.5", "interfaces": []})


class TestTheControlPlaneDialsThePodWhenItHasOne:
    def _node(self, **fields):
        from simplyblock_core.models.storage_node import StorageNode

        return StorageNode({"mgmt_ip": "192.168.1.9", "rpc_port": 8080, "rpc_username": "u",
                            "rpc_password": "p", "hostname": "w1_8080", **fields})

    def test_a_node_with_an_spdk_address_is_dialed_there_and_the_worker_address_stays_the_host(self):
        node = self._node(spdk_ip="10.0.0.5")

        with patch("simplyblock_core.models.storage_node.RPCClient") as client, \
                patch("simplyblock_core.models.storage_node.Settings") as settings:
            settings.return_value.tls_connect = "disabled"
            node.rpc_client()

        assert client.call_args.args[0] == "10.0.0.5"
        assert node.mgmt_ip == "192.168.1.9"

    def test_a_node_without_one_is_dialed_at_its_management_address_as_before(self):
        with patch("simplyblock_core.models.storage_node.RPCClient") as client, \
                patch("simplyblock_core.models.storage_node.Settings") as settings:
            settings.return_value.tls_connect = "disabled"
            self._node().rpc_client()

        assert client.call_args.args[0] == "192.168.1.9"


class TestApplyingAReportToANode:
    def _node(self):
        from simplyblock_core.models.iface import IFace
        from simplyblock_core.models.storage_node import StorageNode

        node = StorageNode({"mgmt_ip": "192.168.1.9", "spdk_ip": "10.0.0.4"})
        node.data_nics = [IFace({"uuid": "keep-me", "if_name": "data0", "ip4_address": "10.10.10.99",
                                 "status": "Up", "net_type": "attached", "trtype": "TCP"})]
        return node

    def test_a_restart_refreshes_the_addresses_and_keeps_each_nics_identity(self):
        from simplyblock_core import storage_network

        node = self._node()
        storage_network.apply_report(node, REPORT)

        assert node.spdk_ip == "10.0.0.5"
        by_name = {n.if_name: n for n in node.data_nics}
        assert by_name["data0"].uuid == "keep-me" and by_name["data0"].ip4_address == "10.10.10.101"
        assert by_name["data1"].uuid and by_name["data1"].uuid != "keep-me"
        assert node.mgmt_ip == "192.168.1.9", "the worker's address is not the pod's"

    def test_restart_applies_the_report_and_skips_the_hosts_probes_for_attached_nics(self):
        import inspect

        from simplyblock_core import storage_node_ops

        source = inspect.getsource(storage_node_ops._restart_storage_node_impl)
        assert "storage_network.apply_report(snode, results)" in source
        assert "if snode.networks:" in source

    def test_add_node_records_what_the_pod_reported(self):
        import inspect

        from simplyblock_core import storage_node_ops

        source = inspect.getsource(storage_node_ops.add_node)
        assert "storage_network.from_report(results)" in source
        assert "snode.spdk_ip = spdk_ip" in source


class TestTheWorkersAddressOfANodeOnAttachedNetworks:
    INFO: ClassVar[dict] = {"hostname": "w1", "host_ip": "192.168.10.62", "network_interface": {"eth0": {"ip": "10.42.1.9"}}}

    def test_a_named_host_interface_still_decides_it(self):
        from simplyblock_core import storage_network

        assert storage_network.host_mgmt(self.INFO, "eth0", networks=[]) == ("10.42.1.9", "eth0")

    def test_with_networks_and_no_interface_the_agent_reports_its_hosts_address(self):
        from simplyblock_core import storage_network

        assert storage_network.host_mgmt(self.INFO, "", networks=[{"interface": "data0"}]) == ("192.168.10.62", "")

    def test_with_networks_the_hosts_address_wins_over_the_pods_own_interface(self):
        from simplyblock_core import storage_network

        assert storage_network.host_mgmt(self.INFO, "eth0", networks=[{"interface": "data0"}])[0] == "192.168.10.62"

    def test_without_networks_and_without_an_interface_there_is_no_address(self):
        from simplyblock_core import storage_network

        assert storage_network.host_mgmt(self.INFO, "", networks=[]) is None

    def test_an_agent_that_reports_no_host_address_gives_none_for_a_networked_node(self):
        from simplyblock_core import storage_network

        assert storage_network.host_mgmt({"network_interface": {}}, "", networks=[{"interface": "data0"}]) is None

    def test_add_node_asks_this_instead_of_the_host_interface_directly(self):
        import inspect

        from simplyblock_core import storage_node_ops

        source = inspect.getsource(storage_node_ops.add_node)
        assert source.count("storage_network.host_mgmt(node_info, iface_name, networks)") == 2

    def test_the_agent_reports_its_host_address(self):
        import inspect

        from simplyblock_web.api.internal.storage_node import kubernetes

        assert '"host_ip"' in inspect.getsource(kubernetes.get_info)
