"""A storage node on the pod network: the pod serves on the cluster CNI's interface with
no attachment, a Service in front of it is the address peers and clients hold, and the
target binds the pod's own address. What the agent reports becomes the node, every
listener binds the listen address, no socket is pinned to a device, and the Service
forwards every port the node serves on."""

import json
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_network
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.snode_client import SNodeClient, SNodeClientException

POD_REPORT = {"pod_ip": "10.244.5.31", "service_ip": "10.103.252.64", "interfaces": []}


class TestTheReportOfAPodOnThePodNetwork:
    def test_the_service_address_is_held_and_the_pod_address_is_bound(self):
        spdk_ip, nics = storage_network.from_report(POD_REPORT)

        assert spdk_ip == "10.103.252.64"
        [nic] = nics
        assert (nic.if_name, nic.ip4_address, nic.listen_address) == ("eth0", "10.103.252.64", "10.244.5.31")
        assert nic.net_type == storage_network.POD_NET_TYPE and nic.trtype == "TCP" and nic.uuid

    def test_a_restart_keeps_the_nic_identity_and_the_service_and_takes_the_new_pod_address(self):
        known = [IFace({"uuid": "keep-me", "if_name": "eth0", "ip4_address": "10.103.252.64",
                        "listen_ip4": "10.244.5.9", "net_type": "pod", "trtype": "TCP"})]

        _, [nic] = storage_network.from_report(POD_REPORT, known)

        assert nic.uuid == "keep-me" and nic.ip4_address == "10.103.252.64" and nic.listen_address == "10.244.5.31"

    def test_a_report_with_neither_interfaces_nor_a_service_is_refused(self):
        with pytest.raises(ValueError, match="no interfaces"):
            storage_network.from_report({"pod_ip": "10.244.5.31", "interfaces": []})


class TestAnInterfaceOffThePodNetworkListensWhereItIsReached:
    def test_the_listen_address_is_the_address_when_none_is_set(self):
        nic = IFace({"if_name": "eth1", "ip4_address": "10.10.10.5"})

        assert nic.listen_address == "10.10.10.5"

    def test_the_listen_address_survives_the_record_round_trip(self):
        nic = IFace(IFace({"ip4_address": "10.103.252.64", "listen_ip4": "10.244.5.31"}).to_dict())

        assert nic.listen_address == "10.244.5.31"


def _node(**fields):
    return StorageNode({"uuid": "n1", "rpc_port": 4426, "nvmf_port": 4427, "lvol_subsys_port": 4432,
                        "hublvol": {"nvmf_port": 4433}, "api_endpoint": "w4:5000", "cluster_id": "c1", **fields})


POD_NIC = {"if_name": "eth0", "ip4_address": "10.103.252.64", "listen_ip4": "10.244.5.31", "net_type": "pod", "trtype": "TCP"}
VF_NIC = {"if_name": "data0", "ip4_address": "10.10.10.223", "net_type": "attached", "trtype": "TCP"}


class TestTheNodeKnowsWhichNetworkItIsOn:
    def test_a_pod_nic_puts_the_node_on_the_pod_network_without_networks(self):
        node = _node(data_nics=[POD_NIC])

        assert node.pod_network and node.on_pod_network and not node.networks

    def test_an_attached_nic_is_off_the_host_network_but_not_on_the_pod_network(self):
        node = _node(data_nics=[VF_NIC], networks=[{"name": "fabric", "interface": "data0"}])

        assert node.on_pod_network and not node.pod_network

    def test_a_host_network_node_is_neither(self):
        node = _node(data_nics=[{"if_name": "ens5", "ip4_address": "10.10.10.145"}])

        assert not node.on_pod_network and not node.pod_network


class TestSocketsArePinnedToTheOneDataNicExceptOnThePodNetwork:
    def test_one_host_or_attached_nic_pins_the_sockets_to_it(self):
        assert _node(data_nics=[VF_NIC]).socket_bind_device == "data0"

    def test_two_nics_pin_nothing(self):
        assert _node(data_nics=[VF_NIC, {"if_name": "data1", "ip4_address": "10.20.0.5"}]).socket_bind_device is None

    def test_the_pod_network_pins_nothing_because_the_service_traffic_arrives_on_the_pod_interface(self):
        assert _node(data_nics=[POD_NIC]).socket_bind_device is None


class TestTheServiceForwardsEveryPortTheNodeServesOn:
    def test_the_nodes_own_ports_and_every_lvstore_it_serves(self):
        node = _node(data_nics=[POD_NIC], lvstore_ports={
            "LVS_95": {"lvol_subsys_port": 4432, "hublvol_port": 4433},
            "LVS_101": {"lvol_subsys_port": 4434, "hublvol_port": 4435},
        })

        assert storage_network.published_ports(node) == [4426, 4427, 4432, 4433, 4434, 4435]

    def test_unset_ports_are_left_out(self):
        node = StorageNode({"rpc_port": 4420, "nvmf_port": 0, "lvol_subsys_port": 0, "data_nics": [POD_NIC]})

        assert storage_network.published_ports(node) == [4420]

    def test_publishing_hands_the_agent_the_set(self):
        node = _node(data_nics=[POD_NIC])
        client = MagicMock()
        client.spdk_service_publish.return_value = ("10.0.0.1", None)

        with patch.object(StorageNode, "client", return_value=client):
            storage_network.publish_ports(node)

        client.spdk_service_publish.assert_called_once_with(4426, "c1", [4426, 4427, 4432, 4433])

    def test_the_monitors_repeat_is_rate_limited_per_node(self):
        node = _node(data_nics=[POD_NIC])
        client = MagicMock()
        client.spdk_service_publish.return_value = ("10.0.0.1", None)
        storage_network._published_at.clear()

        with patch.object(StorageNode, "client", return_value=client):
            storage_network.publish_ports(node, at_most_every=60)
            storage_network.publish_ports(node, at_most_every=60)
            storage_network.publish_ports(_node(uuid="n2", data_nics=[POD_NIC]), at_most_every=60)
            storage_network.publish_ports(node)  # an explicit publish is never held back

        assert client.spdk_service_publish.call_count == 3

    def test_a_node_off_the_pod_network_publishes_nothing(self):
        with patch.object(StorageNode, "client") as client:
            storage_network.publish_ports(_node(data_nics=[VF_NIC]))

        client.assert_not_called()

    def test_an_agent_error_is_logged_and_left_to_the_monitor(self, caplog):
        node = _node(data_nics=[POD_NIC])
        client = MagicMock()
        client.spdk_service_publish.side_effect = SNodeClientException("agent down")

        with patch.object(StorageNode, "client", return_value=client), caplog.at_level("WARNING"):
            storage_network.publish_ports(node)

        assert "agent down" in caplog.text

    def test_a_refusal_the_agent_answers_in_its_body_is_logged_too(self, caplog):
        # the agent reports a failure as (results, error) in a 200, not as an exception
        node = _node(data_nics=[POD_NIC])
        client = MagicMock()
        client.spdk_service_publish.return_value = ([], "Service for port 4426 not published: Forbidden")

        with patch.object(StorageNode, "client", return_value=client), caplog.at_level("WARNING"):
            storage_network.publish_ports(node)

        assert "Forbidden" in caplog.text


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


class TestTheAgentRequests:
    def test_the_start_request_says_pod_network_and_the_ports_to_forward(self, snode_client):
        snode_client.spdk_process_start("0-1", 1024, pod_network=True, published_ports=[4426, 4432])

        posted = _posted(snode_client)
        assert posted["pod_network"] is True and posted["published_ports"] == [4426, 4432]

    def test_the_start_request_of_a_host_network_node_is_unchanged(self, snode_client):
        snode_client.spdk_process_start("0-1", 1024)

        posted = _posted(snode_client)
        assert "pod_network" not in posted and "published_ports" not in posted

    def test_publishing_posts_the_ports(self, snode_client):
        snode_client.spdk_service_publish(4426, "c1", [4426, 4432])

        assert _posted(snode_client) == {"rpc_port": 4426, "cluster_id": "c1", "ports": [4426, 4432]}


class TestTheV2NodeAdd:
    def test_pod_network_reaches_the_task(self):
        from simplyblock_web.api.v2.cluster import storage_node as v2
        from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

        cluster = MagicMock()
        cluster.get_id.return_value = "00000000-0000-0000-0000-0000000000c1"
        parameters = StorageNodeParams(node_address="10.0.0.1:5000", interface_name="", pod_network=True)

        with patch.object(v2.tasks_controller, "add_node_add_task", return_value="00000000-0000-0000-0000-000000000001") as add, \
                patch.object(v2.util, "creation_response"):
            v2.add(MagicMock(), cluster, parameters)

        assert add.call_args.args[1]["pod_network"] is True
        assert "networks" not in add.call_args.args[1]


class TestTheV2NodeAddRefusesBothWaysOffTheHostNetwork:
    def test_networks_and_pod_network_together_are_rejected_before_a_task_exists(self):
        from pydantic import ValidationError

        from simplyblock_web.api.v2.cluster.storage_node import StorageNodeParams

        with pytest.raises(ValidationError, match="not both"):
            StorageNodeParams(node_address="a:1", interface_name="", pod_network=True,
                              networks=[{"name": "fabric", "interface": "data0"}])


class TestTheWorkersAddressOfANodeOnThePodNetwork:
    def test_the_agent_reports_its_hosts_address(self):
        from simplyblock_core import utils

        info = {"host_ip": "192.168.10.62", "network_interface": {"eth0": {"ip": "10.42.1.9"}}}

        assert utils.get_mgmt_ip(info, "eth0", None, pod_network=True) == ("192.168.10.62", "")


# The listen-versus-connect split reaches the migration and replication services,
# which build their own path entries from the NICs.

def _pod_node(uuid, rpc_port, lvs="LVS_1"):
    return StorageNode({"uuid": uuid, "rpc_port": rpc_port, "lvol_subsys_port": 4432, "lvstore": lvs,
                        "status": StorageNode.STATUS_ONLINE, "mgmt_ip": "192.168.10.1",
                        "data_nics": [POD_NIC], "active_tcp": True})


class TestTheMigrationPathsBindTheListenAddressAndConnectToTheService:
    def test_a_migration_path_entry_carries_both(self):
        from simplyblock_core.services.tasks_runner_lvol_migration import _build_paths

        src, tgt = _pod_node("src", 4420), _pod_node("tgt", 4422)
        src_paths, tgt_paths, _ = _build_paths(src, tgt, MagicMock(), MagicMock())

        for path in (src_paths[0], tgt_paths[0]):
            assert path["ip"] == "10.103.252.64" and path["ips"] == ["10.103.252.64"]
            assert path["listen_ips"] == ["10.244.5.31"]

    def test_a_replication_path_entry_carries_both_and_flips_on_the_listen_address(self):
        from simplyblock_core.services import replication_final_step as step

        node = _pod_node("n1", 4420)
        rpc = MagicMock()
        with patch.object(StorageNode, "rpc_client", return_value=rpc):
            path = step._path_entry(node, "LVS_1")
            step._flip(path, "optimized", "nqn.test", "lead", ns_id=1)

        assert (path["ip"], path["listen_ip"]) == ("10.103.252.64", "10.244.5.31")
        rpc.nvmf_subsystem_listener_set_ana_state.assert_called_once_with(
            "nqn.test", "10.244.5.31", 4432, trtype="TCP", ana="optimized", anagrpid=1)

    def test_a_host_network_path_has_the_same_address_for_both(self):
        from simplyblock_core.services import replication_final_step as step

        node = StorageNode({"uuid": "h1", "lvol_subsys_port": 4432, "data_nics": [VF_NIC]})
        with patch.object(StorageNode, "rpc_client", return_value=MagicMock()):
            path = step._path_entry(node, "LVS_1")

        assert path["ip"] == path["listen_ip"] == "10.10.10.223"


class TestTheArbiterProbesThePeersOwnAddress:
    def test_on_the_pod_network_the_proxy_pings_the_peers_pod_address(self):
        from simplyblock_core.arbitration.arbiter import default_link_probe

        node, peer = _pod_node("a", 4420), _pod_node("b", 4422)
        rpc = MagicMock()
        rpc.net_check.return_value = True
        with patch.object(StorageNode, "rpc_client", return_value=rpc), patch.object(StorageNode, "client") as agent:
            assert default_link_probe(node, peer) is True

        rpc.net_check.assert_called_once_with("eth0", "10.244.5.31")
        agent.assert_not_called()

    def test_on_the_host_network_the_agent_pings_as_before(self):
        from simplyblock_core.arbitration.arbiter import default_link_probe

        node = StorageNode({"uuid": "a", "data_nics": [VF_NIC]})
        peer = StorageNode({"uuid": "b", "data_nics": [{"if_name": "data0", "ip4_address": "10.10.10.220", "trtype": "TCP"}]})
        agent = MagicMock()
        agent.ping_ip.return_value = (True, None)
        with patch.object(StorageNode, "client", return_value=agent):
            assert default_link_probe(node, peer) is True

        agent.ping_ip.assert_called_once_with("10.10.10.220", "data0")


class TestTheServiceIsDeletedThroughTheAPIServer:
    def test_the_name_is_the_pods_rule(self):
        assert storage_network.service_name(4426, "15260308-01ab-4256-a01c-8bbf121bbeed") == "snode-spdk-svc-4426-152603"

    def test_deletion_goes_to_the_api_server_not_the_agent(self):
        from kubernetes.client import ApiException

        from simplyblock_core import storage_node_ops

        core = MagicMock()
        node = _pod_node("n1", 4426)
        with patch.object(storage_node_ops.utils, "get_k8s_core_client", return_value=core), \
                patch.object(StorageNode, "client") as agent:
            storage_node_ops._delete_spdk_service(node, 4426, "15260308-01ab-4256-a01c-8bbf121bbeed")

        core.delete_namespaced_service.assert_called_once_with("snode-spdk-svc-4426-152603", storage_node_ops.constants.K8S_NAMESPACE)
        agent.assert_not_called()

        core.delete_namespaced_service.side_effect = ApiException(status=404)
        with patch.object(storage_node_ops.utils, "get_k8s_core_client", return_value=core), \
                patch.object(StorageNode, "client") as agent:
            storage_node_ops._delete_spdk_service(node, 4426, "15260308-01ab-4256-a01c-8bbf121bbeed")
        agent.assert_not_called()

    def test_the_agent_is_the_fallback_when_the_api_server_cannot_be_reached(self):
        from simplyblock_core import storage_node_ops

        node = _pod_node("n1", 4426)
        agent = MagicMock()
        with patch.object(storage_node_ops.utils, "get_k8s_core_client", side_effect=RuntimeError("no kubeconfig")), \
                patch.object(StorageNode, "client", return_value=agent):
            storage_node_ops._delete_spdk_service(node, 4426, "c1")

        agent.spdk_service_delete.assert_called_once_with(4426, "c1")
