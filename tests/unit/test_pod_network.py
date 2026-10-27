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


class TestTheWorkersAddressOfANodeOnThePodNetwork:
    def test_the_agent_reports_its_hosts_address(self):
        from simplyblock_core import utils

        info = {"host_ip": "192.168.10.62", "network_interface": {"eth0": {"ip": "10.42.1.9"}}}

        assert utils.get_mgmt_ip(info, "eth0", None, pod_network=True) == ("192.168.10.62", "")
