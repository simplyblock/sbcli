"""Live network checks answered by the SPDK proxy, from inside the pod's network
namespace (operator design-storage-network.md §6, P0-2).

The node agent runs in the host's namespace, so it cannot see an interface attached to
the SPDK pod. The proxy runs in the pod, answers these as local JSON-RPC methods, and
the control plane's health checks ask it for a node whose pod is on attached networks."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import pod_net
from tests.unit.test_spdk_proxy_unit import make_proxy


def test_a_link_with_carrier_is_up(tmp_path):
    (tmp_path / "data0").mkdir()
    (tmp_path / "data0" / "carrier").write_text("1\n")

    assert pod_net.link_state("data0", sysfs=tmp_path) is True


def test_a_link_without_carrier_or_a_missing_one_is_down(tmp_path):
    (tmp_path / "data0").mkdir()
    (tmp_path / "data0" / "carrier").write_text("0\n")

    assert pod_net.link_state("data0", sysfs=tmp_path) is False
    assert pod_net.link_state("data9", sysfs=tmp_path) is False


@pytest.mark.parametrize("name", ["../etc", "a b", "", "x" * 16, "d;rm"])
def test_an_interface_name_the_kernel_would_refuse_is_never_looked_up(name, tmp_path):
    assert pod_net.link_state(name, sysfs=tmp_path) is False
    with patch.object(pod_net.subprocess, "run") as run:
        assert pod_net.ping(name, "10.0.0.1") is False
    run.assert_not_called()


def test_a_ping_is_sent_out_of_the_named_interface():
    with patch.object(pod_net.subprocess, "run", return_value=MagicMock(returncode=0)) as run:
        assert pod_net.ping("data0", "10.10.10.102") is True

    command = run.call_args.args[0]
    assert command[0] == "ping" and "-I" in command and command[command.index("-I") + 1] == "data0"
    assert command[-1] == "10.10.10.102"


def test_a_ping_that_gets_no_answer_is_false():
    with patch.object(pod_net.subprocess, "run", return_value=MagicMock(returncode=1)):
        assert pod_net.ping("data0", "10.10.10.102") is False


def test_a_target_that_is_not_an_address_is_never_pinged():
    with patch.object(pod_net.subprocess, "run") as run:
        assert pod_net.ping("data0", "10.0.0.1; reboot") is False
    run.assert_not_called()


def _call(proxy, method, **params):
    request = json.dumps({"id": 1, "method": method, "params": params}).encode()
    return json.loads(asyncio.run(proxy.rpc_call(request)))


def test_the_proxy_answers_a_link_report_itself_and_never_asks_spdk():
    proxy = make_proxy()

    with patch.object(pod_net, "link_state", return_value=True), \
            patch.object(proxy, "_exchange", side_effect=AssertionError("SPDK must not be asked")):
        answer = _call(proxy, "sbcli_link_state", ifname="data0")

    assert answer["result"] is True and answer["id"] == 1


def test_the_proxy_answers_a_ping_itself():
    proxy = make_proxy()

    with patch.object(pod_net, "ping", return_value=False) as ping, \
            patch.object(proxy, "_exchange", side_effect=AssertionError("SPDK must not be asked")):
        answer = _call(proxy, "sbcli_net_ping", ifname="data0", ip="10.10.10.102")

    assert answer["result"] is False
    ping.assert_called_once_with("data0", "10.10.10.102")


def test_every_other_method_still_goes_to_spdk():
    proxy = make_proxy()

    async def forwarded(req, req_data, log):
        return json.dumps({"id": 1, "result": "from spdk"})

    with patch.object(proxy, "_exchange", side_effect=forwarded):
        assert _call(proxy, "bdev_get_bdevs")["result"] == "from spdk"


def test_the_rpc_client_asks_the_proxy():
    from pydantic import SecretStr

    from simplyblock_core.rpc_client import RPCClient

    client = RPCClient("10.0.0.5", 8080, "u", SecretStr("p"))
    with patch.object(client, "_request", return_value=True) as request:
        assert client.net_ping("data0", "10.10.10.102") is True
        assert client.link_state("data0") is True

    assert request.call_args_list[0].args == ("sbcli_net_ping",)
    assert request.call_args_list[0].kwargs == {"ifname": "data0", "ip": "10.10.10.102"}
    assert request.call_args_list[1].args == ("sbcli_link_state",)


class TestTheHealthCheckAsksWhereTheInterfaceIs:
    def test_a_node_on_attached_networks_is_asked_through_its_proxy_not_the_agent(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock()
        node.networks = [{"name": "fabric", "interface": "data0"}]
        node.rpc_client.return_value.link_state.return_value = True
        node.rpc_client.return_value.net_ping.return_value = True

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is True
        node.client.assert_not_called()

    def test_a_dead_link_fails_the_check_without_a_ping(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock()
        node.networks = [{"name": "fabric"}]
        node.rpc_client.return_value.link_state.return_value = False

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is False
        node.rpc_client.return_value.net_ping.assert_not_called()

    def test_a_proxy_that_cannot_be_reached_is_inconclusive(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock()
        node.networks = [{"name": "fabric"}]
        node.rpc_client.return_value.link_state.side_effect = RuntimeError("timeout")

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is None

    def test_a_host_network_node_is_asked_through_the_agent_as_before(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock()
        node.networks = []
        node.client.return_value.ping_ip.return_value = (True, None)

        assert health_controller._check_ping_from_node("10.0.0.5", ifname="eth1", node=node) is True
        node.rpc_client.assert_not_called()
