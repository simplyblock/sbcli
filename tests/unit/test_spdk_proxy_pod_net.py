"""Live network checks answered by the SPDK proxy, from inside the pod's network
namespace (operator design-storage-network.md §6, P0-2).

The node agent runs in the host's namespace, so it cannot see an interface attached to
the SPDK pod. The proxy runs in the pod, answers this as a local JSON-RPC method, and
the control plane's health checks ask it for a node whose pod is on attached networks."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import pod_net
from tests.unit.test_spdk_proxy_unit import make_proxy


def _carrier(tmp_path, value):
    (tmp_path / "data0").mkdir()
    (tmp_path / "data0" / "carrier").write_text(value)


def test_a_link_with_carrier_whose_address_answers_passes(tmp_path):
    _carrier(tmp_path, "1\n")

    with patch.object(pod_net.subprocess, "run", return_value=MagicMock(returncode=0)) as run:
        assert pod_net.check("data0", "10.10.10.102", sysfs=tmp_path) is True

    command = run.call_args.args[0]
    assert command[0] == "ping" and command[command.index("-I") + 1] == "data0" and command[-1] == "10.10.10.102"
    assert run.call_args.kwargs["timeout"] == pod_net._PING_TIMEOUT_SEC


def test_a_dead_link_fails_without_a_ping(tmp_path):
    _carrier(tmp_path, "0\n")

    with patch.object(pod_net.subprocess, "run") as run:
        assert pod_net.check("data0", "10.10.10.102", sysfs=tmp_path) is False
        assert pod_net.check("data9", "10.10.10.102", sysfs=tmp_path) is False
    run.assert_not_called()


def test_an_address_that_gets_no_answer_fails(tmp_path):
    _carrier(tmp_path, "1\n")

    with patch.object(pod_net.subprocess, "run", return_value=MagicMock(returncode=1)):
        assert pod_net.check("data0", "10.10.10.102", sysfs=tmp_path) is False


def test_a_ping_that_hangs_fails(tmp_path):
    _carrier(tmp_path, "1\n")

    with patch.object(pod_net.subprocess, "run", side_effect=pod_net.subprocess.TimeoutExpired("ping", 10)):
        assert pod_net.check("data0", "10.10.10.102", sysfs=tmp_path) is False


@pytest.mark.parametrize(("name", "ip"), [
    ("../etc", "10.0.0.1"), ("a b", "10.0.0.1"), ("", "10.0.0.1"), ("x" * 16, "10.0.0.1"), ("d;rm", "10.0.0.1"),
    ("data0", "10.0.0.1; reboot"), ("data0", ""),
])
def test_a_name_or_address_that_would_reach_sysfs_or_a_shell_is_never_used(name, ip, tmp_path):
    _carrier(tmp_path, "1\n")

    with patch.object(pod_net.subprocess, "run") as run:
        assert pod_net.check(name, ip, sysfs=tmp_path) is False
    run.assert_not_called()


def _call(proxy, method, **params):
    request = json.dumps({"id": 1, "method": method, "params": params}).encode()
    return json.loads(asyncio.run(proxy.rpc_call(request)))


def test_the_proxy_answers_a_network_check_itself_and_never_asks_spdk():
    proxy = make_proxy()

    with patch.object(pod_net, "check", return_value=False) as check, \
            patch.object(proxy, "_exchange", side_effect=AssertionError("SPDK must not be asked")):
        answer = _call(proxy, "sbcli_net_check", ifname="data0", ip="10.10.10.102")

    assert answer["result"] is False and answer["id"] == 1
    check.assert_called_once_with("data0", "10.10.10.102")


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
        assert client.net_check("data0", "10.10.10.102") is True

    assert request.call_args.args == ("sbcli_net_check",)
    assert request.call_args.kwargs == {"ifname": "data0", "ip": "10.10.10.102"}


class TestTheHealthCheckAsksWhereTheInterfaceIs:
    def test_a_node_on_attached_networks_is_asked_through_its_proxy_not_the_agent(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock(on_pod_network=True)
        node.rpc_client.return_value.net_check.return_value = True

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is True
        node.client.assert_not_called()

    def test_a_failed_check_fails_the_node_check(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock(on_pod_network=True)
        node.rpc_client.return_value.net_check.return_value = False

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is False

    def test_a_proxy_that_cannot_be_reached_is_inconclusive(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock(on_pod_network=True)
        node.rpc_client.return_value.net_check.side_effect = RuntimeError("timeout")

        assert health_controller._check_ping_from_node("10.10.10.101", ifname="data0", node=node) is None

    def test_a_host_network_node_is_asked_through_the_agent_as_before(self):
        from simplyblock_core.controllers import health_controller

        node = MagicMock(on_pod_network=False)
        node.client.return_value.ping_ip.return_value = (True, None)

        assert health_controller._check_ping_from_node("10.0.0.5", ifname="eth1", node=node) is True
        node.rpc_client.assert_not_called()
