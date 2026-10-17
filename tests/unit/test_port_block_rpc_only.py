"""Port fencing goes through the SPDK RPCs only, never the firewall API.

Fences used to go through ``utils.port_block``, which sent them to the
legacy firewall webapp (``FirewallClient``, ``mgmt_ip:firewall_port``)
whenever ``node.spdk_version`` was empty. 25.10.x records never carried that
field, so every node upgraded to 26.3 took that path -- and a 26.3 host has
nothing listening on the firewall port (the SPDK image no longer ships the
webapp or iptables), so every fence on an upgraded node failed with
connection refused. Callers now use the SPDK RPCs on ``node.rpc_client(...)``
directly; there is no other path to fall back to.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from simplyblock_core.controllers import health_controller
from simplyblock_core.rpc_client import RPCClient, RPCErrorCode, RPCRemoteError


def _client():
    return RPCClient("127.0.0.1", 8080, "user", SecretStr("pass"), timeout=1, retry=0)


@pytest.mark.parametrize("reply, expected", [
    ({"blocked_ports": [{"port": 9101}, {"port": "9031"}]}, {9101, 9031}),
    ({"blocked_ports": []}, set()),
    ({}, set()),
    (None, set()),
])
def test_blocked_ports_are_projected_to_a_set(reply, expected):
    client = _client()
    with patch.object(client, "_request3", return_value=reply) as request:
        assert client.nvmf_get_blocked_ports() == expected
    request.assert_called_once_with("nvmf_get_blocked_ports")


def _node(rpc):
    return SimpleNamespace(rpc_client=lambda **_: rpc, get_id=lambda: "node-1")


def test_port_checks_read_the_rpc():
    rpc = MagicMock()
    rpc.nvmf_get_blocked_ports.return_value = {9101, 9031}
    node = _node(rpc)

    assert health_controller.check_port_on_node(node, 9101) is False
    assert health_controller.check_port_on_node(node, "9100") is True
    assert health_controller.check_ports_on_node(node, [9101, 9100]) == {
        9101: False, 9100: True}


def test_method_not_found_propagates_instead_of_falling_back():
    rpc = MagicMock()
    rpc.nvmf_get_blocked_ports.side_effect = RPCRemoteError(
        "Method not found", RPCErrorCode.method_not_found)

    with pytest.raises(RPCRemoteError):
        health_controller.check_ports_on_node(_node(rpc), [9101])
    with pytest.raises(RPCRemoteError):
        health_controller.check_port_on_node(_node(rpc), 9101)
