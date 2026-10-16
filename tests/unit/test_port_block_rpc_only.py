"""Port fencing goes through the SPDK RPCs only, never the firewall API.

``port_block.set_port`` used to send fences to the legacy firewall webapp
(``FirewallClient``, ``mgmt_ip:firewall_port``) whenever ``node.spdk_version``
was empty. 25.10.x records never carried that field, so every node upgraded
to 26.3 took that path -- and a 26.3 host has nothing listening on the
firewall port (the SPDK image no longer ships the webapp or iptables), so
every fence on an upgraded node failed with connection refused. A
method-not-found from the RPC was redirected to the same dead port.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from simplyblock_core.controllers import health_controller
from simplyblock_core.rpc_client import RPCErrorCode, RPCRemoteError
from simplyblock_core.utils import port_block


def _node(rpc, spdk_version=""):
    return SimpleNamespace(spdk_version=spdk_version, rpc_client=lambda **_: rpc,
                           get_id=lambda: "node-1")


@pytest.mark.parametrize("spdk_version", ["", "R26.2-PRE-latest", "R26.3"])
def test_block_and_unblock_use_rpc_whatever_the_recorded_version(spdk_version):
    rpc = MagicMock()
    node = _node(rpc, spdk_version)

    port_block.set_port(node, 9101, block=True, is_reject=True)
    port_block.set_port(node, 9101, block=False)

    rpc.nvmf_port_block.assert_called_once_with(9101, is_reject=True)
    rpc.nvmf_port_unblock.assert_called_once_with(9101)


def test_method_not_found_propagates_instead_of_falling_back():
    rpc = MagicMock()
    rpc.nvmf_port_block.side_effect = RPCRemoteError(
        "Method not found", RPCErrorCode.method_not_found)

    with pytest.raises(RPCRemoteError):
        port_block.set_port(_node(rpc), 9101, block=True)


def test_blocked_ports_come_from_the_rpc():
    rpc = MagicMock()
    rpc.nvmf_get_blocked_ports.return_value = {
        "blocked_ports": [{"port": 9101}, {"port": 9031}]}
    node = _node(rpc)

    assert port_block.get_blocked_ports_set(node) == {9101, 9031}
    assert port_block.is_port_blocked(node, 9101) is True
    assert port_block.is_port_blocked(node, 9100) is False
    assert health_controller.check_ports_on_node(node, [9101, 9100]) == {
        9101: False, 9100: True}


def test_blocked_ports_method_not_found_propagates():
    rpc = MagicMock()
    rpc.nvmf_get_blocked_ports.side_effect = RPCRemoteError(
        "Method not found", RPCErrorCode.method_not_found)

    with pytest.raises(RPCRemoteError):
        health_controller.check_ports_on_node(_node(rpc), [9101])
