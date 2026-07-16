# coding=utf-8
"""Unit tests for host_auth._register_key_on_node key-delivery selection.

The control plane prefers the spdk-proxy's keyring_add_key interceptor and only
falls back to the deprecated write_key_file + keyring_file_add_key path when the
proxy predates interception support — signalled by SPDK rejecting the forwarded
keyring_add_key with -32601 ("Method not found").
"""

from unittest.mock import MagicMock

from pydantic import SecretStr

from simplyblock_core.controllers.host_auth import _register_key_on_node
from simplyblock_core.rpc_client import RPCException

KEY = SecretStr("DHHC-1:00:c2VjcmV0LWtleS1tYXRlcmlhbA==:")


def _snode():
    snode = MagicMock()
    snode.get_id.return_value = "node-1"
    return snode


def test_uses_interceptor_when_supported():
    """keyring_add_key succeeds → key registered inline, no on-disk fallback."""
    snode = _snode()
    rpc = MagicMock()
    rpc.keyring_add_key.return_value = None

    assert _register_key_on_node(snode, rpc, "key_1", KEY) is True

    rpc.keyring_add_key.assert_called_once_with("key_1", KEY, allow_existing=True)
    snode.client.assert_not_called()
    rpc.keyring_file_add_key.assert_not_called()


def test_falls_back_on_method_not_found():
    """-32601 → old proxy: fall back to write_key_file + keyring_file_add_key."""
    snode = _snode()
    snode.client.return_value.write_key_file.return_value = ("/keys/key_1", None)
    rpc = MagicMock()
    rpc.keyring_add_key.side_effect = RPCException("Method not found", code=-32601)
    rpc.keyring_file_add_key.return_value = None

    assert _register_key_on_node(snode, rpc, "key_1", KEY) is True

    snode.client.return_value.write_key_file.assert_called_once_with("key_1", KEY)
    rpc.keyring_file_add_key.assert_called_once_with(
        "key_1", "/keys/key_1", allow_existing=True)


def test_no_fallback_on_other_rpc_error():
    """A non -32601 error is a real failure: no fallback, returns False."""
    snode = _snode()
    rpc = MagicMock()
    rpc.keyring_add_key.side_effect = RPCException("bad params", code=-32602)

    assert _register_key_on_node(snode, rpc, "key_1", KEY) is False

    snode.client.assert_not_called()
    rpc.keyring_file_add_key.assert_not_called()


def test_fallback_write_key_file_error_returns_false():
    """When the fallback write_key_file reports an error, registration fails."""
    snode = _snode()
    snode.client.return_value.write_key_file.return_value = (None, "disk full")
    rpc = MagicMock()
    rpc.keyring_add_key.side_effect = RPCException("Method not found", code=-32601)

    assert _register_key_on_node(snode, rpc, "key_1", KEY) is False

    rpc.keyring_file_add_key.assert_not_called()
