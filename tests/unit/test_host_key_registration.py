# coding=utf-8
"""Tests for host_auth._register_dhchap_keys_on_node keyring error handling.

After routing keyring registration through rpc_client.keyring_file_add_key
(which raises RPCException on failure, absorbing the -17 "already exists" case),
a key that fails to register must be skipped from the returned name map — not
propagated and not silently included.
"""
from types import SimpleNamespace

from simplyblock_core.controllers import host_auth
from simplyblock_core.rpc_client import RPCException


class _Api:
    def write_key_file(self, name, value):
        return f"/keys/{name}", None


def _snode():
    return SimpleNamespace(client=lambda: _Api(), get_id=lambda: "node-1")


def test_register_includes_key_on_success():
    rpc = SimpleNamespace(keyring_file_add_key=lambda *a, **k: None)
    names = host_auth._register_dhchap_keys_on_node(_snode(), "nqn:host", {"dhchap_key": "raw"}, rpc)
    assert names == {"dhchap_key": "dhchap_key_nqn_host"}


def test_register_skips_key_on_rpc_error():
    def _raise(*a, **k):
        raise RPCException("boom", code=-1)

    rpc = SimpleNamespace(keyring_file_add_key=_raise)
    names = host_auth._register_dhchap_keys_on_node(_snode(), "nqn:host", {"dhchap_key": "raw"}, rpc)
    assert names == {}
