# coding=utf-8
"""EdgeRpcClient transport behavior."""
import pytest
from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient, RPCException
from simplyblock_edge.rpc import EdgeRpcClient


def _client():
    return EdgeRpcClient("10.0.0.1", 8080, "u", SecretStr("p"))


def test_connection_error_is_retried(monkeypatch):
    """The proxy closes its side after each response while requests reuses
    connections (keep-alive), so an rpc can hit a just-closed socket."""
    calls = []

    def flaky(self, method, params=None, request_timeout=None):
        calls.append(method)
        if len(calls) < 3:
            raise RPCException("connection error")
        return {"ok": True}, None

    monkeypatch.setattr(RPCClient, "_request2", flaky)
    monkeypatch.setattr("simplyblock_edge.rpc.time.sleep", lambda s: None)
    assert _client()._request("framework_start_init") == {"ok": True}
    assert len(calls) == 3


def test_connection_error_exhausts_and_raises(monkeypatch):
    def dead(self, method, params=None, request_timeout=None):
        raise RPCException("connection error")

    monkeypatch.setattr(RPCClient, "_request2", dead)
    monkeypatch.setattr("simplyblock_edge.rpc.time.sleep", lambda s: None)
    with pytest.raises(RPCException, match="connection error"):
        _client()._request("get_version")


def test_rpc_level_errors_are_not_retried(monkeypatch):
    calls = []

    def rpc_error(self, method, params=None, request_timeout=None):
        calls.append(method)
        raise RPCException("Lvol store not found")

    monkeypatch.setattr(RPCClient, "_request2", rpc_error)
    with pytest.raises(RPCException, match="not found"):
        _client()._request("bdev_lvol_update_lvstore")
    assert len(calls) == 1
