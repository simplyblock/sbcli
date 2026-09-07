"""Regression tests for RPCClient HTTP retry policy.

SPDK JSON-RPC sends every call (including non-idempotent mutations) as a POST.
A read-error retry of a POST silently re-applies the mutation, so POST must be
excluded from the urllib3 retry's allowed_methods. Connection-error retries are
governed separately (the `connect` count) and are safe, so they must remain.
"""
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient


def _make_client(retry=3, **kwargs):
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=retry, **kwargs)


def _mounted_retries(client):
    # session is mocked; each mount() call got (prefix, HTTPAdapter(max_retries=Retry)).
    adapters = [call.args[1] for call in client.session.mount.call_args_list]
    assert adapters, "expected at least one mounted adapter"
    return [ad.max_retries for ad in adapters]


def test_post_not_in_default_allowed_methods():
    assert "POST" not in RPCClient.DEFAULT_ALLOWED_METHODS


def test_mounted_retry_excludes_post_keeps_reads():
    client = _make_client()
    for retries in _mounted_retries(client):
        methods = {m.upper() for m in retries.allowed_methods}
        assert "POST" not in methods       # no read-retry of mutating calls
        assert "GET" in methods            # idempotent reads still retried


def test_connect_retries_preserved():
    client = _make_client(retry=3)
    for retries in _mounted_retries(client):
        # Connection-level retries (request never reached the node) stay enabled
        # even though POST read-retries are off.
        assert retries.connect == 3


def test_same_retry_value_shares_one_mounted_adapter_set():
    with patch("requests.session"):
        c1 = RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=3)
        c2 = RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=3)
    assert c1.session is c2.session
    assert c1.session.mount.call_count == 2  # http:// + https://, mounted once


def test_different_retry_value_gets_independently_mounted_adapter():
    with patch("requests.session", side_effect=MagicMock):
        c1 = RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=2)
        c2 = RPCClient("127.0.0.1", 8081, "user", SecretStr("pass"), timeout=1, retry=5)
    assert c1.session is not c2.session
    retries_c1 = {ad.max_retries.total for ad in
                 [call.args[1] for call in c1.session.mount.call_args_list]}
    retries_c2 = {ad.max_retries.total for ad in
                 [call.args[1] for call in c2.session.mount.call_args_list]}
    assert retries_c1 == {2}
    assert retries_c2 == {5}
