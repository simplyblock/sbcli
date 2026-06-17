# coding=utf-8
"""Regression tests for RPCClient HTTP retry policy.

SPDK JSON-RPC sends every call (including non-idempotent mutations) as a POST.
A read-error retry of a POST silently re-applies the mutation, so POST must be
excluded from the urllib3 retry's allowed_methods. Connection-error retries are
governed separately (the `connect` count) and are safe, so they must remain.
"""
from unittest.mock import patch

from simplyblock_core.rpc_client import RPCClient


def _make_client(retry=3, **kwargs):
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8081, "user", "pass", timeout=1, retry=retry, **kwargs)


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
