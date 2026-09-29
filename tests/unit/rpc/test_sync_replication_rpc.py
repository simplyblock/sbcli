"""RPC payloads of the sync-replication additions: new params are sent only when set."""
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient


def _client():
    with patch("requests.session"):
        return RPCClient("127.0.0.1", 8080, "user", SecretStr("pass"), timeout=1, retry=0)


def _distrib_create_params(**kwargs):
    client = _client()
    with patch.object(client, "get_bdevs", return_value=[]), \
            patch.object(client, "_request", return_value=True) as request:
        client.bdev_distrib_create(
            "distrib_1", 1, 2, 1, 1000, 4096, ["jm_n1", "remote_jm_n2n1"], 4096,
            jm_vuid=7, **kwargs)
    method, params = request.call_args.args
    assert method == "bdev_distrib_create"
    return params


def test_distrib_create_without_sync_params_is_unchanged():
    params = _distrib_create_params()
    assert "synchronous_replication_mode" not in params
    assert "jm_n_local" not in params
    assert params["jm_names"] == "jm_n1,remote_jm_n2n1"
    assert params["jm_vuid"] == 7


def test_distrib_create_sends_sync_params():
    params = _distrib_create_params(synchronous_replication_mode=1, jm_n_local=3)
    assert params["synchronous_replication_mode"] == 1
    assert params["jm_n_local"] == 3


def test_distrib_create_sends_explicit_zeroes():
    """0 is a real value ("disabled" / "all local"), not "unset"."""
    params = _distrib_create_params(synchronous_replication_mode=0, jm_n_local=0)
    assert params["synchronous_replication_mode"] == 0
    assert params["jm_n_local"] == 0


@pytest.mark.parametrize("method", ["distr_add_nodes", "distr_add_devices"])
def test_add_without_name_targets_every_distrib(method):
    client = _client()
    payload = {"map_cluster": {"n1": {"status": "online"}}}
    with patch.object(client, "_request", return_value=True) as request:
        getattr(client, method)(payload)
    assert request.call_args.args == (method, {"map_cluster": {"n1": {"status": "online"}}})


@pytest.mark.parametrize("method", ["distr_add_nodes", "distr_add_devices"])
def test_add_with_name_targets_one_distrib_without_mutating_the_caller(method):
    client = _client()
    payload = {"map_cluster": {"n1": {"status": "online", "replica": True}}}
    with patch.object(client, "_request", return_value=True) as request:
        getattr(client, method)(payload, name="distrib_1")
    sent_method, sent = request.call_args.args
    assert sent_method == method
    assert sent["name"] == "distrib_1"
    assert sent["map_cluster"] == payload["map_cluster"]
    assert "name" not in payload


def test_sync_replication_status_for_all_distribs():
    client = _client()
    with patch.object(client, "_request", return_value=[]) as request:
        assert client.distr_sync_replication_status() == []
    request.assert_called_once_with("distr_sync_replication_status", {})


def test_sync_replication_status_for_one_distrib():
    client = _client()
    answer = [{"name": "distrib_1", "vuid": 1, "ha_leader": True,
               "sync_replication_mode": "full", "status": "synced"}]
    with patch.object(client, "_request", return_value=answer) as request:
        assert client.distr_sync_replication_status("distrib_1") == answer
    request.assert_called_once_with("distr_sync_replication_status", {"name": "distrib_1"})
