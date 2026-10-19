"""RPC payloads of the sync-replication additions, as posted on the wire: new
params are sent only when set."""
import itertools
import json
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient

# RPCSessionPool caches a session per (host, port, ...): a port per client
# keeps each test on the session it patched.
_ports = itertools.count(19100)


def _response(result):
    payload = {"jsonrpc": "2.0", "id": 1, "result": result}
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload
    response.text = json.dumps(payload)
    return response


def _client(*results):
    """A client whose session answers ``results`` in order."""
    with patch("simplyblock_core.rpc_client.requests.session") as session_factory:
        session = MagicMock()
        session_factory.return_value = session
        client = RPCClient("127.0.0.1", next(_ports), "user", SecretStr("pass"), timeout=1, retry=0)
    session.post.side_effect = [_response(r) for r in results]
    return client, session


def _posted(session):
    """Every posted JSON-RPC request, in order."""
    return [json.loads(call.kwargs["data"]) for call in session.post.call_args_list]


def _distrib_create_request(**kwargs):
    # The create probes for an existing bdev first: none.
    client, session = _client([], True)
    client.bdev_distrib_create(
        "distrib_1", 1, 2, 1, 1000, 4096, ["jm_n1", "remote_jm_n2n1"], 4096,
        jm_vuid=7, **kwargs)
    probe, create = _posted(session)
    assert probe["method"] == "bdev_get_bdevs" and probe["params"] == {"name": "distrib_1"}
    assert create["method"] == "bdev_distrib_create"
    return create["params"]


def test_distrib_create_omits_sync_fields_by_default():
    params = _distrib_create_request()
    assert "synchronous_replication_mode" not in params
    assert "jm_n_local" not in params
    assert params["jm_names"] == "jm_n1,remote_jm_n2n1"
    assert params["jm_vuid"] == 7


def test_distrib_create_sends_sync_params():
    params = _distrib_create_request(synchronous_replication_mode=1, jm_n_local=3)
    assert params["synchronous_replication_mode"] == 1
    assert params["jm_n_local"] == 3


def test_distrib_create_sends_explicit_zeroes():
    """0 is a real value ("disabled" / "all local"), not "unset"."""
    params = _distrib_create_request(synchronous_replication_mode=0, jm_n_local=0)
    assert params["synchronous_replication_mode"] == 0
    assert params["jm_n_local"] == 0


@pytest.mark.parametrize("method", ["distr_add_nodes", "distr_add_devices"])
@pytest.mark.parametrize("name", [None, ""])
def test_add_without_name_targets_every_distrib(method, name):
    client, session = _client(True)
    payload = {"map_cluster": {"n1": {"status": "online"}}}
    getattr(client, method)(payload, name=name)
    assert _posted(session) == [{"id": 1, "method": method,
                                 "params": {"map_cluster": {"n1": {"status": "online"}}}}]


@pytest.mark.parametrize("method", ["distr_add_nodes", "distr_add_devices"])
def test_add_with_name_targets_one_distrib(method):
    client, session = _client(True)
    payload = {"map_cluster": {"n1": {"status": "online", "replica": True}}}
    getattr(client, method)(payload, name="distrib_1")
    (sent,) = _posted(session)
    assert sent["method"] == method
    assert sent["params"] == {"map_cluster": payload["map_cluster"], "name": "distrib_1"}
    assert "name" not in payload


def test_sync_replication_status_for_all_distribs():
    client, session = _client([])
    assert client.distr_sync_replication_status() == []
    # No name: no params key at all, not an empty object.
    assert _posted(session) == [{"id": 1, "method": "distr_sync_replication_status"}]


def test_sync_replication_status_for_one_distrib():
    answer = [{"name": "distrib_1", "vuid": 1, "ha_leader": True,
               "sync_replication_mode": "full", "status": "synced"}]
    client, session = _client(answer)
    assert client.distr_sync_replication_status("distrib_1") == answer
    assert _posted(session) == [{"id": 1, "method": "distr_sync_replication_status",
                                 "params": {"name": "distrib_1"}}]
