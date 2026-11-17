"""bdev_lvol_merge_base carries the replication's IO class, and still starts
the merge on an SPDK that does not know the parameter."""
from unittest.mock import patch

import pytest

from simplyblock_core.rpc_client import RPCClient, RPCRemoteError


@pytest.fixture
def client():
    with patch.object(RPCClient, "__init__", lambda self, *a, **k: None):
        c = RPCClient()
    c.host, c.port = "10.0.0.1", 8080
    return c


def test_no_class_sends_the_old_request(client):
    with patch.object(client, "_request", return_value={"state": "running"}) as req:
        assert client.bdev_lvol_merge_base("LVS/L") == {"state": "running"}
    req.assert_called_once_with("bdev_lvol_merge_base", lvol_name="LVS/L")


def test_the_class_is_sent(client):
    with patch.object(client, "_request", return_value={"state": "running"}) as req:
        client.bdev_lvol_merge_base("LVS/L", priority_class=7)
    req.assert_called_once_with("bdev_lvol_merge_base", lvol_name="LVS/L", priority_class=7)


def test_an_older_spdk_merges_without_the_class(client):
    calls = []

    def fake(method, **params):
        calls.append(params)
        if "priority_class" in params:
            raise RPCRemoteError("spdk_json_decode_object failed", -32603)
        return {"state": "running"}

    with patch.object(client, "_request", side_effect=fake):
        assert client.bdev_lvol_merge_base("LVS/L", priority_class=7) == {"state": "running"}
    assert calls == [{"lvol_name": "LVS/L", "priority_class": 7}, {"lvol_name": "LVS/L"}]


def test_other_errors_are_not_swallowed(client):
    with patch.object(client, "_request", side_effect=RPCRemoteError("No such device", -19)):
        with pytest.raises(RPCRemoteError):
            client.bdev_lvol_merge_base("LVS/L", priority_class=7)
