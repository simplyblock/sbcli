"""Verifies that secrets never reach log records but do reach the wire body."""
import json
import logging
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient
from simplyblock_core.snode_client import SNodeClient


def _make_json_response(payload):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload
    response.content = json.dumps(payload).encode()
    response.text = json.dumps(payload)
    return response


def _captured_logs_text(caplog) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


@pytest.fixture
def rpc_client():
    with patch("simplyblock_core.rpc_client.requests.session") as session_factory:
        session = MagicMock()
        session_factory.return_value = session
        client = RPCClient("host", 9999, "user", SecretStr("ctor-secret"))
        client._fake_session = session  # expose for assertions
        yield client


def test_rpc_client_session_auth_carries_unwrapped_password(rpc_client):
    user, password = rpc_client._fake_session.auth
    assert user == "user"
    assert password == "ctor-secret"


def test_rpc_client_request_body_carries_unwrapped_param(rpc_client, caplog):
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": {"ok": True},
    })

    with caplog.at_level(logging.DEBUG):
        rpc_client._request2("nvmf_subsystem_add_host", {
            "nqn": "nqn.example",
            "dhchap_key": SecretStr("DHCHAPVALUE"),
        })

    posted_body = rpc_client._fake_session.post.call_args.kwargs["data"]
    parsed = json.loads(posted_body)
    assert parsed["params"]["dhchap_key"] == "DHCHAPVALUE"

    assert "DHCHAPVALUE" not in _captured_logs_text(caplog)
    assert "ctor-secret" not in _captured_logs_text(caplog)


def test_rpc_client_response_body_hidden_when_flag_off(rpc_client, caplog, monkeypatch):
    monkeypatch.setenv("SB_LOG_RESPONSE_BODIES", "false")
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": {"sensitive": "RESPVALUE"},
    })
    with caplog.at_level(logging.DEBUG):
        rpc_client._request2("some_method", {})

    assert "RESPVALUE" not in _captured_logs_text(caplog)


def test_rpc_client_response_body_logged_when_flag_on(rpc_client, caplog, monkeypatch):
    monkeypatch.setenv("SB_LOG_RESPONSE_BODIES", "true")
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": {"sensitive": "RESPVALUE"},
    })
    with caplog.at_level(logging.DEBUG):
        rpc_client._request2("some_method", {})

    assert "RESPVALUE" in _captured_logs_text(caplog)


def _sent_params(client):
    return json.loads(client._fake_session.post.call_args.kwargs["data"])["params"]


def test_bdev_s3_create_keys_reach_the_wire_but_not_the_log(rpc_client, caplog):
    # bdev_s3_create is the only RPC carrying S3 keys, and it goes through
    # _request3, which logs its parameter dict directly -- only a SecretStr
    # masks there.
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": True,
    })

    with caplog.at_level(logging.DEBUG):
        rpc_client.bdev_s3_create(
            name="s3_lvs_test",
            access_key_id=SecretStr("AKIAEXAMPLE"),
            secret_access_key=SecretStr("s3cr3t"),
        )

    params = _sent_params(rpc_client)
    assert params["access_key_id"] == "AKIAEXAMPLE"
    assert params["secret_access_key"] == "s3cr3t"

    logged = _captured_logs_text(caplog)
    assert "AKIAEXAMPLE" not in logged
    assert "s3cr3t" not in logged
    assert "**********" in logged


def test_bdev_s3_create_omits_absent_credentials(rpc_client):
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": True,
    })

    rpc_client.bdev_s3_create(name="s3_lvs_test")

    params = _sent_params(rpc_client)
    assert "access_key_id" not in params
    assert "secret_access_key" not in params


def test_bdev_s3_create_does_not_send_empty_credentials_as_keys(rpc_client):
    # An empty key pair is not an absent one to the AWS SDK: it reads as a valid
    # anonymous identity, and the default provider chain (the node's instance
    # role) is then never consulted.
    rpc_client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": True,
    })

    rpc_client.bdev_s3_create(
        name="s3_lvs_test",
        access_key_id=SecretStr(""), secret_access_key=SecretStr(""),
    )

    params = _sent_params(rpc_client)
    assert "access_key_id" not in params
    assert "secret_access_key" not in params


@pytest.fixture
def snode_client():
    with patch("simplyblock_core.snode_client.requests.session") as session_factory:
        session = MagicMock()
        session_factory.return_value = session
        session.headers = {}
        client = SNodeClient("snode.host")
        client._fake_session = session
        yield client


def test_snode_request_wire_unwraps_secrets(snode_client, caplog):
    snode_client._fake_session.request.return_value = _make_json_response({"results": "ok"})

    with caplog.at_level(logging.DEBUG):
        snode_client._request("POST", "spdk_process_start", {
            "rpc_username": "u",
            "rpc_password": SecretStr("PWVALUE"),
        })

    posted_body = snode_client._fake_session.request.call_args.kwargs["data"]
    parsed = json.loads(posted_body)
    assert parsed["rpc_password"] == "PWVALUE"

    assert "PWVALUE" not in _captured_logs_text(caplog)


def test_snode_response_body_hidden_when_flag_off(snode_client, caplog, monkeypatch):
    monkeypatch.setenv("SB_LOG_RESPONSE_BODIES", "false")
    snode_client._fake_session.request.return_value = _make_json_response({"results": {"x": "RESPVAL"}})
    with caplog.at_level(logging.DEBUG):
        snode_client._request("GET", "info")
    assert "RESPVAL" not in _captured_logs_text(caplog)
