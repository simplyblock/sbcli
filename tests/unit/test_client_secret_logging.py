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


def test_rpc_client_response_body_hidden_by_default(rpc_client, caplog):
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


def test_snode_response_body_hidden_by_default(snode_client, caplog):
    snode_client._fake_session.request.return_value = _make_json_response({"results": {"x": "RESPVAL"}})
    with caplog.at_level(logging.DEBUG):
        snode_client._request("GET", "info")
    assert "RESPVAL" not in _captured_logs_text(caplog)
