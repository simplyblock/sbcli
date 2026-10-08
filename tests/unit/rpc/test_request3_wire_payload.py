"""Regression test: _request3 must omit "params" entirely for a zero-argument
call, not send an empty object.

Some SPDK RPC handlers (framework_start_init is the one that broke in
production) reject any request carrying a "params" field at all, even `{}` --
"framework_start_init requires no parameters". _request2 already got this
right (`if params: payload['params'] = params`); _request3 regressed it by
always setting `'params': kwargs`.
"""
import json
from unittest.mock import MagicMock, patch

from pydantic import SecretStr

from simplyblock_core.rpc_client import RPCClient


def _make_json_response(payload):
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = payload
    response.content = json.dumps(payload).encode()
    response.text = json.dumps(payload)
    return response


def _make_client():
    with patch("simplyblock_core.rpc_client.requests.session") as session_factory:
        session = MagicMock()
        session_factory.return_value = session
        client = RPCClient("host", 9999, "user", SecretStr("pass"))
        client._fake_session = session
        return client


def _posted_payload(client):
    return json.loads(client._fake_session.post.call_args.kwargs["data"])


def test_zero_argument_call_omits_params_key():
    client = _make_client()
    client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": True,
    })

    client.framework_start_init()

    assert "params" not in _posted_payload(client)


def test_call_with_arguments_still_sends_params():
    client = _make_client()
    client._fake_session.post.return_value = _make_json_response({
        "jsonrpc": "2.0", "id": 1, "result": True,
    })

    client._request3("bdev_examine", name="foo")

    assert _posted_payload(client)["params"] == {"name": "foo"}
