"""Regression tests for SecretStr serialization in the v1 Flask API.

`flask.jsonify` calls stdlib `json.dumps` underneath and chokes on
`SecretStr`/`SecretBytes` wrappers — the same failure mode the CLI hit
on 882e2f475c. v1 endpoints feed `BaseModel.get_clean_dict()` (which keeps
wrappers) into `utils.get_response()`, so without an unwrap at the wire
boundary the response raises TypeError.

These tests pin the unwrap-at-boundary behavior in
`simplyblock_web.utils._to_jsonable`, mirroring v2's
`@field_serializer(when_used='json')` semantics: the API is auth-protected
and clients (sbctl, dashboard) need plaintext.
"""

import json

import pytest
from flask import Flask
from pydantic import SecretStr

from simplyblock_web import utils
from simplyblock_core.models.storage_node import StorageNode


@pytest.fixture
def app_ctx():
    app = Flask(__name__)
    with app.app_context():
        yield


def test_get_response_unwraps_secretstr(app_ctx):
    resp = utils.get_response({"password": SecretStr("hunter2")})
    body = json.loads(resp.get_data(as_text=True))
    assert body["status"] is True
    assert body["results"] == {"password": "hunter2"}


def test_get_response_unwraps_nested_secretstr(app_ctx):
    resp = utils.get_response({
        "outer": {"password": SecretStr("hunter2")},
        "list": [{"token": SecretStr("t0k3n")}],
    })
    body = json.loads(resp.get_data(as_text=True))
    assert body["results"] == {
        "outer": {"password": "hunter2"},
        "list": [{"token": "t0k3n"}],
    }


def test_get_response_serializes_storage_node_dict(app_ctx):
    """Reproduces the v1 failure: list_lvols / storage_node detail / etc.
    feed `node.get_clean_dict()` straight into `get_response`. Without the
    fix, Flask's encoder raises TypeError on the SecretStr fields."""
    node = StorageNode()
    node.rpc_password = SecretStr("rpc-secret")
    node.ctrl_secret = SecretStr("ctrl-secret")
    node.host_secret = SecretStr("host-secret")

    data = node.get_clean_dict()
    with pytest.raises(TypeError, match="SecretStr"):
        json.dumps(data)

    resp = utils.get_response(data)
    body = json.loads(resp.get_data(as_text=True))
    assert body["status"] is True
    assert body["results"]["rpc_password"] == "rpc-secret"
    assert body["results"]["ctrl_secret"] == "ctrl-secret"
    assert body["results"]["host_secret"] == "host-secret"
