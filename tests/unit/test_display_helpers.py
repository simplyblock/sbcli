"""Tests for ``utils.dump_json`` and ``utils.print_table`` secret handling.

These are the two helpers that controllers and the CLI use to render
user-visible output. Both must:

  * accept dicts/lists carrying ``SecretStr`` wrappers without raising
    (raw ``json.dumps`` chokes on ``SecretStr`` — the regression behind
    ``sbctl sn get <id>`` reported on 882e2f475c after the SecretStr
    rollout).
  * mask wrappers as ``**********`` by default.
  * reveal plaintext when ``unwrap_secrets=True`` is passed by an
    authenticated display site.
"""

import json

import pytest
from pydantic import SecretStr

from simplyblock_core import utils
from simplyblock_core.models.storage_node import StorageNode


def test_dump_json_masks_secretstr_by_default():
    out = utils.dump_json({"password": SecretStr("hunter2")})
    assert "hunter2" not in out
    assert "**********" in out


def test_dump_json_unwraps_secretstr_when_requested():
    out = utils.dump_json({"password": SecretStr("hunter2")}, unwrap_secrets=True)
    assert json.loads(out) == {"password": "hunter2"}


def test_dump_json_handles_nested_secretstr():
    out = utils.dump_json(
        {"outer": {"password": SecretStr("hunter2")}},
        unwrap_secrets=True,
    )
    assert json.loads(out) == {"outer": {"password": "hunter2"}}


def test_dump_json_reproduces_sn_get_regression():
    """Plain ``json.dumps`` on a StorageNode dict raises (the reported bug);
    ``utils.dump_json`` must succeed."""
    snode = StorageNode()
    snode.rpc_password = SecretStr("rpc-secret")
    snode.ctrl_secret = SecretStr("ctrl-secret")
    snode.host_secret = SecretStr("host-secret")
    data = snode.get_clean_dict()

    with pytest.raises(TypeError, match="SecretStr"):
        json.dumps(data)

    masked = utils.dump_json(data)
    assert "rpc-secret" not in masked
    assert "ctrl-secret" not in masked
    assert "host-secret" not in masked

    revealed = json.loads(utils.dump_json(data, unwrap_secrets=True))
    assert revealed["rpc_password"] == "rpc-secret"
    assert revealed["ctrl_secret"] == "ctrl-secret"
    assert revealed["host_secret"] == "host-secret"


def test_print_table_masks_secretstr_by_default():
    rendered = utils.print_table([{"key": "password", "value": SecretStr("hunter2")}])
    assert rendered is not None
    assert "hunter2" not in rendered
    assert "**********" in rendered


def test_print_table_unwraps_secretstr_when_requested():
    rendered = utils.print_table(
        [{"key": "password", "value": SecretStr("hunter2")}],
        unwrap_secrets=True,
    )
    assert rendered is not None
    assert "hunter2" in rendered
    assert "**********" not in rendered


def test_print_table_does_not_mutate_input():
    """The helper renders to a string; the caller's data must be untouched."""
    rows = [{"key": "password", "value": SecretStr("hunter2")}]
    utils.print_table(rows, unwrap_secrets=True)
    assert isinstance(rows[0]["value"], SecretStr)
    assert rows[0]["value"].get_secret_value() == "hunter2"
