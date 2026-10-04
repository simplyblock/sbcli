import logging

from pydantic import SecretBytes, SecretStr

from simplyblock_core.utils.secrets import unwrap_secret, unwrap_secrets_for_send


def test_unwrap_scalar_secretstr():
    assert unwrap_secrets_for_send(SecretStr("v")) == "v"


def test_unwrap_scalar_secretbytes():
    assert unwrap_secrets_for_send(SecretBytes(b"v")) == b"v"


def test_unwrap_nested_dict():
    obj = {"k": SecretStr("v"), "nested": {"k2": SecretStr("v2"), "ok": 1}}
    out = unwrap_secrets_for_send(obj)
    assert out == {"k": "v", "nested": {"k2": "v2", "ok": 1}}


def test_unwrap_list_and_tuple_preserve_type():
    obj = [SecretStr("a"), (SecretStr("b"), 2)]
    out = unwrap_secrets_for_send(obj)
    assert out == ["a", ("b", 2)]
    assert isinstance(out[1], tuple)


def test_unwrap_leaves_non_secret_values_unchanged():
    obj = {"x": 1, "y": True, "z": None, "s": "plain"}
    assert unwrap_secrets_for_send(obj) == obj


def test_repr_of_dict_with_secretstr_masks_value():
    assert "v" not in repr({"k": SecretStr("v")})


def test_logging_pipeline_masks_secretstr():
    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname="", lineno=0,
        msg="payload: %s", args=({"k": SecretStr("hunter2")},), exc_info=None,
    )
    assert "hunter2" not in record.getMessage()


def test_unwrap_secret_scalar_helper():
    assert unwrap_secret(None) is None
    assert unwrap_secret("plain") == "plain"
    assert unwrap_secret(SecretStr("v")) == "v"
