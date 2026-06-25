import json

import pytest
from pydantic import SecretStr

from simplyblock_core.models.cluster import Cluster


def test_secret_field_round_trip_through_dict():
    cluster = Cluster()
    cluster.cli_pass = SecretStr("xyz")

    persisted = cluster.to_dict(unwrap_secrets=True)
    assert persisted["cli_pass"] == "xyz"

    revived = Cluster().from_dict(persisted)
    assert isinstance(revived.cli_pass, SecretStr)
    assert revived.cli_pass.get_secret_value() == "xyz"


def test_repr_masks_secret_value():
    cluster = Cluster()
    cluster.secret = SecretStr("super-secret")
    text = repr(cluster)
    assert "super-secret" not in text
    assert "**********" in text


def test_to_dict_default_keeps_wrapper():
    cluster = Cluster()
    cluster.secret = SecretStr("xyz")
    d = cluster.to_dict()
    assert isinstance(d["secret"], SecretStr)
    assert "xyz" not in repr(d)


def test_write_path_serializable_only_with_unwrap_flag():
    cluster = Cluster()
    cluster.secret = SecretStr("xyz")

    assert json.dumps(cluster.to_dict(unwrap_secrets=True))

    with pytest.raises(TypeError):
        json.dumps(cluster.to_dict())


def test_from_dict_lifts_plain_string_into_secretstr():
    """Backward-compat: existing FoundationDB records store plaintext."""
    revived = Cluster().from_dict({"secret": "legacy-plain"})
    assert isinstance(revived.secret, SecretStr)
    assert revived.secret.get_secret_value() == "legacy-plain"


def test_secretstr_inside_dict_field_unwrapped():
    """SecretStr values inside a dict field must be unwrapped for JSON."""
    cluster = Cluster()
    cluster.tls_config = {"token": SecretStr("s3cret"), "plain": "ok"}

    d = cluster.to_dict(unwrap_secrets=True)
    assert d["tls_config"]["token"] == "s3cret"
    assert d["tls_config"]["plain"] == "ok"
    # Must be JSON-serializable
    json.dumps(d)


def test_secretstr_inside_dict_field_stays_wrapped():
    """Without unwrap, SecretStr in dict fields stays wrapped."""
    cluster = Cluster()
    cluster.tls_config = {"token": SecretStr("s3cret")}

    d = cluster.to_dict(unwrap_secrets=False)
    assert isinstance(d["tls_config"]["token"], SecretStr)


def test_secretstr_inside_nested_dict_unwrapped():
    """SecretStr deeply nested in a dict field must be unwrapped."""
    cluster = Cluster()
    cluster.tls_config = {"outer": {"inner": SecretStr("deep")}}

    d = cluster.to_dict(unwrap_secrets=True)
    assert d["tls_config"]["outer"]["inner"] == "deep"
    json.dumps(d)
