"""Protocol-level round-trip tests for KMIPClient against a real KMIP server.

Mirrors ``test_local_kms_fdb.py``'s role for ``LocalKMS``: this confirms the
Create/Encrypt/Decrypt/Destroy composition in ``simplyblock_core/kms/_kmip.py``
actually works against a KMIP wire implementation, not just that the pykmip
client calls type-check.
"""
import uuid

import pytest
from pydantic import SecretStr

from simplyblock_core.db_controller import DBController
from simplyblock_core.kms import KMSException
from simplyblock_core.kms._kmip import KMIPClient


@pytest.fixture()
def kmip_client(pykmip_server):
    if DBController().kv_store is None:
        pytest.skip("FoundationDB is not available")
    client = KMIPClient(
        pykmip_server.server_url,
        pykmip_server.ca, pykmip_server.certificate, pykmip_server.key,
    )
    with client:
        yield client


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


def test_round_trip_returns_stored_keys(kmip_client):
    kek_name = _unique("kek")
    path = _unique("cluster/test/lvol")
    kmip_client.create_key_encryption_key(kek_name)
    try:
        kmip_client.create_data_encryption_keys(path, kek_name)
        key1, key2 = kmip_client.get_data_encryption_keys(path, kek_name)
        assert key1.get_secret_value() != key2.get_secret_value()
        assert len(key1.get_secret_value()) == 64  # 32 bytes, hex-encoded
    finally:
        kmip_client.delete_data_encryption_keys(path)
        kmip_client.delete_key_encryption_key(kek_name)


def test_import_returns_the_same_keys_back(kmip_client):
    kek_name = _unique("kek")
    path = _unique("cluster/test/lvol")
    keys = (SecretStr("ab" * 32), SecretStr("cd" * 32))
    kmip_client.create_key_encryption_key(kek_name)
    try:
        kmip_client.import_data_encryption_keys(path, kek_name, keys)
        assert kmip_client.get_data_encryption_keys(path, kek_name) == keys
    finally:
        kmip_client.delete_data_encryption_keys(path)
        kmip_client.delete_key_encryption_key(kek_name)


def test_delete_data_encryption_keys_removes_them(kmip_client):
    kek_name = _unique("kek")
    path = _unique("cluster/test/lvol")
    kmip_client.create_key_encryption_key(kek_name)
    try:
        kmip_client.create_data_encryption_keys(path, kek_name)
        kmip_client.delete_data_encryption_keys(path)
        with pytest.raises(KMSException):
            kmip_client.get_data_encryption_keys(path, kek_name)
    finally:
        kmip_client.delete_key_encryption_key(kek_name)


def test_get_with_no_stored_keys_is_refused(kmip_client):
    with pytest.raises(KMSException):
        kmip_client.get_data_encryption_keys(_unique("cluster/test/lvol"), _unique("kek"))


def test_get_with_unknown_kek_is_refused(kmip_client):
    """The DEK path exists, but the wrapping key does not: a clear error,
    not a silent misdecrypt under the wrong key."""
    path = _unique("cluster/test/lvol")
    kek_name = _unique("kek")
    kmip_client.create_key_encryption_key(kek_name)
    try:
        kmip_client.create_data_encryption_keys(path, kek_name)
        with pytest.raises(KMSException):
            kmip_client.get_data_encryption_keys(path, _unique("kek"))
    finally:
        kmip_client.delete_data_encryption_keys(path)
        kmip_client.delete_key_encryption_key(kek_name)


def test_rekey_moves_keys_to_a_new_kek_and_path(kmip_client):
    """What backup/migration/snapshot-restore do: decrypt under the source
    KEK, re-encrypt under a different, freshly created destination KEK."""
    src_kek, dst_kek = _unique("kek-src"), _unique("kek-dst")
    src_path, dst_path = _unique("cluster/test/lvol"), _unique("cluster/test/backup")
    kmip_client.create_key_encryption_key(src_kek)
    kmip_client.create_key_encryption_key(dst_kek)
    try:
        kmip_client.create_data_encryption_keys(src_path, src_kek)
        original = kmip_client.get_data_encryption_keys(src_path, src_kek)

        kmip_client.rekey_data_encryption_keys(src_path, src_kek, dst_path, dst_kek)

        assert kmip_client.get_data_encryption_keys(dst_path, dst_kek) == original
    finally:
        kmip_client.delete_data_encryption_keys(src_path)
        kmip_client.delete_data_encryption_keys(dst_path)
        kmip_client.delete_key_encryption_key(src_kek)
        kmip_client.delete_key_encryption_key(dst_kek)


def test_delete_key_encryption_key_of_an_unknown_name_is_a_no_op(kmip_client):
    kmip_client.delete_key_encryption_key(_unique("kek"))


# -- Opaque secrets (credentials for external systems) ----------------------

_CTX = "array/a1/https://pure01.example:443"


def test_wrap_secret_round_trip(kmip_client):
    kek_name = _unique("kek")
    kmip_client.ensure_key_encryption_key(kek_name)
    try:
        wrapped = kmip_client.wrap_secret(
            kek_name, {"username": SecretStr("u"), "password": SecretStr("p")}, _CTX)
        fields = kmip_client.unwrap_secret(kek_name, wrapped, _CTX)
        assert {k: v.get_secret_value() for k, v in fields.items()} == {
            "username": "u", "password": "p"}
        with pytest.raises(KMSException, match="different context"):
            kmip_client.unwrap_secret(kek_name, wrapped, "array/a1/https://evil.example:443")
    finally:
        kmip_client.delete_key_encryption_key(kek_name)


def test_ensure_key_encryption_key_keeps_existing_key(kmip_client):
    kek_name = _unique("kek")
    kmip_client.ensure_key_encryption_key(kek_name)
    try:
        wrapped = kmip_client.wrap_secret(kek_name, {"a": SecretStr("b")}, _CTX)
        kmip_client.ensure_key_encryption_key(kek_name)
        assert kmip_client.unwrap_secret(kek_name, wrapped, _CTX)["a"].get_secret_value() == "b"
    finally:
        kmip_client.delete_key_encryption_key(kek_name)


@pytest.fixture()
def kmip_client_no_fdb(pykmip_server):
    """SecretData Get never touches FoundationDB."""
    client = KMIPClient(
        pykmip_server.server_url,
        pykmip_server.ca, pykmip_server.certificate, pykmip_server.key,
    )
    with client:
        yield client


def test_get_secret_data_returns_customer_secret(kmip_client_no_fdb):
    from kmip.core import enums
    from kmip.pie import objects

    value = b'{"username": "u", "password": "p"}'
    uid = kmip_client_no_fdb.client.register(
        objects.SecretData(value, enums.SecretDataType.PASSWORD))
    try:
        assert kmip_client_no_fdb.get_secret_data(uid) == value
    finally:
        kmip_client_no_fdb.client.destroy(uid)


def test_get_secret_data_refuses_a_key_object(kmip_client_no_fdb):
    from kmip.core import enums

    uid = kmip_client_no_fdb.client.create(enums.CryptographicAlgorithm.AES, 256)
    try:
        with pytest.raises(KMSException, match="not SecretData"):
            kmip_client_no_fdb.get_secret_data(uid)
    finally:
        kmip_client_no_fdb.client.destroy(uid)
