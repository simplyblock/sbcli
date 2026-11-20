"""Credentials for external systems: KMS secret envelope + CredentialProvider."""
import base64
import json
from unittest.mock import MagicMock

import hvac.exceptions
import pytest
from kmip.core import enums
from kmip.pie import objects as kmip_objects
from pydantic import SecretStr

from simplyblock_core.credentials import (
    CONTEXT_FIELD,
    Credential,
    CredentialError,
    CredentialProvider,
    KMIPSecretCredentialRef,
    ManagedCredentialRef,
    VaultKVCredentialRef,
    kek_name_for,
)
from simplyblock_core.kms import KMS, HCPClient, KMIPClient, KMSException, LocalKMS
from simplyblock_core.models.cluster import Cluster

CTX = "array/a1/https://pure01.example:443"


class FakeKMS(KMS):
    """Reversible, keyed 'encryption' so tests can see wrong-key failures."""

    def __init__(self):
        self.keks: set[str] = set()
        self.created: list[str] = []

    def create_data_encryption_keys(self, path, kek_name): ...
    def import_data_encryption_keys(self, path, kek_name, keys): ...
    def get_data_encryption_keys(self, path, kek_name): ...
    def delete_data_encryption_keys(self, path): ...

    def create_key_encryption_key(self, name):
        self.created.append(name)
        self.keks.add(name)

    def delete_key_encryption_key(self, name):
        self.keks.discard(name)

    def ensure_key_encryption_key(self, name):
        if name not in self.keks:
            self.create_key_encryption_key(name)

    def _wrap_bytes(self, kek_name, plaintext):
        if kek_name not in self.keks:
            raise KMSException("no such KEK")
        return base64.b64encode(kek_name.encode() + b"|" + plaintext).decode()

    def _unwrap_bytes(self, kek_name, wrapped):
        if kek_name not in self.keks:
            raise KMSException("no such KEK")
        prefix, _, plaintext = base64.b64decode(wrapped).partition(b"|")
        if prefix != kek_name.encode():
            raise KMSException("wrong KEK")
        return plaintext


def _fields(**kw):
    return {k: SecretStr(v) for k, v in kw.items()}


def _plain(fields):
    return {k: v.get_secret_value() for k, v in fields.items()}


# -- Envelope (KMS.wrap_secret / unwrap_secret) -----------------------------

class TestEnvelope:

    def setup_method(self):
        self.kms = FakeKMS()
        self.kms.create_key_encryption_key("k")

    def test_round_trip(self):
        wrapped = self.kms.wrap_secret("k", _fields(username="u", password="p"), CTX)
        assert _plain(self.kms.unwrap_secret("k", wrapped, CTX)) == {
            "username": "u", "password": "p"}

    def test_other_context_is_refused(self):
        wrapped = self.kms.wrap_secret("k", _fields(password="p"), CTX)
        with pytest.raises(KMSException, match="different context"):
            self.kms.unwrap_secret("k", wrapped, "array/a1/https://evil.example:443")

    def test_tampered_payload_is_refused(self):
        wrapped = self.kms.wrap_secret("k", _fields(password="p"), CTX)
        raw = bytearray(base64.b64decode(wrapped))
        raw[-3] ^= 0x01  # inside the JSON payload, after the digest
        with pytest.raises(KMSException, match="integrity"):
            self.kms.unwrap_secret("k", base64.b64encode(bytes(raw)).decode(), CTX)

    def test_truncated_blob_is_refused(self):
        wrapped = base64.b64encode(b"k|short").decode()
        with pytest.raises(KMSException, match="integrity"):
            self.kms.unwrap_secret("k", wrapped, CTX)

    def test_unknown_version_is_refused(self):
        import hashlib
        payload = json.dumps({"v": 99, "ctx": CTX, "fields": {"a": "b"}}).encode()
        wrapped = self.kms._wrap_bytes("k", hashlib.sha256(payload).digest() + payload)
        with pytest.raises(KMSException, match="unsupported format"):
            self.kms.unwrap_secret("k", wrapped, CTX)

    def test_empty_secret_is_refused(self):
        with pytest.raises(KMSException):
            self.kms.wrap_secret("k", {}, CTX)

    def test_backend_without_wrap_support_says_so(self):
        class NoWrap(FakeKMS):
            _wrap_bytes = KMS._wrap_bytes
        with pytest.raises(NotImplementedError, match="cannot wrap"):
            NoWrap().wrap_secret("k", _fields(a="b"), CTX)


# -- Backends ---------------------------------------------------------------

class TestLocalKMSWrapping:

    def _kms(self):
        return object.__new__(LocalKMS)  # no FDB needed for wrap/unwrap

    def test_round_trip(self):
        kms = self._kms()
        wrapped = kms.wrap_secret("ignored", _fields(token="t"), CTX)
        assert wrapped.startswith("local:")
        assert _plain(kms.unwrap_secret("ignored", wrapped, CTX)) == {"token": "t"}

    def test_foreign_ciphertext_is_refused(self):
        with pytest.raises(KMSException, match="local KMS"):
            self._kms().unwrap_secret("k", "vault:v1:abc", CTX)

    def test_corrupt_encoding_is_refused(self):
        with pytest.raises(KMSException):
            self._kms().unwrap_secret("k", "local:!!!", CTX)


class _FakeKVStore:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def clear(self, key):
        self.data.pop(key, None)


@pytest.fixture()
def kmip(monkeypatch):
    client = object.__new__(KMIPClient)
    client.client = MagicMock()
    kv = _FakeKVStore()
    monkeypatch.setattr(KMIPClient, "_kv_store", staticmethod(lambda: kv))
    uids = iter(f"uid-{i}" for i in range(100))
    client.client.create.side_effect = lambda *a, **k: next(uids)
    client.client.encrypt.side_effect = lambda data, **k: (bytes(b ^ 0x5A for b in data), b"\x01" * 16)
    client.client.decrypt.side_effect = lambda data, **k: bytes(b ^ 0x5A for b in data)
    return client, kv


class TestKMIPWrapping:

    def test_round_trip(self, kmip):
        client, _ = kmip
        client.ensure_key_encryption_key("k")
        wrapped = client.wrap_secret("k", _fields(password="p"), CTX)
        assert json.loads(wrapped)["iv"] == "01" * 16
        assert _plain(client.unwrap_secret("k", wrapped, CTX)) == {"password": "p"}

    def test_ensure_does_not_replace_existing_kek(self, kmip):
        client, kv = kmip
        client.ensure_key_encryption_key("k")
        client.ensure_key_encryption_key("k")
        assert client.client.create.call_count == 1
        assert kv.get(b"kms/kmip/kek/k") == b"uid-0"

    def test_cbc_bit_flip_is_caught_by_digest(self, kmip):
        client, _ = kmip
        client.ensure_key_encryption_key("k")
        record = json.loads(client.wrap_secret("k", _fields(password="p"), CTX))
        ct = bytearray.fromhex(record["ciphertext"])
        ct[40] ^= 0x01
        record["ciphertext"] = ct.hex()
        with pytest.raises(KMSException, match="integrity"):
            client.unwrap_secret("k", json.dumps(record), CTX)

    def test_garbage_record_is_refused(self, kmip):
        client, _ = kmip
        client.ensure_key_encryption_key("k")
        for bad in ("not json", "[]", '{"ciphertext": "zz"}'):
            with pytest.raises(KMSException, match="not valid"):
                client.unwrap_secret("k", bad, CTX)

    def test_get_secret_data(self, kmip):
        client, _ = kmip
        client.client.get.return_value = kmip_objects.SecretData(
            b'{"password": "p"}', enums.SecretDataType.PASSWORD)
        assert client.get_secret_data("u1") == b'{"password": "p"}'

    def test_get_secret_data_refuses_keys(self, kmip):
        client, _ = kmip
        client.client.get.return_value = kmip_objects.SymmetricKey(
            enums.CryptographicAlgorithm.AES, 128, b"\x00" * 16)
        with pytest.raises(KMSException, match="not SecretData"):
            client.get_secret_data("u1")


@pytest.fixture()
def vault():
    client = object.__new__(HCPClient)
    client.client = MagicMock()
    client.transit_mount = "t"
    client.kv_mount = "kv"
    return client


class TestVault:

    def test_ensure_creates_only_when_missing(self, vault):
        transit = vault.client.secrets.transit
        transit.read_key.side_effect = hvac.exceptions.InvalidPath()
        vault.ensure_key_encryption_key("k")
        transit.create_key.assert_called_once()

        transit.reset_mock()
        transit.read_key.side_effect = None
        vault.ensure_key_encryption_key("k")
        transit.create_key.assert_not_called()

    def test_wrap_uses_transit(self, vault):
        transit = vault.client.secrets.transit
        store = {}
        transit.encrypt_data.side_effect = lambda name, plaintext, mount_point: (
            store.setdefault("pt", plaintext), {"data": {"ciphertext": "vault:v1:x"}})[1]
        transit.decrypt_data.side_effect = lambda name, ciphertext, mount_point: {
            "data": {"plaintext": store["pt"]}}
        wrapped = vault.wrap_secret("k", _fields(password="p"), CTX)
        assert wrapped == "vault:v1:x"
        assert _plain(vault.unwrap_secret("k", wrapped, CTX)) == {"password": "p"}

    def test_read_kv_secret(self, vault):
        vault.client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"username": "u", "password": "p"}}}
        assert _plain(vault.read_kv_secret("arrays", "pure01")) == {
            "username": "u", "password": "p"}

    def test_read_kv_secret_refuses_non_string_fields_without_values(self, vault):
        vault.client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"password": "hunter2", "port": 443}}}
        with pytest.raises(KMSException) as e:
            vault.read_kv_secret("arrays", "pure01")
        assert "port" in e.value.message and "hunter2" not in e.value.message

    def test_read_kv_secret_missing(self, vault):
        vault.client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.InvalidPath()
        with pytest.raises(KMSException, match="No secret"):
            vault.read_kv_secret("arrays", "nope")


# -- CredentialProvider -----------------------------------------------------

def _cluster(kms_settings=None, allow_local=False):
    cluster = Cluster()
    cluster.uuid = "c1"
    cluster.set_kms_settings(kms_settings)
    cluster.allow_local_kms_credentials = allow_local
    return cluster


VAULT = {"provider": "vault", "base_url": "https://vault.example:8200"}
KMIP = {"provider": "kmip", "server_url": "kmip.example:5696"}


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def _provider(cluster, kms, clock=None):
    factory = MagicMock(return_value=kms)
    provider = CredentialProvider(cluster, kms_factory=factory, clock=clock or Clock())
    return provider, factory


class TestOptIn:

    def test_local_kms_is_refused_without_opt_in(self):
        provider, factory = _provider(_cluster(), FakeKMS())
        with pytest.raises(CredentialError, match="opt in"):
            provider.store({"password": "p"}, CTX)
        factory.assert_not_called()

    def test_local_kms_refuses_resolve_after_opt_in_is_withdrawn(self):
        cluster = _cluster(allow_local=True)
        provider, _ = _provider(cluster, FakeKMS())
        ref = provider.store({"password": "p"}, CTX)
        cluster.allow_local_kms_credentials = False
        with pytest.raises(CredentialError, match="opt in"):
            CredentialProvider(cluster, kms_factory=MagicMock(return_value=FakeKMS())).resolve(ref, CTX)

    def test_local_kms_works_with_opt_in(self):
        provider, _ = _provider(_cluster(allow_local=True), FakeKMS())
        ref = provider.store({"password": "p"}, CTX)
        assert ref.backend == "local"
        assert provider.resolve(ref, CTX).get("password").get_secret_value() == "p"

    def test_cluster_default_is_no_opt_in(self):
        assert Cluster().allow_local_kms_credentials is False


class TestManaged:

    def test_store_and_resolve_round_trip(self):
        kms = FakeKMS()
        provider, factory = _provider(_cluster(VAULT), kms)
        ref = provider.store({"username": "u", "password": SecretStr("hunter2")}, CTX)
        assert ref == ManagedCredentialRef(
            backend="vault", kek_name=kek_name_for("c1"), wrapped=ref.wrapped)
        assert kms.created == ["credentials-c1"]
        cred = provider.resolve(ref.model_dump(), CTX)  # dict form, as stored in FDB
        assert cred.get("password").get_secret_value() == "hunter2"
        assert "hunter2" not in repr(cred)

    def test_kek_is_reused(self):
        kms = FakeKMS()
        provider, _ = _provider(_cluster(VAULT), kms)
        provider.store({"a": "1"}, CTX)
        provider.store({"a": "2"}, CTX)
        assert kms.created == ["credentials-c1"]

    def test_other_context_is_refused(self):
        provider, _ = _provider(_cluster(KMIP), FakeKMS())
        ref = provider.store({"password": "p"}, CTX)
        with pytest.raises(CredentialError, match="different context"):
            provider.resolve(ref, "array/a1/https://evil.example:443")

    def test_backend_change_is_reported(self):
        kms = FakeKMS()
        provider, _ = _provider(_cluster(VAULT), kms)
        ref = provider.store({"password": "p"}, CTX)
        provider2, _ = _provider(_cluster(KMIP), kms)
        with pytest.raises(CredentialError, match="stored with the vault"):
            provider2.resolve(ref, CTX)

    def test_empty_inputs_are_refused(self):
        provider, _ = _provider(_cluster(VAULT), FakeKMS())
        with pytest.raises(CredentialError):
            provider.store({}, CTX)
        with pytest.raises(CredentialError):
            provider.store({"a": "b"}, "")

    def test_credentials_cert_role_is_used(self):
        settings = dict(VAULT, credentials_cert_role="simplyblock-credentials")
        provider, factory = _provider(_cluster(settings), FakeKMS())
        provider.store({"a": "b"}, CTX)
        assert factory.call_args.kwargs == {"cert_role": "simplyblock-credentials"}

    def test_kms_connection_failure_is_wrapped(self):
        provider = CredentialProvider(
            _cluster(VAULT), kms_factory=MagicMock(side_effect=KMSException("Authentication failed")))
        with pytest.raises(CredentialError, match="Authentication failed"):
            provider.store({"a": "b"}, CTX)


class TestCache:

    def test_ttl_and_invalidate(self):
        kms = FakeKMS()
        clock = Clock()
        provider, factory = _provider(_cluster(VAULT), kms, clock)
        ref = provider.store({"a": "b"}, CTX)
        factory.reset_mock()

        provider.resolve(ref, CTX)
        provider.resolve(ref, CTX)
        assert factory.call_count == 1

        clock.t += 301
        provider.resolve(ref, CTX)
        assert factory.call_count == 2

        provider.invalidate(ref)
        provider.resolve(ref, CTX)
        assert factory.call_count == 3

        provider.invalidate()
        provider.resolve(ref, CTX)
        assert factory.call_count == 4

    def test_cache_is_per_context(self):
        provider, _ = _provider(_cluster(VAULT), FakeKMS())
        ref = provider.store({"a": "b"}, CTX)
        provider.resolve(ref, CTX)
        with pytest.raises(CredentialError):
            provider.resolve(ref, "other")


class TestExternal:

    def _vault_kms(self, data):
        kms = MagicMock(spec=HCPClient)
        kms.__enter__.return_value = kms
        kms.read_kv_secret.return_value = {k: SecretStr(v) for k, v in data.items()}
        return kms

    def _kmip_kms(self, raw):
        kms = MagicMock(spec=KMIPClient)
        kms.__enter__.return_value = kms
        kms.get_secret_data.return_value = raw
        return kms

    def test_vault_kv(self):
        kms = self._vault_kms({"username": "u", "password": "p"})
        provider, _ = _provider(_cluster(VAULT), kms)
        ref = VaultKVCredentialRef(mount="customer-kv", path="arrays/pure01", version=3)
        cred = provider.resolve(ref, CTX)
        assert cred.field_names == {"username", "password"}
        kms.read_kv_secret.assert_called_once_with("customer-kv", "arrays/pure01", 3)

    def test_vault_kv_context_field(self):
        provider, _ = _provider(
            _cluster(VAULT), self._vault_kms({"password": "p", CONTEXT_FIELD: CTX}))
        assert provider.resolve(
            VaultKVCredentialRef(mount="m", path="p"), CTX).field_names == {"password"}

        provider, _ = _provider(
            _cluster(VAULT), self._vault_kms({"password": "p", CONTEXT_FIELD: "other"}))
        with pytest.raises(CredentialError, match="different context"):
            provider.resolve(VaultKVCredentialRef(mount="m", path="p"), CTX)

    def test_vault_kv_needs_vault(self):
        provider, _ = _provider(_cluster(KMIP), self._kmip_kms(b"{}"))
        with pytest.raises(CredentialError, match="needs the cluster KMS to be Vault"):
            provider.resolve(VaultKVCredentialRef(mount="m", path="p"), CTX)

    def test_kmip_json_object(self):
        provider, _ = _provider(
            _cluster(KMIP), self._kmip_kms(b'{"username": "u", "password": "p"}'))
        cred = provider.resolve({"kind": "kmip-secret", "uid": "42"}, CTX)
        assert cred.get("password").get_secret_value() == "p"

    def test_kmip_raw_value_as_field(self):
        provider, _ = _provider(_cluster(KMIP), self._kmip_kms(b"s3cr3t"))
        cred = provider.resolve(KMIPSecretCredentialRef(uid="42", field="api_token"), CTX)
        assert cred.get("api_token").get_secret_value() == "s3cr3t"

    def test_kmip_raw_value_without_field_is_refused(self):
        provider, _ = _provider(_cluster(KMIP), self._kmip_kms(b"s3cr3t"))
        with pytest.raises(CredentialError, match="set 'field'") as e:
            provider.resolve(KMIPSecretCredentialRef(uid="42"), CTX)
        assert "s3cr3t" not in str(e.value)

    def test_kmip_needs_kmip(self):
        provider, _ = _provider(_cluster(VAULT), self._vault_kms({"a": "b"}))
        with pytest.raises(CredentialError, match="needs the cluster KMS to be KMIP"):
            provider.resolve(KMIPSecretCredentialRef(uid="42"), CTX)

    def test_external_refs_refused_under_local_kms_without_opt_in(self):
        provider, _ = _provider(_cluster(), FakeKMS())
        with pytest.raises(CredentialError, match="opt in"):
            provider.resolve(VaultKVCredentialRef(mount="m", path="p"), CTX)


def test_credential_missing_field_names_fields_not_values():
    cred = Credential(_fields(username="u", password="hunter2"))
    with pytest.raises(CredentialError) as e:
        cred.get("token")
    assert "password" in str(e.value) and "hunter2" not in str(e.value)


# -- cluster set: bool parsing ---------------------------------------------

@pytest.mark.parametrize("raw, expected", [
    ("true", True), ("True", True), ("1", True), ("yes", True),
    ("false", False), ("False", False), ("0", False), ("off", False),
])
def test_cluster_set_parses_bools(monkeypatch, raw, expected):
    from simplyblock_core import cluster_ops

    cluster = Cluster()
    cluster.allow_local_kms_credentials = not expected
    cluster.write_to_db = MagicMock()
    db = MagicMock()
    db.get_cluster_by_id.return_value = cluster
    monkeypatch.setattr(cluster_ops, "db_controller", db)

    cluster_ops.set_("c1", "allow_local_kms_credentials", raw)

    assert cluster.allow_local_kms_credentials is expected
    cluster.write_to_db.assert_called_once()


def test_cluster_set_rejects_non_bool(monkeypatch):
    from simplyblock_core import cluster_ops

    cluster = Cluster()
    cluster.write_to_db = MagicMock()
    db = MagicMock()
    db.get_cluster_by_id.return_value = cluster
    monkeypatch.setattr(cluster_ops, "db_controller", db)

    with pytest.raises(ValueError):
        cluster_ops.set_("c1", "allow_local_kms_credentials", "maybe")
    cluster.write_to_db.assert_not_called()
