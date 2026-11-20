import base64
import binascii
import json
from typing import TYPE_CHECKING

from pydantic import SecretStr

from simplyblock_core.utils import generate_hex_string

from ._base import KMS, secret_plaintext, secret_wrapped
from ._exceptions import KMSException

if TYPE_CHECKING:
    # Type-hint only: avoids a cycle, since db_controller imports models.cluster,
    # which imports this package for KMSSettings (see kms/__init__.py).
    from simplyblock_core.models.cluster import Cluster


class LocalKMS(KMS):
    def __init__(self, cluster: "Cluster"):
        # Imported here, not at module level, for the same reason as above:
        # db_controller imports models.cluster, which imports this package.
        from simplyblock_core.db_controller import DBController

        kv_store = DBController().kv_store
        if kv_store is None:
            raise KMSException("No database connection")
        self._kv_store = kv_store

    @staticmethod
    def _key(path: str) -> bytes:
        return f"keys/{path}".encode()

    @staticmethod
    def _secret_key(path: str) -> bytes:
        # A prefix of its own: a generic secret never shares a key with a DEK
        # pair stored at the same path.
        return f"secrets/{path}".encode()

    def create_data_encryption_keys(self, path: str, kek_name: str) -> None:
        self.import_data_encryption_keys(
            path, kek_name,
            (SecretStr(generate_hex_string(32)), SecretStr(generate_hex_string(32))),
        )

    def import_data_encryption_keys(
        self, path: str, kek_name: str, keys: tuple[SecretStr, SecretStr],
    ) -> None:
        # The persistence boundary, where plaintext is the stored form —
        # the same exception ``BaseModel.write_to_db`` makes.
        self._kv_store.set(
            self._key(path),
            json.dumps([key.get_secret_value() for key in keys]).encode(),
        )

    def get_data_encryption_keys(self, path: str, kek_name: str) -> tuple[SecretStr, SecretStr]:
        raw = self._kv_store.get(self._key(path))
        if not raw:
            raise KMSException(f"No keys found at {path}")
        key1, key2 = json.loads(raw)
        return SecretStr(key1), SecretStr(key2)

    def delete_data_encryption_keys(self, path: str) -> None:
        self._kv_store.clear(self._key(path))

    def create_key_encryption_key(self, name: str) -> None:
        pass

    def delete_key_encryption_key(self, name: str) -> None:
        pass

    def put_secret(self, path: str, data: dict) -> None:
        # The persistence boundary, like import_data_encryption_keys.
        self._kv_store.set(self._secret_key(path), json.dumps(secret_plaintext(data)).encode())

    def get_secret(self, path: str) -> dict:
        raw = self._kv_store.get(self._secret_key(path))
        if not raw:
            raise KMSException(f"No secret found at {path}")
        return secret_wrapped(json.loads(raw))

    def delete_secret(self, path: str) -> None:
        self._kv_store.clear(self._secret_key(path))

    # There is no KEK here: the "wrapped" form is only encoded, not encrypted,
    # so a secret wrapped by LocalKMS is effectively plaintext in FoundationDB.
    # Credential storage gates this behind an explicit per-cluster opt-in
    # (see simplyblock_core.credentials).
    _LOCAL_PREFIX = "local:"

    def _wrap_bytes(self, kek_name: str, plaintext: bytes) -> str:
        return self._LOCAL_PREFIX + base64.b64encode(plaintext).decode()

    def _unwrap_bytes(self, kek_name: str, wrapped: str) -> bytes:
        if not wrapped.startswith(self._LOCAL_PREFIX):
            raise KMSException("Wrapped secret was not produced by the local KMS")
        try:
            return base64.b64decode(wrapped[len(self._LOCAL_PREFIX):], validate=True)
        except binascii.Error as e:
            raise KMSException("Wrapped secret is not valid") from e
