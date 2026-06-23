import json

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.utils import generate_hex_string

from ._base import KMS
from ._exceptions import KMSException


class LocalKMS(KMS):
    def __init__(self, cluster: Cluster):
        kv_store = DBController().kv_store
        if kv_store is None:
            raise KMSException("No database connection")
        self._kv_store = kv_store

    @staticmethod
    def _key(name: str) -> bytes:
        return f"keys/{name}".encode()

    def create_data_encryption_keys(self, name: str, kek_name: str) -> None:
        self.import_data_encryption_keys(
            name, kek_name, (generate_hex_string(32), generate_hex_string(32)),
        )

    def import_data_encryption_keys(self, name: str, kek_name: str, keys: tuple[str, str]) -> None:
        self._kv_store.set(self._key(name), json.dumps(list(keys)).encode())

    def get_data_encryption_keys(self, name: str, kek_name: str) -> tuple[str, str]:
        raw = self._kv_store.get(self._key(name)).wait()
        if not raw.present():
            raise KMSException(f"No keys found for {name}")
        key1, key2 = json.loads(bytes(raw))
        return key1, key2

    def delete_data_encryption_keys(self, name: str) -> None:
        self._kv_store.clear(self._key(name))

    def create_key_encryption_key(self, name: str) -> None:
        pass

    def delete_key_encryption_key(self, name: str) -> None:
        pass
