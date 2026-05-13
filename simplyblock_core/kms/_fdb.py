import re
from typing import Optional

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.utils import generate_hex_string

from ._base import KMS, WrappedDataEncryptionKeys
from ._exceptions import KMSException

_KEY_NAME_PATTERN = re.compile(r"^crypto_(?P<lvol_bdev>.*)$")


class LocalKMS(KMS):
    """KMS backed by FoundationDB.

    The wrapped DEK payload is the plaintext hex pair — there is no
    separate KEK to wrap with. Unwrapping is therefore the identity.
    This means the control plane *does* hold plaintext for LocalKMS
    (and FDB stores them at rest in the LVol record); the
    "core-never-sees-plaintext" property only holds for backends that
    perform real wrapping (see ``HCPClient``).

    The constructor accepts an optional ``Cluster`` because the
    SPDK-proxy path only ever invokes :meth:`unwrap_data_encryption_keys`,
    which is purely functional and does not need cluster/FDB state.
    """

    def __init__(self, cluster: Optional[Cluster] = None):
        self._cluster = cluster
        self._db_controller_cache = None

    @property
    def _db_controller(self):
        if self._db_controller_cache is None:
            from simplyblock_core.db_controller import DBController
            self._db_controller_cache = DBController()
        return self._db_controller_cache

    @property
    def _cluster_id(self) -> str:
        if self._cluster is None:
            raise KMSException("LocalKMS requires a cluster for this operation")
        return self._cluster.get_id()

    def _lvol_by_data_encryption_key_name(self, name: str) -> LVol:
        if (match := re.match(_KEY_NAME_PATTERN, name)) is None:
            raise KMSException("Key name does not match expectations")

        lvol_bdev = match.group("lvol_bdev")
        try:
            return next(
                lvol
                for lvol in self._db_controller.get_lvols(cluster_id=self._cluster_id)
                if lvol.lvol_bdev == lvol_bdev
            )
        except StopIteration:
            raise KMSException(f"No LVol found for key {name}")

    def create_data_encryption_keys(self, lvol: LVol) -> None:
        self.import_data_encryption_keys(lvol, (generate_hex_string(32), generate_hex_string(32)))

    def import_data_encryption_keys(self, lvol: LVol, keys: tuple[str, str]) -> None:
        lvol.crypto_key1, lvol.crypto_key2 = keys

    def get_wrapped_data_encryption_keys(self, lvol: LVol) -> WrappedDataEncryptionKeys:
        return {"type": "local", "keys": [lvol.crypto_key1, lvol.crypto_key2]}

    def unwrap_data_encryption_keys(self, wrapped: WrappedDataEncryptionKeys) -> tuple[str, str]:
        try:
            key1, key2 = wrapped["keys"]
        except (KeyError, ValueError) as e:
            raise KMSException("Malformed wrapped DEK payload") from e
        return key1, key2

    def copy_data_encryption_keys(self, src_lvol: LVol, dst_lvol: LVol) -> None:
        dst_lvol.crypto_key1 = src_lvol.crypto_key1
        dst_lvol.crypto_key2 = src_lvol.crypto_key2

    def delete_data_encryption_keys(self, name: str) -> None:
        lvol = self._lvol_by_data_encryption_key_name(name)
        lvol.crypto_key1 = None  # type: ignore[assignment]
        lvol.crypto_key2 = None  # type: ignore[assignment]
        lvol.write_to_db(self._db_controller.kv_store)

    def create_key_encryption_key(self, name: str) -> None:
        pass

    def delete_key_encryption_key(self, name: str) -> None:
        pass
