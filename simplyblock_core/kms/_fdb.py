import re

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.utils import generate_hex_string

from ._base import KMS
from ._exceptions import KMSException

_KEY_NAME_PATTERN = re.compile(r"^crypto_(?P<lvol_bdev>.*)$")


class LocalKMS(KMS):
    def __init__(self, cluster: Cluster):
        self._db_controller = DBController()
        self._cluster_id = cluster.get_id()

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

    def create_data_encryption_keys(self, kek_name: str, name: str) -> None:
        self.import_data_encryption_keys(kek_name, name, (generate_hex_string(32), generate_hex_string(32)))

    def import_data_encryption_keys(self, kek_name: str, name: str, keys: tuple[str, str]) -> None:
        lvol = self._lvol_by_data_encryption_key_name(name)
        lvol.crypto_key1, lvol.crypto_key2 = keys

    def get_data_encryption_keys(self, kek_name: str, name: str) -> tuple[str, str]:
        lvol = self._lvol_by_data_encryption_key_name(name)
        return lvol.crypto_key1, lvol.crypto_key2

    def delete_data_encryption_keys(self, name: str) -> None:
        lvol = self._lvol_by_data_encryption_key_name(name)
        lvol.crypto_key1 = None  # type: ignore[assignment]
        lvol.crypto_key2 = None  # type: ignore[assignment]
        lvol.write_to_db(self._db_controller.kv_store)

    def create_key_encryption_key(self, name: str) -> None:
        pass

    def delete_key_encryption_key(self, name: str) -> None:
        pass
