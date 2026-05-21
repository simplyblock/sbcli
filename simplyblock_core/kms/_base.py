from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from simplyblock_core.models.lvol_model import LVol


class KMS(AbstractContextManager):
    def __exit__(  # Has to be defined to make the type-checker happy
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        return None

    @abstractmethod
    def create_data_encryption_keys(self, kek_name: str, name: str, lvol: "LVol | None" = None) -> None: ...

    @abstractmethod
    def import_data_encryption_keys(self, kek_name: str, name: str, keys: tuple[str, str], lvol: "LVol | None" = None) -> None: ...

    @abstractmethod
    def get_data_encryption_keys(self, kek_name: str, name: str, lvol: "LVol | None" = None) -> tuple[str, str]: ...

    @abstractmethod
    def delete_data_encryption_keys(self, name: str) -> None: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
