from __future__ import annotations

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
    def create_data_encryption_keys(self, lvol: "LVol") -> None:
        raise NotImplementedError

    @abstractmethod
    def import_data_encryption_keys(self, lvol: "LVol", keys: tuple[str, str]) -> None:
        pass

    @abstractmethod
    def get_data_encryption_keys(self, lvol: "LVol") -> tuple[str, str]:
        pass

    @abstractmethod
    def delete_data_encryption_keys(self, name: str) -> None: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
