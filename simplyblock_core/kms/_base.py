from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType


class KMS(AbstractContextManager):
    def __exit__(  # Has to be defined to make the type-checker happy
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        return None

    @abstractmethod
    def create_data_encryption_keys(self, path: str, kek_name: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def import_data_encryption_keys(self, path: str, kek_name: str, keys: tuple[str, str]) -> None:
        pass

    @abstractmethod
    def get_data_encryption_keys(self, path: str, kek_name: str) -> tuple[str, str]:
        pass

    @abstractmethod
    def delete_data_encryption_keys(self, path: str) -> None: ...

    def rekey_data_encryption_keys(
        self,
        src_path: str, src_kek_name: str,
        dst_path: str, dst_kek_name: str,
    ) -> None:
        # TODO: Implementing this as a combined operation via a plugin for HCP enables
        # us to prevent the controller from seeing the plaintext altogether.
        keys = self.get_data_encryption_keys(src_path, src_kek_name)
        self.import_data_encryption_keys(dst_path, dst_kek_name, keys)

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
