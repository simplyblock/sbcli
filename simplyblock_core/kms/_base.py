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
    def create_data_encryption_keys(self, kek_name: str, name: str) -> None: ...

    @abstractmethod
    def import_data_encryption_keys(self, kek_name: str, name: str, keys: tuple[str, str]) -> None: ...

    @abstractmethod
    def get_data_encryption_keys(self, kek_name: str, name: str) -> tuple[str, str]: ...

    @abstractmethod
    def delete_data_encryption_keys(self, name: str) -> None: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
