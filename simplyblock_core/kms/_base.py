from abc import ABC, abstractmethod


class KMS(ABC):
    @abstractmethod
    def create_data_encryption_keys(self, kek_name: str, name: str) -> None: ...

    @abstractmethod
    def get_data_encryption_keys(self, kek_name: str, name: str) -> tuple[str, str]: ...

    @abstractmethod
    def delete_data_encryption_keys(self, name: str) -> None: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
