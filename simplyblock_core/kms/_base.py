from abc import ABC, abstractmethod


class KMS(ABC):
    @abstractmethod
    def get_keys(self, key_name) -> dict: ...

    @abstractmethod
    def save_keys(self, key: str, key1: str, key2: str) -> None: ...

    @abstractmethod
    def decrypt(self, key: str, ciphertext: str) -> str: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key(self, key: str) -> None: ...
