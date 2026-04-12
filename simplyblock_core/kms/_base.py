from abc import ABC, abstractmethod

from simplyblock_core.models.pool import Pool


class KMS(ABC):
    @abstractmethod
    def get_keys(self, key_name) -> dict: ...

    @abstractmethod
    def save_keys(self, key: str, key1: str, key2: str) -> None: ...

    @abstractmethod
    def decrypt(self, key: str, ciphertext: str) -> str: ...

    @abstractmethod
    def create_pool_key(self, pool: Pool) -> None: ...

    @abstractmethod
    def update_pool_key(self, pool: Pool) -> None: ...

    @abstractmethod
    def delete_pool_key(self, pool: Pool) -> None: ...

    @abstractmethod
    def delete_key(self, key: str) -> None: ...
