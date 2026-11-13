from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType

from pydantic import SecretStr


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
    def import_data_encryption_keys(
        self, path: str, kek_name: str, keys: tuple[SecretStr, SecretStr],
    ) -> None:
        pass

    @abstractmethod
    def get_data_encryption_keys(self, path: str, kek_name: str) -> tuple[SecretStr, SecretStr]:
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

    # Generic secrets: a flat ``{name: value}`` dict stored at ``path`` (array
    # credentials, see simplyblock_core.array). Values go in as ``SecretStr``
    # (plain strings are wrapped) and come back as ``SecretStr``; only the
    # persistence boundary of each implementation sees the plaintext.

    @abstractmethod
    def put_secret(self, path: str, data: dict) -> None:
        """Store ``data`` at ``path``, replacing whatever was there."""

    @abstractmethod
    def get_secret(self, path: str) -> dict:
        """The secret stored at ``path``, every value a ``SecretStr``.

        Raises:
            KMSException: nothing is stored at ``path``, or the read failed.
        """

    @abstractmethod
    def delete_secret(self, path: str) -> None:
        """Delete the secret at ``path``; deleting a missing one succeeds."""


def secret_plaintext(data: dict) -> dict:
    """``data`` with every value unwrapped to a plain string, for the
    persistence boundary of a KMS only. Keys must be strings; values are
    ``SecretStr`` or ``str`` (anything else is refused, so a nested structure
    cannot carry an unmasked secret past the wrapper)."""
    if not isinstance(data, dict):
        raise TypeError("a secret is a dict of strings")
    out = {}
    for k, v in data.items():
        if not isinstance(k, str):
            raise TypeError("secret keys must be strings")
        if isinstance(v, SecretStr):
            out[k] = v.get_secret_value()
        elif isinstance(v, str):
            out[k] = v
        else:
            raise TypeError(f"secret value of {k!r} must be a string")
    return out


def secret_wrapped(data: dict) -> dict:
    """``data`` read back from a KMS, every value wrapped in ``SecretStr``."""
    return {str(k): v if isinstance(v, SecretStr) else SecretStr(str(v))
            for k, v in (data or {}).items()}
