from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractContextManager
from types import TracebackType
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from simplyblock_core.models.lvol_model import LVol


WrappedDataEncryptionKeys = dict[str, Any]
"""Opaque JSON-serialisable payload representing the AES-XTS DEK pair.

The exact shape is owned by each KMS backend. The control plane must
treat instances as opaque and round-trip them unchanged from
:meth:`KMS.get_wrapped_data_encryption_keys` to
:meth:`KMS.unwrap_data_encryption_keys` (typically on the SPDK proxy
side). Depending on the backend, the payload may or may not be
encrypted — see the implementation docstrings.
"""


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
    def get_wrapped_data_encryption_keys(self, lvol: "LVol") -> WrappedDataEncryptionKeys:
        """Return the wrapped DEK pair for the given lvol.

        The returned payload is opaque to the caller: depending on the
        backend it may carry plaintext keys (e.g. the local FDB
        backend, which has no separate KEK) or backend-specific
        ciphertexts (e.g. HashiCorp Vault Transit-wrapped values). The
        payload is intended to be transported as-is to a node trusted
        to hold plaintext (the SPDK proxy) and there be passed to
        :meth:`unwrap_data_encryption_keys` to recover the AES-XTS hex
        keys.
        """

    @abstractmethod
    def unwrap_data_encryption_keys(self, wrapped: WrappedDataEncryptionKeys) -> tuple[str, str]:
        """Unwrap a payload produced by :meth:`get_wrapped_data_encryption_keys`.

        Returns the two AES-XTS hex strings expected by SPDK's
        ``accel_crypto_key_create``. For backends that emit plaintext
        wrapped payloads this is the identity; for Vault-style
        backends this performs the actual decryption.
        """

    @abstractmethod
    def copy_data_encryption_keys(self, src_lvol: "LVol", dst_lvol: "LVol") -> None:
        """Copy encrypted key material from *src_lvol* to *dst_lvol*.
        """

    @abstractmethod
    def delete_data_encryption_keys(self, name: str) -> None: ...

    @abstractmethod
    def create_key_encryption_key(self, name: str) -> None: ...

    @abstractmethod
    def delete_key_encryption_key(self, name: str) -> None: ...
