from __future__ import annotations

import hashlib
import hmac
import json
from abc import abstractmethod
from collections.abc import Mapping
from contextlib import AbstractContextManager
from types import TracebackType

from pydantic import SecretStr

from ._exceptions import KMSException

#: Version of the envelope ``wrap_secret`` produces. Bump on any change to the
#: payload layout; ``unwrap_secret`` refuses versions it does not know.
_SECRET_ENVELOPE_VERSION = 1
_DIGEST_LEN = hashlib.sha256().digest_size


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

    def ensure_key_encryption_key(self, name: str) -> None:
        """Create the KEK ``name`` unless it already exists.

        Must never replace an existing key: everything wrapped under it would
        become unreadable. Backends whose ``create_key_encryption_key`` is not
        already idempotent override this.
        """
        self.create_key_encryption_key(name)

    # -- Opaque secrets (credentials for external systems) ------------------
    #
    # Unlike data encryption keys these are not random key material but small
    # sets of named string fields (username/password, API token, PEM client
    # certificate, ...). Only the wrapped form is ever persisted.

    def _wrap_bytes(self, kek_name: str, plaintext: bytes) -> str:
        """Encrypt ``plaintext`` under the KEK, returning a storable string."""
        raise NotImplementedError(f"{type(self).__name__} cannot wrap secrets")

    def _unwrap_bytes(self, kek_name: str, wrapped: str) -> bytes:
        """Inverse of :meth:`_wrap_bytes`."""
        raise NotImplementedError(f"{type(self).__name__} cannot unwrap secrets")

    def wrap_secret(
        self, kek_name: str, fields: Mapping[str, SecretStr], context: str,
    ) -> str:
        """Wrap ``fields`` under ``kek_name``, bound to ``context``.

        ``context`` names what the secret is *for* (e.g. the external system's
        identity and endpoint). :meth:`unwrap_secret` refuses to return the
        secret for any other context, so a record whose endpoint was rewritten
        in the database cannot be used to send the credential elsewhere.

        A SHA-256 digest of the payload is wrapped together with it. Vault
        Transit (AES-GCM) already authenticates its ciphertext; the KMIP
        backend wraps with AES-CBC (see ``_kmip.py``), which does not, and the
        embedded digest turns blind tampering with that ciphertext into a
        refusal instead of a silently altered secret or context.
        """
        if not fields:
            raise KMSException("Refusing to wrap an empty secret")
        payload = json.dumps(
            {
                "v": _SECRET_ENVELOPE_VERSION,
                "ctx": context,
                "fields": {name: value.get_secret_value() for name, value in fields.items()},
            },
            sort_keys=True, separators=(",", ":"),
        ).encode()
        return self._wrap_bytes(kek_name, hashlib.sha256(payload).digest() + payload)

    def unwrap_secret(
        self, kek_name: str, wrapped: str, context: str,
    ) -> dict[str, SecretStr]:
        """Inverse of :meth:`wrap_secret`; ``context`` must match exactly."""
        blob = self._unwrap_bytes(kek_name, wrapped)
        digest, payload = blob[:_DIGEST_LEN], blob[_DIGEST_LEN:]
        if len(digest) != _DIGEST_LEN or not hmac.compare_digest(
            digest, hashlib.sha256(payload).digest(),
        ):
            raise KMSException("Wrapped secret failed its integrity check")
        try:
            data = json.loads(payload)
        except ValueError as e:
            raise KMSException("Wrapped secret is not valid") from e
        if not isinstance(data, dict) or data.get("v") != _SECRET_ENVELOPE_VERSION:
            raise KMSException("Wrapped secret has an unsupported format")
        if not hmac.compare_digest(str(data.get("ctx", "")).encode(), context.encode()):
            raise KMSException("Wrapped secret is bound to a different context")
        fields = data.get("fields")
        if not isinstance(fields, dict) or not fields or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in fields.items()
        ):
            raise KMSException("Wrapped secret is not valid")
        return {name: SecretStr(value) for name, value in fields.items()}

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
