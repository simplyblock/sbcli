"""Credentials for external systems (storage arrays, ...), kept out of the database.

Plugins that drive an external system (provision/resize/delete a LUN on a
storage array, ...) never store or fetch secrets themselves. Their records
hold a :data:`CredentialRef`; at call time they ask a
:class:`CredentialProvider` to resolve it into a :class:`Credential`, which
exists only in memory.

Two kinds of reference:

*Managed* (:class:`ManagedCredentialRef`)
    simplyblock received the secret (CLI/API) and keeps it wrapped under a
    cluster KEK in the cluster's KMS: Vault Transit or a KMIP appliance. Only
    the ciphertext is stored, inside the ref. Revoking the KEK in the KMS
    makes every managed credential of the cluster unreadable at once.

*External* (:class:`VaultKVCredentialRef`, :class:`KMIPSecretCredentialRef`)
    The customer owns and rotates the secret in their own Vault KV v2 mount or
    as a KMIP ``SecretData`` object; simplyblock stores only the pointer.

Every resolution is bound to a *context* string naming what the credential is
for (e.g. ``"array/<id>/https://pure01.example:443"``). A managed credential
carries its context inside the wrapped payload and is refused for any other,
so rewriting an array's endpoint in the database cannot redirect the
credential to another host. An external secret may opt into the same check by
holding a ``_context`` field.

With no external KMS configured the cluster falls back to ``LocalKMS``, which
only encodes. Managed credentials are then refused unless the cluster has
explicitly opted in via ``Cluster.allow_local_kms_credentials``.

Resolved credentials are cached in memory for ``ttl_s``. On an authentication
failure from the external system the caller invalidates the ref and resolves
again once, which picks up a rotated external secret without a restart.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, SecretStr, TypeAdapter

from simplyblock_core.kms import (
    KMS,
    HCPClient,
    KMIPClient,
    KMIPKMSSettings,
    KMSException,
    VaultKMSSettings,
)

if TYPE_CHECKING:
    from simplyblock_core.models.cluster import Cluster

logger = logging.getLogger(__name__)

#: Field an external secret may hold to bind itself to one context.
CONTEXT_FIELD = "_context"

Backend = Literal["vault", "kmip", "local"]


class CredentialError(Exception):
    """A credential could not be stored or resolved. Never carries secret values."""


class ManagedCredentialRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["managed"] = "managed"
    #: The KMS backend that wrapped it; a different backend cannot unwrap it.
    backend: Backend
    kek_name: str = Field(min_length=1)
    wrapped: str = Field(min_length=1)


class VaultKVCredentialRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["vault-kv"] = "vault-kv"
    mount: str = Field(min_length=1)
    path: str = Field(min_length=1)
    #: Pin a KV version; ``None`` follows the latest (i.e. picks up rotation).
    version: int | None = Field(default=None, ge=1)


class KMIPSecretCredentialRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["kmip-secret"] = "kmip-secret"
    uid: str = Field(min_length=1)
    #: ``None``: the object's value is a JSON object of string fields.
    #: Set: the raw value (UTF-8) is that single field, e.g. ``"password"``.
    field: str | None = Field(default=None, min_length=1)


CredentialRef = Annotated[
    Union[ManagedCredentialRef, VaultKVCredentialRef, KMIPSecretCredentialRef],
    Field(discriminator="kind"),
]
CREDENTIAL_REF_ADAPTER: TypeAdapter[CredentialRef] = TypeAdapter(CredentialRef)


class Credential:
    """Resolved secret fields, in memory only. ``repr`` shows field names only."""

    __slots__ = ("_fields",)

    def __init__(self, fields: Mapping[str, SecretStr]):
        self._fields = dict(fields)

    @property
    def field_names(self) -> frozenset[str]:
        return frozenset(self._fields)

    def get(self, name: str) -> SecretStr:
        try:
            return self._fields[name]
        except KeyError:
            raise CredentialError(
                f"Credential has no field {name!r} (has: {', '.join(sorted(self._fields))})"
            ) from None

    def get_optional(self, name: str) -> SecretStr | None:
        return self._fields.get(name)

    def __repr__(self) -> str:
        return f"Credential(fields={sorted(self._fields)})"


def kek_name_for(cluster_id: str) -> str:
    """The cluster KEK that wraps managed credentials."""
    return f"credentials-{cluster_id}"


def _backend_of(cluster: Cluster) -> Backend:
    settings = cluster.get_kms_settings()
    if settings is None:
        return "local"
    if isinstance(settings, VaultKMSSettings):
        return "vault"
    if isinstance(settings, KMIPKMSSettings):
        return "kmip"
    raise CredentialError(f"Unsupported KMS settings: {type(settings).__name__}")


class CredentialProvider:
    """Stores and resolves credentials for one cluster. Thread-safe."""

    def __init__(
        self,
        cluster: Cluster,
        *,
        ttl_s: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
        kms_factory: Callable[..., KMS] | None = None,
    ):
        self._cluster = cluster
        self._ttl_s = ttl_s
        self._clock = clock
        self._kms_factory = kms_factory
        self._lock = threading.Lock()
        self._cache: dict[tuple[str, str], tuple[float, Credential]] = {}

    # -- KMS access ---------------------------------------------------------

    def _open_kms(self) -> KMS:
        backend = _backend_of(self._cluster)
        if backend == "local" and not self._cluster.allow_local_kms_credentials:
            raise CredentialError(
                "No external KMS (Vault or KMIP) is configured for this cluster. "
                "Configure one, or explicitly opt in to keeping credentials in the "
                "cluster database (effectively unencrypted) by setting "
                "allow_local_kms_credentials on the cluster."
            )
        cert_role = None
        settings = self._cluster.get_kms_settings()
        if isinstance(settings, VaultKMSSettings):
            cert_role = settings.credentials_cert_role
        factory = self._kms_factory
        if factory is None:
            # Looked up per call, not bound at import: the KMS connection is
            # replaced in tests and by callers that patch the package.
            from simplyblock_core import kms as kms_pkg
            factory = kms_pkg.create_kms_connection
        try:
            return factory(self._cluster, cert_role=cert_role)
        except KMSException as e:
            raise CredentialError(f"Cannot connect to the KMS: {e.message}") from e

    # -- Store --------------------------------------------------------------

    def store(
        self, fields: Mapping[str, SecretStr | str], context: str,
    ) -> ManagedCredentialRef:
        """Wrap ``fields`` under the cluster's credentials KEK, bound to
        ``context``. Persist the returned ref; the plaintext is not kept."""
        if not context:
            raise CredentialError("A credential needs a non-empty context")
        if not fields:
            raise CredentialError("Refusing to store an empty credential")
        secret_fields = {
            name: value if isinstance(value, SecretStr) else SecretStr(value)
            for name, value in fields.items()
        }
        backend = _backend_of(self._cluster)
        kek_name = kek_name_for(self._cluster.get_id())
        try:
            with self._open_kms() as kms:
                kms.ensure_key_encryption_key(kek_name)
                wrapped = kms.wrap_secret(kek_name, secret_fields, context)
        except KMSException as e:
            raise CredentialError(f"Storing credential failed: {e.message}") from e
        logger.info("Stored managed credential (backend=%s, %d fields, context=%s)",
                    backend, len(secret_fields), context)
        return ManagedCredentialRef(backend=backend, kek_name=kek_name, wrapped=wrapped)

    # -- Resolve ------------------------------------------------------------

    @staticmethod
    def _cache_key(ref: CredentialRef, context: str) -> tuple[str, str]:
        return ref.model_dump_json(), context

    def resolve(self, ref: CredentialRef | Mapping, context: str) -> Credential:
        """Return the credential for ``ref``, bound to ``context``."""
        if isinstance(ref, Mapping):
            ref = CREDENTIAL_REF_ADAPTER.validate_python(ref)
        key = self._cache_key(ref, context)
        now = self._clock()
        with self._lock:
            hit = self._cache.get(key)
            if hit is not None and hit[0] > now:
                return hit[1]

        credential = self._fetch(ref, context)

        with self._lock:
            self._cache[key] = (self._clock() + self._ttl_s, credential)
        return credential

    def invalidate(self, ref: CredentialRef | None = None) -> None:
        """Drop cached resolutions of ``ref`` (all, if ``None``). Call after
        the external system rejected the credential, then resolve once more."""
        with self._lock:
            if ref is None:
                self._cache.clear()
                return
            dumped = ref.model_dump_json()
            for key in [k for k in self._cache if k[0] == dumped]:
                del self._cache[key]

    def _fetch(self, ref: CredentialRef, context: str) -> Credential:
        if isinstance(ref, ManagedCredentialRef):
            backend = _backend_of(self._cluster)
            if ref.backend != backend:
                raise CredentialError(
                    f"Credential was stored with the {ref.backend} KMS backend, but the "
                    f"cluster now uses {backend}; store it again"
                )
        try:
            with self._open_kms() as kms:
                if isinstance(ref, ManagedCredentialRef):
                    return Credential(kms.unwrap_secret(ref.kek_name, ref.wrapped, context))
                if isinstance(ref, VaultKVCredentialRef):
                    if not isinstance(kms, HCPClient):
                        raise CredentialError(
                            "A Vault KV credential needs the cluster KMS to be Vault")
                    fields = kms.read_kv_secret(ref.mount, ref.path, ref.version)
                    where = f"Vault {ref.mount}/{ref.path}"
                else:
                    if not isinstance(kms, KMIPClient):
                        raise CredentialError(
                            "A KMIP secret credential needs the cluster KMS to be KMIP")
                    fields = self._parse_secret_data(kms.get_secret_data(ref.uid), ref)
                    where = f"KMIP object {ref.uid}"
        except KMSException as e:
            raise CredentialError(f"Resolving credential failed: {e.message}") from e
        return Credential(self._check_external_context(fields, context, where))

    @staticmethod
    def _parse_secret_data(
        raw: bytes, ref: KMIPSecretCredentialRef,
    ) -> dict[str, SecretStr]:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as e:
            raise CredentialError(f"KMIP object {ref.uid} is not UTF-8 text") from e
        if ref.field is not None:
            return {ref.field: SecretStr(text)}
        try:
            data = json.loads(text)
        except ValueError:
            data = None
        if not isinstance(data, dict) or not data or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()
        ):
            raise CredentialError(
                f"KMIP object {ref.uid} is not a JSON object of string fields; "
                "set 'field' on the reference to use its raw value as one field"
            )
        return {name: SecretStr(value) for name, value in data.items()}

    @staticmethod
    def _check_external_context(
        fields: dict[str, SecretStr], context: str, where: str,
    ) -> dict[str, SecretStr]:
        bound = fields.pop(CONTEXT_FIELD, None)
        if bound is not None and bound.get_secret_value() != context:
            raise CredentialError(f"Secret at {where} is bound to a different context")
        if not fields:
            raise CredentialError(f"Secret at {where} has no credential fields")
        return fields
