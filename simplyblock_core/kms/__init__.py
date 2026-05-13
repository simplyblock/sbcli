import logging
from typing import Literal, Optional

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.settings import Settings

from ._base import KMS, WrappedDataEncryptionKeys
from ._exceptions import KMSException
from ._hcp import HCPClient
from ._fdb import LocalKMS

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


Backend = Literal["local", "vault"]


def _require_tls_material(settings: Settings) -> None:
    if (missing := {
        path
        for path in
        [
            settings.tls_certificate_authority,
            settings.tls_certificate,
            settings.tls_key,
        ]
        if not path.is_file()
    }):
        raise KMSException("Missing certificates: " + ", ".join(map(str, missing)))


def _build_kms(
    *,
    backend: Backend,
    vault_url: Optional[str] = None,
    cluster: Optional[Cluster] = None,
    transit_mount: Optional[str] = None,
    kv_mount: Optional[str] = None,
    cert_role: Optional[str] = None,
) -> KMS:
    """Single internal factory used by all public entry points.

    Public callers should use :func:`create_kms_connection` (with a
    cluster) or :func:`create_kms_connection_for_wrapped` (with a
    wrapped DEK payload); both route through here so backend-
    construction logic lives in exactly one place.
    """
    if backend == "local":
        return LocalKMS(cluster=cluster)

    if backend == "vault":
        if not vault_url:
            raise KMSException("Vault backend requires a base URL")
        settings = Settings()
        _require_tls_material(settings)
        cluster_id = cluster.get_id() if cluster is not None else None
        return HCPClient(
            vault_url,
            settings.tls_certificate_authority,
            settings.tls_certificate,
            settings.tls_key,
            cluster_id,
            **({} if transit_mount is None else {"transit_mount": transit_mount}),
            **({} if kv_mount is None else {"kv_mount": kv_mount}),
            **({} if cert_role is None else {"cert_role": cert_role}),
        )

    raise KMSException(f"Unknown KMS backend: {backend!r}")


def create_kms_connection(cluster: Cluster) -> KMS:
    """Construct the KMS connection used by ``simplyblock_core``.

    Backend selection comes from the cluster object: clusters with
    ``hashicorp_vault_settings`` configured use Vault; otherwise the
    local FDB-backed implementation is used.
    """
    if not cluster.hashicorp_vault_settings:
        return _build_kms(backend="local", cluster=cluster)
    vault = cluster.hashicorp_vault_settings
    return _build_kms(
        backend="vault",
        vault_url=vault.base_url,
        cluster=cluster,
        transit_mount=vault.transit_mount,
        kv_mount=vault.kv_mount,
        cert_role=vault.cert_role,
    )


def create_kms_connection_for_wrapped(wrapped: WrappedDataEncryptionKeys) -> KMS:
    """Construct a KMS suitable for unwrapping a wrapped DEK payload.

    The payload is self-describing: its ``type`` field selects the
    backend and its remaining fields carry whatever addressing the
    unwrap side needs (e.g. ``base_url`` for Vault). This is the
    entry point used by the SPDK proxy — it lets the proxy hold no
    KMS state of its own.

    TLS material is read from :class:`~simplyblock_core.settings.Settings`
    on the local node; it is never carried in the payload.
    """
    backend = wrapped.get("type")
    if backend == "local":
        return _build_kms(backend="local")
    if backend == "vault":
        vault_url = wrapped.get("base_url")
        if not vault_url:
            raise KMSException("Vault wrapped payload missing 'base_url'")
        return _build_kms(backend="vault", vault_url=vault_url)
    raise KMSException(f"Unknown KMS backend in wrapped payload: {backend!r}")


__all__ = [
    "KMS",
    "KMSException",
    "WrappedDataEncryptionKeys",
    "create_kms_connection",
    "create_kms_connection_for_wrapped",
]
