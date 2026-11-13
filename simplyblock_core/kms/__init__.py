import logging
from typing import TYPE_CHECKING

from simplyblock_core.settings import Settings

from ._base import KMS
from ._exceptions import KMSException
from ._fdb import LocalKMS
from ._hcp import HCPClient
from ._kmip import KMIPClient
from ._settings import KMIPKMSSettings, KMSSettings, VaultKMSSettings

if TYPE_CHECKING:
    # Type-hint only: models.cluster imports this package for KMSSettings, so a
    # module-level import here would cycle back through it.
    from simplyblock_core.models.cluster import Cluster

__all__ = [
    "KMS",
    "KMIPKMSSettings",
    "KMSException",
    "KMSSettings",
    "VaultKMSSettings",
    "backup_dek_path",
    "backup_kek_name",
    "create_kms_connection",
    "lvol_dek_path",
    "pool_kek_name",
]

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


def lvol_dek_path(cluster_id: str, lvol_id: str) -> str:
    return f"cluster/{cluster_id}/lvol/{lvol_id}"


def backup_dek_path(cluster_id: str, backup_id: str) -> str:
    return f"cluster/{cluster_id}/backup/{backup_id}"


def array_secret_path(cluster_id: str, array_id: str) -> str:
    """Where the credentials of a cluster's storage array live in the KMS
    (multi-attach array LUN mode, simplyblock_core.array)."""
    return f"cluster/{cluster_id}/array/{array_id}"


def pool_kek_name(pool_id: str) -> str:
    return f"pool-{pool_id}"


def backup_kek_name(backup_id: str) -> str:
    return f"backup-{backup_id}"


def create_kms_connection(cluster: "Cluster") -> KMS:
    kms_settings = cluster.get_kms_settings()
    if kms_settings is None:
        return LocalKMS(cluster)

    settings = Settings()
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

    if isinstance(kms_settings, VaultKMSSettings):
        return HCPClient(
            str(kms_settings.base_url),
            settings.tls_certificate_authority,
            settings.tls_certificate,
            settings.tls_key,
            transit_mount=kms_settings.transit_mount,
            kv_mount=kms_settings.kv_mount,
            cert_role=kms_settings.cert_role,
        )

    return KMIPClient(
        kms_settings.server_url,
        settings.tls_certificate_authority,
        settings.tls_certificate,
        settings.tls_key,
    )
