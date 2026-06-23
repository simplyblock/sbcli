import logging

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.settings import Settings

from ._base import KMS
from ._exceptions import KMSException
from ._hcp import HCPClient
from ._fdb import LocalKMS

logger = logging.getLogger()

logger.setLevel(logging.DEBUG)


def dek_path(cluster_id: str, lvol_id: str) -> str:
    return f"cluster/{cluster_id}/lvol/{lvol_id}"


def kek_path(cluster_id: str, pool_id: str) -> str:
    return f"cluster/{cluster_id}/pool/{pool_id}"


def create_kms_connection(cluster: Cluster) -> KMS:
    if not cluster.hashicorp_vault_settings:
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
    vault = cluster.hashicorp_vault_settings
    return HCPClient(
        vault.base_url,
        settings.tls_certificate_authority,
        settings.tls_certificate,
        settings.tls_key,
        transit_mount=vault.transit_mount,
        kv_mount=vault.kv_mount,
        cert_role=vault.cert_role,
    )
