import base64
import logging
from pathlib import Path
from typing import Optional
from uuid import UUID

import hvac
import hvac.exceptions

from ._base import KMS, WrappedDataEncryptionKeys
from ._exceptions import KMSException

logger = logging.getLogger(__name__)

class HCPClient(KMS):
    """KMS backed by HashiCorp Vault / OpenBao.

    DEKs are stored encrypted in the Vault KV engine, wrapped by a
    per-pool KEK in the Transit engine. The wrapped payload returned
    by :meth:`get_wrapped_data_encryption_keys` carries those Vault
    ciphertexts plus the ``kek_name`` needed to decrypt them.

    :meth:`unwrap_data_encryption_keys` calls Vault Transit to decrypt
    and is intended to run on the SPDK proxy, so that the control
    plane never holds plaintext DEKs.

    ``cluster_id`` is only required for the KV path used to read/write
    wrapped DEKs (i.e. for the core side). The proxy side, which only
    calls :meth:`unwrap_data_encryption_keys`, may construct this
    client with ``cluster_id=None``.
    """

    def __init__(
        self,
        base_url: str,
        tls_certificate_authority: Path,
        tls_certificate: Path,
        tls_key: Path,
        cluster_id: Optional[UUID] = None,
        transit_mount: str = "simplyblock/transit",
        kv_mount: str = "simplyblock/kv",
        cert_role: str = "simplyblock-webappapi",
        timeout: int = 300,
        retry: int = 5,
    ):
        self.base_url = base_url
        self.cluster_id = cluster_id
        self.transit_mount = transit_mount
        self.kv_mount = kv_mount
        self.client = hvac.Client(
            url=base_url,
            cert=(str(tls_certificate), str(tls_key)),
            verify=str(tls_certificate_authority),
            timeout=timeout,
        )
        try:
            self.client.auth.cert.login(name=cert_role)
        except hvac.exceptions.VaultError as e:
            raise KMSException("Authentication failed") from e

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def _require_cluster_id(self) -> UUID:
        if self.cluster_id is None:
            raise KMSException("HCPClient requires a cluster_id for this operation")
        return self.cluster_id

    def _create_data_encryption_key(self, kek_name: str) -> str:
        try:
            return self.client.secrets.transit.generate_data_key(
                name=kek_name, key_type='wrapped', mount_point=self.transit_mount,
            )['data']['ciphertext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def _encrypt(self, kek_name: str, plaintext_hex: str) -> str:
        plaintext_b64 = base64.b64encode(bytes.fromhex(plaintext_hex)).decode()
        try:
            return self.client.secrets.transit.encrypt_data(
                name=kek_name, plaintext=plaintext_b64, mount_point=self.transit_mount,
            )['data']['ciphertext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def _decrypt(self, kek_name: str, ciphertext: str) -> str:
        try:
            plaintext_b64 = self.client.secrets.transit.decrypt_data(
                name=kek_name, ciphertext=ciphertext, mount_point=self.transit_mount,
            )['data']['plaintext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e
        return base64.b64decode(plaintext_b64).hex()

    def create_data_encryption_keys(self, lvol) -> None:
        kek_name, name = str(lvol.pool_uuid), lvol.crypto_bdev
        try:
            self.client.secrets.kv.v1.create_or_update_secret(
                path=f"{self._require_cluster_id()}/{name}",
                secret={"keys": [
                    self._create_data_encryption_key(kek_name),
                    self._create_data_encryption_key(kek_name),
                ], "kek_name": kek_name},
                mount_point=self.kv_mount,
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def import_data_encryption_keys(self, lvol, keys: tuple[str, str]) -> None:
        kek_name, name = str(lvol.pool_uuid), lvol.crypto_bdev
        try:
            self.client.secrets.kv.v1.create_or_update_secret(
                path=f"{self._require_cluster_id()}/{name}",
                secret={"keys": [
                    self._encrypt(kek_name, keys[0]),
                    self._encrypt(kek_name, keys[1]),
                ], "kek_name": kek_name},
                mount_point=self.kv_mount,
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def get_wrapped_data_encryption_keys(self, lvol) -> WrappedDataEncryptionKeys:
        kek_name, name = str(lvol.pool_uuid), lvol.crypto_bdev
        try:
            secret = self.client.secrets.kv.v1.read_secret(
                path=f"{self._require_cluster_id()}/{name}",
                mount_point=self.kv_mount,
            )['data']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e
        ciphertext1, ciphertext2 = secret['keys']
        return {
            "type": "vault",
            "base_url": self.base_url,
            "ciphertexts": [ciphertext1, ciphertext2],
            "kek_name": kek_name,
        }

    def unwrap_data_encryption_keys(self, wrapped: WrappedDataEncryptionKeys) -> tuple[str, str]:
        try:
            ciphertext1, ciphertext2 = wrapped["ciphertexts"]
            kek_name = wrapped["kek_name"]
        except (KeyError, ValueError) as e:
            raise KMSException("Malformed wrapped DEK payload") from e
        return (
            self._decrypt(kek_name, ciphertext1),
            self._decrypt(kek_name, ciphertext2),
        )

    def copy_data_encryption_keys(self, src_lvol, dst_lvol) -> None:
        cluster_id = self._require_cluster_id()
        try:
            secret = self.client.secrets.kv.v1.read_secret(
                path=f"{cluster_id}/{src_lvol.crypto_bdev}",
                mount_point=self.kv_mount,
            )['data']
            self.client.secrets.kv.v1.create_or_update_secret(
                path=f"{cluster_id}/{dst_lvol.crypto_bdev}",
                secret=secret,
                mount_point=self.kv_mount,
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Failed to copy encryption keys") from e

    def delete_data_encryption_keys(self, name: str) -> None:
        try:
            self.client.secrets.kv.v1.delete_secret(
                path=f"{self._require_cluster_id()}/{name}",
                mount_point=self.kv_mount,
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def create_key_encryption_key(self, name: str) -> None:
        try:
            self.client.secrets.transit.create_key(
                name=name, key_type='aes256-gcm96', exportable=False,
                mount_point=self.transit_mount,
            )
            self.client.secrets.transit.update_key_configuration(
                name=name, deletion_allowed=True, mount_point=self.transit_mount,
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def delete_key_encryption_key(self, name: str) -> None:
        try:
            self.client.secrets.transit.delete_key(name=name, mount_point=self.transit_mount)
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e
