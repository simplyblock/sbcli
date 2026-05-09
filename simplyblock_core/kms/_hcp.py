import base64
import logging
from pathlib import Path
from uuid import UUID

import hvac
import hvac.exceptions

from ._base import KMS
from ._exceptions import KMSException

logger = logging.getLogger(__name__)


class HCPClient(KMS):
    def __init__(
        self,
        base_url: str,
        tls_certificate_authority: Path,
        tls_certificate: Path,
        tls_key: Path,
        cluster_id: UUID,
        timeout: int = 300,
        retry: int = 5,
    ):
        self.cluster_id = cluster_id
        self.client = hvac.Client(
            url=base_url,
            cert=(str(tls_certificate), str(tls_key)),
            verify=str(tls_certificate_authority),
            timeout=timeout,
        )
        try:
            self.client.auth.cert.login()
        except hvac.exceptions.VaultError as e:
            raise KMSException("Authentication failed") from e

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def _create_data_encryption_key(self, kek_name: str) -> str:
        try:
            return self.client.secrets.transit.generate_data_key(
                name=kek_name, key_type='wrapped'
            )['data']['ciphertext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def _encrypt(self, kek_name: str, plaintext_hex: str) -> str:
        plaintext_b64 = base64.b64encode(bytes.fromhex(plaintext_hex)).decode()
        try:
            return self.client.secrets.transit.encrypt_data(
                name=kek_name, plaintext=plaintext_b64
            )['data']['ciphertext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def _decrypt(self, kek_name: str, ciphertext: str) -> str:
        try:
            plaintext_b64 = self.client.secrets.transit.decrypt_data(
                name=kek_name, ciphertext=ciphertext
            )['data']['plaintext']
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e
        return base64.b64decode(plaintext_b64).hex()

    def create_data_encryption_keys(self, kek_name: str, name: str) -> None:
        try:
            self.client.secrets.kv.v1.create_or_update_secret(
                path=f"{self.cluster_id}/{name}",
                secret={"keys": [
                    self._create_data_encryption_key(kek_name),
                    self._create_data_encryption_key(kek_name),
                ]},
                mount_point="kv",
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def import_data_encryption_keys(self, kek_name: str, name: str, keys: tuple[str, str]) -> None:
        try:
            self.client.secrets.kv.v1.create_or_update_secret(
                path=f"{self.cluster_id}/{name}",
                secret={"keys": [
                    self._encrypt(kek_name, keys[0]),
                    self._encrypt(kek_name, keys[1]),
                ]},
                mount_point="kv",
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def get_data_encryption_keys(self, kek_name: str, name: str) -> tuple[str, str]:
        try:
            encrypted_key1, encrypted_key2 = self.client.secrets.kv.v1.read_secret(
                path=f"{self.cluster_id}/{name}",
                mount_point="kv",
            )['data']['keys']
            return (
                self._decrypt(kek_name, encrypted_key1),
                self._decrypt(kek_name, encrypted_key2),
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def delete_data_encryption_keys(self, name: str) -> None:
        try:
            self.client.secrets.kv.v1.delete_secret(
                path=f"{self.cluster_id}/{name}",
                mount_point="kv",
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def create_key_encryption_key(self, name: str) -> None:
        try:
            self.client.secrets.transit.create_key(
                name=name, key_type='aes256-gcm96', exportable=False
            )
            self.client.secrets.transit.update_key_configuration(
                name=name, deletion_allowed=True
            )
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e

    def delete_key_encryption_key(self, name: str) -> None:
        try:
            self.client.secrets.transit.delete_key(name=name)
        except hvac.exceptions.VaultError as e:
            raise KMSException("Request failed") from e
