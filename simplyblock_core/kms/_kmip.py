import json
from pathlib import Path

from kmip.core import enums
from kmip.pie import exceptions as kmip_exceptions
from kmip.pie.client import ProxyKmipClient
from pydantic import SecretStr

from simplyblock_core.utils import generate_hex_string

from ._base import KMS
from ._exceptions import KMSException

#: Errors raised by the pykmip client, covering both failed operations and
#: connection problems — mirrors ``_hcp.py`` catching ``hvac.exceptions.VaultError``.
_KMIP_ERRORS = (
    kmip_exceptions.KmipOperationFailure,
    kmip_exceptions.ClientConnectionFailure,
    kmip_exceptions.ClientConnectionNotOpen,
)

#: Fixed across every Create/Encrypt/Decrypt call: the KEK is always created as
#: AES-256, and Encrypt/Decrypt have to be told the same algorithm/mode they
#: were (or will be) used with, since the server does not infer it from the
#: key alone.
#:
#: CBC/PKCS5, not GCM: verified against PyKMIP's own reference server (which
#: this backend is tested against), whose Decrypt operation demands an
#: authentication tag its pie client has no way to supply or retrieve for
#: Encrypt -- a wrapper-level gap in that implementation, not a KMIP spec
#: requirement. The DEK this wraps is random key material with no other
#: integrity requirement (LocalKMS's FDB records carry it as plaintext with no
#: integrity protection at all), so CBC's confidentiality-only guarantee is not
#: a regression against the existing backends.
_CRYPTOGRAPHIC_PARAMETERS = {
    "cryptographic_algorithm": enums.CryptographicAlgorithm.AES,
    "block_cipher_mode": enums.BlockCipherMode.CBC,
    "padding_method": enums.PaddingMethod.PKCS5,
}

_KEK_KEY_PREFIX = "kms/kmip/kek/"


class KMIPClient(KMS):
    """KMIP-speaking backend: the KEK lives as a managed symmetric key on the
    appliance; DEKs are generated locally (like ``LocalKMS``) and wrapped with
    the appliance's ``Encrypt``/``Decrypt`` operations against that KEK — the
    same shape ``HCPClient`` uses against Vault's Transit engine.

    KMIP has no equivalent of Vault's KV secrets engine, so wrapped DEK
    ciphertext and the KEK-name -> appliance-UID mapping are both kept in this
    process's own key/value store (FoundationDB), not on the appliance. The
    appliance's ``Locate`` operation is deliberately not used for the KEK
    lookup: attribute/Locate support varies too much across KMIP vendors to
    depend on for identity.
    """

    def __init__(
        self,
        server_url: str,
        tls_certificate_authority: Path,
        tls_certificate: Path,
        tls_key: Path,
    ):
        host, _, port = server_url.rpartition(":")
        if not host or not port.isdigit():
            raise KMSException(f"Invalid KMIP server URL: {server_url!r}, expected host:port")
        # ProxyKmipClient has no connection-timeout parameter, unlike hvac.Client.
        self.client = ProxyKmipClient(
            hostname=host,
            port=int(port),
            cert=str(tls_certificate),
            key=str(tls_key),
            ca=str(tls_certificate_authority),
        )

    def __enter__(self):
        try:
            self.client.open()
        except Exception as e:
            raise KMSException("Authentication failed") from e
        return self

    def __exit__(self, *args):
        self.client.close()

    @staticmethod
    def _kv_store():
        # Imported here, not at module level: db_controller imports
        # models.cluster, which imports this package for KMSSettings.
        from simplyblock_core.db_controller import DBController

        kv_store = DBController().kv_store
        if kv_store is None:
            raise KMSException("No database connection")
        return kv_store

    @staticmethod
    def _kek_key(name: str) -> bytes:
        return (_KEK_KEY_PREFIX + name).encode()

    @staticmethod
    def _dek_key(path: str) -> bytes:
        return f"keys/{path}".encode()

    def _kek_uid(self, kek_name: str) -> str:
        raw = self._kv_store().get(self._kek_key(kek_name))
        if not raw:
            raise KMSException(f"No key encryption key found for {kek_name}")
        return raw.decode()

    def create_key_encryption_key(self, name: str) -> None:
        try:
            uid = self.client.create(enums.CryptographicAlgorithm.AES, 256)
            self.client.activate(uid)
        except _KMIP_ERRORS as e:
            raise KMSException("Request failed") from e
        self._kv_store().set(self._kek_key(name), uid.encode())

    def delete_key_encryption_key(self, name: str) -> None:
        kv_store = self._kv_store()
        raw = kv_store.get(self._kek_key(name))
        if not raw:
            return
        try:
            # KMIP refuses to Destroy an object in the Active state
            # (PERMISSION_DENIED); Revoke transitions it out first.
            uid = raw.decode()
            self.client.revoke(enums.RevocationReasonCode.CESSATION_OF_OPERATION, uid)
            self.client.destroy(uid)
        except _KMIP_ERRORS as e:
            raise KMSException("Request failed") from e
        kv_store.clear(self._kek_key(name))

    def _encrypt(self, kek_uid: str, plaintext_hex: SecretStr) -> dict:
        try:
            ciphertext, iv_counter_nonce = self.client.encrypt(
                bytes.fromhex(plaintext_hex.get_secret_value()),
                uid=kek_uid,
                cryptographic_parameters=dict(_CRYPTOGRAPHIC_PARAMETERS),
            )
        except _KMIP_ERRORS as e:
            raise KMSException("Request failed") from e
        return {
            "ciphertext": ciphertext.hex(),
            "iv": iv_counter_nonce.hex() if iv_counter_nonce else "",
        }

    def _decrypt(self, kek_uid: str, wrapped: dict) -> SecretStr:
        try:
            plaintext = self.client.decrypt(
                bytes.fromhex(wrapped["ciphertext"]),
                uid=kek_uid,
                cryptographic_parameters=dict(_CRYPTOGRAPHIC_PARAMETERS),
                iv_counter_nonce=bytes.fromhex(wrapped["iv"]) if wrapped.get("iv") else None,
            )
        except _KMIP_ERRORS as e:
            raise KMSException("Request failed") from e
        return SecretStr(plaintext.hex())

    def create_data_encryption_keys(self, path: str, kek_name: str) -> None:
        self.import_data_encryption_keys(
            path, kek_name,
            (SecretStr(generate_hex_string(32)), SecretStr(generate_hex_string(32))),
        )

    def import_data_encryption_keys(
        self, path: str, kek_name: str, keys: tuple[SecretStr, SecretStr],
    ) -> None:
        kek_uid = self._kek_uid(kek_name)
        wrapped = [self._encrypt(kek_uid, key) for key in keys]
        self._kv_store().set(self._dek_key(path), json.dumps(wrapped).encode())

    def get_data_encryption_keys(self, path: str, kek_name: str) -> tuple[SecretStr, SecretStr]:
        raw = self._kv_store().get(self._dek_key(path))
        if not raw:
            raise KMSException(f"No keys found at {path}")
        kek_uid = self._kek_uid(kek_name)
        wrapped1, wrapped2 = json.loads(raw)
        return self._decrypt(kek_uid, wrapped1), self._decrypt(kek_uid, wrapped2)

    def delete_data_encryption_keys(self, path: str) -> None:
        self._kv_store().clear(self._dek_key(path))

    # Generic secrets (the array credentials of a multi-attach cluster,
    # simplyblock_core.controllers.array_controller). Not supported on KMIP
    # yet: KMIP keeps keys, and storing secrets would mean wrapping them with
    # an appliance KEK the way the DEKs above are, which has not been tested
    # against a KMIP appliance. Refused explicitly rather than half-done.
    _NO_SECRETS = ("The KMIP key management backend does not store array credentials yet; "
                   "use HashiCorp Vault / OpenBao or the local KMS for a multi-attach cluster "
                   "with array credentials, or register the array without credentials "
                   "(e.g. aws-ebs with an instance role)")

    def put_secret(self, path: str, data: dict) -> None:
        raise KMSException(self._NO_SECRETS)

    def get_secret(self, path: str) -> dict:
        raise KMSException(self._NO_SECRETS)

    def delete_secret(self, path: str) -> None:
        raise KMSException(self._NO_SECRETS)
