# coding=utf-8
"""Wrapping a backup's data encryption keys under an operator-held secret.

An encrypted volume's backup is ciphertext in a bucket; the key that decrypts it
lives in the originating cluster's KMS. With HashiCorp Vault that key is
reachable only by a cluster that can still reach the same Vault. With the
FoundationDB-backed LocalKMS it is stored in the originating cluster's own
database, so once that cluster is gone the backup is undecryptable by anyone --
including its owner.

Key wrapping is the way out: at backup time the data encryption keys are wrapped under
a key derived from a secret the operator holds, and the wrapped blob travels
with the backup. Recovery then needs the bucket, credentials for it, and the
passphrase -- and nothing from the cluster that is gone.

This is a deliberate, narrow exception to "a manifest never carries secrets": it
carries key material, but only ever as ciphertext under a secret that is not in
the manifest, not in the bucket, and not in the database. Security rests
entirely on the strength of that passphrase, which is why the KDF is Argon2id
with deliberately expensive parameters rather than a bare hash.

Key wrapping is opt-in per cluster. Without it, an encrypted backup records only a
descriptor of where its key lives, and is recoverable only by a cluster that can
still reach that KMS.
"""
import base64
import json
import os
from typing import Tuple

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.argon2 import Argon2id
from pydantic import BaseModel, ConfigDict, SecretStr


KDF_ARGON2ID = "argon2id"

#: OWASP's baseline Argon2id parameters (19 MiB, 2 iterations, 1 lane). The cost
#: is paid once per backup and once per restore, so it can afford to be high;
#: the passphrase is the only thing standing between the bucket and the
#: plaintext.
ARGON2_MEMORY_KIB = 19 * 1024
ARGON2_ITERATIONS = 2
ARGON2_LANES = 1

SALT_BYTES = 16
NONCE_BYTES = 12
KEY_BYTES = 32



class KeyWrappingError(Exception):
    """Keys could not be wrapped or unwrapped."""


class Argon2Params(BaseModel):
    """The KDF parameters used, so a future change to the defaults stays readable."""
    model_config = ConfigDict(extra="forbid")

    memory_kib: int = ARGON2_MEMORY_KIB
    iterations: int = ARGON2_ITERATIONS
    lanes: int = ARGON2_LANES


class WrappedKeys(BaseModel):
    """A wrapped data encryption key pair, safe to store beside the ciphertext.

    Every field here is either public or ciphertext. The passphrase that opens
    it is held by the operator and appears nowhere in this structure.
    """
    model_config = ConfigDict(extra="forbid")

    kdf: str = KDF_ARGON2ID
    params: Argon2Params = Argon2Params()

    #: Base64, per-backup. A shared salt would let one cracked passphrase
    #: precompute against every backup at once.
    salt: str
    nonce: str
    ciphertext: str


def _derive(passphrase: SecretStr, salt: bytes, params: Argon2Params) -> bytes:
    return Argon2id(
        salt=salt,
        length=KEY_BYTES,
        iterations=params.iterations,
        lanes=params.lanes,
        memory_cost=params.memory_kib,
    ).derive(passphrase.get_secret_value().encode())


def wrap(keys: Tuple[str, str], passphrase: SecretStr) -> WrappedKeys:
    """Wrap a data encryption key pair under an operator secret.

    Raises:
        KeyWrappingError: The passphrase is empty. An empty secret would produce a
            deterministic key and leave the backup effectively unencrypted,
            which is worse than having no wrapped_key at all.
    """
    if not passphrase.get_secret_value():
        raise KeyWrappingError("Refusing to wrap keys under an empty passphrase")

    params = Argon2Params()
    salt = os.urandom(SALT_BYTES)
    nonce = os.urandom(NONCE_BYTES)
    plaintext = json.dumps(list(keys)).encode()

    ciphertext = AESGCM(_derive(passphrase, salt, params)).encrypt(nonce, plaintext, None)

    return WrappedKeys(
        params=params,
        salt=base64.b64encode(salt).decode(),
        nonce=base64.b64encode(nonce).decode(),
        ciphertext=base64.b64encode(ciphertext).decode(),
    )


def unwrap(wrapped_key: WrappedKeys, passphrase: SecretStr) -> Tuple[str, str]:
    """Recover a data encryption key pair from its wrapped form.

    Raises:
        KeyWrappingError: The passphrase is wrong, the blob is corrupt, or it was
            wrapped by a scheme this build does not implement. AES-GCM
            authenticates, so a wrong passphrase fails here rather than
            returning a plausible-looking wrong key.
    """
    if wrapped_key.kdf != KDF_ARGON2ID:
        raise KeyWrappingError(
            f"Wrapped key uses unknown key derivation '{wrapped_key.kdf}'; "
            f"this build implements '{KDF_ARGON2ID}'")

    try:
        salt = base64.b64decode(wrapped_key.salt)
        nonce = base64.b64decode(wrapped_key.nonce)
        ciphertext = base64.b64decode(wrapped_key.ciphertext)
    except ValueError as e:
        raise KeyWrappingError("Key wrappinged key material is not valid base64") from e

    try:
        plaintext = AESGCM(_derive(passphrase, salt, wrapped_key.params)).decrypt(
            nonce, ciphertext, None)
    except InvalidTag as e:
        raise KeyWrappingError(
            "Could not open wrapped keys: wrong passphrase or corrupt data") from e

    try:
        key1, key2 = json.loads(plaintext)
    except (ValueError, TypeError) as e:
        raise KeyWrappingError("Key wrappinged key material is malformed") from e

    return key1, key2
