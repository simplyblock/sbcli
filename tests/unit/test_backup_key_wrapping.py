"""Wrapping and unwrapping a backup's data encryption keys.

Key wrapping is the one place a backup carries key material, so the properties that
matter are: the plaintext key never appears in the wrapped form, a wrong
passphrase fails loudly rather than returning a plausible wrong key, and two
backups wrapped under the same passphrase share nothing an attacker could reuse.
"""
import base64
import json

import pytest
from pydantic import SecretStr

from simplyblock_core import backup_key_wrapping
from simplyblock_core.backup_key_wrapping import KeyWrappingError, WrappedKeys


KEYS = ("a" * 64, "b" * 64)
PASSPHRASE = SecretStr("correct horse battery staple")


class TestRoundTrip:
    def test_wrap_then_unwrap_recovers_the_keys(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        assert backup_key_wrapping.unwrap(wrapped, PASSPHRASE) == KEYS

    def test_survives_json_serialization(self):
        """The wrapped form travels inside a manifest."""
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)

        restored = WrappedKeys.model_validate(json.loads(wrapped.model_dump_json()))

        assert backup_key_wrapping.unwrap(restored, PASSPHRASE) == KEYS

    def test_keys_are_opaque_to_the_encoding(self):
        """Nothing about a key's content may affect whether it round-trips."""
        awkward = ('{"not": "hex"}', 'has:colons,and"quotes')
        wrapped = backup_key_wrapping.wrap(awkward, PASSPHRASE)

        assert backup_key_wrapping.unwrap(wrapped, PASSPHRASE) == awkward


class TestSecrecy:
    def test_plaintext_key_is_absent_from_the_wrapped_form(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        blob = wrapped.model_dump_json()

        assert KEYS[0] not in blob
        assert KEYS[1] not in blob

    def test_passphrase_is_absent_from_the_wrapped_form(self):
        wrapped = backup_key_wrapping.wrap(KEYS, SecretStr("hunter2"))
        assert "hunter2" not in wrapped.model_dump_json()

    def test_each_wrapping_uses_a_fresh_salt_and_nonce(self):
        """A shared salt would let one cracked passphrase precompute against all."""
        first = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        second = backup_key_wrapping.wrap(KEYS, PASSPHRASE)

        assert first.salt != second.salt
        assert first.nonce != second.nonce
        assert first.ciphertext != second.ciphertext

    def test_salt_is_full_length(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        assert len(base64.b64decode(wrapped.salt)) == backup_key_wrapping.SALT_BYTES
        assert len(base64.b64decode(wrapped.nonce)) == backup_key_wrapping.NONCE_BYTES


class TestFailureModes:
    def test_wrong_passphrase_is_rejected_not_silently_wrong(self):
        """AES-GCM authenticates, so this cannot return a plausible wrong key."""
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)

        with pytest.raises(KeyWrappingError, match="wrong passphrase or corrupt"):
            backup_key_wrapping.unwrap(wrapped, SecretStr("not the passphrase"))

    def test_tampered_ciphertext_is_rejected(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        raw = bytearray(base64.b64decode(wrapped.ciphertext))
        raw[0] ^= 0xFF
        wrapped.ciphertext = base64.b64encode(bytes(raw)).decode()

        with pytest.raises(KeyWrappingError):
            backup_key_wrapping.unwrap(wrapped, PASSPHRASE)

    def test_empty_passphrase_is_refused_at_wrap_time(self):
        """It would derive a fixed key, leaving the backup effectively unencrypted."""
        with pytest.raises(KeyWrappingError, match="empty passphrase"):
            backup_key_wrapping.wrap(KEYS, SecretStr(""))

    def test_unknown_kdf_is_refused(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        wrapped.kdf = "rot13"

        with pytest.raises(KeyWrappingError, match="unknown key derivation"):
            backup_key_wrapping.unwrap(wrapped, PASSPHRASE)

    def test_corrupt_base64_is_reported_clearly(self):
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        wrapped.salt = "not base64!!"

        with pytest.raises(KeyWrappingError):
            backup_key_wrapping.unwrap(wrapped, PASSPHRASE)


class TestParameters:
    def test_recorded_parameters_are_used_when_opening(self):
        """A later change to the defaults must not orphan existing backups."""
        wrapped = backup_key_wrapping.wrap(KEYS, PASSPHRASE)
        assert wrapped.params.memory_kib == backup_key_wrapping.ARGON2_MEMORY_KIB

        original = backup_key_wrapping.ARGON2_MEMORY_KIB
        try:
            backup_key_wrapping.ARGON2_MEMORY_KIB = original * 2
            assert backup_key_wrapping.unwrap(wrapped, PASSPHRASE) == KEYS
        finally:
            backup_key_wrapping.ARGON2_MEMORY_KIB = original
