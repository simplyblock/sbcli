# coding=utf-8
"""Unit tests for the KMS wrap/unwrap interface.

Exercises both the LocalKMS (FDB-backed) and HCPClient (Vault-backed)
implementations through ``get_wrapped_data_encryption_keys`` and
``unwrap_data_encryption_keys``, plus the unified factory entry
points.
"""

import unittest
from unittest.mock import MagicMock, patch


class TestLocalKMSWrapUnwrap(unittest.TestCase):
    """LocalKMS treats the wrapped payload as plaintext."""

    def _make_lvol(self, key1: str, key2: str):
        lvol = MagicMock()
        lvol.lvol_bdev = "lvol42"
        lvol.crypto_key1 = key1
        lvol.crypto_key2 = key2
        return lvol

    def test_wrapped_payload_carries_plaintext_keys(self):
        from simplyblock_core.kms._fdb import LocalKMS

        lvol = self._make_lvol("a" * 64, "b" * 64)
        kms = LocalKMS(cluster=None)
        wrapped = kms.get_wrapped_data_encryption_keys(lvol)
        self.assertEqual(wrapped, {"type": "local", "keys": ["a" * 64, "b" * 64]})

    def test_unwrap_is_identity(self):
        from simplyblock_core.kms._fdb import LocalKMS

        kms = LocalKMS(cluster=None)
        key1, key2 = kms.unwrap_data_encryption_keys({"type": "local", "keys": ["a" * 64, "b" * 64]})
        self.assertEqual(key1, "a" * 64)
        self.assertEqual(key2, "b" * 64)

    def test_unwrap_rejects_malformed(self):
        from simplyblock_core.kms._fdb import LocalKMS
        from simplyblock_core.kms import KMSException

        kms = LocalKMS(cluster=None)
        with self.assertRaises(KMSException):
            kms.unwrap_data_encryption_keys({})

    def test_roundtrip(self):
        from simplyblock_core.kms._fdb import LocalKMS

        lvol = self._make_lvol("c" * 64, "d" * 64)
        kms = LocalKMS(cluster=None)
        wrapped = kms.get_wrapped_data_encryption_keys(lvol)
        unwrapped = kms.unwrap_data_encryption_keys(wrapped)
        self.assertEqual(unwrapped, ("c" * 64, "d" * 64))

    def test_copy_duplicates_keys(self):
        from simplyblock_core.kms._fdb import LocalKMS

        src = self._make_lvol("a" * 64, "b" * 64)
        dst = MagicMock()
        dst.crypto_key1 = None
        dst.crypto_key2 = None
        kms = LocalKMS(cluster=None)
        kms.copy_data_encryption_keys(src, dst)
        self.assertEqual(dst.crypto_key1, "a" * 64)
        self.assertEqual(dst.crypto_key2, "b" * 64)


class TestHCPClientWrapUnwrap(unittest.TestCase):
    """HCPClient wraps with Vault Transit and unwraps via decrypt."""

    def _make_client(self, cluster_id="cluster-A", base_url="https://vault.example:8200"):
        from simplyblock_core.kms._hcp import HCPClient

        with patch("simplyblock_core.kms._hcp.hvac.Client") as mock_hvac:
            instance = MagicMock()
            mock_hvac.return_value = instance
            client = HCPClient.__new__(HCPClient)
            client.base_url = base_url
            client.cluster_id = cluster_id
            client.transit_mount = "simplyblock/transit"
            client.kv_mount = "simplyblock/kv"
            client.client = instance
            return client, instance

    def _make_lvol(self, pool_uuid="kek-pool", crypto_bdev="crypto_lvol42"):
        lvol = MagicMock()
        lvol.pool_uuid = pool_uuid
        lvol.crypto_bdev = crypto_bdev
        return lvol

    def test_get_wrapped_payload_shape(self):
        client, hvac_inst = self._make_client()
        hvac_inst.secrets.kv.v1.read_secret.return_value = {
            "data": {"keys": ["vault:v1:cipher1", "vault:v1:cipher2"], "kek_name": "kek-pool"},
        }

        lvol = self._make_lvol()
        wrapped = client.get_wrapped_data_encryption_keys(lvol)
        self.assertEqual(wrapped, {
            "type": "vault",
            "base_url": "https://vault.example:8200",
            "ciphertexts": ["vault:v1:cipher1", "vault:v1:cipher2"],
            "kek_name": "kek-pool",
        })
        hvac_inst.secrets.kv.v1.read_secret.assert_called_once_with(
            path="cluster-A/crypto_lvol42", mount_point="simplyblock/kv",
        )

    def test_unwrap_decrypts_each_ciphertext(self):
        import base64
        client, hvac_inst = self._make_client()

        def _decrypt_side_effect(*, name, ciphertext, mount_point):
            self.assertEqual(name, "kek-pool")
            mapping = {
                "vault:v1:cipher1": "a" * 64,
                "vault:v1:cipher2": "b" * 64,
            }
            return {"data": {"plaintext": base64.b64encode(bytes.fromhex(mapping[ciphertext])).decode()}}

        hvac_inst.secrets.transit.decrypt_data.side_effect = _decrypt_side_effect

        key1, key2 = client.unwrap_data_encryption_keys({
            "ciphertexts": ["vault:v1:cipher1", "vault:v1:cipher2"],
            "kek_name": "kek-pool",
        })
        self.assertEqual(key1, "a" * 64)
        self.assertEqual(key2, "b" * 64)

    def test_unwrap_rejects_malformed_payload(self):
        from simplyblock_core.kms import KMSException

        client, _ = self._make_client()
        with self.assertRaises(KMSException):
            client.unwrap_data_encryption_keys({"keys": ["x", "y"]})  # wrong shape

    def test_get_wrapped_without_cluster_id_raises(self):
        from simplyblock_core.kms import KMSException

        client, _ = self._make_client(cluster_id=None)
        with self.assertRaises(KMSException):
            client.get_wrapped_data_encryption_keys(self._make_lvol())

    def test_copy_does_not_decrypt(self):
        """copy_data_encryption_keys must not call transit.decrypt_data."""
        client, hvac_inst = self._make_client()
        src = MagicMock()
        src.crypto_bdev = "crypto_src"
        dst = MagicMock()
        dst.crypto_bdev = "crypto_dst"
        hvac_inst.secrets.kv.v1.read_secret.return_value = {
            "data": {"keys": ["vault:v1:c1", "vault:v1:c2"], "kek_name": "pool"}
        }
        client.copy_data_encryption_keys(src, dst)
        hvac_inst.secrets.transit.decrypt_data.assert_not_called()
        hvac_inst.secrets.kv.v1.create_or_update_secret.assert_called_once_with(
            path="cluster-A/crypto_dst",
            secret={"keys": ["vault:v1:c1", "vault:v1:c2"], "kek_name": "pool"},
            mount_point="simplyblock/kv",
        )

    def test_unwrap_succeeds_without_cluster_id(self):
        """The proxy constructs HCPClient with cluster_id=None — unwrap must still work."""
        import base64
        client, hvac_inst = self._make_client(cluster_id=None)
        hvac_inst.secrets.transit.decrypt_data.return_value = {
            "data": {"plaintext": base64.b64encode(bytes.fromhex("e" * 64)).decode()},
        }
        key1, key2 = client.unwrap_data_encryption_keys({
            "ciphertexts": ["vault:v1:c1", "vault:v1:c2"],
            "kek_name": "kek",
        })
        self.assertEqual(key1, "e" * 64)
        self.assertEqual(key2, "e" * 64)


class TestKmsFactory(unittest.TestCase):
    """Both entry points must route through ``_build_kms``."""

    def test_create_kms_connection_local_for_cluster_without_vault(self):
        from simplyblock_core.kms import create_kms_connection
        from simplyblock_core.kms._fdb import LocalKMS
        from tests._mocks import make_mock_cluster

        cluster = make_mock_cluster()
        kms = create_kms_connection(cluster)
        self.assertIsInstance(kms, LocalKMS)

    def test_create_kms_connection_for_wrapped_local(self):
        from simplyblock_core.kms import create_kms_connection_for_wrapped
        from simplyblock_core.kms._fdb import LocalKMS

        kms = create_kms_connection_for_wrapped({"type": "local", "keys": ["a" * 64, "b" * 64]})
        self.assertIsInstance(kms, LocalKMS)

    def test_create_kms_connection_for_wrapped_vault_routes_to_hcp(self):
        from simplyblock_core.kms import create_kms_connection_for_wrapped
        from simplyblock_core.kms._hcp import HCPClient

        wrapped = {
            "type": "vault",
            "base_url": "https://vault.example:8200",
            "ciphertexts": ["vault:v1:a", "vault:v1:b"],
            "kek_name": "kek",
        }

        # Bypass real TLS-cert / hvac construction; assert routing only.
        with patch("simplyblock_core.kms._require_tls_material"), \
             patch("simplyblock_core.kms.HCPClient") as mock_hcp:
            mock_hcp.return_value = MagicMock(spec=HCPClient)
            create_kms_connection_for_wrapped(wrapped)

        mock_hcp.assert_called_once()
        # First positional arg to HCPClient is base_url.
        self.assertEqual(mock_hcp.call_args.args[0], "https://vault.example:8200")

    def test_create_kms_connection_for_wrapped_vault_requires_url(self):
        from simplyblock_core.kms import create_kms_connection_for_wrapped, KMSException

        with self.assertRaises(KMSException):
            create_kms_connection_for_wrapped({"type": "vault", "ciphertexts": [], "kek_name": ""})

    def test_create_kms_connection_for_wrapped_unknown_type_raises(self):
        from simplyblock_core.kms import create_kms_connection_for_wrapped, KMSException

        with self.assertRaises(KMSException):
            create_kms_connection_for_wrapped({"type": "bogus"})


if __name__ == "__main__":
    unittest.main()
