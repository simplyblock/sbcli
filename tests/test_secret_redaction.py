# coding=utf-8
"""Tests for SecretStr-based redaction for BaseModel.

Goal: confirm that secrets are masked anywhere a model or its dict
representation is rendered for humans (repr/str/pprint/log formatters), while
plaintext is still available where business logic requires it (FDB
persistence, explicit consumer reads).
"""
import json
import logging
import pprint
import unittest

from pydantic import SecretStr

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.storage_node import StorageNode


SECRET_VALUE = "super-secret-token-12345"
MASK = "**********"


def _everywhere(rendering: str, value: str) -> bool:
    return value in rendering


class TestBaseModelSecretMasking(unittest.TestCase):
    def setUp(self):
        self.cluster = Cluster()
        self.cluster.uuid = "c-1"
        self.cluster.secret = SecretStr(SECRET_VALUE)
        self.cluster.cli_pass = SecretStr("cli-" + SECRET_VALUE)
        self.cluster.grafana_secret = SecretStr("grafana-" + SECRET_VALUE)
        self.cluster.db_connection = SecretStr("user:pw@host:4500")

    def test_repr_masks_secrets(self):
        rendered = repr(self.cluster)
        self.assertNotIn(SECRET_VALUE, rendered)
        self.assertNotIn("cli-" + SECRET_VALUE, rendered)
        self.assertIn(MASK, rendered)

    def test_str_masks_secrets(self):
        rendered = str(self.cluster)
        self.assertNotIn(SECRET_VALUE, rendered)
        self.assertIn(MASK, rendered)

    def test_to_str_masks_secrets(self):
        self.assertNotIn(SECRET_VALUE, self.cluster.to_str())

    def test_pprint_to_dict_masks_secrets(self):
        rendered = pprint.pformat(self.cluster.to_dict())
        self.assertNotIn(SECRET_VALUE, rendered)
        self.assertIn(MASK, rendered)

    def test_to_dict_default_masks(self):
        d = self.cluster.to_dict()
        self.assertEqual(str(d["secret"]), MASK)
        self.assertEqual(str(d["cli_pass"]), MASK)
        self.assertEqual(str(d["db_connection"]), MASK)

    def test_to_dict_unwrap_returns_plaintext(self):
        d = self.cluster.to_dict(unwrap_secrets=True)
        self.assertEqual(d["secret"], SECRET_VALUE)
        self.assertEqual(d["db_connection"], "user:pw@host:4500")

    def test_log_formatter_masks_secrets(self):
        logger = logging.getLogger("test_secret_redaction")
        with self.assertLogs(logger, level="DEBUG") as captured:
            logger.debug("cluster: %s", self.cluster)
        self.assertFalse(any(SECRET_VALUE in msg for msg in captured.output))


class TestBaseModelFDBRoundtrip(unittest.TestCase):
    """FoundationDB persistence must store the real value, and reading back
    must rehydrate the SecretStr wrapper around the original plaintext."""

    def test_roundtrip_preserves_secret(self):
        original = Cluster()
        original.uuid = "c-roundtrip"
        original.secret = SecretStr(SECRET_VALUE)

        # write_to_db serializes to_dict(unwrap_secrets=True) — verify the
        # JSON blob carries the plaintext, then rehydrate via from_dict.
        wire = json.dumps(original.to_dict(unwrap_secrets=True))
        self.assertIn(SECRET_VALUE, wire)

        restored = Cluster()
        restored.from_dict(json.loads(wire))
        self.assertIsInstance(restored.secret, SecretStr)
        self.assertEqual(restored.secret.get_secret_value(), SECRET_VALUE)


class TestStorageNodeAndPoolMasking(unittest.TestCase):
    def test_storage_node_to_dict_masks(self):
        node = StorageNode()
        node.uuid = "n-1"
        node.ctrl_secret = SecretStr("ctrl-secret-xyz")
        node.host_secret = SecretStr("host-secret-xyz")
        node.rpc_password = SecretStr("rpc-pw")

        d = node.to_dict()
        self.assertEqual(str(d["ctrl_secret"]), MASK)
        self.assertEqual(str(d["host_secret"]), MASK)
        self.assertEqual(str(d["rpc_password"]), MASK)

    def test_pool_to_dict_masks(self):
        pool = Pool()
        pool.uuid = "p-1"
        pool.dhchap_key = SecretStr("DHHC-1:01:abc=:")
        pool.dhchap_ctrlr_key = SecretStr("DHHC-1:01:def=:")

        d = pool.to_dict()
        self.assertEqual(str(d["dhchap_key"]), MASK)
        self.assertEqual(str(d["dhchap_ctrlr_key"]), MASK)


if __name__ == "__main__":
    unittest.main()
