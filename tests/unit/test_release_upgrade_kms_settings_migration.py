"""Phase placement of the kms_settings-migration upgrade plugin.

Mirrors ``test_release_upgrade_database_indices.py``'s reasoning: ``pre_update``
runs before a single container image has been replaced, so some processes are
still running old code that only reads ``hashicorp_vault_settings`` -- clearing
it there would make that code see no KMS configuration at all. The rewrite
belongs in ``upgrade_complete``, the first point at which the new code is the
only code, and these tests pin that.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster, HashicorpVaultSettings
from simplyblock_core.release_upgrades import kms_settings_migration


class _Phase:
    """Plugin under test with ``DBController`` stubbed; ``cluster`` is a real
    ``Cluster`` so ``set_kms_settings``/``get_kms_settings`` behave for real."""

    def __enter__(self):
        self.cluster = Cluster()
        self.cluster.uuid = "cl-1"
        self.db = MagicMock()
        self.db.get_cluster_by_id.return_value = self.cluster
        self._patch = patch(
            'simplyblock_core.db_controller.DBController', return_value=self.db)
        self._patch.start()
        self.plugin = kms_settings_migration.KMSSettingsMigration()
        return self

    def __exit__(self, *exc):
        self._patch.stop()


def _legacy_vault(**overrides):
    fields = {"base_url": "https://vault.example.com",
              "transit_mount": "sb/transit", "kv_mount": "sb/kv"}
    fields.update(overrides)
    return HashicorpVaultSettings(fields)


class TestApplies(unittest.TestCase):
    def test_always_applies(self):
        """Not gated to one release, unlike jc_compression_upgrade: it has to
        run on every upgrade path, including one that skips several releases."""
        self.assertTrue(kms_settings_migration.KMSSettingsMigration().applies(MagicMock()))


class TestPreUpdate(unittest.TestCase):

    def test_claims_the_upgrade(self):
        with _Phase() as phase:
            phase.plugin.pre_update(phase.cluster)
            self.assertIn(kms_settings_migration.STATE_KEY,
                          phase.cluster.release_upgrade_state)

    def test_does_not_touch_the_legacy_field(self):
        """Old-code processes still reading hashicorp_vault_settings during the
        roll must keep seeing it populated."""
        with _Phase() as phase:
            phase.cluster.hashicorp_vault_settings = _legacy_vault()
            phase.plugin.pre_update(phase.cluster)
            self.assertEqual(phase.cluster.kms_settings, {})
            self.assertIsNotNone(phase.cluster.hashicorp_vault_settings)


class TestUpgradeComplete(unittest.TestCase):

    def test_migrates_legacy_vault_settings(self):
        with _Phase() as phase:
            phase.cluster.hashicorp_vault_settings = _legacy_vault()

            messages = phase.plugin.upgrade_complete(phase.cluster)

            self.assertIsNone(phase.cluster.hashicorp_vault_settings)
            settings = phase.cluster.get_kms_settings()
            self.assertEqual(settings.provider, "vault")
            self.assertEqual(str(settings.base_url), "https://vault.example.com/")
            self.assertEqual(settings.transit_mount, "sb/transit")
            self.assertEqual(settings.kv_mount, "sb/kv")
            self.assertIn("Migrated", messages[0])

    def test_no_legacy_settings_is_a_no_op(self):
        with _Phase() as phase:
            messages = phase.plugin.upgrade_complete(phase.cluster)

            self.assertIsNone(phase.cluster.get_kms_settings())
            self.assertIn("No legacy", messages[0])

    def test_existing_kms_settings_are_not_overwritten(self):
        """A stale legacy field alongside an already-migrated kms_settings must
        not clobber it -- e.g. a cluster that set kms_settings directly (a
        fresh KMIP configuration) while an old vault field still lingers."""
        with _Phase() as phase:
            phase.cluster.set_kms_settings({"provider": "kmip", "server_url": "kmip.internal:5696"})
            phase.cluster.hashicorp_vault_settings = _legacy_vault()

            phase.plugin.upgrade_complete(phase.cluster)

            settings = phase.cluster.get_kms_settings()
            self.assertEqual(settings.provider, "kmip")

    def test_release_of_the_claim_is_persisted(self):
        with _Phase() as phase:
            phase.cluster.release_upgrade_state[kms_settings_migration.STATE_KEY] = {}

            phase.plugin.upgrade_complete(phase.cluster)

            self.assertNotIn(kms_settings_migration.STATE_KEY,
                             phase.cluster.release_upgrade_state)
