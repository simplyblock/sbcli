"""Phase placement of the database-indices upgrade plugin.

``pre_update`` runs as the first step of ``cluster update``, before a single
container image has been replaced: every API instance, service and task runner
is still executing the previous release. That code maintains no index and still
reads the legacy ``name_index/`` and ``lvol_snaps/`` key families. Doing either
half of the migration there loses data silently — records created in the window
never reach an index that already reports ``ready``, and a cleared name family
makes the running code answer "name free" for every name it holds.

So both halves belong in ``upgrade_complete``, and that is what these tests
pin.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.release_upgrades import database_indices


def _cluster():
    cluster = MagicMock()
    cluster.get_id.return_value = "cl-1"
    cluster.release_upgrade_state = {}
    return cluster


class _Phase:
    """Plugin under test with ``DBController`` and ``index_ops`` stubbed."""

    def __enter__(self):
        self.cluster = _cluster()
        self.db = MagicMock()
        self.db.get_cluster_by_id.return_value = self.cluster
        self._patches = [
            patch('simplyblock_core.db_controller.DBController', return_value=self.db),
            patch.object(database_indices.index_ops, 'build_indices', return_value=['built']),
        ]
        for p in self._patches:
            p.start()
        self.build_indices = database_indices.index_ops.build_indices
        self.plugin = database_indices.DatabaseIndices()
        return self

    def __exit__(self, *exc):
        for p in self._patches:
            p.stop()

    @property
    def cleared(self):
        return [call.args[0] for call
                in self.db.kv_store.clear_range_startswith.call_args_list]


class TestPreUpdate(unittest.TestCase):

    def test_builds_nothing(self):
        """No index may reach `ready` while the old image is still writing."""
        with _Phase() as phase:
            phase.plugin.pre_update(phase.cluster)
            phase.build_indices.assert_not_called()

    def test_keeps_the_legacy_key_families(self):
        """The old image still reads them for name uniqueness and chain linking."""
        with _Phase() as phase:
            phase.plugin.pre_update(phase.cluster)
            self.assertEqual(phase.cleared, [])

    def test_claims_the_upgrade(self):
        """`run_upgrade_complete` selects plugins by state key, so pre_update
        has to leave one behind or the second half never runs."""
        with _Phase() as phase:
            phase.plugin.pre_update(phase.cluster)
            self.assertIn(database_indices.STATE_KEY, phase.cluster.release_upgrade_state)
            phase.cluster.write_to_db.assert_called_once()


class TestUpgradeComplete(unittest.TestCase):

    def test_builds_and_flips(self):
        with _Phase() as phase:
            messages = phase.plugin.upgrade_complete(phase.cluster)
            phase.build_indices.assert_called_once()
            self.assertIn('built', messages)

    def test_clears_the_legacy_key_families(self):
        with _Phase() as phase:
            phase.plugin.upgrade_complete(phase.cluster)
            self.assertEqual(phase.cleared, list(database_indices.OBSOLETE_PREFIXES))

    def test_release_of_the_claim_is_persisted(self):
        """The caller re-reads the cluster to stamp the installed release, so a
        pop that is not written back is discarded."""
        with _Phase() as phase:
            phase.cluster.release_upgrade_state[database_indices.STATE_KEY] = {}
            phase.plugin.upgrade_complete(phase.cluster)
            self.assertNotIn(database_indices.STATE_KEY, phase.cluster.release_upgrade_state)
            phase.cluster.write_to_db.assert_called_once()
