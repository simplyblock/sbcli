"""Migrations on 1- and 2-node clusters must ask ultra for relaxed placement.

ultra relaxes write placement by itself when fewer nodes are available than
chunks per stripe, but ``distr_migration_*_start`` only relaxes when the
caller sets ``relaxed_mode`` (default false). Without it a failed-device or
new-device migration on a single-node or two-node cluster has no strict
target and never completes: rebalancing would need a third node.
"""
import unittest
from unittest import mock

from simplyblock_core import rpc_client as rpc_mod
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import migration_task_common as mig


class TestNeedsRelaxedPlacement(unittest.TestCase):

    def test_single_node_is_relaxed_for_every_scheme(self):
        for ndcs, npcs in [(1, 0), (1, 1), (2, 1), (2, 2), (4, 2)]:
            self.assertTrue(mig.needs_relaxed_placement(1, ndcs, npcs), (ndcs, npcs))

    def test_two_nodes_are_relaxed_for_every_scheme(self):
        for ndcs, npcs in [(1, 1), (2, 1), (2, 2)]:
            self.assertTrue(mig.needs_relaxed_placement(2, ndcs, npcs), (ndcs, npcs))

    def test_fewer_nodes_than_chunks_is_relaxed(self):
        self.assertTrue(mig.needs_relaxed_placement(5, 4, 2))
        self.assertTrue(mig.needs_relaxed_placement(3, 2, 2))

    def test_enough_nodes_stay_strict(self):
        self.assertFalse(mig.needs_relaxed_placement(3, 1, 1))
        self.assertFalse(mig.needs_relaxed_placement(3, 2, 1))
        self.assertFalse(mig.needs_relaxed_placement(6, 4, 2))


class TestMigrationRelaxedLookup(unittest.TestCase):

    def _nodes(self, *statuses):
        return [mock.Mock(status=s) for s in statuses]

    def test_removed_nodes_do_not_count(self):
        cluster = mock.Mock(distr_ndcs=2, distr_npcs=1)
        nodes = self._nodes(StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE,
                            StorageNode.STATUS_REMOVED)
        with mock.patch.object(mig, "db") as db:
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_nodes_by_cluster_id.return_value = nodes
            self.assertTrue(mig.migration_relaxed("c1"))

    def test_offline_member_still_counts(self):
        cluster = mock.Mock(distr_ndcs=2, distr_npcs=1)
        nodes = self._nodes(StorageNode.STATUS_ONLINE, StorageNode.STATUS_ONLINE,
                            StorageNode.STATUS_OFFLINE)
        with mock.patch.object(mig, "db") as db:
            db.get_cluster_by_id.return_value = cluster
            db.get_storage_nodes_by_cluster_id.return_value = nodes
            self.assertFalse(mig.migration_relaxed("c1"))


class TestRpcSendsRelaxedMode(unittest.TestCase):

    def _client(self):
        client = rpc_mod.RPCClient.__new__(rpc_mod.RPCClient)
        client._request = mock.Mock(return_value=True)
        return client

    def test_failure_start_sends_flag_only_when_relaxed(self):
        c = self._client()
        c.distr_migration_failure_start("d", 3, relaxed_mode=True)
        self.assertIs(c._request.call_args.kwargs["relaxed_mode"], True)
        c.distr_migration_failure_start("d", 3)
        self.assertNotIn("relaxed_mode", c._request.call_args.kwargs)

    def test_expansion_start_sends_flag_only_when_relaxed(self):
        c = self._client()
        c.distr_migration_expansion_start("d", relaxed_mode=True)
        self.assertIs(c._request.call_args.kwargs["relaxed_mode"], True)
        c.distr_migration_expansion_start("d")
        self.assertNotIn("relaxed_mode", c._request.call_args.kwargs)


class TestRunnersPassRelaxedMode(unittest.TestCase):
    """Every migration runner hands the cluster's relaxed decision to the RPC."""

    def test_each_runner_passes_relaxed_mode(self):
        import inspect

        from simplyblock_core.services import (
            tasks_runner_failed_migration,
            tasks_runner_migration,
            tasks_runner_new_dev_migration,
        )
        for mod in (tasks_runner_failed_migration, tasks_runner_migration, tasks_runner_new_dev_migration):
            src = inspect.getsource(mod)
            self.assertIn("relaxed_mode=mig.migration_relaxed(", src, mod.__name__)


if __name__ == "__main__":
    unittest.main()
