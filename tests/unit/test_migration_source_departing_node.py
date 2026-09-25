"""A node being drained must still be usable as a migration *source*.

The Kubernetes drain runs the device rebuild before it moves the volumes, and
``node_drain_steps.start_device_decommission`` stamps PENDING_REMOVAL before it
fails the devices -- without that stamp the rebuild tasks get queued on the
departing node itself and never run (see
test_departing_node_no_migration_tasks.py).

That stamp then has to survive the *next* phase. ``start_migration`` guarded its
source with an allow-list of ONLINE/SUSPENDED, so once the device phase had
stamped the node, every volume migration off it was refused with
"Source node is not online (status=pending_removal)" -- and the drain could
never finish. Observed live on 2026-09-25: the Migrating phase sat at
"0 of 1 volumes migrated" while the operator retried every 5s, each attempt
leaving another orphaned migration record behind (5 before the CR wedged).

The distinction the guard has to make is "can this node still serve", not "is it
staying in the cluster". A PENDING_REMOVAL node is still serving -- during that
same run it sustained ~2.4 MB/s with all three of its devices failed.
"""
import unittest
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import migration_controller as ctl
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.lvol_migration import LVolMigration
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status=StorageNode.STATUS_ONLINE):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.status = status
    return node


def _lvol(lvol_id, node_id):
    lvol = MagicMock()
    lvol.get_id.return_value = lvol_id
    lvol.node_id = node_id
    lvol.status = LVol.STATUS_ONLINE
    return lvol


class SourceStatusTests(unittest.TestCase):
    """Only the guard is under test, so the call is expected to fail *past* it.

    Reaching a later precondition is the pass condition: it proves the source
    status was accepted, without standing up a whole migration.
    """

    def _rejected_source(self, status):
        """Run start_migration with a source in `status`; return the source
        rejection message, or None if the guard let it through."""
        mig = MagicMock()
        # Anything else is rejected before the guard under test is reached.
        mig.phase = LVolMigration.PHASE_PRE_CREATED
        mig.lvol_id = "lvol-1"
        mig.target_node_id = "node-tgt"
        mig.cluster_id = "c1"
        db = MagicMock()
        db.get_lvol_by_id.return_value = _lvol("lvol-1", "node-src")
        db.get_migration_by_id.return_value = mig
        db.get_storage_node_by_id.side_effect = lambda i: (
            _node("node-src", status) if i == "node-src" else _node("node-tgt")
        )
        # Fails right after the source/target guards, so any later precondition
        # raising is proof the guard passed.
        db.get_cluster_by_id.side_effect = RuntimeError("past the status guard")

        with patch.object(ctl, 'db', db):
            try:
                ctl.start_migration("mig-uuid")
            except ValueError as e:
                if "Source node" in str(e):
                    return str(e)
                raise
            except RuntimeError:
                return None
        return None

    def test_pending_removal_is_accepted_as_a_source(self):
        """The regression: the drain stamps this before moving the volumes."""
        rejected = self._rejected_source(StorageNode.STATUS_PENDING_REMOVAL)
        self.assertIsNone(
            rejected,
            "a draining node was refused as a migration source, so the drain's "
            f"own volume migration can never run: {rejected}")

    def test_online_and_suspended_are_still_accepted(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED):
            with self.subTest(status=status):
                self.assertIsNone(self._rejected_source(status))

    def test_a_node_whose_spdk_has_stopped_is_still_refused(self):
        """The guard must not become 'anything that is leaving'. Past
        IN_REMOVAL the SPDK is gone, so there is nothing to read from."""
        for status in (StorageNode.STATUS_IN_REMOVAL, StorageNode.STATUS_REMOVED):
            with self.subTest(status=status):
                self.assertIsNotNone(
                    self._rejected_source(status),
                    f"{status} has no SPDK to serve the migration, but was accepted")

    def test_unreachable_states_are_still_refused(self):
        for status in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_DOWN,
                       StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_RESTARTING):
            with self.subTest(status=status):
                self.assertIsNotNone(self._rejected_source(status))


class SourceStatusSetTests(unittest.TestCase):

    def test_the_allow_list_and_the_shut_down_set_cannot_overlap(self):
        """Stated as an invariant rather than a list, so adding a drain status
        later (migrating_devices, migrating_lvols) cannot quietly admit a node
        whose SPDK is already stopped."""
        for status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES:
            self.assertNotIn(status, StorageNode.MIGRATION_SOURCE_STATUSES)

    def test_the_runner_guards_the_source_with_the_shared_list(self):
        """The API guard and the runner's per-phase re-check were two
        hand-written copies of the same tuple. Fixing only the API side let a
        drain's migration be accepted and then suspended by the runner on its
        very next phase, which is how the duplicate was found.

        Checked on the runner's own guard line rather than the whole module:
        the file uses (ONLINE, SUSPENDED) legitimately elsewhere, for target
        and peer checks that are a different question.
        """
        import inspect
        import re

        from simplyblock_core.services import tasks_runner_lvol_migration as runner

        guards = [
            line.strip() for line in inspect.getsource(runner).splitlines()
            if re.search(r'src_node\.status\s+not\s+in', line)
        ]
        self.assertTrue(guards, "the runner's source-status guard has moved or gone")
        for guard in guards:
            self.assertIn(
                "StorageNode.MIGRATION_SOURCE_STATUSES", guard,
                f"the runner guards its source with a local list: {guard}")

    def test_pending_removal_is_departing_but_still_serving(self):
        """The two facts that together make this bug possible, pinned so the
        next reader sees why one status sits on both sides of the line."""
        self.assertIn(StorageNode.STATUS_PENDING_REMOVAL,
                      StorageNode.DEPARTING_STATUSES)
        self.assertNotIn(StorageNode.STATUS_PENDING_REMOVAL,
                         StorageNode.REMOVAL_SHUT_DOWN_STATUSES)
        self.assertIn(StorageNode.STATUS_PENDING_REMOVAL,
                      StorageNode.MIGRATION_SOURCE_STATUSES)


if __name__ == '__main__':
    pytest.main([__file__])
