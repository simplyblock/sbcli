"""A node mid-restart is waited for, never stood in for and never RPC'd.

* resolve_source_node fell through to a replica when the primary was
  RESTARTING, exactly like a primary the removal had stopped. The primary
  takes its lvstore's leadership back when it returns, so a migration pinned
  to the replica would run its source-side cutover on a non-leader. It now
  refuses with a retryable PreconditionError.
* create_migration kept a RESTARTING target replica in its replica set and
  RPC'd it -- during the one window in which its SPDK does not answer -- so
  the create died deep inside with RPCConnectionError after the target bdev
  and subsystem already existed. It now refuses up front, before anything is
  built, and the runner's own lookups already suspend and retry for such a
  replica, so the two sides agree.
"""
import inspect
import unittest
from unittest.mock import MagicMock, patch

import simplyblock_core.services.tasks_runner_lvol_migration as runner
from simplyblock_core.controllers import migration_controller as mc
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status, secondary="", tertiary=""):
    n = MagicMock()
    n.get_id.return_value = node_id
    n.status = status
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    return n


class TestSourceResolutionWaitsForARestart(unittest.TestCase):

    def test_a_restarting_primary_is_not_replaced_by_its_replica(self):
        primary = _node("p", StorageNode.STATUS_RESTARTING, secondary="s")
        replica = _node("s", StorageNode.STATUS_ONLINE)
        with patch.object(mc, "db") as db:
            db.get_storage_node_by_id.return_value = replica
            with self.assertRaises(PreconditionError):
                mc.resolve_source_node(primary)
            db.get_storage_node_by_id.assert_not_called()

    def test_a_stopped_primary_still_resolves_to_its_replica(self):
        primary = _node("p", StorageNode.STATUS_MIGRATING_LVOLS, secondary="s")
        replica = _node("s", StorageNode.STATUS_ONLINE)
        with patch.object(mc, "db") as db:
            db.get_storage_node_by_id.return_value = replica
            self.assertIs(mc.resolve_source_node(primary), replica)


class TestCreateRefusesARestartingTargetReplica(unittest.TestCase):

    def _refuse(self, sec_status, ter_status=StorageNode.STATUS_ONLINE, ha_type="ha"):
        tgt = _node("t", StorageNode.STATUS_ONLINE, secondary="s", tertiary="x")
        lvol = MagicMock()
        lvol.ha_type = ha_type
        nodes = {"s": _node("s", sec_status), "x": _node("x", ter_status)}
        with patch.object(mc, "db") as db:
            db.get_storage_node_by_id.side_effect = lambda i: nodes[i]
            mc._refuse_restarting_target_replicas(tgt, lvol)

    def test_restarting_secondary_is_refused(self):
        with self.assertRaises(PreconditionError):
            self._refuse(StorageNode.STATUS_RESTARTING)

    def test_restarting_tertiary_is_refused(self):
        with self.assertRaises(PreconditionError):
            self._refuse(StorageNode.STATUS_ONLINE, ter_status=StorageNode.STATUS_RESTARTING)

    def test_every_other_status_passes(self):
        for status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_OFFLINE,
                       StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_DOWN, StorageNode.STATUS_MIGRATING_LVOLS):
            self._refuse(status)  # other gates decide these; no refusal here

    def test_a_single_replica_volume_ignores_the_secondary(self):
        self._refuse(StorageNode.STATUS_RESTARTING, ha_type="single")

    def test_the_refusal_runs_before_anything_is_built(self):
        body = inspect.getsource(mc.create_migration)
        self.assertLess(body.index("_refuse_restarting_target_replicas(tgt_node, lvol)"),
                        body.index("create_lvol("))


class TestTheRunnerWaitsForARestartingReplica(unittest.TestCase):
    """Already the case; pinned so the two sides cannot drift apart again."""

    def test_target_secondary_lookup_answers_with_an_error_not_a_node(self):
        tgt = _node("t", StorageNode.STATUS_ONLINE, secondary="s")
        with patch.object(runner, "db") as db:
            db.get_storage_node_by_id.return_value = _node("s", StorageNode.STATUS_RESTARTING)
            node, err = runner._get_target_secondary_node(tgt, "src")
        self.assertIsNone(node)
        self.assertIn(StorageNode.STATUS_RESTARTING, err)


if __name__ == "__main__":
    unittest.main()
