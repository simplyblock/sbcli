# coding=utf-8
"""
test_stale_restart_phase.py — regression tests for incident 2026-07-10
(K8sNativeResilientFailoverTest): lvol cef09c39's subsystem was never
created on its tertiary node.

Chain: the tertiary's restart cleared its restart phase for LVS_6 at
09:01:03, but a concurrent full-object node write resurrected the stale
phase in FDB. At lvol create (09:13:30) check_non_leader_for_operation
read the phase, returned "queue", and the registration went into the
webappapi's in-memory drain queue — which only drains on phase
transitions in the process that performs them. Nothing ever drained it;
the volume served 2/3 paths until a dual outage within FTT killed all IO.

Fixes under test:
* ``get_restart_phase`` self-heals: a non-empty phase with no owning flow
  (node not RESTARTING, cluster not IN_ACTIVATION/IN_EXPANSION) is stale —
  cleared atomically and reported as "not in restart".
* ``_set_restart_phase`` writes via atomic_update (no full-object write).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


def _node(status, phases):
    n = MagicMock(spec=StorageNode)
    n.status = status
    n.restart_phases = dict(phases)
    n.cluster_id = "cl-1"
    n.get_id = MagicMock(return_value="node-1")
    return n


def _cluster(status):
    c = MagicMock(spec=Cluster)
    c.status = status
    return c


class TestGetRestartPhaseStaleness(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(storage_node_ops, "DBController")
        self.mock_db_cls = patcher.start()
        self.addCleanup(patcher.stop)
        self.db = self.mock_db_cls.return_value

    def _setup(self, node_status, cluster_status,
               phase=StorageNode.RESTART_PHASE_POST_UNBLOCK):
        self.db.get_storage_node_by_id.return_value = _node(
            node_status, {"LVS_6": phase})
        self.db.get_cluster_by_id.return_value = _cluster(cluster_status)

    def test_phase_valid_while_node_restarting(self):
        self._setup(StorageNode.STATUS_RESTARTING, Cluster.STATUS_ACTIVE)
        self.assertEqual(
            storage_node_ops.get_restart_phase("node-1", "LVS_6"),
            StorageNode.RESTART_PHASE_POST_UNBLOCK)
        self.db.atomic_update.assert_not_called()

    def test_phase_valid_during_activation_and_expansion(self):
        for cl_status in (Cluster.STATUS_IN_ACTIVATION,
                          Cluster.STATUS_IN_EXPANSION):
            self.db.reset_mock()
            self._setup(StorageNode.STATUS_ONLINE, cl_status)
            self.assertEqual(
                storage_node_ops.get_restart_phase("node-1", "LVS_6"),
                StorageNode.RESTART_PHASE_POST_UNBLOCK)
            self.db.atomic_update.assert_not_called()

    def test_stale_phase_cleared_and_not_reported(self):
        # Incident shape: node ONLINE, cluster ACTIVE, phase non-empty.
        self._setup(StorageNode.STATUS_ONLINE, Cluster.STATUS_ACTIVE)
        self.assertEqual(
            storage_node_ops.get_restart_phase("node-1", "LVS_6"), "")
        self.db.atomic_update.assert_called_once()
        # The clear mutates only the phase entry on the fresh row.
        mutate = self.db.atomic_update.call_args[0][1]
        fresh = _node(StorageNode.STATUS_ONLINE,
                      {"LVS_6": "post_unblock", "LVS_9": "blocked"})
        mutate(fresh)
        self.assertNotIn("LVS_6", fresh.restart_phases)
        self.assertIn("LVS_9", fresh.restart_phases)

    def test_empty_phase_short_circuits(self):
        self.db.get_storage_node_by_id.return_value = _node(
            StorageNode.STATUS_ONLINE, {})
        self.assertEqual(
            storage_node_ops.get_restart_phase("node-1", "LVS_6"), "")
        self.db.get_cluster_by_id.assert_not_called()
        self.db.atomic_update.assert_not_called()

    def test_gate_returns_proceed_on_stale_phase(self):
        # End-to-end through the sync-op gate: stale phase must yield
        # "proceed", not "delay" (which would queue into the dead queue).
        self._setup(StorageNode.STATUS_ONLINE, Cluster.STATUS_ACTIVE)
        self.assertEqual(
            storage_node_ops.wait_or_delay_for_restart_gate("node-1", "LVS_6"),
            "proceed")


class TestSetRestartPhaseAtomic(unittest.TestCase):
    def test_set_and_clear_use_atomic_update(self):
        db = MagicMock()
        snode = _node(StorageNode.STATUS_RESTARTING, {})
        db.get_storage_node_by_id.return_value = snode

        storage_node_ops._set_restart_phase(
            snode, "LVS_6", StorageNode.RESTART_PHASE_BLOCKED, db)
        db.atomic_update.assert_called_once()
        mutate = db.atomic_update.call_args[0][1]
        fresh = _node(StorageNode.STATUS_RESTARTING, {})
        mutate(fresh)
        self.assertEqual(fresh.restart_phases["LVS_6"],
                         StorageNode.RESTART_PHASE_BLOCKED)

        db.reset_mock()
        snode.restart_phases = {"LVS_6": StorageNode.RESTART_PHASE_POST_UNBLOCK}
        db.get_storage_node_by_id.return_value = snode
        storage_node_ops._set_restart_phase(snode, "LVS_6", "", db)
        mutate = db.atomic_update.call_args[0][1]
        fresh = _node(StorageNode.STATUS_RESTARTING,
                      {"LVS_6": StorageNode.RESTART_PHASE_POST_UNBLOCK})
        mutate(fresh)
        self.assertNotIn("LVS_6", fresh.restart_phases)


if __name__ == "__main__":
    unittest.main()
