# coding=utf-8
"""
test_fd_dead_recovery.py — unit tests for the failure-domain parallel
restart gate (fd_dead_recovery_allowed).

Background (2026-07-17): recovery of a fully-rebooted 16-node failure
domain ran strictly serially (~3 min/node, ~48 min total) because the
2026-07-16 storm fix removed the same-FD parallel carve-out entirely.
The reinstated carve-out allows concurrent restarts ONLY when the
target's whole failure domain is dead — restarting its members in
parallel cannot reduce served availability.

Invariants covered (the acceptance contract for FD-scale recovery):

  1. Fully-offline FD in a multi-FD cluster  -> parallel allowed.
  2. Any ONLINE member in the target's FD    -> strict serial.
  3. Single/unset failure domain             -> strict serial
     (whole-cluster-down is the drained-suspension path's job).
  4. RESTARTING/IN_SHUTDOWN node OUTSIDE the target's FD -> strict serial
     (cross-domain concurrency stays forbidden).
  5. Same-FD peers already RESTARTING do NOT block (that is the point).
  6. DB read failure -> strict serial (fail closed).
"""

import unittest
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _node(uuid, fd, status):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "c1"
    n.failure_domain = fd
    n.status = status
    return n


def _db_with(nodes):
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    return db


class TestFdDeadRecoveryAllowed(unittest.TestCase):

    def test_fully_offline_fd_allows_parallel(self):
        nodes = (
            [_node(f"a{i}", 0, StorageNode.STATUS_OFFLINE) for i in range(4)]
            + [_node(f"b{i}", 1, StorageNode.STATUS_ONLINE) for i in range(4)]
        )
        self.assertTrue(
            storage_node_ops.fd_dead_recovery_allowed(_db_with(nodes), nodes[0]))

    def test_same_fd_restarting_peers_do_not_block(self):
        nodes = (
            [_node("a0", 0, StorageNode.STATUS_OFFLINE),
             _node("a1", 0, StorageNode.STATUS_RESTARTING),
             _node("a2", 0, StorageNode.STATUS_RESTARTING)]
            + [_node(f"b{i}", 1, StorageNode.STATUS_ONLINE) for i in range(3)]
        )
        self.assertTrue(
            storage_node_ops.fd_dead_recovery_allowed(_db_with(nodes), nodes[0]))

    def test_online_member_in_target_fd_stays_strict(self):
        nodes = (
            [_node("a0", 0, StorageNode.STATUS_OFFLINE),
             _node("a1", 0, StorageNode.STATUS_ONLINE)]
            + [_node(f"b{i}", 1, StorageNode.STATUS_ONLINE) for i in range(2)]
        )
        self.assertFalse(
            storage_node_ops.fd_dead_recovery_allowed(_db_with(nodes), nodes[0]))

    def test_single_failure_domain_stays_strict(self):
        nodes = [_node(f"a{i}", 0, StorageNode.STATUS_OFFLINE) for i in range(4)]
        self.assertFalse(
            storage_node_ops.fd_dead_recovery_allowed(_db_with(nodes), nodes[0]))

    def test_cross_fd_restart_in_flight_stays_strict(self):
        nodes = (
            [_node("a0", 0, StorageNode.STATUS_OFFLINE),
             _node("a1", 0, StorageNode.STATUS_OFFLINE)]
            + [_node("b0", 1, StorageNode.STATUS_RESTARTING),
               _node("b1", 1, StorageNode.STATUS_ONLINE)]
        )
        self.assertFalse(
            storage_node_ops.fd_dead_recovery_allowed(_db_with(nodes), nodes[0]))

    def test_db_error_fails_closed(self):
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.side_effect = RuntimeError("fdb down")
        self.assertFalse(
            storage_node_ops.fd_dead_recovery_allowed(
                db, _node("a0", 0, StorageNode.STATUS_OFFLINE)))


if __name__ == "__main__":
    unittest.main()
