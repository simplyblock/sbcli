# coding=utf-8
"""
``cluster_ops.cluster_grace_shutdown`` must not act on departed nodes.

Found live 2026-09-03. The sweep enumerated every node record with no status
filter at all and force-shut-down each one. ``shutdown_storage_node`` drives
in_shutdown -> offline, so nodes that had been REMOVED came back as plain
offline members and had to be repaired by hand.

That is not cosmetic: ``failure_domain_host_map`` skips only STATUS_REMOVED,
so the resurrected records immediately start occupying failure-domain host
slots again -- the cluster went from 8 hosts at 2/2/2/2 to 12 at 3/3/3/3 --
and a later activation or startup then acts on nodes whose devices are
already failed_and_migrated and which own no lvstore.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, status=StorageNode.STATUS_ONLINE):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = "c1"
    return n


class TestGracefulShutdownSkipsDepartedNodes(unittest.TestCase):

    def _run(self, nodes, resurrect=()):
        """Drive the sweep. A real shutdown drives the node to OFFLINE, so the
        mock does too -- otherwise the settle check would see every node as a
        straggler. ``resurrect`` names nodes that come back ONLINE behind the
        sweep (the live 2026-09-03 failure) and are only stopped for good on
        the second attempt."""
        by_id = {n.get_id(): n for n in nodes}
        pending = dict.fromkeys(resurrect, True)

        def _shutdown(node_id, **_kw):
            node = by_id[node_id]
            if pending.pop(node_id, False):
                node.status = StorageNode.STATUS_ONLINE
            else:
                node.status = StorageNode.STATUS_OFFLINE
            return True

        db = MagicMock()
        db.get_cluster_by_id = MagicMock(return_value=MagicMock())
        db.get_storage_nodes_by_cluster_id = MagicMock(return_value=nodes)
        sn = MagicMock()
        sn.shutdown_storage_node = MagicMock(side_effect=_shutdown)
        with patch.object(cluster_ops, "db_controller", db), \
                patch.object(cluster_ops, "storage_node_ops", sn):
            cluster_ops.cluster_grace_shutdown("c1")
        swept = [c.args[0] for c in sn.shutdown_storage_node.call_args_list]
        suspended = [c.args[0] for c in sn.suspend_storage_node.call_args_list]
        return swept, suspended

    def test_a_removed_node_is_never_shut_down(self):
        # The live failure: one graceful-shutdown turned four removed nodes
        # back into offline members.
        swept, suspended = self._run([
            _node("live", StorageNode.STATUS_ONLINE),
            _node("gone", StorageNode.STATUS_REMOVED),
        ])
        self.assertEqual(swept, ["live"])
        self.assertEqual(suspended, ["live"])

    def test_a_node_mid_removal_is_left_to_its_orchestrator(self):
        swept, _ = self._run([
            _node("live", StorageNode.STATUS_ONLINE),
            _node("leaving", StorageNode.STATUS_IN_REMOVAL),
        ])
        self.assertEqual(swept, ["live"])

    def test_everything_else_is_still_swept(self):
        # The point of the command: offline/unreachable/restarting members
        # and nodes only queued for removal must all be stopped. In
        # particular PENDING_REMOVAL is still up and serving, so a
        # full-cluster shutdown has to stop it like any other member.
        nodes = [
            _node("online", StorageNode.STATUS_ONLINE),
            _node("offline", StorageNode.STATUS_OFFLINE),
            _node("unreachable", StorageNode.STATUS_UNREACHABLE),
            _node("restarting", StorageNode.STATUS_RESTARTING),
            _node("pending", StorageNode.STATUS_PENDING_REMOVAL),
        ]
        swept, _ = self._run(nodes)
        self.assertEqual(
            swept, ["online", "offline", "unreachable", "restarting", "pending"])


class TestGracefulShutdownSettles(unittest.TestCase):
    """The sweep is serial, so a node it already passed can come back behind
    it. Live 2026-09-03: queued restart rows put s7457 and zdgtb back ONLINE
    seconds after the sweep shut them down, and the command still returned as
    though the cluster were down."""

    _run = TestGracefulShutdownSkipsDepartedNodes._run

    def test_a_node_that_comes_back_is_shut_down_again(self):
        swept, _ = self._run(
            [_node("a"), _node("b")], resurrect=("a",))
        self.assertEqual(swept, ["a", "b", "a"])

    def test_no_second_pass_when_everything_settled(self):
        swept, _ = self._run([_node("a"), _node("b")])
        self.assertEqual(swept, ["a", "b"])

    def test_a_removed_node_is_not_treated_as_a_straggler(self):
        # It is never OFFLINE, so an unfiltered settle check would keep
        # trying to shut it down -- reintroducing the resurrection bug.
        swept, _ = self._run(
            [_node("live"), _node("gone", StorageNode.STATUS_REMOVED)])
        self.assertEqual(swept, ["live"])


if __name__ == "__main__":
    unittest.main()
