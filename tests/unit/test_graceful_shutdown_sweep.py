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

    def _run(self, nodes):
        db = MagicMock()
        db.get_cluster_by_id = MagicMock(return_value=MagicMock())
        db.get_storage_nodes_by_cluster_id = MagicMock(return_value=nodes)
        sn = MagicMock()
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


if __name__ == "__main__":
    unittest.main()
