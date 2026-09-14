"""
Regression test: add_device_failed_mig_task() / add_new_device_mig_task()
must not schedule a rebalancing task against a node that cannot possibly run
it -- one that's mid-removal (SPDK already shut down) or already removed.

Before this fix, only STATUS_REMOVED was excluded despite the code's own
comment describing STATUS_IN_REMOVAL as needing the same treatment ("a
migration task targeting their distribs can never run and would stall the
node-removal completion check forever"). In practice this meant: when a
node's own device fails as part of node removal (see storage_node_ops.py's
_decommission_node_devices), a failed_device_migration task got scheduled
against that same (now offline) node, which retried "node is not online"
forever and permanently tripped migration_controller._can_add_lvol_migration()'s
cluster-wide "rebalancing in progress" gate -- deadlocking node removal's own
lvol-migration step indefinitely (observed live 2026-09-13).
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import tasks_controller as tc
from simplyblock_core.models.storage_node import StorageNode


def _device(cluster_id="cluster-1"):
    dev = MagicMock()
    dev.cluster_id = cluster_id
    dev.get_id = MagicMock(return_value="dev-1")
    return dev


def _node(node_id, status):
    n = MagicMock(spec=StorageNode)
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.lvstore_stack = [{"type": "bdev_distr", "name": f"distrib_{node_id}"}]
    return n


def _run(fn, nodes):
    mock_db = MagicMock()
    mock_db.get_storage_device_by_id.return_value = _device()
    mock_db.get_storage_nodes_by_cluster_id.return_value = nodes
    with patch.object(tc, "db", mock_db), \
         patch.object(tc, "_add_task") as add_task:
        fn("dev-1")
    return add_task


_ALL_STATUS_NODES = [
    _node("n-online", StorageNode.STATUS_ONLINE),
    _node("n-removed", StorageNode.STATUS_REMOVED),
    _node("n-in-removal", StorageNode.STATUS_IN_REMOVAL),
    _node("n-pending-migration", StorageNode.STATUS_PENDING_MIGRATION),
]


class TestDeviceMigTasksSkipUnreachableNodes(unittest.TestCase):

    def test_failed_dev_mig_only_targets_the_online_node(self):
        add_task = _run(tc.add_device_failed_mig_task, _ALL_STATUS_NODES)
        targeted = {call.args[2] for call in add_task.call_args_list}
        self.assertEqual(targeted, {"n-online"})

    def test_new_dev_mig_only_targets_the_online_node(self):
        add_task = _run(tc.add_new_device_mig_task, _ALL_STATUS_NODES)
        targeted = {call.args[2] for call in add_task.call_args_list}
        self.assertEqual(targeted, {"n-online"})


if __name__ == "__main__":
    unittest.main()
