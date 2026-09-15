"""test_auto_repair_guards.py — auto_repair must not strand the cluster.

Two faults, both reachable from `sbctl sn auto-repair`:

1. ``safe_delete_bdev`` resolved the secondary node with an unguarded
   ``get_storage_node_by_id(primary_node.secondary_node_id)``. That field is
   "" on a node with no secondary (ha_type single), and a blank id raises
   KeyError by contract (tests/unit/test_blank_id_lookups.py pins the getter
   side). Every other caller in the codebase guards it -- including the
   sibling lookup in this same module -- so this was the lone instance. It
   fired on the FIRST orphan, meaning the repair deleted nothing and exited
   as a traceback.

2. The window between "set IN_ACTIVATION" and "set ACTIVE" had no
   try/finally, so any raise in a delete loop left the cluster parked in
   IN_ACTIVATION with no path back. The restore also wrote ACTIVE
   unconditionally, although the entry guard admits DEGRADED -- so a repair
   run on a degraded cluster silently promoted it to healthy.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.cluster import Cluster


def _node(node_id, secondary_node_id="", lvstore="LVS_1"):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.secondary_node_id = secondary_node_id
    node.lvstore = lvstore
    return node


class TestSafeDeleteBdevWithoutSecondary(unittest.TestCase):
    """A node with no secondary must delete on the primary and stop there."""

    @staticmethod
    def _run(primary, lookups):
        """Run safe_delete_bdev with a DBController whose by-id getter raises
        KeyError for a blank id, exactly as the real one does."""
        def _get(node_id):
            if not node_id:
                raise KeyError(node_id)
            return lookups[node_id]

        db = MagicMock()
        db.get_storage_node_by_id.side_effect = _get

        # delete_lvol -> (True, None); delete-status 0 == "already gone/done"
        primary.rpc_client.return_value.delete_lvol.return_value = (True, None)
        primary.rpc_client.return_value.bdev_lvol_get_lvol_delete_status.return_value = 0

        with patch.object(storage_node_ops, "DBController", return_value=db), \
                patch.object(storage_node_ops.time, "sleep"):
            return storage_node_ops.safe_delete_bdev("SNAP_14", "node-a")

    def test_blank_secondary_does_not_raise(self):
        primary = _node("node-a", secondary_node_id="")
        result = self._run(primary, {"node-a": primary})

        self.assertTrue(result, "delete on a node with no secondary must succeed")

    def test_primary_still_receives_the_qualified_name(self):
        primary = _node("node-a", secondary_node_id="", lvstore="LVS_1")
        self._run(primary, {"node-a": primary})

        sent = [c.args[0] for c in
                primary.rpc_client.return_value.delete_lvol.call_args_list]
        self.assertTrue(sent, "no delete was issued at all")
        for name in sent:
            self.assertEqual(name, "LVS_1/SNAP_14",
                             "the RPC takes <lvstore>/<name>, not the bare name")

    def test_secondary_leg_still_runs_when_there_is_one(self):
        primary = _node("node-a", secondary_node_id="node-b")
        secondary = _node("node-b")
        secondary.rpc_client.return_value.delete_lvol.return_value = (True, None)

        result = self._run(primary, {"node-a": primary, "node-b": secondary})

        self.assertTrue(result)
        secondary.rpc_client.return_value.delete_lvol.assert_called_once()
        self.assertEqual(
            secondary.rpc_client.return_value.delete_lvol.call_args.args[0],
            "LVS_1/SNAP_14",
            "the secondary is addressed with the primary's lvstore name")


class TestAutoRepairRestoresClusterStatus(unittest.TestCase):
    """The IN_ACTIVATION window must always be closed."""

    def _run_auto_repair(self, cluster_status, delete_raises=False, **kwargs):
        snode = _node("node-a", secondary_node_id="node-b")
        snode.status = storage_node_ops.StorageNode.STATUS_ONLINE
        snode.cluster_id = "cl-1"
        snode.rpc_client.return_value.bdev_lvol_get_lvstores.return_value = [
            {"uuid": "lvs-uuid"}]
        # One blob in SPDK that mgmt knows nothing about -> the delete path.
        snode.rpc_client.return_value.bdev_lvs_dump_tree.return_value = {
            "lvols": [{"blobid": 4294967344, "uuid": "c32876e5", "name": "SNAP_14", "ref": 2}]}

        cluster = MagicMock()
        cluster.get_id.return_value = "cl-1"
        cluster.status = cluster_status

        db = MagicMock()
        db.get_storage_node_by_id.return_value = snode
        db.get_cluster_by_id.return_value = cluster

        delete = MagicMock(side_effect=RuntimeError("rpc blew up")
                           if delete_raises else None)

        with patch.object(storage_node_ops, "DBController", return_value=db), \
                patch.object(storage_node_ops, "cluster_ops") as cluster_ops_mock, \
                patch.object(storage_node_ops, "safe_delete_bdev", delete), \
                patch.object(storage_node_ops.lvol_controller, "list_by_node",
                             return_value=[]), \
                patch.object(storage_node_ops.snapshot_controller, "list_snapshots",
                             return_value=[]), \
                patch.object(storage_node_ops.time, "sleep"):
            try:
                storage_node_ops.auto_repair("node-a", **kwargs)
            except RuntimeError:
                pass  # finding 2: the raise must not skip the restore
        statuses = [c.args[1] for c in cluster_ops_mock.set_cluster_status.call_args_list]
        return statuses, delete

    def test_status_is_restored_when_a_delete_raises(self):
        statuses, delete = self._run_auto_repair(Cluster.STATUS_ACTIVE, delete_raises=True)

        delete.assert_called()
        self.assertEqual(statuses[0], Cluster.STATUS_IN_ACTIVATION)
        self.assertEqual(statuses[-1], Cluster.STATUS_ACTIVE,
                         "an exception mid-repair must not leave the cluster "
                         "parked in IN_ACTIVATION")

    def test_degraded_cluster_is_not_promoted_to_active(self):
        statuses, _ = self._run_auto_repair(Cluster.STATUS_DEGRADED)

        self.assertEqual(statuses[0], Cluster.STATUS_IN_ACTIVATION)
        self.assertEqual(statuses[-1], Cluster.STATUS_DEGRADED,
                         "running a repair must not mark a degraded cluster healthy")

    def test_validate_only_never_touches_cluster_status(self):
        statuses, delete = self._run_auto_repair(Cluster.STATUS_ACTIVE, validate_only=True)

        self.assertEqual(statuses, [],
                         "a dry run must not move the cluster in or out of activation")
        delete.assert_not_called()


if __name__ == "__main__":
    unittest.main()
