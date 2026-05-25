# coding=utf-8
"""Unit tests for the clone namespace auto-grouping race fixes.

Two fixes covered:

1. ``add_lvol_on_node`` is self-healing when the namespaced target
   subsystem (chosen earlier by ``get_next_available_subsystem_on_node``)
   has been torn down by a concurrent lvol-delete before the
   ``nvmf_subsystem_add_ns`` RPC fires. Previously the RPC failed with
   ``Unable to find subsystem with NQN ...`` and the lvol was lost; now
   the lvol is downgraded to its own subsystem and creation proceeds.

2. Any post-``_create_bdev_stack`` failure inside ``add_lvol_on_node``
   now rolls back the bdev stack via ``_remove_bdev_stack``. Previously
   a failed listener-add or failed ``nvmf_subsystem_add_ns`` returned
   without deleting the orphaned ``bdev_lvol_clone`` blob, which then
   blocked the parent snapshot's delete with
   ``vbdev_lvol_destroy: ... cannot destroy: has N clones``.

All external dependencies (DBController, RPC, SPDK) are mocked.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode


def _cluster():
    c = Cluster()
    c.uuid = "cluster-1"
    c.nqn = "nqn.test:cluster-1"
    c.status = Cluster.STATUS_ACTIVE
    return c


def _node():
    n = StorageNode()
    n.uuid = "node-1"
    n.cluster_id = "cluster-1"
    n.status = StorageNode.STATUS_ONLINE
    n.active_tcp = False
    n.max_lvol = 100
    nic = MagicMock()
    nic.ip4_address = "10.0.0.1"
    nic.trtype = "TCP"
    n.data_nics = [nic]
    n.get_lvol_subsys_port = MagicMock(return_value=4420)
    return n


def _lvol_for_add(uuid, namespace="", nqn=None):
    lv = LVol()
    lv.uuid = uuid
    lv.lvol_name = f"VOL_{uuid}"
    lv.lvol_bdev = f"CLN_{uuid}"
    lv.top_bdev = f"LVS_100/{lv.lvol_bdev}"
    lv.lvs_name = "LVS_100"
    lv.node_id = "node-1"
    lv.cluster_id = "cluster-1"
    lv.snapshot_name = "LVS_100/SNAP_parent"
    lv.guid = "0123456789abcdef"
    lv.ha_type = "single"
    lv.nqn = nqn or ("nqn.test:cluster-1:lvol:" + uuid)
    lv.namespace = namespace
    lv.allowed_hosts = []
    lv.fabric = "tcp"
    lv.max_namespace_per_subsys = 32
    lv.bdev_stack = [{
        "type": "bdev_lvol_clone",
        "name": lv.top_bdev,
        "params": {
            "snapshot_name": lv.snapshot_name,
            "clone_name": lv.lvol_bdev,
        },
    }]
    return lv


def _rpc_client():
    mock = MagicMock()
    mock.lvol_clone.return_value = {"uuid": "lvol-bdev-uuid"}
    # Happy default: target subsystem exists.
    mock.subsystem_list.return_value = [{"namespaces": [{"uuid": "x"}]}]
    mock.subsystem_create.return_value = True
    mock.nvmf_subsystem_add_listener.return_value = (True, None)
    mock.nvmf_subsystem_add_ns.return_value = 7
    mock.ultra21_util_get_malloc_stats.return_value = {}
    mock.get_bdevs.return_value = [
        {"uuid": "lvol-bdev-uuid",
         "driver_specific": {"lvol": {"blobid": 12345}}},
    ]
    # _remove_bdev_stack's bdev_lvol_clone branch calls this.
    mock.delete_lvol.return_value = (True, None)
    return mock


# ---------------------------------------------------------------------------
# Fix 1: self-healing namespaced-attach
# ---------------------------------------------------------------------------

class TestNamespacedAttachRace(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_namespaced_attach_happy_path(self, mock_db_cls):
        """When the target subsystem still exists on the node, the attach
        path runs unchanged: subsystem_create is skipped, the original NQN
        is used, and lvol.namespace is preserved."""
        from simplyblock_core.controllers import lvol_controller

        original_nqn = "nqn.test:cluster-1:lvol:OTHER"
        lvol = _lvol_for_add("u1", namespace="ex-host-lvol-id",
                             nqn=original_nqn)
        node = _node()
        rpc = _rpc_client()
        rpc.subsystem_list.return_value = [{"namespaces": [{"uuid": "y"}]}]
        node.rpc_client = MagicMock(return_value=rpc)
        mock_db_cls.return_value = MagicMock()

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertIsNone(err)
        self.assertEqual(bdev["uuid"], "lvol-bdev-uuid")
        rpc.subsystem_list.assert_called_with(original_nqn)
        rpc.subsystem_create.assert_not_called()
        rpc.nvmf_subsystem_add_listener.assert_not_called()
        rpc.nvmf_subsystem_add_ns.assert_called_once()
        self.assertEqual(rpc.nvmf_subsystem_add_ns.call_args[0][0],
                         original_nqn)
        # lvol.namespace and lvol.nqn preserved
        self.assertEqual(lvol.namespace, "ex-host-lvol-id")
        self.assertEqual(lvol.nqn, original_nqn)

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_namespaced_attach_subsystem_missing_falls_back_to_standalone(
            self, mock_db_cls):
        """When the target subsystem was torn down between the DB lookup
        and the add_ns RPC, downgrade the lvol to its own subsystem and
        run the standalone subsystem_create+listener+add_ns path."""
        from simplyblock_core.controllers import lvol_controller

        original_nqn = "nqn.test:cluster-1:lvol:OTHER"
        lvol = _lvol_for_add("u2", namespace="ex-host-lvol-id",
                             nqn=original_nqn)

        # Capture lvol state at each write_to_db call so we can prove the
        # downgrade was persisted BEFORE the standalone block ran.
        write_snapshots = []
        lvol.write_to_db = MagicMock(side_effect=lambda kv: write_snapshots.append(
            {"nqn": lvol.nqn, "namespace": lvol.namespace, "ns_id": lvol.ns_id}))

        node = _node()
        rpc = _rpc_client()
        rpc.subsystem_list.return_value = []  # subsystem gone
        node.rpc_client = MagicMock(return_value=rpc)

        db = MagicMock()
        db.get_cluster_by_id.return_value = _cluster()
        db.kv_store = MagicMock()
        mock_db_cls.return_value = db

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertIsNone(err)
        self.assertEqual(bdev["uuid"], "lvol-bdev-uuid")

        # Downgrade happened
        self.assertEqual(lvol.namespace, "")
        self.assertEqual(lvol.nqn, "nqn.test:cluster-1:lvol:u2")
        self.assertEqual(lvol.ns_id, 7)  # set later by add_ns

        # Standalone path ran against the rewritten NQN
        rpc.subsystem_create.assert_called_once()
        self.assertEqual(rpc.subsystem_create.call_args[0][0],
                         "nqn.test:cluster-1:lvol:u2")
        rpc.nvmf_subsystem_add_listener.assert_called()
        self.assertEqual(rpc.nvmf_subsystem_add_ns.call_args[0][0],
                         "nqn.test:cluster-1:lvol:u2")

        # The downgraded state (namespace="", new NQN) was persisted to
        # the DB at least once with namespace cleared.
        self.assertTrue(any(w["namespace"] == "" and
                            w["nqn"].endswith(":lvol:u2")
                            for w in write_snapshots),
                        f"downgrade not persisted; writes={write_snapshots}")

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_standalone_lvol_unchanged(self, mock_db_cls):
        """A lvol with empty namespace already takes the standalone path
        and must continue to do so. subsystem_list is NOT consulted; the
        resolver short-circuits."""
        from simplyblock_core.controllers import lvol_controller

        lvol = _lvol_for_add("u5")  # namespace="" by default
        node = _node()
        rpc = _rpc_client()
        node.rpc_client = MagicMock(return_value=rpc)
        mock_db_cls.return_value = MagicMock()

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertIsNone(err)
        rpc.subsystem_list.assert_not_called()
        rpc.subsystem_create.assert_called_once()
        rpc.nvmf_subsystem_add_ns.assert_called_once()


# ---------------------------------------------------------------------------
# Fix 2: rollback bdev stack on post-bdev-stack failure
# ---------------------------------------------------------------------------

class TestPostBdevStackRollback(unittest.TestCase):

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_add_ns_failure_rolls_back_bdev_stack(self, mock_db_cls):
        """If nvmf_subsystem_add_ns fails after _create_bdev_stack
        succeeded, the orphan bdev_lvol_clone blob must be removed —
        otherwise the parent snapshot's delete will EBUSY with
        ``has N clones``."""
        from simplyblock_core.controllers import lvol_controller

        lvol = _lvol_for_add("u3")  # standalone path
        node = _node()
        rpc = _rpc_client()
        rpc.nvmf_subsystem_add_ns.return_value = False
        node.rpc_client = MagicMock(return_value=rpc)
        mock_db_cls.return_value = MagicMock()

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertFalse(bdev)
        self.assertIn("Failed to add bdev to subsystem", err)
        # _remove_bdev_stack's bdev_lvol_clone branch calls delete_lvol.
        rpc.delete_lvol.assert_called()
        # The rollback targeted the right blob (top_bdev).
        self.assertEqual(rpc.delete_lvol.call_args[0][0], lvol.top_bdev)

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_listener_failure_rolls_back_bdev_stack(self, mock_db_cls):
        """A listener-add failure (non -32602) after _create_bdev_stack
        must also roll back the orphan blob."""
        from simplyblock_core.controllers import lvol_controller

        lvol = _lvol_for_add("u4")  # standalone path
        node = _node()
        rpc = _rpc_client()
        rpc.nvmf_subsystem_add_listener.return_value = (False, {"code": -32000})
        node.rpc_client = MagicMock(return_value=rpc)
        mock_db_cls.return_value = MagicMock()

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertFalse(bdev)
        self.assertIn("Failed to create listener", err)
        # add_ns was never reached.
        rpc.nvmf_subsystem_add_ns.assert_not_called()
        # Rollback fired.
        rpc.delete_lvol.assert_called()
        self.assertEqual(rpc.delete_lvol.call_args[0][0], lvol.top_bdev)

    @patch("simplyblock_core.controllers.lvol_controller.DBController")
    def test_listener_already_exists_does_not_roll_back(self, mock_db_cls):
        """A -32602 ``listener already exists`` is a benign warning —
        the existing code logs and continues, and rollback must NOT
        fire (otherwise we'd delete a perfectly good clone)."""
        from simplyblock_core.controllers import lvol_controller

        lvol = _lvol_for_add("u6")  # standalone path
        node = _node()
        rpc = _rpc_client()
        rpc.nvmf_subsystem_add_listener.return_value = (False, {"code": -32602})
        node.rpc_client = MagicMock(return_value=rpc)
        mock_db_cls.return_value = MagicMock()

        bdev, err = lvol_controller.add_lvol_on_node(lvol, node)

        self.assertIsNone(err)
        # add_ns was reached and succeeded.
        rpc.nvmf_subsystem_add_ns.assert_called_once()
        # No rollback.
        rpc.delete_lvol.assert_not_called()


if __name__ == "__main__":
    unittest.main()
