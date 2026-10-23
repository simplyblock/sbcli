"""Service class reserved for cross-cluster snapshot replication.

Wire layout (3 LBA bits): 0 = Default, 1 = internal metadata/migration,
lvol_priority_class k at k + 1. Class id 6 (wire 7) is reserved for replication
traffic, so users get Default plus class ids 1..5.
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.controllers import qos_controller
from simplyblock_core.services import snapshot_replication


def _cls(class_id, weight=10, name=None):
    return SimpleNamespace(class_id=class_id, weight=weight,
                           class_name=name or ("Default" if class_id == 0 else f"c{class_id}"))


def _db(classes, qos_set=None, repl_weight=10, status="unready", lvols=()):
    db = MagicMock()
    db.get_qos.side_effect = lambda *_: sorted(classes, key=lambda c: c.class_id)
    cluster = MagicMock()
    cluster.is_qos_set.return_value = (any(c.class_id for c in classes)
                                       if qos_set is None else qos_set)
    cluster.status = status
    cluster.qos_replication_weight = repl_weight
    db.get_cluster_by_id.return_value = cluster
    db.get_lvols.return_value = list(lvols)
    return db


class TestWireClass(unittest.TestCase):

    def test_user_class_shifts_by_one_when_qos_is_set(self):
        with patch.object(qos_controller, "db", _db([_cls(0), _cls(1), _cls(2)])):
            self.assertEqual(qos_controller.wire_priority_class("c", 0), 0)
            self.assertEqual(qos_controller.wire_priority_class("c", 2), 3)
            self.assertEqual(qos_controller.wire_priority_class(
                "c", constants.QOS_REPLICATION_CLASS_ID), constants.QOS_REPLICATION_WIRE_CLASS)

    def test_no_qos_means_default_for_user_classes(self):
        with patch.object(qos_controller, "db", _db([_cls(0)])):
            self.assertEqual(qos_controller.wire_priority_class("c", 2), 0)

    def test_replication_class_does_not_need_user_classes(self):
        with patch.object(qos_controller, "db", _db([])):
            self.assertEqual(qos_controller.wire_priority_class(
                "c", constants.QOS_REPLICATION_CLASS_ID), constants.QOS_REPLICATION_WIRE_CLASS)

    def test_replication_class_is_the_top_wire_class(self):
        self.assertEqual(constants.QOS_REPLICATION_WIRE_CLASS, 7)
        self.assertLess(constants.QOS_MAX_USER_CLASS_ID, constants.QOS_REPLICATION_CLASS_ID)


class TestReplicationClassAvailable(unittest.TestCase):

    def test_on_without_any_class(self):
        with patch.object(qos_controller, "db", _db([])):
            self.assertTrue(qos_controller.replication_class_available("c"))

    def test_available_with_user_classes(self):
        with patch.object(qos_controller, "db", _db([_cls(0), _cls(1)])):
            self.assertTrue(qos_controller.replication_class_available("c"))

    def test_legacy_cluster_with_six_user_classes_keeps_its_class(self):
        legacy = [_cls(i) for i in range(7)]
        with patch.object(qos_controller, "db", _db(legacy)):
            self.assertFalse(qos_controller.replication_class_available("c"))


class TestWeights(unittest.TestCase):

    def test_layout_and_sum(self):
        classes = [_cls(2, 30), _cls(0, 50), _cls(1, 20)]   # unordered on purpose
        with patch.object(qos_controller, "db", _db(classes)):
            w = qos_controller.get_qos_weights_list("c")
        self.assertEqual(len(w), 8)
        self.assertEqual(sum(w), 100)
        self.assertEqual(w[1], constants.qos_class_meta_and_migration_weight_percent)
        self.assertEqual(w[constants.QOS_REPLICATION_WIRE_CLASS],
                         constants.qos_class_replication_weight_percent)
        user = 100 - w[1] - w[7]
        self.assertEqual(w[2], int(20 / 100 * user))       # class id 1 -> wire 2
        self.assertEqual(w[3], int(30 / 100 * user))       # class id 2 -> wire 3
        self.assertEqual(w[4:7], [0, 0, 0])

    def test_configured_replication_share(self):
        with patch.object(qos_controller, "db", _db([_cls(0, 50), _cls(1, 50)], repl_weight=30)):
            w = qos_controller.get_qos_weights_list("c")
        self.assertEqual(w[7], 30)
        self.assertEqual(sum(w), 100)

    def test_no_classes_still_gives_replication_its_share(self):
        with patch.object(qos_controller, "db", _db([])):
            w = qos_controller.get_qos_weights_list("c")
        self.assertEqual(w, [65, 25, 0, 0, 0, 0, 0, 10])

    def test_legacy_class_six_keeps_wire_seven(self):
        legacy = [_cls(i, 10) for i in range(7)]
        with patch.object(qos_controller, "db", _db(legacy)):
            w = qos_controller.get_qos_weights_list("c")
        self.assertEqual(sum(w), 100)
        self.assertEqual(w[7], int(10 / 70 * (100 - w[1])))


class TestAddClassCap(unittest.TestCase):

    def test_rejects_a_sixth_user_class(self):
        full = [_cls(i) for i in range(constants.QOS_MAX_USER_CLASS_ID + 1)]
        db = _db(full)
        with patch.object(qos_controller, "db", db), \
                patch.object(qos_controller.Cluster, "STATUS_UNREADY", "unready"):
            self.assertFalse(qos_controller.add_class("extra", 5, "c"))

    def test_next_index_never_hands_out_the_reserved_id(self):
        full = [_cls(i) for i in range(constants.QOS_MAX_USER_CLASS_ID + 1)]
        with patch.object(qos_controller, "db", _db(full)):
            with self.assertRaises(ValueError):
                qos_controller.get_next_index_id("c")


class TestClassOnCluster(unittest.TestCase):

    def test_kept_where_defined_else_default(self):
        with patch.object(qos_controller, "db", _db([_cls(0), _cls(2)])):
            self.assertEqual(qos_controller.class_on_cluster("t", 2), 2)
            self.assertEqual(qos_controller.class_on_cluster("t", 3), 0)
            self.assertEqual(qos_controller.class_on_cluster("t", 0), 0)


class TestResolveClass(unittest.TestCase):

    def test_by_name_or_id(self):
        with patch.object(qos_controller, "db", _db([_cls(0), _cls(2, name="gold")])):
            self.assertEqual(qos_controller.resolve_class("c", "gold"), 2)
            self.assertEqual(qos_controller.resolve_class("c", 2), 2)
            self.assertEqual(qos_controller.resolve_class("c", "2"), 2)
            self.assertEqual(qos_controller.resolve_class("c", "Default"), 0)
            self.assertEqual(qos_controller.resolve_class("c", 0), 0)
            with self.assertRaises(ValueError):
                qos_controller.resolve_class("c", "silver")

    def test_reserved_class_is_not_for_users(self):
        with patch.object(qos_controller, "db", _db([_cls(0)])):
            with self.assertRaises(ValueError):
                qos_controller.resolve_class("c", constants.QOS_REPLICATION_CLASS_ID)


class TestSetConfig(unittest.TestCase):

    def _run(self, db, *args):
        written = []
        with patch.object(qos_controller, "db", db),                 patch.object(qos_controller.QOSClass, "write_to_db",
                             lambda self, *_: written.append(self)),                 patch.object(qos_controller, "push_qos_weights", return_value=[]) as push:
            result = qos_controller.set_config("c", *args)
        return result, written, push

    def test_creates_default_and_classes_with_lowest_free_ids(self):
        classes = []
        db = _db(classes)
        result, written, push = self._run(db, 50, [{"name": "gold", "weight": 30},
                                                   {"name": "bronze", "weight": 5}])
        self.assertEqual(result, [])
        by_name = {c.class_name: c for c in written}
        self.assertEqual(by_name["Default"].class_id, 0)
        self.assertEqual(by_name["Default"].weight, 50)
        self.assertEqual(by_name["gold"].weight, 30)
        push.assert_not_called()               # unready: activation pushes

    def test_existing_class_keeps_its_id(self):
        gold = _cls(3, 10, "gold")
        gold.write_to_db = MagicMock()
        default = _cls(0, 10)
        default.write_to_db = MagicMock()
        db = _db([default, gold], status="active")
        _, _, push = self._run(db, 10, [{"name": "gold", "weight": 99}])
        self.assertEqual(gold.class_id, 3)
        self.assertEqual(gold.weight, 99)
        push.assert_called_once_with("c")      # active: pushed live

    def test_removing_a_used_class_is_refused(self):
        gold = _cls(1, 10, "gold")
        gold.remove = MagicMock()
        db = _db([_cls(0), gold], lvols=[SimpleNamespace(lvol_priority_class=1)])
        with patch.object(qos_controller, "db", db):
            with self.assertRaises(RuntimeError):
                qos_controller.set_config("c", 10, [])
        gold.remove.assert_not_called()

    def test_removing_an_unused_class(self):
        gold = _cls(1, 10, "gold")
        gold.remove = MagicMock()
        default = _cls(0)
        default.write_to_db = MagicMock()
        db = _db([default, gold])
        self._run(db, 10, [])
        gold.remove.assert_called_once()

    def test_replication_weight_is_stored(self):
        db = _db([])
        self._run(db, 10, [], 20)
        db.atomic_update.assert_called_once()

    def test_validation(self):
        db = _db([])
        for args in [(0, []), (10, [{"name": "Default", "weight": 1}]),
                     (10, [{"name": "a", "weight": 1}, {"name": "a", "weight": 2}]),
                     (10, [{"name": f"c{i}", "weight": 1} for i in range(6)]),
                     (10, [], 80)]:
            with patch.object(qos_controller, "db", db), self.assertRaises(ValueError):
                qos_controller.set_config("c", *args)


class TestTransferClass(unittest.TestCase):

    def _node(self, status="online"):
        node = MagicMock()
        node.status = status
        return node

    def test_set_only_on_online_members(self):
        up, down = self._node(), self._node("offline")
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda i: {"a": up, "b": down}[i]
        with patch.object(snapshot_replication, "db", db):
            snapshot_replication._set_transfer_class(["a", "b"], "lvs/SNAP", 7, "x")
        up.rpc_client.return_value.bdev_lvol_set_priority_class.assert_called_once_with("lvs/SNAP", 7)
        down.rpc_client.assert_not_called()

    def test_rpc_failure_is_not_fatal(self):
        up = self._node()
        up.rpc_client.return_value.bdev_lvol_set_priority_class.side_effect = RuntimeError("boom")
        db = MagicMock()
        db.get_storage_node_by_id.return_value = up
        with patch.object(snapshot_replication, "db", db), \
                self.assertLogs(snapshot_replication.logger, "WARNING"):
            snapshot_replication._set_transfer_class(["a"], "lvs/SNAP", 7, "x")

    def test_lookup_failure_falls_back_to_default(self):
        with patch.object(qos_controller, "replication_class_available",
                          side_effect=RuntimeError("fdb")), \
                self.assertLogs(snapshot_replication.logger, "WARNING"):
            self.assertFalse(snapshot_replication._replication_class_on("c"))


class TestTransferReadClass(unittest.TestCase):
    """bdev_lvol_transfer reads its source in its own class, so the source
    snapshot's class, which client reads through it use, is never touched."""

    def _client(self, side_effect):
        from simplyblock_core.rpc_client import RPCClient
        client = RPCClient.__new__(RPCClient)
        client._request = MagicMock(side_effect=side_effect)
        return client

    def test_class_is_sent(self):
        client = self._client([True])
        client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", lvol_id=3, priority_class=7)
        self.assertEqual(client._request.call_args.kwargs["priority_class"], 7)

    def test_no_class_sends_no_parameter(self):
        client = self._client([True])
        client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", lvol_id=3)
        self.assertNotIn("priority_class", client._request.call_args.kwargs)

    def test_older_spdk_is_retried_without_the_class(self):
        from simplyblock_core.rpc_client import RPCRemoteError
        client = self._client([RPCRemoteError("spdk_json_decode_object failed", -32603), True])
        self.assertTrue(client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", priority_class=7))
        self.assertEqual(client._request.call_count, 2)
        self.assertNotIn("priority_class", client._request.call_args.kwargs)

    def test_other_errors_are_not_masked(self):
        from simplyblock_core.rpc_client import RPCRemoteError
        client = self._client([RPCRemoteError("No such device", -19)])
        with self.assertRaises(RPCRemoteError):
            client.bdev_lvol_transfer("lvs/SNAP", 0, 16, "hub", "replicate", priority_class=7)


if __name__ == "__main__":
    unittest.main()
