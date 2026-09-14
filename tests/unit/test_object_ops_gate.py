"""test_object_ops_gate.py — 'cluster op-stop' refuses object lifecycle work.

op-stop must cover creation, deletion AND modification of volumes, snapshots,
clones and pools — a resize or a QoS change is as much an object operation as a
create. It must not touch read paths, and it must not touch what the cluster
does to itself (restarts, migrations, rebalancing), so a stopped cluster stays
observable and still recovers from faults.

The gate lives in the controllers because that is the one layer both the CLI
(via clibase) and the v2 API funnel through.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import cluster_ops
from simplyblock_core.controllers import (
    lvol_controller, ops_gate, pool_controller, snapshot_controller,
)
from simplyblock_core.models.cluster import Cluster


class TestGateItself(unittest.TestCase):

    def _cluster(self, stopped):
        cluster = MagicMock()
        cluster.object_ops_stopped = stopped
        return cluster

    def test_allowed_when_running(self):
        with patch.object(ops_gate, "DBController") as db:
            db.return_value.get_cluster_by_id.return_value = self._cluster(False)
            ops_gate.assert_object_ops_allowed("volume create", cluster_id="cl-1")

    def test_refused_when_stopped(self):
        with patch.object(ops_gate, "DBController") as db:
            db.return_value.get_cluster_by_id.return_value = self._cluster(True)
            with self.assertRaises(ValueError) as caught:
                ops_gate.assert_object_ops_allowed("volume create", cluster_id="cl-1")
        message = str(caught.exception)
        self.assertIn("volume create", message)
        self.assertIn("op-start", message, "the message should say how to resume")

    def test_pool_uuid_is_resolved_to_its_cluster(self):
        with patch.object(ops_gate, "DBController") as db:
            db.return_value.get_pool_by_id.return_value = MagicMock(cluster_id="cl-9")
            db.return_value.get_cluster_by_id.return_value = self._cluster(True)
            with self.assertRaises(ValueError):
                ops_gate.assert_object_ops_allowed("volume resize", pool_uuid="p-1")
            db.return_value.get_cluster_by_id.assert_called_with("cl-9")

    def test_unknown_cluster_does_not_block(self):
        """The gate must never be the reason an operation cannot be attempted."""
        with patch.object(ops_gate, "DBController") as db:
            db.return_value.get_cluster_by_id.side_effect = KeyError("gone")
            ops_gate.assert_object_ops_allowed("volume create", cluster_id="cl-1")

    def test_no_identifier_does_not_block(self):
        ops_gate.assert_object_ops_allowed("volume create")

    def test_default_is_running(self):
        """A cluster that never saw op-stop must behave exactly as before."""
        self.assertFalse(Cluster().object_ops_stopped)


class TestEntryPointsAreGated(unittest.TestCase):
    """Each gated call site must refuse before doing any work."""

    def setUp(self):
        stopped = patch.object(ops_gate, "object_ops_stopped", return_value=True)
        stopped.start()
        self.addCleanup(stopped.stop)
        resolver = patch.object(ops_gate, "DBController")
        resolver.start().return_value.get_pool_by_id.return_value = MagicMock(
            cluster_id="cl-1")
        self.addCleanup(resolver.stop)

    def test_pool_delete(self):
        with patch.object(pool_controller, "DBController") as db:
            db.return_value.get_pool_by_id_or_name.return_value = MagicMock(
                cluster_id="cl-1")
            with self.assertRaises(ValueError):
                pool_controller.delete_pool("p-1")

    def test_pool_create(self):
        with patch.object(pool_controller, "DBController"):
            with self.assertRaises(ValueError):
                pool_controller.add_pool("pool1", 0, 0, 0, 0, 0, 0, "cl-1")

    def test_volume_resize(self):
        with patch.object(lvol_controller, "DBController") as db:
            db.return_value.get_lvol_by_id.return_value = MagicMock(pool_uuid="p-1")
            with self.assertRaises(ValueError):
                lvol_controller.resize_lvol("lv-1", 1024)

    def test_volume_parameter_change(self):
        with patch.object(lvol_controller, "DBController") as db:
            db.return_value.get_lvol_by_id.return_value = MagicMock(pool_uuid="p-1")
            with self.assertRaises(ValueError):
                lvol_controller.set_lvol("lv-1", 0, 0, 0, 0)

    def test_snapshot_delete(self):
        with patch.object(snapshot_controller, "db_controller") as db:
            db.get_snapshot_by_id.return_value = MagicMock(cluster_id="cl-1")
            with self.assertRaises(ValueError):
                snapshot_controller.delete("snap-1")

    def test_clone_create(self):
        with patch.object(snapshot_controller, "db_controller") as db:
            db.get_snapshot_by_id.return_value = MagicMock(cluster_id="cl-1")
            with self.assertRaises(ValueError):
                snapshot_controller.clone("snap-1", "clone1")


class TestSwitch(unittest.TestCase):

    def _run(self, current, requested):
        cluster = MagicMock()
        cluster.object_ops_stopped = current
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "cluster_events") as events:
            db.get_cluster_by_id.return_value = cluster
            result = cluster_ops.set_object_ops("cl-1", requested)
            return result, db.atomic_update, events

    def test_stopping_persists_the_flag(self):
        result, atomic_update, events = self._run(False, True)
        self.assertTrue(result)
        atomic_update.assert_called_once()
        target = MagicMock()
        atomic_update.call_args[0][1](target)
        self.assertTrue(target.object_ops_stopped)
        events.cluster_object_ops_change.assert_called_once()

    def test_starting_persists_the_flag(self):
        _, atomic_update, _ = self._run(True, False)
        target = MagicMock()
        atomic_update.call_args[0][1](target)
        self.assertFalse(target.object_ops_stopped)

    def test_no_write_when_already_in_that_state(self):
        result, atomic_update, _ = self._run(True, True)
        self.assertTrue(result)
        atomic_update.assert_not_called()

    def test_event_failure_does_not_fail_the_switch(self):
        cluster = MagicMock()
        cluster.object_ops_stopped = False
        with patch.object(cluster_ops, "db_controller") as db, \
                patch.object(cluster_ops, "cluster_events") as events, \
                patch.object(cluster_ops, "logger"):
            db.get_cluster_by_id.return_value = cluster
            events.cluster_object_ops_change.side_effect = RuntimeError("no db")
            self.assertTrue(cluster_ops.set_object_ops("cl-1", True))


if __name__ == "__main__":
    unittest.main()
