# coding=utf-8
"""Cluster-state gate on object operations (lvol delete/resize, snapshot delete).

A cluster in in_activation/suspended/unready/inactive cannot complete object
deletions: the controller only runs the leader-side async delete, while the
sync deletes on the non-leaders and the record removal are driven by
lvol_monitor/snapshot_monitor, which skip clusters in those states. Accepting
the operation would strand the object in_deletion forever (2026-07-12
mass-delete run: 8.7k lvol deletes and ~60k snapshot deletes accepted while
the cluster was stuck in_activation, none ever completed). Resize grows the
allocation, so it is gated like create (active/degraded only).

All DB access is mocked; no live FoundationDB is required.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

BLOCKED_FOR_DELETE = [
    Cluster.STATUS_SUSPENDED, Cluster.STATUS_IN_ACTIVATION,
    Cluster.STATUS_UNREADY, Cluster.STATUS_INACTIVE,
]


def _mk_cluster(status):
    cluster = Cluster()
    cluster.uuid = 'cl-1'
    cluster.status = status
    return cluster


def _mk_lvol():
    lvol = LVol()
    lvol.uuid = 'lv-1'
    lvol.node_id = 'node-1'
    lvol.pool_uuid = 'pool-1'
    lvol.status = LVol.STATUS_ONLINE
    lvol.size = 1024
    return lvol


def _mk_snode():
    snode = StorageNode()
    snode.uuid = 'node-1'
    snode.cluster_id = 'cl-1'
    snode.lvstore_status = 'ready'
    return snode


def _mk_pool():
    pool = Pool()
    pool.uuid = 'pool-1'
    pool.cluster_id = 'cl-1'
    pool.status = Pool.STATUS_ACTIVE
    return pool


class TestLvolDeleteGate(unittest.TestCase):

    def _run_delete(self, cluster_status, force_delete=False):
        from simplyblock_core.controllers import lvol_controller

        lvol = _mk_lvol()
        db = MagicMock()
        db.get_storage_node_by_id.return_value = _mk_snode()
        db.get_pool_by_id.return_value = _mk_pool()
        db.get_cluster_by_id.return_value = _mk_cluster(cluster_status)
        db.get_lvol_by_id.return_value = lvol

        with patch.object(lvol_controller, 'DBController', return_value=db), \
                patch('simplyblock_core.controllers.migration_controller.'
                      'get_active_migration_for_lvol', return_value=None), \
                patch.object(lvol_controller, 'lvol_events'), \
                patch.object(lvol_controller, '_delete_lvol_from_all_nodes') as delete_all:
            lvol_controller.delete_lvol(lvol, force_delete=force_delete, lock=False)
        return delete_all

    def test_blocked_states_refuse_delete(self):
        for status in BLOCKED_FOR_DELETE:
            with self.assertRaises(PreconditionError, msg=status) as ctx:
                self._run_delete(status)
            self.assertIn(status, str(ctx.exception))

    def test_operational_states_allow_delete(self):
        # read_only is deliberately allowed: deletes free space and are the
        # way out of a capacity-critical cluster.
        for status in (Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
                       Cluster.STATUS_READONLY):
            delete_all = self._run_delete(status)
            delete_all.assert_called_once()

    def test_force_delete_bypasses_gate(self):
        delete_all = self._run_delete(Cluster.STATUS_IN_ACTIVATION, force_delete=True)
        delete_all.assert_called_once()


class TestLvolResizeGate(unittest.TestCase):

    def _run_resize(self, cluster_status):
        from simplyblock_core.controllers import lvol_controller

        lvol = _mk_lvol()
        db = MagicMock()
        db.get_lvol_by_id.return_value = lvol
        db.get_storage_node_by_id.return_value = _mk_snode()
        db.get_pool_by_id.return_value = _mk_pool()
        db.get_cluster_by_id.return_value = _mk_cluster(cluster_status)

        with patch.object(lvol_controller, 'DBController', return_value=db), \
                patch('simplyblock_core.controllers.migration_controller.'
                      'get_active_migration_for_lvol', return_value=None):
            # new_size == lvol.size returns right after the gate ("nothing to
            # do"), so reaching that return proves the gate let it through.
            lvol_controller.resize_lvol(lvol.uuid, lvol.size, lock=False)

    def test_non_operational_states_refuse_resize(self):
        # Same allow-list as create: growing allocation needs active/degraded.
        for status in (Cluster.STATUS_SUSPENDED, Cluster.STATUS_IN_ACTIVATION,
                       Cluster.STATUS_UNREADY, Cluster.STATUS_INACTIVE,
                       Cluster.STATUS_READONLY, Cluster.STATUS_IN_EXPANSION):
            with self.assertRaises(PreconditionError, msg=status):
                self._run_resize(status)

    def test_operational_states_allow_resize(self):
        for status in (Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED):
            self._run_resize(status)  # must not raise


class TestSnapshotDeleteGate(unittest.TestCase):

    def _run_delete(self, cluster_status, force_delete=False):
        from simplyblock_core.controllers import snapshot_controller

        snap = SnapShot()
        snap.uuid = 'snap-1'
        snap.cluster_id = 'cl-1'
        snap.status = SnapShot.STATUS_ONLINE
        snap.lvol = _mk_lvol()

        db = MagicMock()
        db.get_snapshot_by_id.return_value = snap
        db.get_storage_node_by_id.return_value = _mk_snode()
        db.get_cluster_by_id.return_value = _mk_cluster(cluster_status)

        migration = MagicMock()
        # An "active migration" right after the gate stops the flow there:
        # reaching this check proves the gate let the delete through.
        migration.get_active_migration_for_lvol.return_value = MagicMock(uuid='mig-1')

        with patch.object(snapshot_controller, 'db_controller', db), \
                patch.object(snapshot_controller, 'migration_controller', migration):
            result = snapshot_controller.delete(
                snap.uuid, force_delete=force_delete, lock=False)
        return result, migration.get_active_migration_for_lvol

    def test_blocked_states_refuse_delete(self):
        for status in BLOCKED_FOR_DELETE:
            result, migration_check = self._run_delete(status)
            self.assertFalse(result, msg=status)
            migration_check.assert_not_called()

    def test_operational_states_pass_gate(self):
        for status in (Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED,
                       Cluster.STATUS_READONLY):
            _, migration_check = self._run_delete(status)
            migration_check.assert_called_once()


if __name__ == '__main__':
    unittest.main()
