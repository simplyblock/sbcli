"""Node removal as three steps, and "shrinking" as a flag beside the status.

* The calculated cluster status counts a node that is being removed only
  while some of its devices are not yet rebuilt, and not at all afterwards.
  It used to count it until REMOVED, which kept a k=1 cluster DEGRADED for the
  whole removal and stalled the operator, which pauses on anything not ACTIVE.
* "Shrinking" is Cluster.is_shrinking, derived from the node statuses, not the
  IN_SHRINK status; is_degraded_by_removal says the removal alone makes it
  DEGRADED.
* The removal's steps: prepare-removal (admit, pending_removal, shut down,
  rebuild devices -> migrating_lvols), verify-drained (nothing left on the
  node), then the node DELETE.
"""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.controllers import node_drain_steps
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


def _dev(status):
    d = NVMeDevice()
    d.status = status
    return d


def _node(uuid, ip, status=StorageNode.STATUS_ONLINE, devices=(NVMeDevice.STATUS_ONLINE,)):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = "c1"
    n.mgmt_ip = ip
    n.status = status
    n.nvme_devices = [_dev(s) for s in devices]
    n.rpc_client = MagicMock()
    n.rpc_client.return_value.get_lvstore.return_value = None
    return n


def _cluster(npcs):
    c = Cluster()
    c.uuid = "c1"
    c.ha_type = "ha"
    c.distr_ndcs = 2
    c.distr_npcs = npcs
    c.status = Cluster.STATUS_ACTIVE
    return c


class TestStatusLeavesARebuiltRemovalNodeOut(unittest.TestCase):

    def setUp(self):
        with patch("simplyblock_core.db_controller.DBController"):
            from simplyblock_core.services import storage_node_monitor as snm
        self.snm = snm
        p = patch.object(snm, "is_new_migrated_node", return_value=False)
        p.start()
        self.addCleanup(p.stop)

    def _verdicts(self, cluster, nodes):
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
        with patch.object(self.snm, "db", db):
            return self.snm._cluster_status_verdicts(cluster.get_id())

    def _online(self, count):
        return [_node(f"n{i}", f"10.0.0.{i}") for i in range(count)]

    def test_k1_is_degraded_only_by_the_removal_while_its_devices_rebuild(self):
        leaving = _node("x", "10.0.9.9", StorageNode.STATUS_MIGRATING_DEVICES,
                        (NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED))
        status, without = self._verdicts(_cluster(npcs=1), self._online(4) + [leaving])
        assert status == Cluster.STATUS_DEGRADED
        assert without == Cluster.STATUS_ACTIVE

    def test_k2_stays_active_while_the_removal_rebuilds(self):
        leaving = _node("x", "10.0.9.9", StorageNode.STATUS_MIGRATING_DEVICES,
                        (NVMeDevice.STATUS_FAILED,))
        status, _ = self._verdicts(_cluster(npcs=2), self._online(5) + [leaving])
        assert status == Cluster.STATUS_ACTIVE

    def test_k1_the_removals_own_shutdown_is_already_the_removals(self):
        # prepare_node_for_removal shuts the node down while it is still
        # pending_removal: its devices go unavailable before it reaches
        # migrating_devices. That must not read as an outage beside the removal.
        leaving = _node("x", "10.0.9.9", StorageNode.STATUS_PENDING_REMOVAL,
                        (NVMeDevice.STATUS_UNAVAILABLE,))
        status, without = self._verdicts(_cluster(npcs=1), self._online(4) + [leaving])
        assert status == Cluster.STATUS_DEGRADED
        assert without == Cluster.STATUS_ACTIVE

    def test_a_serving_pending_removal_node_counts_as_online(self):
        leaving = _node("x", "10.0.9.9", StorageNode.STATUS_PENDING_REMOVAL)
        status, without = self._verdicts(_cluster(npcs=1), self._online(4) + [leaving])
        assert (status, without) == (Cluster.STATUS_ACTIVE, Cluster.STATUS_ACTIVE)

    def test_another_outage_is_not_blamed_on_the_removal(self):
        leaving = _node("x", "10.0.9.9", StorageNode.STATUS_MIGRATING_DEVICES,
                        (NVMeDevice.STATUS_FAILED,))
        down = _node("d", "10.0.0.99", StorageNode.STATUS_OFFLINE)
        status, without = self._verdicts(_cluster(npcs=1), self._online(4) + [leaving, down])
        assert status == Cluster.STATUS_SUSPENDED
        assert without == Cluster.STATUS_DEGRADED


@pytest.mark.parametrize("status", [
    StorageNode.STATUS_MIGRATING_DEVICES, StorageNode.STATUS_MIGRATING_LVOLS,
    StorageNode.STATUS_IN_REMOVAL, StorageNode.STATUS_REMOVED_FAILED,
    StorageNode.STATUS_REMOVED])
def test_a_rebuilt_removal_node_does_not_count(status):
    with patch("simplyblock_core.db_controller.DBController"):
        from simplyblock_core.services import storage_node_monitor as snm
    leaving = _node("x", "10.0.9.9", status, (NVMeDevice.STATUS_FAILED_AND_MIGRATED,) * 3)
    nodes = [_node(f"n{i}", f"10.0.0.{i}") for i in range(4)] + [leaving]
    db = MagicMock()
    db.get_cluster_by_id.return_value = _cluster(npcs=1)
    db.get_primary_storage_nodes_by_cluster_id.return_value = nodes
    with patch.object(snm, "db", db), patch.object(snm, "is_new_migrated_node", return_value=False):
        assert snm._cluster_status_verdicts("c1") == (Cluster.STATUS_ACTIVE, Cluster.STATUS_ACTIVE)


# --- the removal holds no cluster status; the dismantling owns restart phases

def test_a_node_in_removal_owns_restart_phases():
    node = _node("t", "10.0.0.1")
    node.restart_phases = {"LVS_1": "relocating"}
    peer = _node("x", "10.0.9.9", StorageNode.STATUS_IN_REMOVAL)
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    db.get_cluster_by_id.return_value = _cluster(npcs=2)
    db.get_storage_nodes_by_cluster_id.return_value = [node, peer]
    with patch.object(storage_node_ops, "DBController", return_value=db):
        assert storage_node_ops.get_restart_phase("t", "LVS_1") == "relocating"


def test_display_status_shows_shrinking_beside_the_status():
    c = _cluster(npcs=2)
    c.is_re_balancing = True
    c.is_shrinking = True
    assert cluster_ops.display_status(c) == "active - ReBalancing - Shrinking"


def test_expansion_waits_for_a_removal():
    from simplyblock_core.controllers.cluster_expansion import preconditions
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = [
        _node("a", "10.0.0.1"), _node("x", "10.0.9.9", StorageNode.STATUS_PENDING_REMOVAL)]
    ok, reason = preconditions.check_expansion_preconditions(_cluster(npcs=2), db, MagicMock())
    assert ok is False and "removal is in progress" in reason


# --- the three steps -----------------------------------------------------------

class TestPrepareNodeForRemoval(unittest.TestCase):

    def setUp(self):
        node_drain_steps._reset_for_test()
        self.addCleanup(node_drain_steps._reset_for_test)

    def _run(self, status, admission=(True, "")):
        node = _node("n1", "10.0.0.1", status)
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        stamp = MagicMock(side_effect=lambda nid, st, **k: setattr(node, "status", st))
        with patch.object(node_drain_steps, "DBController", return_value=db), \
                patch.object(storage_node_ops, "check_removal_admission",
                             return_value=admission) as admit, \
                patch.object(storage_node_ops, "set_node_status", stamp), \
                patch.object(node_drain_steps, "start_device_decommission",
                             return_value=True) as start:
            result = node_drain_steps.prepare_node_for_removal("n1")
        return result, admit, stamp, start

    def test_admits_marks_pending_removal_and_starts_the_rebuild(self):
        result, admit, stamp, start = self._run(StorageNode.STATUS_ONLINE)
        admit.assert_called_once()
        assert admit.call_args.kwargs["check_snapshots"] is False
        stamp.assert_called_once_with("n1", StorageNode.STATUS_PENDING_REMOVAL, caused_by="remove")
        start.assert_called_once_with("n1")
        assert result["status"] == StorageNode.STATUS_PENDING_REMOVAL

    def test_a_refused_admission_changes_nothing(self):
        with pytest.raises(PreconditionError, match="would leave"):
            self._run(StorageNode.STATUS_ONLINE, admission=(False, "would leave FTT short"))

    def test_a_node_already_departing_is_not_readmitted(self):
        _, admit, stamp, start = self._run(StorageNode.STATUS_MIGRATING_DEVICES)
        admit.assert_not_called()
        stamp.assert_not_called()
        start.assert_called_once_with("n1")

    def test_a_removed_node_is_refused(self):
        with pytest.raises(PreconditionError):
            self._run(StorageNode.STATUS_REMOVED)


@pytest.mark.parametrize("status", [
    StorageNode.STATUS_MIGRATING_LVOLS, StorageNode.STATUS_IN_REMOVAL, StorageNode.STATUS_REMOVED])
def test_the_device_step_never_rewinds_a_later_status(status):
    node_drain_steps._reset_for_test()
    node = _node("n1", "10.0.0.1", status)
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    with patch.object(node_drain_steps, "DBController", return_value=db), \
            patch.object(storage_node_ops, "set_node_status") as stamp, \
            patch.object(node_drain_steps, "_spawn") as spawn:
        assert node_drain_steps.start_device_decommission("n1") is False
    stamp.assert_not_called()
    spawn.assert_not_called()


def test_the_rebuilt_node_moves_on_to_migrating_lvols():
    node_drain_steps._reset_for_test()
    node = _node("n1", "10.0.0.1", StorageNode.STATUS_MIGRATING_DEVICES)
    db = MagicMock()
    db.get_storage_node_by_id.return_value = node
    captured = {}
    with patch.object(node_drain_steps, "DBController", return_value=db), \
            patch.object(storage_node_ops, "set_node_status"), \
            patch.object(node_drain_steps, "_spawn",
                         side_effect=lambda nid, step, drive: captured.setdefault("drive", drive)), \
            patch.object(storage_node_ops, "_fail_and_migrate_node_devices", return_value=True), \
            patch.object(node_drain_steps, "mark_migrating_lvols") as mark:
        node_drain_steps.start_device_decommission("n1")
        assert captured["drive"]() is True
    mark.assert_called_once_with("n1")


def test_verify_drained_lists_what_is_left():
    lvol = MagicMock()
    lvol.get_id.return_value = "lv1"
    snap = MagicMock(deleted=False)
    snap.get_id.return_value = "s1"
    db = MagicMock()
    db.get_lvols_by_node_id.return_value = [lvol]
    db.get_snapshots.return_value = [snap]
    with patch.object(node_drain_steps, "DBController", return_value=db), \
            patch.object(storage_node_ops, "_snapshot_lives_on_node", return_value=True):
        assert node_drain_steps.verify_node_drained("n1") == {
            'drained': False, 'lvols': ['lv1'], 'snapshots': ['s1']}
    db.get_lvols_by_node_id.return_value = []
    db.get_snapshots.return_value = []
    with patch.object(node_drain_steps, "DBController", return_value=db):
        assert node_drain_steps.verify_node_drained("n1")['drained'] is True
