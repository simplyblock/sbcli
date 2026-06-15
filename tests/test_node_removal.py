# coding=utf-8
"""
Unit tests for online storage-node removal (inverse of cluster expansion).

Covers:
  * remove_storage_node() precondition gating (online-only, all-peers-online,
    no LVols, no snapshots, Case-B relocation feasibility) and task queueing.
  * _check_replica_relocation_feasible() / _pick_replica_relocation_node().
  * The orchestration helpers' idempotent bookkeeping:
      - _teardown_replicas_of_primary() (Case A)
      - _relocate_one_replica()         (Case B)
      - _decommission_node_devices()    (remove/fail/migrate completion gate)
  * The in_removal status code mapping.

All data-plane RPCs / device-controller / DB access is mocked — these are
pure control-flow + bookkeeping tests.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.cluster import Cluster


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _cluster(ha_type="ha", npcs=1, ndcs=2, ft=1, mode="docker"):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.ha_type = ha_type
    cl.distr_npcs = npcs
    cl.distr_ndcs = ndcs
    cl.max_fault_tolerance = ft
    cl.mode = mode
    cl.status = Cluster.STATUS_ACTIVE
    cl.nqn = "nqn.2023-01.io.simplyblock:cluster-1"
    return cl


def _node(node_id, status=StorageNode.STATUS_ONLINE, lvstore="",
          secondary_id="", tertiary_id="",
          stack_secondary="", stack_tertiary="", n_devices=0, with_jm=False):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = "cluster-1"
    n.lvstore = lvstore
    n.lvstore_stack = [{"type": "bdev_distr", "name": "distrib_1"},
                       {"type": "bdev_raid", "name": "raid_1"},
                       {"type": "bdev_lvstore", "name": lvstore or "LVS"}]
    n.secondary_node_id = secondary_id
    n.tertiary_node_id = tertiary_id
    n.lvstore_stack_secondary = stack_secondary
    n.lvstore_stack_tertiary = stack_tertiary
    n.mgmt_ip = f"10.0.0.{abs(hash(node_id)) % 250 + 1}"
    n.write_to_db = MagicMock()
    n.rpc_client = MagicMock(return_value=MagicMock())
    n.hublvol_nqn_for_lvstore = MagicMock(return_value=f"nqn:hub:{lvstore}")
    n.client = MagicMock(return_value=MagicMock())
    n.rpc_port = 8080

    devs = []
    for i in range(n_devices):
        d = NVMeDevice()
        d.uuid = f"dev-{node_id}-{i}"
        d.node_id = node_id
        d.status = NVMeDevice.STATUS_ONLINE
        d.pcie_address = f"0000:00:0{i}.0"
        d.cluster_device_order = i
        devs.append(d)
    n.nvme_devices = devs

    if with_jm:
        jm = JMDevice()
        jm.uuid = f"jm-{node_id}"
        jm.node_id = node_id
        jm.status = JMDevice.STATUS_ONLINE
        n.jm_device = jm
    else:
        n.jm_device = None
    return n


class FakeDB:
    def __init__(self, cluster, nodes, lvols=None, snaps=None):
        self.cluster = cluster
        self.nodes = {n.get_id(): n for n in nodes}
        self.lvols = lvols or {}
        self.snaps = snaps or []
        self.kv_store = MagicMock()
        # devices indexed by id, pulled from the nodes
        self.devices = {}
        for n in nodes:
            for d in n.nvme_devices:
                self.devices[d.get_id()] = d

    def get_cluster_by_id(self, _):
        return self.cluster

    def get_storage_nodes_by_cluster_id(self, _):
        return list(self.nodes.values())

    def get_storage_node_by_id(self, nid):
        if nid in self.nodes:
            return self.nodes[nid]
        raise KeyError(nid)

    def get_lvols_by_node_id(self, nid):
        return self.lvols.get(nid, [])

    def get_snapshots(self):
        return self.snaps

    def get_storage_device_by_id(self, did):
        return self.devices[did]


# ---------------------------------------------------------------------------
# remove_storage_node — preconditions
# ---------------------------------------------------------------------------

class TestRemovePreconditions(unittest.TestCase):

    def _run(self, db, **patches):
        tc = MagicMock()
        tc.get_active_node_removal_task.return_value = patches.get("active_removal", False)
        tc.get_active_node_tasks.return_value = patches.get("active_tasks", [])
        tc.add_node_removal_task.return_value = patches.get("task_id", "task-uuid-1")
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "tasks_controller", tc), \
             patch.object(storage_node_ops, "_check_ftt_allows_node_removal",
                          return_value=patches.get("ftt", (True, ""))), \
             patch.object(storage_node_ops, "_check_replica_relocation_feasible",
                          return_value=patches.get("feasible", (True, ""))):
            ret = storage_node_ops.remove_storage_node("n1")
        return ret, tc

    def test_happy_path_queues_task(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2"), _node("n3")]
        ret, tc = self._run(FakeDB(cl, nodes))
        self.assertEqual(ret, "task-uuid-1")
        tc.add_node_removal_task.assert_called_once()

    def test_reject_target_not_online(self):
        cl = _cluster()
        nodes = [_node("n1", status=StorageNode.STATUS_OFFLINE), _node("n2")]
        ret, tc = self._run(FakeDB(cl, nodes))
        self.assertFalse(ret)
        tc.add_node_removal_task.assert_not_called()

    def test_reject_peer_not_online(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2", status=StorageNode.STATUS_DOWN), _node("n3")]
        ret, tc = self._run(FakeDB(cl, nodes))
        self.assertFalse(ret)
        tc.add_node_removal_task.assert_not_called()

    def test_removed_peer_is_ignored(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2"),
                 _node("n3", status=StorageNode.STATUS_REMOVED)]
        ret, _ = self._run(FakeDB(cl, nodes))
        self.assertEqual(ret, "task-uuid-1")

    def test_reject_lvols_present(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2")]
        db = FakeDB(cl, nodes, lvols={"n1": [MagicMock()]})
        ret, tc = self._run(db)
        self.assertFalse(ret)
        tc.add_node_removal_task.assert_not_called()

    def test_reject_snapshots_present(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2")]
        snap = MagicMock()
        snap.lvol.node_id = "n1"
        snap.deleted = False
        db = FakeDB(cl, nodes, snaps=[snap])
        ret, tc = self._run(db)
        self.assertFalse(ret)

    def test_reject_relocation_infeasible(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2")]
        ret, tc = self._run(FakeDB(cl, nodes),
                            feasible=(False, "no host-disjoint node"))
        self.assertFalse(ret)
        tc.add_node_removal_task.assert_not_called()

    def test_reject_ftt(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2")]
        ret, tc = self._run(FakeDB(cl, nodes), ftt=(False, "ftt blocks"))
        self.assertFalse(ret)

    def test_idempotent_returns_existing_task(self):
        cl = _cluster()
        nodes = [_node("n1"), _node("n2")]
        ret, tc = self._run(FakeDB(cl, nodes), active_removal="existing-task")
        self.assertEqual(ret, "existing-task")
        tc.add_node_removal_task.assert_not_called()


# ---------------------------------------------------------------------------
# Replica relocation feasibility / picking
# ---------------------------------------------------------------------------

class TestRelocationFeasibility(unittest.TestCase):

    def test_no_hosted_replicas_is_feasible(self):
        cl = _cluster()
        removed = _node("n1")
        db = FakeDB(cl, [removed, _node("n2")])
        ok, _ = storage_node_ops._check_replica_relocation_feasible(removed, db)
        self.assertTrue(ok)

    def test_infeasible_when_no_target(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1")
        db = FakeDB(cl, [removed, primary])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value=None):
            ok, reason = storage_node_ops._check_replica_relocation_feasible(removed, db)
        self.assertFalse(ok)
        self.assertIn("secondary", reason)

    def test_feasible_when_target_exists(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1")
        db = FakeDB(cl, [removed, primary])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"):
            ok, _ = storage_node_ops._check_replica_relocation_feasible(removed, db)
        self.assertTrue(ok)

    def test_pick_secondary_uses_get_secondary_nodes(self):
        cl = _cluster()
        primary = _node("p1", secondary_id="n1", tertiary_id="n9")
        removed = _node("n1")
        db = FakeDB(cl, [primary, removed])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["n5"]) as gsn:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "n5")
        # the removed node and the tertiary are excluded from candidates
        _, kwargs = gsn.call_args
        self.assertIn("n1", kwargs["exclude_ids"])
        self.assertIn("n9", kwargs["exclude_ids"])


# ---------------------------------------------------------------------------
# Case A — teardown of own primary's replicas
# ---------------------------------------------------------------------------

class TestTeardownOwnReplicas(unittest.TestCase):

    def test_clears_bookkeeping_both_sides(self):
        cl = _cluster()
        removed = _node("n1", lvstore="LVS_1",
                        secondary_id="n2", tertiary_id="n3")
        sec = _node("n2", stack_secondary="n1")
        tert = _node("n3", stack_tertiary="n1")
        db = FakeDB(cl, [removed, sec, tert])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._teardown_replicas_of_primary(removed)
        self.assertTrue(ret)
        self.assertEqual(removed.secondary_node_id, "")
        self.assertEqual(removed.tertiary_node_id, "")
        self.assertEqual(sec.lvstore_stack_secondary, "")
        self.assertEqual(tert.lvstore_stack_tertiary, "")
        self.assertEqual(drp.call_count, 2)


# ---------------------------------------------------------------------------
# Case B — relocate a hosted replica
# ---------------------------------------------------------------------------

class TestRelocateOneReplica(unittest.TestCase):

    def test_relocate_success_moves_bookkeeping(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1", lvstore="LVS_p1")
        new = _node("n3")
        db = FakeDB(cl, [removed, primary, new])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec:
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")
        self.assertTrue(ret)
        self.assertEqual(primary.secondary_node_id, "n3")
        self.assertEqual(new.lvstore_stack_secondary, "p1")
        # back-reference cleared only after a successful rebuild
        self.assertEqual(removed.lvstore_stack_secondary, "")
        rec.assert_called_once()

    def test_relocate_failure_keeps_backref(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1", lvstore="LVS_p1")
        new = _node("n3")
        db = FakeDB(cl, [removed, primary, new])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=False):
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")
        self.assertFalse(ret)
        # forward bookkeeping committed, but back-ref NOT cleared -> retry resumes
        self.assertEqual(primary.secondary_node_id, "n3")
        self.assertEqual(removed.lvstore_stack_secondary, "p1")

    def test_relocate_resume_reuses_committed_target(self):
        # Simulates a retry: primary already points at the new node, removed
        # still holds the back-ref. Must NOT pick a fresh node.
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n3", lvstore="LVS_p1")
        new = _node("n3", stack_secondary="p1")
        db = FakeDB(cl, [removed, primary, new])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="SHOULD-NOT-BE-USED") as pick, \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True):
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")
        self.assertTrue(ret)
        pick.assert_not_called()
        self.assertEqual(removed.lvstore_stack_secondary, "")

    def test_relocate_missing_primary_just_clears(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="gone")
        db = FakeDB(cl, [removed])
        with patch.object(storage_node_ops, "DBController", return_value=db):
            ret = storage_node_ops._relocate_one_replica(removed, "gone", "secondary")
        self.assertTrue(ret)
        self.assertEqual(removed.lvstore_stack_secondary, "")


# ---------------------------------------------------------------------------
# Device decommission completion gate
# ---------------------------------------------------------------------------

class TestDecommissionDevices(unittest.TestCase):

    def test_first_pass_drives_and_waits(self):
        cl = _cluster()
        removed = _node("n1", n_devices=2, with_jm=True)
        db = FakeDB(cl, [removed])

        dc = MagicMock()

        def _remove(dev_id, force=True):
            db.get_storage_device_by_id(dev_id).status = NVMeDevice.STATUS_REMOVED
            return True

        def _fail(dev_id):
            db.get_storage_device_by_id(dev_id).status = NVMeDevice.STATUS_FAILED
            return True

        dc.device_remove.side_effect = _remove
        dc.device_set_failed.side_effect = _fail

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc):
            ret = storage_node_ops._decommission_node_devices(removed)

        # devices are FAILED (migration queued) but not yet FAILED_AND_MIGRATED
        self.assertFalse(ret)
        dc.remove_jm_device.assert_called_once()
        self.assertEqual(dc.device_remove.call_count, 2)
        self.assertEqual(dc.device_set_failed.call_count, 2)

    def test_complete_when_all_migrated(self):
        cl = _cluster()
        removed = _node("n1", n_devices=2, with_jm=False)
        for d in removed.nvme_devices:
            d.status = NVMeDevice.STATUS_FAILED_AND_MIGRATED
        db = FakeDB(cl, [removed])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc):
            ret = storage_node_ops._decommission_node_devices(removed)
        self.assertTrue(ret)
        dc.device_remove.assert_not_called()


# ---------------------------------------------------------------------------
# Status code mapping
# ---------------------------------------------------------------------------

class TestStatusCode(unittest.TestCase):

    def test_in_removal_status_mapped(self):
        self.assertEqual(StorageNode.STATUS_IN_REMOVAL, "in_removal")
        self.assertIn(StorageNode.STATUS_IN_REMOVAL, StorageNode._STATUS_CODE_MAP)


if __name__ == "__main__":
    unittest.main()
