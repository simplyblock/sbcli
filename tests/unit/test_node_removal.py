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
from unittest.mock import DEFAULT, MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.rpc_client import RPCConnectionError, RPCException


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _cluster(ha_type="ha", npcs=1, ndcs=2, ft=1, mode="docker",
             enable_failure_domain=False):
    cl = Cluster()
    cl.uuid = "cluster-1"
    cl.ha_type = ha_type
    cl.distr_npcs = npcs
    cl.distr_ndcs = ndcs
    cl.max_fault_tolerance = ft
    cl.mode = mode
    cl.status = Cluster.STATUS_ACTIVE
    cl.nqn = "nqn.2023-01.io.simplyblock:cluster-1"
    cl.enable_failure_domain = enable_failure_domain
    return cl


def _node(node_id, status=StorageNode.STATUS_ONLINE, lvstore="",
          secondary_id="", tertiary_id="",
          stack_secondary="", stack_tertiary="", n_devices=0, with_jm=False,
          failure_domain=-1, mgmt_ip=None):
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
    n.failure_domain = failure_domain
    n.mgmt_ip = mgmt_ip or f"10.0.0.{abs(hash(node_id)) % 250 + 1}"
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
# _find_splice_target_for_relocation — the removal-repair fallback used when
# get_secondary_nodes/_2 offer no free (unclaimed) cross-domain candidate.
#
# Regression coverage for the 2026-08-07 chained-removal incident: two
# removals in a row can strand a third node's secondary with zero free
# cross-domain candidates, even though an existing pairing two hops away
# could absorb it. Mirrors splice_stranded_secondary/_tertiary's edge search
# (used for the identical dead end at activation time), generalized with an
# exclude list for the removal path.
# ---------------------------------------------------------------------------

class TestFindSpliceTargetForRelocation(unittest.TestCase):

    def test_splices_into_existing_secondary_edge(self):
        cl = _cluster(enable_failure_domain=True)
        p = _node("p", secondary_id="x", failure_domain=0, mgmt_ip="10.0.0.1")
        x = _node("x", failure_domain=1, mgmt_ip="10.0.0.2")
        stranded = _node("s", failure_domain=2, mgmt_ip="10.0.0.3")
        db = FakeDB(cl, [p, x, stranded])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertEqual(got, ("p", "x"))

    def test_prefers_edge_domain_disjoint_on_both_ends(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=0, mgmt_ip="10.0.0.9")
        bad_p = _node("bad_p", secondary_id="bad_x", failure_domain=0, mgmt_ip="10.0.0.1")
        bad_x = _node("bad_x", failure_domain=0, mgmt_ip="10.0.0.2")
        good_p = _node("good_p", secondary_id="good_x", failure_domain=1, mgmt_ip="10.0.0.3")
        good_x = _node("good_x", failure_domain=2, mgmt_ip="10.0.0.4")
        db = FakeDB(cl, [stranded, bad_p, bad_x, good_p, good_x])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertEqual(got, ("good_p", "good_x"))

    def test_excludes_ids_passed_by_caller(self):
        cl = _cluster(enable_failure_domain=True)
        p = _node("p", secondary_id="x", failure_domain=0, mgmt_ip="10.0.0.1")
        x = _node("x", failure_domain=1, mgmt_ip="10.0.0.2")
        stranded = _node("s", failure_domain=2, mgmt_ip="10.0.0.3")
        db = FakeDB(cl, [p, x, stranded])
        got = storage_node_ops._find_splice_target_for_relocation(
            stranded, "secondary", db, exclude_ids=["x"])
        self.assertIsNone(got)

    def test_no_edge_exists_returns_none(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=0, mgmt_ip="10.0.0.1")
        other = _node("other", failure_domain=1, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [stranded, other])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertIsNone(got)

    def test_skips_edge_with_offline_endpoint(self):
        cl = _cluster(enable_failure_domain=True)
        p = _node("p", secondary_id="x", failure_domain=0, mgmt_ip="10.0.0.1")
        x = _node("x", failure_domain=1, mgmt_ip="10.0.0.2",
                  status=StorageNode.STATUS_OFFLINE)
        stranded = _node("s", failure_domain=2, mgmt_ip="10.0.0.3")
        db = FakeDB(cl, [p, x, stranded])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertIsNone(got)

    def test_skips_edge_not_host_disjoint_from_stranded(self):
        stranded = _node("s", failure_domain=0, mgmt_ip="10.0.0.5")
        p = _node("p", secondary_id="x", failure_domain=1, mgmt_ip="10.0.0.1")
        x = _node("x", failure_domain=2, mgmt_ip="10.0.0.5")  # shares stranded's host
        cl = _cluster(enable_failure_domain=True)
        db = FakeDB(cl, [stranded, p, x])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertIsNone(got)

    def test_tertiary_edge_respects_secondary_host_disjointness(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=0, secondary_id="s_sec", mgmt_ip="10.0.0.9")
        s_sec = _node("s_sec", failure_domain=1, mgmt_ip="10.0.0.50")
        p = _node("p", tertiary_id="x", failure_domain=0, mgmt_ip="10.0.0.60")
        x = _node("x", failure_domain=1, mgmt_ip="10.0.0.61")
        db = FakeDB(cl, [stranded, s_sec, p, x])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "tertiary", db)
        self.assertEqual(got, ("p", "x"))


# ---------------------------------------------------------------------------
# _pick_replica_relocation_node — falls back to the splice finder above when
# the direct free-candidate search comes up empty (no candidates at all, or
# none that satisfy the hard cross-domain requirement).
# ---------------------------------------------------------------------------

class TestPickReplicaRelocationSpliceFallback(unittest.TestCase):

    def test_falls_back_to_splice_when_no_free_candidate_at_all(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", failure_domain=2, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        edge_p = _node("edge_p", secondary_id="edge_x", failure_domain=0, mgmt_ip="10.0.0.1")
        edge_x = _node("edge_x", failure_domain=1, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [primary, removed, edge_p, edge_x])
        with patch.object(storage_node_ops, "get_secondary_nodes", return_value=[]):
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "edge_x")

    def test_falls_back_to_splice_when_only_candidate_is_same_domain(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", failure_domain=2, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        same_domain_cand = _node("same_domain_cand", failure_domain=2, mgmt_ip="10.0.0.8")
        edge_p = _node("edge_p", secondary_id="edge_x", failure_domain=0, mgmt_ip="10.0.0.1")
        edge_x = _node("edge_x", failure_domain=1, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [primary, removed, same_domain_cand, edge_p, edge_x])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["same_domain_cand"]):
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "edge_x")

    def test_returns_none_when_no_free_and_no_splice_candidate(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        db = FakeDB(cl, [primary, removed])
        with patch.object(storage_node_ops, "get_secondary_nodes", return_value=[]):
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertIsNone(got)

    def test_does_not_use_splice_when_free_cross_domain_candidate_exists(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        free1 = _node("free1", failure_domain=1, mgmt_ip="10.0.0.1")
        db = FakeDB(cl, [primary, removed, free1])
        with patch.object(storage_node_ops, "get_secondary_nodes", return_value=["free1"]), \
             patch.object(storage_node_ops, "_find_splice_target_for_relocation") as finder:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "free1")
        finder.assert_not_called()

    def test_fd_disabled_never_needs_splice_when_candidate_exists(self):
        # Non-FD clusters take the unconditional cands[0] path -- splice is
        # only relevant once cands is genuinely empty.
        cl = _cluster(enable_failure_domain=False)
        primary = _node("p1", secondary_id="n1", mgmt_ip="10.0.0.9")
        removed = _node("n1", mgmt_ip="10.0.0.99")
        db = FakeDB(cl, [primary, removed])
        with patch.object(storage_node_ops, "get_secondary_nodes", return_value=["free1"]), \
             patch.object(storage_node_ops, "_find_splice_target_for_relocation") as finder:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "free1")
        finder.assert_not_called()


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
# _delete_replica_on_peer — the peer-side hublvol+bdev teardown _teardown_
# replicas_of_primary and _relocate_replica_between both call.
#
# Regression coverage for a real bug found live (2026-08-10): the hublvol
# subsystem check called rpc_client.subsystem_list(nqn), but subsystem_list()
# takes no arguments at all (it lists everything, unfiltered) -- every call
# raised TypeError, silently swallowed by the surrounding best-effort
# try/except, so the hublvol subsystem was never actually torn down on any
# peer during node removal. subsystem_get(nqn) is the existing, correct,
# server-side-filtered method for this.
# ---------------------------------------------------------------------------

class TestDeleteReplicaOnPeer(unittest.TestCase):

    def test_deletes_subsystem_when_present(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1", lvstore="LVS_1")  # hublvol_nqn_for_lvstore's mocked return value bakes in this lvstore
        rpc = peer.rpc_client()
        rpc.subsystem_get.return_value = {"nqn": "nqn:hub:LVS_1"}
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.subsystem_get.assert_called_once_with("nqn:hub:LVS_1")
        rpc.subsystem_delete.assert_called_once_with("nqn:hub:LVS_1")

    def test_skips_delete_when_subsystem_absent(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1", lvstore="LVS_1")  # hublvol_nqn_for_lvstore's mocked return value bakes in this lvstore
        rpc = peer.rpc_client()
        rpc.subsystem_get.return_value = None
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.subsystem_get.assert_called_once_with("nqn:hub:LVS_1")
        rpc.subsystem_delete.assert_not_called()

    def test_subsystem_get_failure_is_caught_not_raised(self):
        # Best-effort: an RPC failure here must not propagate and block removal.
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1", lvstore="LVS_1")  # hublvol_nqn_for_lvstore's mocked return value bakes in this lvstore
        rpc = peer.rpc_client()
        rpc.subsystem_get.side_effect = RPCConnectionError("connection error")
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)  # must not raise
        rpc.subsystem_delete.assert_not_called()
        # Teardown of the other artifacts still proceeds despite this failure.
        rpc.bdev_lvol_delete_hublvol.assert_called_once_with("LVS_1")

    def test_no_op_when_primary_has_no_lvstore(self):
        cl = _cluster()
        primary = _node("p1", lvstore="")
        peer = _node("peer1", lvstore="LVS_1")  # hublvol_nqn_for_lvstore's mocked return value bakes in this lvstore
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.subsystem_get.assert_not_called()
        rpc.bdev_lvol_delete_hublvol.assert_not_called()


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
# Case B, splice fallback — _pick_replica_relocation_node returned a BUSY
# node (a splice candidate, per _find_splice_target_for_relocation) instead
# of a free one. _relocate_one_replica must evict that node's current
# occupant onto the stranded primary before claiming the slot for itself.
# ---------------------------------------------------------------------------

class TestRelocateOneReplicaSpliceExecution(unittest.TestCase):

    def test_relocate_via_splice_evicts_occupant_first(self):
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")  # x is busy, not free
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        drp.assert_called_once()  # occupant's old replica torn down off x
        self.assertEqual(occupant.secondary_node_id, "stranded")  # occupant re-homed onto stranded
        self.assertEqual(stranded.secondary_node_id, "x")  # stranded takes over x's freed slot
        self.assertEqual(x.lvstore_stack_secondary, "stranded")
        self.assertEqual(rec.call_count, 2)  # occupant's rebuild + stranded's own rebuild
        self.assertEqual(removed.lvstore_stack_secondary, "")

    def test_relocate_via_splice_occupant_rebuild_failure_leaves_old_copy_untouched(self):
        # Create-before-destroy: a failed rebuild on the stranded node must
        # NOT tear down or repoint the occupant's still-intact old copy on x
        # -- that copy is occupant's ONLY surviving replica under FTT1, so a
        # failed build must change nothing about it (2026-08-07 incident).
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=False), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertFalse(ret)
        drp.assert_not_called()  # old copy on x never torn down
        self.assertEqual(occupant.secondary_node_id, "x")  # unchanged -- still protected
        self.assertEqual(x.lvstore_stack_secondary, "occupant")  # unchanged
        # The outer splice claim (stranded -> x) never got committed either.
        self.assertEqual(stranded.secondary_node_id, "n1")
        self.assertEqual(removed.lvstore_stack_secondary, "stranded")

    def test_relocate_via_splice_occupant_rebuild_raises_treated_as_failure(self):
        # The 2026-08-07 incident's actual failure mode: recreate_lvstore_on_non_leader
        # RAISED (a hublvol attach error) instead of returning False. Must be
        # caught and handled identically to a returned False -- old copy on x
        # stays untouched either way.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          side_effect=Exception("connect_to_hublvol failed for LVS_18")), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertFalse(ret)
        drp.assert_not_called()
        self.assertEqual(occupant.secondary_node_id, "x")
        self.assertEqual(x.lvstore_stack_secondary, "occupant")
        self.assertEqual(stranded.secondary_node_id, "n1")

    def test_relocate_via_splice_resumes_teardown_after_crash_between_writes(self):
        # occupant's move was already built + committed by a PRIOR attempt
        # (forward pointer already points at stranded), but the process
        # crashed before the old copy on x was torn down -- x's backref is
        # still stale. A retry must skip re-building (already done) and go
        # straight to finishing the teardown, without erroring.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="stranded", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")  # stale -- not yet cleared
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        drp.assert_called_once()  # the deferred teardown finally runs
        self.assertEqual(stranded.secondary_node_id, "x")
        self.assertEqual(x.lvstore_stack_secondary, "stranded")
        # rebuild still runs once for stranded's own claim on x -- the
        # occupant's rebuild was already done, so it must NOT run again.
        self.assertEqual(rec.call_count, 1)

    def test_relocate_via_splice_own_rebuild_failure_after_occupant_moved(self):
        # occupant's move succeeds (evicted + rebuilt on stranded), but
        # stranded's own rebuild on the freed slot x fails.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          side_effect=[True, False]), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertFalse(ret)
        drp.assert_called_once()
        self.assertEqual(occupant.secondary_node_id, "stranded")
        # Forward pointers for stranded's own claim ARE committed (pre-build,
        # same idempotent pattern) even though the rebuild on x failed.
        self.assertEqual(stranded.secondary_node_id, "x")
        self.assertEqual(x.lvstore_stack_secondary, "stranded")
        self.assertEqual(removed.lvstore_stack_secondary, "stranded")

    def test_relocate_via_splice_tertiary_role(self):
        cl = _cluster()
        removed = _node("n1", stack_tertiary="stranded")
        stranded = _node("stranded", tertiary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", tertiary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_tertiary="occupant")
        db = FakeDB(cl, [removed, stranded, occupant, x])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "tertiary")

        self.assertTrue(ret)
        drp.assert_called_once()
        self.assertEqual(occupant.tertiary_node_id, "stranded")
        self.assertEqual(stranded.tertiary_node_id, "x")
        self.assertEqual(x.lvstore_stack_tertiary, "stranded")
        self.assertEqual(removed.lvstore_stack_tertiary, "")

    def test_relocate_free_target_never_triggers_splice_eviction(self):
        # Regression guard: when the picked target is genuinely free (no
        # backref set), _relocate_one_replica must behave exactly as before
        # -- no eviction, no extra recreate_lvstore_on_non_leader call.
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1", lvstore="LVS_p1")
        free_node = _node("n3")  # stack_secondary="" -- genuinely free
        db = FakeDB(cl, [removed, primary, free_node])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")

        self.assertTrue(ret)
        drp.assert_not_called()
        rec.assert_called_once()
        self.assertEqual(primary.secondary_node_id, "n3")
        self.assertEqual(free_node.lvstore_stack_secondary, "p1")


# ---------------------------------------------------------------------------
# Device decommission completion gate
# ---------------------------------------------------------------------------

class TestDecommissionDevices(unittest.TestCase):

    def test_first_pass_drives_devices(self):
        cl = _cluster()
        removed = _node("n1", n_devices=2, with_jm=True)
        db = FakeDB(cl, [removed])

        dc = MagicMock()

        def _set_state(dev_id, status, *args, **kwargs):
            db.get_storage_device_by_id(dev_id).status = status
            return True

        def _fail(dev_id):
            db.get_storage_device_by_id(dev_id).status = NVMeDevice.STATUS_FAILED
            return True

        dc.device_set_state.side_effect = _set_state
        dc.device_set_failed.side_effect = _fail

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc):
            ret = storage_node_ops._decommission_node_devices(removed)

        # Each device is driven ONLINE -> REMOVED -> FAILED (queuing failure
        # migration on the surviving nodes). The completion gate's early
        # `return False` is currently commented out, so the first pass reports
        # True rather than waiting for FAILED_AND_MIGRATED.
        self.assertTrue(ret)
        dc.remove_jm_device.assert_called_once()
        self.assertEqual(dc.device_set_state.call_count, 2)
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

    def test_skips_already_removed_peer_with_stale_jm_ids(self):
        # 2026-08-11 incident: an earlier-removed node can still carry the
        # currently-removed node's JM id in its own stale jm_ids (never
        # cleared on ITS OWN removal) -- get_storage_nodes_by_cluster_id
        # returns every node regardless of status, including removed ones.
        # Must be skipped outright, not "fixed" via its own (permanently
        # dead) rpc_client.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        stale_peer = _node("stale-peer", status=StorageNode.STATUS_REMOVED)
        stale_peer.jm_ids = [removed.jm_device.get_id()]
        db = FakeDB(cl, [removed, stale_peer])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs") as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        dc.remove_jm_device.assert_called_once()
        connect_mock.assert_not_called()
        # The stale peer's own bookkeeping is left alone -- it's dead, not "fixed".
        self.assertEqual(stale_peer.jm_ids, [removed.jm_device.get_id()])
        stale_peer.write_to_db.assert_not_called()


# ---------------------------------------------------------------------------
# node_removal_orchestrate — phase-5 resume gap
#
# Phase 4 flips node status to REMOVED *before* phase 5 (device/JM
# decommission) runs. "status == REMOVED" therefore means phases 1/3a/3b/4
# committed, NOT that removal is fully done -- a resumed attempt must still
# (re)run phase 5 rather than short-circuiting to "done" (2026-08-10
# incident: an RPC error mid phase 5 left a peer's lvstore un-rebuilt while
# the task still reported "Node removed").
# ---------------------------------------------------------------------------

class TestNodeRemovalOrchestrateResumesPhase5(unittest.TestCase):

    def _patch_all(self):
        return patch.multiple(
            storage_node_ops,
            DBController=DEFAULT,
            cluster_ops=DEFAULT,
            shutdown_storage_node=DEFAULT,
            _teardown_replicas_of_primary=DEFAULT,
            _relocate_replicas_hosted_on=DEFAULT,
            _finalize_node_removal=DEFAULT,
            set_node_status=DEFAULT,
            _decommission_node_devices=DEFAULT,
        )

    def test_already_removed_skips_phases_1_to_4_but_reruns_phase5(self):
        cl = _cluster()
        node = _node("n1", status=StorageNode.STATUS_REMOVED)
        db = FakeDB(cl, [node])
        with self._patch_all() as mocks:
            mocks["DBController"].return_value = db
            mocks["_decommission_node_devices"].return_value = True
            ret = storage_node_ops.node_removal_orchestrate("n1")

        self.assertTrue(ret)
        mocks["shutdown_storage_node"].assert_not_called()
        mocks["_teardown_replicas_of_primary"].assert_not_called()
        mocks["_relocate_replicas_hosted_on"].assert_not_called()
        mocks["_finalize_node_removal"].assert_not_called()
        mocks["set_node_status"].assert_not_called()
        mocks["_decommission_node_devices"].assert_called_once_with(node)

    def test_already_removed_reports_incomplete_if_phase5_fails_again(self):
        # The regression this guards: a prior attempt raised mid phase 5
        # after the status flip had already committed. The retry must
        # actually retry phase 5, not silently report done because status
        # already reads REMOVED.
        cl = _cluster()
        node = _node("n1", status=StorageNode.STATUS_REMOVED)
        db = FakeDB(cl, [node])
        with self._patch_all() as mocks:
            mocks["DBController"].return_value = db
            mocks["_decommission_node_devices"].return_value = False
            ret = storage_node_ops.node_removal_orchestrate("n1")

        self.assertFalse(ret)
        mocks["_decommission_node_devices"].assert_called_once_with(node)

    def test_fresh_removal_still_runs_all_phases_then_phase5(self):
        # Regression guard the other way: a from-scratch removal (status
        # still ONLINE) must not skip phases 1/3a/3b/4.
        cl = _cluster()
        node = _node("n1", status=StorageNode.STATUS_ONLINE)
        db = FakeDB(cl, [node])
        with self._patch_all() as mocks:
            mocks["DBController"].return_value = db
            mocks["shutdown_storage_node"].return_value = True
            mocks["_teardown_replicas_of_primary"].return_value = True
            mocks["_relocate_replicas_hosted_on"].return_value = True
            mocks["_decommission_node_devices"].return_value = True
            ret = storage_node_ops.node_removal_orchestrate("n1")

        self.assertTrue(ret)
        mocks["shutdown_storage_node"].assert_called_once()
        mocks["_teardown_replicas_of_primary"].assert_called_once()
        mocks["_relocate_replicas_hosted_on"].assert_called_once()
        mocks["_finalize_node_removal"].assert_called_once()
        mocks["set_node_status"].assert_called_once_with(
            "n1", StorageNode.STATUS_REMOVED, caused_by="remove")
        mocks["_decommission_node_devices"].assert_called_once()


# ---------------------------------------------------------------------------
# _connect_to_remote_jm_devs — bounded retry + degrade-not-crash on a
# transient RPC/DNS failure during the fallback bdev-existence poll
#
# The primary connect_device() failure already degrades gracefully (logs
# "Failed to connect to ...", sets connect_failed=True). The get_bdevs()
# poll called right after it, against the same rpc_client, hits the
# identical transport and gets a bounded retry (3 attempts, 1s apart) to
# ride out a DNS blip; only once that's exhausted does it degrade to "this
# JM not connected" instead of raising -- 2026-08-10 incident: this exact
# call raised RPCException uncaught and killed a node-removal task mid
# phase 5.
# ---------------------------------------------------------------------------

class TestConnectToRemoteJmDevsDegradesOnRpcException(unittest.TestCase):

    def _owner_setup(self, this_node_id="this-node"):
        jm_dev = JMDevice()
        jm_dev.uuid = "jm-owner"
        jm_dev.jm_bdev = "jm_owner_bdev"
        jm_dev.status = NVMeDevice.STATUS_ONLINE

        owner_node = MagicMock(spec=StorageNode)
        owner_node.get_id = MagicMock(return_value="owner-node")
        owner_node.status = StorageNode.STATUS_ONLINE
        owner_node.jm_device = jm_dev

        this_node = MagicMock(spec=StorageNode)
        this_node.get_id = MagicMock(return_value=this_node_id)
        this_node.jm_ids = []
        this_node.lvstore_stack_secondary = ""
        this_node.lvstore_stack_tertiary = ""
        this_node.remote_jm_devices = []
        rpc_client = MagicMock()
        this_node.rpc_client = MagicMock(return_value=rpc_client)

        db = MagicMock()
        db.get_jm_device_by_id.return_value = jm_dev
        db.get_storage_nodes.return_value = [owner_node]

        return this_node, rpc_client, db

    def test_get_bdevs_rpc_exception_exhausts_retries_and_does_not_raise(self):
        # Persistent failure (all 3 bounded-retry attempts fail): must
        # still degrade, not raise -- this is the exact call chain that
        # took down a live node-removal task before the retry was added.
        this_node, rpc_client, db = self._owner_setup()
        rpc_client.get_bdevs.side_effect = RPCException("connection error")

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "connect_device",
                          side_effect=RPCException("connection error")):
            result = storage_node_ops._connect_to_remote_jm_devs(
                this_node, jm_ids=["jm-owner"])

        self.assertEqual(result, [])
        # 3 bounded-retry attempts, one get_bdevs call each (remote_bdev
        # is empty so the first branch short-circuits without calling).
        self.assertEqual(rpc_client.get_bdevs.call_count, 3)

    def test_transient_failure_recovers_on_retry(self):
        # A blip that clears within the retry budget must be caught, not
        # just tolerated -- the whole point of adding the bounded retry
        # instead of degrading on the very first failure.
        this_node, rpc_client, db = self._owner_setup()
        rpc_client.get_bdevs.side_effect = [
            RPCException("connection error"),
            RPCException("connection error"),
            {"name": "remote_jm_owner_bdevn1"},
        ]

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "connect_device",
                          side_effect=RPCException("connection error")), \
             patch.object(storage_node_ops.time, "sleep"):
            result = storage_node_ops._connect_to_remote_jm_devs(
                this_node, jm_ids=["jm-owner"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].remote_bdev, "remote_jm_owner_bdevn1")
        self.assertEqual(rpc_client.get_bdevs.call_count, 3)

    def test_transient_failure_does_not_block_a_clean_connect(self):
        # Once the blip has cleared, the same code path must still succeed
        # normally -- the new guard must not swallow a real success too.
        this_node, rpc_client, db = self._owner_setup()
        rpc_client.get_bdevs.return_value = {"name": "remote_jm_owner_bdevn1"}

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "connect_device",
                          return_value="remote_jm_owner_bdevn1"):
            result = storage_node_ops._connect_to_remote_jm_devs(
                this_node, jm_ids=["jm-owner"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].remote_bdev, "remote_jm_owner_bdevn1")


class TestShrinkStatusDoesNotDeadlockRemoval(unittest.TestCase):
    """``node_removal_orchestrate`` holds ``Cluster.STATUS_IN_SHRINK`` for the
    duration of an attempt, so that the restart phases its replica relocation
    sets on an ONLINE target are honoured rather than reclaimed.

    That makes the migration runners' cluster-status gates load-bearing: the
    removal cannot finish until every data device reaches
    FAILED_AND_MIGRATED (``_decommission_node_devices``), and only
    ``tasks_runner_failed_migration`` sets that. If its gate refuses IN_SHRINK,
    the removal deadlocks against a status it set itself — and nothing in the
    integration tier would catch it, because it only bites on a real removal.
    """

    def _run_gate(self, cluster_status):
        """Drive the runner's cluster-status gate; True == it proceeded."""
        from simplyblock_core.services import tasks_runner_failed_migration as runner
        from simplyblock_core.models.job_schedule import JobSchedule

        task = MagicMock(spec=JobSchedule)
        task.node_id, task.cluster_id, task.retry = "n1", "cl-1", 0
        task.status = JobSchedule.STATUS_RUNNING

        cluster = MagicMock(spec=Cluster)
        cluster.status = cluster_status
        db = MagicMock()
        db.get_cluster_by_id.return_value = cluster
        db.get_storage_node_by_id.return_value = _node("n1", n_devices=1, with_jm=False)

        with patch.object(runner, "db", db):
            try:
                runner.task_runner(task)
            except Exception:
                # Admitted by the gate, then reached real device/DB work this
                # test deliberately does not mock. The gate's decision is
                # already recorded on task.status by that point, and it is the
                # only thing under test here.
                pass
        return task.status != JobSchedule.STATUS_SUSPENDED

    def test_failed_migration_runner_admits_in_shrink(self):
        self.assertTrue(
            self._run_gate(Cluster.STATUS_IN_SHRINK),
            "tasks_runner_failed_migration must run while the cluster is "
            "IN_SHRINK — _decommission_node_devices waits on the device states "
            "only this runner produces, so refusing here deadlocks removal")

    def test_failed_migration_runner_still_refuses_inactive(self):
        self.assertFalse(
            self._run_gate(Cluster.STATUS_SUSPENDED),
            "the gate must still hold for genuinely non-serving clusters")


if __name__ == "__main__":
    unittest.main()
