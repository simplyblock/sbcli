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
from unittest.mock import DEFAULT, MagicMock, call, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteJMDevice
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.rpc_client import RPCConnectionError, RPCException, RPCRemoteError


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
          failure_domain=-1, mgmt_ip=None, jm_vuid=0):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    n.get_id = MagicMock(return_value=node_id)
    n.status = status
    n.cluster_id = "cluster-1"
    n.lvstore = lvstore
    n.jm_vuid = jm_vuid
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
        tc.get_active_node_restart_task.return_value =  []
        tc.get_active_lvol_migration.return_value =  []
        tc.add_node_removal_task.return_value = patches.get("task_id", "task-uuid-1")
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "shutdown_storage_node", return_value=True), \
             patch.object(storage_node_ops, "set_node_status", return_value=True), \
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

    # -----------------------------------------------------------------
    # P's OWN other role is PREFERRED to stay diverse from stranded once
    # P's `field` is repointed onto it, but this is a soft preference, not
    # a hard filter -- regression coverage for the 2026-08-27 live finding:
    # splicing kc25l into 56mg5's secondary slot collided with 56mg5's
    # pre-existing, untouched tertiary in the same domain, when another
    # edge elsewhere in the ring was collision-free the whole time. The old
    # avoid_domains-only check had no way to prefer it (it only ever looked
    # at X's domain, never P's) -- but an outright reject would have been
    # more restrictive than useful, since a real cluster usually has
    # several candidate edges and one of them is typically clean.
    # -----------------------------------------------------------------

    def test_prefers_edge_whose_p_stays_diverse_over_one_that_collides(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=3, tertiary_id="s_ter", mgmt_ip="10.0.0.9")
        s_ter = _node("s_ter", failure_domain=4, mgmt_ip="10.0.0.10")
        # bad_p's own tertiary shares stranded's domain (3) -- splicing
        # stranded into bad_p's secondary slot would collide with bad_p's
        # own untouched tertiary. bad_p/bad_x score higher on the plain
        # domain-mismatch metric (both ends fully unlike stranded's own
        # domain), so the old code picked this edge unconditionally.
        bad_p = _node("bad_p", secondary_id="bad_x", tertiary_id="bad_p_ter",
                       failure_domain=1, mgmt_ip="10.0.0.1")
        bad_p_ter = _node("bad_p_ter", failure_domain=3, mgmt_ip="10.0.0.11")
        bad_x = _node("bad_x", failure_domain=2, mgmt_ip="10.0.0.2")
        # good_p's own tertiary does NOT collide -- lower domain-mismatch
        # score (good_p shares stranded's domain), but must still win since
        # it leaves nothing degraded.
        good_p = _node("good_p", secondary_id="good_x", tertiary_id="good_p_ter",
                        failure_domain=3, mgmt_ip="10.0.0.3")
        good_p_ter = _node("good_p_ter", failure_domain=2, mgmt_ip="10.0.0.13")
        good_x = _node("good_x", failure_domain=4, mgmt_ip="10.0.0.4")
        db = FakeDB(cl, [stranded, s_ter, bad_p, bad_p_ter, bad_x, good_p, good_p_ter, good_x])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertEqual(got, ("good_p", "good_x"))

    def test_falls_back_to_colliding_edge_with_warning_when_its_the_only_one(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=3, tertiary_id="s_ter", mgmt_ip="10.0.0.9")
        s_ter = _node("s_ter", failure_domain=4, mgmt_ip="10.0.0.10")
        p = _node("p", secondary_id="x", tertiary_id="p_ter", failure_domain=1, mgmt_ip="10.0.0.1")
        p_ter = _node("p_ter", failure_domain=3, mgmt_ip="10.0.0.20")
        x = _node("x", failure_domain=2, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [stranded, s_ter, p, p_ter, x])
        with patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertEqual(got, ("p", "x"))
        log.warning.assert_called_once()

    def test_uses_edge_when_ps_other_role_does_not_collide(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=3, tertiary_id="s_ter", mgmt_ip="10.0.0.9")
        s_ter = _node("s_ter", failure_domain=4, mgmt_ip="10.0.0.10")
        p = _node("p", secondary_id="x", tertiary_id="p_ter", failure_domain=1, mgmt_ip="10.0.0.1")
        p_ter = _node("p_ter", failure_domain=2, mgmt_ip="10.0.0.20")  # no collision
        x = _node("x", failure_domain=2, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [stranded, s_ter, p, p_ter, x])
        with patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertEqual(got, ("p", "x"))
        log.warning.assert_not_called()

    def test_falls_back_to_colliding_tertiary_edge_when_its_the_only_one(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=3, secondary_id="s_sec", mgmt_ip="10.0.0.9")
        s_sec = _node("s_sec", failure_domain=4, mgmt_ip="10.0.0.10")
        # p's OWN secondary shares stranded's domain (3) -- splicing stranded
        # into p's tertiary slot collides with p's own untouched secondary,
        # but it's the only edge available, so it's used anyway.
        p = _node("p", tertiary_id="x", secondary_id="p_sec", failure_domain=1, mgmt_ip="10.0.0.60")
        p_sec = _node("p_sec", failure_domain=3, mgmt_ip="10.0.0.70")
        x = _node("x", failure_domain=2, mgmt_ip="10.0.0.61")
        db = FakeDB(cl, [stranded, s_sec, p, p_sec, x])
        with patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._find_splice_target_for_relocation(stranded, "tertiary", db)
        self.assertEqual(got, ("p", "x"))
        log.warning.assert_called_once()


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
# _pick_replica_relocation_node — full pairwise diversity across
# {primary, secondary, tertiary}.
#
# Regression coverage for the 2026-08-27 finding: the old logic only ever
# enforced ">=1 cross-domain role" -- once the OTHER already-assigned role
# happened to be cross-domain, the role actually being relocated was placed
# on cands[0] with zero domain check, so it could land in the primary's own
# domain or in the other role's domain. These tests cover the tiered
# replacement: (1) prefer a direct candidate diverse from BOTH the primary
# and the other role, (2) splice for the same full diversity, (3) relax to
# the old weaker floor (diverse from the primary alone) only when full
# diversity is unreachable anywhere, logging the degraded outcome.
# ---------------------------------------------------------------------------

class TestPickReplicaRelocationFullDiversity(unittest.TestCase):

    def test_prefers_direct_candidate_diverse_from_both_primary_and_other_role(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", tertiary_id="t1",
                         failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        other = _node("t1", failure_domain=1, mgmt_ip="10.0.0.50")
        same_as_other = _node("bad", failure_domain=1, mgmt_ip="10.0.0.2")
        fully_diverse = _node("good", failure_domain=2, mgmt_ip="10.0.0.3")
        db = FakeDB(cl, [primary, removed, other, same_as_other, fully_diverse])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["bad", "good"]):
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "good")

    def test_falls_back_to_splice_for_full_diversity_when_direct_only_matches_other_role(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", tertiary_id="t1",
                         failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        other = _node("t1", failure_domain=1, mgmt_ip="10.0.0.50")
        same_as_other = _node("bad", failure_domain=1, mgmt_ip="10.0.0.2")
        edge_p = _node("edge_p", secondary_id="edge_x", failure_domain=3, mgmt_ip="10.0.0.10")
        edge_x = _node("edge_x", failure_domain=2, mgmt_ip="10.0.0.11")
        db = FakeDB(cl, [primary, removed, other, same_as_other, edge_p, edge_x])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["bad"]):
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "edge_x")

    def test_relaxes_to_weaker_floor_with_warning_when_full_diversity_unreachable(self):
        # Only domains 0 (primary) and 1 (other role + every candidate)
        # exist -- no direct candidate or splice target can be diverse from
        # BOTH roles, so this must relax to "diverse from the primary alone"
        # rather than refuse the relocation outright.
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", tertiary_id="t1",
                         failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        other = _node("t1", failure_domain=1, mgmt_ip="10.0.0.50")
        weak_cand = _node("weak_cand", failure_domain=1, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [primary, removed, other, weak_cand])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["weak_cand"]), \
             patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "weak_cand")
        log.warning.assert_called_once()

    def test_relaxes_to_weaker_splice_with_warning_when_full_diversity_unreachable(self):
        # No direct candidate at all; the only splice target's far end sits
        # in the other role's domain, so full diversity is unreachable --
        # must relax to the weaker splice (diverse from the primary alone)
        # instead of returning None.
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", tertiary_id="t1",
                         failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        other = _node("t1", failure_domain=1, mgmt_ip="10.0.0.50")
        edge_p = _node("edge_p", secondary_id="edge_x", failure_domain=3, mgmt_ip="10.0.0.10")
        edge_x = _node("edge_x", failure_domain=1, mgmt_ip="10.0.0.11")
        db = FakeDB(cl, [primary, removed, other, edge_p, edge_x])
        with patch.object(storage_node_ops, "get_secondary_nodes", return_value=[]), \
             patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "edge_x")
        log.warning.assert_called_once()

    def test_no_relaxation_warning_when_no_other_role_is_assigned(self):
        # With no other role placed yet, full_avoid == weak_avoid already --
        # this is the ordinary single-constraint case, not a degraded
        # fallback, so no warning should fire.
        cl = _cluster(enable_failure_domain=True)
        primary = _node("p1", secondary_id="n1", failure_domain=0, mgmt_ip="10.0.0.9")
        removed = _node("n1", failure_domain=1, mgmt_ip="10.0.0.99")
        cand = _node("cand", failure_domain=1, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [primary, removed, cand])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["cand"]), \
             patch.object(storage_node_ops, "logger") as log:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db)
        self.assertEqual(got, "cand")
        log.warning.assert_not_called()


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
        # Case A: removed IS the node going away -- destroying its lvstore
        # here is correct (default destroy_lvstore=True, not overridden).
        drp.assert_any_call(sec, removed, cl)
        drp.assert_any_call(tert, removed, cl)


# ---------------------------------------------------------------------------
# _delete_replica_on_peer — the peer-side hublvol+bdev teardown _teardown_
# replicas_of_primary and _relocate_replica_between both call.
#
# peer's own hublvol subsystem/bdev teardown (subsystem_get/subsystem_delete/
# bdev_lvol_delete_hublvol) is deliberately commented out as of f5a052f3 --
# that subsystem has no consumers and is harmless to leave until peer's next
# restart -- so these tests do NOT assert those calls.
#
# Regression coverage for a real bug found live (2026-08-14): peer's NVMe-oF
# controller connecting TO primary's hublvol (kept live the whole time peer
# held this replica) was never detached on eviction. Left dangling, it can
# later be found wedged in a non-enabled state when peer is re-selected to
# host a replica again, and the reconcile's detach-and-wait-gone can then
# time out and abort the rebuild -- this exact sequence took ffznh's SPDK
# down after a splice eviction left this connection behind.
# ---------------------------------------------------------------------------

class TestDeleteReplicaOnPeer(unittest.TestCase):

    def test_detaches_hublvol_controller_when_present(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        primary.hublvol = MagicMock(bdev_name="LVS_1/hublvol")
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.bdev_nvme_detach_controller.assert_called_once_with("LVS_1/hublvol")

    def test_skips_hublvol_detach_when_primary_has_no_hublvol(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        primary.hublvol = None
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.bdev_nvme_detach_controller.assert_not_called()

    def test_hublvol_detach_failure_is_caught_not_raised(self):
        # Best-effort: an RPC failure here must not propagate and block removal.
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        primary.hublvol = MagicMock(bdev_name="LVS_1/hublvol")
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        rpc.bdev_nvme_detach_controller.side_effect = RPCConnectionError("connection error")
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)  # must not raise

    def test_no_op_when_primary_has_no_lvstore(self):
        cl = _cluster()
        primary = _node("p1", lvstore="")
        primary.hublvol = MagicMock(bdev_name="LVS_1/hublvol")
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.bdev_nvme_detach_controller.assert_not_called()

    # -----------------------------------------------------------------
    # destroy_lvstore -- regression coverage for a real bug found live
    # (2026-08-16): the splice/relocation eviction path called this with
    # the default (destroy) behavior, which calls bdev_lvol_delete_lvstore
    # on a peer holding only a non-leader examine copy -- destroying the
    # SHARED on-disk blobstore metadata out from under the still-live
    # primary elsewhere. This corrupted LVS_1's on-disk metadata during a
    # splice eviction, surfacing later as a superblock validation failure
    # when the primary tried to reload it on restart.
    # -----------------------------------------------------------------

    def test_destroy_lvstore_default_true_deletes_shared_blobstore(self):
        # Case A (node removal): primary IS the node going away, so
        # destroying its lvstore here is correct.
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl)
        rpc.bdev_lvol_delete_lvstore.assert_called_once_with("LVS_1")

    def test_destroy_lvstore_false_never_deletes_shared_blobstore(self):
        # Splice/relocation eviction: primary survives, only its host on
        # this peer is moving -- the shared blobstore must NOT be touched,
        # only the local raid/distrib examine bdevs hot-removed.
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1", lvstore="LVS_1")
        rpc = peer.rpc_client()
        storage_node_ops._delete_replica_on_peer(peer, primary, cl, destroy_lvstore=False)
        rpc.bdev_lvol_delete_lvstore.assert_not_called()
        rpc.bdev_raid_delete.assert_called_once_with("raid_1")
        rpc.bdev_distrib_delete.assert_called_once_with("distrib_1")


# ---------------------------------------------------------------------------
# _update_lvol_nodes_for_replica_move — regression coverage for a real bug
# found live (2026-08-18): a node-removal relocation correctly repointed the
# storage-node-level secondary_node_id/tertiary_node_id bookkeeping, but left
# every LVol hosted on the surviving primary with the just-removed/vacated
# host still listed in its own `nodes` -- the field the CSI/host initiator
# actually connects to for multipath failover. That stranded a live lvol on
# a single path (the primary only) with nothing ever repointing it, since
# nothing re-syncs `nodes` later. Mirrors the identical fix already applied
# for expansion-triggered rebalancing in cluster_expansion/executor.py.
# ---------------------------------------------------------------------------

def _lvol(node_id, nodes, lvol_id=None, nqn=None):
    lv = MagicMock()
    lv.nodes = list(nodes)
    lv.write_to_db = MagicMock()
    lv.get_id = MagicMock(return_value=lvol_id or f"lvol-{node_id}")
    lv.nqn = nqn or f"nqn:{node_id}"
    return lv


class TestUpdateLvolNodesForReplicaMove(unittest.TestCase):

    def test_repoints_old_host_to_new_host(self):
        cl = _cluster()
        db = FakeDB(cl, [], lvols={"p1": [_lvol("p1", ["p1", "old"])]})
        storage_node_ops._update_lvol_nodes_for_replica_move("p1", "old", "new", db)
        lvol = db.lvols["p1"][0]
        self.assertEqual(lvol.nodes, ["p1", "new"])
        lvol.write_to_db.assert_called_once()

    def test_leaves_unrelated_hosts_untouched(self):
        cl = _cluster()
        db = FakeDB(cl, [], lvols={"p1": [_lvol("p1", ["p1", "old", "tert"])]})
        storage_node_ops._update_lvol_nodes_for_replica_move("p1", "old", "new", db)
        self.assertEqual(db.lvols["p1"][0].nodes, ["p1", "new", "tert"])

    def test_no_op_when_old_host_not_present(self):
        # e.g. a tertiary-only move must not touch an lvol with no tertiary.
        cl = _cluster()
        lvol = _lvol("p1", ["p1", "sec"])
        db = FakeDB(cl, [], lvols={"p1": [lvol]})
        storage_node_ops._update_lvol_nodes_for_replica_move("p1", "old", "new", db)
        self.assertEqual(lvol.nodes, ["p1", "sec"])
        lvol.write_to_db.assert_not_called()

    def test_multiple_lvols_on_the_same_primary_all_updated(self):
        cl = _cluster()
        lvols = [_lvol("p1", ["p1", "old"]) for _ in range(3)]
        db = FakeDB(cl, [], lvols={"p1": lvols})
        storage_node_ops._update_lvol_nodes_for_replica_move("p1", "old", "new", db)
        for lvol in lvols:
            self.assertEqual(lvol.nodes, ["p1", "new"])

    def test_redundant_call_is_a_safe_no_op(self):
        # Simulates a retry after an earlier attempt already applied it.
        cl = _cluster()
        lvol = _lvol("p1", ["p1", "new"])
        db = FakeDB(cl, [], lvols={"p1": [lvol]})
        storage_node_ops._update_lvol_nodes_for_replica_move("p1", "old", "new", db)
        self.assertEqual(lvol.nodes, ["p1", "new"])
        lvol.write_to_db.assert_not_called()


# ---------------------------------------------------------------------------
# _teardown_lvol_subsystems_on_vacated_peer — regression coverage for a real
# bug found live (2026-08-18): _delete_replica_on_peer(destroy_lvstore=False)
# tears down the vacated peer's raid/distrib bdev stack (which cascades to
# remove each hosted lvol's namespace), but never touches the per-lvol NVMe-
# oF subsystem+listener registered separately via add_lvol_thread. Left
# behind, the listener keeps accepting connections in front of a now-empty
# subsystem, and since the peer is no longer in lvol.nodes nothing ever
# tells the CSI/host initiator to drop that connection either -- the volume
# carries a third, live-but-empty path indefinitely alongside its correct
# two.
# ---------------------------------------------------------------------------

class TestTeardownLvolSubsystemsOnVacatedPeer(unittest.TestCase):

    def test_deletes_subsystem_for_every_lvol_hosted_on_the_primary(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1")
        rpc = peer.rpc_client()
        lvols = [
            _lvol("p1", ["p1", "peer1"], lvol_id="lv-a", nqn="nqn:a"),
            _lvol("p1", ["p1", "peer1"], lvol_id="lv-b", nqn="nqn:b"),
        ]
        db = FakeDB(cl, [primary, peer], lvols={"p1": lvols})
        storage_node_ops._teardown_lvol_subsystems_on_vacated_peer(peer, primary, db)
        rpc.subsystem_delete.assert_any_call("nqn:a")
        rpc.subsystem_delete.assert_any_call("nqn:b")
        self.assertEqual(rpc.subsystem_delete.call_count, 2)

    def test_no_op_when_primary_hosts_no_lvols(self):
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1")
        rpc = peer.rpc_client()
        db = FakeDB(cl, [primary, peer], lvols={})
        storage_node_ops._teardown_lvol_subsystems_on_vacated_peer(peer, primary, db)
        rpc.subsystem_delete.assert_not_called()

    def test_rpc_failure_on_one_lvol_does_not_block_the_others(self):
        # Best-effort: an RPC failure here must not propagate and must not
        # stop the remaining lvols from being cleaned up.
        cl = _cluster()
        primary = _node("p1", lvstore="LVS_1")
        peer = _node("peer1")
        rpc = peer.rpc_client()
        rpc.subsystem_delete.side_effect = [RPCConnectionError("connection error"), None]
        lvols = [
            _lvol("p1", ["p1", "peer1"], lvol_id="lv-a", nqn="nqn:a"),
            _lvol("p1", ["p1", "peer1"], lvol_id="lv-b", nqn="nqn:b"),
        ]
        db = FakeDB(cl, [primary, peer], lvols={"p1": lvols})
        storage_node_ops._teardown_lvol_subsystems_on_vacated_peer(peer, primary, db)  # must not raise
        self.assertEqual(rpc.subsystem_delete.call_count, 2)


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

    def test_relocate_repoints_primarys_lvol_nodes_off_the_removed_host(self):
        # Regression: the direct (non-splice) relocation path must repoint
        # every LVol hosted on the primary from the removed host to the new
        # one -- see TestUpdateLvolNodesForReplicaMove's docstring.
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1", lvstore="LVS_p1")
        new = _node("n3")
        lvol = _lvol("p1", ["p1", "n1"])
        db = FakeDB(cl, [removed, primary, new], lvols={"p1": [lvol]})
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True):
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")
        self.assertTrue(ret)
        self.assertEqual(lvol.nodes, ["p1", "n3"])

    def test_relocate_failure_does_not_repoint_lvol_nodes(self):
        # A failed rebuild must leave the lvol pointed at the still-intact
        # old copy -- nothing to repoint to yet.
        cl = _cluster()
        removed = _node("n1", stack_secondary="p1")
        primary = _node("p1", secondary_id="n1", lvstore="LVS_p1")
        new = _node("n3")
        lvol = _lvol("p1", ["p1", "n1"])
        db = FakeDB(cl, [removed, primary, new], lvols={"p1": [lvol]})
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="n3"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=False):
            ret = storage_node_ops._relocate_one_replica(removed, "p1", "secondary")
        self.assertFalse(ret)
        self.assertEqual(lvol.nodes, ["p1", "n1"])


class TestRecreateOnNonLeaderReconnectsRegardlessOfActivationMode(unittest.TestCase):
    """2026-08-25 regression: _relocate_one_replica's call into
    recreate_lvstore_on_non_leader always passes activation_mode=False (the
    primary is online and stays leader during a live relocation, so the
    peer-quiescing steps activation_mode skips are still needed) -- but the
    soft-reconnect prelude (remote devices + remote JMs) is a SEPARATE,
    documented-idempotent concern that must run every time, not just during
    cluster_activate(). Gating it behind activation_mode left a node newly
    taking over a relocated secondary/tertiary replica silently missing
    connections to redundancy-set peers it had no other prior reason to
    already be connected to (found live 2026-08-25 via a node-removal
    ripple: a relocated host's JM stayed unreachable because this prelude
    never ran for it).
    """

    def test_soft_reconnect_prelude_runs_with_activation_mode_false(self):
        snode = MagicMock(spec=StorageNode)
        snode.get_id = MagicMock(return_value="snode-1")
        snode.lvstore = ""
        snode.lvstore_stack_secondary = ""
        snode.lvstore_stack_tertiary = ""
        snode.raid = "raid_1"
        snode.rpc_client = MagicMock(return_value=MagicMock())
        snode.write_to_db = MagicMock()

        db = MagicMock()
        db.get_storage_node_by_id = MagicMock(return_value=snode)
        db.get_lvols_by_node_id = MagicMock(return_value=[])

        primary_node = MagicMock()
        primary_node.get_id = MagicMock(return_value="primary-1")
        primary_node.raid = "raid_1"
        primary_node.lvstore = "LVS_1"
        primary_node.lvstore_stack = []

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_connect_to_remote_devs",
                          return_value=["dev-sentinel"]) as devs_mock, \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=["jm-sentinel"]) as jms_mock, \
             patch.object(storage_node_ops, "_set_restart_phase"), \
             patch.object(storage_node_ops, "_create_bdev_stack",
                          return_value=(False, "boom")):
            ret = storage_node_ops._recreate_lvstore_on_non_leader_impl(
                snode, leader_node=primary_node, primary_node=primary_node,
                activation_mode=False)

        # _create_bdev_stack was made to fail so the function returns early,
        # right after the prelude -- proving the prelude itself ran (not
        # that the whole rebuild happened to succeed).
        self.assertFalse(ret)
        devs_mock.assert_called_once_with(snode, reattach=False)
        jms_mock.assert_called_once_with(snode)
        self.assertEqual(snode.remote_devices, ["dev-sentinel"])
        self.assertEqual(snode.remote_jm_devices, ["jm-sentinel"])


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
        # occupant's old replica torn down off x -- must NOT destroy the
        # shared lvstore: occupant survives this relocation, only its host
        # moves (2026-08-16: destroying it here corrupted the on-disk
        # blobstore for a still-live primary).
        drp.assert_called_once_with(x, occupant, cl, destroy_lvstore=False)
        self.assertEqual(occupant.secondary_node_id, "stranded")  # occupant re-homed onto stranded
        self.assertEqual(stranded.secondary_node_id, "x")  # stranded takes over x's freed slot
        self.assertEqual(x.lvstore_stack_secondary, "stranded")
        self.assertEqual(rec.call_count, 2)  # occupant's rebuild + stranded's own rebuild
        self.assertEqual(removed.lvstore_stack_secondary, "")

    def test_relocate_via_splice_repoints_lvol_nodes_for_both_moved_primaries(self):
        # Regression: BOTH moves this splice performs -- occupant's replica
        # x -> stranded, and stranded's own replica n1 -> x -- must repoint
        # every lvol hosted on their respective primaries. See
        # TestUpdateLvolNodesForReplicaMove's docstring.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        occupant_lvol = _lvol("occupant", ["occupant", "x"])
        stranded_lvol = _lvol("stranded", ["stranded", "n1"])
        db = FakeDB(cl, [removed, stranded, occupant, x],
                     lvols={"occupant": [occupant_lvol], "stranded": [stranded_lvol]})
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="x"), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True), \
             patch.object(storage_node_ops, "_delete_replica_on_peer"):
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        self.assertEqual(occupant_lvol.nodes, ["occupant", "stranded"])
        self.assertEqual(stranded_lvol.nodes, ["stranded", "x"])

    def test_relocate_via_splice_tears_down_lvol_subsystems_on_the_vacated_peer(self):
        # Regression: evicting occupant's replica off x must also delete
        # occupant's own lvols' NVMe-oF subsystems on x -- see
        # TestTeardownLvolSubsystemsOnVacatedPeer's docstring. Must NOT run
        # for stranded's own vacate-of-n1 (n1 is the node being removed,
        # already shut down, not a surviving peer to clean up on).
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
                          return_value=True), \
             patch.object(storage_node_ops, "_delete_replica_on_peer"), \
             patch.object(storage_node_ops,
                          "_teardown_lvol_subsystems_on_vacated_peer") as teardown:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        teardown.assert_called_once_with(x, occupant, db)

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

    def test_relocate_via_splice_vacates_strandeds_preexisting_occupant_first(self):
        # 2026-08-12 live incident: `stranded` already hosts an unrelated
        # occupant `z` via its own back-reference BEFORE this removal starts
        # -- every node in a full ring already hosts someone. Splicing
        # `occupant` onto `stranded` without first moving `z` elsewhere
        # would silently drop z's back-reference (a single-value field
        # can't hold both z and occupant): live symptom was `sn list`
        # showing two different primaries both claiming the same secondary
        # node, and a physically-live replica invisible to lvstore_ports.
        # `z` must be relocated first -- onto a genuinely free node -- before
        # `occupant` claims the freed slot.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded",
                          stack_secondary="z")  # already hosting z
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        z = _node("z", secondary_id="stranded", lvstore="LVS_z")
        free_node = _node("free", lvstore="LVS_free")  # genuinely unclaimed
        db = FakeDB(cl, [removed, stranded, occupant, x, z, free_node])

        def pick_side_effect(primary, exclude_node, role, db_controller):
            if primary.get_id() == "stranded":
                self.assertEqual(exclude_node.get_id(), "n1")
                return "x"
            if primary.get_id() == "z":
                self.assertEqual(exclude_node.get_id(), "stranded")
                return "free"
            raise AssertionError(f"unexpected pick for {primary.get_id()}")

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          side_effect=pick_side_effect), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        # z relocated off stranded onto the genuinely free node.
        self.assertEqual(z.secondary_node_id, "free")
        self.assertEqual(free_node.lvstore_stack_secondary, "z")
        # stranded's slot no longer holds the stale "z" -- it now correctly
        # reflects occupant, the relationship this splice actually created.
        self.assertEqual(stranded.lvstore_stack_secondary, "occupant")
        # occupant evicted off x onto the now-vacated stranded.
        self.assertEqual(occupant.secondary_node_id, "stranded")
        # stranded's own claim on x (the originally-picked splice target).
        self.assertEqual(stranded.secondary_node_id, "x")
        self.assertEqual(x.lvstore_stack_secondary, "stranded")
        self.assertEqual(removed.lvstore_stack_secondary, "")
        self.assertEqual(rec.call_count, 3)  # z's + occupant's + stranded's own rebuild
        self.assertEqual(drp.call_count, 2)  # old z copy off stranded, old occupant copy off x

    def test_relocate_via_splice_refuses_when_preexisting_occupant_has_no_target(self):
        # Same setup, but z has nowhere to go. Must fail closed -- refuse the
        # whole splice rather than overload stranded's single-value slot.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded",
                          stack_secondary="z")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        z = _node("z", secondary_id="stranded", lvstore="LVS_z")
        db = FakeDB(cl, [removed, stranded, occupant, x, z])

        def pick_side_effect(primary, exclude_node, role, db_controller):
            if primary.get_id() == "stranded":
                return "x"
            if primary.get_id() == "z":
                return None  # nothing free anywhere for z
            raise AssertionError(f"unexpected pick for {primary.get_id()}")

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          side_effect=pick_side_effect), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertFalse(ret)
        drp.assert_not_called()
        rec.assert_not_called()
        # Nothing committed -- z, occupant, x, stranded, removed all untouched.
        self.assertEqual(z.secondary_node_id, "stranded")
        self.assertEqual(stranded.lvstore_stack_secondary, "z")
        self.assertEqual(occupant.secondary_node_id, "x")
        self.assertEqual(stranded.secondary_node_id, "n1")
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

    def test_relocate_via_splice_tertiary_vacates_strandeds_preexisting_occupant_first(self):
        # FTT2 (dual fault tolerance) variant of
        # test_relocate_via_splice_vacates_strandeds_preexisting_occupant_first:
        # secondary and tertiary live in separate fields on every node, so a
        # node hosting one primary's secondary AND a different primary's
        # tertiary at once is fine -- that was never the collision. The
        # collision is within a single field, and the cascade fix is
        # parameterized by role throughout, so this exercises the same path
        # for lvstore_stack_tertiary specifically.
        cl = _cluster()
        removed = _node("n1", stack_tertiary="stranded")
        stranded = _node("stranded", tertiary_id="n1", lvstore="LVS_stranded",
                          stack_tertiary="z")  # already hosting z's tertiary
        occupant = _node("occupant", tertiary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_tertiary="occupant")
        z = _node("z", tertiary_id="stranded", lvstore="LVS_z")
        free_node = _node("free", lvstore="LVS_free")
        db = FakeDB(cl, [removed, stranded, occupant, x, z, free_node])

        def pick_side_effect(primary, exclude_node, role, db_controller):
            self.assertEqual(role, "tertiary")
            if primary.get_id() == "stranded":
                return "x"
            if primary.get_id() == "z":
                return "free"
            raise AssertionError(f"unexpected pick for {primary.get_id()}")

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          side_effect=pick_side_effect), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True) as rec, \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "tertiary")

        self.assertTrue(ret)
        self.assertEqual(z.tertiary_node_id, "free")
        self.assertEqual(free_node.lvstore_stack_tertiary, "z")
        self.assertEqual(stranded.lvstore_stack_tertiary, "occupant")
        self.assertEqual(occupant.tertiary_node_id, "stranded")
        self.assertEqual(stranded.tertiary_node_id, "x")
        self.assertEqual(x.lvstore_stack_tertiary, "stranded")
        self.assertEqual(removed.lvstore_stack_tertiary, "")
        self.assertEqual(rec.call_count, 3)
        self.assertEqual(drp.call_count, 2)

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

    def test_refreshes_peer_holding_dead_jm_only_via_hosted_primary(self):
        # 2026-08-14 incident: _connect_to_remote_jm_devs populates
        # remote_jm_devices from TWO sources -- a node's own jm_ids (its
        # redundancy set for its own JM), AND, separately, whichever
        # primary it hosts as secondary/tertiary pulls in THAT primary's
        # jm_ids too (lvstore_stack_secondary/_tertiary). A peer reachable
        # only through the second path never touches its own jm_ids at
        # all, so the jm_ids-only guard above never even looks at it, and
        # its remote_jm_devices entry for the dead JM is left stale
        # forever. A plain removal that never reshuffles who-hosts-whom
        # never surfaces this (peer's remote_jm_devices happens to already
        # be right) -- it takes a splice reshuffle to expose it.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        peer = _node("peer1", stack_secondary="some-primary")
        peer.jm_ids = []  # clean -- the dead JM was never in ITS OWN set
        stale_remote = RemoteJMDevice()
        stale_remote.uuid = removed.jm_device.get_id()
        peer.remote_jm_devices = [stale_remote]
        db = FakeDB(cl, [removed, peer])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[]) as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        dc.remove_jm_device.assert_called_once()
        connect_mock.assert_called_once_with(peer, peer.jm_ids)
        self.assertEqual(peer.remote_jm_devices, [])
        peer.write_to_db.assert_called_once()

    def test_does_not_refresh_peer_with_no_dead_jm_reference_at_all(self):
        # Regression guard for the new elif's condition itself: a peer with
        # neither the dead JM in its own jm_ids NOR in remote_jm_devices
        # must be left completely untouched.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        peer = _node("peer1")
        peer.jm_ids = []
        peer.remote_jm_devices = []
        db = FakeDB(cl, [removed, peer])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs") as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        connect_mock.assert_not_called()
        peer.write_to_db.assert_not_called()

    def test_picks_replacement_connects_it_and_calls_jc_replace_jm(self):
        # Baseline: the replacement is connected under its OWN natural name
        # (no override), and jc_replace_jm is told to swap consumer's live
        # JC member from whatever it currently is (name_old, taken from
        # consumer's own remote_jm_devices record) to that new name, via a
        # single-entry replacements list keyed by consumer's own jm_vuid.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True, jm_vuid=7)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        consumer.remote_jm_devices = [live_old]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        replacement.jm_device.jm_bdev = "jm_replacement"
        db = FakeDB(cl, [removed, consumer, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]) as sorted_jms_mock, \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]) as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        # No more `limit=` -- the batched-replacements RPC no longer needs
        # a caller-side retry pool (see rpc_client.jc_replace_jm).
        sorted_jms_mock.assert_called_once_with(consumer)
        connect_mock.assert_called_once_with(
            consumer, jm_ids=[replacement.jm_device.get_id()],
            only_node_id=replacement.get_id())
        consumer.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[{"jm_vuid": 7, "name_new": "remote_jm_replacementn1"}])
        self.assertIn(replacement.jm_device.get_id(), consumer.jm_ids)
        self.assertNotIn(removed.jm_device.get_id(), consumer.jm_ids)
        self.assertEqual(consumer.remote_jm_devices, [connected_new])
        # The now-superseded connection to the removed node is torn down --
        # jc_replace_jm only swaps the live JC membership, it doesn't detach
        # the bdev/controller it swapped away from.
        consumer.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")
        self.assertFalse(any(rd.uuid == removed.jm_device.get_id() for rd in consumer.remote_jm_devices))
        consumer.write_to_db.assert_called()

    def test_name_old_is_whatever_the_consumer_currently_has_live_not_removeds_own_name(self):
        # A prior replacement (back when this used the retired
        # override_name_on_node trick, or simply a longer chain of
        # replacements) can leave consumer's live JC member named something
        # that has nothing to do with removed_node's own jm_bdev. name_old
        # must reflect reality (consumer's own remote_jm_devices record),
        # never removed_node.jm_device.jm_bdev blindly.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        removed.jm_device.jm_bdev = "jm_n1"  # consumer's JC never actually used this name
        consumer = _node("consumer", n_devices=0, with_jm=True, jm_vuid=7)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "jm_A"  # the name actually live in consumer's JC
        consumer.remote_jm_devices = [live_old]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        db = FakeDB(cl, [removed, consumer, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]):
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        consumer.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="jm_A",
            replacements=[{"jm_vuid": 7, "name_new": "remote_jm_replacementn1"}])
        # "jm_A" doesn't follow the normal {controller}n1 bdev-name convention
        # (it's a legacy override-style name) -- the cleanup falls back to
        # using it as-is rather than mis-stripping a trailing "n1" that isn't
        # there.
        consumer.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("jm_A")

    def test_prefers_a_replacement_from_the_removed_nodes_own_failure_domain(self):
        # get_sorted_ha_jms ranks candidates by the CONSUMER's own domain
        # balance -- it has no notion of "a node is being removed". Without
        # re-ranking, a removal could swap a same-domain member for a
        # cross-domain one purely because of insertion-order tie-breaks
        # inside get_sorted_ha_jms, reshuffling this consumer's domain
        # distribution for no reason. Re-ranking so a same-FD-as-removed
        # candidate goes first keeps the distribution identical to what it
        # was before the removal -- the least-disruptive choice. Here
        # get_sorted_ha_jms is mocked to rank the DIFFERENT-domain candidate
        # first; the removal path must still pick "same-fd" ahead of it.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True, failure_domain=2)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        consumer.remote_jm_devices = [live_old]
        other_fd = _node("other-fd", n_devices=0, with_jm=True, failure_domain=1)
        other_fd.jm_ids = []
        other_fd.jm_device.jm_bdev = "jm_other_fd"
        same_fd = _node("same-fd", n_devices=0, with_jm=True, failure_domain=2)
        same_fd.jm_ids = []
        same_fd.jm_device.jm_bdev = "jm_same_fd"
        db = FakeDB(cl, [removed, consumer, other_fd, same_fd])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            other_fd.jm_device.get_id(): other_fd.jm_device,
            same_fd.jm_device.get_id(): same_fd.jm_device,
        }[jid])
        dc = MagicMock()
        connected_same_fd = RemoteJMDevice()
        connected_same_fd.uuid = same_fd.jm_device.get_id()
        connected_same_fd.remote_bdev = "remote_jm_same_fdn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[other_fd.jm_device.get_id(), same_fd.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_same_fd]) as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        connect_mock.assert_called_once_with(
            consumer, jm_ids=[same_fd.jm_device.get_id()], only_node_id=same_fd.get_id())
        self.assertIn(same_fd.jm_device.get_id(), consumer.jm_ids)
        self.assertNotIn(other_fd.jm_device.get_id(), consumer.jm_ids)
        consumer.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")

    def test_reusing_a_candidate_across_different_jm_vuids_is_not_avoided(self):
        # The new RPC explicitly allows the SAME name_new to cover multiple
        # jm_vuids in one call -- only reuse WITHIN one jm_vuid's own member
        # list is invalid. Two unrelated primaries, each missing the same
        # dead JM from their own redundancy set, independently land on the
        # SAME replacement candidate (get_sorted_ha_jms filtering is scoped
        # to each primary's own jm_ids, not a cluster-wide "already claimed
        # elsewhere" check) -- and neither call needs to avoid the other.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        primary_a = _node("primary-a", n_devices=0, with_jm=True, jm_vuid=1)
        primary_a.jm_ids = [removed.jm_device.get_id()]
        live_old_a = RemoteJMDevice()
        live_old_a.uuid = removed.jm_device.get_id()
        live_old_a.remote_bdev = "remote_jm_n1n1"
        primary_a.remote_jm_devices = [live_old_a]
        primary_b = _node("primary-b", n_devices=0, with_jm=True, jm_vuid=2)
        primary_b.jm_ids = [removed.jm_device.get_id()]
        live_old_b = RemoteJMDevice()
        live_old_b.uuid = removed.jm_device.get_id()
        live_old_b.remote_bdev = "remote_jm_n1n1"
        primary_b.remote_jm_devices = [live_old_b]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        db = FakeDB(cl, [removed, primary_a, primary_b, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]):
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        primary_a.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[{"jm_vuid": 1, "name_new": "remote_jm_replacementn1"}])
        primary_b.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[{"jm_vuid": 2, "name_new": "remote_jm_replacementn1"}])
        self.assertIn(replacement.jm_device.get_id(), primary_a.jm_ids)
        self.assertIn(replacement.jm_device.get_id(), primary_b.jm_ids)
        primary_a.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")
        primary_b.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")

    def test_batches_own_and_hosted_jm_vuid_replacements_into_one_call(self):
        # A node that is BOTH a primary needing repair AND hosts another
        # primary's replica also referencing the dead JM must cover both
        # jm_vuids in a SINGLE jc_replace_jm call -- the RPC rejects a call
        # that leaves any local jm_vuid using name_old uncovered (-17).
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        hosted_primary = _node("hosted-primary", n_devices=0, with_jm=True, jm_vuid=20)
        hosted_primary.jm_ids = [removed.jm_device.get_id()]
        host = _node("host", n_devices=0, with_jm=True, jm_vuid=10,
                     stack_secondary="hosted-primary")
        host.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        host.remote_jm_devices = [live_old]
        hosted_primary.remote_jm_devices = [live_old]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        db = FakeDB(cl, [removed, hosted_primary, host, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]):
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        # host's own jm_vuid (10) is appended before the hosted primary's
        # jm_vuid (20) -- own case first, then secondary/tertiary in order.
        host.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[
                {"jm_vuid": 10, "name_new": "remote_jm_replacementn1"},
                {"jm_vuid": 20, "name_new": "remote_jm_replacementn1"},
            ])
        self.assertIn(replacement.jm_device.get_id(), host.jm_ids)
        # hosted_primary's OWN jm_ids is updated by ITS OWN separate pass
        # through the loop (it's a live node in its own right), not by
        # host's call.
        self.assertIn(replacement.jm_device.get_id(), hosted_primary.jm_ids)
        # ONE detach per node, even though host's call covered TWO jm_vuid
        # targets -- both targets shared the same name_old (one physical
        # bdev can back multiple local jm_vuids at once).
        host.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")

    def test_replacement_is_hosts_own_local_jm_uses_local_name_no_remote_connect(self):
        # 2026-08-25 incident: _pick_replacement can legitimately land on
        # the HOST's own local JM as the replacement candidate for a
        # primary it hosts as secondary/tertiary (get_sorted_ha_jms has no
        # notion of "this candidate is the very node doing the patching").
        # _connect_to_remote_jm_devs deliberately skips self-connections --
        # a node never remote-attaches its own JM -- so routing this
        # candidate through it always raised "failed to connect" and left
        # the slot permanently short. The fix references it the same way
        # get_node_jm_names does for a local member: the plain jm_bdev
        # name, with no connect call at all.
        #
        # hosted_primary is a live node in its own right too, so it
        # independently applies the SAME decision to its OWN local JC copy
        # -- and from ITS perspective host's JM genuinely IS remote, so
        # that call must go through the normal connect path unaffected by
        # this fix. Both cases are exercised here to keep them distinct.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        hosted_primary = _node("hosted-primary", n_devices=0, with_jm=True, jm_vuid=20)
        hosted_primary.jm_ids = [removed.jm_device.get_id()]
        host = _node("host", n_devices=0, with_jm=True, jm_vuid=10,
                     stack_secondary="hosted-primary")
        host.jm_ids = []  # host's OWN group never referenced the dead JM
        host.jm_device.jm_bdev = "jm_host"
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        hosted_primary.remote_jm_devices = [live_old]
        host.remote_jm_devices = [live_old]
        db = FakeDB(cl, [removed, hosted_primary, host])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            host.jm_device.get_id(): host.jm_device,
        }[jid])
        dc = MagicMock()
        connected_remote = RemoteJMDevice()
        connected_remote.uuid = host.jm_device.get_id()
        connected_remote.remote_bdev = "remote_jm_hostn1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[host.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_remote]) as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        # Only ONE real connect call -- hosted_primary patching its OWN
        # local JC copy, for which host's JM is genuinely remote.
        connect_mock.assert_called_once_with(
            hosted_primary, jm_ids=[host.jm_device.get_id()],
            only_node_id=host.get_id())
        # host's copy (the self-candidate case) skips the connect entirely
        # and references its own device by its plain local name.
        host.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[{"jm_vuid": 20, "name_new": "jm_host"}])
        # hosted_primary's own copy goes through the normal remote path.
        hosted_primary.rpc_client().jc_replace_jm.assert_called_once_with(
            name_old="remote_jm_n1n1",
            replacements=[{"jm_vuid": 20, "name_new": "remote_jm_hostn1"}])
        self.assertIn(host.jm_device.get_id(), hosted_primary.jm_ids)
        host.rpc_client().bdev_nvme_detach_controller.assert_called_once_with("remote_jm_n1")

    def test_no_candidate_leaves_slot_honestly_short(self):
        # get_sorted_ha_jms comes back empty (or every ranked candidate is
        # already in consumer's own jm_ids) -- no candidate at all, so
        # nothing is even attempted; leave the redundancy slot honestly
        # short rather than claim a phantom repair.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        consumer.remote_jm_devices = [live_old]
        db = FakeDB(cl, [removed, consumer])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms", return_value=[]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs") as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        connect_mock.assert_not_called()
        self.assertNotIn(removed.jm_device.get_id(), consumer.jm_ids)
        consumer.write_to_db.assert_called()

    def test_jc_replace_jm_failure_leaves_slot_short_and_detaches_unused_connection(self):
        # The candidate connects fine but the swap itself is rejected (e.g.
        # a timeout connecting to the new JM bdev, code -6) -- don't claim
        # the replacement, and don't leave the now-unused connection
        # dangling: best-effort detach it.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        consumer.remote_jm_devices = [live_old]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        replacement.jm_device.jm_bdev = "jm_replacement"
        db = FakeDB(cl, [removed, consumer, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        consumer.rpc_client.return_value.jc_replace_jm.side_effect = RPCRemoteError(
            "timed out connecting to the new JM bdev", code=-6)
        # The replacement's bdev did not exist before this call -- the
        # connect step created it fresh, so it's eligible for cleanup.
        consumer.rpc_client.return_value.get_bdevs.return_value = None
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]):
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        self.assertNotIn(replacement.jm_device.get_id(), consumer.jm_ids)
        self.assertNotIn(removed.jm_device.get_id(), consumer.jm_ids)
        # Restored to its pre-attempt state -- the freshly-connected
        # candidate was just detached by the cleanup above, so bookkeeping
        # must not go on claiming that connection still exists.
        self.assertEqual(consumer.remote_jm_devices, [live_old])
        consumer.rpc_client.return_value.bdev_nvme_detach_controller.assert_called_once_with(
            "remote_jm_replacement")
        consumer.write_to_db.assert_called()

    def test_pre_existing_connection_skips_the_detach_cleanup_on_failure(self):
        # If the candidate's bdev already existed BEFORE this call (e.g. it's
        # legitimately serving some other JC membership, like a hosted
        # replica's own journal group), the cleanup on failure must not
        # detach it -- that connection isn't this call's to tear down.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True)
        consumer.jm_ids = [removed.jm_device.get_id()]
        live_old = RemoteJMDevice()
        live_old.uuid = removed.jm_device.get_id()
        live_old.remote_bdev = "remote_jm_n1n1"
        consumer.remote_jm_devices = [live_old]
        replacement = _node("replacement", n_devices=0, with_jm=True)
        replacement.jm_ids = []
        replacement.jm_device.jm_bdev = "jm_replacement"
        db = FakeDB(cl, [removed, consumer, replacement])
        db.get_jm_device_by_id = MagicMock(side_effect=lambda jid: {
            removed.jm_device.get_id(): removed.jm_device,
            replacement.jm_device.get_id(): replacement.jm_device,
        }[jid])
        dc = MagicMock()
        connected_new = RemoteJMDevice()
        connected_new.uuid = replacement.jm_device.get_id()
        connected_new.remote_bdev = "remote_jm_replacementn1"
        consumer.rpc_client.return_value.jc_replace_jm.side_effect = RPCRemoteError(
            "this jm_vuid uses name_new already", code=-14)
        consumer.rpc_client.return_value.get_bdevs.return_value = {"name": "remote_jm_replacementn1"}
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms",
                          return_value=[replacement.jm_device.get_id()]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[connected_new]):
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        self.assertNotIn(replacement.jm_device.get_id(), consumer.jm_ids)
        consumer.rpc_client.return_value.bdev_nvme_detach_controller.assert_not_called()

    def test_no_candidate_at_all_still_persists_the_removed_jm_id(self):
        # Regression guard for a real bug found while explaining this
        # branch: the ONLY prior action, "no jm_id found" -> logger.error,
        # never called node.write_to_db(). The node.jm_ids.remove() earlier
        # in this same code path was therefore silently discarded -- the DB
        # kept referencing a JM device that no longer exists, forever, with
        # nothing left to show even that removal was attempted. An
        # explicitly short jm_ids is a strictly more honest persisted state.
        cl = _cluster()
        removed = _node("n1", n_devices=0, with_jm=True)
        removed.jm_ids = []
        consumer = _node("consumer", n_devices=0, with_jm=True)
        consumer.jm_ids = [removed.jm_device.get_id()]
        db = FakeDB(cl, [removed, consumer])
        dc = MagicMock()
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller", dc), \
             patch.object(storage_node_ops, "get_sorted_ha_jms", return_value=[]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs") as connect_mock:
            ret = storage_node_ops._decommission_node_devices(removed)

        self.assertTrue(ret)
        self.assertNotIn(removed.jm_device.get_id(), consumer.jm_ids)
        consumer.write_to_db.assert_called()
        connect_mock.assert_not_called()


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
            _decommission_node_jm=DEFAULT,
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
        mocks["_decommission_node_jm"].assert_not_called()
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
        mocks["_decommission_node_jm"].assert_not_called()
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
        # Phase 2 runs BEFORE phase 3a/3b -- see _decommission_node_jm's
        # docstring for why relocation must never see a not-yet-patched
        # jm_ids.
        mocks["_decommission_node_jm"].assert_called_once()
        mocks["_teardown_replicas_of_primary"].assert_called_once()
        mocks["_relocate_replicas_hosted_on"].assert_called_once()
        mocks["_finalize_node_removal"].assert_called_once()
        # Two transitions: IN_REMOVAL right after shutdown (so other code /
        # monitors can see the node is mid-removal, not still ONLINE), then
        # REMOVED once phase 4 finalizes.
        self.assertEqual(mocks["set_node_status"].call_args_list, [
            call("n1", StorageNode.STATUS_IN_REMOVAL, caused_by="remove"),
            call("n1", StorageNode.STATUS_REMOVED, caused_by="remove"),
        ])
        mocks["_decommission_node_devices"].assert_called_once()

    def test_replica_teardown_then_jm_decommission_then_relocation(self):
        # 2026-08-25 incidents (two, found back to back):
        #
        # 1. Phase 3b (relocate hosted replicas) builds the new host's JC
        #    group construct from the hosted primary's CURRENT jm_ids via
        #    get_node_jm_names(), baking in whatever it finds by name
        #    regardless of whether the connection succeeds. If this node's
        #    dying JM were still listed there when 3b ran, the new host
        #    would permanently reference an unreachable member with no live
        #    connection to ever hand jc_replace_jm afterwards. JM
        #    decommission must therefore run, and complete, before 3b.
        #
        # 2. A peer hosting THIS node's own secondary/tertiary replica (Case
        #    A, torn down in 3a) runs a local JC instance for that replica
        #    too, and get_node_jm_names() bakes this node's own JM into that
        #    instance's construct too -- a second local jm_vuid on that peer
        #    sharing the same name_old, invisible to JM decommission's own
        #    target-gathering. Left standing, jc_replace_jm's own multi-
        #    target safety check rejects the whole batched call (-17). 3a
        #    must therefore run, and complete, BEFORE JM decommission --
        #    tearing the replica down removes the second instance entirely.
        #
        # Net required order: 3a, then JM decommission, then 3b.
        cl = _cluster()
        node = _node("n1", status=StorageNode.STATUS_ONLINE)
        db = FakeDB(cl, [node])
        order = []
        with self._patch_all() as mocks:
            mocks["DBController"].return_value = db
            mocks["shutdown_storage_node"].return_value = True
            mocks["_decommission_node_jm"].side_effect = \
                lambda *a, **k: order.append("jm")
            mocks["_teardown_replicas_of_primary"].side_effect = \
                lambda *a, **k: order.append("3a") or True
            mocks["_relocate_replicas_hosted_on"].side_effect = \
                lambda *a, **k: order.append("3b") or True
            mocks["_decommission_node_devices"].return_value = True
            ret = storage_node_ops.node_removal_orchestrate("n1")

        self.assertTrue(ret)
        self.assertEqual(order, ["3a", "jm", "3b"])


# ---------------------------------------------------------------------------
# _finalize_node_removal — clearing the removed node's OWN stale bookkeeping
#
# Case A/B relocation clears every forward/back-reference field as each
# relationship is moved elsewhere, but neither touches lvstore_ports -- it
# isn't part of any relocation, just a port-reuse cache for this node's own
# restarts. Left uncleared, `sn list`'s "LVS Ports" column keeps showing
# entries for a node with no SPDK process left to back them (2026-08-13,
# found live after a removal).
# ---------------------------------------------------------------------------

class TestFinalizeNodeRemovalClearsLvstorePorts(unittest.TestCase):

    def _run(self, removed, cluster_mode="kubernetes", node_api_up=False):
        cl = _cluster(mode=cluster_mode)
        db = FakeDB(cl, [removed])
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops.health_controller, "_check_node_api",
                          return_value=node_api_up):
            storage_node_ops._finalize_node_removal(removed)
        return removed

    def test_clears_stale_lvstore_ports(self):
        removed = _node("n1", lvstore="LVS_1")
        removed.lvstore_ports = {"LVS_1": {"lvol_subsys_port": 4440, "hublvol_port": 4441},
                                  "LVS_9": {"lvol_subsys_port": 4450, "hublvol_port": 4451}}
        removed = self._run(removed)
        self.assertEqual(removed.lvstore_ports, {})
        removed.write_to_db.assert_called()

    def test_no_op_when_already_empty(self):
        # Don't write to the DB at all when there's nothing to clear.
        removed = _node("n1", lvstore="LVS_1")
        removed.lvstore_ports = {}
        removed = self._run(removed)
        self.assertEqual(removed.lvstore_ports, {})
        removed.write_to_db.assert_not_called()


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


# ---------------------------------------------------------------------------
# _connect_to_remote_jm_devs — remote_device.jm_bdev must record the name
# THIS NODE actually connects under
#
# Used to matter most when an override applied (a replacement JM connecting
# under a removed peer's old bdev name, per the now-retired
# override_name_on_node -- see _decommission_node_devices for why
# jc_replace_jm replaced that trick): remote_device.jm_bdev had to record
# the resolved (override) name, not org_dev's own natural name, or
# health_controller's diagnostic controller lookup
# (f'remote_{remote_device.jm_bdev}') queried the wrong, never-connected
# name every cycle. Now there's only ever one name to record -- the owner's
# own -- but the field still has to be right for the same diagnostic lookup.
# ---------------------------------------------------------------------------

class TestConnectToRemoteJmDevsRecordsResolvedName(unittest.TestCase):
    # override_name_on_node (and the drop_stale_overrides parameter that
    # existed only to retire a stale entry) is gone now that SPDK's
    # jc_replace_jm RPC swaps a live JC member by name directly --
    # _connect_to_remote_jm_devs always connects under the owner's own
    # current name. This class is now just a baseline regression guard for
    # that natural-name path.

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

    def test_connects_under_owners_own_natural_name(self):
        this_node, rpc_client, db = self._owner_setup()
        rpc_client.get_bdevs.return_value = {"name": "remote_jm_owner_bdevn1"}

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "connect_device",
                          return_value="remote_jm_owner_bdevn1") as connect_mock:
            result = storage_node_ops._connect_to_remote_jm_devs(
                this_node, jm_ids=["jm-owner"])

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].jm_bdev, "jm_owner_bdev")
        self.assertEqual(result[0].remote_bdev, "remote_jm_owner_bdevn1")
        self.assertEqual(connect_mock.call_args[0][0], "remote_jm_owner_bdev")


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
