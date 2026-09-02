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
          failure_domain=-1, mgmt_ip=None, jm_vuid=0,
          is_secondary_node=False, physical_label=0):
    n = MagicMock(spec=StorageNode)
    n.uuid = node_id
    # Defaults matching the real model: without them these come back as
    # truthy child mocks, which silently steers placement code down the
    # dedicated-secondary-node / physical-label branches.
    n.is_secondary_node = is_secondary_node
    n.physical_label = physical_label
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

    def test_extra_exclude_ids_forwarded_to_candidate_search(self):
        # Regression coverage for the 2026-08-28 finding: _relocate_replica_
        # between's nested vacate must be able to rule out a node beyond
        # just the one being removed (specifically, the primary mid-
        # relocation in the enclosing call) -- see extra_exclude_ids'
        # docstring.
        cl = _cluster()
        primary = _node("p1", secondary_id="n1", tertiary_id="n9")
        removed = _node("n1")
        db = FakeDB(cl, [primary, removed])
        with patch.object(storage_node_ops, "get_secondary_nodes",
                          return_value=["n5"]) as gsn:
            got = storage_node_ops._pick_replica_relocation_node(
                primary, removed, "secondary", db, extra_exclude_ids=("n7",))
        self.assertEqual(got, "n5")
        _, kwargs = gsn.call_args
        self.assertIn("n7", kwargs["exclude_ids"])


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

    def test_hard_excludes_edge_where_p_shares_strandeds_domain(self):
        # Once spliced, p.<field> is repointed onto stranded itself (see
        # _relocate_replica_between) -- if p's own domain matches
        # stranded's, p ends up with a role-target in its own domain, the
        # same violation X's domain is already hard-excluded against. This
        # must refuse (None) even when it's the ONLY candidate edge --
        # degrading a node uninvolved in this removal is worse than
        # refusing the relocation (2026-08-28 finding: this was only ever
        # soft-scored before, and a live splice let it through).
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=3, mgmt_ip="10.0.0.9")
        p = _node("p", secondary_id="x", failure_domain=3, mgmt_ip="10.0.0.1")  # same domain as stranded
        x = _node("x", failure_domain=2, mgmt_ip="10.0.0.2")
        db = FakeDB(cl, [stranded, p, x])
        got = storage_node_ops._find_splice_target_for_relocation(stranded, "secondary", db)
        self.assertIsNone(got)

    def test_tertiary_edge_respects_secondary_host_disjointness(self):
        cl = _cluster(enable_failure_domain=True)
        stranded = _node("s", failure_domain=0, secondary_id="s_sec", mgmt_ip="10.0.0.9")
        s_sec = _node("s_sec", failure_domain=1, mgmt_ip="10.0.0.50")
        p = _node("p", tertiary_id="x", failure_domain=2, mgmt_ip="10.0.0.60")
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
        # bad_p's own domain differs from stranded's (so it isn't hard-
        # excluded), but its OWN tertiary shares stranded's domain (3) --
        # splicing stranded into bad_p's secondary slot would collide with
        # bad_p's own untouched tertiary.
        bad_p = _node("bad_p", secondary_id="bad_x", tertiary_id="bad_p_ter",
                       failure_domain=1, mgmt_ip="10.0.0.1")
        bad_p_ter = _node("bad_p_ter", failure_domain=3, mgmt_ip="10.0.0.11")
        bad_x = _node("bad_x", failure_domain=2, mgmt_ip="10.0.0.2")
        # good_p's own tertiary does NOT collide -- same domain-mismatch
        # score as bad_p/bad_x (both ends unlike stranded's domain), tied
        # only broken by the other-role preference, so it must still win.
        good_p = _node("good_p", secondary_id="good_x", tertiary_id="good_p_ter",
                        failure_domain=2, mgmt_ip="10.0.0.3")
        good_p_ter = _node("good_p_ter", failure_domain=1, mgmt_ip="10.0.0.13")
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

    def test_relocate_via_splice_repairs_occupants_other_role_afterwards(self):
        # Wiring check: after a successful splice, _relocate_one_replica must
        # call _repair_occupants_other_role_after_splice so occupant's OTHER,
        # untouched role gets a chance to be moved away from a collision with
        # stranded's domain (see that function's own docstring/tests for the
        # actual repair logic).
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
                          "_repair_occupants_other_role_after_splice") as repair:
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        repair.assert_called_once_with("occupant", "stranded", "secondary", db)

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

        def pick_side_effect(primary, exclude_node, role, db_controller, extra_exclude_ids=()):
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

    def test_relocate_via_splice_vacate_excludes_the_in_flight_occupant(self):
        # 2026-08-28 finding: occupant is being spliced onto stranded in
        # THIS call, but stranded already hosts z. Picking z's new target
        # must rule out occupant explicitly -- occupant can't simultaneously
        # be the thing moving onto stranded AND the target z vacates onto.
        # Without passing that exclusion through, the picker's one and only
        # candidate can BE occupant, and the whole splice dead-ends
        # retrying the identical failure forever instead of looking past it.
        cl = _cluster()
        removed = _node("n1", stack_secondary="stranded")
        stranded = _node("stranded", secondary_id="n1", lvstore="LVS_stranded",
                          stack_secondary="z")
        occupant = _node("occupant", secondary_id="x", lvstore="LVS_occupant")
        x = _node("x", stack_secondary="occupant")
        z = _node("z", secondary_id="stranded", lvstore="LVS_z")
        free_node = _node("free", lvstore="LVS_free")
        db = FakeDB(cl, [removed, stranded, occupant, x, z, free_node])

        def pick_side_effect(primary, exclude_node, role, db_controller, extra_exclude_ids=()):
            if primary.get_id() == "stranded":
                return "x"
            if primary.get_id() == "z":
                # The exclusion must be in place BEFORE the search runs, not
                # discovered as a dead end after the fact.
                self.assertIn("occupant", extra_exclude_ids)
                return "free"
            raise AssertionError(f"unexpected pick for {primary.get_id()}")

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          side_effect=pick_side_effect), \
             patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True), \
             patch.object(storage_node_ops, "_delete_replica_on_peer"):
            ret = storage_node_ops._relocate_one_replica(removed, "stranded", "secondary")

        self.assertTrue(ret)
        self.assertEqual(z.secondary_node_id, "free")
        self.assertEqual(stranded.secondary_node_id, "x")
        self.assertEqual(occupant.secondary_node_id, "stranded")

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

        def pick_side_effect(primary, exclude_node, role, db_controller, extra_exclude_ids=()):
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

        def pick_side_effect(primary, exclude_node, role, db_controller, extra_exclude_ids=()):
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
# Plan-driven phase 3b — with failure domains on, relocation goes through the
# global planner (simplyblock_core.controllers.replica_placement) instead of
# the per-replica greedy picker. The planner is unit-tested on its own in
# tests/unit/test_replica_placement.py; what matters here is the wiring:
# which clusters it takes, which it declines, and that the moves it plans are
# actually executed against the DB bookkeeping.
# ---------------------------------------------------------------------------

def _fd_cluster_nodes(domains=4, per_domain=3, ftt=2):
    """A cluster laid out the way cluster_activate leaves it: nodes
    round-robined across domains, secondary/tertiary one and two steps along
    that order, so every LVS starts fully domain-diverse."""
    order = [f"d{d}n{i}" for i in range(per_domain) for d in range(domains)]
    nodes = {}
    for node_id in order:
        nodes[node_id] = _node(
            node_id, lvstore=f"LVS_{node_id}",
            failure_domain=int(node_id[1]),
            mgmt_ip=f"10.0.{node_id[1]}.{node_id[-1]}")
    size = len(order)
    for k, node_id in enumerate(order):
        sec = order[(k + 1) % size]
        tert = order[(k + 2) % size] if ftt >= 2 else ""
        nodes[node_id].secondary_node_id = sec
        nodes[node_id].tertiary_node_id = tert
        nodes[sec].lvstore_stack_secondary = node_id
        if tert:
            nodes[tert].lvstore_stack_tertiary = node_id
    return nodes


def _layout_of(db, ftt=2):
    return {
        n.get_id(): (n.secondary_node_id, n.tertiary_node_id if ftt >= 2 else "")
        for n in db.nodes.values()
        if n.status != StorageNode.STATUS_REMOVED
    }


class TestRelocationPlannerApplicability(unittest.TestCase):

    def _db(self, **cluster_kwargs):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True, **cluster_kwargs)
        nodes = _fd_cluster_nodes()
        return cl, FakeDB(cl, list(nodes.values())), nodes

    def test_takes_an_fd_enabled_cluster(self):
        cl, db, nodes = self._db()
        got = storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db)
        self.assertIsNotNone(got)
        surviving_ids, fd_by_node, host_by_node, _, current_layout, ftt = got
        self.assertNotIn("d0n0", surviving_ids)
        self.assertEqual(len(surviving_ids), 11)
        self.assertEqual(ftt, 2)
        self.assertEqual(fd_by_node["d1n0"], 1)
        self.assertEqual(host_by_node["d1n0"], nodes["d1n0"].mgmt_ip)
        # the layout is read raw, still naming the node being removed
        self.assertIn("d0n0", [pl.secondary for pl in current_layout.values()])

    def test_declines_when_failure_domains_are_off(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=False)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        self.assertIsNone(
            storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db))

    def test_declines_on_a_node_without_a_domain(self):
        cl, db, nodes = self._db()
        nodes["d2n1"].failure_domain = -1
        self.assertIsNone(
            storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db))

    def test_declines_when_a_dedicated_secondary_node_exists(self):
        # Such a node may host more than one replica, breaking the
        # one-slot-per-node permutation the planner is built on.
        cl, db, nodes = self._db()
        nodes["d2n1"].is_secondary_node = True
        self.assertIsNone(
            storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db))

    def test_declines_when_a_peer_is_not_online(self):
        cl, db, nodes = self._db()
        nodes["d2n1"].status = StorageNode.STATUS_OFFLINE
        self.assertIsNone(
            storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db))

    def test_already_removed_nodes_are_not_survivors(self):
        cl, db, nodes = self._db()
        nodes["d3n2"].status = StorageNode.STATUS_REMOVED
        got = storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db)
        self.assertIsNotNone(got)
        self.assertNotIn("d3n2", got[0])

    def test_ftt1_cluster_ignores_the_tertiary(self):
        cl = _cluster(npcs=1, ft=1, enable_failure_domain=True)
        nodes = _fd_cluster_nodes(ftt=1)
        db = FakeDB(cl, list(nodes.values()))
        got = storage_node_ops._relocation_planner_inputs(nodes["d0n0"], db)
        self.assertEqual(got[5], 1)
        self.assertTrue(all(pl.tertiary == "" for pl in got[4].values()))


class TestPlanDrivenRelocation(unittest.TestCase):
    """The reported case end to end against the DB bookkeeping: 4 domains x 3
    hosts, FTT2 ("2+2"), removing one host per domain. Every surviving LVS
    must keep primary/secondary/tertiary in three distinct domains -- the
    guarantee the per-replica picker could not hold, because the repair it
    needs (swapping two replicas that are both already placed) is not
    expressible one stranded role at a time."""

    def _run_removal(self, db, nodes, victim_id, ftt=2):
        victim = nodes[victim_id]
        # phase 3a: the victim's own LVS replicas come down.
        for field, backref in (("secondary_node_id", "lvstore_stack_secondary"),
                               ("tertiary_node_id", "lvstore_stack_tertiary")):
            peer_id = getattr(victim, field)
            if peer_id and getattr(nodes[peer_id], backref) == victim_id:
                setattr(nodes[peer_id], backref, "")
            setattr(victim, field, "")
        victim.status = StorageNode.STATUS_IN_REMOVAL

        moved = []

        def _fake_move(primary_id, old_host_id, new_host_id, role, _db, _seen=None):
            field = "secondary_node_id" if role == "secondary" else "tertiary_node_id"
            backref = ("lvstore_stack_secondary" if role == "secondary"
                       else "lvstore_stack_tertiary")
            self.assertEqual(getattr(nodes[new_host_id], backref), "",
                             f"planned move onto an occupied {role} slot on {new_host_id}")
            moved.append((primary_id, role, old_host_id, new_host_id))
            setattr(nodes[primary_id], field, new_host_id)
            setattr(nodes[new_host_id], backref, primary_id)
            if getattr(nodes[old_host_id], backref) == primary_id:
                setattr(nodes[old_host_id], backref, "")
            return True

        with patch.object(storage_node_ops, "_relocate_replica_between",
                          side_effect=_fake_move):
            ret = storage_node_ops._relocate_replicas_hosted_on(victim)
        self.assertTrue(ret)

        victim.status = StorageNode.STATUS_REMOVED
        del db.nodes[victim_id]
        del nodes[victim_id]
        return moved

    def _assert_fully_diverse(self, nodes, ftt=2):
        for node_id, node in nodes.items():
            domains = [node.failure_domain, nodes[node.secondary_node_id].failure_domain]
            if ftt >= 2:
                domains.append(nodes[node.tertiary_node_id].failure_domain)
            self.assertEqual(
                len(set(domains)), len(domains),
                f"{node_id} roles share a domain: sec={node.secondary_node_id} "
                f"tert={node.tertiary_node_id} domains={domains}")
        for field, backref in (("secondary_node_id", "lvstore_stack_secondary"),
                               ("tertiary_node_id", "lvstore_stack_tertiary")):
            if ftt < 2 and field == "tertiary_node_id":
                continue
            holders = [getattr(n, field) for n in nodes.values()]
            self.assertCountEqual(holders, list(nodes), f"{field} is not a permutation")
            for node_id, node in nodes.items():
                self.assertEqual(getattr(nodes[getattr(node, field)], backref), node_id)

    def test_one_removal_per_domain_keeps_full_diversity(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        with patch.object(storage_node_ops, "DBController", return_value=db):
            for domain in range(4):
                self._run_removal(db, nodes, f"d{domain}n0")
                self._assert_fully_diverse(nodes)
        self.assertEqual(len(nodes), 8)

    def test_the_removed_nodes_backrefs_are_cleared(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        victim = nodes["d0n0"]
        with patch.object(storage_node_ops, "DBController", return_value=db):
            self._run_removal(db, nodes, "d0n0")
        self.assertEqual(victim.lvstore_stack_secondary, "")
        self.assertEqual(victim.lvstore_stack_tertiary, "")

    def test_a_failed_move_fails_the_phase_for_retry(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        victim = nodes["d0n0"]
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_relocate_replica_between",
                          return_value=False):
            ret = storage_node_ops._relocate_replicas_hosted_on(victim)
        self.assertFalse(ret)
        # nothing cleared -> the retry re-plans from the same state
        self.assertNotEqual(victim.lvstore_stack_secondary, "")

    def test_an_already_correct_cluster_plans_no_moves(self):
        # Removing a node whose slots are already free and whose own replicas
        # are already torn down must not churn the rest of the cluster.
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        with patch.object(storage_node_ops, "DBController", return_value=db):
            moved = self._run_removal(db, nodes, "d0n0")
        # exactly the two roles the victim hosted, plus whatever re-shuffle
        # full diversity needs -- never the whole cluster.
        self.assertLess(len(moved), 8, moved)
        self.assertGreaterEqual(len(moved), 2, moved)

    def test_falls_back_to_the_greedy_path_when_the_planner_declines(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=False)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        victim = nodes["d0n0"]
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "_relocate_one_replica",
                          return_value=True) as one:
            ret = storage_node_ops._relocate_replicas_hosted_on(victim)
        self.assertTrue(ret)
        self.assertEqual(one.call_count, 2)


class TestFeasibilityUsesThePlanner(unittest.TestCase):

    def test_admits_a_removal_the_planner_can_satisfy(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes()
        db = FakeDB(cl, list(nodes.values()))
        with patch.object(storage_node_ops, "_pick_replica_relocation_node") as pick:
            ok, reason = storage_node_ops._check_replica_relocation_feasible(
                nodes["d0n0"], db)
        self.assertTrue(ok, reason)
        pick.assert_not_called()

    def test_admits_but_warns_when_full_diversity_is_unreachable(self):
        # 2 domains at FTT2: a tertiary can never avoid both the primary's
        # and the secondary's domain. Admitted (host-disjointness still
        # holds) but every degraded LVS is named in the log.
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes(domains=2, per_domain=3)
        db = FakeDB(cl, list(nodes.values()))
        with self.assertLogs(storage_node_ops.logger, level="WARNING") as logs:
            ok, _ = storage_node_ops._check_replica_relocation_feasible(
                nodes["d0n0"], db)
        self.assertTrue(ok)
        self.assertTrue(any("cannot be made fully domain-diverse" in line
                            for line in logs.output))

    def test_refuses_when_no_host_disjoint_layout_exists(self):
        cl = _cluster(npcs=2, ft=2, enable_failure_domain=True)
        nodes = _fd_cluster_nodes(domains=3, per_domain=1)
        db = FakeDB(cl, list(nodes.values()))
        ok, reason = storage_node_ops._check_replica_relocation_feasible(
            nodes["d0n0"], db)
        self.assertFalse(ok)
        self.assertTrue(reason)


# ---------------------------------------------------------------------------
# _repair_occupants_other_role_after_splice — a splice edge protects the node
# actually being relocated (and, since the diversity fix, prefers one where
# the occupant it repoints stays diverse too) but still accepts a colliding
# edge as a last resort. This actively closes that gap: after the splice,
# check whether occupant's OTHER, untouched role now shares a domain with
# the node it was just repointed onto, and if so, relocate that role too via
# the same picker + mover (2026-08-28 finding, following directly from the
# "prefer, don't require" splice fix).
# ---------------------------------------------------------------------------

class TestRepairOccupantsOtherRoleAfterSplice(unittest.TestCase):

    def test_no_op_when_occupants_other_role_does_not_collide(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        ter = _node("ter", failure_domain=2)  # no collision with primary's domain(3)
        occupant = _node("occupant", failure_domain=1, secondary_id="primary", tertiary_id="ter")
        db = FakeDB(cl, [primary, ter, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node") as pick, \
             patch.object(storage_node_ops, "_relocate_replica_between") as move:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        pick.assert_not_called()
        move.assert_not_called()

    def test_relocates_occupants_other_role_when_it_collides(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        old_ter = _node("old_ter", failure_domain=3)  # collides with primary's domain
        occupant = _node("occupant", failure_domain=1, secondary_id="primary", tertiary_id="old_ter")
        db = FakeDB(cl, [primary, old_ter, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="replacement") as pick, \
             patch.object(storage_node_ops, "_relocate_replica_between",
                          return_value=True) as move:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        pick.assert_called_once_with(occupant, old_ter, "tertiary", db)
        move.assert_called_once_with("occupant", "old_ter", "replacement", "tertiary", db)

    def test_tertiary_role_checks_secondary_as_the_other_role(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        old_sec = _node("old_sec", failure_domain=3)
        occupant = _node("occupant", failure_domain=1, tertiary_id="primary", secondary_id="old_sec")
        db = FakeDB(cl, [primary, old_sec, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="replacement") as pick, \
             patch.object(storage_node_ops, "_relocate_replica_between",
                          return_value=True) as move:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "tertiary", db)
        pick.assert_called_once_with(occupant, old_sec, "secondary", db)
        move.assert_called_once_with("occupant", "old_sec", "replacement", "secondary", db)

    def test_logs_warning_when_no_replacement_found(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        old_ter = _node("old_ter", failure_domain=3)
        occupant = _node("occupant", failure_domain=1, secondary_id="primary", tertiary_id="old_ter")
        db = FakeDB(cl, [primary, old_ter, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node", return_value=None), \
             patch.object(storage_node_ops, "_relocate_replica_between") as move, \
             patch.object(storage_node_ops, "logger") as log:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        move.assert_not_called()
        log.warning.assert_called_once()

    def test_logs_warning_when_relocation_itself_fails(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        old_ter = _node("old_ter", failure_domain=3)
        occupant = _node("occupant", failure_domain=1, secondary_id="primary", tertiary_id="old_ter")
        db = FakeDB(cl, [primary, old_ter, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node",
                          return_value="replacement"), \
             patch.object(storage_node_ops, "_relocate_replica_between", return_value=False), \
             patch.object(storage_node_ops, "logger") as log:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        log.warning.assert_called_once()

    def test_no_op_when_fd_disabled(self):
        cl = _cluster(enable_failure_domain=False)
        primary = _node("primary", failure_domain=3)
        old_ter = _node("old_ter", failure_domain=3)
        occupant = _node("occupant", failure_domain=1, secondary_id="primary", tertiary_id="old_ter")
        db = FakeDB(cl, [primary, old_ter, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node") as pick:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        pick.assert_not_called()

    def test_no_op_when_occupant_has_no_other_role_assigned(self):
        cl = _cluster(enable_failure_domain=True)
        primary = _node("primary", failure_domain=3)
        occupant = _node("occupant", failure_domain=1, secondary_id="primary")  # no tertiary at all
        db = FakeDB(cl, [primary, occupant])
        with patch.object(storage_node_ops, "_pick_replica_relocation_node") as pick:
            storage_node_ops._repair_occupants_other_role_after_splice(
                "occupant", "primary", "secondary", db)
        pick.assert_not_called()


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


# ---------------------------------------------------------------------------
# _relocate_replica_between — vacating one role must not tear down the stack
# when the SAME node still holds the OTHER role for the SAME primary.
#
# Live regression (2026-09-01, 12-node/4-domain FTT2). The global planner
# emits both of a primary's roles in one removal; for pq8h9/LVS_45 it emitted
#   1. secondary: 94dht -> fvgtl   (fvgtl already held LVS_45 as tertiary)
#   2. tertiary:  fvgtl -> nq2mm
# Step 2's teardown fired on fvgtl because it only consulted
# lvstore_stack_tertiary, and bdev_raid_delete(raid0_45) removed the very
# replica step 1 had just promoted -- 1s after nq2mm's copy was built:
#   14:50:26 nq2mm bdev_raid_create raid0_45
#   14:50:27 fvgtl bdev_raid_delete raid0_45
# pq8h9 was left recorded as FTT2 with a single physical replica. Secondary
# and tertiary of one primary are ONE stack (raid0_<vuid> + LVS_<vuid>), so
# the guard has to be per-primary, not per-role.
# ---------------------------------------------------------------------------
class TestRelocateReplicaBetweenSamePrimaryOtherRole(unittest.TestCase):

    def _run(self, x_stack_secondary, x_stack_tertiary, role="tertiary"):
        cl = _cluster()
        primary = _node("P", lvstore="LVS_P", secondary_id="X", tertiary_id="X")
        x = _node("X", stack_secondary=x_stack_secondary, stack_tertiary=x_stack_tertiary)
        y = _node("Y")
        db = FakeDB(cl, [primary, x, y])
        with patch.object(storage_node_ops, "recreate_lvstore_on_non_leader",
                          return_value=True), \
             patch.object(storage_node_ops, "_delete_replica_on_peer") as drp, \
             patch.object(storage_node_ops,
                          "_teardown_lvol_subsystems_on_vacated_peer") as tls, \
             patch.object(storage_node_ops, "_prune_stale_lvstore_ports") as psp, \
             patch.object(storage_node_ops, "_update_lvol_nodes_for_replica_move"):
            ret = storage_node_ops._relocate_replica_between("P", "X", "Y", role, db)
        return ret, x, primary, drp, tls, psp

    def test_keeps_stack_when_node_still_holds_other_role_for_same_primary(self):
        # Moving P's TERTIARY off X while X is still P's SECONDARY.
        ret, x, primary, drp, tls, psp = self._run(
            x_stack_secondary="P", x_stack_tertiary="P", role="tertiary")

        self.assertTrue(ret)
        drp.assert_not_called()
        tls.assert_not_called()
        psp.assert_not_called()
        # Only the role being vacated is cleared; the other stays.
        self.assertEqual(x.lvstore_stack_tertiary, "")
        self.assertEqual(x.lvstore_stack_secondary, "P")
        self.assertEqual(primary.tertiary_node_id, "Y")

    def test_keeps_stack_in_the_mirror_case_moving_secondary_away(self):
        # Same shape with the roles swapped: moving P's SECONDARY off X while
        # X remains P's TERTIARY.
        ret, x, primary, drp, tls, psp = self._run(
            x_stack_secondary="P", x_stack_tertiary="P", role="secondary")

        self.assertTrue(ret)
        drp.assert_not_called()
        tls.assert_not_called()
        psp.assert_not_called()
        self.assertEqual(x.lvstore_stack_secondary, "")
        self.assertEqual(x.lvstore_stack_tertiary, "P")
        self.assertEqual(primary.secondary_node_id, "Y")

    def test_still_tears_down_when_node_holds_no_other_role(self):
        # Control: the ordinary case must be unchanged.
        ret, x, _p, drp, tls, psp = self._run(
            x_stack_secondary="", x_stack_tertiary="P", role="tertiary")

        self.assertTrue(ret)
        drp.assert_called_once()
        tls.assert_called_once()
        psp.assert_called_once()
        self.assertEqual(x.lvstore_stack_tertiary, "")

    def test_still_tears_down_when_other_role_belongs_to_a_different_primary(self):
        # The guard must compare the PRIMARY, not merely "other backref set".
        # X hosting some unrelated primary's secondary shares no stack with P,
        # so P's tertiary copy on X must still be torn down.
        ret, x, _p, drp, tls, psp = self._run(
            x_stack_secondary="OTHER", x_stack_tertiary="P", role="tertiary")

        self.assertTrue(ret)
        drp.assert_called_once()
        tls.assert_called_once()
        psp.assert_called_once()
        self.assertEqual(x.lvstore_stack_tertiary, "")
        self.assertEqual(x.lvstore_stack_secondary, "OTHER")


# ---------------------------------------------------------------------------
# replica_stack_violations / _verify_replica_stacks — the invariant whose
# absence let the bug above ship silently. Bookkeeping agreed with itself at
# every layer; only the device knew.
# ---------------------------------------------------------------------------
class TestReplicaStackViolations(unittest.TestCase):

    def _cluster_nodes(self):
        # P1 hosted by A (secondary) and B (tertiary); P2 hosted by A (tertiary).
        p1 = _node("P1", lvstore="LVS_1", secondary_id="A", tertiary_id="B")
        p2 = _node("P2", lvstore="LVS_2", tertiary_id="A")
        a = _node("A", lvstore="LVS_A", stack_secondary="P1", stack_tertiary="P2")
        b = _node("B", lvstore="LVS_B", stack_tertiary="P1")
        return [p1, p2, a, b]

    def test_no_violations_when_every_claimed_stack_is_present(self):
        nodes = self._cluster_nodes()
        self.assertEqual(
            storage_node_ops.replica_stack_violations(nodes, lambda n, lvs: True), [])

    def test_flags_a_claimed_but_absent_stack(self):
        nodes = self._cluster_nodes()

        def present(node, lvstore):
            # Exactly the live failure: A is recorded as P1's secondary but
            # LVS_1 is not on it.
            return not (node.get_id() == "A" and lvstore == "LVS_1")

        self.assertEqual(
            storage_node_ops.replica_stack_violations(nodes, present),
            [("A", "LVS_1", "P1", "secondary")])

    def test_reports_every_missing_stack_not_just_the_first(self):
        nodes = self._cluster_nodes()
        found = storage_node_ops.replica_stack_violations(nodes, lambda n, lvs: False)
        self.assertEqual(
            sorted(found),
            sorted([("A", "LVS_1", "P1", "secondary"),
                    ("A", "LVS_2", "P2", "tertiary"),
                    ("B", "LVS_1", "P1", "tertiary")]))

    def test_ignores_nodes_that_claim_nothing(self):
        idle = _node("idle", lvstore="LVS_idle")
        calls = []

        def present(node, lvstore):
            calls.append((node.get_id(), lvstore))
            return True

        self.assertEqual(
            storage_node_ops.replica_stack_violations([idle], present), [])
        self.assertEqual(calls, [], "a node with no back-references must not be probed")

    def test_ignores_a_backref_whose_owner_has_no_lvstore(self):
        # Nothing to probe for -- a different kind of bookkeeping problem, and
        # reporting it here would be a false positive on the stack invariant.
        owner = _node("P", lvstore="")
        host = _node("H", stack_secondary="P")
        self.assertEqual(
            storage_node_ops.replica_stack_violations([owner, host],
                                                      lambda n, lvs: False), [])

    def test_ignores_a_backref_to_a_node_not_in_the_online_set(self):
        host = _node("H", stack_secondary="gone")
        self.assertEqual(
            storage_node_ops.replica_stack_violations([host], lambda n, lvs: False), [])


class TestVerifyReplicaStacks(unittest.TestCase):

    def _db(self, probe_result):
        cl = _cluster()
        p = _node("P", lvstore="LVS_P", secondary_id="A")
        a = _node("A", lvstore="LVS_A", stack_secondary="P")
        a.rpc_client.return_value.bdev_lvol_get_lvstores = MagicMock(**probe_result)
        return FakeDB(cl, [p, a]), a

    def test_reports_a_missing_stack(self):
        db, _a = self._db({"return_value": []})
        self.assertEqual(
            storage_node_ops._verify_replica_stacks("cluster-1", db),
            [("A", "LVS_P", "P", "secondary")])

    def test_clean_when_the_probe_finds_the_stack(self):
        db, _a = self._db({"return_value": [{"name": "LVS_P"}]})
        self.assertEqual(storage_node_ops._verify_replica_stacks("cluster-1", db), [])

    def test_an_unreachable_node_is_not_reported_as_a_violation(self):
        # Absence of proof is not proof of absence. A probe that cries wolf on
        # a transient RPC error is a check people learn to ignore.
        db, _a = self._db({"side_effect": RPCConnectionError("node unreachable")})
        self.assertEqual(storage_node_ops._verify_replica_stacks("cluster-1", db), [])

    def test_offline_nodes_are_not_probed(self):
        cl = _cluster()
        p = _node("P", lvstore="LVS_P", secondary_id="A")
        a = _node("A", status=StorageNode.STATUS_OFFLINE, stack_secondary="P")
        a.rpc_client.return_value.bdev_lvol_get_lvstores = MagicMock(return_value=[])
        db = FakeDB(cl, [p, a])
        self.assertEqual(storage_node_ops._verify_replica_stacks("cluster-1", db), [])


# ---------------------------------------------------------------------------
# jc_remove_jm — hand the JM back to JC before deleting its bdev.
#
# JC holds an open descriptor + IO channel on the JM bdev. Deleting the bdev
# first leaves JC naming something that no longer exists (observed live
# 2026-09-02: a peer's JC member list carried a remote_jm_* absent from that
# node's own bdev_get_bdevs). jc_remove_jm closes JC's side, and its -22 is
# positive proof that some jm_vuid still references the JM -- including one the
# control plane cannot enumerate, because a vuid whose primary was already
# removed appears in no `decisions` entry and under no back-reference.
# ---------------------------------------------------------------------------
class TestJcRemoveJmBeforeBdevDelete(unittest.TestCase):

    def _rpc(self, jc_remove_jm):
        rpc = MagicMock()
        rpc.jc_remove_jm = jc_remove_jm
        rpc.jc_replace_jm = MagicMock(return_value=True)
        rpc.bdev_nvme_detach_controller = MagicMock(return_value=True)
        rpc.get_bdevs = MagicMock(return_value=[])
        return rpc

    def _run(self, jc_remove_jm):
        """Drive _decommission_node_jm with one peer that needs its JM replaced."""
        cl = _cluster()
        removed = _node("dead", with_jm=True, jm_vuid=9, lvstore="LVS_9", failure_domain=1)
        peer = _node("peer", with_jm=True, jm_vuid=37, lvstore="LVS_37", failure_domain=2)
        spare = _node("spare", with_jm=True, jm_vuid=41, lvstore="LVS_41", failure_domain=3)
        peer.jm_ids = ["jm-dead", "jm-peer"]
        removed.jm_ids = ["jm-dead"]
        spare.jm_ids = ["jm-spare"]
        rd = RemoteJMDevice()
        rd.uuid = "jm-dead"
        rd.remote_bdev = "remote_jm_deadn1"
        peer.remote_jm_devices = [rd]
        rpc = self._rpc(jc_remove_jm)
        peer.rpc_client = MagicMock(return_value=rpc)

        db = FakeDB(cl, [removed, peer, spare])
        db.get_jm_device_by_id = MagicMock(
            side_effect=lambda i: {"jm-dead": removed.jm_device,
                                  "jm-peer": peer.jm_device,
                                  "jm-spare": spare.jm_device}.get(i))
        new_rd = RemoteJMDevice()
        new_rd.uuid = "jm-spare"
        new_rd.remote_bdev = "remote_jm_sparen1"
        # Faithful to _connect_to_remote_jm_devs' delta mode: it carries the
        # existing different-owner entries over untouched and adds the new one.
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller"), \
             patch.object(storage_node_ops, "get_sorted_ha_jms", return_value=["jm-spare"]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[rd, new_rd]):
            storage_node_ops._decommission_node_jm(removed)
        return rpc, peer

    def test_jc_remove_jm_is_called_before_the_bdev_is_deleted(self):
        calls = []
        rpc, peer = self._run(
            jc_remove_jm=MagicMock(side_effect=lambda n: calls.append(("remove", n)) or True))
        rpc.bdev_nvme_detach_controller.side_effect = None
        self.assertEqual(rpc.jc_remove_jm.call_args[0][0], "remote_jm_deadn1")
        rpc.bdev_nvme_detach_controller.assert_called_once_with("remote_jm_dead")

    def test_minus_22_leaves_the_bdev_in_place(self):
        # The whole point: a jm_vuid we could not enumerate still references
        # this JM. Deleting the bdev now is what corrupts JC's view.
        rpc, peer = self._run(
            jc_remove_jm=MagicMock(side_effect=RPCRemoteError("in use", -22)))
        rpc.bdev_nvme_detach_controller.assert_not_called()
        self.assertIn("jm-dead", [rd.uuid for rd in peer.remote_jm_devices],
                      "the bookkeeping entry must survive too -- the bdev is still there")

    def test_other_jc_errors_also_leave_the_bdev_in_place(self):
        # -13 deliberately excluded: see the dedicated test below.
        for code in (-3, -6, -10, -12, -21):
            with self.subTest(code=code):
                rpc, _peer = self._run(
                    jc_remove_jm=MagicMock(side_effect=RPCRemoteError("nope", code)))
                rpc.bdev_nvme_detach_controller.assert_not_called()

    def test_minus_13_not_used_by_jc_is_the_success_path(self):
        # Measured live 2026-09-02 on spdk R26.3: a jc_replace_jm that swaps
        # the JM out of every vuid on the node ALSO drops it from JC, so the
        # follow-up jc_remove_jm finds nothing and answers -13 -- on every
        # node. Treating that as a failure strands the bdev and its
        # remote_jm_devices entry on every peer, which is the opposite of what
        # this whole sequence is for.
        rpc, peer = self._run(
            jc_remove_jm=MagicMock(side_effect=RPCRemoteError("not used by JC", -13)))
        rpc.bdev_nvme_detach_controller.assert_called_once_with("remote_jm_dead")
        self.assertNotIn("jm-dead", [rd.uuid for rd in peer.remote_jm_devices])

    def test_a_raised_exception_leaves_the_bdev_in_place(self):
        rpc, _peer = self._run(
            jc_remove_jm=MagicMock(side_effect=RPCConnectionError("unreachable")))
        rpc.bdev_nvme_detach_controller.assert_not_called()

    def test_unsupported_build_keeps_the_previous_behaviour(self):
        # spdk:main-latest as of 2026-09-02 has no jc_remove_jm. Degrade to the
        # old delete rather than stranding every superseded controller forever.
        from simplyblock_core.rpc_client import RPC_UNSUPPORTED
        rpc, peer = self._run(jc_remove_jm=MagicMock(return_value=RPC_UNSUPPORTED))
        rpc.bdev_nvme_detach_controller.assert_called_once_with("remote_jm_dead")
        uuids = [rd.uuid for rd in peer.remote_jm_devices]
        self.assertNotIn("jm-dead", uuids, "the dead JM's entry must be dropped")
        self.assertIn("jm-spare", uuids, "the replacement's entry must survive")


# ---------------------------------------------------------------------------
# The leftover vuid: removed_node's OWN jm_vuid survives on the peers that
# hosted its replica, still naming the dying JM. Its primary is removed_node
# (not in live_nodes, so in no `decisions` entry) and phase 3a has already
# cleared its back-reference -- so Pass 2 cannot reach it through either
# source. It must be covered in the SAME jc_replace_jm call as that peer's
# other vuids, or jc_replace_jm's -17 rejects the whole batch.
#
# Live 2026-09-02: the one peer whose replace failed -17 was exactly the peer
# hosting the removed node's own lvstore.
# ---------------------------------------------------------------------------
class TestLeftoverVuidOnReplicaPeers(unittest.TestCase):

    def _run(self, replica_peer_ids):
        cl = _cluster()
        removed = _node("dead", with_jm=True, jm_vuid=2, lvstore="LVS_2", failure_domain=1)
        peer = _node("peer", with_jm=True, jm_vuid=37, lvstore="LVS_37", failure_domain=2)
        spare = _node("spare", with_jm=True, jm_vuid=41, lvstore="LVS_41", failure_domain=3)
        removed.jm_ids = ["jm-dead"]
        peer.jm_ids = ["jm-dead", "jm-peer"]
        spare.jm_ids = ["jm-spare"]
        rd = RemoteJMDevice()
        rd.uuid = "jm-dead"
        rd.remote_bdev = "remote_jm_deadn1"
        peer.remote_jm_devices = [rd]
        rpc = MagicMock()
        rpc.jc_replace_jm = MagicMock(return_value=True)
        rpc.jc_remove_jm = MagicMock(return_value=True)
        rpc.bdev_nvme_detach_controller = MagicMock(return_value=True)
        rpc.get_bdevs = MagicMock(return_value=[])
        peer.rpc_client = MagicMock(return_value=rpc)

        db = FakeDB(cl, [removed, peer, spare])
        db.get_jm_device_by_id = MagicMock(
            side_effect=lambda i: {"jm-dead": removed.jm_device,
                                  "jm-peer": peer.jm_device,
                                  "jm-spare": spare.jm_device}.get(i))
        new_rd = RemoteJMDevice()
        new_rd.uuid = "jm-spare"
        new_rd.remote_bdev = "remote_jm_sparen1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller"), \
             patch.object(storage_node_ops, "get_sorted_ha_jms", return_value=["jm-spare"]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[rd, new_rd]):
            storage_node_ops._decommission_node_jm(
                removed, replica_peer_ids=replica_peer_ids)
        return rpc

    def test_replica_peer_gets_the_leftover_vuid_in_the_same_call(self):
        rpc = self._run(("peer",))
        rpc.jc_replace_jm.assert_called_once()
        kw = rpc.jc_replace_jm.call_args.kwargs
        vuids = sorted(r["jm_vuid"] for r in kw["replacements"])
        self.assertEqual(vuids, [2, 37],
                         "one call must cover the peer's own vuid 37 AND the "
                         "removed node's leftover vuid 2")
        self.assertEqual(kw["name_old"], "remote_jm_deadn1")

    def test_without_the_captured_peer_ids_the_leftover_is_missed(self):
        # Exactly the pre-fix behaviour, and what produced the live -17.
        rpc = self._run(())
        vuids = sorted(r["jm_vuid"] for r in rpc.jc_replace_jm.call_args.kwargs["replacements"])
        self.assertEqual(vuids, [37])

    def test_leftover_coverage_lets_jc_remove_jm_run_and_the_bdev_be_deleted(self):
        rpc = self._run(("peer",))
        rpc.jc_remove_jm.assert_called_once_with("remote_jm_deadn1")
        rpc.bdev_nvme_detach_controller.assert_called_once_with("remote_jm_dead")

    def test_a_non_replica_peer_is_unaffected(self):
        # A node that never hosted the removed node's replica has no leftover
        # vuid, so nothing extra may be added to its batch.
        rpc = self._run(("someone-else",))
        vuids = sorted(r["jm_vuid"] for r in rpc.jc_replace_jm.call_args.kwargs["replacements"])
        self.assertEqual(vuids, [37])

    def test_removed_node_whose_own_jm_ids_lack_the_dead_jm_does_not_abort_phase_2(self):
        # Regression, found live 2026-09-02. The removed node is itself in
        # live_nodes (status in_removal), so if the leftover replacement is
        # stored in `decisions` it becomes a normal Pass-2 consumer -- and the
        # unguarded node.jm_ids.remove(removed_jm_id) then raised ValueError,
        # aborting phase 2 before ANY peer was patched. Strictly worse than the
        # gap it was meant to close.
        cl = _cluster()
        removed = _node("dead", with_jm=True, jm_vuid=2, lvstore="LVS_2", failure_domain=1)
        peer = _node("peer", with_jm=True, jm_vuid=37, lvstore="LVS_37", failure_domain=2)
        spare = _node("spare", with_jm=True, jm_vuid=41, lvstore="LVS_41", failure_domain=3)
        # The removed node's OWN jm_ids does NOT list its own dying JM.
        removed.jm_ids = ["jm-other-a", "jm-other-b"]
        removed.status = StorageNode.STATUS_IN_REMOVAL
        peer.jm_ids = ["jm-dead", "jm-peer"]
        spare.jm_ids = ["jm-spare"]
        rd = RemoteJMDevice()
        rd.uuid = "jm-dead"
        rd.remote_bdev = "remote_jm_deadn1"
        peer.remote_jm_devices = [rd]
        rpc = MagicMock()
        rpc.jc_replace_jm = MagicMock(return_value=True)
        rpc.jc_remove_jm = MagicMock(return_value=True)
        rpc.bdev_nvme_detach_controller = MagicMock(return_value=True)
        rpc.get_bdevs = MagicMock(return_value=[])
        peer.rpc_client = MagicMock(return_value=rpc)

        db = FakeDB(cl, [removed, peer, spare])
        db.get_jm_device_by_id = MagicMock(
            side_effect=lambda i: {"jm-dead": removed.jm_device,
                                  "jm-peer": peer.jm_device,
                                  "jm-spare": spare.jm_device}.get(i))
        new_rd = RemoteJMDevice()
        new_rd.uuid = "jm-spare"
        new_rd.remote_bdev = "remote_jm_sparen1"
        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "device_controller"), \
             patch.object(storage_node_ops, "get_sorted_ha_jms", return_value=["jm-spare"]), \
             patch.object(storage_node_ops, "_connect_to_remote_jm_devs",
                          return_value=[rd, new_rd]):
            # Must not raise.
            storage_node_ops._decommission_node_jm(removed, replica_peer_ids=("peer",))

        # And the peer must still have been patched, covering both vuids.
        rpc.jc_replace_jm.assert_called_once()
        vuids = sorted(r["jm_vuid"] for r in rpc.jc_replace_jm.call_args.kwargs["replacements"])
        self.assertEqual(vuids, [2, 37])
        self.assertEqual(removed.jm_ids, ["jm-other-a", "jm-other-b"],
                         "the removed node's unrelated jm_ids must be left alone")


class TestOrchestratorCapturesReplicaPeersBeforeTeardown(unittest.TestCase):

    def test_peer_ids_are_captured_before_phase_3a_clears_them(self):
        # phase 3a clears snode.secondary_node_id/_tertiary_node_id, so the
        # capture has to happen first or phase 2 gets an empty tuple.
        cl = _cluster()
        snode = _node("dead", secondary_id="sec", tertiary_id="ter",
                      with_jm=True, jm_vuid=2, lvstore="LVS_2")
        sec = _node("sec", stack_secondary="dead")
        ter = _node("ter", stack_tertiary="dead")
        db = FakeDB(cl, [snode, sec, ter])
        seen = {}

        def fake_teardown(node):
            # emulate phase 3a wiping the pointers
            snode.secondary_node_id = ""
            snode.tertiary_node_id = ""
            return True

        with patch.object(storage_node_ops, "DBController", return_value=db), \
             patch.object(storage_node_ops, "cluster_ops"), \
             patch.object(storage_node_ops, "shutdown_storage_node", return_value=True), \
             patch.object(storage_node_ops, "set_node_status"), \
             patch.object(storage_node_ops, "_teardown_replicas_of_primary",
                          side_effect=fake_teardown), \
             patch.object(storage_node_ops, "_decommission_node_jm",
                          side_effect=lambda n, replica_peer_ids=(): seen.update(
                              ids=replica_peer_ids)), \
             patch.object(storage_node_ops, "_relocate_replicas_hosted_on", return_value=True), \
             patch.object(storage_node_ops, "_verify_replica_stacks", return_value=[]), \
             patch.object(storage_node_ops, "_finalize_node_removal"), \
             patch.object(storage_node_ops, "_decommission_node_devices", return_value=True):
            storage_node_ops.node_removal_orchestrate("dead")

        self.assertEqual(sorted(seen.get("ids", ())), ["sec", "ter"])


class TestJcRemoveJmClient(unittest.TestCase):
    """The client wrapper's contract: success / unsupported / coded error."""

    def _client(self, response):
        from simplyblock_core import rpc_client as rc
        c = rc.RPCClient.__new__(rc.RPCClient)
        c._request2 = MagicMock(return_value=response)  # type: ignore[method-assign]
        return c

    def test_success_returns_the_result(self):
        self.assertTrue(self._client((True, None)).jc_remove_jm("remote_jm_xn1"))

    def test_method_not_found_returns_the_unsupported_sentinel(self):
        from simplyblock_core.rpc_client import RPC_UNSUPPORTED
        c = self._client((None, {"code": -32601, "message": "Method not found"}))
        self.assertEqual(c.jc_remove_jm("remote_jm_xn1"), RPC_UNSUPPORTED)

    def test_still_in_use_raises_with_code_22(self):
        c = self._client((None, {"code": -22, "message": "still in use"}))
        with self.assertRaises(RPCRemoteError) as ctx:
            c.jc_remove_jm("remote_jm_xn1")
        self.assertEqual(ctx.exception.code, -22)

    def test_other_errors_raise_with_their_code(self):
        c = self._client((None, {"code": -12, "message": "another removal in progress"}))
        with self.assertRaises(RPCRemoteError) as ctx:
            c.jc_remove_jm("remote_jm_xn1")
        self.assertEqual(ctx.exception.code, -12)


if __name__ == "__main__":
    unittest.main()
