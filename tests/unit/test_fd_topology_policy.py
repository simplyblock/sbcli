"""Unit tests for the failure-domain topology policy.

Policy under test (2026-08-04 design decision):

* Role invariant — every LVS keeps at least one CROSS-domain non-leader
  role (``compute_fd_layout_violations``). Role placement only affects
  availability, never durability, so this is the whole role-level contract.
* Interleaved rotation — ``fd_interleaved_host_order`` +
  ``rotation_layout`` produce layouts satisfying the invariant for
  balanced and +/-1 populations, deterministically.
* Admission (+/-1 rule) — add/remove keep the per-domain HOST split within
  one host (``check_fd_admission_for_add`` / ``_for_remove``); removal
  additionally keeps >= 2 hosts per domain once an HA layout exists.
* FD migration is forbidden — a known host cannot change domains.
* Expansion planning — ``_plan_moves_with_failure_domains`` recovers the
  actual rotation from secondary pointers and refuses plans that would
  violate the invariant (e.g. FTT1 growing to odd populations).
"""

import unittest
from typing import ClassVar
from unittest.mock import MagicMock

from simplyblock_core.controllers.cluster_expansion import planner
from simplyblock_core.controllers.cluster_expansion.executor import (
    _plan_moves_with_failure_domains,
    _rotation_order_from_layout,
)
from simplyblock_core.controllers.cluster_expansion.preconditions import (
    check_fd_admission_for_add,
    check_fd_admission_for_remove,
    check_fd_balance_current,
)
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode


def _cluster(enable_failure_domain=True, ftt=2):
    c = Cluster()
    c.uuid = "cluster-1"
    c.ha_type = "ha"
    c.enable_failure_domain = enable_failure_domain
    c.max_fault_tolerance = ftt
    return c


def _node(uuid, mgmt_ip, fd, status=StorageNode.STATUS_ONLINE,
          lvstore="lvs", secondary="", tertiary="", is_secondary_node=False):
    n = StorageNode()
    n.uuid = uuid
    n.mgmt_ip = mgmt_ip
    n.failure_domain = fd
    n.status = status
    n.cluster_id = "cluster-1"
    n.lvstore = lvstore
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    n.is_secondary_node = is_secondary_node
    return n


def _db(nodes):
    db = MagicMock()
    db.get_storage_nodes_by_cluster_id.return_value = nodes
    return db


# ---------------------------------------------------------------------------
# planner.fd_interleaved_host_order
# ---------------------------------------------------------------------------

class TestInterleavedOrder(unittest.TestCase):

    def test_balanced_two_domains_alternate_perfectly(self):
        order = planner.fd_interleaved_host_order(
            [("a1", 0), ("a2", 0), ("b1", 1), ("b2", 1)])
        self.assertEqual(order, ["a1", "b1", "a2", "b2"])

    def test_plus_one_imbalance_puts_extra_host_last(self):
        order = planner.fd_interleaved_host_order(
            [("a1", 0), ("a2", 0), ("a3", 0), ("b1", 1), ("b2", 1)])
        self.assertEqual(order, ["a1", "b1", "a2", "b2", "a3"])

    def test_three_domains(self):
        order = planner.fd_interleaved_host_order(
            [("a", 0), ("b", 1), ("c", 2), ("a2", 0), ("b2", 1), ("c2", 2)])
        self.assertEqual(order, ["a", "b", "c", "a2", "b2", "c2"])

    def test_empty(self):
        self.assertEqual(planner.fd_interleaved_host_order([]), [])


# ---------------------------------------------------------------------------
# planner.rotation_layout / compute_fd_layout_violations
# ---------------------------------------------------------------------------

class TestFdLayoutInvariant(unittest.TestCase):

    FDS_2x2: ClassVar[dict] = {"a1": 0, "b1": 1, "a2": 0, "b2": 1}

    def test_balanced_interleaved_ftt2_valid(self):
        topo = [["a1"], ["b1"], ["a2"], ["b2"]]
        self.assertEqual(
            planner.compute_fd_layout_violations(topo, 2, self.FDS_2x2), [])

    def test_balanced_interleaved_ftt2_all_secondaries_cross_domain(self):
        layout = planner.rotation_layout([["a1"], ["b1"], ["a2"], ["b2"]], 2)
        for primary, (sec, _tert) in layout.items():
            self.assertNotEqual(
                self.FDS_2x2[primary], self.FDS_2x2[sec],
                f"secondary of {primary} not cross-domain")

    def test_plus_one_ftt2_valid_with_one_degraded_lvs(self):
        fds = dict(self.FDS_2x2, a3=0)
        topo = [["a1"], ["b1"], ["a2"], ["b2"], ["a3"]]
        self.assertEqual(planner.compute_fd_layout_violations(topo, 2, fds), [])
        # exactly one primary has a same-domain secondary (the odd host),
        # and its tertiary covers the invariant
        layout = planner.rotation_layout(topo, 2)
        degraded = [p for p, (s, _t) in layout.items() if fds[p] == fds[s]]
        self.assertEqual(degraded, ["a3"])
        self.assertNotEqual(fds["a3"], fds[layout["a3"][1]])

    def test_grouped_order_ftt2_violates(self):
        fds = {"a1": 0, "a2": 0, "a3": 0, "b1": 1, "b2": 1, "b3": 1}
        topo = [["a1"], ["a2"], ["a3"], ["b1"], ["b2"], ["b3"]]
        violations = planner.compute_fd_layout_violations(topo, 2, fds)
        self.assertTrue(violations)  # a1: sec a2, tert a3 — both same domain

    def test_ftt1_odd_population_violates(self):
        fds = dict(self.FDS_2x2, a3=0)
        topo = [["a1"], ["b1"], ["a2"], ["b2"], ["a3"]]
        violations = planner.compute_fd_layout_violations(topo, 1, fds)
        self.assertEqual(len(violations), 1)
        self.assertIn("a3", violations[0])

    def test_unset_domains_are_skipped(self):
        fds = {"a1": -1, "b1": -1, "a2": -1, "b2": -1}
        topo = [["a1"], ["b1"], ["a2"], ["b2"]]
        self.assertEqual(planner.compute_fd_layout_violations(topo, 2, fds), [])

    def test_explicit_layout_override(self):
        # actual (drifted) layout where a1's roles are both in domain 0
        layout = {"a1": ("a2", "a3")}
        fds = {"a1": 0, "a2": 0, "a3": 0}
        violations = planner.compute_fd_layout_violations(
            [], 2, fds, layout=layout)
        self.assertEqual(len(violations), 1)

    def test_rotation_layout_refuses_too_few_hosts_for_ftt(self):
        with self.assertRaises(ValueError):
            planner.rotation_layout([["a1"], ["b1"]], 2)


# ---------------------------------------------------------------------------
# planner.fd_balance_violation
# ---------------------------------------------------------------------------

class TestFdBalance(unittest.TestCase):

    def test_balanced_ok(self):
        self.assertIsNone(planner.fd_balance_violation({0: 2, 1: 2}))

    def test_plus_one_ok(self):
        self.assertIsNone(planner.fd_balance_violation({0: 3, 1: 2}))

    def test_plus_two_violates(self):
        self.assertIsNotNone(planner.fd_balance_violation({0: 4, 1: 2}))

    def test_floor_violates(self):
        self.assertIsNotNone(
            planner.fd_balance_violation({0: 1, 1: 2}, min_hosts_per_fd=2))

    def test_empty_ok(self):
        self.assertIsNone(planner.fd_balance_violation({}))

    def test_unset_domain_ignored(self):
        self.assertIsNone(planner.fd_balance_violation({-1: 7, 0: 2, 1: 2}))


# ---------------------------------------------------------------------------
# planner.fd_activation_domain_count_violation
# ---------------------------------------------------------------------------

class TestFdActivationDomainCount(unittest.TestCase):

    def test_npcs1_two_domains_violates(self):
        self.assertIsNotNone(
            planner.fd_activation_domain_count_violation(1, 2))

    def test_npcs1_three_domains_ok(self):
        self.assertIsNone(
            planner.fd_activation_domain_count_violation(1, 3))

    def test_npcs1_one_domain_violates(self):
        self.assertIsNotNone(
            planner.fd_activation_domain_count_violation(1, 1))

    def test_npcs2_two_domains_violates(self):
        self.assertIsNotNone(
            planner.fd_activation_domain_count_violation(2, 2))

    def test_npcs2_three_domains_violates(self):
        self.assertIsNotNone(
            planner.fd_activation_domain_count_violation(2, 3))

    def test_npcs2_four_domains_ok(self):
        self.assertIsNone(
            planner.fd_activation_domain_count_violation(2, 4))

    def test_npcs2_more_than_four_domains_ok(self):
        self.assertIsNone(
            planner.fd_activation_domain_count_violation(2, 6))


# ---------------------------------------------------------------------------
# preconditions: add / remove / current admission
# ---------------------------------------------------------------------------

class TestAddAdmission(unittest.TestCase):

    def _nodes_2x2(self):
        return [_node("a1", "10.0.0.1", 0), _node("b1", "10.0.0.2", 1),
                _node("a2", "10.0.0.3", 0), _node("b2", "10.0.0.4", 1)]

    def test_disabled_feature_ok(self):
        ok, _ = check_fd_admission_for_add(
            _cluster(enable_failure_domain=False), _db([]), None)
        self.assertTrue(ok)

    def test_balanced_plus_one_ok(self):
        ok, reason = check_fd_admission_for_add(
            _cluster(), _db(self._nodes_2x2()), 0, new_mgmt_ip="10.0.0.5")
        self.assertTrue(ok, reason)

    def test_second_host_ahead_refused(self):
        nodes = self._nodes_2x2() + [_node("a3", "10.0.0.5", 0)]
        ok, reason = check_fd_admission_for_add(
            _cluster(), _db(nodes), 0, new_mgmt_ip="10.0.0.6")
        self.assertFalse(ok)
        self.assertIn("unbalanced", reason)

    def test_new_slot_on_known_host_ok_same_domain(self):
        ok, reason = check_fd_admission_for_add(
            _cluster(), _db(self._nodes_2x2()), 0, new_mgmt_ip="10.0.0.1")
        self.assertTrue(ok, reason)

    def test_fd_migration_refused(self):
        ok, reason = check_fd_admission_for_add(
            _cluster(), _db(self._nodes_2x2()), 1, new_mgmt_ip="10.0.0.1")
        self.assertFalse(ok)
        self.assertIn("not supported", reason)

    def test_missing_domain_id_refused(self):
        ok, _ = check_fd_admission_for_add(_cluster(), _db([]), None)
        self.assertFalse(ok)

    def test_removed_and_secondary_nodes_ignored(self):
        nodes = self._nodes_2x2() + [
            _node("gone", "10.0.0.7", 0, status=StorageNode.STATUS_REMOVED),
            _node("sec", "10.0.0.8", 0, is_secondary_node=True),
        ]
        ok, reason = check_fd_admission_for_add(
            _cluster(), _db(nodes), 0, new_mgmt_ip="10.0.0.9")
        self.assertTrue(ok, reason)


class TestRemoveAdmission(unittest.TestCase):

    def _nodes(self, spec):
        # spec: list of (uuid, ip, fd)
        return [_node(u, ip, fd) for u, ip, fd in spec]

    def test_pre_activation_removal_free(self):
        nodes = [_node("a1", "10.0.0.1", 0, lvstore=""),
                 _node("b1", "10.0.0.2", 1, lvstore="")]
        ok, reason = check_fd_admission_for_remove(
            _cluster(), _db(nodes), nodes[0])
        self.assertTrue(ok, reason)

    def test_remove_from_larger_domain_ok(self):
        nodes = self._nodes([("a1", "10.0.0.1", 0), ("b1", "10.0.0.2", 1),
                             ("a2", "10.0.0.3", 0), ("b2", "10.0.0.4", 1),
                             ("a3", "10.0.0.5", 0)])
        ok, reason = check_fd_admission_for_remove(
            _cluster(), _db(nodes), nodes[4])
        self.assertTrue(ok, reason)

    def test_remove_from_smaller_domain_refused(self):
        nodes = self._nodes([("a1", "10.0.0.1", 0), ("b1", "10.0.0.2", 1),
                             ("a2", "10.0.0.3", 0), ("b2", "10.0.0.4", 1),
                             ("a3", "10.0.0.5", 0)])
        ok, reason = check_fd_admission_for_remove(
            _cluster(), _db(nodes), nodes[1])
        self.assertFalse(ok)

    def test_remove_below_two_hosts_per_domain_refused(self):
        nodes = self._nodes([("a1", "10.0.0.1", 0), ("b1", "10.0.0.2", 1),
                             ("a2", "10.0.0.3", 0), ("b2", "10.0.0.4", 1)])
        ok, reason = check_fd_admission_for_remove(
            _cluster(), _db(nodes), nodes[0])
        self.assertFalse(ok)
        self.assertIn("at least 2", reason)

    def test_remove_one_slot_of_multislot_host_ok(self):
        nodes = self._nodes([("a1", "10.0.0.1", 0), ("a1b", "10.0.0.1", 0),
                             ("b1", "10.0.0.2", 1), ("a2", "10.0.0.3", 0),
                             ("b2", "10.0.0.4", 1)])
        ok, reason = check_fd_admission_for_remove(
            _cluster(), _db(nodes), nodes[0])
        self.assertTrue(ok, reason)


class TestBalanceCurrent(unittest.TestCase):

    def test_balanced_ok(self):
        nodes = [_node("a1", "10.0.0.1", 0), _node("b1", "10.0.0.2", 1)]
        ok, _ = check_fd_balance_current(_cluster(), _db(nodes))
        self.assertTrue(ok)

    def test_two_ahead_refused(self):
        nodes = [_node("a1", "10.0.0.1", 0), _node("a2", "10.0.0.2", 0),
                 _node("a3", "10.0.0.3", 0), _node("b1", "10.0.0.4", 1)]
        ok, _ = check_fd_balance_current(_cluster(), _db(nodes))
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# executor: rotation recovery + FD-aware planning
# ---------------------------------------------------------------------------

class TestRotationRecovery(unittest.TestCase):

    def test_clean_cycle_recovered(self):
        layout = {"a1": ("b1", ""), "b1": ("a2", ""),
                  "a2": ("b2", ""), "b2": ("a1", "")}
        order = _rotation_order_from_layout(["a1", "b1", "a2", "b2"], layout)
        self.assertEqual(order, ["a1", "b1", "a2", "b2"])

    def test_broken_chain_returns_none(self):
        layout = {"a1": ("b1", ""), "b1": ("a1", ""),  # short sub-cycle
                  "a2": ("b2", ""), "b2": ("a2", "")}
        self.assertIsNone(
            _rotation_order_from_layout(["a1", "b1", "a2", "b2"], layout))

    def test_missing_pointer_returns_none(self):
        layout = {"a1": ("b1", ""), "b1": ("", "")}
        self.assertIsNone(_rotation_order_from_layout(["a1", "b1"], layout))


class TestFdAwarePlanning(unittest.TestCase):

    def _nodes_2x2_interleaved(self):
        # actual layout as produced by an interleaved fresh activation
        return [
            _node("a1", "10.0.0.1", 0, secondary="b1", tertiary="a2"),
            _node("b1", "10.0.0.2", 1, secondary="a2", tertiary="b2"),
            _node("a2", "10.0.0.3", 0, secondary="b2", tertiary="a1"),
            _node("b2", "10.0.0.4", 1, secondary="a1", tertiary="b1"),
        ]

    def test_ftt2_expansion_plans_and_keeps_invariant(self):
        existing = self._nodes_2x2_interleaved()
        newcomer = _node("a3", "10.0.0.5", 0, lvstore="",
                         secondary="", tertiary="")
        moves = _plan_moves_with_failure_domains(
            _cluster(ftt=2), MagicMock(), existing, newcomer)
        # newcomer gets its three create-moves
        creates = [m for m in moves if m.is_create]
        self.assertEqual({m.role for m in creates},
                         {"primary", "secondary", "tertiary"})
        # every re-home move names a real donor and a different recipient
        for m in moves:
            if not m.is_create:
                self.assertNotEqual(m.from_node_id, m.to_node_id)

    def test_ftt1_odd_population_refused(self):
        existing = [
            _node("a1", "10.0.0.1", 0, secondary="b1"),
            _node("b1", "10.0.0.2", 1, secondary="a2"),
            _node("a2", "10.0.0.3", 0, secondary="b2"),
            _node("b2", "10.0.0.4", 1, secondary="a1"),
        ]
        newcomer = _node("a3", "10.0.0.5", 0, lvstore="")
        with self.assertRaises(RuntimeError):
            _plan_moves_with_failure_domains(
                _cluster(ftt=1), MagicMock(), existing, newcomer)

    def test_multislot_host_refused(self):
        existing = self._nodes_2x2_interleaved()
        newcomer = _node("a1b", "10.0.0.1", 0, lvstore="")  # same host as a1
        with self.assertRaises(RuntimeError) as ctx:
            _plan_moves_with_failure_domains(
                _cluster(ftt=2), MagicMock(), existing, newcomer)
        self.assertIn("one", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
