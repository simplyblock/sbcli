# coding=utf-8
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
* Expansion planning — ``_plan_moves_with_failure_domains`` asks the
  removal's global planner (``replica_placement``) for the cheapest layout
  with primary/secondary/tertiary in pairwise-distinct domains and runs the
  moves in an order that only ever lands on a free slot. Where full
  diversity is unreachable (two domains, odd populations with too few
  domains) it falls back to the rotation-shift search, which holds the
  >=1-cross-domain floor, and still refuses plans below that floor (e.g.
  FTT1 growing to odd populations).
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




def _apply_role_moves(existing_nodes, moves, ftt):
    """Replay ``moves`` on a layout dict, in order, the way the executor
    would -- and fail the moment a re-home lands on a slot that is still
    occupied, because that is the one thing the executor cannot survive
    (its recipient write overwrites the back-reference in place)."""
    layout = {n.get_id(): [n.secondary_node_id or "", n.tertiary_node_id or ""]
              for n in existing_nodes}
    holders = {("secondary", sec): p for p, (sec, _t) in layout.items() if sec}
    holders.update({("tertiary", ter): p for p, (_s, ter) in layout.items() if ter})
    for m in moves:
        if m.role == "primary":
            layout.setdefault(m.to_node_id, ["", ""])
            continue
        idx = 0 if m.role == "secondary" else 1
        occupant = holders.get((m.role, m.to_node_id))
        if occupant is not None and occupant != m.lvs_primary_node_id:
            raise AssertionError(
                f"move {m} lands on {m.to_node_id}'s {m.role} slot while "
                f"{occupant} still holds it")
        if m.from_node_id:
            holders.pop((m.role, m.from_node_id), None)
        layout.setdefault(m.lvs_primary_node_id, ["", ""])[idx] = m.to_node_id
        holders[(m.role, m.to_node_id)] = m.lvs_primary_node_id
    return {p: tuple(v) for p, v in layout.items()}


class TestPlannerDrivenExpansion(unittest.TestCase):
    """Expansion now reaches for the same guarantee the removal does."""

    def _ring(self, domains, hosts_per_domain, newcomer_domain=0):
        """An interleaved, fully diverse FTT2 ring: a b c .. a b c .. and a
        newcomer for ``newcomer_domain`` that makes that domain the odd one."""
        ids, fds = [], {}
        for i in range(hosts_per_domain):
            for d in range(domains):
                n = f"{chr(97 + d)}{i + 1}"
                ids.append(n)
                fds[n] = d
        nodes = [_node(n, f"10.0.0.{i + 1}", fds[n],
                       secondary=ids[(i + 1) % len(ids)], tertiary=ids[(i + 2) % len(ids)])
                 for i, n in enumerate(ids)]
        new_id = f"{chr(97 + newcomer_domain)}{hosts_per_domain + 1}"
        fds[new_id] = newcomer_domain
        newcomer = _node(new_id, f"10.0.0.{len(ids) + 1}", newcomer_domain, lvstore="")
        return nodes, newcomer, fds

    def test_on_four_domains_a_ninth_node_lands_with_every_lvs_pairwise_diverse(self):
        """The case the rotation-shift planner deliberately left degraded --
        FTT2 growing to an odd population -- is fully diverse whenever the
        domains allow it. Four domains is the activation floor for a 2+2
        cluster (npcs+2), so this is the realistic shape."""
        nodes, newcomer, fds = self._ring(4, 2)
        moves = _plan_moves_with_failure_domains(_cluster(ftt=2), MagicMock(), nodes, newcomer)
        final = _apply_role_moves(nodes, moves, 2)
        self.assertIn(newcomer.get_id(), final)
        for primary, (sec, ter) in final.items():
            domains = [fds[primary], fds[sec], fds[ter]]
            self.assertEqual(len(set(domains)), 3,
                             f"LVS@{primary}: sec={sec} ter={ter} domains={domains}")

    def test_on_three_domains_the_odd_host_is_degraded_by_necessity(self):
        """With three domains and three roles, the odd host's tertiary has no
        domain left to land in (the planner reports the tertiary-blocking
        pattern), so full diversity is unreachable and the rotation floor
        decides: every LVS still keeps at least one cross-domain role."""
        nodes, newcomer, fds = self._ring(3, 2)
        moves = _plan_moves_with_failure_domains(_cluster(ftt=2), MagicMock(), nodes, newcomer)
        final = _apply_role_moves(nodes, moves, 2)
        degraded = [p for p, (sec, ter) in final.items()
                    if not (fds[sec] != fds[p] and fds[ter] != fds[p] and fds[sec] != fds[ter])]
        self.assertTrue(degraded, "three domains cannot be fully diverse at an odd population")
        for primary, (sec, ter) in final.items():
            self.assertTrue(fds[sec] != fds[primary] or fds[ter] != fds[primary],
                            f"LVS@{primary} keeps no cross-domain role: sec={sec} ter={ter}")

    def test_moves_never_land_on_an_occupied_slot(self):
        """The executor overwrites the recipient's back-reference, so the
        plan's order is part of its correctness: replaying it must never hit
        a slot whose occupant has not moved out yet -- on the planner path
        and on the fallback alike."""
        for domains in (4, 3):
            with self.subTest(domains=domains):
                nodes, newcomer, _ = self._ring(domains, 2)
                moves = _plan_moves_with_failure_domains(
                    _cluster(ftt=2), MagicMock(), nodes, newcomer)
                _apply_role_moves(nodes, moves, 2)  # raises on an occupied landing

    def test_the_newcomer_primary_is_created_before_its_replicas(self):
        """On the planner path the newcomer's own replica creates may be
        sequenced anywhere; its primary LVS therefore comes first."""
        nodes, newcomer, _ = self._ring(4, 2)
        moves = _plan_moves_with_failure_domains(_cluster(ftt=2), MagicMock(), nodes, newcomer)
        self.assertEqual((moves[0].role, moves[0].to_node_id), ("primary", newcomer.get_id()))
        own = [i for i, m in enumerate(moves)
               if m.lvs_primary_node_id == newcomer.get_id() and m.role != "primary"]
        self.assertEqual(len(own), 2)

    def test_two_domains_fall_back_to_the_rotation_floor(self):
        """Pairwise diversity needs three domains for three roles; on two the
        planner cannot deliver it and its own fallback drops domains entirely.
        The rotation search still holds the >=1-cross-domain floor, so that
        is what decides the degraded case -- exactly the pre-planner result."""
        nodes = [
            _node("a1", "10.0.0.1", 0, secondary="b1", tertiary="a2"),
            _node("b1", "10.0.0.2", 1, secondary="a2", tertiary="b2"),
            _node("a2", "10.0.0.3", 0, secondary="b2", tertiary="a1"),
            _node("b2", "10.0.0.4", 1, secondary="a1", tertiary="b1"),
        ]
        fds = {"a1": 0, "b1": 1, "a2": 0, "b2": 1, "a3": 0}
        newcomer = _node("a3", "10.0.0.5", 0, lvstore="")
        moves = _plan_moves_with_failure_domains(_cluster(ftt=2), MagicMock(), nodes, newcomer)
        final = _apply_role_moves(nodes, moves, 2)
        for primary, (sec, ter) in final.items():
            self.assertTrue(fds[sec] != fds[primary] or fds[ter] != fds[primary],
                            f"LVS@{primary} keeps no cross-domain role: sec={sec} ter={ter}")

    def test_a_layout_below_the_floor_is_still_refused(self):
        """FTT1 growing to an odd population on two domains: the odd primary
        can have no cross-domain role at all. Refused before any move."""
        nodes = [
            _node("a1", "10.0.0.1", 0, secondary="b1"),
            _node("b1", "10.0.0.2", 1, secondary="a2"),
            _node("a2", "10.0.0.3", 0, secondary="b2"),
            _node("b2", "10.0.0.4", 1, secondary="a1"),
        ]
        newcomer = _node("a3", "10.0.0.5", 0, lvstore="")
        with self.assertRaises(RuntimeError) as ctx:
            _plan_moves_with_failure_domains(_cluster(ftt=1), MagicMock(), nodes, newcomer)
        self.assertIn("cross-domain", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
