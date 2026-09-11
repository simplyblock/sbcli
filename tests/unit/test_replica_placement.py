# coding=utf-8
"""
Unit tests for the global replica placement planner
(``simplyblock_core.controllers.replica_placement``).

The planner replaces the per-replica greedy relocation used by node removal
under failure domains. What is asserted here:

  * the matching itself (optimality, forbidden-edge rejection);
  * full pairwise domain diversity is reached whenever it is mathematically
    reachable -- including the reported 4-domain x 3-host cluster shrunk one
    host per domain, which the greedy path could not hold;
  * when it is NOT reachable, that is reported rather than silently relaxed;
  * the planned moves are minimal, and ordered so each one lands on a slot
    that is genuinely free at that point -- including the rotation cycles
    that a per-replica mover cannot execute at all.

Pure logic: no DB, no RPC, no mocks.
"""

import itertools
import random
import unittest

from simplyblock_core.controllers import replica_placement as rp
from simplyblock_core.controllers.replica_placement import (
    InfeasiblePlacement, Placement)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _grid(domains, per_domain):
    """``domains`` x ``per_domain`` nodes named ``d<domain>n<index>``."""
    nodes = [f"d{d}n{i}" for d in range(domains) for i in range(per_domain)]
    fd = {n: int(n[1:].split("n")[0]) for n in nodes}
    return nodes, fd


def _rotation(nodes, fd, ftt):
    """The FD-interleaved rotation cluster_activate produces: round-robin the
    domains, then take the next / next-next node as secondary / tertiary."""
    by_fd = {}
    for n in nodes:
        by_fd.setdefault(fd[n], []).append(n)
    order = []
    for idx in range(max(len(v) for v in by_fd.values())):
        for d in sorted(by_fd):
            if idx < len(by_fd[d]):
                order.append(by_fd[d][idx])
    size = len(order)
    return {
        p: Placement(order[(k + 1) % size],
                     order[(k + 2) % size] if ftt >= 2 else "")
        for k, p in enumerate(order)
    }


def _apply(current, moves, ftt):
    """Replay ``moves`` against ``current``, asserting each one lands on a
    slot that is free at that moment -- the property that makes the plan
    executable by a mover with single-valued back-reference fields."""
    state = dict(current)
    slots = {}
    for primary, placement in current.items():
        if placement.secondary:
            slots[(rp.ROLE_SECONDARY, placement.secondary)] = primary
        if ftt >= 2 and placement.tertiary:
            slots[(rp.ROLE_TERTIARY, placement.tertiary)] = primary
    for move in moves:
        key = (move.role, move.to_node_id)
        assert key not in slots, (
            f"{move} lands on a slot already held by {slots[key]}")
        if move.from_node_id:
            slots.pop((move.role, move.from_node_id), None)
        slots[key] = move.lvs_primary_node_id
        placement = state[move.lvs_primary_node_id]
        state[move.lvs_primary_node_id] = (
            Placement(move.to_node_id, placement.tertiary)
            if move.role == rp.ROLE_SECONDARY
            else Placement(placement.secondary, move.to_node_id))
    return state


def _remove(layout, victim):
    """The layout as phase 3b sees it: the victim's own LVS is gone (phase
    3a) and every role it hosted is homeless."""
    alive = [p for p in layout if p != victim]
    current = {
        p: Placement(
            layout[p].secondary if layout[p].secondary != victim else "",
            layout[p].tertiary if layout[p].tertiary != victim else "")
        for p in alive
    }
    return alive, current


# ---------------------------------------------------------------------------
# The matcher
# ---------------------------------------------------------------------------

class TestMinCostMatching(unittest.TestCase):

    def test_finds_the_optimum_on_a_small_matrix(self):
        cost = [[4, 1, 3], [2, 0, 5], [3, 2, 2]]
        got = rp.min_cost_matching(cost)
        best = min(
            sum(cost[i][perm[i]] for i in range(3))
            for perm in itertools.permutations(range(3)))
        self.assertEqual(sum(cost[i][got[i]] for i in range(3)), best)

    def test_agrees_with_brute_force_on_random_matrices(self):
        rng = random.Random(1)
        for _ in range(50):
            n = rng.randint(1, 6)
            cost = [[rng.randint(0, 20) for _ in range(n)] for _ in range(n)]
            got = rp.min_cost_matching(cost)
            best = min(
                sum(cost[i][perm[i]] for i in range(n))
                for perm in itertools.permutations(range(n)))
            self.assertEqual(sum(cost[i][got[i]] for i in range(n)), best)

    def test_routes_around_forbidden_edges(self):
        cost = [[rp.FORBIDDEN, 1], [1, rp.FORBIDDEN]]
        self.assertEqual(rp.min_cost_matching(cost), [1, 0])

    def test_empty_matrix(self):
        self.assertEqual(rp.min_cost_matching([]), [])

    def test_rejects_more_rows_than_columns(self):
        with self.assertRaises(ValueError):
            rp.min_cost_matching([[1], [2]])


# ---------------------------------------------------------------------------
# Diversity checking
# ---------------------------------------------------------------------------

class TestFullDiversityViolations(unittest.TestCase):

    def test_clean_layout_has_none(self):
        layout = {"a": Placement("b", "c")}
        fd = {"a": 0, "b": 1, "c": 2}
        self.assertEqual(rp.full_diversity_violations(layout, fd, 2), [])

    def test_secondary_and_tertiary_sharing_a_domain_is_a_violation(self):
        # The exact state the ">=1 cross-domain role" floor accepts and the
        # incremental relocation path kept producing: the secondary IS
        # cross-domain from the primary, so the old check passed, but one
        # domain outage still costs two of the three copies.
        layout = {"a": Placement("b", "c")}
        fd = {"a": 0, "b": 1, "c": 1}
        violations = rp.full_diversity_violations(layout, fd, 2)
        self.assertEqual(len(violations), 1)
        self.assertIn("shares a domain with secondary b", violations[0])

    def test_role_in_the_primarys_own_domain_is_a_violation(self):
        layout = {"a": Placement("b", "c")}
        fd = {"a": 0, "b": 1, "c": 0}
        violations = rp.full_diversity_violations(layout, fd, 2)
        self.assertEqual(len(violations), 1)
        self.assertIn("shares a domain with primary a", violations[0])

    def test_tertiary_ignored_on_ftt1(self):
        layout = {"a": Placement("b", "")}
        fd = {"a": 0, "b": 1}
        self.assertEqual(rp.full_diversity_violations(layout, fd, 1), [])

    def test_unset_domain_on_a_holder_is_a_violation(self):
        layout = {"a": Placement("b", "c")}
        fd = {"a": 0, "b": 1, "c": -1}
        self.assertIn("no failure domain set",
                      rp.full_diversity_violations(layout, fd, 2)[0])

    def test_primary_without_a_domain_is_skipped(self):
        layout = {"a": Placement("b", "c")}
        fd = {"a": -1, "b": 1, "c": 1}
        self.assertEqual(rp.full_diversity_violations(layout, fd, 2), [])

    def test_missing_role_is_a_violation(self):
        layout = {"a": Placement("b", "")}
        fd = {"a": 0, "b": 1}
        self.assertIn("has no tertiary",
                      rp.full_diversity_violations(layout, fd, 2)[0])


# ---------------------------------------------------------------------------
# Structural feasibility
# ---------------------------------------------------------------------------

class TestFeasibilityConditions(unittest.TestCase):

    def test_a_domain_holding_more_than_half_blocks_diversity(self):
        self.assertEqual(rp.secondary_overloaded_domains({0: 5, 1: 4}), [0])
        self.assertEqual(rp.secondary_overloaded_domains({0: 4, 1: 4}), [])

    def test_tertiary_blocking_pair_detected(self):
        # 3 domains sized 3/3/2: every primary in domain 0 or 1 whose
        # secondary is in the other must put its tertiary in domain 2, which
        # only has 2 slots.
        sizes = {0: 3, 1: 3, 2: 2}
        self.assertEqual(rp.tertiary_blocking_pairs({(0, 1): 3}, sizes), [(0, 1)])
        self.assertEqual(rp.tertiary_blocking_pairs({(0, 1): 2}, sizes), [])


# ---------------------------------------------------------------------------
# The planner
# ---------------------------------------------------------------------------

class TestPlanDiverseLayout(unittest.TestCase):

    def _assert_diverse(self, plan, fd, ftt):
        self.assertTrue(plan.full_diversity, plan.violations)
        self.assertEqual(plan.violations, [])
        for primary, placement in plan.layout.items():
            domains = [fd[primary], fd[placement.secondary]]
            if ftt >= 2:
                domains.append(fd[placement.tertiary])
            self.assertEqual(len(set(domains)), len(domains),
                             f"{primary} -> {placement} domains {domains}")

    def test_builds_a_diverse_layout_from_scratch(self):
        nodes, fd = _grid(4, 3)
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        self._assert_diverse(plan, fd, 2)

    def test_roles_are_permutations(self):
        nodes, fd = _grid(4, 3)
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        for index in (0, 1):
            holders = [pl[index] for pl in plan.layout.values()]
            self.assertCountEqual(holders, nodes)

    def test_an_already_diverse_layout_is_left_alone(self):
        nodes, fd = _grid(4, 2)
        layout = _rotation(nodes, fd, 2)
        plan = rp.plan_diverse_layout(nodes, fd, layout, 2)
        self.assertEqual(plan.layout, layout)
        self.assertEqual(rp.plan_moves(layout, plan.layout, nodes, 2), [])

    def test_repairs_one_bad_placement_with_the_fewest_moves(self):
        nodes, fd = _grid(4, 2)
        layout = dict(_rotation(nodes, fd, 2))
        # Break exactly one LVS by swapping two tertiaries into a collision.
        victim = "d0n0"
        other = next(p for p in nodes
                     if layout[p].tertiary != layout[victim].tertiary and p != victim)
        layout[victim] = Placement(layout[victim].secondary, layout[other].tertiary)
        layout[other] = Placement(layout[other].secondary,
                                  _rotation(nodes, fd, 2)[victim].tertiary)
        plan = rp.plan_diverse_layout(nodes, fd, layout, 2)
        self._assert_diverse(plan, fd, 2)
        moves = rp.plan_moves(layout, plan.layout, nodes, 2)
        # Only the two swapped tertiaries move (plus one scratch hop to break
        # the rotation, if the planner needs one).
        self.assertLessEqual(len(moves), 3)
        self.assertTrue(all(m.role == rp.ROLE_TERTIARY for m in moves))

    def test_honours_host_anti_affinity_beyond_the_domain(self):
        # Two nodes per host, two hosts per domain: FD diversity alone would
        # let two roles share a host.
        nodes, fd, host = [], {}, {}
        for d in range(4):
            for h in range(2):
                for s in range(2):
                    nid = f"d{d}h{h}s{s}"
                    nodes.append(nid)
                    fd[nid] = d
                    host[nid] = f"d{d}h{h}"
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2, host_by_node=host)
        self._assert_diverse(plan, fd, 2)
        for primary, placement in plan.layout.items():
            hosts = {host[primary], host[placement.secondary], host[placement.tertiary]}
            self.assertEqual(len(hosts), 3)

    def test_ftt1_leaves_the_tertiary_empty(self):
        nodes, fd = _grid(3, 2)
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 1)
        self._assert_diverse(plan, fd, 1)
        self.assertTrue(all(pl.tertiary == "" for pl in plan.layout.values()))

    def test_two_domains_at_ftt2_is_reported_degraded_not_faked(self):
        # A 2-domain layout can never place a tertiary outside both the
        # primary's and the secondary's domain. The planner must say so.
        nodes, fd = _grid(2, 3)
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        self.assertFalse(plan.full_diversity)
        self.assertTrue(plan.violations)
        self.assertTrue(plan.notes)
        # ...but the host-disjointness floor is still held.
        for primary, placement in plan.layout.items():
            self.assertEqual(len({primary, placement.secondary, placement.tertiary}), 3)

    def test_unbalanced_three_domain_layout_is_reported_degraded(self):
        # 3 domains sized 3/3/2 at FTT2 is provably unsatisfiable (verified by
        # brute force): the tertiaries of every 0<->1 pairing all have to fit
        # into domain 2's two slots.
        nodes = [f"d0n{i}" for i in range(3)] + [f"d1n{i}" for i in range(3)] \
            + [f"d2n{i}" for i in range(2)]
        fd = {n: int(n[1]) for n in nodes}
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        self.assertFalse(plan.full_diversity)
        self.assertTrue(plan.notes)

    def test_domain_holding_more_than_half_is_noted(self):
        nodes = [f"d0n{i}" for i in range(5)] + [f"d1n{i}" for i in range(2)]
        fd = {n: int(n[1]) for n in nodes}
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 1)
        self.assertFalse(plan.full_diversity)
        self.assertTrue(any("more than half" in note for note in plan.notes))

    def test_too_few_nodes_raises(self):
        with self.assertRaises(InfeasiblePlacement):
            rp.plan_diverse_layout(["a", "b"], {"a": 0, "b": 1},
                                   {"a": Placement("", ""), "b": Placement("", "")}, 2)

    def test_rejects_a_bad_ftt(self):
        with self.assertRaises(ValueError):
            rp.plan_diverse_layout(["a"], {"a": 0}, {}, 3)

    def test_empty_cluster(self):
        plan = rp.plan_diverse_layout([], {}, {}, 2)
        self.assertEqual(plan.layout, {})
        self.assertTrue(plan.full_diversity)

    def test_disabled_domains_still_produce_a_host_disjoint_layout(self):
        # All nodes in one domain -- the feature is effectively off. The
        # planner must not claim a diversity it cannot have, but must still
        # return a usable host-disjoint layout.
        nodes = [f"n{i}" for i in range(4)]
        fd = {n: 0 for n in nodes}
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        self.assertTrue(plan.full_diversity)   # nothing to violate
        for primary, placement in plan.layout.items():
            self.assertEqual(len({primary, placement.secondary, placement.tertiary}), 3)


# ---------------------------------------------------------------------------
# The reported scenario
# ---------------------------------------------------------------------------

class TestFourDomainShrink(unittest.TestCase):
    """The live case the greedy path could not hold: 4 failure domains x 3
    hosts, FTT2 (npcs=2, "2+2"), removing one host from each domain in turn.
    Full pairwise diversity must survive all four removals."""

    def _shrink(self, victims):
        nodes, fd = _grid(4, 3)
        layout = _rotation(nodes, fd, 2)
        self.assertEqual(rp.full_diversity_violations(layout, fd, 2), [])
        alive = list(nodes)
        for victim in victims:
            alive, current = _remove(layout, victim)
            plan = rp.plan_diverse_layout(alive, fd, current, 2)
            moves = rp.plan_moves(current, plan.layout, alive, 2)
            layout = _apply(current, moves, 2)
            self.assertEqual(layout, plan.layout)
            self.assertEqual(
                rp.full_diversity_violations(layout, fd, 2), [],
                f"diversity lost after removing {victim}")
            for index in (0, 1):
                holders = [pl[index] for pl in layout.values()]
                self.assertCountEqual(holders, alive)
        return layout, fd

    def test_one_removal_per_domain_keeps_full_diversity(self):
        self._shrink([f"d{d}n0" for d in range(4)])

    def test_order_of_removals_does_not_matter(self):
        for victims in itertools.permutations([f"d{d}n0" for d in range(4)]):
            self._shrink(list(victims))

    def test_removing_all_four_in_one_planning_pass(self):
        nodes, fd = _grid(4, 3)
        layout = _rotation(nodes, fd, 2)
        alive = [n for n in nodes if not n.endswith("n0")]
        current = {
            p: Placement(layout[p].secondary if layout[p].secondary in alive else "",
                         layout[p].tertiary if layout[p].tertiary in alive else "")
            for p in alive
        }
        plan = rp.plan_diverse_layout(alive, fd, current, 2)
        moves = rp.plan_moves(current, plan.layout, alive, 2)
        final = _apply(current, moves, 2)
        self.assertEqual(rp.full_diversity_violations(final, fd, 2), [])


# ---------------------------------------------------------------------------
# Move planning and ordering
# ---------------------------------------------------------------------------

class TestPlanMoves(unittest.TestCase):

    def test_diff_only_reports_actual_changes(self):
        current = {"a": Placement("b", "c")}
        target = {"a": Placement("b", "d")}
        moves = rp.diff_layout(current, target, 2)
        self.assertEqual(
            moves, [rp.ReplicaMove("a", rp.ROLE_TERTIARY, "c", "d")])

    def test_diff_ignores_the_tertiary_on_ftt1(self):
        current = {"a": Placement("b", "c")}
        target = {"a": Placement("b", "d")}
        self.assertEqual(rp.diff_layout(current, target, 1), [])

    def test_chain_is_ordered_so_every_target_is_free(self):
        # b's slot is free (nobody hosts a secondary there); a wants b, and
        # c wants a's current host -- so a must move first.
        current = {"p1": Placement("h1", ""), "p2": Placement("", "")}
        target = {"p1": Placement("h2", ""), "p2": Placement("h1", "")}
        moves = rp.order_moves(
            rp.diff_layout(current, target, 1), current, ["h1", "h2", "p1", "p2"], 1)
        self.assertEqual([m.lvs_primary_node_id for m in moves], ["p1", "p2"])

    def test_rotation_cycle_is_broken_with_a_scratch_hop(self):
        # p1 and p2 swap secondaries; h3 is free and used to park one of them.
        current = {"p1": Placement("h1", ""), "p2": Placement("h2", "")}
        target = {"p1": Placement("h2", ""), "p2": Placement("h1", "")}
        nodes = ["h1", "h2", "h3", "p1", "p2"]
        moves = rp.order_moves(rp.diff_layout(current, target, 1), current, nodes, 1)
        self.assertEqual(sum(1 for m in moves if m.scratch), 1)
        final = _apply(current, moves, 1)
        self.assertEqual(final, target)

    def test_rotation_cycle_without_a_free_slot_is_refused(self):
        # A full permutation with no free slot cannot rotate while
        # lvstore_stack_secondary holds a single value. Refusing beats
        # emitting a plan the mover would deadlock on.
        current = {"p1": Placement("p2", ""), "p2": Placement("p1", "")}
        target = {"p1": Placement("p1", ""), "p2": Placement("p2", "")}
        with self.assertRaises(InfeasiblePlacement):
            rp.order_moves(rp.diff_layout(current, target, 1), current, ["p1", "p2"], 1)

    def test_node_being_removed_is_never_used_as_scratch(self):
        # "gone" is not in the surviving set: freeing its slot must not make
        # it a parking spot.
        current = {"p1": Placement("gone", ""), "p2": Placement("h1", "")}
        target = {"p1": Placement("h1", ""), "p2": Placement("h2", "")}
        nodes = ["h1", "h2", "p1", "p2"]
        moves = rp.order_moves(rp.diff_layout(current, target, 1), current, nodes, 1)
        self.assertNotIn("gone", [m.to_node_id for m in moves])
        self.assertEqual(_apply(current, moves, 1), target)

    def test_describe_plan_names_the_degradation(self):
        nodes, fd = _grid(2, 3)
        empty = {n: Placement("", "") for n in nodes}
        plan = rp.plan_diverse_layout(nodes, fd, empty, 2)
        self.assertIn("DEGRADED", rp.describe_plan(plan, []))
        nodes, fd = _grid(4, 2)
        clean = rp.plan_diverse_layout(nodes, fd, _rotation(nodes, fd, 2), 2)
        summary = rp.describe_plan(clean, [])
        self.assertIn("fully domain-diverse", summary)
        self.assertNotIn("DEGRADED", summary)


# ---------------------------------------------------------------------------
# Randomised end-to-end properties
# ---------------------------------------------------------------------------

class TestRandomisedProperties(unittest.TestCase):

    def _run(self, domains, per_domain, ftt, trials, seed):
        rng = random.Random(seed)
        nodes, fd = _grid(domains, per_domain)
        for trial in range(trials):
            # An arbitrarily bad starting layout -- the state repeated greedy
            # relocations can leave behind -- then one more removal on top.
            while True:
                sec = nodes[:]
                rng.shuffle(sec)
                if all(s != p for p, s in zip(nodes, sec)):
                    break
            ter = [""] * len(nodes)
            if ftt >= 2:
                while True:
                    ter = nodes[:]
                    rng.shuffle(ter)
                    if all(t != p and t != s for p, s, t in zip(nodes, sec, ter)):
                        break
            layout = {p: Placement(s, t) for p, s, t in zip(nodes, sec, ter)}
            alive, current = _remove(layout, rng.choice(nodes))
            plan = rp.plan_diverse_layout(alive, fd, current, ftt)
            moves = rp.plan_moves(current, plan.layout, alive, ftt)
            final = _apply(current, moves, ftt)
            self.assertEqual(final, plan.layout, f"trial {trial}")
            self.assertEqual(
                rp.full_diversity_violations(final, fd, ftt), [],
                f"trial {trial}: {plan.notes}")
            for index in range(ftt):
                self.assertCountEqual([pl[index] for pl in final.values()], alive)

    def test_four_domains_ftt2(self):
        self._run(4, 3, 2, trials=60, seed=11)

    def test_five_domains_ftt2(self):
        self._run(5, 2, 2, trials=60, seed=17)

    def test_three_domains_ftt1(self):
        self._run(3, 2, 1, trials=60, seed=23)


if __name__ == "__main__":
    unittest.main()
