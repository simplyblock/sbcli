# coding=utf-8
"""Unit tests for ``simplyblock_core.cluster_expand_planner.compute_role_diff``.

These tests exercise the pure planning logic for single-node cluster expansion
and assert the ordering / invariants the orchestrator relies on. No SPDK or
DB access is involved — the planner is intentionally side-effect-free.
"""

import json
import unittest

from simplyblock_core.cluster_expand_planner import (
    EXPAND_PHASE_ABORTED,
    EXPAND_PHASE_COMPLETED,
    EXPAND_PHASE_IN_PROGRESS,
    EXPAND_STATE_SCHEMA_VERSION,
    ROLE_PRIMARY,
    ROLE_SECONDARY,
    ROLE_TERTIARY,
    RoleMove,
    compute_role_diff,
    expand_state_abort,
    expand_state_advance,
    expand_state_complete,
    is_expand_in_progress,
    is_expand_state_compatible,
    make_expand_state,
    move_from_dict,
    move_to_dict,
    pending_moves,
)


def _by_lvs_role(moves):
    """Index moves by ``(lvs_primary, role)`` for order-independent assertions."""
    return {(m.lvs_primary_node_id, m.role): m for m in moves}


def _phase_b_starts_at(moves, new_node_id):
    """Return the index where Phase B (newcomer creates) begins.

    Phase B moves are those whose ``lvs_primary_node_id`` is the newcomer.
    The planner's contract is that all Phase A moves come first.
    """
    for i, m in enumerate(moves):
        if m.lvs_primary_node_id == new_node_id:
            return i
    return len(moves)


# ---------------------------------------------------------------------------
# 1. The worked example from the design plan: 4-node FTT2 cluster + 1
# ---------------------------------------------------------------------------

class TestFtt2FourToFive(unittest.TestCase):
    """Add n5 to {n1..n4} in FTT2. The expected diff is documented in the
    feature plan (single_node_expansion_plan.md, Phase A/B example)."""

    def setUp(self):
        self.existing = ["n1", "n2", "n3", "n4"]
        self.moves = compute_role_diff(self.existing, "n5", ftt=2)

    def test_exact_six_moves(self):
        self.assertEqual(len(self.moves), 6)

    def test_phase_a_re_homes(self):
        idx = _by_lvs_role(self.moves)
        # LVS3.tertiary moves n1 -> n5
        self.assertEqual(
            idx[("n3", ROLE_TERTIARY)],
            RoleMove("n3", ROLE_TERTIARY, "n1", "n5"))
        # LVS4.secondary moves n1 -> n5
        self.assertEqual(
            idx[("n4", ROLE_SECONDARY)],
            RoleMove("n4", ROLE_SECONDARY, "n1", "n5"))
        # LVS4.tertiary moves n2 -> n1
        self.assertEqual(
            idx[("n4", ROLE_TERTIARY)],
            RoleMove("n4", ROLE_TERTIARY, "n2", "n1"))

    def test_phase_b_creates_new_lvs(self):
        idx = _by_lvs_role(self.moves)
        self.assertEqual(
            idx[("n5", ROLE_PRIMARY)],
            RoleMove("n5", ROLE_PRIMARY, "", "n5"))
        self.assertEqual(
            idx[("n5", ROLE_SECONDARY)],
            RoleMove("n5", ROLE_SECONDARY, "", "n1"))
        self.assertEqual(
            idx[("n5", ROLE_TERTIARY)],
            RoleMove("n5", ROLE_TERTIARY, "", "n2"))

    def test_phase_a_precedes_phase_b(self):
        # All Phase A (re-homes) must come before any Phase B (creates) so
        # existing LVStores never drop below FTT during the transition.
        b_start = _phase_b_starts_at(self.moves, "n5")
        for m in self.moves[:b_start]:
            self.assertNotEqual(m.lvs_primary_node_id, "n5",
                                f"Phase B move {m} appeared in Phase A region")
            self.assertFalse(m.is_create,
                             f"Phase A move {m} should not be a create")
        for m in self.moves[b_start:]:
            self.assertEqual(m.lvs_primary_node_id, "n5")
            self.assertTrue(m.is_create)


# ---------------------------------------------------------------------------
# 2. FTT1 cases
# ---------------------------------------------------------------------------

class TestFtt1(unittest.TestCase):

    def test_two_to_three(self):
        """k=2 -> k=3: only LVS2.sec re-homes (n1 -> n3); newcomer adds 2."""
        moves = compute_role_diff(["n1", "n2"], "n3", ftt=1)
        idx = _by_lvs_role(moves)
        self.assertEqual(
            idx[("n2", ROLE_SECONDARY)],
            RoleMove("n2", ROLE_SECONDARY, "n1", "n3"))
        self.assertEqual(
            idx[("n3", ROLE_PRIMARY)],
            RoleMove("n3", ROLE_PRIMARY, "", "n3"))
        self.assertEqual(
            idx[("n3", ROLE_SECONDARY)],
            RoleMove("n3", ROLE_SECONDARY, "", "n1"))
        # FTT1 → no tertiary moves
        for m in moves:
            self.assertNotEqual(m.role, ROLE_TERTIARY)
        self.assertEqual(len(moves), 3)

    def test_three_to_four(self):
        moves = compute_role_diff(["n1", "n2", "n3"], "n4", ftt=1)
        idx = _by_lvs_role(moves)
        # LVS3.sec was n1 (3 mod 3 = 0 → n1); becomes n4 (3 mod 4 = 3 → n4)
        self.assertEqual(
            idx[("n3", ROLE_SECONDARY)],
            RoleMove("n3", ROLE_SECONDARY, "n1", "n4"))
        # LVS1, LVS2 sec unchanged → no moves for those LVSes in Phase A
        self.assertNotIn(("n1", ROLE_SECONDARY), idx)
        self.assertNotIn(("n2", ROLE_SECONDARY), idx)
        # Newcomer
        self.assertEqual(
            idx[("n4", ROLE_PRIMARY)],
            RoleMove("n4", ROLE_PRIMARY, "", "n4"))
        self.assertEqual(
            idx[("n4", ROLE_SECONDARY)],
            RoleMove("n4", ROLE_SECONDARY, "", "n1"))


# ---------------------------------------------------------------------------
# 3. FTT2 minimum cluster (3 -> 4)
# ---------------------------------------------------------------------------

class TestFtt2MinimumCluster(unittest.TestCase):

    def test_three_to_four(self):
        moves = compute_role_diff(["n1", "n2", "n3"], "n4", ftt=2)
        idx = _by_lvs_role(moves)
        # LVS1: sec=n2 (unchanged), tert=n3 (unchanged) → no Phase A moves
        self.assertNotIn(("n1", ROLE_SECONDARY), idx)
        self.assertNotIn(("n1", ROLE_TERTIARY), idx)
        # LVS2: sec=n3 (unchanged); tert was n1 (3 mod 3 = 0), becomes n4
        self.assertNotIn(("n2", ROLE_SECONDARY), idx)
        self.assertEqual(
            idx[("n2", ROLE_TERTIARY)],
            RoleMove("n2", ROLE_TERTIARY, "n1", "n4"))
        # LVS3: sec was n1, becomes n4; tert was n2, becomes n1
        self.assertEqual(
            idx[("n3", ROLE_SECONDARY)],
            RoleMove("n3", ROLE_SECONDARY, "n1", "n4"))
        self.assertEqual(
            idx[("n3", ROLE_TERTIARY)],
            RoleMove("n3", ROLE_TERTIARY, "n2", "n1"))
        # Newcomer creates: pri=n4, sec=n1, tert=n2
        self.assertEqual(
            idx[("n4", ROLE_PRIMARY)],
            RoleMove("n4", ROLE_PRIMARY, "", "n4"))
        self.assertEqual(
            idx[("n4", ROLE_SECONDARY)],
            RoleMove("n4", ROLE_SECONDARY, "", "n1"))
        self.assertEqual(
            idx[("n4", ROLE_TERTIARY)],
            RoleMove("n4", ROLE_TERTIARY, "", "n2"))


# ---------------------------------------------------------------------------
# 4. FTT2 5 -> 6 (sanity check rotation continues to work past 4->5)
# ---------------------------------------------------------------------------

class TestFtt2FiveToSix(unittest.TestCase):

    def test_invariants(self):
        existing = ["n1", "n2", "n3", "n4", "n5"]
        moves = compute_role_diff(existing, "n6", ftt=2)
        # Newcomer always brings exactly 3 create-moves under FTT2.
        creates = [m for m in moves if m.is_create]
        self.assertEqual(len(creates), 3)
        roles = sorted(m.role for m in creates)
        self.assertEqual(roles, sorted(
            [ROLE_PRIMARY, ROLE_SECONDARY, ROLE_TERTIARY]))

        # Every move must have distinct from/to (no-op moves are not emitted).
        for m in moves:
            if not m.is_create:
                self.assertNotEqual(m.from_node_id, m.to_node_id)


# ---------------------------------------------------------------------------
# 5. Layout invariants on the post-expand desired state
# ---------------------------------------------------------------------------

class TestPostExpandLayoutInvariants(unittest.TestCase):
    """Reconstruct the post-expand layout from the moves and assert that each
    LVS still has distinct primary/secondary/tertiary on different nodes."""

    def _reconstruct(self, existing, new_node_id, ftt):
        from simplyblock_core.cluster_expand_planner import _rotation_layout
        layout = {
            primary: {ROLE_PRIMARY: primary,
                      ROLE_SECONDARY: sec,
                      ROLE_TERTIARY: tert if ftt >= 2 else ""}
            for primary, sec, tert in _rotation_layout(list(existing), ftt)
        }
        moves = compute_role_diff(existing, new_node_id, ftt)
        for m in moves:
            if m.is_create and m.role == ROLE_PRIMARY:
                layout[m.lvs_primary_node_id] = {
                    ROLE_PRIMARY: m.to_node_id,
                    ROLE_SECONDARY: "",
                    ROLE_TERTIARY: "",
                }
            else:
                layout.setdefault(m.lvs_primary_node_id, {})
                layout[m.lvs_primary_node_id][m.role] = m.to_node_id
        return layout

    def test_ftt2_layout_distinct_per_lvs(self):
        layout = self._reconstruct(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        for primary_id, roles in layout.items():
            holders = [roles[ROLE_PRIMARY],
                       roles[ROLE_SECONDARY],
                       roles[ROLE_TERTIARY]]
            self.assertEqual(len(set(holders)), 3,
                             f"LVS {primary_id} has duplicate holders: {holders}")

    def test_ftt1_layout_distinct_per_lvs(self):
        layout = self._reconstruct(["n1", "n2", "n3"], "n4", ftt=1)
        for primary_id, roles in layout.items():
            self.assertNotEqual(roles[ROLE_PRIMARY], roles[ROLE_SECONDARY],
                                f"LVS {primary_id}: primary == secondary")


# ---------------------------------------------------------------------------
# 6. Input validation
# ---------------------------------------------------------------------------

class TestInputValidation(unittest.TestCase):

    def test_bad_ftt(self):
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n2"], "n3", ftt=0)
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n2"], "n3", ftt=3)

    def test_empty_new_node(self):
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n2"], "", ftt=1)

    def test_newcomer_already_present(self):
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n2", "n3"], "n2", ftt=1)

    def test_cluster_too_small_for_ftt(self):
        # FTT2 needs at least 3 nodes pre-expand (so primary/sec/tert distinct).
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n2"], "n3", ftt=2)
        # FTT1 needs at least 2.
        with self.assertRaises(ValueError):
            compute_role_diff(["n1"], "n2", ftt=1)

    def test_duplicate_existing_ids(self):
        with self.assertRaises(ValueError):
            compute_role_diff(["n1", "n1", "n2"], "n3", ftt=1)


# ---------------------------------------------------------------------------
# 7. Persistence helpers — RoleMove (de)serialization
# ---------------------------------------------------------------------------

class TestRoleMoveSerialization(unittest.TestCase):

    def test_round_trip(self):
        original = RoleMove("n3", ROLE_TERTIARY, "n1", "n5")
        self.assertEqual(move_from_dict(move_to_dict(original)), original)

    def test_create_move_round_trip(self):
        # Phase B create-moves have empty from_node_id; that must survive.
        original = RoleMove("n5", ROLE_PRIMARY, "", "n5")
        self.assertEqual(move_from_dict(move_to_dict(original)), original)

    def test_dict_is_json_safe(self):
        # The expand_state lives in FoundationDB as JSON. Every move dict must
        # round-trip through json without coercion.
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        wire = json.dumps([move_to_dict(m) for m in moves])
        decoded = [move_from_dict(d) for d in json.loads(wire)]
        self.assertEqual(decoded, moves)


# ---------------------------------------------------------------------------
# 8. Persistence helpers — make_expand_state and lifecycle
# ---------------------------------------------------------------------------

class TestExpandStateLifecycle(unittest.TestCase):

    def _fresh_state(self):
        moves = compute_role_diff(["n1", "n2", "n3", "n4"], "n5", ftt=2)
        return make_expand_state("n5", moves), moves

    def test_make_expand_state_defaults(self):
        state, moves = self._fresh_state()
        self.assertEqual(state["schema_version"], EXPAND_STATE_SCHEMA_VERSION)
        self.assertEqual(state["phase"], EXPAND_PHASE_IN_PROGRESS)
        self.assertEqual(state["new_node_id"], "n5")
        self.assertEqual(state["cursor"], 0)
        self.assertEqual(len(state["moves"]), len(moves))

    def test_make_expand_state_rejects_empty_new_node(self):
        with self.assertRaises(ValueError):
            make_expand_state("", [])

    def test_empty_state_is_not_in_progress(self):
        self.assertFalse(is_expand_in_progress({}))
        self.assertEqual(pending_moves({}), [])

    def test_pending_moves_initial(self):
        state, moves = self._fresh_state()
        self.assertEqual(pending_moves(state), moves)

    def test_advance_progresses_cursor(self):
        state, moves = self._fresh_state()
        for i in range(len(moves)):
            self.assertEqual(state["cursor"], i)
            self.assertEqual(len(pending_moves(state)), len(moves) - i)
            state = expand_state_advance(state)
        # All moves consumed → cursor at end, pending list empty
        self.assertEqual(state["cursor"], len(moves))
        self.assertEqual(pending_moves(state), [])

    def test_advance_returns_new_dict(self):
        # Pure function: must not mutate input.
        state, _ = self._fresh_state()
        before = dict(state)
        new_state = expand_state_advance(state)
        self.assertEqual(state, before)
        self.assertIsNot(state, new_state)

    def test_advance_past_end_raises(self):
        state, moves = self._fresh_state()
        for _ in moves:
            state = expand_state_advance(state)
        with self.assertRaises(ValueError):
            expand_state_advance(state)

    def test_advance_aborted_state_raises(self):
        state, _ = self._fresh_state()
        aborted = expand_state_abort(state, reason="user requested")
        with self.assertRaises(ValueError):
            expand_state_advance(aborted)

    def test_complete_requires_all_moves_done(self):
        state, _ = self._fresh_state()
        # cursor still at 0 — completing must refuse
        with self.assertRaises(ValueError):
            expand_state_complete(state)

    def test_complete_marks_phase(self):
        state, moves = self._fresh_state()
        for _ in moves:
            state = expand_state_advance(state)
        completed = expand_state_complete(state)
        self.assertEqual(completed["phase"], EXPAND_PHASE_COMPLETED)
        # Idempotent
        again = expand_state_complete(completed)
        self.assertEqual(again["phase"], EXPAND_PHASE_COMPLETED)

    def test_complete_rejects_aborted(self):
        state, _ = self._fresh_state()
        aborted = expand_state_abort(state, reason="x")
        with self.assertRaises(ValueError):
            expand_state_complete(aborted)

    def test_abort_preserves_cursor_and_moves(self):
        state, _ = self._fresh_state()
        state = expand_state_advance(state)
        state = expand_state_advance(state)
        aborted = expand_state_abort(state, reason="JC quorum lost")
        self.assertEqual(aborted["phase"], EXPAND_PHASE_ABORTED)
        self.assertEqual(aborted["abort_reason"], "JC quorum lost")
        self.assertEqual(aborted["cursor"], 2)
        self.assertEqual(aborted["moves"], state["moves"])

    def test_abort_requires_reason(self):
        state, _ = self._fresh_state()
        with self.assertRaises(ValueError):
            expand_state_abort(state, reason="")

    def test_abort_empty_state_raises(self):
        with self.assertRaises(ValueError):
            expand_state_abort({}, reason="x")


# ---------------------------------------------------------------------------
# 9. Schema version compatibility check
# ---------------------------------------------------------------------------

class TestSchemaCompat(unittest.TestCase):

    def test_empty_state_is_compatible(self):
        self.assertTrue(is_expand_state_compatible({}))

    def test_current_version_compatible(self):
        state, _ = TestExpandStateLifecycle()._fresh_state()
        self.assertTrue(is_expand_state_compatible(state))

    def test_future_version_incompatible(self):
        bad = {"schema_version": EXPAND_STATE_SCHEMA_VERSION + 1,
               "phase": EXPAND_PHASE_IN_PROGRESS, "moves": [], "cursor": 0,
               "new_node_id": "n5"}
        self.assertFalse(is_expand_state_compatible(bad))

    def test_missing_schema_version_incompatible(self):
        bad = {"phase": EXPAND_PHASE_IN_PROGRESS, "moves": [], "cursor": 0,
               "new_node_id": "n5"}
        self.assertFalse(is_expand_state_compatible(bad))


if __name__ == "__main__":
    unittest.main()
