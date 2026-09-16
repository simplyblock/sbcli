"""Validate a drain target before committing to it, not after it fails.

_get_next_3_nodes answers "where would a NEW volume go?" -- capacity, subsystem
slots, load weighting. That is the right question for placement and not the
whole question for a migration: a candidate can be a perfectly good placement
and still be refused by create_migration or the task runner.

Learning that from the exception is expensive. It spends one of the drain's ten
attempts against that target plus NODE_DRAIN_RETRY_WAIT_SEC (300s) of wall
clock, to discover something answerable from the DB for free -- and with 10
attempts per target across several targets, a deterministic refusal can burn
most of an hour before the removal gives up.

check_target_viable asks the same questions create_migration and the runner
ask, in advance, so _pick_drain_target can move to the next candidate instead.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops


class TestPickerSkipsNonViableCandidates(unittest.TestCase):

    def _pick(self, candidates, viability):
        """viability: {node_id: (ok, reason)}"""
        snode = MagicMock()
        snode.get_id.return_value = "departing"
        snode.cluster_id = "c1"
        lvol = MagicMock()
        lvol.get_id.return_value = "lv1"
        lvol.size = 1024
        lvol.max_namespace_per_subsys = 1
        acting = MagicMock()
        acting.get_id.return_value = "acting-source"
        asked = []

        def _check(lvol_id, node_id):
            asked.append(node_id)
            return viability.get(node_id, (True, ""))

        with patch.object(storage_node_ops.lvol_controller, "_get_next_3_nodes",
                          return_value=list(candidates)), \
             patch.object(storage_node_ops.migration_controller,
                          "resolve_source_node", return_value=acting), \
             patch.object(storage_node_ops.migration_controller,
                          "check_target_viable", side_effect=_check):
            got = storage_node_ops._pick_drain_target(snode, lvol, [], MagicMock())
        return got, asked

    def test_a_non_viable_candidate_is_skipped_for_the_next(self):
        got, asked = self._pick(
            ["n1", "n2"],
            {"n1": (False, "target has a data migration in progress")})
        self.assertEqual(got, "n2")
        self.assertEqual(asked, ["n1", "n2"], "it must actually ask about n1")

    def test_the_first_viable_candidate_wins(self):
        got, asked = self._pick(["n1", "n2"], {})
        self.assertEqual(got, "n1")
        self.assertEqual(asked, ["n1"], "no need to ask past the first yes")

    def test_all_non_viable_returns_none(self):
        got, _ = self._pick(
            ["n1", "n2"],
            {"n1": (False, "target is down, not online"),
             "n2": (False, "target is currently serving as the fallback source")})
        self.assertIsNone(got, "exhausting candidates must escalate, not guess")

    def test_excluded_candidates_are_not_even_asked(self):
        """The cheap exclusions still short-circuit before the DB check."""
        snode = MagicMock()
        snode.get_id.return_value = "departing"
        snode.cluster_id = "c1"
        lvol = MagicMock()
        lvol.get_id.return_value = "lv1"
        lvol.size = 1024
        lvol.max_namespace_per_subsys = 1
        acting = MagicMock()
        acting.get_id.return_value = "acting-source"
        asked = []
        with patch.object(storage_node_ops.lvol_controller, "_get_next_3_nodes",
                          return_value=["departing", "acting-source", "good"]), \
             patch.object(storage_node_ops.migration_controller,
                          "resolve_source_node", return_value=acting), \
             patch.object(storage_node_ops.migration_controller, "check_target_viable",
                          side_effect=lambda l, n: asked.append(n) or (True, "")):
            got = storage_node_ops._pick_drain_target(snode, lvol, [], MagicMock())
        self.assertEqual(got, "good")
        self.assertEqual(asked, ["good"],
                         "the departing node and the acting source are excluded "
                         "before any viability query")


class TestTheCheckCoversTheAdmissionRules(unittest.TestCase):
    """check_target_viable must ask the same questions the admission path does,
    so selection and admission cannot disagree."""

    def test_it_checks_each_documented_rule(self):
        import inspect
        from simplyblock_core.controllers import migration_controller as mc
        src = inspect.getsource(mc.check_target_viable)
        for probe in ("STATUS_ONLINE",              # target online
                      "lvstore",                    # target has an lvstore
                      "already hosts this volume",  # not the volume's primary
                      "resolve_source_node",        # not the acting source
                      "_get_target_secondary_node", # target replica gate
                      "_get_target_tertiary_node",
                      "get_active_node_mig_task"):  # no device migration running
            self.assertIn(probe, src, f"viability check must consider {probe}")

    def test_it_is_read_only(self):
        """It runs inside target selection; it must not mutate anything."""
        import inspect
        from simplyblock_core.controllers import migration_controller as mc
        fn = mc.check_target_viable
        body = inspect.getsource(fn).replace(fn.__doc__ or "", "")
        for forbidden in ("write_to_db", "rpc_client", "create_migration"):
            self.assertNotIn(forbidden, body,
                             f"a selection-time check must not {forbidden}")


if __name__ == "__main__":
    unittest.main()
