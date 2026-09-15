"""The removal re-checks its admission conditions once the node is down.

Admission asks three questions -- are the peers online, is there FTT headroom,
does the per-domain balance still hold -- and then the removal spends minutes to
hours shutting the node down, rebuilding its devices onto peers and draining its
volumes. Another node can go offline in that window. Committing to the teardown
on a judgement made before all of that is how a removal proceeds into a cluster
that can no longer absorb it.

The re-check asks the same three questions, not a looser set: one that admitted
something admission would have refused would be worse than none at all.

It gets its own budget rather than the whole-removal one. A drain may
legitimately run for hours; a peer that has not returned within half an hour is
not returning on the removal's timescale, and waiting the full budget out would
hold a shut-down node hostage to it.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


def _peer(node_id, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = node_id
    node.cluster_id = "c1"
    node.status = status
    return node


class TestRecheckConditions(unittest.TestCase):

    def setUp(self):
        self.node = _peer("n1")
        self.db = MagicMock()
        self.db.get_storage_nodes_by_cluster_id.return_value = [
            self.node, _peer("n2"), _peer("n3")]
        self.db.get_cluster_by_id.return_value = MagicMock()

    def _check(self, ftt=(True, ""), fd=(True, "")):
        with patch.object(storage_node_ops, "_check_ftt_allows_node_removal",
                          return_value=ftt), \
             patch("simplyblock_core.controllers.cluster_expansion.preconditions"
                   ".check_fd_admission_for_remove", return_value=fd):
            return storage_node_ops._recheck_removal_conditions(self.node, self.db)

    def test_passes_when_the_cluster_is_still_able_to_absorb_the_removal(self):
        ok, reason = self._check()
        self.assertTrue(ok)
        self.assertEqual(reason, "")

    def test_fails_when_a_peer_went_offline_after_admission(self):
        self.db.get_storage_nodes_by_cluster_id.return_value = [
            self.node, _peer("n2"), _peer("n3", StorageNode.STATUS_OFFLINE)]
        ok, reason = self._check()
        self.assertFalse(ok)
        self.assertIn("n3", reason)

    def test_an_already_removed_peer_does_not_count_against_it(self):
        """A node removed earlier is gone, not unavailable -- counting it would
        make every removal after the first one impossible."""
        self.db.get_storage_nodes_by_cluster_id.return_value = [
            self.node, _peer("n2"), _peer("old", StorageNode.STATUS_REMOVED)]
        self.assertTrue(self._check()[0])

    def test_a_peer_that_failed_its_own_removal_does_not_count_either(self):
        self.db.get_storage_nodes_by_cluster_id.return_value = [
            self.node, _peer("n2"), _peer("bad", StorageNode.STATUS_REMOVED_FAILED)]
        self.assertTrue(self._check()[0])

    def test_fails_when_ftt_headroom_is_gone(self):
        ok, reason = self._check(ftt=(False, "cluster would drop below FTT"))
        self.assertFalse(ok)
        self.assertIn("FTT", reason)

    def test_fails_when_the_domain_balance_no_longer_holds(self):
        ok, reason = self._check(fd=(False, "domain would be left with one host"))
        self.assertFalse(ok)
        self.assertIn("domain", reason)


class TestRecheckIsBounded(unittest.TestCase):

    def test_it_has_its_own_budget_separate_from_the_removal(self):
        self.assertLess(constants.NODE_REMOVAL_CONDITION_WAIT_SEC,
                        constants.NODE_REMOVAL_MAX_WAIT_SEC,
                        "a condition that will never pass must not be able to "
                        "consume the whole removal budget")

    def test_the_cursor_can_time_a_single_step(self):
        cur = storage_node_ops._NullCursor()
        cur.enter("recheck_conditions", "re-check")
        self.assertLess(cur.elapsed(), 5)

    def test_re_entering_a_step_does_not_restart_its_clock(self):
        """Otherwise a step retried every few seconds could never age out."""
        cur = storage_node_ops._NullCursor()
        cur.enter("recheck_conditions", "re-check")
        cur._entered["recheck_conditions"] -= 600
        cur.enter("recheck_conditions", "re-check")
        self.assertGreater(cur.elapsed(), 500)

    def test_advancing_to_a_new_step_does_restart_it(self):
        cur = storage_node_ops._NullCursor()
        cur.enter("recheck_conditions", "re-check")
        cur._entered["recheck_conditions"] -= 600
        cur.enter("drain_lvols", "drain")
        self.assertLess(cur.elapsed(), 5)


class TestRecheckRunsBeforeAnythingIsTouched(unittest.TestCase):

    def test_it_precedes_the_device_and_drain_work(self):
        src = __import__("inspect").getsource(
            storage_node_ops.node_removal_orchestrate)
        recheck = src.index('cursor.enter("recheck_conditions"')
        devices = src.index('cursor.enter("migrate_devices"')
        self.assertLess(
            recheck, devices,
            "the re-check is only useful while nothing has been done to the "
            "node yet -- after that there is no cheap way back")


if __name__ == "__main__":
    unittest.main()
