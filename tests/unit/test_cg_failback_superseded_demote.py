"""Consistency-group fail-back demote after the 2026-10-01 DR test.

An UNPLANNED fail-over shuts the source cluster's data plane down, so
``replication_stop`` never runs on its volumes to clear ``do_replicate``. The
recovered old primary therefore comes back with its group members still
``do_replicate=True``. The superseded-source fast-demote path -- which fences
and marks the old primary demoted at once, since the peer's clone already holds
every post-fail-over write -- was gated on ``do_replicate=False``, so it skipped
those members and ``demote_group`` fell to the ship-home path: a group snapshot
on the stale old primary, waited on to replicate to a peer that is itself the
new primary. It never converged ("group demote is still converging"
indefinitely), so the DRPC's PeerReady never flipped and the relocate home could
not be issued.

A member that is the SOURCE of a FAILED_OVER relationship IS the recovered old
primary, whether or not ``do_replicate`` was cleared -- the planned-relocate
source the gate was protecting is never FAILED_OVER, so the relationship state
is the real discriminator.
"""
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import consistency_group_controller as cgc
from simplyblock_core.models.lvol_model import LVolReplication

_REP_FOR = "simplyblock_core.controllers.lvol_controller._replication_for_lvol"


def _member(lvol_id, do_replicate):
    m = MagicMock()
    m.get_id.return_value = lvol_id
    m.do_replicate = do_replicate
    return m


def _failed_over_with_source(source_id):
    rep = MagicMock(state=LVolReplication.STATE_FAILED_OVER)
    rep.source_lvol.get_id.return_value = source_id
    rep.target_lvol.get_id.return_value = "peer-" + source_id
    return rep


class TestSupersededSourceDetection(unittest.TestCase):

    def test_do_replicate_true_source_is_still_superseded(self):
        # The 2026-10-01 bug: an unplanned fail-over leaves the recovered old
        # primary's members do_replicate=True; they must still be recognised as
        # the superseded source so demote_group fences them instead of shipping.
        members = [_member(f"m{i}", do_replicate=True) for i in range(8)]
        with patch(_REP_FOR, side_effect=lambda db, lvol_id: _failed_over_with_source(lvol_id)):
            self.assertTrue(cgc._members_are_superseded_source(members))

    def test_a_member_not_failed_over_is_not_superseded(self):
        # A planned-relocate source has a live pipe and a non-FAILED_OVER
        # relationship -- it must NOT be treated as a superseded old primary.
        members = [_member("m0", do_replicate=True)]
        rep = MagicMock(state=LVolReplication.STATE_REPLICATING
                        if hasattr(LVolReplication, "STATE_REPLICATING") else "replicating")
        rep.source_lvol.get_id.return_value = "m0"
        with patch(_REP_FOR, side_effect=lambda db, lvol_id: rep):
            self.assertFalse(cgc._members_are_superseded_source(members))

    def test_a_target_side_member_is_not_a_superseded_source(self):
        # The TARGET of a FAILED_OVER relationship (the new primary's clone) is
        # not the superseded source -- only the source side is.
        members = [_member("m0", do_replicate=False)]
        rep = MagicMock(state=LVolReplication.STATE_FAILED_OVER)
        rep.source_lvol.get_id.return_value = "someone-else"
        rep.target_lvol.get_id.return_value = "m0"
        with patch(_REP_FOR, side_effect=lambda db, lvol_id: rep):
            self.assertFalse(cgc._members_are_superseded_source(members))

    def test_a_member_without_any_relationship_is_not_superseded(self):
        members = [_member("m0", do_replicate=False)]
        with patch(_REP_FOR, side_effect=lambda db, lvol_id: None):
            self.assertFalse(cgc._members_are_superseded_source(members))
