"""Replication does not run ahead of snapshot deletion by more than a few rounds.

2026-11-03 (bed bf11): the cutover endgame held up to 12 snapshots per volume
and deletion lagged; every kept snapshot is one more copy generation behind
every rewritten page. A volume with REPL_MAX_DELETION_BACKLOG snapshots waiting
to be deleted takes no new internal snapshot, and an endgame holds at most
REPL_CUTOVER_MAX_SHRINK_ROUNDS (4) rounds.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.services import snapshot_replication as sr


def _snap(status=SnapShot.STATUS_ONLINE, replicated=True, kind=SnapShot.TYPE_INTERNAL):
    return SimpleNamespace(status=status, snap_type=kind,
                           target_replicated_snap_uuid="T" if replicated else "")


LVOL = SimpleNamespace(get_id=lambda: "LV1", do_replicate=True, replication_interval_min=1,
                       group_id="", replication_policy_id="")


@pytest.fixture
def snaps(monkeypatch):
    st = {"snaps": []}
    monkeypatch.setattr(sm.db, "get_snapshots_by_lvol_id", lambda lid: st["snaps"])
    monkeypatch.setattr(sr, "_keep_replicated_for", lambda lvol: 2)
    return st


def test_the_pairs_retention_keeps_are_no_backlog(snaps):
    snaps["snaps"] = [_snap(), _snap(), _snap(replicated=False)]
    assert sm._deletion_backlog(LVOL) == 0


def test_replicated_snapshots_beyond_retention_and_deletes_in_flight_count(snaps):
    snaps["snaps"] = [_snap()] * 4 + [_snap(status=SnapShot.STATUS_IN_DELETION)]
    assert sm._deletion_backlog(LVOL) == 3      # 2 beyond keep + 1 in deletion


def test_user_snapshots_are_not_counted(snaps):
    snaps["snaps"] = [_snap(kind=SnapShot.TYPE_USER if hasattr(SnapShot, "TYPE_USER") else "user")] * 5
    assert sm._deletion_backlog(LVOL) == 0


def _run(backlog):
    db = MagicMock()
    db.get_lvols.return_value = [LVOL]
    db.get_replication_policies.return_value = []
    db.get_mini_snapshots.return_value = []
    db.get_cluster_by_id.return_value = SimpleNamespace(sync_replication=False)
    with patch.object(sm, "db", db), \
            patch.object(sm.migration_turn, "waiting_lvol_ids", return_value=set()), \
            patch.object(sm, "_cadence_capacity_hold", return_value=None), \
            patch.object(sm, "_due_for_internal_snapshot", return_value=True), \
            patch.object(sm, "_outstanding_internal_snapshot", return_value=None), \
            patch.object(sm, "_deletion_backlog", return_value=backlog), \
            patch.object(sm.replication_delta, "add_snapshot", return_value=("S", None)) as add:
        sm.take_due_internal_snapshots("CL", 1000)
    return add


def test_a_deletion_backlog_at_the_limit_holds_the_next_snapshot():
    assert constants.REPL_MAX_DELETION_BACKLOG == 3
    _run(3).assert_not_called()


def test_below_the_limit_the_cadence_continues():
    _run(2).assert_called_once()


def test_an_endgame_holds_at_most_four_rounds():
    assert constants.REPL_CUTOVER_MAX_SHRINK_ROUNDS == 4
