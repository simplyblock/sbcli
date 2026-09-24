"""Unit tests for is_lvol_removal_deferred_for_pending_failover.

lvol_monitor's process_lvol_delete_finish reaps a deleted LVol's FDB record
right after its physical data is destroyed (lvol_monitor.py:349). For a
volume that was demoted and is awaiting a pending PromoteVolume call
addressed by this SAME id (design-csi-addons-replication.md; Ramen relocate
M-02, confirmed live 2026-09-24), reaping the record strands that promote
with "LVol not found" -- even though there is nothing left to protect except
a few bookkeeping fields (replication_node_id, nqn/ns_id, cluster ids) the
clone needs. This predicate decides whether the record's removal must be
withheld.
"""
from datetime import datetime, timedelta

from simplyblock_core import constants
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot


def _lvol(demote_snapshot_id=""):
    lv = LVol()
    lv.uuid = "LV1"
    lv.replication_demote_snapshot_id = demote_snapshot_id
    return lv


def _snap(uuid, age_sec=0):
    s = SnapShot()
    s.uuid = uuid
    s.create_dt = str(datetime.now() - timedelta(seconds=age_sec))
    return s


def _relationship(source_id="LV1", state=LVolReplication.STATE_REPLICATING):
    rep = LVolReplication()
    rep.uuid = "REP1"
    source = LVol()
    source.uuid = source_id
    rep.source_lvol = source
    rep.state = state
    return rep


class _FakeDB:
    def __init__(self, snapshots=None, replications=None):
        self._snapshots = snapshots or {}
        self._replications = list(replications or [])

    def get_snapshot_by_id(self, uuid):
        try:
            return self._snapshots[uuid]
        except KeyError:
            raise KeyError(f"Snapshot {uuid} not found")

    def get_lvol_replication_objects(self):
        return self._replications


def test_not_demoted_is_never_deferred():
    lvol = _lvol(demote_snapshot_id="")
    db = _FakeDB()
    assert not lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_demoted_with_no_relationship_yet_is_deferred():
    lvol = _lvol(demote_snapshot_id="S1")
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=5)})
    assert lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_demoted_but_already_failed_over_is_not_deferred():
    lvol = _lvol(demote_snapshot_id="S1")
    rep = _relationship(state=LVolReplication.STATE_FAILED_OVER)
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=5)}, replications=[rep])
    assert not lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_demoted_but_cutover_done_is_not_deferred():
    lvol = _lvol(demote_snapshot_id="S1")
    rep = _relationship(state=LVolReplication.STATE_CUTOVER_DONE)
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=5)}, replications=[rep])
    assert not lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_demoted_still_replicating_relationship_stays_deferred():
    """A relationship that has NOT reached fail-over yet is not a release
    signal -- the demote's own fail-over point is still what a pending
    promote would use."""
    lvol = _lvol(demote_snapshot_id="S1")
    rep = _relationship(state=LVolReplication.STATE_CUTOVER_PENDING)
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=5)}, replications=[rep])
    assert lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_hold_window_expired_gives_up(monkeypatch):
    monkeypatch.setattr(constants, "LVOL_DEMOTE_FAILOVER_HOLD_SEC", 60)
    lvol = _lvol(demote_snapshot_id="S1")
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=61)})
    assert not lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_hold_window_not_yet_expired_stays_deferred(monkeypatch):
    monkeypatch.setattr(constants, "LVOL_DEMOTE_FAILOVER_HOLD_SEC", 60)
    lvol = _lvol(demote_snapshot_id="S1")
    db = _FakeDB(snapshots={"S1": _snap("S1", age_sec=59)})
    assert lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)


def test_fail_over_point_snapshot_already_gone_is_not_deferred():
    """Nothing left to protect once the snapshot itself is gone -- do not
    hold the record hostage for a fail-over point that no longer exists."""
    lvol = _lvol(demote_snapshot_id="S1")
    db = _FakeDB(snapshots={})
    assert not lvol_controller.is_lvol_removal_deferred_for_pending_failover(db, lvol)
