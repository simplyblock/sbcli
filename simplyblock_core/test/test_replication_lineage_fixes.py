"""Lineage bookkeeping found on the bf6 back-and-forth bed (2026-10-29).

- A deleted snapshot left its chain neighbours pointing at it, so walking a
  migrated volume's lineage stopped at the first pruned landing copy and every
  older copy was never retired (65 snapshots, 74 GiB, on a cluster with no
  volumes).
- A chain ancestor queued without a destination resolved it from its REP_*
  receiving volume and was dropped, and the descendant waited on it until it
  gave up (vol-5, ~15 minutes in round 5).
- A volume deleted mid-migration left its cutover task suspended for good.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import lvol_controller, replication_snapshot_retire
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.services import snapshot_replication as sr


def _snap(uuid, prev="", nxt=""):
    return SimpleNamespace(uuid=uuid, prev_snap_uuid=prev, next_snap_uuid=nxt,
                           cluster_id="CL", get_id=lambda u=uuid: u)


class _SnapDB:
    def __init__(self, snaps, lvols=()):
        self.snaps = {s.uuid: s for s in snaps}
        self.lvols = list(lvols)

    def get_snapshot_by_id(self, uuid):
        if uuid not in self.snaps:
            raise KeyError(uuid)
        return self.snaps[uuid]

    def atomic_update(self, obj, fn):
        fn(obj)
        return obj

    def get_lvols(self, cluster_id=None):
        return self.lvols


# --- a deleted snapshot closes the gap in its chain ---------------------------

@pytest.fixture
def chain(monkeypatch):
    """C1 <- C2 <- C3: landing copies, C3 the newest."""
    db = _SnapDB([_snap("C1", nxt="C2"), _snap("C2", prev="C1", nxt="C3"), _snap("C3", prev="C2")])
    monkeypatch.setattr(sm, "db", db)
    monkeypatch.setattr(replication_snapshot_retire, "_sync_mini", lambda db, s: None)
    return db


def test_deleting_a_middle_snapshot_relinks_its_neighbours(chain):
    sm._unlink_from_chain(chain.snaps.pop("C2"))
    assert chain.snaps["C3"].prev_snap_uuid == "C1"
    assert chain.snaps["C1"].next_snap_uuid == "C3"


def test_deleting_the_oldest_leaves_the_successor_without_a_predecessor(chain):
    sm._unlink_from_chain(chain.snaps.pop("C1"))
    assert chain.snaps["C2"].prev_snap_uuid == ""


def test_a_neighbour_that_points_elsewhere_is_left_alone(chain):
    chain.snaps["C3"].prev_snap_uuid = "OTHER"
    sm._unlink_from_chain(chain.snaps.pop("C2"))
    assert chain.snaps["C3"].prev_snap_uuid == "OTHER"
    assert chain.snaps["C1"].next_snap_uuid == "C3"


# --- a chain ancestor travels for the live volume cloned from it --------------

def _lvol(uuid, cloned_from, destination="TNODE"):
    return SimpleNamespace(uuid=uuid, cloned_from_snap=cloned_from,
                           replication_node_id=destination, get_id=lambda u=uuid: u)


def test_the_descendant_is_the_volume_whose_ancestry_holds_the_snapshot(monkeypatch):
    live = _lvol("VOL5", "C3")
    monkeypatch.setattr(sr, "db", _SnapDB(
        [_snap("C1"), _snap("C2", prev="C1"), _snap("C3", prev="C2")],
        [_lvol("OTHER", "X1"), live]))
    assert sr._descendant_destination_lvol(_snap("C1")) is live


def test_a_volume_without_a_destination_is_not_a_descendant(monkeypatch):
    monkeypatch.setattr(sr, "db", _SnapDB(
        [_snap("C1"), _snap("C2", prev="C1")], [_lvol("REP", "C2", destination="")]))
    assert sr._descendant_destination_lvol(_snap("C1")) is None


def test_a_snapshot_outside_every_ancestry_has_no_descendant(monkeypatch):
    monkeypatch.setattr(sr, "db", _SnapDB(
        [_snap("C1"), _snap("C2", prev="C1")], [_lvol("VOL5", "C2")]))
    assert sr._descendant_destination_lvol(_snap("ELSEWHERE")) is None


def test_a_broken_link_ends_the_walk(monkeypatch):
    monkeypatch.setattr(sr, "db", _SnapDB(
        [_snap("C2", prev="GONE")], [_lvol("VOL5", "C2")]))
    assert sr._descendant_destination_lvol(_snap("C1")) is None


# --- a volume that stops replicating ends its cutover -------------------------

def _task(fn, lvol_id, status=JobSchedule.STATUS_SUSPENDED, canceled=False, uuid="T"):
    return SimpleNamespace(function_name=fn, function_params={"lvol_id": lvol_id},
                           status=status, canceled=canceled, uuid=uuid)


def test_only_the_volumes_open_cutover_is_cancelled(monkeypatch):
    cancelled: list[str] = []
    monkeypatch.setattr(lvol_controller.tasks_controller, "cancel_task", cancelled.append)
    tasks = [
        _task(JobSchedule.FN_REPLICATION_FINAL, "V1", uuid="open"),
        _task(JobSchedule.FN_REPLICATION_FINAL, "V1", status=JobSchedule.STATUS_DONE, uuid="done"),
        _task(JobSchedule.FN_REPLICATION_FINAL, "V1", canceled=True, uuid="cancelled"),
        _task(JobSchedule.FN_REPLICATION_FINAL, "V2", uuid="other-volume"),
        _task(JobSchedule.FN_SNAPSHOT_REPLICATION, "V1", uuid="not-a-cutover"),
    ]
    lvol_controller._cancel_replication_cutover("V1", tasks)
    assert cancelled == ["open"]


def test_a_committed_cutover_wrapping_up_is_not_cancelled(monkeypatch):
    # The cutover's own wrap-up stops the source's replication and deletes the
    # source (--delete-source) while the task is still running; both reach
    # _cancel_replication_cutover. A committed cutover (end_time recorded) must
    # finish as done, not canceled (bf7 2026-10-30: reported failed, PV never
    # pointed at the target).
    cancelled: list[str] = []
    monkeypatch.setattr(lvol_controller.tasks_controller, "cancel_task", cancelled.append)
    committed = _task(JobSchedule.FN_REPLICATION_FINAL, "V1", status=JobSchedule.STATUS_RUNNING,
                      uuid="committed")
    committed.function_params["end_time"] = 1793380000
    lvol_controller._cancel_replication_cutover("V1", [committed])
    assert cancelled == []
