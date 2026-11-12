"""Replication chains stay bounded in the replication phase.

Bed bf21 2026-11-10 (EKS, 2 x 5 nodes, 8 x 10 GiB volumes under heavy fio
rewrites, migration rounds back and forth with deleteSource): blob chains of
live migrated volumes reached depth 7, landing volumes depth 4; each layer
pinned data the client had overwritten since, the clusters reached 93-99 %
device use (cannot_allocate) and client writes got EIO. Spare landing volumes
of migrated-away volumes piled up (15-21 per cluster for 8 volumes).

Here: a volume's next transfer and its next cadence snapshot wait while the
target has not merged its earlier layers away (or the source has too many
replication snapshots pending); spares are not re-created after the discard
and are swept once their volume no longer replicates.
"""
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core import replication_spare as rs
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_replication_final as final

ONLINE = SnapShot.STATUS_ONLINE
DELETING = SnapShot.STATUS_IN_DELETION


def _snap(uuid, created, target="", status=ONLINE, kind=SnapShot.TYPE_INTERNAL, prev=""):
    return SimpleNamespace(uuid=uuid, get_id=lambda u=uuid: u, created_at=created,
                           target_replicated_snap_uuid=target,
                           source_replicated_snap_uuid="", status=status,
                           snap_type=kind, prev_snap_uuid=prev, group_id="")


def _task(snapshot_id, transfer_done=False, status=JobSchedule.STATUS_RUNNING):
    t = JobSchedule()
    t.uuid = f"T_{snapshot_id}"
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.function_params = {"snapshot_id": snapshot_id, "replicate_to_source": False}
    if transfer_done:
        t.function_params["transfer_done"] = True
    t.status = status
    return t


LVOL = SimpleNamespace(get_id=lambda: "LV1", do_replicate=True, replication_interval_min=1,
                       group_id="", replication_policy_id="", node_id="N1",
                       status=LVol.STATUS_ONLINE)


class _World:
    """The source volume's snapshots, their copies on the target, and tasks."""

    def __init__(self):
        self.source: list = []
        self.copies: dict = {}
        self.tasks: list = []

    def add_pair(self, n, copy_status=ONLINE, src_status=ONLINE, replicated=True, prev=""):
        cid = f"C{n}"
        if replicated:
            self.copies[cid] = _snap(cid, n, status=copy_status, kind="user", prev=prev)
        self.source.append(_snap(f"S{n}", n, target=cid if replicated else "",
                                 status=src_status))
        return cid

    def get_snapshots_by_lvol_id(self, lvol_id):
        return list(self.source)

    def get_snapshot_by_id(self, sid):
        for s in self.source:
            if s.uuid == sid:
                return s
        if sid in self.copies:
            return self.copies[sid]
        raise KeyError(sid)

    def get_job_tasks_by_function(self, cluster_id, fn):
        return list(self.tasks)

    def get_lvol_by_id(self, lvol_id):
        return LVOL


@pytest.fixture
def world(monkeypatch):
    w = _World()
    monkeypatch.setattr(sr, "db", w)
    monkeypatch.setattr(sm, "db", w)
    monkeypatch.setattr(sr, "_keep_replicated_for", lambda lvol: 2)
    monkeypatch.setattr(sr, "_retention_schedule_for", lambda lvol: [])
    monkeypatch.setattr(sr, "_has_dependent_clone", lambda sid: False)
    monkeypatch.setattr(sr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())
    monkeypatch.setattr(sr, "_cutover_owns", lambda lvol_id, cluster_id: False)
    pruned: list = []
    monkeypatch.setattr(sr, "prune_async", lambda lvol, keep=None: pruned.append(lvol.get_id()))
    w.pruned = pruned
    monkeypatch.setattr(sm, "_MERGE_HOLDS", {})
    return w


# --- the signal: unmerged landing layers ------------------------------------

def test_a_volume_that_keeps_up_has_no_backlog(world):
    world.add_pair(1)
    world.add_pair(2, prev="C1")
    assert sr.merge_backlog(LVOL, "CL")[0] == 0


def test_pairs_beyond_retention_copies_being_merged_and_landing_merges_count(world):
    world.add_pair(1, copy_status=DELETING, src_status=DELETING)  # being merged away
    world.add_pair(2, prev="C1")
    world.add_pair(3, prev="C2")
    world.add_pair(4, prev="C3")                                  # C2 beyond keep=2
    world.add_pair(5, replicated=False)                          # transfer done, merging
    world.tasks = [_task("S5", transfer_done=True)]
    count, detail, prunable = sr.merge_backlog(LVOL, "CL")
    assert prunable == 1
    assert count == 3, detail
    assert "1 beyond retention" in detail and "1 being merged" in detail
    assert "1 landing" in detail


def test_a_copy_whose_source_record_is_gone_is_found_down_the_chain(world):
    """Prune deletes the source too; its copy may still be merging after the
    source record is removed."""
    world.copies["C0"] = _snap("C0", 0, status=DELETING, kind="user")
    world.add_pair(1, prev="C0")
    world.add_pair(2, prev="C1")
    assert sr.merge_backlog(LVOL, "CL")[0] == 1


def test_held_on_purpose_pairs_are_not_backlog(world, monkeypatch):
    for n in (1, 2, 3):
        world.add_pair(n)
    monkeypatch.setattr(sr, "_has_dependent_clone", lambda sid: sid == "C1")
    assert sr.merge_backlog(LVOL, "CL")[0] == 0
    monkeypatch.setattr(sr, "_has_dependent_clone", lambda sid: False)
    monkeypatch.setattr(sr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: {"S1"})
    assert sr.merge_backlog(LVOL, "CL")[0] == 0


# --- the transfer waits for the merge ----------------------------------------

def _behind(world):
    world.add_pair(1, copy_status=DELETING, src_status=DELETING)
    world.add_pair(2, prev="C1")
    world.add_pair(3, prev="C2")
    world.add_pair(4, prev="C3")
    world.add_pair(5, replicated=False)


def _start_task():
    t = _task("S5", status=JobSchedule.STATUS_NEW)
    t.write_to_db = MagicMock()
    return t


def test_the_next_transfer_is_held_while_the_merge_is_behind(world):
    _behind(world)
    task, snap = _start_task(), world.get_snapshot_by_id("S5")
    snap.lvol = LVOL
    why = sr._merge_behind_hold(task, snap)
    assert why.startswith("merge behind: 2 unmerged landing layers")
    assert task.function_params["merge_hold"]["value"] == 2
    # the merge it waits for is made to run, not only waited for
    assert world.pruned == ["LV1"]


def test_the_transfer_resumes_once_the_merge_caught_up(world):
    _behind(world)
    task, snap = _start_task(), world.get_snapshot_by_id("S5")
    snap.lvol = LVOL
    assert sr._merge_behind_hold(task, snap)
    world.source = [s for s in world.source if s.uuid not in ("S1", "S2")]
    world.copies.pop("C1")
    world.copies["C3"].prev_snap_uuid = ""
    assert sr._merge_behind_hold(task, snap) == ""
    assert "merge_hold" not in task.function_params


def test_the_hold_is_replication_phase_only(world, monkeypatch):
    _behind(world)
    snap = world.get_snapshot_by_id("S5")
    snap.lvol = LVOL
    monkeypatch.setattr(sr, "_cutover_owns", lambda lvol_id, cluster_id: True)
    assert sr._merge_behind_hold(_start_task(), snap) == ""       # final cutover
    monkeypatch.setattr(sr, "_cutover_owns", lambda lvol_id, cluster_id: False)
    snap.snap_type = "user"
    assert sr._merge_behind_hold(_start_task(), snap) == ""       # not a cadence snapshot
    snap.snap_type = SnapShot.TYPE_INTERNAL
    t = _start_task()
    t.function_params["replicate_to_source"] = True
    assert sr._merge_behind_hold(t, snap) == ""                   # fail-back direction


def test_a_hold_without_progress_expires_once_loudly(world, monkeypatch):
    _behind(world)
    task, snap = _start_task(), world.get_snapshot_by_id("S5")
    snap.lvol = LVOL
    clock = [1000.0]
    monkeypatch.setattr(sr.time, "time", lambda: clock[0])
    assert sr._merge_behind_hold(task, snap)
    clock[0] += constants.REPL_MERGE_HOLD_MAX_SEC - 1
    assert sr._merge_behind_hold(task, snap)
    clock[0] += 2
    assert sr._merge_behind_hold(task, snap) == ""
    assert "merge_hold" not in task.function_params


def test_progress_restarts_the_hold_clock():
    hold, expired = sr._hold_progress(None, 3, 100.0)
    assert hold == {"since": 100.0, "value": 3} and not expired
    hold, expired = sr._hold_progress(hold, 2, 100.0 + constants.REPL_MERGE_HOLD_MAX_SEC)
    assert hold["since"] == 100.0 + constants.REPL_MERGE_HOLD_MAX_SEC and not expired
    _, expired = sr._hold_progress(hold, 2, 100.0 + 2 * constants.REPL_MERGE_HOLD_MAX_SEC)
    assert expired


def test_a_measurement_error_never_holds(world, monkeypatch):
    def boom(lvol, cluster_id, exclude_snapshot_id=""):
        raise RuntimeError("fdb")
    monkeypatch.setattr(sr, "merge_backlog", boom)
    snap = _snap("S5", 5)
    snap.lvol = LVOL
    assert sr._merge_behind_hold(_start_task(), snap) == ""


def test_start_reports_the_hold_in_the_task_result(monkeypatch):
    task = _start_task()
    snap = MagicMock()
    with patch.object(sr, "_lvs_transfer_hold", return_value=""), \
            patch.object(sr, "_merge_behind_hold", return_value="merge behind: 2 ..."), \
            patch.object(sr, "_select_source_node") as select:
        assert sr.process_snap_replicate_start(task, snap) is False
    assert task.function_result == "held: merge behind: 2 ..."
    select.assert_not_called()          # no landing volume, no transfer


# --- the cadence waits too ----------------------------------------------------

def test_the_cadence_is_held_while_the_target_merge_is_behind(world, monkeypatch):
    _behind(world)
    world.tasks = []
    why = sm._merge_behind_hold(LVOL, "CL")
    assert "merge behind" in why and "2 unmerged landing layers" in why
    # no transfer of the volume is live to restart retention: the monitor does
    assert world.pruned == ["LV1"]


def test_the_cadence_is_held_while_source_snapshots_pile_up(world, monkeypatch):
    monkeypatch.setattr(sr, "merge_backlog", lambda lvol, cid, exclude_snapshot_id="": (0, "", 0))
    world.add_pair(1)
    world.add_pair(2)
    world.add_pair(3)                                    # beyond retention
    world.add_pair(4, src_status=DELETING)               # being deleted
    world.add_pair(5, replicated=False)                  # unreplicated
    assert sm._pending_source_snapshots(LVOL) == 3
    assert "3 source snapshots pending" in sm._merge_behind_hold(LVOL, "CL")


def test_the_cadence_resumes_when_caught_up(world, monkeypatch):
    backlog = [(2, "x", 0)]
    monkeypatch.setattr(sr, "merge_backlog", lambda lvol, cid, exclude_snapshot_id="": backlog[0])
    monkeypatch.setattr(sm, "_pending_source_snapshots", lambda lvol: 0)
    assert sm._merge_behind_hold(LVOL, "CL")
    assert "LV1" in sm._MERGE_HOLDS
    backlog[0] = (1, "x", 0)
    assert sm._merge_behind_hold(LVOL, "CL") == ""
    assert "LV1" not in sm._MERGE_HOLDS


def test_a_live_transfer_restarts_retention_itself(world, monkeypatch):
    _behind(world)
    world.tasks = [_task("S4", status=JobSchedule.STATUS_NEW)]
    assert sm._merge_behind_hold(LVOL, "CL")
    assert world.pruned == []


def _cadence(held):
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
            patch.object(sm, "_deletion_backlog", return_value=0), \
            patch.object(sm, "_merge_behind_hold", return_value=held), \
            patch.object(sm.replication_delta, "add_snapshot", return_value=("S", None)) as add:
        sm.take_due_internal_snapshots("CL", 1000)
    return add


def test_a_held_volume_takes_no_cadence_snapshot():
    _cadence("merge behind: ...").assert_not_called()
    _cadence("").assert_called_once()


def test_bounds_are_small_constants():
    assert constants.REPL_MAX_UNMERGED_LANDING_LAYERS == 1
    assert constants.REPL_MAX_PENDING_SOURCE_SNAPSHOTS == 2


# --- spares of migrated-away volumes -----------------------------------------

def _snapshot(lvol_id="LV1"):
    snap = MagicMock()
    snap.lvol.get_id.return_value = lvol_id
    snap.size = 10 * 2**30
    return snap


def test_no_spare_is_staged_for_a_volume_that_stopped_replicating():
    create = MagicMock(return_value=("NEW", None))
    stopped = SimpleNamespace(do_replicate=False, status=LVol.STATUS_ONLINE)
    with patch.object(rs.db, "get_lvol_by_id", return_value=stopped), \
            patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError):
        assert rs.prestage(_snapshot(), MagicMock(pool_uuid="POOL"), create) is None
    create.assert_not_called()
    with patch.object(rs.db, "get_lvol_by_id", side_effect=KeyError):
        assert rs.prestage(_snapshot(), MagicMock(pool_uuid="POOL"), create) is None
    create.assert_not_called()


def test_a_spare_created_while_the_cutover_stopped_the_volume_is_discarded():
    create = MagicMock(return_value=("NEW", None))
    states = iter([True, False])
    with patch.object(rs, "_replicating", side_effect=lambda lid: next(states)), \
            patch.object(rs.db, "get_lvol_by_name", side_effect=KeyError), \
            patch.object(rs.db, "get_storage_node_by_id", return_value=MagicMock()), \
            patch.object(rs, "discard") as discard:
        assert rs.prestage(_snapshot(), MagicMock(pool_uuid="POOL"), create) is None
    create.assert_called_once()
    discard.assert_called_once_with("LV1", "POOL")


def test_the_sweep_discards_spares_of_volumes_that_no_longer_replicate():
    def spare(owner):
        return SimpleNamespace(lvol_name=rs.spare_name(owner), status=LVol.STATUS_ONLINE,
                               pool_uuid="POOL", get_id=lambda o=owner: f"SP_{o}")
    lvols = [spare("LIVE"), spare("GONE"), spare("STOPPED"),
             SimpleNamespace(lvol_name="vol1", status=LVol.STATUS_ONLINE, pool_uuid="POOL",
                             get_id=lambda: "V1")]
    replicating = {"LIVE": True, "GONE": False, "STOPPED": False}
    with patch.object(rs.db, "get_lvols", return_value=lvols), \
            patch.object(rs, "_replicating", side_effect=lambda lid: replicating[lid]), \
            patch.object(rs, "discard", return_value=True) as discard:
        assert rs.sweep("CL") == 2
    assert sorted(c.args[0] for c in discard.call_args_list) == ["GONE", "STOPPED"]


def test_cutover_done_discards_the_spare_after_stopping_the_cadence():
    """--delete-source included: the discard runs after the cadence stopped,
    so a transfer draining after it cannot bring the spare back."""
    src = inspect.getsource(final._record_cutover_done)
    assert src.index("_stop_source_replication(task)") < src.index("_discard_source_spare(task)")
    assert src.index("_discard_source_spare(task)") < src.index("_delete_source_if_requested(task)")
    task = JobSchedule()
    task.function_params = {"lvol_id": "LV1", "delete_source": True}
    with patch.object(rs, "discard") as discard:
        final._discard_source_spare(task)
    discard.assert_called_once_with("LV1")


def test_a_retired_snapshot_is_never_a_counterpart(monkeypatch):
    """Linked as a counterpart, a retired snapshot of an earlier leg ended up
    in a live chain, where neither the reaper nor the orphan sweep reaches it."""
    lvol = SimpleNamespace(lvs_name="LVS_1")
    retired = SimpleNamespace(get_id=lambda: "R", data_uuid="D", deleted=True,
                              status=ONLINE, lvol=lvol)
    fresh = SimpleNamespace(get_id=lambda: "F", data_uuid="D", deleted=False,
                            status=ONLINE, lvol=lvol)
    node = SimpleNamespace(lvstore="LVS_1", cluster_id="CL_b")
    snap = SimpleNamespace(get_id=lambda: "S", data_uuid="D")
    db = MagicMock()
    db.get_snapshots.return_value = [retired]
    monkeypatch.setattr(sr, "db", db)
    assert sr._counterpart_on_destination(snap, node) is None
    db.get_snapshots.return_value = [retired, fresh]
    assert sr._counterpart_on_destination(snap, node) is fresh
