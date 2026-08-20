"""The backlog has to be visible, and honest, in both directions.

The configured interval is a TARGET. A full initial sync, a slow link or a
large delta can all exceed it without anything being broken, so what an
operator needs is the size and AGE of the backlog next to the target — not a
boolean that goes red on the first missed tick.

Three defects are pinned here, all seen in the 2026-08-19/20 labs:

  * ``replicated_count`` counted every snapshot that had a task, replicated or
    not, so a volume where nothing had reached the target reported a healthy
    count (the harness read min_replicated=8 with zero copies on the target).
  * ``_is_replicated`` tested only ``target_replicated_snap_uuid``. Fail-back
    records ``source_replicated_snap_uuid`` and never the target one, so every
    failing-back volume reported 0 replicated and ``lag_seconds`` stayed None
    for ever — no gate on lag could pass in that direction.
  * A stuck initial sync has no replicated snapshot at all, so ``lag_seconds``
    was None, the verdict fell through to "replicating" and ``healthy`` stayed
    True while transfers had been going nowhere for twenty minutes.
"""
import time

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule


NOW = int(time.time())
LVOL_ID = "LV1"


class _LvolRef:
    def get_id(self):
        return LVOL_ID


class _Snap:
    def __init__(self, uuid, age_sec, target="", source="", used=1024):
        self.uuid = uuid
        self.created_at = NOW - age_sec
        self.target_replicated_snap_uuid = target
        self.source_replicated_snap_uuid = source
        self.used_size = used
        self.lvol = _LvolRef()

    def get_id(self):
        return self.uuid

    def to_dict(self):
        return {"uuid": self.uuid}


class _Task:
    function_name = JobSchedule.FN_SNAPSHOT_REPLICATION

    def __init__(self, snap_uuid, status=JobSchedule.STATUS_RUNNING, result=""):
        self.date = NOW
        self.updated_at = NOW
        self.status = status
        self.canceled = False
        self.function_result = result
        self.function_params = {"snapshot_id": snap_uuid}

    def to_dict(self):
        return {"snapshot_id": self.function_params["snapshot_id"]}


class _Lvol:
    uuid = LVOL_ID
    lvol_name = "replvol0"
    node_id = "N1"
    replication_interval_min = 1

    def get_id(self):
        return LVOL_ID


class _Node:
    cluster_id = "CL1"


class _FakeDB:
    def __init__(self, snaps, tasks):
        self._snaps = {s.uuid: s for s in snaps}
        self._tasks = tasks

    def get_lvols(self, cluster_id=None):
        return [_Lvol()]

    def get_storage_node_by_id(self, nid):
        return _Node()

    def get_job_tasks(self, cluster_id, reverse=True):
        return self._tasks

    def get_snapshot_by_id(self, uuid):
        return self._snaps[uuid]


@pytest.fixture
def patch_db(monkeypatch):
    def _install(snaps, tasks):
        monkeypatch.setattr(lvol_controller, "DBController",
                            lambda: _FakeDB(snaps, tasks))
        return lvol_controller.get_replication_info(LVOL_ID)
    return _install


def test_replicated_count_counts_only_what_replicated(patch_db):
    snaps = [_Snap("S1", 300, target="T1"), _Snap("S2", 30), _Snap("S3", 10)]
    info = patch_db(snaps, [_Task("S1", JobSchedule.STATUS_DONE),
                            _Task("S2"), _Task("S3")])
    assert info["replicated_count"] == 1
    assert info["outstanding_count"] == 2


def test_failback_direction_counts_as_replicated(patch_db):
    """source_replicated_snap_uuid is how the to-source direction records it."""
    info = patch_db([_Snap("S1", 120, source="SRC1")], [_Task("S1")])
    assert info["replicated_count"] == 1
    assert info["outstanding_count"] == 0
    assert info["lag_seconds"] is not None


def test_backlog_age_is_reported(patch_db):
    snaps = [_Snap("S_OLD", 900), _Snap("S_NEW", 20)]
    info = patch_db(snaps, [_Task("S_OLD"), _Task("S_NEW")])
    # NOW is stamped at import, the controller reads the clock when called, so
    # the gap grows with however long the rest of the suite takes to get here.
    # Only the age ORDER matters: the oldest outstanding one, not the newest.
    assert 900 <= info["oldest_outstanding_seconds"] < 900 + 3600
    assert info["oldest_outstanding"]
    assert info["cadence_target_seconds"] == 60


def test_cadence_met_while_within_one_interval(patch_db):
    """One transfer in flight inside its own interval is keeping up."""
    info = patch_db([_Snap("S1", 20)], [_Task("S1")])
    assert info["cadence_met"] is True


def test_cadence_behind_when_backlog_outlives_the_interval(patch_db):
    info = patch_db([_Snap("S1", 600)], [_Task("S1")])
    assert info["cadence_met"] is False


def test_stuck_initial_sync_is_not_reported_healthy(patch_db):
    """The regression: nothing replicated ever, so lag_seconds is None.

    The verdict used to fall through to "replicating" and healthy=True while
    the volume had been getting nowhere for twenty minutes.
    """
    snaps = [_Snap(f"S{i}", 1200 - i * 60) for i in range(20)]
    info = patch_db(snaps, [_Task(s.uuid) for s in snaps])
    assert info["lag_seconds"] is None
    assert info["replicated_count"] == 0
    assert info["state"] == "lagging"
    assert info["healthy"] is False


def test_no_backlog_reports_in_sync(patch_db):
    info = patch_db([_Snap("S1", 30, target="T1")],
                    [_Task("S1", JobSchedule.STATUS_DONE)])
    assert info["outstanding_count"] == 0
    assert info["oldest_outstanding_seconds"] is None
    assert info["state"] == "in_sync"
    assert info["healthy"] is True
