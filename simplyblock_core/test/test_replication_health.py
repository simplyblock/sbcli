"""Per-volume replication health verdict.

Replication failures only ever lived in per-task ``function_result`` strings —
"node is not online, retrying", "no online source LVS leader, retrying" — which
no status view surfaced. A volume could sit hours behind while everything
looked normal, and the staleness was only discovered by a fail-over returning
old data. get_replication_info now derives a verdict from the tasks.
"""
import time

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule


class _LVol:
    def __init__(self, uuid="LV1", interval=1):
        self.uuid = uuid
        self.lvol_name = "vol0"
        self.node_id = "N1"
        self.replication_interval_min = interval

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid, lvol, created_at, target_repl="", used=1024):
        self.uuid = uuid
        self.lvol = lvol
        self.created_at = created_at
        self.target_replicated_snap_uuid = target_repl
        # Set by the to-source (fail-back) direction; these cases all replicate
        # to the target, so it stays empty.
        self.source_replicated_snap_uuid = ""
        self.used_size = used

    def get_id(self):
        return self.uuid

    def to_dict(self):
        return {"uuid": self.uuid}


class _Task:
    def __init__(self, snap_uuid, status, result="", canceled=False, date=0):
        self.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
        self.function_params = {"snapshot_id": snap_uuid}
        self.status = status
        self.function_result = result
        self.canceled = canceled
        self.date = date
        self.updated_at = "now"

    def to_dict(self):
        return {"status": self.status}


def _patch(monkeypatch, lvol, snaps, tasks):
    class _DB:
        def get_lvols(self):
            return [lvol]

        def get_storage_node_by_id(self, uuid):
            return type("N", (), {"cluster_id": "C1"})()

        def get_job_tasks(self, cluster_id):
            return tasks

        def get_snapshot_by_id(self, uuid):
            for s in snaps:
                if s.uuid == uuid:
                    return s
            raise KeyError(uuid)

    monkeypatch.setattr(lvol_controller, "DBController", lambda: _DB())


def test_in_sync_when_everything_is_replicated(monkeypatch):
    lvol = _LVol()
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 10, target_repl="T1")]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE)]
    _patch(monkeypatch, lvol, snaps, tasks)

    info = lvol_controller.get_replication_info("LV1")
    assert info["state"] == "in_sync"
    assert info["healthy"] is True
    assert info["last_error"] == ""


def test_degraded_surfaces_the_retry_reason(monkeypatch):
    """The case-6 signature: tasks retrying because a node is unavailable."""
    lvol = _LVol()
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 400, target_repl="T1"),
             _Snap("S2", lvol, now - 60)]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE, date=1),
             _Task("S2", JobSchedule.STATUS_SUSPENDED,
                   result="no online source LVS leader, retrying", date=2)]
    _patch(monkeypatch, lvol, snaps, tasks)

    info = lvol_controller.get_replication_info("LV1")
    assert info["state"] == "degraded"
    assert info["healthy"] is False
    assert "no online source LVS leader" in info["last_error"]
    assert info["failing_count"] == 1


def test_error_when_a_task_gave_up(monkeypatch):
    lvol = _LVol()
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 60)]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE, result="max retry reached")]
    _patch(monkeypatch, lvol, snaps, tasks)

    info = lvol_controller.get_replication_info("LV1")
    assert info["state"] == "error"
    assert info["healthy"] is False
    assert info["max_retry_reached"] == 1


def test_lagging_when_the_newest_copy_is_older_than_the_budget(monkeypatch):
    """Nothing failing, but the target is far behind — still not healthy."""
    lvol = _LVol(interval=1)      # budget = max(3*60, 300) = 300s
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 3000, target_repl="T1")]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE)]
    _patch(monkeypatch, lvol, snaps, tasks)

    info = lvol_controller.get_replication_info("LV1")
    assert info["state"] == "lagging"
    assert info["healthy"] is False
    assert info["lag_seconds"] >= 3000


def test_replicating_while_a_transfer_is_in_flight(monkeypatch):
    lvol = _LVol()
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 30, target_repl="T1"),
             _Snap("S2", lvol, now - 5)]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE, date=1),
             _Task("S2", JobSchedule.STATUS_RUNNING, date=2)]
    _patch(monkeypatch, lvol, snaps, tasks)

    info = lvol_controller.get_replication_info("LV1")
    assert info["state"] == "replicating"
    assert info["healthy"] is True
    assert info["outstanding_count"] == 1


def test_one_missed_cycle_is_not_an_incident(monkeypatch):
    lvol = _LVol(interval=1)
    now = int(time.time())
    snaps = [_Snap("S1", lvol, now - 120, target_repl="T1")]
    tasks = [_Task("S1", JobSchedule.STATUS_DONE)]
    _patch(monkeypatch, lvol, snaps, tasks)

    assert lvol_controller.get_replication_info("LV1")["state"] == "in_sync"
