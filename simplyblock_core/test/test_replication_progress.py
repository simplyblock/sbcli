"""D4 unit tests for replication progress (time lag + outstanding bytes)."""
import time

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot


def _lvol():
    lv = LVol()
    lv.uuid = "LV1"
    lv.lvol_name = "vol1"
    lv.node_id = "N1"
    return lv


def _node():
    from simplyblock_core.models.storage_node import StorageNode
    n = StorageNode()
    n.uuid = "N1"
    n.cluster_id = "CL1"
    return n


def _snap(uuid, created_at, used_size, target=""):
    lv = LVol()
    lv.uuid = "LV1"
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.used_size = used_size
    s.target_replicated_snap_uuid = target
    s.lvol = lv
    return s


def _task(snap_uuid, status, date):
    t = JobSchedule()
    t.uuid = f"task-{snap_uuid}"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.function_params = {"snapshot_id": snap_uuid}
    t.status = status
    t.date = date
    return t


class _FakeDB:
    kv_store = None

    def __init__(self, lvol, node, tasks, snaps):
        self._lvol = lvol
        self._node = node
        self._tasks = tasks
        self._snaps = {s.uuid: s for s in snaps}

    def get_lvols(self):
        return [self._lvol]

    def get_storage_node_by_id(self, node_id):
        return self._node

    def get_job_tasks(self, cluster_id):
        return self._tasks

    def get_snapshot_by_id(self, uuid):
        return self._snaps[uuid]


def _install(monkeypatch, db):
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)


def test_progress_lag_and_outstanding(monkeypatch):
    now = int(time.time())
    # done+target: replicated 400s ago; another done 100s ago;
    # one still running (outstanding 5GB) and one new (outstanding 3GB).
    snaps = [
        _snap("s_old", now - 400, 1_000, target="t_old"),
        _snap("s_recent", now - 100, 2_000, target="t_recent"),
        _snap("s_run", now - 50, 5 * 1024 ** 3),
        _snap("s_new", now - 10, 3 * 1024 ** 3),
    ]
    tasks = [
        _task("s_old", JobSchedule.STATUS_DONE, 1),
        _task("s_recent", JobSchedule.STATUS_DONE, 2),
        _task("s_run", JobSchedule.STATUS_RUNNING, 3),
        _task("s_new", JobSchedule.STATUS_NEW, 4),
    ]
    db = _FakeDB(_lvol(), _node(), tasks, snaps)
    _install(monkeypatch, db)

    info = lvol_controller.get_replication_info("LV1")

    assert info["outstanding_count"] == 2
    assert info["outstanding_bytes"] == 8 * 1024 ** 3
    # Lag is measured against the newest replicated snapshot (s_recent, 100s ago).
    assert 95 <= info["lag_seconds"] <= 130
    assert info["replicated_count"] == 4


def test_progress_no_replicated_yet(monkeypatch):
    now = int(time.time())
    snaps = [_snap("s_new", now - 10, 4_000)]
    tasks = [_task("s_new", JobSchedule.STATUS_NEW, 1)]
    db = _FakeDB(_lvol(), _node(), tasks, snaps)
    _install(monkeypatch, db)

    info = lvol_controller.get_replication_info("LV1")

    assert info["lag_seconds"] is None
    assert info["lag"] == ""
    assert info["outstanding_count"] == 1
    assert info["outstanding_bytes"] == 4_000


def test_progress_no_tasks(monkeypatch):
    db = _FakeDB(_lvol(), _node(), [], [])
    _install(monkeypatch, db)

    info = lvol_controller.get_replication_info("LV1")

    assert info["outstanding_count"] == 0
    assert info["outstanding_bytes"] == 0
    assert info["lag_seconds"] is None
