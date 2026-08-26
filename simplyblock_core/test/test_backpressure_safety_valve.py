"""Back-pressure must not turn a terminal failure into a silent halt.

"Never start the next transfer until this one finishes" is right for a
transfer in progress. Conditioning it on a marker that may never arrive is not:
one stuck transfer then stops the volume's cadence for good -- no new
snapshots, nothing queued, nothing in flight, and no error.

Lab 2026-08-20 (case 4): a chaining bug made transfers refuse to finalize, so
the replicated marker never came and the cadence froze for the rest of the run
(outstanding=0, not one new snapshot in 20 minutes). The chaining bug is fixed;
this pins the guard so no FUTURE terminal failure can halt replication quietly.
"""
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm


LVOL_ID = "LV1"


class _LvolRef:
    node_id = "NODE_A"
    uuid = LVOL_ID

    def get_id(self):
        return LVOL_ID


class _Mini:
    def __init__(self, uuid, created_at=100):
        self.uuid = uuid
        self.created_at = created_at
        self.snap_type = SnapShot.TYPE_INTERNAL
        self.status = SnapShot.STATUS_ONLINE
        self.lvol = _LvolRef()

    def get_id(self):
        return self.uuid


class _Full:
    def __init__(self, uuid, target="", source=""):
        self.uuid = uuid
        self.target_replicated_snap_uuid = target
        self.source_replicated_snap_uuid = source
        self.lvol = _LvolRef()

    def get_id(self):
        return self.uuid


class _Task:
    def __init__(self, snap_uuid, status=JobSchedule.STATUS_RUNNING, canceled=False,
                 fn=JobSchedule.FN_SNAPSHOT_REPLICATION):
        self.function_name = fn
        self.function_params = {"snapshot_id": snap_uuid}
        self.status = status
        self.canceled = canceled


class _Lvol:
    uuid = LVOL_ID
    do_replicate = True
    replication_interval_min = 1
    status = "online"

    def get_id(self):
        return LVOL_ID


def _db(full, tasks, raise_on_tasks=False):
    class _DB:
        def get_snapshot_by_id(self, uuid):
            return full

        def get_storage_node_by_id(self, nid):
            return type("N", (), {"cluster_id": "CL1"})()

        def get_job_tasks(self, cluster_id):
            if raise_on_tasks:
                raise RuntimeError("FDB unavailable")
            return tasks
    return _DB()


def test_running_transfer_still_blocks_the_next(monkeypatch):
    """The rule itself: a live transfer holds the cadence back."""
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), [_Task("S1")]))
    out = sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")])
    assert out is not None and out.get_id() == "S1"


def test_suspended_retrying_transfer_still_blocks(monkeypatch):
    """A task retrying is still going somewhere; keep waiting for it."""
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", JobSchedule.STATUS_SUSPENDED)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is not None


def test_finished_task_without_a_marker_does_not_block(monkeypatch):
    """The regression: DONE but never replicated is terminal, not pending."""
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", JobSchedule.STATUS_DONE)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


def test_cancelled_task_does_not_block(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", JobSchedule.STATUS_RUNNING, canceled=True)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


def test_no_task_at_all_does_not_block(monkeypatch):
    """A dropped task never sets the marker -- waiting on it is waiting for ever."""
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), []))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


def test_another_snapshots_task_does_not_count_as_live(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), [_Task("SOMETHING_ELSE")]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


def test_a_non_replication_task_does_not_count_as_live(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", fn="other_function")]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


def test_unreadable_tasks_keep_back_pressure_on(monkeypatch):
    """Cannot tell => assume live. Conservative: never over-queue on a guess."""
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), [], raise_on_tasks=True))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is not None


def test_a_replicated_snapshot_never_blocks(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1", target="T1"), [_Task("S1", JobSchedule.STATUS_DONE)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None
