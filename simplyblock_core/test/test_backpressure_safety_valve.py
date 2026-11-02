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
import pytest

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
        self.cluster_id = "CL1"
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


@pytest.fixture
def requeued(monkeypatch):
    """Records the transfers the cadence re-queues for an outstanding snapshot."""
    from simplyblock_core.controllers import tasks_controller
    calls = []
    monkeypatch.setattr(tasks_controller, "add_snapshot_replication_task",
                        lambda cluster_id, node_id, snap_id, **kw: calls.append(snap_id))
    return calls


# A transfer that ended without its marker is terminal. It is queued again,
# a bounded number of times, before the cadence moves on: resuming at once
# stacked a new snapshot behind every transfer that gave up (bf6 2026-10-29:
# 14 unreplicated snapshots on one volume). After the limit the cadence is
# released, so a snapshot that cannot replicate still never halts it.

def test_finished_task_without_a_marker_is_queued_again(monkeypatch, requeued):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", JobSchedule.STATUS_DONE)]))
    out = sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")])
    assert out is not None and out.get_id() == "S1"
    assert requeued == ["S1"]


def test_cancelled_task_is_queued_again(monkeypatch, requeued):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", JobSchedule.STATUS_RUNNING, canceled=True)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is not None
    assert requeued == ["S1"]


def test_no_task_at_all_is_queued_again(monkeypatch, requeued):
    """A dropped task never sets the marker: queue the transfer again."""
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), []))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is not None
    assert requeued == ["S1"]


def test_after_the_limit_the_cadence_is_released(monkeypatch, requeued):
    """The guard against a silent halt: a snapshot that keeps failing lets go."""
    tasks = [_Task("S1", JobSchedule.STATUS_DONE)
             for _ in range(sm._CADENCE_REQUEUE_LIMIT + 1)]
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), tasks))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None
    assert requeued == []


def test_another_snapshots_task_does_not_count_as_live(monkeypatch, requeued):
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), [_Task("SOMETHING_ELSE")]))
    sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")])
    assert requeued == ["S1"]


def test_a_non_replication_task_does_not_count_as_live(monkeypatch, requeued):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1"), [_Task("S1", fn="other_function")]))
    sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")])
    assert requeued == ["S1"]


def test_unreadable_tasks_keep_back_pressure_on(monkeypatch):
    """Cannot tell => assume live. Conservative: never over-queue on a guess."""
    monkeypatch.setattr(sm, "db", _db(_Full("S1"), [], raise_on_tasks=True))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is not None


def test_a_replicated_snapshot_never_blocks(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(
        _Full("S1", target="T1"), [_Task("S1", JobSchedule.STATUS_DONE)]))
    assert sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1")]) is None


# --- overlap: the next transfer may run while the target merges the last copy

def _db_multi(fulls, tasks):
    class _DB:
        def get_snapshot_by_id(self, uuid):
            return fulls[uuid]

        def get_storage_node_by_id(self, nid):
            return type("N", (), {"cluster_id": "CL1"})()

        def get_job_tasks(self, cluster_id):
            return tasks
    return _DB()


def _transferred(snap_uuid):
    t = _Task(snap_uuid)
    t.function_params["transfer_done"] = True
    return t


def test_a_merging_snapshot_lets_the_next_one_transfer(monkeypatch):
    """S2 has arrived and is only being merged on the target; S1 has its copy."""
    monkeypatch.setattr(sm, "db", _db_multi(
        {"S1": _Full("S1", target="T1"), "S2": _Full("S2")}, [_transferred("S2")]))
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_Mini("S1", 100), _Mini("S2", 200)]) is None


def test_a_transfer_in_progress_still_blocks_the_next(monkeypatch):
    monkeypatch.setattr(sm, "db", _db_multi(
        {"S1": _Full("S1", target="T1"), "S2": _Full("S2")}, [_Task("S2")]))
    out = sm._outstanding_internal_snapshot(_Lvol(), [_Mini("S1", 100), _Mini("S2", 200)])
    assert out is not None and out.get_id() == "S2"


def test_at_most_two_snapshots_in_flight(monkeypatch):
    """S2 merging is not enough while S1 has no copy yet either."""
    monkeypatch.setattr(sm, "db", _db_multi(
        {"S1": _Full("S1"), "S2": _Full("S2")}, [_transferred("S1"), _transferred("S2")]))
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_Mini("S1", 100), _Mini("S2", 200)]) is not None


def test_overlap_switched_off_waits_for_the_copy(monkeypatch):
    monkeypatch.setattr(sm.constants, "REPL_OVERLAP_MERGE", False)
    monkeypatch.setattr(sm, "db", _db_multi(
        {"S1": _Full("S1", target="T1"), "S2": _Full("S2")}, [_transferred("S2")]))
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_Mini("S1", 100), _Mini("S2", 200)]) is not None
