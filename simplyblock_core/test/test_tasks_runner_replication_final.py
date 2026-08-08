"""D6 unit tests for the replication-final task handler.

The runner sits on the shared driver (``task_runner_base``), so the handler
is void: it returns on success and raises ``TaskRetry`` for every outcome the
driver should suspend and re-attempt. Task status/retry transitions themselves
belong to the driver and are covered by tests/unit/tasks/test_task_runner_base.py.
"""
import pytest

from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskRetry
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode


def _task(**params):
    t = JobSchedule()
    t.uuid = "task-1"
    t.function_name = JobSchedule.FN_REPLICATION_FINAL
    t.status = JobSchedule.STATUS_NEW
    t.retry = 0
    t.max_retry = 5
    t.canceled = False
    t.function_params = {
        "lvol_id": "LV1",
        "src_node_id": "S1",
        "tgt_node_id": "T1",
        "tgt_lvol_composite": "lvs_tgt/LVOL_1",
        "tgt_map_id": 42,
        "tgt_snap_composite": "lvs_tgt/SNAP1",
        "operation": "replicate",
        "replication_id": "REP1",
        "final_state": LVolReplication.STATE_CUTOVER_DONE,
    }
    t.function_params.update(params)
    return t


def _node(uuid, status=StorageNode.STATUS_ONLINE):
    n = StorageNode()
    n.uuid = uuid
    n.status = status
    return n


class _FakeDB:
    kv_store = "KV"

    def __init__(self, nodes, rep):
        self._nodes = nodes
        self._rep = rep

    def get_lvol_by_id(self, lid):
        lv = LVol()
        lv.uuid = lid
        return lv

    def get_storage_node_by_id(self, nid):
        if nid not in self._nodes:
            raise KeyError(nid)
        return self._nodes[nid]

    def get_lvol_replication_by_id(self, rid):
        return self._rep


@pytest.fixture(autouse=True)
def _no_db_writes(monkeypatch):
    monkeypatch.setattr(JobSchedule, "write_to_db", lambda self, kv=None: None)
    monkeypatch.setattr(LVolReplication, "write_to_db", lambda self, kv=None: None)


def _install(monkeypatch, nodes, rep, cutover_ret):
    db = _FakeDB(nodes, rep)
    monkeypatch.setattr(runner, "db", db)
    calls = []

    def _run_cutover(src, tgt, lvol, comp, map_id, snap, operation="replicate"):
        calls.append((src.get_id(), tgt.get_id(), comp, map_id, snap, operation))
        return cutover_ret
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    return calls


def test_happy_path_returns_and_updates_state(monkeypatch):
    rep = LVolReplication()
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    task = _task()
    assert runner.task_runner(task) is None

    assert len(calls) == 1
    assert calls[0][5] == "replicate"
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE
    assert task.function_result == "cutover done"
    assert task.function_params["start_time"] <= task.function_params["end_time"]


def test_failed_cutover_is_retryable(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install(monkeypatch, nodes, rep, (False, "boom"))

    with pytest.raises(TaskRetry, match="boom"):
        runner.task_runner(_task())


def test_target_offline_is_retryable_without_cutover(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1", status=StorageNode.STATUS_OFFLINE)}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    with pytest.raises(TaskRetry):
        runner.task_runner(_task())
    assert calls == []


def test_missing_source_node_is_retryable_without_cutover(monkeypatch):
    rep = LVolReplication()
    calls = _install(monkeypatch, {"T1": _node("T1")}, rep, (True, None))

    with pytest.raises(TaskRetry, match="source node not found"):
        runner.task_runner(_task())
    assert calls == []


def test_missing_lvol_id_is_retryable(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    with pytest.raises(TaskRetry, match="missing lvol_id"):
        runner.task_runner(_task(lvol_id=""))
    assert calls == []
