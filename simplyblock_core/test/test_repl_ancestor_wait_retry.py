"""Waiting for a chain ancestor does not spend the task's retries.

bf11 (2026-11-03, round 3 of a back-and-forth migration): the volume's chain
base on the new side had lost its counterpart on the other cluster (retired
at the previous cutover), so its first transfer waited for the base to be
copied again. Every pass of the runner counted the wait as a retry; the task
reached retry 7 of 8 before the 10 GiB base had arrived, one pass from being
dropped as "max retry reached". Only a wait for an ancestor that nothing is
working on is a failure.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr


class _Task:
    def __init__(self):
        self.function_params: dict = {"replicate_to_source": False, "snapshot_id": "S1"}
        self.node_id = "N_HOST"
        self.cluster_id = "CL"
        self.status = JobSchedule.STATUS_NEW
        self.function_result = ""
        self.retry = 0
        self.uuid = "T1"

    def write_to_db(self, kv=None):
        pass


def _snap():
    lv = LVol()
    lv.uuid, lv.node_id, lv.replication_node_id = "LV", "N_HOST", "N_REMOTE"
    s = SnapShot()
    s.uuid, s.lvol, s.cluster_id = "S1", lv, "CL"
    s.target_replicated_snap_uuid = ""
    return s


@pytest.fixture
def gate(monkeypatch):
    ancestor = SimpleNamespace(get_id=lambda: "ANC")
    monkeypatch.setattr(sr, "_lvs_transfer_hold", lambda t, s: "")
    monkeypatch.setattr(sr, "_select_source_node",
                        lambda s, exclude=(): (SimpleNamespace(get_id=lambda: "N_HOST"), "primary"))
    monkeypatch.setattr(sr, "_unreplicated_local_ancestor",
                        lambda snode, snapshot, to_source: ("pending", ancestor, ""))
    queued = []
    monkeypatch.setattr(tasks_controller, "add_snapshot_replication_task",
                        lambda *a, **k: queued.append(a[2]))
    return queued


def test_wait_with_a_live_ancestor_task_spends_no_retry(gate, monkeypatch):
    monkeypatch.setattr(sr, "_live_replication_task", lambda cid, sid: object())
    task = _Task()
    for _ in range(20):
        sr.process_snap_replicate_start(task, _snap())
    assert task.retry == 0
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert "waiting for chain ancestor ANC" in task.function_result
    assert gate == ["ANC"] * 20


def test_wait_is_delayed_not_spun(gate, monkeypatch):
    monkeypatch.setattr(sr, "_live_replication_task", lambda cid, sid: object())
    task = _Task()
    sr.process_snap_replicate_start(task, _snap())
    assert int(task.function_params["not_before"]) > 0


def test_wait_for_an_ancestor_nothing_works_on_counts(gate, monkeypatch):
    monkeypatch.setattr(sr, "_live_replication_task", lambda cid, sid: None)
    task = _Task()
    sr.process_snap_replicate_start(task, _snap())
    assert task.retry == 1
