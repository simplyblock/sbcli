"""Work that cannot succeed or is not needed must not eat the serial
replication runner.

bf8 (2026-11-02, round 6): ~35 % of the snapshot-replication runner went to
(a) 66 replication tasks for one already-replicated repl_commit base, queued
again by every replication task of the clones built on it, and (b) a chain
blocked for good (a blob without a snapshot record) re-checked every minute
at ~5.5 s each. Every other volume's snapshot-to-copy time rose to 61-74 s,
above the 50 s cutover gate, and round 6 stalled.
"""
import inspect
import time
from types import SimpleNamespace
from typing import Any

import pytest

from simplyblock_core.controllers import tasks_controller as tc
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.peer.protocol import FinishResponse
from simplyblock_core.services import snapshot_replication as sr


def _snap(sid, target_copy="", ref="", instances=()):
    return SimpleNamespace(get_id=lambda: sid, cluster_id="CL", target_replicated_snap_uuid=target_copy,
                           snap_ref_id=ref, instances=list(instances))


def _task(sid, status="new", canceled=False):
    return SimpleNamespace(function_name=JobSchedule.FN_SNAPSHOT_REPLICATION, status=status,
                           canceled=canceled, function_params={"snapshot_id": sid}, uuid="T-" + sid)


@pytest.fixture
def env(monkeypatch):
    state: dict[str, Any] = {"snaps": {}, "tasks": [], "added": []}
    monkeypatch.setattr(tc.db, "get_snapshot_by_id", lambda sid: state["snaps"][sid])
    monkeypatch.setattr(tc.db, "get_job_tasks", lambda cid: state["tasks"])
    monkeypatch.setattr(tc, "_add_task", lambda fn, cid, nid, dev, **kw: state["added"].append(
        kw["function_params"]["snapshot_id"]))
    return state


def test_an_already_replicated_base_is_not_queued_again(env):
    env["snaps"]["BASE"] = _snap("BASE", target_copy="COPY")
    tc._check_snap_instance_on_node("BASE", "N1")
    assert env["added"] == []


def test_a_base_with_an_open_task_is_not_queued_twice(env):
    env["snaps"]["BASE"] = _snap("BASE")
    env["tasks"] = [_task("BASE", status="suspended")]
    tc._check_snap_instance_on_node("BASE", "N1")
    assert env["added"] == []


def test_done_or_canceled_tasks_do_not_count(env):
    env["snaps"]["BASE"] = _snap("BASE")
    env["tasks"] = [_task("BASE", status="done"), _task("BASE", canceled=True)]
    tc._check_snap_instance_on_node("BASE", "N1")
    assert env["added"] == ["BASE"]


def test_an_unreplicated_base_is_still_queued(env):
    env["snaps"]["BASE"] = _snap("BASE")
    tc._check_snap_instance_on_node("BASE", "N1")
    assert env["added"] == ["BASE"]


class _T:
    def __init__(self):
        self.retry, self.status, self.function_result = 0, "running", ""
        self.function_params: dict[str, Any] = {"snapshot_id": "S"}
        self.uuid = "T"

    def write_to_db(self):
        pass


def test_a_blocked_chain_backs_off():
    src = inspect.getsource(sr)
    i = src.index('if verdict == "blocked":')
    block = src[i:src.index("return", i)]
    assert "_suspend_for_retry(" in block and "backoff=True" in block
    t = _T()
    sr._suspend_for_retry(t, "replication chain of S is incomplete", backoff=True)
    assert t.status == JobSchedule.STATUS_SUSPENDED and t.retry == 1
    assert t.function_params["not_before"] >= int(time.time()) + sr._backoff_seconds(1) - 1


def test_a_leaderless_target_keeps_the_finish_pending(monkeypatch):
    """Both target members down is an outage, not a failure: the finish stays
    pending (re-checked every pass) instead of taking the doubling retry
    backoff that delayed the resume by 7 min on bf8 (2026-11-02, round 6).
    The destination reports it (finish: waiting_for_leader)."""
    t = _T()
    t.function_params.update(remote_lvol_id="LAND", replicate_to_source=False)

    class _Peer:
        def finish(self, request):
            return FinishResponse(state="pending", waiting_for_leader=True,
                                  reason="waiting for the target lvstore to have a leader "
                                         "(0 members report leadership)")

    monkeypatch.setattr(sr, "_deferred_chain_gate", lambda task, snap, to_source: "ok")
    monkeypatch.setattr(sr, "_previous_replicated_snapshot", lambda snap, to_source: None)
    monkeypatch.setattr(sr, "_task_peer", lambda task, snap: _Peer())
    snap = SimpleNamespace(get_id=lambda: "S", cluster_id="CL", snap_name="s", size=1,
                           used_size=0, data_uuid="", lvol=SimpleNamespace(get_id=lambda: "LV", to_dict=dict))
    assert sr.process_snap_replicate_finish(t, snap) == sr.FINISH_PENDING
    assert t.retry == 0 and "not_before" not in t.function_params
    assert "leader" in t.function_params["pending_why"]


