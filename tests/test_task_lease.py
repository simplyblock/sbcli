# coding=utf-8
"""Unit tests for the JobSchedule task lease (tasks_controller.claim_task).

These exercise the claim/staleness decision logic without a live FoundationDB
by stubbing DBController.atomic_update with a faithful in-memory stand-in: it
invokes the mutator on the object (in place) and returns it, mirroring the real
helper's contract (returns the object, or None if it no longer exists).
"""
import datetime

from simplyblock_core import constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule


def _now_iso(offset_sec=0):
    return str(datetime.datetime.now(datetime.timezone.utc)
               + datetime.timedelta(seconds=offset_sec))


def _task(status=JobSchedule.STATUS_NEW, owner="", canceled=False, age_sec=0):
    t = JobSchedule()
    t.uuid = "task-1"
    t.status = status
    t.owner = owner
    t.canceled = canceled
    t.updated_at = _now_iso(-age_sec)
    return t


def _patch_atomic_update(monkeypatch, present=True):
    def fake(obj, mutate_fn):
        if not present:
            return None
        mutate_fn(obj)
        return obj
    monkeypatch.setattr(tasks_controller.db, "atomic_update", fake)


def test_claim_unowned_task_succeeds(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(owner="")
    assert tasks_controller.claim_task(t, owner="hostA") is True
    assert t.owner == "hostA"


def test_claim_own_task_refreshes_lease(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(owner="hostA", status=JobSchedule.STATUS_RUNNING, age_sec=10)
    old = t.updated_at
    assert tasks_controller.claim_task(t, owner="hostA") is True
    assert t.owner == "hostA"
    assert t.updated_at != old  # lease refreshed


def test_claim_blocked_by_other_live_host(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(owner="hostA", status=JobSchedule.STATUS_RUNNING, age_sec=5)
    assert tasks_controller.claim_task(t, owner="hostB") is False
    assert t.owner == "hostA"  # untouched


def test_claim_takes_over_stale_lease(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(owner="hostA", status=JobSchedule.STATUS_RUNNING,
              age_sec=constants.TASK_LEASE_TTL_SEC + 60)
    assert tasks_controller.claim_task(t, owner="hostB") is True
    assert t.owner == "hostB"


def test_done_task_never_claimed(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(status=JobSchedule.STATUS_DONE, owner="")
    assert tasks_controller.claim_task(t, owner="hostA") is False


def test_canceled_task_never_claimed(monkeypatch):
    _patch_atomic_update(monkeypatch)
    t = _task(canceled=True, owner="")
    assert tasks_controller.claim_task(t, owner="hostA") is False


def test_missing_task_returns_false(monkeypatch):
    _patch_atomic_update(monkeypatch, present=False)
    t = _task(owner="")
    assert tasks_controller.claim_task(t, owner="hostA") is False


def test_lease_stale_helper():
    assert tasks_controller._task_lease_is_stale(_task(age_sec=constants.TASK_LEASE_TTL_SEC + 1))
    assert not tasks_controller._task_lease_is_stale(_task(age_sec=0))
    empty = _task()
    empty.updated_at = ""
    assert tasks_controller._task_lease_is_stale(empty)
