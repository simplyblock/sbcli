# coding=utf-8
"""Unit tests for simplyblock_lib.tasks.lease.TaskLease.

The db is a faithful in-memory stand-in for DBController.atomic_update: it
invokes the mutator on the object (in place) and returns it, mirroring the
real helper's contract (returns the object, or None if it no longer exists).
The task is a plain duck-typed object — the lease must not require the
JobSchedule model.
"""
import datetime
import threading

from simplyblock_lib.tasks.lease import TaskLease

TTL = 180
HEARTBEAT = 0.05


class FakeTask:
    def __init__(self, status='new', owner='', age_sec=0):
        self.uuid = "task-1"
        self.status = status
        self.owner = owner
        self.updated_at = str(datetime.datetime.now(datetime.timezone.utc)
                              - datetime.timedelta(seconds=age_sec))


class FakeDB:
    def __init__(self, present=True):
        self.present = present

    def atomic_update(self, obj, mutate_fn):
        if not self.present:
            return None
        mutate_fn(obj)
        return obj


def _lease(present=True, owner="hostA"):
    return TaskLease(FakeDB(present), ttl_sec=TTL, heartbeat_sec=HEARTBEAT, owner=owner)


def test_claim_unowned_task_succeeds():
    t = FakeTask(owner="")
    assert _lease().claim(t) is True
    assert t.owner == "hostA"


def test_claim_own_task_refreshes_lease():
    t = FakeTask(owner="hostA", status='running', age_sec=10)
    old = t.updated_at
    assert _lease().claim(t) is True
    assert t.owner == "hostA"
    assert t.updated_at != old  # lease refreshed


def test_claim_blocked_by_other_live_host():
    t = FakeTask(owner="hostA", status='running', age_sec=5)
    assert _lease(owner="hostB").claim(t) is False
    assert t.owner == "hostA"  # untouched


def test_claim_takes_over_stale_lease():
    t = FakeTask(owner="hostA", status='running', age_sec=TTL + 60)
    assert _lease(owner="hostB").claim(t) is True
    assert t.owner == "hostB"


def test_claim_owner_argument_overrides_default():
    t = FakeTask(owner="")
    assert _lease(owner="hostA").claim(t, owner="hostZ") is True
    assert t.owner == "hostZ"


def test_done_task_never_claimed():
    t = FakeTask(status='done', owner="")
    assert _lease().claim(t) is False


def test_custom_done_status_respected():
    lease = TaskLease(FakeDB(), ttl_sec=TTL, heartbeat_sec=HEARTBEAT,
                      owner="hostA", done_status='finished')
    assert lease.claim(FakeTask(status='finished')) is False
    assert lease.claim(FakeTask(status='done')) is True  # 'done' is not terminal here


def test_missing_task_returns_false():
    t = FakeTask(owner="")
    assert _lease(present=False).claim(t) is False


def test_is_stale():
    lease = _lease()
    assert lease.is_stale(FakeTask(age_sec=TTL + 1))
    assert not lease.is_stale(FakeTask(age_sec=0))
    empty = FakeTask()
    empty.updated_at = ""
    assert lease.is_stale(empty)
    garbage = FakeTask()
    garbage.updated_at = "not-a-timestamp"
    assert lease.is_stale(garbage)


def test_naive_timestamp_treated_as_utc():
    t = FakeTask()
    t.updated_at = str(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None))
    assert not _lease().is_stale(t)


def test_refresh_own_lease():
    t = FakeTask(owner="hostA", status='running', age_sec=10)
    old = t.updated_at
    assert _lease().refresh(t) is True
    assert t.updated_at != old


def test_refresh_lost_lease_returns_false():
    t = FakeTask(owner="hostB", status='running')
    old = t.updated_at
    assert _lease().refresh(t) is False
    assert t.updated_at == old  # untouched


def test_refresh_done_task_returns_false():
    t = FakeTask(owner="hostA", status='done')
    assert _lease().refresh(t) is False


class FreshReadDB:
    """Mimics the REAL DBController.atomic_update contract: the mutator runs on
    a fresh read of the record, NOT on the object the caller holds."""

    def __init__(self, stored):
        import copy
        self._copy = copy.copy
        self.stored = self._copy(stored)

    def atomic_update(self, obj, mutate_fn):
        fresh = self._copy(self.stored)
        if mutate_fn(fresh) is not False:
            self.stored = fresh
        return fresh


def test_claim_syncs_callers_copy_with_committed_lease():
    """After a successful claim, the caller's object must carry the committed
    owner/updated_at — a later full-object write by the runner (e.g. marking
    the task RUNNING) would otherwise clobber the lease back to its stale
    pre-claim value."""
    caller_copy = FakeTask(owner="")
    db = FreshReadDB(caller_copy)
    lease = TaskLease(db, ttl_sec=TTL, heartbeat_sec=HEARTBEAT, owner="hostA")

    assert lease.claim(caller_copy) is True
    assert db.stored.owner == "hostA"
    assert caller_copy.owner == "hostA"
    assert caller_copy.updated_at == db.stored.updated_at


def test_failed_claim_leaves_callers_copy_untouched():
    caller_copy = FakeTask(owner="hostB", status='running', age_sec=0)
    db = FreshReadDB(caller_copy)
    lease = TaskLease(db, ttl_sec=TTL, heartbeat_sec=HEARTBEAT, owner="hostA")

    assert lease.claim(caller_copy) is False
    assert caller_copy.owner == "hostB"
    assert db.stored.owner == "hostB"


def test_refresh_syncs_callers_copy():
    caller_copy = FakeTask(owner="hostA", status='running', age_sec=100)
    old = caller_copy.updated_at
    db = FreshReadDB(caller_copy)
    lease = TaskLease(db, ttl_sec=TTL, heartbeat_sec=HEARTBEAT, owner="hostA")

    assert lease.refresh(caller_copy) is True
    assert caller_copy.updated_at != old
    assert caller_copy.updated_at == db.stored.updated_at


def test_heartbeat_refreshes_until_exit():
    lease = _lease()
    t = FakeTask(owner="hostA", status='running')
    refreshed = threading.Event()

    original_refresh = lease.refresh

    def spy(task, owner=None):
        refreshed.set()
        return original_refresh(task, owner)

    lease.refresh = spy
    with lease.heartbeat(t):
        assert refreshed.wait(timeout=2.0)
    # After the with-block, no further refreshes happen.
    refreshed.clear()
    assert not refreshed.wait(timeout=3 * HEARTBEAT)


def test_heartbeat_stops_when_lease_lost():
    lease = _lease()
    t = FakeTask(owner="hostA", status='running')
    calls = []

    def spy(task, owner=None):
        calls.append(1)
        return False  # lease lost to another host

    lease.refresh = spy
    with lease.heartbeat(t):
        deadline = datetime.datetime.now() + datetime.timedelta(seconds=2)
        while not calls and datetime.datetime.now() < deadline:
            threading.Event().wait(HEARTBEAT / 2)
        assert calls, "heartbeat never fired"
        # Give the thread a few more beats; it must have stopped after False.
        threading.Event().wait(5 * HEARTBEAT)
        assert len(calls) == 1
