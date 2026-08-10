# coding=utf-8
"""Integration tests for simplyblock_lib.tasks.lease.TaskLease against the real
FoundationDB provisioned by tests/integration/conftest.py.

The lease is exercised through the real ``DBController.atomic_update`` CAS and
real ``JobSchedule`` records, i.e. exactly the code paths the tasks runners
use in production (tasks_controller.claim_task delegates here).
"""
import datetime
import time
import uuid as uuid_lib

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.tasks.lease import TaskLease

CLUSTER_ID = "lease-it-cluster"


@pytest.fixture()
def db():
    controller = DBController()
    if controller.kv_store is None:
        pytest.skip("FoundationDB is not available")
    return controller


@pytest.fixture(autouse=True)
def _clean_keyspace(db):
    db.kv_store.clear_range(b"\x00", b"\xff")
    yield


def _seed_task(db, status=JobSchedule.STATUS_NEW):
    task = JobSchedule()
    task.uuid = str(uuid_lib.uuid4())
    task.cluster_id = CLUSTER_ID
    task.date = int(time.time())
    task.function_name = "lib_lease_test"
    task.status = status
    task.write_to_db(db.kv_store)
    return task


def _lease(db, owner, ttl=constants.TASK_LEASE_TTL_SEC):
    return TaskLease(db, ttl_sec=ttl, heartbeat_sec=0.05, owner=owner)


def _age_lease(db, task, seconds):
    """Backdate the persisted lease timestamp through the real CAS path."""
    stamp = str(datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(seconds=seconds))

    def _mutate(t):
        t.updated_at = stamp
        return True

    assert db.atomic_update(task, _mutate) is not None


def test_claim_persists_owner(db):
    task = _seed_task(db)
    assert _lease(db, "hostA").claim(task) is True

    persisted = db.get_task_by_id(task.uuid)
    assert persisted.owner == "hostA"
    assert persisted.updated_at


def test_second_host_locked_out_until_stale(db):
    task = _seed_task(db)
    assert _lease(db, "hostA").claim(task) is True

    # A different host is locked out while the lease is fresh …
    fresh = db.get_task_by_id(task.uuid)
    assert _lease(db, "hostB").claim(fresh) is False
    assert db.get_task_by_id(task.uuid).owner == "hostA"

    # … and takes over once the lease is stale.
    _age_lease(db, db.get_task_by_id(task.uuid), constants.TASK_LEASE_TTL_SEC + 60)
    stale = db.get_task_by_id(task.uuid)
    assert _lease(db, "hostB").claim(stale) is True
    assert db.get_task_by_id(task.uuid).owner == "hostB"


def test_same_host_always_reclaims(db):
    task = _seed_task(db)
    assert _lease(db, "hostA").claim(task) is True
    reread = db.get_task_by_id(task.uuid)
    assert _lease(db, "hostA").claim(reread) is True


def test_done_task_never_claimed(db):
    task = _seed_task(db, status=JobSchedule.STATUS_DONE)
    assert _lease(db, "hostA").claim(task) is False
    assert db.get_task_by_id(task.uuid).owner == ""


def test_refresh_updates_persisted_lease(db):
    task = _seed_task(db)
    lease = _lease(db, "hostA")
    assert lease.claim(task) is True
    _age_lease(db, db.get_task_by_id(task.uuid), 100)
    before = db.get_task_by_id(task.uuid).updated_at

    assert lease.refresh(db.get_task_by_id(task.uuid)) is True
    after = db.get_task_by_id(task.uuid).updated_at
    assert after != before


def test_refresh_after_takeover_returns_false(db):
    task = _seed_task(db)
    lease_a = _lease(db, "hostA")
    assert lease_a.claim(task) is True

    _age_lease(db, db.get_task_by_id(task.uuid), constants.TASK_LEASE_TTL_SEC + 60)
    assert _lease(db, "hostB").claim(db.get_task_by_id(task.uuid)) is True

    # hostA lost the lease; its refresh must fail and leave hostB's lease alone.
    assert lease_a.refresh(db.get_task_by_id(task.uuid)) is False
    assert db.get_task_by_id(task.uuid).owner == "hostB"


def test_tasks_controller_delegation_against_fdb(db):
    """The public tasks_controller entry points drive the same lib lease."""
    task = _seed_task(db)
    assert tasks_controller.claim_task(task, owner="hostA") is True
    assert db.get_task_by_id(task.uuid).owner == "hostA"
    assert tasks_controller.refresh_task_lease(
        db.get_task_by_id(task.uuid), owner="hostA") is True
    with tasks_controller.task_lease_heartbeat(task, owner="hostA"):
        pass
