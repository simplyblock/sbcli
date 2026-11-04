"""The poll loops of the two sync-replication task runners, both on the shared
driver, against the real FoundationDB: a failing read of the cluster list ends
the loop with that error instead of being logged and retried - the process
exits and the orchestrator restarts it with a fresh FDB client. Also: when an
earlier attempt of a promote task started an LVS hand-off and the driver then
ends the task without running its handler again, that LVS keeps its
``moving:`` marker.

The failing read is a Cluster record whose value does not decode: the real
``get_clusters`` range read raises on it.
"""
import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import (
    tasks_runner_sync_promote,
    tasks_runner_sync_resync,
)
from tests.integration import task_driver
from tests.integration import test_sync_replication_lvs_stack as stack


@pytest.fixture()
def corrupt_cluster():
    db = DBController()
    key = Cluster().get_db_id(str(uuid.uuid4())).encode()
    db.kv_store[key] = b"not json"
    try:
        yield
    finally:
        db.kv_store.clear(key)


@pytest.mark.parametrize("spec", [
    pytest.param(tasks_runner_sync_promote.SPEC, id="promote"),
    pytest.param(tasks_runner_sync_resync.SPEC, id="resync"),
])
def test_cluster_read_error_ends_loop(corrupt_cluster, spec):
    # one_cycle ends at the driver's sleep: the error escaping it is raised
    # before the loop got there.
    with pytest.raises(json.JSONDecodeError):
        task_driver.one_cycle(spec)


# ---------------------------------------------------------------------------
# the promote's markers at the end of a task
# ---------------------------------------------------------------------------

MOVING_B = "moving:" + stack.SITE_B


def _transferring_promote(db, **task_fields):
    """A promote task to site B whose hand-off of LVS_1 (owner a0) and LVS_2
    (owner b0) was under way: both owners still ``moving:site-b``."""
    cluster, a, b, owner_a, owner_b = stack._two_owner_layout(db)
    owners = [stack._update(db, o, lvs_active_site=MOVING_B) for o in (owner_a, owner_b)]
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.function_name = JobSchedule.FN_SYNC_PROMOTE
    task.status = JobSchedule.STATUS_RUNNING
    task.max_retry = 5
    task.function_params = {
        "site": stack.SITE_B, "lvol_ids": [],
        "owners": {o.lvstore: o.get_id() for o in owners},
        "moves": {o.lvstore: "" for o in owners},
        "transferring": [o.lvstore for o in owners]}
    for key, value in task_fields.items():
        setattr(task, key, value)
    task.write_to_db(db.kv_store)
    return db.get_task_by_id(task.uuid), owners


@pytest.mark.parametrize("task_fields, result", [
    pytest.param({"canceled": True}, "canceled", id="canceled"),
    # At the retry limit the driver ends the task without running its handler.
    pytest.param({"retry": 5, "function_result": "unhandled error: lost answer"},
                 "max retry reached (5) after: unhandled error: lost answer", id="retry-ceiling"),
])
def test_ended_task_leaves_started_moves(task_fields, result):
    """The driver ends the task without running its handler. Both hand-offs
    had started: the runner reconciles neither, both markers stay for the next
    promote call and the storage-node monitor."""
    db = DBController()
    task, owners = _transferring_promote(db, **task_fields)
    handler = MagicMock()
    with patch.object(storage_node_ops, "reconcile_lvs_move") as reconcile:
        done = task_driver.drive(tasks_runner_sync_promote.SPEC, task, handler=handler)
    handler.assert_not_called()
    assert (done.status, done.function_result) == (JobSchedule.STATUS_DONE, result)
    reconcile.assert_not_called()
    assert [stack._fresh(db, o).lvs_active_site for o in owners] == [MOVING_B, MOVING_B]
