"""The own poll loops of the two sync-replication task runners against the
real FoundationDB: a failing read of the cluster list ends the loop with that
error instead of being logged and retried, as for the runners on the driver -
the process exits and the orchestrator restarts it with a fresh FDB client.
And the promote's settle of its ``moving:`` markers at the end of a task.

The failing read is a Cluster record whose value does not decode: the real
``get_clusters`` range read raises on it.
"""
import json
import time
import types
import uuid
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.rpc_client import RPCRemoteError
from simplyblock_core.services import (
    tasks_runner_sync_promote,
    tasks_runner_sync_resync,
)
from tests.integration import test_sync_replication_lvs_stack as stack


class _StopLoop(BaseException):
    """Raised by the loop's sleep: the pass got to the end."""


@pytest.fixture()
def corrupt_cluster():
    db = DBController()
    key = Cluster().get_db_id(str(uuid.uuid4())).encode()
    db.kv_store[key] = b"not json"
    try:
        yield
    finally:
        db.kv_store.clear(key)


@pytest.mark.parametrize("runner", [tasks_runner_sync_promote, tasks_runner_sync_resync])
def test_a_cluster_read_error_ends_the_loop(corrupt_cluster, runner):
    clock = types.SimpleNamespace(sleep=MagicMock(side_effect=_StopLoop), time=time.time)
    with patch.object(runner, "time", clock), pytest.raises(json.JSONDecodeError):
        runner.main()
    clock.sleep.assert_not_called()


# ---------------------------------------------------------------------------
# the promote's marker settle at the end of a task
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


def _first_reconcile_raises(owners):
    """reconcile_lvs_move: the first owner's member RPC fails, the second
    owner's move settles on site B."""
    def _reconcile(owner_id):
        if owner_id == owners[0].get_id():
            raise RPCRemoteError(message="busy", code=-16)
        assert storage_node_ops.set_lvs_active_site(owner_id, stack.SITE_B, expect=MOVING_B)
        return stack.SITE_B
    return patch.object(storage_node_ops, "reconcile_lvs_move", side_effect=_reconcile)


def test_a_failing_reconcile_leaves_its_marker_and_settles_the_others():
    db = DBController()
    task, owners = _transferring_promote(db)
    with _first_reconcile_raises(owners) as reconcile:
        assert tasks_runner_sync_promote._end(task, "canceled") is True
    assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE
    assert [c.args[0] for c in reconcile.call_args_list] == [o.get_id() for o in owners]
    # the failed one is left to the storage-node monitor's reconcile
    assert stack._fresh(db, owners[0]).lvs_active_site == MOVING_B
    assert stack._fresh(db, owners[1]).lvs_active_site == stack.SITE_B


def test_a_failing_reconcile_does_not_escape_the_unexpected_error_path():
    # At max_retry the unexpected-error path ends the task like a failure,
    # inside the loop's except handler: nothing may escape it.
    db = DBController()
    task, owners = _transferring_promote(db, retry=4)
    with _first_reconcile_raises(owners):
        tasks_runner_sync_promote._record_unexpected_error(task, RuntimeError("lost answer"))
    done = db.get_task_by_id(task.uuid)
    assert done.status == JobSchedule.STATUS_DONE
    assert done.function_result.startswith("failed: max retry reached")
    assert stack._fresh(db, owners[0]).lvs_active_site == MOVING_B
    assert stack._fresh(db, owners[1]).lvs_active_site == stack.SITE_B
