# coding=utf-8
"""Regression tests for the lvol delete/create exclusivity and retry fixes.

Three defects, all found by reading the delete/create paths 2026-08-28:

  1. FN_LVOL_SYNC_DEL declared max_retry=10 while its runner never
     incremented task.retry nor compared it -- a bound documented in the
     record and absent from the code. Giving up would leak the volume in
     in_deletion (the async delete has already succeeded by then), so the
     fix makes the unboundedness explicit rather than accidental.

  2. add_lvol_ha queues its non-leader register tasks in the pre-check but
     writes the lvol record only at the end of the create. The runner polls
     every 3s, so a task picked up inside that window hit KeyError and was
     marked DONE ("lvol no longer exists") -- permanently dropping the
     registration and leaving the replica missing (the LVOL_109 class).

  3. force_delete disabled BOTH locks, so a recovery delete ran fully
     unlocked and could interleave with any create/delete/resize on the same
     chain. It must still not block forever behind a holder that died on a
     node that is now gone, hence best-effort rather than plain enforcement.
"""
import time
from unittest import mock

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import snapshot_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.exceptions import PreconditionError


# --- 1. sync delete is explicitly unbounded -----------------------------

def test_sync_delete_task_is_explicitly_unbounded():
    with mock.patch.object(tasks_controller, "_add_task") as add:
        tasks_controller.add_lvol_sync_del_task("cl", "node", "LVS_1/LVOL_1", "prim")
    assert add.call_args.kwargs["max_retry"] == -1, (
        "a bound that the runner does not enforce is worse than none: it "
        "documents a ceiling that never fires")


def test_sync_delete_matches_register_retry_semantics():
    """Both are unbounded-and-self-obsoleting; they must not disagree."""
    with mock.patch.object(tasks_controller, "_add_task") as add:
        tasks_controller.add_lvol_sync_del_task("cl", "n", "LVS_1/L", "p")
        delete_retry = add.call_args.kwargs["max_retry"]
    with mock.patch.object(tasks_controller, "_add_task") as add:
        tasks_controller.add_lvol_sync_op_task("cl", "n", "lvol", "register")
        register_retry = add.call_args.kwargs["max_retry"]
    assert delete_retry == register_retry == -1


# --- 2. a not-yet-written lvol record must not drop the registration ----

def _task(age_seconds):
    t = mock.MagicMock()
    t.canceled = False
    t.date = int(time.time()) - age_seconds
    t.function_params = {"lvol_id": "lvol-1", "op": "register",
                         "secondary_index": 0}
    t.status = JobSchedule.STATUS_NEW
    return t


def test_register_defers_while_the_create_is_still_writing_the_record():
    task = _task(age_seconds=5)
    with mock.patch.object(tasks_controller.db, "get_lvol_by_id",
                           side_effect=KeyError("not yet")):
        tasks_controller.run_lvol_sync_op_task(task)
    assert task.status == JobSchedule.STATUS_SUSPENDED, (
        "inside the grace window a missing record means the create has not "
        "committed yet, not that the volume was deleted")


def test_register_completes_once_the_record_is_genuinely_gone():
    task = _task(age_seconds=constants.LVOL_SYNC_OP_RECORD_GRACE_SEC + 60)
    with mock.patch.object(tasks_controller.db, "get_lvol_by_id",
                           side_effect=KeyError("deleted")):
        tasks_controller.run_lvol_sync_op_task(task)
    assert task.status == JobSchedule.STATUS_DONE


# --- 3. force delete takes the locks best-effort, never bypasses them ---

@pytest.mark.parametrize("lock_factory,args", [
    (snapshot_controller.lvstore_op_lock, ("cl", "LVS_1")),
    (snapshot_controller.object_mutation_lock, ("cl", "obj-1")),
])
def test_lock_raises_when_not_best_effort(lock_factory, args):
    with mock.patch.object(snapshot_controller,
                           "_acquire_lvstore_lock_blocking", return_value=False):
        with pytest.raises(PreconditionError):
            with lock_factory(*args):
                pass


@pytest.mark.parametrize("lock_factory,args", [
    (snapshot_controller.lvstore_op_lock, ("cl", "LVS_1")),
    (snapshot_controller.object_mutation_lock, ("cl", "obj-1")),
])
def test_lock_proceeds_when_best_effort(lock_factory, args):
    """Recovery must not deadlock behind a holder on a node that is gone."""
    entered = False
    with mock.patch.object(snapshot_controller,
                           "_acquire_lvstore_lock_blocking", return_value=False):
        with lock_factory(*args, best_effort=True):
            entered = True
    assert entered


def test_force_delete_wait_is_bounded():
    assert 0 < constants.FORCE_DELETE_LOCK_WAIT_SEC <= 120
