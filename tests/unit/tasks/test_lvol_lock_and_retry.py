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
import contextlib
import time
from unittest import mock

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import snapshot_controller, tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
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


# --- 4. a leg that keeps failing must raise an alert --------------------

def _register_task(retry=0):
    t = mock.MagicMock()
    t.canceled = False
    t.date = int(time.time())
    t.retry = retry
    t.node_id = "node-2"
    t.cluster_id = "cl-1"
    t.function_result = ""
    t.function_params = {"lvol_id": "lvol-1", "op": "register",
                         "secondary_index": 0}
    t.status = JobSchedule.STATUS_NEW
    return t


def _run_register_failing(task, err="boom"):
    """Drive run_lvol_sync_op_task to its genuine-failure branch."""
    lvol = mock.MagicMock()
    lvol.status = "online"
    lvol.nodes = ["node-1", "node-2"]
    lvol.lvs_name = "LVS_1"
    node = mock.MagicMock()
    node.get_id.return_value = "node-2"
    node.status = "online"
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(
            tasks_controller.db, "get_lvol_by_id", return_value=lvol))
        stack.enter_context(mock.patch.object(
            tasks_controller.db, "get_storage_node_by_id", return_value=node))
        stack.enter_context(mock.patch(
            "simplyblock_core.storage_node_ops.get_restart_phase",
            return_value=None))
        stack.enter_context(mock.patch(
            "simplyblock_core.storage_node_ops."
            "repair_lvol_registration_on_non_leader",
            return_value=(False, err)))
        event = stack.enter_context(mock.patch.object(
            tasks_controller.events_controller, "log_event_cluster"))
        tasks_controller.run_lvol_sync_op_task(task)
    return event


def test_first_failures_do_not_alert():
    """Transient failures are normal; only persistence is abnormal."""
    for retry in range(constants.TASK_FAILURE_ALERT_THRESHOLD):
        task = _register_task(retry=retry)
        event = _run_register_failing(task)
        assert event.call_count == 0, f"alerted at retry={retry + 1}"
        assert task.status == JobSchedule.STATUS_SUSPENDED


def test_alert_once_past_the_threshold():
    task = _register_task(retry=constants.TASK_FAILURE_ALERT_THRESHOLD)
    event = _run_register_failing(task)
    assert event.call_count == 1
    assert event.call_args.kwargs["event_level"] == "Critical"
    assert task.retry == constants.TASK_FAILURE_ALERT_THRESHOLD + 1


def test_same_error_does_not_alert_every_cycle():
    """A node that stays broken must not write an event every 3 seconds."""
    task = _register_task(retry=constants.TASK_FAILURE_ALERT_THRESHOLD + 5)
    task.function_result = "registration failed: boom"
    event = _run_register_failing(task, err="boom")
    assert event.call_count == 0


def test_changed_error_alerts_again():
    task = _register_task(retry=constants.TASK_FAILURE_ALERT_THRESHOLD + 5)
    task.function_result = "registration failed: something else"
    event = _run_register_failing(task, err="boom")
    assert event.call_count == 1


def test_enodev_is_never_counted_or_alerted():
    """-19 means the object is already gone; not an actionable alert."""
    task = _register_task(retry=constants.TASK_FAILURE_ALERT_THRESHOLD + 5)
    event = _run_register_failing(task, err={"code": -19})
    assert event.call_count == 0
    assert task.retry == constants.TASK_FAILURE_ALERT_THRESHOLD + 5, (
        "-19 must not advance the failure counter")


# --- 5. a peer restart is waited out under the lock, but not forever ----

def _phase_sequence(*phases):
    """get_restart_phase stub returning each value in turn, then the last."""
    seq = list(phases)

    def _get(node_id, lvs_name):
        return seq.pop(0) if len(seq) > 1 else seq[0]
    return _get


def _check(monkeypatch, phase_fn, wait):
    from simplyblock_core import storage_node_ops
    node = mock.MagicMock()
    node.secondary_node_id = None
    node.tertiary_node_id = None
    node.cluster_id = "cl-1"
    monkeypatch.setattr(storage_node_ops.DBController, "__new__",
                        lambda cls, *a, **k: mock.MagicMock(
                            get_storage_node_by_id=lambda _: node))
    monkeypatch.setattr(storage_node_ops, "_check_peer_disconnected",
                        lambda *a, **k: False)
    monkeypatch.setattr(storage_node_ops, "get_restart_phase", phase_fn)
    monkeypatch.setattr(storage_node_ops, "_is_node_rpc_responsive",
                        lambda *a, **k: True)
    monkeypatch.setattr(storage_node_ops.time, "sleep", lambda *_: None)
    return storage_node_ops.check_non_leader_for_operation(
        "node-2", "LVS_1", wait_for_restart=wait)


def test_restart_is_queued_when_the_caller_does_not_wait(monkeypatch):
    """Default behaviour is unchanged for the callers that hold no lock."""
    phase = _phase_sequence(StorageNode.RESTART_PHASE_BLOCKED)
    assert _check(monkeypatch, phase, wait=0) == "queue"


def test_restart_that_clears_lets_the_leg_run_under_the_lock(monkeypatch):
    phase = _phase_sequence(StorageNode.RESTART_PHASE_BLOCKED,
                            StorageNode.RESTART_PHASE_BLOCKED, "")
    assert _check(monkeypatch, phase, wait=30) == "proceed"


def test_wedged_restart_still_defers_rather_than_pinning_the_chain(monkeypatch):
    """RESTART_TASK_EXEC_INTERVAL_MAX_SEC is 3600; the lock must not be held that long."""
    phase = _phase_sequence(StorageNode.RESTART_PHASE_BLOCKED)
    assert _check(monkeypatch, phase, wait=1) == "queue"


def test_restart_wait_is_bounded():
    assert 0 < constants.DEFERRED_LEG_RESTART_WAIT_SEC <= 300
