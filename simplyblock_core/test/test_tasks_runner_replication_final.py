"""D6 unit tests for the replication-final task handler.

The runner sits on the shared driver (``task_runner_base``), so the handler
is void: it returns on success, raises ``TaskRetry`` for every outcome the
driver should suspend and re-attempt, and ``TaskDefer`` while the delta-shrink
phase waits on the target without burning a retry. Task status/retry transitions
themselves belong to the driver and are covered by
tests/unit/tasks/test_task_runner_base.py.
"""
import pytest

from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer, TaskRetry


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
        # These tests cover the FREEZE, so the task starts past the endgame
        # entry: the lvstore is already claimed and the convergence rounds are
        # behind it (round 0 = no round in flight).
        "cutover_lvs": "lvs_src",
        "shrink_snap_id": "S_endgame",
        "shrink_round": 0,
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


def _primaries_freeze_and_receive(monkeypatch, db):
    """Who freezes and who receives is covered by test_cutover_node_failover;
    here both are the primaries."""
    monkeypatch.setattr(runner, "_cutover_source",
                        lambda lvol: (db.get_storage_node_by_id("S1"), ""))
    monkeypatch.setattr(runner, "_cutover_target",
                        lambda params, tgt: (tgt, ""))


def _install(monkeypatch, nodes, rep, cutover_ret):
    db = _FakeDB(nodes, rep)
    monkeypatch.setattr(runner, "db", db)
    calls = []

    def _run_cutover(src, tgt, lvol, comp, map_id, snap, operation="replicate",
                     src_exec=None, tgt_exec=None):
        calls.append((src.get_id(), tgt.get_id(), comp, map_id, snap, operation))
        return cutover_ret
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    # Who freezes and who receives is covered by test_cutover_node_failover;
    # here both are the primaries.
    monkeypatch.setattr(runner, "_cutover_source",
                        lambda lvol: (db.get_storage_node_by_id("S1"), ""))
    monkeypatch.setattr(runner, "_cutover_target",
                        lambda params, tgt: (tgt, ""))
    return calls


def test_happy_path_returns_and_updates_state(monkeypatch):
    rep = LVolReplication()
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    task = _task()
    assert runner.task_runner(task.frozen_view(), []) is None

    assert len(calls) == 1
    assert calls[0][5] == "replicate"
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE
    assert task.function_result == "cutover done"
    assert task.function_params["start_time"] <= task.function_params["end_time"]


def _grouped_rep():
    """A cutover-ready relationship whose source volume is in a consistency
    group."""
    rep = LVolReplication()
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.cutover_proceed = True
    rep.source_lvol = LVol()
    rep.source_lvol.uuid = "LV1"
    rep.source_lvol.group_id = "G1"
    rep.target_lvol = LVol()
    rep.target_lvol.uuid = "TLV1"
    rep.target_cluster_id = "TC1"
    return rep


def _stored_volume(monkeypatch, uuid):
    """The destination volume as the database holds it under ``uuid``: a
    distinct object from the relationship's embedded copy."""
    stored = LVol()
    stored.uuid = uuid
    stored.lvol_name = "stored-destination"
    read = runner.db.get_lvol_by_id
    monkeypatch.setattr(runner.db, "get_lvol_by_id",
                        lambda lid: stored if lid == uuid else read(lid))
    return stored


def _reconstitute(monkeypatch, side_effect=None):
    from simplyblock_core.controllers import consistency_group_controller
    calls = []

    def _record(source, dest, target_cluster_id):
        calls.append((source, dest, target_cluster_id))
        if side_effect is not None:
            raise side_effect
    monkeypatch.setattr(consistency_group_controller, "reconstitute_group_after_handoff", _record)
    return calls


def test_cutover_re_forms_the_consistency_group_on_the_destination(monkeypatch):
    rep = _grouped_rep()
    _install(monkeypatch, {"S1": _node("S1"), "T1": _node("T1")}, rep, (True, None))
    stored = _stored_volume(monkeypatch, "TLV1")
    calls = _reconstitute(monkeypatch)

    task = _task()
    assert runner.task_runner(task.frozen_view(), []) is None

    assert len(calls) == 1
    source, dest, target_cluster_id = calls[0]
    assert (source, target_cluster_id) == (rep.source_lvol, "TC1")
    assert dest is stored
    assert task.function_result == "cutover done"


def test_failback_re_forms_the_group_under_the_swapped_uuid(monkeypatch):
    """The fail-back UUID swap runs first, so the member is keyed by the
    volume's final UUID - the original source's."""
    rep = _grouped_rep()
    _install(monkeypatch, {"S1": _node("S1"), "T1": _node("T1")}, rep, (True, None))
    monkeypatch.setattr(runner, "_swap_failback_lvol_uuid",
                        lambda rep_, original_id: setattr(rep_.target_lvol, "uuid", original_id))
    stored = _stored_volume(monkeypatch, "ORIG1")
    calls = _reconstitute(monkeypatch)

    task = _task(failback_source_lvol_id="ORIG1")
    assert runner.task_runner(task.frozen_view(), []) is None

    assert len(calls) == 1
    source, dest, target_cluster_id = calls[0]
    assert (source, target_cluster_id) == (rep.source_lvol, "TC1")
    assert dest is stored


def test_a_failed_group_re_form_does_not_fail_the_cutover(monkeypatch):
    rep = _grouped_rep()
    _install(monkeypatch, {"S1": _node("S1"), "T1": _node("T1")}, rep, (True, None))
    calls = _reconstitute(monkeypatch, side_effect=RuntimeError("group gone"))

    task = _task()
    assert runner.task_runner(task.frozen_view(), []) is None

    assert len(calls) == 1
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE
    assert task.function_result == "cutover done"


def test_a_failed_final_step_burns_a_retry_at_once(monkeypatch):
    """The target online, its hub attached, and the final step failing is
    not a connectivity problem: it fails the same way on every attempt
    (2026-10-23, an SPDK final step failing at offset 0, retried about 80
    times). It burns a retry at once, with no cooldown loop in front."""
    rep = LVolReplication()
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install(monkeypatch, nodes, rep, (False, "transfer_state='Failed'"))

    task = _task()
    with pytest.raises(TaskRetry, match="Failed"):
        runner.task_runner(task.frozen_view(), [])

    assert "cutover_transient_since" not in task.function_params
    assert "cutover_retry_after" not in task.function_params


def _install_unreachable(monkeypatch, nodes, rep):
    db = _FakeDB(nodes, rep)
    monkeypatch.setattr(runner, "db", db)

    def _run_cutover(*_args, **_kwargs):
        raise runner.replication_final_step.CutoverUnreachable("hub attach failed")
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    _primaries_freeze_and_receive(monkeypatch, db)


def test_an_unreachable_target_waits_without_burning_a_retry(monkeypatch):
    """A hub that cannot be attached is transient -- a target node restarting
    before the control plane marks it down -- and is waited out behind a
    cooldown, without burning a retry, inside one window."""
    rep = LVolReplication()
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install_unreachable(monkeypatch, nodes, rep)

    task = _task()
    with pytest.raises(TaskDefer, match="hub attach failed"):
        runner.task_runner(task.frozen_view(), [])

    assert task.retry == 0, "a transient failure inside the window must not burn task.retry"
    assert task.function_params["cutover_transient_since"] > 0
    assert task.function_params["cutover_retry_after"] > 0


def test_an_unreachable_target_past_the_window_burns_a_retry_and_keeps_the_window(monkeypatch):
    """Past the window the failure burns a retry, and the window is kept: it
    is one budget for the whole task, so a burned retry does not buy another
    full window -- the two limits do not multiply."""
    import time

    from simplyblock_core import constants
    rep = LVolReplication()
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install_unreachable(monkeypatch, nodes, rep)

    since = int(time.time()) - constants.REPL_CUTOVER_TRANSIENT_WINDOW_SEC - 1
    task = _task(cutover_transient_since=since)
    with pytest.raises(TaskRetry, match="hub attach failed"):
        runner.task_runner(task.frozen_view(), [])

    assert task.function_params["cutover_transient_since"] == since
    assert "cutover_retry_after" not in task.function_params


def test_target_offline_defers_without_burning_a_retry(monkeypatch):
    """An offline target primary before the clone exists is transient:
    deferring waits it out, where retrying would exhaust the ceiling long
    before the node comes back. (Once the clone exists the cutover follows the
    target's settled leader instead -- test_cutover_node_failover.)"""
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1", status=StorageNode.STATUS_OFFLINE)}
    calls = _install(monkeypatch, nodes, rep, (True, None))
    task = _task()
    del task.function_params["tgt_lvol_composite"]

    with pytest.raises(TaskDefer, match="target node not online"):
        runner.task_runner(task.frozen_view(), [])
    assert calls == []


def test_missing_source_node_is_retryable_without_cutover(monkeypatch):
    rep = LVolReplication()
    calls = _install(monkeypatch, {"T1": _node("T1")}, rep, (True, None))

    with pytest.raises(TaskRetry, match="source node not found"):
        runner.task_runner(_task().frozen_view(), [])
    assert calls == []


def test_missing_lvol_id_is_retryable(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    with pytest.raises(TaskRetry, match="missing lvol_id"):
        runner.task_runner(_task(lvol_id="").frozen_view(), [])
    assert calls == []


# ---- delta-shrink state machine ------------------------------------------ #

class _ShrinkSnap:
    def __init__(self, replicated):
        self.target_replicated_snap_uuid = "T1" if replicated else ""


class _ShrinkLvol:
    uuid = "LV1"

    def get_id(self):
        return self.uuid


class _ShrinkDB:
    kv_store = "KV"

    def __init__(self, snaps):
        self._snaps = snaps

    def get_snapshot_by_id(self, sid):
        if sid not in self._snaps:
            raise KeyError(sid)
        return self._snaps[sid]


def _shrink_task(params):
    task = JobSchedule()
    task.uuid = "shrink-1"
    task.status = JobSchedule.STATUS_RUNNING
    task.max_retry = 100
    task.function_params = dict(params)
    return task


def _mk(monkeypatch, snaps, params):
    import simplyblock_core.services.tasks_runner_replication_final as runner
    monkeypatch.setattr(runner, "db", _ShrinkDB(snaps))
    return runner, _shrink_task(params)


def _shrink(runner, task, lvol=None):
    """Drive one shrink pass the way the driver would: on a read-only view."""
    return runner._shrink_step(task.frozen_view(), lvol or _ShrinkLvol())


def test_shrink_waits_until_replicated(monkeypatch):
    """An unreplicated round yields the pass instead of failing.

    It now POLLS for a short window first (a round that lands just after the
    pass is handed back would otherwise cost a full TASK_EXEC_INTERVAL_SEC of
    writes in the next round), so squeeze the inline window to nothing to keep
    the test instant.
    """
    from simplyblock_core import constants
    monkeypatch.setattr(constants, "REPL_CUTOVER_MIN_INLINE_SEC", 0)
    monkeypatch.setattr(constants, "REPL_CUTOVER_CONVERGE_BUDGET_SEC", 0)
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=False)},
                       {"shrink_round": 1, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60})
    with pytest.raises(TaskDefer, match="waiting"):
        _shrink(runner, task)


def test_a_fast_round_converges_instead_of_taking_another(monkeypatch):
    """The criterion is transfer TIME, not a round count.

    A round that replicated within REPL_CUTOVER_CONVERGE_TARGET_SEC means the
    next such window -- the one the freeze copies -- is about as small, so the
    cutover starts immediately rather than taking more snapshots for nothing.
    """
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=True)},
                       {"shrink_round": 1, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60,
                        # holding the lvstore == the endgame; converging in the
                        # open asks for it instead of freezing
                        "cutover_lvs": "LVS_1",
                        "shrink_started_at": __import__("time").time()})
    taken = []

    def _add(lid, name, snap_type="user"):
        taken.append((lid, snap_type))
        return "S2", None
    import simplyblock_core.controllers.snapshot_controller as sc
    monkeypatch.setattr(sc, "add", _add)

    _shrink(runner, task)  # returns => the freeze may start
    assert taken == [], "a converged round must not take another snapshot"
    assert "converged" in task.function_result


def test_shrink_takes_next_snapshot_immediately(monkeypatch):
    """A SLOW round is followed by the next one straight away.

    The delta the next round carries is only what was written during this
    round's transfer -- which is the whole mechanism by which the freeze gets
    shorter.
    """
    import time as _time

    from simplyblock_core import constants
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=True)},
                       {"shrink_round": 1, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60,
                        # started long enough ago to be well over the target
                        "shrink_started_at": _time.time() - 60})
    taken = []

    def _add(lid, name, snap_type="user"):
        taken.append((lid, snap_type))
        # the second round reports as still in flight, so the loop yields
        runner.db._snaps["S2"] = _ShrinkSnap(replicated=False)
        return "S2", None
    import simplyblock_core.controllers.snapshot_controller as sc
    monkeypatch.setattr(sc, "add", _add)
    monkeypatch.setattr(constants, "REPL_CUTOVER_MIN_INLINE_SEC", 0)
    monkeypatch.setattr(constants, "REPL_CUTOVER_CONVERGE_BUDGET_SEC", 0)

    with pytest.raises(TaskDefer):
        _shrink(runner, task)
    assert taken and taken[0][0] == "LV1"
    assert task.function_params["shrink_round"] == 2
    assert task.function_params["shrink_snap_id"] == "S2"


def test_shrink_hands_over_when_it_cannot_converge(monkeypatch):
    """Written faster than it replicates: freeze anyway, but say so -- as
    long as the round fits the final step's safe window."""
    import time as _time

    from simplyblock_core import constants
    monkeypatch.setattr(constants, "REPL_CUTOVER_MAX_SHRINK_ROUNDS", 3)
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=True)},
                       {"shrink_round": 3, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60,
                        "cutover_lvs": "LVS_1",
                        "shrink_started_at": _time.time() - 4})
    # Returning (rather than raising) is what hands over to the freeze.
    _shrink(runner, task)
    assert "not converged" in task.function_result


def test_shrink_cap_keeps_converging_when_the_final_step_would_time_out(monkeypatch):
    """bf12 2026-11-03: a 22s round at the cap froze "anyway" and the final
    step failed at SPDK's 12s limit. Past the cap a round that long takes
    another round instead of freezing."""
    import time as _time

    from simplyblock_core import constants
    monkeypatch.setattr(constants, "REPL_CUTOVER_MAX_SHRINK_ROUNDS", 3)
    monkeypatch.setattr(constants, "REPL_CUTOVER_MIN_INLINE_SEC", 0)
    monkeypatch.setattr(constants, "REPL_CUTOVER_CONVERGE_BUDGET_SEC", 0)
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=True)},
                       {"shrink_round": 3, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60,
                        "cutover_lvs": "LVS_1",
                        "shrink_started_at": _time.time() - 22})
    taken = []

    def _add(lid, name, snap_type="user"):
        taken.append(lid)
        runner.db._snaps["S2"] = _ShrinkSnap(replicated=False)
        return "S2", None
    import simplyblock_core.controllers.snapshot_controller as sc
    monkeypatch.setattr(sc, "add", _add)

    with pytest.raises(TaskDefer):
        _shrink(runner, task)
    assert taken == ["LV1"], "a round too long to freeze is followed by another"
    assert task.function_params["shrink_round"] == 4
    assert "not converged" not in str(task.function_result)


def test_shrink_deadline_without_a_freezable_round_backs_off(monkeypatch):
    """An expired deadline with no round short enough for the final step
    abandons the endgame (one attempt burned, lvstore released) instead of
    freezing into SPDK's terminate timeout."""
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=False)},
                       {"shrink_round": 1, "shrink_snap_id": "S1",
                        "shrink_deadline": 1, "cutover_lvs": "LVS_1"})
    with pytest.raises(TaskRetry, match="shrink deadline expired"):
        _shrink(runner, task)
    assert "cutover_lvs" not in task.function_params
    assert "shrink_snap_id" not in task.function_params
    assert task.function_params["endgame_retry_after"] > 1


def _install_raising(monkeypatch, nodes, rep, exc):
    db = _FakeDB(nodes, rep)
    monkeypatch.setattr(runner, "db", db)

    def _run_cutover(*_args, **_kwargs):
        raise exc
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    _primaries_freeze_and_receive(monkeypatch, db)


def test_a_dropped_connection_waits_without_burning_a_retry(monkeypatch):
    from simplyblock_core.rpc_client import RPCConnectionError
    rep = LVolReplication()
    rep.cutover_proceed = True
    _install_raising(monkeypatch, {"S1": _node("S1"), "T1": _node("T1")}, rep,
                     RPCConnectionError("Could not reach remote"))

    with pytest.raises(TaskDefer, match="Could not reach remote"):
        runner.task_runner(_task().frozen_view(), [])


def test_an_error_the_target_answered_burns_a_retry(monkeypatch):
    """A JSON-RPC error proves the server was reached: it fails the same way
    on every attempt and is not waited out."""
    from simplyblock_core.rpc_client import RPCRemoteError
    rep = LVolReplication()
    rep.cutover_proceed = True
    _install_raising(monkeypatch, {"S1": _node("S1"), "T1": _node("T1")}, rep,
                     RPCRemoteError("Invalid parameters", code=-32602))

    with pytest.raises(TaskRetry, match="Invalid parameters"):
        runner.task_runner(_task().frozen_view(), [])


# ---- a failed final step is recoverable ---------------------------------- #

def _pending_rep(monkeypatch):
    rep = LVolReplication()
    rep.uuid = "REP1"
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.cutover_proceed = True
    return rep


def test_a_failed_final_step_returns_the_volume_to_replicating(monkeypatch):
    """bf12 2026-11-03: the final step hit SPDK's terminate timeout, the
    source paths were restored (fio saw no error), and the volume then sat in
    cutover_pending for 15h+. Nothing moved, so it is recoverable: the
    attempt is counted, the prepared clone and relationship are discarded,
    and the endgame starts over from converging."""
    rep = _pending_rep(monkeypatch)
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (False, "transfer_state='Failed'"))

    task = _task(tgt_lvol_id="CLONE1", cutover_proceed_timeout=1,
                 shrink_round=4, shrink_round_times=[22.0])
    with pytest.raises(TaskRetry, match="Failed"):
        runner.task_runner(task.frozen_view(), [])

    assert len(calls) == 1
    p = task.function_params
    for key in runner._PREPARED_CUTOVER_PARAMS + runner._ENDGAME_ROUND_PARAMS:
        assert key not in p, f"{key} must not survive a failed final step"
    assert "cutover_lvs" not in p, "the lvstore claim must be released"
    assert p["discard_tgt_lvol_id"] == "CLONE1"
    assert p["discard_replication_id"] == "REP1"
    # Back to replicating, so detach/stop no longer refuse it and the
    # operator no longer sees a cutover pending.
    assert rep.state == LVolReplication.STATE_REPLICATING
    assert rep.cutover_proceed is False, "the next clone needs its own signal"
    assert "shrink_snap_id" not in p, "the endgame's retention hold is released"
    assert "Failed" in p["last_error"]


def test_the_retry_after_a_failed_final_step_does_not_freeze_again(monkeypatch):
    """The next pass must not go straight back into run_cutover with the old
    clone (a second freeze from a stale base): it discards the clone first,
    then waits for the volume to catch up again like any endgame."""
    rep = _pending_rep(monkeypatch)
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (False, "transfer_state='Failed'"))
    task = _task(tgt_lvol_id="CLONE1")
    with pytest.raises(TaskRetry):
        runner.task_runner(task.frozen_view(), [])

    # Inside SPDK's terminate window: nothing is touched yet.
    with pytest.raises(TaskDefer, match="SPDK"):
        runner.task_runner(task.frozen_view(), [])

    # Past it, with the clone already gone: back to the catch-up gate.
    task.function_params["discard_after"] = 0
    monkeypatch.setattr(runner, "_delete_cutover_clone", lambda clone_id: True)
    monkeypatch.setattr(runner, "_replication_lag_sec", lambda lvol: 500.0)
    with pytest.raises(TaskDefer, match="catch up"):
        runner.task_runner(task.frozen_view(), [])
    assert len(calls) == 1, "no second freeze"
    assert "discard_tgt_lvol_id" not in task.function_params


def _discard_task(**params):
    t = _task(**params)
    for key in runner._PREPARED_CUTOVER_PARAMS:
        t.function_params.pop(key, None)
    return t


def test_the_failed_clone_is_deleted_before_a_new_one_is_built(monkeypatch):
    """The next clone takes the same bdev name; the idempotent create would
    adopt the old one, chained on the old base."""
    monkeypatch.setattr(runner, "db", _FakeDB({}, None))
    state = {"gone": False}
    monkeypatch.setattr(runner, "_delete_cutover_clone",
                        lambda clone_id: state["gone"])
    task = _discard_task(discard_tgt_lvol_id="CLONE1",
                         discard_after=int(__import__("time").time()) - 1)

    with pytest.raises(TaskDefer, match="to be deleted"):
        runner._discard_failed_cutover(task.frozen_view())
    assert task.function_params["discard_tgt_lvol_id"] == "CLONE1"

    state["gone"] = True
    runner._discard_failed_cutover(task.frozen_view())
    assert "discard_tgt_lvol_id" not in task.function_params


def test_a_clone_that_cannot_be_deleted_ends_the_task(monkeypatch):
    from simplyblock_core import constants
    from simplyblock_core.services.task_runner_base import TaskAbort
    monkeypatch.setattr(runner, "db", _FakeDB({}, None))
    monkeypatch.setattr(runner, "_delete_cutover_clone", lambda clone_id: False)
    task = _discard_task(
        discard_tgt_lvol_id="CLONE1",
        discard_after=1 - constants.REPL_CUTOVER_SHRINK_TIMEOUT_SEC)
    with pytest.raises(TaskAbort, match="not deleted"):
        runner._discard_failed_cutover(task.frozen_view())


def test_a_spent_budget_releases_the_clone_and_the_relationship(monkeypatch):
    """When the driver gives up, the handler never runs again: on_finish is
    what takes the volume out of cutover_pending and deletes the clone, so
    the plan's failed volume holds nothing."""
    rep = _pending_rep(monkeypatch)
    monkeypatch.setattr(runner, "db", _FakeDB({}, rep))
    deleted = []
    monkeypatch.setattr(runner, "_delete_cutover_clone",
                        lambda clone_id: deleted.append(clone_id) or False)
    assert runner.SPEC.on_finish is runner._cleanup_unfinished_cutover

    task = _task(tgt_lvol_id="CLONE1")
    task.status = JobSchedule.STATUS_DONE
    task.function_result = "max retry reached (5) after: transfer_state='Failed'"
    runner._cleanup_unfinished_cutover(task)
    assert rep.state == LVolReplication.STATE_REPLICATING
    assert deleted == ["CLONE1"]


def test_a_completed_cutover_keeps_its_clone(monkeypatch):
    rep = _pending_rep(monkeypatch)
    monkeypatch.setattr(runner, "db", _FakeDB({}, rep))
    deleted = []
    monkeypatch.setattr(runner, "_delete_cutover_clone",
                        lambda clone_id: deleted.append(clone_id))
    task = _task(tgt_lvol_id="CLONE1", end_time=5)
    task.status = JobSchedule.STATUS_DONE
    task.function_result = "cutover done"
    runner._cleanup_unfinished_cutover(task)
    assert rep.state == LVolReplication.STATE_CUTOVER_PENDING and deleted == []


def test_a_canceled_task_whose_cutover_landed_keeps_its_clone(monkeypatch):
    """A cancel that raced the freeze can leave the task without end_time or
    "cutover done" although the relationship cut over: the clone serves IO
    then and must not be deleted."""
    rep = _pending_rep(monkeypatch)
    rep.state = LVolReplication.STATE_CUTOVER_DONE
    monkeypatch.setattr(runner, "db", _FakeDB({}, rep))
    deleted = []
    monkeypatch.setattr(runner, "_delete_cutover_clone",
                        lambda clone_id: deleted.append(clone_id))
    task = _task(tgt_lvol_id="CLONE1")
    task.status = JobSchedule.STATUS_DONE
    task.canceled = True
    task.function_result = "canceled"
    runner._cleanup_unfinished_cutover(task)
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE and deleted == []


def test_the_freeze_is_marked_before_the_source_is_fenced(monkeypatch):
    """A plan cancel or detach spares a cutover only while it freezes; the
    marker has to be on the row before run_cutover fences anything."""
    rep = _pending_rep(monkeypatch)
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    seen = []
    task = _task()

    def _run_cutover(*_a, **_kw):
        seen.append(task.function_params.get("freeze_started_at"))
        return True, None
    _install(monkeypatch, nodes, rep, (True, None))
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    runner.task_runner(task.frozen_view(), [])
    assert seen and seen[0], "freeze_started_at must be recorded before the freeze"


def test_freezing_is_what_spares_a_cutover():
    from simplyblock_core import constants
    from simplyblock_core.controllers import tasks_controller as tc
    now = 10_000.0
    assert not tc.cutover_is_freezing(_task(), now=now), "prepared but waiting"
    assert tc.cutover_is_freezing(_task(freeze_started_at=now - 5), now=now)
    assert not tc.cutover_is_freezing(
        _task(freeze_started_at=now - constants.REPL_CUTOVER_FREEZE_GUARD_SEC - 1),
        now=now), "a pass that died mid-freeze does not pin the task forever"
    assert tc.cutover_is_freezing(_task(end_time=1), now=now), "already moved"


def test_a_failed_endgame_does_not_wait_forever_to_catch_up(monkeypatch):
    """Under heavy IO a volume can be written about as fast as its cadence
    replicates; after a failed endgame its lag never gets back under the
    gate, and the task deferred there forever, holding its node's turn."""
    from simplyblock_core import constants
    task = _discard_task(endgame_started_at=1.0)
    task.function_params.pop("cutover_lvs")
    clock = {"now": 10_000}
    monkeypatch.setattr(runner.time, "time", lambda: clock["now"])

    runner._bound_catchup_after_failed_endgame(task.frozen_view(), 500.0)
    assert task.function_params["catchup_since"] == 10_000

    clock["now"] += constants.REPL_CUTOVER_SHRINK_TIMEOUT_SEC
    runner._bound_catchup_after_failed_endgame(task.frozen_view(), 500.0)

    clock["now"] += 1
    with pytest.raises(TaskRetry, match="did not catch up"):
        runner._bound_catchup_after_failed_endgame(task.frozen_view(), 500.0)
    assert "catchup_since" not in task.function_params


def test_the_first_catch_up_is_not_bounded(monkeypatch):
    """A volume that never entered an endgame is still shipping its initial
    copy, which takes as long as it takes."""
    task = _discard_task()
    runner._bound_catchup_after_failed_endgame(task.frozen_view(), 10 ** 6)
    assert "catchup_since" not in task.function_params


def test_a_bf12_task_left_by_the_old_code_is_released(monkeypatch):
    """The 2026-11-03 tasks: prepared clone, claim released by the failed
    attempt, relationship cutover_pending, lag growing for 16h. The next pass
    discards the stale clone and reverts the relationship; it never freezes
    from the stale base."""
    rep = _pending_rep(monkeypatch)
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))
    monkeypatch.setattr(runner, "_replication_lag_sec", lambda lvol: 57017.0)
    task = _task(tgt_lvol_id="CLONE1", endgame_started_at=1.0)
    task.function_params.pop("cutover_lvs")

    with pytest.raises(TaskDefer, match="stale cutover clone"):
        runner.task_runner(task.frozen_view(), [])
    assert calls == []
    assert rep.state == LVolReplication.STATE_REPLICATING
    p = task.function_params
    assert p["discard_tgt_lvol_id"] == "CLONE1"
    assert "tgt_lvol_composite" not in p and "shrink_snap_id" not in p
