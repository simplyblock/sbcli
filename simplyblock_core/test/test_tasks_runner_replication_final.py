"""D6 unit tests for the replication-final task runner lifecycle."""
import pytest

from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode


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


def _install(monkeypatch, nodes, rep, cutover_ret):
    db = _FakeDB(nodes, rep)
    monkeypatch.setattr(runner, "db", db)
    calls = []

    def _run_cutover(src, tgt, lvol, comp, map_id, snap, operation="replicate"):
        calls.append((src.get_id(), tgt.get_id(), comp, map_id, snap, operation))
        return cutover_ret
    monkeypatch.setattr(runner.replication_final_step, "run_cutover", _run_cutover)
    return calls


def test_happy_path_marks_done_and_updates_state(monkeypatch):
    rep = LVolReplication()
    rep.state = LVolReplication.STATE_CUTOVER_PENDING
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    res = runner.task_runner(_task())

    assert res is True
    assert len(calls) == 1
    assert calls[0][5] == "replicate"
    assert rep.state == LVolReplication.STATE_CUTOVER_DONE


def test_failure_enters_hub_cooldown_without_burning_a_retry(monkeypatch):
    """With the clone already prepared (tgt_lvol_composite set), a run_cutover
    failure is treated as likely connectivity trouble: the task suspends
    behind a cooldown and task.retry stays intact for the first
    REPL_CUTOVER_MAX_HUB_ATTEMPTS attempts — burning retries at the poll
    interval would exhaust the ceiling long before a restarting node
    recovers."""
    rep = LVolReplication()
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install(monkeypatch, nodes, rep, (False, "boom"))

    task = _task()
    res = runner.task_runner(task)

    assert res is False
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.function_result == "boom"
    assert task.retry == 0, "a transient hub attempt must not burn task.retry"
    assert task.function_params["cutover_hub_attempts"] == 1
    assert task.function_params["cutover_retry_after"] > 0


def test_failure_burns_a_retry_once_hub_attempts_are_exhausted(monkeypatch):
    """Past the hub-attempt cap with the target node online, the failure is
    real: the cooldown state resets and one retry is burned, so the ceiling
    in task_runner can eventually end a cutover that keeps failing."""
    from simplyblock_core import constants
    rep = LVolReplication()
    rep.cutover_proceed = True
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    _install(monkeypatch, nodes, rep, (False, "boom"))

    task = _task(cutover_hub_attempts=constants.REPL_CUTOVER_MAX_HUB_ATTEMPTS)
    res = runner.task_runner(task)

    assert res is False
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 1
    assert "cutover_hub_attempts" not in task.function_params
    assert "cutover_retry_after" not in task.function_params


def test_max_retry_marks_done_without_cutover(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1")}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    task = _task()
    task.retry = 5  # == max_retry
    res = runner.task_runner(task)

    assert res is True
    assert task.status == JobSchedule.STATUS_DONE
    assert calls == []  # cutover never attempted


def test_target_offline_suspends(monkeypatch):
    rep = LVolReplication()
    nodes = {"S1": _node("S1"), "T1": _node("T1", status=StorageNode.STATUS_OFFLINE)}
    calls = _install(monkeypatch, nodes, rep, (True, None))

    task = _task()
    res = runner.task_runner(task)

    assert res is False
    assert task.status == JobSchedule.STATUS_SUSPENDED
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


class _ShrinkTask:
    max_retry = 100
    canceled = False

    def __init__(self, params):
        self.function_params = params
        self.function_result = ""
        self.retry = 0
        self.status = "running"

    def write_to_db(self, kv=None):
        pass


def _mk(monkeypatch, snaps, params):
    import simplyblock_core.services.tasks_runner_replication_final as runner
    monkeypatch.setattr(runner, "db", _ShrinkDB(snaps))
    return runner, _ShrinkTask(params)


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
    done, err = runner._shrink_step(task, _ShrinkLvol())
    assert (done, err) == (False, None)
    assert "waiting" in task.function_result


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

    done, err = runner._shrink_step(task, _ShrinkLvol())
    assert (done, err) == (True, None)
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

    done, err = runner._shrink_step(task, _ShrinkLvol())
    assert (done, err) == (False, None)
    assert taken and taken[0][0] == "LV1"
    assert task.function_params["shrink_round"] == 2
    assert task.function_params["shrink_snap_id"] == "S2"


def test_shrink_hands_over_when_it_cannot_converge(monkeypatch):
    """Written faster than it replicates: freeze anyway, but say so."""
    import time as _time
    from simplyblock_core import constants
    monkeypatch.setattr(constants, "REPL_CUTOVER_MAX_SHRINK_ROUNDS", 3)
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=True)},
                       {"shrink_round": 3, "shrink_snap_id": "S1",
                        "shrink_deadline": 2**60,
                        "cutover_lvs": "LVS_1",
                        "shrink_started_at": _time.time() - 60})
    done, err = runner._shrink_step(task, _ShrinkLvol())
    assert (done, err) == (True, None), \
        "the round cap must hand over to the freeze, not fail the cutover"
    assert "not converged" in task.function_result


def test_shrink_deadline_proceeds_to_cutover(monkeypatch):
    """An expired deadline stops adding rounds and hands straight over to the
    freeze: the residual delta is slightly larger than a converged one, but
    proceeding always beats failing the task and waiting out another
    900-second shrink window."""
    runner, task = _mk(monkeypatch, {"S1": _ShrinkSnap(replicated=False)},
                       {"shrink_round": 1, "shrink_snap_id": "S1",
                        "shrink_deadline": 1})
    done, err = runner._shrink_step(task, _ShrinkLvol())
    assert (done, err) == (True, None), \
        "the deadline must hand over to the freeze, not fail the cutover"
