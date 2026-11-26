"""bf36 (2026-11-26), cutover endgames of one source cluster.

One volume could not converge (every replication of it was a full copy: one
member of its target lvstore could not hold the landing volumes). Its endgame
held the cluster's only slot for the 15-minute shrink deadline, backed off for
5 minutes, and took the slot again right after -- five times in R8a -- while
another volume waited 1496 passes. A free slot now goes to the volume that has
waited longest, an abandoned endgame goes to the back of the queue, and past the
round cap an endgame that is not converging gives the slot up as soon as
another volume waits for it.
"""
import pytest

from simplyblock_core import constants
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer, TaskRetry
from simplyblock_core.test.test_migration_space_bf29 import (
    _DB,
    NOW,
    _lvol,
    _task,
    _use,
)


@pytest.fixture
def clock(monkeypatch):
    now = {"t": NOW}
    monkeypatch.setattr(runner.time, "time", lambda: now["t"])
    return now


def _waiter(uuid, lvol_id, queued_ago, seen_ago=5, cluster="A"):
    return _task(uuid, lvol_id, endgame_queued_at=NOW - queued_ago,
                 endgame_queue_seen=NOW - seen_ago, endgame_queue_cluster=cluster)


def test_a_free_slot_goes_to_the_volume_that_waited_longest(monkeypatch, clock):
    waiting = _waiter("T1", "LV1", queued_ago=600)
    mine = _task("T2", "LV2")                        # caught up just now
    _use(monkeypatch, _DB([waiting, mine]))
    with pytest.raises(TaskDefer, match="1 volume\\(s\\) queued earlier"):
        runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [waiting, mine])
    assert not mine.function_params.get("cutover_lvs")
    assert mine.function_params["endgame_queued_at"] == NOW
    assert mine.function_params["endgame_queue_cluster"] == "A"


def test_the_longest_waiter_takes_the_free_slot(monkeypatch, clock):
    later = _waiter("T1", "LV1", queued_ago=100)
    mine = _waiter("T2", "LV2", queued_ago=900)
    _use(monkeypatch, _DB([later, mine]))
    out = runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [later, mine])
    assert out.function_params["cutover_lvs"] == "LVS_2"
    for key in runner._ENDGAME_QUEUE_PARAMS:
        assert key not in out.function_params, "the claim ends the wait"


def test_a_waiter_that_stopped_renewing_does_not_hold_the_queue(monkeypatch, clock):
    gone = _waiter("T1", "LV1", queued_ago=900,
                   seen_ago=constants.REPL_ENDGAME_QUEUE_FRESH_SEC + 1)
    mine = _task("T2", "LV2")
    _use(monkeypatch, _DB([gone, mine]))
    out = runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [gone, mine])
    assert out.function_params["cutover_lvs"] == "LVS_2"


def test_a_waiter_of_another_cluster_is_not_ahead(monkeypatch, clock):
    other = _waiter("T1", "LV1", queued_ago=900, cluster="B")
    mine = _task("T2", "LV2")
    _use(monkeypatch, _DB([other, mine]))
    out = runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [other, mine])
    assert out.function_params["cutover_lvs"] == "LVS_2"


def test_a_busy_slot_keeps_the_first_wait_time(monkeypatch, clock):
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_cluster="A",
                   cutover_claimed_at=NOW - 60)
    mine = _waiter("T2", "LV2", queued_ago=300)
    _use(monkeypatch, _DB([holder, mine]))
    with pytest.raises(TaskDefer, match="1 of 1 in use"):
        runner._acquire_lvs_claim(mine.frozen_view(), _lvol("LV2", "LVS_2"), [holder, mine])
    assert mine.function_params["endgame_queued_at"] == NOW - 300
    assert mine.function_params["endgame_queue_seen"] == NOW


def test_an_abandoned_endgame_queues_again_at_the_back(monkeypatch, clock):
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_cluster="A",
                   cutover_claimed_at=NOW - 900, shrink_round=14,
                   endgame_queued_at=NOW - 2000, endgame_queue_cluster="A")
    _use(monkeypatch, _DB([holder]))
    monkeypatch.setattr(runner, "_discard_spare_landing", lambda t: None)
    monkeypatch.setattr(runner, "_requeue_direct_rounds", lambda t: None)
    with pytest.raises(TaskRetry):
        runner._abandon_endgame(holder.frozen_view(), _lvol("LV1", "LVS_1"), "did not converge")
    params = holder.function_params
    assert "cutover_lvs" not in params
    for key in runner._ENDGAME_QUEUE_PARAMS:
        assert key not in params, "after the back-off it waits behind those already queued"


def test_waiters_are_counted_for_the_early_yield(monkeypatch, clock):
    holder = _task("T1", "LV1", cutover_lvs="LVS_1", cutover_cluster="A")
    fresh = _waiter("T2", "LV2", queued_ago=300)
    stale = _waiter("T3", "LV3", queued_ago=300,
                    seen_ago=constants.REPL_ENDGAME_QUEUE_FRESH_SEC + 1)
    other = _waiter("T4", "LV4", queued_ago=300, cluster="B")
    _use(monkeypatch, _DB([holder, fresh, stale, other]))
    assert runner._endgame_waiters(holder, "A") == 1


def test_the_yield_is_wired_into_the_round_cap():
    """Past REPL_CUTOVER_MAX_SHRINK_ROUNDS, a round too long to freeze on abandons
    the endgame when volumes wait (before the 'taking another round' path)."""
    import inspect
    src = inspect.getsource(runner._shrink_step)
    cap = src.index("REPL_CUTOVER_MAX_SHRINK_ROUNDS")
    yield_at = src.index("_endgame_waiters(", cap)
    assert yield_at < src.index("freezing, taking another round", cap)
    assert "yielding the endgame slot" in src
