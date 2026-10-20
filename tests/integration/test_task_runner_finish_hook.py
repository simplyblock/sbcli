"""``RunnerSpec.finish``: the spec's own terminal commit, against real FDB.

The hook replaces the driver's DONE write on every terminal transition, is told
why the task ends, and returns the row as it committed it. ``on_finish`` runs
only when that row is DONE; a hook that commits the task non-DONE re-arms it.
"""
import json
from unittest.mock import MagicMock

import fdb
import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import FrozenTaskError, JobSchedule
from simplyblock_core.services import task_runner_base as trb
from tests.integration.task_driver import drive

db = trb.db

CLUSTER = "cluster-1"
FN = "finish_hook_test"
DOMAIN_KEY = b"test/finish_hook/domain"


@pytest.fixture(autouse=True)
def cluster():
    c = Cluster()
    c.uuid = CLUSTER
    c.nqn = f"nqn.test:{CLUSTER}"
    c.status = Cluster.STATUS_ACTIVE
    c.write_to_db(db.kv_store)
    return c


def _seed(status=JobSchedule.STATUS_NEW, retry=0, max_retry=3, canceled=False, result=""):
    task = JobSchedule()
    task.uuid = "6b8563cd-8237-4136-9b37-7b767c15bfb8"
    task.cluster_id = CLUSTER
    task.function_name = FN
    task.status = status
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = canceled
    task.function_result = result
    task.write_to_db(db.kv_store)
    return task


def _spec(handler, finish, on_finish=None):
    return trb.RunnerSpec(function_names=[FN], handler=handler, finish=finish,
                          on_finish=on_finish or MagicMock())


def _returns(_task):
    return None


def _sets_result(task):
    trb.set_result(task, "all moved")


def _aborts(_task):
    raise trb.TaskAbort("volume gone")


@pytest.mark.parametrize("seed,handler,ending,result", [
    pytest.param({}, _returns, trb.Ending.SUCCEEDED, "completed", id="returns"),
    pytest.param({}, _sets_result, trb.Ending.SUCCEEDED, "all moved", id="set_result"),
    pytest.param({}, _aborts, trb.Ending.ABORTED, "volume gone", id="abort"),
    pytest.param({"canceled": True}, None, trb.Ending.CANCELED, "canceled", id="canceled"),
    pytest.param({"retry": 3, "result": "boom"}, None, trb.Ending.GAVE_UP,
                 "max retry reached (3) after: boom", id="gave_up"),
])
def test_finish_is_told_why_the_task_ends(seed, handler, ending, result):
    task = _seed(**seed)
    reached = MagicMock(side_effect=handler)
    calls = []

    def finish(t, why, message):
        calls.append((t.uuid, why, message))
        with pytest.raises(FrozenTaskError):
            t.status = JobSchedule.STATUS_DONE
        return trb.finish_done(t, message)

    stored = drive(_spec(reached, finish), task)

    assert calls == [(task.uuid, ending, result)]
    assert reached.called is (handler is not None)
    assert stored.status == JobSchedule.STATUS_DONE
    assert stored.function_result == result


def test_a_hook_commits_done_with_domain_state_in_one_transaction():
    task = _seed()
    indexes = JobSchedule.active_indexes(db.kv_store)

    @fdb.transactional
    def _finish_tx(tr, key, result):
        fresh = JobSchedule().from_dict(json.loads(bytes(tr.get(key).wait())))
        if fresh.status == JobSchedule.STATUS_DONE:
            return None
        fresh.status = JobSchedule.STATUS_DONE
        fresh.function_result = result
        db._put_tx(tr, fresh, indexes)
        tr[DOMAIN_KEY] = result.encode()
        return fresh

    def finish(t, _why, message):
        return _finish_tx(db.kv_store, t.get_db_id().encode(), message)

    seen = []

    def on_finish(t):
        seen.append((t.uuid, t.status, db.get_task_by_id(t.uuid).status,
                     db.kv_store.get(DOMAIN_KEY)))

    stored = drive(_spec(_returns, finish, on_finish), task)

    assert stored.status == JobSchedule.STATUS_DONE
    assert stored.function_result == "completed"
    assert db.kv_store.get(DOMAIN_KEY) == b"completed"
    assert seen == [(task.uuid, JobSchedule.STATUS_DONE, JobSchedule.STATUS_DONE, b"completed")]


def test_a_hook_that_commits_nothing_leaves_the_task_running():
    task = _seed()
    on_finish = MagicMock()

    stored = drive(_spec(_returns, lambda *_: None, on_finish), task)

    assert stored.status == JobSchedule.STATUS_RUNNING
    on_finish.assert_not_called()


def _suspend(t):
    t.status = JobSchedule.STATUS_SUSPENDED
    t.function_result = "re-armed"


def test_a_re_armed_task_is_dispatched_again_at_once(cluster):
    task = _seed()
    handler = MagicMock(return_value=None)
    on_finish = MagicMock()
    endings = []

    def finish(t, why, message):
        endings.append(why)
        if len(endings) == 1:
            return db.atomic_update(t, _suspend)
        return trb.finish_done(t, message)

    runner = trb.TaskRunner(_spec(handler, finish, on_finish))
    # An expired backoff entry: the re-arm must forget it, not keep it.
    runner._next_attempt[task.uuid] = 0
    try:
        runner._dispatch(db.get_task_by_id(task.uuid), cluster, [])

        rearmed = db.get_task_by_id(task.uuid)
        assert rearmed.status == JobSchedule.STATUS_SUSPENDED
        assert rearmed.function_result == "re-armed"
        assert rearmed.retry == 0
        on_finish.assert_not_called()
        assert task.uuid not in runner._next_attempt
        assert task.uuid not in runner._inflight

        runner._dispatch(rearmed, cluster, [])
    finally:
        runner._executor.shutdown(wait=True)

    assert handler.call_count == 2
    assert endings == [trb.Ending.SUCCEEDED, trb.Ending.SUCCEEDED]
    assert db.get_task_by_id(task.uuid).status == JobSchedule.STATUS_DONE
    on_finish.assert_called_once()


def test_a_raising_hook_propagates_and_leaves_the_task_open(cluster):
    task = _seed()
    on_finish = MagicMock()

    def finish(*_):
        raise RuntimeError("terminal commit failed")

    runner = trb.TaskRunner(_spec(_returns, finish, on_finish))
    try:
        with pytest.raises(RuntimeError, match="terminal commit failed"):
            runner._process(db.get_task_by_id(task.uuid), cluster, [])
    finally:
        runner._executor.shutdown(wait=True)

    stored = db.get_task_by_id(task.uuid)
    assert stored.status == JobSchedule.STATUS_RUNNING
    assert stored.retry == 0
    on_finish.assert_not_called()


def test_finish_done_refuses_a_done_task():
    task = _seed(status=JobSchedule.STATUS_DONE, result="earlier")

    assert trb.finish_done(task, "later") is None

    stored = db.get_task_by_id(task.uuid)
    assert stored.status == JobSchedule.STATUS_DONE
    assert stored.function_result == "earlier"


def test_finish_done_finishes_a_canceled_task():
    task = _seed(status=JobSchedule.STATUS_RUNNING, canceled=True)

    committed = trb.finish_done(task, "canceled")

    assert committed.status == JobSchedule.STATUS_DONE
    stored = db.get_task_by_id(task.uuid)
    assert (stored.status, stored.function_result, stored.canceled) == (
        JobSchedule.STATUS_DONE, "canceled", True)


def test_without_a_hook_the_driver_commits_done_itself():
    task = _seed()
    on_finish = MagicMock()

    stored = drive(_spec(_returns, None, on_finish), task)

    assert stored.status == JobSchedule.STATUS_DONE
    assert stored.function_result == "completed"
    on_finish.assert_called_once()
    assert on_finish.call_args.args[0].status == JobSchedule.STATUS_DONE
