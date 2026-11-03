"""The sync-replication resync runner against the real FoundationDB. A zone
desync recorded through the real event path queues the LVS's catch-up. The
runner waits until the lagging zone is up, then starts the catch-up
(``distr_migration_replica_sync_start``) on whichever member of the active
triplet leads the LVS, and polls it. A start the data plane answers
EALREADY, EBUSY or EAGAIN is judged per answer; an open device migration on
the leader's node holds the start back. It finishes when every started
catch-up has ended and the leader reports ``synced`` for every distrib; the
runner then resolves the zone events. Otherwise it runs the catch-up again
with backoff, but never while the site is lost, the zone is down or the
leadership is moving; a new leader restarts it without counting a run. The
runner is driven through the shared task driver:
``task_driver.drive`` for one pass, or one driver held across dispatches where
its in-memory backoff is the subject. Storage nodes (RPC) are mocked; the
database never is. The pure rules are in
tests/unit/tasks/test_tasks_runner_sync_resync.py.
"""
import errno
import itertools
import threading
import types
import uuid
from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.rpc_client import RPCConnectionError, RPCException, RPCRemoteError
from simplyblock_core.services import task_runner_base as trb
from simplyblock_core.services import tasks_runner_sync_resync as runner
from simplyblock_core.utils import ttl_cache
from tests.integration import task_driver
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs
env = stack.env

DISTRIBS = ["distrib_11", "distrib_12"]
ERROR_REPLICA_SYNC = 1 << 11
ERROR_SHUTDOWN = 1 << 8


class _Clock:
    """The runner's (and a held driver's) ``time``: a settable ``time()``, and
    ``sleep`` that must never be reached by a pass (a pass never sleeps in
    place)."""

    def __init__(self):
        self.now = 1_000_000.0

    def time(self):
        return self.now

    def sleep(self, _seconds):
        raise AssertionError("the runner slept in place")


@pytest.fixture()
def clock():
    c = _Clock()
    with patch.object(runner, "time", types.SimpleNamespace(time=c.time, sleep=c.sleep)):
        yield c


@pytest.fixture()
def held(db, clock):
    """ONE driver over the test clock, as ``serve`` holds it: its in-memory
    backoff applies between dispatches. Calling it dispatches the task through
    the driver's own ``_dispatch`` and returns the task as stored."""
    driver = trb.TaskRunner(runner.SPEC)

    def dispatch(task):
        fresh = db.get_task_by_id(task.uuid)
        driver._dispatch(fresh, db.get_cluster_by_id(fresh.cluster_id), [])
        return db.get_task_by_id(task.uuid)

    dispatch.driver = driver
    try:
        with patch.object(trb, "time", types.SimpleNamespace(time=clock.time, sleep=clock.sleep)):
            yield dispatch
    finally:
        driver._executor.shutdown(wait=True)


@pytest.fixture()
def alerts():
    with patch.object(runner.events_controller, "log_event_cluster") as log:
        yield log


_ts_seq = iter(range(1000000))


def _desync(node, vuid=11, status="secondary_zone_unavailable"):
    """A zone desync event of ``node``'s instance, through the real path."""
    return src.record_sync_event(node.get_id(), {
        "timestamp": f"2026-09-30T11:00:00.{next(_ts_seq):06d}Z", "event_type": "sync_replication_status",
        "status": status, "vuid": vuid})


def _layout(db, *, active_site=""):
    """a0 owns LVS_1: home triplet a0 a1 a2, remote triplet b0 b1 b2."""
    cluster = stack._seed_cluster(db)
    a, b = stack._sites(db, cluster)
    owner = stack._make_owner(db, a[0], a, b, lvs_id=1)
    if active_site:
        owner = stack._update(db, owner, lvs_active_site=active_site)
    return cluster, a, b, owner


def _task(db, cluster):
    tasks = db.get_active_sync_resync_tasks(cluster.get_id())
    assert len(tasks) == 1, tasks
    return tasks[0]


def _fresh_task(db, task):
    return db.get_task_by_id(task.uuid)


IN_PROGRESS = "Migration is already in progress"


class _Catchup:
    """The catch-up as the leader's data plane runs and reports it: one
    migration slot per distrib (its latest migration), a start answered
    EBUSY while that slot is active, EALREADY for a distrib in ``in_sync``,
    and the sync status of each distrib (``sync_status``, or ``sync_of``
    per name)."""

    def __init__(self, rpc):
        self.rpc = rpc
        self.migration = {name: {"name": name, "status": "none"} for name in DISTRIBS}
        self.sync_status = "synced"
        self.sync_of = {}
        self.in_sync = set()
        rpc.distr_migration_replica_sync_start.side_effect = self._start
        rpc.distr_migration_status.side_effect = lambda name: [self.migration[name]]
        rpc.distr_sync_replication_status.side_effect = \
            lambda name=None: [{"name": name, "status": self.sync_of.get(name, self.sync_status)}]

    def _start(self, name, *args, **kwargs):
        if self.migration[name]["status"] in runner.MIGRATION_ACTIVE_STATES:
            raise RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY)
        if name in self.in_sync:
            raise RPCRemoteError(message="zones are in sync, no resync needed", code=errno.EALREADY)
        self.migration[name] = {"name": name, "status": "running", "error": 0, "progress": 0, "type": 3}
        return True

    def complete(self, error=0, name=None):
        for n in [name] if name else DISTRIBS:
            self.migration[n] = {"name": n, "status": "completed", "error": error, "progress": 100,
                                 "type": 3}

    def starts(self):
        return [c.args[0] for c in self.rpc.distr_migration_replica_sync_start.call_args_list]


def _pass(task):
    """One driver pass over ``task`` (a new driver: no backoff); the task as
    stored afterwards."""
    return task_driver.drive(runner.SPEC, task)


def _row(task):
    return (task.status, task.function_result, task.retry, task.owner, task.updated_at)


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

class TestConvergence:

    def test_a_catch_up_on_the_leader_resolves_the_desync(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        _desync(b[1], vuid=12)
        src.record_sync_event(a[0].get_id(), {
            "timestamp": "2026-09-30T11:00:01.000000Z", "event_type": "sync_replication_status",
            "status": "remote_journal_unsynced", "jm_vuid": 1})
        task = _task(db, cluster)

        started = _pass(task)
        assert catchup.starts() == DISTRIBS
        assert started.status == JobSchedule.STATUS_RUNNING
        assert started.function_params["resync"]["leader_id"] == a[0].get_id()

        clock.now += 30
        assert _pass(task).status == JobSchedule.STATUS_RUNNING     # still running

        catchup.complete()
        clock.now += 12
        done = _pass(task)
        assert (done.status, done.function_result) == (
            JobSchedule.STATUS_DONE, f"synced on {a[0].get_id()}; catch-up took 42.0s")
        assert done.function_params["duration_sec"] == 42
        assert "resync" not in done.function_params
        assert "converged" not in done.function_params
        assert catchup.starts() == DISTRIBS              # never re-started
        # the zone desyncs are over; the journal drop is not the catch-up's business
        events = db.get_sync_replication_events(cluster.get_id(), "LVS_1")
        assert {(e.kind, e.resolved) for e in events} == {
            (SyncReplicationEvent.KIND_ZONE_UNAVAILABLE, True),
            (SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED, False)}
        # nothing else was asked of any other node
        for node in (*a[1:], *b):
            rpcs[node.get_id()].distr_migration_replica_sync_start.assert_not_called()

    def test_a_remote_led_lvs_catches_up_on_the_remote_primary(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[0])
        catchup = _Catchup(rpcs[b[0].get_id()])
        _desync(b[0], status="primary_zone_unavailable")
        task = _task(db, cluster)
        _pass(task)
        catchup.complete()
        done = _pass(task)
        assert done.status == JobSchedule.STATUS_DONE
        assert done.function_result.startswith(f"synced on {b[0].get_id()}; ")
        assert catchup.starts() == DISTRIBS
        for node in (*a, *b[1:]):
            rpcs[node.get_id()].distr_migration_replica_sync_start.assert_not_called()

    def test_a_catch_up_already_in_progress_is_adopted(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        catchup.migration["distrib_11"] = {"name": "distrib_11", "status": "running", "error": 0,
                                           "type": 3}
        _desync(a[0])
        task = _task(db, cluster)
        adopted = _pass(task)
        assert (adopted.status, adopted.retry) == (JobSchedule.STATUS_RUNNING, 0)
        assert adopted.function_params["resync"]["distribs"] == DISTRIBS
        assert catchup.starts() == DISTRIBS                # distrib_11 answered EBUSY
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE

    def test_a_failed_terminal_commit_is_closed_by_the_next_pass(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        clock.now += 7
        real_finish = DBController.finish_sync_resync
        calls = []

        def finish(self, *args, **kwargs):
            calls.append(args)
            if len(calls) == 1:
                raise RuntimeError("commit lost")
            return real_finish(self, *args, **kwargs)

        with patch.object(DBController, "finish_sync_resync", finish):
            with pytest.raises(RuntimeError, match="commit lost"):
                _pass(task)
            still = _fresh_task(db, task)
            assert (still.status, still.retry) == (JobSchedule.STATUS_RUNNING, 0)
            assert {"resync", "converged"} <= set(still.function_params)
            assert db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")
            done = _pass(task)                            # the same clock: the same duration
        assert len(calls) == 2
        assert (done.status, done.function_result) == (
            JobSchedule.STATUS_DONE, f"synced on {a[0].get_id()}; catch-up took 7.0s")
        assert not {"resync", "converged"} & set(done.function_params)
        assert not db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")
        assert catchup.starts() == DISTRIBS


def _refused(rpc, catchup, error):
    """The start of distrib_11 raises ``error``, the others go to the model."""
    def _start(name, *args, **kwargs):
        if name == "distrib_11":
            raise error
        return catchup._start(name)
    rpc.distr_migration_replica_sync_start.side_effect = _start


def _on_leader(db, rpcs):
    """a0 leads LVS_1 and its replica zone is behind; the catch-up task."""
    cluster, a, b, _ = _layout(db)
    rpcs.lead(a[0])
    rpc = rpcs[a[0].get_id()]
    catchup = _Catchup(rpc)
    _desync(a[0])
    return a, rpc, catchup, _task(db, cluster)


def _element(name, status, mtype, error=0):
    return {"name": name, "status": status, "error": error, "type": mtype}


class TestRefusedStart:
    """A start the leader does not answer ``true``, per answer: EBUSY probes
    the slot (a catch-up there is adopted, another migration is waited for),
    EAGAIN waits, every other answer fails the run without a probe."""

    def _failed(self, db, task, text):
        fresh = _fresh_task(db, task)
        assert (fresh.status, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "resync" not in fresh.function_params
        assert text in fresh.function_result
        return fresh

    def _waiting(self, db, task, result):
        fresh = _fresh_task(db, task)
        assert (fresh.status, fresh.retry, fresh.function_result) == (JobSchedule.STATUS_SUSPENDED, 0, result)
        assert "resync" not in fresh.function_params
        return fresh

    @pytest.mark.parametrize("status", ["running", "completed"])
    def test_ebusy_adopts_the_catch_up_in_the_slot(self, db, rpcs, clock, status):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.migration["distrib_11"] = _element("distrib_11", status, 3)
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY))
        adopted = _pass(task)
        assert (adopted.status, adopted.retry) == (JobSchedule.STATUS_RUNNING, 0)
        assert adopted.function_params["resync"]["distribs"] == DISTRIBS
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE

    def test_an_adopted_catch_up_with_an_error_is_a_non_converging_run(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.migration["distrib_11"] = _element("distrib_11", "completed", 3, error=ERROR_REPLICA_SYNC)
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY))
        assert _pass(task).status == JobSchedule.STATUS_RUNNING
        catchup.complete(name="distrib_12")
        _pass(task)
        self._failed(db, task, "distrib_11: catch-up ended with")

    @pytest.mark.parametrize("state", runner.MIGRATION_ACTIVE_STATES)
    @pytest.mark.parametrize("mtype", [1, 2])
    def test_ebusy_waits_while_another_migration_holds_the_slot(self, db, rpcs, clock, held, mtype, state):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.migration["distrib_11"] = _element("distrib_11", state, mtype)
        held(task)
        self._waiting(db, task, f"distrib_11: migration slot held by a type {mtype} migration "
                                f"on {a[0].get_id()}")
        assert task.uuid not in held.driver._next_attempt
        assert catchup.starts() == ["distrib_11"]

        # the holder ended: the next dispatch (the same clock) starts the catch-up
        catchup.migration["distrib_11"] = _element("distrib_11", "completed", mtype)
        assert held(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == ["distrib_11", *DISTRIBS]

    @pytest.mark.parametrize("element", [{"name": "distrib_11", "status": "none"},
                                         _element("distrib_11", "completed", 2)],
                             ids=["none", "completed-type-2"])
    def test_ebusy_with_the_holder_just_ended_waits_one_pass(self, db, rpcs, clock, element):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.migration["distrib_11"] = element
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY))
        _pass(task)
        self._waiting(db, task, f"distrib_11: migration slot just freed on {a[0].get_id()}")

    @pytest.mark.parametrize("answer", ["garbage", [], [{"name": "distrib_99", "status": "none"}]],
                             ids=["not-a-list", "empty", "other-distrib"])
    def test_ebusy_with_no_migration_in_the_probe_fails_the_run(self, db, rpcs, clock, answer):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY))
        rpc.distr_migration_status.side_effect = lambda name: answer
        _pass(task)
        self._failed(db, task, "distrib_11: status probe on")

    def test_ebusy_with_a_failed_status_probe_fails_the_run(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=errno.EBUSY))
        rpc.distr_migration_status.side_effect = RPCConnectionError("down")
        _pass(task)
        self._failed(db, task, "distrib_11: status probe failed")

    def test_eagain_waits(self, db, rpcs, clock, held):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _refused(rpc, catchup, RPCRemoteError(message="sync status unknown", code=errno.EAGAIN))
        held(task)
        self._waiting(db, task, f"distrib_11: sync status not loaded yet on {a[0].get_id()}")
        assert task.uuid not in held.driver._next_attempt
        rpc.distr_migration_status.assert_not_called()

    @pytest.mark.parametrize("code", [errno.ENOTSUP, errno.ENODEV, errno.ENOMEM, -32603, -errno.EBUSY])
    def test_any_other_error_code_fails_the_run_without_a_probe(self, db, rpcs, clock, code):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _refused(rpc, catchup, RPCRemoteError(message="refused", code=code))
        _pass(task)
        self._failed(db, task, f"distrib_11: start refused on {a[0].get_id()}: refused (code {code})")
        rpc.distr_migration_status.assert_not_called()

    def test_a_transport_error_fails_the_run_without_a_probe(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _refused(rpc, catchup, RPCConnectionError("down"))
        _pass(task)
        self._failed(db, task, "distrib_11: start failed")
        rpc.distr_migration_status.assert_not_called()

    @pytest.mark.parametrize("answer", [False, None, 1, {"ok": 1}])
    def test_an_answer_other_than_true_fails_the_run(self, db, rpcs, clock, answer):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        rpc.distr_migration_replica_sync_start.side_effect = lambda *args, **kwargs: answer
        _pass(task)
        self._failed(db, task, f"distrib_11: start refused on {a[0].get_id()}: answered {answer!r}")


# ---------------------------------------------------------------------------
# non-converging runs: suspended, retried with backoff, never finished
# ---------------------------------------------------------------------------

def _started(db, rpcs, clock, **layout):
    cluster, a, b, owner = _layout(db, **layout)
    rpcs.lead(a[0])
    catchup = _Catchup(rpcs[a[0].get_id()])
    _desync(a[0])
    task = _task(db, cluster)
    _pass(task)
    assert catchup.starts() == DISTRIBS
    return cluster, a, b, owner, catchup, task


class TestRerun:

    def test_bit_11_suspends_and_re_runs_after_the_backoff(self, db, rpcs, clock, held):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete(error=ERROR_REPLICA_SYNC, name="distrib_12")
        catchup.complete(name="distrib_11")
        failed_at = clock.now
        failed = held(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "resync" not in failed.function_params
        assert f"re-run in {runner.retry_delay(1)}s" in failed.function_result

        # dispatches before the backoff ends do not reach the handler
        for offset in (5, runner.retry_delay(1) - 1):
            clock.now = failed_at + offset
            assert _row(held(task)) == _row(failed)
        assert catchup.starts() == DISTRIBS

        clock.now = failed_at + runner.retry_delay(1)
        assert held(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == DISTRIBS * 2
        catchup.complete()
        assert held(task).status == JobSchedule.STATUS_DONE

    def test_the_backoff_doubles_per_non_converging_run_up_to_the_cap(self, db, rpcs, clock, held):
        base, cap = constants.SYNC_RESYNC_RETRY_BASE_SEC, constants.SYNC_RESYNC_RETRY_MAX_SEC
        expected = [min(base * 2 ** k, cap) for k in range(6)]
        assert expected[-1] == cap > expected[-2]
        _, _, _, _, catchup, task = _started(db, rpcs, clock)
        for runs, delay in enumerate(expected, start=1):
            catchup.complete(error=1)
            failed_at = clock.now
            failed = held(task)
            assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, runs)
            clock.now = failed_at + delay - 1
            assert _row(held(task)) == _row(failed)       # still backing off
            clock.now = failed_at + delay
            assert held(task).status == JobSchedule.STATUS_RUNNING    # the re-run starts
            assert catchup.starts() == DISTRIBS * (runs + 1)

    @pytest.mark.parametrize("error", [ERROR_SHUTDOWN, 1, 1 << 3])
    def test_a_shutdown_or_io_error_waits_for_the_zone(
            self, db, rpcs, clock, error):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        # the lagging (replica, site B) zone is lost again: the data plane
        # stops the catch-up toward it
        dev = b[2].nvme_devices[0]
        stack._update(db, b[2], nvme_devices=[_with_status(dev, NVMeDevice.STATUS_UNAVAILABLE)])
        catchup.complete(error=error)
        assert _pass(task).status == JobSchedule.STATUS_SUSPENDED

        for _ in range(3):
            waiting = _pass(task)
        assert waiting.status == JobSchedule.STATUS_SUSPENDED
        assert waiting.retry == 1                         # waiting is not a run
        assert dev.get_id() in waiting.function_result
        assert catchup.starts() == DISTRIBS

        stack._update(db, b[2], nvme_devices=[_with_status(dev, NVMeDevice.STATUS_ONLINE)])
        _pass(task)
        assert catchup.starts() == DISTRIBS * 2
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE

    def test_completed_but_not_synced_is_not_done(self, db, rpcs, clock):
        _, _, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        catchup.sync_status = "replica_unsynced"
        failed = _pass(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "converged" not in failed.function_params

    def test_a_poll_error_or_lost_migration_is_non_converging(self, db, rpcs, clock):
        _, a, _, _, catchup, task = _started(db, rpcs, clock)
        rpc = rpcs[a[0].get_id()]
        rpc.distr_migration_status.side_effect = RPCException("connection error")
        assert _pass(task).retry == 1
        rpc.distr_migration_status.side_effect = lambda name: [catchup.migration[name]]
        rerun = _pass(task)                               # the re-run adopts the running catch-ups
        assert rerun.function_params["resync"]["distribs"] == DISTRIBS
        catchup.migration = {n: {"name": n, "status": "none"} for n in DISTRIBS}
        assert _pass(task).retry == 2                     # its migration is gone (restart)

    def test_shutdown_on_the_same_leader_is_a_non_converging_run(self, db, rpcs, clock):
        _, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete(error=ERROR_SHUTDOWN)
        failed = _pass(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "catch-up ended with" in failed.function_result

    def test_a_leadership_change_restarts_on_the_new_leader(self, db, rpcs, clock, held):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        old = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)
        assert held(task).function_params["resync"]["leader_id"] == a[0].get_id()

        rpcs[a[0].get_id()].get_lvstore.return_value = {"lvs leadership": False}
        rpcs.lead(a[1])
        ttl_cache.leader_cache.invalidate()
        new = _Catchup(rpcs[a[1].get_id()])
        moved = held(task)
        assert (moved.status, moved.retry, moved.function_result) == (
            JobSchedule.STATUS_SUSPENDED, 0,
            (f"leadership moved from {a[0].get_id()} to {a[1].get_id()} during the catch-up; "
             f"restarting on {a[1].get_id()}"))
        assert "resync" not in moved.function_params
        assert task.uuid not in held.driver._next_attempt

        restarted = held(task)                            # the same clock: no backoff
        assert restarted.status == JobSchedule.STATUS_RUNNING
        assert restarted.function_params["resync"]["leader_id"] == a[1].get_id()
        assert (old.starts(), new.starts()) == (DISTRIBS, DISTRIBS)
        new.complete()
        done = held(task)
        assert (done.status, done.retry) == (JobSchedule.STATUS_DONE, 0)
        assert done.function_result.startswith(f"synced on {a[1].get_id()}; ")

    def test_one_alert_after_n_non_converging_runs(self, db, rpcs, clock, alerts):
        with patch.object(runner.constants, "SYNC_RESYNC_ALERT_RUNS", 3):
            _, _, _, owner, catchup, task = _started(db, rpcs, clock)
            for run in range(1, 6):
                catchup.complete(error=ERROR_REPLICA_SYNC)
                assert _pass(task).retry == run
                assert len(_alerts(alerts)) == (0 if run < 3 else 1)
                _pass(task)                               # the re-run starts
        kwargs = _alerts(alerts)[0].kwargs
        assert (kwargs["event_level"], kwargs["node_id"]) == ("Error", owner.get_id())
        assert _fresh_task(db, task).status != JobSchedule.STATUS_DONE

    def test_a_failed_alert_write_is_retried_next_run(self, db, rpcs, clock, alerts):
        failures = []

        def log(*args, **kwargs):
            if kwargs.get("event") == "SYNC_RESYNC_NOT_CONVERGING" and not failures:
                failures.append(kwargs)
                raise RuntimeError("event log write failed")
        alerts.side_effect = log
        with patch.object(runner.constants, "SYNC_RESYNC_ALERT_RUNS", 1):
            _, _, _, _, catchup, task = _started(db, rpcs, clock)
            catchup.complete(error=ERROR_REPLICA_SYNC)
            # the write error is the driver's: a failed attempt, the run counted
            failed = _pass(task)
            assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
            assert "event log write failed" in failed.function_result
            assert "alerted" not in failed.function_params
            assert "resync" not in failed.function_params
            _pass(task)                                   # the re-run starts
            catchup.complete(error=ERROR_REPLICA_SYNC)
            _pass(task)
        assert len(failures) == 1
        assert len(_alerts(alerts)) == 2                   # the failed one, then the written one
        assert _fresh_task(db, task).function_params["alerted"] is True


def _alerts(log_event_cluster):
    """The resync alerts among the cluster events logged (other code logs its
    own through the same function)."""
    return [c for c in log_event_cluster.call_args_list
            if c.kwargs.get("event") == "SYNC_RESYNC_NOT_CONVERGING"]


class TestAlertOnce:
    """The one alert path of both resync alerts (non-converging runs and a
    long hold): each alert has its own flag on the task."""

    FLAGS = ("alerted", "held_alerted")

    @pytest.mark.parametrize("flag", FLAGS)
    def test_the_other_flag_does_not_suppress_the_alert(self, db, rpcs, alerts, flag):
        a, _, _, task = _on_leader(db, rpcs)
        [other] = [f for f in self.FLAGS if f != flag]
        task = trb.checkpoint(task, **{other: True})
        owner = db.get_storage_node_by_id(task.node_id)

        runner._alert_once(task, owner, flag, "the message")
        [alert] = _alerts(alerts)
        assert (alert.kwargs["message"], alert.kwargs["event_level"], alert.kwargs["node_id"]) == (
            "the message", "Error", a[0].get_id())
        stored = _fresh_task(db, task)
        assert (stored.function_params[flag], stored.function_params[other]) == (True, True)

        runner._alert_once(stored, owner, flag, "the message")
        assert len(_alerts(alerts)) == 1

    def test_a_cancel_during_the_alert_write_aborts_without_the_flag(self, db, rpcs, alerts):
        _, _, _, task = _on_leader(db, rpcs)
        owner = db.get_storage_node_by_id(task.node_id)
        alerts.side_effect = lambda *args, **kwargs: tasks_controller.cancel_task(task.uuid)
        with pytest.raises(trb.TaskAbort, match="canceled"):
            runner._alert_once(task, owner, "alerted", "the message")
        assert len(_alerts(alerts)) == 1
        assert "alerted" not in _fresh_task(db, task).function_params


def _with_status(dev, status):
    dev = NVMeDevice(dev.to_dict())
    dev.status = status
    return dev


# ---------------------------------------------------------------------------
# waits: nothing started, no run counted
# ---------------------------------------------------------------------------

class TestWaits:

    def _assert_waiting(self, db, rpcs, task, a, b, text):
        for _ in range(3):
            waiting = _pass(task)
        assert (waiting.status, waiting.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert text in waiting.function_result
        for node in (*a, *b):
            rpcs[node.get_id()].distr_migration_replica_sync_start.assert_not_called()

    # "" is a record without a state; draining and down are the storage-node
    # monitor's record (no fail-over).
    @pytest.mark.parametrize("state", ["", Cluster.LOST_SITE_DRAINING, Cluster.LOST_SITE_DOWN])
    def test_while_a_site_is_lost(self, db, rpcs, clock, state):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.lost_site, cluster.lost_site_state = SITE_B, state
        cluster.write_to_db(db.kv_store)
        task = _task(db, cluster)
        self._assert_waiting(db, rpcs, task, a, b, f"site {SITE_B} is lost")
        cluster.lost_site, cluster.lost_site_state = "", ""
        cluster.write_to_db(db.kv_store)
        _pass(task)
        rpcs[a[0].get_id()].distr_migration_replica_sync_start.assert_called()

    def test_while_a_lagging_zone_node_is_offline(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])                                     # the replica zone (site B) is behind
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        task = _task(db, cluster)
        self._assert_waiting(db, rpcs, task, a, b, f"node:{b[0].get_id()}")
        stack._update(db, b[0], status=StorageNode.STATUS_ONLINE)
        _pass(task)
        rpcs[a[0].get_id()].distr_migration_replica_sync_start.assert_called()

    def test_a_wait_consumes_no_retry_and_no_backoff(self, db, rpcs, clock, held):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])                                     # the replica zone (site B) is behind
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        task = _task(db, cluster)
        waiting = held(task)
        assert (waiting.status, waiting.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert task.uuid not in held.driver._next_attempt
        stack._update(db, b[0], status=StorageNode.STATUS_ONLINE)
        assert held(task).status == JobSchedule.STATUS_RUNNING    # the same clock
        assert catchup.starts() == DISTRIBS

    def test_a_down_node_outside_the_lagging_zone_is_ignored(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])                                     # replica zone (B) behind
        stack._update(db, a[2], status=StorageNode.STATUS_OFFLINE)
        _pass(_task(db, cluster))
        assert catchup.starts() == DISTRIBS

    def test_while_the_leadership_is_moving(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db)
        rpcs.lead(a[0])
        _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        stack._update(db, owner, lvs_active_site=f"moving:{SITE_B}")
        task = _task(db, cluster)
        with patch.object(runner.storage_node_ops, "reconcile_lvs_move") as reconcile:
            self._assert_waiting(db, rpcs, task, a, b, "is moving")
        reconcile.assert_not_called()

    def test_while_no_leader_answers_in_the_active_triplet(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db)
        _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)
        with patch.object(runner.storage_node_ops, "find_leader_with_failover",
                          return_value=(None, [])):
            self._assert_waiting(db, rpcs, task, a, b, "no leader")

    def test_a_leaderless_lvs_waits_without_a_grant(self, db, rpcs, env, clock):
        # the real lookup: led from site B, nobody leads (as between a promote
        # and the first client IO), and a recovery WOULD grant on b0 - its
        # journal is ready and a grant on it takes
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        rpc = rpcs[b[0].get_id()]
        rpc.jc_get_jm_status.return_value = {"jm_x": True, "remote_jm_yn1": True}
        rpc.bdev_lvol_set_leader.side_effect = lambda *args, **kwargs: rpcs.lead(b[0])
        catchup = _Catchup(rpc)
        _desync(b[0], status="primary_zone_unavailable")
        task = _task(db, cluster)
        with patch.object(runner.storage_node_ops.time, "sleep"):
            self._assert_waiting(db, rpcs, task, a, b, "no leader")
        for node in (*a, *b):
            rpcs[node.get_id()].bdev_lvol_update_lvstore.assert_not_called()
            rpcs[node.get_id()].bdev_lvol_set_leader.assert_not_called()

        # the first IO made the active primary the leader: the next pass
        # starts the catch-up on it
        rpcs.lead(b[0])
        started = _pass(task)
        assert catchup.starts() == DISTRIBS
        assert started.status == JobSchedule.STATUS_RUNNING
        assert started.function_params["resync"]["leader_id"] == b[0].get_id()


# ---------------------------------------------------------------------------
# the executor: whichever member of the active triplet leads the LVS
# ---------------------------------------------------------------------------

class TestExecutor:

    def _assert_only_on(self, rpcs, a, b, executor):
        for node in (*a, *b):
            if node.get_id() != executor.get_id():
                rpcs[node.get_id()].distr_migration_replica_sync_start.assert_not_called()

    # The lagging zone is on the other site, not on the site of the acting
    # leader, so the lagging-zone check passes while the primary is down.
    @pytest.mark.parametrize("active_site, desync", [
        ("", "secondary_zone_unavailable"), (SITE_B, "primary_zone_unavailable")])
    @pytest.mark.parametrize("acting", [1, 2], ids=["secondary", "tertiary"])
    def test_an_acting_member_runs_the_catch_up(self, db, rpcs, clock, active_site, desync, acting):
        cluster, a, b, _ = _layout(db, active_site=active_site)
        triplet = b if active_site else a
        primary, member = triplet[0], triplet[acting]
        stack._update(db, primary, status=StorageNode.STATUS_OFFLINE)
        rpcs.lead(member)
        catchup = _Catchup(rpcs[member.get_id()])
        _desync(member, status=desync)
        task = _task(db, cluster)
        started = _pass(task)
        assert started.status == JobSchedule.STATUS_RUNNING
        assert started.function_params["resync"]["leader_id"] == member.get_id()
        assert catchup.starts() == DISTRIBS
        self._assert_only_on(rpcs, a, b, member)
        catchup.complete()
        done = _pass(task)
        assert done.status == JobSchedule.STATUS_DONE
        assert done.function_result.startswith(f"synced on {member.get_id()}; ")

    def test_an_active_triplet_without_a_primary_starts_on_the_leading_member(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        stack._update(db, owner, remote_primary_node_id="")
        rpcs.lead(b[1])
        catchup = _Catchup(rpcs[b[1].get_id()])
        _desync(b[1], status="primary_zone_unavailable")
        task = _task(db, cluster)
        started = _pass(task)
        assert started.function_params["resync"]["leader_id"] == b[1].get_id()
        assert catchup.starts() == DISTRIBS
        self._assert_only_on(rpcs, a, b, b[1])

    def test_site_a_returns_while_the_secondary_of_site_b_leads(self, db, rpcs, clock):
        """Site B leads, B's primary is offline and B's secondary leads; site A
        was lost and comes back. The catch-up runs on B's secondary only."""
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        rpcs.lead(b[1])
        catchup = _Catchup(rpcs[b[1].get_id()])
        _desync(b[1], status="primary_zone_unavailable")  # the home zone (site A) is behind
        for node in a[1:]:
            stack._update(db, node, status=StorageNode.STATUS_OFFLINE)
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.lost_site, cluster.lost_site_state = SITE_A, Cluster.LOST_SITE_DOWN
        cluster.write_to_db(db.kv_store)
        task = _task(db, cluster)
        assert f"site {SITE_A} is lost" in _pass(task).function_result

        cluster.lost_site, cluster.lost_site_state = "", ""
        cluster.write_to_db(db.kv_store)
        assert "waiting for the lagging zone(s)" in _pass(task).function_result
        for node in a[1:]:
            stack._update(db, node, status=StorageNode.STATUS_ONLINE)
        assert catchup.starts() == []

        clock.now += 1
        started = _pass(task)
        assert (started.status, started.retry) == (JobSchedule.STATUS_RUNNING, 0)
        assert catchup.starts() == DISTRIBS
        self._assert_only_on(rpcs, a, b, b[1])
        clock.now += 9
        catchup.complete()
        done = _pass(task)
        assert (done.status, done.function_result) == (
            JobSchedule.STATUS_DONE, f"synced on {b[1].get_id()}; catch-up took 9.0s")


# ---------------------------------------------------------------------------
# start answers over the LVS's distribs
# ---------------------------------------------------------------------------

class TestStartAnswers:

    def test_ealready_on_every_distrib_converges_without_a_poll(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.in_sync = set(DISTRIBS)
        started = _pass(task)
        assert started.status == JobSchedule.STATUS_RUNNING
        assert started.function_params["resync"]["distribs"] == []
        done = _pass(task)
        assert (done.status, done.function_result) == (
            JobSchedule.STATUS_DONE, f"synced on {a[0].get_id()}; catch-up took 0.0s")
        rpc.distr_migration_status.assert_not_called()
        assert sorted(c.args[0] for c in rpc.distr_sync_replication_status.call_args_list) == DISTRIBS

    def test_ealready_and_a_started_distrib(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.in_sync = {"distrib_11"}
        assert _pass(task).function_params["resync"]["distribs"] == ["distrib_12"]
        catchup.complete(name="distrib_12")
        assert _pass(task).status == JobSchedule.STATUS_DONE
        assert [c.args[0] for c in rpc.distr_migration_status.call_args_list] == ["distrib_12"]
        assert sorted(c.args[0] for c in rpc.distr_sync_replication_status.call_args_list) == DISTRIBS

    def test_the_final_check_covers_a_distrib_that_answered_ealready(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.in_sync = {"distrib_11"}
        _pass(task)
        catchup.complete(name="distrib_12")
        catchup.sync_of["distrib_11"] = "replica_unsynced"
        failed = _pass(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "distrib_11: replica_unsynced after the catch-up" in failed.function_result

    def test_a_partial_start_then_a_busy_slot_is_resumed_on_the_next_pass(self, db, rpcs, clock, held):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        catchup.migration["distrib_12"] = _element("distrib_12", "running", 2)
        waiting = held(task)
        assert (waiting.status, waiting.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert "resync" not in waiting.function_params
        assert catchup.starts() == DISTRIBS
        assert catchup.migration["distrib_11"]["type"] == 3      # distrib_11 did start

        catchup.migration["distrib_12"] = _element("distrib_12", "completed", 2)
        resumed = held(task)
        assert resumed.status == JobSchedule.STATUS_RUNNING
        assert resumed.function_params["resync"]["distribs"] == DISTRIBS
        assert catchup.starts() == DISTRIBS * 2                 # distrib_11 answered EBUSY: adopted
        catchup.complete()
        assert held(task).status == JobSchedule.STATUS_DONE


# ---------------------------------------------------------------------------
# the poll: another migration type in the slot ends the catch-up
# ---------------------------------------------------------------------------

class TestForeignPoll:

    @pytest.mark.parametrize("element", [("completed", 2), ("running", 1)], ids=["completed-2", "running-1"])
    def test_a_foreign_element_ends_the_catch_up_and_synced_converges(self, db, rpcs, clock, element):
        _, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.migration["distrib_11"] = _element("distrib_11", *element)
        catchup.complete(name="distrib_12")
        assert _pass(task).status == JobSchedule.STATUS_DONE

    def test_a_foreign_element_and_not_synced_is_a_non_converging_run(self, db, rpcs, clock):
        _, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.migration["distrib_11"] = _element("distrib_11", "completed", 2)
        catchup.complete(name="distrib_12")
        catchup.sync_status = "replica_unsynced"
        failed = _pass(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "converged" not in failed.function_params


# ---------------------------------------------------------------------------
# the reverse guard: an open device migration on the leader holds the start
# ---------------------------------------------------------------------------

_mig_dates = itertools.count(1)


def _mig(db, cluster, function_name, node, distr="distrib_11", *, lvs_name="LVS_1", owner_id="",
         started=False, canceled=False, marker=None):
    """An open device-migration task of ``distr`` on ``node``, as the
    migration runners keep it; ``started`` gives it the migration marker."""
    task = JobSchedule()
    task.uuid = str(uuid.uuid4())
    task.cluster_id = cluster.get_id()
    task.node_id = node.get_id()
    task.function_name = function_name
    task.function_params = {"distr_name": distr, "lvs_name": lvs_name, "lvs_owner": owner_id}
    if started:
        task.function_params["migration"] = {"name": distr} if marker is None else marker
    task.status = JobSchedule.STATUS_RUNNING if started else JobSchedule.STATUS_NEW
    task.canceled = canceled
    task.date = next(_mig_dates)
    task.max_retry = -1
    task.write_to_db(db.kv_store)
    return task


def _close(db, mig):
    def _apply(fresh):
        fresh.status = JobSchedule.STATUS_DONE
        return True
    db.atomic_update(db.get_task_by_id(mig.uuid), _apply)


def _holding(a, mig):
    return (f"waiting for open migration task {mig.uuid} ({mig.function_name}) of "
            f"{mig.function_params['distr_name']} on {a[0].get_id()} to finish")


class TestReverseGuard:

    @pytest.mark.parametrize("function_name, distr, kwargs", [
        (JobSchedule.FN_DEV_MIG, "distrib_11", {"started": True}),
        (JobSchedule.FN_NEW_DEV_MIG, "distrib_12", {"started": True}),
        (JobSchedule.FN_DEV_MIG, "distrib_11", {"started": True, "marker": {}}),
        (JobSchedule.FN_DEV_MIG, "distrib_11", {"started": True, "canceled": True}),
        (JobSchedule.FN_FAILED_DEV_MIG, "distrib_11", {}),
        (JobSchedule.FN_FAILED_DEV_MIG, "distrib_12", {"started": True}),
        (JobSchedule.FN_FAILED_DEV_MIG, "distrib_11", {"canceled": True}),
    ], ids=["started-dev", "started-new-dev", "falsy-marker", "canceled-started-dev",
            "failed-dev", "started-failed-dev", "canceled-failed-dev"])
    def test_a_holding_task_on_the_leader_defers_the_start(self, db, rpcs, clock, held,
                                                           function_name, distr, kwargs):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        mig = _mig(db, db.get_cluster_by_id(task.cluster_id), function_name, a[0], distr,
                   owner_id=a[0].get_id(), **kwargs)
        waiting = held(task)
        assert (waiting.status, waiting.retry, waiting.function_result) == (
            JobSchedule.STATUS_SUSPENDED, 0, _holding(a, mig))
        assert task.uuid not in held.driver._next_attempt
        assert catchup.starts() == []

    @pytest.mark.parametrize("function_name", [JobSchedule.FN_DEV_MIG, JobSchedule.FN_NEW_DEV_MIG])
    def test_a_not_started_re_balance_on_the_leader_does_not_hold(self, db, rpcs, clock, function_name):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        _mig(db, db.get_cluster_by_id(task.cluster_id), function_name, a[0], owner_id=a[0].get_id())
        assert _pass(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == DISTRIBS

    def test_tasks_on_the_offline_primary_do_not_hold_the_acting_leader(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db, active_site=SITE_B)
        stack._update(db, b[0], status=StorageNode.STATUS_OFFLINE)
        rpcs.lead(b[1])
        catchup = _Catchup(rpcs[b[1].get_id()])
        _desync(b[1], status="primary_zone_unavailable")
        _mig(db, cluster, JobSchedule.FN_DEV_MIG, b[0], owner_id=a[0].get_id(), started=True)
        _mig(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, b[0], "distrib_12", owner_id=a[0].get_id())
        started = _pass(_task(db, cluster))
        assert started.function_params["resync"]["leader_id"] == b[1].get_id()
        assert catchup.starts() == DISTRIBS

    def test_tasks_of_another_lvs_do_not_hold(self, db, rpcs, clock):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        cluster = db.get_cluster_by_id(task.cluster_id)
        _mig(db, cluster, JobSchedule.FN_DEV_MIG, a[0], "distrib_21", lvs_name="LVS_2", started=True)
        _mig(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[0], "distrib_11", lvs_name="LVS_2")
        assert _pass(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == DISTRIBS


class TestHeld:
    """The held state of a catch-up the reverse guard holds back: the holder
    and the time of the first hold, one alert after SYNC_RESYNC_HELD_ALERT_SEC."""

    def _held(self, db, rpcs, function_name=JobSchedule.FN_DEV_MIG, started=True):
        a, rpc, catchup, task = _on_leader(db, rpcs)
        cluster = db.get_cluster_by_id(task.cluster_id)
        mig = _mig(db, cluster, function_name, a[0], owner_id=a[0].get_id(), started=started)
        return a, cluster, catchup, task, mig

    def test_a_hold_records_the_holder_and_its_start(self, db, rpcs, clock, alerts):
        a, _, _, task, mig = self._held(db, rpcs)
        waiting = _pass(task)
        assert waiting.function_params["held_by"] == {
            "task": mig.uuid, "function": JobSchedule.FN_DEV_MIG, "node": a[0].get_id(), "since": clock.now}
        assert "held_alerted" not in waiting.function_params
        assert _alerts(alerts) == []

    def test_a_change_of_holder_keeps_the_start_of_the_hold(self, db, rpcs, clock, alerts):
        a, cluster, _, task, first = self._held(db, rpcs)
        since = clock.now
        _pass(task)
        _close(db, first)
        second = _mig(db, cluster, JobSchedule.FN_FAILED_DEV_MIG, a[0], "distrib_12", owner_id=a[0].get_id())
        clock.now += 100
        waiting = _pass(task)
        assert waiting.function_result == _holding(a, second)
        assert waiting.function_params["held_by"] == {
            "task": second.uuid, "function": JobSchedule.FN_FAILED_DEV_MIG, "node": a[0].get_id(),
            "since": since}

    def test_one_alert_once_the_hold_reaches_the_threshold(self, db, rpcs, clock, alerts):
        a, _, catchup, task, mig = self._held(db, rpcs)
        since = clock.now
        _pass(task)
        clock.now = since + constants.SYNC_RESYNC_HELD_ALERT_SEC - 1
        assert "held_alerted" not in _pass(task).function_params
        assert _alerts(alerts) == []

        clock.now = since + constants.SYNC_RESYNC_HELD_ALERT_SEC
        alerted = _pass(task)
        assert alerted.function_params["held_alerted"] is True
        assert (alerted.status, alerted.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        [alert] = _alerts(alerts)
        assert (alert.kwargs["event_level"], alert.kwargs["node_id"]) == ("Error", a[0].get_id())
        assert alert.kwargs["message"] == (
            f"LVS LVS_1: the zone catch-up has been held for {constants.SYNC_RESYNC_HELD_ALERT_SEC}s "
            f"by migration task {mig.uuid} ({JobSchedule.FN_DEV_MIG}) on {a[0].get_id()}")

        clock.now += 50
        _pass(task)
        assert len(_alerts(alerts)) == 1
        assert catchup.starts() == []

    def test_a_failed_alert_write_is_raised_again_on_the_next_pass(self, db, rpcs, clock, alerts):
        failures = []

        def log(*args, **kwargs):
            if kwargs.get("event") == "SYNC_RESYNC_NOT_CONVERGING" and not failures:
                failures.append(kwargs)
                raise RuntimeError("event log write failed")
        alerts.side_effect = log
        _, _, _, task, _ = self._held(db, rpcs)
        _pass(task)
        clock.now += constants.SYNC_RESYNC_HELD_ALERT_SEC
        failed = _pass(task)
        assert "event log write failed" in failed.function_result
        assert "held_alerted" not in failed.function_params
        assert _pass(task).function_params["held_alerted"] is True
        assert len(failures) == 1
        assert len(_alerts(alerts)) == 2                   # the failed one, then the written one

    def test_the_hold_ends_when_the_holder_closes(self, db, rpcs, clock, alerts):
        _, _, catchup, task, mig = self._held(db, rpcs, JobSchedule.FN_FAILED_DEV_MIG, started=False)
        _pass(task)
        clock.now += constants.SYNC_RESYNC_HELD_ALERT_SEC
        assert {"held_by", "held_alerted"} <= set(_pass(task).function_params)
        _close(db, mig)
        started = _pass(task)
        assert started.status == JobSchedule.STATUS_RUNNING
        assert not {"held_by", "held_alerted"} & set(started.function_params)
        assert catchup.starts() == DISTRIBS

    def test_a_held_state_left_by_a_leaderless_pass_is_cleared_before_the_start(self, db, rpcs, clock):
        _, _, catchup, task, mig = self._held(db, rpcs)
        _pass(task)
        with patch.object(runner.storage_node_ops, "find_leader_with_failover", return_value=(None, [])):
            waiting = _pass(task)
        assert "no leader" in waiting.function_result
        assert "held_by" in waiting.function_params
        _close(db, mig)
        started = _pass(task)
        assert started.status == JobSchedule.STATUS_RUNNING
        assert "held_by" not in started.function_params
        assert catchup.starts() == DISTRIBS


# ---------------------------------------------------------------------------
# DONE never loses a newer desync
# ---------------------------------------------------------------------------

class TestNewerDesync:

    def test_a_desync_in_the_final_check_keeps_the_task(self, db, rpcs, clock):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        before = db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")
        newer = []

        def status(name=None):
            if not newer:
                newer.append(_desync(b[0], vuid=12))
            return [{"name": name, "status": "synced"}]

        rpcs[a[0].get_id()].distr_sync_replication_status.side_effect = status
        again = _pass(task)                               # re-armed by the terminal commit
        assert (again.status, again.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert not {"resync", "converged"} & set(again.function_params)
        open_ids = {e.get_id() for e in db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")}
        assert open_ids == {newer[0].get_id()}
        assert not open_ids & {e.get_id() for e in before}
        assert len(db.get_active_sync_resync_tasks(cluster.get_id())) == 1

        # the new run converges and finishes
        rpcs[a[0].get_id()].distr_sync_replication_status.side_effect = \
            lambda name=None: [{"name": name, "status": "synced"}]
        assert _pass(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == DISTRIBS * 2
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE

    def test_a_desync_racing_the_finish_keeps_the_task(self, db, rpcs, clock):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        original = DBController._read_sync_state_tx
        armed = []
        newer = []

        def read_state(tr, key):
            state = original(tr, key)
            if armed:
                armed.clear()
                # another collector commits a desync while the finish
                # transaction is between its read and its commit
                t = threading.Thread(target=lambda: newer.append(_desync(b[0], vuid=12)))
                t.start()
                t.join()
            return state

        real_finish = DBController.finish_sync_resync

        def finish(self, *args, **kwargs):
            armed.append(True)
            return real_finish(self, *args, **kwargs)

        with patch.object(DBController, "_read_sync_state_tx", staticmethod(read_state)), \
                patch.object(DBController, "finish_sync_resync", finish):
            again = _pass(task)
        assert newer and newer[0] is not None
        assert (again.status, again.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert [e.get_id() for e in db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")] \
            == [newer[0].get_id()]
        # the desync did not get a second task: the running one covers it
        assert [t.uuid for t in db.get_active_sync_resync_tasks(cluster.get_id())] == [task.uuid]

    def test_a_desync_after_done_gets_a_new_task(self, db, rpcs, clock):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE
        _desync(b[0])
        tasks = db.get_active_sync_resync_tasks(cluster.get_id())
        assert len(tasks) == 1 and tasks[0].uuid != task.uuid


# ---------------------------------------------------------------------------
# task life cycle and the main loop
# ---------------------------------------------------------------------------

class TestLifecycle:

    def test_a_canceled_task_ends_without_touching_the_node(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        _desync(a[0])
        task = _task(db, cluster)
        db.atomic_update(task, lambda t: setattr(t, "canceled", True))
        ended = _pass(task)
        assert (ended.status, ended.function_result) == (JobSchedule.STATUS_DONE, "canceled")
        rpcs[a[0].get_id()].distr_migration_replica_sync_start.assert_not_called()

    def test_a_cancel_during_a_start_stops_further_starts(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)

        def start(name, *args, **kwargs):
            tasks_controller.cancel_task(task.uuid)       # while the RPC is in flight
            return catchup._start(name)

        rpcs[a[0].get_id()].distr_migration_replica_sync_start.side_effect = start
        ended = _pass(task)
        assert (ended.canceled, ended.status, ended.function_result) == (
            True, JobSchedule.STATUS_DONE, "canceled")
        assert catchup.starts() == ["distrib_11"]

    def test_a_cancel_during_a_status_poll_ends_the_task(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)

        def status(name):
            tasks_controller.cancel_task(task.uuid)
            return [catchup.migration[name]]

        rpcs[a[0].get_id()].distr_migration_status.side_effect = status
        # still running: the progress write is refused on the canceled row
        still = _pass(task)
        assert (still.canceled, still.status) == (True, JobSchedule.STATUS_RUNNING)
        ended = _pass(task)
        assert (ended.canceled, ended.status, ended.function_result) == (
            True, JobSchedule.STATUS_DONE, "canceled")
        assert catchup.starts() == DISTRIBS

    def test_a_cancel_during_a_failing_poll_ends_the_task(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)

        def status(name):
            tasks_controller.cancel_task(task.uuid)
            raise RPCException("connection error")

        rpcs[a[0].get_id()].distr_migration_status.side_effect = status
        ended = _pass(task)
        assert (ended.canceled, ended.status, ended.function_result, ended.retry) == (
            True, JobSchedule.STATUS_DONE, "canceled", 0)
        assert catchup.starts() == DISTRIBS

    def test_a_removed_owner_ends_the_task(self, db, rpcs, clock):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _task(db, cluster)
        stack._update(db, owner, status=StorageNode.STATUS_REMOVED)
        ended = _pass(task)
        assert ended.status == JobSchedule.STATUS_DONE
        assert "no longer owned" in ended.function_result

    def test_a_task_of_a_cluster_without_sync_replication_ends(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        _desync(a[0])
        task = _task(db, cluster)
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
        ended = _pass(task)
        assert (ended.status, ended.function_result) == (
            JobSchedule.STATUS_DONE, "not a sync-replication cluster")
        rpcs[a[0].get_id()].distr_migration_replica_sync_start.assert_not_called()

    def test_one_driver_cycle_runs_the_tasks_of_sync_clusters(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)
        task_driver.one_cycle(runner.SPEC)
        assert catchup.starts() == DISTRIBS
        assert _fresh_task(db, task).owner                 # claimed through the lease


# ---------------------------------------------------------------------------
# tasks written by an earlier version of the runner
# ---------------------------------------------------------------------------

class TestUpgrade:

    def test_a_leftover_backoff_deadline_is_ignored(self, db, rpcs, clock, held):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)

        def backing_off(t):
            t.retry = 1
            t.status = JobSchedule.STATUS_SUSPENDED
            t.function_params["next_run_at"] = clock.now + 600

        db.atomic_update(task, backing_off)
        assert held(task).status == JobSchedule.STATUS_RUNNING
        assert catchup.starts() == DISTRIBS

    def test_a_running_catch_up_is_polled_on(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        for name in DISTRIBS:
            catchup._start(name)
        _desync(a[0])
        task = _task(db, cluster)

        def running(t):
            t.status = JobSchedule.STATUS_RUNNING
            t.function_params["resync"] = {"leader_id": a[0].get_id(), "started_at": clock.now - 20,
                                           "distribs": DISTRIBS}

        db.atomic_update(task, running)
        assert _pass(task).status == JobSchedule.STATUS_RUNNING
        catchup.complete()
        done = _pass(task)
        assert (done.status, done.function_params["duration_sec"]) == (JobSchedule.STATUS_DONE, 20)
        assert catchup.starts() == []
