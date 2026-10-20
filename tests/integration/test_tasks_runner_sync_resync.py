"""The sync-replication resync runner against the real FoundationDB: a zone
desync recorded through the real event path queues the LVS's catch-up; the
runner starts it on the leader of the active triplet once the lagging zone is
up, polls it, finishes when every distrib completed without error and the
leader reports ``synced`` (resolving the zone events), and otherwise re-runs
it with backoff - never while the site is lost, the zone is down or the
leadership is moving. The runner is driven through the shared task driver:
``task_driver.drive`` for one pass, or one driver held across dispatches where
its in-memory backoff is the subject. Storage nodes (RPC) are mocked; the
database never is. The pure rules are in
tests/unit/tasks/test_tasks_runner_sync_resync.py.
"""
import threading
import types
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


class _Catchup:
    """The catch-up as the leader's data plane reports it."""

    def __init__(self, rpc):
        self.rpc = rpc
        self.migration = {name: {"name": name, "status": "none"} for name in DISTRIBS}
        self.sync_status = "synced"
        rpc.distr_migration_expansion_start.side_effect = self._start
        rpc.distr_migration_status.side_effect = lambda name: [self.migration[name]]
        rpc.distr_sync_replication_status.side_effect = \
            lambda name=None: [{"name": name, "status": self.sync_status}]

    def _start(self, name, *args, **kwargs):
        self.migration[name] = {"name": name, "status": "running", "error": 0, "progress": 0}
        return True

    def complete(self, error=0, name=None):
        for n in [name] if name else DISTRIBS:
            self.migration[n] = {"name": n, "status": "completed", "error": error, "progress": 100}

    def starts(self):
        return [c.args[0] for c in self.rpc.distr_migration_expansion_start.call_args_list]


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
            JobSchedule.STATUS_DONE, "synced; catch-up took 42.0s")
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
            rpcs[node.get_id()].distr_migration_expansion_start.assert_not_called()

    def test_a_remote_led_lvs_catches_up_on_the_remote_leader(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db, active_site=SITE_B)
        rpcs.lead(b[1])
        catchup = _Catchup(rpcs[b[1].get_id()])
        _desync(b[1], status="primary_zone_unavailable")
        task = _task(db, cluster)
        _pass(task)
        catchup.complete()
        assert _pass(task).status == JobSchedule.STATUS_DONE
        assert catchup.starts() == DISTRIBS
        for node in a:
            rpcs[node.get_id()].distr_migration_expansion_start.assert_not_called()

    def test_a_catch_up_already_in_progress_is_adopted(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        catchup.migration["distrib_11"] = {"name": "distrib_11", "status": "running", "error": 0}
        _refused(rpcs[a[0].get_id()], catchup, RPCRemoteError(message=IN_PROGRESS, code=-16))
        _desync(a[0])
        task = _task(db, cluster)
        adopted = _pass(task)
        assert (adopted.status, adopted.retry) == (JobSchedule.STATUS_RUNNING, 0)
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
            JobSchedule.STATUS_DONE, "synced; catch-up took 7.0s")
        assert not {"resync", "converged"} & set(done.function_params)
        assert not db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")
        assert catchup.starts() == DISTRIBS


IN_PROGRESS = "Migration is already in progress"


def _refused(rpc, catchup, error):
    """The start of distrib_11 raises ``error``, the others start."""
    def _start(name, *args, **kwargs):
        if name == "distrib_11":
            raise error
        return catchup._start(name)
    rpc.distr_migration_expansion_start.side_effect = _start


class TestRefusedStart:
    """A start the leader refuses: a remote error is followed by the status
    probe (an "already in progress" run is adopted), a transport error fails
    the run without a probe."""

    def _failed(self, db, task):
        fresh = _fresh_task(db, task)
        assert (fresh.status, fresh.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "distrib_11: start" in fresh.function_result
        return fresh

    def test_a_refusal_without_a_run_in_progress_fails_the_run(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        rpc = rpcs[a[0].get_id()]
        catchup = _Catchup(rpc)
        _refused(rpc, catchup, RPCRemoteError(message="busy", code=-16))
        _desync(a[0])
        task = _task(db, cluster)
        _pass(task)
        assert "refused" in self._failed(db, task).function_result
        rpc.distr_migration_status.assert_called_with("distrib_11")

    def test_a_failed_status_probe_fails_the_run(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        rpc = rpcs[a[0].get_id()]
        catchup = _Catchup(rpc)
        _refused(rpc, catchup, RPCRemoteError(message=IN_PROGRESS, code=-16))
        rpc.distr_migration_status.side_effect = RPCConnectionError("down")
        _desync(a[0])
        task = _task(db, cluster)
        _pass(task)
        assert "start failed" in self._failed(db, task).function_result

    def test_a_transport_error_fails_the_run_without_a_probe(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        rpc = rpcs[a[0].get_id()]
        catchup = _Catchup(rpc)
        _refused(rpc, catchup, RPCConnectionError("down"))
        _desync(a[0])
        task = _task(db, cluster)
        _pass(task)
        assert "start failed" in self._failed(db, task).function_result
        rpc.distr_migration_status.assert_not_called()


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
        rpc.distr_migration_status.side_effect = lambda name: [{"name": name, "status": "none"}]
        _pass(task)                                       # re-run starts
        assert _pass(task).retry == 2                     # its migration is gone (restart)

    def test_a_refused_start_is_a_non_converging_run(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        rpcs[a[0].get_id()].distr_migration_expansion_start.side_effect = lambda *args, **kwargs: None
        _desync(a[0])
        task = _task(db, cluster)
        failed = _pass(task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert catchup.migration["distrib_11"]["status"] == "none"

    def test_a_leadership_change_re_runs_the_catch_up(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        rpcs[a[0].get_id()].get_lvstore.return_value = {"lvs leadership": False}
        rpcs.lead(a[1])
        ttl_cache.leader_cache.invalidate()
        failed = _pass(task)
        assert failed.retry == 1
        assert "leadership moved" in failed.function_result

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
            rpcs[node.get_id()].distr_migration_expansion_start.assert_not_called()

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
        rpcs[a[0].get_id()].distr_migration_expansion_start.assert_called()

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
        rpcs[a[0].get_id()].distr_migration_expansion_start.assert_called()

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
        rpcs[a[0].get_id()].distr_migration_expansion_start.assert_not_called()

    def test_a_cancel_during_a_start_stops_further_starts(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)

        def start(name, *args, **kwargs):
            tasks_controller.cancel_task(task.uuid)       # while the RPC is in flight
            return catchup._start(name)

        rpcs[a[0].get_id()].distr_migration_expansion_start.side_effect = start
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
        rpcs[a[0].get_id()].distr_migration_expansion_start.assert_not_called()

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
