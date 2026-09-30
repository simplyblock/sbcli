"""The sync-replication resync runner against the real FoundationDB: a zone
desync recorded through the real event path queues the LVS's catch-up; the
runner starts it on the leader of the active triplet once the lagging zone is
up, polls it, finishes when every distrib completed without error and the
leader reports ``synced`` (resolving the zone events), and otherwise re-runs
it with backoff - never while the site is lost, the zone is down or the
leadership is moving. Storage nodes (RPC) are mocked; the database never is.
The pure rules are in tests/unit/tasks/test_tasks_runner_sync_resync.py.
"""
import threading
import types
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import tasks_runner_sync_resync as runner
from simplyblock_core.utils import ttl_cache
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs

DISTRIBS = ["distrib_11", "distrib_12"]
ERROR_REPLICA_SYNC = 1 << 11
ERROR_SHUTDOWN = 1 << 8


class _Clock:
    """The runner's ``time``: a settable ``time()``, and ``sleep`` that must
    never be reached by a pass (a pass never sleeps in place)."""

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
    return runner.task_runner(task)


# ---------------------------------------------------------------------------
# happy path
# ---------------------------------------------------------------------------

class TestConvergence:

    def test_catch_up_on_the_leader_then_done_and_the_desync_resolved(self, db, rpcs, clock):
        cluster, a, b, owner = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        _desync(b[1], vuid=12)
        src.record_sync_event(a[0].get_id(), {
            "timestamp": "2026-09-30T11:00:01.000000Z", "event_type": "sync_replication_status",
            "status": "remote_journal_unsynced", "jm_vuid": 1})
        task = _task(db, cluster)

        assert _pass(task) is False
        assert catchup.starts() == DISTRIBS
        started = _fresh_task(db, task)
        assert started.status == JobSchedule.STATUS_RUNNING
        assert started.function_params["resync"]["leader_id"] == a[0].get_id()

        clock.now += 30
        assert _pass(task) is False                      # still running
        assert _fresh_task(db, task).status == JobSchedule.STATUS_RUNNING

        catchup.complete()
        clock.now += 12
        assert _pass(task) is True
        done = _fresh_task(db, task)
        assert done.status == JobSchedule.STATUS_DONE
        assert done.function_params["duration_sec"] == 42
        assert "resync" not in done.function_params
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
        assert _pass(task) is True
        assert catchup.starts() == DISTRIBS
        for node in a:
            rpcs[node.get_id()].distr_migration_expansion_start.assert_not_called()

    def test_a_catch_up_already_in_progress_is_adopted(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        catchup.migration["distrib_11"] = {"name": "distrib_11", "status": "running", "error": 0}
        rpc = rpcs[a[0].get_id()]
        rpc.distr_migration_expansion_start.side_effect = \
            lambda name, *args, **kwargs: None if name == "distrib_11" else catchup._start(name)
        _desync(a[0])
        task = _task(db, cluster)
        _pass(task)
        assert _fresh_task(db, task).status == JobSchedule.STATUS_RUNNING
        assert _fresh_task(db, task).retry == 0
        catchup.complete()
        assert _pass(task) is True


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

    def test_bit_11_suspends_and_re_runs_after_the_backoff(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete(error=ERROR_REPLICA_SYNC, name="distrib_12")
        catchup.complete(name="distrib_11")
        assert _pass(task) is False
        failed = _fresh_task(db, task)
        assert (failed.status, failed.retry) == (JobSchedule.STATUS_SUSPENDED, 1)
        assert "resync" not in failed.function_params
        assert failed.function_params["next_run_at"] == clock.now + runner.retry_delay(1)

        # passes before the deadline start nothing
        for _ in range(3):
            clock.now += 5
            _pass(task)
        assert catchup.starts() == DISTRIBS
        assert _fresh_task(db, task).retry == 1

        clock.now = failed.function_params["next_run_at"]
        _pass(task)
        assert catchup.starts() == DISTRIBS * 2
        catchup.complete()
        assert _pass(task) is True
        assert _fresh_task(db, task).status == JobSchedule.STATUS_DONE

    def test_the_backoff_doubles_per_non_converging_run(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        delays = []
        for _ in range(3):
            catchup.complete(error=1)
            before = clock.now
            _pass(task)
            delays.append(_fresh_task(db, task).function_params["next_run_at"] - before)
            clock.now = _fresh_task(db, task).function_params["next_run_at"]
            _pass(task)                                   # the re-run starts
        assert delays == [runner.retry_delay(1), runner.retry_delay(2), runner.retry_delay(3)]
        assert delays[1] == 2 * delays[0]

    @pytest.mark.parametrize("error", [ERROR_SHUTDOWN, 1, 1 << 3])
    def test_shutdown_or_io_error_waits_for_the_zone_and_re_runs_when_it_returns(
            self, db, rpcs, clock, error):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        # the lagging (replica, site B) zone is lost again: the data plane
        # stops the catch-up toward it
        dev = b[2].nvme_devices[0]
        stack._update(db, b[2], nvme_devices=[_with_status(dev, NVMeDevice.STATUS_UNAVAILABLE)])
        catchup.complete(error=error)
        assert _pass(task) is False
        assert _fresh_task(db, task).status == JobSchedule.STATUS_SUSPENDED

        clock.now += runner.retry_delay(5)
        for _ in range(3):
            assert _pass(task) is False
        waiting = _fresh_task(db, task)
        assert waiting.status == JobSchedule.STATUS_SUSPENDED
        assert waiting.retry == 1                         # waiting is not a run
        assert dev.get_id() in waiting.function_result
        assert catchup.starts() == DISTRIBS

        stack._update(db, b[2], nvme_devices=[_with_status(dev, NVMeDevice.STATUS_ONLINE)])
        _pass(task)
        assert catchup.starts() == DISTRIBS * 2
        catchup.complete()
        assert _pass(task) is True

    def test_completed_but_not_synced_is_not_done(self, db, rpcs, clock):
        _, _, _, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        catchup.sync_status = "replica_unsynced"
        assert _pass(task) is False
        assert (_fresh_task(db, task).status, _fresh_task(db, task).retry) == (
            JobSchedule.STATUS_SUSPENDED, 1)

    def test_a_status_query_error_or_a_lost_migration_is_a_non_converging_run(self, db, rpcs, clock):
        _, a, _, _, catchup, task = _started(db, rpcs, clock)
        rpc = rpcs[a[0].get_id()]
        rpc.distr_migration_status.side_effect = RPCException("connection error")
        assert _pass(task) is False
        assert _fresh_task(db, task).retry == 1
        rpc.distr_migration_status.side_effect = lambda name: [{"name": name, "status": "none"}]
        clock.now += runner.retry_delay(1)
        _pass(task)                                       # re-run starts
        assert _pass(task) is False                       # its migration is gone (restart)
        assert _fresh_task(db, task).retry == 2

    def test_a_refused_start_is_a_non_converging_run(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        rpcs[a[0].get_id()].distr_migration_expansion_start.side_effect = lambda *args, **kwargs: None
        _desync(a[0])
        task = _task(db, cluster)
        assert _pass(task) is False
        assert (_fresh_task(db, task).status, _fresh_task(db, task).retry) == (
            JobSchedule.STATUS_SUSPENDED, 1)
        assert catchup.migration["distrib_11"]["status"] == "none"

    def test_a_leadership_change_during_the_catch_up_re_runs_it(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)
        rpcs[a[0].get_id()].bdev_lvol_get_lvstores.return_value = [{"lvs leadership": False}]
        rpcs.lead(a[1])
        ttl_cache.leader_cache.invalidate()
        assert _pass(task) is False
        assert _fresh_task(db, task).retry == 1
        assert "leadership moved" in _fresh_task(db, task).function_result

    def test_one_alert_after_n_non_converging_runs(self, db, rpcs, clock, alerts):
        with patch.object(runner.constants, "SYNC_RESYNC_ALERT_RUNS", 3):
            _, _, _, owner, catchup, task = _started(db, rpcs, clock)
            for run in range(1, 6):
                catchup.complete(error=ERROR_REPLICA_SYNC)
                _pass(task)
                assert _fresh_task(db, task).retry == run
                assert len(_alerts(alerts)) == (0 if run < 3 else 1)
                clock.now = _fresh_task(db, task).function_params["next_run_at"]
                _pass(task)
        kwargs = _alerts(alerts)[0].kwargs
        assert (kwargs["event_level"], kwargs["node_id"]) == ("Error", owner.get_id())
        assert _fresh_task(db, task).status != JobSchedule.STATUS_DONE

    def test_an_alert_that_failed_to_be_written_is_raised_on_the_next_run(self, db, rpcs, clock, alerts):
        failures = []

        def log(*args, **kwargs):
            if kwargs.get("event") == "SYNC_RESYNC_NOT_CONVERGING" and not failures:
                failures.append(kwargs)
                raise RuntimeError("event log write failed")
        alerts.side_effect = log
        with patch.object(runner.constants, "SYNC_RESYNC_ALERT_RUNS", 1):
            _, _, _, _, catchup, task = _started(db, rpcs, clock)
            catchup.complete(error=ERROR_REPLICA_SYNC)
            with pytest.raises(RuntimeError):
                _pass(task)
            failed = _fresh_task(db, task)
            assert failed.retry == 1                      # the run itself is recorded
            assert "alerted" not in failed.function_params
            clock.now = failed.function_params["next_run_at"]
            _pass(task)
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
            assert _pass(task) is False
        waiting = _fresh_task(db, task)
        assert (waiting.status, waiting.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert text in waiting.function_result
        for node in (*a, *b):
            rpcs[node.get_id()].distr_migration_expansion_start.assert_not_called()

    def test_while_a_site_is_lost(self, db, rpcs, clock):
        cluster, a, b, _ = _layout(db)
        rpcs.lead(a[0])
        _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        cluster = db.get_cluster_by_id(cluster.get_id())
        cluster.lost_site = SITE_B
        cluster.write_to_db(db.kv_store)
        task = _task(db, cluster)
        self._assert_waiting(db, rpcs, task, a, b, f"site {SITE_B} is lost")
        cluster.lost_site = ""
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

    def test_a_down_node_outside_the_lagging_zone_does_not_hold_it(self, db, rpcs, clock):
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


# ---------------------------------------------------------------------------
# DONE never loses a newer desync
# ---------------------------------------------------------------------------

class TestNewerDesync:

    def test_a_desync_during_the_final_check_keeps_the_task_and_the_event(self, db, rpcs, clock):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        before = db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")
        newer = []

        def status(name=None):
            if not newer:
                newer.append(_desync(b[0], vuid=12))
            return [{"name": name, "status": "synced"}]

        rpcs[a[0].get_id()].distr_sync_replication_status.side_effect = status
        assert _pass(task) is False
        again = _fresh_task(db, task)
        assert (again.status, again.retry) == (JobSchedule.STATUS_SUSPENDED, 0)
        assert "resync" not in again.function_params
        open_ids = {e.get_id() for e in db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")}
        assert open_ids == {newer[0].get_id()}
        assert not open_ids & {e.get_id() for e in before}
        assert len(db.get_active_sync_resync_tasks(cluster.get_id())) == 1

        # the new run converges and finishes
        rpcs[a[0].get_id()].distr_sync_replication_status.side_effect = \
            lambda name=None: [{"name": name, "status": "synced"}]
        _pass(task)
        catchup.complete()
        assert _pass(task) is True

    def test_a_desync_committed_inside_the_finish_transaction_forces_its_retry(self, db, rpcs, clock):
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
            assert _pass(task) is False
        assert newer and newer[0] is not None
        again = _fresh_task(db, task)
        assert again.status == JobSchedule.STATUS_SUSPENDED
        assert [e.get_id() for e in db.get_unresolved_sync_replication_events(cluster.get_id(), "LVS_1")] \
            == [newer[0].get_id()]
        # the desync did not get a second task: the running one covers it
        assert [t.uuid for t in db.get_active_sync_resync_tasks(cluster.get_id())] == [task.uuid]

    def test_a_desync_after_done_gets_a_new_task(self, db, rpcs, clock):
        cluster, a, b, _, catchup, task = _started(db, rpcs, clock)
        catchup.complete()
        assert _pass(task) is True
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
        assert _pass(task) is True
        assert _fresh_task(db, task).status == JobSchedule.STATUS_DONE
        rpcs[a[0].get_id()].distr_migration_expansion_start.assert_not_called()

    def test_a_cancel_during_a_start_rpc_stays_and_stops_further_starts(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)

        def start(name, *args, **kwargs):
            tasks_controller.cancel_task(task.uuid)       # while the RPC is in flight
            return catchup._start(name)

        rpcs[a[0].get_id()].distr_migration_expansion_start.side_effect = start
        _pass(task)
        ended = _fresh_task(db, task)
        assert ended.canceled is True
        assert ended.status == JobSchedule.STATUS_DONE
        assert catchup.starts() == ["distrib_11"]

    def test_a_cancel_during_a_status_rpc_stays_and_ends_the_task(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)

        def status(name):
            tasks_controller.cancel_task(task.uuid)
            return [catchup.migration[name]]

        rpcs[a[0].get_id()].distr_migration_status.side_effect = status
        assert _pass(task) is False                       # still running: progress written
        assert _fresh_task(db, task).canceled is True
        assert _pass(task) is True
        ended = _fresh_task(db, task)
        assert (ended.canceled, ended.status, ended.function_result) == (
            True, JobSchedule.STATUS_DONE, "canceled")
        assert catchup.starts() == DISTRIBS

    def test_a_cancel_during_a_failing_poll_stays(self, db, rpcs, clock):
        cluster, a, _, _, catchup, task = _started(db, rpcs, clock)

        def status(name):
            tasks_controller.cancel_task(task.uuid)
            raise RPCException("connection error")

        rpcs[a[0].get_id()].distr_migration_status.side_effect = status
        _pass(task)
        failed = _fresh_task(db, task)
        assert (failed.canceled, failed.retry) == (True, 1)
        clock.now = failed.function_params["next_run_at"]
        assert _pass(task) is True
        assert catchup.starts() == DISTRIBS

    def test_a_removed_owner_ends_the_task(self, db, rpcs, clock):
        cluster, a, _, owner = _layout(db)
        _desync(a[0])
        task = _task(db, cluster)
        stack._update(db, owner, status=StorageNode.STATUS_REMOVED)
        assert _pass(task) is True
        assert "no longer owned" in _fresh_task(db, task).function_result

    def test_main_runs_the_tasks_of_sync_clusters(self, db, rpcs, clock):
        cluster, a, _, _ = _layout(db)
        rpcs.lead(a[0])
        catchup = _Catchup(rpcs[a[0].get_id()])
        _desync(a[0])
        task = _task(db, cluster)

        class _Stop(BaseException):
            pass

        def sleep(_seconds):
            raise _Stop

        with patch.object(runner, "time", types.SimpleNamespace(time=clock.time, sleep=sleep)):
            with pytest.raises(_Stop):
                runner.main()
        assert catchup.starts() == DISTRIBS
        assert _fresh_task(db, task).owner                 # claimed through the lease
