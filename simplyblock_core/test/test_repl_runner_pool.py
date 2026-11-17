"""The snapshot replication runner runs its tasks on a worker pool.

bf13 (2026-11-05, 6 volumes under heavy IO): one runner executed every task
one after the other. A landing-volume create of 29 s for one volume kept a
converging cutover's shrink snapshot waiting for its transfer, four
replicating volumes got 1.7-way concurrency, and no volume but one ever
reached its cutover gate.
"""
import threading
import time
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.services import snapshot_replication as sr


def _task(uuid):
    t = JobSchedule()
    t.uuid = uuid
    t.cluster_id = "CL"
    t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
    t.function_params = {"snapshot_id": f"S-{uuid}"}
    t.status = JobSchedule.STATUS_NEW
    return t


def _wait_until(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


class TestWorkerPool:

    def test_two_slow_tasks_run_at_the_same_time(self):
        """The bf13 case: one task holds a worker for seconds; another task
        is not delayed by it."""
        started, release = [], threading.Event()

        def slow(task):
            started.append(task.uuid)
            release.wait(5)
            return True
        pool = sr._Workers(2)
        with patch.object(sr, "_dispatch", side_effect=slow):
            pool.submit(_task("A"))
            pool.submit(_task("B"))
            assert _wait_until(lambda: len(started) == 2, 2.0), started
            release.set()
            assert _wait_until(lambda: pool.running() == 0)

    def test_a_task_is_on_one_worker_at_a_time(self):
        runs, release = [], threading.Event()

        def slow(task):
            runs.append(task.uuid)
            release.wait(5)
            return True
        pool = sr._Workers(4)
        with patch.object(sr, "_dispatch", side_effect=slow):
            t = _task("A")
            pool.submit(t)
            assert _wait_until(lambda: len(runs) == 1)
            pool.submit(t)                     # still running: no second run
            pool.submit(_task("A"))            # same task id, fresh object
            time.sleep(0.1)
            assert runs == ["A"]
            release.set()
            assert _wait_until(lambda: pool.running() == 0)

    def test_a_failed_task_pauses_before_it_is_run_again(self):
        runs = []
        pool = sr._Workers(2)
        def fail(task):
            runs.append(task.uuid)
            return False
        with patch.object(sr, "_dispatch", side_effect=fail), \
                patch.object(constants, "REPL_RUNNER_RETRY_PAUSE_SEC", 0.3):
            t = _task("A")
            pool.submit(t)
            assert _wait_until(lambda: pool.running() == 0)
            assert pool.paused(t)
            pool.submit(t)
            time.sleep(0.05)
            assert runs == ["A"], "paused: not run again at once"
            assert _wait_until(lambda: not pool.paused(t), 1.0)
            pool.submit(t)
            assert _wait_until(lambda: len(runs) == 2)

    def test_no_workers_runs_inline_and_returns_the_result(self):
        pool = sr._Workers(0)
        with patch.object(sr, "_dispatch", return_value=False):
            assert pool.submit(_task("A")) is False
        assert pool.running() == 0

    def test_the_endgame_wake_is_served_while_a_worker_is_busy(self):
        """bf13 round 7: the shrink snapshot's transfer waited 33 s because the
        runner was inside another volume's landing-volume create."""
        started, release = [], threading.Event()

        def run(task):
            started.append(task.uuid)
            if task.uuid == "SLOW":
                release.wait(5)
            return True
        wake = MagicMock()
        wake.watching.return_value = True
        wake.poll.return_value = {"CL": {"S-SHRINK"}}
        shrink = _task("SHRINK")
        db = MagicMock()
        db.get_job_tasks.return_value = [shrink]
        pool = sr._Workers(2)
        with patch.object(sr, "_dispatch", side_effect=run), \
                patch.object(sr, "db", db), \
                patch.object(sr, "_WORKERS", pool):
            pool.submit(_task("SLOW"))
            assert _wait_until(lambda: started == ["SLOW"])
            t0 = time.time()
            sr._check_endgame_wake(wake)
            assert _wait_until(lambda: "SHRINK" in started, 1.0)
            assert time.time() - t0 < 1.0
            release.set()
            assert _wait_until(lambda: pool.running() == 0)

    def test_hand_off_goes_through_the_pool(self):
        """Finishing the successor inline from another task's worker would let
        two workers mutate one task row."""
        succ = _task("SUCC")
        succ.function_params.update(chain_deferred_to="S1", transfer_done=True)
        db = MagicMock()
        db.get_job_tasks.return_value = [succ]
        snap = MagicMock()
        snap.get_id.return_value = "S1"
        with patch.object(sr, "db", db), \
                patch.object(sr, "_WORKERS", sr._Workers(2)), \
                patch.object(sr, "_submit") as submit, \
                patch.object(sr, "_resume_finish") as inline:
            sr._hand_off_to_successor(_task("PRED"), snap)
        submit.assert_called_once()
        assert submit.call_args.args[0].uuid == "SUCC"
        inline.assert_not_called()
        # without a pool the successor is finished inline, as before
        with patch.object(sr, "db", db), \
                patch.object(sr, "_WORKERS", None), \
                patch.object(sr, "_resume_finish") as inline:
            sr._hand_off_to_successor(_task("PRED"), snap)
        inline.assert_called_once()


class TestHubLock:

    def test_one_lock_per_hub_session(self):
        a = sr._hub_lock("src1", "tgt1")
        assert a is sr._hub_lock("src1", "tgt1")
        assert a is not sr._hub_lock("src1", "tgt2")
        assert a is not sr._hub_lock("src2", "tgt1")

    def test_the_start_holds_the_hub_lock_from_attach_to_running(self):
        import inspect
        src = inspect.getsource(sr.process_snap_replicate_start)
        i = src.index("with _hub_lock(")
        assert src.index("ensure_hub_attached(", i) > i
        assert src.index("task.status = JobSchedule.STATUS_RUNNING", i) > i
        # the long inline wait for the transfer is outside the lock
        assert src.index("_await_transfer_completion(task, snapshot, snode)") > \
            src.index("task.status = JobSchedule.STATUS_RUNNING")

    def test_the_finish_decides_the_detach_under_the_hub_lock(self):
        import inspect
        src = inspect.getsource(sr.process_snap_replicate_finish)
        i = src.index("with _hub_lock(")
        assert src.index("_other_active_transfers_to_node(", i) > i
        assert src.index("bdev_nvme_detach_controller(hub_ctrl)", i) > i
