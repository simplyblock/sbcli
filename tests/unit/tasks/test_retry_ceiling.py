# coding=utf-8
"""Behavioural regression tests for task-runner retry ceilings.

Background: an S3 backup whose ``bdev_lvol_s3_backup`` RPC crashed SPDK was
re-issued every poll cycle forever. ``tasks_runner_backup`` incremented
``task.retry`` (to 21 in the incident) but never compared it against
``task.max_retry`` (10), so the task stayed ``suspended`` and the backup never
transitioned to ``failed`` — only a 4h time-based timeout could end it.

Every task runner that increments ``task.retry`` on failure must therefore
eventually give up: once the ceiling is hit the task must reach
``STATUS_DONE`` instead of looping. These tests verify that **behaviourally** —
by driving each runner's real ``main()`` loop with the actual work mocked to
fail every cycle and ``sleep`` neutralised — rather than by scanning source for
a guard expression (which false-flags equivalent-but-differently-spelled
ceilings and can't tell a live guard from dead code).

How the harness works: each runner's ``main()`` is an unbounded ``while True``.
We replace the module's ``time`` with a fake whose ``sleep()`` never sleeps and
instead trips a circuit breaker — it raises :class:`_StopMainLoop` once the task
has terminated (the normal exit) or after a hard cap of iterations (which only
happens if the ceiling is missing and the loop would otherwise spin forever).
Runners that fan work out onto a thread pool get a synchronous stand-in so the
retry accrues deterministically in-process.
"""
import importlib
import re
import types
from concurrent.futures import Future
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from simplyblock_core.models.backup import Backup
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode


# --------------------------------------------------------------------------
# Backup runner: terminates instead of looping forever.
#
# The backup runner's loop is inline under ``if __name__ == '__main__'`` (it has
# no ``def main()``), so it is driven at the ``process_task`` level here. The
# other runners below are driven through their real ``main()``.
# --------------------------------------------------------------------------

@pytest.fixture
def runner(monkeypatch):
    """Import the backup runner with its FDB writes and events neutralised.

    The module's ``while True`` loop is guarded by ``if __name__ ==
    '__main__'``, so importing it only defines functions and a (kv_store=None)
    ``DBController`` singleton — safe under the unit tier's stubbed ``fdb``.
    """
    import simplyblock_core.services.tasks_runner_backup as runner
    monkeypatch.setattr(runner, "backup_events", MagicMock())
    # Model writes would otherwise dereference the None kv_store and exit(1).
    monkeypatch.setattr(Backup, "write_to_db", MagicMock())
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())
    return runner


def _backup_task(retry, max_retry):
    task = JobSchedule()
    task.uuid = "task-1"
    task.function_name = JobSchedule.FN_BACKUP
    task.function_params = {"backup_id": "bk-1"}
    task.retry = retry
    task.max_retry = max_retry
    task.canceled = False
    # Recent enough that the 4h time-based timeout does not fire first.
    task.date = int(__import__("time").time())
    return task


def _cluster():
    cl = MagicMock()
    cl.backup_timeout_seconds = 14400
    return cl


def test_backup_fails_when_max_retry_reached(runner, monkeypatch):
    """retry >= max_retry must fail the backup and finish the task, not poll."""
    backup = Backup()
    backup.uuid = "bk-1"
    backup.status = Backup.STATUS_IN_PROGRESS

    fake_db = MagicMock()
    fake_db.get_backup_by_id.return_value = backup
    monkeypatch.setattr(runner, "db", fake_db)
    # If the ceiling is missing this would run and (in the incident) re-issue
    # the crash-inducing RPC; assert it is never reached.
    run_backup = MagicMock()
    monkeypatch.setattr(runner, "_run_backup", run_backup)

    task = _backup_task(retry=10, max_retry=10)
    runner.process_task(task, _cluster())

    assert backup.status == Backup.STATUS_FAILED
    assert task.status == JobSchedule.STATUS_DONE
    assert "max retry" in task.function_result
    run_backup.assert_not_called()


def test_backup_runs_below_max_retry(runner, monkeypatch):
    """Below the ceiling the task still advances (dispatches its step)."""
    monkeypatch.setattr(runner, "db", MagicMock())
    run_backup = MagicMock()
    monkeypatch.setattr(runner, "_run_backup", run_backup)

    task = _backup_task(retry=9, max_retry=10)
    runner.process_task(task, _cluster())

    run_backup.assert_called_once_with(task)


def test_no_process_poll_counts_as_a_retry(runner, monkeypatch):
    """The 'No process' re-issue branch must advance task.retry, otherwise the
    ceiling can never bind to that path and the backup re-issues forever."""
    backup = Backup()
    backup.uuid = "bk-1"
    backup.status = Backup.STATUS_IN_PROGRESS
    backup.snapshot_id = "snap-1"

    snode = MagicMock()
    snode.status = StorageNode.STATUS_ONLINE
    rpc = MagicMock()
    rpc.bdev_lvol_transfer_stat.return_value = {"transfer_state": "No process"}
    snode.rpc_client.return_value = rpc

    snapshot = MagicMock()
    snapshot.snap_bdev = "lvs/snap0"

    fake_db = MagicMock()
    fake_db.get_backup_by_id.return_value = backup
    fake_db.get_storage_node_by_id.return_value = snode
    fake_db.get_snapshot_by_id.return_value = snapshot
    monkeypatch.setattr(runner, "db", fake_db)

    task = _backup_task(retry=3, max_retry=10)
    runner._run_backup(task)

    assert task.retry == 4, "No-process re-issue must count toward the ceiling"
    assert backup.status == Backup.STATUS_PENDING
    rpc.bdev_lvol_s3_backup.assert_not_called()  # this poll only resets state


# --------------------------------------------------------------------------
# Drive-main harness: exercise each runner's real retry loop end-to-end.
# --------------------------------------------------------------------------

class _StopMainLoop(BaseException):
    """Sentinel raised from the patched ``sleep`` to break a runner's
    ``while True`` main loop. A ``BaseException`` so the runners' own
    ``except Exception`` per-task guards do not swallow it."""


class _InlineExecutor:
    """Synchronous stand-in for ``ThreadPoolExecutor`` so a runner that fans
    task processing onto a pool runs it in-process, deterministically."""

    def __init__(self, *args, **kwargs):
        pass

    def submit(self, fn, *args, **kwargs):
        fut: Future = Future()
        try:
            fut.set_result(fn(*args, **kwargs))
        except BaseException as exc:  # noqa: BLE001 - mirror pool semantics
            fut.set_exception(exc)
        return fut

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def shutdown(self, *args, **kwargs):
        pass


# Iterations of no-op sleep after which the circuit breaker trips regardless of
# task state. A correctly-ceilinged runner terminates in well under this; only a
# missing ceiling reaches it — and then the assertions below fail loudly instead
# of the test hanging.
_SLEEP_CAP = 1000


def _patch_clock(monkeypatch, runner, task):
    """Replace the runner module's ``time`` with a fake that never blocks.

    ``sleep()`` trips the circuit breaker once the task is DONE (normal exit) or
    after ``_SLEEP_CAP`` calls (missing-ceiling backstop). ``time()`` advances a
    large step each call so any wall-clock backoff gate inside the runner has
    already elapsed by the next poll.
    """
    state = {"clock": 1_000_000_000, "sleeps": 0}

    def fake_sleep(_seconds=0):
        state["sleeps"] += 1
        state["clock"] += 1_000_000
        if task.status == JobSchedule.STATUS_DONE or state["sleeps"] > _SLEEP_CAP:
            raise _StopMainLoop

    def fake_time():
        state["clock"] += 1_000_000
        return state["clock"]

    monkeypatch.setattr(
        runner, "time", types.SimpleNamespace(sleep=fake_sleep, time=fake_time))
    return state


def _make_task(function_name, max_retry=3, **params):
    task = JobSchedule()
    task.uuid = "task-1"
    task.cluster_id = "cl-1"
    task.node_id = "node-1"
    task.device_id = "dev-1"
    task.function_name = function_name
    task.function_params = dict(params)
    task.retry = 0
    task.max_retry = max_retry
    task.canceled = False
    task.status = JobSchedule.STATUS_NEW
    return task


def _base_db(task):
    """A DBController mock wired to yield one cluster and one always-failing
    task, re-fetchable by uuid so retry accrues across polls."""
    db = MagicMock()

    cluster = MagicMock()
    cluster.get_id.return_value = task.cluster_id
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.suspend_drain_complete = False
    cluster.expand_state = {}

    node = db.get_storage_node_by_id.return_value
    node.status = StorageNode.STATUS_ONLINE
    node.data_nics = []
    node.nvme_devices = []

    db.get_clusters.return_value = [cluster]
    db.get_job_tasks.return_value = [task]
    db.get_task_by_id.return_value = task
    db.get_cluster_by_id.return_value = cluster
    db.get_storage_nodes_by_cluster_id.return_value = [node]
    return db, cluster, node


def _assert_terminates_at_ceiling(runner, task):
    with pytest.raises(_StopMainLoop):
        runner.main()
    assert task.status == JobSchedule.STATUS_DONE, (
        f"{runner.__name__}.main() never terminated the perpetually-failing "
        f"task (retry={task.retry}/{task.max_retry}) — missing retry ceiling")
    assert task.retry >= task.max_retry, (
        f"{runner.__name__} finished the task at retry={task.retry} before "
        f"reaching the ceiling max_retry={task.max_retry}")
    assert "max retry" in task.function_result.lower(), (
        f"{runner.__name__} finished with {task.function_result!r}, "
        f"not a max-retry stop")


def _wire_base(runner, monkeypatch, task):
    """Apply the mocks every drive-main spec needs and return (db, cluster,
    node) for the spec to specialise."""
    db, cluster, node = _base_db(task)
    monkeypatch.setattr(runner, "db", db)
    monkeypatch.setattr(JobSchedule, "write_to_db", MagicMock())
    _patch_clock(monkeypatch, runner, task)
    return db, cluster, node


# Per-runner setup for the drive-main test. Each spec builds a task whose work
# fails every cycle and mocks exactly the collaborators that runner touches, then
# returns the task. The setup is necessarily runner-specific because each runner
# calls different DB accessors and different work functions — there is no generic
# "make it fail" that fits every runner. What is NOT hand-maintained is *which*
# runners get tested: the parametrised test below discovers them from the source
# tree, so a new retry-driven runner shows up as a failing case until a spec is
# added here.

def _spec_cluster_expand(runner, monkeypatch):
    task = _make_task(JobSchedule.FN_CLUSTER_EXPAND, new_node_id="new-1")
    _wire_base(runner, monkeypatch, task)
    monkeypatch.setattr(runner.tasks_controller, "claim_task",
                        lambda *a, **k: True)
    # The actual expansion work fails every cycle.
    monkeypatch.setattr(
        runner, "integrate_new_node_into_cluster",
        MagicMock(side_effect=RuntimeError("expand boom")))
    return task


def _spec_node_add(runner, monkeypatch):
    task = _make_task(JobSchedule.FN_NODE_ADD)
    _wire_base(runner, monkeypatch, task)
    monkeypatch.setattr(runner, "ThreadPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(runner, "_inflight", set())
    monkeypatch.setattr(runner.tasks_controller, "claim_task",
                        lambda *a, **k: True)
    # add_node fails (returns falsy) every cycle.
    monkeypatch.setattr(runner.storage_node_ops, "add_node",
                        MagicMock(return_value=False))
    return task


def _spec_replication_final(runner, monkeypatch):
    task = _make_task(
        JobSchedule.FN_REPLICATION_FINAL,
        lvol_id="lv-1", tgt_node_id="tgt-1", src_node_id="src-1")
    db, _cluster, node = _wire_base(runner, monkeypatch, task)
    # Target node never comes online -> cutover cannot proceed, retry each poll.
    node.status = StorageNode.STATUS_OFFLINE
    db.get_lvol_by_id.return_value = MagicMock()
    return task


def _spec_jc_comp(runner, monkeypatch):
    task = _make_task(JobSchedule.FN_JC_COMP_RESUME)
    _wire_base(runner, monkeypatch, task)
    # A task is always active on the same node -> resume is deferred, retry each
    # poll (this is the branch that increments task.retry).
    monkeypatch.setattr(runner.tasks_controller, "get_active_node_tasks",
                        lambda *a, **k: [MagicMock()])
    return task


def _spec_restart(runner, monkeypatch):
    task = _make_task(JobSchedule.FN_NODE_RESTART)
    _db, _cluster, node = _wire_base(runner, monkeypatch, task)
    # Node is offline and stays unreachable -> restart keeps failing, retry
    # each poll.
    node.status = StorageNode.STATUS_OFFLINE
    monkeypatch.setattr(runner, "_restart_pool", _InlineExecutor())
    monkeypatch.setattr(runner, "_restart_next_attempt", {})
    monkeypatch.setattr(runner, "_restart_inflight", {})
    monkeypatch.setattr(runner, "_node_inflight", {})
    monkeypatch.setattr(runner.tasks_controller, "claim_task",
                        lambda *a, **k: True)
    monkeypatch.setattr(runner.tasks_controller, "is_auto_restart_paused",
                        lambda *a, **k: False)
    monkeypatch.setattr(runner.tasks_controller, "add_node_to_auto_restart",
                        MagicMock())
    monkeypatch.setattr(runner.storage_node_ops, "set_node_status", MagicMock())
    # Node never reachable -> the reachability check fails and retry advances.
    monkeypatch.setattr(runner.health_controller, "_check_node_ping",
                        lambda *a, **k: False)
    monkeypatch.setattr(runner.health_controller, "_check_node_api",
                        lambda *a, **k: False)
    return task


# name -> spec for the runners driven through their real main() loop.
_MAIN_DRIVEN_SPECS = {
    "tasks_runner_cluster_expand.py": _spec_cluster_expand,
    "tasks_runner_node_add.py": _spec_node_add,
    "tasks_runner_replication_final.py": _spec_replication_final,
    "tasks_runner_jc_comp.py": _spec_jc_comp,
    "tasks_runner_restart.py": _spec_restart,
}

# Retry-driven runners covered by a dedicated test elsewhere rather than the
# generic drive-main harness. The backup runner's loop is inline under
# ``if __name__ == '__main__'`` (no ``def main()``), so it is exercised at the
# ``process_task`` level by ``test_backup_*`` above.
_COVERED_ELSEWHERE = {
    "tasks_runner_backup.py": "driven via process_task in test_backup_* above",
}

# Runners that increment task.retry but are intentionally UNBOUNDED: the
# migration family is created with max_retry=-1 and gates retries on resource
# recovery (see _migration_retry_allowed) rather than a fixed count. Value is
# the reason, surfaced in the skip message.
INTENTIONALLY_UNBOUNDED = {
    "tasks_runner_migration.py": "created with max_retry=-1; retry gated on resource recovery",
    "tasks_runner_failed_migration.py": "created with max_retry=-1; retry gated on resource recovery",
    "tasks_runner_new_dev_migration.py": "created with max_retry=-1; retry gated on resource recovery",
    "tasks_runner_lvol_migration.py": "created with max_retry=-1; retry gated on resource recovery",
}

_INCREMENTS_RETRY = re.compile(r"\.retry\s*\+=\s*1")


def _runner_files():
    import simplyblock_core.services as services_pkg
    services_dir = Path(services_pkg.__file__).parent
    files = sorted(services_dir.glob("tasks_runner_*.py"))
    assert files, f"no task runners found under {services_dir}"
    return files


def _retry_driven_runner_files():
    """Every runner whose source increments task.retry — the set that must
    enforce a ceiling. Used only to *discover* which runners to parametrise
    over; the ceiling itself is verified behaviourally per runner below."""
    return [p for p in _runner_files() if _INCREMENTS_RETRY.search(p.read_text())]


@pytest.mark.parametrize(
    "runner_file", _retry_driven_runner_files(), ids=lambda p: p.name)
def test_runner_enforces_retry_ceiling(runner_file, monkeypatch):
    """One case per retry-driven runner (discovered from the source tree): drive
    its real loop with the work mocked to fail forever and assert the task
    terminates at the ceiling instead of looping.

    A newly-added retry-driven runner automatically appears here as a failing
    case until it is either given a spec in ``_MAIN_DRIVEN_SPECS`` (or a
    dedicated test noted in ``_COVERED_ELSEWHERE``) or documented as
    intentionally unbounded — so coverage can't silently drift."""
    name = runner_file.name
    if name in INTENTIONALLY_UNBOUNDED:
        pytest.skip(f"{name}: intentionally unbounded — {INTENTIONALLY_UNBOUNDED[name]}")
    if name in _COVERED_ELSEWHERE:
        pytest.skip(f"{name}: {_COVERED_ELSEWHERE[name]}")

    spec = _MAIN_DRIVEN_SPECS.get(name)
    assert spec is not None, (
        f"No retry-ceiling test for {name}. Add a spec to _MAIN_DRIVEN_SPECS "
        "that mocks the runner's work to fail and returns the task, or — if the "
        "runner is intentionally unbounded (max_retry=-1) — add it to "
        "INTENTIONALLY_UNBOUNDED with a reason."
    )

    module_name = f"simplyblock_core.services.{name[:-len('.py')]}"
    runner = importlib.import_module(module_name)
    task = spec(runner, monkeypatch)
    _assert_terminates_at_ceiling(runner, task)


def test_registries_are_not_stale():
    """Renaming/removing a listed runner must force revisiting the registries,
    otherwise a stale entry would silently stop matching any real runner."""
    names = {p.name for p in _runner_files()}
    listed = (set(_MAIN_DRIVEN_SPECS)
              | set(_COVERED_ELSEWHERE)
              | set(INTENTIONALLY_UNBOUNDED))
    missing = listed - names
    assert not missing, f"listed runners no longer exist: {missing}"
