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

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_migration_group import LVolMigrationGroup
from simplyblock_core.models.storage_node import StorageNode


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
    # Dict-ish cluster state must be stubbed explicitly: an unstubbed MagicMock
    # attribute auto-creates, so `cluster.<field>.get(key)` returns a truthy
    # mock and any guard reading it silently flips on. That is what happened
    # when the JC-compression upgrade hold landed -- resume_is_held() saw a
    # phantom in-progress upgrade, the runner suspended the task every cycle
    # and never reached its retry ceiling. These clusters are deliberately
    # "not mid-upgrade", which is the state the ceiling contract is about.
    cluster.release_upgrade_state = {}

    node = db.get_storage_node_by_id.return_value
    node.status = StorageNode.STATUS_ONLINE
    node.data_nics = []
    node.nvme_devices = []

    db.get_clusters.return_value = [cluster]
    db.get_job_tasks.return_value = [task]
    db.get_task_by_id.return_value = task
    db.get_cluster_by_id.return_value = cluster
    db.get_storage_nodes_by_cluster_id.return_value = [node]
    # Mirror DBController.atomic_update's contract: apply the mutator to the
    # (fresh) object and return it. The restart runner's task writes go
    # through this instead of write_to_db (stale-copy lost-update fix).
    db.atomic_update.side_effect = lambda obj, fn: (fn(obj), obj)[1]
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
    # Every runner claims the lease before running a task; grant it so the loop
    # reaches the work under test instead of skipping the task as foreign-owned.
    monkeypatch.setattr(runner.tasks_controller, "claim_task", lambda *a, **k: True)
    return db, cluster, node


# Per-runner setup for the drive-main test. Each spec builds a task whose work
# fails every cycle and mocks exactly the collaborators that runner touches, then
# returns the task. The setup is necessarily runner-specific because each runner
# calls different DB accessors and different work functions — there is no generic
# "make it fail" that fits every runner. What is NOT hand-maintained is *which*
# runners get tested: the parametrised test below discovers them from the source
# tree, so a new retry-driven runner shows up as a failing case until a spec is
# added here.

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


def _spec_batch_migration(runner, monkeypatch):
    task = _make_task(JobSchedule.FN_LVOL_BATCH_MIG, group_id="grp-1")
    db, _cluster, _ = _wire_base(runner, monkeypatch, task)

    group = MagicMock()
    group.phase = LVolMigrationGroup.PHASE_SNAP_COPY
    group.source_node_id = "src-1"
    group.target_node_id = "tgt-1"
    group.cluster_id = "cl-1"
    group.members = [{"migration_id": "mig-1"}]
    db.get_migration_group_by_id.return_value = group
    # The main loop calls get_active_batch_migration_tasks, not get_job_tasks.
    db.get_active_batch_migration_tasks.return_value = [task]

    # Worker migrations appear terminal so CLEANUP_TARGET can complete.
    worker_mig = MagicMock()
    worker_mig.is_active.return_value = False
    db.get_migration_by_id.return_value = worker_mig

    # Source is offline (retry path); target is online (not the fast-fail path).
    src_node = MagicMock()
    src_node.status = StorageNode.STATUS_OFFLINE
    src_node.get_id.return_value = "src-1"
    tgt_node = MagicMock()
    tgt_node.status = StorageNode.STATUS_ONLINE
    tgt_node.get_id.return_value = "tgt-1"

    def _get_node(node_id):
        if node_id == "src-1":
            return src_node
        return tgt_node

    db.get_storage_node_by_id.side_effect = _get_node
    # _make_rpc is imported into the runner module; stub it so no real connections.
    monkeypatch.setattr(runner, "_make_rpc", MagicMock())
    # Stub collaborators that hit real infrastructure (DB, events, network).
    monkeypatch.setattr(runner.tasks_controller, "get_active_cluster_expand_task",
                        lambda *a, **k: False)
    # main() lease-gates each task via claim_task before running it (so two
    # runner replicas can't both drive the same group); without this the real
    # claim_task hits the module's own uninitialized DBController, "loses" the
    # claim every time, and main() skips task_runner forever.
    monkeypatch.setattr(runner.tasks_controller, "claim_task",
                        lambda *a, **k: True)
    monkeypatch.setattr(runner, "_delete_target_subsystem", MagicMock())
    monkeypatch.setattr(runner, "migration_events", MagicMock())
    monkeypatch.setattr(runner, "tasks_events", MagicMock())
    return task


# name -> spec for the runners driven through their real main() loop.
_MAIN_DRIVEN_SPECS = {
    "tasks_runner_restart.py": _spec_restart,
    "tasks_runner_batch_migration.py": _spec_batch_migration,
}

# Retry-driven runners covered by a dedicated test elsewhere rather than the
# generic drive-main harness. The backup runner's loop is inline under
# ``if __name__ == '__main__'`` (no ``def main()``), so it is exercised at the
# ``process_task`` level by ``test_backup_*`` above.
_COVERED_ELSEWHERE: dict = {}

# Runners migrated onto the shared driver (``task_runner_base``). They no longer
# own a loop or a retry counter — the driver enforces the ceiling for all of
# them at once, covered by tests/unit/tasks/test_task_runner_base.py. They are
# therefore not discovered by _retry_driven_runner_files(); listing them here
# keeps that disappearance deliberate rather than silent, and
# test_migrated_runners_delegate_retry below pins that they really did hand the
# retry counter over.
_DRIVER_MIGRATED = {
    "tasks_runner_fdb_backup.py",
    "tasks_runner_jc_comp.py",
    "tasks_runner_replication_final.py",
    "tasks_runner_sync_lvol_del.py",
    "tasks_runner_backup.py",
    "tasks_runner_cluster_expand.py",
    "tasks_runner_node_add.py",
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
    "tasks_runner_node_removal.py": "created with max_retry=-1; multi-hour removal gated on failure-migration completion",
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
    # encoding="utf-8": the repo is UTF-8; Path.read_text() defaults to the
    # platform codec (cp1252 on Windows), which chokes on runner sources
    # containing non-latin-1 characters.
    return [p for p in _runner_files()
            if _INCREMENTS_RETRY.search(p.read_text(encoding='utf-8'))]


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
              | set(INTENTIONALLY_UNBOUNDED)
              | _DRIVER_MIGRATED)
    missing = listed - names
    assert not missing, f"listed runners no longer exist: {missing}"


@pytest.mark.parametrize("name", sorted(_DRIVER_MIGRATED))
def test_migrated_runners_delegate_retry(name):
    """A runner listed as migrated must not have kept a retry counter of its
    own: the driver owns task.retry, and a runner that still increments it
    would be applying two ceilings at once."""
    source = (Path(_runner_files()[0]).parent / name).read_text(encoding='utf-8')
    assert not _INCREMENTS_RETRY.search(source), (
        f"{name} is listed as migrated to task_runner_base but still increments "
        "task.retry itself")
    assert "serve(SPEC)" in source, (
        f"{name} is listed as migrated to task_runner_base but does not run the "
        "shared driver")
