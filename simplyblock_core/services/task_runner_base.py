# coding=utf-8
"""Shared driver for the task runners.

A task runner is a long-lived service that polls FoundationDB for `JobSchedule`
tasks of one or more function names and advances each one. Historically every
runner hand-rolled its own ``while True`` loop, lease handling, retry ceiling and
error plumbing, which drifted apart. This module centralizes that skeleton so a
runner is reduced to a :class:`RunnerSpec` — most importantly a *handler* that
does only the domain work.

Handler contract
----------------
The handler is a callable ``handler(task) -> None``. It performs its domain work
(including mutating its own domain models — Backup, LVol, migration, … — and
writing those) but it MUST NOT touch task lifecycle state (``status`` /
``retry``) or call ``task.write_to_db`` for the task: the driver owns all of
that. The handler signals its outcome purely through ordinary Python control
flow:

- **return** (``None``) — the task is terminally complete → ``STATUS_DONE``.
- **raise** :class:`TaskDefer` — not terminal: still in progress, or blocked on
  external state. Re-poll next cycle; **no retry consumed**; no backoff.
- **raise** :class:`TaskRetry` (or any other, unexpected ``Exception``) — a
  retryable failure. Suspend, **consume a retry**, and back off before the next
  attempt.
- **raise** :class:`TaskAbort` — a permanent, non-retryable stop (missing param,
  object gone, "not needed"). Finish the task (``STATUS_DONE``) with the reason.

Two task fields ARE the handler's to set: ``function_result`` (the message the
outcome is recorded with) and ``function_params`` (where a multi-cycle handler
records progress — ``recovery_started``, ``merge_started``, ``fail_count``).
Both are carried onto the row when the driver commits the outcome.

A handler that does something destructive should re-read the task immediately
before doing it. The driver's pre-run re-fetch is authoritative for the
*lifecycle* decisions it makes, but it happens before the handler starts, and by
the time a long handler reaches its point of no return the task may have been
canceled.

DB errors are deliberately NOT caught: an unhandled ``get_clusters`` /
``get_job_tasks`` failure propagates out of :func:`serve`, exits the process
non-zero, and lets the orchestrator restart it with a fresh FDB connection.

Task writes are compare-and-set, never full-object writes: see
:meth:`TaskRunner._cas`.
"""
import datetime
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule

logger = utils.get_logger(__name__)

db = db_controller.DBController()

# Cap the per-task exponential backoff so a permanently-failing task can't grow
# its retry delay without bound.
_BACKOFF_CAP_SEC = constants.RESTART_TASK_EXEC_INTERVAL_MAX_SEC


class TaskDefer(Exception):
    """Handler signal: the task is not terminal — still in progress, or blocked
    on external state. Re-poll next cycle without consuming a retry."""


class TaskRetry(Exception):
    """Handler signal: a retryable failure. Suspend, consume a retry, back off.

    Any other unexpected ``Exception`` from a handler is treated identically."""


class TaskAbort(Exception):
    """Handler signal: a permanent, non-retryable stop. Finish the task."""


def _default_eligible(task: JobSchedule, cluster: Any) -> bool:
    return True


def _commit(task: JobSchedule, apply: Callable[[JobSchedule], None],
            terminal: bool = False, context: str = "task-runner") -> Optional[JobSchedule]:
    """Commit a change onto the task row as it exists NOW.

    Never a full-object ``write_to_db`` of ``task``: that copy was read before
    the handler ran, and a handler runs for minutes (node add, restart,
    migration). Writing it back reinstates every field another actor changed
    meanwhile — it un-cancels a task that ``cancel_pending_node_restart_tasks``
    canceled when the node came back ONLINE, and reclaims a lease another host
    has since taken. That pair of lost updates is what re-ran a restart against
    an already-recovered node (2026-07-29 double restart).

    Only the two fields a handler owns are carried over from ``task``:
    ``function_result`` and ``function_params`` (where handlers record progress
    like ``recovery_started`` / ``merge_started``, which must survive so the
    next attempt does not repeat the step).

    A terminal commit may finish a canceled task — that IS the cancellation
    being carried out; a non-terminal one would be reviving it.

    Returns the committed task, or None if another actor already owns the
    outcome — the caller must stop driving it.
    """
    result = task.function_result
    params = task.function_params
    now = str(datetime.datetime.now(datetime.timezone.utc))
    won = {"ok": False}

    def _mutate(fresh: JobSchedule):
        if fresh.status == JobSchedule.STATUS_DONE:
            return False
        if not terminal and fresh.canceled:
            return False
        fresh.function_result = result
        fresh.function_params = params
        apply(fresh)
        fresh.updated_at = now
        won["ok"] = True
        return True

    committed = db.atomic_update(task, _mutate)
    if committed is None or not won["ok"]:
        logger.info(f"{context}: task {task.uuid} was finished or canceled "
                    f"concurrently; another actor owns the outcome")
        return None
    return committed


def checkpoint(task: JobSchedule, **params) -> Optional[JobSchedule]:
    """Record handler progress on the task, mid-handler.

    For a long handler with a step that must not be repeated — a cleanup
    shutdown, an issued transfer — mark it done the moment it succeeds rather
    than when the handler returns, where a crash in between would lose the fact
    and repeat the step on the next attempt.

    Doubles as the cancellation probe such a handler needs anyway: returns the
    fresh task to carry on with, or None if the task was canceled or finished
    underneath it, in which case the handler must stop rather than proceed to
    the next destructive step.
    """
    def _apply(fresh: JobSchedule) -> None:
        fresh.function_params = dict(fresh.function_params, **params)

    return _commit(task, _apply)


@dataclass
class RunnerSpec:
    """Describes one task runner. ``function_names`` and ``handler`` are the only
    required fields; the rest default to a simple serial runner."""

    function_names: Sequence[str]
    handler: Callable[[JobSchedule], None]
    name: str = "task-runner"
    # Pure, side-effect-free "can I run this task right now?" predicate. The
    # default always-eligible keeps simple runners trivial. A task judged
    # ineligible is skipped this cycle without a lease claim or a write, exactly
    # like the ad-hoc IN_ACTIVATION / same-node-sibling gates it replaces.
    is_eligible: Callable[[JobSchedule, Any], bool] = _default_eligible
    interval: float = constants.TASK_EXEC_INTERVAL_SEC
    # Serial by default. > 1 runs tasks on a thread pool of this size.
    concurrency: int = 1
    # Optional per-key mutual exclusion for concurrent mode: two tasks whose
    # exclusion_key() is equal never run at the same time (e.g. one restart per
    # node). Ignored when concurrency == 1.
    exclusion_key: Optional[Callable[[JobSchedule], Any]] = None
    # Optional cleanup, called once the task has reached STATUS_DONE and been
    # written — whichever way it got there (handler success, TaskAbort, cancel
    # or retry ceiling). For releasing state the task held, which would
    # otherwise leak on the terminal paths the handler never sees. Runs only
    # for the caller that won the terminal transition.
    on_finish: Optional[Callable[[JobSchedule], None]] = None
    # Optional per-task, per-cycle "must this one run to completion before the
    # loop moves on?". Defaults to serializing exactly when the pool has a
    # single worker. A runner whose mode depends on live cluster state (node
    # restart fans out only for a drained suspension or a fully-dead failure
    # domain) supplies a predicate instead of a fixed concurrency.
    serialize: Optional[Callable[[JobSchedule, Any], bool]] = None
    # Optional per-cluster work, run once each cycle after that cluster's tasks
    # are dispatched. For upkeep a runner owns that is not attached to any task
    # — the restart runner's watchdog for nodes left in a transitional state
    # with no task owning them. Failures are logged, never fatal.
    on_cycle: Optional[Callable[[Any], None]] = None
    # Optional delay-before-next-attempt for a task that consumed a retry,
    # given the new retry count. Defaults to interval * 2**(retry-1), capped.
    # A runner whose recovery curve is tuned to its own workload (node restart
    # holds a steady lead-in cadence before backing off) supplies its own.
    backoff: Optional[Callable[[int], float]] = None

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")


class TaskRunner:
    """Drives the tasks matched by a :class:`RunnerSpec`. See module docstring
    for the handler contract."""

    def __init__(self, spec: RunnerSpec):
        self.spec = spec
        self._executor = ThreadPoolExecutor(max_workers=spec.concurrency,
                                            thread_name_prefix=spec.name)
        self._lock = threading.Lock()
        # task uuid -> Future-in-flight guard (this host), so the dispatch loop
        # never hands the same task to two workers. Cross-host duplicate
        # execution is prevented separately by the per-task lease.
        self._inflight: set = set()
        self._inflight_keys: dict = {}   # exclusion key -> task uuid
        self._next_attempt: dict = {}     # task uuid -> earliest retry timestamp

    # -- public entrypoint --------------------------------------------------

    def run(self) -> None:
        logger.info(f"Starting {self.spec.name}...")
        while True:
            # DB errors are intentionally uncaught: they propagate out, exit the
            # process, and the orchestrator restarts us with a fresh FDB client.
            clusters = db.get_clusters()
            if not clusters:
                logger.error("No clusters found!")
            else:
                for cl in clusters:
                    for task in db.get_job_tasks(cl.get_id(), reverse=False):
                        if task.function_name not in self.spec.function_names:
                            continue
                        if task.status == JobSchedule.STATUS_DONE:
                            self._forget(task.uuid)
                            continue
                        self._dispatch(task, cl)
                    self._run_cycle_hook(cl)
            time.sleep(self.spec.interval)

    def _run_cycle_hook(self, cluster: Any) -> None:
        if self.spec.on_cycle is None:
            return
        # Upkeep failing must not stop the loop from serving tasks — but a DB
        # error still propagates, since that means the process should exit.
        try:
            self.spec.on_cycle(cluster)
        except Exception as e:  # noqa: BLE001 - upkeep failure is not fatal
            logger.error(f"{self.spec.name}: cycle hook failed for "
                         f"cluster {cluster.get_id()}: {e}")
            logger.exception(e)

    # -- dispatch -----------------------------------------------------------

    def _dispatch(self, task: JobSchedule, cluster: Any) -> None:
        uuid = task.uuid
        # Backoff gate: a task not yet due is skipped so a waiting task does not
        # block the others behind it (the loop revisits every task each cycle).
        if time.time() < self._next_attempt.get(uuid, 0):
            return

        with self._lock:
            if uuid in self._inflight:
                return
            key = self.spec.exclusion_key(task) if self.spec.exclusion_key else None
            if key is not None and key in self._inflight_keys:
                return
            self._inflight.add(uuid)
            if key is not None:
                self._inflight_keys[key] = uuid

        # Single dispatch path: serialized execution submits to the pool and
        # waits, rather than running inline. A split — one branch registering
        # in-flight and another not — is what let a dispatch-mode flip
        # mid-restart re-enter a task that was still running, and force-shut an
        # already-recovered node (2026-07-29 double restart). Going through the
        # registry either way makes a flip harmless in both directions.
        future = self._executor.submit(self._process_worker, task, cluster)
        if self._serialized(task, cluster):
            future.result()

    def _serialized(self, task: JobSchedule, cluster: Any) -> bool:
        if self.spec.serialize is not None:
            return self.spec.serialize(task, cluster)
        return self.spec.concurrency == 1

    def _process_worker(self, task: JobSchedule, cluster: Any) -> None:
        # A worker crash must be contained to this task, never kill the service
        # loop or leave the task wedged in the in-flight set.
        try:
            self._process(task, cluster)
        except Exception as e:  # noqa: BLE001 - contain crash to this worker
            logger.error(f"{self.spec.name}: task {task.uuid} crashed in worker: {e}")
            logger.exception(e)
        finally:
            self._release_inflight(task.uuid)

    # -- per-task lifecycle -------------------------------------------------

    def _process(self, task: JobSchedule, cluster: Any) -> None:
        uuid = task.uuid

        # Pre-run skip-gate 1 — eligibility (pure, no write): not ready yet.
        if not self.spec.is_eligible(task, cluster):
            return

        # Pre-run skip-gate 2 — lease: another live host owns this task.
        if not tasks_controller.claim_task(task):
            logger.info(f"{self.spec.name}: task {uuid} owned by another runner host; skipping")
            return

        # Authoritative re-fetch AFTER the claim: claim_task mutated the DB row
        # (owner / updated_at) but not this local object, and the lifecycle
        # decisions below — canceled, retry ceiling — must be made on the row as
        # it stands, not on whatever the dispatch loop happened to read.
        task = db.get_task_by_id(uuid)
        if task is None or task.status == JobSchedule.STATUS_DONE:
            self._forget(uuid)
            return

        if task.canceled:
            self._finish(task, "canceled")
            return
        if 0 <= task.max_retry <= task.retry:
            self._finish(task, "max retry reached")
            return

        if task.status != JobSchedule.STATUS_RUNNING:
            running = self._cas(task, self._to(JobSchedule.STATUS_RUNNING))
            if running is None:
                self._forget(uuid)
                return
            task = running

        # Drop the previous attempt's result so a task that fails and later
        # succeeds does not finish carrying the stale failure message. Handlers
        # that set a success message overwrite this; the rest get "completed".
        task.function_result = ""

        try:
            # Heartbeat the lease for the duration of the handler: TASK_LEASE_TTL
            # is far shorter than a node-add / restart / migration, so a lease
            # refreshed only on task writes would go stale mid-handler and let a
            # second host claim and double-drive the task.
            with tasks_controller.task_lease_heartbeat(task):
                self.spec.handler(task)
        except TaskDefer as e:
            self._defer(task, str(e))
        except TaskAbort as e:
            self._finish(task, str(e) or "aborted")
        except TaskRetry as e:
            self._fail(task, str(e) or "retry")
        except Exception as e:  # noqa: BLE001 - unexpected == retryable failure
            logger.error(f"{self.spec.name}: task {uuid} handler raised: {e}")
            logger.exception(e)
            self._fail(task, f"unhandled error: {e}")
        else:
            self._succeed(task)

    # -- outcome transitions (the only places task state is mutated) --------

    def _cas(self, task: JobSchedule, apply: Callable[[JobSchedule], None],
             terminal: bool = False) -> Optional[JobSchedule]:
        return _commit(task, apply, terminal=terminal, context=self.spec.name)

    @staticmethod
    def _to(status: str) -> Callable[[JobSchedule], None]:
        def _apply(task: JobSchedule) -> None:
            task.status = status
        return _apply

    def _succeed(self, task: JobSchedule) -> None:
        if not task.function_result:
            task.function_result = "completed"
        self._write_terminal(task)

    def _finish(self, task: JobSchedule, result: str) -> None:
        """Terminal DONE for a non-handler-success reason (canceled, max retry,
        abort)."""
        task.function_result = result
        self._write_terminal(task)

    def _write_terminal(self, task: JobSchedule) -> None:
        committed = self._cas(task, self._to(JobSchedule.STATUS_DONE), terminal=True)
        self._forget(task.uuid)
        if committed is None or self.spec.on_finish is None:
            # Losing the transition means someone else finished the task and
            # owns its cleanup too; running it here would release the resource
            # twice.
            return
        # Cleanup runs after the terminal write, so a hook that inspects the
        # task's own state (a lock held until no active task remains) sees it
        # as finished. A failing hook must not take the loop down with it.
        try:
            self.spec.on_finish(committed)
        except Exception as e:  # noqa: BLE001 - cleanup failure is not fatal
            logger.error(f"{self.spec.name}: task {task.uuid} on_finish failed: {e}")
            logger.exception(e)

    def _defer(self, task: JobSchedule, reason: str) -> None:
        if reason:
            task.function_result = reason
        if self._cas(task, self._to(JobSchedule.STATUS_SUSPENDED)) is None:
            self._forget(task.uuid)
            return
        self._clear_backoff(task.uuid)

    def _fail(self, task: JobSchedule, reason: str) -> None:
        task.function_result = reason

        def _apply(fresh: JobSchedule) -> None:
            fresh.retry += 1
            fresh.status = JobSchedule.STATUS_SUSPENDED

        committed = self._cas(task, _apply)
        if committed is None:
            self._forget(task.uuid)
            return
        # Back off on the committed retry count, not the stale local one.
        with self._lock:
            self._next_attempt[task.uuid] = time.time() + self._backoff_delay(committed.retry)

    # -- bookkeeping --------------------------------------------------------

    def _backoff_delay(self, retry: int) -> float:
        if retry <= 0:
            return 0.0
        if self.spec.backoff is not None:
            return self.spec.backoff(retry)
        exp = min(retry - 1, 16)  # guard the shift against absurd retry counts
        return min(self.spec.interval * (2 ** exp), _BACKOFF_CAP_SEC)

    def _clear_backoff(self, uuid: str) -> None:
        with self._lock:
            self._next_attempt.pop(uuid, None)

    def _release_inflight(self, uuid: str) -> None:
        with self._lock:
            self._inflight.discard(uuid)
            for key, owner_uuid in list(self._inflight_keys.items()):
                if owner_uuid == uuid:
                    del self._inflight_keys[key]

    def _forget(self, uuid: str) -> None:
        with self._lock:
            self._next_attempt.pop(uuid, None)
            self._inflight.discard(uuid)
            for key, owner_uuid in list(self._inflight_keys.items()):
                if owner_uuid == uuid:
                    del self._inflight_keys[key]


def serve(spec: RunnerSpec) -> None:
    """Instantiate and run the driver for ``spec`` (a runner's ``main``)."""
    TaskRunner(spec).run()
