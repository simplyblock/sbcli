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

DB errors are deliberately NOT caught: an unhandled ``get_clusters`` /
``get_job_tasks`` failure propagates out of :func:`serve`, exits the process
non-zero, and lets the orchestrator restart it with a fresh FDB connection.
"""
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

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("concurrency must be >= 1")


class TaskRunner:
    """Drives the tasks matched by a :class:`RunnerSpec`. See module docstring
    for the handler contract."""

    def __init__(self, spec: RunnerSpec):
        self.spec = spec
        self._executor = (
            ThreadPoolExecutor(max_workers=spec.concurrency,
                               thread_name_prefix=spec.name)
            if spec.concurrency > 1 else None)
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
            time.sleep(self.spec.interval)

    # -- dispatch -----------------------------------------------------------

    def _dispatch(self, task: JobSchedule, cluster: Any) -> None:
        uuid = task.uuid
        # Backoff gate: a task not yet due is skipped so a waiting task does not
        # block the others behind it (the loop revisits every task each cycle).
        if time.time() < self._next_attempt.get(uuid, 0):
            return

        if self._executor is None:
            self._process(task, cluster)
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
        self._executor.submit(self._process_worker, task, cluster)

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

        # Authoritative re-fetch AFTER the claim: claim_task mutated the DB copy
        # (owner / updated_at) but not this local object, and write_to_db writes
        # the whole object — reading the fresh, lease-stamped copy here is what
        # keeps a later write from clobbering the owner.
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
            task.status = JobSchedule.STATUS_RUNNING
            task.write_to_db(db.kv_store)

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

    def _succeed(self, task: JobSchedule) -> None:
        task.status = JobSchedule.STATUS_DONE
        if not task.function_result:
            task.function_result = "completed"
        task.write_to_db(db.kv_store)
        self._forget(task.uuid)

    def _finish(self, task: JobSchedule, result: str) -> None:
        """Terminal DONE for a non-handler-success reason (canceled, max retry,
        abort)."""
        task.function_result = result
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        self._forget(task.uuid)

    def _defer(self, task: JobSchedule, reason: str) -> None:
        if reason:
            task.function_result = reason
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        self._clear_backoff(task.uuid)

    def _fail(self, task: JobSchedule, reason: str) -> None:
        task.retry += 1
        task.function_result = reason
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        with self._lock:
            self._next_attempt[task.uuid] = time.time() + self._backoff_delay(task.retry)

    # -- bookkeeping --------------------------------------------------------

    def _backoff_delay(self, retry: int) -> float:
        if retry <= 0:
            return 0.0
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
