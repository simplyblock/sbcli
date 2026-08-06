# coding=utf-8
"""Poll-loop base class for DB-backed task runners.

Encapsulates the loop skeleton that every ``tasks_runner_*`` service used to
hand-roll: sweep clusters → read the cluster's task table → filter by
function name → skip done → re-read (cancel may have raced) → honor the
retry ceiling → claim the host lease → mark RUNNING → execute under a lease
heartbeat → record the outcome — plus the cross-cutting behaviors that were
only ever implemented in *some* runners:

- **DB-wedge self-restart**: a persistent read failure — or an unexpectedly
  empty cluster list — on a long-lived process means the DB client is wedged
  (the FDB client caches the Database per process; only a fresh process
  recovers). After ``db_failure_threshold`` consecutive failures the runner
  exits(1) so the orchestrator restarts it with a clean connection.
- **Retry backoff**: an in-memory per-task next-attempt gate with exponential
  doubling, capped at ``retry_backoff_max_sec``.

Duck-typed dependencies (no model imports here):

- ``db`` needs ``get_clusters()``, ``get_job_tasks(cluster_id)``,
  ``get_task_by_id(uuid)`` and ``kv_store`` (passed to ``task.write_to_db``).
- task objects are JobSchedule-shaped: ``uuid``, ``status``, ``canceled``,
  ``retry``, ``max_retry``, ``function_name``, ``function_result``,
  ``write_to_db(kv_store)``.

Subclasses implement ``execute(task) -> Optional[TaskResult]`` and may
override ``on_canceled(task)`` for cleanup. ``execute`` returning ``None``
means "the task body managed the record itself (or wants an unconditional
re-poll next cycle)" — the runner writes nothing.
"""

import contextlib
import logging
import sys
import time

STATUS_NEW = 'new'
STATUS_RUNNING = 'running'
STATUS_SUSPENDED = 'suspended'
STATUS_DONE = 'done'

DEFAULT_DB_FAILURE_THRESHOLD = 60


class TaskResult:
    """Outcome of one ``execute()`` attempt."""

    DONE = 'done'
    RETRY = 'retry'
    SUSPEND = 'suspend'

    def __init__(self, kind, message=''):
        self.kind = kind
        self.message = message

    @classmethod
    def done(cls, message=''):
        """Terminal: mark the task done with ``message`` as function_result."""
        return cls(cls.DONE, message)

    @classmethod
    def retry(cls, message=''):
        """Failed attempt: consume one retry and re-attempt after backoff."""
        return cls(cls.RETRY, message)

    @classmethod
    def suspend(cls, message=''):
        """Defer without consuming a retry (e.g. a precondition isn't met yet)."""
        return cls(cls.SUSPEND, message)


class TaskRunner:
    """Base class for a single-task-family poll-loop runner service."""

    # Task function names this runner processes; override in subclasses or
    # pass function_names= to __init__.
    function_names: tuple = ()

    def __init__(self, db, lease=None, *, function_names=None,
                 interval_sec=10, error_interval_sec=3,
                 db_failure_threshold=DEFAULT_DB_FAILURE_THRESHOLD,
                 retry_backoff_base_sec=None, retry_backoff_max_sec=3600,
                 cluster_filter=None, logger=None,
                 sleep=time.sleep, monotonic=time.monotonic):
        if function_names is not None:
            self.function_names = tuple(function_names)
        if not self.function_names:
            raise ValueError("TaskRunner requires at least one task function name")
        self._db = db
        self._lease = lease
        self.interval_sec = interval_sec
        self.error_interval_sec = error_interval_sec
        self.db_failure_threshold = db_failure_threshold
        self.retry_backoff_base_sec = retry_backoff_base_sec
        self.retry_backoff_max_sec = retry_backoff_max_sec
        # cluster_filter(cluster) -> bool; False skips the cluster this cycle
        # (e.g. sbcli runners skip clusters in activation).
        self._cluster_filter = cluster_filter
        self._logger = logger or logging.getLogger(type(self).__name__)
        self._sleep = sleep
        self._monotonic = monotonic
        self._consecutive_db_failures = 0
        self._next_attempt_at: dict = {}  # task uuid -> monotonic deadline

    # ------------------------------------------------------------------ hooks

    def execute(self, task):
        """Run one attempt of ``task``; return a TaskResult, or None if the
        task body already wrote its own outcome. Exceptions are logged and the
        task is retried on the next cycle without consuming a retry."""
        raise NotImplementedError

    def on_canceled(self, task):
        """Cleanup hook invoked before a canceled task is finalized."""

    # -------------------------------------------------------------- machinery

    def run_forever(self):
        self._logger.info(f"Starting {type(self).__name__} for {list(self.function_names)}...")
        while True:
            self.run_cycle()
            self._sleep(self.interval_sec)

    def run_cycle(self):
        """One sweep over all clusters' task tables."""
        try:
            clusters = self._db.get_clusters()
        except Exception as e:
            self._register_db_failure(f"Failed to get clusters: {e}")
            return
        if not clusters:
            self._register_db_failure("No clusters found!")
            return

        for cluster in clusters:
            if self._cluster_filter is not None and not self._cluster_filter(cluster):
                continue
            try:
                tasks = self._db.get_job_tasks(cluster.get_id())
            except Exception as e:
                self._register_db_failure(
                    f"Failed to read tasks for cluster {cluster.get_id()}: {e}")
                continue
            self._consecutive_db_failures = 0
            for task in tasks:
                if task.function_name not in self.function_names:
                    continue
                if task.status == STATUS_DONE:
                    continue
                try:
                    self.process_task(task)
                except Exception as e:
                    self._logger.error(f"Task {task.uuid} crashed: {e}")
                    self._logger.exception(e)

    def process_task(self, task):
        """Drive one task through cancel/retry-ceiling/claim/execute/outcome."""
        # Re-read: it may have been canceled or finished concurrently.
        task = self._db.get_task_by_id(task.uuid)
        if task.status == STATUS_DONE:
            return

        if task.canceled:
            self.on_canceled(task)
            self._finalize(task, "canceled")
            return

        if 0 <= task.max_retry <= task.retry:
            self._finalize(task, "max retry reached, stopping task")
            return

        deadline = self._next_attempt_at.get(task.uuid)
        if deadline is not None and self._monotonic() < deadline:
            return  # backing off

        if self._lease is not None and not self._lease.claim(task):
            return  # another live runner host owns it

        if task.status != STATUS_RUNNING:
            task.status = STATUS_RUNNING
            task.write_to_db(self._db.kv_store)

        heartbeat = (self._lease.heartbeat(task) if self._lease is not None
                     else contextlib.nullcontext())
        with heartbeat:
            result = self.execute(task)

        if result is None:
            return
        if result.kind == TaskResult.DONE:
            self._finalize(task, result.message)
        elif result.kind == TaskResult.RETRY:
            task.retry += 1
            task.function_result = result.message
            task.write_to_db(self._db.kv_store)
            self._schedule_backoff(task)
        elif result.kind == TaskResult.SUSPEND:
            task.status = STATUS_SUSPENDED
            task.function_result = result.message
            task.write_to_db(self._db.kv_store)
        else:
            raise ValueError(f"Unknown task result kind: {result.kind!r}")

    def _finalize(self, task, message):
        task.function_result = message
        task.status = STATUS_DONE
        task.write_to_db(self._db.kv_store)
        self._next_attempt_at.pop(task.uuid, None)

    def _schedule_backoff(self, task):
        if not self.retry_backoff_base_sec:
            return
        delay = min(self.retry_backoff_base_sec * (2 ** max(task.retry - 1, 0)),
                    self.retry_backoff_max_sec)
        self._next_attempt_at[task.uuid] = self._monotonic() + delay

    def _register_db_failure(self, message):
        """Count a failed DB sweep; exit for a clean restart once the client is
        presumed wedged (the orchestrator restarts the service)."""
        self._consecutive_db_failures += 1
        self._logger.error(f"{message} ({self._consecutive_db_failures})")
        if (self.db_failure_threshold is not None
                and self._consecutive_db_failures >= self.db_failure_threshold):
            self._logger.error(
                "DB unreadable for too long (client likely wedged); "
                "exiting for a clean restart")
            sys.exit(1)
        self._sleep(self.error_interval_sec)
