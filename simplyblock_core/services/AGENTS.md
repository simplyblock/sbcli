# AGENTS.md — background services

Health checks, snapshot/lvol/storage-node monitors, and the task runners (`tasks_runner_*.py`)
that drive multi-cycle work: backup, restore, merge, migration, node add/removal, restart, ...

## Task runners

Every `tasks_runner_*.py` migrated onto the shared driver reduces to a `handler(task) -> None`
that signals its outcome through control flow alone — `TaskDefer`/`TaskProgress` (not ready, no
retry consumed), `TaskRetry` (retryable), `TaskAbort` (permanent). Read `task_runner_base.py`'s
module docstring before touching a handler; it is the one place this contract is written down.

**The driver owns the retry ceiling. A handler must not keep its own counter.** Raising
`TaskRetry` already increments `task.retry` and checks it against `task.max_retry`, with backoff,
centrally — `task_runner_base.py` exists specifically to replace what every runner used to
hand-roll separately ("a retry ceiling ... which drifted apart", its own docstring). A
`function_params` counter alongside `TaskRetry` is two ceilings enforcing one decision. A task
type that should give up sooner than its siblings gets a lower `max_retry` of its own instead —
see `BACKUP_TASK_MAX_RETRIES` beside `BACKUP_MAX_RETRIES` in `constants.py`.

`tests/unit/tasks/test_retry_ceiling.py` checks this behaviourally, per runner, by driving each
one's real loop with its work mocked to fail forever: a newly-added retry-driven runner shows up
there as a failing case until it is covered.
