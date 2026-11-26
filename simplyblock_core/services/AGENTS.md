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

**A terminal step that must commit domain state in the same transaction as DONE uses
`RunnerSpec.finish`, never `on_finish`.** `on_finish` runs after the driver's DONE commit, only for a
DONE row, at most once, and its failures are only logged — a crash in between leaves DONE without the
domain state. `finish` replaces the driver's own DONE commit on every terminal ending and is told
why the task ends (`Ending`); it commits DONE together with the domain state onto the row as stored
(the task it gets is frozen, not the row to write back), falls back to `finish_done` for endings it
does not specialise, and returns the row as committed, or None when it committed nothing. It may
also re-arm the task by committing it non-DONE; the driver then runs it again on its next cycle. See
the *Terminal commit* section of `task_runner_base.py`'s docstring, and the sync promote / resync
runners for the two uses.

`tests/unit/tasks/test_retry_ceiling.py` checks this behaviourally, per runner, by driving each
one's real loop with its work mocked to fail forever: a newly-added retry-driven runner shows up
there as a failing case until it is covered.

## Replication: the destination goes through the peer client

`snapshot_replication.py` runs on the **source** cluster's control plane. Everything it does on the
destination goes through the peer client (`simplyblock_core.peer.client`), and the destination's
side runs in `simplyblock_core/peer/service.py`. That covers:

- creating or clearing a landing volume;
- chaining, merging or converting it;
- settling or recovering the destination's leadership;
- reading the destination's records.

The same code serves both deployments:

- **One control plane manages both clusters:** `LocalPeerClient` calls `peer.service` in process.
- **The destination is a peer control plane (the control-plane split):** `HttpPeerClient` calls it
  over `/api/v2/_peer`. This control plane cannot reach the destination's nodes or read its records.

A destination-side call written straight into `snapshot_replication` passes every unit test and
works when one control plane manages both clusters. It fails only across a split. So:

- **Put the operation in `peer/service.py`.** If the source must pass something, add an optional
  field to the request in `peer/protocol.py`, with a default that keeps the old behavior. If the
  source must decide something, give it a field in the response.
- **State that needs both clusters' records in one database runs only behind `_target_is_local`.**
  Spare landing volumes are an example. List it as a gap of the split.

`tests/unit/test_replication_split_boundary.py` enforces this by rejecting destination-side calls
in `snapshot_replication`. `tests/integration/test_peer_service.py` runs the peer API end to end.
