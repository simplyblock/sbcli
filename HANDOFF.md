# Task-runner rework — handoff

Continuation notes for the `rework-task-runners` branch. The full design lives
in **`TASK_RUNNER_REWORK_PLAN.md`** (same dir) — read it first; this file is
just the current state + how to continue.

## Where the work is

Branch `rework-task-runners`, rebased onto `origin/main` (2026-08-05, commit
`4cb16a20b`). Pre-rebase safety backup: branch `backup/rework-pre-rebase3`.

```
fa26b7121  Migrate node add runner…              (B3)
f954be2b9  Migrate backup and cluster expand…    (B2)
d6cc94478  Migrate lvol sync runner…             (B1d)
b436fa79d  Migrate replication cutover runner…   (B1c)
9c9118761  Migrate JC compression resume runner… (B1b)
d17c20064  Migrate FDB backup runner…            (B1a)
3899c2452  Reset task result before each handler attempt
ad4768462  Introduce generic task runner         (B0)
<A5/A3/A2/A1 commits below>
```

## Status: Phase A done, B0 done, B1–B3 done

Migrated onto the driver (7): `fdb_backup`, `jc_comp`, `replication_final`,
`sync_lvol_del`, `backup`, `cluster_expand`, `node_add`.

**Remaining (B4…B9), in order:**

4. migration trio — `migration`, `new_dev_migration`, `failed_migration`
   (serial; `get_active_node_mig_task` / same-node-sibling gating becomes
   `is_eligible`)
5. `port_allow` (large; its `is_eligible` omits the `IN_ACTIVATION` check — the
   documented opt-out — and keeps the recovery logic)
6. `restart` (concurrency + per-node `exclusion_key`; `is_auto_restart_paused`
   and `fd_dead_recovery_allowed` become part of `is_eligible`)
7. `lvol_migration` (largest; migrate the loop/lease/retry shell only, leave the
   snapshot-copy state machine intact)
8. `node_removal` (serial, already leased, unbounded)
9. `batch_migration` (migrate the loop/lease shell, leave group orchestration)

## What the driver grew during B1–B3

Beyond what the plan describes, `task_runner_base` gained two things the
migrations needed:

- **`function_result` is cleared before each handler attempt**, so a task that
  fails and later succeeds doesn't finish carrying the stale failure message.
- **`RunnerSpec.on_finish(task)`** — cleanup called after the task reaches
  STATUS_DONE and is written, on *every* terminal path (success, `TaskAbort`,
  cancel, retry ceiling). Needed because a handler never sees the terminal
  paths the driver owns, yet resources it holds must still be released:
  `sync_lvol_del` frees the primary's del-sync lock there, `backup` fails/
  un-merges the backup resource there. Both are written to be no-ops when the
  handler already finished the resource.

## Gotchas

- **`tests/unit/tasks/test_retry_ceiling.py` no longer hangs** — upstream fixed
  it before this rebase. It parametrizes over runners *discovered from source*
  by the presence of `.retry += 1`, so a runner migrating to the driver silently
  drops out of it. Each migration therefore also moves the runner into that
  file's `_DRIVER_MIGRATED` set, which is asserted to really have handed the
  retry counter over (`test_migrated_runners_delegate_retry`).
- Per-runner behaviour tests for migrated runners live in
  **`tests/unit/tasks/test_runner_specs.py`** (one section per runner: handler
  outcome vocabulary + eligibility + `on_finish`). Extend it as you migrate.
- Several migrations fix latent bugs (a failure path that suspended without ever
  incrementing retry, so a declared `max_retry` could never bind). Each is called
  out in its commit message — keep doing that rather than folding them in
  silently.

## Verification

```bash
tox run-parallel -e lint,types
tox run -e unit -- tests/unit/tasks/ tests/unit/test_lvol_sync_op_task.py
tox run -e unit                    # full unit tier, ~45s, currently green
```

Do not run `tox run` (the integration tier is broken independently of this work).

## Before opening the PR

Delete these two handoff files (`HANDOFF.md`, `TASK_RUNNER_REWORK_PLAN.md`) —
they are transfer artifacts, not part of the change.
