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

## Status: the planned scope is complete

All thirteen runners in scope are on the driver: `fdb_backup`, `jc_comp`,
`replication_final`, `sync_lvol_del`, `backup`, `cluster_expand`, `node_add`,
`restart`, `migration`, `new_dev_migration`, `failed_migration`, `node_removal`,
`port_allow`.

Nothing outside a runner writes task lifecycle state any more:
`utils.handle_task_result` and `tasks_controller.defer_task_for_expansion` are
gone, and the three cancellation paths commit by CAS.

**Deferred to a follow-up PR:** `lvol_migration` and `batch_migration` — under
active upstream rewrite (~17 commits in the last window, several still labelled
`TEMP:`). They keep their own loops meanwhile, which is why
`tasks_cluster_status.py` is not the only file left untouched. Migrating them is
the same exercise: their pre-run guard chains become `is_eligible`, their poll
loops `TaskProgress`, and their `_suspend_task(charge_retry=...)` helper maps
directly onto TaskDefer vs TaskRetry.

## What the driver grew during the migrations

Beyond what the plan describes, `task_runner_base` gained:

- **`function_result` is cleared before each handler attempt**, so a task that
  fails and later succeeds doesn't finish carrying the stale failure message.
- **`RunnerSpec.on_finish(task)`** — cleanup called after the task reaches
  STATUS_DONE and is written, on *every* terminal path (success, `TaskAbort`,
  cancel, retry ceiling). Needed because a handler never sees the terminal
  paths the driver owns, yet resources it holds must still be released:
  `sync_lvol_del` frees the primary's del-sync lock there, `backup` fails/
  un-merges the backup resource there. Both are written to be no-ops when the
  handler already finished the resource.
- **All task writes are compare-and-set** (`_commit`), never full-object
  `write_to_db`. This is not a refinement — the original driver reproduced the
  lost update behind upstream's 2026-07-29 double-restart incident, and held the
  stale copy for the whole handler duration. See the plan's "Upstream
  reconciliation (2026-08)".
- **One dispatch path**: serialized execution submits to the pool and waits
  rather than running inline, so the inflight registry is the single
  mutual-exclusion authority. `RunnerSpec.serialize` is a per-task predicate,
  because restart picks its mode from live cluster state.
- **`checkpoint(task, **params)`** — persist handler progress mid-handler, for a
  destructive step that must not repeat after a crash. Doubles as the
  cancellation probe before the next destructive step.
- **`RunnerSpec.on_cycle(cluster)`** — per-cluster upkeep attached to no task
  (restart's orphaned-node watchdog).
- **`RunnerSpec.backoff(retry)`** — override the default curve where a runner
  has a tuned one (restart's 1-minute lead-in).

## Deployment entry point

`simplyblock-task-runner <name>` (`simplyblock_core/services/task_runners.py`,
declared in `pyproject.toml`) starts a runner by its `RunnerSpec.name` instead of
by a source path inside the image. It resolves the name to a module, checks the
spec's own name matches, and calls `serve(spec)`; `lvol_migration` and
`batch_migration`, which still own their loops, fall back to their module's
`main()` and are listed in `_NOT_ON_DRIVER` until they migrate.

Switched over: every task runner in `docker-compose-swarm.yml`, and the runner
services `cluster_ops.update_cluster_mgmt` creates on upgrade (which meant
`utils.create_docker_service` / `create_k8s_service` now take a `command` list
rather than a source path). Not switched: the non-runner services, the Helm
charts and the operator, which live outside this repository — so the path form
keeps working and `tests/unit/test_service_entrypoints.py` still pins it.

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
tox run -e unit -- tests/unit/tasks/ tests/unit/test_lvol_sync_op_task.py \
                   tests/unit/test_task_cancellation.py
tox run -e unit                    # full unit tier, ~45s, currently green
```

Do not run `tox run` (the integration tier is broken independently of this work).

## Before opening the PR

Delete these two handoff files (`HANDOFF.md`, `TASK_RUNNER_REWORK_PLAN.md`) —
they are transfer artifacts, not part of the change.
