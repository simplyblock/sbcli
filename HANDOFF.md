# Task-runner rework — handoff

Continuation notes for picking up the `rework-task-runners` branch on another
machine. The full design lives in **`TASK_RUNNER_REWORK_PLAN.md`** (same dir) —
read it first; this file is just the current state + how to continue.

## Where the work is

Branch `rework-task-runners`, rebased onto `origin/main`. Five commits:

```
6ea41859c  Introduce generic task runner            (B0: driver + lease heartbeat + tests)
8458a5e44  Reclassify backup policy evaluation…      (A5: backup_merge -> backup_merge_service)
6079ed010  Apply task lease consistently…            (A3: claim_task on the lease-less runners)
4882cecac  Unify task retry semantics                (A2: canonical 0 <= max_retry <= retry)
9da6cd5f7  Consistently crash task runners on DB…    (A1: remove DB-error masking)
```

Pre-rebase safety backup: branch `backup/rework-pre-rebase2` (`61cd76770`).

## Status: Phase A done, B0 done, Phase B (migrations) NOT started

- **A1/A2/A3/A5** — done. **A4** was folded into Phase B (no commit — see plan).
- **B0** — `simplyblock_core/services/task_runner_base.py` (the `TaskRunner`
  driver: loop, single post-claim re-fetch, eligibility + lease pre-run gates,
  `task_lease_heartbeat` around the handler, retry ceiling + backoff, serial /
  opt-in concurrency) + `tests/unit/tasks/test_task_runner_base.py`. **Done.**
- **B1…B9** — migrate each runner onto the driver, one commit each, in the order
  listed in the plan (`fdb_backup, jc_comp, replication_final, sync_lvol_del` →
  `backup, cluster_expand` → `node_add` → migration trio → `port_allow` →
  `restart` → `lvol_migration` → `node_removal` → `batch_migration`). **Not started.**

## Gotchas (read before continuing)

- **`tests/unit/tasks/test_retry_ceiling.py` HANGS — and it hangs on `origin/main`
  itself.** Upstream restructured the runners it drives (module-level loops,
  `task_runner`→`process_task`) without updating it. Do NOT run the full
  `tests/unit/tasks/` dir until it is skipped/removed. It exercises the old
  `main()` loops that Phase B deletes; plan is to retire it as runners migrate.
- **Upstream already reworked the runners**: renamed `task_runner`→`process_task`
  and added `tasks_controller.task_lease_heartbeat` to the six long-blocking
  runners. B0's driver already folds heartbeat in, so migrating a runner should
  DELETE its per-runner `process_task` + `claim_task` + `task_lease_heartbeat`
  boilerplate in favor of `serve(SPEC)`.
- **Two new upstream runners** to migrate: `tasks_runner_node_removal.py`
  (`FN_NODE_REMOVAL`) and `tasks_runner_batch_migration.py` (`FN_LVOL_BATCH_MIG`).
- Each migrated `main()` collapses to `serve(SPEC)`; each handler becomes **void**
  and signals via `TaskDefer` / `TaskRetry` / `TaskAbort` (see plan §B0). Keep the
  handler importable — tests call it directly.

## Environment setup on the new machine

```bash
git fetch <remote>
git checkout rework-task-runners
pip install -e .                      # editable install
# FoundationDB 7.3.3 client library (libfdb_c) must be installed on the host
```

## Verification (was NOT runnable on the origin machine — offline)

The origin box had no network and a wiped `.tox`, so lint/types/tox could not run
there; the driver was verified with an ad-hoc stub harness only. **Re-run the real
checks on the new machine:**

```bash
tox run-parallel -e lint,types
tox run -e unit -- tests/unit/tasks/test_task_runner_base.py \
                   tests/unit/tasks/test_max_retry_semantics.py \
                   tests/unit/test_task_lease.py \
                   tests/unit/test_lvol_sync_op_task.py
# NOTE: do not add tests/unit/tasks/test_retry_ceiling.py — it hangs (see gotchas)
```

Follow the `tox-verify` skill / `AGENTS.md` for the full workflow.

## Before opening the PR

Delete these two handoff files (`HANDOFF.md`, `TASK_RUNNER_REWORK_PLAN.md`) — they
are transfer artifacts, not part of the change.
