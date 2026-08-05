# Rework task runners into a resilient, concurrent, deduplicated framework

## Context

`simplyblock_core/services/` contains ~15 `tasks_runner_*.py` files. Each is a
hand-written `main()` `while True` loop plus a `task_runner()` that share the
same skeleton but diverge in almost every detail. The divergence is
**accidental** — the result of copy-paste plus independent incident fixes, not
deliberate per-runner design — and it produces real correctness gaps:

- **DB-error handling is broken in most runners.** The dominant idiom calls
  `db.get_clusters()` once inside a `try` (discarding the result), then calls it
  **again unguarded** and uses *that* — so the guard is dead code and any
  `FDBError` crashes the process (`tasks_runner_backup/migration/restart/`
  `port_allow/lvol_migration/jc_comp/new_dev_migration/failed_migration`).
  `tasks_runner_fdb_backup` has no guard at all. Per-cluster `get_job_tasks()`
  is unguarded in almost all of them — this is the 2026-07-16 incident that
  killed several runners with no auto-restart. Only `node_add` and
  `sync_lvol_del` handle DB errors coherently.
- **`max_retry` semantics differ** (`retry >= max_retry` vs
  `0 <= max_retry <= retry` vs `max_retry > 0 and retry >= max_retry`). With the
  model default `max_retry = -1` (meant to be "unbounded"), the first form
  terminates a task *immediately* (`0 >= -1`). Pinned intent lives in
  `tests/unit/tasks/test_max_retry_semantics.py` and `test_retry_ceiling.py`.
- **The task lease (`tasks_controller.claim_task`) is applied in only 6 of the
  runners.** The rest can double-execute side-effecting tasks during rolling
  deploys / dual-manager windows.
- **Concurrency exists in only two runners** (`node_add`, `restart`:
  `ThreadPoolExecutor` + per-task/per-node inflight sets + capped backoff);
  `cluster_expand` has inline backoff; the rest are strictly serial.
- Sleep intervals, `IN_ACTIVATION` skipping, and task re-fetch placement are all
  ad-hoc.

**Deployment context:** runners are Docker Swarm services
(`simplyblock_core/scripts/docker-compose-swarm.yml`), which restart a container
on exit by default. So the intended way to survive a wedged FDB client is to
**let a DB error surface as a process failure (`sys.exit(1)`) and rely on Swarm
to restart with a fresh FDB connection.**

**Intended outcome:** one shared driver that owns the loop, DB-error-to-exit,
task lease, standardized retry/lifecycle, and opt-in concurrency — so each
runner is reduced to its domain-specific "advance one task" handler. Reached via
small single-purpose commits so the branch stays easy to rebase onto upstream.

### Decisions (confirmed with user)

- **DB-error model: immediate `sys.exit(1)`.** No consecutive-failure threshold.
  This *simplifies* the current `sync_lvol_del` threshold counter down to the
  common model.
- **Scope: convert all task runners** onto the shared driver, with two
  exceptions:
  - `tasks_cluster_status.py` — a shell-command runner, not `FN_*`-task based;
    being replaced by a deployment-level solution. **Leave untouched.**
  - `tasks_runner_backup_merge.py` — a periodic *policy/schedule evaluator* over
    lvols with no `FN_*` tasks. **Reclassify as a service**: rename to follow the
    `*_service.py` / `*_monitor.py` convention already used in the directory
    (`snapshot_monitor.py`, `lvol_monitor.py`, `health_check_service.py`, …).
- **Execution model: serial by default, concurrency opt-in** (per-task and
  per-key exclusion + capped backoff), generalizing `node_add`/`restart`.
- **Exceptions are the failure-signaling mechanism.** Specific task functions
  return `None` (void) and never touch task state; they *raise* to signal
  failure, using ordinary Python function semantics. The driver owns **all**
  `task.status` / `task.retry` / `write_to_db` mutation and translates the
  handler's return-or-raise into the task's next state. Retryable vs
  non-retryable is disambiguated by exception type (see the vocabulary in B0).
  - **Placement pushback (accepted friction):** this cannot land in Phase A. A
    handler can only stop mutating task state once a driver exists to own that
    state — before then, a raised exception has nothing to catch it and would
    crash the runner. So the contract is *defined* in **B0** and *realized*
    per-runner during each **B1…Bn** migration. Phase A therefore leaves handler
    internals alone on purpose (Phase B rewrites them), and A2/A4 only touch the
    loop-level and terminate-decision code, not the handlers' error plumbing.

### Constraints to preserve

- Each runner keeps an **importable, unit-testable per-task entry point**
  (`tests/unit/tasks/` imports the module and calls `task_runner(...)` directly;
  this works because `main()` sits behind `if __name__ == "__main__"`). The
  handler function must stay module-level and callable in isolation.
- Reuse existing helpers, do not reinvent: `tasks_controller.claim_task`,
  `get_active_node_tasks`, `get_active_node_mig_task`, `is_auto_restart_paused`;
  `constants.TASK_EXEC_INTERVAL_SEC`, `RESTART_TASK_EXEC_INTERVAL_MAX_SEC`,
  `NODE_RESTART_MAX_PARALLEL_SUSPENDED`; `storage_node_ops.fd_dead_recovery_allowed`;
  `snapshot_controller.lvstore_op_lock`.
- Keep documented deliberate exceptions (e.g. `port_allow` intentionally does
  **not** skip `IN_ACTIVATION` clusters — preserve with its comment).

---

## Phase A — Reconcile inconsistencies (no shared abstraction yet)

Each item is one focused commit spanning the affected runners. These land first
so behavior is uniform *before* it is centralized, which also makes Phase B a
mechanical extraction.

**A1 — DB errors become runner failures (immediate exit).**
In every converted runner's `main()`: delete the discard-then-recall
`get_clusters()` double-call; call it once and let exceptions propagate. Wrap the
loop body so any DB access failure (`get_clusters`, `get_job_tasks`,
`get_cluster_by_id`, …) logs and `sys.exit(1)`. Remove `sync_lvol_del`'s
`_DB_FAILURE_RESTART_THRESHOLD` counter (now redundant). Task-*handler* errors
stay caught and non-fatal — only infra/DB errors exit.
Files: all `tasks_runner_*` in scope.

**A2 — Standardize `max_retry` semantics.**
One canonical rule: `max_retry < 0` ⇒ unbounded; otherwise terminate when
`retry >= max_retry`. Fix the runners using the immediate-terminate form. Verify
against `tests/unit/tasks/test_max_retry_semantics.py` and `test_retry_ceiling.py`.
Files: `node_add`, `jc_comp`, `fdb_backup`, `replication_final`, `backup`,
`cluster_expand`, migration trio, `restart`, `port_allow`, `lvol_migration`,
`sync_lvol_del`.

**A3 — Apply the task lease uniformly.**
Add `tasks_controller.claim_task(task)` (skip-if-owned) to every side-effecting
runner that lacks it: `backup`, `fdb_backup`, `jc_comp`, `new_dev_migration`,
`failed_migration`, `replication_final`, `sync_lvol_del`.

**A4 — Loop hygiene → folded into Phase B (no standalone commit).**
Investigation showed the three "hygiene" concerns are premature to reconcile in
Phase A and are owned correctly by the B0 driver + `RunnerSpec`:
- **Interval** — the literals deliberately differ (`3`/`5`/`10`/`60`s;
  `TASK_EXEC_INTERVAL_SEC` is `10`), so unifying onto one constant changes polling
  cadence. Each runner keeps its value via `spec.interval`.
- **`IN_ACTIVATION` skip** — task-dependent (some runners run during activation;
  `port_allow` opts out by design). Folded into the **eligibility predicate** as
  the condition `cluster.status != IN_ACTIVATION`, dropping the separate
  `skip_in_activation` flag.
- **Re-fetch** — every runner *already* re-fetches before running; only the
  *location* differs (main vs handler), which is harmless. The driver performs a
  single consistent re-fetch before the handler; handlers stop re-fetching.

**A5 — Reclassify `backup_merge` as a service.**
Rename `tasks_runner_backup_merge.py` → `backup_merge_service.py` (fix its own
dead double `get_clusters()` guard while moving it). Update the `command:` in
`docker-compose-swarm.yml` and any other references. It stays a plain periodic
service and does **not** move onto the driver in Phase B.

---

## Phase B — Shared driver + per-runner migration

**B0 — Introduce `simplyblock_core/services/task_runner_base.py`.** One commit,
new module + unit tests, no runner changed yet. Provides:

- `serve(spec)` — the entrypoint wrapper: runs the loop; on any uncaught
  DB/infra exception, logs and `sys.exit(1)` (the A1 model, centralized).
- The loop: `get_clusters()` → per-cluster `get_job_tasks()` → filter by the
  spec's `function_names` and `status != DONE` → dispatch. All DB calls unguarded
  (failures exit). (Activation-skipping is not a loop concern — it is an
  eligibility condition, below.)
- Per-task lifecycle wrapper applied around the runner's handler, in order:
  **single re-fetch** (`db.get_task_by_id`, the one authoritative fresh read —
  handlers no longer re-fetch) → canceled→DONE → `max_retry` terminate → **pre-run
  skip-gates** (below) → set `RUNNING` → **call the void handler** → map its
  return-or-raise to the next task state and `write_to_db`. The wrapper is the
  *only* place task state is mutated. Handler exceptions are caught here → task
  updated per the vocabulary below, **loop survives**.
- **Pre-run skip-gates.** One category of gate, evaluated before the handler,
  sharing one outcome: *skip this cycle, do not consume a retry, do not mutate task
  lifecycle state*. Two members, applied in this order:
  1. **Eligibility** — `spec.is_eligible(task, cluster) -> bool`, **default
     `True`**, a side-effect-free "can I run right now?" read. The default keeps
     simple runners trivial; complex tasks supply a predicate over
     cluster/node/sibling-task state. It is the abstraction for what the migration
     family does ad-hoc today: `lvol_migration`'s pre-run chain (`src`/`tgt` node
     online, `cluster.status in (ACTIVE, DEGRADED)`, no open
     `get_active_cluster_expand_task`), the `get_active_node_mig_task` /
     same-node-sibling exclusion in the migration trio, and `restart`'s
     `is_auto_restart_paused`. **`IN_ACTIVATION` skipping is just an eligibility
     condition** (`cluster.status != IN_ACTIVATION`): runners that pause during
     activation include it in their predicate; `port_allow` (documented opt-out)
     omits it — replacing the former separate `skip_in_activation` flag. Modeling
     these as a *pre-run* gate rather than `TaskDefer` (an *in-handler* raise)
     keeps the "just wait, never consume a retry" family unbounded exactly as
     `test_retry_ceiling.py`'s `INTENTIONALLY_UNBOUNDED` set documents.
  2. **Lease** — the built-in `tasks_controller.claim_task(task)` gate, **always
     applied** (no opt-out: it is universal double-execution protection).
     Conceptually the lease *is* an eligibility question ("is this host eligible to
     run this?") and shares the skip-without-retry outcome, so it lives in the same
     category — but it stays a **distinct built-in gate, not folded into
     `is_eligible`**, for three reasons: (a) a
     successful claim has a **side-effect** (writes `owner`/`updated_at` to
     acquire/refresh ownership) whereas `is_eligible` is a pure read; (b) it is
     **cross-cutting** double-execution protection that a custom predicate must
     never be able to silently drop; (c) it runs **after** the cheap pure
     eligibility so an ineligible task short-circuits *before* the driver churns
     the lease's `updated_at` on a task it will skip anyway.
- **Exception vocabulary** (defined here; handlers raise these, driver
  interprets):
  - *returns normally* → success → `STATUS_DONE`.
  - `TaskRetry` (and any other/unexpected `Exception`, the safe default) →
    retryable failure → `STATUS_SUSPENDED`, `retry += 1`, capped backoff.
  - `TaskDefer(reason)` → not a failure, just "can't proceed yet" (node not
    online, peer restart in flight, sibling task on same node) →
    `STATUS_SUSPENDED`, **retry NOT consumed**, short re-poll. Replaces today's
    scattered "suspend but don't increment retry" branches.
  - `TaskAbort(reason)` → permanent / non-retryable (missing param, object not
    found, "not needed") → `STATUS_DONE` with a failure/short-circuit result.
    Replaces the handlers that currently mark DONE mid-body.
- **Task writes are compare-and-set, never full-object writes** (revised after
  the 2026-08 rebase — see "Upstream reconciliation" below). Every transition
  runs as a mutator against the row as it stands (`db.atomic_update`), refusing
  a row another actor has finished — and, for non-terminal transitions, one it
  has canceled — and reporting whether it won. Only the two handler-owned
  fields (`function_result`, `function_params`) are carried over from the
  driver's copy. `on_finish` runs only for the winner of the terminal
  transition.
- Execution: **one dispatch path**. Every execution is submitted to the pool and
  registered in the per-task inflight set (plus the optional
  `exclusion_key(task)` per-key set); *serialized* execution submits and waits
  on the future rather than running inline. Capped exponential backoff on
  failure. Generalizes `node_add`/`restart`.
- A `RunnerSpec` describing: `function_names`, `handler` (a void callable),
  `is_eligible` (default `lambda task, cluster: True`), `interval` (each runner
  keeps its current cadence), `concurrency`, `exclusion_key`, `on_finish`,
  `serialize` (per-task/per-cycle predicate; defaults to `concurrency == 1`).

**B1…Bn — Migrate one runner per commit**, simplest → hardest, each preserving
behavior and keeping the module-level handler importable for the existing tests:

1. ✅ `fdb_backup`, `jc_comp`, `replication_final`, `sync_lvol_del` (trivial serial)
2. ✅ `backup`, `cluster_expand`
3. ✅ `node_add` (concurrency opt-in, `node_addr` exclusion key)
4. `restart` — **moved up** (was 6th). Concurrency + per-node exclusion; its
   parallel-vs-serialized choice is the spec's `serialize` predicate
   (`suspend_drain_complete` / `fd_dead_recovery_allowed`), and
   `is_auto_restart_paused` becomes part of `is_eligible`. The driver's
   `exclusion_key` + backoff subsumes `_node_inflight`/`_restart_next_attempt`,
   and its `_task_finish`/`_task_update` CAS helpers are subsumed by the
   driver's. Migrated first of the remainder because it is the runner that
   defines the driver's hard requirements — if the design holds here it holds
   everywhere.
5. migration trio — `migration`, `new_dev_migration`, `failed_migration`
   (serial; `get_active_node_mig_task` / same-node-sibling gating becomes the
   spec's `is_eligible` predicate, not a concurrency `exclusion_key`)
6. `port_allow` (large; its `is_eligible` omits the `IN_ACTIVATION` check — the
   documented opt-out — and keeps the recovery logic)
7. `node_removal` (`FN_NODE_REMOVAL`; already leased, module-level loop,
   unbounded — a straightforward serial migration)

**Deferred to a follow-up PR** (see rebase-friendliness below — both are under
active upstream development, and neither blocks the rest):

- `lvol_migration` (largest; migrate the loop/lease/retry shell only, leave the
  domain snapshot-copy state machine intact. Its pre-run guard chain — node
  status, `cluster.status in (ACTIVE, DEGRADED)`, open cluster-expand task —
  becomes the spec's `is_eligible` predicate; unbounded `max_retry=-1` is kept)
- `batch_migration` (`FN_LVOL_BATCH_MIG`; migrate the loop/lease shell, leave
  its group orchestration intact)

**Upstream reconciliation (2026-08 rebase onto origin/main `4cb16a20b`).**
Upstream landed `a61b00ad4` — *"fix(restart): stop double execution of a
node-restart task (2026-07-29 incident)"* — which invalidates two of B0's
original choices, both now corrected above:

- **Full-object task writes are unsafe.** A `task.write_to_db()` of a copy read
  before a long handler ran reverts whatever other actors committed meanwhile:
  it un-canceled a task that `cancel_pending_node_restart_tasks` canceled when
  the node came back ONLINE, and reclaimed a lease another host had taken. The
  driver held that stale copy for the *entire* handler duration, i.e. the widest
  possible window, and centralizing it would have propagated the defect to every
  runner. All transitions are CAS now; the migrated runners needed no changes,
  since none of them touch task state.
- **A split dispatch path re-enters running tasks.** Restart chooses parallel vs
  serialized per task per cycle from live cluster state (`suspend_drain_complete`,
  `fd_dead_recovery_allowed`), and a mode flip mid-restart re-entered a task
  still running on the pool because the inline branch consulted no inflight map.
  Hence the single dispatch path and the `serialize` predicate: `concurrency`
  cannot be a static number for restart.

Also revised: B0 originally said handlers stop re-fetching. They stop
re-fetching for *lifecycle* decisions, but a handler must re-read the task
immediately before a destructive step — upstream added exactly that before
restart's shutdown.

**Rebase-friendliness (2026-08).** `lvol_migration` and `batch_migration` took
~17 upstream commits in this window (cleanup state machine rewritten, multipath,
retry ceilings removed), several still labelled `TEMP:`. B7/B9 are therefore
deferred to a follow-up PR rather than rebased repeatedly against a moving
target; `restart` moves up to be migrated first, since it is the runner that
defines the driver's hard requirements.

**Earlier upstream reconciliation (2026-07 rebase).**
Upstream independently reworked the runners: renamed `task_runner`→`process_task`
and added `tasks_controller.task_lease_heartbeat` (a lease-keepalive thread) to
the six long-blocking runners. This is now **folded into the B0 driver** — the
driver wraps every handler call in `task_lease_heartbeat`, so each migrated runner
gets keepalive for free and the per-runner boilerplate is deleted on migration.
The branch's original "Align task runner structure" commit was dropped as
superseded; A1/A2/A3 were re-applied on top of upstream (and A1/A3 extended to the
two new runners). Note: `tests/unit/tasks/test_retry_ceiling.py` **hangs on
origin/main itself** — upstream restructured the runners it drives without updating
it. It is stale and will be superseded by the driver's own tests as runners migrate
(each `main()` becomes `serve(SPEC)`); it should be skipped/removed rather than
propped up.

Each `main()` collapses to `serve(SPEC)`. Each `task_runner(task)` becomes the
spec's **void handler**: it keeps only the domain work, returns `None` on
success, and raises `TaskRetry` / `TaskDefer` / `TaskAbort` (per B0) instead of
setting `task.status`/`retry` or calling `write_to_db`. Existing per-runner
mappings to preserve when translating: `jc_comp`'s "compression not needed" →
`TaskAbort`; "node not online" / "task on same node" → `TaskDefer`; RPC failure
→ `TaskRetry`. `restart`'s defer-vs-fail distinction (currently inferred by
comparing `retry` before/after) becomes explicit `TaskDefer` vs `TaskRetry`,
removing that bookkeeping. The module-level handler stays importable so
`tests/unit/tasks/` keeps calling it directly (tests assert on raised exceptions
+ driver-applied state rather than in-handler writes).

---

## Files

- **New:** `simplyblock_core/services/task_runner_base.py` (+ `tests/unit/tasks/test_task_runner_base.py`)
- **Renamed:** `tasks_runner_backup_merge.py` → `backup_merge_service.py`
- **Modified (all in `simplyblock_core/services/`):** `tasks_runner_fdb_backup`,
  `_jc_comp`, `_replication_final`, `_sync_lvol_del`, `_backup`, `_cluster_expand`,
  `_node_add`, `_migration`, `_new_dev_migration`, `_failed_migration`,
  `_port_allow`, `_restart`, `_lvol_migration`
- **Modified:** `simplyblock_core/scripts/docker-compose-swarm.yml` (backup_merge command; no other command changes — filenames of converted runners stay the same)
- **Untouched:** `tasks_cluster_status.py`
- **Reused (no change):** `simplyblock_core/controllers/tasks_controller.py`,
  `constants.py`, `storage_node_ops.py`, `controllers/snapshot_controller.py`

## Verification

Per the `tox-verify` skill — after each commit, targeted; full suite before finishing:

1. `tox run-parallel -e lint,types` — must be green.
2. Targeted unit tests while iterating:
   `tox run -e unit -- tests/unit/tasks/ tests/unit/test_task_lease.py \
   tests/unit/test_port_allow_recovery_refactor.py \
   tests/unit/test_drain_replaced_with_fixed_sleep.py \
   tests/unit/test_lvol_sync_op_task.py \
   tests/unit/tasks/test_task_runner_base.py`
   The existing retry/lease tests must pass unchanged (they pin the standardized
   semantics); extend them where new behavior warrants. New `test_task_runner_base.py`
   asserts the exception vocabulary: a void return → DONE, `TaskRetry`/unexpected
   `Exception` → SUSPENDED+retry+backoff, `TaskDefer` → SUSPENDED without
   consuming a retry, `TaskAbort` → DONE; that an `is_eligible → False` task is
   deferred without entering its handler or consuming a retry (default-`True`
   spec always runs); and that a handler exception never escapes the loop while a
   DB error does exit.
3. Import-smoke every converted runner (`python -c "import simplyblock_core.services.tasks_runner_X"`)
   to confirm module-level definitions still load under the stubbed-fdb unit env.
4. `tox run -e unit` full unit tier; then the integration test that patches the
   lvol-migration runner (`tests/integration/test_dual_fault_tolerance.py`) if
   Docker + `libfdb_c` are available.
5. **DB-failure behavior (manual, infra-dependent):** run one converted runner
   against a dev-compose FDB, stop FDB, and confirm the process exits non-zero
   (immediate `sys.exit(1)`) rather than hanging — i.e. it surfaces as a Swarm
   restart. Confirm a task-handler exception instead only suspends that task and
   the loop keeps polling.

### Rebase-friendliness note

Commits are single-purpose and mostly additive (new module) or line-local
(loop-body edits), so conflicts against ongoing upstream runner fixes stay small
and localized — mirroring the resolution already done for
`tasks_runner_sync_lvol_del.py` on this branch.
