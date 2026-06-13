# Single-node cluster expansion (FTT2) — implementation plan

**Branch:** `feature-single-node-expansion` (worktree at `C:\Users\Michael\sbcli-feature-single-node-expansion`, based on `origin/main`)

> Line-number references in this plan were collected against the `feature-device-namespaces` working copy and may drift on `main`. Verify paths before editing.

> **Design revisions (2026-04-13)** after conversation review:
> 1. **Entry point is `sbctl sn add --expansion <node>`**, not a change to `cluster complete-expand`. `cluster_expand` and the bulk path are unchanged.
> 2. **No JC quorum polling** — the synchronous create chain in `recreate_lvstore_on_sec` is its own guarantee. Restart works without a poll; expansion can too.
> 3. **No refactor of `recreate_lvstore_on_sec`**. We reuse it as-is. The orchestrator sets DB back-references first, then calls the existing function on the affected nodes.
> 4. **Sibling multipath reconfigure** (sec_2 when sec_1 moves) uses a narrow `bdev_nvme_attach_controller` + `bdev_nvme_remove_trid` pair on the sibling, not a full `recreate_lvstore_on_sec(sibling)`.
>
> See `step6_design.md` for the SPDK glue per move type. The sections below are the original plan and remain accurate for steps 1–4 (planner, teardown helper, expand_state, orchestrator). Step 5's `cluster_expand` rebalance branch was reverted in favor of `add_node(..., expansion=True)`.

## Problem

`cluster_expand()` (`simplyblock_core/cluster_ops.py:734-796`) requires adding N new nodes at once:
- `:752-757` — raises "minimum of 2 new nodes required" (FTT1)
- `:767-773` — raises "minimum of 3 new nodes required" (FTT2)

It pairs each new node with a fresh secondary (and tertiary on FTT2), then calls `create_lvstore()`. There is **no code path to re-home an existing secondary/tertiary from an old node to a new node**, which is why single-node adds are rejected.

Goal: enable adding one node at a time by computing a **role re-home diff** against the existing rotation and executing it safely.

## Desired layout model

Initial placement done by `cluster_activate()` at `simplyblock_core/cluster_ops.py:587-627` via `get_secondary_nodes()` / `get_secondary_nodes_2()` (`simplyblock_core/storage_node_ops.py:5053-5113`) is a rotation:

```
primary(LVSi)   = Ni
secondary(LVSi) = N((i) mod k + 1)
tertiary(LVSi)  = N((i+1) mod k + 1)
```

Adding `N_{k+1}` means recomputing that rotation with modulus `k+1`. Worked example (4→5, FTT2):

Before: `n1:(LVS1,LVS4,LVS3)`, `n2:(LVS2,LVS1,LVS4)`, `n3:(LVS3,LVS2,LVS1)`, `n4:(LVS4,LVS3,LVS2)`

Diff:
- `LVS3.tertiary`: n1 → n5
- `LVS4.secondary`: n1 → n5
- `LVS4.tertiary`: n2 → n1
- `LVS5.primary`: — → n5 (new)
- `LVS5.secondary`: — → n1 (new)
- `LVS5.tertiary`: — → n2 (new)

Step 1 of implementation is a pure function:

```
compute_role_diff(cluster, current_layout, new_node) -> List[RoleMove]
```

`RoleMove = {lvs_id, role ∈ {sec, tert}, from_node, to_node}` plus create-new entries for the newcomer's LVS. Lives next to the existing `get_secondary_nodes*` helpers. Pure — unit-testable without SPDK. For FTT1 this degenerates to one sec move + new-LVS primary/secondary.

## CLI / API surface

**Recommend (a): relax `cluster_expand()` itself.**
- Drop the min-node guards at `cluster_ops.py:752-757` and `:767-773`.
- When `len(new_nodes) < FTT+1`, switch to the **rebalance path**; otherwise keep the existing bulk path unchanged.
- Same command (`sbctl cluster complete-expand`) works for both — users and tests aren't forked.
- Add `--dry-run` that prints the computed `RoleMove` list and stops.

Alternative (b): new command `cluster add-node-rebalance`. Cleaner isolation but duplicates orchestration and forces users to pick — skip unless there's a product reason.

## Orchestration phases (per single-node add)

Ordering must preserve FTT redundancy at every step and respect the port-block / restart-phase machinery in:
- `simplyblock_core/storage_node_ops.py:4278-4450` (`recreate_lvstore_on_non_leader`)
- `simplyblock_core/storage_node_ops.py:4581-4800` (`recreate_lvstore`, `_set_restart_phase`)
- `simplyblock_core/models/storage_node.py:113` (`restart_phases` dict: `pre_block` / `blocked` / `post_unblock`)

### Phase A — Create-before-destroy for re-homed roles

For each `RoleMove(sec|tert, from→to)`:

1. **On `to_node`**: build the stack via `recreate_lvstore_on_non_leader(to_node, leader=primary, primary_node=primary)`. Reuses `_create_bdev_stack`, `subsystem_create` (min_cntlid 1000/2000 per role, `simplyblock_core/controllers/lvol_controller.py:861-920`), `connect_to_hublvol` (`simplyblock_core/models/storage_node.py:348-416`), port block/unblock (`storage_node_ops.py:4346-4375`).
2. **Wait for JC quorum**: primary must report ≥2 healthy JMs including the new replica. Log keyword `ctx_per_jm_RetryConnect … recovered` (per `first_analysis.md`). Only then does the LVS transiently have 3 replicas — the safe window.
3. **On `from_node`**: tear down the old non-leader stack (delete subsystem/listeners, `bdev_distr_delete`/`bdev_raid_delete`, disconnect-hublvol). Factor out `teardown_non_leader_lvstore(node, lvs_uuid)` from existing node-decommission paths rather than writing inline.
4. **DB update**: on the **primary node's** record (`simplyblock_core/models/storage_node.py:66-69`), update `secondary_node_id` / `tertiary_node_id` and move the `lvstore_stack_secondary` / `lvstore_stack_tertiary` list to the new holder. Note: these pointers live on the primary's record, not the donor's.

### Phase B — Create newcomer's primary LVS

5. On `new_node`: `create_lvstore()` for `LVS_{k+1}` (same call `cluster_expand` already makes at `cluster_ops.py:790`).
6. Immediately create its sec/tert via `recreate_lvstore_on_non_leader` on the rotation-designated holders (n1, n2 in the 4→5 example).

### Phase C — Client path update (see §6)

### Phase D — Commit

Bump cluster node count, persist final role mapping, clear transitional flags, log the full diff to cluster event log (`sbctl cluster get-logs`).

### Why A before B

If we created `LVS_{k+1}` first and then started moving existing sec/terts, a crash mid-way leaves the newcomer with a lonely primary and donor nodes still holding roles they no longer own per DB — painful to recover. Moving re-homed roles first keeps existing LVSes healthier and uses the new node as just-another-sec/tert before elevating it to own a primary.

## Port block / restart phase / JC invariants

Reuse the phase machine: each re-home move wraps `pre_block → blocked → post_unblock` on both from-node and to-node, coordinated the same way `recreate_lvstore_on_non_leader` already does at `storage_node_ops.py:4346-4375`.

Gate each move on JC quorum before advancing the cursor. Abort on `JC helper_service_history_append has failed` (keyword `JCERR`).

Rebalance must be resumable: persist `cluster.expand_state = {in_progress, moves[], cursor}` in FDB so an mgmt-node crash doesn't leave silent half-state.

## Client reconnection

Currently implicit — clients rely on NVMe ANA (`listeners_create(..., ana_state=...)` at `simplyblock_core/rpc_client.py:311-329`, ANA set in `lvol_controller.py:861-920`). For re-homed roles this is **not sufficient** — the old listener IP+port disappears from the subsystem for that lvol's sec/tert path.

Per re-homed role, for every lvol in that LVS:
1. On new host: `subsystem_create` + `listeners_create` with correct `min_cntlid` (1000 sec, 2000 tert) and ANA `non_optimized` — already handled by `recreate_lvol_on_node`.
2. Push an updated path set to clients. **Open question:** map the cluster-map push / client-notify path before committing to the design here (not fully covered in the initial exploration). Goal: clients must add the new listener *before* the old one is removed so ANA carries them over without IO error.
3. On old host: remove listener, remove subsystem for that lvol.

If cluster-map push already propagates listener changes atomically per lvol, driving the sequence is enough. If not, we need an explicit "listener added on new host" barrier before Phase A step 3.

## DB / model changes

No structural changes needed — fields on `simplyblock_core/models/storage_node.py:66-69` already cover it; role changes are edits to `secondary_node_id`, `tertiary_node_id`, and `lvstore_stack_*` on the primary's record.

Two additions:
- `Cluster.expand_state: dict` — `{in_progress: bool, moves: [...], cursor: int}` for resumability.
- Audit log entries to cluster event log per `RoleMove`.

## FTT1 generalization

Same `compute_role_diff` with rotation modulus bumped by 1 yields a smaller diff (only `LVS_last.secondary` moves from the wrap-around node to the newcomer, plus newcomer's own primary+secondary). Keep the codepath unified — useful for node-replacement rebalancing even on FTT1.

## Edge cases

- **New node fails mid-rebalance** → abort Phase B; Phase A moves remain committed (cluster is still at `k` LVSes with one sec/tert moved). Resume via `expand_state`.
- **Existing node offline when expected to give up a role.** `recreate_lvstore_on_non_leader` handles offline `leader_node` partially (`recreate_lvstore` takes `lvs_primary` at `:4606-4609`). Decision: **refuse** rebalance if any donor is offline for the first cut — simpler. Relax later if needed.
- **In-flight IO / ongoing lvol create.** Gate on sync deletes/registrations being in `pre_block`/`post_unblock` — the restart phase machinery already enforces this.
- **User adds multiple nodes at once but < FTT+1.** Loop `compute_role_diff` once per added node, executed sequentially. If `len(new_nodes) ≥ FTT+1`, optionally fall back to today's bulk path.
- **LVS count imbalance after many single adds.** Rotation stays correct — no special handling.

## Tests

Extend `tests/ftt2/`:
- **Unit**: `compute_role_diff` — 4→5, 5→6, 3→4 (FTT2 minimum), FTT1 parallels. Assert move count and invariants (each LVS has exactly one primary/secondary/tertiary on distinct nodes; modulus rotation preserved).
- **Integration**: single-node-add mirroring `e2e/e2e_tests/add_node_fio_run.py:1-100` but calling the rebalance path under active FIO. Assert no client IO error during the rebalance window.
- **Failure injection**: kill mgmt node mid-rebalance; verify `expand_state` resume reaches the same final layout.
- **Peer/hublvol**: reuse patterns from `tests/ftt2/test_hublvol_paths.py` to assert multipath correctness after a re-home.

## Incremental implementation order

1. `compute_role_diff` + unit tests (pure, no SPDK).
2. `teardown_non_leader_lvstore(node, lvs_uuid)` helper factored from existing node-decommission paths.
3. `cluster.expand_state` persistence scaffolding.
4. Rebalance orchestrator that executes a `RoleMove` list with phase machine + JC gate.
5. Wire into `cluster_expand()` as the `len(new_nodes) < FTT+1` branch.
6. Client path update (after investigating cluster-map push — §6).
7. Tests (ftt2 integration + failure injection).
8. CLI `--dry-run`.

## Open questions to resolve before coding step 6+

- **Cluster-map push path**: where is listener-change propagation to clients implemented today? Determines whether client-reconnect is a sequencing problem or a new-feature problem.
- **Can `min_cntlid` be bumped on an existing subsystem**, or does moving a role always mean full subsystem recreate on the new host? If recreate, the per-lvol loop must be resilient to partial completion.
- **Does distrib placement map need re-seeding** when a sec/tert moves? Assumption: no, distrib placement is alceml-based and independent of LVS role. Confirm before Phase A.

## Relevant file paths

| Purpose | Path |
|---|---|
| Expand CLI | `simplyblock_cli/cli.py:477-479`, `simplyblock_cli/clibase.py:511-513` |
| Expand controller | `simplyblock_core/cluster_ops.py:734-796` |
| Cluster activate / initial rotation | `simplyblock_core/cluster_ops.py:546-695` |
| Rotation helpers | `simplyblock_core/storage_node_ops.py:5053-5113` |
| Node model / role fields | `simplyblock_core/models/storage_node.py:66-69`, `:113`, `:348-416` |
| Cluster FTT config | `simplyblock_core/models/cluster.py:74` |
| Non-leader recreate | `simplyblock_core/storage_node_ops.py:4278-4450` |
| Primary recreate | `simplyblock_core/storage_node_ops.py:4581-4800` |
| Lvol subsystem / listener | `simplyblock_core/controllers/lvol_controller.py:767-920` |
| RPC | `simplyblock_core/rpc_client.py:210-329` |
| Add-node entry | `simplyblock_core/storage_node_ops.py:1276-1500` |
| FTT2 test fixtures | `tests/ftt2/conftest.py:1-24` |
| FTT2 restart tests | `tests/ftt2/test_restart_scenarios.py`, `test_hublvol_paths.py` |
| Existing expand E2E | `e2e/e2e_tests/add_node_fio_run.py` |
