# Dual-outage case matrix, resume-from-failure, and multipath axis

Planning doc, 2026-09-14. Not committed, not implemented yet.

## Context

Two gaps in the current outage coverage, both surfaced by recent failures.

**Coverage.** Every existing multi-outage test picks its victims through
`_pick_outage_nodes` (`e2e/stress_test/continuous_failover_ha_multi_outage.py:125`),
which explicitly **blocks a node and its own secondary from being chosen
together**. So the one topology that matters most, losing both nodes that can
serve the same lvstore, is the one case no test can currently produce. The
2026-09-13 k8s RCA landed exactly there: LVS_7 lost its primary while its
secondary and tertiary were mid role-change, and a read failed with EREMOTEIO
inside the documented fault budget. We want that relationship tested
deliberately, across every combination of outage type, rather than hoping a
random sample hits it.

**Cost of a failure.** A stress run is 5 to 27 hours. When it fails at
iteration 15 the cluster still holds all its objects (`stress.py:158` passes
`delete_lvols=False`), but the next run's `setup()` deletes every lvol, snapshot,
clone and pool before doing anything else. There is no way to pick up where the
last run stopped, so every investigation costs a full re-run.

Outcome wanted: a 90-case dual-outage matrix that runs on docker and k8s-native,
a resume path that re-enters the loop on an existing cluster without wiping it,
and multipath NIC disconnect as a real axis rather than a coin flip.

## Decisions taken (2026-09-14)

- **Resume = adopt existing objects and continue.** Re-derive the inventory from
  the cluster, reconnect and remount without reformatting, re-kick FIO fresh,
  resume the loop at the recorded iteration. Not restoring in-flight FIO or exact
  device paths.
- **Migration axis = wait for drain vs do not wait.** `wait_for_balancing=True`
  plus asserting `validate_migration_for_node` (what the classic N+K tests do)
  against `wait_for_balancing=False` with no assertion (what the rapid no-gap
  tests do). The docker-only "scale `app_TasksRunnerMigration` to 0" variant is
  **not** an axis, it is a no-op on k8s (`quick_outage.py:1860`) and would make
  the matrix ragged.
- **90 definitions, run on both platforms** = 180 executions from one table.
- **One driver plus a case table**, not 90 leaf classes.
- **Multipath**: make the docker path deterministic, and build the k8s-native
  equivalent, which does not exist today.

## The matrix

```
outage types (5): graceful_shutdown, forced_shutdown, container_stop,
                  storage_node_reboot, interface_full_network_interrupt
pairs with repetition                          = 15
x separation {0, 1, 2}                         = 45
x migration {drain, inflight}                  = 90 case definitions
run on docker and on k8s-native                = 180 executions
```

**Separation** is distance along the secondary chain, which forms a ring
(`sn_primary_secondary_map[primary] = secondary_node_id`; confirmed on the live
6-node cluster as worker-0 -> 1 -> 2 -> 3 -> 4 -> 5 -> 0):

| sep | victim B | meaning |
|---|---|---|
| 0 | `secondary(A)` | A and its own secondary. **The case no current test can produce.** |
| 1 | `secondary^2(A)` | one node between them, i.e. A and its tertiary |
| 2 | `secondary^3(A)` | two nodes between them |

Requires >= 6 storage nodes (sep 2 needs 4 distinct nodes plus headroom) and
`npcs >= 2`. Guard and skip with a clear message below that.

## Part 1, the dual-outage driver

**New file: `e2e/stress_test/dual_outage_matrix.py`**

- `CASES`, a module-level list of ~90 dicts, generated once from the three axes
  by an explicit `itertools` expression at import time so the table is readable
  and greppable, with a stable `id` per case:
  `dual_<typeA>_<typeB>_sep<N>_<drain|inflight>`.
- `_DualOutageMixin`, all tunables as class attributes in the house style of
  `_MassCreateDeleteMixin` (`e2e/stress_test/mass_create_delete_stress.py:71`):
  the case id, the timing knobs below, `MULTIPATH_MODE`, `MIN_NODES = 6`.
  Mutable state initialised in `_init_mixin_state()`, never as class attributes.
- `_pick_pair_by_separation(sep)`, walks `sn_primary_secondary_map` `sep+1` hops
  from a random start. Deliberately **does not** call `_pick_outage_nodes`,
  because that helper exists to forbid exactly what sep 0 requires. Raises a
  skip-shaped exception when the topology cannot satisfy the separation.
- `_DualOutageDocker(_DualOutageMixin, TestLvolHACluster)` and
  `_DualOutageK8s(_DualOutageMixin, K8sNativeFailoverTest)`, mixin first in the
  MRO, matching `_MassCreateDeleteDocker` (`:1560`) / `_MassCreateDeleteK8s`
  (`:3248`).
- Two registered leaf classes only: `DualOutageMatrixDocker`,
  `DualOutageMatrixK8s`. The case is selected by a new `--case` argument on
  `stress.py`, defaulting to running the whole table sequentially.

Reuse rather than reimplement: `_graceful_shutdown_node`
(`continuous_failover_ha_multi_outage.py:545`), `_forced_shutdown_node`
(`..._all_nodes.py:92`), `ssh_obj.stop_spdk_process`, `ssh_obj.reboot_node`,
`_disconnect_full_interface` (`multi_outage.py:582`), and on k8s
`_k8s_stop_spdk_pod` / `_k8s_network_outage` / `_operator_shutdown_node`
(`continuous_k8s_native_failover.py:2213` / `:2239` / `:2286`).
`restart_nodes_after_failover` and `log_outage_event` are used unchanged.

**Timing knobs**, currently hard-coded sleeps, promoted to class attributes so
each case can set them:

| Attribute | Replaces | Today |
|---|---|---|
| `INTRA_PAIR_DELAY_SEC` | thread-launch spacing | 0 docker / 3 all-nodes / 10 k8s |
| `PRE_RESTART_DELAY_SEC` | `sleep_n_sec(280)` `multi_outage.py:1424` | 280 |
| `INTER_RESTART_DELAY_SEC` | `sleep_n_sec(100)` `:1437` | 100 |
| `INTER_SET_GAP_SEC` | `MIN/MAX_OUTAGE_GAP_SEC` `cluster_test_base.py:198-199` | 50-90 |

## Part 2, resume from the point of failure

**New file: `e2e/utils/run_state.py`**, a small `RunState` helper: `save()`,
`load()`, `clear()`, writing JSON to a **stable, non-timestamped** path so the
next process can find it: `<NFS_LOG_BASE>/_resume/<test_name>__<cluster_id>.json`.
The run dir is timestamped, so state cannot live there alone; also copy it into
the run dir each checkpoint for the record.

Persisted fields, deliberately small, enough to adopt and not to replay:

```
iter, iteration, case_id, cluster_id, test_name, updated_at,
lvol_base, clone_base, snap_base,          # name prefixes, else objects are unrecognisable
pool_name,
lvols[], clones[], snapshots[],            # names only; details re-derived from the API
last_outage: {nodes[], types[], started, completed}
```

`lvol_base` / `clone_base` / `snap_base` are random per process
(`quick_outage.py:85-87`); without persisting them a resumed run cannot tell its
own objects from anything else on the cluster. This is the detail that makes or
breaks the feature.

**The wipe is the blocker.** Cleanup must be gated at every site, not just the
base, because eleven classes replace `setup()` without calling `super()`:

- `e2e/e2e_tests/cluster_test_base.py:429-448`, `delete_all_clones` /
  `delete_all_snapshots` / `delete_all_lvols` / `delete_all_storage_pools`
- `e2e/stress_test/continuous_k8s_native_failover.py:222-234` and `:248`
  (`_kill_orphaned_k8s_resources`)

Add one predicate on `TestClusterBase`, `_should_wipe_existing_objects()`,
returning False when resume is active, and call it at both sites. The k8s branch
at `cluster_test_base.py:441-448` already skips pool deletion with a "will reuse
existing pool" comment, the same precedent, widened.

**Adoption path** on resume:
1. Load state; verify `cluster_id` matches, else refuse and say why.
2. `list_lvols()` and filter by the persisted prefixes; reconcile against
   `lvols[]`. Report anything missing rather than failing silently.
3. Reconnect and mount **without formatting**. `_register_and_mount_device`
   (`quick_outage.py:1226`) formats in the create path, so this needs a
   `format=False` branch; keep the `_assert_device_unclaimed` guard
   (`cluster_test_base.py`) which exists because of the earlier
   two-clients-one-namespace corruption.
4. Re-kick FIO fresh via `_kick_fio_for_all`. FIO is only started at checkpoints,
   so resuming mid-window without this runs outages against an idle cluster, the
   failure mode already called out at `continuous_k8s_native_failover.py:5467`.
5. Set `self._iter` and `iteration` from state and re-enter the loop. Both
   counters matter on k8s, where `iteration` scales the churn delete count
   (`:5511`).

**Entry point:** `--resume` on `stress.py`, plus `RESUME=1` env for the
workflows. Wire it in `stress.py` next to the existing `start_alert_collection`
call so it is not lost inside an overridden `setup()`.

Save state at the top of every iteration and at each checkpoint. On a clean
finish, `clear()`.

## Part 3, multipath as a real axis

**Docker**, the helpers exist but sit behind `random.random() < 0.5`
(`multi_outage.py:467`, `..._all_nodes.py:128`). Replace the coin flip with
`MULTIPATH_MODE` in {`off`, `single_nic_down`, `random_flap`}. Keep
`_is_multipath_enabled` (`:152`), `_disconnect_single_data_nic_all_nodes`
(`:161`), `_reconnect_multipath_nics` (`:217`) unchanged. Fix the log line so a
skip says *which* precondition failed; today one message covers both "fewer than
2 data NICs" and "lost the coin flip", which is not diagnosable.

**K8s-native**, nothing exists. Build `_k8s_disconnect_data_nic(node, iface)`
modelled directly on `_k8s_network_outage`
(`continuous_k8s_native_failover.py:2239`), which already does nsenter plus
iptables DROP with a scheduled auto-flush; scope it to one data NIC instead of
all traffic, and mirror the auto-restore so a stuck test cannot strand a node.

`random_flap` is the "random disconnect between interfaces" ask: pick a random
data NIC on a random node every `MULTIPATH_FLAP_INTERVAL_SEC`, down it, bring it
back, on a background thread, using the existing `start_*`/`stop_*` daemon
pattern (`cluster_test_base.py:3567`).

## Part 4, applying resume to the existing tests

Resume is built on `TestClusterBase` plus the two rapid loops, so it reaches the
existing combination tests with no per-test work beyond calling the save hook:
`RandomRapidFailoverNoGapV2WithMigration` (`quick_outage.py:637`),
`RandomRapidFailoverNoGapV2NoMigration` (`:1839`),
`K8sNativeRapidFailoverNoGapTest` (`continuous_k8s_native_failover.py:5421`),
`RandomMultiClientMultiFailoverTest`, `...AllNodesTest`,
`RandomK8sMultiOutageFailoverTest`. Each gets two lines: a `RunState.save()` at
the top of its loop, and the gated wipe.

## Files

| File | Change |
|---|---|
| `e2e/stress_test/dual_outage_matrix.py` | new, CASES table, mixin, 2 platform bases, 2 leaf classes |
| `e2e/utils/run_state.py` | new, RunState save/load/clear |
| `e2e/e2e_tests/cluster_test_base.py` | `_should_wipe_existing_objects()`, gate `:429-448`, timing constants, no-format mount branch |
| `e2e/stress_test/continuous_k8s_native_failover.py` | gate `:222-234` and `:248`, `_k8s_disconnect_data_nic`, save hook in `_stress_loop` |
| `e2e/stress_test/continuous_failover_ha_multi_outage.py` | `MULTIPATH_MODE` replaces the coin flip at `:467`, promote the hard sleeps |
| `e2e/stress_test/continuous_failover_ha_multi_outage_all_nodes.py` | same at `:128` |
| `e2e/stress_test/continuous_failover_ha_multi_client_quick_outage.py` | save hook in `_outage_loop`, `format=False` branch |
| `e2e/stress.py` | `--case`, `--resume`; call the resume hook beside `start_alert_collection` |
| `e2e/__init__.py` | register the 2 leaf classes (import, module list, `get_stress_tests()`) |

## Verification

```bash
# table is well formed and 90 long, no cluster needed
cd e2e && python3 -c "from stress_test.dual_outage_matrix import CASES; \
  print(len(CASES), len({c['id'] for c in CASES})); print(CASES[0], CASES[-1])"

# discovery
python3 -c "from __init__ import get_stress_tests; \
  print([c.__name__ for c in get_stress_tests() if 'DualOutage' in c.__name__])"

# one case, docker; sep 0 is the highest-value case, run it first
python3 -u stress.py --testname DualOutageMatrixDocker \
  --case dual_graceful_shutdown_container_stop_sep0_drain --ndcs 2 --npcs 2

# same on k8s
python3 -u stress.py --testname DualOutageMatrixK8s \
  --case dual_graceful_shutdown_container_stop_sep0_drain --ndcs 2 --npcs 2 --run_k8s True

# resume: kill the run above mid-loop, then
python3 -u stress.py --testname DualOutageMatrixDocker --case <same> --resume
```

Expected on resume: log lines showing state loaded, the wipe skipped, N lvols
adopted by prefix, mounts restored without mkfs, FIO re-kicked, and the loop
re-entering at the saved `_iter`. Then confirm on the cluster that the lvol UUIDs
are the same ones from before the kill, that is the assertion that matters.

Expected for sep 0: both victims serve the same lvstore. If IO fails there while
`npcs=2` says it should not, that is a genuine product finding and the reason
this matrix exists.

## Risks

- **Sep 0 may legitimately fail IO** if the product's real guarantee is weaker
  than npcs implies. Run it first, on one case, before building the other 89 out,
  the answer changes whether the matrix is a bug hunt or a regression suite.
- **Resume cannot restore client-side mounts the CI already destroyed.**
  `stress-run-only.yml:255-301` disconnects all NVMe and `rm -rf`s `/mnt` on the
  clients before the run. Resume must therefore reconnect and remount from
  scratch, which is why step 3 above is mandatory and not an optimisation.
- **`logs/cleanup.py` deletes the previous outage log** on the runner
  (`stress-run-only.yml:362-367`); the durable copy is on NFS. The state file
  must not live under `e2e/logs/`.
- **180 executions is a lot of cluster time.** Sequence: prove one sep-0 case on
  each platform, then the 15 pairs at sep 0, then widen. Do not schedule the full
  table until a single case has passed end to end.
