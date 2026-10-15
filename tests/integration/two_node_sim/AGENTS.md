# two_node_sim — the two-node HA simulator

The AWS two-node campaign (c1–c7b, 2026-10) found 18 bugs (F1–F18) that no
unit or integration test caught: they live in the interplay of the arbiter,
the node's HA state machine, SPDK's fence/ANA/port handling and the
control-plane flows, and in their timing. This suite runs the bed's scenarios
and one regression scenario per finding, in CI:

| Tier | tox | CI job | Contents |
|---|---|---|---|
| fast (required on every PR) | `tox run -e two-node-sim` | "Two-node simulation" | smoke, 01b crash, 02b freeze of the vol1 primary, 07 partition, F3/F16 proofs |
| full | `tox run -e two-node-sim-full` | "Two-node simulation (full)" | every bed scenario and every finding's fixed/detected proof |

## What is real, what is modelled

| Real (the production code runs) | Modelled (`simnode.py`, `actors.py`) |
|---|---|
| `arbitration/arbiter.py`, `decision.py`, `events.py`, `planned.py` | the two storage nodes: JC HA state machine (ported rule by rule from ultra `alg_two_node_ha.cpp`), SPDK lvol fence/unfence, port blocks with ownership, ANA groups, takeover latch, devices |
| the arbiter's default helpers: leadership restore, device check, rebuild-task bookkeeping, the leaderless reconcile and its probe | the network (storage link, management link, client link, lossy link) |
| every `RPCClient` wrapper — only `RPCClient._request` is routed to the simulated node | the client's NVMe multipath IO (KATO 4 s, fast_io_fail 8 s) |
| the guards the flows consult: `_single_peer_jm_is_complete`, `_two_node_fence_holds_ports`, `_may_unblock_peer_port`, `_jc_compression_status_with_retry`, `_failover_primary_ana`, `_failback_primary_ana`, `_set_lvol_ana_on_node` | the outer shell of the flows the arbiter does not own: monitor (offline, auto-restart, stale-port remediation), restart runner, port-allow runner, migration runner, shutdown flow. Each keeps the real guard calls and models only the RPC-heavy work |
| `tasks_controller` task records, `set_node_status`, all FDB reads and writes (real FoundationDB, as the integration tier requires) | the Go event collector (same contract: in order per node, `ha_resync` on a new instance) |

Time is virtual (`vclock.py`): `time.time`/`monotonic`/`sleep` and the
arbiter's clock read one clock; nothing sleeps or waits on the wall clock. A
node RPC takes its latency -- or its whole timeout against a frozen SPDK -- in
virtual time:

- on the main thread (the decision and lease loops) it is charged to the
  loop, whose next run starts after it: a blocking RPC on the decision thread
  delays the next verdict (F16);
- in the arbiter's pools and recovery threads it parks a real thread
  (`CoopTask`) until the virtual clock reaches the call's end, so a node is
  marked unreachable when its call times out, not before. One thread runs at a
  time: runs are deterministic.

**Runtime.** No real sleeps or timeouts remain; the wall time is FoundationDB
round trips of the real code (the arbiter alone does about 13 reads per 100 ms
tick, about 6 of them of the same `ClusterArbitration` record). Measured on a
developer box with ~2 ms per FDB round trip (WSL2): fast tier 28 s, full tier
8.5 min. A native Linux runner has a fraction of that latency.

## Invariants (checked continuously or at the end, `runner.py`)

- never two writers for one LVS;
- no write served by a node missing writes while its up-to-date peer is down (F10);
- the survivor is granted within lease TTL + margin + a tick (scenario `grant_within_s`);
- after a grant, no `bdev_lvol_update_lvstore`/`set_leader` reaches the survivor while its peer is gone (F17);
- client stall ≤ 3 s for node failures, ≤ 5 s for the client path -- measured as the bed does (tn2 `it/analyze.py`): whole seconds of fio's 1 s IOPS log without a completion, from the injection; no IO errors outside partitions;
- no node abort except the frozen node's abort-on-wake;
- the pair is healthy again **without a manual step**: arbiter steady, every LVS normal, remote JMs healthy, no port blocked, devices present, the primary leads its LVS, ANA optimized/non_optimized on the namespaces' groups (all groups for F18), no open task, no pending rebuild.

## Proof that the scenarios catch the findings

`test_finding_is_detected` runs each finding's scenario with the bug put back
(`scenarios.BUGS`: a node-side switch, or the real function patched back to its
pre-fix behaviour) and asserts that it fails. `test_finding_is_fixed` runs it on
main and asserts that it passes.

## Known limits

- F9 (distribs rejecting the cluster map after a reboot) is below the RPC level and not modelled.
- F11 (remote JMs not reconnecting after both links healed) had no identified root cause and did not recur; not modelled beyond 08-both-links.
- F18 is open on main: its scenario is an xfail checked against its failure signature; flip it with the fix (`simnode.RESTORE_ALL_ANA_GROUPS`).
- F19 is open on main (since #1485): a primary frozen for 6 s stalls its volume 4 s, because nothing moves its LVS to the granted survivor; IO resumes when the hublvol keep-alive (`constants.KATO`, 5 s) fails the redirect. Xfail `F19-long-freeze-primary`.
- Faults are injected 0.5 s into a second of fio's log; the bed's alignment was random, so a bed stall can be one second shorter or longer.
- The flows' shells are models: a change inside `exec_port_allow_task`, the restart flow or the monitor that is not one of the guards above is not exercised here.
- `TWO_NODE_STABLE_FOR_S` is 5 s in the simulator (30 s in production); it only delays the heal.

## Adding a finding

1. Model the behaviour that exposed it (node rule in `simnode.py`, or flow step in `actors.py` calling the real guard).
2. Add a bug switch to `scenarios.BUGS`/`apply_bugs` that restores the pre-fix behaviour.
3. Add `REGRESSIONS["Fnn"] = (Scenario(...), {bug switches})`.
4. Run `tox run -e two-node-sim -- tests/integration/two_node_sim -k Fnn`: fixed passes, detected fails.
