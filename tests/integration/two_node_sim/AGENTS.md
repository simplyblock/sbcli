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
- F23 (bed c9/02a) is open on main: after the solo grant the survivor's writes still wait on a frozen peer's data devices (each stripe has a chunk there) until the peer aborts on wake and its connections close (`actors.DEVICE_TEARDOWN_S`, 1.1 s). Modelled only as the bed proved it: the leader of the fio target's LVS (vol1, LVS_6) that was writing when the peer froze (`Lvstore.leader_since`). The same wait very likely hits any LVS the survivor led before the freeze (vol0 / LVS_1 in 02b), but the bed measured only vol1, so it is not modelled there. A survivor's writes after it took the LVS over are not modelled as stuck; instead the takeover itself is (bed c9/c10/c11 02c): while the peer is still frozen, the takeover's lvstore metadata read waits on its devices until its connections close after its abort on wake (F23, `F24-frozen-primary-takeover-read` is an xfail on main). After the peer's abort the hublvol redirect fails only once the connections close, so the takeover then finds the devices torn down (02b: 3 s). `02a-hang-sn1` and `F23-frozen-secondary-data-io` are xfails on main; the fix (ultra #219) is the capability `CAP_STOPLIST_PEER_DEVICES`.
- F24 (bed c11/02c): #219 as first run in c11 isolated the peer's devices only by closing them; their status still read online, a follower's takeover read went to a closed device and the survivor aborted. Modelled when the survivor is solo and has `CAP_STOPLIST_PEER_DEVICES` but not `CAP_ISOLATE_MARKS_UNAVAILABLE` (#219's F24 commit marks the devices unavailable first). `test_f24_needs_its_own_fix` proves it.
- F25 (bed c12/02c): with F24 fixed the takeover read goes degraded, but the new leader re-locks the appender on every JM and waited for its lock request on the frozen peer's JM, which neither completes nor fails until the peer aborts on wake (or the 8 s NVMe IO timeout): 6-8 s client stall, 0 IO errors. Modelled in the takeover when the survivor is solo with `CAP_STOPLIST_PEER_DEVICES` and `CAP_ISOLATE_MARKS_UNAVAILABLE` but not `CAP_APPENDER_LOCK_SKIPS_PEER_JM` (#219's F25 commit): the takeover lasts until the peer's `frozen_until`. `F25-frozen-primary-takeover-append-lock` is judged on the client stall (default limit), unlike the F24 scenario, which has no stall limit; `test_f25_needs_its_own_fix` proves it.
- F26 (bed c12b/02c-r1, intermittent): with F21 and F25 fixed, a history read the survivor's JC reader already had in flight on the peer's JM when the peer froze still neither completed nor failed until the 8 s NVMe IO timeout (or the peer's abort on wake); the takeover's reader restart waited for it (7 s client stall, 0 IO errors). Modelled in the takeover under the condition `reader_read_inflight_on_peer_jm` when the survivor is solo with `CAP_READER_SKIPS_DEAD_PEER_JM` but not `CAP_READER_RESETS_PEER_JM_READ` (#219's F26 commit resets the dead peer JM's bdev once, failing the read at once). `F26-takeover-read-inflight-on-peer-jm` is judged on the client stall; `test_f26_needs_its_own_fix` proves it.
- F18 and F19 are open on main: their scenarios are xfails checked against their failure signature. Their fixes exist as pending PRs (SPDK #80, #81; ultra #215), modelled as node capabilities in `simnode.py` (`CAP_ANA_RESTORE_ALL`, `CAP_GRANT_STOPS_REDIRECT`). `test_pending_fix_passes` runs each with its fix's capability and must pass; `test_bed_scenario_with_pending_caps` runs every bed scenario with all pending capabilities. When a fix merges into the deployed node image, add its capability to `simnode.DEPLOYED_CAPS` and remove the scenario from `KNOWN_FAILING` and `PENDING_FIX`.
- F19 is open on main (since #1485): a primary frozen for 6 s stalls its volume 4 s, because nothing moves its LVS to the granted survivor; IO resumes when the hublvol keep-alive (`constants.KATO`, 5 s) fails the redirect. Xfail `F19-long-freeze-primary`.
- Faults are injected 0.5 s into a second of fio's log; the bed's alignment was random, so a bed stall can be one second shorter or longer.
- The flows' shells are models: a change inside `exec_port_allow_task`, the restart flow or the monitor that is not one of the guards above is not exercised here.
- `TWO_NODE_STABLE_FOR_S` is 5 s in the simulator (30 s in production); it only delays the heal.
- F6's restart remedy (`F6c-partition-restart-remedy`, `F6b-restart-precheck-fenced-peer`): the condition `jm_reconnect_ineffective` makes only a node restart bring the remote JMs back (c1/c2), so the partition recovery hands a node to the restart flow. The restart's jc_compression precheck then runs on the presumed acting leader (the real `_presumed_acting_leader`), the fenced peer. With `jc_error_while_fenced` that peer's JC is in error mode (ultra keeps a holding or fenced vuid in JCERR until `jc_grant_solo`/`jc_unfence`) and answers -6, and the restart aborts: F6b is open on main (review of #1559). The restart runner's retries are not modelled; each would meet the same fenced peer, because the arbiter unfences only after the restarting node is healthy.

## Adding a finding

1. Model the behaviour that exposed it (node rule in `simnode.py`, or flow step in `actors.py` calling the real guard).
2. Add a bug switch to `scenarios.BUGS`/`apply_bugs` that restores the pre-fix behaviour.
3. Add `REGRESSIONS["Fnn"] = (Scenario(...), {bug switches})`.
4. Run `tox run -e two-node-sim -- tests/integration/two_node_sim -k Fnn`: fixed passes, detected fails.
