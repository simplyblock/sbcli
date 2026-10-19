# Failback writer conflict and duplicate listener adds — lblk_rapid_outage, 2026-09-26

Run: `lblk_rapid_outage_docker-20260925-171809` (jump host `/mnt/nfs_share/…`), sbcli main, 4 storage nodes, FTT=1.

| Node | Mgmt IP | RPC port | Role for LVS_1 |
|---|---|---|---|
| 69f1030f | 192.168.10.201 | 4420 | primary |
| c8a7e64d | 192.168.10.202 | 4422 | secondary |
| 9d367dd6 | 192.168.10.203 | 4424 | — |
| c03f70cf | 192.168.10.204 | 4426 | — |

LVS_1: client port 4428, hublvol port 4429, journal `jm_vuid=1`.

Sources: `graylog_collected/sb_logs_20260926_221804_60m` (cluster log, `TasksRunnerRestart.log`, `spdk_44xx.log`, `spdk_proxy_44xx.log`), `automation_logs/outage_log_20260925_171809.log`.

## a) Writer conflict on LVS_1 during the failback of 4420

### Timeline (cluster time)

| Time | Node | Event | Source |
|---|---|---|---|
| 22:22:37 | 4420 | test: `container_stop` outage starts | outage log |
| 22:22:38.892 | 4420 | `online -> offline` | cluster log |
| 22:22:39.677 | 4422 | `Leadership changed due to receive new IO. group id: 1` | spdk_4422 |
| 22:22:40.004–.036 | 4422 | every distrib of `jm_vuid=1` "state changed to leader"; its JC takes the journal writer lock | spdk_4422 |
| 22:22:47.534 | CP | restart of 4420 starts | TasksRunnerRestart |
| 22:23:20.738 | CP | `=== Phase: Primary LVS recreation ===` | TasksRunnerRestart |
| 22:23:21.007 | CP→4422 | `bdev_lvol_get_lvstores(LVS_1)` (the leader probe). **No "Current leader for LVS_1" is logged**: the probe did not see 4422 as leader | proxy_4422, TasksRunnerRestart |
| 22:23:21.074 | CP | `Restart phase for LVS_1 on 69f1030f: pre_block` | TasksRunnerRestart |
| 22:23:22.95–23.71 | CP→4422 | `nvmf_subsystem_listener_set_ana_state … → non_optimized` per namespace | proxy_4422 |
| 22:23:23.026 | 4422 | its JC excludes 4420's JM (`helper_sync_setter … JM is excluded`) | spdk_4422 |
| 22:23:24.447 | CP→4422 | `nvmf_port_block 4428` — the only quiesce RPC 4422 receives | proxy_4422 |
| 22:23:24.463 | CP→**4420** | `bdev_distrib_force_to_non_leader(jm_vuid=1)` — to the restarting node, not to 4422 | proxy_4420 |
| 22:23:24.492 | CP→4420 | `bdev_examine raid0_1` (45 ms after the block) | proxy_4420 |
| 22:23:24.927 | CP→4420 | `bdev_lvol_set_leader_all(LVS_1, true)` | proxy_4420 |
| 22:23:25.122 | 4422 | JC: `journal manager recovered` for 4420's JM | spdk_4422 |
| 22:23:25.216 | 4422 | `JC replication task has started for JM remote_jm_69f1030f… on jm_vuid 1` | spdk_4422 |
| 22:23:25.217 | 4420 | `JM (jm_vuid= 1) -- started journal replication` | spdk_4420 |
| 22:23:25.276 | 4420 | `JC [ jm_69f1030f… ] received a writer_conflict event for jm_vuid= 1`; distribs drop leadership; `Lvolstore on conflict set poller` (25.281); port 4428 blocked by SPDK (25.332) | spdk_4420 |
| 22:23:25.763 | CP | `Unblocking c8a7e64d for LVS_1 while it is NO LONGER leader` | TasksRunnerRestart |
| 22:23:56.609 | 4420 | `in_restart -> online`; 22:24:03.844 `online -> down` | cluster log |

On 4422, between 22:23:15 and 22:23:29, the proxy log shows **no** `jc_get_jm_status(1)`, `jc_disable_replication`, `bdev_distrib_check_inflight_io`, `bdev_lvol_set_leader(false)` or `bdev_distrib_force_to_non_leader` for LVS_1.

### Root cause

The colleague's reading is right in substance: the control plane examined LVS_1 on the primary and made it leader while journal replication on the secondary was still live. It did not suspend it or check in-flight IO first. The reason it skipped those steps is in the leader probe:

- `_recreate_lvstore_impl` quiesces the acting leader in a dedicated sequence:
  1. wait for JM replication tasks;
  2. block the leader's port;
  3. run `jc_disable_replication` until it reports no active replication;
  4. drain in-flight IO with `bdev_distrib_check_inflight_io`;
  5. drop leadership with `bdev_lvol_set_leader(false)` and `bdev_distrib_force_to_non_leader`.
- That sequence runs only for `current_leader`, which is chosen by `bdev_lvol_get_lvstores(...)["lvs leadership"]` on each connected peer.
- 4422 had been the acting leader since 22:22:39.68, and its distribs and JC were leader for `jm_vuid=1`. The probe at 22:23:21 still did not report it as leader. The unblock check at 22:23:25.763 read the same flag as false too. So `current_leader` was None. 4422 was treated as an ordinary non-leader peer: its port was blocked, and nothing else happened.
- 4420 was then examined and promoted. 4422's JC was still the journal writer. When 4420's JM came back at 22:23:25.1, that JC started replicating into it at 22:23:25.2. 4420, now also a writer, hit the writer conflict 60 ms later.

The step that ran too early is the examine and promotion on 4420 (22:23:24.49 / 24.93). It needed to wait for the acting leader 4422 in this order:
1. Journal replication suspended (`jc_disable_replication(1)` returning True).
2. In-flight IO drained (`bdev_distrib_check_inflight_io(1)` returning false).
3. Leadership dropped (`bdev_lvol_set_leader(false)` and `bdev_distrib_force_to_non_leader(1)`).

All of that existed, and was skipped only because the leader probe came back empty.

### Fix

`_presumed_acting_leader()` in `storage_node_ops.py`:
- **When it applies:** the probe finds no leader, and the restarting node is recreating its own primary LVS.
- **What it picks:** the connected secondary, otherwise the connected tertiary, as the presumed acting leader. The existing quiesce sequence then runs on it unchanged.
- **Why it is safe:** every step is harmless on a peer that is not leading. There is no replication to suspend and nothing in flight, and setting leadership to false is a no-op.
- **Diagnostics:** the probe now logs the lvstore record when a peer does not report leadership, so the next occurrence shows which field it returned.

## b) "Listener already exists" — add_listener on existing listeners

### Statistics (whole run, 31 one-hour windows)

| Measure | Count |
|---|---|
| `nvmf_subsystem_add_listener` calls | 7,660 |
| SPDK `nvmf_rpc_listen_paused: *ERROR*: Listener already exists` | 3,000 (39%) |
| other listener RPC errors | 0 |
| `nvmf_subsystem_listener_set_ana_state` calls | 10,660 |

The RPC still answers HTTP 200 with an error result, so the control plane did not notice any of them.

### Cause

Step 11 of the primary restart, "demote old leader's subsystems to non_optimized", called `nvmf_subsystem_add_listener(…, ana_state="non_optimized")` for every lvol on every connected peer. Those listeners already existed, so SPDK refused each add and the ANA state did not change through this step. The per-namespace `set_ana_state` before the port block was what actually demoted them.

- **Incident hour:** all 35 refusals coincide with the step: 22:20:53 on 4424, 22:23:27–28 on 4422, 22:29:37 on 4424.
- **06:18–07:18 hour, spot check:**
  - **Matched:** 134 of 154 refusals fall on the same node and minute as a logged demote step.
  - **The other 20:** on 4420 at 06:41, they have the same signature: c03f70cf was in restart, and one add per subsystem hit its LVS port 4432. The restart runner's log has a gap at that minute.

This matches the SPDK engineer's observation: on failback, the control plane changed ANA state by calling add_listener instead of the ANA RPC.

### Fix

`simplyblock_core/utils/nvmf_listener.ensure_listener()` applies one rule:
- **New listener:** created once, with the intended `ana_state`.
- **Existing listener:** never added again. Its ANA state changes, when the caller asks for it, through `nvmf_subsystem_listener_set_ana_state` for the volume's own ANA group (`anagrpid=ns_id`), and only if the listener does not already report that state.

| Call site | New behaviour |
|---|---|
| Restart step 11 (`demote_old_leader_listeners`) | `set_ana_state` per volume group on existing listeners; creates only a missing one, directly `non_optimized` |
| `publish_lvol_listeners` (lvol create / register) | creates a missing listener in its role's state; an existing one is left alone (no duplicate add) |
| `recreate_lvol_on_node` | same |
| restart `_register_lvols_on_node`, hublvol expose/prestage, migration | already checked existence first; unchanged |
