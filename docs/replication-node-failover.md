# Async replication when source or target nodes go away

A snapshot replication must not wait for one particular node. Every member of
a source lvstore holds the full snapshot stack, so any online member can send a
snapshot. On the target, only the lvstore's leader can receive a transfer and
persist the convert, so a replication follows the target's leadership. It
starts only once that leadership has settled.

## How it worked before

The runner is `simplyblock_core/services/snapshot_replication.py`. It runs as a
single-threaded loop with one task per snapshot.

| Step | Behaviour before |
|---|---|
| Choose the source | `_source_leader_node`: the online member that reports `lvs leadership` for the source lvstore. With no such member, the task waited (`no online source LVS leader`, not counted). |
| Gate in `task_runner` | The same probe ran for every task status. A RUNNING task whose leader probe found nobody was suspended, so its transfer was neither polled nor restarted. |
| Poll a running transfer | `bdev_lvol_transfer_stat` on whatever member led *now*, not on the member the transfer ran on. After a leadership move it read "No process", counted a retry and restarted. |
| Source lost mid-transfer | No explicit handling. An unreachable member raised out of `task_runner`, the main loop caught it, and the task stayed RUNNING until a later pass. |
| Choose the target | `_receiving_leader_node`: the online member that reports `lvs leadership`, preferring the recorded node. With none, `_recover_target_leader` ran the leaderless-LVS recovery. |
| Target leadership moving | One probe only. A member that was leader at the probe, but no longer a moment later, got the transfer. The hub then refused it ("receive io for hublvol in nonleader mode") and the task counted a retry with backoff. |
| Transfer `Failed` | Always counted as a retry with backoff, whatever the cause. |
| Convert | `_receiving_leader_node`, or the recorded node, then `_require_lvs_leader`. |
| RPO | Recorded when the finish marks the snapshot replicated. That doesn't change here. |

Consequences:

- **Source stalls without a visible leader.** The source stopped whenever
  `lvs leadership` showed no leader, though any online member could have
  sent the snapshot. That covers leaderless windows, and an acting secondary
  that does not report the flag. sbcli #1439 found the second case in a
  fail-back: 4422 led LVS_1 without reporting leadership. With a primary in
  maintenance and no member visibly leading, the RPO grew for the whole
  outage.
- **Lost transfers waited too long.** A transfer lost with its source member
  was not restarted until the leader probe moved on.
- **Moves into the target cost retries.** A leadership move on the target spent
  the task's retries, though nothing was wrong with the transfer.

## Design

### Source: any online member, primary first

`_select_source_node(snapshot)` takes the members of the source lvstore in role
order: `lvol.node_id`, the primary, then `lvol.nodes`, the secondary and the
tertiary. It returns the first member that passes three checks:

1. **Online:** the member's DB status is `online`.
2. **Reachable:** the member answers an RPC within 10 s.
3. **Holds the snapshot:** `bdev_get(<snap_bdev>)` returns a bdev on that
   member.

Leadership is not consulted, because a transfer only reads the snapshot. The
function also returns a reason string, for example `secondary 7f3a… (skipped:
primary 4b21… offline)`. The reason is stored on the task as `source_fallback`
and logged.

- **When it runs.** Selection happens on every attempt. A member that went
  down in the idle interval between two runs is skipped without any special
  case.
- **Which member is used.** The task records it as `source_node_id`. A RUNNING
  transfer is polled on that member only.
- **Losing the member.** If that member is no longer online or cannot be
  asked, `_restart_elsewhere` suspends the task. The next attempt starts the
  snapshot over on whichever member `_select_source_node` picks.
- **No member available.** The task waits with backoff. This is not counted,
  see the retry bounds below.
- **Consistency groups.** Members of one group share the source lvstore, so
  they share the role order. With the same node states, every member picks
  the same source.

### Target: a settled leader, checked before every transfer

`_stable_target_leader(remote_lv)` probes every online member of the target
lvstore with `lvol_controller.is_node_leader`. That is SPDK's own
`lvs leadership` flag, and it is exactly what the hub's receive path and the
convert check. Leadership counts as re-established when both of these hold:

1. **One leader:** exactly one online member reports leadership.
2. **The same leader again:** `REPL_LEADER_SETTLE_SEC` (2 s) later, the same
   single member reports it.

Anything else returns no node, with the reason. That covers no leader, two
leaders, or the leader changing between the probes.

- **Settled-leader cache.** The runner sends many volumes into one lvstore, so
  a confirmed leader is cached per (cluster, lvstore) for
  `REPL_LEADER_SETTLED_FOR_SEC` (30 s). Within that window a single probe
  showing the same sole leader is enough. Any other answer clears the cache.
- **Why not more signals.** The #1439 finding still matters here. The
  `lvs leadership` flag can miss an acting leader, so the source no longer
  depends on it. On the target, the flag is the gate itself. A member that
  does not report leadership is refused by the hub and by the convert, so
  sending it anything would fail. Requiring the flag to be settled is what
  "started on the new leader" means for the receive side.

In `process_snap_replicate_start`, before any transfer:

1. **Settled leader.** Resolve the settled target leader.
2. **Leaderless lvstore.** If no member reports leadership at all, run the
   existing leaderless-LVS recovery, `_recover_target_leader`. Then require the
   recovered leadership to settle like any other. Two members reporting
   leadership is a conflict, not a leaderless lvstore, so the recovery does
   not run.
3. **No settled leader.** Wait with backoff. This is not counted. No
   transfer is sent.
4. **Record.** Record the leader as `target_node_id`. If it differs from the
   previous attempt, the stored offset is dropped and the snapshot is sent from
   the start.

Moves during a run:

- **Transfer fails after a move.** `_target_leadership_moved` compares
  `target_node_id` with the current leaders. A move, to no leader or to a
  different one, restarts the replication on the new leader. This is
  `_restart_elsewhere`, which is not counted.
- **Transfer fails without a move.** It stays an ordinary counted retry with
  backoff.
- **Finish (chain and convert).** It resolves the settled leader again and
  does not finish while leadership is unsettled. If leadership moved after a
  completed transfer, the finish runs on the new leader. The data the old
  leader acknowledged is already in the HA landing volume on every member. The
  hub session is detached on the member the transfer was sent to.
- **Moves in the idle window.** These are covered because the leader is
  resolved before every transfer.

### Target: the primary or the secondary receives, never the tertiary

A settled leader is necessary, but it is not sufficient. The transfer reaches
the target through a **transfer hub**. These facts decide which hub, and which
member may receive:

| Fact | Where |
|---|---|
| SPDK demuxes hub IO by map id in the **hub's own** lvstore: `lvol->lvol_store->lvol_map.lvol[EXTRACT_ID(offset)]`. The leader gate checks the same lvstore. | `vbdev_hublvol_submit_request`, spdk `module/bdev/lvol/vbdev_lvol.c` |
| A node's regular transfer hub lives on `self.lvstore`, the lvstore that node is **primary** of. | `StorageNode.create_transfer_hublvol` |
| A hub blob is in-memory: `spdk_bs_create_hubblob` uses blob id `UINT64_MAX`, starts CLEAN, and writes no metadata. Any member can create one on any lvstore it hosts. | spdk `lib/blob/blobstore.c` |
| On a tertiary, hub IO is never demuxed: the branch requires `lvs->node_role != NODE_TERTIARY`. | `vbdev_lvol_submit_request` |
| A non-leading secondary forwards hub IO to the leader with the map-id offset intact (`is_hublvol_io`). | `vbdev_redirect_request_to_hublvol` |

**The rule (`process_snap_replicate_start`):**

- **Primary leads:** it receives through its own transfer hub, as before.
- **Secondary leads:** it receives through a **per-lvstore hub** that it
  creates in the target lvstore, `<lvs>/transferhub_<node8>`. The hub has its
  own NQN, `…:transferhub:<lvs>:<node uuid>`, and listens on the target
  lvstore's hub port (`replication_final_step.receiving_hub` and
  `ensure_hub_attached(src_rpc, node, lvs_primary)`).
  - The name has to be distinct, because the source reuses an attached
    controller by name. A shared name would keep sending to the old primary.
  - The hub is derived, not persisted. Creating and exposing it is idempotent,
    which also heals it after a restart.
  - If the secondary loses leadership mid-transfer, SPDK forwards its hub IO to
    the new leader.
- **Tertiary leads:** the start waits with backoff, and the wait is not
  counted. This is reached only when the primary and the secondary are both
  out.
- **Detach:** the controller the source attached is recorded on the task as
  `hub_ctrl`. Both detach paths close exactly that controller.

**Before this change** the receiving node always used its regular transfer hub
(`_receiving_leader_node`, and #1441 after it). A secondary that led the target
lvstore therefore resolved the landing volume's map id in **its own** lvstore,
whose leader gate it passes. If no volume there has that map id, the IO fails
("orglvol is none"). If one does, the snapshot is written into an unrelated
volume.

**To check on beds:**

- Target SPDK logs containing `FAILED - orglvol is none`.
- Older replication tasks whose `target_node_id` is not the landing volume's
  `node_id`.

**Still open:**

1. **Tertiary demux (SPDK fork).** Allow hub demux on a tertiary that leads,
   for example `lvol->hublvol && (lvs->node_role != NODE_TERTIARY || lvs->leader)`.
   This needs review by the SPDK owners. After that, drop the tertiary wait.
2. **Cross-cluster cutover.** `replication_final_step.run_cutover` still
   receives on the target primary only. With `receiving_hub` it can take the
   acting secondary too (branch `feat/online-migration-gaps`).

### Retry bounds

A node switch is not the transfer's fault, so it does not spend retries. Both
restarts after a lost source member and restarts after a target leadership
move count as switches. Each forced switch increments `node_switches`. Past
`REPL_MAX_NODE_SWITCHES` (6), the waits and restarts count as retries again,
so a flapping node cannot keep a task alive for ever. `max_retry` then ends
it, as before.

### Restart point

A transfer moved to another source member or another target leader starts its
snapshot at offset 0. The landing volume may hold writes the old path never
acknowledged, and a full resend is always correct. A transfer that continues on
the same member and the same leader keeps its offset, as before.

### Invariants kept

These are unchanged by this change:

- **Data rules:** the chain gate (`_unreplicated_local_ancestor`), the
  star-chain predecessor rules, the never-transfer-into-a-snapshot guard, the
  recovery-point rules and the monotonic group generation.
- **Group gating:** the per-lvstore transfer holds for consistency groups.
- **One task per snapshot:** the runner still dispatches only one RUNNING task
  per snapshot.
- **One target per run:** two runs cannot target different members of one
  lvstore at the same time. Each needs the single settled leader, and the
  runner is single-threaded.
- **RPO:** it is still recorded when the finish marks the snapshot replicated.

### Observability

| Where | What is recorded |
|---|---|
| Task `function_params` | `source_node_id`, `source_fallback` (the reason another member was used), `target_node_id`, `node_switches` |
| Task `function_result` | The reason for each wait or restart, for example `source member P is offline mid-transfer; restarting the transfer on the next available member` or `transfer failed at offset 8192: target leadership of LVS_9 moved from TP to TS` |
| WARNING logs | Every switch, naming the old and new node |
| INFO logs | Every transfer sent from a member other than the primary |

## Tests

`tests/unit/test_replication_node_failover.py`:

- **Source selection:**
  - primary, then secondary, then tertiary;
  - none online, which defers;
  - a member without the snapshot or unreachable is skipped;
  - leadership is never asked;
  - consistency-group members choose the same member.
- **Lost source:**
  - a source going offline mid-transfer restarts elsewhere without a retry;
  - a flapping member eventually spends retries;
  - a switch restarts from offset 0.
- **Target leadership:**
  - a settled leader is used;
  - moving, none or two leaders get no transfer;
  - the cached leader needs no second wait;
  - a failed transfer after a move restarts on the new leader;
  - a failure with an unchanged leader is an ordinary retry;
  - the finish waits while leadership is unsettled;
  - the start never transfers without a settled leader;
  - a leaderless target runs the recovery and then requires settling.

Existing tests that patched `_source_leader_node` or `_receiving_leader_node`
in the start, runner and finish paths now patch the new helpers.
