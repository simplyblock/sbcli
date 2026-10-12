# Two-node arbitration: implementation contract

Status: approved for implementation (2026-10-09); revised 2026-10-12 after the integration
campaign c1 (no preferred node; bookkeeping, explicit-sync, fence-port, SPDK-down and client
rules). Design rationale, diagrams and failure analysis:
[`two-node-ha-design.pdf`](two-node-ha-design.pdf) (revision 2; where it describes a preferred
node, this document wins). This document is the binding contract between the work packages:
ultra (JC, distrib), the SPDK fork (lvol, blobstore), sbcli (arbiter, Go event collector, API)
and the simplyblock operator.

Scope: clusters whose storage-node membership is exactly two (`apply_jc_dual_node` in
`simplyblock_core/storage_node_ops.py` already sets `jc_set_dual_node(true)` for them). The
central control plane (CP) is the witness. Three-node and larger clusters are unchanged.

## 1. Terms

| Term | Meaning |
| --- | --- |
| LVS | One lvstore, identified on the node by the `jm_vuid` of its journal. |
| Peer | The other storage node of the cluster. |
| Epoch | Monotonic 64-bit counter per cluster, owned by the arbiter, stored in FDB. |
| Lease | Time-limited permission a node holds from the arbiter; renewed by the CP. |
| Holding | Node lost its peer's JM for an LVS and holds client writes awaiting a verdict. |
| Solo | Node leads an LVS alone with the peer fenced, under a grant of the current epoch. |
| Fenced | Node is non-leader for an LVS, its LVS client port is blocked (never the hublvol port, section 6.1), held IO returned as a path error. |
| Lost | The CP has positive evidence that a node's SPDK is gone (section 7.6). |

There is **no preferred node**. No node ever continues alone without a grant: at the hold
deadline without a verdict a node always fences itself, so a full isolation (both links down,
the CP reaches neither node) stops IO on both nodes.

## 2. Invariants

1. A node leads an LVS alone only with a `jc_grant_solo` of the current epoch. At the hold
   deadline without a verdict it fences itself; there is no exception for any node.
2. The arbiter sends `jc_grant_solo` for an LVS only if the peer is fenced for it: the peer
   acknowledged `jc_fence` in this epoch; or the peer's lease expired more than
   `lease_ttl_ms + lease_margin_ms` ago (it fenced itself at its hold deadline or on lease
   expiry while holding); or the peer is lost (section 7.6: its SPDK is gone); or there is
   positive fencing evidence from the operator (BMC fence or `out-of-service` taint).
3. A lost node that answers again is fenced, for every LVS its peer leads alone, before it gets
   a lease again (section 7.6).
4. A lease matters only while a node is holding. While the remote JM is healthy, an expired
   lease has no effect: missed renewals, WAN latency or a lost uplink never fence a healthy pair.
5. A node rejects every verdict RPC whose epoch is lower than the highest epoch it has seen.
6. A fenced node does not resume on its own: JC's error-mode resume
   (`n_jms >= jc_ha_nmin_jms_resume()`) requires `jc_grant_solo` or `jc_unfence`, and the
   10 s rule-removal poller (`lvol.c:spdk_lvs_remove_rules_poller`) does not lift rules on it.
7. Held IO is never completed with EIO by the protocol: grant executes it, fence returns it as a
   path error (ANA inaccessible), so hosts retry on the peer's path.

## 3. Node RPCs (ultra JC)

All RPCs are JSON-RPC 2.0 over the node's SPDK RPC socket, reached by the CP through the
existing proxy path. Errors use JSON-RPC errors with `code` = negative errno and a `message`.

| Error | Code | When |
| --- | --- | --- |
| `ESTALE` | -116 | `epoch` lower than the highest epoch the node has seen |
| `EBUSY` | -16 | the LVS is fenced in this epoch (for `jc_grant_solo`) |
| `EAGAIN` | -11 | resync gate not passed (for `jc_unfence`, section 3.5) |
| `ENOENT` | -2 | unknown `jm_vuid` |
| `EINVAL` | -22 | malformed arguments; dual-node mode not enabled for HA RPCs |

### 3.1 `jc_set_dual_node` (extended)

New fields are optional; existing callers (`{"enable": bool}`) keep working with defaults.

```json
{"enable": true, "hold_ms": 2500, "lease_ttl_ms": 1500, "arbitration": true,
 "remote_jm_timeout_ms": 1000}
```

`remote_jm_timeout_ms` (campaign c2, F5): a remote JM that has not acknowledged for this long
counts as unavailable and the node starts holding. A frozen peer keeps its TCP connections, so
connection loss alone (NVMe keep-alive 5 s, IO timeout 8 s) is detected too late for the 3 s
budget. The node clamps it below `hold_ms` and reports it in `jc_ha_status`; the arbiter
configures a node again whose reported value differs. Should a node reject the field, the CP
retries without it, and only then falls back to the plain flag (`arbitration.set_dual_node`).

A node that wakes from a stall longer than its lease fences **every** LVS (normal and solo too)
with `ha_self_fenced` reason `stalled`, treats its lease as expired and does not abort. The
arbiter re-admits it like any self-fence (section 7.9).

Result: `true`. `arbitration: false` (default for old callers) keeps today's behaviour
(dual-node tolerance without hold); the CP sets it `true` only when the cluster flag
`two_node_arbitration` is on.

Every call resets the omitted fields to their defaults. On a cluster with
`two_node_arbitration` **every** `jc_set_dual_node` call of the CP (arbiter, node add, restart
flow) therefore carries the full arbitration config (`arbitration.dual_node_params`); a call
with only `enable` switches arbitration off on that node (campaign c1, B2). A node that rejects
the fields (built without the protocol) still gets the plain `{"enable": true}`, which keeps the
survivor from aborting. `preferred` is deprecated: the node still accepts it, ignores it and logs
a deprecation; the CP never sends it. `jc_ha_status` no longer reports it.

### 3.2 `jc_lease_renew`

```json
{"epoch": 8, "ttl_ms": 1500}
```

Result:

```json
{"epoch_seen": 8, "state": "normal", "lease_left_ms": 1500}
```

`state` is the most severe per-LVS state (`fenced` > `holding` > `solo` > `normal`).
Errors: `ESTALE`.

### 3.3 `jc_grant_solo`

```json
{"epoch": 8, "lvs": [3, 4], "ttl_ms": 1500}
```

Result: `{"granted": [3, 4]}`. Releases the hold (held IO executes) and lets the LVS lead
alone. Idempotent within an epoch. Errors: `ESTALE`, `EBUSY`, `ENOENT`.

### 3.4 `jc_fence`

```json
{"epoch": 8, "lvs": [3]}
```

Result: `{"fenced": [3]}`, returned only after the LVS is non-leader, its client port is
blocked and all held IO has been returned. The hublvol port is never blocked (section 6.1).
Idempotent. Errors: `ESTALE`, `ENOENT`.

### 3.5 `jc_unfence`

```json
{"epoch": 9, "lvs": [3]}
```

Result: `{"unfenced": [3]}`. The node may rejoin as secondary (the CP then runs the restart
flow, section 8). Errors: `ESTALE`, `ENOENT`, and `EAGAIN` while the resync gate is not passed.
The gate depends on the node's role for the LVS: on a **follower** it means "the remote JM has
reconnected"; on the **leader** it means "the remote JM is in sync".

### 3.6 `jc_ha_status`

No arguments. Result:

```json
{"instance": "6f1c2a90", "seq": 41, "dual_node": true, "arbitration": true,
 "epoch": 8, "lease_left_ms": 1210,
 "lvs": [{"jm_vuid": 3, "state": "holding", "since_ms": 640, "held_ios": 17,
          "hold_left_ms": 1860,
          "remote_jm": {"name": "remote_jm_8f2", "health": "unhealthy",
                        "reason": "unavailable"}}]}
```

`state` per LVS: `normal | holding | solo | fenced`. `reason`: `unavailable | blocked |
out_of_sync`. Used for resync after a restart or a queue overflow (section 4), and polled by
the arbiter to reconcile its view (section 7.5). `seq` is the highest event sequence the node
has pushed, so events with `seq` at or below it are already reflected in the status.

### 3.7 `jc_wait_events` (long-poll)

Replaces fixed-interval polling of `distr_status_events_get` for HA events. Still initiated by
the CP: there is no connection from the edge to the CP.

```json
{"instance": "6f1c2a90", "after_seq": 40, "timeout_ms": 20000, "max": 256}
```

- `instance`: the instance id the collector last saw, or `""` on first call.
- `after_seq`: the highest `seq` the collector has processed (acknowledgement); `0` on first call.
- `timeout_ms`: how long the node may hold the request (default 20000, max 60000).
- `max`: maximum events per answer (default 256).

Result:

```json
{"instance": "6f1c2a90", "seq": 42, "overflow": false, "superseded": false,
 "events": [ {"seq": 41, "...": "event, section 5"}, {"seq": 42, "...": "..."} ]}
```

Behaviour:

1. If events with `seq > after_seq` are queued and `instance` matches, answer at once with up to
   `max` of them.
2. Otherwise hold the request (deferred response) until an event is pushed, then answer; or
   answer with `events: []` at `timeout_ms`. `seq` in the answer is the node's current highest seq.
3. `instance` is generated at SPDK start. If the caller's `instance` differs (node restarted),
   answer at once with the current `instance`, `seq` and `events: []`; the collector resyncs from
   `jc_ha_status` and continues with `after_seq` = the returned `seq`.
4. Delivery is at least once. Events stay in a bounded ring (default 1024) until a later call
   acknowledges them with a higher `after_seq`. If unacknowledged events are dropped, the next
   answer sets `overflow: true`; the collector resyncs from `jc_ha_status`.
5. One waiter per node: a new call completes a pending one at once with `events: []` and
   `superseded: true`, and takes its place.
6. SPDK implementation: the handler keeps the `spdk_jsonrpc_request` and does not respond;
   a timer poller on the RPC thread handles the timeout. Pushing an event (from any thread) sends a
   message to the RPC thread, which completes the waiting request. Timeout, supersession and
   completion all run on the RPC thread, so no locking around the request is needed.

Capability probe: the CP calls `rpc_get_methods`; the protocol is used only if it lists
`jc_wait_events`, `jc_ha_status`, `jc_lease_renew`, `jc_grant_solo`, `jc_fence` and
`jc_unfence`. Older nodes keep today's behaviour and are read with `distr_status_events_get`
every 1 s.

## 4. Collector (sbcli, Go)

- Runs in the CP image as its own process, one goroutine per storage node of each cluster with
  `two_node_arbitration` on.
- Each goroutine loops on `jc_wait_events`; on transport errors it reconnects with backoff
  (100 ms, doubling, max 5 s) and re-sends the last `instance` and `after_seq`.
- On `instance` change or `overflow: true`: call `jc_ha_status`, emit a synthetic
  `ha_resync` event carrying the full status to the arbiter, continue from the returned `seq`.
- Events go to the arbiter in order per node with `(cluster_id, node_id, instance, seq)`; the
  arbiter deduplicates on that key.
- Events also go to the existing cluster event log (`events_controller`), so they appear in
  `sbctl cluster get-logs`.

## 5. Events

All events delivered by `jc_wait_events` share these fields:

```json
{"seq": 41, "timestamp": "2026-10-09T10:15:02.418Z", "event_type": "device_status",
 "jm_vuid": 3, "status": "remote_jm_unhealthy", "reason": "unavailable",
 "ha_state": "holding", "epoch": 8}
```

`status` values. The first two exist today and are emitted by `dst_to_string` in
`bdev_distrib_impl.cpp` (the `jm_unhealthy` name in a header comment is stale):

| `status` | Meaning |
| --- | --- |
| `remote_jm_unhealthy` | The peer's JM became unusable (`reason`: `unavailable`, `blocked`, `out_of_sync`). |
| `remote_jm_healthy` | The peer's JM is back in sync. |
| `ha_hold_started` | The LVS froze and holds writes (dual-node with arbitration only). |
| `ha_hold_expired` | The hold deadline passed without a verdict; the LVS fenced itself (`ha_state` `fenced`). |
| `ha_solo` | The LVS leads alone under a `jc_grant_solo`. |
| `ha_fenced` | The LVS is fenced by `jc_fence`. |
| `ha_self_fenced` | The LVS fenced itself (deadline or lease expired while holding). |
| `ha_lease_expired` | The lease expired while holding. |
| `ha_unfenced` | `jc_unfence` completed. |
| `ha_normal` | The LVS went back to `normal` outside unfence; `reason` `demoted`: a demotion of the LVS reset `solo` to `normal`. |

Existing consumers of `distr_status_events_get` keep receiving `remote_jm_unhealthy` and
`remote_jm_healthy`; the new fields are additive.

## 6. Node state machine (per LVS)

| From | To | Trigger |
| --- | --- | --- |
| normal | holding | remote JM unhealthy, dual-node with arbitration |
| holding | solo | `jc_grant_solo` of the current epoch before the deadline; nothing else |
| holding | fenced | `jc_fence`; or the hold deadline without a verdict (every node); or lease expired while holding |
| solo | normal | the remote JM is healthy again (after the resync on the leader, section 8); or the LVS was demoted (`ha_normal`, reason `demoted`) |
| fenced | normal | `jc_unfence` after the resync gate |
| any | aborted | `abort` verdict or the existing network-outage abort (0 JMs left) |

A JC error on an LVS that is holding or fenced does not abort the node: the LVS stays fenced
until a grant or an unfence.

### 6.1 What a fence blocks

A fence (`jc_fence`, self-fence at the deadline or on lease expiry) blocks only the LVS's
**client** port. It never blocks the LVS's hublvol port: the returning peer's restart flow needs
that port (`connect_to_hublvol`) to resync, and a blocked hublvol port deadlocks healing
(campaign c1, B2: the restart failed with `connect_to_hublvol failed`, and `jc_unfence`
answered `EAGAIN` until the peer's JM was healthy). The 10 s rule-removal poller
(`spdk_lvs_remove_rules_poller`) only lifts a hublvol-port rule; it never lifts the client-port
rule of a fenced LVS (invariant 6).

## 7. Arbiter (sbcli)

### 7.1 State machine (per cluster)

| From | To | Trigger and action |
| --- | --- | --- |
| steady | deciding | unhealthy, hold or lease-expiry event |
| deciding | partitioned | both nodes report each other and the CP reaches both (a cut link, both alive and renewing): policy `fence_both`. Raise epoch, `jc_fence` every LVS on **both** nodes, grant nobody: IO stops |
| deciding | degraded | one node silent with its lease expired, lost (section 7.6), stopped by the CP (planned loss, section 7.8) or positively fenced: raise epoch, `jc_grant_solo` the survivor |
| steady | degraded | the same, **without a report from the survivor** (section 7.8): the arbiter knows the peer is gone, the survivor is reachable and still runs an LVS normal or holding |
| degraded | degraded | a new loss while degraded (campaign c2, F2): the winner holds again, or its peer is gone again while the winner runs an LVS normally: decide afresh at a new epoch, grant again |
| degraded | degraded | a lost node answers again: raise epoch, `jc_fence` it for every LVS its peer leads alone, before its next lease renewal |
| partitioned, degraded | healing | both report `remote_jm_healthy` and stay healthy for `stable_for_s` |
| partitioned | partitioned | link between the nodes answers again but the remote JMs stay unavailable: reconnect them, then hand one node to the restart flow (section 7.9) |
| healing | steady | resync gate passed, restart flow done, `jc_unfence` done, optional failback done; remove the taint |

The arbiter writes the new epoch and the verdict to FDB in one transaction **before** sending
any verdict RPC. On restart or CP failover it reloads `ClusterArbitration`, raises the epoch and
re-sends the last verdict.

### 7.2 FDB record `ClusterArbitration`

One record per cluster, key `{cluster_id}`, model in `simplyblock_core/models/`:

```json
{"cluster_id": "1102ec3c-...", "epoch": 8, "state": "degraded",
 "lvs": [{"jm_vuid": 3, "leader": "e40b4b28-...", "state": "solo",
          "fenced_node": "d5e8dc90-...", "since": 1760004902418}],
 "leases": {"e40b4b28-...": {"granted_at": 1760004902100, "ttl_ms": 1500},
            "d5e8dc90-...": {"granted_at": 1760004900900, "ttl_ms": 1500}},
 "verdicts": [{"epoch": 8, "at": 1760004902418, "kind": "partition",
               "winner": "e40b4b28-...", "signals": {"...": "events and reachability"}}]}
```

`verdicts` keeps the last 50 entries. A partition verdict lists both fenced nodes per LVS in
`fence_all`. `leadership_restored_at` is the time of the last leadership reconciliation
(section 7.7). `planned_loss` holds the CP-initiated losses in progress (section 7.8):
`{node_id: {"at": ms, "instance": "...", "reason": "restart"}}`. Each verdict records the state
it was decided in (`from_state`); the heal uses it (section 8). `preferred_node` is deprecated: stored records keep loading, the field is
ignored and never written.

### 7.3 Lease renewal

The arbiter renews each node's lease every `lease_renew_ms` with `jc_lease_renew`, as long as
the node is reachable. A failed renewal is not a verdict: it only matters if the node is holding
(invariant 4).

### 7.4 CP API

| Method | Path | Body / result | Use |
| --- | --- | --- | --- |
| GET | `/api/v2/clusters/{cluster_id}/arbitration` | the `ClusterArbitration` record plus live lease ages | console, operator, support |
| PUT | `/api/v2/clusters/{cluster_id}/arbitration/preferred` | `{"node_id": "..."}` | **deprecated, ignored**: answers 204 for older operators, stores and pushes nothing |
| POST | `/api/v2/clusters/{cluster_id}/arbitration/override` | `{"winner": "node_id", "reason": "..."}` | operator decision when the arbiter cannot decide; audited in the cluster log |
| PUT | `/api/v2/clusters/{cluster_id}` | `{"two_node_arbitration": true}` | feature flag (cluster field) |

### 7.5 Bookkeeping: the arbiter's view is reconciled, never append-only

The arbiter's per-node, per-LVS view (holding, solo, fenced, remote-JM health) is a mirror of the
node, not a log of past events:

- it is replaced from `jc_ha_status` every `reconcile_ms` (1 s), and at once after a reconnect
  (a node that answers again after failed calls), after an `instance` change (node restarted) and
  after a gap in the event sequence;
- events between two polls update it in between; an event at or below the status `seq` is
  dropped;
- a status that shows `arbitration: false` makes the arbiter push its `jc_set_dual_node` again.

Every verdict that depends on the view (fencing a returning node, grants after a peer fence,
partition decisions) uses the reconciled state. An append-only view fenced the legitimate
survivor after a link restore in the campaign (c1, 08 run 1).

### 7.6 SPDK-down evidence (lost node)

The CP reaches a node's SPDK through the storage node's HTTP proxy. When SPDK is gone but the
proxy still runs, the proxy answers **HTTP 500** instead of refusing the connection (campaign c1,
01b and 02b: the grant was withheld and the survivor fenced itself at the deadline). The arbiter
treats HTTP 500 on the SPDK-facing calls it makes (`jc_lease_renew`, `jc_ha_status`) as loss of
the node, like a refused connection:

- it takes **2 consecutive** 500 answers (`spdk_down_http500_streak`; lease renewals and status
  polls count alike, any other outcome resets the streak). The two-poll rule is the smaller of
  the two options considered (the other was "within one lease TTL"): with renewals every 250 ms it
  costs about 250 ms, and a single transient 500 never grants;
- a lost node counts as fenced for invariant 2: its peer is granted at once, without waiting for
  the lease to expire;
- when a lost node answers again, it is fenced first (raise epoch, `jc_fence`) for every LVS its
  peer leads alone according to the peer's fresh `jc_ha_status` (the record's grants only if the
  peer cannot be read), and only then gets a lease again. A node that answers `ENOENT`/`EINVAL`
  is a restarted SPDK that does not know the LVS and cannot be a stale writer; the restart flow
  re-admits it.

Caveat: the proxy also answers 500 when an SPDK RPC times out on the socket, so a stalled (not
dead) SPDK that times out twice in a row is treated as lost. Its peer is then granted while it
is alive; the returning-node fence closes that window at its next answer.

### 7.7 Leadership reconciliation

After fence -> resync -> unfence an LVS can be left without a leader on either node (campaign c1,
B6). Every `leadership_reconcile_s` (10 s), while the cluster is `steady`, the arbiter checks
each node's primary LVS. One whose primary is online and healthy, whose members are all online
and answer, whose HA state is `normal` on both nodes, and that no member leads on two
consecutive passes, is restored through the existing guarded leaderless-recovery path
(`storage_node_ops._recover_leaderless_lvs`: single-flight lock with cooldown, no restart or
port-allow task active, JM quorum on the primary, metadata reload before the grant, grant
verified), never by ad-hoc RPCs. A restore, and every verdict epoch, resets the migration
runners' in-memory retry backoff, so a rebuild deferred on "not the leader" resumes at once.

### 7.8 Peer gone without a report: planned loss and frozen peers (campaign c2)

Campaign c2 found two cases where the survivor reports nothing for seconds although its peer is
gone, so a rule that waits for the survivor's hold misses the 3 s budget or lets the survivor
fence itself at the hold deadline:

- **F5, frozen peer** (SIGSTOP, 02a): the frozen SPDK keeps its TCP connections, so the survivor
  saw the remote JM as connected until the peer woke and aborted (6.2 s). The node side now
  holds after `remote_jm_timeout_ms` without an acknowledgement (section 3.1) and fences itself
  when it wakes from a stall longer than its lease.
- **F3, CP restart of a live node**: the restarting node's proxy stays up and answers "framework
  not initialized", never HTTP 500, so the node was neither lost nor silent; its peer held and
  fenced both LVS at the deadline.

Rules:

- **Grant without a report.** The arbiter grants the survivor at once when it knows the peer is
  gone -- positively (HTTP 500 streak, planned loss, operator fencing) or because the peer is
  silent and its lease ran out beyond `lease_margin_ms` -- the survivor is reachable, and it
  still runs an LVS `normal` or `holding`. Not when both nodes are unreachable (uplink loss,
  invariant 4), not for a peer that was never leased, and not again once the survivor is solo
  everywhere. Safe without the survivor's report: a peer whose lease has expired fences itself
  as soon as it starts holding, a node that wakes from a stall longer than its lease fences
  itself, and a gone peer does not write at all. A grant to a survivor whose remote JM is still
  healthy only permits it to continue alone if it loses the peer later.
- **Planned loss.** Every CP path that stops SPDK (restart, shutdown, restart abort, the
  kill-until-dead helper) first records a planned-loss marker in the record
  (`arbitration.planned.note_planned_loss`, best effort, never fails or delays the stop). A node
  with a marker counts as gone: if it still answers, the verdict fences it first, best effort
  (only `ESTALE` withholds the grant; it is going away anyway), then grants its peer. The marker
  ends when the node answers with a new SPDK instance, when it still serves the same instance
  60 s later (the stop did not happen), or after `planned_loss_max_s`.

### 7.9 Self-fenced nodes and partition recovery (campaign c2)

- **F4, self-fenced node.** A node that fenced itself (hold deadline, lease expiry, stall) while
  no verdict is in force stayed fenced for 30 minutes in c2: unfence ran only in `healing`. In
  `steady` and `degraded` the arbiter now calls `jc_unfence` at the current epoch for every LVS
  the node fenced itself, once the node's own view reports the remote JM healthy, at most every
  `self_fence_readmit_ms` (EAGAIN until the resync gate passes). An LVS the arbiter fenced (the
  record's `fenced_node`) is left to the heal. Leadership then comes back through section 7.7.
- **F6, partition recovery.** After a partition, a both-links cut or a lossy link healed, the
  remote JMs stayed unavailable and both nodes fenced, so the stable-window heal never started
  (c2: 07, 08, 10b; only a node restart recovered). While `partitioned` with both nodes
  reachable by the CP, the arbiter probes the link between them every `link_probe_s` (ping of
  the peer's data IP from each node's data NIC, both ways). While it answers and the remote JMs
  stay unhealthy, it reconnects the remote JMs (JM mesh repair) every `jm_reconnect_s`; after
  `partition_restart_after_s` it hands one node (the one whose primary LVS has no leader, else
  the second member) to the restart flow, once per partition. Probes and reconnects run off the
  decision loop. Healthy remote JMs then lead to the ordinary heal (section 8).

## 8. Healing and re-admission

1. Both nodes report `remote_jm_healthy`; after `stable_for_s` the arbiter enters `healing`.
2. Resync gate: JC replicates the missing journal to the returning JM; distrib rebuilds the
   writes the fenced node missed. `jc_unfence` returns `EAGAIN` until the gate passes (section
   3.5: remote reconnected on a follower, in sync on the leader).
3. After a **partition** (`fence_both`, the heal verdict's `from_state`), nobody was granted and
   no node wrote alone: the heal is `jc_unfence` on both (the leader re-levels its journal by
   itself, step 4) and the leadership reconciliation in `steady` (section 7.7); no node goes
   through the restart flow (campaign c2, F6). After **degraded**, re-admission reuses the
   restart flow on the fenced node
   (`storage_node_ops.py`: `recreate_lvstore_on_non_leader` / `recreate_lvstore_on_sec`,
   hublvol reconnect, non-optimized ANA path), then `jc_unfence`. The fence leaves the hublvol
   port open (section 6.1), so the reconnect works while the survivor is fenced.
4. Resync on the leader. The survivor stays `solo` after its peer returns until JC re-levels
   the recovered remote JM (campaign c1, B1). The node starts that resync by itself, on the LVS
   leader only, and retries it every 20 s. As a harmless backup, the CP calls
   `jc_explicit_synchronization {"jm_vuid": v}` at the end of the restart flow for each LVS the
   surviving node still reports `solo`, on that LVS's **leader** only (confirmed by its
   `lvs leadership` right before the call). **The CP never calls it on a follower**: explicit
   synchronization makes the calling node's JC the journal writer, so on a follower it raises a
   `writer_conflict` on the real leader, which drops LVS leadership and blocks its ports (B6). An
   LVS without a confirmed leader is skipped (section 7.7). The call is idempotent.
5. Optional failback as a planned handover with the acting-leader quiesce from #1439
   (`_presumed_acting_leader`: `jc_disable_replication`, `bdev_distrib_check_inflight_io`,
   set non-leader).
6. The arbiter asks the operator to remove the storage-fenced taint.

### 8.1 Restart flow precheck during a re-level (campaign c2, F1)

Right after the returning node's SPDK is up, the LVS leader re-levels the returning JM by itself
(step 4). While it does, `jc_compression` on the leader answers "Compression request failed";
the restart used to abort on that one answer and kill the new SPDK (c2 01b). The precheck now
retries for `RESTART_JC_COMPRESSION_RETRY_S` (60 s) and aborts only if the failure outlasts it.

## 9. Operator (simplyblock-operator, managed profile on the edge)

- **Taint** `storage.simplyblock.io/fenced=<epoch>:NoExecute` on the Kubernetes node of a fenced
  storage node, set and removed on the arbiter's request; KubeVirt then restarts the affected
  VMs on the other node.
- **Request channel**: the operator watches the arbiter state through the CP API
  (`GET .../arbitration`); a `fenced_node` with a taint request is applied, a healed state removes
  it. No new connection from the edge to the CP is needed beyond what the managed profile uses.
- **Status to the CP**: the operator reports the node's remediation state (Node Health Check
  condition, `out-of-service` taint present, BMC fence done) on the StorageNode status; the
  arbiter reads it as positive fencing evidence.
- **CR fields** on the cluster (managed profile): `spec.twoNode.arbitration: bool`,
  `spec.twoNode.holdMs`, `spec.twoNode.leaseTtlMs` (all optional; defaults below).
  `spec.twoNode.preferredNode` is deprecated: accepted on stored objects, ignored.

### 9.1 Client requirements

Hosts connect over NVMe/TCP native multipath (iopolicy `numa`) with:

| Setting | Value | Why |
| --- | --- | --- |
| `--keep-alive-tmo` | 4 s | a silent path must be detected within the hold window (`hold_ms` 2500 < KATO) |
| `--fast_io_fail_tmo` | 8 s | more than KATO + `hold_ms`: held IO is answered before the kernel fails it |
| `--ctrl-loss-tmo` | 3600 s | a fenced path is retried, not dropped |
| `--reconnect-delay` | 2 s | |

The CP generates these connect parameters for clusters with `two_node_arbitration`
(`utils/nvme.py`; other clusters keep keep-alive 8 s). Even so, a silent path loss stalls IO for
KATO plus up to one keep-alive interval (about 5 s measured) before multipath fails over; no
setting brings that below KATO.

## 10. Defaults

| Setting | Default | Where |
| --- | --- | --- |
| `hold_ms` | 2500 | `jc_set_dual_node`; must stay below the hosts' KATO (`constants.KATO`, 5000 ms) |
| `lease_ttl_ms` | 1500 | `jc_set_dual_node`, `jc_lease_renew` |
| `lease_renew_ms` | 250 | arbiter |
| `lease_margin_ms` | 500 | arbiter, invariant 2 |
| `stable_for_s` | 30 | arbiter, healing |
| `reconcile_ms` | 1000 | arbiter, `jc_ha_status` reconciliation (section 7.5) |
| `spdk_down_http500_streak` | 2 | arbiter, consecutive HTTP 500 answers that make a node lost (section 7.6) |
| `leadership_reconcile_s` | 10 | arbiter, leadership reconciliation pass (section 7.7) |
| `remote_jm_timeout_ms` | 1000 | `jc_set_dual_node`, remote JM acknowledgement timeout (section 3.1) |
| `planned_loss_max_s` | 900 | arbiter, longest planned-loss marker (section 7.8) |
| `self_fence_readmit_ms` | 2000 | arbiter, unfence retry of a self-fenced node (section 7.9) |
| `link_probe_s` | 10 | arbiter, link probe while partitioned (section 7.9) |
| `jm_reconnect_s` | 30 | arbiter, remote JM reconnect while partitioned (section 7.9) |
| `partition_restart_after_s` | 120 | arbiter, link up but JMs unavailable before the restart flow (section 7.9) |
| `jc_compression` precheck retry | 60 s | restart flow, while the leader re-levels the returning JM (section 8.1) |
| client keep-alive | 4 s | connect parameters of arbitrated clusters (section 9.1) |
| `wait_timeout_ms` | 20000 | collector, `jc_wait_events` |
| event ring | 1024 events | node |
| collector backoff | 100 ms doubling to 5 s | collector |
| fallback polling | 1 s | collector, nodes without `jc_wait_events` |

## 11. Work packages

| Repo | Package |
| --- | --- |
| ultra | Hold trigger in dual-node mode; lease and epoch; `jc_grant_solo`, `jc_fence`, `jc_unfence`, `jc_lease_renew`, `jc_ha_status`, `jc_wait_events`; event fields and new statuses; resume gating after JCERR |
| spdk fork | Hold on JC's signal through the frozen-IO queue (`blobstore.c:blob_execute_queued_io`); path error instead of EIO on fence; no rule removal while fenced (`lvol.c:spdk_lvs_remove_rules_poller`) |
| sbcli | Arbiter service, `ClusterArbitration` model, API, verdict RPC client methods, healing flow, feature flag; RPC client methods for the new RPCs |
| sbcli | Go collector (section 4), packaged in the CP image |
| operator | Taint handling, remediation status, CR fields |
| test | Unit (arbiter table, epochs, lease arithmetic, long-poll sequencing); two-SPDK tests with tc/iptables; bed scenarios: storage link cut, uplink cut, both, power-off, SIGSTOP, CP restart mid-hold, latency injection; OpenShift two-node with arbiter and with fencing |

## 12. Open questions

- Alireza: trigger the lvstore freeze directly from `helper_check_jm_health`, or in distrib on the
  event? Is holding user writes while JC keeps compressing and replicating on the local JM safe?
- Alireza: fix the stale `jm_unhealthy` header comment; add `reason`, `ha_state`, `epoch`.
- SPDK team: return held IO as an ANA/path error instead of EIO on fence; confirm no timer
  re-promotes a fenced LVS.
- Team: per-site defaults for `hold_ms`, `lease_ttl_ms`, `stable_for_s` over real edge WAN links;
  whether the arbiter runs in the CP task runner or as its own deployment.
- After a `fence_both` partition both nodes are fenced and no LVS has a leader; healing relies on
  `jc_unfence` passing its resync gate without a leader, then on leadership reconciliation
  (section 7.7). Not yet verified on a bed (in c1 the fenced follower aborted, B7).
