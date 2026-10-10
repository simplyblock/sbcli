# Two-node arbitration: implementation contract

Status: approved for implementation (2026-10-09). Design rationale, diagrams and failure
analysis: [`two-node-ha-design.pdf`](two-node-ha-design.pdf) (revision 2). This document is
the binding contract between the work packages: ultra (JC, distrib), the SPDK fork (lvol,
blobstore), sbcli (arbiter, Go event collector, API) and the simplyblock operator.

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
| Fenced | Node is non-leader for an LVS, its LVS ports are blocked, held IO returned as a path error. |
| Preferred node | The node allowed to continue alone when neither the CP nor the peer is reachable. |

## 2. Invariants

1. A node leads an LVS alone only with a `jc_grant_solo` of the current epoch, or at the hold
   deadline if it is the preferred node.
2. The arbiter sends `jc_grant_solo` for an LVS only if the peer is fenced for it: the peer
   acknowledged `jc_fence` in this epoch, or the peer's lease expired more than
   `lease_ttl_ms + lease_margin_ms` ago while the peer was holding.
3. The arbiter never grants the non-preferred node solo operation merely because the preferred
   node went silent; that needs positive fencing (BMC fence or `out-of-service` taint).
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
| `EAGAIN` | -11 | resync not complete (for `jc_unfence`) |
| `ENOENT` | -2 | unknown `jm_vuid` |
| `EINVAL` | -22 | malformed arguments; dual-node mode not enabled for HA RPCs |

### 3.1 `jc_set_dual_node` (extended)

New fields are optional; existing callers (`{"enable": bool}`) keep working with defaults.

```json
{"enable": true, "preferred": false, "hold_ms": 2500, "lease_ttl_ms": 1500,
 "arbitration": true}
```

Result: `true`. `arbitration: false` (default for old callers) keeps today's behaviour
(dual-node tolerance without hold); the CP sets it `true` only when the cluster flag
`two_node_arbitration` is on.

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

Result: `{"fenced": [3]}`, returned only after the LVS is non-leader, its ports are blocked and
all held IO has been returned. Idempotent. Errors: `ESTALE`, `ENOENT`.

### 3.5 `jc_unfence`

```json
{"epoch": 9, "lvs": [3]}
```

Result: `{"unfenced": [3]}`. The node may rejoin as secondary (the CP then runs the restart
flow, section 8). Errors: `EAGAIN` until JC has resynced the remote JM, `ESTALE`, `ENOENT`.

### 3.6 `jc_ha_status`

No arguments. Result:

```json
{"instance": "6f1c2a90", "seq": 41, "dual_node": true, "arbitration": true,
 "preferred": false, "epoch": 8, "lease_left_ms": 1210,
 "lvs": [{"jm_vuid": 3, "state": "holding", "since_ms": 640, "held_ios": 17,
          "hold_left_ms": 1860,
          "remote_jm": {"name": "remote_jm_8f2", "health": "unhealthy",
                        "reason": "unavailable"}}]}
```

`state` per LVS: `normal | holding | solo | fenced`. `reason`: `unavailable | blocked |
out_of_sync`. Used for resync after a restart or a queue overflow (section 4).

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
| `ha_hold_expired` | The hold deadline passed without a verdict; `ha_state` tells the outcome. |
| `ha_solo` | The LVS leads alone (grant, or preferred node at the deadline). |
| `ha_fenced` | The LVS is fenced by `jc_fence`. |
| `ha_self_fenced` | The LVS fenced itself (deadline or lease expired while holding). |
| `ha_lease_expired` | The lease expired while holding. |
| `ha_unfenced` | `jc_unfence` completed. |

Existing consumers of `distr_status_events_get` keep receiving `remote_jm_unhealthy` and
`remote_jm_healthy`; the new fields are additive.

## 6. Node state machine (per LVS)

| From | To | Trigger |
| --- | --- | --- |
| normal | holding | remote JM unhealthy, dual-node with arbitration |
| holding | solo | `jc_grant_solo` of the current epoch before the deadline; or deadline and preferred node |
| holding | fenced | `jc_fence`; or deadline without verdict (non-preferred); or lease expired while holding |
| solo | normal | the peer's `jc_unfence` completed and the remote JM is healthy again |
| fenced | normal | `jc_unfence` after the resync gate |
| any | aborted | `abort` verdict or the existing network-outage abort (0 JMs left) |

## 7. Arbiter (sbcli)

### 7.1 State machine (per cluster)

| From | To | Trigger and action |
| --- | --- | --- |
| steady | deciding | unhealthy, hold or lease-expiry event |
| deciding | partitioned | both nodes report each other and both leases are valid: raise epoch, `jc_fence` the loser per LVS (the current leader wins), then `jc_grant_solo` the winner; request the storage-fenced taint for the loser |
| deciding | degraded | one node silent with its lease expired while holding, or positively fenced: raise epoch, `jc_grant_solo` the survivor |
| partitioned, degraded | healing | both report `remote_jm_healthy` and stay healthy for `stable_for_s` |
| healing | steady | resync gate passed, restart flow done, `jc_unfence` done, optional failback done; remove the taint |

The arbiter writes the new epoch and the verdict to FDB in one transaction **before** sending
any verdict RPC. On restart or CP failover it reloads `ClusterArbitration`, raises the epoch and
re-sends the last verdict.

### 7.2 FDB record `ClusterArbitration`

One record per cluster, key `{cluster_id}`, model in `simplyblock_core/models/`:

```json
{"cluster_id": "1102ec3c-...", "epoch": 8, "state": "partitioned",
 "preferred_node": "e40b4b28-...",
 "lvs": [{"jm_vuid": 3, "leader": "e40b4b28-...", "state": "solo",
          "fenced_node": "d5e8dc90-...", "since": 1760004902418}],
 "leases": {"e40b4b28-...": {"granted_at": 1760004902100, "ttl_ms": 1500},
            "d5e8dc90-...": {"granted_at": 1760004900900, "ttl_ms": 1500}},
 "verdicts": [{"epoch": 8, "at": 1760004902418, "kind": "partition",
               "winner": "e40b4b28-...", "signals": {"...": "events and reachability"}}]}
```

`verdicts` keeps the last 50 entries.

### 7.3 Lease renewal

The arbiter renews each node's lease every `lease_renew_ms` with `jc_lease_renew`, as long as
the node is reachable. A failed renewal is not a verdict: it only matters if the node is holding
(invariant 4).

### 7.4 CP API

| Method | Path | Body / result | Use |
| --- | --- | --- | --- |
| GET | `/api/v2/clusters/{cluster_id}/arbitration` | the `ClusterArbitration` record plus live lease ages | console, operator, support |
| PUT | `/api/v2/clusters/{cluster_id}/arbitration/preferred` | `{"node_id": "..."}` | choose the preferred node; pushed with `jc_set_dual_node` |
| POST | `/api/v2/clusters/{cluster_id}/arbitration/override` | `{"winner": "node_id", "reason": "..."}` | operator decision when the arbiter cannot decide; audited in the cluster log |
| PUT | `/api/v2/clusters/{cluster_id}` | `{"two_node_arbitration": true}` | feature flag (cluster field) |

## 8. Healing and re-admission

1. Both nodes report `remote_jm_healthy`; after `stable_for_s` the arbiter enters `healing`.
2. Resync gate: JC replicates the missing journal to the returning JM; distrib rebuilds the
   writes the fenced node missed. `jc_unfence` returns `EAGAIN` until both are complete.
3. Re-admission reuses the restart flow on the fenced node
   (`storage_node_ops.py`: `recreate_lvstore_on_non_leader` / `recreate_lvstore_on_sec`,
   hublvol reconnect, non-optimized ANA path), then `jc_unfence`.
4. Optional failback as a planned handover with the acting-leader quiesce from #1439
   (`_presumed_acting_leader`: `jc_disable_replication`, `bdev_distrib_check_inflight_io`,
   set non-leader).
5. The arbiter asks the operator to remove the storage-fenced taint.

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
  `spec.twoNode.preferredNode: <k8s node name>`, `spec.twoNode.holdMs`, `spec.twoNode.leaseTtlMs`
  (all optional; defaults below).

## 10. Defaults

| Setting | Default | Where |
| --- | --- | --- |
| `hold_ms` | 2500 | `jc_set_dual_node`; must stay below the hosts' KATO (`constants.KATO`, 5000 ms) |
| `lease_ttl_ms` | 1500 | `jc_set_dual_node`, `jc_lease_renew` |
| `lease_renew_ms` | 250 | arbiter |
| `lease_margin_ms` | 500 | arbiter, invariant 2 |
| `stable_for_s` | 30 | arbiter, healing |
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
  how the preferred node is chosen; whether the arbiter runs in the CP task runner or as its own
  deployment.
