# Two-node clusters: aligning storage fencing with Kubernetes

Status: draft for review (2026-10-22). Extends
[`two-node-arbitration.md`](two-node-arbitration.md) (the arbitration contract); where this
document says nothing, that one applies unchanged.

Two change requests, one per deployment topology. They are mirror images:

| | Mode `witness` (CR A) | Mode `pacemaker` (CR B) |
|---|---|---|
| Who decides which node continues | The arbiter on the hub control plane | Pacemaker in the OpenShift two-node-with-fencing topology (Redfish power fencing) |
| simplyblock arbiter in the fencing decision | yes | **no** (there is no witness) |
| Pacemaker | none | leads |
| Who follows | Kubernetes follows the storage | the storage follows Pacemaker; Kubernetes follows the fence |
| Control plane on the hub | decides, fences, heals | reachable, unchanged management (restart flow, rebuild, failback); never decides |
| Cluster setting | `two_node_fencing_authority: witness` (default) | `two_node_fencing_authority: pacemaker` |

Both modes require `two_node_arbitration: true`: the node-side protocol (hold, lease, epoch,
grant, fence, unfence, section 3 of the arbitration contract) is the same in both. Only the party
that sends the verdicts differs.

## 1. Shared: the Kubernetes side

In both modes the losing node's Kubernetes `Node` ends up with the condition

```yaml
type: StorageFenced
status: "True"
reason: WitnessVerdict | PacemakerFence
message: "epoch 12: fenced by simplyblock; host NQN revoked on <winner>"
```

and a NodeHealthCheck (medik8s) remediates on it:

```yaml
apiVersion: remediation.medik8s.io/v1alpha1
kind: NodeHealthCheck
metadata: {name: simplyblock-storage-fenced}
spec:
  selector: {matchExpressions: [{key: node-role.kubernetes.io/worker, operator: Exists}]}
  maxUnhealthy: 1          # REQUIRED: the default minHealthy 51% never remediates 1 of 2 nodes
  unhealthyConditions:
    - {type: StorageFenced, status: "True", duration: 2s}
  remediationTemplate:
    apiVersion: storage.simplyblock.io/v1alpha1
    kind: StorageFenceRemediationTemplate
    namespace: simplyblock
    name: default
```

`StorageFenceRemediation` is an NHC external remediation (simplyblock operator). It skips power
fencing and applies `node.kubernetes.io/out-of-service=nodeshutdown:NoExecute` (non-graceful node
shutdown): pods are force-deleted, VolumeAttachments force-detached, and the VMs restart on the
other node. Before it applies the taint it checks again, every time:

1. the `StorageFenced` condition is still `True` with the same epoch;
2. the storage side confirms the fence: mode `witness`, `host_fences[<node>].state == revoked`
   in `GET /clusters/{id}/arbitration`; mode `pacemaker`, the node agent saw Pacemaker's fence
   of that node (section 4.3);
3. every VolumeAttachment on the node belongs to the simplyblock CSI driver. The out-of-service
   taint force-detaches **every** volume of the node, and only simplyblock volumes are fenced.
   Another driver's attachment blocks the taint (`spec.allowForeignVolumes: true` overrides it).

When NHC deletes the remediation (the condition is gone), the remediation removes the taint.

Who sets and clears the condition is mode specific (sections 3.4 and 4.4). The controller that
writes it runs as a DaemonSet (`simplyblock-fence-agent`), not as a leader-elected Deployment:
when the leader was on the lost node, leader election alone costs about 15 s. Both nodes'
agents write the same condition with the same content; the write is idempotent.

## 2. When a node's host is fenced, and when not

The out-of-service taint is safe only when the loser cannot write any more, through **any**
path. A fence of the loser's LVS blocks only the loser's own client port (arbitration contract,
section 6.1); the loser's host still has its NVMe paths to the winner, and the winner, now solo,
accepts any host (subsystems are created with `allow_any_host` unless the volume has
`allowed_hosts`). A VM still running on the loser would keep writing through the winner while
Kubernetes starts a second instance of it on the winner.

Hence **host fencing**: the loser's client host NQN is removed from every volume subsystem on the
winner (section 3.2). `StorageFenced` is set only after that.

Host fencing applies only when the loser's host state is **unknown**. It is never applied when the
host is known to be alive and only the storage service is affected, because the VMs there keep
running through the winner's path and must not be restarted:

| Loss | Host fence + `StorageFenced` |
|---|---|
| Peer silent, lease expired, survivor reports the peer's JM unhealthy | yes |
| Partition, winner policy (section 3.3) | yes (the loser) |
| Operator override (`POST .../arbitration/override`) | yes (operator-asserted) |
| Positive fencing evidence (`PUT .../arbitration/remediation`) | yes (host already off) |
| SPDK down, proxy answers HTTP 500 (`spdk_lost`) | **no**: the host is alive |
| Planned loss (CP restart/shutdown of the storage node) | **no** |
| Precautionary grant: peer unreachable from the hub, survivor's remote JM still healthy (hub-uplink loss of the peer) | **no**, until the survivor itself reports the peer's JM unhealthy |

## 3. Mode `witness` (CR A): the hub decides, Kubernetes follows

### 3.1 Settings

| Field | Where | Meaning |
|---|---|---|
| `two_node_fencing_authority` | Cluster | `witness` (default) |
| `two_node_host_fencing` | Cluster | `true` enables host fencing and the Kubernetes integration |
| `client_host_nqn` | StorageNode | kernel NVMe host NQN of the Kubernetes node co-located with this storage node; reported by the operator (`PUT .../arbitration/client-host`) |

Without `client_host_nqn` for the loser nothing is revoked, `host_fences[<loser>].state` is
`unavailable` and `StorageFenced` is never set: Kubernetes is not told to act on an unverified
fence. Logged as `TWO_NODE_HOST_FENCE_UNAVAILABLE`.

### 3.2 Host fence lifecycle

Record field `ClusterArbitration.host_fences`, keyed by the fenced node:

```json
{"<loser>": {"state": "revoked", "on": "<winner>", "host_nqn": "nqn.2014-08.org.nvmexpress:uuid:...",
             "epoch": 12, "since": 1761130000000, "revoked_at": 1761130000412,
             "reasserted_at": 1761130010412, "release_requested": false,
             "subsystems": {"<nqn>": {"mode": "any", "added": ["<winner host nqn>"]}},
             "error": ""}}
```

| From | To | Trigger and action |
|---|---|---|
| (none) | `revoking` | a degraded verdict marked host-fence eligible (section 2), and the winner reports the loser's JM unhealthy |
| `revoking` | `revoked` | every volume subsystem on the winner revoked (below); written to FDB, then visible to the operator |
| `revoked` | `revoked` | every `host_fence_reassert_s` the revocation is applied again (idempotent): covers subsystems the restart flow, the lvol monitor or a new volume created with `allow_any_host` |
| `revoked` | `releasable` | the storage no longer needs the fence: the cluster left `degraded` for this loser (heal, or a new verdict), the loser is reachable and none of its LVS is fenced or holding |
| `releasable` | `restoring` | the operator asks for the release (`POST .../arbitration/host-fence/release`) after it has verified that the node is clean (section 3.4) |
| `restoring` | (removed) | every change undone (below); recorded in the verdict history and logged as `TWO_NODE_HOST_FENCE_RELEASED` |

Revocation, per volume subsystem on the winner (never the hublvol subsystems, `:hublvol:` in the
NQN: the returning peer's SPDK needs them to resync, campaign c1 B2):

- **Allow-list subsystem** (`allow_any_host: false`): `nvmf_subsystem_remove_host(loser)` if the
  loser is listed. SPDK disconnects the host's controllers as part of the call.
- **Allow-any subsystem**: add every host that should keep access (the winner's
  `client_host_nqn` and every other host connected to that subsystem except the loser, from
  `nvmf_subsystem_get_controllers`), switch the subsystem to an allow-list
  (`nvmf_subsystem_allow_any_host false`), then `nvmf_subsystem_add_host(loser)` followed by
  `nvmf_subsystem_remove_host(loser)`: the remove is what disconnects the loser's live
  controllers (SPDK answers `remove_host` for an unlisted host with an error and disconnects
  nothing).

Release undoes exactly what the revocation recorded: allow-list subsystems get the loser back
through `host_auth` (with its DH-HMAC-CHAP keys, if any); converted subsystems get
`allow_any_host true`. The hosts the revocation added stay listed: harmless while any host is
allowed, and removing them would disconnect them. Subsystems deleted meanwhile are skipped.

All of it runs on the arbiter's work pool, never on the decision thread (campaign c7, F16), and
**after** the grant: until Kubernetes restarts the loser's VMs there is only one instance of
each, so the loser writing through the winner meanwhile is not a split brain. Only the taint,
i.e. the second instance, waits for `revoked`. The 3 s failover budget is untouched.

### 3.3 Partition: winner policy

On `two_node_host_fencing` clusters a partition (both nodes report each other, both reachable)
no longer fences both. The arbiter picks a winner deterministically:

1. a node whose data rebuild is pending never wins (arbitration contract, section 7.10); both
   pending: `fence_both`;
2. the node that leads more LVS (more volumes keep their optimized path);
3. tie: the lower node id.

Raise the epoch, `jc_fence` the loser for every LVS and wait for its acknowledgement (both are
reachable, so the fence is required, not best effort: a failed fence withholds the grant and is
re-sent), grant the winner, mark the loser for its data rebuild, then host-fence the loser
(section 3.2). The cluster is then `degraded`, and the heal re-admits the loser through the
restart flow. Clusters without `two_node_host_fencing` keep `fence_both`.

Planned refinement: prefer the node whose Kubernetes node is `Ready` once the operator reports
it (between rules 2 and 3).

### 3.4 Operator (mode `witness`)

- Reports `client_host_nqn` per storage node at deployment and whenever it changes.
- Polls `GET /clusters/{id}/arbitration` every second on each node's agent.
- `host_fences[<n>].state == revoked`: set `StorageFenced=True` (reason `WitnessVerdict`,
  epoch) on node `<n>`.
- `releasable`: once node `<n>` is `Ready` again and runs no pod that was scheduled before the
  fence (the kubelet has killed them), call `.../host-fence/release`.
- Entry gone: set `StorageFenced=False`; NHC deletes the remediation, which removes the taint.
  The order (release, then condition, then taint) makes sure no pod is scheduled on the node
  before its host can reach the winner again.

### 3.5 API additions

| Method | Path | Body | Use |
|---|---|---|---|
| PUT | `/api/v2/clusters/{id}/arbitration/client-host` | `{"node_id", "host_nqn"}` | operator: host NQN of the co-located Kubernetes node |
| POST | `/api/v2/clusters/{id}/arbitration/host-fence/release` | `{"node_id"}` | operator: node is clean, give its host access back |
| GET | `/api/v2/clusters/{id}/arbitration` | | adds `host_fences`, `fencing_authority`, `host_fencing` |
| PUT | `/api/v2/clusters/{id}` | `{"two_node_fencing_authority", "two_node_host_fencing"}` | settings |

## 4. Mode `pacemaker` (CR B): Pacemaker decides, the storage follows

### 4.1 How it works without a witness

Pacemaker replaces the third vote with power fencing: each node can power the other off over its
BMC (Redfish). On a loss both race to fence each other; the BMC executes one power-off, and the
loser is off before its own request lands (a configured delay on one node decides the race). The
survivor then provably runs alone. The storage never decides; it waits for that proof.

### 4.2 Control plane

- The arbiter does not act on the cluster at all: no leases, epochs, grants, fences, unfences,
  heals or leadership restoration. The Go collector does not poll its nodes (the node agent owns
  the single `jc_wait_events` waiter), legacy remote-JM events are not queued for the arbiter, and
  no planned-loss markers are written.
- `jc_set_dual_node` still carries the full arbitration config on every call (node add, restart
  flow), exactly as for `witness`: the node protocol must stay on.
- Everything else is unchanged: the restart flow re-admits the fenced node when it boots, queues
  the data rebuild and hands leadership back (failback). A power-fenced node always comes back
  through the restart flow, so no unfence-based re-admission is needed.

### 4.3 Node agent (`simplyblock-fence-agent`, mode `pacemaker`)

One per node, privileged only as far as section 4.5 requires. It talks only to its own node's
SPDK, through the local RPC socket:

| Duty | How |
|---|---|
| Lease | `jc_lease_renew` on its own node every 250 ms. An agent that dies while its node holds lets the lease run out: the node fences itself (safe). |
| Epoch | Local, monotonic, persisted on the host; after a restart it continues above `jc_ha_status.epoch`. |
| Events | `jc_wait_events` long-poll on its own node. |
| Decision | On `ha_hold_started`: wait for Pacemaker's fencing result for the peer, newer than the hold. Confirmed: `jc_grant_solo` for every held or self-fenced LVS. No confirmation: nothing; IO stays frozen (hosts queue it, no errors). |
| Stale gate | Never grant while this node's data rebuild is pending (arbitration contract, section 7.10). A marker is set on the host when the node boots after a fence and cleared when the control plane reports its rebuild finished; while the control plane cannot be read the marker stays. |
| Peer attestation | SPDK-only loss of the peer (crash, planned restart) does not make Pacemaker fence: the host is up. The agent asks the peer's agent; if the peer's agent answers that its SPDK process is gone and will rejoin only through the restart flow, that is proof of no second writer, and the agent grants. A peer agent that cannot be reached proves nothing. |
| Unfence | After the peer rejoined and the resync gate passed: `jc_unfence` for every LVS the node fenced itself. |
| Kubernetes | Pacemaker fenced the peer: `StorageFenced=True` (reason `PacemakerFence`) on the peer's `Node`. Cleared once the peer is back and `Ready`. No host fence is needed: the peer is powered off. |

### 4.4 Node side change (ultra, SPDK fork)

A Redfish fence takes about 10-60 s; the hold is 2.5 s. At the hold deadline the node fences
itself, and today a later `jc_grant_solo` is refused with `EBUSY`
(`alg_two_node_ha.cpp:660`). Required: a grant at a higher epoch lifts a **self**-fence (hold
deadline, lease expiry) - not a fence a verdict ordered - releases the fence's own client-port
block, re-enables the IO-driven takeover and restores ANA. While self-fenced, the paths are ANA
inaccessible, so native NVMe multipath queues IO without error and without command timeouts:
guests see latency, not errors.

A long `hold_ms` instead is not recommended: held IO longer than the host's
`nvme_core.io_timeout` (30 s on RHCOS) triggers controller resets with IO still held.

### 4.5 Reading Pacemaker's decision

Read-only; the agent never asks Pacemaker to fence. In order of preference:

1. The Kubernetes API, if the platform publishes Pacemaker's fencing state (to be confirmed
   with Red Hat).
2. A Pacemaker alert agent (`CRM_alert_kind=fencing`) writing to a node-local file the agent
   reads (one alert added to the Pacemaker configuration).
3. `crm_mon --as-xml` / `stonith_admin --history <peer>` (read-only, needs host access).
4. Redfish `GET` of the peer's power state with read-only BMC credentials. Reliable only with
   fence action `off`: with `reboot` (Pacemaker's default) the off window can be missed.

### 4.6 Deployment requirements

- Corosync runs over the storage replication links (or over all links redundantly). Otherwise
  a storage-only partition is invisible to Pacemaker, nobody is fenced and the storage stays
  frozen on both nodes. The agent refuses to enable the mode when the links differ.
- The BMC network is independent of the storage and cluster networks.
- `wait_for_all` (or no automatic cluster start after a fence), so a fenced node that boots into
  a still-broken link does not fence its peer in turn.
- A fence delay on exactly one node, so the race always has a winner.

### 4.7 Limits of mode `pacemaker` (compared with `witness`)

- **Total power loss of the peer**, BMC included: the fence fails, Pacemaker never confirms, the
  storage stays frozen until a person confirms the fence. This is the most common hardware
  failure; `witness` handles it automatically.
- Failover takes the heartbeat timeout plus fence delay plus Redfish power-off (10-60 s), against
  about 2-3 s with `witness`.
- A failure that takes out the BMC path together with the data path stops failover.
- A partition powers off a healthy node and restarts all its VMs.
- Misconfigured delays can power off both nodes.
- The fencing configuration is owned by the platform (Red Hat), not by simplyblock.

## 5. Defaults

| Setting | Default | Where |
|---|---|---|
| `host_fence_reassert_s` | 10 | arbiter, re-apply a revocation (section 3.2) |
| `host_fence_rpc_timeout_s` | 5 | arbiter, each subsystem RPC of a revocation or release |
| NHC `duration` for `StorageFenced` | 2 s | NodeHealthCheck |
| agent arbitration poll | 1 s | operator, mode `witness` |
| agent lease renewal | 250 ms | node agent, mode `pacemaker` |

## 6. Work packages

| Repo | Package | Mode |
|---|---|---|
| sbcli | settings, `client_host_nqn`, `host_fences`, API (section 3.5) | both |
| sbcli | host fence lifecycle in the arbiter (section 3.2) | witness |
| sbcli | winner policy for partitions (section 3.3) | witness |
| sbcli | `pacemaker` gates: arbiter, Go collector, event queue, planned loss (section 4.2) | pacemaker |
| operator | fence agent DaemonSet, `StorageFenced`, `StorageFenceRemediation(Template)` CRDs + controller, NHC sample, release protocol | both |
| operator | node agent duties of section 4.3, Pacemaker readers of section 4.5 | pacemaker |
| ultra + SPDK fork | grant lifts a self-fence (section 4.4) | pacemaker |
| CSI | forward keep-alive and fast_io_fail (arbitration contract 9.1, still open) | both |
| test | simulator scenarios for every row of section 2; bed: OpenShift two-node with arbiter + KubeVirt (witness), two-node with fencing (pacemaker) | both |

## 7. Open questions

- Red Hat: is there a supported, read-only way to consume Pacemaker's fencing result on the
  two-node-with-fencing topology (section 4.5, options 1 and 2)?
- Operator: what proves "no pod from before the fence runs on the node" robustly enough for the
  release (section 3.4) - pod start times against the fence time, or a kubelet restart?
- Whether host fencing should also be used in mode `pacemaker` as a second safeguard (the
  control plane is reachable and could revoke on the agent's report).
