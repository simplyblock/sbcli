# Stretched control plane

A cluster whose storage spans two sites can run a control plane on each of them: both share one
FoundationDB that FDB itself stretches across the sites, one of them leads, the other stands by.
This page is the operator's and the integrator's reference; the design discussion (why FDB's own
replication, why leadership is an operator decision, what a cut-off site can and cannot do) is in
the design document *Multi-Site Control Plane for Stretched Clusters*.

## 1. Model

| Term | Meaning |
|---|---|
| Site | A name (`[A-Za-z0-9][A-Za-z0-9._-]{0,62}`) carried by every storage node (`StorageNode.site`) and, on a stretched control plane, by every management node (`MgmtNode.site`). |
| Control-plane site | A site with a management node. Exactly two per cluster. |
| Leader site | `Cluster.leader_site`: the site whose control plane should lead. Moved only by an operator (`set-leader-site`, `takeover`); on a sync-replication cluster it is also the site whose Kubernetes operator leads, as before. |
| Lease | `ControlPlaneLease`, one record per cluster in FDB: the site and host that hold the lead, an `epoch`, a `state` (`held`, `draining`, `released`) and an expiry. Written only by the lease agent and the two operator commands. |
| Mode | What a control plane is right now: `single` (the cluster is not stretched), `leader`, `standby`, `draining`, `disconnected` (FoundationDB does not answer). |

The data plane is unchanged. A stretched control plane is allowed with exactly three data-plane
schemes, and `cluster create` refuses every other combination:

| Scheme | Flags | Storage sites | What the site does for placement |
|---|---|---|---|
| any n+k with two-side synchronous replication | `--sync-replication` | 2 | names the replication side (see `sync-replication.md`) |
| 1+1 with failure domains across two sites | `--enable-failure-domain`, `--distr-ndcs 1 --distr-npcs 1` | 2 | a node's failure domain **is** its site (assigned per site at the first add-node, `Cluster.site_failure_domains`) |
| 1+2 with failure domains across three sites | `--enable-failure-domain`, `--distr-ndcs 1 --distr-npcs 2` | 3 (control planes on two of them) | as above; the third site runs storage nodes only |

## 2. Who does what

- **Leader.** Runs the full service set (every monitor, collector and task runner) exactly as a
  management node does today, with direct RPC to every storage node of every site. Answers reads
  and writes.
- **Standby.** Runs the API and the lease agent, nothing else. Reads are served from FoundationDB
  (which never serves stale data). Mutating calls (`POST`/`PUT`/`PATCH`/`DELETE`) are answered
  with **307** to the leader's API endpoint (`Cluster.site_api_endpoints[<leader site>]`), or with
  **503** when that endpoint is not configured. The `/control-plane` routes, `/_meta` and the
  metrics are always answered locally.
- **Disconnected.** A control plane whose FoundationDB does not answer (the site lost the
  database's majority): the API answers 503 except for health and metrics; the services fail
  their database calls as they always did, exit, and are restarted by the orchestrator until
  FoundationDB answers again, so nothing is written anywhere. Its storage nodes keep serving what
  the data plane allows.
- **Lease agent.** A thread of `mgmt_node_monitor` on every management node
  (`control_plane_controller.lease_agent_forever`). Every `CONTROL_PLANE_LEASE_RENEW_SEC` (3 s) it
  reads the lease and acts on one rule: the desired site (`leader_site`) acquires a lease that is
  missing, released or expired; the holder renews while desired, drains for
  `CONTROL_PLANE_DRAIN_SEC` (6 s) and releases when another site is desired; **nobody ever takes a
  live lease another site holds**. A renewal that fails, for any reason, stops the holder's
  services at once (`services_enabled` reads the lease, cached 1 s).
- **Task leases.** A running task's lease heartbeat (`tasks_controller.task_lease_heartbeat`) stops
  when its control plane no longer leads; the task's lease goes stale and the new leader's runner
  resumes it through the existing resume paths.

Each control plane learns its own site from `SB_CONTROL_PLANE_SITE` (the operator sets it on every
control-plane pod) or, on Docker, from its `MgmtNode` record by hostname. A control plane without a
site never leads.

## 3. Deployment

| Step | Command |
|---|---|
| Create the cluster on site A | `sbctl cluster create ... --ha-type ha --stretched-control-plane --site A --site-api-endpoint https://cp-a:443 --leader-site A` plus one of the scheme flags (v2: `stretched_control_plane`, `site`, `site_api_endpoint`, `leader_site` in the create body) |
| Add the second control plane on site B | `sbctl control-plane add <cluster-ip> <cluster-id> <secret> --site B --api-endpoint https://cp-b:443` |
| Add storage nodes | `sbctl storage-node add-node ... --site A` / `--site B` (`--site C` for the third storage site of 1+2). With the failure-domain schemes `--failure-domain` is derived from the site and may be omitted. |
| Watch | `sbctl control-plane status <cluster-id>` |

FoundationDB itself is stretched by the deployment, not by sbcli: one FDB cluster in region
configuration with `redundancy_mode: triple` inside each region (three copies per control-plane
site), each site's datacenter the primary of its own region and a synchronous satellite of the
other, coordinators 3 at the leader site and 2 at the other, or 3 + 3 + 3 with an optional third
coordinator site. Every FDB process, control plane and storage node must reach every FDB process
by IP (no NAT between the sites). `DBController` sets a transaction timeout
(`KVD_DB_TIMEOUT_MS`), which is what turns a lost majority into a paused service instead of a hung
one. The Kubernetes side is the simplyblock operator's part of this feature
(simplyblock-operator `docs/designs/design-stretched-control-plane.md`,
`ControlPlane.spec.source.local.stretch`): each site's `FoundationDBCluster` is one region of the
shared database (`dataCenter` = site, the Join site seeded with the Bootstrap site's connection
string), every control-plane pod gets `SB_CONTROL_PLANE_SITE`, and the second site registers its
control plane with `POST /api/v2/management-nodes?cluster_id=<id>` (`{site, api_endpoint}`) instead
of `control-plane add`. That route is served by every control plane whatever its mode, so the
joining site registers against its own, standby, API. Kubernetes control planes all register under
the placeholder address `0.0.0.0`; two registrations are told apart by their site, and the
takeover probe asks such a site's recorded API endpoint rather than pinging the address.

## 4. Moving the lead

### Planned: `sbctl control-plane set-leader-site <cluster-id> <site>`

Refused while any readiness gate of the target site is red; `--force` overrides (audited in the
cluster events). The gates, also shown per site by `control-plane status` as `problems` /
`failback_ready`:

| Gate | Green when |
|---|---|
| Management node | at least one management node of the site is `online` |
| Storage nodes | every storage node of the site is `online` |
| Lost site | the site is not recorded lost (`Cluster.lost_site`) and no site activation runs |
| Cluster | status `active` |
| Sync replication (scheme a) | the cluster-wide sync status is settled: not degraded, not resyncing, not diverged, peer ready |
| FoundationDB | `status json` reports `datacenter_lag` at most `CONTROL_PLANE_MAX_FDB_LAG_SEC` (1 s) and a healthy data-distribution state |
| Leader quiescent | no task is `running` |

Once accepted, `leader_site` changes; within one agent cycle the holder drains (its services
stop, its API redirects), releases after the drain period, and the target site's agent acquires.
Expect the hand-over to take about `CONTROL_PLANE_DRAIN_SEC + CONTROL_PLANE_LEASE_RENEW_SEC`
seconds.

### Unplanned: `sbctl control-plane takeover <cluster-id> --site <survivor> --confirm-site-down <leader>`

For the leading site being down. The command:

1. refuses unless the cluster is stretched, the survivor has a control plane, and
   `--confirm-site-down` names the site that leads;
2. probes the confirmed site (ping of every management node and storage node host, the node API
   of every storage node; a Kubernetes control plane through its recorded API endpoint instead of
   a ping) and **refuses while anything answers** (`detail.alive` in the API): a
   takeover while the old leader is alive would drive the same storage nodes from two control
   planes;
3. sets `leader_site` to the survivor and releases the lease on the holder's behalf with a new
   epoch, so the survivor's agent acquires within one cycle and a holder that comes back fails its
   renewal and stops;
4. records a cluster event naming the operator.

FoundationDB must answer on the survivor for the command to run at all. Without a third
coordinator site, the loss of the leader site takes the coordinator majority with it; the FDB
recovery (the FDB operator's multi-region recovery to the surviving region, then
`force_recovery_with_data_loss`; with synchronous satellite logs the discarded window is empty)
comes **first**, and the survivor's control plane leaves `disconnected` by itself once FoundationDB
answers. With a third coordinator site FoundationDB survives and the command is only the lease
move. Afterwards the data plane is failed over as today (on a sync-replication cluster: the site
fence and forced promotes).

### Return of the lost site

Automatic at every layer. FoundationDB catches the returning region up on its own; the returning
control plane obtains a read version, leaves `disconnected`, and stands by; the leader's monitors
reach the returning nodes again on their next tick, and the existing site-return / fail-back paths
of the data plane apply. Nothing is merged: the cut-off site wrote nothing. When
`control-plane status` shows the returned site `failback_ready`, `set-leader-site` moves the lead
back.

## 5. API

| Route | Purpose |
|---|---|
| `GET /api/v2/clusters/{id}/control-plane/` | mode of the answering control plane, lease, per-site readiness, FDB region lag |
| `POST /api/v2/clusters/{id}/control-plane/leader-site` `{site, force}` | planned move; 409 with `detail.problems` while a gate is red |
| `POST /api/v2/clusters/{id}/control-plane/takeover` `{site, confirm_site_down, operator}` | unplanned move; 409 with `detail.alive` while the confirmed site answers |
| `PUT /api/v2/clusters/{id}` `{leader_site}` | unchanged: the operators' role swap; on a stretched control plane it moves the lease too, **without** the readiness gates |

`ClusterDTO.stretched_control_plane` and `ManagementNodeDTO.site` expose the new fields.

## 6. Known limitations

- Exactly two control-plane sites; a third site holds storage nodes (1+2) and, outside sbcli, FDB
  coordinators and satellite logs.
- No automatic takeover. Between the loss of the leader site and the operator's command the
  cluster has no leader: no promotion, migration or capacity decision is made. Storage nodes keep
  serving what the data plane allows.
- A standby redirects mutating calls with 307 only when the leader site's API endpoint is recorded
  (`site_api_endpoints`); otherwise it answers 503 naming the gap.
- A control plane without a site (no `SB_CONTROL_PLANE_SITE`, no `MgmtNode.site`) on a stretched
  cluster stands by forever and logs why.
- The readiness gate reads FoundationDB's region lag from `status json`; on a store without that
  document (a single-region lab) the gate is skipped, not failed.
