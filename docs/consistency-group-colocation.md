# Consistency groups: migration, late join and subsystem co-location

Status: implemented in part (2026-10-03). Sections say what is built, what is
behind a flag, and what is a specified follow-up.
Code: `simplyblock_core/controllers/cg_colocation.py`, `migration_controller.py`
(group migration), the slot claim (`db_controller.claim_lvol_ns_slot`,
`lvol_controller.get_next_available_subsystem_on_node`); simplyblock-operator
`csi-driver/internal/csi/controller/cg_membership_watcher.go`,
`cg_prejoin_migration.go`, `atlas-lib/devmapper`,
`atlas-lib/volstack/layers/dmlinear.go`, `operator/internal/autoplacement`.
Background: operator `docs/designs/design-consistency-groups.md` (§4 lifecycle,
§8.4 members excluded from migration, §9.5 the VolumeMigration webhook).

## 1. The invariant and what broke it

A consistency group's members live on ONE node and logical volume store (the
pin): the frozen group snapshot (`bdev_lvol_snapshot_group`) operates on one
store. Subsystem sharing is an efficiency (design §4.2), placement is not.

Before this change:

- **Migration ignored groups.** A live migration moves one NVMe subsystem
  (every namespace in it) to another node, keeping its NQN; clients follow
  through ANA after the operator's VolumeMigration attached the target paths.
  Nothing read `lvol.group_id`, so a member could be moved alone and the
  group was split. The operator webhook refused members entirely (§9.5).
- **A group spanning subsystems could not move at all**, and a subsystem can
  hold volumes of several groups and of none.
- **A late join never moved a volume** (§4.5): a volume off the pin was
  refused for good.
- **Create-time subsystem sharing was luck**: the slot claim fills the pool's
  fullest subsystem first, which is usually, not always, the group's.

## 2. Migration scope

`cg_colocation.migration_scope(seeds, lvols, groups)` is the closure over two
relations: *shares a subsystem with* (a migration moves the subsystem) and
*is an open member of the same group as* (the group must stay together). A
group spanning subsystems A and B pulls in both; a non-member in A comes
along; a member of another group in B pulls in that group too. Departed
members (closed epochs) and deleted volumes are not in it. A subsystem only
counts as shared when `max_namespace_per_subsys > 1`, as for batch migration.

**Guard.** `create_migration` (single) and `create_batch_migration` (one
subsystem) call `require_whole_groups`: a request smaller than its scope is
refused, naming what it would leave behind and the group migration route. The
operator's rebalancer skips PVCs with the group label (they are treated like
pinned volumes in `BuildPinnedSet`), and its VolumeMigration webhook keeps
refusing members, now pointing at the group migration.

## 3. Group migration (built)

```
POST   /clusters/{c}/consistency-groups/{g}/migration        {target_node_id}
POST   /clusters/{c}/consistency-groups/{g}/migration/start
GET    /clusters/{c}/consistency-groups/{g}/migration        -> none|running|done|failed
DELETE /clusters/{c}/consistency-groups/{g}/migration        (cancel)
```

`create_group_migration` pre-creates one migration per subsystem of the scope
(a batch migration for a shared subsystem, a single one otherwise), all or
none: a failure cancels what was already pre-created. It returns every item's
connect strings; the client side attaches all of them, then `start` launches
every item. The record (`ConsistencyGroup.migration`: target and items) is
written on every group of the scope; a second group migration is refused while
one is active.

While it runs the group is split (subsystems finish at different times): group
snapshots are refused by `_precheck_members` (members off the pinned store),
including the cadence snapshot of a replicating group; whether the cadence
retries within the interval is not verified here. When
the last member's record switches (`_apply_migration_to_db` calls
`repin_after_member_moved`), every open member lives on one store and the pin
follows; the group migration record is cleared then.

**Follow-up (operator).** The operator's VolumeMigration moves one subsystem
and validates the connections of one. Driving a group migration from
Kubernetes needs a VolumeMigration scope (`spec.consistencyGroup`) whose
controller calls the group route, validates the connections of every item on
the consumer hosts, and starts them together. Until then the group route is
the backend surface, used from `sbctl`/REST.

## 4. Create time: forcing the group's subsystem (built) and flip-out (flagged)

**Forcing.** For a namespaced member, the create path computes the group's
subsystem (`group_subsystem_nqn`: the NQN holding most open members on the
pin) and passes it to the transactional slot claim as `prefer_nqn`. The
fullest-first picker ranks it first when it is joinable (pool-aligned) and
has a free slot; otherwise the ordinary pick applies. Because the preference
is applied inside the FDB claim transaction, a concurrent create recounts
with this record visible, as before.

**Flip-out.** When the group's subsystem is full, a non-member namespace is
moved out to make room: `colocate_new_member` swaps the new member (brand new,
nothing attached) with a victim:

- Victim: belongs to no consistency group (moving a member of this group out
  defeats the purpose; one of another group breaks that group), not being
  created, deleted or migrated. Preference: no host connected (its move needs
  no client swap), then smallest, then highest nsid (most recently added).
- Destination: the new member's own subsystem, which must have a free slot;
  the victim stays in its pool (subsystems are pool-aligned) and on its node.
- Slot accounting: occupancy is counted from lvol records, so switching the
  victim's and the member's records (`nqn`, `ns_id`, `namespace`) transfers
  the slots. **Open:** the switch is not yet inside the claim transaction; a
  concurrent claim on the node can observe the intermediate state. The fix is
  to write both records and bump the node's allocator key in one transaction.
- Gated by `cg_colocation.NAMESPACE_MOVES_ENABLED` (off). With it off, or with
  an attached victim and no client swap, the member stays in its own
  subsystem (best effort, as before) and the outcome is logged.

Clone paths (labeled restores, fail-over clones joining a group) do not apply
the forcing yet; they place as before and can be co-located later (§5).

## 5. Late join (built: plan + pre-join migration; flagged: co-location)

`POST .../members/plan {lvol_id}` returns the steps without taking any:

| Situation | Steps |
| --- | --- |
| group has no open member | `join` (the volume pins it) |
| volume on the pin | `join`, then `colocate` when not in the group's subsystem |
| volume off the pin | `migrate` (`migrate_lvol_ids`, `target_node_id`), `join`, `colocate` |
| volume in another group / another pool / siblings in another group | 409 |

The CSI membership watcher drives it. A join refused for placement becomes a
VolumeMigration `cg-join-<lvol>` (driver namespace, label
`storage.simplyblock.io/purpose=consistency-group-join`, `spec.pvName`,
`spec.targetNodeUUID` = the pin): the operator attaches the target paths on
the consumer host and runs the live migration; the watcher reports
`ConsistencyGroupMigrating` / `ConsistencyGroupMigrated` /
`ConsistencyGroupMigrationFailed` and joins on the next pass. The VolumeMigration
moves the volume's whole subsystem, so its siblings move to the group's node
too: harmless for volumes in no group, refused for another group's members.

**Deviation from the requested flow.** The requested late join first migrates
the volume into a new, own subsystem on the target LVS, then moves it as a
namespace into the group's subsystem. Extracting one namespace from a shared
subsystem into a new subsystem is itself a subsystem change for the client,
which needs the device-mapper swap (§6). Until that is deployed the pre-join
migration keeps the volume's subsystem (client-transparent), and the
co-location step is the only subsystem change; with §6 deployed, extraction
becomes the preferred first step and the siblings stay where they are.

`POST .../members/{id}/colocate {client_swap_ready}` moves a member into the
group's subsystem (flip-out as in §4 when it is full; refused when the
member's own subsystem has no slot for the victim -- a three-way move through
a fresh subsystem is a follow-up). The watcher calls it after a join only with
`SPDKCSI_CG_COLOCATE=true`; a refusal is a Normal event.

## 6. The client data path for subsystem moves

A namespace that moves to another subsystem appears on the client as a
different block device: native NVMe multipath groups paths per subsystem, so
the new subsystem gets a new controller set and a new namespace head
(`nvmeXnY`), even though the namespace identity (UUID/NGUID, which
`move_namespace` keeps) is the same. ANA state is per subsystem and listener;
the target subsystem's listeners and ANA groups are already set up for its
existing members.

**Indirection (built, flag-gated).** The node plugin can stage raw block and
plain volumes behind a dm-linear device (`atlas-lib/volstack/layers/dmlinear.go`,
plan rows `IndirectRawBlock` / `IndirectPlain`): `/dev/mapper/sb-<lvol>` maps
the whole namespace device, writes nothing to it, and is what the pod or the
filesystem uses. Its Heal re-points the mapping when the device below changed:
`dmsetup suspend` (in-flight I/O drains, new I/O queues), `reload` with the
new device, `resume`; a refused reload resumes on the old table so the device
is never left suspended. `SPDKCSI_DM_INDIRECTION=true` applies it to fresh
stages; a staged volume follows its stack record, so the layer is never
inserted under or pulled from under a live consumer. LVM-backed volumes keep
their own device-mapper stack and have no indirect row.

**Swap protocol (specified, not built).** Make-before-break needs the bdev
exported by two subsystems at once; whether the target allows that is not
established here. The protocol therefore uses a node handshake and a short
suspend window:

1. CP marks the move pending (`lvol.pending_nqn`, the target subsystem) and
   exposes it on the volume's connect answer.
2. Node (a reconcile loop over staged volumes with a dmLinear layer, or a
   per-PV annotation the CSI controller sets): connects the target subsystem's
   paths, suspends the mapping, acknowledges (`POST .../volumes/{id}/move-ready`).
3. CP removes the namespace from the old subsystem and adds it to the target
   with one nsid on every HA node (`move_namespace`), switches the record.
4. Node: rescans, finds the namespace by UUID under the target subsystem,
   reloads and resumes the mapping, disconnects the old subsystem when no
   other staged volume uses it.
5. Bounds: the mapping is suspended at most `T` (seconds, configurable); CP
   steps time out and roll back (namespace re-added to the old subsystem);
   the node resumes on the old table when the CP did not confirm within `T`.

Until steps 1, 2 and 4 exist, `move_namespace` refuses a volume whose
subsystem has a connected host unless the caller asserts `client_swap_ready`,
and `NAMESPACE_MOVES_ENABLED` is off.

## 7. Replication and DR

- **Landing volumes** (`REP_*`, internal) are never group members and are not
  co-located; they are created by the system and exempt from the subsystem
  cap. A group migration's scope can contain one only as a subsystem sibling.
- **Target-side placement.** Members of a replicating group already replicate
  to one target node (`_group_replication_node`), so their copies share one
  store there. A group migration on the source does not change
  `replication_node_id`; replication continues to the same target.
- **Fail-over clones** form the target-side group through
  `reconstitute_group_after_handoff`, pinned by the clones' placement; the
  create-time forcing does not apply to clones yet (§4), so a fail-over group
  may span subsystems on the target, which is valid.
- **Group-wide generations.** A group snapshot during a group migration is
  refused (split group); generations resume once the pin followed. A
  generation taken before a late join does not contain the joined volume
  (`joined_seq`), unchanged.
- **Migrating a replicating group** moves its primaries; the replication
  relationship is per volume and survives (the volume keeps its id).

## 8. Flags

| Flag | Where | Default | Effect |
| --- | --- | --- | --- |
| `cg_colocation.NAMESPACE_MOVES_ENABLED` | control plane | off | namespace moves between subsystems (flip-out, colocate) |
| `SPDKCSI_CG_PREJOIN_MIGRATION` | CSI controller | on | a late join off the pin requests a VolumeMigration |
| `SPDKCSI_CG_COLOCATE` | CSI controller | off | the watcher calls colocate after a join |
| `SPDKCSI_CG_CLIENT_SWAP_READY` | CSI controller | off | asserts every node swaps paths (§6) |
| `SPDKCSI_DM_INDIRECTION` | CSI node | off | stage new raw block / plain volumes behind dmLinear |

## 9. Tested

- sbcli `simplyblock_core/test/test_cg_colocation.py` (24): scope closure
  (group across subsystems, transitive across groups, departed members,
  non-shared subsystems), the guard, group migration all-or-nothing and
  conflict, re-pin, the claim's preference and fallback, victim choice, the
  late-join plan and its refusals, namespace moves disabled / refused when
  attached, the move's shared nsid, record switch and rollback.
  `tests/unit/web/api/v2/test_consistency_group_endpoints.py`: the plan,
  colocate and group-migration routes.
- operator: the watcher's pre-join migration (requested, completed, failed,
  never-succeeding, disabled), co-location on/off and refusal; the
  VolumeMigration requester against a fake dynamic client; the rebalancer's
  pinned set; the RBAC rule; `devmapper` (table parsing, swap order, resume
  on a failed reload); the dmLinear layer (create, stale detection, heal,
  failed swap, release) and its place in the layer contract test; the node
  plugin's indirect rows, record-governed shape choice and teardown.

Not tested: anything on a live cluster. The group migration's interplay with
the real migration runner, the VolumeMigration path for a pre-join move, and
`dmsetup` on a real node have only been exercised through fakes.

## 10. Open questions

1. May an SPDK NVMe-oF target export one bdev in two subsystems at once? If
   so, §6 becomes make-before-break and the suspend window shrinks to the
   reload.
2. Should the operator expose group migration as a VolumeMigration scope (§3)
   or as its own kind?
3. Flip-out of an attached victim is a client-visible move of a volume that
   did not ask for anything. Is that acceptable for the efficiency gain, or
   should forcing only flip unattached victims and otherwise accept a second
   subsystem?
