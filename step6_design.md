# Step 6 design — final

Supersedes the earlier draft of this file. Reflects the design decisions
landed in conversation:

* Entry point is **`sbctl sn add --expansion <node>`**, not a change to
  `cluster complete-expand`.
* **No JC quorum polling** — the synchronous create chain
  (`_create_bdev_stack` → `connect_to_hublvol` → `bdev_examine`) is its own
  guarantee. Restart works without it; expansion can too.
* **No refactor** of `recreate_lvstore_on_sec`. We reuse it as-is. The
  newcomer is fresh so its iteration scope is exactly the LVSes we just
  assigned to it. Where we *also* call it on existing nodes (n1, n2 in the
  4→5 example), we accept that it iterates all back-references currently
  present — and we ensure the back-references reflect the desired state
  before the call.
* **Sibling multipath reconfigure** (sec_2 when sec_1 moves) is handled
  with a narrow `bdev_nvme_attach_controller` (add new failover) +
  `bdev_nvme_remove_trid` (prune old) pair. No full recreate on the
  sibling.

## Sequence inside `sn add --expansion`

```
sbctl sn add --expansion <node-addr> ...
```

1. **Existing add_node work**: register node, start SPDK, set up devices,
   alcemls, JM device. No change from today's `add_node` flow. At this
   point the node is online but has no LVS roles.
2. **Plan**: `compute_role_diff(existing_node_ids, new_node_id, ftt)`.
3. **If `cluster.expand_state` is in_progress at entry**: refuse to add a
   new node, resume the existing plan instead. (Prevents starting plan B
   while plan A is half-done.)
4. **Drive the orchestrator** with the real `SpdkMoveExecutor`:
   - For each `RoleMove`, set the relevant DB back-references for the
     desired final state, then invoke the appropriate existing primitive.
   - Cursor persists per move on `cluster.expand_state` (already
     implemented in step 3).

## Per-move executor implementations

The orchestrator iterates `RoleMove`s in the order
`compute_role_diff` produced (Phase A re-homes first, then Phase B
creates). For each move the executor does:

### create-primary (Phase B, `role=primary`, on `to_node = new_node`)

```
1. cluster = db.get_cluster_by_id(...)  # fresh
2. records = db.get_cluster_capacity(cluster); max_size = records[0]['size_total']
3. storage_node_ops.create_lvstore(new_node, cluster.distr_ndcs,
                                   cluster.distr_npcs, cluster.distr_bs,
                                   cluster.distr_chunk_bs,
                                   cluster.page_size_in_blocks, max_size)
4. new_node.lvstore_status = "ready"; persist.
```
Lifted verbatim from the existing bulk loop (`cluster_ops.py:825-834`).

### create-secondary / create-tertiary (Phase B, `role=secondary|tertiary`, `is_create=True`)

```
1. Set on the holder (to_node):
     to_node.lvstore_stack_secondary_{1|2} = lvs_primary_node_id
   persist.
2. Set on the primary (lvs_primary_node_id):
     primary.secondary_node_id{|_2} = to_node_id
   persist.
3. recreate_lvstore_on_sec(to_node)
   → iterates exactly the primaries pointing at to_node. For the newcomer
     the only such primary is the one we just set the back-reference for,
     so this is surgical by construction.
```

For an *existing* node gaining a new role (e.g. n1 gaining LVS5.sec_1),
the same call iterates all primaries pointing at it after our DB updates.
That includes any unchanged roles n1 already held. Re-running them is
acceptable because:
- The function is designed to be idempotent for already-set-up roles
  (it's the same code that runs on every restart).
- The cost is bounded by the number of primaries hosting on that node
  (at most FTT for a single role assignment).

### re-home secondary / re-home tertiary (Phase A, `is_create=False`)

```
Pre-check:
  donor.status == ONLINE          # else abort with clear message
  recipient.status == ONLINE      # else abort with clear message

1. Set on recipient: lvstore_stack_secondary_{1|2} = lvs_primary_node_id;
   persist.
2. Set on primary: secondary_node_id{|_2} = recipient_id; persist.
3. recreate_lvstore_on_sec(recipient)
   → builds the new sec/tert stack on recipient. Iterates over all
     primaries currently pointing at recipient; that's the new role plus
     any existing roles recipient already held. Idempotent on existing.
4. teardown_non_leader_lvstore(donor, primary_node)
   → tears down the sec/tert stack on donor for this LVS.
   (Step 2's helper.)
5. Clear on donor: lvstore_stack_secondary_{1|2} = ""; persist.
   (teardown_non_leader_lvstore does this internally.)
6. If this was a sec_1 move and the LVS has a sec_2:
     For sec_2's nvme controller for this primary's hublvol:
       bdev_nvme_attach_controller(hublvol_bdev, nqn, recipient_ip,
                                   port, multipath="multipath")
       bdev_nvme_remove_trid(hublvol_bdev, donor_ip, port)
   → narrow multipath reconfigure, no recreate on sibling.
```

Step 6 add: a small helper `_reattach_sibling_failover(sibling_node,
primary_node, old_failover_node, new_failover_node)` wrapping the two
RPCs.

## Open implementation details (resolve while coding)

- **Exact RPC name for path removal**: SPDK has `bdev_nvme_remove_trid` —
  verify the wrapper in `rpc_client.py` exposes it (or add the wrapper).
  If it auto-prunes dead paths, we may not need the explicit remove.
- **DB pointer ordering for re-home**: setting recipient's back-reference
  before clearing donor's is fine because `recreate_lvstore_on_sec`
  iterates by `get_primary_storage_nodes_by_secondary_node_id` (DB
  query). Need to confirm that query reads from the field we updated and
  picks up the new state.
- **Atomicity of `set_lvol_subsys_port`-side back-references**: the
  per-lvstore-port copy in `recreate_lvstore_on_sec:3582-3587` reads
  `primary.lvstore_ports[primary.lvstore]`. Already populated when the
  primary was created, so this works the same for the newcomer once its
  primary is created (Phase B happens after Phase A in
  `compute_role_diff`).

## What this does NOT change

- `recreate_lvstore_on_sec` body — unchanged.
- `recreate_lvstore` body — unchanged.
- `cluster_expand` — unchanged (rolled back the step-5 modifications).
- Restart paths — unchanged.

## Test plan for step 6

- Unit-test `SpdkMoveExecutor.execute` per role with mocked DB / RPC,
  asserting which back-references are set and which existing functions
  are called in what order.
- Add a unit test for `_reattach_sibling_failover` against mocked RPC.
- Update `tests/test_lvs_role_assignment.py` regression suite continues
  to pass (it covers the existing `recreate_lvstore_on_sec` behavior
  we're not changing).
- Delete `TestSpdkExecutorIsStillStubbed` from the (already-deleted)
  integration test file — n/a now.
- A new integration test that drives `add_node(..., expansion=True)`
  end-to-end with mocked DB / RPC and asserts the final desired layout
  (back-references and which RPC calls were made).

## Implementation order for step 6

1. Add `bdev_nvme_remove_trid` wrapper to `rpc_client.py` if missing.
2. Add `_reattach_sibling_failover` helper in `storage_node_ops.py`.
3. Replace `SpdkMoveExecutor`'s 5 stub methods with real implementations,
   each calling the existing primitives per the sequences above.
4. Add `expansion` parameter to `add_node(...)`. When True, after the
   existing add work: compute the diff, call `execute_expand_plan`.
5. Wire the CLI flag `--expansion` on `sbctl sn add`.
6. Tests.
