"""A replicated copy is complete only if the whole chain below it went too.

bdev_lvol_transfer sends a blob's OWN cluster map and nothing else
(prepare_s3_clusters copies blob->active.clusters; inherited clusters are 0
and are skipped). Two shapes put other blobs' data under a snapshot:

  * a fail-over volume is a CLONE -- its pre-fail-over history lives in base
    snapshots. Lab 2026-08-20 case 4: XFS allocation group 3, written once at
    mkfs and never rewritten (fio only overwrites), was all zeros on the fresh
    cluster; the fs failed to mount with "Structure needs cleaning" while
    everything fio rewrote after the fail-over had arrived fine.
  * a USER snapshot between two internal cadence snapshots absorbs the writes
    made before it; the next internal snapshot's own map no longer has them.

So ancestors replicate FIRST, bottom-up. And because clones make the chain a
TREE, a shared ancestor must be transferred exactly ONCE and recognized as
already existent on the target by every other descendant.
"""


from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr


LVS = "LVS_1"


class _LvolRef:
    def __init__(self, uuid="LV1", node_id="NODE_A", replication_node_id=""):
        self.uuid = uuid
        self.node_id = node_id
        self.replication_node_id = replication_node_id

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, bdev, target="", source="", status=SnapShot.STATUS_ONLINE,
                 lvol=None):
        self.uuid = "id-" + bdev.split("/")[-1]
        self.snap_bdev = bdev
        self.cluster_id = "CL1"
        self.target_replicated_snap_uuid = target
        self.source_replicated_snap_uuid = source
        self.status = status
        self.snap_ref_id = ""
        self.next_snap_uuid = ""
        self.lvol = lvol or _LvolRef()

    def get_id(self):
        return self.uuid


class _RPC:
    """get_bdevs returning each blob's base_snapshot -- the chain topology."""

    def __init__(self, bases):
        self._bases = bases      # {bdev_name: base bdev short name or None}

    def get_bdevs(self, name):
        if name not in self._bases:
            return None
        base = self._bases[name]
        return [{"driver_specific": {"lvol": {"base_snapshot": base}}}]


class _Node:
    def __init__(self, rpc):
        self._rpc = rpc
        self.uuid = "NODE_A"

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return self._rpc


class _DB:
    def __init__(self, snaps):
        self._snaps = list(snaps)

    def get_snapshots_by_node_id(self, node_id):
        return self._snaps


def _walk(monkeypatch, bases, snaps, top, to_source=False):
    monkeypatch.setattr(sr, "db", _DB(snaps))
    return sr._unreplicated_local_ancestor(_Node(_RPC(bases)), top, to_source)


def test_self_contained_root_is_ok(monkeypatch):
    top = _Snap(f"{LVS}/SNAP_2")
    verdict, rec, _ = _walk(monkeypatch, {f"{LVS}/SNAP_2": None}, [top], top)
    assert (verdict, rec) == ("ok", None)


def test_replicated_base_is_ok(monkeypatch):
    base = _Snap(f"{LVS}/SNAP_1", target="REMOTE_1")
    top = _Snap(f"{LVS}/SNAP_2")
    verdict, rec, _ = _walk(
        monkeypatch, {f"{LVS}/SNAP_2": "SNAP_1", f"{LVS}/SNAP_1": None},
        [base, top], top)
    assert (verdict, rec) == ("ok", None)


def test_unreplicated_base_is_pending(monkeypatch):
    """The case-4 shape: a fail-over clone's history must go first."""
    base = _Snap(f"{LVS}/SNAP_1")
    top = _Snap(f"{LVS}/SNAP_2")
    verdict, rec, _ = _walk(
        monkeypatch, {f"{LVS}/SNAP_2": "SNAP_1", f"{LVS}/SNAP_1": None},
        [base, top], top)
    assert verdict == "pending"
    assert rec.get_id() == base.get_id()


def test_deepest_unreplicated_ancestor_goes_first(monkeypatch):
    """Bottom-up: with SNAP_3 -> SNAP_2 -> SNAP_1 all unreplicated, SNAP_1
    (deepest, self-contained) is the one to transfer first."""
    s1 = _Snap(f"{LVS}/SNAP_1")
    s2 = _Snap(f"{LVS}/SNAP_2")
    top = _Snap(f"{LVS}/SNAP_3")
    verdict, rec, _ = _walk(
        monkeypatch,
        {f"{LVS}/SNAP_3": "SNAP_2", f"{LVS}/SNAP_2": "SNAP_1", f"{LVS}/SNAP_1": None},
        [s1, s2, top], top)
    assert verdict == "pending"
    assert rec.get_id() == s1.get_id()


def test_user_snapshot_in_the_chain_is_pending_too(monkeypatch):
    """A user snapshot between cadence snapshots holds writes the next internal
    snapshot no longer carries -- it must be transferred like any ancestor."""
    replicated = _Snap(f"{LVS}/SNAP_1", target="REMOTE_1")
    user = _Snap(f"{LVS}/SNAP_USER")
    top = _Snap(f"{LVS}/SNAP_3")
    verdict, rec, _ = _walk(
        monkeypatch,
        {f"{LVS}/SNAP_3": "SNAP_USER", f"{LVS}/SNAP_USER": "SNAP_1",
         f"{LVS}/SNAP_1": None},
        [replicated, user, top], top)
    assert verdict == "pending"
    assert rec.get_id() == user.get_id()


def test_shared_ancestor_is_found_by_every_descendant(monkeypatch):
    """The tree: three clones' snapshots all sit on one shared base. Every
    walk names the SAME record, so task dedupe collapses them to one transfer."""
    shared = _Snap(f"{LVS}/SNAP_BASE")
    tops = [_Snap(f"{LVS}/SNAP_C{i}") for i in (1, 2, 3)]
    bases: dict[str, str | None] = {f"{LVS}/SNAP_C{i}": "SNAP_BASE" for i in (1, 2, 3)}
    bases[f"{LVS}/SNAP_BASE"] = None
    picked = set()
    for top in tops:
        verdict, rec, _ = _walk(monkeypatch, bases, [shared] + tops, top)
        assert verdict == "pending"
        picked.add(rec.get_id())
    assert picked == {shared.get_id()}, "all descendants must converge on ONE transfer"


def test_replicated_shared_ancestor_is_recognized_as_existent(monkeypatch):
    """Once the shared base is on the target, every other descendant sees it."""
    shared = _Snap(f"{LVS}/SNAP_BASE", target="REMOTE_BASE")
    top = _Snap(f"{LVS}/SNAP_C2")
    verdict, rec, _ = _walk(
        monkeypatch,
        {f"{LVS}/SNAP_C2": "SNAP_BASE", f"{LVS}/SNAP_BASE": None},
        [shared, top], top)
    assert (verdict, rec) == ("ok", None)


def test_failback_direction_reads_the_source_marker(monkeypatch):
    base = _Snap(f"{LVS}/SNAP_1", source="REMOTE_1")
    top = _Snap(f"{LVS}/SNAP_2")
    layout = {f"{LVS}/SNAP_2": "SNAP_1", f"{LVS}/SNAP_1": None}
    verdict, _, _ = _walk(monkeypatch, layout, [base, top], top, to_source=True)
    assert verdict == "ok"
    verdict, _, _ = _walk(monkeypatch, layout, [base, top], top, to_source=False)
    assert verdict == "pending"


def test_untracked_chain_blob_blocks(monkeypatch):
    """A blob with no snapshot record cannot be replicated -- transferring on
    top of it would ship a copy with holes, so the transfer must not start."""
    top = _Snap(f"{LVS}/SNAP_2")
    verdict, _, why = _walk(
        monkeypatch, {f"{LVS}/SNAP_2": "GHOST", f"{LVS}/GHOST": None}, [top], top)
    assert verdict == "blocked"
    assert "no snapshot record" in why


def test_mid_deletion_ancestor_blocks(monkeypatch):
    dying = _Snap(f"{LVS}/SNAP_1", status=SnapShot.STATUS_IN_DELETION)
    top = _Snap(f"{LVS}/SNAP_2")
    verdict, rec, _ = _walk(
        monkeypatch, {f"{LVS}/SNAP_2": "SNAP_1", f"{LVS}/SNAP_1": None},
        [dying, top], top)
    assert verdict == "blocked"
    assert rec.get_id() == dying.get_id()


# --- delete-side guard: no swap-merge under a running transfer -------------


def _guard(monkeypatch, successor_status):
    from simplyblock_core.controllers import snapshot_controller as sc

    successor = _Snap(f"{LVS}/SNAP_2", status=successor_status)
    victim = _Snap(f"{LVS}/SNAP_1")
    victim.next_snap_uuid = successor.get_id()

    class _DB2:
        def get_snapshot_by_id(self, uuid):
            return {successor.get_id(): successor}[uuid]

    monkeypatch.setattr(sc, "db_controller", _DB2())
    return sc._successor_mid_replication(victim)


def test_delete_guard_refuses_predecessor_of_a_transferring_snapshot(monkeypatch):
    assert _guard(monkeypatch, SnapShot.STATUS_IN_REPLICATION) is True


def test_delete_guard_allows_when_successor_is_idle(monkeypatch):
    assert _guard(monkeypatch, SnapShot.STATUS_ONLINE) is False


def test_delete_guard_allows_a_chain_tail(monkeypatch):
    from simplyblock_core.controllers import snapshot_controller as sc
    tail = _Snap(f"{LVS}/SNAP_9")
    tail.next_snap_uuid = ""
    assert sc._successor_mid_replication(tail) is False


def test_delete_guard_tolerates_a_vanished_successor(monkeypatch):
    from simplyblock_core.controllers import snapshot_controller as sc
    victim = _Snap(f"{LVS}/SNAP_1")
    victim.next_snap_uuid = "GONE"

    class _DB3:
        def get_snapshot_by_id(self, uuid):
            raise KeyError(uuid)

    monkeypatch.setattr(sc, "db_controller", _DB3())
    assert sc._successor_mid_replication(victim) is False


# --- to-source auto-enqueue must yield to a forward policy ------------------


def test_no_backward_task_for_a_policy_managed_clone():
    """A fail-over clone under a FORWARD policy must not also ship its
    snapshots back to the original cluster: both directions then race on the
    same snapshots and the cutover's target-side gate starves (2026-08-21,
    two of five case-4 cutovers dead on max retry)."""
    import inspect
    from simplyblock_core.controllers import snapshot_controller as sc
    src = inspect.getsource(sc.add)
    gate = 'if lvol.cloned_from_snap and not getattr(lvol, "replication_policy_id", "")'
    assert gate in src, "the to-source enqueue must be gated on no forward policy"
    assert src.index(gate) < src.index("replicate_to_source=True"), \
        "the gate must guard the to-source enqueue"


# --- fail-back cutover: the preserved identity must evict the stale ns ------


class _EvictRPC:
    def __init__(self, namespaces, linger_polls=0):
        self.removed = []
        self._ns = list(namespaces)
        self._linger = linger_polls   # polls before a removed ns disappears

    def subsystem_get(self, nqn):
        # the real client returns ONE dict (single_or_none), never a list
        if self._linger > 0:
            self._linger -= 1
            return {"nqn": nqn, "namespaces": self._ns}
        live = [n for n in self._ns
                if (nqn, n.get("nsid")) not in self.removed]
        return {"nqn": nqn, "namespaces": live}

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        # acknowledges immediately; disappearance is governed by linger_polls,
        # modelling the fork's async remove_ns false-success
        self.removed.append((nqn, nsid))
        return True


class _EvictNode:
    def __init__(self, rpc):
        self._rpc = rpc

    def get_id(self):
        return "NODE_R"

    def rpc_client(self):
        return self._rpc


class _CloneLvol:
    nqn = "nqn.test:lvol:ORIG"
    ns_id = 7
    top_bdev = "LVS_1/LVOL_CLONE"


def test_failback_evicts_the_recovered_sources_stale_namespace():
    """2026-08-24: on a recovered source the preserved-NQN subsystem still held
    the ORIGINAL volume's namespace at the clone's nsid; add_ns failed -32602
    on every retry and 5/5 fail-back cutovers died on max retry."""
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _EvictRPC([{"nsid": 7, "bdev_name": "LVS_1/LVOL_ORIG"}])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == [("nqn.test:lvol:ORIG", 7)]


def test_failback_eviction_is_idempotent_for_its_own_namespace():
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _EvictRPC([{"nsid": 7, "bdev_name": "LVS_1/LVOL_CLONE"}])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == []


def test_failback_eviction_leaves_other_namespaces_alone():
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _EvictRPC([{"nsid": 3, "bdev_name": "LVS_1/OTHER"}])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == []


def test_failback_eviction_tolerates_a_missing_subsystem():
    from simplyblock_core.controllers import lvol_controller as lc

    class _NoSubsysRPC(_EvictRPC):
        def subsystem_get(self, nqn):
            return None
    rpc = _NoSubsysRPC([])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))   # must not raise
    assert rpc.removed == []


def test_failback_eviction_waits_out_the_async_removal(monkeypatch):
    """remove_ns acknowledges before it completes (the PVC-expand
    false-success); the eviction must confirm the namespace is GONE before
    add_ns runs, or the add races the removal and loses (run 20260824_110959:
    40 evictions immediately followed by 40 add_ns -32602)."""
    from simplyblock_core.controllers import lvol_controller as lc
    monkeypatch.setattr(lc.time, "sleep", lambda s: None)
    rpc = _EvictRPC([{"nsid": 7, "bdev_name": "LVS_1/LVOL_ORIG"}], linger_polls=3)
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == [("nqn.test:lvol:ORIG", 7)]
    # after the helper returns, the namespace must actually be gone
    assert rpc.subsystem_get("nqn.test:lvol:ORIG")["namespaces"] == []


def test_failback_eviction_matches_by_uuid_too(monkeypatch):
    from simplyblock_core.controllers import lvol_controller as lc
    monkeypatch.setattr(lc.time, "sleep", lambda s: None)
    rpc = _EvictRPC([{"nsid": 3, "uuid": "LV_UUID", "bdev_name": "LVS_1/LVOL_ORIG"}])
    class _C(_CloneLvol):
        uuid = "LV_UUID"
    lc._evict_stale_namespace(_C(), _EvictNode(rpc))
    assert rpc.removed == [("nqn.test:lvol:ORIG", 3)]


def test_failback_evicts_on_every_ha_node_not_just_the_primary(monkeypatch):
    """Run 20260824_113711: eviction on the primary made ITS add_ns succeed
    (result: 1) while the HA peer's failed with the same -32602 -- the
    preserved-NQN subsystem exists on EVERY node of the recovered set, each
    still holding the original namespace. The peer failure rolled the whole
    cutover back: 0/5. The clone path must evict per node."""
    from simplyblock_core.controllers import lvol_controller as lc

    def _fake_add_lvol_on_node(lvol, node, is_primary=True):
        added.append((node.get_id(), is_primary))
        return {"uuid": "U", "driver_specific": {"lvol": {"blobid": 9}}}, None

    evicted, added = [], []
    monkeypatch.setattr(lc, "_evict_stale_namespace",
                        lambda lvol, node: evicted.append(node.get_id()))
    monkeypatch.setattr(lc, "add_lvol_on_node", _fake_add_lvol_on_node)

    class _N:
        def __init__(self, nid, secondary="", tertiary=""):
            self._id, self.secondary_node_id, self.tertiary_node_id = nid, secondary, tertiary
            self.lvstore = "LVS_1"
            self.status = lc.StorageNode.STATUS_ONLINE
        def get_id(self):
            return self._id

    primary = _N("P", secondary="S")
    peer = _N("S")

    class _DB:
        kv_store = None
        def get_storage_node_by_id(self, nid):
            return {"P": primary, "S": peer}[nid]
        def release_lvol_ns_slot(self, lvol):
            pass

    class _Lvol:
        uuid = "ORIG"
        nqn = "nqn.test:lvol:ORIG"
        ns_id = 7
        lvol_bdev = "LVOL_C"
        crypto_bdev = ""
        def __deepcopy__(self, memo):
            c = _Lvol()
            c.__dict__.update(self.__dict__)
            return c
        def write_to_db(self, kv=None):
            pass

    class _Snap:
        cluster_id = "C1"
        snap_bdev = "LVS_1/SNAP_1"
        def get_id(self):
            return "SNAP1"

    new_lvol, error = lc._create_target_lvol_clone(_DB(), _Lvol(), primary, "POOL", _Snap())
    assert error is None
    assert evicted == ["P", "S"], \
        "stale-namespace eviction must run on the primary AND every online HA peer"
    assert ("S", False) in added


def test_interrupted_landing_volume_is_adopted_or_cleared():
    """Case 6, run 20260824_144226: a node outage mid-create left a REP_*
    landing volume whose id was never stored on the task; every retry then
    died on "LVol name must be unique" and three volumes' chains stalled for
    the rest of the run. Before creating the landing volume, the runner must
    look for a record already wearing the derived name and adopt it (online),
    wait for it (in_deletion), or clear it (half-created)."""
    import inspect
    from simplyblock_core.services import snapshot_replication as sr
    src = inspect.getsource(sr)
    probe = src.index('rep_name = f"REP_{snapshot.snap_name}"')
    create = src.index("lvol_controller.add_lvol_ha")
    assert probe < create, "the adopt/clear probe must run before the create"
    adopt = src.index("Adopting landing volume")
    assert probe < adopt < create
    for handled in ("STATUS_ONLINE", "STATUS_IN_DELETION", "force_delete=True"):
        assert src.index(handled, probe) < create, \
            f"collision handling must cover {handled} before creating"


def test_failover_guard_matches_nqn_and_nsid_not_nqn_alone():
    """Soak case 7 (run 20260824_174611): namespaced volumes SHARE a subsystem,
    so the fail-over idempotency guard's nqn-only match fired for every
    namespace after the first — namespaces 2..N returned namespace 1's target
    lvol id and were never failed over, losing 9 of 10 volumes in a DR event
    while reporting success. The guard must compare the FULL preserved
    identity: nqn AND ns_id."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc.replicate_lvol_on_target_cluster)
    assert "lv.nqn == lvol.nqn and lv.ns_id == lvol.ns_id" in src, \
        "the fail-over existing-copy guard must match nqn AND nsid"


def test_namespaced_siblings_replicate_to_the_same_target_node():
    """Soak case 7 (run 20260824_215758): the replication destination was
    picked per volume by capacity, so volumes SHARING a subsystem were
    scattered across the target cluster's nodes. A fail-over copy preserves
    the NQN, so that splits one shared subsystem across unrelated primaries
    -- each advertising the same NQN with only part of the namespaces.
    Siblings must inherit the node their subsystem already replicates to."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc.add_lvol_ha)
    assert "sibling_node_id" in src, "namespaced siblings must share a replication node"
    pick = src.index("_get_next_3_nodes(replication_cluster_id")
    check = src.index("sibling_node_id")
    assert check < pick, "the sibling lookup must precede the capacity-based pick"
    assert "lv.nqn == lvol.nqn" in src, "siblings are identified by shared NQN"


def test_clone_register_confirms_the_bdev_before_add_ns():
    """Run 20260825_122423: bdev_lvol_clone_register acknowledges before the
    bdev is examinable, and the peer's nvmf_subsystem_add_ns raced it and lost
    (-32602 with the subsystem EMPTY; the bdev existed moments later). Third
    member of the acknowledge-before-complete family, after remove_ns
    (PVC-expand) and the case-3 eviction. The stack build must poll the bdev
    into existence before the namespace add runs."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc._create_bdev_stack)
    reg = src.index("bdev_lvol_clone_register")
    assert "not appear within 20s" in src[reg:], \
        "clone_register must be followed by a bdev confirmation poll"
    poll = src.index("not appear within 20s", reg)
    assert "get_bdevs" in src[reg:poll], "the poll must probe get_bdevs"


def test_retired_landing_records_are_record_only_deletions():
    """A retired landing volume's record deliberately carries an EMPTY
    bdev_stack: its blob lives on as the converted, chained snapshot. The
    monitor must retire such a record without issuing ANY bdev delete (runs
    20260824/20260825: interrupted retirements left records in_deletion that
    the delete flow could never finish -- status poll 4, forever -- and a
    naive top_bdev fallback delete would have destroyed the replicated
    snapshot's data)."""
    import inspect
    from simplyblock_core.services import lvol_monitor as lm
    src = inspect.getsource(lm.check_node)
    guard = src.index("if not lvol.bdev_stack:")
    flow = src.index("delete_lvol_from_node", guard)
    finish = src.index("process_lvol_delete_finish", guard)
    assert finish < flow, "empty-stack records must retire BEFORE the delete flow"


def test_retirement_tears_down_plumbing_without_delete_lvol():
    """The retirement path must not route through delete_lvol: that flips the
    record to in_deletion for the monitor's async machinery, so any
    interruption before remove() strands the record."""
    import inspect
    from simplyblock_core.services import snapshot_replication as sr
    src = inspect.getsource(sr)
    empty = src.index("remote_lv.bdev_stack = []")
    remove = src.index("remote_lv.remove(db.kv_store)", empty)
    seg = src[empty:remove]
    assert "delete_lvol_from_node" in seg, "teardown must be the direct per-node call"
    assert "delete_lvol(remote_lv" not in seg, "must not route through delete_lvol"


def test_shared_subsystem_survives_one_members_teardown():
    """Run 20260825_224221: a stuck in_deletion member's teardown loop saw the
    SHARED subsystem transiently empty on the HA peer (between one member's
    teardown and the next member's add) and deleted it -- every following
    namespaced fail-over then died in add_ns on a missing subsystem (8/20
    landed). Delete-on-empty must first prove no other live volume claims
    the NQN."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc._remove_lvol_subsys_from_node)
    guard = src.index("other")
    delete = src.index("subsystem_delete")
    assert guard < delete, "the other-claimants check must precede subsystem_delete"
    assert "x.nqn == lvol.nqn" in src, "claimants are identified by shared NQN"
    assert "Leaving subsystem" in src
