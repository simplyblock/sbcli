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

from typing import ClassVar, Optional

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
    bases: dict[str, Optional[str]] = {f"{LVS}/SNAP_C{i}": "SNAP_BASE" for i in (1, 2, 3)}
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
    secondary_node_id = None
    tertiary_node_id = None

    def __init__(self, rpc):
        self._rpc = rpc

    def get_id(self):
        return "NODE_R"

    def rpc_client(self):
        return self._rpc


class _CloneLvol:
    nqn = "nqn.test:lvol:ORIG"
    ns_id = 7
    # A real LVol always carries a uuid, and the eviction matches on it --
    # a fake without one made the matcher raise into the best-effort except
    # and silently evict nothing, the exact failure mode this suite exists
    # to catch.
    uuid = "11111111-1111-1111-1111-111111111111"
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

    evicted, added = [], []
    monkeypatch.setattr(lc, "_evict_stale_namespace",
                        lambda lvol, node, **kw: evicted.append(node.get_id()))

    def _fake_add_lvol_on_node(lvol, node, is_primary=True, **kw):
        # The real signature also takes min_cntlid / ns_uuid / primary_nsid;
        # accept them so this fake cannot drift out of call-compatibility.
        added.append((node.get_id(), is_primary))
        return {"uuid": "U", "driver_specific": {"lvol": {"blobid": 9}}}, None

    monkeypatch.setattr(lc, "add_lvol_on_node", _fake_add_lvol_on_node)

    class _N:
        def __init__(self, nid, secondary="", tertiary=""):
            self._id, self.secondary_node_id, self.tertiary_node_id = nid, secondary, tertiary
            self.lvstore = "LVS_1"
            self.status = lc.StorageNode.STATUS_ONLINE
            self.cluster_id = "CL_tgt"
            self.lvol_subsys_port = 4420

        def rpc_client(self):
            # Real StorageNode hands out an RPC client; the nsid claim asks it
            # what this subsystem already holds. Nothing here yet.
            class _R:
                @staticmethod
                def subsystem_get(nqn):
                    return None
            return _R()

        def get_lvol_subsys_port(self, lvstore):
            # Real StorageNode resolves a per-lvstore listener port; the clone
            # copies it onto the new volume so suspend_lvol addresses the right
            # listener.
            return self.lvol_subsys_port
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
        def get_lvols(self):
            # No copy of this subsystem exists on the target yet, so the
            # one-subsystem-one-primary guard has nothing to redirect to.
            return []
        def get_lvol_replication_objects(self):
            # This volume is not a fail-over copy, so no original is
            # superseded and nothing gets retired.
            return []

    class _Lvol:
        uuid = "ORIG"; nqn = "nqn.test:lvol:ORIG"; ns_id = 7
        # Real LVol carries these; the clone reads namespace to decide whether
        # it attaches to a sibling's subsystem or creates its own.
        namespace = ""
        max_namespace_per_subsys = 1
        lvol_bdev = "LVOL_C"; crypto_bdev = ""
        def __deepcopy__(self, memo):
            c = _Lvol(); c.__dict__.update(self.__dict__); return c
        def write_to_db(self, kv=None):
            pass

    class _Snap:
        cluster_id = "C1"; snap_bdev = "LVS_1/SNAP_1"
        def get_id(self):
            return "SNAP1"

    new_lvol, error = lc._create_target_lvol_clone(_DB(), _Lvol(), primary, "POOL", _Snap())
    assert error is None
    assert evicted == ["P", "S"], \
        "stale-namespace eviction must run on the primary AND every online HA peer"
    assert ("S", False) in added


def test_failback_clone_keeps_the_client_visible_wire_identity(monkeypatch):
    """The fail-back clone must advertise the SOURCE's wire identity — what
    the connected client's multipath head currently holds — or the kernel
    rejects every preconnected target path ("IDs don't match for shared
    namespace N") and deleteSource then removes the head's only live paths
    (run 2026-09-02 ~19:00, subsystem 20d8a917: no available path, XFS
    shutdown on a restaged pod). Explicitly NOT the superseded original's
    uuid: that variant was tried (run 2026-09-02 17:00) and only served
    clients still riding the unfenced superseded original, whose writes
    fail-back discards anyway."""
    from simplyblock_core.controllers import lvol_controller as lc

    added = []
    monkeypatch.setattr(lc, "_evict_stale_namespace", lambda lvol, node, **kw: None)
    monkeypatch.setattr(lc.utils, "get_random_vuid", lambda *a, **kw: 999)

    def _fake_add_lvol_on_node(lvol, node, is_primary=True, ns_uuid=None, **kw):
        added.append((node.get_id(), ns_uuid, lvol.guid))
        return {"uuid": "U", "driver_specific": {"lvol": {"blobid": 9}}}, None

    monkeypatch.setattr(lc, "add_lvol_on_node", _fake_add_lvol_on_node)

    class _Original:
        uuid = "ORIG_ID"
        guid = "ORIG_NGUID"

    monkeypatch.setattr(lc, "_superseded_original", lambda *a, **kw: _Original())

    class _N:
        def __init__(self, nid, secondary=""):
            self._id, self.secondary_node_id, self.tertiary_node_id = nid, secondary, ""
            self.lvstore = "LVS_1"
            self.status = lc.StorageNode.STATUS_ONLINE
            self.cluster_id = "CL_tgt"

        def get_lvol_subsys_port(self, lvstore):
            return 4420

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

        def get_lvols(self):
            return []

    class _Lvol:
        uuid = "DR_ID"; nqn = "nqn.test:lvol:SHARED"; ns_id = 3
        guid = "DR_NGUID"
        ns_uuid = ""
        namespace = ""
        max_namespace_per_subsys = 10
        lvol_bdev = "LVOL_28"; crypto_bdev = ""

        def __deepcopy__(self, memo):
            c = _Lvol(); c.__dict__.update(self.__dict__); return c

        def write_to_db(self, kv=None):
            pass

    class _Snap:
        cluster_id = "C1"; snap_bdev = "LVS_1/SNAP_1"

        def get_id(self):
            return "SNAP1"

    new_lvol, error = lc._create_target_lvol_clone(
        _DB(), _Lvol(), primary, "POOL", _Snap(), for_migration=True)
    assert error is None
    # Every node's add_ns must carry the DR source's wire identity — what the
    # client's head holds — never the superseded original's.
    assert [(nid, ns) for nid, ns, _ in added] == [("P", "DR_ID"), ("S", "DR_ID")]
    assert all(guid == "DR_NGUID" for _, _, guid in added)
    # The wire identity is persisted so connect_lvol can report it.
    assert new_lvol.ns_uuid == "DR_ID"
    # And the clone still got its own bdev name (adoption guard).
    assert new_lvol.lvol_bdev == "LVOL_999"

    # A second fail-back cycle: the DR source's own wire identity is already
    # borrowed (its ns_uuid points at an earlier generation). The chain must
    # propagate — the client's head knows only the ORIGINAL wire id.
    added.clear()

    class _Gen2Lvol(_Lvol):
        ns_uuid = "GEN0_WIRE_ID"

    new_lvol, error = lc._create_target_lvol_clone(
        _DB(), _Gen2Lvol(), primary, "POOL", _Snap(), for_migration=True)
    assert error is None
    assert [(nid, ns) for nid, ns, _ in added] == [
        ("P", "GEN0_WIRE_ID"), ("S", "GEN0_WIRE_ID")]
    assert new_lvol.ns_uuid == "GEN0_WIRE_ID"


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
    # The guard now resolves the copy through the replication record, because
    # merging R26.3 made BOTH string matches unreliable: siblings share the
    # NQN, and a fail-over claims a target-local nsid so the numbers need not
    # match. nqn+nsid remains the fallback when no record exists yet.
    assert "own_copies" in src, \
        "the fail-over existing-copy guard must identify the copy explicitly"
    assert "rep.source_lvol.get_id() == lvol.get_id()" in src, \
        "the copy is the target of THIS volume's replication record"
    assert "lv.ns_id != lvol.ns_id" in src, \
        "nqn+nsid must remain the fallback when there is no record"


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
    assert "_sibling_replication_node" in src,         "namespaced siblings must share a replication node"
    pick = src.index("_get_next_3_nodes(replication_cluster_id")
    check = src.index("_sibling_replication_node")
    assert check < pick, "the sibling lookup must precede the capacity-based pick"

    # ...and it must run AGAIN after claim_lvol_ns_slot. That transaction is
    # what authoritatively decides the subsystem: the earlier lookup only saw
    # the ADVISORY pick from _resolve_lvol_subsystem, so a volume the
    # transaction rehomed into an existing shared subsystem would otherwise
    # keep a target node chosen for a subsystem it is no longer in -- which is
    # how the group was still split in run 20260826_230358 (peer held nsids
    # 1..7, the split-off primary asked for 1).
    claim = src.index("claim_lvol_ns_slot")
    realign = src.index("_realign_replication_node_after_claim")
    assert claim < realign,         "the target node must be re-derived from the subsystem the CLAIM chose"
    assert "lv.nqn == lvol.nqn" in inspect.getsource(lc._sibling_replication_node),         "siblings are identified by shared NQN"


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


def test_eviction_never_removes_a_siblings_namespace():
    """On a SHARED subsystem the nsid is not this volume's identity: siblings
    hold the other slots, and new_lvol.ns_id is still the SOURCE cluster's
    number (the destination primary auto-assigns and only then overwrites the
    record). Matching on nsid would evict a sibling's live, already-failed-over
    namespace -- taking a healthy volume's device away from its client."""
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _EvictRPC([
        {"nsid": 7, "bdev_name": "LVS_1/LVOL_SIBLING",
         "uuid": "22222222-2222-2222-2222-222222222222"},   # same nsid, other volume
        {"nsid": 3, "bdev_name": "LVS_1/LVOL_MINE_OLD",
         "uuid": "11111111-1111-1111-1111-111111111111"},   # THIS volume, stale
    ])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == [("nqn.test:lvol:ORIG", 3)], \
        "must evict only this volume's own namespace, never the sibling at nsid 7"


def test_eviction_still_uses_nsid_on_a_single_namespace_subsystem():
    """A dedicated subsystem cannot hold anyone else's namespace, so nsid stays
    a safe match there -- which is the fail-back-to-a-recovered-source case the
    eviction was written for, where the old record may carry a different uuid."""
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _EvictRPC([{"nsid": 7, "bdev_name": "LVS_1/LVOL_ORIG",
                      "uuid": "99999999-9999-9999-9999-999999999999"}])
    lc._evict_stale_namespace(_CloneLvol(), _EvictNode(rpc))
    assert rpc.removed == [("nqn.test:lvol:ORIG", 7)]


class _SubsysProbeRPC:
    def __init__(self, existing_nqns):
        self._existing = set(existing_nqns)
        self.probed = []

    def subsystem_get(self, nqn):
        self.probed.append(nqn)
        return {"nqn": nqn, "namespaces": []} if nqn in self._existing else None


class _ProbeNode:
    def get_id(self):
        return "NODE_PEER"


class _NsLvol:
    nqn = "nqn.test:lvol:SHARED"
    namespace = "SHARED"          # truthy => shared/namespaced subsystem


class _DedicatedLvol:
    nqn = "nqn.test:lvol:OWN"
    namespace = ""                # dedicated subsystem


def test_shared_subsystem_is_created_on_a_node_that_lacks_it():
    """Case 7, run 20260826_214011: the very first namespaced fail-over died
    with 'subsystem does not exist on <peer>'. The create-vs-attach decision
    was read from the DB alone -- which says the volume SHARES a subsystem,
    not whether THIS node has one. The primary self-heals through the -32602
    fallback; a replica cannot (its nsid is fixed by the primary), so it must
    create the subsystem when the node genuinely lacks it."""
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _SubsysProbeRPC(existing_nqns=[])           # peer has nothing
    assert lc._resolve_namespaced_subsystem(_NsLvol(), rpc, _ProbeNode()) is True
    assert rpc.probed == ["nqn.test:lvol:SHARED"]


def test_shared_subsystem_is_reused_where_it_already_exists():
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _SubsysProbeRPC(existing_nqns=["nqn.test:lvol:SHARED"])
    assert lc._resolve_namespaced_subsystem(_NsLvol(), rpc, _ProbeNode()) is False


def test_dedicated_subsystem_needs_no_probe():
    """A non-namespaced volume always creates its own -- do not spend an RPC."""
    from simplyblock_core.controllers import lvol_controller as lc
    rpc = _SubsysProbeRPC(existing_nqns=[])
    assert lc._resolve_namespaced_subsystem(_DedicatedLvol(), rpc, _ProbeNode()) is True
    assert rpc.probed == []


def test_probe_failure_falls_back_to_the_record():
    """A probe that raises must not decide: assume the record is right
    (attach), which is the pre-existing behaviour."""

    class _Boom:
        def subsystem_get(self, nqn):
            raise RuntimeError("rpc down")

    from simplyblock_core.controllers import lvol_controller
    assert lvol_controller._resolve_namespaced_subsystem(
        _NsLvol(), _Boom(), _ProbeNode()) is False


class _RollbackRPC:
    def __init__(self, ns):
        self.ns = list(ns)
        self.removed_ns, self.deletes = [], []

    def subsystem_get(self, nqn):
        return {"nqn": nqn, "namespaces": self.ns}

    def nvmf_subsystem_remove_ns(self, nqn, nsid):
        self.removed_ns.append(nsid)
        self.ns = [n for n in self.ns if n.get("nsid") != nsid]
        return True

    def get_bdevs(self, name):
        return [{"name": name}]

    def delete_lvol(self, name, sync=False):
        self.deletes.append((name, sync))
        return True, None


def test_replica_rollback_clears_its_namespace_and_syncs_the_delete():
    """Case 7, run 20260826_221806: a failed replica leg left BOTH its bdev
    and its namespace on the peer, because the rollback issued the
    leader-gated async delete a non-leader refuses ("Deleting async lvol on
    non-leader lvs"). Four such leftovers accumulated on the peer's shared
    subsystem, so the primary -- starting clean -- handed nsid 1 to a
    different volume and every later fail-over was rejected. The rollback
    must remove the namespace it added and delete the bdev synchronously."""
    from simplyblock_core.controllers import lvol_controller as lc

    class _Lvol:
        nqn = "nqn.test:lvol:SHARED"
        top_bdev = "LVS_1/LVOL_NEW"
        bdev_stack: ClassVar[list] = [
            {"type": "bdev_lvol_clone", "name": "LVS_1/LVOL_NEW"}]
        status = ""
        def get_id(self):
            return "LV_NEW"
        def write_to_db(self, *a, **kw):
            pass

    rpc = _RollbackRPC([
        {"nsid": 1, "bdev_name": "LVS_1/LVOL_OTHER"},   # someone else's
        {"nsid": 5, "bdev_name": "LVS_1/LVOL_NEW"},     # this attempt's
    ])
    ok, _msg = lc._fail_after_bdev(_Lvol(), rpc, "boom", is_primary=False)
    assert ok is False
    assert rpc.removed_ns == [5], "must drop only THIS attempt's namespace"
    assert rpc.deletes and all(sync for _n, sync in rpc.deletes), \
        "a replica rollback must use the SYNC delete a non-leader accepts"


def test_failover_rollback_covers_every_placed_node_with_ids():
    """Case 7, run 20260826_223631: a peer add failure rolled back only the
    PRIMARY, so a third target node's failure left the SECONDARY holding the
    namespace. The next sibling's primary then auto-assigned an nsid the peer
    had already given away, and its replica add was rejected with
    'wanted nsid=2 ... holds=[(2, <other volume>)]'. Worse, the rollback
    passed the LVol/StorageNode OBJECTS to delete_lvol_from_node(lvol_id,
    node_id), whose 'except KeyError: return True' swallowed the mismatch --
    so it reported success while deleting nothing."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc
    src = inspect.getsource(lc._create_target_lvol_clone)
    assert "placed_nodes" in src, "rollback must track every node that got the copy"
    assert "for node in placed_nodes:" in src
    assert "new_lvol.get_id(), node.get_id()" in src, "must pass ids, not records"
    assert "delete_lvol_from_node(new_lvol, target_node)" not in src

    # the fail-back clone had the same object-vs-id bug
    whole = inspect.getsource(lc)
    assert "delete_lvol_from_node(new_lvol," not in whole, \
        "no rollback may hand records to an id-taking function"


def test_policy_attach_also_keeps_a_subsystem_on_one_target_node():
    """Run 20260826_233417: add_lvol_ha had the sibling rule but
    replication_start -- the path a `volume add --replication-policy` takes --
    picked the target node purely by capacity. One 10-namespace subsystem
    ended up on THREE target primaries (nsids 1,2,3,6 / 1,2,4,5,7-10 /
    3-10); no node advertised the whole set and a client saw 0 of the 10.

    Both entry points must consult the sibling rule, and _create_target_lvol_clone
    re-checks it at creation time as the last line of defence."""
    import inspect
    from simplyblock_core.controllers import lvol_controller as lc

    src = inspect.getsource(lc.replication_start)
    assert "_sibling_replication_node" in src,         "attaching a policy must honour the shared-subsystem rule too"
    assert src.index("_sibling_replication_node") < src.index("_get_next_3_nodes"),         "the sibling lookup must precede the capacity-based pick"

    clone = inspect.getsource(lc._create_target_lvol_clone)
    assert "_subsystem_home_node" in clone,         "the copy must not be built on a node that splits the subsystem"
