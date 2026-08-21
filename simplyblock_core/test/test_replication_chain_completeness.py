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
    bases = {f"{LVS}/SNAP_C{i}": "SNAP_BASE" for i in (1, 2, 3)}
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
