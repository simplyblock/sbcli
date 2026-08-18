"""D2 unit tests for internal-snapshot retention on source + target."""
import itertools

from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_replication as sr


def _mk_snap(uuid, created_at, snap_type, lvol_uuid, node_id,
             status=SnapShot.STATUS_ONLINE, target=""):
    lv = LVol()
    lv.uuid = lvol_uuid
    lv.node_id = node_id
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.snap_type = snap_type
    s.status = status
    s.target_replicated_snap_uuid = target
    s.lvol = lv
    return s


class _Clone:
    def __init__(self, uuid, cloned_from, status=LVol.STATUS_ONLINE):
        self.uuid = uuid
        self.cloned_from_snap = cloned_from
        self.status = status

    def get_id(self):
        return self.uuid


class _TargetCopy:
    """A replicated snapshot as it exists on the remote cluster.

    ``prev_snap_uuid`` is the chain link retention checks before it deletes a
    predecessor: it is only written once bdev_lvol_add_clone + convert succeeded.
    """

    def __init__(self, uuid, prev_snap_uuid="", node_id="TN1"):
        self.uuid = uuid
        self.prev_snap_uuid = prev_snap_uuid
        self.snap_bdev = f"LVS_T/{uuid}"
        self.snap_uuid = f"uuid-{uuid}"
        lv = LVol()
        lv.uuid = "T_LV1"
        lv.node_id = node_id
        self.lvol = lv

    def get_id(self):
        return self.uuid


class _FakeNode:
    """Target node. Offline by default so the SPDK fallback stays out of the
    way unless a test explicitly opts into it."""

    def __init__(self, status=LVol.STATUS_OFFLINE, bdevs=None):
        self.status = status
        self._bdevs = bdevs or []

    def rpc_client(self):
        node = self

        class _RPC:
            def get_bdevs(self, name):
                return [b for b in node._bdevs if b.get("name") == name]

        return _RPC()


class _FakeDB:
    def __init__(self, source_snaps, existing_uuids, clones=(), chain=None, node=None):
        self._source_snaps = source_snaps
        self._existing = set(existing_uuids)
        self._clones = list(clones)
        self._chain = dict(chain or {})
        self._node = node or _FakeNode()

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._source_snaps if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        if uuid in self._existing:
            return _TargetCopy(uuid, self._chain.get(uuid, ""))
        raise KeyError(uuid)

    def get_storage_node_by_id(self, node_id):
        return self._node

    def get_mini_lvols(self):
        return self._clones


def _healthy_chain(source_snaps):
    """Link each replicated internal target copy onto its predecessor's.

    This is the state a converged replication leaves behind, so it is the
    default for the existing cases: they assert on retention, not on chaining.
    """
    chain = {}
    per_lvol = {}
    for s in source_snaps:
        if s.snap_type != SnapShot.TYPE_INTERNAL or not s.target_replicated_snap_uuid:
            continue
        per_lvol.setdefault(s.lvol.get_id(), []).append(s)
    for snaps in per_lvol.values():
        snaps.sort(key=lambda s: s.created_at)
        for prev, nxt in itertools.pairwise(snaps):
            chain[nxt.target_replicated_snap_uuid] = prev.target_replicated_snap_uuid
    return chain


class _FakeSnapCtl:
    def __init__(self, db):
        self.deleted = []
        self._db = db

    def delete(self, uuid, force_delete=False):
        self.deleted.append(uuid)
        self._db._existing.discard(uuid)
        return True


def _patch(monkeypatch, source_snaps, existing_uuids, clones=(), chain=None, node=None):
    if chain is None:
        chain = _healthy_chain(source_snaps)
    db = _FakeDB(source_snaps, existing_uuids, clones, chain, node)
    snapctl = _FakeSnapCtl(db)
    monkeypatch.setattr(sr, "db", db)
    monkeypatch.setattr(sr, "snapshot_controller", snapctl)
    return snapctl


def test_prunes_older_internal_keeps_newest_and_users(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("user_mid", 150, SnapShot.TYPE_USER, "LV1", "N1", target="T_user"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_user", "T_mid", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    # Target copy deleted before the source snapshot; the newest PAIR is kept
    # so an arriving snapshot always has a predecessor to chain onto.
    assert snapctl.deleted == ["T_old", "int_old"]
    for kept in ("int_mid", "T_mid", "int_new", "T_new", "user_mid", "T_user"):
        assert kept not in snapctl.deleted


def test_single_internal_not_pruned(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [_mk_snap("int_only", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_only")]
    snapctl = _patch(monkeypatch, snaps, {"T_only"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []


def test_unreplicated_internal_ignored(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    # Newest internal not yet replicated (no target) -> excluded; the only
    # replicated internal is the single oldest, so nothing is pruned.
    snaps = [
        _mk_snap("int_repl", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_repl"),
        _mk_snap("int_pending", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target=""),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_repl"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []


def test_missing_target_still_cleans_source(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_gone"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    # T_gone already deleted on target -> only source snapshot is cleaned up.
    snapctl = _patch(monkeypatch, snaps, {"T_mid", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["int_old"]


def test_other_lvol_snapshots_untouched(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
        # Different lvol on same node — must never be pruned.
        _mk_snap("other_old", 50, SnapShot.TYPE_INTERNAL, "LV2", "N1", target="TO_old"),
        _mk_snap("other_new", 250, SnapShot.TYPE_INTERNAL, "LV2", "N1", target="TO_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new", "TO_old", "TO_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_never_prunes_snapshot_a_failed_over_volume_is_cloned_from(monkeypatch):
    """Root cause of the all-zeros DR fail-over (labs 2026-08-10/11).

    Fail-over clones the volume from the last replicated TARGET snapshot; the
    prune, keyed only on the SOURCE snapshot age, then deleted that target copy.
    The delete reaches SPDK as bdev_lvol_delete(sync=False) and frees the blocks
    immediately, so no DB-level guard downstream can save the clone. Retention
    must skip a target snapshot with a live dependent clone.
    """
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    # A failed-over volume lives on the OLD target snapshot.
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_new"},
                     clones=[_Clone("FO_VOL", "T_old")])

    sr._prune_internal_snapshots(source_lvol)

    assert "T_old" not in snapctl.deleted, (
        "pruned the target snapshot a failed-over volume is cloned from — "
        "its blocks are freed by SPDK immediately (sync=False), the volume "
        "reads zeros from then on")
    # The source-side copy must survive too (it pairs with the kept target).
    assert "int_old" not in snapctl.deleted


def test_in_deletion_clone_does_not_pin_the_snapshot(monkeypatch):
    """A clone that is itself going away must not block retention forever."""
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new"},
                     clones=[_Clone("DYING", "T_old", status=LVol.STATUS_IN_DELETION)])

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_require_lvs_leader_gate(monkeypatch):
    """Convert on a non-leader returns success WITHOUT persisting (silent
    conversion error) — leadership must be checked BEFORE the operation and a
    non-leader must fail-and-retry, never proceed."""
    import simplyblock_core.controllers.lvol_controller as lc

    class _N:
        def get_id(self):
            return "N1"

    monkeypatch.setattr(lc, "is_node_leader", lambda node, lvs: False)
    assert sr._require_lvs_leader(_N(), "LVS_1", "convert") is False

    monkeypatch.setattr(lc, "is_node_leader", lambda node, lvs: True)
    assert sr._require_lvs_leader(_N(), "LVS_1", "convert") is True


def test_newest_pair_is_kept_so_arrivals_have_a_chain_parent(monkeypatch):
    """Chain continuity: a replicated snapshot holds only its own clusters, and
    deleting one swap-merges its segments into the successor CHAINED to it.
    Keeping just the newest pruned the predecessor the instant a replication
    finished, so the next arrival had nothing to chain onto and kept only its
    delta — the target then held the last delta over holes (labs run 15 vs 19,
    same case passing then failing on timing alone)."""
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_prev", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_prev"),
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_prev", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == [], (
        "pruned the predecessor the next arrival must chain onto")


def test_defers_prune_until_the_successor_is_actually_chained(monkeypatch):
    """The count cushion is not the precondition — the chain link is.

    Keeping the newest N only widens the window in which chaining is expected to
    have happened. If it lagged or failed for one snapshot while newer ones kept
    arriving, the predecessor was still pruned, and because the delete reaches
    SPDK as bdev_lvol_delete(sync=False) its segments were freed instead of
    swap-merged into the successor. The target then holds the newest delta over
    holes and a fail-over clone reads zeros (labs 2026-08-10..17). Retention must
    verify the link and defer while it is absent.
    """
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    # T_mid arrived but was never chained onto T_old.
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new"}, chain={})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == [], (
        "pruned a predecessor whose successor is not chained onto it — SPDK frees "
        "the blocks immediately, so those segments are lost rather than merged")


def test_prunes_once_the_chain_is_established(monkeypatch):
    """The deferral must release as soon as chaining catches up (no livelock)."""
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new"},
                     chain={"T_mid": "T_old", "T_new": "T_mid"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_spdk_verdict_releases_a_missing_db_link(monkeypatch):
    """A missing link must not pin the pair for ever.

    The link write is best-effort, and snapshots replicated before chaining was
    implemented have none at all. SPDK is the real authority, so when the DB has
    no link we ask the target node before giving up — otherwise retention would
    never release those snapshots and both chains would grow without bound.
    """
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    # SPDK reports T_mid as a clone whose base is T_old, while the DB link is absent.
    node = _FakeNode(status=LVol.STATUS_ONLINE, bdevs=[{
        "name": "LVS_T/T_mid",
        "driver_specific": {"lvol": {"clone": True, "base_snapshot": "LVS_T/T_old"}},
    }])
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new"},
                     chain={}, node=node)

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


def test_unchained_in_spdk_is_not_pruned_even_when_node_is_reachable(monkeypatch):
    """An online target that reports a standalone blob must still defer."""
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_mid", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_mid"),
        _mk_snap("int_new", 300, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    node = _FakeNode(status=LVol.STATUS_ONLINE, bdevs=[{
        "name": "LVS_T/T_mid",
        "driver_specific": {"lvol": {"clone": False, "base_snapshot": None}},
    }])
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_mid", "T_new"},
                     chain={}, node=node)

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == []
