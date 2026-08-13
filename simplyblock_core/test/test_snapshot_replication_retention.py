"""D2 unit tests for internal-snapshot retention on source + target."""
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


class _FakeDB:
    def __init__(self, source_snaps, existing_uuids, clones=()):
        self._source_snaps = source_snaps
        self._existing = set(existing_uuids)
        self._clones = list(clones)

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._source_snaps if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        if uuid in self._existing:
            return object()
        raise KeyError(uuid)

    def get_mini_lvols(self):
        return self._clones


class _FakeSnapCtl:
    def __init__(self, db):
        self.deleted = []
        self._db = db

    def delete(self, uuid, force_delete=False):
        self.deleted.append(uuid)
        self._db._existing.discard(uuid)
        return True


def _patch(monkeypatch, source_snaps, existing_uuids, clones=()):
    db = _FakeDB(source_snaps, existing_uuids, clones)
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
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_user", "T_new"})

    sr._prune_internal_snapshots(source_lvol)

    # Target copy deleted before the source snapshot, oldest internal only.
    assert snapctl.deleted == ["T_old", "int_old"]
    for kept in ("int_new", "T_new", "user_mid", "T_user"):
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
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    # T_gone already deleted on target -> only source snapshot is cleaned up.
    snapctl = _patch(monkeypatch, snaps, {"T_new"})

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["int_old"]


def test_other_lvol_snapshots_untouched(monkeypatch):
    source_lvol = LVol()
    source_lvol.uuid = "LV1"
    source_lvol.node_id = "N1"

    snaps = [
        _mk_snap("int_old", 100, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_old"),
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
        # Different lvol on same node — must never be pruned.
        _mk_snap("other_old", 50, SnapShot.TYPE_INTERNAL, "LV2", "N1", target="TO_old"),
        _mk_snap("other_new", 250, SnapShot.TYPE_INTERNAL, "LV2", "N1", target="TO_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_new", "TO_old", "TO_new"})

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
        _mk_snap("int_new", 200, SnapShot.TYPE_INTERNAL, "LV1", "N1", target="T_new"),
    ]
    snapctl = _patch(monkeypatch, snaps, {"T_old", "T_new"},
                     clones=[_Clone("DYING", "T_old", status=LVol.STATUS_IN_DELETION)])

    sr._prune_internal_snapshots(source_lvol)

    assert snapctl.deleted == ["T_old", "int_old"]


class _ChainNode:
    def __init__(self, uuid, children_map, decouple_ok=True):
        self.uuid = uuid
        self.secondary_node_id = ""
        self.status = "online"
        self._children_map = children_map
        self.decoupled = []
        self._ok = decouple_ok

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return self

    def get_bdevs(self, name):
        kids = self._children_map.get(name, [])
        return [{"driver_specific": {"lvol": {"clones": kids}}}]

    def bdev_lvol_decouple_parent(self, name):
        if not self._ok:
            return None
        self.decoupled.append(name)
        return True


class _ChainSnapLvol:
    node_id = "N_tgt"


class _ChainSnap:
    def __init__(self, uuid, bdev):
        self.uuid = uuid
        self.snap_bdev = bdev
        self.lvol = _ChainSnapLvol()

    def get_id(self):
        return self.uuid


def _chain_db(monkeypatch, node, tsnap):
    class _DB:
        def get_snapshot_by_id(self, sid):
            if sid == tsnap.get_id():
                return tsnap
            raise KeyError(sid)

        def get_storage_node_by_id(self, nid):
            if nid == "N_tgt":
                return node
            raise KeyError(nid)

    monkeypatch.setattr(sr, "db", _DB())
    return node


def test_decouples_snapshot_children_before_prune(monkeypatch):
    """A target snapshot's CHILD SNAPSHOT (the newer link, chained via
    add_clone at receive time) reads through it. Retention must decouple the
    child before the parent is deleted, or the surviving chain loses the data
    underneath — the 2026-08-13 empty-XFS signature."""
    tsnap = _ChainSnap("T_old", "LVS_9/LVOL_100")
    node = _chain_db(monkeypatch, _ChainNode(
        "N_tgt", {"LVS_9/LVOL_100": ["LVOL_200"]}), tsnap)
    assert sr._decouple_snapshot_children("T_old") is True
    assert node.decoupled == ["LVS_9/LVOL_200"], "child was not decoupled"


def test_prune_kept_when_decouple_fails(monkeypatch):
    tsnap = _ChainSnap("T_old", "LVS_9/LVOL_100")
    _chain_db(monkeypatch, _ChainNode(
        "N_tgt", {"LVS_9/LVOL_100": ["LVOL_200"]}, decouple_ok=False), tsnap)
    assert sr._decouple_snapshot_children("T_old") is False, (
        "decouple failed but retention would still delete the parent")


def test_childless_snapshot_needs_no_decouple(monkeypatch):
    tsnap = _ChainSnap("T_old", "LVS_9/LVOL_100")
    node = _chain_db(monkeypatch, _ChainNode("N_tgt", {}), tsnap)
    assert sr._decouple_snapshot_children("T_old") is True
    assert node.decoupled == []
