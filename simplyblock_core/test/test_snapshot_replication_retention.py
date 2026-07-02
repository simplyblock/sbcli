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


class _FakeDB:
    def __init__(self, source_snaps, existing_uuids):
        self._source_snaps = source_snaps
        self._existing = set(existing_uuids)

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._source_snaps if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        if uuid in self._existing:
            return object()
        raise KeyError(uuid)


class _FakeSnapCtl:
    def __init__(self, db):
        self.deleted = []
        self._db = db

    def delete(self, uuid, force_delete=False):
        self.deleted.append(uuid)
        self._db._existing.discard(uuid)
        return True


def _patch(monkeypatch, source_snaps, existing_uuids):
    db = _FakeDB(source_snaps, existing_uuids)
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
