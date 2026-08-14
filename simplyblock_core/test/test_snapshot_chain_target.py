"""Chain-target resolution for replicated snapshots.

Root cause of the case-2 all-zeros fail-over (lab 2026-08-14, run 9):
``snap_ref_id`` is never populated on internal replication snapshots and the
old lookup matched target-cluster nodes against source-cluster instances, so
``bdev_lvol_add_clone`` was never attempted (chain_attempts=0 across the whole
run). Every replicated snapshot ended up a standalone blob: fail-over clones
read only the last delta and zeros elsewhere.
"""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_replication as sr


def _mk_snap(uuid, created_at, lvol_uuid, node_id, target="", source="",
             status=SnapShot.STATUS_ONLINE, ref=""):
    lv = LVol()
    lv.uuid = lvol_uuid
    lv.node_id = node_id
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.status = status
    s.snap_ref_id = ref
    s.target_replicated_snap_uuid = target
    s.source_replicated_snap_uuid = source
    s.snap_bdev = f"LVS/{uuid}"
    s.lvol = lv
    return s


class _FakeDB:
    def __init__(self, snaps):
        self._snaps = list(snaps)

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._snaps if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        for s in self._snaps:
            if s.uuid == uuid:
                return s
        raise KeyError(uuid)


class _Node:
    def __init__(self, uuid):
        self._uuid = uuid

    def get_id(self):
        return self._uuid


def _patch(monkeypatch, snaps):
    monkeypatch.setattr(sr, "db", _FakeDB(snaps))


def test_picks_newest_older_replicated_of_same_lvol(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    snaps = [
        _mk_snap("old1", 100, "LV1", "N1", target="T1"),
        _mk_snap("old2", 200, "LV1", "N1", target="T2"),
        _mk_snap("newer", 400, "LV1", "N1", target="T4"),      # newer than cur
        _mk_snap("other", 250, "LV2", "N1", target="TO"),      # other lvol
        _mk_snap("unrepl", 260, "LV1", "N1"),                  # no target copy
        _mk_snap("dying", 270, "LV1", "N1", target="TD",
                 status=SnapShot.STATUS_IN_DELETION),
        cur,
    ]
    _patch(monkeypatch, snaps)
    prev = sr._previous_replicated_snapshot(cur, replicate_to_source=False)
    assert prev is not None and prev.uuid == "old2"


def test_first_snapshot_has_no_predecessor(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [cur])
    assert sr._previous_replicated_snapshot(cur, False) is None


def test_snap_ref_id_wins_when_set(monkeypatch):
    ref = _mk_snap("refsnap", 50, "LV1", "N1", target="TR")
    cur = _mk_snap("cur", 300, "LV1", "N1", ref="refsnap")
    newer_cand = _mk_snap("cand", 200, "LV1", "N1", target="TC")
    _patch(monkeypatch, [ref, cur, newer_cand])
    prev = sr._previous_replicated_snapshot(cur, False)
    assert prev.uuid == "refsnap"


def test_failback_uses_source_replicated_uuid(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    snaps = [
        _mk_snap("fwd_only", 100, "LV1", "N1", target="T1"),   # no source copy
        _mk_snap("back", 200, "LV1", "N1", source="S2"),
        cur,
    ]
    _patch(monkeypatch, snaps)
    prev = sr._previous_replicated_snapshot(cur, replicate_to_source=True)
    assert prev.uuid == "back"


def test_resolve_chain_target_happy_path(monkeypatch):
    remote_copy = _mk_snap("T2", 210, "RLV", "REMOTE")
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [_mk_snap("old2", 200, "LV1", "N1", target="T2"),
                         remote_copy, cur])
    tps, prev_db, ok = sr._resolve_chain_target(cur, False, _Node("REMOTE"))
    assert ok is True
    assert tps == {"snap_bdev": "LVS/T2"}
    assert prev_db.uuid == "T2"


def test_resolve_chain_target_no_predecessor_is_ok(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [cur])
    tps, prev_db, ok = sr._resolve_chain_target(cur, False, _Node("REMOTE"))
    assert (tps, prev_db, ok) == (None, None, True)


def test_resolve_chain_target_missing_remote_copy_fails_loudly(monkeypatch):
    """A predecessor exists but its remote copy is gone: the finish must FAIL
    (retry later), never finalize an unchained snapshot silently."""
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [_mk_snap("old2", 200, "LV1", "N1", target="T_GONE"), cur])
    tps, prev_db, ok = sr._resolve_chain_target(cur, False, _Node("REMOTE"))
    assert ok is False


def test_resolve_chain_target_wrong_node_fails_loudly(monkeypatch):
    remote_copy = _mk_snap("T2", 210, "RLV", "OTHER_NODE")
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [_mk_snap("old2", 200, "LV1", "N1", target="T2"),
                         remote_copy, cur])
    tps, prev_db, ok = sr._resolve_chain_target(cur, False, _Node("REMOTE"))
    assert ok is False
