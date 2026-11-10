"""Chain-target resolution for replicated snapshots.

Root cause of the case-2 all-zeros fail-over (lab 2026-08-14, run 9): the
old lookup matched target-cluster nodes against source-cluster instances, so
``bdev_lvol_add_clone`` was never attempted (chain_attempts=0 across the whole
run). Every replicated snapshot ended up a standalone blob: fail-over clones
read only the last delta and zeros elsewhere.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import FinishRequest, SourceSnapshot
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


def test_a_replicated_sibling_wins_over_snap_ref_id(monkeypatch):
    """snap_ref_id names a clone lineage's ORIGIN (snapshot_controller.add
    sets it on every snapshot of a clone), not the snapshot before this one.
    Letting it win chained every delta onto the origin (2026-10-01)."""
    ref = _mk_snap("refsnap", 50, "LV1", "N1", target="TR")
    cur = _mk_snap("cur", 300, "LV1", "N1", ref="refsnap")
    newer_cand = _mk_snap("cand", 200, "LV1", "N1", target="TC")
    _patch(monkeypatch, [ref, cur, newer_cand])
    prev = sr._previous_replicated_snapshot(cur, False)
    assert prev.uuid == "cand"


def test_snap_ref_id_is_used_only_without_a_sibling(monkeypatch):
    ref = _mk_snap("refsnap", 50, "LV1", "N1", target="TR")
    cur = _mk_snap("cur", 300, "LV1", "N1", ref="refsnap")
    _patch(monkeypatch, [ref, cur])
    assert sr._previous_replicated_snapshot(cur, False).uuid == "refsnap"


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


def test_the_predecessors_copy_is_named_for_the_chain(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [_mk_snap("old2", 200, "LV1", "N1", target="T2"),
                         _mk_snap("T2", 210, "RLV", "REMOTE"), cur])
    assert sr._predecessor_copy_id(cur, False) == "T2"


def test_no_predecessor_names_no_copy(monkeypatch):
    cur = _mk_snap("cur", 300, "LV1", "N1")
    _patch(monkeypatch, [cur])
    assert sr._predecessor_copy_id(cur, False) == ""


def _finish_onto(copy_id, copies):
    """The destination's finish of a landing volume on REMOTE, chained onto
    *copy_id* (peer.service.finish_landing)."""
    db = MagicMock()
    db.get_lvol_by_id.return_value = SimpleNamespace(status="online", lvs_name="LVS",
                                                     top_bdev="LVS/REP_cur")

    def snapshot(uuid):
        if uuid not in copies:
            raise KeyError(uuid)
        return copies[uuid]

    db.get_snapshot_by_id.side_effect = snapshot
    leader = MagicMock()
    leader.get_id.return_value = "REMOTE"
    with patch.object(peer_service, "stable_leader", return_value=(leader, "")):
        return peer_service.finish_landing(db, FinishRequest(
            source_cluster_id="SRC", landing_lvol_id="L", predecessor_id=copy_id, link="source",
            snapshot=SourceSnapshot(snapshot_id="cur", snap_name="cur", size=1)))


def test_a_missing_remote_copy_fails_the_finish_loudly():
    """A predecessor exists but its remote copy is gone: the finish must FAIL
    (retry later), never finalize an unchained snapshot silently."""
    answer = _finish_onto("T_GONE", {})
    assert answer.state == "retry" and "cannot be resolved" in answer.reason


def test_a_copy_outside_the_leaders_lvstore_fails_the_finish_loudly():
    answer = _finish_onto("T2", {"T2": _mk_snap("T2", 210, "RLV", "OTHER_NODE")})
    assert answer.state == "retry" and "across lvstores" in answer.reason
