"""Unit tests for demote_lvol (P0-3, design-csi-addons-replication.md §5.2).

Demote fences the source and confirms the last write replicated, WITHOUT
touching any target volume -- that is a separate, later PromoteVolume call.
It is synchronous and re-drivable: the driver's DemoteVolume RPC calls it
repeatedly until it reports done, so each call does only the work its current
state calls for (fence + trigger once, then just check the marker).
"""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode


def _lvol(node_id="N_src"):
    lv = LVol()
    lv.uuid = "LV1"
    lv.nqn = "nqn.orig:lvol:LV1"
    lv.ns_id = 7
    lv.node_id = node_id
    return lv


def _node(uuid, lvstore="lvs_src"):
    n = StorageNode()
    n.uuid = uuid
    n.lvstore = lvstore
    n.status = StorageNode.STATUS_ONLINE
    return n


def _snap(uuid, replicated=""):
    s = SnapShot()
    s.uuid = uuid
    s.target_replicated_snap_uuid = replicated
    return s


class _FakeDB:
    kv_store = "KV"

    def __init__(self, lvol, node, snaps=None):
        self._lvol = lvol
        self._node = node
        self._snaps = snaps or {}

    def get_lvol_by_id(self, lid):
        return self._lvol

    def get_storage_node_by_id(self, nid):
        return self._node

    def get_snapshot_by_id(self, sid):
        return self._snaps[sid]


@pytest.fixture
def patched(monkeypatch):
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)

    fenced = []
    monkeypatch.setattr(
        lvol_controller.replication_final_step, "fence_source_paths",
        lambda node, lvstore, nqn, ns_id: fenced.append((node.get_id(), lvstore, nqn, ns_id)))

    snap_add_calls = []

    def _fake_snap_add(lid, name, snap_type=SnapShot.TYPE_USER):
        snap_add_calls.append((lid, name, snap_type))
        return "SNAP1", False
    monkeypatch.setattr(lvol_controller.snapshot_controller, "add", _fake_snap_add)

    return {"fenced": fenced, "snap_add_calls": snap_add_calls}


def test_demote_fences_before_triggering_the_final_snapshot(patched, monkeypatch):
    """First call: fence THEN snapshot, never the other order.

    A write landing between snapshot and fence would be silently lost
    (replication_final_step.fence_source_paths' own documented invariant) --
    fencing must happen first.
    """
    lvol = _lvol()
    node = _node("N_src")
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _FakeDB(lvol, node))

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": False}, "not confirmed replicated yet"
    assert patched["fenced"] == [("N_src", "lvs_src", "nqn.orig:lvol:LV1", 7)]
    assert len(patched["snap_add_calls"]) == 1
    assert patched["snap_add_calls"][0][0] == "LV1"
    assert patched["snap_add_calls"][0][2] == SnapShot.TYPE_INTERNAL
    assert lvol.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING
    assert lvol.replication_demote_snapshot_id == "SNAP1"


def test_demote_does_not_refence_or_retrigger_once_pending(patched, monkeypatch):
    """Second call while still waiting: check the marker, nothing else.

    Re-fencing is harmless (ANA flips are idempotent) but re-triggering would
    orphan the first snapshot's wait and never converge.
    """
    lvol = _lvol()
    lvol.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
    lvol.replication_demote_snapshot_id = "SNAP1"
    node = _node("N_src")
    db = _FakeDB(lvol, node, snaps={"SNAP1": _snap("SNAP1", replicated="")})
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": False}
    assert patched["fenced"] == [], "already fenced, must not re-fence"
    assert patched["snap_add_calls"] == [], "already triggered, must not re-trigger"
    assert lvol.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING


def test_demote_completes_once_the_snapshot_carries_the_replicated_marker(patched, monkeypatch):
    lvol = _lvol()
    lvol.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
    lvol.replication_demote_snapshot_id = "SNAP1"
    node = _node("N_src")
    db = _FakeDB(lvol, node, snaps={"SNAP1": _snap("SNAP1", replicated="TGT_SNAP1")})
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": True}
    assert lvol.replication_demote_state == LVol.REPLICATION_DEMOTE_DONE


def test_demote_is_idempotent_once_done(patched, monkeypatch):
    lvol = _lvol()
    lvol.replication_demote_state = LVol.REPLICATION_DEMOTE_DONE
    lvol.replication_demote_snapshot_id = "SNAP1"
    node = _node("N_src")
    db = _FakeDB(lvol, node)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": True}
    assert patched["fenced"] == []
    assert patched["snap_add_calls"] == []


def test_demote_retriggers_when_tracked_snapshot_was_superseded_before_replicating(patched, monkeypatch):
    """Recovers from a demote requested before replication was ever enabled.

    The tracked snapshot was taken while do_replicate was False (e.g. Ramen
    creating a Secondary VolumeReplication from scratch, with no prior S3-seeded
    state -- confirmed live 2026-09-23), so it was never queued for cross-cluster
    replication and superseded by later cadence snapshots (next_snap_uuid set)
    once a policy was finally attached. Waiting on it forever (the existing
    "never re-trigger" behavior, correct for a snapshot still genuinely in
    flight) would never converge. do_replicate now True + the tracked snapshot
    already superseded is proof it never will land, so retrigger against a
    fresh one instead.
    """
    lvol = _lvol()
    lvol.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
    lvol.replication_demote_snapshot_id = "SNAP1"
    lvol.do_replicate = True
    node = _node("N_src")
    stale_snap = _snap("SNAP1", replicated="")
    stale_snap.next_snap_uuid = "SNAP2"
    db = _FakeDB(lvol, node, snaps={"SNAP1": stale_snap})
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    monkeypatch.setattr(lvol_controller.snapshot_controller, "add",
                        lambda lid, name, snap_type=SnapShot.TYPE_USER: (
                            patched["snap_add_calls"].append((lid, name, snap_type)) or "SNAP1_NEW", False))

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": False}, "the fresh snapshot has not replicated yet either"
    assert patched["fenced"] == [], "already fenced by the original call; re-fencing is pointless"
    assert len(patched["snap_add_calls"]) == 1, "must retrigger against a new snapshot"
    assert patched["snap_add_calls"][0][2] == SnapShot.TYPE_INTERNAL
    assert lvol.replication_demote_snapshot_id == "SNAP1_NEW", \
        "must track the fresh snapshot, not keep waiting on the superseded one"
    assert lvol.replication_demote_state == LVol.REPLICATION_DEMOTE_PENDING


def test_demote_does_not_retrigger_a_snapshot_still_genuinely_converging(patched, monkeypatch):
    """The existing, already-tested "never re-trigger" behavior must still hold
    for a snapshot that simply has not replicated yet, normal in-flight case
    (do_replicate True, not yet superseded by anything newer)."""
    lvol = _lvol()
    lvol.replication_demote_state = LVol.REPLICATION_DEMOTE_PENDING
    lvol.replication_demote_snapshot_id = "SNAP1"
    lvol.do_replicate = True
    node = _node("N_src")
    db = _FakeDB(lvol, node, snaps={"SNAP1": _snap("SNAP1", replicated="")})
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)

    result = lvol_controller.demote_lvol("LV1")

    assert result == {"demoted": False}
    assert patched["fenced"] == []
    assert patched["snap_add_calls"] == [], "not superseded yet -- still the normal converging case"
    assert lvol.replication_demote_snapshot_id == "SNAP1"


def test_demote_surfaces_a_snapshot_creation_failure(patched, monkeypatch):
    lvol = _lvol()
    node = _node("N_src")
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _FakeDB(lvol, node))
    monkeypatch.setattr(lvol_controller.snapshot_controller, "add",
                        lambda lid, name, snap_type=SnapShot.TYPE_USER: (False, "no space"))

    result = lvol_controller.demote_lvol("LV1")

    assert result == (False, "no space")
    assert lvol.replication_demote_state == "", "must not record pending on a failed trigger"
