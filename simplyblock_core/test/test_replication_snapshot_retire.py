"""A migrated-away volume's replication snapshots are retired and reaped.

Back-and-forth bed 2026-10-28: after three migration legs cluster A had no
volumes and ~430 GiB allocated, all of it repl_commit_/repl_internal_
snapshots (and landing copies of them) whose volume had been deleted at
cutover; cluster B went read-only at 96% and fio saw EIO.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import replication_snapshot_retire as rsr
from simplyblock_core.models.snapshot import SnapShot

CLUSTER = "CL_a"


def _lvol(uuid, cloned_from_snap=""):
    return SimpleNamespace(uuid=uuid, get_id=lambda: uuid, node_id="N1", nodes=[],
                           lvs_name="LVS_1", cloned_from_snap=cloned_from_snap,
                           status="online")


def _snap(uuid, lvol, name, snap_type=SnapShot.TYPE_INTERNAL, source="", prev="",
          group_id=""):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.snap_name = name
    s.snap_type = snap_type
    s.source_replicated_snap_uuid = source
    s.prev_snap_uuid = prev
    s.status = SnapShot.STATUS_ONLINE
    s.cluster_id = CLUSTER
    s.snap_bdev = f"LVS_1/{uuid}"
    s.group_id = group_id
    return s


class _FakeDB:
    def __init__(self, snaps, lvols):
        self.snaps = {s.uuid: s for s in snaps}
        self.lvols = {lv.uuid: lv for lv in lvols}
        self.minis_synced: list = []

    def get_snapshots_by_node_id(self, node_id):
        return list(self.snaps.values())

    def get_snapshot_by_id(self, sid):
        if sid not in self.snaps:
            raise KeyError(sid)
        return self.snaps[sid]

    def atomic_update(self, obj, fn):
        fn(obj)
        return obj

    def get_mini_snapshots(self):
        return list(self.snaps.values())

    def get_mini_lvols(self):
        return list(self.lvols.values())


@pytest.fixture
def leg(monkeypatch):
    """Source volume SRC on cluster A, cloned from the previous leg's landing
    copies C1 <- C2, with internal snapshots S1, S2 taken of it on this leg,
    plus a user snapshot U and an unrelated volume's internal snapshot X."""
    rep1, rep2 = _lvol("REP1"), _lvol("REP2")
    src = _lvol("SRC", cloned_from_snap="C2")
    other = _lvol("OTHER")
    snaps = [
        _snap("C1", rep1, "repl_internal_aa_1", SnapShot.TYPE_USER, source="B1"),
        _snap("C2", rep2, "repl_commit_aa", SnapShot.TYPE_USER, source="B2", prev="C1"),
        _snap("S1", src, "repl_internal_src_1"),
        _snap("S2", src, "repl_commit_src", prev="S1"),
        _snap("U", src, "nightly", SnapShot.TYPE_USER),
        _snap("X", other, "repl_internal_other_1"),
    ]
    db = _FakeDB(snaps, [src, other])
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())
    monkeypatch.setattr(rsr, "_sync_mini", lambda db, snap: db.minis_synced.append(snap.uuid))
    return db, src


def test_lineage_is_own_replication_snapshots_and_landing_chain(leg):
    db, src = leg
    ids = {s.uuid for s in rsr.source_lineage(db, src)}
    assert ids == {"C1", "C2", "S1", "S2"}


def test_retire_marks_lineage_but_never_user_or_foreign_snapshots(leg):
    db, src = leg
    assert set(rsr.retire_source_lineage(db, src)) == {"C1", "C2", "S1", "S2"}
    assert not db.snaps["U"].deleted
    assert not db.snaps["X"].deleted
    # The reaper scans the mini table; a flag only on the full record is invisible.
    assert set(db.minis_synced) == {"C1", "C2", "S1", "S2"}


def test_group_recovery_point_is_not_retired(leg, monkeypatch):
    db, src = leg
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: {"S2"})
    assert "S2" not in rsr.retire_source_lineage(db, src)
    assert not db.snaps["S2"].deleted


@pytest.fixture
def reaper(leg, monkeypatch):
    db, src = leg
    rsr.retire_source_lineage(db, src)
    deleted = []

    def fake_delete(uuid):
        deleted.append(uuid)
        db.snaps.pop(uuid)
        return True

    from simplyblock_core.controllers import snapshot_controller
    monkeypatch.setattr(snapshot_controller, "delete", fake_delete)
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: [])
    return db, deleted


def test_nothing_is_reaped_while_the_source_volume_exists(reaper):
    db, deleted = reaper
    assert rsr.reap_retired(db, CLUSTER) == 0
    assert deleted == []


def test_chain_drains_leaf_first_once_the_source_is_gone(reaper):
    db, deleted = reaper
    db.lvols.pop("SRC")
    passes = []
    while rsr.reap_retired(db, CLUSTER):
        passes.append(sorted(deleted))
        deleted.clear()
    # S1 and C1 each still carry a chained successor in the first pass.
    assert passes == [["C2", "S2"], ["C1", "S1"]]
    assert set(db.snaps) == {"U", "X"}


def test_a_clone_of_a_retired_snapshot_keeps_it(reaper):
    db, deleted = reaper
    db.lvols.pop("SRC")
    db.lvols["USERCLONE"] = _lvol("USERCLONE", cloned_from_snap="S2")
    rsr.reap_retired(db, CLUSTER)
    assert "S2" not in deleted and "S1" not in deleted


def test_spdk_clones_or_unknown_spdk_state_keep_it(reaper, monkeypatch):
    db, deleted = reaper
    db.lvols.pop("SRC")
    monkeypatch.setattr(rsr, "_spdk_clones",
                        lambda db, snap: None if snap.uuid == "S2" else ["LVS_1/x"])
    assert rsr.reap_retired(db, CLUSTER) == 0


def test_snapshots_of_other_clusters_are_left_to_their_own_pass(reaper):
    db, deleted = reaper
    db.lvols.pop("SRC")
    assert rsr.reap_retired(db, "CL_b") == 0


# --- the retired lineage lets go of its copies on the target cluster --------
#
# After a migration each copy on the target cluster is the base the migrated
# volume is cloned from, so it has a live clone as long as that volume lives.
# The delete path holds back a snapshot whose copy has a live clone (it would
# hand its delete on to that copy), and left paired the whole retired lineage
# stayed on the vacated cluster (bf6 2026-10-29: 112 retired snapshots, 0
# volumes, half of the cluster's allocation).

def _paired(snap, copy_id, other_cluster="CL_b"):
    snap.target_replicated_snap_uuid = copy_id
    snap.instances = [{"uuid": copy_id, "cluster_id": other_cluster},
                      {"uuid": snap.uuid + "_local", "cluster_id": CLUSTER}]


def test_retire_detaches_the_copies_on_the_other_cluster(leg):
    db, src = leg
    _paired(db.snaps["S2"], "T2")
    rsr.retire_source_lineage(db, src)
    s2 = db.snaps["S2"]
    assert s2.deleted
    assert s2.target_replicated_snap_uuid == ""
    assert s2.instances == [{"uuid": "S2_local", "cluster_id": CLUSTER}]


def test_a_retired_snapshot_is_no_longer_held_by_its_copys_clone(leg, monkeypatch):
    from simplyblock_core.controllers import snapshot_controller
    db, src = leg
    _paired(db.snaps["S2"], "T2")
    monkeypatch.setattr(snapshot_controller, "_has_live_clone", lambda sid: sid == "T2")
    assert snapshot_controller._replicated_copy_with_live_clone(db.snaps["S2"]) == "T2"
    rsr.retire_source_lineage(db, src)
    assert snapshot_controller._replicated_copy_with_live_clone(db.snaps["S2"]) == ""


def test_a_snapshot_that_is_not_retired_keeps_its_copy_and_the_hold(leg, monkeypatch):
    """A fail-over's source is not retired: it stays paired and held (KI-12)."""
    from simplyblock_core.controllers import snapshot_controller
    db, src = leg
    _paired(db.snaps["X"], "TX")
    monkeypatch.setattr(snapshot_controller, "_has_live_clone", lambda sid: sid == "TX")
    rsr.retire_source_lineage(db, src)
    assert db.snaps["X"].target_replicated_snap_uuid == "TX"
    assert snapshot_controller._replicated_copy_with_live_clone(db.snaps["X"]) == "TX"
