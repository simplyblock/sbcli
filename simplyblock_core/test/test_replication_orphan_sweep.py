"""Orphaned replication copies are swept to the reaper; live bases never are.

Bed bf25 (2026-11-16, 2 x 5 nodes, 8 x 10 GiB volumes under whole-volume
rewrite, cluster-scope online migration with deleteSource, alternating A->B
and B->A): after 8 rounds cluster B held 81 snapshots for 9 volumes, all of
them ``repl_internal_*`` landing copies whose volume no longer existed, and
cluster A 98. Each pinned up to its volume's data; both clusters filled up
and the clients got EIO.

Round 1, lvol 8a711e9c: the cutover clone was built early (10:50:54) on the
copy of 243055f1 (LVS_19/LVOL_43); the cadence snapshot b8d7400a, already in
flight, landed 24 s later as LVS_19/LVOL_47, chained onto LVOL_43 beside the
clone. Retention kept b8d7400a (newest pair), the --delete-source retire
detached it from its copy, and both copies were still allocated at 18:30.
LVOL_43 was retired on the next leg but never reaped: LVOL_47 sat on it.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import replication_snapshot_retire as rsr
from simplyblock_core.models.snapshot import SnapShot

A, B = "CL_a", "CL_b"
OLD = 1_000          # created_at far past the grace age


def _lvol(uuid, cloned_from_snap="", status="online"):
    return SimpleNamespace(uuid=uuid, get_id=lambda: uuid, node_id="N1", nodes=[],
                           lvs_name="LVS_1", cloned_from_snap=cloned_from_snap,
                           status=status)


def _snap(uuid, lvol, name, cluster, snap_type=SnapShot.TYPE_USER, source="",
          target="", prev="", created_at=OLD, group_id=""):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.snap_name = name
    s.snap_type = snap_type
    s.source_replicated_snap_uuid = source
    s.target_replicated_snap_uuid = target
    s.prev_snap_uuid = prev
    s.status = SnapShot.STATUS_ONLINE
    s.cluster_id = cluster
    s.snap_bdev = f"LVS_1/{uuid}"
    s.created_at = created_at
    s.group_id = group_id
    return s


class _FakeDB:
    def __init__(self, snaps, lvols):
        self.snaps = {s.uuid: s for s in snaps}
        self.lvols = {lv.uuid: lv for lv in lvols}

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
def harness(monkeypatch):
    """Records deletes, treats SPDK as agreeing with the records, and starts
    every test with the sweep due."""
    deleted: list = []
    holder: dict = {}

    def fake_delete(uuid):
        deleted.append(uuid)
        holder["db"].snaps.pop(uuid)
        return True

    from simplyblock_core.controllers import snapshot_controller
    monkeypatch.setattr(snapshot_controller, "delete", fake_delete)
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: [])
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())
    monkeypatch.setattr(rsr, "_sync_mini", lambda db, snap: None)
    getattr(rsr, "_LAST_SWEEP", {}).clear()

    def use(db):
        holder["db"] = db
        return db
    return use, deleted


def _drain(db, cluster):
    """Monitor passes, each with the sweep due, until nothing more is reaped."""
    while True:
        getattr(rsr, "_LAST_SWEEP", {}).clear()
        if not rsr.reap_retired(db, cluster):
            return


def _round1_on_b():
    """After the A->B leg of 8a711e9c, before its --delete-source retire.

    A: SRC (the migrating volume) with S_BASE (copy X, the clone's base) and
       S_NEW, the cadence snapshot that was in flight when the clone was built.
    B: X <- C_NEW (S_NEW's copy, chained onto X beside the clone), and VOL_B,
       the migrated volume, cloned from X."""
    src = _lvol("SRC")
    rep_x, rep_new = _lvol("REP_X"), _lvol("REP_NEW")
    vol_b = _lvol("VOL_B", cloned_from_snap="X")
    snaps = [
        _snap("S_BASE", src, "repl_internal_8a711e9c_1", A, SnapShot.TYPE_INTERNAL,
              target="X"),
        _snap("S_NEW", src, "repl_internal_8a711e9c_2", A, SnapShot.TYPE_INTERNAL,
              target="C_NEW", prev="S_BASE"),
        _snap("X", rep_x, "repl_internal_8a711e9c_1", B, source="S_BASE"),
        _snap("C_NEW", rep_new, "repl_internal_8a711e9c_2", B, source="S_NEW", prev="X"),
    ]
    return _FakeDB(snaps, [src, vol_b]), src, vol_b


def test_bed_bf25_copy_landed_beside_the_clone_is_deleted(harness):
    use, deleted = harness
    db, src, vol_b = _round1_on_b()
    use(db)
    # cutover, --delete-source: the source goes, its lineage is retired
    db.lvols.pop("SRC")
    rsr.retire_source_lineage(db, src)
    _drain(db, A)
    _drain(db, B)
    assert "C_NEW" not in db.snaps, "the copy that landed after the clone was built leaked"
    assert "S_BASE" not in db.snaps and "S_NEW" not in db.snaps
    # the migrated volume still reads through X
    assert "X" in db.snaps and not db.snaps["X"].deleted
    assert "X" not in deleted


def test_bed_bf25_clone_base_goes_once_its_volume_migrated_on(harness):
    """Next leg: VOL_B migrates back and is deleted; its retire marks X, which
    is a leaf now that the copy beside the clone is gone."""
    use, deleted = harness
    db, src, vol_b = _round1_on_b()
    use(db)
    db.lvols.pop("SRC")
    rsr.retire_source_lineage(db, src)
    _drain(db, B)
    db.lvols.pop("VOL_B")
    rsr.retire_source_lineage(db, vol_b)
    _drain(db, B)
    assert "X" not in db.snaps
    assert set(db.snaps) <= {"S_BASE", "S_NEW"}


def test_already_leaked_chain_drains_leaf_first(harness):
    """What bf25 left behind: X retired (deleted, online) under an unmarked
    copy; both clusters' source halves long gone."""
    use, deleted = harness
    rep_x, rep_new = _lvol("REP_X"), _lvol("REP_NEW")
    x = _snap("X", rep_x, "repl_internal_8a711e9c_1", B, source="S_BASE")
    x.deleted = True
    db = use(_FakeDB([x, _snap("C_NEW", rep_new, "repl_internal_8a711e9c_2", B,
                               source="S_NEW", prev="X")], []))
    passes = []
    getattr(rsr, "_LAST_SWEEP", {}).clear()
    while rsr.reap_retired(db, B):
        passes.append(list(deleted))
        deleted.clear()
    assert passes == [["C_NEW"], ["X"]]


# --- what the sweep must never hand over ------------------------------------

def _orphan(uuid="O", **kw):
    kw.setdefault("source", "GONE")
    return _snap(uuid, _lvol("REP_" + uuid), "repl_internal_aaaaaaaa_1", B, **kw)


def test_live_volumes_clone_base_and_the_chain_below_it_are_kept(harness):
    use, deleted = harness
    db = use(_FakeDB([_orphan("BELOW"), _orphan("BASE", prev="BELOW")],
                     [_lvol("VOL", cloned_from_snap="BASE")]))
    assert rsr.sweep_orphaned(db, B) == []
    _drain(db, B)
    assert deleted == []


def test_a_clone_being_deleted_still_keeps_its_base(harness):
    use, deleted = harness
    db = use(_FakeDB([_orphan("BASE")],
                     [_lvol("VOL", cloned_from_snap="BASE", status="in_deletion")]))
    assert rsr.sweep_orphaned(db, B) == []


def test_a_copy_still_paired_with_its_source_is_kept(harness):
    """A DR copy: its source on the other cluster still links to it."""
    use, deleted = harness
    src = _snap("S", _lvol("GONE_VOL"), "repl_internal_bbbbbbbb_1", A,
                SnapShot.TYPE_INTERNAL, target="COPY")
    db = use(_FakeDB([src, _orphan("COPY", source="S")], []))
    assert rsr.sweep_orphaned(db, B) == []
    # nor the source half, on its own cluster
    assert rsr.sweep_orphaned(db, A) == []


def test_a_copy_paired_by_instance_only_is_kept(harness):
    use, deleted = harness
    src = _snap("S", _lvol("GONE_VOL"), "repl_internal_bbbbbbbb_1", A,
                SnapShot.TYPE_INTERNAL)
    src.instances = [{"uuid": "COPY", "cluster_id": B}]
    db = use(_FakeDB([src, _orphan("COPY", source="")], []))
    assert rsr.sweep_orphaned(db, A) == []


def test_a_fail_over_hold_is_kept(harness):
    """KI-12: the failed-over volume is a clone of the copy; the source half
    stays paired with it and the copy is the clone's base."""
    use, deleted = harness
    src = _snap("S", _lvol("GONE_VOL"), "repl_internal_cccccccc_1", A,
                SnapShot.TYPE_INTERNAL, target="COPY")
    db = use(_FakeDB([src, _orphan("COPY", source="S")],
                     [_lvol("FAILED_OVER", cloned_from_snap="COPY")]))
    assert rsr.sweep_orphaned(db, A) == [] and rsr.sweep_orphaned(db, B) == []


def test_snapshots_of_a_live_volume_are_left_to_retention(harness):
    use, deleted = harness
    vol = _lvol("VOL")
    db = use(_FakeDB([_snap("S", vol, "repl_internal_dddddddd_1", B,
                            SnapShot.TYPE_INTERNAL)], [vol]))
    assert rsr.sweep_orphaned(db, B) == []


def test_a_fresh_copy_is_left_alone(harness):
    use, deleted = harness
    import time
    db = use(_FakeDB([_orphan(created_at=int(time.time()) - 5)], []))
    assert rsr.sweep_orphaned(db, B) == []


def test_user_and_foreign_cluster_snapshots_are_never_swept(harness):
    use, deleted = harness
    user = _snap("U", _lvol("GONE_VOL"), "nightly", B, source="GONE")
    db = use(_FakeDB([user, _orphan()], []))
    assert rsr.sweep_orphaned(db, A) == []
    assert rsr.sweep_orphaned(db, B) == ["O"]
    assert not db.snaps["U"].deleted


def test_group_recovery_point_is_never_swept(harness, monkeypatch):
    use, deleted = harness
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: {"O"})
    db = use(_FakeDB([_orphan(group_id="G")], []))
    assert rsr.sweep_orphaned(db, B) == []


def test_a_snapshot_spdk_still_has_clones_of_is_neither_marked_nor_deleted(harness,
                                                                          monkeypatch):
    """The records can be wrong (a landing volume being chained onto it, a
    successor whose link was never written): the data plane is asked first."""
    use, deleted = harness
    db = use(_FakeDB([_orphan()], []))
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: ["LVS_1/child"])
    _drain(db, B)
    assert not db.snaps["O"].deleted and deleted == []
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: None)    # leader unknown
    _drain(db, B)
    assert not db.snaps["O"].deleted and deleted == []
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: [])      # a leaf now
    _drain(db, B)
    assert deleted == ["O"]


def test_a_chain_of_orphans_is_marked_one_layer_per_sweep(harness, monkeypatch):
    use, deleted = harness
    db = use(_FakeDB([_orphan("LOW"), _orphan("TOP", prev="LOW")], []))
    monkeypatch.setattr(rsr, "_spdk_clones",
                        lambda db, snap: ["LVS_1/TOP"] if snap.uuid == "LOW" and
                        "TOP" in db.snaps else [])
    assert rsr.sweep_orphaned(db, B) == ["TOP"]
    _drain(db, B)
    assert deleted == ["TOP", "LOW"]


def test_the_sweep_is_throttled_per_cluster(harness, monkeypatch):
    use, deleted = harness
    db = use(_FakeDB([], []))
    calls = []

    def _sweep(db, cluster_id, now=None):
        calls.append(cluster_id)
        return []
    monkeypatch.setattr(rsr, "sweep_orphaned", _sweep)
    rsr.reap_retired(db, B)
    rsr.reap_retired(db, B)
    rsr.reap_retired(db, A)
    assert calls == [B, A]


def _moved_on_pair(live_volume_reads=""):
    """Bed bf26, 2026-11-17: volume b062cac2 migrated A->B->A...; it keeps its
    id. Its old landing copy on B (a485d9a8) was re-sent to A as a chain
    ancestor (6c1ff88b) when it migrated back. Both landing volumes are gone;
    each half links to the other."""
    live = _lvol("b062cac2-live", cloned_from_snap=live_volume_reads)
    on_b = _snap("a485d9a8", _lvol("REP_e38b166f"), "repl_internal_b062cac2_1794912311", B,
                 target="6c1ff88b")
    on_b.instances = [{"uuid": "6c1ff88b", "cluster_id": A}]
    on_a = _snap("6c1ff88b", _lvol("REP_baaa72c7"), "repl_internal_b062cac2_1794912311", A,
                 source="a485d9a8")
    return _FakeDB([on_a, on_b], [live])


def test_bed_bf26_both_halves_of_a_moved_on_pair_are_swept(harness):
    use, deleted = harness
    db = use(_moved_on_pair())
    assert rsr.sweep_orphaned(db, A) == ["6c1ff88b"]
    getattr(rsr, "_LAST_SWEEP", {}).clear()
    assert rsr.sweep_orphaned(db, B) == ["a485d9a8"]
    _drain(db, A)
    _drain(db, B)
    assert sorted(deleted) == ["6c1ff88b", "a485d9a8"]


def test_a_moved_on_pair_half_the_volume_reads_through_keeps_the_other(harness):
    """The live volume was cloned from the half on A: that half is its base,
    and the half on B stays paired with it."""
    use, deleted = harness
    db = use(_moved_on_pair(live_volume_reads="6c1ff88b"))
    assert rsr.sweep_orphaned(db, A) == []
    getattr(rsr, "_LAST_SWEEP", {}).clear()
    assert rsr.sweep_orphaned(db, B) == []


def test_a_pair_whose_volume_is_gone_altogether_is_kept(harness):
    """A deleted DR source: no live volume carries the name; the pair is
    retention's business, not the sweep's."""
    use, deleted = harness
    db = _moved_on_pair()
    db.lvols = {}
    use(db)
    assert rsr.sweep_orphaned(db, A) == []
    getattr(rsr, "_LAST_SWEEP", {}).clear()
    assert rsr.sweep_orphaned(db, B) == []


def test_a_namesake_being_deleted_does_not_count_as_moved_on(harness):
    use, deleted = harness
    db = _moved_on_pair()
    for lv in db.lvols.values():
        lv.status = "in_deletion"
    use(db)
    assert rsr.sweep_orphaned(db, A) == [] and rsr.sweep_orphaned(db, B) == []
