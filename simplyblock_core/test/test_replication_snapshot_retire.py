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


def test_retiring_a_landing_copy_unlinks_its_source_on_the_other_cluster(leg):
    """The source of a retired landing copy lives on in the other cluster's
    chain; still pointing at the deleted copy, it made the next return trip
    refuse to finalize (bf6 round 4: 56 tasks gave up)."""
    db, src = leg
    origin = _snap("B2", _lvol("BACK"), "repl_commit_back")
    origin.cluster_id = "CL_b"
    origin.target_replicated_snap_uuid = "C2"
    origin.instances = [{"uuid": "C2", "cluster_id": CLUSTER},
                        {"uuid": "B2_mirror", "cluster_id": "CL_b"}]
    db.snaps["B2"] = origin
    rsr.retire_source_lineage(db, src)
    assert origin.target_replicated_snap_uuid == ""
    assert origin.instances == [{"uuid": "B2_mirror", "cluster_id": "CL_b"}]
    assert not origin.deleted


def test_a_source_that_points_elsewhere_is_left_alone(leg):
    db, src = leg
    origin = _snap("B2", _lvol("BACK"), "repl_commit_back")
    origin.target_replicated_snap_uuid = "SOMETHING_ELSE"
    db.snaps["B2"] = origin
    rsr.retire_source_lineage(db, src)
    assert origin.target_replicated_snap_uuid == "SOMETHING_ELSE"


# --- orphaned copies (bed bf21 2026-11-10) -----------------------------------
# Landing copies beside the lineage path (newer than the clone base, on an
# abandoned branch) were never retired; three legs filled cluster B to 95%.

NOW = 10000


def _orphan_world(monkeypatch):
    """LIVE is cloned from copy BASE (chained onto ANC). Beside it: NEWER (a
    copy newer than the base), OLD (an old leaf), PAIRED (a copy whose source LS
    belongs to the live volume LIVESRC), YOUNG (an orphan too young to judge),
    USER (a user snapshot). GONE's record no longer exists."""
    gone = _lvol("GONE")
    live = _lvol("LIVE", cloned_from_snap="BASE")
    live_src = _lvol("LIVESRC")
    snaps = [
        _snap("ANC", gone, "repl_internal_v_1", SnapShot.TYPE_USER, source="S0"),
        _snap("BASE", gone, "repl_internal_v_2", SnapShot.TYPE_USER, source="S1", prev="ANC"),
        _snap("NEWER", gone, "repl_internal_v_3", SnapShot.TYPE_USER, source="S2"),
        _snap("OLD", gone, "repl_internal_w_1", SnapShot.TYPE_USER, source="S3"),
        _snap("PAIRED", gone, "repl_internal_x_1", SnapShot.TYPE_USER, source="LS"),
        _snap("LS", live_src, "repl_internal_x_1"),
        _snap("YOUNG", gone, "repl_internal_y_1", SnapShot.TYPE_USER, source="S4"),
        _snap("USER", gone, "nightly", SnapShot.TYPE_USER),
    ]
    for s in snaps:
        s.created_at = 1000
    snaps[6].created_at = NOW - 60
    db = _FakeDB(snaps, [live, live_src])
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())
    monkeypatch.setattr(rsr, "_sync_mini", lambda db, snap: db.minis_synced.append(snap.uuid))
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db, snap: [])
    return db


def test_sweep_retires_copies_beside_the_lineage(monkeypatch):
    db = _orphan_world(monkeypatch)
    assert set(rsr.retire_orphan_copies(db, CLUSTER, now=NOW)) == {"NEWER", "OLD"}
    assert db.snaps["NEWER"].deleted and db.snaps["OLD"].deleted
    assert set(db.minis_synced) == {"NEWER", "OLD"}


def test_sweep_keeps_live_chain_live_pairs_young_copies_and_user_snapshots(monkeypatch):
    db = _orphan_world(monkeypatch)
    rsr.retire_orphan_copies(db, CLUSTER, now=NOW)
    for kept in ("ANC", "BASE", "PAIRED", "LS", "YOUNG", "USER"):
        assert not db.snaps[kept].deleted, kept


def test_sweep_keeps_copies_spdk_still_clones_or_cannot_read(monkeypatch):
    db = _orphan_world(monkeypatch)
    monkeypatch.setattr(rsr, "_spdk_clones",
                        lambda db, snap: ["LVOL_9"] if snap.uuid == "NEWER" else None)
    assert rsr.retire_orphan_copies(db, CLUSTER, now=NOW) == []


def test_sweep_leaves_other_clusters_and_recovery_points(monkeypatch):
    db = _orphan_world(monkeypatch)
    db.snaps["OLD"].cluster_id = "CL_b"
    db.snaps["NEWER"].group_id = "G1"
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: {"NEWER"})
    assert rsr.retire_orphan_copies(db, CLUSTER, now=NOW) == []


def test_swept_copies_are_then_reaped(monkeypatch):
    db = _orphan_world(monkeypatch)
    rsr.retire_orphan_copies(db, CLUSTER, now=NOW)
    deleted = []

    def fake_delete(uuid):
        deleted.append(uuid)
        db.snaps.pop(uuid)
        return True
    from simplyblock_core.controllers import snapshot_controller
    monkeypatch.setattr(snapshot_controller, "delete", fake_delete)
    rsr.reap_retired(db, CLUSTER)
    assert set(deleted) == {"NEWER", "OLD"}


def test_a_requeued_endgame_round_copy_is_a_replication_snapshot():
    copy = _snap("R", _lvol("REP"), "repl_round_0b7c", SnapShot.TYPE_USER, source="B9")
    assert rsr.is_replication_snapshot(copy)
    user = _snap("U2", _lvol("V"), "repl_round_by_hand", SnapShot.TYPE_USER)
    assert not rsr.is_replication_snapshot(user)


# --- migrated chains are collapsed (bed bf21 2026-11-10) ---------------------
# A migrated volume is a clone of its newest landing copy, which sat on every
# older copy and on the chain ancestors earlier legs sent along: depth up to 7
# after five rounds, 93-99 % device use. The ancestors below the clone base are
# merged away one per pass; the base itself, user snapshots and anything a
# transfer or a live volume's retention still uses stay.

def _chain_world(monkeypatch):
    """MIG (live) is cloned from BASE <- X1 <- X2 <- U (user) <- X3, all landing
    copies of a gone volume except U; BASE's source S_BASE is gone too."""
    gone = _lvol("GONE")
    mig = _lvol("MIG", cloned_from_snap="BASE")
    snaps = [
        _snap("X3", gone, "repl_internal_m_0", SnapShot.TYPE_USER, source="S3"),
        _snap("U", gone, "nightly", SnapShot.TYPE_USER, prev="X3"),
        _snap("X2", gone, "repl_internal_m_1", SnapShot.TYPE_USER, source="S2", prev="U"),
        _snap("X1", gone, "repl_commit_m", SnapShot.TYPE_USER, source="S1", prev="X2"),
        _snap("BASE", gone, "repl_commit_m2", SnapShot.TYPE_USER, source="S_BASE", prev="X1"),
    ]
    db = _FakeDB(snaps, [mig])
    db.tasks = []
    db.get_job_tasks_by_function = lambda cluster_id, fn: db.tasks
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: set())
    monkeypatch.setattr(rsr, "_sync_mini", lambda db, snap: db.minis_synced.append(snap.uuid))

    def spdk_clones(db_, snap):
        kids = [s for s in db.snaps.values() if s.prev_snap_uuid == snap.uuid]
        return [k.snap_bdev for k in kids]
    monkeypatch.setattr(rsr, "_spdk_clones", spdk_clones)
    deleted: list = []

    def fake_delete(uuid):
        """The delete swap-merges *uuid* into its child; the monitor relinks."""
        deleted.append(uuid)
        gone_snap = db.snaps.pop(uuid)
        for s in db.snaps.values():
            if s.prev_snap_uuid == uuid:
                s.prev_snap_uuid = gone_snap.prev_snap_uuid
        return True
    from simplyblock_core.controllers import snapshot_controller
    monkeypatch.setattr(snapshot_controller, "delete", fake_delete)
    return db, deleted


def test_collapse_merges_ancestors_one_per_pass_down_to_a_user_snapshot(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    passes = []
    while rsr.collapse_migrated_chains(db, CLUSTER):
        passes.append(list(deleted))
        deleted.clear()
    assert passes == [["X1"], ["X2"]]
    # the base stays (the delta base of a migration back), so do the user
    # snapshot and everything below it
    assert set(db.snaps) == {"BASE", "U", "X3"}
    assert db.snaps["BASE"].prev_snap_uuid == "U"


def test_collapse_detaches_the_victim_from_its_source(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    src = _snap("S1", _lvol("OTHER_GONE"), "repl_commit_m")
    src.cluster_id = "CL_b"
    src.target_replicated_snap_uuid = "X1"
    db.snaps["S1"] = src
    rsr.collapse_migrated_chains(db, CLUSTER)
    assert deleted == ["X1"]
    assert src.target_replicated_snap_uuid == ""


def test_collapse_waits_for_a_delete_in_flight_or_a_transfer(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    db.snaps["X2"].status = SnapShot.STATUS_IN_DELETION
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    db.snaps["X2"].status = SnapShot.STATUS_ONLINE
    db.snaps["BASE"].status = SnapShot.STATUS_IN_REPLICATION
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    db.snaps["BASE"].status = SnapShot.STATUS_ONLINE
    from simplyblock_core.models.job_schedule import JobSchedule
    t = JobSchedule()
    t.function_params = {"snapshot_id": "X1"}
    t.status = JobSchedule.STATUS_NEW
    db.tasks = [t]
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    t.status = JobSchedule.STATUS_DONE
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 1
    assert deleted == ["X1"]


def test_collapse_never_touches_a_pair_of_a_live_volume(monkeypatch):
    """Until the migrated-away source is gone its pairs are its retention's."""
    db, deleted = _chain_world(monkeypatch)
    src_vol = _lvol("SRCVOL")
    db.lvols["SRCVOL"] = src_vol
    db.snaps["S_BASE"] = _snap("S_BASE", src_vol, "repl_commit_m2")
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    assert deleted == []


def test_collapse_skips_ancestors_with_other_children_clones_or_recovery_points(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    # X1 has a second child in the DB: deleting it would not merge into BASE alone
    db.snaps["SIDE"] = _snap("SIDE", _lvol("GONE"), "repl_internal_side", SnapShot.TYPE_USER,
                             source="S9", prev="X1")
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 1
    assert deleted == ["X2"]            # X1 skipped, the next one merges into X1
    deleted.clear()
    db.snaps.pop("SIDE")
    db.lvols["CLONE"] = _lvol("CLONE", cloned_from_snap="X1")
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    db.lvols.pop("CLONE")
    db.snaps["X1"].group_id = "G1"
    monkeypatch.setattr(rsr.replication_recovery_points, "group_recovery_point_ids",
                        lambda gids, db=None: {"X1"})
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    assert deleted == []


def test_collapse_trusts_spdk_over_the_db_link(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db_, snap: ["LVS_1/SOMETHING_ELSE"])
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    monkeypatch.setattr(rsr, "_spdk_clones", lambda db_, snap: None)
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    assert deleted == []


def test_collapse_leaves_volumes_not_cloned_from_a_replication_copy(monkeypatch):
    db, deleted = _chain_world(monkeypatch)
    db.snaps["BASE"].snap_name = "golden-image"
    db.snaps["BASE"].source_replicated_snap_uuid = ""
    assert rsr.collapse_migrated_chains(db, CLUSTER) == 0
    assert rsr.collapse_migrated_chains(db, "CL_b") == 0
