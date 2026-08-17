"""Replication backlog = the volume's whole blob chain, oldest first.

A volume's data is its own clusters plus everything inherited from the
snapshots below it; a volume sitting on another volume's snapshot is the same
structure, not a special case. Queueing only snapshots recorded against the
volume's own uuid is complete for a volume that owns its chain (plain
migration, case 1) but skips the ancestors of a failed-over volume — so a
destination that does not already hold them receives the upper deltas with
holes underneath.
"""
from simplyblock_core.controllers import lvol_controller


class _LVol:
    def __init__(self, uuid, cloned_from=""):
        self.uuid = uuid
        self.cloned_from_snap = cloned_from

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid, lvol, created_at):
        self.uuid = uuid
        self.lvol = lvol
        self.created_at = created_at

    def get_id(self):
        return self.uuid


class _DB:
    def __init__(self, snaps):
        self._snaps = {s.uuid: s for s in snaps}

    def get_snapshots(self):
        return list(self._snaps.values())

    def get_snapshot_by_id(self, uuid):
        if uuid in self._snaps:
            return self._snaps[uuid]
        raise KeyError(uuid)


def test_plain_volume_backlog_is_its_own_snapshots():
    vol = _LVol("VOL")
    snaps = [_Snap("S1", vol, 100), _Snap("S2", vol, 200)]
    out = lvol_controller.replication_backlog(_DB(snaps), vol)
    assert [s.get_id() for s in out] == ["S1", "S2"], "oldest first, all of ours"


def test_failed_over_volume_includes_its_ancestors():
    """The regression: N1 (the branch point) must be replicated too."""
    orig = _LVol("ORIG")
    n1 = _Snap("N1", orig, 100)
    clone = _LVol("CLONE", cloned_from="N1")
    d1 = _Snap("D1", clone, 300)
    out = lvol_controller.replication_backlog(_DB([n1, d1]), clone)
    assert [s.get_id() for s in out] == ["N1", "D1"], (
        "without N1 the destination gets D1's clusters and holes underneath")


def test_snapshots_taken_after_the_branch_point_are_excluded():
    """Data written on the ancestor AFTER we branched is not ours."""
    orig = _LVol("ORIG")
    n0 = _Snap("N0", orig, 50)
    n1 = _Snap("N1", orig, 100)
    n2 = _Snap("N2", orig, 150)      # taken after CLONE branched off N1
    clone = _LVol("CLONE", cloned_from="N1")
    d1 = _Snap("D1", clone, 300)
    out = lvol_controller.replication_backlog(_DB([n0, n1, n2, d1]), clone)
    assert [s.get_id() for s in out] == ["N0", "N1", "D1"]
    assert "N2" not in [s.get_id() for s in out]


def test_multi_level_chain_is_walked_to_the_bottom():
    base = _LVol("BASE")
    b1 = _Snap("B1", base, 10)
    mid = _LVol("MID", cloned_from="B1")
    m1 = _Snap("M1", mid, 100)
    top = _LVol("TOP", cloned_from="M1")
    t1 = _Snap("T1", top, 200)
    out = lvol_controller.replication_backlog(_DB([b1, m1, t1]), top)
    assert [s.get_id() for s in out] == ["B1", "M1", "T1"]


def test_missing_ancestor_does_not_crash():
    clone = _LVol("CLONE", cloned_from="GONE")
    d1 = _Snap("D1", clone, 300)
    out = lvol_controller.replication_backlog(_DB([d1]), clone)
    assert [s.get_id() for s in out] == ["D1"]


def test_cycle_terminates():
    a = _LVol("A", cloned_from="SB")
    b = _LVol("B", cloned_from="SA")
    sa = _Snap("SA", a, 100)
    sb = _Snap("SB", b, 90)
    out = lvol_controller.replication_backlog(_DB([sa, sb]), a)  # must not hang
    assert out
