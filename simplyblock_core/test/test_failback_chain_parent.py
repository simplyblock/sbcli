"""Fail-back: the first delta must chain onto the fail-over point.

Fail-over clones the volume from snapshot n(1) on the target. When we later
fail back, only the delta the target added is transferred — but on arrival it
must be chained onto the ORIGINAL source's copy of n(1), or it is a standalone
blob holding just the delta (its own clusters plus zeros), the same failure as
an unchained forward replication.

The first fail-back snapshot has no older SIBLING, so sibling-only lookup
returned None and skipped chaining. A clone's chain parent is the snapshot it
was cloned from.
"""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_replication as sr


class _LVol:
    def __init__(self, uuid, node_id="N1", cloned_from="", lvs="LVS_1"):
        self.uuid = uuid
        self.node_id = node_id
        self.cloned_from_snap = cloned_from
        self.lvs_name = lvs

    def get_id(self):
        return self.uuid


def _snap(uuid, lvol, created_at=100, source_repl="", target_repl="",
          status=SnapShot.STATUS_ONLINE):
    s = SnapShot()
    s.uuid = uuid
    s.lvol = lvol
    s.created_at = created_at
    s.snap_ref_id = ""
    s.status = status
    s.source_replicated_snap_uuid = source_repl
    s.target_replicated_snap_uuid = target_repl
    s.snap_bdev = f"LVS_1/{uuid}"
    return s


class _DB:
    def __init__(self, snaps, lvols=()):
        self._snaps = {s.uuid: s for s in snaps}
        self._lvols = {lv.uuid: lv for lv in lvols}

    def get_snapshots_by_node_id(self, node_id):
        return [s for s in self._snaps.values() if s.lvol.node_id == node_id]

    def get_snapshot_by_id(self, uuid):
        if uuid in self._snaps:
            return self._snaps[uuid]
        raise KeyError(uuid)

    def get_lvol_by_id(self, uuid):
        if uuid in self._lvols:
            return self._lvols[uuid]
        raise KeyError(uuid)


def test_first_failback_delta_chains_onto_the_failover_point(monkeypatch):
    # On the target: N1_COPY is the replicated copy of the source's n(1); the
    # failed-over volume CLONE was created from it; DELTA is its first snapshot.
    clone = _LVol("CLONE", cloned_from="N1_COPY")
    n1_copy = _snap("N1_COPY", _LVol("ORIG_VOL"), created_at=50,
                    source_repl="N1_ON_SOURCE")
    delta = _snap("DELTA", clone, created_at=200)

    monkeypatch.setattr(sr, "db", _DB([n1_copy, delta], [clone]))

    prev = sr._previous_replicated_snapshot(delta, replicate_to_source=True)
    assert prev is not None and prev.get_id() == "N1_COPY", (
        "first fail-back delta would land unchained -> reads delta + zeros")


def test_later_failback_snapshots_still_use_the_sibling(monkeypatch):
    clone = _LVol("CLONE", cloned_from="N1_COPY")
    n1_copy = _snap("N1_COPY", _LVol("ORIG_VOL"), created_at=50)
    n1_copy.source_replicated_snap_uuid = "N1_ON_SOURCE"
    first = _snap("DELTA1", clone, created_at=200, source_repl="DELTA1_ON_SOURCE")
    second = _snap("DELTA2", clone, created_at=300)

    monkeypatch.setattr(sr, "db", _DB([n1_copy, first, second], [clone]))

    prev = sr._previous_replicated_snapshot(second, replicate_to_source=True)
    assert prev.get_id() == "DELTA1", "must chain onto the newest replicated sibling"


def test_clone_parent_without_a_remote_copy_is_not_used(monkeypatch):
    """No counterpart on the other side -> nothing to chain to; caller decides."""
    clone = _LVol("CLONE", cloned_from="LOCAL_ONLY")
    local_only = _snap("LOCAL_ONLY", _LVol("ORIG_VOL"), created_at=50)
    delta = _snap("DELTA", clone, created_at=200)

    monkeypatch.setattr(sr, "db", _DB([local_only, delta], [clone]))
    assert sr._previous_replicated_snapshot(delta, replicate_to_source=True) is None


def test_plain_volume_first_snapshot_has_no_parent(monkeypatch):
    plain = _LVol("PLAIN")
    first = _snap("S1", plain, created_at=100)
    monkeypatch.setattr(sr, "db", _DB([first], [plain]))
    assert sr._previous_replicated_snapshot(first, replicate_to_source=False) is None


def test_forward_direction_also_uses_the_clone_parent(monkeypatch):
    """A clone replicated forward has the same first-snapshot shape."""
    clone = _LVol("CLONE", cloned_from="BASE")
    base = _snap("BASE", _LVol("ORIG_VOL"), created_at=50, target_repl="BASE_ON_TGT")
    delta = _snap("DELTA", clone, created_at=200)

    monkeypatch.setattr(sr, "db", _DB([base, delta], [clone]))
    prev = sr._previous_replicated_snapshot(delta, replicate_to_source=False)
    assert prev is not None and prev.get_id() == "BASE"
