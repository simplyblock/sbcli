"""Chain-scoped mutual exclusion.

A delete inside a blob chain swap-merges the snapshot's segments into its
neighbour and re-links parents, so ANY concurrent operation elsewhere in the
same chain (create, clone, delete) mutates the structure the first one is
walking. Exclusion must therefore be per LVS+chain, not per object. Distinct
chains share no blob links and must stay fully parallel.
"""
import pytest

from simplyblock_core.controllers import snapshot_controller as sc


class _LVol:
    def __init__(self, uuid, lvs="LVS_1", cloned_from=""):
        self.uuid = uuid
        self.lvs_name = lvs
        self.cloned_from_snap = cloned_from

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid, lvol):
        self.uuid = uuid
        self.lvol = lvol

    def get_id(self):
        return self.uuid


class _DB:
    def __init__(self, lvols, snaps):
        self._lvols = {lv.uuid: lv for lv in lvols}
        self._snaps = {s.uuid: s for s in snaps}

    def get_lvol_by_id(self, uuid):
        if uuid in self._lvols:
            return self._lvols[uuid]
        raise KeyError(uuid)

    def get_snapshot_by_id(self, uuid):
        if uuid in self._snaps:
            return self._snaps[uuid]
        raise KeyError(uuid)


def _chain_db():
    #   BASE (lvol)
    #     +-- SNAP_A            (snapshot of BASE)
    #           +-- CLONE_1     (clone of SNAP_A)
    #                 +-- SNAP_B  (snapshot of CLONE_1)
    #                       +-- CLONE_2
    #   OTHER (unrelated lvol) -- its own chain
    base = _LVol("BASE")
    clone1 = _LVol("CLONE_1", cloned_from="SNAP_A")
    clone2 = _LVol("CLONE_2", cloned_from="SNAP_B")
    other = _LVol("OTHER")
    snap_a = _Snap("SNAP_A", base)
    snap_b = _Snap("SNAP_B", clone1)
    return _DB([base, clone1, clone2, other], [snap_a, snap_b])


@pytest.mark.parametrize("member", ["BASE", "SNAP_A", "CLONE_1", "SNAP_B", "CLONE_2"])
def test_every_chain_member_resolves_to_the_same_root(monkeypatch, member):
    monkeypatch.setattr(sc, "db_controller", _chain_db())
    root, lvs = sc.resolve_chain_root(member)
    assert root == "BASE"
    assert lvs == "LVS_1"


def test_unrelated_volume_is_its_own_chain(monkeypatch):
    monkeypatch.setattr(sc, "db_controller", _chain_db())
    assert sc.resolve_chain_root("OTHER")[0] == "OTHER"


def test_unknown_uuid_never_aliases_another_chain(monkeypatch):
    monkeypatch.setattr(sc, "db_controller", _chain_db())
    assert sc.resolve_chain_root("GHOST")[0] == "GHOST"


def test_broken_parent_link_stops_the_walk(monkeypatch):
    orphan = _LVol("ORPHAN", cloned_from="SNAP_GONE")
    monkeypatch.setattr(sc, "db_controller", _DB([orphan], []))
    assert sc.resolve_chain_root("ORPHAN")[0] == "SNAP_GONE"


def test_cyclic_links_terminate(monkeypatch):
    a = _LVol("A", cloned_from="SB")
    b = _LVol("B", cloned_from="SA")
    monkeypatch.setattr(sc, "db_controller",
                        _DB([a, b], [_Snap("SA", a), _Snap("SB", b)]))
    root, _lvs = sc.resolve_chain_root("A")  # must not hang
    assert root in {"A", "B", "SA", "SB"}


def test_lock_key_is_shared_across_a_chain_and_distinct_between_chains(monkeypatch):
    """Members of one chain must take the SAME key (they exclude each other);
    members of different chains must take different keys (they run parallel)."""
    monkeypatch.setattr(sc, "db_controller", _chain_db())
    keys = []

    def _acquire(_db, _cluster, key, _owner, **kw):
        keys.append(key)
        return True

    class _FakeDBLock:
        def release_lvstore_lock(self, *a, **kw):
            pass

        def get_lvol_by_id(self, uuid):
            return _chain_db().get_lvol_by_id(uuid)

        def get_snapshot_by_id(self, uuid):
            return _chain_db().get_snapshot_by_id(uuid)

    monkeypatch.setattr(sc, "_acquire_lvstore_lock_blocking", _acquire)
    monkeypatch.setattr(sc, "_lvstore_lock_heartbeat", lambda *a, **kw: None)
    monkeypatch.setattr(sc, "db_controller", _FakeDBLock())

    for member in ("CLONE_2", "SNAP_A", "BASE"):
        with sc.object_mutation_lock("C1", member):
            pass
    with sc.object_mutation_lock("C1", "OTHER"):
        pass

    assert keys[0] == keys[1] == keys[2], "chain members must share one lock key"
    assert keys[3] != keys[0], "an unrelated chain must not block on it"
    assert keys[0].endswith("LVS_1:BASE")
