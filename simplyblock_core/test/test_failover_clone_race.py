"""The fail-over clone must be atomic against replication retention.

Lab runs 2026-08-10 and 2026-08-11: fail-over reported success on all five
volumes (state=failed_over, valid connection strings, lvols online) yet every
device read as ALL ZEROS with no filesystem, and every clone parent was
hard-deleted afterwards.

Mechanism: _prune_internal_snapshots deletes each replicated internal snapshot
older than the newest, and snapshot_controller._delete_locked only spares a
snapshot that ALREADY has a clone. Between "pick the last replicated snapshot"
and "create the clone on it" there is no clone yet, so retention hard-deletes
the parent and the clone is built on nothing. snapshot_controller.delete()
documents the invariant it relies on: a concurrent clone-create "holds the same
lock for its whole sequence". The fail-over clone path did not.
"""
from typing import Any

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.snapshot import SnapShot

CLUSTER = "CL_tgt"
LVOL_ID = "LV1"


class _Lvol:
    def __init__(self, uuid=LVOL_ID):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


def _snap(uuid, created_at, status=SnapShot.STATUS_ONLINE):
    s = SnapShot()
    s.uuid = uuid
    s.created_at = created_at
    s.status = status
    s.cluster_id = CLUSTER
    s.snap_bdev = f"lvs/{uuid}"
    return s


class _FakeDB:
    def __init__(self, order):
        self._order = list(order)
        self._snaps = {sid: _snap(sid, i) for i, sid in enumerate(order)}

    def get_snapshot_by_id(self, sid):
        if sid not in self._snaps:
            raise KeyError(f"Snapshot {sid} not found")
        return self._snaps[sid]

    def drop(self, sid):
        """Simulate retention hard-deleting a snapshot."""
        self._snaps.pop(sid, None)


@pytest.fixture
def harness(monkeypatch):
    state: dict[str, Any] = {
        "locks": [], "cloned_from": [], "selected": [], "prune_on_lock": None}

    class _Lock:
        def __init__(self, cluster_id, uuid, enabled=True):
            self.uuid = uuid

        def __enter__(self):
            state["locks"].append(self.uuid)
            # A retention pass that fires while the lock is held must not be
            # able to delete this snapshot -- but if it sneaks in BEFORE the
            # lock (the bug), the re-read inside must catch it.
            if state["prune_on_lock"]:
                state["prune_on_lock"]()
                state["prune_on_lock"] = None
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(lvol_controller.snapshot_controller,
                        "object_mutation_lock", _Lock)

    def _fake_select(db, lvol_id, cluster_id, generation=0):
        """Stand-in for _last_replicated_target_snapshot.

        Mirrors the real selector: newest first, skipping anything missing or
        already in deletion (see test_failover_snapshot_selection.py).
        """
        state["selected"].append(db._order[-1] if db._order else None)
        for sid in reversed(db._order):
            snap = db._snaps.get(sid)
            if snap is not None and snap.status != SnapShot.STATUS_IN_DELETION:
                return snap
        return None

    monkeypatch.setattr(lvol_controller, "_last_replicated_target_snapshot", _fake_select)

    def _fake_clone(db, lvol, target_node, pool_uuid, snapshot, for_migration=False):
        state["cloned_from"].append(snapshot.get_id())
        return object(), None

    monkeypatch.setattr(lvol_controller, "_create_target_lvol_clone", _fake_clone)
    return state


def test_clone_takes_the_snapshot_lock(harness):
    db = _FakeDB(["S_old", "S_new"])
    new_lvol, snap, err = lvol_controller._clone_from_last_replicated(
        db, LVOL_ID, _Lvol(), object(), "pool", CLUSTER)
    assert err is None and new_lvol is not None
    assert harness["cloned_from"] == ["S_new"]
    # The clone must have been serialised against snapshot deletion.
    assert harness["locks"] == ["S_new"], "clone-create did not hold the snapshot lock"


def test_falls_back_when_retention_deletes_the_chosen_snapshot(harness):
    """Retention removes the picked snapshot before the lock is acquired."""
    db = _FakeDB(["S_old", "S_new"])
    harness["prune_on_lock"] = lambda: db.drop("S_new")

    new_lvol, snap, err = lvol_controller._clone_from_last_replicated(
        db, LVOL_ID, _Lvol(), object(), "pool", CLUSTER)

    assert err is None and new_lvol is not None
    # It must NOT have cloned from the deleted S_new; it re-selects S_old.
    assert harness["cloned_from"] == ["S_old"], (
        "cloned from a snapshot that retention had already deleted — "
        "this is the all-zeros fail-over")
    assert snap.get_id() == "S_old"


def test_skips_snapshot_that_entered_deletion(harness):
    db = _FakeDB(["S_old", "S_new"])

    def _mark():
        db._snaps["S_new"].status = SnapShot.STATUS_IN_DELETION

    harness["prune_on_lock"] = _mark
    new_lvol, snap, err = lvol_controller._clone_from_last_replicated(
        db, LVOL_ID, _Lvol(), object(), "pool", CLUSTER)
    assert err is None
    assert harness["cloned_from"] == ["S_old"]


def test_gives_up_rather_than_cloning_nothing(harness):
    """No usable snapshot at all -> refuse, never return a parentless clone."""
    db = _FakeDB([])
    new_lvol, snap, err = lvol_controller._clone_from_last_replicated(
        db, LVOL_ID, _Lvol(), object(), "pool", CLUSTER)
    assert new_lvol is None and snap is None
    assert err and "replicated snapshot" in err.lower()
    assert harness["cloned_from"] == []
