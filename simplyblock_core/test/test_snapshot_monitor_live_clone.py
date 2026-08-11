"""A live clone must block the snapshot monitor's hard-delete.

Root cause of the all-zeros cross-cluster fail-over (labs 2026-08-10 and
2026-08-11). process_snap_delete only treated an IN_DELETION clone as a blocker,
so a HEALTHY clone did not stop the delete: the fail-over volume's parent
snapshot was removed ~40 minutes after the volume was created, every read then
returned zeros (no filesystem, md5 mismatch) while every status field still
reported success.

snapshot_controller._delete_locked already treats a live clone as blocking (it
soft-deletes and keeps the blob). The monitor finalises that same delete, so it
has to honour the same rule or it undoes the protection.
"""
import pytest

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_monitor

SNAP_ID = "SNAP_parent"


class _Mini:
    def __init__(self, uuid, cloned_from, status):
        self.uuid = uuid
        self.cloned_from_snap = cloned_from
        self.status = status

    def get_id(self):
        return self.uuid


class _Lvol:
    def __init__(self, uuid, status):
        self.uuid = uuid
        self.status = status

    def get_id(self):
        return self.uuid


class _SnapLvol:
    lvs_name = "lvs"
    uuid = "LV_parent"

    def get_id(self):
        return self.uuid


class _Snap:
    def __init__(self, uuid=SNAP_ID):
        self.uuid = uuid
        self.snap_bdev = "lvs/SNAP_parent"
        self.deletion_status = ""
        self.lvol = _SnapLvol()

    def get_id(self):
        return self.uuid


class _Node:
    def __init__(self):
        self.uuid = "N1"
        self.status = "unreachable"      # keep the leadership probe out of the way
        self.secondary_node_id = ""
        self.tertiary_node_id = ""
        self.lvstore = "lvs"

    def get_id(self):
        return self.uuid


@pytest.fixture
def db(monkeypatch):
    store = {"lvols": {}}

    class _DB:
        def get_lvol_by_id(self, lid):
            if lid not in store["lvols"]:
                raise KeyError(lid)
            return store["lvols"][lid]

        def get_mini_lvols(self):
            return []

        def get_storage_node_by_id(self, nid):
            raise KeyError(nid)

    monkeypatch.setattr(snapshot_monitor, "db", _DB())
    return store


BLOCK_MSG = "still depends on it"


def _call(minis):
    """Run the delete pass. Beyond the clone check the function needs a leader
    and live RPCs, so the assertions below key on the clone decision itself
    rather than on the overall return value."""
    try:
        return snapshot_monitor.process_snap_delete(_Snap(), _Node(), all_mini_lvols=minis)
    except Exception:
        return "proceeded-past-clone-check"


def test_live_clone_blocks_delete(db, caplog):
    """The regression: an ONLINE clone must stop the snapshot being deleted."""
    db["lvols"]["CLONE1"] = _Lvol("CLONE1", LVol.STATUS_ONLINE)
    result = _call([_Mini("CLONE1", SNAP_ID, LVol.STATUS_ONLINE)])
    assert result is False, (
        "monitor deleted a snapshot that a live clone still depends on — "
        "this is the all-zeros fail-over volume")
    assert BLOCK_MSG in caplog.text


def test_clone_of_another_snapshot_does_not_block(db, caplog):
    db["lvols"]["CLONE2"] = _Lvol("CLONE2", LVol.STATUS_ONLINE)
    _call([_Mini("CLONE2", "SOME_OTHER_SNAP", LVol.STATUS_ONLINE)])
    assert BLOCK_MSG not in caplog.text, "blocked on a clone of an unrelated snapshot"


def test_vanished_clone_does_not_block(db, caplog):
    """Stale mini record for a clone that is already gone must not block."""
    _call([_Mini("GONE", SNAP_ID, LVol.STATUS_ONLINE)])
    assert BLOCK_MSG not in caplog.text, "blocked on a clone that no longer exists"


def test_in_deletion_clone_still_blocks(db):
    """Pre-existing behaviour preserved: an in-flight clone delete blocks too."""
    db["lvols"]["CLONE3"] = _Lvol("CLONE3", LVol.STATUS_IN_DELETION)
    assert _call([_Mini("CLONE3", SNAP_ID, LVol.STATUS_IN_DELETION)]) is False
