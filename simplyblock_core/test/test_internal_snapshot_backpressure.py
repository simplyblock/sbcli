"""One outstanding replication transfer per volume at a time.

Lab 2026-08-20, case 4: the interval-driven cadence in
``take_due_internal_snapshots`` minted an internal snapshot every minute for
five volumes regardless of whether the previous transfer had finished. Each one
creates a REP_* landing volume on the receiving node, so in 20 minutes that
node held 75 of them and hit its subsystem cap. Retention could not help: it
only prunes snapshots carrying ``target_replicated_snap_uuid``, which is set
exactly when a transfer succeeds — so a stalled pipeline disarms the only
mechanism that would have bounded it.

The interval is a cadence for a pipeline that keeps up. When it does not keep
up, the correct behaviour is to skip the tick, not to queue another transfer.
"""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm


LVOL_ID = "LV1"


class _LvolRef:
    def __init__(self, uuid=LVOL_ID):
        self.uuid = uuid

    def get_id(self):
        return self.uuid


class _MiniSnap:
    def __init__(self, uuid, created_at, snap_type=SnapShot.TYPE_INTERNAL,
                 status=SnapShot.STATUS_ONLINE, lvol_id=LVOL_ID):
        self.uuid = uuid
        self.created_at = created_at
        self.snap_type = snap_type
        self.status = status
        self.lvol = _LvolRef(lvol_id)

    def get_id(self):
        return self.uuid


class _FullSnap:
    def __init__(self, uuid, target="", source=""):
        self.uuid = uuid
        self.target_replicated_snap_uuid = target
        self.source_replicated_snap_uuid = source

    def get_id(self):
        return self.uuid


class _Lvol:
    uuid = LVOL_ID
    do_replicate = True
    replication_interval_min = 1
    status = "online"

    def get_id(self):
        return LVOL_ID


class _FakeDB:
    def __init__(self, full_snaps):
        self._full = full_snaps

    def get_snapshot_by_id(self, uuid):
        return self._full[uuid]


def test_no_previous_snapshot_is_not_outstanding(monkeypatch):
    monkeypatch.setattr(sm, "db", _FakeDB({}))
    assert sm._outstanding_internal_snapshot(_Lvol(), []) is None


def test_replicated_previous_snapshot_is_not_outstanding(monkeypatch):
    monkeypatch.setattr(sm, "db", _FakeDB({"S1": _FullSnap("S1", target="T1")}))
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_MiniSnap("S1", 100)]) is None


def test_unreplicated_previous_snapshot_blocks_the_next(monkeypatch):
    """The regression: an unfinished transfer must stop the next tick."""
    monkeypatch.setattr(sm, "db", _FakeDB({"S1": _FullSnap("S1")}))
    outstanding = sm._outstanding_internal_snapshot(_Lvol(), [_MiniSnap("S1", 100)])
    assert outstanding is not None
    assert outstanding.get_id() == "S1"


def test_only_the_newest_internal_snapshot_decides(monkeypatch):
    """An older unreplicated snapshot does not block once a newer one landed.

    Retention prunes older generations, and a transfer can complete out of
    order; what gates the next tick is the state of the most recent one.
    """
    monkeypatch.setattr(sm, "db", _FakeDB({
        "S_OLD": _FullSnap("S_OLD"),
        "S_NEW": _FullSnap("S_NEW", target="T2"),
    }))
    snaps = [_MiniSnap("S_OLD", 100), _MiniSnap("S_NEW", 200)]
    assert sm._outstanding_internal_snapshot(_Lvol(), snaps) is None


def test_failback_direction_counts_as_delivered(monkeypatch):
    """Replication to the SOURCE records source_replicated_snap_uuid."""
    monkeypatch.setattr(sm, "db", _FakeDB({"S1": _FullSnap("S1", source="S_SRC")}))
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_MiniSnap("S1", 100)]) is None


def test_user_snapshots_do_not_gate_the_cadence(monkeypatch):
    """Only internal snapshots drive (and gate) replication."""
    monkeypatch.setattr(sm, "db", _FakeDB({"S_USER": _FullSnap("S_USER")}))
    snaps = [_MiniSnap("S_USER", 100, snap_type=SnapShot.TYPE_USER)]
    assert sm._outstanding_internal_snapshot(_Lvol(), snaps) is None


def test_snapshot_being_deleted_does_not_gate_the_cadence(monkeypatch):
    monkeypatch.setattr(sm, "db", _FakeDB({"S1": _FullSnap("S1")}))
    snaps = [_MiniSnap("S1", 100, status=SnapShot.STATUS_IN_DELETION)]
    assert sm._outstanding_internal_snapshot(_Lvol(), snaps) is None


def test_another_volumes_backlog_is_irrelevant(monkeypatch):
    monkeypatch.setattr(sm, "db", _FakeDB({"S_OTHER": _FullSnap("S_OTHER")}))
    snaps = [_MiniSnap("S_OTHER", 100, lvol_id="LV_OTHER")]
    assert sm._outstanding_internal_snapshot(_Lvol(), snaps) is None


def test_vanished_snapshot_does_not_wedge_the_cadence(monkeypatch):
    """A record deleted under us must not stall replication for ever."""
    class _MissingDB:
        def get_snapshot_by_id(self, uuid):
            raise KeyError(uuid)

    monkeypatch.setattr(sm, "db", _MissingDB())
    assert sm._outstanding_internal_snapshot(
        _Lvol(), [_MiniSnap("S1", 100)]) is None
