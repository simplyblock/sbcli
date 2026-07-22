"""D3 unit tests for the interval-driven internal-snapshot scheduler."""
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.services import snapshot_monitor as sm


def _lvol(uuid="LV1", do_replicate=True, interval=5, status=LVol.STATUS_ONLINE):
    lv = LVol()
    lv.uuid = uuid
    lv.do_replicate = do_replicate
    lv.replication_interval_min = interval
    lv.status = status
    lv.node_id = "N1"
    return lv


def _snap(lvol_uuid, created_at, snap_type):
    lv = LVol()
    lv.uuid = lvol_uuid
    s = SnapShot()
    s.created_at = created_at
    s.snap_type = snap_type
    s.lvol = lv
    return s


def test_not_due_when_replication_disabled():
    lv = _lvol(do_replicate=False)
    assert sm._due_for_internal_snapshot(lv, [], 10_000) is False


def test_not_due_when_interval_zero():
    lv = _lvol(interval=0)
    assert sm._due_for_internal_snapshot(lv, [], 10_000) is False


def test_not_due_when_not_online():
    lv = _lvol(status=LVol.STATUS_OFFLINE)
    assert sm._due_for_internal_snapshot(lv, [], 10_000) is False


def test_first_snapshot_is_due():
    lv = _lvol(interval=5)
    # No internal snapshot yet -> first one taken immediately.
    assert sm._due_for_internal_snapshot(lv, [], 10_000) is True


def test_not_due_within_interval():
    lv = _lvol(interval=5)  # 300s
    snaps = [_snap("LV1", 9_800, SnapShot.TYPE_INTERNAL)]  # 200s ago
    assert sm._due_for_internal_snapshot(lv, snaps, 10_000) is False


def test_due_after_interval():
    lv = _lvol(interval=5)  # 300s
    snaps = [_snap("LV1", 9_600, SnapShot.TYPE_INTERNAL)]  # 400s ago
    assert sm._due_for_internal_snapshot(lv, snaps, 10_000) is True


def test_user_snapshots_do_not_reset_interval():
    lv = _lvol(interval=5)  # 300s
    snaps = [
        _snap("LV1", 9_990, SnapShot.TYPE_USER),       # recent user snap
        _snap("LV1", 9_600, SnapShot.TYPE_INTERNAL),   # internal 400s ago
    ]
    # Interval is measured only against internal snapshots -> due.
    assert sm._due_for_internal_snapshot(lv, snaps, 10_000) is True


def test_other_lvol_internal_snaps_ignored():
    lv = _lvol(uuid="LV1", interval=5)
    snaps = [_snap("LV2", 9_990, SnapShot.TYPE_INTERNAL)]  # different lvol
    # No internal snap for LV1 -> first one due.
    assert sm._due_for_internal_snapshot(lv, snaps, 10_000) is True


def test_take_due_internal_snapshots_creates_internal_type(monkeypatch):
    due = _lvol(uuid="LV1", interval=5)
    not_due = _lvol(uuid="LV2", do_replicate=False)

    class _DB:
        def get_lvols(self, cluster_id):
            return [due, not_due]

        def get_mini_snapshots(self):
            return []

    calls = []

    class _SnapCtl:
        def add(self, lvol_id, name, snap_type=SnapShot.TYPE_USER):
            calls.append((lvol_id, snap_type))
            return "snap-uuid", None

    monkeypatch.setattr(sm, "db", _DB())
    monkeypatch.setattr(sm, "snapshot_controller", _SnapCtl())

    sm.take_due_internal_snapshots("CL1", 10_000)

    assert calls == [("LV1", SnapShot.TYPE_INTERNAL)]
