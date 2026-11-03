"""No new cadence snapshot while the source cluster is past its capacity
warning threshold.

A cadence snapshot pins every block overwritten while it is outstanding: under
sustained overwrites faster than replication ships them, one outstanding
snapshot per volume pins up to the volume's whole used size (twice that raw,
with 1+1). The EKS bed of 2026-10-22 went 81% -> 90% in three minutes, the
cluster turned read_only, writes failed with EIO and the storage nodes went
down. Holding new snapshots past cap_warn lets retention reclaim what already
replicated instead of pinning more.
"""
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.services import snapshot_monitor as sm


def _lvol(uuid="LV1"):
    lv = LVol()
    lv.uuid = uuid
    lv.do_replicate = True
    lv.replication_interval_min = 1
    lv.status = LVol.STATUS_ONLINE
    return lv


class _Rec:
    def __init__(self, util):
        self.size_util = util


class _Cluster:
    cap_warn = 80
    sync_replication = False

    def get_id(self):
        return "CL1"


def _db(util):
    class _DB:
        def get_lvols(self, cluster_id):
            return [_lvol()]

        def get_mini_snapshots(self):
            return []

        def get_replication_policies(self, cluster_id=None):
            return []

        def get_cluster_by_id(self, cid):
            return _Cluster()

        def get_cluster_capacity(self, cl, limit=1):
            return [] if util is None else [_Rec(util)]
    return _DB()


def _run(monkeypatch, util):
    calls = []

    class _Delta:
        # The cadence takes its snapshots through replication_delta (#1537).
        def add_snapshot(self, lvol, name, snap_type=SnapShot.TYPE_USER):
            calls.append(lvol.get_id())
            return "snap-uuid", None
    monkeypatch.setattr(sm, "db", _db(util))
    monkeypatch.setattr(sm, "replication_delta", _Delta())
    sm.take_due_internal_snapshots("CL1", 10_000)
    return calls


def test_no_cadence_snapshot_past_the_warning_threshold(monkeypatch):
    assert _run(monkeypatch, 85) == []


def test_cadence_snapshot_below_the_threshold(monkeypatch):
    assert _run(monkeypatch, 60) == ["LV1"]


def test_no_capacity_reading_does_not_block(monkeypatch):
    assert _run(monkeypatch, None) == ["LV1"]


def test_the_reason_names_the_utilization_and_the_threshold(monkeypatch):
    monkeypatch.setattr(sm, "db", _db(91))
    reason = sm._cadence_capacity_hold("CL1")
    assert "91%" in reason and "80%" in reason


def test_only_the_nodes_first_migrating_volume_takes_cadence_snapshots(monkeypatch):
    # One volume per source node migrates at a time: the second volume of N1
    # takes no snapshot (so ships nothing) until the first has cut over.
    def mig(uuid, node, queued):
        lv = _lvol(uuid)
        lv.node_id = node
        lv.replication_mode = "migration"
        lv.migration_queued_at = queued
        return lv
    lvols = [mig("N1-second", "N1", 20), mig("N1-first", "N1", 10), mig("N2-only", "N2", 30)]
    db = _db(None)
    db.get_lvols = lambda cluster_id: lvols
    db.get_job_tasks = lambda cluster_id, reverse=True, limit=0: []
    calls = []

    class _Delta:
        # The cadence takes its snapshots through replication_delta (#1537).
        def add_snapshot(self, lvol, name, snap_type=SnapShot.TYPE_USER):
            calls.append(lvol.get_id())
            return "snap-uuid", None
    monkeypatch.setattr(sm, "db", db)
    monkeypatch.setattr(sm, "replication_delta", _Delta())
    sm.take_due_internal_snapshots("CL1", 10_000)
    assert sorted(calls) == ["N1-first", "N2-only"]
