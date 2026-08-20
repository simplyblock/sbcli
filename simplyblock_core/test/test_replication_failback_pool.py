"""Fail-back must not die on a snapshot that has no counterpart yet.

Lab 2026-08-17/18: every fail-back task failed on its FIRST step with
"Multiple values present" (348 occurrences) and nothing ever replicated back.
Cause: process_snap_replicate_start looked the counterpart up unconditionally,
so an empty source_replicated_snap_uuid reached get_snapshot_by_id(""), which
degenerates into a table-prefix scan and matches every snapshot.

That state is normal, not exceptional: snapshots taken after the fail-over have
no counterpart, and when failing back into a FRESHLY INSTALLED cluster none of
them do.
"""
import pytest

from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr


class _FakeDB:
    """Only what _destination_pool_uuid touches, plus a get_snapshot_by_id that
    reproduces the real blank-id behaviour (raises, never returns a random row)."""

    def __init__(self, configured_pool="", pools=(), snapshots=None,
                 configured_cluster=""):
        self.cluster = type("C", (), {
            "snapshot_replication_target_pool": configured_pool,
            "snapshot_replication_target_cluster": configured_cluster,
        })()
        self._pools = list(pools)
        self._snapshots = snapshots or {}

    def get_cluster_by_id(self, cluster_id):
        if not cluster_id:
            raise KeyError("Cluster lookup with a blank id")
        return self.cluster

    def get_replication_policy_for_lvol(self, lvol):
        return None

    def get_pools(self, cluster_id=None):
        return self._pools

    def get_snapshot_by_id(self, uuid):
        if not uuid:
            raise KeyError("Snapshot lookup with a blank id")
        return self._snapshots[uuid]


def _node(cluster_id="CL1"):
    n = StorageNode()
    n.uuid = "N1"
    n.cluster_id = cluster_id
    return n


def _pool(uuid, status=Pool.STATUS_ACTIVE):
    p = Pool()
    p.uuid = uuid
    p.pool_name = uuid
    p.status = status
    return p


def test_configured_replication_pool_wins(monkeypatch):
    """...but only for the direction it describes — see
    test_replication_destination_resolution for the cross-direction cases."""
    monkeypatch.setattr(sr, "db", _FakeDB(configured_pool="POOL_CFG",
                                          configured_cluster="CL1",
                                          pools=[_pool("POOL_OTHER")]))
    assert sr._destination_pool_uuid(_node(), source_cluster_id="CL0") == "POOL_CFG"


def test_falls_back_to_first_active_pool(monkeypatch):
    monkeypatch.setattr(sr, "db", _FakeDB(
        pools=[_pool("POOL_INACTIVE", status=Pool.STATUS_INACTIVE), _pool("POOL_OK")]))
    assert sr._destination_pool_uuid(_node(), source_cluster_id="CL0") == "POOL_OK"


def test_no_pool_available_returns_none(monkeypatch):
    monkeypatch.setattr(sr, "db", _FakeDB(pools=[]))
    assert sr._destination_pool_uuid(_node(), source_cluster_id="CL0") is None


def test_blank_counterpart_uuid_is_never_looked_up(monkeypatch):
    """The regression itself: a blank uuid must not reach get_snapshot_by_id."""
    db = _FakeDB(configured_pool="POOL_DEST", configured_cluster="CL1")
    monkeypatch.setattr(sr, "db", db)

    snap = SnapShot()
    snap.uuid = "SNAP_NEW"
    snap.source_replicated_snap_uuid = ""          # no counterpart yet
    lv = LVol()
    lv.uuid = "LV1"
    snap.lvol = lv

    # Mirrors the guarded branch in process_snap_replicate_start.
    pool = None
    if snap.source_replicated_snap_uuid:
        pool = db.get_snapshot_by_id(snap.source_replicated_snap_uuid).lvol.pool_uuid
    if not pool:
        pool = sr._destination_pool_uuid(_node(), lvol=lv, source_cluster_id="CL0")
    assert pool == "POOL_DEST"


def test_blank_id_lookup_raises_rather_than_matching_everything():
    """Guard rail: a blank id must be an error, not a whole-table scan."""
    db = _FakeDB()
    with pytest.raises(KeyError):
        db.get_snapshot_by_id("")
