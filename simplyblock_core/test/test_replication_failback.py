"""D8 unit tests for fail-back configuration (delta vs full)."""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.storage_node import StorageNode


def _failed_over_lvol():
    lv = LVol()
    lv.uuid = "LV_TGT"
    lv.node_id = "N_tgt"
    lv.from_source = False
    return lv


def _rep():
    rep = LVolReplication()
    rep.uuid = "REP1"
    src = LVol()
    src.uuid = "LV1"
    src.node_id = "N_src_orig"
    rep.source_lvol = src
    tgt = LVol()
    tgt.uuid = "LV_TGT"
    rep.target_lvol = tgt
    rep.source_cluster_id = "CL_src"
    rep.target_cluster_id = "CL_tgt"
    return rep


class _FakeDB:
    def __init__(self, reps, nodes):
        self._reps = reps
        self._nodes = nodes

    def get_lvol_by_id(self, lid):
        return _failed_over_lvol()

    def get_lvol_replication_objects(self):
        return self._reps

    def get_storage_node_by_id(self, nid):
        if nid not in self._nodes:
            raise KeyError(nid)
        return self._nodes[nid]


@pytest.fixture
def patched(monkeypatch):
    calls: dict = {"node_writes": []}

    def _lvol_write(self, kv=None):
        if self.replication_node_id:
            calls["node_writes"].append(self.replication_node_id)
    monkeypatch.setattr(LVol, "write_to_db", _lvol_write)

    def _replication_start(lvol_id, replication_cluster_id=None, mode=None, interval_min=None):
        calls["lvol_id"] = lvol_id
        calls["cluster"] = replication_cluster_id
        calls["mode"] = mode
        return True
    monkeypatch.setattr(lvol_controller, "replication_start", _replication_start)
    return calls


def _install(monkeypatch, nodes):
    db = _FakeDB([_rep()], nodes)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    return db


def test_recovered_source_uses_original_node_for_delta(monkeypatch, patched):
    nodes = {"N_src_orig": _node_online()}
    _install(monkeypatch, nodes)

    # No source_cluster_id -> recovered source, delta fail-back.
    lvol_controller.replication_failback("LV_TGT")

    # Delegates to replication_start against the original source cluster, and the
    # replication node was pre-set to the original source node (so backlog
    # matching links the pre-existing snapshots and only the delta replicates).
    assert patched["cluster"] == "CL_src"
    assert patched["mode"] == "migration"
    # Delta: replication pointed at the ORIGINAL source node.
    assert "N_src_orig" in patched["node_writes"]


def test_fresh_source_full_replication(monkeypatch, patched):
    nodes = {"N_src_orig": _node_online()}
    _install(monkeypatch, nodes)

    lvol_controller.replication_failback("LV_TGT", source_cluster_id="CL_fresh")

    assert patched["cluster"] == "CL_fresh"
    assert patched["mode"] == "migration"
    # Fresh source: replication node NOT pre-pinned (full replication, node picked
    # by replication_start in the fresh cluster).
    assert patched["node_writes"] == []


def test_recovered_source_node_offline_fails(monkeypatch, patched):
    nodes = {"N_src_orig": _node_offline()}
    _install(monkeypatch, nodes)

    result = lvol_controller.replication_failback("LV_TGT")
    assert result[0] is False
    # replication_start must not have been invoked.
    assert "cluster" not in patched


def _node_online():
    n = StorageNode()
    n.uuid = "N_src_orig"
    n.status = StorageNode.STATUS_ONLINE
    return n


def _node_offline():
    n = StorageNode()
    n.uuid = "N_src_orig"
    n.status = StorageNode.STATUS_OFFLINE
    return n
