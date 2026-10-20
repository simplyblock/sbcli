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
        # No policy or target unless a test installs one.
        self.policies: dict = {}
        self.targets: dict = {}

    def get_lvol_by_id(self, lid):
        return _failed_over_lvol()

    def get_lvol_replication_objects(self):
        return self._reps

    def get_storage_node_by_id(self, nid):
        if nid not in self._nodes:
            raise KeyError(nid)
        return self._nodes[nid]

    def get_consistency_group_by_id(self, gid):
        raise KeyError(gid)

    def get_replication_policy_by_id(self, pid):
        if pid not in self.policies:
            raise KeyError(pid)
        return self.policies[pid]

    def get_replication_target_by_id(self, tid):
        if tid not in self.targets:
            raise KeyError(tid)
        return self.targets[tid]


@pytest.fixture
def patched(monkeypatch):
    calls: dict = {"node_writes": []}

    def _lvol_write(self, kv=None):
        if self.replication_node_id:
            calls["node_writes"].append(self.replication_node_id)
    monkeypatch.setattr(LVol, "write_to_db", _lvol_write)

    def _replication_start(lvol_id, replication_cluster_id=None, mode=None, interval_min=None,
                           from_policy=False):
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


def _node(uuid, cluster_id):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.status = StorageNode.STATUS_ONLINE
    return n


def test_own_cluster_as_source_recovers_to_the_relationships_source(monkeypatch, patched):
    """#34: the CSI driver's group resync passed the class's sourceClusterID
    through, and on the resyncing site that names the PEER -- the cluster the
    failed-over clone itself sits on. Taken as a fresh source it configured the
    clone to replicate to its own cluster (own node, every transfer beside the
    live clone, status healthy, nothing shipped home). The volume's own cluster
    can only mean "recover to where I came from": the delta branch."""
    nodes = {"N_src_orig": _node("N_src_orig", "CL_src"), "N_tgt": _node("N_tgt", "CL_tgt")}
    _install(monkeypatch, nodes)

    lvol_controller.replication_failback("LV_TGT", source_cluster_id="CL_tgt")

    assert patched["cluster"] == "CL_src", "must NOT replicate to its own cluster CL_tgt"
    assert "N_src_orig" in patched["node_writes"], "delta fail-back to the original node"


def test_own_cluster_as_source_on_a_demoted_primary_replicates_to_the_policy_target(
        monkeypatch, patched):
    """A plain relocate: Ramen demotes this site and resyncs it before promoting
    the peer. The member is the demoted primary -- never a fail-over target, so
    no relationship -- and the data is still here. Handed its own cluster, it
    must replicate to its policy's target as the plain path always did; refusing
    it stalled every relocate (run 20261019-1541-ebs, tests 1 and 3)."""
    nodes = {"N_tgt": _node("N_tgt", "CL_tgt")}
    db = _FakeDB([], nodes)
    db.policies = {"P1": type("P", (), {"target_id": "T1"})()}
    db.targets = {"T1": type("T", (), {"target_cluster_id": "CL_peer"})()}
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    lv = _failed_over_lvol()
    lv.replication_policy_id = "P1"
    db.get_lvol_by_id = lambda lid: lv

    assert lvol_controller.replication_failback("LV_TGT", source_cluster_id="CL_tgt") is True

    assert patched["cluster"] == "CL_peer", "the policy's target, never its own cluster"
    assert patched["mode"] == "migration"


def test_own_cluster_as_source_without_relationship_or_policy_is_refused(monkeypatch, patched):
    nodes = {"N_tgt": _node("N_tgt", "CL_tgt")}
    db = _FakeDB([], nodes)
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)

    result = lvol_controller.replication_failback("LV_TGT", source_cluster_id="CL_tgt")

    assert result[0] is False
    assert "own cluster" in result[1]
    assert "cluster" not in patched, "replication_start must not be invoked"
