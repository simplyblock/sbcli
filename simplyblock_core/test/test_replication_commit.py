"""D7 unit tests for migration-mode cutover orchestration (replication_commit)."""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster


def _src_lvol():
    lv = LVol()
    lv.uuid = "LV1"
    lv.nqn = "nqn.orig:lvol:LV1"
    lv.ns_id = 7
    lv.node_id = "N_src"
    lv.replication_node_id = "N_tgt"
    lv.replication_mode = "migration"
    return lv


def _tgt_lvol():
    lv = LVol()
    lv.uuid = "LV_TGT"
    lv.nqn = "nqn.orig:lvol:LV1"
    lv.ns_id = 7
    lv.lvol_bdev = "LVOL_9"
    lv.top_bdev = "lvs_tgt/LVOL_9"
    lv.node_id = "N_tgt"
    return lv


def _node(uuid, cluster_id, status=StorageNode.STATUS_ONLINE, lvstore="lvs"):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.status = status
    n.lvstore = lvstore
    return n


def _cluster(uuid, target_cluster="", target_pool=""):
    c = Cluster()
    c.uuid = uuid
    c.snapshot_replication_target_cluster = target_cluster
    c.snapshot_replication_target_pool = target_pool
    return c


def _snap():
    s = SnapShot()
    s.uuid = "t1"
    s.snap_bdev = "lvs_tgt/SNAP1"
    return s


class _FakeDB:
    kv_store = "KV"

    def __init__(self, nodes, clusters):
        self._nodes = nodes
        self._clusters = clusters

    def get_lvol_by_id(self, lid):
        return _src_lvol()

    def get_storage_node_by_id(self, nid):
        return self._nodes[nid]

    def get_cluster_by_id(self, cid):
        return self._clusters[cid]


@pytest.fixture
def patched(monkeypatch):
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    captured = {}

    def _rep_write(self, kv=None):
        captured["rep_state"] = self.state
        captured["rep_id"] = self.get_id()
    monkeypatch.setattr(LVolReplication, "write_to_db", _rep_write)

    nodes = {
        "N_src": _node("N_src", "CL_src", lvstore="lvs_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", lvstore="lvs_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _FakeDB(nodes, clusters))

    snap_add = []

    def _fake_snap_add(lid, name, snap_type="user"):
        snap_add.append((lid, snap_type))
        return ("snap", None)

    monkeypatch.setattr(lvol_controller.snapshot_controller, "add", _fake_snap_add)

    monkeypatch.setattr(lvol_controller, "_last_replicated_target_snapshot",
                        lambda db, lid, cid: _snap())

    create_calls = []

    def _create(db, lvol, target_node, pool_uuid, snapshot):
        create_calls.append((target_node.get_id(), pool_uuid, snapshot.get_id()))
        return _tgt_lvol(), None
    monkeypatch.setattr(lvol_controller, "_create_target_lvol_clone", _create)

    suspended = []
    monkeypatch.setattr(lvol_controller, "suspend_lvol", lambda lid: suspended.append(lid))
    monkeypatch.setattr(lvol_controller, "_resolve_target_map_id", lambda node, bdev: 42)

    task_params = {}

    def _add_task(cluster_id, node_id, params):
        task_params.update(params)
        task_params["_cluster"] = cluster_id
        task_params["_node"] = node_id
        return "task-uuid"
    monkeypatch.setattr(lvol_controller.tasks_controller, "add_replication_final_task", _add_task)

    captured.update(snap_add=snap_add, create_calls=create_calls,
                    suspended=suspended, task_params=task_params)
    return captured


def test_commit_enqueues_final_task(patched):
    result = lvol_controller.replication_commit("LV1")

    assert result["cutover_task_queued"] is True
    assert "replication_id" in result and "target_lvol_id" in result

    # A final internal snapshot was taken to minimize the delta.
    assert patched["snap_add"] and patched["snap_add"][0][1] == SnapShot.TYPE_INTERNAL
    # Target writable clone created from the last replicated snapshot in the target pool.
    assert patched["create_calls"] == [("N_tgt", "POOL_tgt", "t1")]
    # Target exposed inaccessible until cutover.
    assert patched["suspended"] == ["LV_TGT"]

    p = patched["task_params"]
    assert p["lvol_id"] == "LV1"
    assert p["src_node_id"] == "N_src"
    assert p["tgt_node_id"] == "N_tgt"
    assert p["tgt_lvol_composite"] == "lvs_tgt/LVOL_9"
    assert p["tgt_map_id"] == 42
    assert p["tgt_snap_composite"] == "lvs_tgt/SNAP1"
    assert p["operation"] == "replicate"
    assert p["final_state"] == LVolReplication.STATE_CUTOVER_DONE
    assert p["_cluster"] == "CL_src" and p["_node"] == "N_src"

    # Replication record stamped cutover-pending.
    assert patched["rep_state"] == LVolReplication.STATE_CUTOVER_PENDING


def test_commit_no_replicated_snapshot(monkeypatch):
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    nodes = {
        "N_src": _node("N_src", "CL_src", lvstore="lvs_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", lvstore="lvs_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    monkeypatch.setattr(lvol_controller, "DBController", lambda: _FakeDB(nodes, clusters))
    monkeypatch.setattr(lvol_controller.snapshot_controller, "add",
                        lambda lid, name, snap_type="user": ("snap", None))
    monkeypatch.setattr(lvol_controller, "_last_replicated_target_snapshot",
                        lambda db, lid, cid: None)

    result = lvol_controller.replication_commit("LV1")
    assert result[0] is False
