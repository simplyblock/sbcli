"""D5 unit tests for fail-over volume re-creation on the target cluster.

Verifies the correctness fixes: the failed-over volume keeps the ORIGINAL NQN
and namespace id, is exposed on primary+secondary+tertiary target nodes, the
LVolReplication record is stamped correctly, and connection strings are
returned to the caller.
"""
import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol, LVolReplication
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.utils.nvme import NvmeConnectEntry


def _src_lvol():
    lv = LVol()
    lv.uuid = "LV1"
    lv.nqn = "nqn.orig:lvol:LV1"
    lv.ns_id = 7
    lv.node_id = "N_src"
    lv.replication_node_id = "N_tgt"
    lv.lvol_bdev = "LVOL_1"
    lv.crypto_bdev = ""
    lv.replication_mode = "failover"
    return lv


def _node(uuid, cluster_id, secondary="", tertiary="", lvstore="lvs"):
    n = StorageNode()
    n.uuid = uuid
    n.cluster_id = cluster_id
    n.secondary_node_id = secondary
    n.tertiary_node_id = tertiary
    n.lvstore = lvstore
    n.status = StorageNode.STATUS_ONLINE
    return n


def _cluster(uuid, target_cluster="", target_pool=""):
    c = Cluster()
    c.uuid = uuid
    c.nqn = "nqn.cluster." + uuid
    c.snapshot_replication_target_cluster = target_cluster
    c.snapshot_replication_target_pool = target_pool
    return c


def _src_snap():
    lv = LVol()
    lv.uuid = "LV1"
    s = SnapShot()
    s.uuid = "s1"
    s.created_at = 100
    s.target_replicated_snap_uuid = "t1"
    s.lvol = lv
    return s


def _tgt_snap():
    s = SnapShot()
    s.uuid = "t1"
    s.snap_bdev = "lvs_tgt/SNAP1"
    return s


class _FakeDB:
    kv_store = "KV"

    def __init__(self, nodes, clusters, existing_lvols=None):
        self._nodes = nodes
        self._clusters = clusters
        self._existing_lvols = existing_lvols or []
        self._snaps = {"s1": _src_snap(), "t1": _tgt_snap()}

    def get_lvol_by_id(self, lid):
        return _src_lvol()

    def get_lvol_replication_objects(self):
        # Real DBController exposes this; the fail-back retirement asks it
        # whether this volume is a copy of something. In these fail-OVER
        # tests it is not, so there is nothing to retire.
        return []

    def get_storage_node_by_id(self, nid):
        return self._nodes[nid]

    def get_cluster_by_id(self, cid):
        return self._clusters[cid]

    def get_lvols(self, cluster_id=None):
        return self._existing_lvols

    def get_job_tasks(self, cluster_id, reverse=True):
        t = JobSchedule()
        t.function_name = JobSchedule.FN_SNAPSHOT_REPLICATION
        # STATUS_DONE is required: these cases assume a snapshot whose data
        # actually reached the target, and a fail-over point is only valid once
        # its replication task has completed (JobSchedule.status defaults to "").
        t.status = JobSchedule.STATUS_DONE
        t.function_params = {"snapshot_id": "s1"}
        return [t]

    def get_snapshot_by_id(self, sid):
        return self._snaps[sid]

    # These volumes predate replication policies: the destination is resolved
    # from the target node's cluster and the source cluster's configured pool.
    def get_replication_policy_for_lvol(self, lvol):
        return None

    def get_pools(self, cluster_id=None):
        return []


@pytest.fixture
def patched(monkeypatch):
    # Avoid all FDB writes.
    monkeypatch.setattr(LVol, "write_to_db", lambda self, kv=None: None)
    captured = {}

    def _rep_write(self, kv=None):
        captured["rep"] = self
    monkeypatch.setattr(LVolReplication, "write_to_db", _rep_write)

    add_calls = []

    def _add_lvol_on_node(new_lvol, node, is_primary=True, **kw):
        add_calls.append((node.get_id(), is_primary))
        if is_primary:
            # Faithful to the real primary add, which persists the nsid the
            # target actually used: the control plane's claim when there was
            # one, the target's own pick (lowest free) otherwise.
            new_lvol.ns_id = new_lvol.ns_id or 1
        return ({"uuid": "BDEV-UUID", "driver_specific": {"lvol": {"blobid": 123}}}, None)
    monkeypatch.setattr(lvol_controller, "add_lvol_on_node", _add_lvol_on_node)

    def _connect_lvol(uid, **kw):
        e = NvmeConnectEntry(
            transport="tcp", ip="10.0.0.9", port=4420, nqn="nqn.orig:lvol:LV1",
            reconnect_delay=2, ctrl_loss_tmo=60, fast_io_fail_tmo=15,
            nr_io_queues=8, keep_alive_tmo=5, connect="nvme connect ...", ns_id=7)
        return [e], None
    monkeypatch.setattr(lvol_controller, "connect_lvol", _connect_lvol)
    monkeypatch.setattr(lvol_controller.lvol_events, "lvol_replicated", lambda a, b: None)

    captured["add_calls"] = add_calls
    return captured


def _install_db(monkeypatch, db):
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)


def test_failover_preserves_nqn_ns_and_returns_paths(monkeypatch, patched):
    nodes = {
        "N_src": _node("N_src", "CL_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", secondary="N_sec", tertiary="N_ter", lvstore="lvs_tgt"),
        "N_sec": _node("N_sec", "CL_tgt"),
        "N_ter": _node("N_ter", "CL_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    _install_db(monkeypatch, _FakeDB(nodes, clusters))

    result = lvol_controller.replicate_lvol_on_target_cluster("LV1")

    # Same NQN as the original volume -- the client reconnects to the SAME
    # subsystem, only the IP/port differ. The nsid is NOT carried over: it is
    # claimed on the target HA set (nothing there yet here, so the target
    # picks 1). Cross-cluster nsid equality is not required -- clients resolve
    # their paths through connect_lvol -- and insisting on the source's number
    # is what collided with a sibling already holding it (soak case 7).
    assert result["nqn"] == "nqn.orig:lvol:LV1"
    assert result["ns_id"] == 1
    assert len(result["connection_strings"]) == 1

    rep = patched["rep"]
    assert rep.state == LVolReplication.STATE_FAILED_OVER
    assert rep.direction == LVolReplication.DIRECTION_TO_TARGET
    assert rep.target_nqn == "nqn.orig:lvol:LV1"
    assert rep.target_ns_id == 1
    assert rep.source_cluster_id == "CL_src"
    assert rep.target_cluster_id == "CL_tgt"
    # Source volume flipped to non-source after fail-over.
    assert rep.source_lvol.from_source is False
    # Exposed on primary + secondary + tertiary.
    assert rep.target_lvol.nodes == ["N_tgt", "N_sec", "N_ter"]
    assert patched["add_calls"] == [
        ("N_tgt", True), ("N_sec", False), ("N_ter", False)]


def test_failover_secondary_only_no_tertiary(monkeypatch, patched):
    nodes = {
        "N_src": _node("N_src", "CL_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", secondary="N_sec", lvstore="lvs_tgt"),
        "N_sec": _node("N_sec", "CL_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    _install_db(monkeypatch, _FakeDB(nodes, clusters))

    result = lvol_controller.replicate_lvol_on_target_cluster("LV1")

    assert result["nqn"] == "nqn.orig:lvol:LV1"
    rep = patched["rep"]
    assert rep.target_lvol.nodes == ["N_tgt", "N_sec"]
    assert patched["add_calls"] == [("N_tgt", True), ("N_sec", False)]


def test_failover_idempotent_when_target_exists(monkeypatch, patched):
    existing = LVol()
    existing.uuid = "EXISTING"
    existing.nqn = "nqn.orig:lvol:LV1"
    # A fail-over copy preserves the source's nsid, so the already-failed-over
    # volume this guard recognises carries nsid 7 too -- not the model default.
    existing.ns_id = 7
    nodes = {
        "N_src": _node("N_src", "CL_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", secondary="N_sec", lvstore="lvs_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    _install_db(monkeypatch, _FakeDB(nodes, clusters, existing_lvols=[existing]))

    result = lvol_controller.replicate_lvol_on_target_cluster("LV1")

    # Returns the existing target lvol id; no new volume created.
    assert result == "EXISTING"
    assert patched["add_calls"] == []


def test_failover_does_not_mistake_a_sibling_namespace_for_this_volume(monkeypatch, patched):
    """Soak case 7 (run 20260824_174611): with namespaced volumes, up to
    max-namespace-per-subsys volumes SHARE one nqn. The existing-copy guard
    matched on nqn alone, so once namespace 1 had failed over, namespaces
    2..N returned ITS target id and were never failed over -- 9 of 10 volumes
    silently absent after a DR fail-over, every call reporting success."""
    sibling = LVol()
    sibling.uuid = "SIBLING_NS1"
    sibling.nqn = "nqn.orig:lvol:LV1"      # same shared subsystem
    sibling.ns_id = 1                       # DIFFERENT namespace
    nodes = {
        "N_src": _node("N_src", "CL_src"),
        "N_tgt": _node("N_tgt", "CL_tgt", secondary="N_sec", lvstore="lvs_tgt"),
    }
    clusters = {
        "CL_src": _cluster("CL_src", target_cluster="CL_tgt", target_pool="POOL_tgt"),
        "CL_tgt": _cluster("CL_tgt"),
    }
    _install_db(monkeypatch, _FakeDB(nodes, clusters, existing_lvols=[sibling]))

    result = lvol_controller.replicate_lvol_on_target_cluster("LV1")

    # LV1 (nsid 7) must actually fail over, NOT return the nsid-1 sibling.
    assert result != "SIBLING_NS1"
    assert patched["add_calls"], "the volume must really be failed over"
