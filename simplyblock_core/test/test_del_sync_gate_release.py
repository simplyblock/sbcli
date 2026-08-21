"""The primary's del-sync gate must be released by the delete that set it.

`lvol_del_sync_lock` is set on the primary whenever a peer looks down during a
delete, and it BLOCKS snapshot/lvol creation on that node. It is cleared only
by the sync-del task runner, i.e. only if a sync-del task exists. Once the
delete path stopped queueing a task for a peer that owes nothing, nothing
cleared the gate any more: node 77bf8979 (lab run 17) silently stopped
producing internal snapshots for its two replicated volumes, which read as
"replication stopped" while the real cause was blocked creation.

Every delete therefore ends with a reset, which keeps the gate only while
sync-del tasks are genuinely pending.
"""
from simplyblock_core.services import snapshot_monitor as sm
from simplyblock_core.models.storage_node import StorageNode


class _RPC:
    def delete_lvol(self, name, sync=False, special_delete=False):
        return True, None

    def bdev_lvol_get_lvol_delete_status(self, name):
        return 0


class _Node:
    def __init__(self, uuid, status=StorageNode.STATUS_ONLINE, sec=None, ter=None):
        self.uuid = uuid
        self.status = status
        self.cluster_id = "C1"
        self.secondary_node_id = sec
        self.tertiary_node_id = ter
        self.reset_calls = 0
        self.lock_calls = 0

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return _RPC()

    def lvol_del_sync_lock(self):
        self.lock_calls += 1
        return True

    def lvol_del_sync_lock_reset(self):
        self.reset_calls += 1
        return True


class _Snap:
    def __init__(self):
        self.uuid = "SNAP1"
        self.cluster_id = "C1"
        self.snap_bdev = "LVS_1/SNAP1"
        self.instances = []
        self.lvol = type("L", (), {"lvs_name": "LVS_1", "node_id": "PRIMARY", "ha_type": "ha",
                                   "get_id": lambda self: "LV1"})()

    def get_id(self):
        return self.uuid

    def remove(self, kv):
        pass


def _run_finish(monkeypatch, nodes, primary):
    class _DB:
        def get_storage_node_by_id(self, uuid):
            return nodes[uuid]

        def unindex_snapshot(self, snap):
            pass

        kv_store = None

    monkeypatch.setattr(sm, "db", _DB())
    monkeypatch.setattr(sm.snapshot_events, "snapshot_delete", lambda s: None)
    monkeypatch.setattr(sm.snapshot_controller, "lvstore_op_lock",
                        lambda *a, **kw: __import__("contextlib").nullcontext())
    monkeypatch.setattr(sm.snapshot_controller, "sync_delete_on_peer",
                        lambda *a, **kw: True)
    sm.process_snap_delete_finish(_Snap(), primary)


def test_gate_is_released_when_a_peer_was_down(monkeypatch):
    """The regression: peer down -> gate set -> no task queued -> must reset."""
    primary = _Node("PRIMARY", sec="PEER")
    peer = _Node("PEER", status=StorageNode.STATUS_DOWN)
    nodes = {"PRIMARY": primary, "PEER": peer}

    _run_finish(monkeypatch, nodes, primary)

    assert primary.lock_calls == 1, "gate should be taken while a peer is down"
    assert primary.reset_calls == 1, (
        "gate never released: creation on this node stays blocked forever")


def test_gate_is_reset_even_when_all_peers_are_healthy(monkeypatch):
    primary = _Node("PRIMARY", sec="PEER")
    peer = _Node("PEER")
    nodes = {"PRIMARY": primary, "PEER": peer}

    _run_finish(monkeypatch, nodes, primary)

    assert primary.lock_calls == 0
    assert primary.reset_calls == 1, "a stale gate from an earlier delete must clear"
