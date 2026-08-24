"""Phase-2 sync delete on a peer: attempt first, classify afterwards.

Run 15 case 6: the node was suspended (step 1 of the supported
suspend -> shutdown sequence), so every subsequent snapshot delete queued a
durable sync-delete task for it instead of trying. Those tasks then refused to
run *because* the node was suspended, and 46 of them blocked the node's own
shutdown — the node could never leave `suspended`.

A sync delete is only owed by a peer that is still running: a node that is
gone loses its in-memory registration with the process, and it is not rebuilt
because the object's record is already deleted.
"""
from simplyblock_core.controllers import snapshot_controller as sc
from simplyblock_core.models.storage_node import StorageNode


class _RPC:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def delete_lvol(self, name, sync=False, special_delete=False):
        self.calls.append((name, sync, special_delete))
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


class _Node:
    def __init__(self, uuid, status, rpc):
        self.uuid = uuid
        self.status = status
        self.cluster_id = "C1"
        self._rpc = rpc

    def get_id(self):
        return self.uuid

    def rpc_client(self):
        return self._rpc


def _patch(monkeypatch, node, db_status=None):
    added = []

    class _DB:
        def get_storage_node_by_id(self, uuid):
            n = _Node(uuid, db_status or node.status, node._rpc)
            return n

    monkeypatch.setattr(sc, "db_controller", _DB())
    monkeypatch.setattr(sc.tasks_controller, "add_lvol_sync_del_task",
                        lambda *a, **kw: added.append(a))
    return added


def test_suspended_peer_is_tried_not_deferred(monkeypatch):
    """The regression: a suspended node is UP, so it must get the RPC."""
    rpc = _RPC((True, None))
    node = _Node("PEER", StorageNode.STATUS_SUSPENDED, rpc)
    added = _patch(monkeypatch, node)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is True
    assert rpc.calls == [("LVS_1/SNAP_1", True, False)], "no sync delete attempted"
    assert added == [], "queued a task for a node that could have done it now"


def test_failure_on_live_peer_queues_a_retry_task(monkeypatch):
    rpc = _RPC((False, {"code": -5, "message": "io error"}))
    node = _Node("PEER", StorageNode.STATUS_ONLINE, rpc)
    added = _patch(monkeypatch, node)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is False
    assert len(added) == 1


def test_failure_on_offline_peer_is_ignored(monkeypatch):
    """Nothing is owed: the registration went away with the process."""
    rpc = _RPC((False, {"code": -5, "message": "unreachable"}))
    node = _Node("PEER", StorageNode.STATUS_ONLINE, rpc)   # stale view
    added = _patch(monkeypatch, node, db_status=StorageNode.STATUS_OFFLINE)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is True
    assert added == [], "queued a task for a node that owes nothing"


def test_rpc_exception_on_gone_peer_is_ignored(monkeypatch):
    rpc = _RPC(TimeoutError("no route to host"))
    node = _Node("PEER", StorageNode.STATUS_ONLINE, rpc)
    added = _patch(monkeypatch, node, db_status=StorageNode.STATUS_UNREACHABLE)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is True
    assert added == []


def test_rpc_exception_on_live_peer_queues_a_task(monkeypatch):
    rpc = _RPC(TimeoutError("rpc timeout"))
    node = _Node("PEER", StorageNode.STATUS_ONLINE, rpc)
    added = _patch(monkeypatch, node)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is False
    assert len(added) == 1


def test_enodev_counts_as_done(monkeypatch):
    rpc = _RPC((False, {"code": -19, "message": "no such device"}))
    node = _Node("PEER", StorageNode.STATUS_ONLINE, rpc)
    added = _patch(monkeypatch, node)

    assert sc.sync_delete_on_peer(node, "LVS_1/SNAP_1", "PRIMARY") is True
    assert added == []
