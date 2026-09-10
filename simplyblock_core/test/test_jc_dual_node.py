"""The JC dual-node flag must track cluster MEMBERSHIP.

Soak case 6 (2026-08-24): gracefully shutting down one node of a 2-node
cluster made the SURVIVOR abort its own SPDK --

    JC detected a network outage nd=1 njms=2
    JC aborts the node due to network outage
    spdk_abort_node: Forcing application shutdown via abort.

-- because the journal component requires jc_ha_nmin_jms() reachable
journals, which is 2 unless the dual-node flag is set (then 1). The fork
implements the tolerance and exposes jc_set_dual_node; the control plane
never called it, so every 2-node cluster lost all availability the moment
either node stopped.
"""
import inspect

import pytest

from simplyblock_core import storage_node_ops
from simplyblock_core.models.storage_node import StorageNode


class _RPC:
    def __init__(self, sink, node_id):
        self._sink, self._node_id = sink, node_id

    def jc_set_dual_node(self, enable):
        self._sink.append((self._node_id, enable))
        return True


class _Node:
    def __init__(self, nid, sink, status=StorageNode.STATUS_ONLINE):
        self._id, self._sink, self.status = nid, sink, status
        self.cluster_id = "CL"

    def get_id(self):
        return self._id

    def rpc_client(self, *a, **kw):
        return _RPC(self._sink, self._id)


def _install(monkeypatch, nodes):
    class _DB:
        def get_storage_nodes_by_cluster_id(self, cluster_id):
            return nodes
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: _DB())


def test_two_node_cluster_enables_dual_node(monkeypatch):
    sink: list[tuple[str, bool]] = []
    nodes = [_Node("A", sink), _Node("B", sink)]
    _install(monkeypatch, nodes)
    storage_node_ops.apply_jc_dual_node("CL")
    assert sink == [("A", True), ("B", True)]


def test_three_node_cluster_disables_dual_node(monkeypatch):
    sink: list[tuple[str, bool]] = []
    nodes = [_Node("A", sink), _Node("B", sink), _Node("C", sink)]
    _install(monkeypatch, nodes)
    storage_node_ops.apply_jc_dual_node("CL")
    assert sink == [("A", False), ("B", False), ("C", False)]


def test_flag_follows_membership_not_how_many_are_online(monkeypatch):
    """A 3-node cluster with one node down still needs 2 journals. Keying
    the flag on the ONLINE count would switch a degraded 3-node cluster into
    dual-node mode -- weakening the journal requirement exactly when a node
    is already missing."""
    sink: list[tuple[str, bool]] = []
    nodes = [_Node("A", sink), _Node("B", sink),
             _Node("C", sink, status=StorageNode.STATUS_OFFLINE)]
    _install(monkeypatch, nodes)
    storage_node_ops.apply_jc_dual_node("CL")
    # offline node is not called, but the two online ones must be told FALSE
    assert sink == [("A", False), ("B", False)]


def test_removed_nodes_do_not_count_towards_membership(monkeypatch):
    sink: list[tuple[str, bool]] = []
    nodes = [_Node("A", sink), _Node("B", sink),
             _Node("C", sink, status=StorageNode.STATUS_REMOVED)]
    _install(monkeypatch, nodes)
    storage_node_ops.apply_jc_dual_node("CL")
    assert sink == [("A", True), ("B", True)]


def test_one_unreachable_node_does_not_stop_the_others(monkeypatch):
    sink: list[tuple[str, bool]] = []

    class _BadNode(_Node):
        def rpc_client(self, *a, **kw):
            raise RuntimeError("node unreachable")

    nodes = [_BadNode("A", sink), _Node("B", sink)]
    _install(monkeypatch, nodes)
    storage_node_ops.apply_jc_dual_node("CL")   # must not raise
    assert sink == [("B", True)]


@pytest.mark.parametrize("path", ["_prepare_cluster_devices_on_restart"])
def test_bring_up_paths_apply_the_flag(path):
    """A restarted node comes back with the JC default (off), so the flag has
    to be re-applied on every bring-up, not only when the node is added."""
    src = inspect.getsource(getattr(storage_node_ops, path))
    assert "apply_jc_dual_node" in src, f"{path} must re-apply the dual-node flag"
