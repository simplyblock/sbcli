"""Replication must send to the target lvstore's CURRENT leader.

Lab run 15, case 5 (target node offline mid-replication): the receiving lvol
is pinned to the node it was created on, but only the LVS leader can accept
hub receive IO or persist a convert. After the node came back, leadership had
settled on the peer, so every transfer and convert for that volume was refused
by the leadership gate and retried forever — outstanding snapshots grew one
per minute while the other four volumes replicated normally. Nothing moves
leadership back on its own, so the retry never converges.
"""
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr


class _LVol:
    def __init__(self, uuid, node_id, nodes, lvs="LVS_13"):
        self.uuid = uuid
        self.node_id = node_id
        self.nodes = nodes
        self.lvs_name = lvs

    def get_id(self):
        return self.uuid


class _Node:
    def __init__(self, uuid, status=StorageNode.STATUS_ONLINE):
        self.uuid = uuid
        self.status = status

    def get_id(self):
        return self.uuid


def _patch(monkeypatch, nodes, leader_of, probe_error=()):
    class _DB:
        def get_storage_node_by_id(self, uuid):
            if uuid in nodes:
                return nodes[uuid]
            raise KeyError(uuid)

    import simplyblock_core.controllers.lvol_controller as lc

    def _is_leader(node, lvs):
        if node.get_id() in probe_error:
            raise RuntimeError("probe failed")
        return leader_of.get(lvs) == node.get_id()

    monkeypatch.setattr(sr, "db", _DB())
    monkeypatch.setattr(lc, "is_node_leader", _is_leader)


def test_follows_leadership_to_the_peer(monkeypatch):
    nodes = {"PINNED": _Node("PINNED"), "PEER": _Node("PEER")}
    _patch(monkeypatch, nodes, {"LVS_13": "PEER"})
    lv = _LVol("RLV", node_id="PINNED", nodes=["PINNED", "PEER"])
    assert sr._receiving_leader_node(lv).get_id() == "PEER"


def test_prefers_the_recorded_node_when_it_still_leads(monkeypatch):
    nodes = {"PINNED": _Node("PINNED"), "PEER": _Node("PEER")}
    _patch(monkeypatch, nodes, {"LVS_13": "PINNED"})
    lv = _LVol("RLV", node_id="PINNED", nodes=["PINNED", "PEER"])
    assert sr._receiving_leader_node(lv).get_id() == "PINNED"


def test_skips_offline_members(monkeypatch):
    nodes = {"PINNED": _Node("PINNED", status=StorageNode.STATUS_OFFLINE),
             "PEER": _Node("PEER")}
    # The offline node still claims leadership in the DB view; it must not be
    # chosen — the transfer would never land.
    _patch(monkeypatch, nodes, {"LVS_13": "PINNED"})
    lv = _LVol("RLV", node_id="PINNED", nodes=["PINNED", "PEER"])
    assert sr._receiving_leader_node(lv) is None


def test_no_leader_returns_none_so_caller_retries(monkeypatch):
    nodes = {"PINNED": _Node("PINNED"), "PEER": _Node("PEER")}
    _patch(monkeypatch, nodes, {"LVS_13": "SOMEONE_ELSE"})
    lv = _LVol("RLV", node_id="PINNED", nodes=["PINNED", "PEER"])
    assert sr._receiving_leader_node(lv) is None


def test_probe_failure_on_one_node_does_not_hide_the_leader(monkeypatch):
    nodes = {"PINNED": _Node("PINNED"), "PEER": _Node("PEER")}
    _patch(monkeypatch, nodes, {"LVS_13": "PEER"}, probe_error={"PINNED"})
    lv = _LVol("RLV", node_id="PINNED", nodes=["PINNED", "PEER"])
    assert sr._receiving_leader_node(lv).get_id() == "PEER"


def test_falls_back_to_node_id_when_nodes_list_is_empty(monkeypatch):
    nodes = {"PINNED": _Node("PINNED")}
    _patch(monkeypatch, nodes, {"LVS_13": "PINNED"})
    lv = _LVol("RLV", node_id="PINNED", nodes=[])
    assert sr._receiving_leader_node(lv).get_id() == "PINNED"
