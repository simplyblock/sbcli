"""Replication must also read from the SOURCE lvstore's current leader.

Lab run 19, case 6 (source primary offline, secondary survives): every
replication task parked on "node is not online, retrying" for the whole
outage — 10 replications before the outage, 10 during. A snapshot is
registered on every member of its lvstore, and the promoted peer serves the
volume, so waiting for the recorded primary stalls replication for no reason.
This is the source-side mirror of the target-side pinning fixed for case 5.
"""
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr


class _LVol:
    def __init__(self, node_id, nodes, lvs="LVS_1"):
        self.node_id = node_id
        self.nodes = nodes
        self.lvs_name = lvs

    def get_id(self):
        return "LV1"


class _Snap:
    def __init__(self, lvol):
        self.lvol = lvol
        self.snap_bdev = "LVS_1/SNAP_1"

    def get_id(self):
        return "SNAP1"


class _Node:
    def __init__(self, uuid, status=StorageNode.STATUS_ONLINE):
        self.uuid = uuid
        self.status = status

    def get_id(self):
        return self.uuid


def _patch(monkeypatch, nodes, leader_of):
    class _DB:
        def get_storage_node_by_id(self, uuid):
            if uuid in nodes:
                return nodes[uuid]
            raise KeyError(uuid)

    import simplyblock_core.controllers.lvol_controller as lc
    monkeypatch.setattr(sr, "db", _DB())
    monkeypatch.setattr(lc, "is_node_leader",
                        lambda node, lvs: leader_of.get(lvs) == node.get_id())


def test_uses_the_promoted_secondary_when_the_primary_is_down(monkeypatch):
    nodes = {"PRIMARY": _Node("PRIMARY", status=StorageNode.STATUS_OFFLINE),
             "SECONDARY": _Node("SECONDARY")}
    _patch(monkeypatch, nodes, {"LVS_1": "SECONDARY"})
    snap = _Snap(_LVol("PRIMARY", ["PRIMARY", "SECONDARY"]))

    node = sr._source_leader_node(snap)
    assert node is not None and node.get_id() == "SECONDARY", (
        "replication stalls for the whole outage if it waits for the primary")


def test_prefers_the_recorded_primary_while_it_leads(monkeypatch):
    nodes = {"PRIMARY": _Node("PRIMARY"), "SECONDARY": _Node("SECONDARY")}
    _patch(monkeypatch, nodes, {"LVS_1": "PRIMARY"})
    snap = _Snap(_LVol("PRIMARY", ["PRIMARY", "SECONDARY"]))
    assert sr._source_leader_node(snap).get_id() == "PRIMARY"


def test_none_when_no_member_leads(monkeypatch):
    """Whole lvstore leaderless -> caller suspends and retries, no RPC."""
    nodes = {"PRIMARY": _Node("PRIMARY"), "SECONDARY": _Node("SECONDARY")}
    _patch(monkeypatch, nodes, {"LVS_1": "SOMEONE_ELSE"})
    snap = _Snap(_LVol("PRIMARY", ["PRIMARY", "SECONDARY"]))
    assert sr._source_leader_node(snap) is None


def test_offline_leader_is_not_selected(monkeypatch):
    nodes = {"PRIMARY": _Node("PRIMARY", status=StorageNode.STATUS_OFFLINE),
             "SECONDARY": _Node("SECONDARY")}
    # Stale DB view still names the dead node as leader.
    _patch(monkeypatch, nodes, {"LVS_1": "PRIMARY"})
    snap = _Snap(_LVol("PRIMARY", ["PRIMARY", "SECONDARY"]))
    assert sr._source_leader_node(snap) is None


def test_single_node_volume_falls_back_to_node_id(monkeypatch):
    nodes = {"ONLY": _Node("ONLY")}
    _patch(monkeypatch, nodes, {"LVS_1": "ONLY"})
    snap = _Snap(_LVol("ONLY", []))
    assert sr._source_leader_node(snap).get_id() == "ONLY"


def test_target_and_source_helpers_share_one_implementation():
    """Both sides must stay in step; they had drifted into two rules."""
    import inspect
    src = inspect.getsource(sr._receiving_leader_node)
    assert "_lvs_leader_among" in src
    assert "_lvs_leader_among" in inspect.getsource(sr._source_leader_node)
