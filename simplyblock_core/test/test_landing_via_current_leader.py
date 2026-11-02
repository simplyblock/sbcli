"""A replication landing volume is created through the current leader of its
lvstore while that lvstore's primary is down.

bf8 (2026-11-02, round 4, tgt-mid): with the target primary held down, every
replication cycle failed with "Error creating remote lvol" for 21 minutes:
add_lvol_ha refused the landing volume because its host -- the lvstore's
primary -- was not online, although the secondary led the lvstore.
"""
import ast
import inspect
import textwrap
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import lvol_controller as lc
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication


def _node(status, sec="SEC", ter="TER"):
    return SimpleNamespace(status=status, secondary_node_id=sec, tertiary_node_id=ter,
                           get_id=lambda: "PRI", lvstore="LVS_16")


@pytest.fixture
def peers(monkeypatch):
    state = {"SEC": StorageNode.STATUS_ONLINE, "TER": StorageNode.STATUS_ONLINE}

    class _DB:
        def get_storage_node_by_id(self, nid):
            if nid not in state:
                raise KeyError(nid)
            return SimpleNamespace(status=state[nid])

    monkeypatch.setattr(lc, "DBController", _DB)
    return state


@pytest.mark.parametrize("status", [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE,
                                    StorageNode.STATUS_DOWN, StorageNode.STATUS_IN_SHUTDOWN])
def test_internal_ha_volume_may_go_through_another_member(peers, status):
    assert lc._may_create_via_current_leader(_node(status), "ha", True, True)


def test_the_secondary_must_be_online_even_if_the_tertiary_is(peers):
    # Target primary AND secondary down: replication stops, RPO grows (the
    # tertiary alone does not take a landing volume).
    peers["SEC"] = StorageNode.STATUS_OFFLINE
    assert not lc._may_create_via_current_leader(_node(StorageNode.STATUS_OFFLINE), "ha", True, True)


def test_secondary_online_tertiary_down_is_enough(peers):
    peers["TER"] = StorageNode.STATUS_OFFLINE
    assert lc._may_create_via_current_leader(_node(StorageNode.STATUS_OFFLINE), "ha", True, True)


@pytest.mark.parametrize("ha_type,internal,via", [
    ("ha", False, True),    # a client volume keeps the old rule
    ("ha", True, False),    # only when the caller asks for it
    ("single", True, True),  # no other member to go through
])
def test_only_asked_for_internal_ha_volumes(peers, ha_type, internal, via):
    assert not lc._may_create_via_current_leader(_node(StorageNode.STATUS_OFFLINE), ha_type, internal, via)


@pytest.mark.parametrize("status", [StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_CREATION,
                                    StorageNode.STATUS_REMOVED])
def test_not_while_a_restart_owns_the_lvstore_or_the_node_is_gone(peers, status):
    assert not lc._may_create_via_current_leader(_node(status), "ha", True, True)


def test_the_online_gate_consults_the_helper():
    src = textwrap.dedent(inspect.getsource(lc.add_lvol_ha))
    gate = src.index("Storage node is not online")
    assert "_may_create_via_current_leader(" in src[gate - 400:gate]


def test_replication_asks_for_it_for_landing_volumes():
    tree = ast.parse(textwrap.dedent(inspect.getsource(snapshot_replication)))
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and getattr(n.func, "attr", "") == "add_lvol_ha"]
    assert calls
    for c in calls:
        kw = {k.arg: k.value for k in c.keywords}
        for name in ("via_current_leader", "internal"):
            node = kw.get(name)
            assert isinstance(node, ast.Constant) and node.value is True, name
