"""connect_lvol refuses a host while a consistency-group subsystem move holds
the volume's subsystem (design-cg-live-subsystem-flip.md §8.3, §9).

A host that connected during the move would hold a path the operator never
recorded: it is not suspended for the window, so its I/O fails when the
namespace leaves, and after the commit it still points at the source. The
attach already has this guard (add_lvol); the connect is the other way a host
reaches a moving subsystem.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import lvol_controller
from simplyblock_core.exceptions import SubsystemMoveInProgressError
from simplyblock_core.models.lvol_model import LVol

NQN = "nqn.2023-02.io.simplyblock:cluster:lvol:src"


def _lvol():
    return SimpleNamespace(
        status=LVol.STATUS_ONLINE, nqn=NQN, node_id="node-1",
        sync_never_connected=False, ns_uuid="", get_id=lambda: "vol-1")


@pytest.fixture()
def world(monkeypatch):
    db = MagicMock()
    db.get_lvol_by_id.return_value = _lvol()
    monkeypatch.setattr(lvol_controller, "DBController", lambda: db)
    monkeypatch.setattr(lvol_controller, "_connect_site", lambda *_: None)
    monkeypatch.setattr(lvol_controller.HostConnectAuth, "resolve", staticmethod(lambda *_: None))
    monkeypatch.setattr(lvol_controller, "_connect_path_volumes", lambda *_: [])
    marked = MagicMock()
    monkeypatch.setattr(lvol_controller, "_mark_connected", marked)
    return SimpleNamespace(db=db, marked=marked)


def test_a_moving_subsystem_refuses_the_connect_before_any_side_effect(world):
    with patch.object(lvol_controller, "_moving_nqns_on_node", return_value={NQN}) as moving:
        with pytest.raises(SubsystemMoveInProgressError, match="subsystem move holds"):
            lvol_controller.connect_lvol("vol-1", host_nqn="nqn.host")
    moving.assert_called_once_with("node-1")
    world.marked.assert_not_called()


def test_a_subsystem_no_move_holds_connects_as_before(world):
    with patch.object(lvol_controller, "_moving_nqns_on_node", return_value={"nqn.other"}):
        entries, err = lvol_controller.connect_lvol("vol-1", host_nqn="nqn.host")
    assert err is None
    assert entries == []


def test_a_host_not_allowed_is_answered_before_the_move_is_consulted(world, monkeypatch):
    def refuse(*_):
        raise ValueError("Host NQN nqn.host not found in allowed hosts")
    monkeypatch.setattr(lvol_controller.HostConnectAuth, "resolve", staticmethod(refuse))
    with patch.object(lvol_controller, "_moving_nqns_on_node") as moving:
        ok, reason = lvol_controller.connect_lvol("vol-1", host_nqn="nqn.host")
    assert ok is False and "allowed hosts" in reason
    moving.assert_not_called()
