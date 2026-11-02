"""suspend_lvol / resume_lvol set the ANA state on EVERY member of the lvstore,
the tertiary included.

bf10 (2026-11-02): a replication cutover exposes the target clone and calls
suspend_lvol so that its paths stay inaccessible until the final step flips
ANA. It hid the primary's and the secondary's paths only; the host wrote
through the tertiary's (non_optimized) path while the final delta was in
transfer, and those writes, forwarded over the hublvol, timed out and moved
the target lvstore's leadership six times."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.controllers import lvol_controller as lc
from simplyblock_core.models.storage_node import StorageNode


def _node(nid, status=StorageNode.STATUS_ONLINE, sec=None, ter=None):
    n = SimpleNamespace(
        status=status, secondary_node_id=sec, tertiary_node_id=ter,
        data_nics=[SimpleNamespace(ip4_address=f"10.0.0.{len(nid)}", trtype="TCP",
                                   listen_address=f"10.0.0.{len(nid)}")])
    n.get_id = lambda nid=nid: nid
    n.rpc = MagicMock()
    n.rpc.nvmf_subsystem_listener_set_ana_state.return_value = True
    n.rpc_client = lambda **kw: n.rpc
    return n


@pytest.fixture
def lvs():
    nodes = {"P": _node("P", sec="S", ter="T"), "S": _node("SS"), "T": _node("TTT")}
    lvol = SimpleNamespace(nqn="nqn.x:lvol:v", fabric="tcp", subsys_port=4438, ns_id=1,
                           node_id="P", get_id=lambda: "v")
    db = MagicMock()
    db.get_lvol_by_id.return_value = lvol
    db.get_storage_node_by_id.side_effect = lambda nid: nodes[nid]
    with patch.object(lc, "DBController", return_value=db):
        yield nodes


def _states(node):
    out = []
    for c in node.rpc.nvmf_subsystem_listener_set_ana_state.call_args_list:
        if c.kwargs.get("ana") == "inaccessible":
            out.append("inaccessible")
        else:
            out.append("optimized" if c.kwargs.get("is_optimized") else "non_optimized")
    return out


def test_suspend_hides_every_member_the_tertiary_included(lvs):
    assert lc.suspend_lvol("v") is True
    assert [_states(lvs[n]) for n in "PST"] == [["inaccessible"]] * 3


def test_resume_reopens_every_member(lvs):
    assert lc.resume_lvol("v") is True
    assert _states(lvs["P"]) == ["optimized"]
    assert _states(lvs["S"]) == ["non_optimized"]
    assert _states(lvs["T"]) == ["non_optimized"]


def test_an_offline_tertiary_is_skipped_a_down_one_is_not(lvs):
    lvs["T"].status = StorageNode.STATUS_OFFLINE
    assert lc.suspend_lvol("v") is True
    assert _states(lvs["T"]) == []
    lvs["T"].status = StorageNode.STATUS_DOWN
    assert lc.suspend_lvol("v") is True
    assert _states(lvs["T"]) == ["inaccessible"]


def test_a_two_member_lvstore_still_works(lvs):
    lvs["P"].tertiary_node_id = None
    assert lc.suspend_lvol("v") is True
    assert [_states(lvs[n]) for n in "PS"] == [["inaccessible"]] * 2
    assert _states(lvs["T"]) == []


def test_a_failed_rpc_reports_failure(lvs):
    lvs["T"].rpc.nvmf_subsystem_listener_set_ana_state.return_value = False
    assert lc.suspend_lvol("v") is False
