"""No node is made leader of an lvstore while another member might still lead.

bf8 (2026-11-02, LVS_10): a forced shutdown leaves the old primary's SPDK
running ~10 s after its management RPC stops answering. The failover treated
"RPC fails" as "not leader", signalled it to drop leadership (best effort),
and accepted the secondary as soon as the secondary reported leadership. Both
led the lvstore for 12 s (JM write-lock conflict), and the lvstore's metadata
was corrupted.
"""
import inspect
from types import SimpleNamespace

import pytest

from simplyblock_core import storage_node_ops as sno
from simplyblock_core.controllers import lvol_controller


class _Node:
    def __init__(self, nid, leader=False, rpc_ok=True, spdk_up=True, agent_ok=True):
        self._id, self.leader, self.rpc_ok = nid, leader, rpc_ok
        self.spdk_up, self.agent_ok = spdk_up, agent_ok
        self.rpc_port, self.cluster_id = 4420, "CL"

    def get_id(self):
        return self._id

    def client(self, **kw):
        if not self.agent_ok:
            raise RuntimeError("agent unreachable")
        return SimpleNamespace(spdk_process_is_up=lambda port, cid: (self.spdk_up, ""))


@pytest.fixture(autouse=True)
def env(monkeypatch):
    def is_node_leader(node, lvs):
        if not node.rpc_ok:
            raise RuntimeError("rpc timeout")
        return node.leader
    monkeypatch.setattr(lvol_controller, "is_node_leader", is_node_leader)
    monkeypatch.setattr(sno.time, "sleep", lambda s: None)
    monkeypatch.setattr(sno.constants, "LEADERSHIP_SAFETY_WINDOW_SEC", 10)


def test_a_member_answering_non_leader_is_proven():
    assert sno._member_proven_non_leader(_Node("A"), "LVS_10")[0]


def test_a_member_answering_leader_blocks():
    assert not sno._member_proven_non_leader(_Node("A", leader=True), "LVS_10")[0]


def test_rpc_failing_but_spdk_still_running_is_a_possible_leader():
    # exactly the in_shutdown old primary of bf8
    ok, why = sno._member_proven_non_leader(_Node("A", rpc_ok=False, spdk_up=True), "LVS_10")
    assert not ok and "still running" in why


def test_rpc_failing_and_agent_unreachable_is_a_possible_leader():
    ok, why = sno._member_proven_non_leader(_Node("A", rpc_ok=False, agent_ok=False), "LVS_10")
    assert not ok and "could not confirm" in why


def test_rpc_failing_and_spdk_confirmed_gone_is_proven():
    assert sno._member_proven_non_leader(_Node("A", rpc_ok=False, spdk_up=False), "LVS_10")[0]


def test_taker_is_free_only_when_every_other_member_is_proven_twice(monkeypatch):
    taker = _Node("T")
    old = _Node("P", rpc_ok=False, spdk_up=False)
    tert = _Node("X")
    slept = []
    monkeypatch.setattr(sno.time, "sleep", lambda s: slept.append(s))
    assert sno._leadership_proven_free([old, taker, tert], "LVS_10", taker)
    assert slept == [10]


def test_a_member_that_regains_leadership_within_the_window_blocks(monkeypatch):
    taker = _Node("T")
    old = _Node("P")
    monkeypatch.setattr(sno.time, "sleep", lambda s: setattr(old, "leader", True))
    assert not sno._leadership_proven_free([old, taker], "LVS_10", taker)


def test_the_in_shutdown_old_primary_blocks_the_failover():
    taker = _Node("T")
    old = _Node("P", rpc_ok=False, spdk_up=True)
    assert not sno._leadership_proven_free([old, taker], "LVS_10", taker)


def test_the_force_branch_no_longer_signals_and_requires_proof():
    src = inspect.getsource(sno._find_leader_with_failover_impl)
    assert "bdev_lvol_set_lvs_signal" not in src
    i = src.index("Leader fabric healthy but RPC failing")
    tail = src[i:]
    assert tail.index("_leadership_proven_free(") < tail.index("_recover_leaderless_lvs(")


def test_the_leaderless_grant_requires_proof_before_set_leader():
    src = inspect.getsource(sno._recover_leaderless_lvs)
    assert src.index("_leadership_proven_free(") < src.index("bdev_lvol_set_leader(")
