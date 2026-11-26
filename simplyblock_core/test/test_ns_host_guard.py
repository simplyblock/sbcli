"""A namespace is never removed from under a host that has no other path.

Bed bf26 (2026-11-17): vol-4 and vol-8 had lost their source paths; an
abandoned cutover then discarded its stale clone and removed the target
namespace, the hosts' only controllers, and the kernel deleted both block
devices for good.
"""
from types import SimpleNamespace

import pytest

from simplyblock_core.controllers import ns_host_guard
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCConnectionError, RPCRemoteError
from simplyblock_core.services import tasks_runner_replication_final as runner
from simplyblock_core.services.task_runner_base import TaskDefer

NQN = "nqn.2023-02.io.simplyblock:CL:lvol:V"
H1, H2 = "nqn.host:1", "nqn.host:2"


def _node(node_id, hosts=(), status=StorageNode.STATUS_ONLINE, error=None):
    def get_controllers(nqn):
        if error is not None:
            raise error
        return [{"hostnqn": h, "cntlid": i} for i, h in enumerate(hosts)]
    rpc = SimpleNamespace(nvmf_subsystem_get_controllers=get_controllers)
    return SimpleNamespace(get_id=lambda: node_id, status=status,
                           rpc_client=lambda **kw: rpc)


class _DB:
    def __init__(self, *nodes):
        self.nodes = {n.get_id(): n for n in nodes}

    def get_storage_node_by_id(self, node_id):
        return self.nodes[node_id]


@pytest.fixture
def guard(monkeypatch):
    # conftest neutralises removal_blocked for the other tests; restore it here
    import importlib
    real = importlib.reload(ns_host_guard)
    monkeypatch.setattr(runner, "ns_host_guard", real)
    return real


def test_a_host_on_both_sides_is_not_stranded(guard):
    db = _DB(_node("T1", [H1]), _node("S1", [H1]))
    assert guard.stranded_hosts(NQN, ["T1"], ["S1"], db=db) == set()
    assert guard.removal_blocked(NQN, ["T1"], ["S1"], "x", db=db) is None


def test_bed_bf26_a_host_only_on_the_side_that_goes_is_stranded(guard):
    db = _DB(_node("T1", [H1, H2]), _node("T2", [H1]), _node("S1", [H2]))
    assert guard.stranded_hosts(NQN, ["T1", "T2"], ["S1"], db=db) == {H1}
    why = guard.removal_blocked(NQN, ["T1", "T2"], ["S1"], "discarding clone C", db=db)
    assert H1 in why and "discarding clone C" in why


def test_bed_bf28_one_machine_under_two_host_nqns_is_not_stranded(guard):
    # Source paths carry the simplyblock host NQN, the early-prepared target
    # paths the kernel default: the same client, never stranded.
    sb = "nqn.2014-08.io.simplyblock:uuid:5892b49a-bc39-41b2-a38a-8daae13a9d0d"
    kernel = "nqn.2014-08.org.nvmexpress:uuid:ec2d55d6-1c18-ced7-92ee-34f409cb58ce"
    db = _DB(_node("S1", [sb]), _node("S2", [sb]), _node("T1", [kernel]), _node("T2", [kernel]))
    # the cutover freeze: the source paths go inaccessible, the target ones stay
    assert guard.stranded_hosts(NQN, ["S1", "S2"], ["T1", "T2"], db=db) == set()
    # a discarded target clone: the target paths go, the source ones stay
    assert guard.removal_blocked(NQN, ["T1", "T2"], ["S1", "S2"], "x", db=db) is None


def test_more_unmatched_hosts_going_than_staying_are_stranded(guard):
    # Two clients on the side that goes, one other name on the side that stays:
    # at least one machine loses its last path; both candidates are named.
    db = _DB(_node("T1", [H1, H2]), _node("S1", ["nqn.host:other"]))
    assert guard.stranded_hosts(NQN, ["T1"], ["S1"], db=db) == {H1, H2}


def test_nodes_that_are_down_or_lack_the_subsystem_serve_no_host(guard):
    db = _DB(_node("T1", [H1]),
             _node("S1", [H1], status=StorageNode.STATUS_OFFLINE),
             _node("S2", error=RPCRemoteError(code=-32602, message="no subsystem")))
    assert guard.stranded_hosts(NQN, ["T1"], ["S1", "S2"], db=db) == {H1}


def test_an_unreachable_node_blocks_rather_than_guesses(guard):
    db = _DB(_node("T1", error=RPCConnectionError("down")), _node("S1", [H1]))
    assert guard.stranded_hosts(NQN, ["T1"], ["S1"], db=db) is None
    assert "could not establish" in guard.removal_blocked(NQN, ["T1"], ["S1"], "x", db=db)


def test_no_host_on_the_side_that_goes_never_asks_the_other_side(guard):
    asked = []
    keep = _node("S1", [H1])
    keep.rpc_client = lambda **kw: asked.append("S1")
    db = _DB(_node("T1", []), keep)
    assert guard.stranded_hosts(NQN, ["T1"], ["S1"], db=db) == set()
    assert asked == []


def test_volume_node_ids_is_primary_then_peers_without_duplicates():
    lv = SimpleNamespace(node_id="N1", nodes=["N1", "N2", "", "N3"])
    assert ns_host_guard.volume_node_ids(lv) == ["N1", "N2", "N3"]


# --- the cutover runner -----------------------------------------------------

def test_a_stranded_host_keeps_the_stale_clone(monkeypatch):
    clone = SimpleNamespace(get_id=lambda: "C1", status="online", nqn=NQN,
                            node_id="T1", nodes=["T2"])
    deleted = []
    monkeypatch.setattr(runner, "db", SimpleNamespace(get_lvol_by_id=lambda i: clone))
    monkeypatch.setattr(runner.ns_host_guard, "removal_blocked",
                        lambda *a, **kw: "host nqn.host:1 reaches it only here")
    import simplyblock_core.controllers.lvol_controller as lc
    monkeypatch.setattr(lc, "delete_lvol", lambda lv, **kw: deleted.append(lv.get_id()))
    with pytest.raises(runner.CloneRemovalBlocked):
        runner._delete_cutover_clone("C1", "SRC")
    assert deleted == []


def test_the_discard_defers_on_a_stranded_host_and_never_aborts(monkeypatch):
    task = SimpleNamespace(function_params={
        "lvol_id": "SRC", "discard_tgt_lvol_id": "C1", "discard_after": 0})

    def blocked(clone_id, src):
        raise runner.CloneRemovalBlocked("host nqn.host:1 reaches it only here")
    monkeypatch.setattr(runner, "_delete_cutover_clone", blocked)
    monkeypatch.setattr(runner, "_revert_cutover_relationship", lambda rid: True)
    monkeypatch.setattr(runner, "_record", lambda t, **kw: t)
    with pytest.raises(TaskDefer, match="reaches it only here"):
        runner._discard_failed_cutover(task)


def test_the_freeze_waits_for_every_source_host_on_the_target(monkeypatch):
    calls = []

    def blocked(nqn, removing, keeping, what, db=None):
        calls.append((nqn, list(removing), list(keeping), what))
        return f"{what}: host nqn.host:1 has no target controller"
    monkeypatch.setattr(runner.ns_host_guard, "removal_blocked", blocked)
    # The check sits right before freeze_started_at is recorded.
    import inspect
    src = inspect.getsource(runner)
    i = src.index("cutover freeze of")
    assert i < src.index("freeze_started_at=time.time()")


def test_bed_bf29_the_source_delete_finds_the_target_by_nqn_after_the_clone_is_gone(monkeypatch):
    # After the cutover the clone named in tgt_lvol_id no longer exists; the
    # volume runs on the other cluster under a new record with the same NQN.
    src = SimpleNamespace(get_id=lambda: "SRC", nqn=NQN, node_id="S1", nodes=["S2"],
                          status="online")
    live = SimpleNamespace(get_id=lambda: "NEW", nqn=NQN, node_id="T1", nodes=["T2", "T3"],
                           status="online")
    other = SimpleNamespace(get_id=lambda: "X", nqn="nqn.other", node_id="T9", nodes=[],
                            status="online")
    going = SimpleNamespace(get_id=lambda: "OLD", nqn=NQN, node_id="T7", nodes=[],
                            status="in_deletion")
    vols = {"A": [src], "B": [live, other, going]}

    def get_lvol_by_id(i):
        raise KeyError(i)
    monkeypatch.setattr(runner, "db", SimpleNamespace(
        get_lvol_by_id=get_lvol_by_id,
        get_clusters=lambda: [SimpleNamespace(get_id=lambda: "A"), SimpleNamespace(get_id=lambda: "B")],
        get_lvols=lambda cid: vols[cid]))
    assert runner._live_target_node_ids(src, {"tgt_lvol_id": "GONE"}) == ["T1", "T2", "T3"]
