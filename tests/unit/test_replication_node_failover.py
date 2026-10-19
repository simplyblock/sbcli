"""Async replication keeps going when source or target nodes go away.

Source: the transfer is driven by the first ONLINE member of the source
lvstore that holds the snapshot (primary, secondary, tertiary); leadership is
irrelevant, and a lost member restarts the snapshot on the next one.
Target: transfer and convert go only to a SETTLED leader of the target
lvstore; a leadership move restarts the replication on the new leader.
See docs/replication-node-failover.md.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr

ONLINE = StorageNode.STATUS_ONLINE
OFFLINE = StorageNode.STATUS_OFFLINE
IS_LEADER = "simplyblock_core.controllers.lvol_controller.is_node_leader"


def _node(node_id, status=ONLINE, has_snap=True, rpc_raises=None):
    rpc = MagicMock()
    if rpc_raises:
        rpc.bdev_get.side_effect = rpc_raises
    else:
        rpc.bdev_get.return_value = {"name": "x"} if has_snap else None
    return SimpleNamespace(get_id=lambda: node_id, status=status,
                           rpc_client=lambda timeout=None: rpc, cluster_id="CL",
                           transfer_hublvol=None)


class _DB:
    def __init__(self, nodes, lvols=None, snapshot=None):
        self.nodes = {n.get_id(): n for n in nodes}
        self.lvols = lvols or {}
        self.snapshot = snapshot

    def get_storage_node_by_id(self, node_id):
        return self.nodes[node_id]

    def get_lvol_by_id(self, lvol_id):
        return self.lvols[lvol_id]

    def get_snapshot_by_id(self, _id):
        return self.snapshot


def _snapshot(primary="P", nodes=("P", "S", "T"), lvs="LVS_1"):
    lvol = SimpleNamespace(node_id=primary, nodes=list(nodes), lvs_name=lvs,
                           get_id=lambda: "lvol-1")
    return SimpleNamespace(lvol=lvol, snap_bdev=f"{lvs}/SNAP_1", get_id=lambda: "snap-1",
                           status="in_replication")


def _task(**params):
    t = SimpleNamespace(uuid="t-1", retry=0, max_retry=8, canceled=False,
                        status=JobSchedule.STATUS_RUNNING, function_result="",
                        cluster_id="CL", function_params=dict(snapshot_id="snap-1", **params))
    t.write_to_db = lambda *a, **k: None
    t.get_id = lambda: "t-1"
    return t


@pytest.fixture(autouse=True)
def _no_settle_wait(monkeypatch):
    monkeypatch.setattr(constants, "REPL_LEADER_SETTLE_SEC", 0)
    sr._SETTLED_LEADER.clear()


# ---- source member selection --------------------------------------------------

@pytest.mark.parametrize("states,expected", [
    ({"P": ONLINE, "S": ONLINE, "T": ONLINE}, "P"),
    ({"P": OFFLINE, "S": ONLINE, "T": ONLINE}, "S"),
    ({"P": OFFLINE, "S": OFFLINE, "T": ONLINE}, "T"),
])
def test_first_online_member_in_role_order_drives_the_transfer(states, expected):
    db = _DB([_node(i, s) for i, s in states.items()])
    with patch.object(sr, "db", db):
        node, why = sr._select_source_node(_snapshot())
    assert node.get_id() == expected
    if expected != "P":
        assert "skipped" in why and "primary" in why


def test_no_online_member_defers():
    db = _DB([_node(i, OFFLINE) for i in "PST"])
    with patch.object(sr, "db", db):
        node, why = sr._select_source_node(_snapshot())
    assert node is None
    assert "offline" in why


def test_a_member_without_the_snapshot_or_unreachable_is_skipped():
    db = _DB([_node("P", has_snap=False), _node("S", rpc_raises=RuntimeError("timeout")),
              _node("T")])
    with patch.object(sr, "db", db):
        node, why = sr._select_source_node(_snapshot())
    assert node.get_id() == "T"
    assert "lacks" in why and "unreachable" in why


def test_leadership_is_not_consulted_on_the_source():
    db = _DB([_node(i) for i in "PST"])
    with patch.object(sr, "db", db), \
            patch(IS_LEADER, side_effect=AssertionError("leadership must not matter on the source")):
        node, _ = sr._select_source_node(_snapshot())
    assert node.get_id() == "P"


def test_group_members_on_one_lvstore_choose_the_same_member():
    """Consistency-group members share the lvstore and its role order, so the
    same node states give every member the same source."""
    db = _DB([_node("P", OFFLINE), _node("S"), _node("T")])
    with patch.object(sr, "db", db):
        a = sr._select_source_node(_snapshot())[0].get_id()
        b = sr._select_source_node(_snapshot())[0].get_id()
    assert a == b == "S"


# ---- a running transfer loses its source member --------------------------------

def _running(task, db):
    db.snapshot = db.snapshot or _snapshot()
    with patch.object(sr, "db", db):
        return sr.task_runner(task)


def test_source_member_going_offline_mid_transfer_restarts_elsewhere_without_a_retry():
    task = _task(source_node_id="P", offset=4096, remote_lvol_id="R")
    db = _DB([_node("P", OFFLINE), _node("S"), _node("T")])
    assert _running(task, db) is False
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0
    assert "offset" not in task.function_params
    assert task.function_params["node_switches"] == 1
    assert "source member P" in task.function_result


def test_a_flapping_member_eventually_spends_retries():
    task = _task(source_node_id="P", node_switches=constants.REPL_MAX_NODE_SWITCHES)
    db = _DB([_node("P", OFFLINE), _node("S")])
    _running(task, db)
    assert task.retry == 1


def test_a_switch_restarts_the_snapshot_from_offset_zero():
    task = _task(source_node_id="P", offset=1 << 20)
    assert sr._note_switch(task, "source_node_id", "S", "source member") is True
    assert "offset" not in task.function_params
    assert task.function_params["source_node_id"] == "S"
    same = _task(source_node_id="S", offset=1 << 20)
    assert sr._note_switch(same, "source_node_id", "S", "source member") is False
    assert same.function_params["offset"] == 1 << 20


# ---- target leadership --------------------------------------------------------

def _remote(nodes=("TP", "TS", "TT")):
    return SimpleNamespace(node_id=nodes[0], nodes=list(nodes), lvs_name="LVS_9", cluster_id="CL2")


def _rounds(sequence):
    """is_node_leader answering from a sequence of leader-id sets, one set per
    probe round over the members."""
    rounds = iter(sequence)
    state = {"cur": None, "seen": set()}

    def is_leader(node, lvs):
        nid = node.get_id()
        if state["cur"] is None or nid in state["seen"]:
            state["cur"] = next(rounds)
            state["seen"] = set()
        state["seen"].add(nid)
        return nid in state["cur"]
    return is_leader


def test_a_settled_single_leader_is_used():
    db = _DB([_node(i) for i in ("TP", "TS", "TT")])
    with patch.object(sr, "db", db), patch(IS_LEADER, side_effect=_rounds([{"TS"}, {"TS"}])):
        node, why = sr._stable_target_leader(_remote())
    assert node.get_id() == "TS" and why == ""


@pytest.mark.parametrize("rounds,fragment", [
    ([{"TP"}, {"TS"}], "moving"),
    ([{"TP"}, set()], "moving"),
    ([set()], "0 members"),
    ([{"TP", "TS"}], "2 members"),
])
def test_leadership_in_flux_or_ambiguous_gets_no_transfer(rounds, fragment):
    db = _DB([_node(i) for i in ("TP", "TS", "TT")])
    with patch.object(sr, "db", db), patch(IS_LEADER, side_effect=_rounds(rounds)):
        node, why = sr._stable_target_leader(_remote())
    assert node is None
    assert fragment in why


def test_a_recently_settled_leader_needs_no_second_wait(monkeypatch):
    db = _DB([_node(i) for i in ("TP", "TS", "TT")])
    slept = []
    monkeypatch.setattr(sr.time, "sleep", lambda s: slept.append(s))
    with patch.object(sr, "db", db), \
            patch(IS_LEADER, side_effect=_rounds([{"TS"}, {"TS"}, {"TS"}])):
        assert sr._stable_target_leader(_remote())[0].get_id() == "TS"
        assert sr._stable_target_leader(_remote())[0].get_id() == "TS"
    assert len(slept) == 1


def test_failed_transfer_after_target_leadership_moved_restarts_on_the_new_leader():
    task = _task(source_node_id="P", target_node_id="TP", remote_lvol_id="R", offset=8192)
    src = _node("P")
    src.rpc_client().bdev_lvol_transfer_stat.return_value = {"transfer_state": "Failed",
                                                             "offset": 8192}
    db = _DB([src, _node("S"), _node("T")] + [_node(i) for i in ("TP", "TS", "TT")],
             lvols={"R": _remote()})
    with patch(IS_LEADER, side_effect=lambda node, lvs: node.get_id() == "TS"):
        assert _running(task, db) is False
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0
    assert "moved from TP to TS" in task.function_result
    assert "offset" not in task.function_params


def test_failed_transfer_with_unchanged_leader_is_an_ordinary_retry():
    task = _task(source_node_id="P", target_node_id="TP", remote_lvol_id="R")
    src = _node("P")
    src.rpc_client().bdev_lvol_transfer_stat.return_value = {"transfer_state": "Failed",
                                                             "offset": 0}
    db = _DB([src] + [_node(i) for i in ("TP", "TS", "TT")], lvols={"R": _remote()})
    with patch(IS_LEADER, side_effect=lambda node, lvs: node.get_id() == "TP"):
        _running(task, db)
    assert task.retry == 1
    assert "transfer failed at offset 0" in task.function_result


def test_finish_waits_while_target_leadership_is_unsettled():
    task = _task(remote_lvol_id="R", target_node_id="TP")
    db = MagicMock()
    db.get_lvol_by_id.return_value = _remote()
    with patch.object(sr, "db", db), \
            patch.object(sr, "_stable_target_leader", return_value=(None, "leadership moving")), \
            patch.object(sr, "_resolve_chain_target") as chain:
        assert sr.process_snap_replicate_finish(task, MagicMock()) is False
    chain.assert_not_called()


def test_start_never_transfers_without_a_settled_target_leader():
    """The gate before the transfer: two members claim leadership -> no
    recovery (it is not leaderless), no transfer, wait without a retry."""
    task = _task(source_node_id="P", remote_lvol_id="R", replicate_to_source=False)
    task.status = JobSchedule.STATUS_SUSPENDED
    snap = _snapshot()
    src = _node("P")
    db = MagicMock()
    db.get_lvol_by_id.return_value = _remote()
    with patch.object(sr, "db", db), \
            patch.object(sr, "_lvs_transfer_hold", return_value=None), \
            patch.object(sr, "_select_source_node", return_value=(src, "primary P")), \
            patch.object(sr, "_unreplicated_local_ancestor", return_value=("ok", None, "")), \
            patch.object(sr, "_stable_target_leader",
                         return_value=(None, "2 members of LVS_9 report leadership")), \
            patch.object(sr, "_leaders_of", return_value=[MagicMock(), MagicMock()]), \
            patch.object(sr, "_recover_target_leader") as recover:
        sr.process_snap_replicate_start(task, snap)
    recover.assert_not_called()
    src.rpc_client().bdev_lvol_transfer.assert_not_called()
    assert task.status == JobSchedule.STATUS_SUSPENDED
    assert task.retry == 0
    assert "target leadership not established" in task.function_result


def test_a_leaderless_target_runs_the_recovery_then_requires_it_settled():
    task = _task(source_node_id="P", remote_lvol_id="R", replicate_to_source=False)
    task.status = JobSchedule.STATUS_SUSPENDED
    src = _node("P")
    db = MagicMock()
    db.get_lvol_by_id.return_value = _remote()
    stable = MagicMock(side_effect=[(None, "0 members"), (None, "leadership of LVS_9 is moving")])
    with patch.object(sr, "db", db), \
            patch.object(sr, "_lvs_transfer_hold", return_value=None), \
            patch.object(sr, "_select_source_node", return_value=(src, "primary P")), \
            patch.object(sr, "_unreplicated_local_ancestor", return_value=("ok", None, "")), \
            patch.object(sr, "_stable_target_leader", stable), \
            patch.object(sr, "_leaders_of", return_value=[]), \
            patch.object(sr, "_recover_target_leader", return_value=MagicMock()) as recover:
        sr.process_snap_replicate_start(task, _snapshot())
    recover.assert_called_once()
    assert stable.call_count == 2
    src.rpc_client().bdev_lvol_transfer.assert_not_called()
    assert "moving" in task.function_result
