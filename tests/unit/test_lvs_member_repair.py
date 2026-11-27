"""bf36 R6 (2026-11-25): a member's copy of LVS_19 failed every registration for
10 hours, and replication into LVS_19 copied a volume in full 379 times.

The control plane now reloads such a member's copy of the lvstore
(controllers/lvs_member_repair) when the registration alert fires, under the
safety rules below, and reports the full copies the member causes.
"""
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core import constants
from simplyblock_core.controllers import lvs_member_repair as rep
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import tasks_runner_sync_lvol_del as runner

ONLINE = StorageNode.STATUS_ONLINE
ERR = "Failed to create BDev: LVOL_2907: No such file or directory"


def _node(nid, status=ONLINE, leads=False, rpc_ok=True, rpc_error=None, lvstore="", ask_error=None):
    rpc = MagicMock()
    if ask_error:
        rpc.get_lvstore.side_effect = ask_error
    else:
        rpc.get_lvstore.return_value = {"name": "LVS_19", "lvs leadership": leads}
    if rpc_error:
        rpc.bdev_lvol_update_lvstore.side_effect = rpc_error
    else:
        rpc.bdev_lvol_update_lvstore.return_value = rpc_ok
    n = SimpleNamespace(uuid=nid, get_id=lambda: nid, status=status, cluster_id="C",
                        lvstore=lvstore, secondary_node_id="", tertiary_node_id="",
                        rpc_client=lambda **kw: rpc)
    n.rpc = rpc
    return n


def _cluster_db(nodes, cluster_status=Cluster.STATUS_ACTIVE):
    db = MagicMock()
    db.get_cluster_by_id.return_value = SimpleNamespace(status=cluster_status)
    db.get_storage_nodes_by_cluster_id.return_value = list(nodes)
    return db


def _lvs19(member_kw=None, leader_kw=None, secondary_kw=None):
    """LVS_19: primary P leads, secondary S, tertiary T (the member repaired)."""
    leader = _node("P", lvstore="LVS_19", **{"leads": True, **(leader_kw or {})})
    sec = _node("S", **(secondary_kw or {}))
    member = _node("T", **(member_kw or {}))
    leader.secondary_node_id, leader.tertiary_node_id = "S", "T"
    db = _cluster_db([leader, sec, member])
    db.get_storage_node_by_id.side_effect = lambda i: {"P": leader, "S": sec, "T": member}[i]
    return member, leader, db


@pytest.fixture
def events():
    with patch.object(rep.storage_events, "snode_lvs_member_repair") as ev, \
            patch.object(rep.storage_node_ops, "get_restart_phase", return_value=""):
        yield ev


# ---------------- the repair and its safety rules ----------------

def test_a_non_leader_member_is_reloaded_while_another_member_leads(events):
    member, _leader, db = _lvs19()
    outcome, _ = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.REPAIRED
    member.rpc.bdev_lvol_update_lvstore.assert_called_once_with("LVS_19")
    assert [c.args[2] for c in events.call_args_list] == ["started", "done"]


def test_never_on_the_leader(events):
    member, _leader, db = _lvs19(member_kw={"leads": True})
    outcome, why = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.SKIPPED and "leads" in why
    member.rpc.bdev_lvol_update_lvstore.assert_not_called()
    events.assert_not_called()


def test_never_when_no_other_member_leads(events):
    # the broken copy must never be the only one serving the lvstore
    member, _leader, db = _lvs19(leader_kw={"leads": False})
    outcome, why = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.SKIPPED and "no other member" in why
    member.rpc.bdev_lvol_update_lvstore.assert_not_called()


def test_an_offline_leader_does_not_count(events):
    member, _leader, db = _lvs19(leader_kw={"status": StorageNode.STATUS_OFFLINE})
    outcome, _ = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.SKIPPED
    member.rpc.bdev_lvol_update_lvstore.assert_not_called()


def test_not_when_the_member_cannot_say_whether_it_leads(events):
    member, _leader, db = _lvs19(member_kw={"ask_error": RuntimeError("Could not reach remote")})
    outcome, why = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.SKIPPED and "does not answer" in why
    member.rpc.bdev_lvol_update_lvstore.assert_not_called()


@pytest.mark.parametrize("member_kw, cluster_status, phase, word", [
    ({"status": StorageNode.STATUS_RESTARTING}, Cluster.STATUS_ACTIVE, "", "node is "),
    ({}, Cluster.STATUS_IN_ACTIVATION, "", "activation"),
    ({}, Cluster.STATUS_ACTIVE, "pre_block", "owns the lvstore"),
])
def test_not_while_the_node_or_the_lvstore_is_busy(member_kw, cluster_status, phase, word):
    member, _leader, db = _lvs19(member_kw=member_kw)
    db.get_cluster_by_id.return_value = SimpleNamespace(status=cluster_status)
    with patch.object(rep.storage_events, "snode_lvs_member_repair"), \
            patch.object(rep.storage_node_ops, "get_restart_phase", return_value=phase):
        outcome, why = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.SKIPPED and word in why
    member.rpc.bdev_lvol_update_lvstore.assert_not_called()


@pytest.mark.parametrize("kw, detail", [
    ({"rpc_ok": False}, "returned False"),
    ({"rpc_error": RuntimeError("Operation not permitted")}, "Operation not permitted"),
])
def test_a_failed_reload_is_reported(events, kw, detail):
    member, _leader, db = _lvs19(member_kw=kw)
    outcome, why = rep.repair_member_copy(member, "LVS_19", db=db)
    assert outcome == rep.FAILED and detail in why
    assert [c.args[2] for c in events.call_args_list] == ["started", "failed"]


def test_the_event_says_when_a_previous_reload_did_not_help():
    node = SimpleNamespace(cluster_id="C", get_id=lambda: "T")
    with patch.object(rep.storage_events.ec, "log_event_cluster") as log:
        rep.storage_events.snode_lvs_member_repair(node, "LVS_19", "done", "reloaded",
                                                   previous_repair_at=1.0)
    msg = log.call_args.kwargs["message"]
    assert "did not stop the failures" in msg and "Restart this storage node" in msg
    assert log.call_args.kwargs["event_level"] == "Warning"


# ---------------- the runner: once per window, on the alert ----------------

@pytest.fixture
def clean_runner():
    for d in (runner._member_register_failures, runner._member_register_alerted, runner._member_repaired):
        d.clear()
    yield
    for d in (runner._member_register_failures, runner._member_register_alerted, runner._member_repaired):
        d.clear()


def _fail_until_alert(node, start):
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    fired = False
    for i in range(n):
        fired = runner._note_member_registration(node, "LVS_19", f"lv{start}-{i}", ERR, now=start + i)
    return fired


def test_the_alert_repairs_the_member(clean_runner):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing"), \
            patch.object(runner.lvs_member_repair, "repair_member_copy",
                         return_value=(rep.REPAIRED, "reloaded")) as repair:
        assert _fail_until_alert(node, 100)
    repair.assert_called_once()
    assert repair.call_args.args[:2] == (node, "LVS_19")
    assert repair.call_args.kwargs["previous_repair_at"] is None
    # a reload that worked restarts the count
    assert ("T", "LVS_19") not in runner._member_register_failures


def test_failing_again_after_a_reload_restarts_the_node(clean_runner):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    realert = constants.LVS_MEMBER_REGISTER_FAIL_REALERT_SEC
    assert constants.LVS_MEMBER_REPAIR_INTERVAL_SEC <= realert
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing"), \
            patch.object(runner.lvs_member_repair, "repair_member_copy",
                         return_value=(rep.REPAIRED, "reloaded")) as repair, \
            patch.object(runner.lvs_member_repair, "auto_restart_member",
                         return_value=(rep.RESTART_STARTED, "graceful shutdown")) as restart:
        _fail_until_alert(node, 100)
        # within the interval: nothing, even if asked directly
        assert runner._repair_member(node, "LVS_19", 100 + 60) is None
        # the failures come back in the next alert window: a restart, not a second reload
        _fail_until_alert(node, 100 + realert + 10)
    assert repair.call_count == 1
    restart.assert_called_once()
    assert restart.call_args.args[:2] == (node, "LVS_19")


def test_a_failed_reload_is_retried_as_a_reload(clean_runner):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    realert = constants.LVS_MEMBER_REGISTER_FAIL_REALERT_SEC
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing"), \
            patch.object(runner.lvs_member_repair, "repair_member_copy",
                         return_value=(rep.FAILED, "returned False")) as repair, \
            patch.object(runner.lvs_member_repair, "auto_restart_member") as restart:
        _fail_until_alert(node, 100)
        _fail_until_alert(node, 100 + realert + 10)
    assert repair.call_count == 2
    restart.assert_not_called()
    assert repair.call_args.kwargs["previous_repair_at"] == 100 + constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS - 1


def test_a_skipped_repair_is_tried_again_at_the_next_alert(clean_runner):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    with patch.object(runner.lvs_member_repair, "repair_member_copy",
                      return_value=(rep.SKIPPED, "no other member answers as leader")):
        assert runner._repair_member(node, "LVS_19", 100) == rep.SKIPPED
    assert ("T", "LVS_19") not in runner._member_repaired


def test_a_failed_repair_keeps_the_alert_count(clean_runner):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing"), \
            patch.object(runner.lvs_member_repair, "repair_member_copy",
                         return_value=(rep.FAILED, "returned False")):
        _fail_until_alert(node, 100)
    assert len(runner._member_register_failures[("T", "LVS_19")]) == constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    assert ("T", "LVS_19") in runner._member_repaired


# ---------------- replication: the full copies are reported ----------------

@pytest.fixture
def clean_sr():
    sr._member_blocks_delta.clear()
    yield
    sr._member_blocks_delta.clear()


def test_full_copies_caused_by_a_member_are_reported_once_per_period(clean_sr):
    node = SimpleNamespace(get_id=lambda: "T", cluster_id="C")
    period = constants.LVS_MEMBER_BLOCKS_DELTA_REPORT_SEC
    with patch.object(sr.storage_events, "snode_lvs_member_blocks_delta") as ev:
        for i in range(10):
            assert sr._note_member_blocks_delta(node, "LVS_19", "No such file", now=100 + i) is False
        assert sr._note_member_blocks_delta(node, "LVS_19", "No such file", now=100 + period) is True
        assert sr._note_member_blocks_delta(node, "LVS_19", "No such file", now=101 + period) is False
    ev.assert_called_once()
    _n, lvs, count, since, err = ev.call_args.args
    assert lvs == "LVS_19" and count == 11 and since == period and err == "No such file"


def test_the_delta_still_needs_every_member():
    # the full copy stays: an unchained member could lead (IO-driven) and read zeros
    src = inspect.getsource(sr._prechain_landing_volume)
    assert "_note_member_blocks_delta(node, remote_lv.lvs_name, why)" in src
    assert "if not (primary_ok and secondary_ok):" in src
