"""bf36 (2026-11-25): one lvstore member failed every registration for 10 hours.

A tertiary that had been shut down read the leader's reused metadata pages as
zero after it came back, so every volume created on its lvstore failed to
register there (411 failures). The per-task alert never fired: each failure was
a different, short-lived replication landing volume. The sync-op runner now
counts failures per (member, lvstore) across volumes and raises one event.
"""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from simplyblock_core import constants
from simplyblock_core.services import tasks_runner_sync_lvol_del as runner

ERR = "Failed to create BDev: LVOL_2907: No such file or directory"


@pytest.fixture(autouse=True)
def _clean():
    runner._member_register_failures.clear()
    runner._member_register_alerted.clear()
    yield
    runner._member_register_failures.clear()
    runner._member_register_alerted.clear()


def _node(nid="119f3891"):
    return SimpleNamespace(get_id=lambda: nid, cluster_id="C")


def test_alerts_once_enough_different_volumes_failed_on_one_member():
    node = _node()
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(n - 1):
            assert runner._note_member_registration(node, "LVS_19", f"lv{i}", ERR, now=100 + i) is False
        assert runner._note_member_registration(node, "LVS_19", "lvN", ERR, now=200) is True
    ev.assert_called_once()
    args = ev.call_args.args
    assert args[1] == "LVS_19" and args[2] == n and ERR in args[4]


def test_one_volume_retrying_is_not_a_member_problem():
    node = _node()
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(20):
            runner._note_member_registration(node, "LVS_19", "lv0", ERR, now=100 + i)
    ev.assert_not_called()


def test_it_does_not_repeat_within_the_realert_interval():
    node = _node()
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(n + 10):
            runner._note_member_registration(node, "LVS_19", f"lv{i}", ERR, now=100 + i)
        assert ev.call_count == 1
        late = 100 + constants.LVS_MEMBER_REGISTER_FAIL_REALERT_SEC + 20
        for i in range(n):
            runner._note_member_registration(node, "LVS_19", f"late{i}", ERR, now=late + i)
        assert ev.call_count == 2


def test_failures_outside_the_window_do_not_add_up():
    node = _node()
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    step = constants.LVS_MEMBER_REGISTER_FAIL_WINDOW_SEC // (n - 1) + 1
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(n + 3):
            runner._note_member_registration(node, "LVS_19", f"lv{i}", ERR, now=100 + i * step)
    ev.assert_not_called()


def test_a_successful_registration_clears_the_member():
    node = _node()
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(n - 1):
            runner._note_member_registration(node, "LVS_19", f"lv{i}", ERR, now=100 + i)
        runner._note_member_registration(node, "LVS_19", "ok", now=150)
        runner._note_member_registration(node, "LVS_19", "lvX", ERR, now=160)
    ev.assert_not_called()


def test_members_and_lvstores_are_counted_apart():
    a, b = _node("A"), _node("B")
    n = constants.LVS_MEMBER_REGISTER_FAIL_ALERT_LVOLS
    with patch.object(runner.storage_events, "snode_lvs_member_register_failing") as ev:
        for i in range(n - 1):
            runner._note_member_registration(a, "LVS_19", f"a{i}", ERR, now=100 + i)
            runner._note_member_registration(b, "LVS_19", f"b{i}", ERR, now=100 + i)
            runner._note_member_registration(a, "LVS_25", f"c{i}", ERR, now=100 + i)
    ev.assert_not_called()


def test_the_event_names_the_remedy():
    from simplyblock_core.controllers import storage_events
    node = SimpleNamespace(get_id=lambda: "N1", cluster_id="C")
    with patch.object(storage_events.ec, "log_event_cluster") as log:
        storage_events.snode_lvs_member_register_failing(node, "LVS_19", 7, 1800, ERR)
    kw = log.call_args.kwargs
    assert kw["event"] == storage_events.ec.EVENT_LVS_MEMBER_REGISTER_FAILING
    assert "LVS_19" in kw["message"] and "Restarting the node" in kw["message"]
