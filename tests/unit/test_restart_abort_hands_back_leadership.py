"""An aborted restart gives the lvstores it belongs to a leader at once.

bf36 R3b (2026-11-26): each restart attempt of 0b26b6da took the leadership of
its own LVS_22 (demoting the acting leader 6344ccd0), then a non-leader rebuild
of another lvstore failed and the attempt aborted, killing SPDK with the
leadership just taken. LVS_22 had no leader until the node monitor's takeover
came round (~100 s), and the next attempt repeated it: the leadership flapped
16:55-17:03.
"""
import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops as ops
from simplyblock_core.models.storage_node import StorageNode

ONLINE = StorageNode.STATUS_ONLINE
OFFLINE = StorageNode.STATUS_OFFLINE


def _n(nid, status=ONLINE):
    return SimpleNamespace(get_id=lambda: nid, status=status, cluster_id="CL")


SNODE = _n("0b26", OFFLINE)


def _run(lvstores, handoff=(True, "down"), leader=None, sync="", two_node=False, find_error=None):
    db = MagicMock()
    db.get_storage_node_by_id.return_value = SNODE
    find = MagicMock()
    if find_error:
        find.side_effect = find_error
    else:
        find.return_value = (leader, [])
    with patch.object(ops, "DBController", return_value=db), \
            patch.object(ops, "_sync_site", return_value=sync), \
            patch.object(ops, "_cluster_two_node_arbitrated", return_value=two_node), \
            patch.object(ops, "_restart_member_lvstores", return_value=lvstores), \
            patch.object(ops, "restart_before_lvstore_handoff", return_value=handoff) as pre, \
            patch.object(ops, "find_leader_with_failover", find):
        out = ops.hand_back_lvs_leadership_after_abort(SNODE)
    return out, find, pre


def test_the_lvstore_gets_its_leader_back_at_once():
    members = [SNODE, _n("6344"), _n("1a47")]
    out, find, pre = _run([("LVS_22", members)], leader=members[1])
    assert out == [("LVS_22", "6344")]
    pre.assert_called_once_with("0b26", "LVS_22")
    find.assert_called_once_with(members, "LVS_22", pre_handoff_restart_of="0b26")


def test_not_while_the_handoff_may_be_under_way():
    out, find, _ = _run([("LVS_22", [SNODE, _n("6344")])], handoff=(False, "SPDK is running"))
    assert out == []
    find.assert_not_called()


def test_not_without_an_online_peer():
    out, find, _ = _run([("LVS_22", [SNODE, _n("6344", OFFLINE)])])
    assert out == []
    find.assert_not_called()


def test_every_member_lvstore_is_handled():
    a, b = [SNODE, _n("6344")], [_n("98b0"), SNODE]
    out, find, _ = _run([("LVS_22", a), ("LVS_2", b)], leader=_n("x"))
    assert [lvs for lvs, _ in out] == ["LVS_22", "LVS_2"]
    assert find.call_count == 2


def test_no_leader_found_is_reported_not_raised():
    out, _, _ = _run([("LVS_22", [SNODE, _n("6344")])], leader=None)
    assert out == [("LVS_22", None)]


def test_a_failing_takeover_never_raises():
    out, _, _ = _run([("LVS_22", [SNODE, _n("6344")])], find_error=RuntimeError("boom"))
    assert out == []


def test_not_on_sync_or_two_node_clusters():
    for kw in ({"sync": "site-a"}, {"two_node": True}):
        out, find, _ = _run([("LVS_22", [SNODE, _n("6344")])], **kw)
        assert out == [] and not find.called


def test_both_recreate_aborts_hand_back_after_the_kill():
    src = inspect.getsource(ops._restart_storage_node_impl).replace("\r\n", "\n")
    for abort in ('_abort_restart(f"LVS recreation failed: {e}")',
                  '_abort_restart("recreate_all_lvstores returned False")'):
        i = src.index(abort)
        j = src.index("hand_back_lvs_leadership_after_abort(snode)", i)
        assert src.index("return False", i) > j
