"""A non-leader never keeps its md-journal examine result.

bf36 R3b (2026-11-26): 6344ccd0 examined LVS_22 at 13:42:42 as its secondary
and SPDK kept the journal result "until the set-leader call", which nobody
sent. Promoted at 16:55:20 "from the examine result, no journal read", it
zeroed the journal and lost the leader's unhomed metadata: a deleted volume
came back and LVS_22 could not be loaded any more. The control plane now
sends set-leader 0 -> 0 after every non-leader examine.
"""
import inspect
from unittest.mock import MagicMock

from simplyblock_core import storage_node_ops as ops


def _rpc(lvs=None, query_error=None, set_error=None):
    rpc = MagicMock()
    if query_error:
        rpc.get_lvstore.side_effect = query_error
    else:
        rpc.get_lvstore.return_value = lvs
    if set_error:
        rpc.bdev_lvol_set_leader.side_effect = set_error
    return rpc


def test_a_non_leader_is_told_it_does_not_lead():
    rpc = _rpc({"name": "LVS_22", "lvs leadership": False})
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is True
    rpc.bdev_lvol_set_leader.assert_called_once_with("LVS_22", leader=False, bs_nonleadership=True)


def test_a_leader_is_never_demoted():
    rpc = _rpc({"name": "LVS_22", "lvs leadership": True})
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is False
    rpc.bdev_lvol_set_leader.assert_not_called()


def test_an_unanswered_query_sends_nothing():
    rpc = _rpc(query_error=RuntimeError("Could not reach remote"))
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is False
    rpc.bdev_lvol_set_leader.assert_not_called()


def test_no_lvstore_sends_nothing():
    rpc = _rpc(None)
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is False
    rpc.bdev_lvol_set_leader.assert_not_called()


def test_an_answer_without_the_leadership_field_sends_nothing():
    # e.g. a stub that only says the lvstore exists
    rpc = _rpc(True)
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is False
    rpc.bdev_lvol_set_leader.assert_not_called()


def test_a_failed_call_is_not_fatal():
    rpc = _rpc({"name": "LVS_22", "lvs leadership": False}, set_error=RuntimeError("boom"))
    assert ops._release_nonleader_examine(rpc, "6344ccd0-x", "LVS_22") is False


def _src(fn):
    return inspect.getsource(fn).replace("\r\n", "\n")


def test_the_non_leader_build_releases_after_the_role_stamp_and_before_the_hub():
    src = _src(ops._recreate_lvstore_on_non_leader_impl)
    stamp = src.index("role_stamped = snode_rpc_client.bdev_lvol_set_lvs_opts(")
    release = src.index("_release_nonleader_examine(snode_rpc_client, snode.get_id(), primary_node.lvstore)")
    # the in-window connect registers the redirect: from then on IO can reach the node
    # (the earlier controller attach is inert until then)
    hub = src.index("ok = snode.connect_to_hublvol(")
    assert stamp < release < hub


def test_a_new_lvstore_releases_on_its_secondaries():
    src = _src(ops.create_lvstore)
    examine = src.index("sec_rpc_client.bdev_wait_for_examine()")
    release = src.index("_release_nonleader_examine(sec_rpc_client, sec_node.get_id(), lvs_name)")
    assert examine < release
