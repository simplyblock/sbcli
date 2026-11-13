"""A create that fails over to a new leader must not delete its own blob.

bf12, 2026-11-04 23:19: LVOL_1744 on LVS_22 failed on the leader 2a8b379e
(HTTP 500), was created on the new leader 36ab8591, and was then "registered"
on 36ab8591 as a replica from the pre-failover plan. The register failed with
"File exists", and the replica rollback sync-deleted LVS_22/LVOL_1744 there:
the volume's only blob.
"""
from unittest.mock import MagicMock, patch

from simplyblock_core.controllers import lvol_controller as lc


def _node(uid):
    n = MagicMock()
    n.get_id.return_value = uid
    return n


def _lvol():
    lvol = MagicMock()
    lvol.get_id.return_value = "7822d91a-e4f0-437a-b228-094bd3d4b2b8"
    lvol.lvs_name = "LVS_22"
    lvol.top_bdev = "LVS_22/LVOL_1744"
    return lvol


class TestReplicasAfterLeaderFailover:
    def test_unchanged_leader_keeps_the_plan(self):
        p, s, t = _node("p"), _node("s"), _node("t")
        with patch.object(lc.tasks_controller, "add_lvol_sync_op_task") as task:
            out = lc._replicas_after_leader_failover(_lvol(), p, p, p, [s, t])
        assert out == [s, t]
        task.assert_not_called()

    def test_new_leader_is_not_registered_as_a_replica(self):
        old, sec, new = _node("2a8b379e"), _node("790fa338"), _node("36ab8591")
        host = _node("2a8b379e")
        host.cluster_id = "c"
        with patch.object(lc.tasks_controller, "add_lvol_sync_op_task") as task, \
                patch.object(lc, "_lvol_secondary_index", return_value=0):
            out = lc._replicas_after_leader_failover(_lvol(), host, old, new, [sec, new])
        assert [n.get_id() for n in out] == ["790fa338"]
        # the old leader still gets its replica, durably and later
        task.assert_called_once()
        assert task.call_args.args[1] == "2a8b379e"
        assert task.call_args.args[3] == "register"


class TestReplicaRollbackGuard:
    def test_no_teardown_on_the_lvstore_leader(self):
        rpc = MagicMock()
        rpc.get_lvstore.return_value = {"name": "LVS_22", "lvs leadership": True}
        lvol = _lvol()
        with patch.object(lc, "_remove_bdev_stack") as remove, \
                patch.object(lc, "DBController") as db:
            ok, msg = lc._fail_after_bdev(lvol, rpc, "File exists", is_primary=False)
        assert ok is False and msg == "File exists"
        remove.assert_not_called()
        rpc.nvmf_subsystem_remove_ns.assert_not_called()
        lvol.write_to_db.assert_not_called()
        db.assert_not_called()

    def test_teardown_on_a_real_replica(self):
        rpc = MagicMock()
        rpc.get_lvstore.return_value = {"name": "LVS_22", "lvs leadership": False}
        with patch.object(lc, "_remove_bdev_stack", return_value=True) as remove, \
                patch.object(lc, "DBController"):
            lc._fail_after_bdev(_lvol(), rpc, "boom", is_primary=False)
        remove.assert_called_once()
        assert remove.call_args.kwargs["sync"] is True

    def test_unanswered_leadership_query_keeps_old_behaviour(self):
        rpc = MagicMock()
        rpc.get_lvstore.side_effect = Exception("timeout")
        with patch.object(lc, "_remove_bdev_stack", return_value=True) as remove, \
                patch.object(lc, "DBController"):
            lc._fail_after_bdev(_lvol(), rpc, "boom", is_primary=False)
        remove.assert_called_once()

    def test_primary_rollback_is_not_gated(self):
        rpc = MagicMock()
        rpc.get_lvstore.return_value = {"lvs leadership": True}
        with patch.object(lc, "_remove_bdev_stack", return_value=True) as remove, \
                patch.object(lc, "DBController"):
            lc._fail_after_bdev(_lvol(), rpc, "boom", is_primary=True)
        remove.assert_called_once()
        rpc.get_lvstore.assert_not_called()
