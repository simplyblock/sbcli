"""A leaderless lvstore whose members are ALL online gets its leader back.

bf32 2026-11-21, round R6 "tgt-dual-p shutdown" (target cluster 70edb423):
the primary and secondary of LVS_7 were shut down and restarted. While the
secondary's restart ran, the SPDK of two other nodes (8de868b9, 2aa1550c) went
away; at 05:16:27.95 a write to LVS_7 on its primary 484ff828 and one to
LVS_10 on its primary 9154330e failed, and each SPDK dropped the leadership
itself ("Queued failed IO and Leadership state changed internally to false
for lvs primary"). SPDK takes it back only on the next IO it receives, and
both lvstores were replication targets: replication waits for a leader before
it sends. The restarts of 8de868b9 and 2aa1550c then rebuilt LVS_10 as
non-leaders "behind" 9154330e without asking whether it led, nobody touched
LVS_7 again, and both stayed leaderless with every member online until a
manual _recover_leaderless_lvs at 06:34 (76 min).

Now the node monitor (for the primary's own lvstore, after a confirmation
window) and a replication transfer waiting on such a target ask
storage_node_ops.restore_healthy_lvs_leadership, which grants only through the
guarded _recover_leaderless_lvs and only while every member is online and
answers non-leader.
"""
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import storage_node_monitor as mon


def _node(nid, lvstore="", sec="", tert="", leader=False, status=StorageNode.STATUS_ONLINE):
    n = MagicMock()
    n.get_id.return_value = nid
    n.status, n.lvstore, n.cluster_id = status, lvstore, "CL"
    n.secondary_node_id, n.tertiary_node_id = sec, tert
    n.is_secondary_node, n.jm_vuid = False, 7 if lvstore else 0
    n.health_check, n.lvstore_status, n.site = True, "ready", ""
    n.leader = leader
    n.rpc_client.return_value.get_lvstore.side_effect = (
        lambda name, _n=n: {"name": name, "lvs leadership": _n.leader})
    return n


class _Base(unittest.TestCase):

    def setUp(self):
        # LVS_7: primary P (484ff828), secondary S (d10f59c9), tertiary T (9154330e)
        self.p = _node("P", "LVS_7", "S", "T")
        self.s = _node("S", "LVS_4", "T", "X")
        self.t = _node("T", "LVS_10", "X", "Y")
        self.nodes = {n.get_id(): n for n in (self.p, self.s, self.t)}
        self.cluster = types.SimpleNamespace(status=Cluster.STATUS_ACTIVE)
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda i: self.nodes[i]
        db.get_storage_nodes_by_cluster_id.return_value = list(self.nodes.values())
        db.get_cluster_by_id.return_value = self.cluster
        self.recover = MagicMock(side_effect=lambda cid, members, lvs, taker: taker)
        self.ops = patch.multiple(
            storage_node_ops, DBController=MagicMock(return_value=db),
            _cluster_two_node_arbitrated=MagicMock(return_value=False),
            _recover_leaderless_lvs=self.recover)
        self.ops.start()
        self.addCleanup(self.ops.stop)


class TestHealthyLeadershipState(_Base):

    def test_leaderless_with_every_member_online(self):
        state, members, why = storage_node_ops.healthy_lvs_leadership_state("P")
        self.assertEqual(state, "leaderless")
        self.assertEqual([m.get_id() for m in members], ["P", "S", "T"])
        self.assertIn("LVS_7", why)

    def test_led(self):
        self.t.leader = True
        state, leaders, _ = storage_node_ops.healthy_lvs_leadership_state("P")
        self.assertEqual((state, [n.get_id() for n in leaders]), ("led", ["T"]))

    def test_a_member_not_online_is_not_ours(self):
        for st in (StorageNode.STATUS_OFFLINE, StorageNode.STATUS_RESTARTING,
                   StorageNode.STATUS_DOWN):
            self.s.status = st
            state, _, why = storage_node_ops.healthy_lvs_leadership_state("P")
            self.assertEqual(state, "skip", st)
            self.assertIn("not online", why)

    def test_a_member_that_cannot_be_asked_may_lead(self):
        self.s.rpc_client.return_value.get_lvstore.side_effect = RuntimeError("timeout")
        state, _, why = storage_node_ops.healthy_lvs_leadership_state("P")
        self.assertEqual(state, "skip")
        self.assertIn("could not be asked", why)

    def test_not_while_the_cluster_is_suspended(self):
        self.cluster.status = Cluster.STATUS_SUSPENDED
        self.assertEqual(storage_node_ops.healthy_lvs_leadership_state("P")[0], "skip")

    def test_not_in_a_restart_window_or_with_a_failed_health_check(self):
        self.p.lvstore_status = "in_creation"
        self.assertEqual(storage_node_ops.healthy_lvs_leadership_state("P")[0], "skip")
        self.p.lvstore_status, self.p.health_check = "ready", False
        self.assertEqual(storage_node_ops.healthy_lvs_leadership_state("P")[0], "skip")

    def test_not_on_sync_or_two_node_clusters(self):
        self.p.site = "site-a"
        self.assertEqual(storage_node_ops.healthy_lvs_leadership_state("P")[0], "skip")
        self.p.site = ""
        with patch.object(storage_node_ops, "_cluster_two_node_arbitrated", return_value=True):
            self.assertEqual(storage_node_ops.healthy_lvs_leadership_state("P")[0], "skip")

    def test_the_primary_is_found_by_lvstore_name(self):
        # a volume's recorded node_id may not be its lvstore's primary
        state, members, _ = storage_node_ops.healthy_lvs_leadership_state("S", "LVS_7")
        self.assertEqual(state, "leaderless")
        self.assertEqual(members[0].get_id(), "P")


class TestRestoreHealthyLeadership(_Base):

    def test_guarded_recovery_on_the_primary(self):
        leader, why = storage_node_ops.restore_healthy_lvs_leadership("P", reason="test")
        self.assertIs(leader, self.p)
        args = self.recover.call_args.args
        self.assertEqual((args[0], [m.get_id() for m in args[1]], args[2], args[3].get_id()),
                         ("CL", ["P", "S", "T"], "LVS_7", "P"))
        self.assertIn("restored", why)

    def test_nothing_granted_when_led(self):
        self.p.leader = True
        leader, _ = storage_node_ops.restore_healthy_lvs_leadership("P")
        self.assertIs(leader, self.p)
        self.recover.assert_not_called()

    def test_nothing_granted_with_a_member_down(self):
        self.t.status = StorageNode.STATUS_OFFLINE
        leader, why = storage_node_ops.restore_healthy_lvs_leadership("P")
        self.assertIsNone(leader)
        self.recover.assert_not_called()

    def test_a_declined_recovery_reports_none(self):
        self.recover.side_effect = None
        self.recover.return_value = None
        leader, why = storage_node_ops.restore_healthy_lvs_leadership("P")
        self.assertIsNone(leader)
        self.assertIn("granted nothing", why)


class TestMonitorRestoresLeadership(unittest.TestCase):

    def setUp(self):
        for d in (mon._healthy_leaderless_checked, mon._healthy_leaderless_since):
            d.clear()
            self.addCleanup(d.clear)
        mon._healthy_leaderless_running.clear()
        self.p = _node("P", "LVS_7", "S", "T")
        self.state = MagicMock(return_value=("leaderless", [self.p], "LVS_7: no member reports"))
        self.restore = MagicMock(return_value=(self.p, "LVS_7: leadership restored on P"))
        pt = patch.multiple(storage_node_ops, healthy_lvs_leadership_state=self.state,
                            restore_healthy_lvs_leadership=self.restore)
        pt.start()
        self.addCleanup(pt.stop)
        # run the recovery thread inline
        pt = patch.object(mon.threading, "Thread", side_effect=lambda target, **kw: types.SimpleNamespace(start=target))
        pt.start()
        self.addCleanup(pt.stop)
        self.clock = [1000.0]
        pt = patch.object(mon.time, "monotonic", side_effect=lambda: self.clock[0])
        pt.start()
        self.addCleanup(pt.stop)

    def _tick(self, dt):
        self.clock[0] += dt
        mon._restore_leadership_if_leaderless(self.p)

    def test_acts_only_after_the_confirmation_window(self):
        self._tick(0)
        self.restore.assert_not_called()
        self._tick(mon.HEALTHY_LEADERLESS_CHECK_INTERVAL_SEC)
        self.restore.assert_not_called()
        self._tick(mon.HEALTHY_LEADERLESS_CONFIRM_SEC)
        self.restore.assert_called_once()
        self.assertEqual(self.restore.call_args.args, ("P",))
        self.assertNotIn(("CL", "LVS_7"), mon._healthy_leaderless_since)

    def test_probe_rate_limited(self):
        self._tick(0)
        self._tick(1)
        self.assertEqual(self.state.call_count, 1)

    def test_any_other_observation_restarts_the_window(self):
        self._tick(0)
        self.state.return_value = ("led", [self.p], "led by P")
        self._tick(mon.HEALTHY_LEADERLESS_CHECK_INTERVAL_SEC)
        self.state.return_value = ("leaderless", [self.p], "")
        self._tick(mon.HEALTHY_LEADERLESS_CONFIRM_SEC)
        self.restore.assert_not_called()

    def test_only_for_a_primary(self):
        self.p.is_secondary_node = True
        self._tick(0)
        self.p.is_secondary_node, self.p.lvstore = False, ""
        self._tick(100)
        self.state.assert_not_called()

    def test_a_failing_recovery_is_contained(self):
        self.restore.side_effect = RuntimeError("boom")
        self._tick(0)
        self._tick(mon.HEALTHY_LEADERLESS_CONFIRM_SEC)
        self.assertNotIn(("CL", "LVS_7"), mon._healthy_leaderless_running)


class TestReplicationAsksForRecovery(unittest.TestCase):

    def setUp(self):
        self.task = types.SimpleNamespace(
            uuid="66c78316", retry=0, status="running", function_result="",
            function_params={"snapshot_id": "snap-1"}, write_to_db=MagicMock())
        self.remote_lv = types.SimpleNamespace(
            node_id="484ff828", nodes=["484ff828", "d10f59c9", "9154330e"],
            lvs_name="LVS_7", cluster_id="CL")
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = (
            lambda i: types.SimpleNamespace(status="online"))
        db.get_snapshot_by_id.return_value = types.SimpleNamespace(lvol="src-lvol")
        for target, attr, new in ((sr, "db", db),
                                  (sr.lvol_events, "replication_target_leaderless", MagicMock())):
            pt = patch.object(target, attr, new)
            pt.start()
            self.addCleanup(pt.stop)
        self.restore = MagicMock(return_value=(None, "declined"))
        pt = patch.object(storage_node_ops, "restore_healthy_lvs_leadership", self.restore)
        pt.start()
        self.addCleanup(pt.stop)

    def _wait(self, age=None):
        if age is not None:
            self.task.function_params["target_leaderless_since"] -= age
        sr._wait_for_target_leader(self.task, self.remote_lv,
                                   "0 members of LVS_7 report leadership")

    def test_not_before_the_recover_delay(self):
        self._wait()
        self.restore.assert_not_called()

    def test_asks_once_the_wait_is_long_and_rate_limits(self):
        self._wait()
        self._wait(constants.REPL_TARGET_LEADERLESS_RECOVER_SEC + 1)
        self.restore.assert_called_once()
        self.assertEqual(self.restore.call_args.args, ("484ff828", "LVS_7"))
        self._wait()
        self.restore.assert_called_once()       # rate-limited
        self.task.function_params["target_recover_tried_at"] -= (
            constants.REPL_TARGET_LEADERLESS_RECOVER_EVERY_SEC + 1)
        self._wait()
        self.assertEqual(self.restore.call_count, 2)
        self.assertEqual(self.task.status, "suspended")    # still waits

    def test_a_failing_recovery_does_not_break_the_wait(self):
        self.restore.side_effect = RuntimeError("fdb")
        self._wait()
        self._wait(constants.REPL_TARGET_LEADERLESS_RECOVER_SEC + 1)
        self.assertEqual(self.task.status, "suspended")

    def test_a_leader_clears_the_rate_limit(self):
        self._wait()
        self._wait(constants.REPL_TARGET_LEADERLESS_RECOVER_SEC + 1)
        sr._target_leader_found(self.task)
        self.assertNotIn("target_recover_tried_at", self.task.function_params)


if __name__ == "__main__":
    unittest.main()
