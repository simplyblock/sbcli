"""A lvstore that lost its leader with an OFFLINE member gets a proven leader.

bf11 (2026-11-03): the target primary of an async-replication pair was shut
down for 42 min. Leadership moves to a secondary only on client IO, and a
replication target has none (the hub refuses receive IO on a non-leader), so
LVS_2 had no leader the whole time and every landing create failed with
"Error creating remote lvol". The node monitor's OFFLINE branch now asks
find_leader_with_failover for a leader (which since #1592 grants only after
every other member is proven non-leader or SPDK-dead, twice). Replication
itself still never grants: a landing create with the primary down only routes
to an EXISTING leader (recover=False).
"""
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import lvol_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor as mon


def _node(nid, status=StorageNode.STATUS_ONLINE, lvstore="", sec="", tert=""):
    n = MagicMock()
    n.get_id.return_value = nid
    n.status, n.lvstore, n.cluster_id = status, lvstore, "CL"
    n.secondary_node_id, n.tertiary_node_id = sec, tert
    n.site = ""
    return n


class TestLeaderlessTakeover(unittest.TestCase):

    def setUp(self):
        mon._leaderless_takeover_tried.clear()
        self.addCleanup(mon._leaderless_takeover_tried.clear)
        # A (offline) is primary of LVS_2 with B, C; B is primary of LVS_3 with C, A.
        self.a = _node("A", StorageNode.STATUS_OFFLINE, "LVS_2", "B", "C")
        self.b = _node("B", lvstore="LVS_3", sec="C", tert="A")
        self.c = _node("C", lvstore="LVS_4", sec="D", tert="")
        self.d = _node("D", lvstore="")
        nodes = {n.get_id(): n for n in (self.a, self.b, self.c, self.d)}
        db = MagicMock()
        db.get_storage_nodes_by_cluster_id.return_value = list(nodes.values())
        db.get_storage_node_by_id.side_effect = lambda i: nodes[i]
        p = patch.object(mon, "db", db)
        p.start()
        self.addCleanup(p.stop)
        self.db = db
        self.ops = patch.multiple(
            storage_node_ops,
            _sync_site=MagicMock(return_value=""),
            _cluster_two_node_arbitrated=MagicMock(return_value=False),
            find_leader_with_failover=MagicMock(return_value=(self.b, [])))
        self.ops.start()
        self.addCleanup(self.ops.stop)
        p = patch.object(mon.tasks_controller, "get_active_node_restart_task",
                         return_value=False)
        p.start()
        self.addCleanup(p.stop)

    def _calls(self):
        return [(c.args[1], [m.get_id() for m in c.args[0]])
                for c in storage_node_ops.find_leader_with_failover.call_args_list]

    def test_every_lvstore_of_the_offline_member_gets_a_leader_lookup(self):
        mon._take_over_leaderless_lvstores(self.a)
        self.assertEqual(sorted(self._calls()),
                         [("LVS_2", ["A", "B", "C"]), ("LVS_3", ["B", "C", "A"])])
        for c in storage_node_ops.find_leader_with_failover.call_args_list:
            # recovery is allowed here: the proof (#1592) guards the grant
            self.assertNotIn("recover", c.kwargs)

    def test_rate_limited_per_lvstore(self):
        mon._take_over_leaderless_lvstores(self.a)
        mon._take_over_leaderless_lvstores(self.a)
        self.assertEqual(storage_node_ops.find_leader_with_failover.call_count, 2)

    def test_not_while_the_restart_flow_runs(self):
        with patch.object(mon.tasks_controller, "get_active_node_restart_task",
                          return_value="T1"):
            self.db.get_task_by_id.return_value = SimpleNamespace(
                status=JobSchedule.STATUS_RUNNING)
            mon._take_over_leaderless_lvstores(self.a)
        storage_node_ops.find_leader_with_failover.assert_not_called()

    def test_a_queued_restart_does_not_block(self):
        with patch.object(mon.tasks_controller, "get_active_node_restart_task",
                          return_value="T1"):
            self.db.get_task_by_id.return_value = SimpleNamespace(status=JobSchedule.STATUS_NEW)
            mon._take_over_leaderless_lvstores(self.a)
        self.assertEqual(storage_node_ops.find_leader_with_failover.call_count, 2)

    def test_not_on_sync_replication_clusters(self):
        storage_node_ops._sync_site.return_value = "site-a"
        mon._take_over_leaderless_lvstores(self.a)
        storage_node_ops.find_leader_with_failover.assert_not_called()

    def test_not_on_two_node_arbitrated_clusters(self):
        storage_node_ops._cluster_two_node_arbitrated.return_value = True
        mon._take_over_leaderless_lvstores(self.a)
        storage_node_ops.find_leader_with_failover.assert_not_called()

    def test_skipped_when_no_member_is_online(self):
        self.b.status = self.c.status = StorageNode.STATUS_OFFLINE
        mon._take_over_leaderless_lvstores(self.a)
        storage_node_ops.find_leader_with_failover.assert_not_called()

    def test_a_failing_lookup_does_not_stop_the_others(self):
        storage_node_ops.find_leader_with_failover.side_effect = [RuntimeError("x"), (None, [])]
        mon._take_over_leaderless_lvstores(self.a)
        self.assertEqual(storage_node_ops.find_leader_with_failover.call_count, 2)


class TestLandingCreateWithPrimaryDown(unittest.TestCase):

    def setUp(self):
        self.a = _node("A", StorageNode.STATUS_OFFLINE, "LVS_2", "B", "C")
        self.b = _node("B")
        self.c = _node("C")
        nodes = {n.get_id(): n for n in (self.a, self.b, self.c)}
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = lambda i: nodes[i]
        p = patch.object(lvol_controller, "DBController", return_value=db)
        p.start()
        self.addCleanup(p.stop)

    def test_existing_leader_is_looked_up_without_recovery(self):
        with patch.object(storage_node_ops, "find_leader_with_failover",
                          return_value=(self.b, [])) as f:
            self.assertTrue(lvol_controller._lvstore_has_existing_leader(self.a))
        self.assertEqual(f.call_args.kwargs.get("recover"), False)
        self.assertEqual([m.get_id() for m in f.call_args.args[0]], ["A", "B", "C"])

    def test_no_leader_means_no_create(self):
        with patch.object(storage_node_ops, "find_leader_with_failover",
                          return_value=(None, [])):
            self.assertFalse(lvol_controller._lvstore_has_existing_leader(self.a))

    def test_no_online_member_means_no_lookup(self):
        self.b.status = self.c.status = StorageNode.STATUS_OFFLINE
        with patch.object(storage_node_ops, "find_leader_with_failover") as f:
            self.assertFalse(lvol_controller._lvstore_has_existing_leader(self.a))
        f.assert_not_called()


class TestExecuteOnLeaderRecoverFlag(unittest.TestCase):

    def test_recover_false_is_passed_to_every_lookup(self):
        leader, other = _node("B"), _node("C")
        with patch.object(storage_node_ops, "find_leader_with_failover",
                          return_value=(other, [])) as f:
            ok, _, _ = storage_node_ops.execute_on_leader_with_failover(
                [leader, other], "LVS_2", lambda n: n is other, known_leader=leader,
                recover=False)
        self.assertTrue(ok)
        self.assertTrue(all(c.kwargs.get("recover") is False for c in f.call_args_list))
        self.assertEqual(f.call_count, 1)

    def test_default_keeps_recovery(self):
        leader = _node("B")
        with patch.object(storage_node_ops, "find_leader_with_failover",
                          return_value=(leader, [])) as f:
            storage_node_ops.execute_on_leader_with_failover([leader], "LVS_2", lambda n: True)
        self.assertIs(f.call_args.kwargs.get("recover"), True)
