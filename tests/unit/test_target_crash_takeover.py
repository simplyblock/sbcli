"""A replication target whose primary CRASHED gets a leader while the primary
restarts, not only after it is back.

bf24 2026-11-14 (also bf21, bf23), round 3 "tgt-mid" crash: the SPDK of
3c0825d7, primary of the target lvstore LVS_22, was killed at 21:08:15 in the
middle of a transfer. The monitor set the node OFFLINE and its auto-restart
task started at 21:08:27. A target lvstore has no client IO, so neither the
secondary nor the tertiary promoted itself, and the node monitor's proven
takeover (#1599) never ran: it was skipped while the restart task ran, the
monitor skips IN_RESTART nodes altogether, and _leadership_moving_tasks_active
refused every grant while ANY restart task of a member existed. LVS_22 stayed
leaderless until the restarted primary took it back; the transfer waited,
"target leadership not established (0 members of LVS_22 report leadership)",
and finished 182 s after the crash with an RPO of 367 s.

The restart owns the lvstore's leadership only once its handoff can run, which
needs the restarted node's SPDK. While the node agent confirms that SPDK is
down and no restart phase is recorded for the lvstore, the proven takeover now
runs during the restart as well; it re-checks right before the grant.
"""
import time
import types
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants, storage_node_ops
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer import service as peer_service
from simplyblock_core.peer.protocol import EnsureHubResponse
from simplyblock_core.rpc_client import RPCConnectionError
from simplyblock_core.services import snapshot_replication as sr
from simplyblock_core.services import storage_node_monitor as mon


def _agent(up):
    agent = MagicMock()
    if isinstance(up, Exception):
        agent.spdk_process_is_up.side_effect = up
    else:
        agent.spdk_process_is_up.return_value = (up, "")
    return agent


def _snode(nid, status=StorageNode.STATUS_RESTARTING, phases=None, spdk_up=False):
    n = MagicMock()
    n.get_id.return_value = nid
    n.status, n.restart_phases = status, phases or {}
    n.rpc_port, n.cluster_id = 4422, "CL"
    n.client.return_value = _agent(spdk_up)
    return n


class TestRestartBeforeLvstoreHandoff(unittest.TestCase):

    def _check(self, node, lvs="LVS_22"):
        with patch.object(storage_node_ops, "DBController") as dbc:
            dbc.return_value.get_storage_node_by_id.return_value = node
            return storage_node_ops.restart_before_lvstore_handoff(node.get_id(), lvs)

    def test_restarting_with_spdk_down_and_no_phase(self):
        ok, why = self._check(_snode("3c0825d7"))
        self.assertTrue(ok, why)

    def test_offline_counts_too(self):
        self.assertTrue(self._check(_snode("3c0825d7", StorageNode.STATUS_OFFLINE))[0])

    def test_spdk_running_means_the_restart_is_under_way(self):
        ok, why = self._check(_snode("3c0825d7", spdk_up=True))
        self.assertFalse(ok)
        self.assertIn("running", why)

    def test_unconfirmed_spdk_state_is_not_down(self):
        node = _snode("3c0825d7")
        node.client.return_value = _agent(RuntimeError("agent unreachable"))
        ok, why = self._check(node)
        self.assertFalse(ok)
        self.assertIn("cannot confirm", why)

    def test_a_recorded_phase_means_the_handoff_began(self):
        ok, _ = self._check(_snode("3c0825d7", phases={"LVS_22": StorageNode.RESTART_PHASE_PRE_BLOCK}))
        self.assertFalse(ok)
        # a phase of another lvstore does not
        self.assertTrue(self._check(_snode("3c0825d7", phases={"LVS_9": "blocked"}))[0])

    def test_an_online_node_is_not_restarting(self):
        self.assertFalse(self._check(_snode("3c0825d7", StorageNode.STATUS_ONLINE))[0])


def _task(fn, node_id, status=JobSchedule.STATUS_RUNNING):
    return types.SimpleNamespace(function_name=fn, node_id=node_id, status=status,
                                 canceled=False, uuid=f"t-{node_id}", function_params={})


class TestMovingTasksExemption(unittest.TestCase):

    def _active(self, tasks, gate=(True, "down"), **kw):
        with patch.object(storage_node_ops, "DBController") as dbc, \
                patch.object(storage_node_ops, "restart_before_lvstore_handoff",
                             return_value=gate) as g:
            dbc.return_value.get_job_tasks.return_value = tasks
            return storage_node_ops._leadership_moving_tasks_active(
                "CL", ["P", "S", "T"], "LVS_22", **kw), g

    def test_restart_of_the_crashed_member_before_its_handoff_does_not_block(self):
        active, gate = self._active([_task(JobSchedule.FN_NODE_RESTART, "P")],
                                    pre_handoff_restart_of="P")
        self.assertFalse(active)
        gate.assert_called_once_with("P", "LVS_22")

    def test_it_blocks_once_the_handoff_can_have_begun(self):
        active, _ = self._active([_task(JobSchedule.FN_NODE_RESTART, "P")],
                                 gate=(False, "SPDK running"), pre_handoff_restart_of="P")
        self.assertTrue(active)

    def test_without_the_exemption_every_restart_blocks(self):
        active, gate = self._active([_task(JobSchedule.FN_NODE_RESTART, "P")])
        self.assertTrue(active)
        gate.assert_not_called()

    def test_other_tasks_still_block(self):
        for task in (_task(JobSchedule.FN_NODE_RESTART, "S"),
                     _task(JobSchedule.FN_PORT_ALLOW, "P")):
            active, _ = self._active([task], pre_handoff_restart_of="P")
            self.assertTrue(active, task.function_name)


def _member(nid, lvstore=""):
    rpc = MagicMock()
    rpc.jc_get_jm_status.return_value = {"jm1": True, "remote_jm2": True}
    rpc.bdev_lvol_update_lvstore.return_value = True
    rpc.get_lvstore.return_value = {"lvs leadership": False}
    calls = []

    def rpc_client(**kw):
        calls.append(kw)
        return rpc
    return types.SimpleNamespace(
        get_id=lambda: nid, status=StorageNode.STATUS_ONLINE, lvstore=lvstore,
        jm_vuid=22, cluster_id="CL", rpc_port=4420, rpc_client=rpc_client,
        client=lambda **kw: _agent(False), _rpc=rpc, _calls=calls)


class TestGrantDuringTheRestart(unittest.TestCase):
    """_recover_leaderless_lvs with the crashed primary's restart active."""

    def setUp(self):
        self.p = _member("P", lvstore="LVS_22")
        self.p.status = StorageNode.STATUS_RESTARTING
        self.p._rpc.get_lvstore.side_effect = RPCConnectionError("Could not reach remote")
        self.s, self.t = _member("S"), _member("T")
        self.leaders = set()
        self.s._rpc.bdev_lvol_set_leader.side_effect = (
            lambda lvs, leader=False: self.leaders.add("S") or True)
        pt = patch.object(storage_node_ops, "DBController")
        self.db = pt.start().return_value
        self.addCleanup(pt.stop)
        self.db.acquire_lvstore_lock.return_value = (True, None)
        self.db.get_job_tasks.return_value = [_task(JobSchedule.FN_NODE_RESTART, "P")]
        self.gate = [(True, "SPDK of P is down")]
        pt = patch.object(storage_node_ops, "restart_before_lvstore_handoff",
                          side_effect=lambda nid, lvs: self.gate[0])
        self.gate_mock = pt.start()
        self.addCleanup(pt.stop)
        for target, attr in ((storage_node_ops.health_controller, "_check_sec_node_hublvol"),
                             (storage_node_ops.time, "sleep")):
            pt = patch.object(target, attr)
            pt.start()
            self.addCleanup(pt.stop)
        pt = patch("simplyblock_core.controllers.lvol_controller.is_node_leader",
                   side_effect=lambda node, lvs: node.get_id() in self.leaders)
        pt.start()
        self.addCleanup(pt.stop)

    def _run(self, taker=None, **kw):
        kw.setdefault("pre_handoff_restart_of", "P")
        return storage_node_ops._recover_leaderless_lvs(
            "CL", [self.p, self.s, self.t], "LVS_22", taker or self.s, **kw)

    def test_the_secondary_is_granted_while_the_primary_spdk_is_down(self):
        self.assertIs(self._run(), self.s)
        self.s._rpc.bdev_lvol_update_lvstore.assert_called_once_with("LVS_22")
        self.s._rpc.bdev_lvol_set_leader.assert_called_once_with("LVS_22", leader=True)
        # a single grant attempt when racing a restart
        self.assertIn({"timeout": 5, "retry": 1}, self.s._calls)

    def test_no_grant_when_the_restart_began_during_the_proof(self):
        def flip(lvs):
            self.gate[0] = (False, "SPDK of P is running")
            return True
        self.s._rpc.bdev_lvol_update_lvstore.side_effect = flip
        self.assertIsNone(self._run())
        self.s._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_no_grant_without_the_exemption(self):
        self.assertIsNone(self._run(pre_handoff_restart_of=None))
        self.s._rpc.bdev_lvol_update_lvstore.assert_not_called()
        self.s._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_never_to_the_member_being_restarted(self):
        self.assertIsNone(self._run(taker=self.p))
        self.p._rpc.bdev_lvol_set_leader.assert_not_called()

    def test_a_crashed_primary_whose_spdk_still_runs_blocks_the_proof(self):
        # its management RPC is down, but the node agent sees SPDK running
        self.p.client = lambda **kw: _agent(True)
        self.assertIsNone(self._run())
        self.s._rpc.bdev_lvol_set_leader.assert_not_called()


class TestFindLeaderPassesTheExemption(unittest.TestCase):

    def test_threaded_to_the_recovery(self):
        nodes = [_member("P", "LVS_22"), _member("S")]
        with patch.object(storage_node_ops, "_find_leader_with_failover_impl",
                          return_value=(None, [])) as impl, \
                patch("simplyblock_core.utils.ttl_cache.no_leader_cache") as cache:
            cache.get.return_value = False
            storage_node_ops.find_leader_with_failover(nodes, "LVS_22",
                                                       pre_handoff_restart_of="P")
        self.assertEqual(impl.call_args.kwargs["pre_handoff_restart_of"], "P")

    def test_the_impl_hands_it_to_the_recovery(self):
        import inspect
        src = inspect.getsource(storage_node_ops._find_leader_with_failover_impl)
        self.assertEqual(src.count("pre_handoff_restart_of=pre_handoff_restart_of"), 2)


class TestMonitorRunsTheTakeoverForARestartingNode(unittest.TestCase):

    def _check_node(self, status, restart_task="T1"):
        node = _snode("P", status)
        node.lvstore_status = "ready"
        db = MagicMock()
        db.get_storage_node_by_id.return_value = node
        with patch.object(mon, "db", db), \
                patch.object(mon.tasks_controller, "get_active_node_restart_task",
                             return_value=restart_task), \
                patch.object(mon, "_take_over_leaderless_lvstores") as take, \
                patch.object(mon.storage_network, "publish_ports"), \
                patch.object(mon, "_ana_failover_once", return_value=False):
            mon.check_node(node)
        return take

    def test_in_restart(self):
        self._check_node(StorageNode.STATUS_RESTARTING).assert_called_once()

    def test_offline_with_its_restart_queued(self):
        self._check_node(StorageNode.STATUS_OFFLINE).assert_called_once()

    def test_not_for_a_node_without_a_restart_task(self):
        # an IN_RESTART node without a task is skipped as before; an OFFLINE
        # one reaches the OFFLINE branch (covered by its own tests)
        self._check_node(StorageNode.STATUS_RESTARTING, restart_task=False).assert_not_called()


class TestReplicationWaitIsObservable(unittest.TestCase):

    def setUp(self):
        self.task = types.SimpleNamespace(
            uuid="89a87876", retry=4, status="running", function_result="",
            function_params={"snapshot_id": "snap-1"}, write_to_db=MagicMock())
        self.remote_lv = types.SimpleNamespace(
            node_id="3c0825d7-aaaa", nodes=["3c0825d7-aaaa", "92188ff1-bbbb", "be487f40-cccc"],
            lvs_name="LVS_22", cluster_id="CL")
        status = {"3c0825d7-aaaa": "in_restart", "92188ff1-bbbb": "online",
                  "be487f40-cccc": "online"}
        db = MagicMock()
        db.get_storage_node_by_id.side_effect = (
            lambda i: types.SimpleNamespace(status=status[i]))
        db.get_snapshot_by_id.return_value = types.SimpleNamespace(lvol="src-lvol")
        pt = patch.object(sr, "db", db)
        pt.start()
        self.addCleanup(pt.stop)
        pt = patch.object(sr.lvol_events, "replication_target_leaderless")
        self.event = pt.start()
        self.addCleanup(pt.stop)

    def _wait(self):
        """One pass with a leaderless target: the target reports its members'
        states (peer.service.member_states, what ensure_hub answers for an
        unsettled lvstore), the source records the wait."""
        sr._wait_for_target_leader(
            self.task, self.remote_lv,
            EnsureHubResponse(state="unsettled",
                              reason="target leadership not established "
                                     "(0 members of LVS_22 report leadership)",
                              members=peer_service.member_states(sr.db, self.remote_lv)),
            time.time())

    def test_waits_without_spending_retries_and_polls_at_the_base_interval(self):
        before = time.time()
        self._wait()
        p = self.task.function_params
        self.assertEqual(self.task.status, "suspended")
        self.assertEqual(self.task.retry, 4)
        # step 0 whatever the earlier retries: polled every base interval
        self.assertLessEqual(p["not_before"],
                             int(before) + constants.REPL_RETRY_BACKOFF_BASE_SEC + 1)
        self.assertIn("for 0s", self.task.function_result)
        self.assertIn("3c0825d7 in_restart, 92188ff1 online, be487f40 online",
                      self.task.function_result)
        self.event.assert_not_called()

    def test_the_wait_is_timed_and_alerts_once(self):
        self._wait()
        self.task.function_params["target_leaderless_since"] -= (
            constants.REPL_TARGET_LEADERLESS_ALERT_SEC + 5)
        self._wait()
        self.assertRegex(self.task.function_result, r"for 18\ds")
        self.event.assert_called_once()
        self.assertEqual(self.event.call_args.args[1], "LVS_22")
        self._wait()
        self.event.assert_called_once()     # rate-limited

    def test_a_leader_ends_the_wait(self):
        self._wait()
        sr._target_leader_found(self.task)
        self.assertNotIn("target_leaderless_since", self.task.function_params)
        self._wait()
        self.assertIn("for 0s", self.task.function_result)


if __name__ == "__main__":
    unittest.main()
