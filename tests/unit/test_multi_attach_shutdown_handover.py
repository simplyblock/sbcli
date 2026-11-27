"""Multi-attach: a planned shutdown hands its LUNs over; the monitor backstops
any stopped node that still serves one.

2026-11-26 bed (multi-attach, 1+0, FTT1, 4 nodes): `sbctl sn shutdown <node>`
stopped SPDK and marked the node's device unavailable, but never took its LUN
over -- the takeover only ran when the monitor itself declared the node lost,
and a shutdown writes OFFLINE itself. On 1+0 every lvstore stripes over every
device: reads failed on all nodes, SPDK fenced and re-fenced ports, LVS_2 and
LVS_8 went leaderless and the cluster suspended. The failed-restart cleanup of
restart_storage_node wrote OFFLINE the same way.

  - shutdown_storage_node: on a multi-attach cluster the LUNs the node serves
    are never marked unavailable nor their peers' controllers detached; they
    are taken over (awaited, bounded) right after the kill, graceful or
    forced. Regular clusters and whole-cluster stops call everything as
    before.
  - _hand_over_luns: the takeover machinery end to end, the failure path
    (device unavailable, as on node loss), a takeover that does not settle.
  - storage_node_monitor._take_over_luns_of_stopped_node: an OFFLINE node, or
    IN_SHUTDOWN past a grace with SPDK dead, still serving a LUN -> takeover;
    never RESTARTING, a live restart claim, a regular cluster, an unavailable
    device or a cluster in activation.
"""

import datetime
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import db_controller as db_module
from simplyblock_core import storage_node_ops
from simplyblock_core.controllers import lun_takeover
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice, RemoteDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import storage_node_monitor
from tests.unit.test_lun_takeover import DEV_ID, _bed, _TakeoverCase


def _shutdown(db, node_id="node-a", force=False, **kw):
    """Run shutdown_storage_node on a _bed() database; returns (result,
    order of the steps, the args each step got)."""
    order, args = [], {}
    node = db.nodes[node_id]
    node.status = StorageNode.STATUS_ONLINE
    node.agent.spdk_process_kill.side_effect = lambda *a, **k: order.append("kill")
    db.kv_store = None

    def _rec(name):
        def _f(*a, **k):
            order.append(name)
            args[name] = a
            return 0
        return _f

    tasks = MagicMock()
    tasks.get_active_lvol_migration.return_value = False
    with patch.object(storage_node_ops, "DBController", return_value=db), \
            patch.object(storage_node_ops, "tasks_controller", tasks), \
            patch.object(storage_node_ops, "check_node_shutdown_preconditions",
                         return_value=(True, "")), \
            patch.object(storage_node_ops, "_remember_cluster_arbitration"), \
            patch.object(storage_node_ops, "set_node_status", return_value=True), \
            patch.object(storage_node_ops, "_note_planned_loss"), \
            patch.object(storage_node_ops, "trigger_ana_failover_for_node"), \
            patch.object(storage_node_monitor, "apply_node_loss_status"), \
            patch.object(StorageNode, "write_to_db"), \
            patch.object(storage_node_ops, "_broadcast_devices_unavailable",
                         side_effect=_rec("unavailable")), \
            patch.object(storage_node_ops, "_detach_remote_controllers_from_peers",
                         side_effect=_rec("detach")), \
            patch.object(storage_node_ops, "_hand_over_luns",
                         side_effect=_rec("handover")):
        res = storage_node_ops.shutdown_storage_node(node_id, force=force, **kw)
    return res, order, args


class TestShutdownHandsOverLuns(unittest.TestCase):

    def test_graceful_shutdown_hands_over_after_the_kill(self):
        db, dev = _bed()
        res, order, args = _shutdown(db)
        self.assertTrue(res)
        self.assertEqual(order, ["unavailable", "detach", "kill", "handover"])
        # Loop 1 and Loop 2 leave the handed-over LUN alone.
        self.assertEqual(args["unavailable"][1], frozenset({DEV_ID}))
        self.assertEqual(args["detach"][2], frozenset({DEV_ID}))
        node, devices, reason = args["handover"]
        self.assertEqual(node.get_id(), "node-a")
        self.assertEqual([d.get_id() for d in devices], [DEV_ID])
        self.assertEqual(reason, "node shutdown")

    def test_forced_shutdown_hands_over_after_the_kill_too(self):
        db, dev = _bed()
        res, order, args = _shutdown(db, force=True)
        self.assertTrue(res)
        self.assertEqual(order, ["kill", "handover", "unavailable"])
        self.assertEqual(args["unavailable"][1], frozenset({DEV_ID}))
        self.assertEqual(args["handover"][2], "node shutdown (forced)")

    def test_luns_taken_over_from_other_owners_are_handed_over_too(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"          # node-a's own LUN: served by B
        foreign = NVMeDevice()
        foreign.uuid, foreign.node_id, foreign.serving_node_id = "foreign", "node-c", "node-a"
        foreign.multi_attach, foreign.status = True, NVMeDevice.STATUS_ONLINE
        db.nodes["node-c"].nvme_devices = [foreign]
        res, order, args = _shutdown(db)
        self.assertEqual([d.get_id() for d in args["handover"][1]], ["foreign"])
        self.assertEqual(args["unavailable"][1], frozenset({"foreign"}))

    def test_unavailable_lun_is_not_handed_over(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        res, order, args = _shutdown(db)
        self.assertEqual(order, ["unavailable", "detach", "kill"])
        self.assertEqual(args["unavailable"], (db.nodes["node-a"],))

    def test_regular_cluster_unchanged(self):
        db, dev = _bed(multi=False)
        res, order, args = _shutdown(db)
        self.assertTrue(res)
        self.assertEqual(order, ["unavailable", "detach", "kill"])
        self.assertEqual(len(args["unavailable"]), 1)
        self.assertEqual(len(args["detach"]), 2)
        res, order, args = _shutdown(db, force=True)
        self.assertEqual(order, ["kill", "unavailable"])
        self.assertEqual(len(args["unavailable"]), 1)

    def test_whole_cluster_stop_does_not_hand_over(self):
        db, dev = _bed()
        res, order, args = _shutdown(db, force=True, hand_over_luns=False)
        self.assertTrue(res)
        self.assertEqual(order, ["kill", "unavailable"])
        self.assertEqual(len(args["unavailable"]), 1)

    def test_failed_kill_hands_nothing_over(self):
        db, dev = _bed()
        node = db.nodes["node-a"]

        def _fail(*a, **k):
            raise storage_node_ops.SNodeClientException("kill failed")
        with patch.object(storage_node_ops, "_hand_over_luns") as handover:
            node.agent.spdk_process_kill.side_effect = _fail
            tasks = MagicMock()
            tasks.get_active_lvol_migration.return_value = False
            db.kv_store = None
            with patch.object(storage_node_ops, "DBController", return_value=db), \
                    patch.object(storage_node_ops, "tasks_controller", tasks), \
                    patch.object(storage_node_ops, "check_node_shutdown_preconditions",
                                 return_value=(True, "")), \
                    patch.object(storage_node_ops, "_remember_cluster_arbitration"), \
                    patch.object(storage_node_ops, "set_node_status", return_value=True), \
                    patch.object(storage_node_ops, "_note_planned_loss"), \
                    patch.object(StorageNode, "write_to_db"), \
                    patch.object(storage_node_ops, "_broadcast_devices_unavailable"), \
                    patch.object(storage_node_ops, "_detach_remote_controllers_from_peers"):
                self.assertFalse(storage_node_ops.shutdown_storage_node("node-a", force=True))
        handover.assert_not_called()


class TestShutdownLoopsSkipHandedOver(unittest.TestCase):

    def test_loop1_keeps_the_handed_over_device_online(self):
        db, dev = _bed()
        node = db.nodes["node-a"]
        node.jm_device = None
        with patch.object(storage_node_ops, "device_controller") as dc:
            storage_node_ops._broadcast_devices_unavailable(node, frozenset({DEV_ID}))
            dc.device_set_unavailable.assert_not_called()
            storage_node_ops._broadcast_devices_unavailable(node)
            dc.device_set_unavailable.assert_called_once_with(DEV_ID)

    def test_loop2_keeps_the_peers_controller_to_the_handed_over_lun(self):
        db, dev = _bed()
        node = db.nodes["node-a"]
        node.status = StorageNode.STATUS_IN_SHUTDOWN
        peer = db.nodes["node-b"]
        rem = RemoteDevice()
        rem.uuid, rem.node_id, rem.remote_bdev = DEV_ID, "node-a", f"remote_alceml_{DEV_ID}n1"
        peer.remote_devices = [rem]
        peer.remote_jm_devices = []
        for n in ("node-c", "node-d"):
            db.nodes[n].remote_devices, db.nodes[n].remote_jm_devices = [], []
        storage_node_ops._detach_remote_controllers_from_peers(node, db, frozenset({DEV_ID}))
        self.assertEqual(peer.spdk.ops("detach"), [])
        storage_node_ops._detach_remote_controllers_from_peers(node, db)
        self.assertEqual(len(peer.spdk.ops("detach")), 1)


class TestHandOverLuns(_TakeoverCase):

    def _run(self, db, **kw):
        self.m_DBController.return_value = db
        node = db.nodes["node-a"]
        node.status = StorageNode.STATUS_IN_SHUTDOWN
        dev = db.get_storage_device_by_id(DEV_ID)
        with patch.object(storage_node_ops, "DBController", return_value=db), \
                patch.object(storage_node_monitor, "db", db):
            return storage_node_ops._hand_over_luns(node, [dev], "node shutdown", **kw)

    def test_shutdown_handover_takes_the_lun_over(self):
        db, _ = _bed()
        self.assertEqual(self._run(db), [])
        dev = self.dev(db)
        self.assertEqual(dev.serving_node(), "node-b")
        self.assertIn("node-a", dev.fenced_node_ids)
        self.assertEqual(dev.status, NVMeDevice.STATUS_ONLINE)
        msgs = [e["message"] for e in self.events]
        self.assertTrue(any(m.startswith("Planned handover of LUN") for m in msgs))
        self.assertTrue(any("taken over from node node-a by node node-b" in m for m in msgs))

    def test_failed_takeover_sets_the_device_unavailable(self):
        db, _ = _bed()

        def _unavailable(did):
            self.dev(db).status = NVMeDevice.STATUS_UNAVAILABLE
        with patch.object(lun_takeover, "take_over_lun_outcome", return_value=lun_takeover.FAILED), \
                patch.object(storage_node_monitor.device_controller, "device_set_unavailable",
                             side_effect=_unavailable) as unavailable:
            self.assertEqual(self._run(db), [])
        unavailable.assert_called_once_with(DEV_ID)
        self.assertEqual(self.dev(db).serving_node(), "node-a")

    def test_takeover_that_does_not_settle_is_reported(self):
        db, _ = _bed()
        with patch.object(lun_takeover, "take_over_lun_outcome",
                          return_value=lun_takeover.IN_PROGRESS), \
                patch.object(storage_node_ops, "LUN_HANDOVER_POLL_SEC", 0.01):
            self.assertEqual(self._run(db, wait_sec=0.1), [DEV_ID])
        self.assertTrue(any("not handed over" in e["message"] for e in self.events))

    def test_takeover_by_another_process_is_awaited(self):
        db, _ = _bed()
        polls = []

        def _poll(_s):
            polls.append(1)
            if len(polls) == 2:
                self.dev(db).serving_node_id = "node-c"
        with patch.object(lun_takeover, "take_over_lun_outcome",
                          return_value=lun_takeover.IN_PROGRESS), \
                patch.object(storage_node_ops.time, "sleep", side_effect=_poll):
            self.assertEqual(self._run(db, wait_sec=30), [])
        self.assertEqual(len(polls), 2)


class TestStoppedNodeBackstop(unittest.TestCase):

    def setUp(self):
        storage_node_monitor._stopped_node_takeover_last.clear()
        self.addCleanup(storage_node_monitor._stopped_node_takeover_last.clear)

    def _run(self, db, node_id="node-a", spdk_dead=True):
        started = []
        with patch.object(storage_node_monitor, "db", db), \
                patch.object(storage_node_monitor, "_start_lun_takeover",
                             side_effect=lambda did, nid, why: started.append((did, nid))), \
                patch.object(storage_node_monitor, "_spdk_is_dead", return_value=spdk_dead):
            ret = storage_node_monitor._take_over_luns_of_stopped_node(db.nodes[node_id])
        self.assertEqual(ret, [d for d, _ in started])
        return started

    def test_offline_node_still_serving_is_taken_over(self):
        # E.g. the failed-restart cleanup of restart_storage_node, which
        # writes OFFLINE directly: nothing else would take the LUN over.
        db, dev = _bed()
        self.assertEqual(self._run(db), [(DEV_ID, "node-a")])

    def test_once_per_interval(self):
        db, dev = _bed()
        self.assertEqual(len(self._run(db)), 1)
        self.assertEqual(self._run(db), [])
        storage_node_monitor._stopped_node_takeover_last[DEV_ID] -= \
            storage_node_monitor.STOPPED_NODE_TAKEOVER_INTERVAL_SEC + 1
        self.assertEqual(len(self._run(db)), 1)

    def test_restarting_or_online_node_is_left_alone(self):
        for status in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_ONLINE,
                       StorageNode.STATUS_UNREACHABLE):
            with self.subTest(status=status):
                db, dev = _bed()
                db.nodes["node-a"].status = status
                self.assertEqual(self._run(db), [])

    def test_live_restart_claim_is_left_alone(self):
        db, dev = _bed()
        with patch.object(db_module, "restart_claim_active", return_value="cli:123"):
            self.assertEqual(self._run(db), [])

    def test_restart_claim_read_from_the_node(self):
        db, dev = _bed()
        node = db.nodes["node-a"]
        node.restart_claim_owner = "tasks-runner:9"
        node.restart_claim_ts = datetime.datetime.now(datetime.UTC).isoformat()
        self.assertEqual(self._run(db), [])

    def test_regular_cluster_unchanged(self):
        db, dev = _bed(multi=False)
        self.assertEqual(self._run(db), [])

    def test_unavailable_device_is_left_alone(self):
        db, dev = _bed(dev_status=NVMeDevice.STATUS_UNAVAILABLE)
        self.assertEqual(self._run(db), [])

    def test_lun_served_elsewhere_is_left_alone(self):
        db, dev = _bed()
        dev.serving_node_id = "node-b"
        self.assertEqual(self._run(db), [])

    def test_cluster_in_activation_is_left_alone(self):
        db, dev = _bed()
        db.cluster.status = Cluster.STATUS_IN_ACTIVATION
        self.assertEqual(self._run(db), [])

    def _in_shutdown(self, db, seconds_ago):
        node = db.nodes["node-a"]
        node.status = StorageNode.STATUS_IN_SHUTDOWN
        node.shutdown_since = (datetime.datetime.now(datetime.UTC)
                               - datetime.timedelta(seconds=seconds_ago)).isoformat()

    def test_in_shutdown_is_left_to_the_shutdown_within_the_grace(self):
        db, dev = _bed()
        self._in_shutdown(db, 1)
        self.assertEqual(self._run(db), [])

    def test_in_shutdown_past_the_grace_with_spdk_dead(self):
        db, dev = _bed()
        self._in_shutdown(db, storage_node_monitor.STOPPED_NODE_IN_SHUTDOWN_GRACE_SEC + 5)
        self.assertEqual(self._run(db), [(DEV_ID, "node-a")])

    def test_in_shutdown_past_the_grace_with_spdk_alive(self):
        db, dev = _bed()
        self._in_shutdown(db, storage_node_monitor.STOPPED_NODE_IN_SHUTDOWN_GRACE_SEC + 5)
        self.assertEqual(self._run(db, spdk_dead=False), [])

    def test_check_node_runs_the_backstop_for_a_stuck_shutdown(self):
        db, dev = _bed()
        self._in_shutdown(db, 1)
        with patch.object(storage_node_monitor, "db", db), \
                patch.object(storage_node_monitor, "_take_over_luns_of_stopped_node") as backstop:
            self.assertFalse(storage_node_monitor.check_node(db.nodes["node-a"]))
        backstop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
