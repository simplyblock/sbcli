"""services/lun_watchdog: fast loss detection of nodes serving multi-attach
LUNs, and the immediate takeover of their LUNs.

  - probe: SPDK RPC first; the node agent decides between a killed SPDK, a
    slow one and a node that is gone.
  - decisions: SPDK down -> lost at once; no answer -> lost after
    MULTI_ATTACH_LOST_AFTER_SEC; SPDK running -> never; an answer resets.
  - fire: every LUN the node serves, rate-limited; never on a regular
    cluster; never while this process may be the one cut off.
  - watched: multi-attach clusters, nodes that serve a LUN.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import array, constants
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import lun_watchdog as wd
from tests.unit.test_lun_takeover import DEV_ID, _bed

LOST = constants.MULTI_ATTACH_LOST_AFTER_SEC


class _Db:
    """FakeDB of test_lun_takeover with get_clusters."""

    def __init__(self, db):
        self.inner = db

    def __getattr__(self, name):
        return getattr(self.inner, name)

    def get_clusters(self):
        return [self.inner.cluster]


def _setup(serving_a=True):
    """Bed of test_lun_takeover with node-a ONLINE and serving its LUN."""
    db, dev = _bed()
    a = db.nodes["node-a"]
    a.status = StorageNode.STATUS_ONLINE
    db.cluster.max_fault_tolerance = 1
    if not serving_a:
        dev.serving_node_id = "node-b"
    return _Db(db), dev


class TestProbe(unittest.TestCase):

    def _node(self, rpc_ok=True, agent=(True, None), agent_raises=False):
        node = MagicMock()
        node.rpc_port, node.cluster_id = 5260, "cluster-1"
        rpc = MagicMock()
        if not rpc_ok:
            rpc.get_version.side_effect = ConnectionError("refused")
        node.rpc_client.return_value = rpc
        api = MagicMock()
        if agent_raises:
            api.spdk_process_is_up.side_effect = RuntimeError("timeout")
        else:
            api.spdk_process_is_up.return_value = agent
        node.client.return_value = api
        return node

    def test_rpc_answers(self):
        node = self._node()
        self.assertEqual(wd.probe(node), wd.OK)
        node.client.assert_not_called()
        kw = node.rpc_client.call_args.kwargs
        self.assertEqual((kw["timeout"], kw["retry"]), (constants.MULTI_ATTACH_PROBE_TIMEOUT_SEC, 0))

    def test_spdk_killed(self):
        self.assertEqual(wd.probe(self._node(False, (False, "socket down"))), wd.SPDK_DOWN)

    def test_spdk_slow(self):
        self.assertEqual(wd.probe(self._node(False, (True, None))), wd.SPDK_UP)

    def test_node_gone(self):
        node = self._node(False, agent_raises=True)
        self.assertEqual(wd.probe(node), wd.NO_ANSWER)
        kw = node.client.call_args.kwargs
        self.assertEqual((kw["retry"], kw["connect_retry"]), (0, 0))

    def test_agent_without_verdict(self):
        self.assertEqual(wd.probe(self._node(False, (None, "error"))), wd.NO_ANSWER)

    def test_failing_asks_both_at_once(self):
        node = self._node(False, agent_raises=True)
        self.assertEqual(wd.probe(node, failing=True), wd.NO_ANSWER)
        node.client.assert_called_once()
        self.assertEqual(wd.probe(self._node(True), failing=True), wd.OK)


class TestDecisions(unittest.TestCase):

    def setUp(self):
        self.db, self.dev = _setup()
        self.w = wd.Watchdog(MagicMock(), db=self.db)
        self.a = self.db.nodes["node-a"]

    def test_spdk_down_is_lost_at_once(self):
        self.assertEqual(self.w.record(self.a, wd.SPDK_DOWN, 100.0), "SPDK not running")

    def test_no_answer_is_lost_after_the_window(self):
        # Probes of a node that does not answer take PROBE_TIMEOUT each.
        t = 100.0
        step = constants.MULTI_ATTACH_PROBE_TIMEOUT_SEC
        reasons = []
        while t < 100.0 + LOST + step:
            reasons.append(self.w.record(self.a, wd.NO_ANSWER, t))
            t += step
        self.assertTrue(all(r is None for r in reasons[:-1]), reasons)
        self.assertTrue(reasons[-1].startswith("no answer for"))
        # Detection inside the 9 s stall budget with room for the takeover.
        self.assertLessEqual(LOST + 2 * step, 4.0)

    def test_running_spdk_is_never_lost(self):
        for t in range(60):
            self.assertIsNone(self.w.record(self.a, wd.SPDK_UP, 100.0 + t))

    def test_an_answer_within_the_window_cancels(self):
        self.assertIsNone(self.w.record(self.a, wd.NO_ANSWER, 100.0))
        self.assertIsNone(self.w.record(self.a, wd.OK, 100.0 + LOST - 0.5))
        self.assertIsNone(self.w.record(self.a, wd.NO_ANSWER, 100.0 + LOST + 0.1))


class TestFire(unittest.TestCase):

    def setUp(self):
        self.db, self.dev = _setup()
        self.start = MagicMock()
        self.w = wd.Watchdog(self.start, db=self.db)
        # Every node probed fine at t=100.
        for n in self.db.nodes.values():
            self.w.record(n, wd.OK, 100.0)

    def _lose(self, nid, t=101.0):
        reason = self.w.record(self.db.nodes[nid], wd.SPDK_DOWN, t)
        return self.w.fire(nid, reason, t)

    def test_takes_over_every_lun_the_node_serves(self):
        self.assertEqual(self._lose("node-a"), [DEV_ID])
        self.start.assert_called_once_with(DEV_ID, "node-a", "node lost: SPDK not running")

    def test_rate_limited(self):
        self._lose("node-a", 101.0)
        self._lose("node-a", 102.0)
        self.assertEqual(self.start.call_count, 1)
        for nid in ("node-b", "node-c", "node-d"):
            self.w.record(self.db.nodes[nid], wd.OK, 101.0 + wd.REFIRE_SEC)
        self._lose("node-a", 101.0 + wd.REFIRE_SEC)
        self.assertEqual(self.start.call_count, 2)

    def test_lun_served_elsewhere_is_not_touched(self):
        self.dev.serving_node_id = "node-b"
        self.assertEqual(self._lose("node-a"), [])
        self.start.assert_not_called()

    def test_failed_device_is_not_taken_over(self):
        self.dev.status = NVMeDevice.STATUS_UNAVAILABLE
        self.assertEqual(self._lose("node-a"), [])

    def test_regular_cluster_never(self):
        self.db.cluster.multi_attach = False
        self.assertEqual(self._lose("node-a"), [])

    def test_node_in_restart_is_left_to_the_restart(self):
        self.db.nodes["node-a"].status = StorageNode.STATUS_RESTARTING
        self.assertEqual(self._lose("node-a"), [])

    def test_not_when_more_nodes_fail_than_tolerated(self):
        # This monitor lost its own network: every node looks gone.
        self.w.record(self.db.nodes["node-b"], wd.NO_ANSWER, 100.5)
        self.assertEqual(self._lose("node-a"), [])
        self.start.assert_not_called()

    def test_two_lost_nodes_with_ftt2(self):
        self.db.cluster.max_fault_tolerance = 2
        self.w.record(self.db.nodes["node-b"], wd.NO_ANSWER, 100.5)
        self.assertEqual(self._lose("node-a"), [DEV_ID])

    def test_not_when_no_other_node_answered_recently(self):
        self.assertEqual(self._lose("node-a", 100.0 + LOST + 10), [])
        self.start.assert_not_called()


class TestWatched(unittest.TestCase):

    def test_nodes_serving_a_lun(self):
        db, _ = _setup()
        w = wd.Watchdog(MagicMock(), db=db)
        with patch.object(array, "cached_plugin_for_cluster"):
            self.assertEqual(sorted(w.watched()), ["node-a"])

    def test_takeover_moves_the_watch(self):
        db, _ = _setup(serving_a=False)
        w = wd.Watchdog(MagicMock(), db=db)
        with patch.object(array, "cached_plugin_for_cluster"):
            self.assertEqual(sorted(w.watched()), ["node-b"])

    def test_regular_and_activating_clusters_are_not_watched(self):
        for change in ("regular", "activation"):
            with self.subTest(change=change):
                db, _ = _setup()
                if change == "regular":
                    db.cluster.multi_attach = False
                else:
                    db.cluster.status = Cluster.STATUS_IN_ACTIVATION
                w = wd.Watchdog(MagicMock(), db=db)
                self.assertEqual(w.watched(), {})

    def test_offline_node_is_not_watched(self):
        db, _ = _setup()
        db.nodes["node-a"].status = StorageNode.STATUS_OFFLINE
        w = wd.Watchdog(MagicMock(), db=db)
        with patch.object(array, "cached_plugin_for_cluster"):
            self.assertEqual(w.watched(), {})

    def test_array_connection_is_warmed(self):
        db, _ = _setup()
        db.cluster.array_plugin = "fake-test"
        w = wd.Watchdog(MagicMock(), db=db)
        with patch.object(array, "cached_plugin_for_cluster") as conn, \
                patch.object(wd.threading, "Thread") as thread:
            thread.side_effect = lambda target, **_k: MagicMock(start=target)
            w.watched()
            w.watched()
        conn.assert_called_once()
        self.assertEqual(conn.call_args.kwargs["max_age"], constants.ARRAY_PLUGIN_CACHE_SEC / 2)


class TestPluginCache(unittest.TestCase):

    def setUp(self):
        from simplyblock_core.array.fake import FakeArrayPlugin
        self.cls = FakeArrayPlugin
        FakeArrayPlugin.reset()
        array.register_plugin("fake-cache", FakeArrayPlugin)
        self.addCleanup(array.unregister_plugin, "fake-cache")
        array.forget_plugin()
        self.addCleanup(array.forget_plugin)
        self.cluster = Cluster()
        self.cluster.uuid = "cluster-c"
        self.cluster.array_plugin = "fake-cache"

    def test_reused_until_the_configuration_changes(self):
        p1 = array.cached_plugin_for_cluster(self.cluster)
        self.assertIs(array.cached_plugin_for_cluster(self.cluster), p1)
        self.cluster.array_options = {"x": 1}
        self.assertIsNot(array.cached_plugin_for_cluster(self.cluster), p1)
        self.assertEqual(len(self.cls.connected), 2)

    def test_expiry_and_forget(self):
        p1 = array.cached_plugin_for_cluster(self.cluster)
        self.assertIsNot(array.cached_plugin_for_cluster(self.cluster, max_age=0), p1)
        p2 = array.cached_plugin_for_cluster(self.cluster)
        array.forget_plugin("cluster-c")
        self.assertIsNot(array.cached_plugin_for_cluster(self.cluster), p2)


if __name__ == "__main__":
    unittest.main()
