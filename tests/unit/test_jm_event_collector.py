"""test_jm_event_collector.py — JM events reach the cluster event log, once each.

jm_get_events returns everything the JM holds on every call; there is no
discard counterpart as there is for distrib events. So the collector has to
filter what it has already logged, or every poll would re-append the same
compression history to the cluster event log.

The other thing pinned down here is that JM and distrib are collected by
separate threads per node. A wedged poll of one source must not stall the
other, and a build too old to serve jm_get_events must not be re-probed every
few seconds forever.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import rpc_client
from simplyblock_core.controllers import events_controller
from simplyblock_core.models.events import EventObj
from simplyblock_core.services import main_distr_event_collector as collector


STARTED = {"timestamp": "2026-08-19T18:59:59.010000Z", "event_type": "jm_compression",
           "jm_vuid": "1", "status": "compression_started", "error_code": 0}
FINISHED = {"timestamp": "2026-08-19T19:01:04.220000Z", "event_type": "jm_compression",
            "jm_vuid": "1", "status": "compression_finished", "error_code": 0}
FAILED = {"timestamp": "2026-08-19T19:03:12.700000Z", "event_type": "jm_compression",
          "jm_vuid": "2", "status": "compression_failed", "error_code": 11}


class _Stop(Exception):
    """Breaks the collector poll loop, which is otherwise endless."""


class TestJmEventLogging(unittest.TestCase):
    """Every event received is written to the cluster event log."""

    def _log(self, event_dict):
        with patch.object(events_controller, "DBController"), \
                patch.object(events_controller, "log_event_based_on_level") as alert:
            obj = events_controller.log_jm_event("cl-1", "node-1", event_dict)
        return obj, alert

    def test_success_is_informational(self):
        obj, _ = self._log(FINISHED)
        self.assertEqual(obj.event_level, EventObj.LEVEL_INFO)
        self.assertEqual(obj.domain, events_controller.DOMAIN_JM)
        self.assertEqual(obj.event, "jm_compression")
        self.assertEqual(obj.message, "compression_finished")
        self.assertEqual(obj.vuid, 1)
        self.assertEqual(obj.node_id, "node-1")

    def test_failure_is_an_error_and_carries_the_code(self):
        obj, _ = self._log(FAILED)
        self.assertEqual(obj.event_level, EventObj.LEVEL_ERROR)
        self.assertIn("error_code=11", obj.message)
        self.assertEqual(obj.vuid, 2)

    def test_nonzero_error_code_outweighs_a_benign_status(self):
        obj, _ = self._log(dict(STARTED, error_code=5))
        self.assertEqual(obj.event_level, EventObj.LEVEL_ERROR)

    def test_unparseable_vuid_does_not_break_logging(self):
        obj, _ = self._log(dict(STARTED, jm_vuid=None))
        self.assertEqual(obj.vuid, -1)

    def test_the_raw_event_is_kept(self):
        obj, _ = self._log(FAILED)
        self.assertEqual(obj.object_dict, FAILED)

    def test_the_alerting_path_is_invoked(self):
        _, alert = self._log(FAILED)
        alert.assert_called_once()


class TestJmCollectorDeduplicates(unittest.TestCase):

    def _run(self, poll_results):
        snode = MagicMock()
        snode.cluster_id = "cl-1"
        client = MagicMock()
        client.jm_get_events.side_effect = poll_results
        snode.rpc_client.return_value = client

        logged = []

        def record(cluster_id, node_id, event_dict):
            logged.append(event_dict)
            return MagicMock()

        sleeps = [None] * (len(poll_results) - 1) + [_Stop()]
        with patch.object(collector, "db") as db, \
                patch.object(collector.events_controller, "log_jm_event", record), \
                patch.object(collector.time, "sleep", side_effect=sleeps):
            db.get_storage_node_by_id.return_value = snode
            collector.start_jm_event_collector_on_node("node-1")
        return logged

    def test_repeated_polls_log_each_event_once(self):
        batch = [STARTED, FINISHED, FAILED]
        logged = self._run([batch, batch, batch])
        self.assertEqual(logged, batch, "a re-read event was logged again")

    def test_new_events_still_get_through(self):
        logged = self._run([[STARTED], [STARTED, FINISHED]])
        self.assertEqual(logged, [STARTED, FINISHED])

    def test_a_failed_poll_is_not_fatal(self):
        logged = self._run([None, [STARTED]])
        self.assertEqual(logged, [STARTED])

    def test_empty_is_quiet(self):
        self.assertEqual(self._run([[], []]), [])


class TestUnsupportedBuild(unittest.TestCase):

    def setUp(self):
        collector.jm_unsupported_nodes.discard("node-1")
        collector.threads_maps.clear()

    def test_collector_stops_and_records_the_node(self):
        snode = MagicMock()
        client = MagicMock()
        client.jm_get_events.return_value = rpc_client.RPC_UNSUPPORTED
        snode.rpc_client.return_value = client
        with patch.object(collector, "db") as db, \
                patch.object(collector.events_controller, "log_jm_event") as log_jm:
            db.get_storage_node_by_id.return_value = snode
            collector.start_jm_event_collector_on_node("node-1")
        log_jm.assert_not_called()
        self.assertIn("node-1", collector.jm_unsupported_nodes)

    def test_such_a_node_is_not_respawned(self):
        collector.jm_unsupported_nodes.add("node-1")
        node = MagicMock()
        node.get_id.return_value = "node-1"
        with patch.object(collector.threading, "Thread") as thread:
            collector.ensure_collectors([node])
        targets = [c.kwargs.get("target") or c.args[0]
                   for c in thread.call_args_list]
        self.assertEqual(targets, [collector.start_event_collector_on_node])


class TestCollectorsRunInParallel(unittest.TestCase):

    def setUp(self):
        collector.threads_maps.clear()
        collector.jm_unsupported_nodes.clear()

    def _nodes(self, *ids):
        nodes = []
        for node_id in ids:
            node = MagicMock()
            node.get_id.return_value = node_id
            nodes.append(node)
        return nodes

    def test_each_node_gets_a_thread_per_source(self):
        with patch.object(collector.threading, "Thread") as thread:
            collector.ensure_collectors(self._nodes("a", "b"))
        self.assertEqual(thread.call_count, 4, "expected distr+jm per node")
        self.assertEqual(sorted(collector.threads_maps),
                         ["a:distr", "a:jm", "b:distr", "b:jm"])

    def test_live_collectors_are_not_restarted(self):
        with patch.object(collector.threading, "Thread") as thread:
            thread.return_value.is_alive.return_value = True
            collector.ensure_collectors(self._nodes("a"))
            first = thread.call_count
            collector.ensure_collectors(self._nodes("a"))
        self.assertEqual(thread.call_count, first, "collectors were respawned")

    def test_a_dead_collector_is_restarted_without_touching_the_other(self):
        distr_thread, jm_thread = MagicMock(), MagicMock()
        distr_thread.is_alive.return_value = True
        jm_thread.is_alive.return_value = False      # only JM died
        collector.threads_maps["a:distr"] = distr_thread
        collector.threads_maps["a:jm"] = jm_thread
        with patch.object(collector.threading, "Thread") as thread:
            collector.ensure_collectors(self._nodes("a"))
        targets = [c.kwargs.get("target") or c.args[0]
                   for c in thread.call_args_list]
        self.assertEqual(targets, [collector.start_jm_event_collector_on_node])


if __name__ == "__main__":
    unittest.main()
