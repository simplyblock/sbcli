"""test_lifecycle_alert_events.py — two conditions an operator must see in the
cluster event log.

1. A sync delete failing after the async delete already succeeded. The data is
   going away, but a node still holds its replica bdev and the volume is pinned
   in_deletion until the deferred task drains -- invisible from the volume list
   alone. The runner retries every 3 seconds, so the event fires only when the
   failure message CHANGES, not per retry.

2. An lvs journal accumulating more than JM_COMPRESSION_BACKLOG_ALERT_RECORDS
   records for compression. The alert latches: it fires on the upward crossing
   and re-arms only after the backlog falls below the re-arm fraction, so a
   count oscillating around the threshold cannot flap events every poll.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.services import main_distr_event_collector as collector
from simplyblock_core.services import tasks_runner_sync_lvol_del as sync_del


THRESHOLD = constants.JM_COMPRESSION_BACKLOG_ALERT_RECORDS
REARM = THRESHOLD * constants.JM_COMPRESSION_BACKLOG_REARM_FRACTION


class TestSyncDeleteFailureEvent(unittest.TestCase):

    def _task(self, previous_result=""):
        task = MagicMock()
        task.function_result = previous_result
        return task

    def _node(self):
        node = MagicMock()
        node.cluster_id = "cl-1"
        node.get_id.return_value = "node-1"
        return node

    def test_first_failure_is_logged_as_an_error(self):
        with patch.object(sync_del.events_controller, "log_event_cluster") as log:
            sync_del._log_sync_delete_failure(
                self._task(), self._node(), "LVS_1/LVOL_5", "connection refused")
        log.assert_called_once()
        kwargs = log.call_args.kwargs
        self.assertEqual(kwargs["event_level"], "Error")
        self.assertEqual(kwargs["event"], "SYNC_DELETE_FAILED")
        self.assertEqual(kwargs["node_id"], "node-1")
        self.assertIn("in_deletion", kwargs["message"])
        self.assertIn("LVS_1/LVOL_5", kwargs["message"])

    def test_a_repeat_of_the_same_failure_is_not_logged_again(self):
        """The runner retries every 3s; identical failures must not flood."""
        task = self._task(previous_result="boom")
        with patch.object(sync_del.events_controller, "log_event_cluster") as log:
            sync_del._log_sync_delete_failure(task, self._node(), "b", "boom")
        log.assert_not_called()

    def test_a_different_failure_is_logged(self):
        task = self._task(previous_result="connection refused")
        with patch.object(sync_del.events_controller, "log_event_cluster") as log:
            sync_del._log_sync_delete_failure(task, self._node(), "b", "timeout")
        log.assert_called_once()

    def test_event_log_trouble_does_not_break_the_runner(self):
        with patch.object(sync_del.events_controller, "log_event_cluster",
                          side_effect=RuntimeError("db gone")), \
                patch.object(sync_del, "logger"):
            sync_del._log_sync_delete_failure(
                self._task(), self._node(), "b", "boom")   # must not raise


class TestCompressionBacklogEvent(unittest.TestCase):

    def _snode(self):
        snode = MagicMock()
        snode.jm_vuid = 4
        snode.cluster_id = "cl-1"
        snode.lvstore = "LVS_4"
        snode.get_id.return_value = "node-1"
        return snode

    def _client(self, total_records):
        client = MagicMock()
        client.bdev_jm_get_status.return_value = {"total_records": total_records}
        return client

    def _run(self, total_records, alerted=False):
        with patch.object(collector.events_controller, "log_event_cluster") as log:
            latch = collector.check_jm_compression_backlog(
                self._client(total_records), self._snode(), alerted)
        return latch, log

    def test_crossing_the_threshold_fires_one_error_event(self):
        latch, log = self._run(THRESHOLD + 1)
        self.assertTrue(latch)
        log.assert_called_once()
        kwargs = log.call_args.kwargs
        self.assertEqual(kwargs["event_level"], "Error")
        self.assertEqual(kwargs["event"], "JM_COMPRESSION_BACKLOG")
        self.assertIn("LVS_4", kwargs["message"])
        self.assertIn(str(THRESHOLD + 1), kwargs["message"])

    def test_below_the_threshold_is_quiet(self):
        latch, log = self._run(THRESHOLD - 1)
        self.assertFalse(latch)
        log.assert_not_called()

    def test_latched_alert_does_not_repeat_while_high(self):
        latch, log = self._run(THRESHOLD + 5, alerted=True)
        self.assertTrue(latch)
        log.assert_not_called()

    def test_oscillation_around_the_threshold_does_not_flap(self):
        """Between the re-arm line and the threshold nothing fires either way."""
        just_below = int(THRESHOLD * 0.95)
        latch, log = self._run(just_below, alerted=True)
        self.assertTrue(latch, "must stay latched until the re-arm line")
        log.assert_not_called()

    def test_rearm_below_the_fraction_then_fire_again(self):
        with patch.object(collector, "logger"):
            latch, log = self._run(int(REARM) - 1, alerted=True)
        self.assertFalse(latch, "backlog drained; the alert must re-arm")
        log.assert_not_called()
        latch, log = self._run(THRESHOLD + 1, alerted=latch)
        self.assertTrue(latch)
        log.assert_called_once()

    def test_a_node_without_a_jm_is_skipped(self):
        snode = self._snode()
        snode.jm_vuid = 0
        client = MagicMock()
        with patch.object(collector.events_controller, "log_event_cluster") as log:
            self.assertFalse(collector.check_jm_compression_backlog(
                client, snode, False))
        client.bdev_jm_get_status.assert_not_called()
        log.assert_not_called()

    def test_a_failing_rpc_is_not_fatal_and_keeps_the_latch(self):
        client = MagicMock()
        client.bdev_jm_get_status.side_effect = RuntimeError("node down")
        with patch.object(collector, "logger"):
            self.assertTrue(collector.check_jm_compression_backlog(
                client, self._snode(), True))


if __name__ == "__main__":
    unittest.main()
