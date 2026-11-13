"""A distrib IO error must reach an operator (prod ams1, 2026-11-01).

The distrib event collector files every ``error_read`` / ``error_write`` /
``error_unmap`` / ``error_open`` a distrib reports as a cluster-log event
with status ``distr error <vuid>`` -- and did nothing else. Those events say
a stripe could not be served from the chunks the distrib can reach, i.e. the
stripe has lost its redundancy and every volume on the owning lvstore fails
that IO the next time one of the stripe's nodes is down. On ams1 the first
``distr error 7857`` was logged on 2026-10-18 12:36; the planned reboot that
turned it into an outage came two weeks later, and nothing in between had
named the lvstore or failed a check.

Two surfaces now carry the signal:

* the collector raises an ERROR event on the owning node, once per
  (distrib, error) per DISTR_ERROR_ALERT_DEBOUNCE_SEC -- the client retried
  the same read every 20 s, so without the debounce one defect is hundreds of
  alerts. The alert is raised after the event is written and never costs the
  event (review of #1580);
* ``sbctl volume check`` / ``sbctl cluster check`` report the lvstore's
  distrib errors from the last DISTR_ERROR_LOOKBACK_SEC, fail while one is
  still arriving (DISTR_ERROR_ACTIVE_SEC) and warn about older ones.
"""

import logging
import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core import constants
from simplyblock_core.controllers import health_controller
from simplyblock_core.models.events import EventObj
from simplyblock_core.services import main_distr_event_collector as collector

VUID = 7857
DEBOUNCE = constants.DISTR_ERROR_ALERT_DEBOUNCE_SEC
ACTIVE = constants.DISTR_ERROR_ACTIVE_SEC


def _node(node_id, lvstore, vuids):
    node = MagicMock()
    node.get_id.return_value = node_id
    node.lvstore = lvstore
    node.cluster_id = "cl-1"
    node.lvstore_stack = [
        {"type": "bdev_distr", "name": f"distrib_{vuids[0]}"},
        {"type": "bdev_raid", "name": f"raid_{lvstore}",
         "distribs_list": [f"distrib_{v}" for v in vuids]},
        {"type": "bdev_lvstore", "name": lvstore},
    ]
    return node


def _nodes():
    return [
        _node("438a0878", "LVS_5685", [11, 12, 13, 14, 15]),
        _node("d3a07483", "LVS_4033", [7857, 5620, 4422, 4950, 4832]),
    ]


def _event(message="error_read", count=2, vuid=VUID, node_id="438a0878", date=1793522115597,
           status=None):
    ev = EventObj()
    ev.uuid = "ev-1"
    ev.cluster_uuid = "cl-1"
    ev.node_id = node_id
    ev.event = "device_status"
    ev.storage_id = -1
    ev.message = message
    ev.count = count
    ev.vuid = vuid
    ev.date = date
    ev.status = f"distr error {vuid}" if status is None else status
    ev.object_dict = {"vuid": vuid, "status": message}
    return ev


class TestOwnerLookup(unittest.TestCase):

    def test_the_distrib_is_mapped_to_the_lvstore_that_holds_it(self):
        owner = collector.lvstore_owner_of_distrib(VUID, _nodes())
        self.assertEqual(owner.lvstore, "LVS_4033")

    def test_an_unknown_distrib_has_no_owner(self):
        self.assertIsNone(collector.lvstore_owner_of_distrib(99999, _nodes()))

    def test_a_node_without_a_stack_is_skipped(self):
        bare = MagicMock()
        bare.lvstore_stack = []
        self.assertIsNone(collector.lvstore_owner_of_distrib(VUID, [bare]))


class TestTheAlert(unittest.TestCase):

    def setUp(self):
        collector._distr_error_alerted.clear()
        self.addCleanup(collector._distr_error_alerted.clear)
        self.log = logging.getLogger("test")
        self.loads = 0

    def _load_nodes(self):
        self.loads += 1
        return _nodes()

    def _alert(self, event, now, vuid=VUID):
        return collector.alert_distr_error(event, vuid, self._load_nodes, self.log, now=now)

    def test_the_first_error_raises_an_event_on_the_owning_node(self):
        with patch.object(collector.storage_events, "snode_distrib_io_error") as alert:
            fired = self._alert(_event(), now=1000.0)
        self.assertTrue(fired)
        alert.assert_called_once()
        owner, vuid, error, count, reported_by = alert.call_args.args
        self.assertEqual(owner.lvstore, "LVS_4033")
        self.assertEqual((vuid, error, count, reported_by), (VUID, "error_read", 2, "438a0878"))

    def test_the_same_error_inside_the_window_is_not_raised_again(self):
        """128 events in nine minutes on ams1; one alert is the signal."""
        with patch.object(collector.storage_events, "snode_distrib_io_error") as alert:
            self._alert(_event(), now=1000.0)
            for i in range(1, 30):
                self._alert(_event(), now=1000.0 + 20 * i)
        alert.assert_called_once()

    def test_suppressed_repeats_do_not_load_the_node_list(self):
        with patch.object(collector.storage_events, "snode_distrib_io_error"):
            self._alert(_event(), now=1000.0)
            for i in range(1, 30):
                self._alert(_event(), now=1000.0 + 20 * i)
        self.assertEqual(self.loads, 1)

    def test_the_same_error_after_the_window_is_raised_again(self):
        with patch.object(collector.storage_events, "snode_distrib_io_error") as alert:
            self._alert(_event(), now=1000.0)
            self._alert(_event(), now=1000.0 + DEBOUNCE)
        self.assertEqual(alert.call_count, 2)

    def test_a_different_error_on_the_same_distrib_is_its_own_alert(self):
        with patch.object(collector.storage_events, "snode_distrib_io_error") as alert:
            self._alert(_event("error_read"), now=1000.0)
            self._alert(_event("error_write"), now=1001.0)
        self.assertEqual(alert.call_count, 2)

    def test_an_unowned_distrib_is_logged_and_not_alerted(self):
        with patch.object(collector.storage_events, "snode_distrib_io_error") as alert, \
             self.assertLogs(self.log, level="ERROR"):
            fired = self._alert(_event(vuid=99999), now=1000.0, vuid=99999)
        self.assertFalse(fired)
        alert.assert_not_called()

    def test_a_failed_alert_is_not_debounced(self):
        """The key is stamped once the alert is out, so the next event
        retries instead of being silenced for a window."""
        with patch.object(collector.storage_events, "snode_distrib_io_error",
                          side_effect=[KeyError("node gone"), None]) as alert:
            with self.assertRaises(KeyError):
                self._alert(_event(), now=1000.0)
            self.assertTrue(self._alert(_event(), now=1020.0))
        self.assertEqual(alert.call_count, 2)


class TestTheEventIsWrittenFirst(unittest.TestCase):
    """The regression Hamdy found: the alert ran before ``write_to_db``, so a
    failing alert left the event in status ``new`` (never counted by the
    check surfaces), made the batch handler re-process the page, and had
    already armed the debounce."""

    def setUp(self):
        collector._distr_error_alerted.clear()
        self.addCleanup(collector._distr_error_alerted.clear)
        self.log = logging.getLogger("test")

    def _process(self, alert_side_effect=None):
        ev = _event(status="new")
        order = []
        with patch.object(collector, "db") as db, \
             patch.object(EventObj, "write_to_db", autospec=True,
                          side_effect=lambda self_, kv: order.append("write")), \
             patch.object(collector.storage_events, "snode_distrib_io_error",
                          side_effect=alert_side_effect or (lambda *a: order.append("alert"))) as alert:
            db.get_storage_nodes_by_cluster_id.return_value = _nodes()
            collector.process_event(ev, self.log)
        return ev, order, alert

    def test_the_status_is_set_and_written_before_the_alert(self):
        ev, order, alert = self._process()
        self.assertEqual(ev.status, f"distr error {VUID}")
        self.assertEqual(order, ["write", "alert"])
        alert.assert_called_once()

    def test_a_failing_alert_costs_neither_the_event_nor_the_batch(self):
        with self.assertLogs(self.log, level="ERROR") as logs:
            ev, order, alert = self._process(alert_side_effect=KeyError("node gone"))
        self.assertEqual(ev.status, f"distr error {VUID}")
        self.assertEqual(order, ["write"])
        self.assertTrue(any("alert failed" in line for line in logs.output), logs.output)
        self.assertEqual(collector._distr_error_alerted, {}, "a failed alert is not debounced")

    def test_an_unknown_message_is_filed_without_an_alert(self):
        ev = _event(message="something_else", status="new")
        with patch.object(collector, "db"), \
             patch.object(EventObj, "write_to_db", autospec=True) as write, \
             patch.object(collector.storage_events, "snode_distrib_io_error") as alert:
            collector.process_event(ev, self.log)
        self.assertEqual(ev.status, "event_unknown")
        write.assert_called_once()
        alert.assert_not_called()


class TestTheCheckSurface(unittest.TestCase):

    def test_summary_aggregates_count_last_seen_errors_and_reporters(self):
        events = [
            _event(count=2, date=100, node_id="438a0878"),
            _event(count=1, date=300, node_id="d3a07483"),
            _event(message="error_write", count=1, date=200, node_id="d3a07483"),
            _event(vuid=3912, count=1, date=150),
        ]
        summary = health_controller.summarize_distr_errors(events)
        self.assertEqual(set(summary), {VUID, 3912})
        self.assertEqual(summary[VUID]["count"], 4)
        self.assertEqual(summary[VUID]["last_ms"], 300)
        self.assertEqual(summary[VUID]["errors"], {"error_read", "error_write"})
        self.assertEqual(summary[VUID]["reported_by"], {"438a0878", "d3a07483"})

    def test_events_that_are_not_distrib_errors_are_ignored(self):
        node_event = _event(vuid=-1, status="processed")
        device_event = _event(vuid=-1, status="processed")
        device_event.storage_id = 11
        self.assertEqual(health_controller.summarize_distr_errors([node_event, device_event]), {})

    def test_the_lvstore_vuids_come_from_the_raid_member_names(self):
        node = _nodes()[1]
        self.assertEqual(health_controller.lvstore_distrib_vuids(node), {7857, 5620, 4422, 4950, 4832})

    def test_errors_are_attributed_to_the_lvstore_that_holds_the_distrib(self):
        summary = health_controller.summarize_distr_errors([_event(), _event(vuid=11)])
        lvs_4033, lvs_5685 = _nodes()[1], _nodes()[0]
        self.assertEqual(set(health_controller.distr_errors_for_lvstore(lvs_4033, summary)), {VUID})
        self.assertEqual(set(health_controller.distr_errors_for_lvstore(lvs_5685, summary)), {11})

    def test_an_error_still_arriving_fails_the_check(self):
        last = 1793522115597
        summary = health_controller.summarize_distr_errors([_event(date=last)])
        with self.assertLogs(health_controller.logger, level="ERROR") as logs:
            ok = health_controller.report_lvstore_distr_errors(
                _nodes()[1], summary, now=last / 1000 + 60)
        self.assertFalse(ok)
        self.assertTrue(any("distrib 7857" in line and "LVS_4033" in line for line in logs.output),
                        logs.output)

    def test_an_old_error_is_a_warning_not_a_failed_check(self):
        """A double outage that is over must not fail every volume on the
        lvstore for the next week."""
        last = 1793522115597
        summary = health_controller.summarize_distr_errors([_event(date=last)])
        with self.assertLogs(health_controller.logger, level="WARNING") as logs:
            ok = health_controller.report_lvstore_distr_errors(
                _nodes()[1], summary, now=last / 1000 + ACTIVE + 1)
        self.assertTrue(ok)
        self.assertTrue(any("distrib 7857" in line and "warning" in line for line in logs.output),
                        logs.output)

    def test_an_lvstore_without_distrib_errors_passes(self):
        summary = health_controller.summarize_distr_errors([_event()])
        self.assertTrue(health_controller.report_lvstore_distr_errors(_nodes()[0], summary))


if __name__ == "__main__":
    unittest.main()
