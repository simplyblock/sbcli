"""test_distr_event_grouping.py — a batch must reach the cluster log intact.

The collector aggregates repeats of the same (storage_id, event_type, status)
within one batch onto a single event carrying a count. That grouping was built
with annotation statements — ``events_groups[sid][et]: {msg: 1}`` — which
evaluate and discard, creating nothing. The first time a node reported a second
event_type for one storage_id in a batch, the following assignment raised
KeyError; the enclosing except aborted the batch, so the remaining events were
neither logged nor discarded, and the next poll re-read them and failed the
same way. Events could stay out of the cluster event log indefinitely.
"""

import unittest
from unittest.mock import MagicMock, patch

from simplyblock_core.services import main_distr_event_collector as collector


def _event(storage_id, event_type, status):
    return {"storage_ID": storage_id, "event_type": event_type, "status": status}


class _Stop(Exception):
    """Breaks the collector poll loop, which is otherwise endless."""


class TestDistrEventGrouping(unittest.TestCase):

    def _run(self, batch):
        """Feed one batch to the collector; return the events it logged."""
        snode = MagicMock()
        snode.cluster_id = "cl-1"
        pending = [batch]

        def poll(discard, count):
            if discard:                      # the discard-then-get ack
                return True
            return pending.pop(0) if pending else []

        client = MagicMock()
        client.distr_status_events_discard_then_get.side_effect = poll
        snode.rpc_client.return_value = client

        logged = []

        def log_distr_event(cluster_id, node_id, event_dict):
            event = MagicMock()
            event.count = 1
            event.get_id.return_value = f"ev{len(logged)}"
            logged.append((event_dict, event))
            return event

        with patch.object(collector, "db") as db, \
                patch.object(collector.events_controller, "log_distr_event",
                             log_distr_event), \
                patch.object(collector, "process_event"), \
                patch.object(collector.time, "sleep", side_effect=_Stop()):
            db.get_storage_node_by_id.return_value = snode
            db.kv_store = MagicMock()
            collector.start_event_collector_on_node("node-1")
        return logged

    def test_two_event_types_for_one_storage_id_both_get_logged(self):
        """The KeyError case: same storage_ID, different event_type."""
        batch = [_event(3, "error_write", "failed"),
                 _event(3, "error_read", "failed")]
        logged = self._run(batch)
        self.assertEqual([d for d, _ in logged], batch,
                         "the second event_type was dropped")

    def test_repeats_are_aggregated_onto_one_event(self):
        batch = [_event(3, "error_write", "failed"),
                 _event(3, "error_write", "failed"),
                 _event(3, "error_write", "failed")]
        logged = self._run(batch)
        self.assertEqual(len(logged), 1, "a repeat was logged separately")
        self.assertEqual(logged[0][1].count, 3)

    def test_distinct_statuses_are_separate_events(self):
        batch = [_event(3, "error_write", "failed"),
                 _event(3, "error_write", "recovered")]
        logged = self._run(batch)
        self.assertEqual(len(logged), 2)

    def test_events_from_different_storage_ids_are_separate(self):
        batch = [_event(3, "error_write", "failed"),
                 _event(4, "error_write", "failed")]
        logged = self._run(batch)
        self.assertEqual(len(logged), 2)

    def test_vuid_keyed_events_are_accepted(self):
        """Events carry either storage_ID or vuid."""
        batch = [{"vuid": 9, "event_type": "error_write", "status": "failed"}]
        logged = self._run(batch)
        self.assertEqual(len(logged), 1)


if __name__ == "__main__":
    unittest.main()
