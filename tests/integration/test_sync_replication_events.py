"""Sync-replication events against the real FoundationDB: the distr event
collector records every ``sync_replication_status`` event of a batch in order
(no aggregation), maps it to its LVS by distrib vuid / jm_vuid on the emitting
node, gives it a per-LVS receive sequence in the write's own transaction,
lets the latest remote-journal event win, ignores a re-read of an event it
already recorded, and asks once per LVS for the zone catch-up. Storage nodes
(RPC) are mocked; the database never is. The pure rules are in
tests/unit/services/test_sync_replication_events.py.
"""
import threading
from unittest.mock import patch

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import main_distr_event_collector as collector
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs

EVENT_INDEX = 'cluster_id+lvs_name+resolved'
ZONE = SyncReplicationEvent.KIND_ZONE_UNAVAILABLE
DROPPED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED
RESTORED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED


def _layout(db, *, sync=True):
    """a0 owns LVS_1 (home a0 a1 a2, distribs vuid 11/12, jm_vuid 1; remote
    triplet b0 b1 b2), b0 owns LVS_2 (home b0 b1 b2, vuid 21/22, jm_vuid 2;
    remote triplet a1 a2 a3). So a0 hosts only LVS_1, b0 both."""
    cluster = stack._seed_cluster(db)
    if not sync:
        cluster.sync_replication = False
        cluster.write_to_db(db.kv_store)
    a, b = stack._sites(db, cluster, per_site=4)
    owner1 = stack._make_owner(db, a[0], a[:3], b[:3], lvs_id=1)
    owner2 = stack._make_owner(db, b[0], b[:3], a[1:], lvs_id=2)
    return cluster, a, b, owner1, owner2


_clock = iter(range(100000))


def _ts():
    """A distinct producer timestamp per call, like the data plane's."""
    return f"2026-09-30T10:00:00.{next(_clock):06d}Z"


def _zone(vuid, status="secondary_zone_unavailable", ts=None):
    return {"timestamp": ts or _ts(), "event_type": "sync_replication_status", "status": status,
            "vuid": vuid}


def _journal(jm_vuid, synced, ts=None):
    return {"timestamp": ts or _ts(), "event_type": "sync_replication_status",
            "status": "remote_journal_synced" if synced else "remote_journal_unsynced",
            "jm_vuid": jm_vuid}


class _Stop(Exception):
    """Ends the collector's otherwise endless poll loop (from its sleep)."""


def _collect(rpcs, node, batch, *, discard_fails=False):
    """Run the collector of ``node`` over one batch. With ``discard_fails`` the
    discard RPC raises, so the batch stays queued (re-read by the next run).
    Returns the discard counts the collector sent."""
    discards = []
    pending = [list(batch)]

    def poll(discard, count):
        if discard:
            if discard_fails:
                raise RPCException("connection lost at discard")
            discards.append(discard)
            pending.clear()
            return True
        return list(pending[0]) if pending else []

    rpcs[node.get_id()].distr_status_events_discard_then_get.side_effect = poll
    with patch.object(collector.time, "sleep", side_effect=_Stop()):
        collector.start_event_collector_on_node(node.get_id())
    return discards


def _events(db, cluster, lvs_name):
    return sorted(db.get_sync_replication_events(cluster.get_id(), lvs_name),
                  key=lambda e: e.receive_seq)


def _open(db, cluster, lvs_name):
    return sorted(db.get_unresolved_sync_replication_events(cluster.get_id(), lvs_name),
                  key=lambda e: e.receive_seq)


def _resync_tasks(db, cluster):
    return db.get_active_sync_resync_tasks(cluster.get_id())


# ---------------------------------------------------------------------------
# recording
# ---------------------------------------------------------------------------

class TestRecording:

    def test_each_status_is_stored_with_its_lvs_node_kind_timestamp_and_order(self, db, rpcs):
        cluster, a, _, owner1, _ = _layout(db)
        batch = [_zone(11, "primary_zone_unavailable"), _zone(12, "secondary_zone_unavailable"),
                 _journal(1, False), _journal(1, True)]
        assert _collect(rpcs, a[0], batch) == [4]

        events = _events(db, cluster, "LVS_1")
        assert [(e.status, e.kind, e.node_id, e.timestamp_utc) for e in events] == [
            (d["status"], kind, a[0].get_id(), d["timestamp"])
            for d, kind in zip(batch, (ZONE, ZONE, DROPPED, RESTORED))]
        assert [e.receive_seq for e in events] == [1, 2, 3, 4]
        # the synced resolved the drop before it; the zones wait for the catch-up
        assert [e.status for e in _open(db, cluster, "LVS_1")] == [
            "primary_zone_unavailable", "secondary_zone_unavailable"]
        assert all(e.resolved for e in events[2:])
        # the cluster event log still gets every one of them
        logged = [e for e in db.get_events() if e.event == "sync_replication_status"]
        assert len(logged) == 4
        assert all(e.status == "new" for e in logged)   # never marked event_unknown

    def test_an_event_of_a_remote_triplet_instance_maps_to_its_lvs(self, db, rpcs):
        cluster, _, b, owner1, owner2 = _layout(db)
        # b0 owns LVS_2 and is the remote primary of LVS_1
        _collect(rpcs, b[0], [_zone(11), _zone(21), _journal(1, False), _journal(2, False)])
        assert [(e.kind, e.node_id) for e in _events(db, cluster, "LVS_1")] == [
            (ZONE, b[0].get_id()), (DROPPED, b[0].get_id())]
        assert [(e.kind, e.node_id) for e in _events(db, cluster, "LVS_2")] == [
            (ZONE, b[0].get_id()), (DROPPED, b[0].get_id())]
        # each LVS has its own receive order
        assert [e.receive_seq for e in _events(db, cluster, "LVS_2")] == [1, 2]

    def test_a_string_jm_vuid_is_accepted(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db)
        event = _journal(1, False)
        event["jm_vuid"] = "1"
        _collect(rpcs, a[0], [event])
        assert [e.kind for e in _events(db, cluster, "LVS_1")] == [DROPPED]

    def test_unknown_subjects_and_statuses_are_skipped_and_the_batch_still_discarded(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db)
        batch = [_zone(99),                                        # no such distrib
                 _zone(21),                                        # LVS_2 has no instance on a0
                 _journal(9, False),                               # no such journal
                 {**_zone(11), "status": "zone_exploded"},         # unknown status
                 {"timestamp": _ts(), "event_type": "sync_replication_status",
                  "status": "remote_journal_unsynced"},            # no jm_vuid
                 _zone(11)]
        assert _collect(rpcs, a[0], batch) == [6]
        assert [(e.kind, e.node_id) for e in _events(db, cluster, "LVS_1")] == [(ZONE, a[0].get_id())]
        assert _events(db, cluster, "LVS_2") == []

    def test_a_cluster_without_sync_replication_records_nothing(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db, sync=False)
        assert _collect(rpcs, a[0], [_zone(11), _journal(1, False)]) == [2]
        assert _events(db, cluster, "LVS_1") == []
        assert _resync_tasks(db, cluster) == []

    def test_other_event_types_keep_their_aggregation(self, db, rpcs):
        _, a, _, _, _ = _layout(db)
        device = {"timestamp": _ts(), "event_type": "device_status", "storage_ID": 7,
                  "status": "error_write", "index": 0, "count": 1}
        batch = [dict(device), _journal(1, False), dict(device), _journal(1, True), dict(device)]
        with patch.object(collector, "process_event") as process_event, \
                patch.object(collector.events_controller, "log_distr_event",
                             wraps=collector.events_controller.log_distr_event) as log_distr_event:
            _collect(rpcs, a[0], batch)
        # the three device events are one aggregated event, the sync ones are not
        assert process_event.call_count == 1
        assert process_event.call_args.args[0].count == 3
        assert [c.args[2]["event_type"] for c in log_distr_event.call_args_list] == [
            "device_status", "sync_replication_status", "sync_replication_status"]


# ---------------------------------------------------------------------------
# the latest remote-journal event is the state
# ---------------------------------------------------------------------------

class TestJournalOrder:

    def test_a_dropped_synced_dropped_flap_in_one_batch_ends_dropped(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db)
        _collect(rpcs, a[0], [_journal(1, False), _journal(1, True), _journal(1, False)])
        events = _events(db, cluster, "LVS_1")
        assert [(e.kind, e.resolved) for e in events] == [
            (DROPPED, True), (RESTORED, True), (DROPPED, False)]
        assert [e.receive_seq for e in _open(db, cluster, "LVS_1")] == [3]

    def test_a_later_drop_reopens_and_a_later_synced_resolves_across_nodes(self, db, rpcs):
        cluster, a, b, _, _ = _layout(db)
        _collect(rpcs, a[0], [_journal(1, False)])      # old JC leader
        _collect(rpcs, b[1], [_journal(1, False)])      # new JC leader on the other site
        assert len(_open(db, cluster, "LVS_1")) == 2
        _collect(rpcs, b[1], [_journal(1, True)])
        assert _open(db, cluster, "LVS_1") == []
        _collect(rpcs, a[0], [_journal(1, False)])
        assert [e.node_id for e in _open(db, cluster, "LVS_1")] == [a[0].get_id()]

    def test_a_re_read_old_restored_does_not_resolve_a_later_drop(self, db, rpcs):
        cluster, a, b, _, _ = _layout(db)
        restored = _journal(1, True)
        # A records its synced, but the discard is lost: the batch stays queued
        _collect(rpcs, a[0], [restored], discard_fails=True)
        first = _events(db, cluster, "LVS_1")
        assert [(e.kind, e.receive_seq) for e in first] == [(RESTORED, 1)]
        # the new JC leader B reports the journal behind
        _collect(rpcs, b[1], [_journal(1, False)])
        # A's collector reads its queue again
        assert _collect(rpcs, a[0], [restored]) == [1]

        events = _events(db, cluster, "LVS_1")
        assert [(e.kind, e.node_id, e.receive_seq) for e in events] == [
            (RESTORED, a[0].get_id(), 1), (DROPPED, b[1].get_id(), 2)]
        assert [e.node_id for e in _open(db, cluster, "LVS_1")] == [b[1].get_id()]
        assert db.get_sync_state(cluster.get_id(), "LVS_1")["journal_restored_seq"] == 1

    def test_concurrent_writers_keep_commit_order(self, db, rpcs):
        cluster, a, b, _, _ = _layout(db)
        barrier = threading.Barrier(2)
        errors = []

        def writer(node, pattern):
            try:
                barrier.wait()
                for synced in pattern:
                    src.record_sync_event(node.get_id(), _journal(1, synced))
            except Exception as e:   # surfaced by the assertion below
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(a[0], [False, True] * 8)),
                   threading.Thread(target=writer, args=(b[1], [True, False] * 8))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []

        events = _events(db, cluster, "LVS_1")
        assert [e.receive_seq for e in events] == list(range(1, 33))
        # replaying the records in receive order gives exactly the stored verdicts
        last_restored = 0
        expected_open = []
        for event in events:
            if event.kind == RESTORED:
                last_restored = event.receive_seq
        for event in events:
            if event.kind == DROPPED and event.receive_seq > last_restored:
                expected_open.append(event.get_id())
        assert [e.get_id() for e in _open(db, cluster, "LVS_1")] == expected_open
        assert all(e.resolved == (e.get_id() not in expected_open) for e in events)


# ---------------------------------------------------------------------------
# bounded state, flags behind the watermarks
# ---------------------------------------------------------------------------

class TestBoundedState:

    @pytest.mark.parametrize("index_state", ["ready", "building", "disabled"])
    def test_a_long_outage_of_drops_is_resolved_by_one_synced(self, db, rpcs, index_state):
        db.set_index_state(SyncReplicationEvent, EVENT_INDEX, index_state)
        cluster, a, _, _, _ = _layout(db)
        key = db._sync_state_key(cluster.get_id(), "LVS_1")
        src.record_sync_event(a[0].get_id(), _journal(1, False))
        size = len(db.kv_store.get(key))
        for _ in range(149):
            src.record_sync_event(a[0].get_id(), _journal(1, False))
        # the state record does not grow with the outage (the seq digits aside)
        assert len(db.kv_store.get(key)) <= size + 2
        assert len(_open(db, cluster, "LVS_1")) == 150

        src.record_sync_event(a[0].get_id(), _journal(1, True))
        assert _open(db, cluster, "LVS_1") == []
        assert all(e.resolved for e in _events(db, cluster, "LVS_1"))

    def test_flags_left_behind_by_a_crash_read_as_resolved_and_are_cleaned_later(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db)
        for _ in range(3):
            src.record_sync_event(a[0].get_id(), _journal(1, False))
        with patch.object(db, "_resolve_covered_sync_events", side_effect=RuntimeError("crash")):
            with pytest.raises(RuntimeError):
                src.record_sync_event(a[0].get_id(), _journal(1, True))
        # the flags are still open, the watermark already says they are over
        assert sum(not e.resolved for e in _events(db, cluster, "LVS_1")) == 3
        assert _open(db, cluster, "LVS_1") == []
        src.record_sync_event(a[0].get_id(), _journal(1, True))
        assert all(e.resolved for e in _events(db, cluster, "LVS_1"))


# ---------------------------------------------------------------------------
# the zone catch-up is asked for once per LVS
# ---------------------------------------------------------------------------

class TestResyncScheduling:

    def test_every_instance_reporting_the_desync_schedules_one_task(self, db, rpcs):
        cluster, a, b, owner1, _ = _layout(db)
        _collect(rpcs, a[0], [_zone(11), _zone(12), _zone(11)])
        _collect(rpcs, a[1], [_zone(11), _zone(12)])
        _collect(rpcs, b[2], [_zone(11)])
        _collect(rpcs, a[0], [_journal(1, False)])        # a journal event asks for nothing
        tasks = _resync_tasks(db, cluster)
        assert len(tasks) == 1
        task = tasks[0]
        assert (task.function_name, task.node_id, task.function_params, task.max_retry,
                task.status) == (JobSchedule.FN_SYNC_RESYNC, owner1.get_id(),
                                 {"lvs_name": "LVS_1"}, -1, JobSchedule.STATUS_NEW)
        assert len(_open(db, cluster, "LVS_1")) == 7

    def test_two_lvs_get_a_task_each(self, db, rpcs):
        cluster, _, b, owner1, owner2 = _layout(db)
        _collect(rpcs, b[0], [_zone(11), _zone(21)])
        assert sorted((t.node_id, t.function_params["lvs_name"]) for t in _resync_tasks(db, cluster)) == \
            sorted([(owner1.get_id(), "LVS_1"), (owner2.get_id(), "LVS_2")])

    def test_concurrent_requests_create_one_task(self, db, rpcs):
        cluster, a, _, owner1, _ = _layout(db)
        barrier = threading.Barrier(4)
        results = []

        def ask():
            barrier.wait()
            results.append(tasks_controller.add_sync_resync_task(
                cluster.get_id(), owner1.get_id(), "LVS_1"))

        threads = [threading.Thread(target=ask) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len([r for r in results if r]) == 1
        assert [t.uuid for t in _resync_tasks(db, cluster)] == [r for r in results if r]

    def test_a_done_or_canceled_task_is_replaced(self, db, rpcs):
        cluster, a, _, owner1, _ = _layout(db)
        src.record_sync_event(a[0].get_id(), _zone(11))
        first = _resync_tasks(db, cluster)[0]
        db.atomic_update(first, lambda t: setattr(t, "canceled", True))
        src.record_sync_event(a[0].get_id(), _zone(12))
        second = [t for t in _resync_tasks(db, cluster) if not t.canceled]
        assert len(second) == 1 and second[0].uuid != first.uuid
        db.atomic_update(second[0], lambda t: setattr(t, "status", JobSchedule.STATUS_DONE))
        src.record_sync_event(a[0].get_id(), _zone(11))
        assert len([t for t in _resync_tasks(db, cluster) if not t.canceled]) == 1

    def test_a_re_read_desync_the_catch_up_covered_asks_for_nothing(self, db, rpcs):
        cluster, a, _, _, _ = _layout(db)
        event = _zone(11)
        _collect(rpcs, a[0], [event], discard_fails=True)
        task = _resync_tasks(db, cluster)[0]
        assert db.finish_sync_resync(task, "LVS_1", db.get_sync_state(cluster.get_id(), "LVS_1")["seq"],
                                     "synced", 1.0)
        _collect(rpcs, a[0], [event])
        assert _resync_tasks(db, cluster) == []
        assert len(_events(db, cluster, "LVS_1")) == 1

    def test_a_re_read_desync_whose_first_pass_stopped_before_the_task_gets_one(self, db, rpcs):
        cluster, a, _, owner1, _ = _layout(db)
        event = _zone(11)
        with patch.object(src.tasks_controller, "add_sync_resync_task",
                          side_effect=RuntimeError("crash after the record")):
            _collect(rpcs, a[0], [event])      # the error aborts the batch undiscarded
        assert len(_events(db, cluster, "LVS_1")) == 1
        assert _resync_tasks(db, cluster) == []
        _collect(rpcs, a[0], [event])
        assert [t.node_id for t in _resync_tasks(db, cluster)] == [owner1.get_id()]
        assert len(_events(db, cluster, "LVS_1")) == 1

