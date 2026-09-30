"""Sync-replication status and gates against the real FoundationDB: the live
query of every online instance, the cluster-wide worst over all LVS with the
desync time taken from the recorded events and the running catch-up tasks,
the volume's role per site, the planned gate (live, never cached) and the
disaster gate (persisted events of the lost site's nodes only), and the
recording of a live "remote journal in sync" answer. Storage nodes (RPC) are
mocked; the database never is. The pure rules are in
tests/unit/test_sync_replication_status.py.
"""
from datetime import UTC, datetime, timedelta

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.exceptions import SyncGateError, SyncReplicationSiteError, SyncReplicationUnsupportedError
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.rpc_client import RPCException
from tests.integration import test_sync_replication_lvs_stack as stack

SITE_A, SITE_B = stack.SITE_A, stack.SITE_B
db = stack.db
rpcs = stack.rpcs

DROPPED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED
RESTORED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED


class _Layout:
    """a0 owns LVS_1 (home a0 a1 a2, distribs distrib_11 / distrib_12, jm_vuid
    1; remote triplet b0 b1 b2), b0 owns LVS_2 (home b0 b1 b2, distrib_21 /
    distrib_22, jm_vuid 2; remote triplet a1 a2 a3). Every instance answers
    ``synced`` in mode ``full``; the owner is HA and JC leader with the remote
    journal in sync. ``set`` overrides one element, ``down`` makes a node's
    query fail."""

    def __init__(self, db, rpcs, *, sync=True):
        self.db, self.rpcs = db, rpcs
        self.cluster = stack._seed_cluster(db)
        if not sync:
            self.cluster.sync_replication = False
            self.cluster.write_to_db(db.kv_store)
        self.a, self.b = stack._sites(db, self.cluster, per_site=4)
        a, b = self.a, self.b
        self.owner1 = stack._make_owner(db, a[0], a[:3], b[:3], lvs_id=1)
        self.owner2 = stack._make_owner(db, b[0], b[:3], a[1:], lvs_id=2)
        self.members = {"LVS_1": [n.get_id() for n in (*a[:3], *b[:3])],
                        "LVS_2": [n.get_id() for n in (*b[:3], *a[1:])]}
        self.leaders = {"LVS_1": a[0].get_id(), "LVS_2": b[0].get_id()}
        self.overrides: dict = {}
        self.down: set = set()
        for node in (*a, *b):
            rpcs[node.get_id()].distr_sync_replication_status.side_effect = self._answer_of(node.get_id())

    def _answer_of(self, node_id):
        def answer(name=None):
            if node_id in self.down:
                raise RPCException("connection refused")
            out = []
            for lvs, members in self.members.items():
                if node_id not in members:
                    continue
                lead = self.leaders[lvs] == node_id
                for d in (1, 2):
                    elem = {"name": f"distrib_{lvs[-1]}{d}", "vuid": int(lvs[-1]) * 10 + d,
                            "ha_leader": lead, "sync_replication_mode": "full", "status": "synced",
                            "n_primary_unsynced_pages": 0, "n_replica_unsynced_pages": 0,
                            "lag_seconds": 0 if lead else None, "jc_leader": lead,
                            "remote_journal_in_sync": True if lead else None}
                    elem.update(self.overrides.get((node_id, elem["name"]), {}))
                    out.append(elem)
            return out
        return answer

    def set(self, node, distrib, **fields):
        self.overrides.setdefault((node.get_id(), distrib), {}).update(fields)

    def calls(self, node):
        return self.rpcs[node.get_id()].distr_sync_replication_status.call_count

    def lvs(self, status, name):
        return next(s for s in status.lvs if s.lvs_name == name)


def _record(node, status, ts, **keys):
    return src.record_sync_event(node.get_id(), {
        "timestamp": ts, "event_type": "sync_replication_status", "status": status, **keys})


def _lvol(owner, site=""):
    lvol = LVol()
    lvol.uuid = "vol-1"
    lvol.node_id = owner.get_id()
    lvol.sync_active_site = site
    return lvol


def _gate_problems(fn, *args):
    with pytest.raises(SyncGateError) as exc:
        fn(*args)
    return exc.value.problems


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

class TestStatus:

    def test_all_synced_is_healthy_rpo_zero_now(self, db, rpcs):
        layout = _Layout(db, rpcs)
        before = datetime.now(UTC)
        status = src.cluster_sync_status(layout.cluster.get_id())
        assert status.state == src.STATE_HEALTHY
        assert (status.lag_seconds, status.bytes_behind) == (0, 0)
        assert before <= status.last_replicated_at <= datetime.now(UTC)
        assert status.completed and status.peer_ready and not status.diverged
        assert sorted(s.lvs_name for s in status.lvs) == ["LVS_1", "LVS_2"]
        # one query per online member, whatever number of LVS it holds; b3
        # holds no instance
        assert [layout.calls(n) for n in (*layout.a, *layout.b)] == [1] * 7 + [0]
        src.check_gate(layout.cluster.get_id())

    def test_one_lvs_unsynced_is_degraded_since_its_desync_event(self, db, rpcs):
        layout = _Layout(db, rpcs)
        a0 = layout.a[0]
        desync = (datetime.now(UTC) - timedelta(hours=1)).replace(microsecond=0)
        _record(a0, "secondary_zone_unavailable", desync.isoformat(), vuid=11)
        _record(layout.b[0], "secondary_zone_unavailable", (desync + timedelta(minutes=5)).isoformat(),
                vuid=11)
        layout.set(a0, "distrib_11", status="replica_unsynced", n_replica_unsynced_pages=3,
                   lag_seconds=5)
        status = src.cluster_sync_status(layout.cluster.get_id())
        assert status.state == src.STATE_DEGRADED and status.degraded and not status.resyncing
        assert status.last_replicated_at == desync
        assert status.lag_seconds == int((status.computed_at - status.last_replicated_at).total_seconds())
        assert status.bytes_behind == 3 * layout.cluster.page_size_in_blocks
        assert status.diverged and not status.peer_ready
        assert layout.lvs(status, "LVS_2").state == src.STATE_HEALTHY
        problems = _gate_problems(src.check_gate, layout.cluster.get_id())
        assert problems == [f"LVS LVS_1: distrib_11 on {a0.get_id()}: replica_unsynced"]

    def test_a_leader_syncing_is_resyncing(self, db, rpcs):
        layout = _Layout(db, rpcs)
        layout.set(layout.b[0], "distrib_21", status="primary_syncing", n_primary_unsynced_pages=1)
        layout.set(layout.b[1], "distrib_21", status="primary_unsynced")
        status = src.cluster_sync_status(layout.cluster.get_id())
        assert status.state == src.STATE_RESYNCING and status.resyncing and not status.degraded
        assert not status.peer_ready

    @pytest.mark.parametrize("task_status, state", [
        (JobSchedule.STATUS_RUNNING, src.STATE_RESYNCING),
        (JobSchedule.STATUS_SUSPENDED, src.STATE_DEGRADED),   # waits for the zone: nothing catches up
    ])
    def test_a_running_catch_up_task_is_resyncing(self, db, rpcs, task_status, state):
        layout = _Layout(db, rpcs)
        # idle LVS during the catch-up: no HA leader, the non-leaders report unsynced
        layout.leaders["LVS_1"] = ""
        layout.set(layout.a[1], "distrib_12", status="replica_unsynced")
        task_id = tasks_controller.add_sync_resync_task(
            layout.cluster.get_id(), layout.owner1.get_id(), "LVS_1")
        task = db.get_task_by_id(task_id)
        db.atomic_update(task, lambda t: setattr(t, "status", task_status))
        status = src.cluster_sync_status(layout.cluster.get_id())
        assert layout.lvs(status, "LVS_1").state == state
        assert status.state == state

    def test_unreachable_or_unknown_instances_are_degraded(self, db, rpcs):
        layout = _Layout(db, rpcs)
        layout.down |= {n.get_id() for n in (*layout.b[:3], *layout.a[1:])}
        status = src.cluster_sync_status(layout.cluster.get_id())
        lvs2 = layout.lvs(status, "LVS_2")
        assert lvs2.state == src.STATE_DEGRADED and lvs2.answering == ()
        assert set(lvs2.distribs.values()) == {src.STATUS_MISSING}
        assert status.state == src.STATE_DEGRADED and not status.completed
        assert "LVS LVS_2: no instance answered" in _gate_problems(src.check_gate, layout.cluster.get_id())

        layout.down.clear()
        layout.set(layout.b[2], "distrib_22", status="unknown")
        layout.leaders["LVS_2"] = ""
        assert src.cluster_sync_status(layout.cluster.get_id()).state == src.STATE_DEGRADED

    def test_offline_nodes_are_not_queried(self, db, rpcs):
        layout = _Layout(db, rpcs)
        stack._update(db, layout.b[1], status=StorageNode.STATUS_OFFLINE)
        status = src.cluster_sync_status(layout.cluster.get_id())
        assert layout.calls(layout.b[1]) == 0
        assert layout.b[1].get_id() not in layout.lvs(status, "LVS_1").answering
        assert status.state == src.STATE_HEALTHY
        # a member that is not ONLINE is not asked, so it does not fail the gate
        src.check_gate(layout.cluster.get_id())

    def test_role_per_site(self, db, rpcs):
        layout = _Layout(db, rpcs)
        assert src.volume_sync_status(_lvol(layout.owner1), SITE_A).role == src.ROLE_PRIMARY
        assert src.volume_sync_status(_lvol(layout.owner1), SITE_B).role == src.ROLE_SECONDARY
        # promoted on B: the volume's active site, not the LVS's home, decides
        moved = _lvol(layout.owner1, SITE_B)
        assert src.volume_sync_status(moved, SITE_B).role == src.ROLE_PRIMARY
        status = src.volume_sync_status(moved, SITE_A)
        assert (status.role, status.site, status.cluster.state) == (
            src.ROLE_SECONDARY, SITE_A, src.STATE_HEALTHY)

    @pytest.mark.parametrize("site", ["", "site-c"])
    def test_a_missing_or_unknown_site_is_refused(self, db, rpcs, site):
        layout = _Layout(db, rpcs)
        with pytest.raises(SyncReplicationSiteError):
            src.volume_sync_status(_lvol(layout.owner1), site)

    def test_a_cluster_without_sync_replication_is_refused(self, db, rpcs):
        layout = _Layout(db, rpcs, sync=False)
        cluster_id = layout.cluster.get_id()
        for call in (lambda: src.cluster_sync_status(cluster_id), lambda: src.check_gate(cluster_id),
                     lambda: src.check_disaster_gate(cluster_id, SITE_A),
                     lambda: src.volume_sync_status(_lvol(layout.owner1), SITE_A)):
            with pytest.raises(SyncReplicationUnsupportedError):
                call()

    def test_the_status_may_be_cached_the_gate_never(self, db, rpcs):
        layout = _Layout(db, rpcs)
        cluster_id = layout.cluster.get_id()
        assert src.cluster_sync_status(cluster_id, max_age=60).state == src.STATE_HEALTHY
        layout.set(layout.a[0], "distrib_12", status="primary_unsynced")
        assert src.cluster_sync_status(cluster_id, max_age=60).state == src.STATE_HEALTHY
        assert layout.calls(layout.a[0]) == 1
        _gate_problems(src.check_gate, cluster_id)
        # a live status query refreshes the cache
        assert src.cluster_sync_status(cluster_id).state == src.STATE_DEGRADED
        assert src.cluster_sync_status(cluster_id, max_age=60).state == src.STATE_DEGRADED


# ---------------------------------------------------------------------------
# gates
# ---------------------------------------------------------------------------

class TestGates:

    def test_a_silent_online_leader_fails_the_planned_gate(self, db, rpcs):
        # a0 (LVS_1's HA leader, which runs the catch-up) does not answer; the
        # others' "synced" is provisional
        layout = _Layout(db, rpcs)
        layout.down.add(layout.a[0].get_id())
        problems = _gate_problems(src.check_gate, layout.cluster.get_id())
        assert problems == [f"LVS LVS_1: {layout.a[0].get_id()} did not answer"]

    def test_a_running_catch_up_fails_the_planned_gate(self, db, rpcs):
        layout = _Layout(db, rpcs)
        cluster_id = layout.cluster.get_id()
        task_id = tasks_controller.add_sync_resync_task(
            cluster_id, layout.owner1.get_id(), "LVS_1")
        task = db.get_task_by_id(task_id)
        db.atomic_update(task, lambda t: setattr(t, "status", JobSchedule.STATUS_RUNNING))
        assert _gate_problems(src.check_gate, cluster_id) == ["LVS LVS_1: catch-up task running"]

    def test_an_unresolved_journal_drop_fails_only_the_disaster_gate(self, db, rpcs):
        layout = _Layout(db, rpcs)
        # a0 is the JC leader of LVS_1 (active on A) and dropped its remote journal
        layout.set(layout.a[0], "distrib_11", remote_journal_in_sync=False)
        layout.set(layout.a[0], "distrib_12", remote_journal_in_sync=False)
        _record(layout.a[0], "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        cluster_id = layout.cluster.get_id()
        src.check_gate(cluster_id)
        problems = _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)
        assert len(problems) == 1 and "LVS LVS_1: remote journal dropped" in problems[0]
        # LVS_1 is not active on B: losing B does not judge it
        src.check_disaster_gate(cluster_id, SITE_B)

    def test_the_surviving_sites_lvs_and_fence_generated_events_do_not_count(self, db, rpcs):
        layout = _Layout(db, rpcs)
        a, b = layout.a, layout.b
        stack._update(db, a[0], status=StorageNode.STATUS_OFFLINE)
        # after the loss of A: LVS_2 (homed on B) goes unsynced on B's nodes ...
        _record(b[0], "secondary_zone_unavailable", "2026-09-30T11:00:00Z", vuid=21)
        _record(b[0], "remote_journal_unsynced", "2026-09-30T11:00:00Z", jm_vuid=2)
        # ... and B's instances of LVS_1 report A's zone unavailable (the fence)
        _record(b[0], "primary_zone_unavailable", "2026-09-30T11:00:01Z", vuid=11)
        _record(b[1], "remote_journal_unsynced", "2026-09-30T11:00:01Z", jm_vuid=1)
        src.check_disaster_gate(layout.cluster.get_id(), SITE_A)

    def test_a_pre_loss_desync_of_a_lost_site_node_blocks(self, db, rpcs):
        layout = _Layout(db, rpcs)
        _record(layout.a[1], "secondary_zone_unavailable", "2026-09-30T10:00:00Z", vuid=12)
        problems = _gate_problems(src.check_disaster_gate, layout.cluster.get_id(), SITE_A)
        assert problems == [f"LVS LVS_1: zone desync secondary_zone_unavailable at 2026-09-30T10:00:00Z "
                            f"reported by {layout.a[1].get_id()}"]

    def test_the_latest_remote_journal_event_of_the_lost_site_wins(self, db, rpcs):
        layout = _Layout(db, rpcs)
        a0, cluster_id = layout.a[0], layout.cluster.get_id()
        _record(a0, "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        _record(a0, "remote_journal_synced", "2026-09-30T10:01:00Z", jm_vuid=1)
        src.check_disaster_gate(cluster_id, SITE_A)
        _record(a0, "remote_journal_unsynced", "2026-09-30T10:02:00Z", jm_vuid=1)
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)

    def test_a_surviving_site_synced_does_not_end_a_lost_site_drop(self, db, rpcs):
        layout = _Layout(db, rpcs)
        _record(layout.a[0], "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        # received later, but from B (e.g. a delayed event of an earlier B JC leadership)
        _record(layout.b[0], "remote_journal_synced", "2026-09-30T09:00:00Z", jm_vuid=1)
        assert db.get_unresolved_sync_replication_events(layout.cluster.get_id(), "LVS_1") == []
        _gate_problems(src.check_disaster_gate, layout.cluster.get_id(), SITE_A)

    def test_an_lvs_moving_is_judged_on_either_side(self, db, rpcs):
        layout = _Layout(db, rpcs)
        # LVS_2 is homed on B, its remote triplet a1 a2 a3 is on A
        _record(layout.a[1], "primary_zone_unavailable", "2026-09-30T10:00:00Z", vuid=21)
        src.check_disaster_gate(layout.cluster.get_id(), SITE_A)
        stack._update(db, layout.owner2, lvs_active_site="moving:" + SITE_A)
        _gate_problems(src.check_disaster_gate, layout.cluster.get_id(), SITE_A)
        stack._update(db, layout.owner2, lvs_active_site=SITE_A)
        _gate_problems(src.check_disaster_gate, layout.cluster.get_id(), SITE_A)


# ---------------------------------------------------------------------------
# a live "remote journal in sync" answer
# ---------------------------------------------------------------------------

def _journal_events(db, layout, lvs="LVS_1"):
    return sorted((e for e in db.get_sync_replication_events(layout.cluster.get_id(), lvs)
                   if e.kind in (DROPPED, RESTORED)), key=lambda e: e.receive_seq)


class TestLiveJournalRestore:

    def test_a_lost_site_jc_leader_in_sync_is_recorded_once_and_opens_the_gate(self, db, rpcs):
        layout = _Layout(db, rpcs)
        a0, cluster_id = layout.a[0], layout.cluster.get_id()
        _record(a0, "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)
        # the "synced" event got lost; the live answer says in sync
        src.cluster_sync_status(cluster_id)
        events = _journal_events(db, layout)
        assert [(e.kind, e.node_id) for e in events] == [(DROPPED, a0.get_id()), (RESTORED, a0.get_id())]
        assert datetime.now(UTC) - src.parse_event_time(events[1].timestamp_utc) < timedelta(minutes=1)
        src.check_disaster_gate(cluster_id, SITE_A)
        # nothing is open any more: polling records nothing
        src.cluster_sync_status(cluster_id)
        src.check_gate(cluster_id)
        assert len(_journal_events(db, layout)) == 2

    def test_another_lost_site_nodes_collected_synced_does_not_end_a_drop_a_live_answer_does(
            self, db, rpcs):
        # A JC handover inside A: a0 dropped, a1's "synced" is received later
        # (independent collectors - it may be older than a0's drop).
        layout = _Layout(db, rpcs)
        a0, a1, cluster_id = layout.a[0], layout.a[1], layout.cluster.get_id()
        _record(a0, "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        _record(a1, "remote_journal_synced", "2026-09-30T09:59:00Z", jm_vuid=1)
        # the status bookkeeping takes the drop as over (any later restore) ...
        assert db.get_unresolved_sync_replication_events(cluster_id, "LVS_1") == []
        drop = next(e for e in _journal_events(db, layout) if e.kind == DROPPED)
        assert drop.resolved
        assert not next(e for e in _journal_events(db, layout) if e.kind == RESTORED).observed_live
        # ... the disaster gate does not
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)
        # the new JC leader a1 answers in sync, live: recorded as observed_live
        layout.leaders["LVS_1"] = a1.get_id()
        src.cluster_sync_status(cluster_id)
        live = _journal_events(db, layout)[-1]
        assert (live.kind, live.node_id, live.observed_live) == (RESTORED, a1.get_id(), True)
        src.check_disaster_gate(cluster_id, SITE_A)
        # a later drop re-opens it
        _record(a1, "remote_journal_unsynced", "2026-09-30T10:10:00Z", jm_vuid=1)
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)

    def test_nothing_is_recorded_without_a_drop_or_from_an_unknown_journal_state(self, db, rpcs):
        layout = _Layout(db, rpcs)
        src.cluster_sync_status(layout.cluster.get_id())
        assert _journal_events(db, layout) == []
        _record(layout.a[0], "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        layout.set(layout.a[0], "distrib_12", remote_journal_in_sync=None)
        src.cluster_sync_status(layout.cluster.get_id())
        assert len(_journal_events(db, layout)) == 1

    def test_a_surviving_site_jc_leader_does_not_open_the_lost_sites_gate(self, db, rpcs):
        layout = _Layout(db, rpcs)
        cluster_id = layout.cluster.get_id()
        _record(layout.a[0], "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        layout.leaders["LVS_1"] = layout.b[0].get_id()     # the JC leadership moved to B
        src.cluster_sync_status(cluster_id)
        assert [e.node_id for e in _journal_events(db, layout)] == [layout.a[0].get_id(), layout.b[0].get_id()]
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)

    def test_a_drop_received_during_the_query_is_not_erased(self, db, rpcs):
        layout = _Layout(db, rpcs)
        a0, cluster_id = layout.a[0], layout.cluster.get_id()
        _record(a0, "remote_journal_unsynced", "2026-09-30T10:00:00Z", jm_vuid=1)
        answer = layout._answer_of(a0.get_id())

        def racing(name=None):
            # the answer is taken (in sync) ... and a newer drop is committed
            # by the collector before the restore is recorded
            out = answer(name)
            _record(a0, "remote_journal_unsynced", "2026-09-30T10:05:00Z", jm_vuid=1)
            return out

        rpcs[a0.get_id()].distr_sync_replication_status.side_effect = racing
        src.cluster_sync_status(cluster_id)
        assert [e.kind for e in _journal_events(db, layout)] == [DROPPED, DROPPED]
        _gate_problems(src.check_disaster_gate, cluster_id, SITE_A)
