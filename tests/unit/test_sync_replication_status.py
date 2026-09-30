"""Pure rules of the sync-replication status and gates: how the live answers
of an LVS's instances make its status, the cluster-wide worst, the last
in-sync time, the volume's role per site, the planned gate's verdict per LVS
and the disaster gate's judgement of the lost site's persisted events. The
flows (live query, events and tasks in the database, the gates end to end) run
against the real database in tests/integration/test_sync_replication_status.py.
"""
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent

ZONE = SyncReplicationEvent.KIND_ZONE_UNAVAILABLE
DROPPED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED
RESTORED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED

PAGE = 2 * 1024 * 1024
LAYOUT = src.LvsLayout("LVS_1", "n1", {"d1": PAGE, "d2": 4096}, ("n1", "n2", "n3"))
NOW = datetime(2026, 9, 30, 12, 0, 0, tzinfo=UTC)


def _e(name, status="synced", *, leader=False, mode="full", primary=0, replica=0, lag=None,
       jc=False, journal=None):
    elem = {"name": name, "vuid": 1, "ha_leader": leader, "sync_replication_mode": mode,
            "status": status, "n_primary_unsynced_pages": primary,
            "n_replica_unsynced_pages": replica, "lag_seconds": lag, "jc_leader": jc,
            "remote_journal_in_sync": journal}
    if status == "unknown":
        del elem["n_primary_unsynced_pages"], elem["n_replica_unsynced_pages"]
    return elem


def _synced(leader_node="n1", journal=True):
    """Every instance answers synced for both distribs; ``leader_node`` is HA
    and JC leader."""
    return {nid: [_e(d, leader=nid == leader_node, jc=nid == leader_node,
                     journal=journal if nid == leader_node else None, lag=0 if nid == leader_node else None)
                  for d in LAYOUT.distribs]
            for nid in LAYOUT.members}


def _set(answers, nid, name, **fields):
    for elem in answers[nid]:
        if elem["name"] == name:
            elem.update(fields)
    return answers


class TestStatusRank:

    def test_order(self):
        ranks = [src.status_rank(s) for s in ("synced", "replica_syncing", "primary_unsynced",
                                              "unknown", "missing", "")]
        assert ranks == [0, 1, 2, 3, 3, 3]


class TestAggregateLvs:

    def test_all_synced_is_healthy_and_passes_the_gate(self):
        status = src.aggregate_lvs(LAYOUT, _synced(), resync_running=False)
        assert status.state == src.STATE_HEALTHY
        assert status.distribs == {"d1": "synced", "d2": "synced"}
        assert status.mode_full and status.gate_problems == ()
        assert status.answering == ("n1", "n2", "n3")
        assert (status.unsynced_pages, status.bytes_behind) == (0, 0)

    def test_the_leader_answer_wins_over_a_non_leader_during_a_catch_up(self):
        answers = _set(_synced(), "n1", "d1", status="replica_syncing", n_replica_unsynced_pages=3)
        _set(answers, "n2", "d1", status="replica_unsynced", n_replica_unsynced_pages=9)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert status.distribs["d1"] == "replica_syncing"
        assert status.state == src.STATE_RESYNCING
        assert (status.unsynced_pages, status.bytes_behind) == (3, 3 * PAGE)
        # the gate judges every answering instance
        assert len(status.gate_problems) == 2

    def test_without_a_leader_the_worst_answer_counts(self):
        answers = _set(_synced(leader_node=""), "n3", "d2", status="primary_unsynced", n_primary_unsynced_pages=2)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert status.distribs == {"d1": "synced", "d2": "primary_unsynced"}
        assert status.state == src.STATE_DEGRADED
        assert status.bytes_behind == 2 * 4096

    def test_an_idle_lvs_without_a_leader_all_synced_passes_the_gate(self):
        status = src.aggregate_lvs(LAYOUT, _synced(leader_node=""), resync_running=False)
        assert status.state == src.STATE_HEALTHY
        assert status.gate_problems == ()
        assert status.remote_journal_in_sync is None

    def test_unknown_is_degraded_even_while_a_catch_up_runs(self):
        answers = _set(_synced(), "n1", "d2", status="unknown")
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=True)
        assert status.state == src.STATE_DEGRADED
        assert status.gate_problems == ("LVS LVS_1: d2 on n1: unknown",)

    @pytest.mark.parametrize("leader_status", ["replica_unsynced", "synced"])
    def test_a_running_catch_up_is_resyncing(self, leader_status):
        answers = _set(_synced(), "n1", "d1", status=leader_status)
        assert src.aggregate_lvs(LAYOUT, answers, resync_running=True).state == src.STATE_RESYNCING
        assert src.aggregate_lvs(LAYOUT, answers, resync_running=False).state == (
            src.STATE_DEGRADED if leader_status != "synced" else src.STATE_HEALTHY)

    def test_no_answer_at_all_is_missing_degraded_and_fails_the_gate(self):
        status = src.aggregate_lvs(LAYOUT, {"n1": None}, resync_running=False)
        assert status.distribs == {"d1": "missing", "d2": "missing"}
        assert status.state == src.STATE_DEGRADED
        assert not status.mode_full
        assert status.gate_problems == ("LVS LVS_1: no instance answered",)

    def test_a_node_answering_without_a_distrib_fails_the_gate_but_not_the_leader_status(self):
        answers = _synced()
        answers["n3"] = [e for e in answers["n3"] if e["name"] != "d2"]
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert status.state == src.STATE_HEALTHY
        assert status.gate_problems == ("LVS LVS_1: d2 on n3: no status",)
        assert not status.mode_full

    @pytest.mark.parametrize("mode", ["disabled", "offset_based"])
    def test_one_instance_not_full_is_not_completed_although_the_leader_is(self, mode):
        answers = _set(_synced(), "n2", "d1", sync_replication_mode=mode)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert status.distribs == {"d1": "synced", "d2": "synced"}      # the leader's answers
        assert status.state == src.STATE_HEALTHY and not status.mode_full
        assert status.gate_problems == (f"LVS LVS_1: d1 on n2: mode {mode}",)
        cluster = src.aggregate_cluster([replace(status, last_replicated_at=NOW)], NOW)
        assert cluster.state == src.STATE_HEALTHY
        assert not cluster.completed and not cluster.peer_ready

    def test_a_node_not_in_the_layout_is_ignored(self):
        answers = {**_synced(), "n9": [_e("d1", "primary_unsynced", leader=True)]}
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert status.state == src.STATE_HEALTHY and "n9" not in status.answering

    @pytest.mark.parametrize("mode", ["offset_based", "disabled"])
    def test_a_mode_other_than_full_fails_the_gate_and_completed(self, mode):
        answers = _synced()
        for nid in LAYOUT.members:
            _set(answers, nid, "d1", sync_replication_mode=mode)
        if mode == "disabled":
            for nid in LAYOUT.members:
                _set(answers, nid, "d1", status=None)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert not status.mode_full
        assert len(status.gate_problems) == 3
        assert all(f"mode {mode}" in p for p in status.gate_problems)

    def test_journal_in_sync_is_the_and_of_the_jc_leader_answers(self):
        status = src.aggregate_lvs(LAYOUT, _synced(), resync_running=False)
        assert (status.remote_journal_in_sync, status.jc_leader_id) == (True, "n1")
        answers = _set(_synced(), "n1", "d2", remote_journal_in_sync=False)
        assert src.aggregate_lvs(LAYOUT, answers, resync_running=False).remote_journal_in_sync is False
        answers = _set(_synced(), "n1", "d2", remote_journal_in_sync=None)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert (status.remote_journal_in_sync, status.jc_leader_id) == (None, "")

    def test_two_jc_leader_claims_across_a_handoff_are_not_known(self):
        answers = _set(_synced(), "n2", "d1", jc_leader=True, remote_journal_in_sync=False)
        status = src.aggregate_lvs(LAYOUT, answers, resync_running=False)
        assert (status.remote_journal_in_sync, status.jc_leader_id) == (None, "")

    def test_the_lag_is_the_largest_leader_lag(self):
        answers = _set(_synced(), "n1", "d1", status="replica_unsynced", lag_seconds=40)
        _set(answers, "n1", "d2", status="replica_unsynced", lag_seconds=70)
        _set(answers, "n2", "d2", lag_seconds=999)       # non-leader: never trusted
        assert src.aggregate_lvs(LAYOUT, answers, resync_running=False).lag_seconds == 70


def _event(kind, node_id, seq, *, status="", ts="2026-09-30T10:00:00Z", resolved=False):
    event = SyncReplicationEvent()
    event.kind = kind
    event.node_id = node_id
    event.receive_seq = seq
    event.status = status or {ZONE: "secondary_zone_unavailable", DROPPED: "remote_journal_unsynced",
                              RESTORED: "remote_journal_synced"}[kind]
    event.timestamp_utc = ts
    event.resolved = resolved or kind == RESTORED
    return event


class TestLastReplicatedAt:

    def _status(self, state, lag=None):
        status = src.aggregate_lvs(LAYOUT, _synced(), resync_running=False)
        return replace(status, state=state, lag_seconds=lag)

    def test_healthy_is_now(self):
        assert src.lvs_last_replicated_at(self._status(src.STATE_HEALTHY), [], NOW) == NOW

    def test_the_earliest_open_zone_event_wins_over_the_lag(self):
        events = [_event(ZONE, "n1", 1, ts="2026-09-30T11:00:00Z"),
                  _event(ZONE, "n2", 2, ts="2026-09-30T10:30:00.500000Z"),
                  _event(DROPPED, "n1", 3, ts="2026-09-30T09:00:00Z"),     # not a zone desync
                  _event(ZONE, "n3", 4, ts="garbage")]
        last = src.lvs_last_replicated_at(self._status(src.STATE_DEGRADED, lag=7200), events, NOW)
        assert last == datetime(2026, 9, 30, 10, 30, 0, 500000, tzinfo=UTC)

    def test_without_an_event_the_leader_lag(self):
        last = src.lvs_last_replicated_at(self._status(src.STATE_RESYNCING, lag=90), [], NOW)
        assert last == NOW - timedelta(seconds=90)

    def test_neither_is_not_known(self):
        assert src.lvs_last_replicated_at(self._status(src.STATE_DEGRADED), [], NOW) is None


def _lvs(state, *, last=None, mode_full=True, bytes_behind=0, name="LVS_1"):
    status = src.aggregate_lvs(LAYOUT, _synced(), resync_running=False)
    return replace(status, lvs_name=name, state=state, last_replicated_at=last, mode_full=mode_full,
                   bytes_behind=bytes_behind)


class TestAggregateCluster:

    def test_all_healthy_is_rpo_zero_now(self):
        status = src.aggregate_cluster([_lvs(src.STATE_HEALTHY, last=NOW)] * 2, NOW)
        assert (status.state, status.last_replicated_at, status.lag_seconds) == (src.STATE_HEALTHY, NOW, 0)
        assert status.completed and status.peer_ready and not status.diverged
        assert not status.degraded and not status.resyncing

    def test_the_worst_state_wins_and_both_flags_are_set(self):
        early = NOW - timedelta(minutes=10)
        status = src.aggregate_cluster([
            _lvs(src.STATE_RESYNCING, last=NOW - timedelta(minutes=2), bytes_behind=5),
            _lvs(src.STATE_DEGRADED, last=early, bytes_behind=7),
            _lvs(src.STATE_HEALTHY, last=NOW)], NOW)
        assert status.state == src.STATE_DEGRADED
        assert status.degraded and status.resyncing and status.diverged and not status.peer_ready
        assert (status.last_replicated_at, status.lag_seconds, status.bytes_behind) == (early, 600, 12)

    def test_resyncing_alone(self):
        status = src.aggregate_cluster([_lvs(src.STATE_RESYNCING), _lvs(src.STATE_HEALTHY, last=NOW)], NOW)
        assert status.state == src.STATE_RESYNCING and not status.degraded and not status.peer_ready
        assert (status.last_replicated_at, status.lag_seconds) == (None, None)

    def test_not_completed_is_not_peer_ready(self):
        status = src.aggregate_cluster([_lvs(src.STATE_HEALTHY, last=NOW, mode_full=False)], NOW)
        assert status.state == src.STATE_HEALTHY
        assert not status.completed and not status.peer_ready


def _node(site, *, lvs_active_site=""):
    node = StorageNode()
    node.uuid = f"owner-{site}"
    node.site = site
    node.lvs_active_site = lvs_active_site
    return node


class TestRoleAndActiveSite:

    def test_role_per_site(self):
        owner = _node("A")
        lvol = LVol()
        assert src.volume_role(lvol, owner, "A") == src.ROLE_PRIMARY     # legacy: home site
        assert src.volume_role(lvol, owner, "B") == src.ROLE_SECONDARY
        lvol.sync_active_site = "B"
        assert src.volume_role(lvol, owner, "B") == src.ROLE_PRIMARY
        assert src.volume_role(lvol, owner, "A") == src.ROLE_SECONDARY

    @pytest.mark.parametrize("roles, expected", [
        ([], src.ROLE_SECONDARY),
        ([src.ROLE_PRIMARY], src.ROLE_PRIMARY),
        ([src.ROLE_PRIMARY, src.ROLE_PRIMARY], src.ROLE_PRIMARY),
        ([src.ROLE_SECONDARY, src.ROLE_SECONDARY], src.ROLE_SECONDARY),
        ([src.ROLE_PRIMARY, src.ROLE_SECONDARY], src.ROLE_SECONDARY),
    ])
    def test_group_role(self, roles, expected):
        assert src.group_role(iter(roles)) == expected

    @pytest.mark.parametrize("home, active, lost, expected", [
        ("A", "", "A", True), ("A", "A", "A", True), ("B", "A", "A", True),
        ("B", "", "A", False), ("A", "B", "A", False),
        ("A", "moving:B", "A", True), ("B", "moving:B", "A", True),
    ])
    def test_lvs_active_on(self, home, active, lost, expected):
        assert src.lvs_active_on(_node(home, lvs_active_site=active), lost) is expected

    def test_layout_skips_pending_remote_members_and_takes_the_page_size(self):
        owner = _node("A")
        owner.lvstore = "LVS_1"
        owner.secondary_node_id, owner.tertiary_node_id = "s", "t"
        owner.remote_primary_node_id, owner.remote_secondary_node_id, owner.remote_tertiary_node_id = "rp", "rs", "rt"
        owner.remote_instances_pending = ["rs"]
        owner.lvstore_stack = [
            {"type": "bdev_distr", "name": "d1", "params": {"pba_page_size": 4096}},
            {"type": "bdev_distr", "name": "d2", "params": {}},
            {"type": "bdev_raid", "name": "raid"}]
        layout = src.lvs_layout(owner, PAGE)
        assert layout.distribs == {"d1": 4096, "d2": PAGE}
        assert layout.members == ("owner-A", "s", "t", "rp", "rt")


SITES = {"t1": "T", "t2": "T", "s1": "S"}
STATE = {"seq": 99, "last_zone_seq": 0, "journal_restored_seq": 0, "zone_synced_seq": 0, "resync_task": ""}


def _problems(events, state=STATE):
    return src.disaster_gate_problems("LVS_1", events, state, "T", SITES)


class TestDisasterGate:

    def test_nothing_recorded_passes(self):
        assert _problems([]) == []

    def test_a_lost_site_drop_blocks(self):
        assert len(_problems([_event(DROPPED, "t1", 1)])) == 1

    @pytest.mark.parametrize("sequence, blocks", [
        ([(DROPPED, "t1"), (RESTORED, "t2")], False),     # a later synced of the lost site ends it
        ([(RESTORED, "t1"), (DROPPED, "t1")], True),      # a later unsynced re-opens it
        ([(DROPPED, "t1"), (RESTORED, "t1"), (DROPPED, "t2")], True),
        ([(DROPPED, "t1"), (RESTORED, "s1")], True),      # a surviving-site synced never ends it
        ([(DROPPED, "t1"), (RESTORED, "gone")], True),    # nor one of an unknown node
        ([(DROPPED, "gone")], True),                      # an unknown node's drop counts
        ([(DROPPED, "s1")], False),                       # a surviving-site drop never counts
        ([(RESTORED, "t1"), (DROPPED, "s1")], False),
    ])
    def test_the_latest_journal_event_of_the_lost_site_wins(self, sequence, blocks):
        events = [_event(kind, node, seq) for seq, (kind, node) in enumerate(sequence, 1)]
        assert bool(_problems(events)) is blocks

    def test_a_legacy_drop_without_receive_seq_is_judged_by_its_flag(self):
        assert _problems([_event(DROPPED, "t1", 0), _event(RESTORED, "t1", 5)])
        assert not _problems([_event(DROPPED, "t1", 0, resolved=True)])

    def test_zone_desyncs_of_the_lost_site_block_until_covered(self):
        events = [_event(ZONE, "t1", 3), _event(ZONE, "s1", 4), _event(ZONE, "gone", 5)]
        assert [p.split(" reported by ")[1] for p in _problems(events)] == ["t1", "gone"]
        covered = {**STATE, "zone_synced_seq": 5}
        assert _problems(events, covered) == []
        assert _problems([_event(ZONE, "t1", 3, resolved=True)]) == []

    def test_latest_journal_drop_with_one_site(self):
        def on_s(node_id):
            return SITES.get(node_id) == "S"

        events = [_event(DROPPED, "t1", 1), _event(DROPPED, "s1", 2), _event(RESTORED, "s1", 3)]
        assert src.latest_journal_drop(events, on_s, on_s) is None
        events.append(_event(DROPPED, "s1", 4))
        assert src.latest_journal_drop(events, on_s, on_s).receive_seq == 4
