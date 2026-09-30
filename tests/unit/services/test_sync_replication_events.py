"""Pure rules of the sync-replication event recording: which events are
sync-replication state changes and what they are about, which LVS on the
emitting node an event belongs to, which site a zone desync leaves behind, and
the stable identity of a producer event. The recording flow (collector,
receive order, resync scheduling) runs against the real database in
tests/integration/test_sync_replication_events.py.
"""
import pytest

from simplyblock_core.controllers import sync_replication_controller as src
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent

ZONE = SyncReplicationEvent.KIND_ZONE_UNAVAILABLE
DROPPED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED
RESTORED = SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED


def _event(status, **keys):
    return {"timestamp": "2026-09-30T10:00:00.000001Z", "event_type": "sync_replication_status",
            "status": status, **keys}


class TestClassify:

    @pytest.mark.parametrize("status, keys, expected", [
        ("primary_zone_unavailable", {"vuid": 5}, (ZONE, 5)),
        ("secondary_zone_unavailable", {"vuid": 5}, (ZONE, 5)),
        ("remote_journal_unsynced", {"jm_vuid": 1}, (DROPPED, 1)),
        ("remote_journal_synced", {"jm_vuid": "1"}, (RESTORED, 1)),
    ])
    def test_the_four_statuses(self, status, keys, expected):
        assert tuple(src.classify_sync_event(_event(status, **keys))) == expected

    @pytest.mark.parametrize("event", [
        _event("zone_exploded", vuid=5),                       # unknown status
        _event("primary_zone_unavailable", jm_vuid=5),          # zone event keyed by the journal
        _event("remote_journal_unsynced", vuid=1),              # journal event keyed by a distrib
        _event("remote_journal_synced", jm_vuid="x"),           # not an integer
        _event("primary_zone_unavailable", vuid=None),
        {**_event("primary_zone_unavailable", vuid=5), "event_type": "device_status"},
    ])
    def test_anything_else_is_not_a_sync_event(self, event):
        assert src.classify_sync_event(event) is None


def _node(uid, site, **fields):
    node = StorageNode()
    node.uuid = uid
    node.site = site
    for key, value in fields.items():
        setattr(node, key, value)
    return node


def _stack(*vuids):
    return [{"type": "bdev_distr", "name": f"distrib_{v}", "params": {"name": f"distrib_{v}", "vuid": v}}
            for v in vuids] + [{"type": "bdev_raid", "name": "raid0", "params": {"vuid": 99}}]


class TestLvsOwnerOfEvent:

    def setup_method(self):
        # o1 owns LVS_1 (secondary n1, remote primary r1); o2 owns LVS_2 with
        # the same node n1 as its tertiary.
        self.n1 = _node("n1", "A", lvstore_stack_secondary="o1", lvstore_stack_tertiary="o2")
        self.r1 = _node("r1", "B")
        self.o1 = _node("o1", "A", lvstore="LVS_1", jm_vuid=1, lvstore_stack=_stack(11, 12),
                        remote_primary_node_id="r1")
        self.o2 = _node("o2", "A", lvstore="LVS_2", jm_vuid=2, lvstore_stack=_stack(21))
        self.nodes = [self.n1, self.r1, self.o1, self.o2]

    def _owner(self, node, kind, vuid):
        owner = src.lvs_owner_of_event(node, self.nodes, src.SyncEventSubject(kind, vuid))
        return owner.get_id() if owner else None

    def test_a_distrib_vuid_names_the_lvs_holding_it(self):
        assert self._owner(self.n1, ZONE, 12) == "o1"
        assert self._owner(self.n1, ZONE, 21) == "o2"
        assert self._owner(self.r1, ZONE, 11) == "o1"      # remote-triplet instance

    def test_a_jm_vuid_names_the_lvs_of_that_journal(self):
        assert self._owner(self.n1, DROPPED, 2) == "o2"
        assert self._owner(self.r1, RESTORED, 1) == "o1"

    def test_only_lvs_with_an_instance_on_the_node_count(self):
        assert self._owner(self.r1, ZONE, 21) is None      # r1 has no LVS_2 instance
        assert self._owner(self.r1, DROPPED, 2) is None
        assert self._owner(self.n1, ZONE, 99) is None      # a raid vuid is not a distrib


def _zone_event(status, kind=ZONE):
    event = SyncReplicationEvent()
    event.kind = kind
    event.status = status
    return event


class TestLaggingSites:

    def test_the_primary_zone_is_the_home_site_and_the_replica_the_other(self):
        assert src.lagging_sites("A", "B", [_zone_event("primary_zone_unavailable")]) == ["A"]
        assert src.lagging_sites("A", "B", [_zone_event("secondary_zone_unavailable")]) == ["B"]

    def test_both_zones_in_first_seen_order_without_repeats(self):
        events = [_zone_event("secondary_zone_unavailable"), _zone_event("primary_zone_unavailable"),
                  _zone_event("secondary_zone_unavailable")]
        assert src.lagging_sites("A", "B", events) == ["B", "A"]

    def test_journal_events_name_no_zone(self):
        assert src.lagging_sites("A", "B", [_zone_event("remote_journal_unsynced", DROPPED)]) == []


class TestEventId:

    def test_a_re_read_event_has_the_same_id(self):
        event = _event("remote_journal_synced", jm_vuid=1)
        assert src.sync_event_id("c", "n", event) == src.sync_event_id("c", "n", dict(event))

    @pytest.mark.parametrize("change", [
        {"timestamp": "2026-09-30T10:00:00.000002Z"},
        {"status": "remote_journal_unsynced"},
        {"jm_vuid": 2},
    ])
    def test_another_transition_has_another_id(self, change):
        event = _event("remote_journal_synced", jm_vuid=1)
        assert src.sync_event_id("c", "n", event) != src.sync_event_id("c", "n", {**event, **change})

    def test_the_emitting_node_and_cluster_are_part_of_it(self):
        event = _event("primary_zone_unavailable", vuid=5)
        ids = {src.sync_event_id(c, n, event) for c, n in (("c", "n"), ("c", "m"), ("d", "n"))}
        assert len(ids) == 3
