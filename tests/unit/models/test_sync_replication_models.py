"""Sync-replication model fields: defaults, old records, round trip."""
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent


def test_new_fields_default_to_non_sync():
    cluster = Cluster()
    assert cluster.sync_replication is False
    assert cluster.lost_site == ""
    assert cluster.lost_site_state == ""

    node = StorageNode()
    assert node.site == ""
    assert node.remote_primary_node_id == ""
    assert node.remote_secondary_node_id == ""
    assert node.remote_tertiary_node_id == ""
    assert node.remote_instances_pending == []
    assert node.lvs_active_site == ""

    lvol = LVol()
    assert lvol.sync_active_site == ""
    assert lvol.sync_demoted_sites == []


def test_old_records_without_the_keys_load_with_defaults():
    """A record written by a release without these fields has none of the keys."""
    cluster = Cluster().from_dict({"uuid": "c1", "ha_type": "ha"})
    assert cluster.sync_replication is False
    assert cluster.lost_site == ""
    assert cluster.lost_site_state == ""

    node = StorageNode().from_dict({"uuid": "n1", "cluster_id": "c1", "failure_domain": 2})
    assert node.site == ""
    assert node.remote_primary_node_id == ""
    assert node.lvs_active_site == ""
    assert node.remote_instances_pending == []
    assert node.failure_domain == 2

    lvol = LVol().from_dict({"uuid": "l1", "lvol_name": "vol"})
    assert lvol.sync_active_site == ""
    assert lvol.sync_demoted_sites == []


def test_round_trip_keeps_the_values():
    cluster = Cluster()
    cluster.sync_replication = True
    cluster.lost_site = "site-b"
    cluster.lost_site_state = Cluster.LOST_SITE_FENCING
    restored = Cluster().from_dict(cluster.to_dict())
    assert restored.sync_replication is True
    assert restored.lost_site == "site-b"
    assert restored.lost_site_state == "fencing"

    node = StorageNode()
    node.site = "site-a"
    node.remote_primary_node_id = "rp"
    node.remote_secondary_node_id = "rs"
    node.remote_tertiary_node_id = "rt"
    node.lvs_active_site = "site-b"
    node.remote_instances_pending = ["rs"]
    restored_node = StorageNode().from_dict(node.to_dict())
    assert (restored_node.site, restored_node.remote_primary_node_id,
            restored_node.remote_secondary_node_id, restored_node.remote_tertiary_node_id,
            restored_node.lvs_active_site) == ("site-a", "rp", "rs", "rt", "site-b")
    assert restored_node.remote_instances_pending == ["rs"]

    lvol = LVol()
    lvol.sync_active_site = "site-a"
    lvol.sync_demoted_sites = ["site-b"]
    restored_lvol = LVol().from_dict(lvol.to_dict())
    assert restored_lvol.sync_active_site == "site-a"
    assert restored_lvol.sync_demoted_sites == ["site-b"]


def test_demoted_sites_are_not_shared_between_instances():
    first, second = LVol(), LVol()
    first.sync_demoted_sites.append("site-a")
    assert second.sync_demoted_sites == []


def test_pending_remote_instances_are_not_shared_between_instances():
    first, second = StorageNode(), StorageNode()
    first.remote_instances_pending.append("rp")
    assert second.remote_instances_pending == []


def test_lost_site_states():
    assert Cluster.LOST_SITE_FENCING == "fencing"
    assert Cluster.LOST_SITE_DONE == "done"


def test_sync_replication_event_round_trip():
    event = SyncReplicationEvent()
    event.uuid = "e1"
    event.cluster_id = "c1"
    event.lvs_name = "LVS_1"
    event.node_id = "n1"
    event.kind = SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED
    event.status = "remote_journal_unsynced"
    event.timestamp_utc = "2026-09-29T10:00:00.000Z"

    restored = SyncReplicationEvent().from_dict(event.to_dict())
    assert restored.get_id() == "e1"
    assert (restored.cluster_id, restored.lvs_name, restored.node_id) == ("c1", "LVS_1", "n1")
    assert restored.kind == "remote_journal_dropped"
    assert restored.status == "remote_journal_unsynced"
    assert restored.timestamp_utc == "2026-09-29T10:00:00.000Z"
    assert restored.resolved is False
    assert restored.observed_live is False

    event.observed_live = True
    assert SyncReplicationEvent().from_dict(event.to_dict()).observed_live is True


def test_sync_replication_event_kinds():
    assert SyncReplicationEvent.KIND_ZONE_UNAVAILABLE == "zone_unavailable"
    assert SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED == "remote_journal_dropped"
    assert SyncReplicationEvent.KIND_REMOTE_JOURNAL_RESTORED == "remote_journal_restored"


def test_sync_replication_event_index_tuples_follow_resolved():
    (index,) = SyncReplicationEvent._INDEXES
    event = SyncReplicationEvent()
    event.cluster_id = "c1"
    event.lvs_name = "LVS_1"
    assert index.tuples(event) == [("c1", "LVS_1", False)]
    event.resolved = True
    assert index.tuples(event) == [("c1", "LVS_1", True)]
