"""A stat RPC that fails must cost one lvol's sample, not the collector.

2026-09-14, regression test 14 (20-member VolumeGroupSnapshot): bdev_get_iostat
read-timed-out (timeout=3) against a node inside a bdev_lvol_snapshot_group
take. The RPCException propagated out of the per-lvol loop and killed the
service, and the restarted process immediately re-polled every lvol against the
still-busy node — piling RPC load onto the take it had just collided with.
"""
from unittest.mock import MagicMock

from simplyblock_core.rpc_client import RPCException
from simplyblock_core.services import lvol_stat_collector as collector


def _lvol(ha_type="single"):
    lvol = MagicMock()
    lvol.uuid = "lvol-uuid"
    lvol.lvol_uuid = "bdev-uuid"
    lvol.ha_type = ha_type
    lvol.nodes = ["primary-node", "secondary-node"]
    lvol.get_id.return_value = "lvol-uuid"
    return lvol


def test_rpc_failure_skips_the_sample_instead_of_raising():
    rpc = MagicMock()
    rpc.get_lvol_stats.side_effect = RPCException("connection error")

    record = collector.collect_lvol_record(MagicMock(), _lvol(), MagicMock(), rpc)

    assert record is None


def test_secondary_rpc_failure_is_contained_too():
    rpc = MagicMock()
    rpc.get_lvol_stats.return_value = None
    rpc.get_bdevs.return_value = None

    sec_node = MagicMock()
    sec_node.status = collector.StorageNode.STATUS_ONLINE
    sec_rpc = MagicMock()
    sec_rpc.get_lvol_stats.side_effect = RPCException("connection error")
    sec_node.rpc_client.return_value = sec_rpc

    db = MagicMock()
    db.get_storage_node_by_id.return_value = sec_node

    original_db = collector.db
    collector.db = db
    try:
        record = collector.collect_lvol_record(
            MagicMock(), _lvol(ha_type="ha"), MagicMock(), rpc)
    finally:
        collector.db = original_db

    assert record is None
