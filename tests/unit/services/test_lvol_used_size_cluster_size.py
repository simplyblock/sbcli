"""The lvol used-size sample converts allocated lvstore clusters into bytes with
the cluster size the lvstore was actually created with, not one recomputed
from the current cluster settings.

Customer report 2026-10-21 (26.3.2.1, 2x2 cluster): ``sbctl lvol list`` showed
39.7 TiB used on a 21 TiB volume. 26.3.2 changed ``create_lvstore`` to size a
cluster at ``page_size x ndcs`` and the collector to assume that size for every
lvstore -- but the customer's lvstores were created on 26.3.1 with one page per
cluster, so every allocated cluster was counted twice. The node record persists
the creation parameters (``lvstore_stack``), and that is the source of truth.
"""
from unittest.mock import MagicMock

from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.services import lvol_stat_collector as collector

PAGE = 2097152          # page_size_in_blocks default
LVS = "LVS_1234"


def _node(cluster_sz):
    node = StorageNode()
    node.uuid = "node-1"
    node.lvstore = LVS
    node.lvstore_stack = [
        {"type": "bdev_raid", "name": "raid0_1234", "params": {}},
        {"type": "bdev_lvstore", "name": LVS,
         "params": {"name": LVS, "bdev_name": "raid0_1234", "cluster_sz": cluster_sz}},
    ]
    return node


def _cluster(ndcs=2):
    cluster = MagicMock()
    cluster.page_size_in_blocks = PAGE
    cluster.distr_ndcs = ndcs
    return cluster


def _lvol():
    lvol = MagicMock()
    lvol.uuid = "lvol-uuid"
    lvol.lvs_name = LVS
    lvol.pool_uuid = "pool-uuid"
    lvol.get_id.return_value = "lvol-uuid"
    return lvol


def test_node_reports_the_persisted_cluster_size():
    assert _node(PAGE).lvstore_cluster_size(LVS) == PAGE
    assert _node(2 * PAGE).lvstore_cluster_size(LVS) == 2 * PAGE


def test_node_without_that_lvstore_reports_none():
    assert _node(PAGE).lvstore_cluster_size("LVS_other") is None
    assert StorageNode().lvstore_cluster_size(LVS) is None


def test_pre_26_3_2_lvstore_counts_one_page_per_cluster():
    # Created with cluster_sz = page_size (x1) on a cluster that is 2x2 today:
    # the persisted size wins over the settings-derived 2 x page.
    assert collector.lvstore_cluster_size(_cluster(ndcs=2), _lvol(), _node(PAGE)) == PAGE


def test_post_26_3_2_lvstore_counts_ndcs_pages_per_cluster():
    assert collector.lvstore_cluster_size(_cluster(ndcs=2), _lvol(), _node(2 * PAGE)) == 2 * PAGE


def test_used_never_exceeds_size_on_a_full_pre_26_3_2_volume():
    # 21 TiB volume, every cluster allocated, 2x2 cluster: the 26.3.2 formula
    # reported 2x the volume size here.
    size = 21 << 40
    allocated_clusters = size // PAGE
    cluster_size = collector.lvstore_cluster_size(_cluster(ndcs=2), _lvol(), _node(PAGE))

    assert allocated_clusters * cluster_size == size


def test_missing_stack_entry_falls_back_to_cluster_settings_and_warns(caplog):
    node = _node(PAGE)
    node.lvstore_stack = []

    with caplog.at_level("WARNING"):
        cluster_size = collector.lvstore_cluster_size(_cluster(ndcs=2), _lvol(), node)

    assert cluster_size == 2 * PAGE
    assert any("no persisted cluster_sz" in r.getMessage() for r in caplog.records)


def test_no_node_falls_back_to_cluster_settings():
    # ndcs 0 is clamped to one page, as before.
    assert collector.lvstore_cluster_size(_cluster(ndcs=0), _lvol(), None) == PAGE
