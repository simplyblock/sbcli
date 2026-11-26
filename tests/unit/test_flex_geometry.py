"""Flex geometry: the lvstore cluster size, and the raid0 strip under it.

Without flex geometry the lvstore cluster is one page per data chunk (1+x: 2 MiB,
2+x: 4 MiB, 4+x: 8 MiB) and the raid0 strip is that cluster. With flex geometry
(chosen at cluster create) both are a fixed 8 MiB for every scheme.

The strip used to be (ndcs + npcs) * 2 MiB rounded up to a power of two (8 MiB
for 1+2), counting parity chunks that never appear in the raid's address space.
"""
from unittest.mock import MagicMock

import pytest

from simplyblock_core import cluster_ops, constants, storage_node_ops, utils
from simplyblock_core.models.cluster import Cluster

MiB = 1024 * 1024
PAGE = 2 * MiB


@pytest.mark.parametrize("ndcs, size", [(1, 2 * MiB), (2, 4 * MiB), (4, 8 * MiB), (0, 2 * MiB)])
def test_default_cluster_size_is_one_page_per_data_chunk(ndcs, size):
    assert utils.lvstore_cluster_size(False, ndcs, PAGE) == size


@pytest.mark.parametrize("ndcs", [1, 2, 4])
def test_flex_cluster_size_is_8_mib_for_every_scheme(ndcs):
    assert utils.lvstore_cluster_size(True, ndcs, PAGE) == 8 * MiB == constants.FLEX_GEOMETRY_LVS_CLUSTER_SIZE


def test_cluster_model_defaults_off_and_reports_its_size():
    c = Cluster()
    assert c.flex_geometry is False
    c.distr_ndcs, c.page_size_in_blocks = 2, PAGE
    assert c.lvs_cluster_size() == 4 * MiB
    c.flex_geometry = True
    assert c.lvs_cluster_size() == 8 * MiB


def test_flex_needs_a_page_size_that_divides_8_mib():
    cluster_ops._validate_flex_geometry(True, PAGE)
    cluster_ops._validate_flex_geometry(False, 3 * MiB)
    with pytest.raises(ValueError, match="flex_geometry"):
        cluster_ops._validate_flex_geometry(True, 3 * MiB)


def _lvstore_params(monkeypatch, ndcs, npcs, flex):
    cluster = MagicMock()
    cluster.flex_geometry = flex
    cluster.page_copy = False
    cluster.shared_placement = True
    cluster.write_protection_v2 = True
    db = MagicMock()
    db.get_cluster_by_id.return_value = cluster
    monkeypatch.setattr(storage_node_ops, "DBController", lambda: db)
    monkeypatch.setattr(storage_node_ops.utils, "get_next_lvstore_ports", lambda cluster_id: (4430, 4431))
    monkeypatch.setattr(storage_node_ops, "storage_network", MagicMock(), raising=False)
    vuids = iter(range(100, 200))
    monkeypatch.setattr(storage_node_ops.utils, "get_random_vuid", lambda: next(vuids))
    captured = {}

    def create_bdev_stack(snode, stack, primary_node=None):
        captured["stack"] = stack
        return None, "stop here"

    monkeypatch.setattr(storage_node_ops, "_create_bdev_stack", create_bdev_stack)
    snode = MagicMock()
    snode.enable_ha_jm = False
    snode.number_of_distribs = 2
    assert storage_node_ops.create_lvstore(snode, ndcs, npcs, 4096, 4096, PAGE, 0) is False
    stack = captured["stack"]
    lvs = next(b for b in stack if b["type"] == "bdev_lvstore")
    raid = next(b for b in stack if b["type"] == "bdev_raid")
    return lvs["params"]["cluster_sz"], raid["params"]["strip_size_kb"]


@pytest.mark.parametrize("ndcs, npcs, cluster_sz", [
    (1, 2, 2 * MiB),   # was an 8 MiB strip under a 2 MiB cluster
    (1, 1, 2 * MiB),
    (2, 1, 4 * MiB),
    (2, 2, 4 * MiB),
    (4, 2, 8 * MiB),
])
def test_default_lvstore_strip_is_its_cluster(monkeypatch, ndcs, npcs, cluster_sz):
    assert _lvstore_params(monkeypatch, ndcs, npcs, False) == (cluster_sz, cluster_sz // 1024)


@pytest.mark.parametrize("ndcs, npcs", [(1, 2), (2, 1), (4, 2)])
def test_flex_lvstore_cluster_and_strip_are_8_mib(monkeypatch, ndcs, npcs):
    assert _lvstore_params(monkeypatch, ndcs, npcs, True) == (8 * MiB, 8 * 1024)


def test_flex_is_passed_through_cluster_create_to_the_record():
    import inspect
    for fn in (cluster_ops.create_cluster, cluster_ops.add_cluster, cluster_ops._add_cluster_impl):
        assert inspect.signature(fn).parameters["flex_geometry"].default is False
