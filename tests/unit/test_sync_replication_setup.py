"""Sync-replication setup rules as pure logic: cluster flags, node site, activation layout."""
import pytest

from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.storage_node_ops import NodeSiteError, validate_node_site


def _cluster(sync=True, ndcs=2, npcs=1):
    cluster = Cluster()
    cluster.uuid = "c1"
    cluster.sync_replication = sync
    cluster.distr_ndcs = ndcs
    cluster.distr_npcs = npcs
    return cluster


def _node(node_id, site, host=None, status=StorageNode.STATUS_ONLINE, secondary=False):
    node = StorageNode()
    node.uuid = node_id
    node.site = site
    node.mgmt_ip = host or f"10.0.0.{node_id}"
    node.status = status
    node.is_secondary_node = secondary
    return node


def _two_sites(per_site=3):
    nodes = [_node(str(i), "site-a") for i in range(1, per_site + 1)]
    nodes += [_node(str(i), "site-b") for i in range(101, 101 + per_site)]
    return nodes


# -- cluster create -----------------------------------------------------------

def test_sync_replication_on_ha_cluster_is_accepted():
    cluster_ops._validate_sync_replication(True, "ha", False)


def test_no_sync_replication_accepts_anything():
    cluster_ops._validate_sync_replication(False, "single", True)


def test_sync_replication_requires_ha():
    with pytest.raises(ValueError, match="ha_type='ha'"):
        cluster_ops._validate_sync_replication(True, "single", False)


def test_sync_replication_refuses_single_node():
    with pytest.raises(ValueError, match="single-node"):
        cluster_ops._validate_sync_replication(True, "ha", True)


# -- node site ----------------------------------------------------------------

@pytest.mark.parametrize("site", ["site-a", "A", "dc1.rack_2", "x" * 63])
def test_valid_site_on_sync_cluster(site):
    assert validate_node_site(_cluster(), site) == site


@pytest.mark.parametrize("site", [None, ""])
def test_missing_site_on_sync_cluster(site):
    with pytest.raises(NodeSiteError, match="--site is required"):
        validate_node_site(_cluster(), site)


@pytest.mark.parametrize("site", ["a:b", "a/b", "-a", "a b", "x" * 64, "site-a\n", "moving:site-a"])
def test_malformed_site_on_sync_cluster(site):
    with pytest.raises(NodeSiteError, match="invalid site"):
        validate_node_site(_cluster(), site)


def test_site_on_non_sync_cluster_is_refused():
    with pytest.raises(NodeSiteError, match="not created with --sync-replication"):
        validate_node_site(_cluster(sync=False), "site-a")


@pytest.mark.parametrize("site", [None, ""])
def test_non_sync_cluster_without_site(site):
    assert validate_node_site(_cluster(sync=False), site) == ""


def test_node_site_error_is_a_value_error():
    assert issubclass(storage_node_ops.NodeSiteError, ValueError)


# -- activation: configured topology -------------------------------------------

def test_two_sites_are_a_valid_topology():
    assert cluster_ops.sync_topology_violation(_cluster(), _two_sites()) is None


def test_topology_is_not_checked_on_a_non_sync_cluster():
    nodes = [_node("1", ""), _node("2", "x"), _node("3", "y"), _node("4", "z")]
    assert cluster_ops.sync_topology_violation(_cluster(sync=False), nodes) is None


def test_third_site_is_refused():
    nodes = _two_sites() + [_node("201", "site-c")]
    violation = cluster_ops.sync_topology_violation(_cluster(), nodes)
    assert "exactly 2 sites" in violation and "site-c" in violation


def test_offline_node_on_a_third_site_is_refused():
    nodes = _two_sites() + [_node("201", "site-c", status=StorageNode.STATUS_OFFLINE)]
    assert "exactly 2 sites" in cluster_ops.sync_topology_violation(_cluster(), nodes)


def test_single_site_is_refused():
    nodes = [_node(str(i), "site-a") for i in range(1, 7)]
    assert "exactly 2 sites" in cluster_ops.sync_topology_violation(_cluster(), nodes)


def test_node_without_site_is_refused():
    nodes = _two_sites() + [_node("201", "", status=StorageNode.STATUS_OFFLINE)]
    assert "without a site: 201" in cluster_ops.sync_topology_violation(_cluster(), nodes)


def test_host_spanning_two_sites_is_refused():
    nodes = _two_sites()
    nodes[-1].mgmt_ip = nodes[0].mgmt_ip
    assert "spans sites" in cluster_ops.sync_topology_violation(_cluster(), nodes)


def test_removed_and_secondary_records_do_not_count():
    nodes = _two_sites() + [
        _node("201", "site-c", status=StorageNode.STATUS_REMOVED),
        _node("202", "", secondary=True),
    ]
    assert cluster_ops.sync_topology_violation(_cluster(), nodes) is None


# -- activation: online capacity per site ---------------------------------------

def test_enough_online_nodes_on_each_site():
    nodes = _two_sites()
    assert cluster_ops.sync_capacity_violation(_cluster(), nodes, nodes) is None


def test_capacity_is_not_checked_on_a_non_sync_cluster():
    nodes = [_node("1", "")]
    assert cluster_ops.sync_capacity_violation(_cluster(sync=False), nodes, nodes) is None


def test_too_few_nodes_for_the_stripe_on_a_site():
    # ndcs + npcs = 4 per site, three hosts per site available
    nodes = _two_sites(per_site=3)
    violation = cluster_ops.sync_capacity_violation(_cluster(ndcs=2, npcs=2), nodes, nodes)
    assert "site site-a has 3 online node(s)" in violation
    assert "site site-b has 3 online node(s)" in violation


def test_too_few_hosts_for_a_triplet_on_a_site():
    nodes = _two_sites(per_site=3)
    for node in nodes[:3]:
        node.mgmt_ip = "10.0.0.1"  # three site-a nodes on one host
    violation = cluster_ops.sync_capacity_violation(_cluster(ndcs=1, npcs=1), nodes, nodes)
    assert violation == "site site-a has 3 online node(s) on 1 host(s), needs 2 node(s) on 3 host(s)"


def test_a_site_with_no_online_node_is_reported():
    nodes = _two_sites()
    online = [n for n in nodes if n.site == "site-a"]
    violation = cluster_ops.sync_capacity_violation(_cluster(), nodes, online)
    assert violation == "site site-b has 0 online node(s) on 0 host(s), needs 3 node(s) on 3 host(s)"


# -- activation: journal hosts per site ---------------------------------------------

def test_every_site_needs_a_host_per_copy_of_the_largest_journal():
    # site b has four hosts, site a three; one node journals 4 + 4, so every
    # node places four copies on site a as well.
    nodes = _two_sites(per_site=3) + [_node("104", "site-b")]
    for node in nodes:
        node.ha_jm_count = 6
    nodes[-1].ha_jm_count = 8
    violation = cluster_ops.sync_capacity_violation(_cluster(ndcs=1, npcs=1), nodes, nodes)
    assert violation == "site site-a has 3 online node(s) on 3 host(s), needs 2 node(s) on 4 host(s)"


def test_the_cluster_journal_rule_needs_four_hosts_per_site_with_failure_domains():
    cluster = _cluster(ndcs=1, npcs=1)
    cluster.enable_failure_domain = True   # per-site journal share of 4
    nodes = _two_sites(per_site=3)
    violation = cluster_ops.sync_capacity_violation(cluster, nodes, nodes)
    assert "site site-a has 3 online node(s) on 3 host(s), needs 2 node(s) on 4 host(s)" in violation
    assert "site site-b" in violation
    assert cluster_ops.sync_capacity_violation(cluster, _two_sites(per_site=4),
                                               _two_sites(per_site=4)) is None


def test_journal_hosts_with_no_online_node_at_all():
    nodes = _two_sites()
    violation = cluster_ops.sync_capacity_violation(_cluster(), nodes, [])
    assert violation == ("site site-a has 0 online node(s) on 0 host(s), needs 3 node(s) on 3 host(s); "
                         "site site-b has 0 online node(s) on 0 host(s), needs 3 node(s) on 3 host(s)")
