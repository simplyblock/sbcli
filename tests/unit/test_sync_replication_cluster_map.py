"""Which LVS instances (and so which distribs, with which home site) a node
hosts - pure logic over node records. The cluster maps and their senders are
in tests/integration/test_sync_replication_cluster_map.py.
"""
import logging

from simplyblock_core import distr_controller
from simplyblock_core.models.storage_node import StorageNode


def _node(node_id, site="", distribs=(), secondary_of="", tertiary_of="",
          remote=("", "", ""), status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = node_id
    node.site = site
    node.status = status
    node.lvstore_stack = [{"type": "bdev_distr", "name": d, "params": {}} for d in distribs]
    if distribs:
        node.lvstore_stack.append({"type": "bdev_lvstore", "name": f"LVS_{node_id}", "params": {}})
    node.lvstore_stack_secondary = secondary_of
    node.lvstore_stack_tertiary = tertiary_of
    node.remote_primary_node_id, node.remote_secondary_node_id, node.remote_tertiary_node_id = remote
    return node


def test_owners_are_self_hosted_local_roles_and_remote_triplets():
    a1 = _node("a1", "a", distribs=["d_a1"])
    b1 = _node("b1", "b", distribs=["d_b1"], remote=("x", "", ""))
    a2 = _node("a2", "a", distribs=["d_a2"], secondary_of="a1")
    x = _node("x", "b", tertiary_of="b1")
    nodes = [a1, b1, a2, x]
    assert [n.get_id() for n in distr_controller.lvs_owners_on_node(a2, nodes)] == ["a2", "a1"]
    # x hosts b1 as tertiary AND as remote primary: listed once.
    assert [n.get_id() for n in distr_controller.lvs_owners_on_node(x, nodes)] == ["b1"]


def test_a_node_with_no_stack_owns_nothing():
    lone = _node("lone", "a")
    assert distr_controller.lvs_owners_on_node(lone, [lone]) == []


def test_missing_and_removed_owners_are_skipped(caplog):
    gone = _node("gone", "a", distribs=["d_gone"], status=StorageNode.STATUS_REMOVED)
    host = _node("h", "a", secondary_of="ghost", tertiary_of="gone")
    with caplog.at_level(logging.WARNING):
        assert distr_controller.lvs_owners_on_node(host, [gone, host]) == []
    assert "ghost" in caplog.text


def test_distribs_carry_the_home_site_of_their_owner():
    a1 = _node("a1", "a", distribs=["d_a1_1", "d_a1_2"], remote=("", "b2", ""))
    b1 = _node("b1", "b", distribs=["d_b1"])
    b2 = _node("b2", "b", distribs=["d_b2"], secondary_of="b1")
    assert distr_controller.distribs_on_node(b2, [a1, b1, b2]) == [
        ("d_b2", "b"), ("d_b1", "b"), ("d_a1_1", "a"), ("d_a1_2", "a")]
