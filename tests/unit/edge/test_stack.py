# coding=utf-8
"""Unit tests for the pure bdev-stack planner (spec §4)."""
import pytest

from simplyblock_edge import stack
from simplyblock_edge.models import EdgeNode, EdgePartition

CLUSTER_ID = "0c0ffee0-0000-0000-0000-000000000000"
CLUSTER_NQN = "nqn.2023-02.io.simplyblock:" + CLUSTER_ID


def _node(uuid, paths, repl_port=4430, data_ip="10.0.0.1"):
    node = EdgeNode()
    node.uuid = uuid
    node.data_ip = data_ip
    node.repl_port = repl_port
    node.partitions = [EdgePartition({"device_path": p}) for p in paths]
    return node


def test_single_partition_is_bare_aio():
    plan = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1"]))
    assert [a.bdev_name for a in plan.aio_bdevs] == ["ea_aaaa1111_0"]
    assert plan.raid is None
    assert plan.top_bdev == "ea_aaaa1111_0"


def test_two_partitions_use_local_raid1():
    plan = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1"]))
    assert plan.raid is not None
    assert plan.raid.raid_level == "1"
    assert plan.raid.base_bdevs == ["ea_aaaa1111_0", "ea_aaaa1111_1"]
    assert plan.top_bdev == "el_aaaa1111"


@pytest.mark.parametrize("count", [3, 5])
def test_three_plus_partitions_use_raid5f(count):
    plan = stack.plan_local_stack(_node("aaaa1111-x", [f"/dev/sd{i}" for i in range(count)]))
    assert plan.raid.raid_level == "5f"
    assert len(plan.raid.base_bdevs) == count
    assert plan.raid.strip_size_kb == 64
    assert plan.top_bdev == "el_aaaa1111"


def test_no_partitions_rejected():
    with pytest.raises(ValueError):
        stack.plan_local_stack(_node("aaaa1111-x", []))


def test_removed_partition_keeps_sibling_indices_stable():
    """aio names are keyed by the ORIGINAL slot index — a removed slot must
    not renumber its siblings (reassembly/replace depend on it)."""
    node = _node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1", "/dev/sdd1"])
    node.partitions[1].status = EdgePartition.STATUS_REMOVED
    plan = stack.plan_local_stack(node)
    assert [a.bdev_name for a in plan.aio_bdevs] == ["ea_aaaa1111_0", "ea_aaaa1111_2"]


def test_mirror_plan():
    primary = _node("aaaa1111-x", ["/dev/sdb1"], data_ip="10.0.0.1")
    secondary = _node("bbbb2222-x", ["/dev/sdb1", "/dev/sdc1"], data_ip="10.0.0.2")
    plan = stack.plan_mirror(CLUSTER_ID, CLUSTER_NQN, primary, secondary)

    assert plan.remote_controller == "er_bbbb2222"
    assert plan.remote_leg == "er_bbbb2222n1"
    assert plan.remote_nqn == f"{CLUSTER_NQN}:edge-repl:bbbb2222-x"
    assert plan.remote_addr == "10.0.0.2"
    assert plan.remote_port == 4430
    assert plan.raid.raid_level == "1"
    # one leg local (primary's top), one leg remote
    assert plan.raid.base_bdevs == ["ea_aaaa1111_0", "er_bbbb2222n1"]
    assert plan.top_bdev == "em_0c0ffee0"


def test_lvstore_base_two_nodes_is_mirror():
    primary = _node("aaaa1111-x", ["/dev/sdb1"])
    assert stack.lvstore_base_bdev(CLUSTER_ID, 2, primary) == "em_0c0ffee0"


def test_lvstore_base_single_node_is_local_top():
    primary = _node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1"])
    assert stack.lvstore_base_bdev(CLUSTER_ID, 1, primary) == "el_aaaa1111"


def test_volume_naming():
    assert stack.volume_nqn(CLUSTER_NQN, "dddd4444-x") == f"{CLUSTER_NQN}:edge-lvol:dddd4444-x"
    assert stack.volume_bdev(CLUSTER_ID, "pvc-1") == "elvs_0c0ffee0/pvc-1"
