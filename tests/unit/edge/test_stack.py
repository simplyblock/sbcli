# coding=utf-8
"""Unit tests for the pure bdev-stack planner (spec §4, v3 active/active)."""
import pytest

from simplyblock_edge import stack
from simplyblock_edge.models import EdgeNode, EdgePartition

CLUSTER_NQN = "nqn.2023-02.io.simplyblock:0c0ffee0-cluster"


def _node(uuid, paths, is_primary=True, data_ip="10.0.0.1", nvmf_port=4420):
    node = EdgeNode()
    node.uuid = uuid
    node.data_ip = data_ip
    node.is_primary = is_primary
    node.nvmf_port = nvmf_port
    node.partitions = [EdgePartition({"device_path": p}) for p in paths]
    return node


def test_single_partition_is_bare_aio():
    plan = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1"]))
    assert [a.bdev_name for a in plan.aio_bdevs] == ["ea_aaaa1111_0"]
    assert plan.raid is None
    assert plan.top_bdev == "ea_aaaa1111_0"


def test_two_partitions_use_local_raid1():
    plan = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1"]))
    assert plan.raid.raid_level == "1"
    assert plan.raid.base_bdevs == ["ea_aaaa1111_0", "ea_aaaa1111_1"]
    assert plan.top_bdev == "el_aaaa1111"


@pytest.mark.parametrize("count", [3, 5])
def test_three_plus_partitions_use_raid5f(count):
    plan = stack.plan_local_stack(_node("aaaa1111-x", [f"/dev/sd{i}" for i in range(count)]))
    assert plan.raid.raid_level == "5f"
    assert len(plan.raid.base_bdevs) == count
    assert plan.raid.strip_size_kb == 64


def test_no_partitions_rejected():
    with pytest.raises(ValueError):
        stack.plan_local_stack(_node("aaaa1111-x", []))


def test_removed_partition_keeps_sibling_indices_stable():
    node = _node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1", "/dev/sdd1"])
    node.partitions[1].status = EdgePartition.STATUS_REMOVED
    plan = stack.plan_local_stack(node)
    assert [a.bdev_name for a in plan.aio_bdevs] == ["ea_aaaa1111_0", "ea_aaaa1111_2"]


def test_split_halves():
    plan = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1"]), split=True)
    assert plan.own_half == "ea_aaaa1111_0p0"
    assert plan.peer_half == "ea_aaaa1111_0p1"
    unsplit = stack.plan_local_stack(_node("aaaa1111-x", ["/dev/sdb1"]))
    assert unsplit.own_half == "ea_aaaa1111_0"
    with pytest.raises(ValueError):
        _ = unsplit.peer_half


def test_store_plan_primary_side():
    """Owner's instance: [its own half, the peer's exported PEER half (ns2)]."""
    node_a = _node("aaaa1111-x", ["/dev/sdb1"], is_primary=True)
    node_b = _node("bbbb2222-x", ["/dev/sdb1"], is_primary=False, data_ip="10.0.0.2")
    plan = stack.plan_store(node_a, node_a, node_b, 4420, 0)
    assert plan.lvs == "elvs_aaaa1111"
    assert plan.role == "primary"
    assert plan.mirror.name == "em_aaaa1111"
    assert plan.mirror.base_bdevs == ["ea_aaaa1111_0p0", "er_bbbb2222n2"]
    assert plan.mirror.superblock
    assert plan.client_port == 4420


def test_store_plan_secondary_side():
    """Secondary's instance of the SAME store: [its own PEER half, the
    owner's exported OWN half (ns1)] — the same two physical copies."""
    node_a = _node("aaaa1111-x", ["/dev/sdb1"], is_primary=True)
    node_b = _node("bbbb2222-x", ["/dev/sdb1"], is_primary=False)
    plan = stack.plan_store(node_b, node_a, node_b, 4420, 0)
    assert plan.lvs == "elvs_aaaa1111"
    assert plan.role == "secondary"
    assert plan.mirror.name == "em_aaaa1111"
    assert plan.mirror.base_bdevs == ["ea_bbbb2222_0p1", "er_aaaa1111n1"]


def test_per_store_client_ports():
    assert stack.store_client_port(4420, 0) == 4420
    assert stack.store_client_port(4420, 1) == 4421


def test_volume_naming():
    assert stack.volume_nqn(CLUSTER_NQN, "dddd4444-x") == f"{CLUSTER_NQN}:edge-lvol:dddd4444-x"
    assert stack.volume_bdev("aaaa1111-x", "pvc-1") == "elvs_aaaa1111/pvc-1"
    assert stack.crypto_bdev("dddd4444-x") == "ecr_dddd4444"


def test_single_node_lvs_base():
    node = _node("aaaa1111-x", ["/dev/sdb1", "/dev/sdc1"])
    assert stack.single_node_lvs_base(node) == "el_aaaa1111"


# --------------------------------------------------------------- cpu layout

@pytest.mark.parametrize("vcpus,app,lvs,nvmf", [
    (1, 0x1, 0x1, 0x1),          # everything on core 0
    (2, 0x1, 0x1, 0x2),          # app+lvs / nvmf
    (3, 0x1, 0x2, 0x4),          # one core each
    (4, 0x1, 0x2, 0xC),          # extra cores -> more nvmf pollers
    (5, 0x1, 0x2, 0x1C),
    (6, 0x1, 0x2, 0x3C),
])
def test_cpu_layout(vcpus, app, lvs, nvmf):
    layout = stack.plan_cpu_layout(vcpus)
    assert (layout.app_mask, layout.lvs_mask, layout.nvmf_mask) == (app, lvs, nvmf)
    assert layout.reactor_mask == (1 << vcpus) - 1


@pytest.mark.parametrize("vcpus", [0, 7, -1])
def test_cpu_layout_bounds(vcpus):
    with pytest.raises(ValueError):
        stack.plan_cpu_layout(vcpus)
