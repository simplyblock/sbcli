# coding=utf-8
"""Unit tests for edge_cluster_ops control flows (v3 active/active) against
the stateful fakes (FakeKV-backed DB, FakeSpdk per node, FakeK8s)."""
import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.db_controller import DBController
from simplyblock_edge import db as edge_db, edge_cluster_ops, stack
from simplyblock_edge.models import EdgeNode, EdgePartition


@pytest.fixture()
def env(kv, spdk, fake_k8s):
    return kv, spdk, fake_k8s


def _create_cluster(name="edge-1"):
    return edge_cluster_ops.create_edge_cluster(name)


def _add_node(cluster, hostname, mgmt_ip, partitions):
    return edge_cluster_ops.add_edge_node(cluster.uuid, hostname, mgmt_ip, partitions)


def _fresh(cluster, node):
    return edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)


# ------------------------------------------------------------------ cluster

def test_create_edge_cluster(env):
    cluster = _create_cluster()
    assert cluster.cluster_type == Cluster.TYPE_EDGE
    assert cluster.status == Cluster.STATUS_UNREADY
    assert cluster.uuid in cluster.nqn
    assert cluster.secret.get_secret_value()
    assert edge_db.get_edge_clusters()[0].uuid == cluster.uuid


def test_create_duplicate_cluster_name_rejected(env):
    _create_cluster("edge-1")
    with pytest.raises(ValueError):
        _create_cluster("edge-1")


# -------------------------------------------------------------------- nodes

def test_add_first_node_builds_flat_stack(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1", "/dev/sdc1"])

    assert node.is_primary and node.status == EdgeNode.STATUS_ONLINE
    rpc = spdk.for_ip("10.0.0.1")
    local = stack.local_raid_name(node.uuid)
    assert rpc.raids[local] == [stack.aio_bdev_name(node.uuid, 0),
                                stack.aio_bdev_name(node.uuid, 1)]
    # single node: no split, no lvstore yet (lazy), repl subsystem present
    assert not rpc.called("bdev_split")
    assert rpc.lvstores == {}
    assert stack.repl_nqn(cluster.nqn, node.uuid) in rpc.subsystems
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_ACTIVE


def test_second_node_forms_active_active(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node_a = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    node_b = _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])

    rpc_a, rpc_b = spdk.for_ip("10.0.0.1"), spdk.for_ip("10.0.0.2")
    # both nodes split their tops and export both halves on the repl subsystem
    for rpc, node in ((rpc_a, node_a), (rpc_b, node_b)):
        assert rpc.called("bdev_split")
        repl = rpc.subsystems[stack.repl_nqn(cluster.nqn, node.uuid)]
        assert len(repl["namespaces"]) == 2

    # store A: primary instance on A, live secondary instance on B
    plan_a = stack.plan_store(node_a, node_a, node_b, node_a.nvmf_port, 0)
    assert rpc_a.raids[plan_a.mirror.name] == plan_a.mirror.base_bdevs
    assert rpc_a.lvstores[plan_a.lvs]["role"] == "primary"
    assert rpc_a.lvstores[plan_a.lvs]["leader"] is True
    sec_a = stack.plan_store(node_b, node_a, node_b, node_a.nvmf_port, 0)
    assert rpc_b.raids[sec_a.mirror.name] == sec_a.mirror.base_bdevs
    assert rpc_b.lvstores[plan_a.lvs]["role"] == "secondary"
    assert rpc_b.called("bdev_lvol_update_lvstore")

    # store B mirrored the other way around
    plan_b = stack.plan_store(node_b, node_b, node_a, node_b.nvmf_port, 1)
    assert rpc_b.lvstores[plan_b.lvs]["role"] == "primary"
    assert rpc_a.lvstores[plan_b.lvs]["role"] == "secondary"

    # records: each node owns + leads its store
    fresh_a, fresh_b = _fresh(cluster, node_a), _fresh(cluster, node_b)
    assert fresh_a.leader_of == [stack.lvs_name(node_a.uuid)]
    assert fresh_b.leader_of == [stack.lvs_name(node_b.uuid)]
    assert fresh_a.lvstore_base == stack.mirror_name(node_a.uuid)


def test_third_node_rejected(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    with pytest.raises(ValueError, match="at most 2"):
        _add_node(cluster, "worker-3", "10.0.0.3", ["/dev/sdb1"])


def test_expansion_under_single_node_lvstore_rejected(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    with pytest.raises(ValueError, match="Add both nodes before creating volumes"):
        _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])


def test_failed_node_add_marks_node_offline(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    spdk.for_ip("10.0.0.1").fail.add("bdev_aio_create")
    with pytest.raises(Exception):
        _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    assert edge_db.get_edge_nodes(cluster.uuid)[0].status == EdgeNode.STATUS_OFFLINE


def test_shutdown_and_restart_node(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])

    edge_cluster_ops.shutdown_node(cluster.uuid, node.uuid)
    assert _fresh(cluster, node).status == EdgeNode.STATUS_DOWN
    assert fake_k8s.deleted == ["worker-1"]

    task_id = edge_cluster_ops.restart_node(cluster.uuid, node.uuid)
    assert fake_k8s.deployed.count("worker-1") == 2
    assert _fresh(cluster, node).status == EdgeNode.STATUS_OFFLINE
    tasks = DBController().get_job_tasks(cluster.uuid)
    assert [t.uuid for t in tasks] == [task_id]
    assert tasks[0].function_name == JobSchedule.FN_EDGE_NODE_RESTART


def test_edge_task_dedupe(env):
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    first = edge_cluster_ops.add_edge_task(JobSchedule.FN_EDGE_NODE_RESTART,
                                           cluster.uuid, node.uuid)
    second = edge_cluster_ops.add_edge_task(JobSchedule.FN_EDGE_NODE_RESTART,
                                            cluster.uuid, node.uuid)
    assert first == second
    assert len(DBController().get_job_tasks(cluster.uuid)) == 1


# ------------------------------------------------------------------ volumes

def test_create_volume_single_node(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 10 * 1024 ** 3)

    rpc = spdk.for_ip("10.0.0.1")
    lvs = stack.lvs_name(node.uuid)
    # lvstore lazily created directly on the local top (flat layout)
    assert rpc.lvstores[lvs]["base"] == stack.aio_bdev_name(node.uuid, 0)
    assert volume.home_node_id == node.uuid
    assert volume.client_port == 4420
    subsystem = rpc.subsystems[volume.nqn]
    assert subsystem["namespaces"][0]["bdev_name"] == f"{lvs}/vol-1"
    assert subsystem["listen_addresses"][0]["ana_state"] == "optimized"


def test_volume_placement_balances_and_registers(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    node_a = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    node_b = _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])

    vol_1 = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    vol_2 = edge_cluster_ops.create_volume(cluster.uuid, "vol-2", 1024 ** 3)
    homes = {vol_1.home_node_id, vol_2.home_node_id}
    assert homes == {node_a.uuid, node_b.uuid}  # balanced across both stores
    assert {vol_1.client_port, vol_2.client_port} == {4420, 4421}

    for volume, owner, peer_rpc_ip in (
            (vol_1 if vol_1.home_node_id == node_a.uuid else vol_2, node_a, "10.0.0.2"),
            (vol_1 if vol_1.home_node_id == node_b.uuid else vol_2, node_b, "10.0.0.1")):
        owner_rpc = spdk.for_ip(owner.mgmt_ip)
        peer_rpc = spdk.for_ip(peer_rpc_ip)
        # created on the leader, REGISTERED on the pairing secondary instance
        assert volume.lvol_bdev in owner_rpc.bdevs
        assert volume.lvol_bdev in peer_rpc.bdevs
        register = peer_rpc.called("bdev_lvol_register")
        assert any(c[1]["lvs_name"] == stack.lvs_name(owner.uuid) for c in register)
        # two paths: optimized on the leader, non-optimized on the peer
        assert owner_rpc.subsystems[volume.nqn]["listen_addresses"][0]["ana_state"] \
            == "optimized"
        assert peer_rpc.subsystems[volume.nqn]["listen_addresses"][0]["ana_state"] \
            == "non_optimized"
        # both namespaces exist (registration made the bdev real on the peer)
        assert peer_rpc.subsystems[volume.nqn]["namespaces"][0]["bdev_name"] == \
            volume.lvol_bdev


def test_create_volume_duplicate_name_rejected(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    with pytest.raises(ValueError, match="already exists"):
        edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)


def test_connect_info_two_paths(env):
    cluster = _create_cluster()
    node_a = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)

    info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
    assert len(info) == 2
    assert info[0]["active"] and not info[1]["active"]
    leader_ip = "10.0.0.1" if volume.home_node_id == node_a.uuid else "10.0.0.2"
    assert info[0]["ip"] == leader_ip
    assert all(e["port"] == volume.client_port for e in info)
    assert all(e["nqn"] == volume.nqn for e in info)


def test_delete_volume(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    edge_cluster_ops.delete_volume(cluster.uuid, volume.uuid)

    for ip in ("10.0.0.1", "10.0.0.2"):
        assert volume.nqn not in spdk.for_ip(ip).subsystems
    assert edge_db.get_edge_volumes(cluster.uuid) == []


def test_resize_volume(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)

    with pytest.raises(ValueError, match="larger"):
        edge_cluster_ops.resize_volume(cluster.uuid, volume.uuid, 1024 ** 3)
    updated = edge_cluster_ops.resize_volume(cluster.uuid, volume.uuid, 2 * 1024 ** 3)
    assert updated.size == 2 * 1024 ** 3
    assert spdk.for_ip("10.0.0.1").called("bdev_lvol_resize")[0][1]["size_in_mib"] == 2048


# ------------------------------------------------------------------ devices

def test_replace_only_partition_of_single_node_rejected(env):
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    with pytest.raises(ValueError, match="no redundancy"):
        edge_cluster_ops.replace_device(cluster.uuid, node.uuid, "/dev/sdb1", "/dev/sdz1")


def test_replace_device_marks_failed_and_enqueues(env):
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1", "/dev/sdc1"])
    task_id = edge_cluster_ops.replace_device(cluster.uuid, node.uuid,
                                              "/dev/sdb1", "/dev/sdz1")
    fresh = _fresh(cluster, node)
    assert fresh.partitions[0].status == EdgePartition.STATUS_FAILED
    task = DBController().get_job_tasks(cluster.uuid)[0]
    assert task.uuid == task_id
    assert task.function_params == {"old_path": "/dev/sdb1", "new_path": "/dev/sdz1"}


def test_add_device_requires_raid5(env):
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1", "/dev/sdc1"])
    with pytest.raises(ValueError, match="raid5"):
        edge_cluster_ops.add_device(cluster.uuid, node.uuid, "/dev/sdz1")


def test_add_device_under_raid5_enqueues(env):
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1",
                     ["/dev/sdb1", "/dev/sdc1", "/dev/sdd1"])
    edge_cluster_ops.add_device(cluster.uuid, node.uuid, "/dev/sde1")
    fresh = _fresh(cluster, node)
    assert fresh.partitions[3].status == EdgePartition.STATUS_NEW
    task = DBController().get_job_tasks(cluster.uuid)[0]
    assert task.function_name == JobSchedule.FN_EDGE_DEVICE_ADD


# ------------------------------------------------- retry after failed add

def test_failed_node_add_is_retryable(env):
    """A node add that fails leaves an offline record behind. That record
    must NOT make the retry impossible — the first live run hit "at most 2
    nodes" on a 1-node cluster after two failed attempts and could never
    recover without manual DB surgery."""
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    spdk.for_ip("10.0.0.1").fail.add("bdev_aio_create")
    for _ in range(3):
        with pytest.raises(Exception):
            _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
        # exactly one (failed) record, never an accumulating pile
        assert len(edge_db.get_edge_nodes(cluster.uuid)) == 1

    # and the retry succeeds once the underlying fault clears
    spdk.for_ip("10.0.0.1").fail.clear()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    assert node.status == EdgeNode.STATUS_ONLINE
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status_reason == ""


def test_failed_node_add_records_the_reason(env):
    """The reason must land on the record: without it a client can only poll
    until its own timeout and report 'timed out (last error: None)'."""
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    spdk.for_ip("10.0.0.1").fail.add("bdev_aio_create")
    with pytest.raises(Exception):
        _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])

    node = edge_db.get_edge_nodes(cluster.uuid)[0]
    assert node.status == EdgeNode.STATUS_OFFLINE
    assert "bdev_aio_create" in node.status_reason


def test_established_nodes_still_capped_at_two(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    with pytest.raises(ValueError, match="at most 2"):
        _add_node(cluster, "worker-3", "10.0.0.3", ["/dev/sdb1"])
