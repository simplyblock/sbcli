# coding=utf-8
"""Unit tests for edge_cluster_ops control flows against the stateful fakes
(FakeKV-backed DB, FakeSpdk per node, FakeK8s)."""
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


# ------------------------------------------------------------------ cluster

def test_create_edge_cluster(env):
    cluster = _create_cluster()
    assert cluster.cluster_type == Cluster.TYPE_EDGE
    assert cluster.status == Cluster.STATUS_UNREADY
    assert cluster.mode == "kubernetes"
    assert cluster.uuid in cluster.nqn
    assert cluster.secret.get_secret_value()

    persisted = DBController().get_cluster_by_id(cluster.uuid)
    assert persisted.cluster_type == Cluster.TYPE_EDGE
    assert edge_db.get_edge_clusters()[0].uuid == cluster.uuid


def test_create_duplicate_cluster_name_rejected(env):
    _create_cluster("edge-1")
    with pytest.raises(ValueError):
        _create_cluster("edge-1")


# -------------------------------------------------------------------- nodes

def test_add_first_node_builds_stack_and_activates(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1", "/dev/sdc1"])

    assert node.is_primary
    assert node.status == EdgeNode.STATUS_ONLINE
    assert fake_k8s.deployed == ["worker-1"]

    rpc = spdk.for_ip("10.0.0.1")
    # local raid1 over the two partitions
    local = stack.local_raid_name(node.uuid)
    assert rpc.raids[local] == [stack.aio_bdev_name(node.uuid, 0),
                                stack.aio_bdev_name(node.uuid, 1)]
    # replication subsystem exposing the local top
    repl = stack.repl_nqn(cluster.nqn, node.uuid)
    assert rpc.subsystems[repl]["namespaces"][0]["bdev_name"] == local
    assert rpc.subsystems[repl]["listen_addresses"][0]["trsvcid"] == "4430"
    # no lvstore yet (lazy)
    assert rpc.lvstores == {}
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_ACTIVE


def test_add_second_node_builds_mirror_and_lvstore(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    primary = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    secondary = _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])

    assert not secondary.is_primary
    primary_rpc = spdk.for_ip("10.0.0.1")
    mirror = stack.mirror_name(cluster.uuid)
    # mirror raid1 = [primary local top, remote leg to worker-2]
    assert primary_rpc.raids[mirror] == [
        stack.aio_bdev_name(primary.uuid, 0),
        stack.remote_leg_bdev(secondary.uuid),
    ]
    # lvstore sits on the mirror, recorded on the primary
    assert primary_rpc.lvstores[stack.lvs_name(cluster.uuid)] == mirror
    assert edge_db.get_edge_node_by_id(cluster.uuid, primary.uuid).lvstore_base == mirror
    # secondary exposes its repl subsystem
    secondary_rpc = spdk.for_ip("10.0.0.2")
    assert stack.repl_nqn(cluster.nqn, secondary.uuid) in secondary_rpc.subsystems


def test_third_node_rejected(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    with pytest.raises(ValueError, match="at most 2"):
        _add_node(cluster, "worker-3", "10.0.0.3", ["/dev/sdb1"])


def test_add_node_requires_partitions(env):
    cluster = _create_cluster()
    with pytest.raises(ValueError, match="partition"):
        _add_node(cluster, "worker-1", "10.0.0.1", [])


def test_expansion_under_existing_lvstore_rejected(env):
    """Spec §10: volumes created on the 1-node layout pin the lvstore to the
    local top; adding a second node afterwards must be rejected."""
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    with pytest.raises(ValueError, match="Add both nodes before creating volumes"):
        _add_node(cluster, "worker-2", "10.0.0.2", ["/dev/sdb1"])


def test_add_node_on_hyperscale_cluster_rejected(env):
    kv, _, _ = env
    cluster = Cluster()
    cluster.uuid = "hyper-1"
    cluster.cluster_name = "hyper"
    cluster.write_to_db(kv)
    with pytest.raises(ValueError, match="not an edge cluster"):
        _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])


def test_failed_node_add_marks_node_offline(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    spdk.for_ip("10.0.0.1").fail.add("bdev_aio_create")
    with pytest.raises(Exception):
        _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    nodes = edge_db.get_edge_nodes(cluster.uuid)
    assert len(nodes) == 1
    assert nodes[0].status == EdgeNode.STATUS_OFFLINE


def test_shutdown_and_restart_node(env):
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])

    edge_cluster_ops.shutdown_node(cluster.uuid, node.uuid)
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == EdgeNode.STATUS_DOWN
    assert fake_k8s.deleted == ["worker-1"]

    task_id = edge_cluster_ops.restart_node(cluster.uuid, node.uuid)
    # pod redeployed, node released from DOWN, reassembly task enqueued
    assert fake_k8s.deployed.count("worker-1") == 2
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == EdgeNode.STATUS_OFFLINE
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
    kv, spdk, fake_k8s = env
    cluster = _create_cluster()
    node = _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 10 * 1024 ** 3)

    rpc = spdk.for_ip("10.0.0.1")
    lvs = stack.lvs_name(cluster.uuid)
    # lvstore created lazily on the local top (single node, one partition)
    assert rpc.lvstores[lvs] == stack.aio_bdev_name(node.uuid, 0)
    assert rpc.called("create_lvol")[0][1]["size_in_mib"] == 10 * 1024
    subsystem = rpc.subsystems[volume.nqn]
    assert subsystem["namespaces"][0]["bdev_name"] == f"{lvs}/vol-1"
    assert subsystem["listen_addresses"][0]["trsvcid"] == "4420"
    assert edge_db.get_edge_volume_by_name(cluster.uuid, "vol-1").uuid == volume.uuid


def test_create_volume_duplicate_name_rejected(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    with pytest.raises(ValueError, match="already exists"):
        edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)


def test_connect_info(env):
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
    assert len(info) == 1
    assert info[0]["transport"] == "tcp"
    assert info[0]["ip"] == "10.0.0.1"
    assert info[0]["port"] == 4420
    assert info[0]["nqn"] == volume.nqn


def test_delete_volume(env):
    kv, spdk, _ = env
    cluster = _create_cluster()
    _add_node(cluster, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    edge_cluster_ops.delete_volume(cluster.uuid, volume.uuid)

    rpc = spdk.for_ip("10.0.0.1")
    assert volume.nqn not in rpc.subsystems
    assert volume.lvol_bdev not in rpc.bdevs
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
    assert edge_db.get_edge_volume_by_id(cluster.uuid, volume.uuid).size == 2 * 1024 ** 3


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
    fresh = edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)
    assert fresh.partitions[0].status == EdgePartition.STATUS_FAILED
    task = DBController().get_job_tasks(cluster.uuid)[0]
    assert task.uuid == task_id
    assert task.function_name == JobSchedule.FN_EDGE_DEVICE_REPLACE
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
    fresh = edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)
    assert fresh.partitions[3].status == EdgePartition.STATUS_NEW
    task = DBController().get_job_tasks(cluster.uuid)[0]
    assert task.function_name == JobSchedule.FN_EDGE_DEVICE_ADD
