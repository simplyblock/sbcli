# coding=utf-8
"""Unit tests for the device lifecycle the e2e suite exercises: graceful
remove -> restart, monitor-detected unavailability (EBS force-detach) ->
reattach + restart, and permanent replacement."""
import pytest

from simplyblock_edge import db as edge_db, edge_cluster_ops, stack
from simplyblock_edge.models import EdgePartition
from simplyblock_edge.services.edge_monitor import EdgeMonitor


@pytest.fixture()
def env(kv, spdk, fake_k8s):
    return kv, spdk, fake_k8s


def _cluster(spdk, paths=("/dev/sdb1", "/dev/sdc1")):
    cluster = edge_cluster_ops.create_edge_cluster("edge-dev")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          list(paths))
    return cluster, node


def _part(cluster, node, path):
    fresh = edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)
    return next(p for p in fresh.partitions if p.device_path == path)


def _monitor():
    return EdgeMonitor("edge-monitor-test", interval_sec=0, sleep=lambda _s: None)


# ------------------------------------------------------- remove + restart

def test_remove_device_takes_raid_member_offline(env):
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    bdev = stack.aio_bdev_name(node.uuid, 0)

    edge_cluster_ops.remove_device(cluster.uuid, node.uuid, "/dev/sdb1")

    rpc = spdk.for_ip("10.0.0.1")
    assert bdev not in rpc.raids[stack.local_raid_name(node.uuid)]
    assert bdev not in rpc.bdevs
    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_OFFLINE
    # idempotent
    edge_cluster_ops.remove_device(cluster.uuid, node.uuid, "/dev/sdb1")


def test_remove_last_redundancy_rejected(env):
    _, spdk, _ = env
    cluster, node = _cluster(spdk, paths=("/dev/sdb1",))
    with pytest.raises(ValueError, match="no redundancy"):
        edge_cluster_ops.remove_device(cluster.uuid, node.uuid, "/dev/sdb1")


def test_restart_device_rejoins_raid(env):
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    edge_cluster_ops.remove_device(cluster.uuid, node.uuid, "/dev/sdb1")

    edge_cluster_ops.restart_device(cluster.uuid, node.uuid, "/dev/sdb1")

    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    assert bdev in rpc.bdevs
    assert bdev in rpc.raids[stack.local_raid_name(node.uuid)]
    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_ONLINE
    # idempotent
    edge_cluster_ops.restart_device(cluster.uuid, node.uuid, "/dev/sdb1")


# -------------------------------------- monitor detection (force-detach)

def test_monitor_marks_detached_device_unavailable(env):
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    rpc.detach_backing_device(bdev)

    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))

    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_UNAVAILABLE
    assert _part(cluster, node, "/dev/sdc1").status == EdgePartition.STATUS_ONLINE
    # node itself keeps serving on the surviving member
    from simplyblock_core.models.cluster import Cluster
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_ACTIVE


def test_monitor_does_not_touch_offline_devices(env):
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    edge_cluster_ops.remove_device(cluster.uuid, node.uuid, "/dev/sdb1")

    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_OFFLINE


def test_unavailable_device_recovers_via_restart(env):
    """The e2e reattach flow: force-detach -> unavailable -> reattach EBS ->
    device restart -> online + raid member again."""
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    rpc.detach_backing_device(bdev)
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_UNAVAILABLE

    edge_cluster_ops.restart_device(cluster.uuid, node.uuid, "/dev/sdb1")

    assert _part(cluster, node, "/dev/sdb1").status == EdgePartition.STATUS_ONLINE
    assert bdev in rpc.raids[stack.local_raid_name(node.uuid)]


# ---------------------------------------------------- permanent replace

def test_permanent_replacement_of_unavailable_device(env):
    """Force-detach -> unavailable -> replace with a NEW volume (different
    path) via the replace task."""
    _, spdk, _ = env
    cluster, node = _cluster(spdk)
    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    rpc.detach_backing_device(bdev)
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))

    task_id = edge_cluster_ops.replace_device(cluster.uuid, node.uuid,
                                              "/dev/sdb1", "/dev/sdx1")
    from simplyblock_core.db_controller import DBController
    task = DBController().get_task_by_id(task_id)
    result = edge_cluster_ops.handle_device_replace_task(task)
    assert result.kind == 'done'

    fresh = _part(cluster, node, "/dev/sdx1")
    assert fresh.status == EdgePartition.STATUS_ONLINE
    assert bdev in rpc.raids[stack.local_raid_name(node.uuid)]
    assert rpc.called("bdev_aio_create")[-1][1]["filename"] == "/dev/sdx1"
