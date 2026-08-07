# coding=utf-8
"""Unit tests for the edge task handlers + runner dispatch (spec §5.5, §5.7)."""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.tasks.runner import TaskResult
from simplyblock_edge import db as edge_db, edge_cluster_ops, stack
from simplyblock_edge.models import EdgeNode, EdgePartition
from simplyblock_edge.services.tasks_runner_edge import EdgeTaskRunner


@pytest.fixture()
def env(kv, spdk, fake_k8s):
    return kv, spdk, fake_k8s


def _two_node_cluster(spdk):
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node_a = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                            ["/dev/sdb1"])
    node_b = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-2", "10.0.0.2",
                                            ["/dev/sdb1", "/dev/sdc1"])
    return cluster, node_a, node_b


def _task(cluster, node, fn=JobSchedule.FN_EDGE_NODE_RESTART, params=None):
    task_id = edge_cluster_ops.add_edge_task(fn, cluster.uuid, node.uuid, params=params)
    return DBController().get_task_by_id(task_id)


def _set_status(node, status):
    def _mutate(fresh):
        fresh.status = status
        return True
    edge_db.atomic_update(node, _mutate)
    node.status = status


# ------------------------------------------------------------- node restart

def test_node_restart_rebuilds_stack_and_readds_legs(env):
    kv, spdk, _ = env
    cluster, node_a, node_b = _two_node_cluster(spdk)

    # node B's pod restarted: SPDK state gone; A's raids dropped B's legs.
    rpc_b = spdk.for_ip("10.0.0.2")
    rpc_b.reset()
    rpc_a = spdk.for_ip("10.0.0.1")
    for raid, leg in ((stack.mirror_name(node_a.uuid), stack.remote_half_bdev(node_b.uuid, 2)),
                      (stack.mirror_name(node_b.uuid), stack.remote_half_bdev(node_b.uuid, 1))):
        if leg in rpc_a.raids.get(raid, []):
            rpc_a.raids[raid].remove(leg)
        rpc_a.bdevs.discard(leg)
    _set_status(node_b, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, node_b))
    assert result.kind == TaskResult.DONE

    # local stack + split + both halves exported again
    assert stack.local_raid_name(node_b.uuid) in rpc_b.raids
    repl = rpc_b.subsystems[stack.repl_nqn(cluster.nqn, node_b.uuid)]
    assert len(repl["namespaces"]) == 2
    # B's legs re-added into BOTH of A's raid instances
    assert stack.remote_half_bdev(node_b.uuid, 2) in rpc_a.raids[stack.mirror_name(node_a.uuid)]
    assert stack.remote_half_bdev(node_b.uuid, 1) in rpc_a.raids[stack.mirror_name(node_b.uuid)]
    # B re-instantiated both stores locally (its own + secondary of A's)
    assert stack.mirror_name(node_b.uuid) in rpc_b.raids
    assert stack.mirror_name(node_a.uuid) in rpc_b.raids
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_b.uuid).status == \
        EdgeNode.STATUS_ONLINE


def test_restart_task_on_down_node_is_a_noop(env):
    kv, spdk, _ = env
    cluster, node_a, _ = _two_node_cluster(spdk)
    _set_status(node_a, EdgeNode.STATUS_DOWN)
    calls_before = len(spdk.for_ip("10.0.0.1").calls)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, node_a))
    assert result.kind == TaskResult.DONE
    assert "down" in result.message
    assert len(spdk.for_ip("10.0.0.1").calls) == calls_before


def test_restart_failure_retries_and_returns_node_offline(env):
    kv, spdk, _ = env
    cluster, node_a, node_b = _two_node_cluster(spdk)
    rpc_b = spdk.for_ip("10.0.0.2")
    rpc_b.reset()
    rpc_b.fail.add("bdev_aio_create")
    _set_status(node_b, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, node_b))
    assert result.kind == TaskResult.RETRY
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_b.uuid).status == \
        EdgeNode.STATUS_OFFLINE


def test_single_node_restart_reloads_lvstore(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1"])
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    rpc = spdk.for_ip("10.0.0.1")
    rpc.reset()
    _set_status(node, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, node))
    assert result.kind == TaskResult.DONE
    assert rpc.called("bdev_examine")[0][1]["name"] == stack.aio_bdev_name(node.uuid, 0)
    assert rpc.subsystems[volume.nqn]["listen_addresses"][0]["ana_state"] == "optimized"


# ------------------------------------------------------------ device tasks

def test_device_replace_handler(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1", "/dev/sdc1"])
    task_id = edge_cluster_ops.replace_device(cluster.uuid, node.uuid,
                                              "/dev/sdb1", "/dev/sdz1")
    result = edge_cluster_ops.handle_device_replace_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE

    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    assert rpc.called("bdev_aio_create")[-1][1]["filename"] == "/dev/sdz1"
    assert bdev in rpc.raids[stack.local_raid_name(node.uuid)]
    fresh = edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)
    assert fresh.partitions[0].device_path == "/dev/sdz1"
    assert fresh.partitions[0].status == EdgePartition.STATUS_ONLINE


def test_device_replace_failure_retries(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1", "/dev/sdc1"])
    task_id = edge_cluster_ops.replace_device(cluster.uuid, node.uuid,
                                              "/dev/sdb1", "/dev/sdz1")
    spdk.for_ip("10.0.0.1").fail.add("bdev_aio_create")
    result = edge_cluster_ops.handle_device_replace_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.RETRY


def test_device_add_handler_grows_raid5(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1", "/dev/sdc1", "/dev/sdd1"])
    task_id = edge_cluster_ops.add_device(cluster.uuid, node.uuid, "/dev/sde1")
    result = edge_cluster_ops.handle_device_add_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE

    rpc = spdk.for_ip("10.0.0.1")
    assert stack.aio_bdev_name(node.uuid, 3) in rpc.raids[stack.local_raid_name(node.uuid)]


def test_runner_dispatch(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1"])
    _set_status(node, EdgeNode.STATUS_OFFLINE)
    edge_cluster_ops.add_edge_task(JobSchedule.FN_EDGE_NODE_RESTART,
                                   cluster.uuid, node.uuid)

    runner = EdgeTaskRunner(DBController(), sleep=lambda _s: None)
    runner.run_cycle()

    tasks = DBController().get_job_tasks(cluster.uuid)
    assert len(tasks) == 1
    assert tasks[0].status == JobSchedule.STATUS_DONE
    assert "online" in tasks[0].function_result
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == \
        EdgeNode.STATUS_ONLINE
