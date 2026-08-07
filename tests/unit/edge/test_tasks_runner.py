# coding=utf-8
"""Unit tests for the edge task handlers + runner dispatch (spec §5.5-5.6)."""
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
    primary = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                             ["/dev/sdb1"])
    secondary = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-2", "10.0.0.2",
                                               ["/dev/sdb1", "/dev/sdc1"])
    return cluster, primary, secondary


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

def test_secondary_restart_rebuilds_and_readds_mirror_leg(env):
    kv, spdk, _ = env
    cluster, primary, secondary = _two_node_cluster(spdk)

    # Simulate: secondary pod restarted (SPDK state gone), raid leg dropped.
    secondary_rpc = spdk.for_ip("10.0.0.2")
    secondary_rpc.reset()
    primary_rpc = spdk.for_ip("10.0.0.1")
    mirror = stack.mirror_name(cluster.uuid)
    leg = stack.remote_leg_bdev(secondary.uuid)
    primary_rpc.raids[mirror].remove(leg)
    primary_rpc.bdevs.discard(leg)
    _set_status(secondary, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, secondary))

    assert result.kind == TaskResult.DONE
    # local stack + repl subsystem rebuilt on the secondary
    assert stack.local_raid_name(secondary.uuid) in secondary_rpc.raids
    assert stack.repl_nqn(cluster.nqn, secondary.uuid) in secondary_rpc.subsystems
    # remote leg re-attached + re-added into the primary's mirror
    assert leg in primary_rpc.raids[mirror]
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).status == \
        EdgeNode.STATUS_ONLINE


def test_secondary_restart_tolerates_leg_never_dropped(env):
    """If the nvme controller auto-reconnected and the raid kept the leg,
    re-adding must not fail the task."""
    kv, spdk, _ = env
    cluster, primary, secondary = _two_node_cluster(spdk)
    spdk.for_ip("10.0.0.2").reset()
    _set_status(secondary, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, secondary))
    assert result.kind == TaskResult.DONE


def test_primary_restart_reloads_lvstore_and_republishes_volumes(env):
    kv, spdk, _ = env
    cluster, primary, secondary = _two_node_cluster(spdk)
    volume = edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)

    primary_rpc = spdk.for_ip("10.0.0.1")
    primary_rpc.reset()
    _set_status(primary, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, primary))

    assert result.kind == TaskResult.DONE
    mirror = stack.mirror_name(cluster.uuid)
    # mirror reassembled and examined (lvstore load)
    assert mirror in primary_rpc.raids
    assert primary_rpc.called("bdev_examine")[0][1]["name"] == mirror
    # client subsystem republished with ns + listener
    subsystem = primary_rpc.subsystems[volume.nqn]
    assert subsystem["namespaces"][0]["bdev_name"] == volume.lvol_bdev
    assert subsystem["listen_addresses"][0]["trsvcid"] == "4420"


def test_restart_task_on_down_node_is_a_noop(env):
    kv, spdk, _ = env
    cluster, primary, _ = _two_node_cluster(spdk)
    _set_status(primary, EdgeNode.STATUS_DOWN)
    calls_before = len(spdk.for_ip("10.0.0.1").calls)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, primary))
    assert result.kind == TaskResult.DONE
    assert "down" in result.message
    assert len(spdk.for_ip("10.0.0.1").calls) == calls_before
    assert edge_db.get_edge_node_by_id(cluster.uuid, primary.uuid).status == \
        EdgeNode.STATUS_DOWN


def test_restart_failure_retries_and_returns_node_offline(env):
    kv, spdk, _ = env
    cluster, primary, secondary = _two_node_cluster(spdk)
    secondary_rpc = spdk.for_ip("10.0.0.2")
    secondary_rpc.reset()
    secondary_rpc.fail.add("bdev_aio_create")
    _set_status(secondary, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, secondary))
    assert result.kind == TaskResult.RETRY
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).status == \
        EdgeNode.STATUS_OFFLINE


def test_single_node_restart_reloads_lvstore(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3)
    rpc = spdk.for_ip("10.0.0.1")
    rpc.reset()
    _set_status(node, EdgeNode.STATUS_OFFLINE)

    result = edge_cluster_ops.handle_node_restart_task(_task(cluster, node))
    assert result.kind == TaskResult.DONE
    # examined the lvstore base (the bare aio top) and republished the volume
    assert rpc.called("bdev_examine")[0][1]["name"] == stack.aio_bdev_name(node.uuid, 0)
    assert len(rpc.subsystems) == 2  # repl + volume subsystem


# ------------------------------------------------------------ device tasks

def test_device_replace_handler(env):
    kv, spdk, _ = env
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1", "/dev/sdc1"])
    task_id = edge_cluster_ops.replace_device(cluster.uuid, node.uuid,
                                              "/dev/sdb1", "/dev/sdz1")
    task = DBController().get_task_by_id(task_id)

    result = edge_cluster_ops.handle_device_replace_task(task)
    assert result.kind == TaskResult.DONE

    rpc = spdk.for_ip("10.0.0.1")
    bdev = stack.aio_bdev_name(node.uuid, 0)
    assert rpc.called("bdev_raid_remove_base_bdev")
    assert rpc.called("bdev_aio_delete")[0][1]["name"] == bdev
    # recreated from the new path and back in the local raid
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
    new_bdev = stack.aio_bdev_name(node.uuid, 3)
    assert new_bdev in rpc.raids[stack.local_raid_name(node.uuid)]
    fresh = edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)
    assert fresh.partitions[3].status == EdgePartition.STATUS_ONLINE


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
