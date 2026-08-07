# coding=utf-8
"""End-to-end edge-cluster lifecycle against real FoundationDB.

Everything above the DB is exercised for real (record persistence, cluster-
prefixed range reads, atomic_update CAS, JobSchedule integration, the monitor
sweep and the task runner); only the node side (SPDK proxy, edge k8s API) is
faked — the same split every other integration test uses.

Flow: create cluster -> add two nodes -> create volume -> connect info ->
secondary outage (monitor degrades) -> pod returns (restart task enqueued) ->
task runner reassembles -> cluster active again.
"""
import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_edge import db as edge_db, edge_cluster_ops, stack
from simplyblock_edge.models import EdgeNode
from simplyblock_edge.services.edge_monitor import EdgeMonitor
from simplyblock_edge.services.tasks_runner_edge import EdgeTaskRunner


@pytest.fixture(autouse=True)
def _clean_keyspace(db):
    db.kv_store.clear_range(b"\x00", b"\xff")
    yield


def _monitor():
    return EdgeMonitor("edge-monitor-it", interval_sec=0, sleep=lambda _s: None)


def test_full_lifecycle(db, spdk, fake_k8s):
    # --- create + populate --------------------------------------------------
    cluster = edge_cluster_ops.create_edge_cluster("edge-it")
    assert db.get_cluster_by_id(cluster.uuid).cluster_type == Cluster.TYPE_EDGE

    primary = edge_cluster_ops.add_edge_node(
        cluster.uuid, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    secondary = edge_cluster_ops.add_edge_node(
        cluster.uuid, "worker-2", "10.0.0.2", ["/dev/sdb1", "/dev/sdc1"])

    nodes = edge_db.get_edge_nodes(cluster.uuid)
    assert {n.hostname for n in nodes} == {"worker-1", "worker-2"}
    assert db.get_cluster_by_id(cluster.uuid).status == Cluster.STATUS_ACTIVE

    # lvstore was created on the mirror at second-node add
    mirror = stack.mirror_name(cluster.uuid)
    assert edge_db.get_edge_node_by_id(cluster.uuid, primary.uuid).lvstore_base == mirror
    assert spdk.for_ip("10.0.0.1").lvstores[stack.lvs_name(cluster.uuid)] == mirror

    # --- volume ---------------------------------------------------------------
    volume = edge_cluster_ops.create_volume(cluster.uuid, "pvc-1", 5 * 1024 ** 3)
    persisted = edge_db.get_edge_volume_by_id(cluster.uuid, volume.uuid)
    assert persisted.nqn == stack.volume_nqn(cluster.nqn, volume.uuid)

    info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
    assert info[0]["ip"] == "10.0.0.1"
    assert info[0]["nqn"] == volume.nqn

    # --- outage: secondary pod dies ------------------------------------------
    fake_k8s.running["worker-2"] = False
    monitor = _monitor()
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_DEGRADED
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).status == \
        EdgeNode.STATUS_OFFLINE

    # --- pod returns: monitor enqueues reassembly, does NOT flip online ------
    fake_k8s.running["worker-2"] = True
    spdk.for_ip("10.0.0.2").reset()  # pod restart lost all SPDK state
    monitor.check_cluster(db.get_cluster_by_id(cluster.uuid))
    tasks = db.get_job_tasks(cluster.uuid)
    restarts = [t for t in tasks if t.function_name == JobSchedule.FN_EDGE_NODE_RESTART]
    assert len(restarts) == 1
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).status == \
        EdgeNode.STATUS_OFFLINE

    # --- task runner reassembles the node ------------------------------------
    runner = EdgeTaskRunner(db, sleep=lambda _s: None)
    runner.run_cycle()

    task = db.get_task_by_id(restarts[0].uuid)
    assert task.status == JobSchedule.STATUS_DONE
    assert "online" in task.function_result
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).status == \
        EdgeNode.STATUS_ONLINE
    # secondary stack rebuilt + its leg back in the primary's mirror
    assert stack.repl_nqn(cluster.nqn, secondary.uuid) in \
        spdk.for_ip("10.0.0.2").subsystems
    assert stack.remote_leg_bdev(secondary.uuid) in \
        spdk.for_ip("10.0.0.1").raids[mirror]

    # --- monitor confirms recovery -------------------------------------------
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_ACTIVE
    assert db.get_cluster_by_id(cluster.uuid).status == Cluster.STATUS_ACTIVE


def test_volume_records_survive_and_are_prefix_scoped(db, spdk, fake_k8s):
    """Two clusters' records never leak into each other's range reads."""
    cluster_a = edge_cluster_ops.create_edge_cluster("edge-a")
    cluster_b = edge_cluster_ops.create_edge_cluster("edge-b")
    edge_cluster_ops.add_edge_node(cluster_a.uuid, "wa", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.add_edge_node(cluster_b.uuid, "wb", "10.0.1.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster_a.uuid, "vol-a", 1024 ** 3)
    edge_cluster_ops.create_volume(cluster_b.uuid, "vol-b", 1024 ** 3)

    assert [v.volume_name for v in edge_db.get_edge_volumes(cluster_a.uuid)] == ["vol-a"]
    assert [v.volume_name for v in edge_db.get_edge_volumes(cluster_b.uuid)] == ["vol-b"]
    assert len(edge_db.get_edge_nodes(cluster_a.uuid)) == 1

    edge_cluster_ops.delete_volume(cluster_a.uuid, edge_db.get_edge_volumes(
        cluster_a.uuid)[0].uuid)
    assert edge_db.get_edge_volumes(cluster_a.uuid) == []
    assert [v.volume_name for v in edge_db.get_edge_volumes(cluster_b.uuid)] == ["vol-b"]


def test_admin_shutdown_is_sticky_across_sweeps(db, spdk, fake_k8s):
    cluster = edge_cluster_ops.create_edge_cluster("edge-it")
    node = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                          ["/dev/sdb1"])
    edge_cluster_ops.shutdown_node(cluster.uuid, node.uuid)

    monitor = _monitor()
    status = monitor.check_cluster(db.get_cluster_by_id(cluster.uuid))
    assert status == Cluster.STATUS_SUSPENDED
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == \
        EdgeNode.STATUS_DOWN
    assert [t for t in db.get_job_tasks(cluster.uuid)
            if t.function_name == JobSchedule.FN_EDGE_NODE_RESTART] == []

    # Explicit admin restart is the way back.
    edge_cluster_ops.restart_node(cluster.uuid, node.uuid)
    EdgeTaskRunner(db, sleep=lambda _s: None).run_cycle()
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == \
        EdgeNode.STATUS_ONLINE
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_ACTIVE
