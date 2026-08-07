# coding=utf-8
"""End-to-end edge-cluster lifecycle against real FoundationDB (v3
active/active). Real: record persistence, prefix reads, atomic_update CAS,
JobSchedule integration, monitor sweep, task runner. Faked: SPDK proxies and
the edge k8s API (same split as the rest of the tier).

Flow: create cluster -> 2 nodes (active/active stores) -> volumes on both
stores -> owner outage (monitor degrades + enqueues fail-over) -> survivor
promotes the secondary lvstore instance -> owner returns (restart task
reassembles, resyncs, port-fenced fail-back) -> cluster active, leadership
home.
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


def _leader_of(cluster_id, lvs):
    return next((n for n in edge_db.get_edge_nodes(cluster_id)
                 if lvs in n.leader_of), None)


def test_full_lifecycle(db, spdk, fake_k8s):
    cluster = edge_cluster_ops.create_edge_cluster("edge-it")
    assert db.get_cluster_by_id(cluster.uuid).cluster_type == Cluster.TYPE_EDGE

    node_a = edge_cluster_ops.add_edge_node(
        cluster.uuid, "worker-1", "10.0.0.1", ["/dev/sdb1"], spdk_cpus=2)
    node_b = edge_cluster_ops.add_edge_node(
        cluster.uuid, "worker-2", "10.0.0.2", ["/dev/sdb1", "/dev/sdc1"])
    assert db.get_cluster_by_id(cluster.uuid).status == Cluster.STATUS_ACTIVE

    # active/active: each node owns + leads its store (persisted)
    lvs_a, lvs_b = stack.lvs_name(node_a.uuid), stack.lvs_name(node_b.uuid)
    assert _leader_of(cluster.uuid, lvs_a).uuid == node_a.uuid
    assert _leader_of(cluster.uuid, lvs_b).uuid == node_b.uuid
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_a.uuid).spdk_cpus == 2

    volumes = [edge_cluster_ops.create_volume(cluster.uuid, f"pvc-{i}", 5 * 1024 ** 3)
               for i in range(2)]
    assert {v.home_node_id for v in volumes} == {node_a.uuid, node_b.uuid}

    for volume in volumes:
        info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
        assert len(info) == 2 and info[0]["active"]

    # --- owner outage --------------------------------------------------------
    fake_k8s.running["worker-1"] = False
    monitor = _monitor()
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_DEGRADED
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_a.uuid).status == \
        EdgeNode.STATUS_OFFLINE

    # fail-over task enqueued and processed by the runner
    runner = EdgeTaskRunner(db, sleep=lambda _s: None)
    runner.run_cycle()
    assert _leader_of(cluster.uuid, lvs_a).uuid == node_b.uuid
    rpc_b = spdk.for_ip("10.0.0.2")
    assert rpc_b.lvstores[lvs_a]["leader"] is True

    # --- owner returns: restart task reassembles + fails back ---------------
    fake_k8s.running["worker-1"] = True
    spdk.for_ip("10.0.0.1").reset()
    monitor.check_cluster(db.get_cluster_by_id(cluster.uuid))
    restarts = [t for t in db.get_job_tasks(cluster.uuid)
                if t.function_name == JobSchedule.FN_EDGE_NODE_RESTART]
    assert len(restarts) == 1

    runner.run_cycle()
    task = db.get_task_by_id(restarts[0].uuid)
    assert task.status == JobSchedule.STATUS_DONE
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_a.uuid).status == \
        EdgeNode.STATUS_ONLINE
    # leadership home, fence used, survivor released
    assert _leader_of(cluster.uuid, lvs_a).uuid == node_a.uuid
    assert rpc_b.called("nvmf_port_block")
    assert not getattr(rpc_b, "blocked_ports", set())
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_ACTIVE


def test_volume_records_survive_and_are_prefix_scoped(db, spdk, fake_k8s):
    cluster_a = edge_cluster_ops.create_edge_cluster("edge-a")
    cluster_b = edge_cluster_ops.create_edge_cluster("edge-b")
    edge_cluster_ops.add_edge_node(cluster_a.uuid, "wa", "10.0.0.1", ["/dev/sdb1"])
    edge_cluster_ops.add_edge_node(cluster_b.uuid, "wb", "10.0.1.1", ["/dev/sdb1"])
    edge_cluster_ops.create_volume(cluster_a.uuid, "vol-a", 1024 ** 3)
    edge_cluster_ops.create_volume(cluster_b.uuid, "vol-b", 1024 ** 3)

    assert [v.volume_name for v in edge_db.get_edge_volumes(cluster_a.uuid)] == ["vol-a"]
    assert [v.volume_name for v in edge_db.get_edge_volumes(cluster_b.uuid)] == ["vol-b"]

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
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_SUSPENDED
    assert [t for t in db.get_job_tasks(cluster.uuid)
            if t.function_name == JobSchedule.FN_EDGE_NODE_RESTART] == []

    edge_cluster_ops.restart_node(cluster.uuid, node.uuid)
    EdgeTaskRunner(db, sleep=lambda _s: None).run_cycle()
    assert edge_db.get_edge_node_by_id(cluster.uuid, node.uuid).status == \
        EdgeNode.STATUS_ONLINE
    assert monitor.check_cluster(db.get_cluster_by_id(cluster.uuid)) == \
        Cluster.STATUS_ACTIVE
