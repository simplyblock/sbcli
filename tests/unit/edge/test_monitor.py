# coding=utf-8
"""Unit tests for the edge monitor sweep (spec §6-7)."""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_edge import db as edge_db, edge_cluster_ops
from simplyblock_edge.models import EdgeNode
from simplyblock_edge.services.edge_monitor import EdgeMonitor, probe_node


@pytest.fixture()
def env(kv, spdk, fake_k8s):
    return kv, spdk, fake_k8s


def _cluster_with_nodes(spdk):
    cluster = edge_cluster_ops.create_edge_cluster("edge-1")
    n1 = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1", ["/dev/sdb1"])
    n2 = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-2", "10.0.0.2", ["/dev/sdb1"])
    return cluster, n1, n2


def _monitor():
    return EdgeMonitor("edge-monitor-test", interval_sec=0, sleep=lambda _s: None)


def _fresh(cluster, node):
    return edge_db.get_edge_node_by_id(cluster.uuid, node.uuid)


def test_probe_all_good(env):
    _, spdk, fake_k8s = env
    cluster, n1, _ = _cluster_with_nodes(spdk)
    probe = probe_node(cluster, n1)
    assert (probe.k8s_reachable, probe.node_ready, probe.pod_running, probe.rpc_alive) == \
        (True, True, True, True)


def test_probe_maps_apiserver_outage(env):
    _, spdk, fake_k8s = env
    cluster, n1, _ = _cluster_with_nodes(spdk)
    fake_k8s.unreachable = True
    probe = probe_node(cluster, n1)
    assert not probe.k8s_reachable


def test_probe_dead_rpc(env):
    _, spdk, fake_k8s = env
    cluster, n1, _ = _cluster_with_nodes(spdk)
    spdk.for_ip("10.0.0.1").alive = False
    probe = probe_node(cluster, n1)
    assert probe.pod_running and not probe.rpc_alive


def test_healthy_sweep_keeps_cluster_active(env):
    _, spdk, _ = env
    cluster, _, _ = _cluster_with_nodes(spdk)
    assert _monitor().check_cluster(cluster) == Cluster.STATUS_ACTIVE
    assert DBController().get_job_tasks(cluster.uuid) == []


def test_one_node_offline_degrades_cluster(env):
    _, spdk, fake_k8s = env
    cluster, n1, n2 = _cluster_with_nodes(spdk)
    fake_k8s.running["worker-2"] = False

    assert _monitor().check_cluster(cluster) == Cluster.STATUS_DEGRADED
    assert _fresh(cluster, n2).status == EdgeNode.STATUS_OFFLINE
    assert _fresh(cluster, n1).status == EdgeNode.STATUS_ONLINE
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_DEGRADED


def test_all_nodes_out_suspends_cluster(env):
    _, spdk, fake_k8s = env
    cluster, n1, n2 = _cluster_with_nodes(spdk)
    fake_k8s.unreachable = True

    assert _monitor().check_cluster(cluster) == Cluster.STATUS_SUSPENDED
    assert _fresh(cluster, n1).status == EdgeNode.STATUS_UNREACHABLE
    assert _fresh(cluster, n2).status == EdgeNode.STATUS_UNREACHABLE
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_SUSPENDED


def test_returned_node_gets_restart_task_not_instant_online(env):
    _, spdk, fake_k8s = env
    cluster, n1, n2 = _cluster_with_nodes(spdk)
    monitor = _monitor()

    fake_k8s.running["worker-2"] = False
    monitor.check_cluster(cluster)
    assert _fresh(cluster, n2).status == EdgeNode.STATUS_OFFLINE

    # Pod comes back: the node must NOT flip straight to online — a
    # reassembly task is enqueued instead (deduped across sweeps).
    fake_k8s.running["worker-2"] = True
    monitor.check_cluster(cluster)
    monitor.check_cluster(cluster)
    assert _fresh(cluster, n2).status == EdgeNode.STATUS_OFFLINE

    tasks = DBController().get_job_tasks(cluster.uuid)
    assert len(tasks) == 1
    assert tasks[0].function_name == JobSchedule.FN_EDGE_NODE_RESTART
    assert tasks[0].node_id == n2.uuid


def test_down_node_is_never_touched_or_restarted(env):
    _, spdk, fake_k8s = env
    cluster, n1, n2 = _cluster_with_nodes(spdk)
    edge_cluster_ops.shutdown_node(cluster.uuid, n2.uuid)

    status = _monitor().check_cluster(cluster)
    assert _fresh(cluster, n2).status == EdgeNode.STATUS_DOWN
    assert status == Cluster.STATUS_DEGRADED
    assert DBController().get_job_tasks(cluster.uuid) == []


def test_tick_isolates_broken_cluster(env):
    """A failing cluster sweep must not prevent the other clusters' sweep."""
    kv, spdk, fake_k8s = env
    cluster, _, _ = _cluster_with_nodes(spdk)
    broken = edge_cluster_ops.create_edge_cluster("edge-broken")

    monitor = _monitor()
    original = monitor.check_cluster

    def exploding(c):
        if c.uuid == broken.uuid:
            raise RuntimeError("boom")
        return original(c)

    monitor.check_cluster = exploding
    assert monitor.tick() is True  # not everything active
    assert edge_db.get_cluster(cluster.uuid).status == Cluster.STATUS_ACTIVE
