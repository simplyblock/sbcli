# coding=utf-8
"""Unit tests for the product-native fail-over/fail-back (spec §5.6-5.7):
secondary lvstore promotion via update+set_leader with ANA flips, port-fenced
fail-back, and crypto volumes across both nodes."""
import pytest

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_lib.tasks.runner import TaskResult
from simplyblock_edge import db as edge_db, edge_cluster_ops, stack
from simplyblock_edge.models import EdgeNode
from simplyblock_edge.services.edge_monitor import EdgeMonitor


@pytest.fixture()
def env(kv, spdk, fake_k8s):
    return kv, spdk, fake_k8s


def _two_node_cluster(spdk, crypto=False):
    cluster = edge_cluster_ops.create_edge_cluster("edge-fo")
    node_a = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                            ["/dev/sdb1"])
    node_b = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-2", "10.0.0.2",
                                            ["/dev/sdb1"])
    volumes = [edge_cluster_ops.create_volume(cluster.uuid, "vol-1", 1024 ** 3,
                                              crypto=crypto),
               edge_cluster_ops.create_volume(cluster.uuid, "vol-2", 1024 ** 3,
                                              crypto=crypto)]
    return cluster, node_a, node_b, volumes


def _set_status(node, status):
    def _mutate(fresh):
        fresh.status = status
        return True
    edge_db.atomic_update(node, _mutate)
    node.status = status


def _monitor():
    return EdgeMonitor("edge-monitor-test", interval_sec=0, sleep=lambda _s: None)


def _failover_tasks(cluster_id):
    return [t for t in DBController().get_job_tasks(cluster_id)
            if t.function_name == JobSchedule.FN_EDGE_FAILOVER]


def _leader_of(cluster_id, lvs):
    return next((n for n in edge_db.get_edge_nodes(cluster_id)
                 if lvs in n.leader_of), None)


def _ana(rpc, volume, ip):
    subsystem = rpc.subsystems[volume.nqn]
    return next(la["ana_state"] for la in subsystem["listen_addresses"]
                if la["traddr"] == ip)


# --------------------------------------------------------------- failover

def test_monitor_enqueues_per_store_failover(env):
    _, spdk, fake_k8s = env
    cluster, node_a, node_b, _ = _two_node_cluster(spdk)
    fake_k8s.running["worker-1"] = False

    monitor = _monitor()
    monitor.check_cluster(edge_db.get_cluster(cluster.uuid))
    monitor.check_cluster(edge_db.get_cluster(cluster.uuid))  # dedupe

    tasks = _failover_tasks(cluster.uuid)
    assert len(tasks) == 1
    assert tasks[0].node_id == node_b.uuid
    assert tasks[0].function_params == {"lvs": stack.lvs_name(node_a.uuid)}


def test_monitor_no_failover_without_survivor(env):
    _, spdk, fake_k8s = env
    cluster, *_ = _two_node_cluster(spdk)
    fake_k8s.unreachable = True
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    assert _failover_tasks(cluster.uuid) == []


def test_failover_promotes_secondary_instance(env):
    _, spdk, fake_k8s = env
    cluster, node_a, node_b, volumes = _two_node_cluster(spdk)
    lvs_a = stack.lvs_name(node_a.uuid)
    fake_k8s.running["worker-1"] = False
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    task = _failover_tasks(cluster.uuid)[0]

    result = edge_cluster_ops.handle_failover_task(task)
    assert result.kind == TaskResult.DONE

    rpc_b = spdk.for_ip("10.0.0.2")
    # promotion = update (refresh in-memory metadata) THEN leadership
    assert any(c[1]["lvs"] == lvs_a for c in rpc_b.called("bdev_lvol_update_lvstore"))
    assert rpc_b.lvstores[lvs_a]["leader"] is True
    # the survivor's paths for store-A volumes flipped to optimized
    for volume in volumes:
        if volume.home_node_id == node_a.uuid:
            assert _ana(rpc_b, volume, "10.0.0.2") == "optimized"
    # records: survivor leads BOTH stores now
    assert sorted(_leader_of(cluster.uuid, lvs_a).leader_of) == \
        sorted([lvs_a, stack.lvs_name(node_b.uuid)])
    # idempotent
    assert edge_cluster_ops.handle_failover_task(task).kind == TaskResult.DONE


def test_failover_retries_until_survivor_online(env):
    _, spdk, _ = env
    cluster, node_a, node_b, _ = _two_node_cluster(spdk)
    _set_status(node_a, EdgeNode.STATUS_OFFLINE)
    _set_status(node_b, EdgeNode.STATUS_OFFLINE)
    task_id = edge_cluster_ops.add_edge_task(
        JobSchedule.FN_EDGE_FAILOVER, cluster.uuid, node_b.uuid,
        params={"lvs": stack.lvs_name(node_a.uuid)})
    task = DBController().get_task_by_id(task_id)
    assert edge_cluster_ops.handle_failover_task(task).kind == TaskResult.RETRY


def test_failover_aborts_when_owner_recovered(env):
    _, spdk, _ = env
    cluster, node_a, node_b, _ = _two_node_cluster(spdk)
    task_id = edge_cluster_ops.add_edge_task(
        JobSchedule.FN_EDGE_FAILOVER, cluster.uuid, node_b.uuid,
        params={"lvs": stack.lvs_name(node_a.uuid)})
    result = edge_cluster_ops.handle_failover_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE
    assert "recovered" in result.message
    assert _leader_of(cluster.uuid, stack.lvs_name(node_a.uuid)).uuid == node_a.uuid


# --------------------------------------------------------------- fail-back

def _take_over(spdk, fake_k8s, cluster, dead, survivor):
    fake_k8s.running[dead.hostname] = False
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    task = _failover_tasks(cluster.uuid)[0]
    assert edge_cluster_ops.handle_failover_task(task).kind == TaskResult.DONE


def test_failback_on_owner_restart(env):
    _, spdk, fake_k8s = env
    cluster, node_a, node_b, volumes = _two_node_cluster(spdk)
    lvs_a = stack.lvs_name(node_a.uuid)
    port_a = stack.store_client_port(node_a.nvmf_port, 0)
    _take_over(spdk, fake_k8s, cluster, node_a, node_b)

    # node A's pod returns empty; the restart task reassembles + fails back.
    spdk.for_ip("10.0.0.1").reset()
    fake_k8s.running["worker-1"] = True
    _set_status(node_a, EdgeNode.STATUS_OFFLINE)
    task_id = edge_cluster_ops.add_edge_task(
        JobSchedule.FN_EDGE_NODE_RESTART, cluster.uuid, node_a.uuid)
    result = edge_cluster_ops.handle_node_restart_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE

    rpc_a, rpc_b = spdk.for_ip("10.0.0.1"), spdk.for_ip("10.0.0.2")
    # the fence: port block + unblock around the handover on the survivor
    assert rpc_b.called("nvmf_port_block")[0][1]["port"] == port_a
    assert rpc_b.called("nvmf_port_unblock")[0][1]["port"] == port_a
    assert not getattr(rpc_b, "blocked_ports", set())
    # leadership handed home: released on B (bs_nonleadership), taken on A
    release = [c for c in rpc_b.called("bdev_lvol_set_leader") if c[1]["lvs"] == lvs_a]
    assert release[-1][1] == {"lvs": lvs_a, "leader": False, "bs_nonleadership": True}
    assert any(c[1]["lvs"] == lvs_a for c in rpc_a.called("bdev_lvol_update_lvstore"))
    assert rpc_a.lvstores[lvs_a]["leader"] is True
    # ANA flipped back for store-A volumes
    for volume in volumes:
        if volume.home_node_id == node_a.uuid:
            assert _ana(rpc_a, volume, "10.0.0.1") == "optimized"
            assert _ana(rpc_b, volume, "10.0.0.2") == "non_optimized"
    # records
    assert _leader_of(cluster.uuid, lvs_a).uuid == node_a.uuid
    fresh_b = edge_db.get_edge_node_by_id(cluster.uuid, node_b.uuid)
    assert fresh_b.leader_of == [stack.lvs_name(node_b.uuid)]
    assert edge_db.get_edge_node_by_id(cluster.uuid, node_a.uuid).status == \
        EdgeNode.STATUS_ONLINE


def test_restart_without_takeover_resumes_own_leadership(env):
    """Restart wins the race against fail-over: the returning node must
    re-take SPDK-side leadership of its own store (it never lost it in the
    records) and flip its paths back to optimized."""
    _, spdk, fake_k8s = env
    cluster, node_a, node_b, volumes = _two_node_cluster(spdk)
    lvs_a = stack.lvs_name(node_a.uuid)

    spdk.for_ip("10.0.0.1").reset()
    _set_status(node_a, EdgeNode.STATUS_OFFLINE)
    task_id = edge_cluster_ops.add_edge_task(
        JobSchedule.FN_EDGE_NODE_RESTART, cluster.uuid, node_a.uuid)
    result = edge_cluster_ops.handle_node_restart_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE

    rpc_a = spdk.for_ip("10.0.0.1")
    assert rpc_a.lvstores[lvs_a]["leader"] is True
    for volume in volumes:
        if volume.home_node_id == node_a.uuid:
            assert _ana(rpc_a, volume, "10.0.0.1") == "optimized"
    # no port fence needed in this path
    assert not spdk.for_ip("10.0.0.2").called("nvmf_port_block")


# ------------------------------------------------------------------ crypto

def test_crypto_volume_exists_on_both_nodes(env):
    kv, spdk, _ = env
    cluster, node_a, node_b, volumes = _two_node_cluster(spdk, crypto=True)
    for volume in volumes:
        for ip in ("10.0.0.1", "10.0.0.2"):
            rpc = spdk.for_ip(ip)
            # key registered + crypto bdev over the (created or registered) lvol
            assert stack.crypto_key_name(volume.uuid) in rpc.crypto_keys
            assert volume.crypto_bdev in rpc.bdevs
            assert rpc.subsystems[volume.nqn]["namespaces"][0]["bdev_name"] == \
                volume.crypto_bdev
        dek_key = f"keys/{stack.volume_dek_path(cluster.uuid, volume.uuid)}".encode()
        assert kv.get(dek_key)


def test_crypto_volume_delete_removes_keys(env):
    kv, spdk, _ = env
    cluster, node_a, node_b, volumes = _two_node_cluster(spdk, crypto=True)
    volume = volumes[0]
    edge_cluster_ops.delete_volume(cluster.uuid, volume.uuid)
    dek_key = f"keys/{stack.volume_dek_path(cluster.uuid, volume.uuid)}".encode()
    assert kv.get(dek_key) is None
