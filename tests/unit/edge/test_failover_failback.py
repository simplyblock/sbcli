# coding=utf-8
"""Unit tests for lvstore fail-over/fail-back and crypto volumes (the spec
corrections of 2026-08-07: dynamic volumes over the lvstore, secondary
takeover, fail-back on primary restart, optional crypto bdevs with KMS keys).
"""
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


def _two_node_cluster(spdk, volume=True, crypto=False):
    cluster = edge_cluster_ops.create_edge_cluster("edge-fo")
    primary = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-1", "10.0.0.1",
                                             ["/dev/sdb1"])
    secondary = edge_cluster_ops.add_edge_node(cluster.uuid, "worker-2", "10.0.0.2",
                                               ["/dev/sdb1"])
    volumes = []
    if volume:
        volumes.append(edge_cluster_ops.create_volume(
            cluster.uuid, "vol-1", 1024 ** 3, crypto=crypto))
    return cluster, primary, secondary, volumes


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


def _host(cluster_id):
    nodes = edge_db.get_edge_nodes(cluster_id)
    return next((n for n in nodes if n.lvstore_base), None)


# --------------------------------------------------------- passive paths

def test_volume_create_publishes_passive_path_on_peer(env):
    _, spdk, _ = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk)
    passive = spdk.for_ip("10.0.0.2").subsystems[volume.nqn]
    assert passive["namespaces"] == []          # no ns until takeover
    assert passive["listen_addresses"][0]["traddr"] == "10.0.0.2"
    active = spdk.for_ip("10.0.0.1").subsystems[volume.nqn]
    assert active["namespaces"][0]["bdev_name"] == volume.lvol_bdev


def test_connect_info_returns_both_paths_active_first(env):
    _, spdk, _ = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk)
    info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
    assert [e["ip"] for e in info] == ["10.0.0.1", "10.0.0.2"]
    assert [e["active"] for e in info] == [True, False]
    assert all(e["nqn"] == volume.nqn for e in info)


# --------------------------------------------------------------- failover

def test_monitor_enqueues_failover_when_host_dies(env):
    _, spdk, fake_k8s = env
    cluster, primary, secondary, _ = _two_node_cluster(spdk)
    fake_k8s.running["worker-1"] = False

    monitor = _monitor()
    monitor.check_cluster(edge_db.get_cluster(cluster.uuid))
    monitor.check_cluster(edge_db.get_cluster(cluster.uuid))  # dedupe check

    tasks = _failover_tasks(cluster.uuid)
    assert len(tasks) == 1
    assert tasks[0].node_id == secondary.uuid


def test_monitor_no_failover_without_survivor(env):
    """Both nodes out -> nobody can take over -> no failover task (the
    cluster suspends instead). Single-node clusters are excluded by the
    2-node guard."""
    _, spdk, fake_k8s = env
    cluster, primary, secondary, _ = _two_node_cluster(spdk)
    fake_k8s.unreachable = True
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    assert _failover_tasks(cluster.uuid) == []


def test_failover_moves_lvstore_to_secondary(env):
    _, spdk, fake_k8s = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk)
    fake_k8s.running["worker-1"] = False
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    task = _failover_tasks(cluster.uuid)[0]

    result = edge_cluster_ops.handle_failover_task(task)
    assert result.kind == TaskResult.DONE

    mirror = stack.mirror_name(cluster.uuid)
    secondary_rpc = spdk.for_ip("10.0.0.2")
    # degraded mirror assembled on the secondary, volume served there
    assert mirror in secondary_rpc.raids
    served = secondary_rpc.subsystems[volume.nqn]
    assert served["namespaces"][0]["bdev_name"] == volume.lvol_bdev
    # records flipped
    host = _host(cluster.uuid)
    assert host.uuid == secondary.uuid
    assert edge_db.get_edge_node_by_id(cluster.uuid, primary.uuid).lvstore_base == ""
    # connect info now leads with the secondary
    info = edge_cluster_ops.get_connect_info(cluster.uuid, volume.uuid)
    assert info[0]["ip"] == "10.0.0.2" and info[0]["active"]

    # idempotent
    assert edge_cluster_ops.handle_failover_task(task).kind == TaskResult.DONE


def test_failover_retries_until_secondary_online(env):
    _, spdk, fake_k8s = env
    cluster, primary, secondary, _ = _two_node_cluster(spdk)
    _set_status(primary, EdgeNode.STATUS_OFFLINE)
    _set_status(secondary, EdgeNode.STATUS_OFFLINE)
    task_id = edge_cluster_ops.add_edge_task(JobSchedule.FN_EDGE_FAILOVER,
                                             cluster.uuid, secondary.uuid)
    task = DBController().get_task_by_id(task_id)
    assert edge_cluster_ops.handle_failover_task(task).kind == TaskResult.RETRY


def test_failover_aborts_when_primary_recovered(env):
    _, spdk, fake_k8s = env
    cluster, primary, secondary, _ = _two_node_cluster(spdk)
    task_id = edge_cluster_ops.add_edge_task(JobSchedule.FN_EDGE_FAILOVER,
                                             cluster.uuid, secondary.uuid)
    task = DBController().get_task_by_id(task_id)
    result = edge_cluster_ops.handle_failover_task(task)
    assert result.kind == TaskResult.DONE
    assert "recovered" in result.message
    assert _host(cluster.uuid).uuid == primary.uuid  # untouched


# --------------------------------------------------------------- fail-back

def test_failback_on_primary_restart(env):
    _, spdk, fake_k8s = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk)

    # takeover first
    fake_k8s.running["worker-1"] = False
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    edge_cluster_ops.handle_failover_task(_failover_tasks(cluster.uuid)[0])
    assert _host(cluster.uuid).uuid == secondary.uuid

    # primary pod returns empty; its restart task runs
    spdk.for_ip("10.0.0.1").reset()
    fake_k8s.running["worker-1"] = True
    _set_status(primary, EdgeNode.STATUS_OFFLINE)
    task_id = edge_cluster_ops.add_edge_task(
        JobSchedule.FN_EDGE_NODE_RESTART, cluster.uuid, primary.uuid)
    result = edge_cluster_ops.handle_node_restart_task(
        DBController().get_task_by_id(task_id))
    assert result.kind == TaskResult.DONE

    mirror = stack.mirror_name(cluster.uuid)
    primary_rpc = spdk.for_ip("10.0.0.1")
    secondary_rpc = spdk.for_ip("10.0.0.2")
    # lvstore is home again: mirror on the primary, active ns there
    assert mirror in primary_rpc.raids
    assert primary_rpc.subsystems[volume.nqn]["namespaces"][0]["bdev_name"] == \
        volume.lvol_bdev
    # secondary released the mirror and holds only the passive path
    assert mirror not in secondary_rpc.raids
    assert secondary_rpc.subsystems[volume.nqn]["namespaces"] == []
    # records flipped back
    assert _host(cluster.uuid).uuid == primary.uuid
    assert edge_db.get_edge_node_by_id(cluster.uuid, secondary.uuid).lvstore_base == ""
    assert edge_db.get_edge_node_by_id(cluster.uuid, primary.uuid).status == \
        EdgeNode.STATUS_ONLINE


# ------------------------------------------------------------------ crypto

def test_crypto_volume_create(env):
    kv, spdk, _ = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk, crypto=True)
    rpc = spdk.for_ip("10.0.0.1")
    assert volume.crypto and volume.crypto_bdev == stack.crypto_bdev(volume.uuid)
    # key registered + crypto bdev over the lvol; ns exposes the CRYPTO bdev
    assert stack.crypto_key_name(volume.uuid) in rpc.crypto_keys
    create = rpc.called("lvol_crypto_create")[0][1]
    assert create["base_name"] == volume.lvol_bdev
    assert rpc.subsystems[volume.nqn]["namespaces"][0]["bdev_name"] == \
        volume.crypto_bdev
    # DEKs persisted through the KMS (LocalKMS -> the shared kv store)
    dek_key = f"keys/{stack.volume_dek_path(cluster.uuid, volume.uuid)}".encode()
    assert kv.get(dek_key)


def test_crypto_volume_delete_removes_keys(env):
    kv, spdk, _ = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk, crypto=True)
    edge_cluster_ops.delete_volume(cluster.uuid, volume.uuid)
    rpc = spdk.for_ip("10.0.0.1")
    assert volume.crypto_bdev not in rpc.bdevs
    dek_key = f"keys/{stack.volume_dek_path(cluster.uuid, volume.uuid)}".encode()
    assert kv.get(dek_key) is None


def test_failover_republishes_crypto_on_secondary(env):
    kv, spdk, fake_k8s = env
    cluster, primary, secondary, (volume,) = _two_node_cluster(spdk, crypto=True)
    fake_k8s.running["worker-1"] = False
    _monitor().check_cluster(edge_db.get_cluster(cluster.uuid))
    result = edge_cluster_ops.handle_failover_task(_failover_tasks(cluster.uuid)[0])
    assert result.kind == TaskResult.DONE

    secondary_rpc = spdk.for_ip("10.0.0.2")
    # the key came back from the KMS and the crypto bdev was rebuilt there
    assert stack.crypto_key_name(volume.uuid) in secondary_rpc.crypto_keys
    assert secondary_rpc.subsystems[volume.nqn]["namespaces"][0]["bdev_name"] == \
        volume.crypto_bdev
