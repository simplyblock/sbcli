"""Sync-replication setup against the real FoundationDB.

Covers the cluster flag at create time, the per-LVS event records and their
accessors, the node site on add-node (preflight, persistence, the node-add
task, the v2 route) and the two-site activation guards. Storage nodes and
every other external side effect are mocked; the database never is.
"""
import uuid
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import SecretStr

import simplyblock_web.api.v2 as v2
import simplyblock_web.api.v2._auth as auth_module
from simplyblock_core import cluster_ops, storage_node_ops
from simplyblock_core.controllers import tasks_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster, DeployConfig
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.sync_replication import SyncReplicationEvent
from simplyblock_core.services import tasks_runner_node_add

EVENT_INDEX = 'cluster_id+lvs_name+resolved'
NODE_ADDR = "10.1.0.10:5000"
HOST_IP = "10.1.0.10"


class PastTheGuard(Exception):
    """Raised by the first call after the activation site guards."""


@pytest.fixture()
def db():
    return DBController()


def _seed_cluster(db, *, sync=True, status=Cluster.STATUS_UNREADY, ndcs=1, npcs=1,
                  activated=(), spdk_vcpu_count=0):
    cluster = Cluster()
    cluster.uuid = str(uuid.uuid4())
    cluster.cluster_name = f"cl-{cluster.uuid[:8]}"
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid}"
    cluster.status = status
    cluster.ha_type = "ha"
    cluster.sync_replication = sync
    cluster.distr_ndcs = ndcs
    cluster.distr_npcs = npcs
    cluster.max_fault_tolerance = 1
    cluster.activated_node_ids = list(activated)
    cluster.mode = "docker"
    cluster.cluster_vip = "10.0.0.1"
    cluster.spdk_vcpu_count = spdk_vcpu_count
    cluster.write_to_db(db.kv_store)
    return cluster


def _seed_node(db, cluster, site, *, host=None, status=StorageNode.STATUS_ONLINE):
    node = StorageNode()
    node.uuid = str(uuid.uuid4())
    node.cluster_id = cluster.get_id()
    node.site = site
    node.mgmt_ip = host or f"10.2.{len(site)}.{uuid.uuid4().int % 250 + 1}"
    node.status = status
    device = NVMeDevice()
    device.uuid = str(uuid.uuid4())
    device.status = NVMeDevice.STATUS_ONLINE
    device.size = 10 * 1024 ** 3
    node.nvme_devices = [device]
    node.write_to_db(db.kv_store)
    return node


def _seed_two_sites(db, cluster, per_site=3, *, b_status=StorageNode.STATUS_ONLINE):
    nodes = [_seed_node(db, cluster, "site-a", host=f"10.3.0.{i}") for i in range(1, per_site + 1)]
    nodes += [_seed_node(db, cluster, "site-b", host=f"10.4.0.{i}", status=b_status)
              for i in range(1, per_site + 1)]
    return nodes


# ---------------------------------------------------------------------------
# cluster create
# ---------------------------------------------------------------------------

def _seed_deploy_config(db):
    cfg = DeployConfig()
    cfg.mode = "docker"
    cfg.disable_monitoring = True
    cfg.grafana_endpoint = "http://grafana.example"
    cfg.grafana_secret = SecretStr("graf-secret")
    cfg.db_connection = SecretStr("db-conn")
    cfg.write_to_db(db.kv_store)


def _add_cluster(name, **kwargs):
    params = dict(
        blk_size=4096, page_size_in_blocks=2, cap_warn=80, cap_crit=90, prov_cap_warn=80,
        prov_cap_crit=90, distr_ndcs=1, distr_npcs=1, distr_bs=4096, distr_chunk_bs=4096,
        ha_type="ha", enable_node_affinity=False, qpair_count=4, max_queue_size=128,
        inflight_io_threshold=64, strict_node_anti_affinity=False, is_single_node=False, name=name,
    )
    params.update(kwargs)
    return cluster_ops.add_cluster(**params)


class TestClusterCreate:

    def test_add_cluster_persists_sync_replication(self, db):
        _seed_deploy_config(db)
        _seed_cluster(db, sync=False)  # not the first cluster: DeployConfig is read
        cluster_id = _add_cluster("sync-cl", sync_replication=True)
        stored = db.get_cluster_by_id(cluster_id)
        assert stored.sync_replication is True
        assert stored.lost_site == ""
        assert stored.lost_site_state == ""

    def test_add_cluster_defaults_to_no_sync_replication(self, db):
        _seed_deploy_config(db)
        _seed_cluster(db, sync=False)
        cluster_id = _add_cluster("plain-cl")
        assert db.get_cluster_by_id(cluster_id).sync_replication is False

    @pytest.mark.parametrize("kwargs, message", [
        ({"ha_type": "single"}, "ha_type='ha'"),
        ({"is_single_node": True}, "single-node"),
    ])
    def test_add_cluster_refuses_invalid_sync_and_writes_nothing(self, db, kwargs, message):
        _seed_deploy_config(db)
        before = {c.get_id() for c in db.get_clusters()}
        with pytest.raises(ValueError, match=message):
            _add_cluster("bad-cl", sync_replication=True, **kwargs)
        assert {c.get_id() for c in db.get_clusters()} == before

    @pytest.mark.parametrize("kwargs, message", [
        ({"ha_type": "single", "is_single_node": False}, "ha_type='ha'"),
        ({"ha_type": "ha", "is_single_node": True}, "single-node"),
    ])
    def test_create_cluster_refuses_before_any_deployment_step(self, db, kwargs, message):
        with patch("simplyblock_core.cluster_ops.scripts") as scripts, \
                patch("simplyblock_core.cluster_ops.docker") as docker, \
                patch("simplyblock_core.cluster_ops.mgmt_node_ops") as mgmt, \
                patch("simplyblock_core.cluster_ops.utils.get_iface_ip", return_value="10.0.0.1"):
            with pytest.raises(ValueError, match=message):
                cluster_ops.create_cluster(
                    blk_size=4096, page_size_in_blocks=2, cli_pass=SecretStr("pass"),
                    cap_warn=80, cap_crit=90, prov_cap_warn=80, prov_cap_crit=90, ifname="eth0",
                    mgmt_ip=None, log_del_interval=7, metrics_retention_period=30,
                    contact_point=None, grafana_endpoint=None, distr_ndcs=1, distr_npcs=1,
                    distr_bs=4096, distr_chunk_bs=4096, mode="docker", enable_node_affinity=False,
                    qpair_count=4, client_qpair_count=4, max_queue_size=128,
                    inflight_io_threshold=64, disable_monitoring=True,
                    strict_node_anti_affinity=False, name="sync-create", tls_secret=None,
                    ingress_host_source=None, dns_name=None, fabric="tcp", client_data_nic="",
                    sync_replication=True, **kwargs)
        assert scripts.mock_calls == []
        assert docker.mock_calls == []
        assert mgmt.mock_calls == []
        assert db.get_clusters() == []


# ---------------------------------------------------------------------------
# SyncReplicationEvent accessors
# ---------------------------------------------------------------------------

def _event(db, cluster_id, lvs_name, *, resolved=False, kind=SyncReplicationEvent.KIND_ZONE_UNAVAILABLE):
    event = SyncReplicationEvent()
    event.uuid = str(uuid.uuid4())
    event.cluster_id = cluster_id
    event.lvs_name = lvs_name
    event.node_id = "n1"
    event.kind = kind
    event.status = "secondary_zone_unavailable"
    event.timestamp_utc = "2026-09-29T10:00:00.000Z"
    event.resolved = resolved
    event.write_to_db(db.kv_store)
    return event


@pytest.mark.parametrize("state", ["building", "ready"])
def test_event_accessors_by_lvs_and_unresolved(db, state):
    db.set_index_state(SyncReplicationEvent, EVENT_INDEX, state)
    open_event = _event(db, "c1", "LVS_1")
    closed_event = _event(db, "c1", "LVS_1", resolved=True,
                          kind=SyncReplicationEvent.KIND_REMOTE_JOURNAL_DROPPED)
    _event(db, "c1", "LVS_10")          # prefix sibling of LVS_1
    _event(db, "c2", "LVS_1")           # same LVS name, other cluster
    assert db.index_state(SyncReplicationEvent, EVENT_INDEX) == state

    assert {e.get_id() for e in db.get_sync_replication_events("c1", "LVS_1")} == {
        open_event.get_id(), closed_event.get_id()}
    assert [e.get_id() for e in db.get_unresolved_sync_replication_events("c1", "LVS_1")] == [
        open_event.get_id()]

    open_event.resolved = True
    open_event.write_to_db(db.kv_store)
    assert db.get_unresolved_sync_replication_events("c1", "LVS_1") == []
    assert len(db.get_sync_replication_events("c1", "LVS_1")) == 2


# ---------------------------------------------------------------------------
# activation guards
# ---------------------------------------------------------------------------

def _activate(cluster, force=False):
    with patch.object(cluster_ops, "_wait_for_full_device_connectivity",
                      side_effect=PastTheGuard()):
        cluster_ops._cluster_activate(cluster.get_id(), force=force)


class TestActivationSiteGuards:

    def test_fresh_activation_with_two_full_sites_passes(self, db):
        cluster = _seed_cluster(db)
        _seed_two_sites(db, cluster)
        with pytest.raises(PastTheGuard):
            _activate(cluster)

    def test_non_sync_cluster_ignores_sites(self, db):
        cluster = _seed_cluster(db, sync=False)
        for i in range(1, 4):
            _seed_node(db, cluster, "", host=f"10.5.0.{i}")
        with pytest.raises(PastTheGuard):
            _activate(cluster)

    @pytest.mark.parametrize("extra_status", [StorageNode.STATUS_ONLINE, StorageNode.STATUS_OFFLINE])
    def test_third_site_is_refused_and_status_restored(self, db, extra_status):
        cluster = _seed_cluster(db)
        _seed_two_sites(db, cluster)
        _seed_node(db, cluster, "site-c", host="10.6.0.1", status=extra_status)
        with pytest.raises(ValueError, match="exactly 2 sites"):
            _activate(cluster)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY

    def test_node_without_site_is_refused(self, db):
        cluster = _seed_cluster(db)
        _seed_two_sites(db, cluster)
        unsited = _seed_node(db, cluster, "", host="10.6.0.2", status=StorageNode.STATUS_OFFLINE)
        with pytest.raises(ValueError, match=f"without a site: {unsited.get_id()}"):
            _activate(cluster)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY

    def test_too_few_hosts_on_a_site_refuses_fresh_activation(self, db):
        cluster = _seed_cluster(db)
        _seed_node(db, cluster, "site-a", host="10.3.0.1")
        _seed_node(db, cluster, "site-a", host="10.3.0.2")
        _seed_node(db, cluster, "site-a", host="10.3.0.3")
        for _ in range(3):
            _seed_node(db, cluster, "site-b", host="10.4.0.1")   # three nodes, one host
        with pytest.raises(ValueError, match="site site-b has 3 online node\\(s\\) on 1 host"):
            _activate(cluster)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY

    def test_stripe_width_per_site_refuses_fresh_activation(self, db):
        cluster = _seed_cluster(db, ndcs=2, npcs=2)
        _seed_two_sites(db, cluster, per_site=3)
        with pytest.raises(ValueError, match="needs 4 node"):
            _activate(cluster)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_UNREADY

    def test_reactivation_after_a_site_loss_warns_and_continues(self, db):
        """Two configured sites, one of them entirely offline: recovery must not
        be blocked by the capacity rule."""
        cluster = _seed_cluster(db, status=Cluster.STATUS_SUSPENDED)
        nodes = _seed_two_sites(db, cluster, b_status=StorageNode.STATUS_OFFLINE)
        cluster.activated_node_ids = [n.get_id() for n in nodes]
        cluster.write_to_db(db.kv_store)
        with patch.object(cluster_ops.logger, "warning") as warning, pytest.raises(PastTheGuard):
            _activate(cluster)
        messages = [str(c.args[0]) % c.args[1:] for c in warning.call_args_list]
        assert any("reduced site capacity" in m and "site site-b has 0 online" in m for m in messages)

    def test_force_reactivation_does_not_bypass_the_topology(self, db):
        cluster = _seed_cluster(db, status=Cluster.STATUS_ACTIVE)
        nodes = _seed_two_sites(db, cluster)
        nodes.append(_seed_node(db, cluster, "site-c", host="10.6.0.3", status=StorageNode.STATUS_OFFLINE))
        cluster.activated_node_ids = [n.get_id() for n in nodes]
        cluster.write_to_db(db.kv_store)
        with pytest.raises(ValueError, match="exactly 2 sites"):
            _activate(cluster, force=True)
        assert db.get_cluster_by_id(cluster.get_id()).status == Cluster.STATUS_ACTIVE


# ---------------------------------------------------------------------------
# add-node
# ---------------------------------------------------------------------------

def _node_config(slot):
    return {
        "isolated": [1 + 4 * slot, 2 + 4 * slot, 3 + 4 * slot, 4 + 4 * slot],
        "cpu_mask": hex(0xf << (1 + 4 * slot)),
        "l-cores": f"{1 + 4 * slot}-{4 + 4 * slot}",
        "socket": slot,
        "number_of_alcemls": 1,
        "small_pool_count": 0,
        "large_pool_count": 0,
        "number_of_distribs": 2,
        "max_lvol": 10,
        "sys_memory": 1024 ** 3,
        "ssd_pcis": [f"0000:0{slot}:00.0"],
        "distribution": {
            "poller_cpu_cores": [1 + 4 * slot],
            "alceml_cpu_cores": [2 + 4 * slot],
            "distrib_cpu_cores": [3 + 4 * slot],
            "alceml_worker_cpu_cores": [2 + 4 * slot],
            "jc_singleton_core": [4 + 4 * slot],
            "app_thread_core": [1 + 4 * slot],
            "jm_cpu_core": [4 + 4 * slot],
            "lvol_poller_core": [4 + 4 * slot],
            "compression_core": None,
        },
    }


def _node_info(slots=1, host_ip=HOST_IP):
    return {
        "nodes_config": {"nodes": [_node_config(slot) for slot in range(slots)]},
        "hostname": "host-a",
        "system_id": "sys-a",
        "cloud_instance": {"id": "i-1", "type": "t", "cloud": "c", "ip": host_ip, "public_ip": host_ip},
        "network_interface": {"eth0": {"ip": host_ip, "status": "up", "net_type": "ether"}},
        "memory_details": {"total": 64 * 1024 ** 3, "free": 32 * 1024 ** 3,
                           "huge_total": 16 * 1024 ** 3, "huge_free": 16 * 1024 ** 3},
        "cpu_count": 16,
        "spdk_pcie_list": [],
    }


def _snode_api(node_info):
    api = MagicMock()
    api.info.return_value = (node_info, None)
    api.spdk_process_start.return_value = (True, None)
    api.read_allowed_list.return_value = ([], None)
    return api


class _AddNodeWorld:
    """Everything add_node reaches beyond the database, mocked."""

    def __init__(self, node_info):
        self.api = _snode_api(node_info)
        self.patches = [
            patch.object(storage_node_ops, "SNodeClient", return_value=self.api),
            patch.object(storage_node_ops, "apply_cluster_vcpu_count", return_value=True),
            patch.object(storage_node_ops, "reserve_cluster_hugepages", return_value=True),
            patch.object(storage_node_ops, "apply_cluster_hugepages", return_value=4 * 1024 ** 3),
            patch.object(storage_node_ops.utils, "calculate_spdk_memory", return_value=(True, None)),
            patch.object(storage_node_ops.utils, "get_max_parallel_node_adds_from_cr", return_value=None),
            patch.object(storage_node_ops.utils, "get_storage_node_api_log_type", return_value=None),
            patch.object(storage_node_ops.time, "sleep"),
            patch("simplyblock_core.models.storage_node.RPCClient"),
            patch.object(storage_node_ops, "addNvmeDevices", return_value=[]),
            patch.object(storage_node_ops, "_connect_to_remote_devs", return_value=[]),
            patch.object(storage_node_ops, "_connect_to_remote_jm_devs", return_value=[]),
            patch.object(storage_node_ops, "distr_controller"),
        ]
        self.mocks = {}

    def __enter__(self):
        for p in self.patches:
            self.mocks[p.attribute] = p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self.patches):
            p.stop()


def _add_node(cluster, site, **kwargs):
    return storage_node_ops.add_node(
        cluster.get_id(), NODE_ADDR, "eth0", [], max_snap=500, spdk_image="img",
        spdk_debug=False, num_partitions_per_dev=1, jm_percent=3, enable_ha_jm=True,
        site=site, **kwargs)


class TestAddNodeSite:

    @pytest.mark.parametrize("slots", [1, 2])
    def test_sync_cluster_add_succeeds_and_persists_the_site_from_the_first_write(self, db, slots):
        cluster = _seed_cluster(db)
        original_write = StorageNode.write_to_db
        persisted = []

        def spy(node, *args, **kwargs):
            persisted.append((node.get_id(), node.site))
            return original_write(node, *args, **kwargs)

        with _AddNodeWorld(_node_info(slots)), \
                patch.object(StorageNode, "write_to_db", autospec=True, side_effect=spy):
            assert _add_node(cluster, "site-a") == "Success"

        nodes = db.get_storage_nodes_by_cluster_id(cluster.get_id())
        assert len(nodes) == slots
        assert {n.site for n in nodes} == {"site-a"}
        assert {n.status for n in nodes} == {StorageNode.STATUS_ONLINE}
        for node in nodes:
            writes = [site for node_id, site in persisted if node_id == node.get_id()]
            assert writes, "the node was never persisted through write_to_db"
            assert writes[0] == "site-a", "the first persisted record lacked the site"
            assert set(writes) == {"site-a"}

    def test_non_sync_cluster_add_succeeds_without_a_site(self, db):
        cluster = _seed_cluster(db, sync=False)
        with _AddNodeWorld(_node_info()):
            assert _add_node(cluster, None) == "Success"
        (node,) = db.get_storage_nodes_by_cluster_id(cluster.get_id())
        assert node.site == ""
        assert node.status == StorageNode.STATUS_ONLINE

    @pytest.mark.parametrize("sync, site, message", [
        (True, None, "--site is required"),
        (True, "a:b", "invalid site"),
        (False, "site-a", "not created with --sync-replication"),
    ])
    def test_invalid_site_is_refused_before_the_host_is_touched(self, db, sync, site, message):
        cluster = _seed_cluster(db, sync=sync, spdk_vcpu_count=4)
        with _AddNodeWorld(_node_info()) as world, \
                pytest.raises(storage_node_ops.NodeSiteError, match=message):
            _add_node(cluster, site)
        world.mocks["apply_cluster_vcpu_count"].assert_not_called()
        world.mocks["reserve_cluster_hugepages"].assert_not_called()
        world.api.spdk_process_start.assert_not_called()
        assert db.get_storage_nodes_by_cluster_id(cluster.get_id()) == []

    def test_host_on_another_site_is_refused_before_the_host_is_touched(self, db):
        cluster = _seed_cluster(db, spdk_vcpu_count=4)
        existing = _seed_node(db, cluster, "site-b", host=HOST_IP)
        with _AddNodeWorld(_node_info()) as world, \
                pytest.raises(storage_node_ops.NodeSiteError, match="already belongs to site 'site-b'"):
            _add_node(cluster, "site-a")
        world.mocks["apply_cluster_vcpu_count"].assert_not_called()
        world.mocks["reserve_cluster_hugepages"].assert_not_called()
        world.api.spdk_process_start.assert_not_called()
        assert [n.get_id() for n in db.get_storage_nodes_by_cluster_id(cluster.get_id())] == [
            existing.get_id()]


def test_node_add_task_with_an_invalid_site_ends_with_the_reason(db):
    cluster = _seed_cluster(db, spdk_vcpu_count=4)
    task_id = tasks_controller.add_node_add_task(cluster.get_id(), {
        "cluster_id": cluster.get_id(), "node_addr": NODE_ADDR, "iface_name": "eth0",
        "data_nics_list": [], "max_snap": 500, "site": None,
    })
    task = db.get_task_by_id(task_id)
    with _AddNodeWorld(_node_info()) as world:
        assert tasks_runner_node_add.process_task(task, cluster) is True
    stored = db.get_task_by_id(task_id)
    assert stored.status == JobSchedule.STATUS_DONE
    assert stored.function_result.startswith("invalid input:")
    assert "--site is required" in stored.function_result
    assert stored.retry == 0
    world.mocks["apply_cluster_vcpu_count"].assert_not_called()
    world.mocks["reserve_cluster_hugepages"].assert_not_called()
    assert db.get_storage_nodes_by_cluster_id(cluster.get_id()) == []


# ---------------------------------------------------------------------------
# v2 route
# ---------------------------------------------------------------------------

@pytest.fixture()
def client():
    app = FastAPI()
    app.include_router(v2.api, prefix='/api/v2')
    app.dependency_overrides[auth_module.verify_api_token] = lambda: None
    return TestClient(app)


def _post_node(client, cluster, **extra):
    return client.post(f"/api/v2/clusters/{cluster.get_id()}/storage-nodes/",
                       json={"node_address": NODE_ADDR, "interface_name": "eth0", **extra})


def _node_add_tasks(db, cluster):
    return [t for t in db.get_job_tasks(cluster.get_id()) if t.function_name == JobSchedule.FN_NODE_ADD]


class TestStorageNodeRoute:

    @pytest.mark.parametrize("sync, extra, message", [
        (True, {}, "--site is required"),
        (True, {"site": "a/b"}, "invalid site"),
        (False, {"site": "site-a"}, "not created with --sync-replication"),
    ])
    def test_invalid_site_is_rejected_without_a_task(self, db, client, sync, extra, message):
        cluster = _seed_cluster(db, sync=sync)
        response = _post_node(client, cluster, **extra)
        assert response.status_code == 400
        assert message in response.json()["detail"]
        assert _node_add_tasks(db, cluster) == []

    def test_valid_site_reaches_the_task(self, db, client):
        cluster = _seed_cluster(db)
        response = _post_node(client, cluster, site="site-a")
        assert response.status_code == 201
        (task,) = _node_add_tasks(db, cluster)
        assert task.function_params["site"] == "site-a"
        assert response.json() == task.uuid

    def test_non_sync_cluster_without_site_reaches_the_task(self, db, client):
        cluster = _seed_cluster(db, sync=False)
        response = _post_node(client, cluster)
        assert response.status_code == 201
        (task,) = _node_add_tasks(db, cluster)
        assert task.function_params["site"] is None
