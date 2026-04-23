# coding=utf-8
"""
conftest.py – fixtures for FTT=2 restart test suite.

Round-robin LVS assignment (4 nodes, 4 LVS):
  LVS_i: primary = node i, secondary = node (i+1)%4, tertiary = node (i+2)%4

  LVS_0: pri=n0, sec=n1, tert=n2
  LVS_1: pri=n1, sec=n2, tert=n3
  LVS_2: pri=n2, sec=n3, tert=n0
  LVS_3: pri=n3, sec=n0, tert=n1

Per-node roles:
  n0: LVS_0=primary, LVS_3=secondary, LVS_2=tertiary
  n1: LVS_1=primary, LVS_0=secondary, LVS_3=tertiary
  n2: LVS_2=primary, LVS_1=secondary, LVS_0=tertiary
  n3: LVS_3=primary, LVS_2=secondary, LVS_1=tertiary

Tests restart n0.  Exactly one other node (n1, n2, or n3) is in outage.
Impact per outage:
  n1 out: LVS_0 sec down,   LVS_3 sibling-tert down, LVS_2 no impact
  n2 out: LVS_0 tert down,  LVS_3 no impact,          LVS_2 pri down
  n3 out: LVS_0 no impact,  LVS_3 pri down (TAKEOVER), LVS_2 sibling-sec down
"""

import os
import time
import uuid as _uuid_mod
from typing import List
from unittest.mock import patch

import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.stats import ClusterStatObject

from tests.ftt2.mock_cluster import FTT2MockRpcServer

NUM_NODES = 4

# ---------------------------------------------------------------------------
# Port allocation (xdist-safe)
# ---------------------------------------------------------------------------

_BASE_PORT = 11100


def _worker_port_offset() -> int:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    try:
        return int(worker.replace("gw", "")) * 20
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# Mock RPC servers (session-scoped)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def mock_rpc_servers():
    offset = _worker_port_offset()
    servers = []
    for i in range(NUM_NODES):
        port = _BASE_PORT + offset + i
        srv = FTT2MockRpcServer(
            host="127.0.0.1", port=port, node_id=f"ftt2-n{i}")
        srv.start()
        servers.append(srv)
    yield servers
    for srv in servers:
        srv.stop()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_nic(ip: str) -> IFace:
    nic = IFace()
    nic.uuid = str(_uuid_mod.uuid4())
    nic.if_name = "eth0"
    nic.ip4_address = ip
    nic.trtype = "TCP"
    nic.net_type = "data"
    return nic


def _make_device(cluster_id: str, node_id: str, idx: int) -> NVMeDevice:
    dev = NVMeDevice()
    dev.uuid = str(_uuid_mod.uuid4())
    dev.cluster_id = cluster_id
    dev.node_id = node_id
    dev.status = NVMeDevice.STATUS_ONLINE
    dev.nvme_bdev = f"nvme_{idx}"
    dev.alceml_bdev = f"alceml_{idx}"
    dev.pt_bdev = f"pt_{idx}"
    dev.testing_bdev = ""
    dev.nvmf_nqn = f"nqn:dev:{node_id[:8]}:{idx}"
    dev.health_check = True
    dev.io_error = False
    dev.size = 100_000_000_000
    return dev


def _vuid_gen():
    n = 2000
    while True:
        yield n
        n += 1


@pytest.fixture(scope="session")
def ensure_db():
    from simplyblock_core.db_controller import DBController
    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")
    yield db


# ---------------------------------------------------------------------------
# Main environment fixture
# ---------------------------------------------------------------------------

@pytest.fixture()
def ftt2_env(ensure_db, mock_rpc_servers):
    """
    Create a 4-node FTT=2 cluster in FDB with round-robin LVS assignment.
    Returns dict with cluster, nodes[], servers[], and topology metadata.
    """
    db = ensure_db
    offset = _worker_port_offset()

    for srv in mock_rpc_servers:
        srv.reset()

    # --- Cluster ---
    cluster = Cluster()
    cluster.uuid = f"ftt2-{_uuid_mod.uuid4().hex[:12]}"
    cluster.status = Cluster.STATUS_ACTIVE
    cluster.ha_type = "ha"
    cluster.max_fault_tolerance = 2
    cluster.blk_size = 4096
    cluster.distr_ndcs = 1
    cluster.distr_npcs = 2
    cluster.distr_bs = 4096
    cluster.distr_chunk_bs = 4096
    cluster.page_size_in_blocks = 4096
    cluster.nqn = f"nqn.2023-02.io.simplyblock:{cluster.uuid[:8]}"
    cluster.full_page_unmap = False
    cluster.fabric_tcp = True
    cluster.fabric_rdma = False
    cluster.mode = "k8s"
    cluster.qpair_count = 1
    cluster.write_to_db(db.kv_store)

    stat = ClusterStatObject(data={
        "cluster_id": cluster.uuid, "uuid": cluster.uuid,
        "date": int(time.time()), "size_total": 1_073_741_824_000,
    })
    stat.write_to_db(db.kv_store)

    # --- Nodes ---
    jm_vuids = [100, 200, 300, 400]
    nodes: List[StorageNode] = []

    for i in range(NUM_NODES):
        port = _BASE_PORT + offset + i
        n = StorageNode()
        n.uuid = str(_uuid_mod.uuid4())
        n.cluster_id = cluster.uuid
        n.status = StorageNode.STATUS_ONLINE
        n.hostname = f"ftt2-host-{i}"
        n.mgmt_ip = "127.0.0.1"
        n.api_endpoint = f"127.0.0.1:{5000 + i}"
        n.rpc_port = port
        n.rpc_username = "spdkuser"
        n.rpc_password = "spdkpass"
        n.is_secondary_node = False
        n.number_of_distribs = 1
        n.active_tcp = True
        n.active_rdma = False
        n.data_nics = [_make_nic("127.0.0.1")]
        n.enable_ha_jm = True
        n.jm_vuid = jm_vuids[i]
        n.nvme_devices = [_make_device(cluster.uuid, n.uuid, d) for d in range(2)]
        n.lvstore = f"LVS_{i}"
        n.lvstore_status = "ready"
        n.health_check = True
        n.spdk_cpu_mask = "0x3"
        n.spdk_image = "mock-spdk:latest"
        n.spdk_mem = 4_000_000_000
        n.max_lvol = 32
        n.iobuf_small_pool_count = 8192
        n.iobuf_large_pool_count = 1024
        n.lvstore_stack = [{'type': 'distrib', 'name': f'distrib_{i}_0',
                            'params': {'name': f'distrib_{i}_0'}}]
        n.raid = f"raid_{i}"
        n.lvstore_ports = {f"LVS_{i}": {"lvol_subsys_port": 4420 + i, "hublvol_port": 4430 + i}}
        nodes.append(n)

    # --- Round-robin wiring ---
    # LVS_i: pri=i, sec=(i+1)%4, tert=(i+2)%4
    for i in range(NUM_NODES):
        sec_idx = (i + 1) % NUM_NODES
        tert_idx = (i + 2) % NUM_NODES
        nodes[i].secondary_node_id = nodes[sec_idx].uuid
        nodes[i].tertiary_node_id = nodes[tert_idx].uuid

    # Back-references: node j is secondary for LVS_(j-1)%4, tertiary for LVS_(j-2)%4
    for j in range(NUM_NODES):
        pri_where_sec = (j - 1) % NUM_NODES  # j is secondary for this primary
        pri_where_tert = (j - 2) % NUM_NODES  # j is tertiary for this primary
        nodes[j].lvstore_stack_secondary = nodes[pri_where_sec].uuid
        nodes[j].lvstore_stack_tertiary = nodes[pri_where_tert].uuid

    # Write nodes
    for n in nodes:
        n.write_to_db(db.kv_store)

    # Default JM connectivity: all see all
    for i, srv in enumerate(mock_rpc_servers):
        for j, other in enumerate(nodes):
            if i != j:
                srv.set_jm_connected(other.uuid, True)
        # Pre-populate lvstore on each mock server (for bdev_lvol_get_lvstores)
        srv.state.lvstores[nodes[i].lvstore] = {
            'name': nodes[i].lvstore, 'base_bdev': '',
            'block_size': 4096, 'cluster_size': 4096,
            'lvs leadership': True, 'lvs_primary': True, 'lvs_read_only': False,
            'lvs_secondary': False, 'lvs_redirect': False,
            'remote_bdev': '', 'connect_state': False,
        }

    env = {
        'db': db,
        'cluster': cluster,
        'nodes': nodes,
        'servers': mock_rpc_servers,
        'jm_vuids': jm_vuids,
    }

    yield env

    # Teardown
    for n in nodes:
        try:
            n.remove(db.kv_store)
        except Exception:
            pass
    try:
        stat.remove(db.kv_store)
    except Exception:
        pass
    try:
        cluster.remove(db.kv_store)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Scenario helpers — set peer node state
# ---------------------------------------------------------------------------

def set_node_offline(env, node_idx: int):
    """OFFLINE: fully shut down (mgmt down, fabric down)."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_OFFLINE
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(False)
    env['servers'][node_idx].set_fabric_up(False)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, False)


def set_node_unreachable_fabric_healthy(env, node_idx: int):
    """UNREACHABLE: mgmt down, fabric up — peers still see JM connected."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_UNREACHABLE
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(False)
    env['servers'][node_idx].set_fabric_up(True)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, True)


def set_node_no_fabric(env, node_idx: int):
    """UNREACHABLE + no fabric: both mgmt and fabric down."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_UNREACHABLE
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(False)
    env['servers'][node_idx].set_fabric_up(False)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, False)


def set_node_down_fabric_healthy(env, node_idx: int):
    """DOWN: mgmt up, NVMe ports blocked, fabric healthy."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_DOWN
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(True)
    env['servers'][node_idx].set_fabric_up(True)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, True)



def set_node_down_no_fabric(env, node_idx: int):
    """DOWN: mgmt up, NVMe ports blocked, fabric disconnected."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_DOWN
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(True)
    env['servers'][node_idx].set_fabric_up(False)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, False)


def set_node_non_leader(env, node_idx: int, lvs_name: str):
    """ONLINE node that is not leader for the given LVS."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_ONLINE
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(True)
    env['servers'][node_idx].set_fabric_up(True)
    env['servers'][node_idx].state.leadership[lvs_name] = False
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, True)


def prepare_node_for_restart(env, node_idx: int):
    """Set node to OFFLINE so restart_storage_node() accepts it.
    Also mark JM as connected on all peers (SPDK is about to start)."""
    db = env['db']
    node = env['nodes'][node_idx]
    node.status = StorageNode.STATUS_OFFLINE
    node.write_to_db(db.kv_store)
    env['servers'][node_idx].set_reachable(True)
    env['servers'][node_idx].set_fabric_up(True)
    # Peers should see this node's JM as connected (it's restarting, SPDK coming up)
    for i, srv in enumerate(env['servers']):
        if i != node_idx:
            srv.set_jm_connected(node.uuid, True)
    # Set leadership: the secondary node is the current leader for this node's LVS
    if node.secondary_node_id:
        for i, n in enumerate(env['nodes']):
            if n.uuid == node.secondary_node_id:
                env['servers'][i].state.leadership[node.lvstore] = True
                break


def create_test_lvol(env, primary_node_idx: int, name: str = "test-vol",
                     encrypted: bool = False, qos: bool = False,
                     dhchap: bool = False) -> LVol:
    """Create an LVol in FDB on the given primary node's LVS."""
    db = env['db']
    node = env['nodes'][primary_node_idx]
    cluster = env['cluster']

    lvol = LVol()
    lvol.uuid = str(_uuid_mod.uuid4())
    lvol.lvol_name = name
    lvol.lvol_uuid = str(_uuid_mod.uuid4())
    lvol.cluster_id = cluster.uuid
    lvol.node_id = node.uuid
    lvol.status = LVol.STATUS_ONLINE
    lvol.size = 1_073_741_824
    lvol.ha_type = "ha"
    lvol.nqn = f"nqn.2023-02.io.simplyblock:lvol-{name}-{lvol.uuid[:8]}"
    lvol.top_bdev = f"{node.lvstore}/{name}"

    bdev_stack = [{'type': 'bdev_lvol', 'name': lvol.top_bdev}]
    if encrypted:
        lvol.crypto_key1 = "a" * 64
        lvol.crypto_key2 = "b" * 64
        lvol.crypto_bdev = f"crypto_{name}"
        bdev_stack.append({
            'type': 'crypto', 'name': lvol.crypto_bdev,
            'params': {'key1': lvol.crypto_key1, 'key2': lvol.crypto_key2},
        })
    if qos:
        lvol.max_rw_iops = 10000
        lvol.max_rw_mbytes = 100
    if dhchap:
        lvol.allowed_hosts = [{
            'nqn': 'nqn.2014-08.org.nvmexpress:uuid:test-host',
            'dhchap_key': 'DHHC-1:00:test-key-value-32bytes-long!!:',
            'dhchap_ctrlr_key': 'DHHC-1:00:test-ctrl-key-32bytes-long!:',
        }]

    lvol.bdev_stack = bdev_stack
    lvol.write_to_db(db.kv_store)
    return lvol


# ---------------------------------------------------------------------------
# External patches
# ---------------------------------------------------------------------------

def patch_externals():
    """Mock all external deps so restart runs purely against mock RPC servers."""
    return [
        patch('simplyblock_core.distr_controller.send_cluster_map_to_distr',
              return_value=True),
        patch('simplyblock_core.distr_controller.send_cluster_map_add_node',
              return_value=True),
        patch('simplyblock_core.distr_controller.parse_distr_cluster_map',
              return_value=([], True)),
        patch('simplyblock_core.distr_controller.send_dev_status_event'),
        patch('simplyblock_core.distr_controller.send_node_status_event'),
        patch('simplyblock_core.utils.ping_host', return_value=True),
        patch('simplyblock_core.utils.get_k8s_node_ip', return_value='127.0.0.1'),
        patch('simplyblock_core.controllers.health_controller._check_node_ping',
              return_value=True),
        patch('simplyblock_core.snode_client.SNodeClient.is_live',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.info',
              return_value=({'hostname': 'mock', 'network_interface': {},
                             'nvme_devices': [], 'spdk_pcie_list': [],
                             'memory_details': {'total': 64_000_000_000, 'free': 32_000_000_000,
                                                'huge_total': 16_000_000_000},
                             'nodes_config': {'nodes': []}}, None)),
        patch('simplyblock_core.snode_client.SNodeClient.ifc_is_tcp', return_value=True),
        patch('simplyblock_core.snode_client.SNodeClient.ifc_is_roce', return_value=False),
        patch('simplyblock_core.snode_client.SNodeClient.bind_device_to_spdk',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.read_allowed_list',
              return_value=([], None)),
        patch('simplyblock_core.snode_client.SNodeClient.recalculate_cores_distribution',
              return_value=({}, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_is_up',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_start',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_kill',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.write_key_file',
              return_value=(True, None)),
        patch('simplyblock_core.controllers.health_controller._check_node_api',
              return_value=True),
        patch('simplyblock_core.fw_api_client.FirewallClient.firewall_set_port',
              return_value=(True, None)),
        patch('simplyblock_core.fw_api_client.FirewallClient.get_firewall',
              return_value=('', None)),
        patch('simplyblock_core.controllers.health_controller.check_port_on_node',
              return_value=True),
        patch('simplyblock_core.utils.get_next_port', return_value=9090),
        patch('simplyblock_core.utils.get_random_vuid', side_effect=_vuid_gen()),
        patch('simplyblock_core.utils.next_free_hublvol_port', return_value=4420),
        patch('simplyblock_core.controllers.tasks_controller.add_jc_comp_resume_task'),
        patch('simplyblock_core.controllers.tasks_controller.add_port_allow_task'),
        patch('simplyblock_core.controllers.tasks_controller.add_device_mig_task_for_node'),
        patch('simplyblock_core.controllers.tasks_controller.get_active_node_restart_task',
              return_value=None),
        patch('simplyblock_core.controllers.storage_events.snode_health_check_change'),
        patch('simplyblock_core.controllers.storage_events.snode_status_change'),
        patch('simplyblock_core.controllers.storage_events.snode_restart_failed'),
        patch('simplyblock_core.controllers.device_events.device_health_check_change'),
        patch('simplyblock_core.controllers.tcp_ports_events.port_deny'),
        patch('simplyblock_core.controllers.tcp_ports_events.port_allowed'),
        patch('simplyblock_core.storage_node_ops._connect_to_remote_jm_devs',
              return_value=[]),
        patch('simplyblock_core.storage_node_ops._connect_to_remote_devs',
              return_value=[]),
        patch('simplyblock_core.storage_node_ops.addNvmeDevices',
              side_effect=lambda rpc, snode, ssds: snode.nvme_devices),
        patch('simplyblock_core.storage_node_ops._prepare_cluster_devices_on_restart',
              return_value=True),
        patch('simplyblock_core.storage_node_ops._refresh_cluster_maps_after_node_recovery'),
        patch('simplyblock_core.storage_node_ops.trigger_ana_failback_for_node'),
        patch('simplyblock_core.storage_node_ops.set_node_status'),
        patch('simplyblock_core.storage_node_ops._failback_primary_ana'),
        patch('simplyblock_core.distr_controller.send_cluster_map_to_node', return_value=True),
        patch('simplyblock_core.controllers.health_controller.check_bdev', return_value=True),
        patch('simplyblock_core.controllers.device_controller.set_jm_device_state'),
        patch('simplyblock_core.controllers.device_events.device_restarted'),
        patch('simplyblock_core.controllers.lvol_controller.connect_lvol_to_pool'),
        patch('simplyblock_core.controllers.qos_controller.get_qos_weights_list',
              return_value=[]),
        patch('simplyblock_core.storage_node_ops.get_sorted_ha_jms', return_value=[]),
        patch('simplyblock_core.storage_node_ops.get_next_physical_device_order',
              return_value=1),
        patch('simplyblock_core.storage_node_ops.time.sleep'),
        patch('simplyblock_core.models.storage_node.time.sleep'),
    ]
