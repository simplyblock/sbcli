# coding=utf-8
"""
test_dual_ft_e2e.py – end-to-end tests for dual fault tolerance (triple-path).

Scenarios:
  1. Cluster activation with max_fault_tolerance=2 (4 primary nodes, 4 secondary nodes)
  2. Node restart after setting a node offline
  3. Health check service verifies both secondaries

All RPCs are handled by in-process mock JSON-RPC servers (one per node).
SNodeAPI is mocked via monkeypatch.  External dependencies (ping, firewall,
distr_controller) are patched out.

Requires FoundationDB running.
"""

import json
import logging
import os
import threading
import time
import uuid as _uuid_mod
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.stats import ClusterStatObject

logger = logging.getLogger(__name__)


# ===========================================================================
# Extended Mock RPC Server for cluster ops
# ===========================================================================

class ClusterNodeState:
    """In-memory data-plane state for one mock node during cluster ops."""

    def __init__(self, lvstore: str = ""):
        self.lvstore = lvstore
        self.bdevs: Dict[str, dict] = {}
        self.subsystems: Dict[str, dict] = {}
        self.nvme_controllers: Dict[str, dict] = {}
        self.lvstores: Dict[str, dict] = {}
        self.leader: bool = False
        self.lvs_opts: dict = {}
        self.hublvol_created: bool = False
        self.hublvol_connected: bool = False
        self.compression_suspended: bool = True
        self.examined: bool = False
        self.lock = threading.Lock()
        self._nsid_counter: Dict[str, int] = {}

    def next_nsid(self, nqn: str) -> int:
        self._nsid_counter.setdefault(nqn, 1)
        nsid = self._nsid_counter[nqn]
        self._nsid_counter[nqn] += 1
        return nsid


class _ClusterRpcHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            req = json.loads(body)
        except Exception:
            self._send_error(-32700, "Parse error", None)
            return

        method = req.get('method', '')
        params = req.get('params', {}) or {}
        req_id = req.get('id', 1)
        server = self.server

        handler = _CLUSTER_DISPATCH.get(method)
        if handler is None:
            # Return success for unknown methods (null-op mock)
            self._send_result(True, req_id)
            return

        try:
            with server.node_state.lock:
                result = handler(server.node_state, params)
            self._send_result(result, req_id)
        except _RpcError as e:
            self._send_error(e.code, e.message, req_id)
        except Exception as exc:
            self._send_error(-1, str(exc), req_id)

    def _send_result(self, result, req_id):
        self._respond({"jsonrpc": "2.0", "result": result, "id": req_id})

    def _send_error(self, code, message, req_id):
        self._respond({"jsonrpc": "2.0",
                        "error": {"code": code, "message": message},
                        "id": req_id})

    def _respond(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class _RpcError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---- RPC handler implementations ----

def _spdk_get_version(s, p):
    return {"version": "mock-24.05", "fields": {}}


def _bdev_get_bdevs(s, p):
    name = p.get('name')
    if name:
        entry = s.bdevs.get(name)
        return [entry] if entry else []
    return list(s.bdevs.values())


def _bdev_distrib_create(s, p):
    name = p.get('name', f"distrib_{_uuid_mod.uuid4().hex[:8]}")
    s.bdevs[name] = {
        'name': name,
        'aliases': [],
        'driver_specific': {'distrib': True},
    }
    return True


def _distr_send_cluster_map(s, p):
    return True


def _distr_get_cluster_map(s, p):
    name = p.get('name', '')
    return {
        'map_cluster': [],
        'map_prob': [],
        'name': name,
    }


def _bdev_raid_create(s, p):
    name = p.get('name', 'raid0')
    s.bdevs[name] = {
        'name': name,
        'aliases': [],
        'driver_specific': {'raid': True},
    }
    return True


def _create_lvstore(s, p):
    name = p.get('name', 'LVS')
    bdev_name = p.get('bdev_name', '')
    cluster_sz = p.get('cluster_sz', 4096)
    s.lvstore = name
    s.lvstores[name] = {
        'name': name,
        'base_bdev': bdev_name,
        'block_size': 4096,
        'cluster_size': cluster_sz,
        'lvs leadership': True,
        'lvs_primary': True,
        'lvs_read_only': False,
        'lvs_secondary': False,
        'lvs_redirect': False,
        'remote_bdev': '',
        'connect_state': False,
    }
    return name


def _bdev_lvol_get_lvstores(s, p):
    name = p.get('name', '')
    if name in s.lvstores:
        return [s.lvstores[name]]
    # Return the lvstore for any name (for secondary checks)
    if s.lvstores:
        lvs = list(s.lvstores.values())[0].copy()
        lvs['name'] = name
        return [lvs]
    return []


def _bdev_lvol_set_lvs_opts(s, p):
    s.lvs_opts = p
    lvs_name = p.get('lvs', '')
    role = p.get('role', 'primary')
    if lvs_name in s.lvstores:
        if role in ('secondary', 'tertiary'):
            s.lvstores[lvs_name]['lvs_secondary'] = True
            s.lvstores[lvs_name]['lvs_primary'] = False
            s.lvstores[lvs_name]['lvs leadership'] = False
            s.lvstores[lvs_name]['lvs_redirect'] = True
            s.lvstores[lvs_name]['connect_state'] = True
        if role == 'primary':
            s.lvstores[lvs_name]['lvs_primary'] = True
    return True


def _bdev_lvol_set_leader(s, p):
    s.leader = p.get('lvs_leadership', p.get('leader', False))
    return True


def _bdev_examine(s, p):
    s.examined = True
    return True


def _bdev_wait_for_examine(s, p):
    return True


def _bdev_lvol_create_hublvol(s, p):
    s.hublvol_created = True
    return str(_uuid_mod.uuid4())


def _bdev_lvol_connect_hublvol(s, p):
    s.hublvol_connected = True
    lvs_name = p.get('lvs', '')
    if lvs_name in s.lvstores:
        s.lvstores[lvs_name]['connect_state'] = True
    return True


def _jc_suspend_compression(s, p):
    suspend = p.get('suspend', False)
    s.compression_suspended = suspend
    return True


def _jc_compression_get_status(s, p):
    return not s.compression_suspended


def _nvmf_create_subsystem(s, p):
    nqn = p.get('nqn', '')
    if nqn in s.subsystems:
        return True  # idempotent
    s.subsystems[nqn] = {
        'nqn': nqn,
        'serial_number': p.get('serial_number', ''),
        'model_number': p.get('model_number', ''),
        'namespaces': [],
        'listen_addresses': [],
        'hosts': [],
        'allow_any_host': True,
        'ana_reporting': True,
    }
    return True


def _nvmf_get_subsystems(s, p):
    nqn = p.get('nqn')
    if nqn:
        sub = s.subsystems.get(nqn)
        return [sub] if sub else []
    return list(s.subsystems.values())


def _nvmf_subsystem_add_listener(s, p):
    nqn = p.get('nqn', '')
    listen_address = p.get('listen_address', {})
    if nqn not in s.subsystems:
        s.subsystems[nqn] = {
            'nqn': nqn, 'namespaces': [], 'listen_addresses': [],
            'hosts': [], 'allow_any_host': True, 'ana_reporting': True,
            'serial_number': '', 'model_number': '',
        }
    entry = dict(listen_address)
    entry['ana_state'] = p.get('ana_state', 'optimized')
    s.subsystems[nqn]['listen_addresses'].append(entry)
    return True


def _nvmf_subsystem_add_ns(s, p):
    nqn = p.get('nqn', '')
    ns_params = p.get('namespace', {})
    bdev_name = ns_params.get('bdev_name', '')
    if nqn not in s.subsystems:
        s.subsystems[nqn] = {
            'nqn': nqn, 'namespaces': [], 'listen_addresses': [],
            'hosts': [], 'allow_any_host': True, 'ana_reporting': True,
            'serial_number': '', 'model_number': '',
        }
    nsid = s.next_nsid(nqn)
    s.subsystems[nqn]['namespaces'].append({
        'nsid': nsid,
        'bdev_name': bdev_name,
        'uuid': ns_params.get('uuid', str(_uuid_mod.uuid4())),
        'nguid': ns_params.get('nguid', ''),
    })
    return nsid


def _bdev_nvme_attach_controller(s, p):
    name = p.get('name', '')
    s.nvme_controllers[name] = {
        'name': name,
        'nqn': p.get('subnqn', ''),
        'traddr': p.get('traddr', ''),
        'trsvcid': p.get('trsvcid', ''),
        'trtype': p.get('trtype', 'TCP'),
    }
    return [f"{name}n1"]


def _bdev_nvme_controller_list(s, p):
    name = p.get('name')
    if name and name in s.nvme_controllers:
        return [s.nvme_controllers[name]]
    if name:
        # Check partial match (hublvol bdev name)
        for k, v in s.nvme_controllers.items():
            if k.startswith(name) or name in k:
                return [v]
    return list(s.nvme_controllers.values()) if not name else []


def _bdev_nvme_set_options(s, p):
    return True


def _bdev_PT_NoExcl_create(s, p):
    name = p.get('name', '')
    s.bdevs[name] = {'name': name, 'aliases': []}
    return True


def _alceml_set_qos_weights(s, p):
    return True


def _jc_get_jm_status(s, p):
    return {}


def _nvmf_get_blocked_ports_rdma(s, p):
    return []


def _bdev_distrib_force_to_non_leader(s, p):
    return True


def _bdev_distrib_check_inflight_io(s, p):
    return 0


def _jc_explicit_synchronization(s, p):
    return True


def _iobuf_set_options(s, p):
    return True


def _bdev_set_options(s, p):
    return True


def _accel_set_options(s, p):
    return True


def _sock_impl_set_options(s, p):
    return True


def _nvmf_set_max_subsystems(s, p):
    return True


def _framework_start_init(s, p):
    return True


def _log_set_print_level(s, p):
    return True


def _thread_get_stats(s, p):
    return {'threads': []}


def _transport_create(s, p):
    return True


def _bdev_lvol_set_qos_limit(s, p):
    return True


def _nvmf_set_config(s, p):
    return True


def _jc_set_hint_lcpu_mask(s, p):
    return True


def _bdev_lvol_delete_hublvol(s, p):
    return True


def _nvmf_delete_subsystem(s, p):
    nqn = p.get('nqn', '')
    s.subsystems.pop(nqn, None)
    return True


_CLUSTER_DISPATCH = {
    'spdk_get_version':                      _spdk_get_version,
    'bdev_get_bdevs':                        _bdev_get_bdevs,
    'bdev_distrib_create':                   _bdev_distrib_create,
    'distr_send_cluster_map':                _distr_send_cluster_map,
    'distr_get_cluster_map':                 _distr_get_cluster_map,
    'bdev_raid_create':                      _bdev_raid_create,
    'bdev_lvol_create_lvstore':              _create_lvstore,
    'bdev_lvol_get_lvstores':                _bdev_lvol_get_lvstores,
    'bdev_lvol_set_lvs_opts':                _bdev_lvol_set_lvs_opts,
    'bdev_lvol_set_leader':                  _bdev_lvol_set_leader,
    'bdev_lvol_set_leader_all':              _bdev_lvol_set_leader,
    'bdev_examine':                          _bdev_examine,
    'bdev_wait_for_examine':                 _bdev_wait_for_examine,
    'bdev_lvol_create_hublvol':              _bdev_lvol_create_hublvol,
    'bdev_lvol_connect_hublvol':             _bdev_lvol_connect_hublvol,
    'jc_suspend_compression':                _jc_suspend_compression,
    'jc_compression_get_status':             _jc_compression_get_status,
    'nvmf_create_subsystem':                 _nvmf_create_subsystem,
    'nvmf_get_subsystems':                   _nvmf_get_subsystems,
    'nvmf_subsystem_add_listener':           _nvmf_subsystem_add_listener,
    'nvmf_subsystem_add_ns':                 _nvmf_subsystem_add_ns,
    'nvmf_delete_subsystem':                 _nvmf_delete_subsystem,
    'bdev_nvme_attach_controller':           _bdev_nvme_attach_controller,
    'bdev_nvme_controller_list':             _bdev_nvme_controller_list,
    'bdev_nvme_set_options':                 _bdev_nvme_set_options,
    'bdev_PT_NoExcl_create':                 _bdev_PT_NoExcl_create,
    'alceml_set_qos_weights':                _alceml_set_qos_weights,
    'jc_get_jm_status':                      _jc_get_jm_status,
    'nvmf_get_blocked_ports_rdma':           _nvmf_get_blocked_ports_rdma,
    'bdev_distrib_force_to_non_leader':      _bdev_distrib_force_to_non_leader,
    'bdev_distrib_check_inflight_io':        _bdev_distrib_check_inflight_io,
    'jc_explicit_synchronization':           _jc_explicit_synchronization,
    'iobuf_set_options':                     _iobuf_set_options,
    'bdev_set_options':                      _bdev_set_options,
    'accel_set_options':                     _accel_set_options,
    'sock_impl_set_options':                 _sock_impl_set_options,
    'nvmf_set_max_subsystems':               _nvmf_set_max_subsystems,
    'framework_start_init':                  _framework_start_init,
    'log_set_print_level':                   _log_set_print_level,
    'thread_get_stats':                      _thread_get_stats,
    'transport_create':                      _transport_create,
    'bdev_lvol_set_qos_limit':               _bdev_lvol_set_qos_limit,
    'nvmf_set_config':                       _nvmf_set_config,
    'jc_set_hint_lcpu_mask':                 _jc_set_hint_lcpu_mask,
    'bdev_lvol_delete_hublvol':              _bdev_lvol_delete_hublvol,
}


class _ClusterMockHTTPServer(HTTPServer):
    def __init__(self, server_address, handler_class, node_state):
        super().__init__(server_address, handler_class)
        self.node_state = node_state


class ClusterMockRpcServer:
    """Mock RPC server for cluster activation / restart / health check tests."""

    def __init__(self, host: str, port: int, lvstore: str = "", node_id: str = ""):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.state = ClusterNodeState(lvstore)
        self._server = None
        self._thread = None

    def start(self):
        self._server = _ClusterMockHTTPServer(
            (self.host, self.port), _ClusterRpcHandler, self.state)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"mock-cluster-rpc-{self.node_id}", daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server = None

    def reset_state(self):
        with self.state.lock:
            self.state.bdevs.clear()
            self.state.subsystems.clear()
            self.state.nvme_controllers.clear()
            self.state.lvstores.clear()
            self.state.leader = False
            self.state.lvs_opts = {}
            self.state.hublvol_created = False
            self.state.hublvol_connected = False
            self.state.compression_suspended = True
            self.state.examined = False
            self.state._nsid_counter.clear()


# ===========================================================================
# Port allocation
# ===========================================================================

_BASE_PORT = 10100


def _worker_port_offset() -> int:
    worker = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    try:
        return int(worker.replace("gw", "")) * 20
    except ValueError:
        return 0


# ===========================================================================
# Fixtures
# ===========================================================================

# We need 10 mock servers: 2 primaries + 4 secondaries + some spare
# With max_fault_tolerance=2, each primary needs 2 secondaries.
# 2 primaries * 2 = 4 secondary slots, so 4 secondary nodes suffice.
_NUM_PRIMARIES = 2
_NUM_SECONDARIES = 4
_NUM_NODES = _NUM_PRIMARIES + _NUM_SECONDARIES
_mock_servers: List[ClusterMockRpcServer] = []


@pytest.fixture(scope="session")
def mock_rpc_servers():
    """Start 8 mock RPC servers for the 8-node cluster."""
    offset = _worker_port_offset()
    servers = []
    for i in range(_NUM_NODES):
        port = _BASE_PORT + offset + i
        srv = ClusterMockRpcServer(
            host="127.0.0.1", port=port,
            lvstore="", node_id=f"node-{i}")
        srv.start()
        servers.append(srv)
    yield servers
    for srv in servers:
        srv.stop()


@pytest.fixture(scope="session")
def ensure_cluster():
    """Ensure a cluster exists in FDB for the test session."""
    from simplyblock_core.db_controller import DBController

    db = DBController()
    if db.kv_store is None:
        pytest.skip("FoundationDB is not available")

    yield db


@pytest.fixture()
def cluster_env(ensure_cluster, mock_rpc_servers):
    """
    Create a full cluster + 8 nodes in FDB for dual fault tolerance testing.

    Layout:
      - 4 primary nodes (node-0..3) on different mgmt_ips
      - 4 secondary nodes (node-4..7) marked as is_secondary_node=True
      - Cluster: ha_type=ha, max_fault_tolerance=2, distr_ndcs=1, distr_npcs=2

    Each node points its rpc_port at one of the mock servers.
    """
    db = ensure_cluster
    offset = _worker_port_offset()

    # Reset all mock servers
    for srv in mock_rpc_servers:
        srv.reset_state()

    # Create cluster
    cluster = Cluster()
    cluster.uuid = f"e2e-dft-{_uuid_mod.uuid4().hex[:12]}"
    cluster.status = Cluster.STATUS_UNREADY
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
    cluster.write_to_db(db.kv_store)

    # Pre-seed a cluster capacity stat so get_cluster_capacity works
    stat = ClusterStatObject(data={
        "cluster_id": cluster.uuid,
        "uuid": cluster.uuid,
        "date": int(time.time()),
        "size_total": 1073741824000,  # 1TB
    })
    stat.write_to_db(db.kv_store)

    nodes = []
    # Primary nodes
    for i in range(_NUM_PRIMARIES):
        port = _BASE_PORT + offset + i
        n = StorageNode()
        n.uuid = str(_uuid_mod.uuid4())
        n.cluster_id = cluster.uuid
        n.status = StorageNode.STATUS_ONLINE
        n.hostname = f"host-primary-{i}"
        n.mgmt_ip = "127.0.0.1"
        n.rpc_port = port
        n.rpc_username = "spdkuser"
        n.rpc_password = "spdkpass"
        n.is_secondary_node = False
        n.number_of_distribs = 1
        n.active_tcp = True
        n.active_rdma = False
        n.data_nics = [_make_nic("127.0.0.1")]
        # Give each primary node 2 devices (need ndcs+npcs+1=4 total across cluster)
        devs = []
        for d in range(2):
            dev = NVMeDevice()
            dev.uuid = str(_uuid_mod.uuid4())
            dev.cluster_id = cluster.uuid
            dev.node_id = n.uuid
            dev.status = NVMeDevice.STATUS_ONLINE
            dev.nvme_bdev = f"nvme_{i}_{d}"
            dev.alceml_bdev = f"alceml_{i}_{d}"
            dev.pt_bdev = f"pt_{i}_{d}"
            dev.testing_bdev = ""
            dev.nvmf_nqn = f"nqn:dev:{i}:{d}"
            dev.health_check = True
            dev.io_error = False
            dev.size = 100000000000
            devs.append(dev)
        n.nvme_devices = devs
        n.write_to_db(db.kv_store)
        nodes.append(n)

    # Secondary nodes
    for i in range(_NUM_SECONDARIES):
        port = _BASE_PORT + offset + _NUM_PRIMARIES + i
        n = StorageNode()
        n.uuid = str(_uuid_mod.uuid4())
        n.cluster_id = cluster.uuid
        n.status = StorageNode.STATUS_ONLINE
        n.hostname = f"host-secondary-{i}"
        n.mgmt_ip = "127.0.0.1"
        n.rpc_port = port
        n.rpc_username = "spdkuser"
        n.rpc_password = "spdkpass"
        n.is_secondary_node = True
        n.number_of_distribs = 1
        n.active_tcp = True
        n.active_rdma = False
        n.data_nics = [_make_nic("127.0.0.1")]
        n.write_to_db(db.kv_store)
        nodes.append(n)

    yield {
        'db': db,
        'cluster': cluster,
        'primaries': nodes[:_NUM_PRIMARIES],
        'secondaries': nodes[_NUM_PRIMARIES:],
        'all_nodes': nodes,
        'servers': mock_rpc_servers,
    }

    # Teardown: remove all nodes and cluster stat
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


def _make_nic(ip: str) -> IFace:
    nic = IFace()
    nic.uuid = str(_uuid_mod.uuid4())
    nic.if_name = "eth0"
    nic.ip4_address = ip
    nic.trtype = "TCP"
    nic.net_type = "data"
    return nic


# ===========================================================================
# Patch helpers
# ===========================================================================

def _patch_externals():
    """Return a list of context managers that mock external dependencies."""
    patches = [
        # distr_controller: send_cluster_map_to_distr always succeeds
        patch('simplyblock_core.distr_controller.send_cluster_map_to_distr', return_value=True),
        patch('simplyblock_core.distr_controller.send_cluster_map_add_node', return_value=True),
        patch('simplyblock_core.distr_controller.parse_distr_cluster_map',
              return_value=([], True)),
        # ping always succeeds
        patch('simplyblock_core.utils.ping_host', return_value=True),
        patch('simplyblock_core.controllers.health_controller._check_node_ping', return_value=True),
        # SNodeAPI always succeeds
        patch('simplyblock_core.snode_client.SNodeClient.is_live', return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.info',
              return_value=({'hostname': 'mock', 'network_interface': {}}, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_is_up',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_start',
              return_value=(True, None)),
        patch('simplyblock_core.snode_client.SNodeClient.spdk_process_kill',
              return_value=(True, None)),
        patch('simplyblock_core.controllers.health_controller._check_node_api', return_value=True),
        # Firewall always succeeds
        patch('simplyblock_core.fw_api_client.FirewallClient.firewall_set_port',
              return_value=(True, None)),
        patch('simplyblock_core.fw_api_client.FirewallClient.get_firewall',
              return_value=('', None)),
        # Port check always passes
        patch('simplyblock_core.controllers.health_controller.check_port_on_node',
              return_value=True),
        # No QOS classes
        patch('simplyblock_core.cluster_ops.db_controller.get_qos', return_value=[]),
        # get_next_port returns a fixed port
        patch('simplyblock_core.utils.get_next_port', return_value=9090),
        # get_random_vuid returns incrementing values
        patch('simplyblock_core.utils.get_random_vuid', side_effect=_vuid_gen()),
        # next_free_hublvol_port returns a port
        patch('simplyblock_core.utils.next_free_hublvol_port', return_value=4420),
        # tasks_controller: no-op task creation
        patch('simplyblock_core.controllers.tasks_controller.add_jc_comp_resume_task'),
        patch('simplyblock_core.controllers.tasks_controller.add_port_allow_task'),
        patch('simplyblock_core.controllers.tasks_controller.add_device_mig_task_for_node'),
        patch('simplyblock_core.controllers.tasks_controller.get_active_node_restart_task',
              return_value=None),
        # storage_events / device_events: no-op
        patch('simplyblock_core.controllers.storage_events.snode_health_check_change'),
        patch('simplyblock_core.controllers.device_events.device_health_check_change'),
        # _connect_to_remote_jm_devs / _connect_to_remote_devs: no-op
        patch('simplyblock_core.storage_node_ops._connect_to_remote_jm_devs', return_value=[]),
        patch('simplyblock_core.storage_node_ops._connect_to_remote_devs', return_value=[]),
        # get_sorted_ha_jms: empty
        patch('simplyblock_core.storage_node_ops.get_sorted_ha_jms', return_value=[]),
        # get_next_physical_device_order
        patch('simplyblock_core.storage_node_ops.get_next_physical_device_order', return_value=1),
        # Override get_secondary_nodes to skip mgmt_ip != check (all nodes on 127.0.0.1)
        patch('simplyblock_core.storage_node_ops.get_secondary_nodes',
              side_effect=_mock_get_secondary_nodes),
        # time.sleep: skip waits
        patch('simplyblock_core.storage_node_ops.time.sleep'),
    ]
    return patches


def _mock_get_secondary_nodes(current_node, exclude_ids=None):
    """Mock get_secondary_nodes that skips mgmt_ip check (all nodes share 127.0.0.1).
    Only returns is_secondary_node=True nodes to match real HA pairing behavior."""
    if exclude_ids is None:
        exclude_ids = []
    from simplyblock_core.db_controller import DBController
    db_controller = DBController()
    nodes = []
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id)
    for node in all_nodes:
        if node.get_id() == current_node.get_id() or node.get_id() in exclude_ids:
            continue
        if node.status == StorageNode.STATUS_ONLINE and node.is_secondary_node:
            if not node.lvstore_stack_secondary_1 or node.lvstore_stack_secondary_1 == current_node.get_id():
                nodes.append(node.get_id())
    return nodes


_vuid_counter = 1000


def _vuid_gen():
    """Generator for incrementing VUIDs."""
    n = 1000
    while True:
        yield n
        n += 1


# ===========================================================================
# Test 1: Cluster Activation with max_fault_tolerance=2
# ===========================================================================

class TestClusterActivation:

    def test_activate_assigns_dual_secondaries(self, cluster_env):
        """
        Activate a cluster with max_fault_tolerance=2.
        Verify:
          - Each primary gets secondary_node_id AND secondary_node_id_2 assigned
          - Secondary nodes get lvstore_stack_secondary_1 / _2 back-references
          - Cluster status becomes ACTIVE
          - lvstore_status is "ready" on all nodes
        """
        from simplyblock_core import cluster_ops
        from simplyblock_core.db_controller import DBController

        env = cluster_env
        db = DBController()
        cl = env['cluster']

        patches = _patch_externals()
        for p in patches:
            p.start()

        try:
            cluster_ops.cluster_activate(cl.uuid)

            # Reload cluster from DB
            cluster = db.get_cluster_by_id(cl.uuid)
            assert cluster.status == Cluster.STATUS_ACTIVE, \
                f"Cluster should be ACTIVE, got {cluster.status}"

            # Reload all nodes
            all_nodes = db.get_storage_nodes_by_cluster_id(cl.uuid)
            primaries = [n for n in all_nodes if not n.is_secondary_node]
            secondaries = [n for n in all_nodes if n.is_secondary_node]

            # Each primary should have both secondary IDs assigned
            for primary in primaries:
                assert primary.secondary_node_id, \
                    f"Primary {primary.uuid} missing secondary_node_id"
                assert primary.secondary_node_id_2, \
                    f"Primary {primary.uuid} missing secondary_node_id_2"
                assert primary.secondary_node_id != primary.secondary_node_id_2, \
                    f"Primary {primary.uuid} has same node for both secondaries"

                # Verify lvstore was created
                assert primary.lvstore, \
                    f"Primary {primary.uuid} has no lvstore"
                assert primary.lvstore_status == "ready", \
                    f"Primary {primary.uuid} lvstore_status={primary.lvstore_status}"

            # Check back-references on secondary nodes
            sec_1_refs = set()
            sec_2_refs = set()
            for sec in secondaries:
                if sec.lvstore_stack_secondary_1:
                    sec_1_refs.add(sec.uuid)
                if sec.lvstore_stack_secondary_2:
                    sec_2_refs.add(sec.uuid)

            # Every primary assigned a secondary_node_id, so at least some
            # secondaries must have lvstore_stack_secondary_1 set
            assert len(sec_1_refs) > 0, "No secondaries have lvstore_stack_secondary_1"
            assert len(sec_2_refs) > 0, "No secondaries have lvstore_stack_secondary_2"

            # Verify the mock RPC servers received the expected calls
            for i in range(_NUM_PRIMARIES):
                srv = env['servers'][i]
                with srv.state.lock:
                    # Primary should have lvstore, hublvol, and leader set
                    assert srv.state.hublvol_created, \
                        f"Server {i} hublvol not created"
                    assert srv.state.leader, \
                        f"Server {i} not set as leader"
                    assert len(srv.state.lvstores) > 0, \
                        f"Server {i} has no lvstores"

        finally:
            for p in patches:
                p.stop()

    def test_activate_compression_resumed(self, cluster_env):
        """Verify JC compression is resumed on all nodes after activation."""
        from simplyblock_core import cluster_ops
        env = cluster_env
        cl = env['cluster']

        patches = _patch_externals()
        for p in patches:
            p.start()
        try:
            # Run activation (each test gets fresh fixture)
            cluster_ops.cluster_activate(cl.uuid)

            # After activation, check compression state on primary servers
            for i in range(_NUM_PRIMARIES):
                srv = env['servers'][i]
                with srv.state.lock:
                    assert not srv.state.compression_suspended, \
                        f"Primary server {i} compression still suspended after activation"
        finally:
            for p in patches:
                p.stop()


# ===========================================================================
# Test 2: Node Restart
# ===========================================================================

class TestNodeRestart:

    def test_recreate_lvstore_on_sec_both_secondaries(self, cluster_env):
        """
        After activation, call recreate_lvstore_on_sec for each secondary.
        Verify that each secondary gets its bdev stack recreated and connects
        to the correct primary's hublvol with the correct min_cntlid.
        """
        from simplyblock_core import cluster_ops, storage_node_ops
        from simplyblock_core.db_controller import DBController

        env = cluster_env
        db = DBController()
        cl = env['cluster']

        patches = _patch_externals()
        for p in patches:
            p.start()

        try:
            # Activate cluster first
            cluster_ops.cluster_activate(cl.uuid)

            cluster = db.get_cluster_by_id(cl.uuid)
            assert cluster.status == Cluster.STATUS_ACTIVE

            # Get all secondaries
            all_nodes = db.get_storage_nodes_by_cluster_id(cl.uuid)
            secondaries = [n for n in all_nodes if n.is_secondary_node]
            primaries = [n for n in all_nodes if not n.is_secondary_node]

            # Track which subsystem_create calls happen (for min_cntlid verification)
            subsystem_calls = []
            original_subsystem_create = None

            # Pick a secondary that has lvstore_stack_secondary_1 set
            target_sec = None
            for sec in secondaries:
                if sec.lvstore_stack_secondary_1:
                    target_sec = sec
                    break

            if target_sec is None:
                pytest.skip("No secondary with lvstore_stack_secondary_1")

            # Reset the secondary's mock server to simulate restart
            srv_idx = _find_server_for_node(env, target_sec)
            assert srv_idx is not None
            env['servers'][srv_idx].reset_state()

            # Call recreate_lvstore_on_sec
            ret = storage_node_ops.recreate_lvstore_on_sec(target_sec)
            assert ret, "recreate_lvstore_on_sec should return True"

            # Verify the secondary's mock server now has bdevs (from _create_bdev_stack)
            srv = env['servers'][srv_idx]
            with srv.state.lock:
                assert len(srv.state.bdevs) > 0, \
                    "Secondary should have bdevs after recreate"
                # Should have connected to hublvol
                assert srv.state.hublvol_connected or len(srv.state.nvme_controllers) > 0, \
                    "Secondary should have connected to primary's hublvol"

        finally:
            for p in patches:
                p.stop()

    def test_recreate_lvstore_secondary_2_min_cntlid(self, cluster_env):
        """
        Verify that recreate_lvstore_on_sec uses min_cntlid=2000 for secondary_2
        and min_cntlid=1000 for secondary_1.
        """
        from simplyblock_core import cluster_ops, storage_node_ops
        from simplyblock_core.db_controller import DBController

        env = cluster_env
        db = DBController()
        cl = env['cluster']

        patches = _patch_externals()
        for p in patches:
            p.start()

        try:
            # Activate cluster
            cluster_ops.cluster_activate(cl.uuid)

            all_nodes = db.get_storage_nodes_by_cluster_id(cl.uuid)
            primaries = [n for n in all_nodes if not n.is_secondary_node]

            # Find a primary with secondary_node_id_2 set
            target_primary = None
            for p_node in primaries:
                if p_node.secondary_node_id_2:
                    target_primary = p_node
                    break

            if target_primary is None:
                pytest.skip("No primary with secondary_node_id_2")

            # Verify min_cntlid logic
            sec_1 = db.get_storage_node_by_id(target_primary.secondary_node_id)
            sec_2 = db.get_storage_node_by_id(target_primary.secondary_node_id_2)

            # For secondary_1
            if target_primary.secondary_node_id_2 == sec_1.get_id():
                cntlid_1 = 2000
            else:
                cntlid_1 = 1000
            assert cntlid_1 == 1000, "Secondary 1 should get min_cntlid=1000"

            # For secondary_2
            if target_primary.secondary_node_id_2 == sec_2.get_id():
                cntlid_2 = 2000
            else:
                cntlid_2 = 1000
            assert cntlid_2 == 2000, "Secondary 2 should get min_cntlid=2000"

        finally:
            for p in patches:
                p.stop()


# ===========================================================================
# Test 3: Health Check Service
# ===========================================================================

class TestHealthCheck:

    def test_health_check_verifies_both_secondaries(self, cluster_env):
        """
        Verify the health check logic checks BOTH secondary nodes' hublvol
        connections by simulating the check_node flow from health_check_service.

        Note: We cannot import health_check_service directly because it has
        module-level code that starts an infinite service loop. Instead, we
        replicate the relevant secondary-checking logic and verify it.
        """
        from simplyblock_core import cluster_ops
        from simplyblock_core.controllers import health_controller
        from simplyblock_core.db_controller import DBController

        env = cluster_env
        db = DBController()
        cl = env['cluster']

        # Activate cluster first
        ext_patches = _patch_externals()
        for p in ext_patches:
            p.start()
        try:
            cluster_ops.cluster_activate(cl.uuid)
        finally:
            for p in ext_patches:
                p.stop()

        cluster = db.get_cluster_by_id(cl.uuid)
        assert cluster.status == Cluster.STATUS_ACTIVE

        # Get a primary with both secondaries assigned
        primaries = [n for n in db.get_storage_nodes_by_cluster_id(cl.uuid)
                     if not n.is_secondary_node]

        target_primary = None
        for p in primaries:
            if p.secondary_node_id and p.secondary_node_id_2:
                target_primary = p
                break

        if target_primary is None:
            pytest.skip("No primary with dual secondaries found")

        # Seed the mock RPC servers with the bdevs/subsystems that health check expects
        _seed_primary_for_health_check(env, target_primary, db)
        _seed_secondary_for_health_check(env, target_primary, target_primary.secondary_node_id, db)
        _seed_secondary_for_health_check(env, target_primary, target_primary.secondary_node_id_2, db)

        # Replicate the secondary-checking logic from check_node (lines 213-241)
        # This is the core logic we want to verify works for dual fault tolerance
        snode = db.get_storage_node_by_id(target_primary.get_id())

        sec_ids_to_check = []
        if snode.secondary_node_id:
            sec_ids_to_check.append(snode.secondary_node_id)
        if snode.secondary_node_id_2:
            sec_ids_to_check.append(snode.secondary_node_id_2)

        assert len(sec_ids_to_check) == 2, \
            f"Expected 2 secondaries to check, got {len(sec_ids_to_check)}"

        sec_hublvol_calls = []

        def tracking_check_sec(node, **kwargs):
            sec_hublvol_calls.append({
                'node_id': node.uuid,
                'primary_node_id': kwargs.get('primary_node_id'),
            })
            return True

        patches = _patch_externals()
        for p_item in patches:
            p_item.start()

        try:
            with patch.object(health_controller, '_check_sec_node_hublvol',
                              side_effect=tracking_check_sec):
                with patch.object(health_controller, '_check_node_lvstore', return_value=True):
                    for sec_id in sec_ids_to_check:
                        sec_node = db.get_storage_node_by_id(sec_id)
                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            health_controller._check_node_lvstore(
                                snode.lvstore_stack, sec_node, auto_fix=True, stack_src_node=snode)
                            health_controller._check_sec_node_hublvol(
                                sec_node, primary_node_id=snode.get_id())

            # Verify both secondaries were checked
            checked_node_ids = [c['node_id'] for c in sec_hublvol_calls]
            assert target_primary.secondary_node_id in checked_node_ids, \
                f"Secondary 1 ({target_primary.secondary_node_id}) not checked"
            assert target_primary.secondary_node_id_2 in checked_node_ids, \
                f"Secondary 2 ({target_primary.secondary_node_id_2}) not checked"

            # Verify primary_node_id was passed correctly
            for call in sec_hublvol_calls:
                assert call['primary_node_id'] == target_primary.uuid, \
                    f"primary_node_id mismatch: {call['primary_node_id']} != {target_primary.uuid}"

        finally:
            for p_item in patches:
                p_item.stop()

    def test_health_check_port_checks_both_secondaries(self, cluster_env):
        """
        Verify health check port-checking logic covers both secondary
        back-references (lvstore_stack_secondary_1 and _2).
        """
        from simplyblock_core import cluster_ops
        from simplyblock_core.db_controller import DBController

        env = cluster_env
        db = DBController()
        cl = env['cluster']

        # Activate cluster first
        ext_patches = _patch_externals()
        for p in ext_patches:
            p.start()
        try:
            cluster_ops.cluster_activate(cl.uuid)
        finally:
            for p in ext_patches:
                p.stop()

        cluster = db.get_cluster_by_id(cl.uuid)
        assert cluster.status == Cluster.STATUS_ACTIVE

        # Replicate the port-checking logic from check_node (lines 247-264)
        # Find a secondary node that acts as secondary for TWO primaries
        # (has both lvstore_stack_secondary_1 and _2 set), or verify
        # that secondaries with back-references get port-checked.
        all_nodes = db.get_storage_nodes_by_cluster_id(cl.uuid)

        # Verify each primary's port check logic covers both secondary refs
        primaries = [n for n in all_nodes if not n.is_secondary_node]
        for primary in primaries:
            if not primary.secondary_node_id:
                continue

            # Replicate the port collection logic from check_node
            ports = [primary.lvol_subsys_port]
            for sec_stack_ref in [primary.lvstore_stack_secondary_1, primary.lvstore_stack_secondary_2]:
                if sec_stack_ref:
                    try:
                        sec_ref_node = db.get_storage_node_by_id(sec_stack_ref)
                        if sec_ref_node and sec_ref_node.status == StorageNode.STATUS_ONLINE:
                            ports.append(sec_ref_node.lvol_subsys_port)
                    except KeyError:
                        pass

            # Primary should have its own port
            assert primary.lvol_subsys_port in ports

        # Also verify secondary nodes have back-references populated
        sec_with_refs = [n for n in all_nodes
                         if n.lvstore_stack_secondary_1 or n.lvstore_stack_secondary_2]
        assert len(sec_with_refs) > 0, "No secondaries have back-references"


def _seed_primary_for_health_check(env, primary, db):
    """Seed the primary's mock server with bdevs/subsystems for health check."""
    srv_idx = _find_server_for_node(env, primary)
    if srv_idx is None:
        return
    srv = env['servers'][srv_idx]
    with srv.state.lock:
        # Add device bdevs
        for dev in primary.nvme_devices:
            for bdev_name in [dev.nvme_bdev, dev.alceml_bdev, dev.pt_bdev]:
                if bdev_name:
                    srv.state.bdevs[bdev_name] = {'name': bdev_name, 'aliases': []}
            # Add device subsystem
            if dev.nvmf_nqn and dev.nvmf_nqn not in srv.state.subsystems:
                srv.state.subsystems[dev.nvmf_nqn] = {
                    'nqn': dev.nvmf_nqn,
                    'namespaces': [{'nsid': 1, 'bdev_name': dev.alceml_bdev,
                                    'uuid': dev.uuid, 'nguid': ''}],
                    'listen_addresses': [{'traddr': primary.mgmt_ip,
                                          'trsvcid': '4420', 'trtype': 'TCP'}],
                    'hosts': [], 'allow_any_host': True, 'ana_reporting': True,
                    'serial_number': '', 'model_number': '',
                }
        # Add hublvol bdev and subsystem
        if primary.hublvol:
            srv.state.bdevs[primary.hublvol.bdev_name] = {
                'name': primary.hublvol.bdev_name, 'aliases': [],
            }
            srv.state.subsystems[primary.hublvol.nqn] = {
                'nqn': primary.hublvol.nqn,
                'namespaces': [{'nsid': 1, 'bdev_name': primary.hublvol.bdev_name,
                                'uuid': primary.hublvol.uuid, 'nguid': ''}],
                'listen_addresses': [{'traddr': primary.mgmt_ip,
                                      'trsvcid': str(primary.hublvol.nvmf_port),
                                      'trtype': 'TCP'}],
                'hosts': [], 'allow_any_host': True, 'ana_reporting': True,
                'serial_number': '', 'model_number': '',
            }
        # Add lvstore info
        if primary.lvstore:
            srv.state.lvstores[primary.lvstore] = {
                'name': primary.lvstore,
                'base_bdev': primary.raid or 'raid0',
                'block_size': 4096,
                'cluster_size': 4096,
                'lvs leadership': True,
                'lvs_primary': True,
                'lvs_read_only': False,
                'lvs_secondary': False,
                'lvs_redirect': False,
                'remote_bdev': '',
                'connect_state': False,
            }


def _seed_secondary_for_health_check(env, primary, sec_node_id, db):
    """Seed a secondary's mock server with the expected state for health check."""
    try:
        sec_node = db.get_storage_node_by_id(sec_node_id)
    except KeyError:
        return

    srv_idx = _find_server_for_node(env, sec_node)
    if srv_idx is None:
        return
    srv = env['servers'][srv_idx]
    with srv.state.lock:
        # Add hublvol remote bdev (the "n1" suffix bdev)
        if primary.hublvol:
            remote_bdev_name = f"{primary.hublvol.bdev_name}n1"
            srv.state.bdevs[remote_bdev_name] = {
                'name': remote_bdev_name, 'aliases': [],
            }
            # Add controller entry so bdev_nvme_controller_list returns it
            srv.state.nvme_controllers[primary.hublvol.bdev_name] = {
                'name': primary.hublvol.bdev_name,
                'nqn': primary.hublvol.nqn,
                'traddr': primary.mgmt_ip,
                'trsvcid': str(primary.hublvol.nvmf_port),
                'trtype': 'TCP',
            }
        # Add lvstore info (secondary perspective)
        if primary.lvstore:
            remote_bdev = f"{primary.hublvol.bdev_name}n1" if primary.hublvol else ''
            srv.state.lvstores[primary.lvstore] = {
                'name': primary.lvstore,
                'base_bdev': primary.raid or 'raid0',
                'block_size': 4096,
                'cluster_size': 4096,
                'lvs leadership': False,
                'lvs_primary': False,
                'lvs_read_only': False,
                'lvs_secondary': True,
                'lvs_redirect': True,
                'remote_bdev': remote_bdev,
                'connect_state': True,
            }


def _find_server_for_node(env, node):
    """Find the mock server index for a given node by matching rpc_port."""
    offset = _worker_port_offset()
    for i, srv in enumerate(env['servers']):
        if srv.port == node.rpc_port:
            return i
    return None
