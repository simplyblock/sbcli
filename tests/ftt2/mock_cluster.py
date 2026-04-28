# coding=utf-8
"""
mock_cluster.py – extended mock RPC server for FTT=2 restart testing.

Extends the ClusterMockRpcServer from test_dual_ft_e2e with:
  - Dynamic quorum responses (per-node JM connectivity configurable per test)
  - Per-LVS leadership tracking
  - Port block/unblock tracking with ownership (restart vs fabric_error)
  - Configurable inflight-IO responses
  - Node availability simulation (can make RPCs fail for offline/unreachable nodes)
"""

import json
import logging
import threading
import time
import uuid as _uuid_mod
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-node state
# ---------------------------------------------------------------------------

class FTT2NodeState:
    """In-memory state for one mock node, extended for restart testing."""

    def __init__(self, node_id: str, lvstore: str = ""):
        self.node_id = node_id
        self.lvstore = lvstore

        # Basic bdev/subsystem state (same as ClusterNodeState)
        self.bdevs: Dict[str, dict] = {}
        self.subsystems: Dict[str, dict] = {}
        self.nvme_controllers: Dict[str, dict] = {}
        self.lvstores: Dict[str, dict] = {}
        self.lvs_opts: dict = {}
        self.compression_suspended: bool = True
        self.examined: bool = False
        self._nsid_counter: Dict[str, int] = {}

        # --- Dynamic state for restart testing ---

        # Leadership per LVS: lvstore_name -> bool
        self.leadership: Dict[str, bool] = {}

        # Hublvol state per LVS
        self.hublvols_created: Set[str] = set()    # lvstore names
        self.hublvols_connected: Set[str] = set()   # lvstore names

        # NVMe controller path tracking: controller_name -> list of path dicts
        # Each entry: {nqn, traddr, trsvcid, trtype, multipath}
        # Used to verify: how many paths a controller has, whether multipath was requested
        self.nvme_controller_paths: Dict[str, list] = {}

        # Port blocking: (port, port_type) -> {"blocker": "restart"|"fabric_error", "ts": float}
        self.blocked_ports: Dict[tuple, dict] = {}

        # JM connectivity: remote_node_id -> bool (True = connected)
        # Configurable per test to control quorum check responses
        self.jm_connectivity: Dict[str, bool] = {}

        # Inflight IO: jm_vuid -> bool (True = has inflight IO)
        self.inflight_io: Dict[int, bool] = {}

        # Whether this node is reachable (False = all RPCs fail)
        self.reachable: bool = True
        # Fabric available (affects jm_connectivity reports from peers)
        self.fabric_up: bool = True

        # RPC call log: list of (timestamp, method, params)
        self.rpc_log: List[tuple] = []

        # Error injection: method_name -> error_message
        # Any method listed here will return an RPC error instead of its normal result.
        self.failing_methods: Dict[str, str] = {}

        # Phase gate for concurrent operation tests (set by test code)
        self._phase_gate = None  # type: ignore

        # RPC hook for mid-restart state changes (set by test code)
        self._rpc_hook = None

        self.lock = threading.Lock()

    def next_nsid(self, nqn: str) -> int:
        self._nsid_counter.setdefault(nqn, 1)
        nsid = self._nsid_counter[nqn]
        self._nsid_counter[nqn] += 1
        return nsid

    def reset(self):
        """Reset all state for a new test."""
        self.bdevs.clear()
        self.subsystems.clear()
        self.nvme_controllers.clear()
        self.lvstores.clear()
        self.lvs_opts = {}
        self.compression_suspended = True
        self.examined = False
        self._nsid_counter.clear()
        self.leadership.clear()
        self.hublvols_created.clear()
        self.hublvols_connected.clear()
        self.blocked_ports.clear()
        self.jm_connectivity.clear()
        self.inflight_io.clear()
        self.nvme_controller_paths.clear()
        self.reachable = True
        self.fabric_up = True
        self.rpc_log.clear()
        self.failing_methods.clear()


# ---------------------------------------------------------------------------
# RPC error
# ---------------------------------------------------------------------------

class _RpcError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# RPC handler implementations — dynamic (quorum, leadership, ports)
# ---------------------------------------------------------------------------

def _jc_get_jm_status(s: FTT2NodeState, p: dict):
    """Return JM connectivity status for this node's perspective.

    The restart code checks `remote_jm_{node_id}n1` keys in the response
    to determine if a peer's JM is connected.
    """
    result = {}
    for remote_id, connected in s.jm_connectivity.items():
        result[f"remote_jm_{remote_id}n1"] = connected
    return result


def _bdev_lvol_set_leader(s: FTT2NodeState, p: dict):
    """Track leadership per LVS."""
    lvs_name = p.get('lvs', s.lvstore)
    leader = p.get('lvs_leadership', p.get('leader', False))
    s.leadership[lvs_name] = leader
    return True


def _bdev_distrib_force_to_non_leader(s: FTT2NodeState, p: dict):
    """Force a distrib to non-leader.  Track via jm_vuid."""
    # The restart code calls this with jm_vuid as the identifier
    return True


def _bdev_distrib_check_inflight_io(s: FTT2NodeState, p: dict):
    """Return inflight IO status.  Configurable per test."""
    jm_vuid = p.get('jm_vuid', p.get('name', 0))
    return 1 if s.inflight_io.get(jm_vuid, False) else 0


def _bdev_lvol_create_hublvol(s: FTT2NodeState, p: dict):
    lvs_name = p.get('lvs', p.get('lvs_name', s.lvstore))
    s.hublvols_created.add(lvs_name)
    return str(_uuid_mod.uuid4())


def _bdev_lvol_connect_hublvol(s: FTT2NodeState, p: dict):
    lvs_name = p.get('lvs', p.get('lvs_name', s.lvstore))
    s.hublvols_connected.add(lvs_name)
    if lvs_name in s.lvstores:
        s.lvstores[lvs_name]['connect_state'] = True
    return True


def _bdev_lvol_delete_hublvol(s: FTT2NodeState, p: dict):
    return True


# ---------------------------------------------------------------------------
# RPC handler implementations — static mocks
# ---------------------------------------------------------------------------

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
    s.bdevs[name] = {'name': name, 'aliases': [], 'driver_specific': {'distrib': True}}
    return True


def _distr_send_cluster_map(s, p):
    return True


def _distr_get_cluster_map(s, p):
    return {'map_cluster': [], 'map_prob': [], 'name': p.get('name', '')}


def _bdev_raid_create(s, p):
    name = p.get('name', 'raid0')
    s.bdevs[name] = {'name': name, 'aliases': [], 'driver_specific': {'raid': True}}
    return True


def _create_lvstore(s, p):
    name = p.get('name', 'LVS')
    s.lvstore = name
    s.lvstores[name] = {
        'name': name, 'base_bdev': p.get('bdev_name', ''),
        'block_size': 4096, 'cluster_size': p.get('cluster_sz', 4096),
        'lvs leadership': True, 'lvs_primary': True, 'lvs_read_only': False,
        'lvs_secondary': False, 'lvs_redirect': False,
        'remote_bdev': '', 'connect_state': False,
    }
    return name


def _bdev_lvol_get_lvstores(s, p):
    name = p.get('name', '')
    if name in s.lvstores:
        lvs = s.lvstores[name].copy()
        # Override leadership from dynamic state if set
        if name in s.leadership:
            lvs['lvs leadership'] = s.leadership[name]
        return [lvs]
    if s.lvstores:
        lvs = list(s.lvstores.values())[0].copy()
        lvs['name'] = name
        # Override leadership from dynamic state if set
        if name in s.leadership:
            lvs['lvs leadership'] = s.leadership[name]
        return [lvs]
    # No lvstores at all — return a minimal one if leadership is set
    if name in s.leadership:
        return [{'name': name, 'lvs leadership': s.leadership[name]}]
    return []


def _bdev_lvol_set_lvs_opts(s, p):
    s.lvs_opts = p
    lvs_name = p.get('lvs', p.get('lvs_name', ''))
    if lvs_name in s.lvstores:
        role = p.get('role', '')
        if role == 'primary':
            s.lvstores[lvs_name]['lvs_primary'] = True
            s.lvstores[lvs_name]['lvs_secondary'] = False
        elif role in ('secondary', 'tertiary'):
            s.lvstores[lvs_name]['lvs_secondary'] = True
            s.lvstores[lvs_name]['lvs_primary'] = False
    return True


def _bdev_examine(s, p):
    s.examined = True
    return True


def _bdev_wait_for_examine(s, p):
    return True


def _jc_suspend_compression(s, p):
    s.compression_suspended = p.get('suspend', False)
    return True


def _jc_compression_get_status(s, p):
    return not s.compression_suspended


def _jc_explicit_synchronization(s, p):
    return True


def _nvmf_create_subsystem(s, p):
    nqn = p.get('nqn', '')
    if nqn not in s.subsystems:
        s.subsystems[nqn] = {
            'nqn': nqn, 'serial_number': p.get('serial_number', ''),
            'model_number': p.get('model_number', ''),
            'namespaces': [], 'listen_addresses': [], 'hosts': [],
            'allow_any_host': p.get('allow_any_host', True), 'ana_reporting': True,
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
    if nqn not in s.subsystems:
        s.subsystems[nqn] = {
            'nqn': nqn, 'namespaces': [], 'listen_addresses': [],
            'hosts': [], 'allow_any_host': True, 'ana_reporting': True,
            'serial_number': '', 'model_number': '',
        }
    entry = dict(p.get('listen_address', {}))
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
        'nsid': nsid, 'bdev_name': bdev_name,
        'uuid': ns_params.get('uuid', str(_uuid_mod.uuid4())),
    })
    return nsid


def _nvmf_subsystem_add_host(s, p):
    nqn = p.get('nqn', '')
    host = p.get('host', '')
    if nqn in s.subsystems:
        s.subsystems[nqn]['hosts'].append({
            'nqn': host,
            'dhchap_key': p.get('dhchap_key', ''),
            'dhchap_ctrlr_key': p.get('dhchap_ctrlr_key', ''),
            'psk': p.get('psk', ''),
        })
    return True


def _nvmf_subsystem_listener_set_ana_state(s, p):
    return True


def _nvmf_delete_subsystem(s, p):
    s.subsystems.pop(p.get('nqn', ''), None)
    return True


def _bdev_nvme_attach_controller(s, p):
    name = p.get('name', '')
    path = {
        'nqn': p.get('subnqn', ''),
        'traddr': p.get('traddr', ''),
        'trsvcid': p.get('trsvcid', ''),
        'trtype': p.get('trtype', 'TCP'),
        'multipath': p.get('multipath', 'disable'),
    }
    if name not in s.nvme_controller_paths:
        s.nvme_controller_paths[name] = []
    s.nvme_controller_paths[name].append(path)
    s.nvme_controllers[name] = {
        'name': name,
        'nqn': path['nqn'],
        'traddr': path['traddr'],
        'trsvcid': path['trsvcid'],
        'trtype': path['trtype'],
        'ctrlrs': s.nvme_controller_paths[name],
    }
    return [f"{name}n1"]


def _bdev_nvme_controller_list(s, p):
    name = p.get('name')
    if name and name in s.nvme_controllers:
        ctrl = s.nvme_controllers[name].copy()
        ctrl['ctrlrs'] = s.nvme_controller_paths.get(name, [])
        return [ctrl]
    if name:
        return []
    result = []
    for n, ctrl in s.nvme_controllers.items():
        c = ctrl.copy()
        c['ctrlrs'] = s.nvme_controller_paths.get(n, [])
        result.append(c)
    return result


def _bdev_set_qos_limit(s, p):
    return True


def _bdev_lvol_set_qos_limit(s, p):
    return True


def _bdev_lvol_add_to_group(s, p):
    return True


def _bdev_lvol_create(s, p):
    lvs_name = p.get('lvs_name', s.lvstore)
    name = p.get('lvol_name', '')
    uuid = str(_uuid_mod.uuid4())
    composite = f"{lvs_name}/{name}"
    s.bdevs[composite] = {
        'name': composite, 'aliases': [composite],
        'uuid': uuid, 'driver_specific': {'lvol': {'lvol_store_uuid': lvs_name}},
    }
    return uuid


def _lvol_crypto_key_create(s, p):
    return True


def _lvol_crypto_create(s, p):
    name = p.get('name', '')
    s.bdevs[name] = {'name': name, 'aliases': [], 'driver_specific': {'crypto': True}}
    return True


def _keyring_file_add_key(s, p):
    return True


# Catch-all static mocks
def _STATIC_TRUE(s, p): return True
def _STATIC_EMPTY_LIST(s, p): return []
def _STATIC_EMPTY_DICT(s, p): return {}


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_FTT2_DISPATCH = {
    # Dynamic handlers (restart-critical)
    'jc_get_jm_status':                      _jc_get_jm_status,
    'bdev_lvol_set_leader':                  _bdev_lvol_set_leader,
    'bdev_lvol_set_leader_all':              _bdev_lvol_set_leader,
    'bdev_distrib_force_to_non_leader':      _bdev_distrib_force_to_non_leader,
    'bdev_distrib_check_inflight_io':        _bdev_distrib_check_inflight_io,
    'bdev_distrib_drop_leadership_remote':   _STATIC_TRUE,
    'bdev_lvol_create_hublvol':              _bdev_lvol_create_hublvol,
    'bdev_lvol_connect_hublvol':             _bdev_lvol_connect_hublvol,
    'bdev_lvol_delete_hublvol':              _bdev_lvol_delete_hublvol,

    # LVS / examine
    'bdev_lvol_create_lvstore':              _create_lvstore,
    'bdev_lvol_get_lvstores':                _bdev_lvol_get_lvstores,
    'bdev_lvol_set_lvs_opts':                _bdev_lvol_set_lvs_opts,
    'bdev_examine':                          _bdev_examine,
    'bdev_wait_for_examine':                 _bdev_wait_for_examine,

    # Distrib / RAID / cluster map
    'bdev_distrib_create':                   _bdev_distrib_create,
    'distr_send_cluster_map':                _distr_send_cluster_map,
    'distr_get_cluster_map':                 _distr_get_cluster_map,
    'bdev_raid_create':                      _bdev_raid_create,

    # Bdev queries
    'bdev_get_bdevs':                        _bdev_get_bdevs,
    'spdk_get_version':                      _spdk_get_version,

    # NVMf subsystems
    'nvmf_create_subsystem':                 _nvmf_create_subsystem,
    'nvmf_get_subsystems':                   _nvmf_get_subsystems,
    'nvmf_subsystem_add_listener':           _nvmf_subsystem_add_listener,
    'nvmf_subsystem_add_ns':                 _nvmf_subsystem_add_ns,
    'nvmf_subsystem_add_host':               _nvmf_subsystem_add_host,
    'nvmf_subsystem_listener_set_ana_state': _nvmf_subsystem_listener_set_ana_state,
    'nvmf_delete_subsystem':                 _nvmf_delete_subsystem,

    # NVMe controllers
    'bdev_nvme_attach_controller':           _bdev_nvme_attach_controller,
    # Real SPDK RPC method name; ``RPCClient.bdev_nvme_controller_list``
    # is just a Python wrapper that issues this method, so the dispatch
    # key must match the wire name. Registering the wrapper name here
    # falls through the unknown-method path and returns ``True``, which
    # then crashes the helper at ``_ctrlrs_from_list`` (``ret[0]`` on a bool).
    'bdev_nvme_get_controllers':             _bdev_nvme_controller_list,
    'bdev_nvme_set_options':                 _STATIC_TRUE,

    # Compression
    'jc_suspend_compression':                _jc_suspend_compression,
    'jc_compression':                        _jc_compression_get_status,
    'jc_compression_get_status':             _jc_compression_get_status,
    'jc_explicit_synchronization':           _jc_explicit_synchronization,

    # QoS
    'bdev_set_qos_limit':                    _bdev_set_qos_limit,
    'bdev_lvol_set_qos_limit':               _bdev_lvol_set_qos_limit,
    'bdev_lvol_add_to_group':                _bdev_lvol_add_to_group,

    # Crypto
    'lvol_crypto_key_create':                _lvol_crypto_key_create,
    'lvol_crypto_create':                    _lvol_crypto_create,
    'keyring_file_add_key':                  _keyring_file_add_key,

    # LVol operations
    'bdev_lvol_create':                      _bdev_lvol_create,

    # Static mocks for SPDK init sequence
    'iobuf_set_options':                     _STATIC_TRUE,
    'bdev_set_options':                      _STATIC_TRUE,
    'accel_set_options':                     _STATIC_TRUE,
    'sock_impl_set_options':                 _STATIC_TRUE,
    'nvmf_set_max_subsystems':               _STATIC_TRUE,
    'framework_start_init':                  _STATIC_TRUE,
    'log_set_print_level':                   _STATIC_TRUE,
    'transport_create':                      _STATIC_TRUE,
    'nvmf_set_config':                       _STATIC_TRUE,
    'jc_set_hint_lcpu_mask':                 _STATIC_TRUE,
    'bdev_PT_NoExcl_create':                 _STATIC_TRUE,
    'alceml_set_qos_weights':                _STATIC_TRUE,
    'nvmf_get_blocked_ports_rdma':           _STATIC_EMPTY_LIST,
    'thread_get_stats':                      lambda s, p: {'threads': []},
    'distr_status_events_update':            _STATIC_TRUE,
}


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class _FTT2RpcHandler(BaseHTTPRequestHandler):
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

        # Check if node is reachable
        if not server.node_state.reachable:
            # Simulate connection timeout / refused
            time.sleep(0.05)
            self._send_error(-32000, "Node unreachable", req_id)
            return

        # Log the RPC call
        server.node_state.rpc_log.append((time.time(), method, params))

        # RPC hook: allow tests to trigger state changes mid-restart
        hook = getattr(server.node_state, '_rpc_hook', None)
        if hook is not None:
            try:
                hook_result = hook(method, params)
                if hook_result is None and not server.node_state.reachable:
                    # Hook disconnected the node — fail this RPC
                    self._send_error(-32000, "Node disconnected by hook", req_id)
                    return
            except Exception as e:
                self._send_error(-32000, f"Hook error: {e}", req_id)
                return

        # Phase gate: pause at specific RPC for concurrent operation tests
        gate = server.node_state._phase_gate
        if gate is not None and gate.should_pause(method):
            gate.pause()

        # Error injection: return configured error for specific methods
        err_msg = server.node_state.failing_methods.get(method)
        if err_msg:
            self._send_error(-32602, err_msg, req_id)
            return

        handler = _FTT2_DISPATCH.get(method)
        if handler is None:
            # Unknown method → success (null-op mock)
            self._send_result(True, req_id)
            return

        try:
            with server.node_state.lock:
                result = handler(server.node_state, params)
            self._send_result(result, req_id)
        except _RpcError as e:
            self._send_error(e.code, e.message, req_id)
        except Exception as exc:
            logger.exception("Unhandled error in FTT2 mock RPC %s", method)
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


class _FTT2HTTPServer(HTTPServer):
    def __init__(self, server_address, handler_class, node_state):
        super().__init__(server_address, handler_class)
        self.node_state = node_state


class FTT2MockRpcServer:
    """Mock RPC server for FTT=2 restart testing with dynamic state."""

    def __init__(self, host: str, port: int, node_id: str, lvstore: str = ""):
        self.host = host
        self.port = port
        self.node_id = node_id
        self.state = FTT2NodeState(node_id, lvstore)
        self._server: Optional[_FTT2HTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._server = _FTT2HTTPServer(
            (self.host, self.port), _FTT2RpcHandler, self.state)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name=f"ftt2-mock-rpc-{self.node_id}", daemon=True)
        self._thread.start()

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server = None

    def reset(self):
        with self.state.lock:
            self.state.reset()

    # --- Test configuration helpers ---

    def set_jm_connected(self, remote_node_id: str, connected: bool):
        """Configure how this node reports connectivity to a remote node's JM."""
        with self.state.lock:
            self.state.jm_connectivity[remote_node_id] = connected

    def set_inflight_io(self, jm_vuid: int, has_inflight: bool):
        """Configure inflight IO response for a specific jm_vuid."""
        with self.state.lock:
            self.state.inflight_io[jm_vuid] = has_inflight

    def set_reachable(self, reachable: bool):
        """Make this node unreachable (all RPCs fail)."""
        self.state.reachable = reachable

    def set_fabric_up(self, up: bool):
        """Mark fabric as up/down for this node."""
        self.state.fabric_up = up

    def set_rpc_hook(self, hook):
        """Install a hook called before each RPC. hook(method, params) can
        return None to let the RPC fail, or a value to override the response.
        Used by tests to trigger state changes (e.g. disconnect) mid-restart."""
        self.state._rpc_hook = hook

    def clear_rpc_hook(self):
        """Remove the RPC hook."""
        self.state._rpc_hook = None

    def get_rpc_calls(self, method: Optional[str] = None) -> list:
        """Get logged RPC calls, optionally filtered by method."""
        with self.state.lock:
            if method:
                return [(ts, m, p) for ts, m, p in self.state.rpc_log if m == method]
            return list(self.state.rpc_log)

    def was_called(self, method: str) -> bool:
        """Check if a specific RPC method was called."""
        return any(m == method for _, m, _ in self.state.rpc_log)

    def get_leadership(self, lvs_name: str) -> Optional[bool]:
        """Get current leadership state for an LVS."""
        return self.state.leadership.get(lvs_name)

    # --- Error injection ---

    def fail_method(self, method: str, error_msg: str = "Simulated RPC error"):
        """Make the given RPC method return an error until cleared."""
        with self.state.lock:
            self.state.failing_methods[method] = error_msg

    def clear_fail_method(self, method: str):
        """Remove a previously injected error for the given method."""
        with self.state.lock:
            self.state.failing_methods.pop(method, None)

    def clear_all_fail_methods(self):
        """Remove all injected errors."""
        with self.state.lock:
            self.state.failing_methods.clear()

    # --- Hublvol state queries ---

    def hublvol_connected(self, lvs_name: str) -> bool:
        """Return True if bdev_lvol_connect_hublvol was received for this LVS."""
        return lvs_name in self.state.hublvols_connected

    def hublvol_created(self, lvs_name: str) -> bool:
        """Return True if bdev_lvol_create_hublvol was received for this LVS."""
        return lvs_name in self.state.hublvols_created
