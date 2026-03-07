# coding=utf-8
"""
mock_rpc_server.py – in-process JSON-RPC 2.0 mock simulating one SPDK storage node.

Each MockRpcServer instance:
  - Runs an HTTP server in a daemon thread on a configurable port.
  - Keeps all state (lvols, snapshots, subsystems, NVMe controllers, async ops)
    in memory.  Nothing is persisted.
  - Supports the full set of RPCs needed by the migration controller / task runner.
  - Supports a "random failure mode" toggled via the special `mock_set_failure_rate`
    RPC; when active, X % of calls return a simulated failure (timeout or an RPC
    error code chosen at random from the method's known error codes).

Async-operation simulation
--------------------------
`delete_lvol(sync=False)` and `bdev_lvol_transfer` / `bdev_lvol_final_migration`
are modelled as asynchronous: they record a ``complete_at`` timestamp drawn from an
exponential distribution centred around 0.2 s (λ = 5), clipped to [0.1, 100] s.
Subsequent poll calls (`bdev_lvol_get_lvol_delete_status`,
`bdev_lvol_transfer_stat`) compare ``time.time()`` against ``complete_at`` and
return in-progress / done accordingly.
"""

import json
import logging
import math
import random
import threading
import time
import uuid as _uuid_mod
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Async-delay helper
# ---------------------------------------------------------------------------

def _async_delay() -> float:
    """
    Return a completion delay (seconds) from an exponential distribution with
    mean 0.2 s (λ = 5), clipped to [0.1, 100].  Values pile up near 0.2 s but
    occasionally reach tens of seconds, matching real-world transfer variance.
    """
    raw = random.expovariate(5.0)
    return max(0.1, min(100.0, raw))


# ---------------------------------------------------------------------------
# Per-node state container
# ---------------------------------------------------------------------------

class NodeState:
    """All mutable data-plane state for one mock storage node."""

    def __init__(self, lvstore: str):
        self.lvstore = lvstore

        # composite_name (e.g. "lvs/myvol") → bdev dict
        self.lvols: Dict[str, dict] = {}
        # composite_name → bdev dict (immutable after convert)
        self.snapshots: Dict[str, dict] = {}
        # nqn → subsystem dict {nqn, serial, model, namespaces:[{nsid,bdev}], listeners:[]}
        self.subsystems: Dict[str, dict] = {}
        # ctrl_name → {nqn, traddr, trsvcid, trtype}
        self.nvme_controllers: Dict[str, dict] = {}
        # async-delete: composite_name → complete_at float
        self.delete_ops: Dict[str, float] = {}
        # async-transfer: composite_name → {complete_at, state}
        # state is "In progress" until complete_at, then "Done" (or "Failed" in fault-inject)
        self.transfer_ops: Dict[str, dict] = {}

        self._blobid_counter = 1000
        self._nsid_counter: Dict[str, int] = {}  # nqn → next nsid
        self.lock = threading.Lock()

    # ---- helpers ----

    def next_blobid(self) -> int:
        bid = self._blobid_counter
        self._blobid_counter += 1
        return bid

    def next_nsid(self, nqn: str) -> int:
        self._nsid_counter.setdefault(nqn, 1)
        nsid = self._nsid_counter[nqn]
        self._nsid_counter[nqn] += 1
        return nsid

    def composite(self, short_name: str) -> str:
        return f"{self.lvstore}/{short_name}"

    def short_name(self, composite: str) -> str:
        if '/' in composite:
            return composite.split('/', 1)[1]
        return composite

    def all_bdevs(self) -> Dict[str, dict]:
        return {**self.lvols, **self.snapshots}


# ---------------------------------------------------------------------------
# HTTP request handler
# ---------------------------------------------------------------------------

class _RpcHandler(BaseHTTPRequestHandler):
    """Single-request JSON-RPC 2.0 handler.  The parent server sets ``node_state``
    and ``failure_rate`` on the HTTPServer instance."""

    # Suppress default access log noise
    def log_message(self, fmt, *args):
        logger.debug("mock-rpc %s %s", self.address_string(), fmt % args)

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

        server: _MockHTTPServer = self.server  # type: ignore[assignment]

        # --- mock_set_failure_rate: intercept before failure injection ---
        if method == 'mock_set_failure_rate':
            server.failure_rate = float(params.get('failure_rate', 0.0))
            server.timeout_seconds = float(
                params.get('timeout_seconds', server.timeout_seconds))
            self._send_result(True, req_id)
            return

        # --- failure injection ---
        if server.failure_rate > 0:
            if random.random() < server.failure_rate:
                failure_type = random.choice(['timeout', 'error'])
                if failure_type == 'timeout':
                    # Sleep beyond typical client timeout to simulate a hang
                    time.sleep(server.timeout_seconds + 1)
                    # (The client will have timed out by now; just return nothing)
                    return
                else:
                    # Pick a random error code from method-specific list or generic
                    codes = _METHOD_ERROR_CODES.get(method, [-1, -22, -2])
                    code = random.choice(codes)
                    self._send_error(code, f"Simulated failure for {method}", req_id)
                    return

        # --- dispatch ---
        handler = _DISPATCH.get(method)
        if handler is None:
            self._send_error(-32601, f"Method not found: {method}", req_id)
            return

        try:
            with server.node_state.lock:
                result = handler(server.node_state, params)
            self._send_result(result, req_id)
        except _RpcError as e:
            self._send_error(e.code, e.message, req_id)
        except Exception as exc:
            logger.exception("Unhandled error in mock RPC %s", method)
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


# ---------------------------------------------------------------------------
# Lookup table: method → [possible error codes]
# ---------------------------------------------------------------------------

_METHOD_ERROR_CODES: Dict[str, list] = {
    "bdev_lvol_create":              [-1, -22, -17],   # generic, EINVAL, EEXIST
    "bdev_lvol_delete":              [-1, -2, -22],    # generic, ENOENT, EINVAL
    "bdev_lvol_get_lvol_delete_status": [-1, -2],
    "bdev_lvol_set_migration_flag":  [-1, -2],
    "bdev_lvol_transfer":            [-1, -22, -2],
    "bdev_lvol_transfer_stat":       [-1, -2],
    "bdev_lvol_add_clone":           [-1, -22, -2],
    "bdev_lvol_convert":             [-1, -22],
    "bdev_lvol_get_lvols":           [-1],
    "bdev_lvol_final_migration":     [-1, -22, -2],
    "bdev_lvol_register":            [-1, -22, -17],
    "bdev_lvol_snapshot_register":   [-1, -22],
    "bdev_get_bdevs":                [-1],
    "nvmf_create_subsystem":         [-1, -17, -22],
    "nvmf_delete_subsystem":         [-1, -2],
    "nvmf_get_subsystems":           [-1],
    "nvmf_subsystem_add_listener":   [-1, -17, -22],
    "nvmf_subsystem_add_ns":         [-1, -17, -22],
    "nvmf_subsystem_remove_ns":      [-1, -2, -22],
    "nvmf_subsystem_listener_set_ana_state": [-1, -2],
    "bdev_nvme_attach_controller":   [-1, -22, -5],    # -5 = EIO
    "bdev_nvme_detach_controller":   [-1, -2],
    "bdev_lvol_snapshot":            [-1, -22],
    "bdev_lvol_clone":               [-1, -22, -2],
    "ultra21_lvol_set":              [-1, -22, -2],
}


# ---------------------------------------------------------------------------
# RPC implementations
# ---------------------------------------------------------------------------

def _req(params: dict, key: str, required=True) -> Any:
    """Extract a required (or optional) parameter, raising RpcError on missing."""
    if key not in params:
        if required:
            raise _RpcError(-22, f"Missing required param: {key}")
        return None
    return params[key]


# ---- lvol lifecycle ----

def _bdev_lvol_create(s: NodeState, p: dict):
    name = _req(p, 'lvol_name')
    size_mib = int(_req(p, 'size_in_mib'))
    lvs = _req(p, 'lvs_name')
    if lvs != s.lvstore:
        raise _RpcError(-22, f"Unknown lvstore {lvs}")
    composite = s.composite(name)
    if composite in s.lvols or composite in s.snapshots:
        raise _RpcError(-17, f"bdev {composite} already exists")
    blobid = s.next_blobid()
    obj_uuid = p.get('uuid') or str(_uuid_mod.uuid4())
    s.lvols[composite] = {
        'name': name,
        'composite': composite,
        'uuid': obj_uuid,
        'blobid': blobid,
        'size_mib': size_mib,
        'migration_flag': False,
        'driver_specific': {
            'lvol': {
                'blobid': blobid,
                'lvs_name': lvs,
                'base_snapshot': None,
                'clone': False,
                'snapshot': False,
                'num_allocated_clusters': size_mib,
            }
        }
    }
    logger.debug("mock create_lvol %s uuid=%s blobid=%d", composite, obj_uuid, blobid)
    return composite


def _bdev_lvol_delete(s: NodeState, p: dict):
    name = _req(p, 'name')
    sync_flag = bool(p.get('sync', False))
    bdevs = s.all_bdevs()
    # Accept short name or composite
    composite = name if name in bdevs else s.composite(name)
    if not sync_flag:
        # Phase 1: start async deletion
        if composite not in bdevs:
            # Not found – return ok (idempotent start)
            return True
        s.delete_ops[composite] = time.time() + _async_delay()
        logger.debug("mock delete_lvol async start %s", composite)
        return True
    else:
        # Phase 3: sync finalize – remove from state
        s.lvols.pop(composite, None)
        s.snapshots.pop(composite, None)
        s.delete_ops.pop(composite, None)
        logger.debug("mock delete_lvol sync finalize %s", composite)
        return True


def _bdev_lvol_get_lvol_delete_status(s: NodeState, p: dict):
    """Returns 0=done, 1=in-progress, 2=not-found."""
    name = _req(p, 'name')
    composite = name if name in s.all_bdevs() or name in s.delete_ops \
        else s.composite(name)
    if composite not in s.delete_ops:
        # Already fully deleted or never started
        return 2
    complete_at = s.delete_ops[composite]
    if time.time() >= complete_at:
        return 0  # done (finalize step will actually remove it)
    return 1  # in progress


def _bdev_lvol_set_migration_flag(s: NodeState, p: dict):
    name = _req(p, 'name')
    composite = name if name in s.lvols else s.composite(name)
    if composite not in s.lvols:
        raise _RpcError(-2, f"bdev {composite} not found")
    s.lvols[composite]['migration_flag'] = True
    return True


# ---- snapshot / clone / convert ----

def _bdev_lvol_snapshot(s: NodeState, p: dict):
    lvol_name = _req(p, 'lvol_name')
    snap_name = _req(p, 'snapshot_name')
    composite_src = lvol_name if lvol_name in s.lvols else s.composite(lvol_name)
    if composite_src not in s.lvols:
        raise _RpcError(-2, f"lvol {composite_src} not found")
    composite_snap = s.composite(snap_name)
    if composite_snap in s.snapshots:
        raise _RpcError(-17, f"snapshot {composite_snap} already exists")
    src = s.lvols[composite_src]
    blobid = s.next_blobid()
    snap_uuid = str(_uuid_mod.uuid4())
    src_size_mib = src.get('size_mib', 1024)
    s.snapshots[composite_snap] = {
        'name': snap_name,
        'composite': composite_snap,
        'uuid': snap_uuid,
        'blobid': blobid,
        'size_mib': src_size_mib,
        'driver_specific': {
            'lvol': {
                'blobid': blobid,
                'lvs_name': s.lvstore,
                'base_snapshot': None,
                'clone': False,
                'snapshot': True,
                'num_allocated_clusters': src_size_mib,
            }
        }
    }
    # The source lvol now references this snapshot as its base
    s.lvols[composite_src]['driver_specific']['lvol']['base_snapshot'] = composite_snap
    return composite_snap


def _bdev_lvol_clone(s: NodeState, p: dict):
    snap_name = _req(p, 'snapshot_name')
    clone_name = _req(p, 'clone_name')
    composite_snap = snap_name if snap_name in s.snapshots else s.composite(snap_name)
    if composite_snap not in s.snapshots:
        raise _RpcError(-2, f"snapshot {composite_snap} not found")
    composite_clone = s.composite(clone_name)
    if composite_clone in s.lvols:
        raise _RpcError(-17, f"clone {composite_clone} already exists")
    blobid = s.next_blobid()
    clone_uuid = str(_uuid_mod.uuid4())
    snap_size_mib = s.snapshots[composite_snap].get('size_mib', 1024)
    s.lvols[composite_clone] = {
        'name': clone_name,
        'composite': composite_clone,
        'uuid': clone_uuid,
        'blobid': blobid,
        'size_mib': snap_size_mib,
        'migration_flag': False,
        'driver_specific': {
            'lvol': {
                'blobid': blobid,
                'lvs_name': s.lvstore,
                'base_snapshot': composite_snap,
                'clone': True,
                'snapshot': False,
                'num_allocated_clusters': snap_size_mib,
            }
        }
    }
    return composite_clone


def _bdev_lvol_add_clone(s: NodeState, p: dict):
    """Link a writable lvol to its parent snapshot (migration pre-step)."""
    lvol_name = _req(p, 'lvol_name')
    parent_snap = _req(p, 'parent_snapshot_name')
    composite = lvol_name if lvol_name in s.lvols else s.composite(lvol_name)
    if composite not in s.lvols:
        raise _RpcError(-2, f"lvol {composite} not found")
    s.lvols[composite]['driver_specific']['lvol']['base_snapshot'] = parent_snap
    s.lvols[composite]['driver_specific']['lvol']['clone'] = True
    return True


def _bdev_lvol_convert(s: NodeState, p: dict):
    """Convert a writable lvol into an immutable snapshot in-place."""
    name = _req(p, 'name')
    composite = name if name in s.lvols else s.composite(name)
    if composite not in s.lvols:
        raise _RpcError(-2, f"lvol {composite} not found for convert")
    entry = s.lvols.pop(composite)
    entry['driver_specific']['lvol']['snapshot'] = True
    entry['driver_specific']['lvol']['clone'] = False
    s.snapshots[composite] = entry
    logger.debug("mock bdev_lvol_convert %s → snapshot", composite)
    return True


def _bdev_lvol_register(s: NodeState, p: dict):
    """Register a remote lvol on the secondary (just inserts it into state)."""
    lvol_name = _req(p, 'lvol_name')
    lvs_name = _req(p, 'lvs_name')
    registered_uuid = _req(p, 'registered_uuid')
    blobid = _req(p, 'blobid')
    composite = s.composite(lvol_name)
    if composite in s.lvols or composite in s.snapshots:
        # Already there – idempotent on secondary
        return True
    s.lvols[composite] = {
        'name': lvol_name,
        'composite': composite,
        'uuid': registered_uuid,
        'blobid': blobid,
        'migration_flag': False,
        'driver_specific': {'lvol': {'blobid': blobid, 'lvs_name': lvs_name,
                                     'base_snapshot': None, 'clone': False, 'snapshot': False,
                                     'num_allocated_clusters': 1024}}
    }
    return True


def _bdev_lvol_snapshot_register(s: NodeState, p: dict):
    """Register a remote snapshot on the secondary."""
    lvol_name = _req(p, 'lvol_name')   # parent lvol/snap path on secondary
    snapshot_name = _req(p, 'snapshot_name')
    registered_uuid = _req(p, 'registered_uuid')
    blobid = _req(p, 'blobid')
    composite = s.composite(snapshot_name)
    if composite in s.snapshots:
        return True
    s.snapshots[composite] = {
        'name': snapshot_name,
        'composite': composite,
        'uuid': registered_uuid,
        'blobid': blobid,
        'driver_specific': {'lvol': {'blobid': blobid, 'lvs_name': s.lvstore,
                                     'base_snapshot': lvol_name, 'clone': False, 'snapshot': True,
                                     'num_allocated_clusters': 1024}}
    }
    return True


# ---- get bdevs / lvols ----

def _bdev_get_bdevs(s: NodeState, p: dict):
    name: Optional[str] = p.get('name')
    all_bdevs = s.all_bdevs()
    if name:
        composite = name if name in all_bdevs else s.composite(name)
        entry = all_bdevs.get(composite)
        return [entry] if entry else []
    return list(all_bdevs.values())


def _bdev_lvol_get_lvols(s: NodeState, p: dict):
    """Return list of {name, uuid, blobid} for all lvols in the given lvstore."""
    lvs_name = _req(p, 'lvs_name')
    if lvs_name != s.lvstore:
        return []
    result = []
    for composite, entry in s.all_bdevs().items():
        result.append({
            'name': entry['composite'],
            'lvol_name': entry['name'],
            'uuid': entry['uuid'],
            'blobid': entry['blobid'],
        })
    return result


def _ultra21_lvol_set(s: NodeState, p: dict):
    """Resize an lvol (accepts either lvol_bdev or clone_bdev key)."""
    name = p.get('lvol_bdev') or p.get('clone_bdev')
    if not name:
        raise _RpcError(-22, "Missing lvol_bdev or clone_bdev")
    composite = name if name in s.lvols else s.composite(name)
    if composite not in s.lvols:
        raise _RpcError(-2, f"lvol {composite} not found")
    blockcnt = _req(p, 'blockcnt')
    s.lvols[composite]['blockcnt'] = int(blockcnt)
    return True


# ---- async transfer / migration ----

def _bdev_lvol_transfer(s: NodeState, p: dict):
    """Start async data transfer from source lvol to a remote bdev."""
    name = _req(p, 'name')
    composite = name if (name in s.lvols or name in s.snapshots) else s.composite(name)
    if composite not in s.lvols and composite not in s.snapshots:
        raise _RpcError(-2, f"source bdev {composite} not found")
    s.transfer_ops[composite] = {
        'complete_at': time.time() + _async_delay(),
        'state': 'In progress',
    }
    logger.debug("mock bdev_lvol_transfer started for %s", composite)
    return True


def _bdev_lvol_transfer_stat(s: NodeState, p: dict):
    """Poll transfer status: {'transfer_state': ..., 'offset': N}."""
    name = _req(p, 'name')
    # Resolve to the key used in transfer_ops (which is always the composite form)
    composite = name if name in s.transfer_ops else s.composite(name)
    if composite not in s.transfer_ops:
        return {'transfer_state': 'No process', 'offset': 0}
    op = s.transfer_ops[composite]
    if op['state'] == 'In progress' and time.time() >= op['complete_at']:
        op['state'] = 'Done'
    return {'transfer_state': op['state'], 'offset': 0}


def _bdev_lvol_final_migration(s: NodeState, p: dict):
    """Start async final-migration (source lvol → target blobstore)."""
    lvol_name = _req(p, 'lvol_name')
    composite = lvol_name if lvol_name in s.lvols else s.composite(lvol_name)
    if composite not in s.lvols:
        raise _RpcError(-2, f"source lvol {composite} not found")
    s.transfer_ops[composite] = {
        'complete_at': time.time() + _async_delay(),
        'state': 'In progress',
    }
    logger.debug("mock bdev_lvol_final_migration started for %s", composite)
    return True


# ---- NVMe-oF subsystems ----

def _nvmf_create_subsystem(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    if nqn in s.subsystems:
        raise _RpcError(-17, f"subsystem {nqn} already exists")
    s.subsystems[nqn] = {
        'nqn': nqn,
        'serial_number': p.get('serial_number', ''),
        'model_number': p.get('model_number', ''),
        'namespaces': [],
        'listen_addresses': [],
        'allow_any_host': True,
        'ana_reporting': True,
    }
    logger.debug("mock subsystem_create %s", nqn)
    return True


def _nvmf_delete_subsystem(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    if nqn not in s.subsystems:
        raise _RpcError(-2, f"subsystem {nqn} not found")
    del s.subsystems[nqn]
    logger.debug("mock subsystem_delete %s", nqn)
    return True


def _nvmf_get_subsystems(s: NodeState, p: dict):
    return list(s.subsystems.values())


def _nvmf_subsystem_add_listener(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    listen_address = _req(p, 'listen_address')
    if nqn not in s.subsystems:
        raise _RpcError(-2, f"subsystem {nqn} not found")
    # Check for duplicate listener
    for existing in s.subsystems[nqn]['listen_addresses']:
        if (existing.get('traddr') == listen_address.get('traddr') and
                existing.get('trsvcid') == listen_address.get('trsvcid')):
            raise _RpcError(-17, "listener already exists")
    entry = dict(listen_address)
    entry['ana_state'] = p.get('ana_state', 'optimized')
    s.subsystems[nqn]['listen_addresses'].append(entry)
    return True


def _nvmf_subsystem_add_ns(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    ns_params = _req(p, 'namespace')
    bdev_name = _req(ns_params, 'bdev_name')
    if nqn not in s.subsystems:
        raise _RpcError(-2, f"subsystem {nqn} not found")
    # Check for duplicate namespace bdev
    for existing in s.subsystems[nqn]['namespaces']:
        if existing['bdev_name'] == bdev_name:
            raise _RpcError(-17, f"namespace {bdev_name} already in subsystem {nqn}")
    nsid = s.next_nsid(nqn)
    ns_entry = {
        'nsid': nsid,
        'bdev_name': bdev_name,
        'uuid': ns_params.get('uuid', str(_uuid_mod.uuid4())),
        'nguid': ns_params.get('nguid', ''),
    }
    s.subsystems[nqn]['namespaces'].append(ns_entry)
    logger.debug("mock add_ns %s nsid=%d bdev=%s", nqn, nsid, bdev_name)
    return nsid


def _nvmf_subsystem_remove_ns(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    nsid = int(_req(p, 'nsid'))
    if nqn not in s.subsystems:
        raise _RpcError(-2, f"subsystem {nqn} not found")
    sub = s.subsystems[nqn]
    before = len(sub['namespaces'])
    sub['namespaces'] = [ns for ns in sub['namespaces'] if ns['nsid'] != nsid]
    if len(sub['namespaces']) == before:
        raise _RpcError(-2, f"namespace {nsid} not found in {nqn}")
    return True


def _nvmf_subsystem_listener_set_ana_state(s: NodeState, p: dict):
    nqn = _req(p, 'nqn')
    listen_address = _req(p, 'listen_address')
    ana_state = p.get('ana_state', 'optimized')
    if nqn not in s.subsystems:
        raise _RpcError(-2, f"subsystem {nqn} not found")
    for listener in s.subsystems[nqn]['listen_addresses']:
        if (listener.get('traddr') == listen_address.get('traddr') and
                listener.get('trsvcid') == listen_address.get('trsvcid')):
            listener['ana_state'] = ana_state
            return True
    raise _RpcError(-2, "listener not found")


# ---- NVMe controller attach / detach ----

def _bdev_nvme_attach_controller(s: NodeState, p: dict):
    name = _req(p, 'name')
    if name in s.nvme_controllers:
        raise _RpcError(-17, f"controller {name} already attached")
    s.nvme_controllers[name] = {
        'name': name,
        'nqn': _req(p, 'subnqn'),
        'traddr': _req(p, 'traddr'),
        'trsvcid': p.get('trsvcid', ''),
        'trtype': p.get('trtype', 'TCP'),
    }
    logger.debug("mock attach_controller %s → %s", name, p.get('subnqn'))
    # Return list of remote bdev names (convention: ctrlname + "n1")
    return [f"{name}n1"]


def _bdev_nvme_detach_controller(s: NodeState, p: dict):
    name = _req(p, 'name')
    s.nvme_controllers.pop(name, None)
    return True


# ---- misc / version ----

def _spdk_get_version(s: NodeState, p: dict):
    return {"version": "mock-24.05", "fields": {}}


# ---------------------------------------------------------------------------
# Dispatch table
# ---------------------------------------------------------------------------

_DISPATCH = {
    'spdk_get_version':                      _spdk_get_version,
    'bdev_lvol_create':                      _bdev_lvol_create,
    'bdev_lvol_delete':                      _bdev_lvol_delete,
    'bdev_lvol_get_lvol_delete_status':      _bdev_lvol_get_lvol_delete_status,
    'bdev_lvol_set_migration_flag':          _bdev_lvol_set_migration_flag,
    'bdev_lvol_snapshot':                    _bdev_lvol_snapshot,
    'bdev_lvol_clone':                       _bdev_lvol_clone,
    'bdev_lvol_add_clone':                   _bdev_lvol_add_clone,
    'bdev_lvol_convert':                     _bdev_lvol_convert,
    'bdev_lvol_register':                    _bdev_lvol_register,
    'bdev_lvol_snapshot_register':           _bdev_lvol_snapshot_register,
    'bdev_get_bdevs':                        _bdev_get_bdevs,
    'bdev_lvol_get_lvols':                   _bdev_lvol_get_lvols,
    'ultra21_lvol_set':                      _ultra21_lvol_set,
    'bdev_lvol_transfer':                    _bdev_lvol_transfer,
    'bdev_lvol_transfer_stat':               _bdev_lvol_transfer_stat,
    'bdev_lvol_final_migration':             _bdev_lvol_final_migration,
    'nvmf_create_subsystem':                 _nvmf_create_subsystem,
    'nvmf_delete_subsystem':                 _nvmf_delete_subsystem,
    'nvmf_get_subsystems':                   _nvmf_get_subsystems,
    'nvmf_subsystem_add_listener':           _nvmf_subsystem_add_listener,
    'nvmf_subsystem_add_ns':                 _nvmf_subsystem_add_ns,
    'nvmf_subsystem_remove_ns':              _nvmf_subsystem_remove_ns,
    'nvmf_subsystem_listener_set_ana_state': _nvmf_subsystem_listener_set_ana_state,
    'bdev_nvme_attach_controller':           _bdev_nvme_attach_controller,
    'bdev_nvme_detach_controller':           _bdev_nvme_detach_controller,
}


# ---------------------------------------------------------------------------
# HTTP server wrapper
# ---------------------------------------------------------------------------

class _MockHTTPServer(HTTPServer):
    def __init__(self, server_address, handler_class, node_state: NodeState,
                 timeout_seconds: float = 6.0):
        super().__init__(server_address, handler_class)
        self.node_state = node_state
        self.failure_rate: float = 0.0
        self.timeout_seconds = timeout_seconds


# ---------------------------------------------------------------------------
# Public MockRpcServer class
# ---------------------------------------------------------------------------

class MockRpcServer:
    """
    A single mock storage-node JSON-RPC 2.0 server.

    Usage::

        srv = MockRpcServer(host='127.0.0.1', port=9901, lvstore='lvs0',
                            node_id='node-aaa')
        srv.start()
        # ... run tests ...
        srv.stop()

    The ``state`` attribute gives direct access to in-memory node state for
    test assertions (bypassing the RPC layer).
    """

    def __init__(self, host: str, port: int, lvstore: str, node_id: str,
                 rpc_username: str = 'spdkuser', rpc_password: str = 'spdkpass'):
        self.host = host
        self.port = port
        self.lvstore = lvstore
        self.node_id = node_id
        self.rpc_username = rpc_username
        self.rpc_password = rpc_password
        self.state = NodeState(lvstore)
        self._server: Optional[_MockHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._server = _MockHTTPServer(
            (self.host, self.port), _RpcHandler, self.state)
        self._thread = threading.Thread(
            target=self._server.serve_forever, name=f"mock-rpc-{self.node_id}",
            daemon=True)
        self._thread.start()
        logger.info("MockRpcServer %s started on %s:%d", self.node_id, self.host, self.port)

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._server = None
        logger.info("MockRpcServer %s stopped", self.node_id)

    def set_failure_rate(self, rate: float, timeout_seconds: float = 6.0):
        """Set random-failure injection directly (no RPC round-trip needed)."""
        if self._server:
            self._server.failure_rate = rate
            self._server.timeout_seconds = timeout_seconds

    def reset_state(self):
        """Wipe all in-memory state (useful between subtests)."""
        with self.state.lock:
            self.state.lvols.clear()
            self.state.snapshots.clear()
            self.state.subsystems.clear()
            self.state.nvme_controllers.clear()
            self.state.delete_ops.clear()
            self.state.transfer_ops.clear()
            self.state._blobid_counter = 1000
            self.state._nsid_counter.clear()
