# coding=utf-8
"""
topology_loader.py – load a JSON topology spec and populate the FDB
control-plane database with the described objects.

JSON schema
-----------
The JSON format mirrors the parameters available through the sbcli CLI so that
test topologies can be defined the same way an operator would configure a real
cluster.  Every field that maps to a CLI flag uses the same name as that flag
(with hyphens replaced by underscores):

  cluster create  → "cluster" object
  sn add-node     → items in "nodes" array
  volume add      → items in "volumes" array
  snapshot add    → items in "snapshots" array
  storage-pool add → items in "pools" array

Example JSON
------------
See tests/migration/topologies/two_node.json for a complete example.

Minimal example::

    {
      "cluster": {
        "ha_type": "single",
        "blk_size": 4096,
        "distr_ndcs": 1,
        "distr_npcs": 1
      },
      "nodes": [
        {
          "id": "src",
          "mgmt_ip": "127.0.0.1",
          "rpc_port": 9901,
          "lvstore": "lvs_src",
          "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]
        },
        {
          "id": "tgt",
          "mgmt_ip": "127.0.0.1",
          "rpc_port": 9902,
          "lvstore": "lvs_tgt",
          "data_nics": [{"if_name": "eth0", "ip": "127.0.0.1", "trtype": "TCP"}]
        }
      ],
      "pools": [
        {"id": "pool1", "name": "default"}
      ],
      "volumes": [
        {"id": "l1", "name": "vol1", "size": "1G",
         "node_id": "src", "pool_id": "pool1"}
      ],
      "snapshots": [
        {"id": "s1", "name": "snap1", "lvol_id": "l1"}
      ]
    }

Node-reference resolution
-------------------------
Within "nodes", "volumes", and "snapshots" arrays the ``id`` field is a
symbolic name that only exists in this spec file.  All cross-references
(e.g. ``volume.node_id``, ``volume.cloned_from_snap``) refer to the symbolic
id; the loader resolves them to the actual UUIDs assigned in FDB before
writing.

Secondary-node HA pairs
-----------------------
To configure an HA pair, set ``secondary_node_id`` on the primary node to the
symbolic id of the secondary, and set ``is_secondary: true`` on the secondary
node.  The loader resolves the reference to the actual UUID.

Volume-namespace sharing (shared NQN/subsystem)
-----------------------------------------------
When multiple volumes should share the same NVMe-oF subsystem (as happens
when ``max_namespace_per_subsys > 1``), specify ``namespace_group`` on each
volume with the same string value.  The loader assigns the same NQN to all
volumes in the group.

Size strings
------------
Volume sizes follow the same format as the CLI: ``"1G"`` (gigabytes),
``"512M"`` (mebibytes), ``"1073741824"`` (raw bytes).  The loader converts
them to bytes using the same ``utils.parse_size()`` function the CLI uses.
"""

import json
import os
import time
import uuid as _uuid_mod
from typing import Any, Dict, List, Optional

from simplyblock_core import constants
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.utils import parse_size

# Lazily initialised so the module can be imported without FDB installed
# (needed for syntax/sanity checks in environments without FoundationDB).
_db = None


def _get_db():
    global _db
    if _db is None:
        from simplyblock_core.db_controller import DBController
        _db = DBController()
    return _db


# Module-level alias kept for back-compat with callers that do ``from … import db``
class _LazyDb:
    def __getattr__(self, name):
        return getattr(_get_db(), name)


db = _LazyDb()


# ---------------------------------------------------------------------------
# TestContext
# ---------------------------------------------------------------------------

class TestContext:
    """
    Holds all objects written to FDB during a topology load.  Tests use this
    to look up object UUIDs by symbolic name and to tear down the cluster.
    """

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id
        # symbolic_id → actual UUID string
        self._node_uuid: Dict[str, str] = {}
        self._pool_uuid: Dict[str, str] = {}
        self._lvol_uuid: Dict[str, str] = {}
        self._snap_uuid: Dict[str, str] = {}
        # actual UUID → model object (for remove())
        self._nodes: Dict[str, StorageNode] = {}
        self._pools: Dict[str, Pool] = {}
        self._lvols: Dict[str, LVol] = {}
        self._snaps: Dict[str, SnapShot] = {}

    # ---- lookups by symbolic id ----

    def node(self, sym_id: str) -> StorageNode:
        return self._nodes[self._node_uuid[sym_id]]

    def node_uuid(self, sym_id: str) -> str:
        return self._node_uuid[sym_id]

    def pool(self, sym_id: str) -> Pool:
        return self._pools[self._pool_uuid[sym_id]]

    def lvol(self, sym_id: str) -> LVol:
        return self._lvols[self._lvol_uuid[sym_id]]

    def lvol_uuid(self, sym_id: str) -> str:
        return self._lvol_uuid[sym_id]

    def snap(self, sym_id: str) -> SnapShot:
        return self._snaps[self._snap_uuid[sym_id]]

    def snap_uuid(self, sym_id: str) -> str:
        return self._snap_uuid[sym_id]

    # ---- registration (internal) ----

    def _reg_node(self, sym_id: str, node: StorageNode):
        self._node_uuid[sym_id] = node.uuid
        self._nodes[node.uuid] = node

    def _reg_pool(self, sym_id: str, pool: Pool):
        self._pool_uuid[sym_id] = pool.uuid
        self._pools[pool.uuid] = pool

    def _reg_lvol(self, sym_id: str, lvol: LVol):
        self._lvol_uuid[sym_id] = lvol.uuid
        self._lvols[lvol.uuid] = lvol

    def _reg_snap(self, sym_id: str, snap: SnapShot):
        self._snap_uuid[sym_id] = snap.uuid
        self._snaps[snap.uuid] = snap

    # ---- runtime mutations (usable mid-test for concurrency scenarios) ----

    def add_lvol(self, sym_id: str, node_sym: str,
                 size: str = "1G", pool_sym: str = "",
                 cloned_from_snap: str = "", name: str = "") -> LVol:
        """
        Create a new LVol in FDB and register it in this context.

        ``node_sym`` and optional ``pool_sym`` are symbolic ids already in this
        context.  ``cloned_from_snap`` is an optional symbolic snapshot id.

        Returns the new LVol so tests can inspect or seed it immediately.
        The object is automatically removed on ``teardown()``.
        """
        node = self.node(node_sym)
        pool_uuid = self._pool_uuid.get(pool_sym, "") if pool_sym else ""
        snap_uuid = self._snap_uuid.get(cloned_from_snap, "") if cloned_from_snap else ""

        spec = {
            "id": sym_id,
            "name": name or sym_id,
            "size": size,
            "node_id": node_sym,
            "pool_id": pool_sym,
            "cloned_from_snap": cloned_from_snap,
        }
        lvol = _make_lvol(
            str(_uuid_mod.uuid4()), spec, node,
            self.cluster_id, pool_uuid,
            shared_nqn=None,
        )
        lvol.cloned_from_snap = snap_uuid
        lvol.write_to_db(db.kv_store)
        self._reg_lvol(sym_id, lvol)
        return lvol

    def add_snapshot(self, sym_id: str, lvol_sym: str,
                     snap_ref_sym: str = "", name: str = "") -> SnapShot:
        """
        Create a new SnapShot in FDB and register it in this context.

        ``lvol_sym`` is the symbolic id of the parent lvol.
        ``snap_ref_sym`` is the optional symbolic id of the parent snapshot
        (for building a chain).

        Returns the new SnapShot.  Automatically removed on ``teardown()``.
        """
        lvol = self.lvol(lvol_sym)
        node = self._nodes[lvol.node_id]
        ref_uuid = self._snap_uuid.get(snap_ref_sym, "") if snap_ref_sym else ""

        spec = {
            "id": sym_id,
            "name": name or sym_id,
            "snap_ref_id": snap_ref_sym,
        }
        snap = _make_snap(
            str(_uuid_mod.uuid4()), spec, lvol, node,
            self.cluster_id, ref_uuid,
        )
        snap.write_to_db(db.kv_store)
        self._reg_snap(sym_id, snap)
        return snap

    def remove_lvol(self, sym_id: str):
        """
        Delete an LVol from FDB and unregister it from this context.

        Safe to call even if the object was already removed (e.g. by the
        migration runner during cleanup).
        """
        lvol_uuid = self._lvol_uuid.pop(sym_id, None)
        if lvol_uuid:
            obj = self._lvols.pop(lvol_uuid, None)
            if obj:
                _safe_remove(obj)

    def remove_snapshot(self, sym_id: str):
        """
        Delete a SnapShot from FDB and unregister it from this context.

        Safe to call even if the snapshot was already removed.
        """
        snap_uuid = self._snap_uuid.pop(sym_id, None)
        if snap_uuid:
            obj = self._snaps.pop(snap_uuid, None)
            if obj:
                _safe_remove(obj)

    # ---- teardown ----

    def teardown(self):
        """Remove every object written to FDB by this topology load.

        The cluster itself is NOT removed — it was pre-existing and is shared
        with the control plane.
        """
        for obj in self._snaps.values():
            _safe_remove(obj)
        for obj in self._lvols.values():
            _safe_remove(obj)
        for obj in self._pools.values():
            _safe_remove(obj)
        for obj in self._nodes.values():
            _safe_remove(obj)


def _safe_remove(obj):
    try:
        obj.remove(db.kv_store)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Main loader entry points
# ---------------------------------------------------------------------------

def load_topology_file(path: str) -> TestContext:
    """Load a topology JSON file and populate FDB.  Returns a TestContext."""
    with open(path, 'r', encoding='utf-8') as fh:
        spec = json.load(fh)
    return load_topology(spec)


def load_topology(spec: Dict[str, Any]) -> TestContext:
    """
    Load a topology from a dict (already parsed from JSON) and populate FDB.
    Returns a TestContext.

    The cluster must already exist in FDB (created during control-plane setup).
    ``cluster.id`` in the spec selects which cluster to attach nodes/volumes/
    snapshots to.  If omitted the first cluster found in FDB is used.
    """
    _validate_spec(spec)

    cluster_spec = spec.get('cluster', {})
    cluster_id = cluster_spec.get('id')

    if cluster_id:
        cluster = db.get_cluster_by_id(cluster_id)
    else:
        clusters = db.get_clusters()
        if not clusters:
            raise RuntimeError(
                "No cluster found in FDB. "
                "Run control-plane setup before executing migration tests.")
        cluster = clusters[0]
        cluster_id = cluster.uuid

    ctx = TestContext(cluster_id)

    # --- Resolve node secondary references (symbolic → uuid) ---
    # First pass: assign UUIDs to all nodes
    node_sym_to_uuid: Dict[str, str] = {}
    for n_spec in spec.get('nodes', []):
        sym = _req_field(n_spec, 'id', 'node')
        node_sym_to_uuid[sym] = str(_uuid_mod.uuid4())

    # Second pass: build StorageNode objects
    for n_spec in spec.get('nodes', []):
        sym = n_spec['id']
        actual_uuid = node_sym_to_uuid[sym]
        sec_sym = n_spec.get('secondary_node_id', '')
        sec_uuid = node_sym_to_uuid.get(sec_sym, '') if sec_sym else ''
        node = _make_node(actual_uuid, n_spec, cluster_id, sec_uuid)
        node.write_to_db(db.kv_store)
        ctx._reg_node(sym, node)

    # --- Pools ---
    pool_sym_to_uuid: Dict[str, str] = {}
    for p_spec in spec.get('pools', []):
        sym = _req_field(p_spec, 'id', 'pool')
        pool_uuid = str(_uuid_mod.uuid4())
        pool_sym_to_uuid[sym] = pool_uuid
        pool = _make_pool(pool_uuid, p_spec, cluster_id)
        pool.write_to_db(db.kv_store)
        ctx._reg_pool(sym, pool)

    # --- Namespace-group → NQN mapping ---
    # Volumes in the same namespace_group share an NQN subsystem.
    ns_group_nqn: Dict[str, str] = {}
    for v_spec in spec.get('volumes', []):
        grp = v_spec.get('namespace_group', '')
        if grp and grp not in ns_group_nqn:
            ns_group_nqn[grp] = (
                f"nqn.2023-02.io.simplyblock:{cluster_id[:8]}:grp:{grp}")

    # --- Volumes ---
    lvol_sym_to_uuid: Dict[str, str] = {}
    for v_spec in spec.get('volumes', []):
        sym = _req_field(v_spec, 'id', 'volume')
        lvol_uuid = str(_uuid_mod.uuid4())
        lvol_sym_to_uuid[sym] = lvol_uuid

    for v_spec in spec.get('volumes', []):
        sym = v_spec['id']
        actual_uuid = lvol_sym_to_uuid[sym]

        node_sym = _req_field(v_spec, 'node_id', 'volume')
        node_uuid = node_sym_to_uuid.get(node_sym)
        if node_uuid is None:
            raise ValueError(f"Volume '{sym}': unknown node_id '{node_sym}'")
        node = ctx._nodes[node_uuid]

        pool_sym = v_spec.get('pool_id', '')
        pool_uuid = pool_sym_to_uuid.get(pool_sym, '') if pool_sym else ''

        clone_snap_sym = v_spec.get('cloned_from_snap', '')
        # cloned_from_snap may reference a snap symbolic id – resolve later
        # (snaps not yet built), so we store as pending sym
        _cloned_sym = clone_snap_sym  # resolved after snap loop below

        grp = v_spec.get('namespace_group', '')
        nqn = ns_group_nqn.get(grp) if grp else None

        lvol = _make_lvol(actual_uuid, v_spec, node, cluster_id, pool_uuid, nqn)
        # cloned_from_snap will be patched below after snapshots are resolved
        lvol.write_to_db(db.kv_store)
        ctx._reg_lvol(sym, lvol)
        v_spec['_cloned_sym'] = _cloned_sym   # stash for second pass

    # --- Snapshots ---
    snap_sym_to_uuid: Dict[str, str] = {}
    for s_spec in spec.get('snapshots', []):
        sym = _req_field(s_spec, 'id', 'snapshot')
        snap_sym_to_uuid[sym] = str(_uuid_mod.uuid4())

    for s_spec in spec.get('snapshots', []):
        sym = s_spec['id']
        actual_uuid = snap_sym_to_uuid[sym]

        lvol_sym = _req_field(s_spec, 'lvol_id', 'snapshot')
        lvol_uuid = lvol_sym_to_uuid.get(lvol_sym)
        if lvol_uuid is None:
            raise ValueError(f"Snapshot '{sym}': unknown lvol_id '{lvol_sym}'")
        lvol = ctx._lvols[lvol_uuid]
        node = ctx._nodes.get(lvol.node_id)
        if node is None:
            raise ValueError(f"Snapshot '{sym}': node '{lvol.node_id}' not found")

        ref_sym = s_spec.get('snap_ref_id', '')
        ref_uuid = snap_sym_to_uuid.get(ref_sym, '') if ref_sym else ''

        snap = _make_snap(actual_uuid, s_spec, lvol, node, cluster_id, ref_uuid)
        snap.write_to_db(db.kv_store)
        ctx._reg_snap(sym, snap)

    # --- Patch lvol.cloned_from_snap now that snap UUIDs are known ---
    for v_spec in spec.get('volumes', []):
        sym = v_spec['id']
        clone_sym = v_spec.get('_cloned_sym', '')
        if clone_sym:
            snap_uuid = snap_sym_to_uuid.get(clone_sym)
            if snap_uuid is None:
                raise ValueError(
                    f"Volume '{sym}': cloned_from_snap references unknown "
                    f"snapshot id '{clone_sym}'")
            lvol = ctx.lvol(sym)
            lvol.cloned_from_snap = snap_uuid
            lvol.write_to_db(db.kv_store)

    return ctx


# ---------------------------------------------------------------------------
# Object factory helpers
# ---------------------------------------------------------------------------

def _req_field(spec: dict, key: str, obj_type: str) -> Any:
    if key not in spec:
        raise ValueError(f"{obj_type} spec is missing required field '{key}'")
    return spec[key]



def _make_node(node_uuid: str, spec: dict, cluster_id: str,
               secondary_uuid: str) -> StorageNode:
    nics = []
    for nic_spec in spec.get('data_nics', []):
        nic = IFace()
        nic.if_name = nic_spec.get('if_name', 'eth0')
        nic.ip4_address = nic_spec.get('ip', spec.get('mgmt_ip', '127.0.0.1'))
        nic.trtype = nic_spec.get('trtype', 'TCP')
        nic.net_type = nic_spec.get('net_type', 'data')
        nics.append(nic)

    if not nics:
        # Default single NIC pointing at mgmt_ip
        nic = IFace()
        nic.if_name = 'eth0'
        nic.ip4_address = spec.get('mgmt_ip', '127.0.0.1')
        nic.trtype = 'TCP'
        nic.net_type = 'data'
        nics.append(nic)

    hub = HubLVol()
    hub.uuid = str(_uuid_mod.uuid4())
    hub.nqn = f"nqn.2023-02.io.simplyblock:hub:{node_uuid[:8]}"
    hub.bdev_name = "hub0"
    hub.nvmf_port = int(spec.get('hub_port', 4420))

    n = StorageNode()
    n.uuid = node_uuid
    n.cluster_id = cluster_id
    n.status = spec.get('status', StorageNode.STATUS_ONLINE)
    n.hostname = spec.get('hostname', f"host-{spec['id']}")
    n.mgmt_ip = spec.get('mgmt_ip', '127.0.0.1')
    n.rpc_port = int(spec.get('rpc_port', 9901))
    n.rpc_username = spec.get('rpc_username', 'spdkuser')
    n.rpc_password = spec.get('rpc_password', 'spdkpass')
    n.lvstore = spec.get('lvstore', f"lvs_{spec['id']}")
    n.lvol_subsys_port = int(spec.get('lvol_subsys_port', 9090))
    n.max_lvol = int(spec.get('max_lvol', 256))
    n.max_snap = int(spec.get('max_snap', 5000))
    n.secondary_node_id = secondary_uuid
    n.data_nics = nics
    n.active_tcp = 'tcp' in spec.get('fabric', 'tcp')
    n.active_rdma = 'rdma' in spec.get('fabric', 'tcp')
    n.hublvol = hub
    return n


def _make_pool(pool_uuid: str, spec: dict, cluster_id: str) -> Pool:
    p = Pool()
    p.uuid = pool_uuid
    p.cluster_id = cluster_id
    p.pool_name = spec.get('name', f"pool-{pool_uuid[:8]}")
    p.status = spec.get('status', Pool.STATUS_ACTIVE)
    if spec.get('pool_max'):
        p.pool_max_size = parse_size(spec['pool_max'])
    if spec.get('lvol_max'):
        p.lvol_max_size = parse_size(spec['lvol_max'])
    return p


def _make_lvol(lvol_uuid: str, spec: dict, node: StorageNode,
               cluster_id: str, pool_uuid: str,
               shared_nqn: Optional[str]) -> LVol:
    size_bytes = parse_size(str(spec.get('size', '1G')))
    max_size_bytes = parse_size(str(spec.get('max_size', '1000T')))
    nqn = shared_nqn or f"nqn.2023-02.io.simplyblock:{lvol_uuid[:8]}"

    l = LVol()
    l.uuid = lvol_uuid
    l.cluster_id = cluster_id
    l.node_id = node.uuid
    l.pool_uuid = pool_uuid
    l.status = spec.get('status', LVol.STATUS_ONLINE)
    l.lvol_name = spec.get('name', f"vol-{lvol_uuid[:8]}")
    l.lvol_bdev = f"lvol_{spec.get('name', lvol_uuid[:8])}"
    l.lvs_name = node.lvstore
    l.size = size_bytes
    l.max_size = max_size_bytes
    l.ha_type = spec.get('ha_type', 'single')
    l.fabric = spec.get('fabric', 'tcp')
    l.nqn = nqn
    l.ns_id = int(spec.get('ns_id', 1))
    l.subsys_port = node.lvol_subsys_port
    l.max_namespace_per_subsys = int(spec.get('max_namespace_per_subsys', 1))
    l.cloned_from_snap = ''  # patched after snapshot loop
    l.rw_ios_per_sec = int(spec.get('max_rw_iops', 0))
    l.rw_mbytes_per_sec = int(spec.get('max_rw_mbytes', 0))
    l.r_mbytes_per_sec = int(spec.get('max_r_mbytes', 0))
    l.w_mbytes_per_sec = int(spec.get('max_w_mbytes', 0))
    return l


def _make_snap(snap_uuid: str, spec: dict, lvol: LVol, node: StorageNode,
               cluster_id: str, snap_ref_uuid: str) -> SnapShot:
    snap_name = spec.get('name', f"snap-{snap_uuid[:8]}")
    snap_bdev = f"{node.lvstore}/snap_{snap_name}"

    s = SnapShot()
    s.uuid = snap_uuid
    s.cluster_id = cluster_id
    s.snap_name = snap_name
    s.snap_bdev = snap_bdev
    s.snap_uuid = snap_uuid
    s.snap_ref_id = snap_ref_uuid
    s.status = spec.get('status', SnapShot.STATUS_ONLINE)
    s.created_at = int(spec.get('created_at', time.time()))
    s.used_size = lvol.size
    s.size = lvol.size
    s.lvol = lvol  # nested object carries node_id / uuid for the DB query path
    return s


# ---------------------------------------------------------------------------
# Status mutation helpers  (also used by test_ctl.py)
# ---------------------------------------------------------------------------

def set_cluster_status(cluster_id: str, status: str):
    cluster = db.get_cluster_by_id(cluster_id)
    cluster.status = status
    cluster.write_to_db(db.kv_store)


def set_node_status(node_id: str, status: str):
    node = db.get_storage_node_by_id(node_id)
    node.status = status
    node.write_to_db(db.kv_store)


def set_lvol_status(lvol_id: str, status: str):
    lvol = db.get_lvol_by_id(lvol_id)
    lvol.status = status
    lvol.write_to_db(db.kv_store)


def set_snap_status(snap_id: str, status: str):
    snap = db.get_snapshot_by_id(snap_id)
    snap.status = status
    snap.write_to_db(db.kv_store)


# ---------------------------------------------------------------------------
# Validate spec (basic structural checks)
# ---------------------------------------------------------------------------

def _validate_spec(spec: dict):
    if not isinstance(spec, dict):
        raise ValueError("topology spec must be a JSON object")
    if 'nodes' not in spec or not spec['nodes']:
        raise ValueError("topology spec must have at least one node")
    node_ids = set()
    for n in spec.get('nodes', []):
        if 'id' not in n:
            raise ValueError("every node must have an 'id' field")
        if n['id'] in node_ids:
            raise ValueError(f"duplicate node id: {n['id']}")
        node_ids.add(n['id'])
    for n in spec.get('nodes', []):
        sec = n.get('secondary_node_id', '')
        if sec and sec not in node_ids:
            raise ValueError(
                f"node '{n['id']}' secondary_node_id '{sec}' not in nodes list")
    for v in spec.get('volumes', []):
        if 'id' not in v:
            raise ValueError("every volume must have an 'id'")
        if 'node_id' not in v:
            raise ValueError(f"volume '{v['id']}' is missing 'node_id'")
    for s in spec.get('snapshots', []):
        if 'id' not in s:
            raise ValueError("every snapshot must have an 'id'")
        if 'lvol_id' not in s:
            raise ValueError(f"snapshot '{s['id']}' is missing 'lvol_id'")
