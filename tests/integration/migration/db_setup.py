# coding=utf-8
"""
db_setup.py – helpers to pre-populate FDB with control-plane objects
(StorageNode, LVol, SnapShot) that migration tests need.

The cluster must already exist in FDB (created during control-plane setup).
Use ClusterSpec.cluster_id to reference the pre-existing cluster.

Usage::

    spec = ClusterSpec(
        cluster_id="<existing-cluster-uuid>",
        nodes=[
            NodeSpec(node_id="src-node", host="127.0.0.1", rpc_port=9901,
                     lvstore="lvs_src"),
            NodeSpec(node_id="tgt-node", host="127.0.0.1", rpc_port=9902,
                     lvstore="lvs_tgt"),
        ],
    )
    ctx = setup_cluster(spec)       # writes nodes to FDB, returns TestContext
    # ... run tests using ctx.cluster_id, ctx.node('src-node'), etc.
    teardown_cluster(ctx)           # removes written nodes/lvols/snaps (not cluster)
"""

import time
import uuid as _uuid_mod
from dataclasses import dataclass, field
from typing import Dict, List

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode

db = DBController()


# ---------------------------------------------------------------------------
# Specification dataclasses (plain data – no DB access)
# ---------------------------------------------------------------------------

@dataclass
class NodeSpec:
    """Describes one storage node to create in FDB."""
    node_id: str
    host: str          # mgmt_ip / data NIC IP (same for tests)
    rpc_port: int
    lvstore: str
    lvol_subsys_port: int = 9090
    secondary_node_id: str = ""
    status: str = StorageNode.STATUS_ONLINE
    rpc_username: str = "spdkuser"
    rpc_password: str = "spdkpass"


@dataclass
class ClusterSpec:
    """References a pre-existing cluster and lists nodes to attach to it."""
    cluster_id: str
    nodes: List[NodeSpec] = field(default_factory=list)


# ---------------------------------------------------------------------------
# TestContext – returned by setup_cluster, used by tests
# ---------------------------------------------------------------------------

class TestContext:
    """Holds references to all objects created by a setup_cluster() call."""

    def __init__(self, spec: ClusterSpec):
        self.cluster_id: str = spec.cluster_id
        self._nodes: Dict[str, StorageNode] = {}
        self._lvols: Dict[str, LVol] = {}        # symbolic_name → LVol
        self._snaps: Dict[str, SnapShot] = {}     # symbolic_name → SnapShot

    def register_node(self, node_id: str, node: StorageNode):
        self._nodes[node_id] = node

    def node(self, node_id: str) -> StorageNode:
        return self._nodes[node_id]

    def register_lvol(self, name: str, lvol: LVol):
        self._lvols[name] = lvol

    def lvol(self, name: str) -> LVol:
        return self._lvols[name]

    def register_snap(self, name: str, snap: SnapShot):
        self._snaps[name] = snap

    def snap(self, name: str) -> SnapShot:
        return self._snaps[name]

    def all_node_ids(self) -> List[str]:
        return list(self._nodes.keys())


# ---------------------------------------------------------------------------
# Cluster / node creation
# ---------------------------------------------------------------------------

def setup_cluster(spec: ClusterSpec) -> TestContext:
    """
    Attach StorageNodes to a pre-existing cluster in FDB.
    Returns a TestContext for use in tests.
    """
    ctx = TestContext(spec)

    for nspec in spec.nodes:
        node = _make_storage_node(nspec, spec.cluster_id)
        node.write_to_db(db.kv_store)
        ctx.register_node(nspec.node_id, node)

    return ctx


def teardown_cluster(ctx: TestContext):
    """Remove nodes, lvols, and snapshots written for this test.

    The cluster itself is NOT removed — it is pre-existing and shared with
    the control plane.
    """
    for lvol in ctx._lvols.values():
        try:
            lvol.remove(db.kv_store)
        except Exception:
            pass
    for snap in ctx._snaps.values():
        try:
            snap.remove(db.kv_store)
        except Exception:
            pass
    for node in ctx._nodes.values():
        try:
            node.remove(db.kv_store)
        except Exception:
            pass


def _make_storage_node(spec: NodeSpec, cluster_id: str) -> StorageNode:
    nic = IFace()
    nic.if_name = "eth0"
    nic.ip4_address = spec.host
    nic.trtype = "TCP"
    nic.net_type = "data"

    hub = HubLVol()
    hub.uuid = str(_uuid_mod.uuid4())
    hub.nqn = f"nqn.2023-02.io.simplyblock:hub:{spec.node_id}"
    hub.bdev_name = "hub0"
    hub.nvmf_port = 4420

    node = StorageNode()
    node.uuid = spec.node_id
    node.cluster_id = cluster_id
    node.status = spec.status
    node.mgmt_ip = spec.host
    node.rpc_port = spec.rpc_port
    node.rpc_username = spec.rpc_username
    node.rpc_password = spec.rpc_password
    node.lvstore = spec.lvstore
    node.lvol_subsys_port = spec.lvol_subsys_port
    node.secondary_node_id = spec.secondary_node_id
    node.data_nics = [nic]
    node.active_tcp = True
    node.active_rdma = False
    node.hublvol = hub
    node.hostname = f"host-{spec.node_id}"
    return node


# ---------------------------------------------------------------------------
# LVol / snapshot helpers
# ---------------------------------------------------------------------------

def add_lvol(ctx: TestContext, name: str, node_id: str,
             size_mib: int = 1024, ha_type: str = "single",
             nqn: str = "", ns_id: int = 1,
             cloned_from_snap: str = "") -> LVol:
    """
    Create an LVol DB record and register it in *ctx*.

    ``name`` is the symbolic test name (e.g. "l1"); ``node_id`` must be one
    of the node IDs registered in *ctx*.
    """
    node = ctx.node(node_id)
    lvol_uuid = str(_uuid_mod.uuid4())
    bdev_name = f"lvol_{name}"

    lvol = LVol()
    lvol.uuid = lvol_uuid
    lvol.cluster_id = ctx.cluster_id
    lvol.node_id = node_id
    lvol.status = LVol.STATUS_ONLINE
    lvol.lvol_name = name
    lvol.lvol_bdev = bdev_name
    lvol.lvs_name = node.lvstore
    lvol.size = size_mib * 1024 * 1024
    lvol.ha_type = ha_type
    lvol.nqn = nqn or f"nqn.2023-02.io.simplyblock:{lvol_uuid[:8]}"
    lvol.ns_id = ns_id
    lvol.subsys_port = node.lvol_subsys_port
    lvol.cloned_from_snap = cloned_from_snap
    lvol.fabric = "tcp"
    lvol.write_to_db(db.kv_store)

    ctx.register_lvol(name, lvol)
    return lvol


def add_snapshot(ctx: TestContext, name: str, parent_lvol_name: str,
                 snap_ref_id: str = "") -> SnapShot:
    """
    Create a SnapShot DB record and register it in *ctx*.

    ``parent_lvol_name`` must be a symbolic lvol name already in *ctx*.
    ``snap_ref_id`` is the UUID of the parent snapshot (for clone chains).
    """
    lvol = ctx.lvol(parent_lvol_name)
    node = ctx.node(lvol.node_id)

    snap_uuid = str(_uuid_mod.uuid4())
    snap_bdev = f"{node.lvstore}/snap_{name}"

    snap = SnapShot()
    snap.uuid = snap_uuid
    snap.cluster_id = ctx.cluster_id
    snap.snap_name = name
    snap.snap_bdev = snap_bdev
    snap.snap_uuid = snap_uuid
    snap.snap_ref_id = snap_ref_id
    snap.status = SnapShot.STATUS_ONLINE
    snap.created_at = int(time.time())
    snap.used_size = lvol.size
    snap.size = lvol.size
    snap.lvol = lvol          # nested LVol object carries node_id / uuid
    snap.write_to_db(db.kv_store)

    ctx.register_snap(name, snap)
    return snap


# ---------------------------------------------------------------------------
# Status mutation helpers (used by test CLI and scenario helpers)
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
