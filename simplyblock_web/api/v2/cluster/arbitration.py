"""Two-node arbitration endpoints (docs/design/two-node-arbitration.md, section 7.4)."""

import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import events_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.arbitration import ClusterArbitration
from simplyblock_core.models.storage_node import StorageNode

from .._dependencies import Cluster

api = APIRouter()
db = DBController()


def _members(cluster_id: str) -> list:
    return [n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
            if n.status != StorageNode.STATUS_REMOVED]


def _record(cluster_id: str) -> ClusterArbitration:
    rec = db.get_cluster_arbitration(cluster_id)
    if rec is None:
        nodes = sorted(_members(cluster_id), key=lambda n: n.get_id())
        if len(nodes) != 2:
            raise HTTPException(409, "Arbitration applies only to two-node clusters")
        rec = ClusterArbitration()
        rec.cluster_id = cluster_id
        rec.preferred_node = nodes[0].get_id()
        rec.write_to_db(db.kv_store)
    return rec


@api.get('/', name='clusters:arbitration:get')
def get(cluster: Cluster) -> dict:
    """The arbitration record plus live lease ages (console, operator, support)."""
    rec = _record(cluster.get_id())
    now = int(time.time() * 1000)
    out = rec.get_clean_dict()
    out["enabled"] = bool(cluster.two_node_arbitration)
    out["lease_age_ms"] = {nid: now - int(lease.get("granted_at", 0))
                           for nid, lease in rec.leases.items()}
    out["fenced_taint"] = constants.TWO_NODE_FENCED_TAINT
    return out


class _Preferred(BaseModel):
    node_id: str


@api.put('/preferred', name='clusters:arbitration:preferred', status_code=204)
def set_preferred(cluster: Cluster, body: _Preferred) -> None:
    """Choose the node that continues alone when neither the CP nor the peer is
    reachable. Pushed to the nodes by the arbiter with ``jc_set_dual_node``."""
    if body.node_id not in {n.get_id() for n in _members(cluster.get_id())}:
        raise HTTPException(422, "node_id is not a member of this cluster")
    rec = _record(cluster.get_id())
    db.atomic_update(rec, lambda fresh: setattr(fresh, "preferred_node", body.node_id))
    events_controller.log_event_cluster(
        cluster.get_id(), events_controller.DOMAIN_CLUSTER, "TWO_NODE_PREFERRED", cluster,
        events_controller.CAUSED_BY_API, f"Preferred node set to {body.node_id}")


class _Override(BaseModel):
    winner: str
    reason: str = Field(min_length=10, max_length=1024)


@api.post('/override', name='clusters:arbitration:override', status_code=202)
def override(cluster: Cluster, body: _Override) -> None:
    """Operator decision when the arbiter cannot decide: ``winner`` leads every
    LVS. Counts as positive fencing of the other node; audited."""
    if body.winner not in {n.get_id() for n in _members(cluster.get_id())}:
        raise HTTPException(422, "winner is not a member of this cluster")
    rec = _record(cluster.get_id())
    order = {"winner": body.winner, "reason": body.reason, "by": "api",
             "at": int(time.time() * 1000)}
    db.atomic_update(rec, lambda fresh: setattr(fresh, "override", order))
    events_controller.log_event_cluster(
        cluster.get_id(), events_controller.DOMAIN_CLUSTER, "TWO_NODE_OVERRIDE", cluster,
        events_controller.CAUSED_BY_API,
        f"Arbitration override: {body.winner} leads; reason: {body.reason}")


class _Remediation(BaseModel):
    node_id: str
    fenced: bool


@api.put('/remediation', name='clusters:arbitration:remediation', status_code=204)
def set_remediation(cluster: Cluster, body: _Remediation) -> None:
    """The edge operator reports positive fencing evidence for a node (BMC
    fence done, ``out-of-service`` taint present), or clears it."""
    node = next((n for n in _members(cluster.get_id()) if n.get_id() == body.node_id), None)
    if node is None:
        raise HTTPException(422, "node_id is not a member of this cluster")
    db.atomic_update(node, lambda fresh: setattr(fresh, "remediation_fenced", body.fenced))
