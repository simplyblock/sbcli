"""Two-node arbitration endpoints (docs/design/two-node-arbitration.md, section 7.4)."""

import time
from typing import Annotated

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from simplyblock_core import constants
from simplyblock_core.controllers import events_controller
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.arbitration import (
    HF_RELEASABLE,
    HF_RESTORING,
    ClusterArbitration,
)
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
    # Campaign c3, F10: a node re-admitted after a fence is not a full replica
    # until its data rebuild has finished.
    out["rebuild_pending"] = sorted(rec.rebuild or {})
    out["redundant"] = not rec.rebuild and not rec.data_unavailable
    # Campaign c4, F12: rebuilds that cannot progress because a device is not
    # available (the node was handed to the restart flow).
    out["rebuild_stuck"] = sorted(nid for nid, mark in (rec.rebuild or {}).items()
                                  if (mark or {}).get("stuck"))
    # docs/design/two-node-kubernetes-fencing.md: the operator sets the
    # StorageFenced condition on a node whose host fence is ``revoked``.
    out["fencing_authority"] = cluster.two_node_fencing_authority or constants.TWO_NODE_AUTHORITY_WITNESS
    out["host_fencing"] = bool(cluster.two_node_host_fencing)
    out["storage_fenced_condition"] = constants.TWO_NODE_STORAGE_FENCED_CONDITION
    out["client_hosts"] = {n.get_id(): list(n.client_host_nqns) for n in _members(cluster.get_id())}
    return out


class _Preferred(BaseModel):
    node_id: str


@api.put('/preferred', name='clusters:arbitration:preferred', status_code=204, deprecated=True)
def set_preferred(cluster: Cluster, body: _Preferred) -> None:
    """Deprecated, ignored: there is no preferred node any more. At the hold
    deadline without a verdict a node always fences itself, and a partition
    fences both nodes. Accepted (204) so older operators keep working; nothing
    is stored and nothing is pushed to the nodes."""
    if body.node_id not in {n.get_id() for n in _members(cluster.get_id())}:
        raise HTTPException(422, "node_id is not a member of this cluster")


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


class _ClientHost(BaseModel):
    node_id: str
    host_nqns: list[Annotated[str, Field(max_length=223)]] = Field(default_factory=list, max_length=8)


@api.put('/client-host', name='clusters:arbitration:client-host', status_code=204)
def set_client_host(cluster: Cluster, body: _ClientHost) -> None:
    """The operator reports the kernel NVMe host NQNs of the Kubernetes node a
    storage node runs on (the kernel default and the simplyblock form); a host
    fence revokes all of them on the peer
    (docs/design/two-node-kubernetes-fencing.md, section 3.1). Empty clears them."""
    node = next((n for n in _members(cluster.get_id()) if n.get_id() == body.node_id), None)
    if node is None:
        raise HTTPException(422, "node_id is not a member of this cluster")
    nqns = sorted({h.strip() for h in body.host_nqns if h.strip()})
    if any(not h.startswith("nqn.") for h in nqns):
        raise HTTPException(422, "every host NQN must be an NVMe qualified name")
    # A host NQN two members report cannot tell their hosts apart: a host fence
    # would revoke the survivor's own access (review on #1527).
    shared = sorted({h for n in _members(cluster.get_id()) if n.get_id() != body.node_id
                     for h in (n.client_host_nqns or [])} & set(nqns))
    if shared:
        raise HTTPException(409, "host NQNs already reported for another storage node: "
                                 + ", ".join(shared))
    db.atomic_update(node, lambda fresh: setattr(fresh, "client_host_nqns", nqns))


class _HostFenceRelease(BaseModel):
    node_id: str


@api.post('/host-fence/release', name='clusters:arbitration:host-fence:release', status_code=202)
def release_host_fence(cluster: Cluster, body: _HostFenceRelease) -> None:
    """The operator verified that the node is clean (Ready, no pod from before
    the fence): give its host access back (contract section 3.2). Accepted only
    once the storage no longer needs the fence (state ``releasable``)."""
    rec = _record(cluster.get_id())
    entry = (rec.host_fences or {}).get(body.node_id)
    if not entry:
        raise HTTPException(404, "no host fence for this node")
    if entry.get("state") not in (HF_RELEASABLE, HF_RESTORING):
        raise HTTPException(409, "host fence is %s, not releasable yet" % entry.get("state"))

    def put(fresh):
        fences = dict(fresh.host_fences or {})
        cur = dict(fences.get(body.node_id) or {})
        if cur.get("state") not in (HF_RELEASABLE, HF_RESTORING):
            return False
        cur.update(state=HF_RESTORING, release_requested=True)
        fences[body.node_id] = cur
        fresh.host_fences = fences
    db.atomic_update(rec, put)
    events_controller.log_event_cluster(
        cluster.get_id(), events_controller.DOMAIN_CLUSTER, "TWO_NODE_HOST_FENCE_RELEASE", cluster,
        events_controller.CAUSED_BY_API, f"Host fence release requested for {body.node_id}")
