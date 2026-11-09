"""Internal API for a peer control plane (the control-plane split).

Mounted at ``/_peer/clusters/{cluster_id}``. A source control plane calls it to
run the target-side steps of replication on a cluster this control plane
manages. ``verify_api_token`` takes ``cluster_id`` from the path, so the caller
must present that cluster's own secret (or an admin credential): a peer holding
one cluster's secret reaches no other cluster here.

Each route is a thin wrapper around ``simplyblock_core.peer.service``, which
``LocalPeerClient`` calls in-process, so both paths run the same code.

Statuses the peer client maps back to its exceptions: 404 for an unknown
cluster or object (``PeerNotFoundError``), 409 for a step the target refuses
(``PeerRefusedError``).
"""
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response

from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import (
    ChainRequest,
    ChainResponse,
    DeleteSnapshotResponse,
    DescribeRequest,
    DescribeResponse,
    EnsureHubRequest,
    EnsureHubResponse,
    FinishRequest,
    FinishResponse,
    LandingRequest,
    LandingResponse,
    PeerInfo,
)

from ._dependencies import Cluster

api = APIRouter()


@api.get('/info', name='peer:info')
def get_info(cluster: Cluster) -> PeerInfo:
    """Identity, peer API version and capabilities of this cluster."""
    return service.info(cluster.get_id())


@api.delete('/landing-volumes/{lvol_id}', name='peer:landing-volumes:delete', status_code=204,
            responses={204: {"content": None}})
def discard_landing(cluster: Cluster, lvol_id: UUID) -> Response:
    """Delete a replication landing volume; one that is already gone is success."""
    try:
        service.discard_landing(cluster.get_id(), str(lvol_id))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PreconditionError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return Response(status_code=204)


@api.post('/describe', name='peer:describe')
def describe(cluster: Cluster, request: DescribeRequest) -> DescribeResponse:
    """Read-only answers about this cluster's side of a replication: who leads a
    landing volume's lvstore, whether copies exist or have dependent clones, and
    whether copies are chained. POST only because the question has a body."""
    return service.describe(cluster.get_id(), request)


@api.post('/landing-volumes', name='peer:landing-volumes:prepare')
def prepare_landing(cluster: Cluster, request: LandingRequest) -> LandingResponse:
    """Make a replicated snapshot's landing volume ready: adopt one a previous
    request made, report a copy of the same data, or create it."""
    try:
        return service.prepare_landing(cluster.get_id(), request)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PreconditionError as e:
        raise HTTPException(status_code=409, detail=str(e))


@api.post('/landing-volumes/{lvol_id}/hub', name='peer:landing-volumes:hub')
def ensure_hub(cluster: Cluster, lvol_id: UUID) -> EnsureHubResponse:
    """Get this cluster ready to receive a transfer into a landing volume: its
    settled leader, the receiving hub's attach address and the volume's map id."""
    try:
        return service.ensure_hub(cluster.get_id(), EnsureHubRequest(landing_lvol_id=str(lvol_id)))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@api.post('/chain', name='peer:chain')
def chain(cluster: Cluster, request: ChainRequest) -> ChainResponse:
    """Chain a landing volume onto the copy of the snapshot before it, on the
    leader and its secondary, so the transfer can ship only the delta."""
    try:
        return service.chain(cluster.get_id(), request)
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))


@api.post('/finish', name='peer:finish')
def finish(cluster: Cluster, request: FinishRequest) -> FinishResponse:
    """Make a landing volume the copy of its source snapshot after the transfer:
    chain, merge and convert it, record the copy and retire the landing volume."""
    return service.finish(cluster.get_id(), request)


@api.delete('/snapshots/{snapshot_id}', name='peer:snapshots:delete')
def delete_snapshot(cluster: Cluster, snapshot_id: UUID) -> DeleteSnapshotResponse:
    """Delete a replicated copy (retention); one already gone counts as deleted."""
    try:
        return service.delete_snapshot(cluster.get_id(), str(snapshot_id))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PreconditionError as e:
        raise HTTPException(status_code=409, detail=str(e))
