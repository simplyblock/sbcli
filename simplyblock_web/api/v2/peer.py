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
from simplyblock_core.peer.protocol import PeerInfo

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
