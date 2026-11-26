"""Internal API for a peer control plane (the control-plane split).

Mounted at ``/_peer/clusters/{cluster_id}``. A source control plane calls it to
run the target-side steps of replication on a cluster this control plane
manages. ``verify_api_token`` takes ``cluster_id`` from the path, so the caller
must present that cluster's own secret (or an admin credential): a peer holding
one cluster's secret reaches no other cluster here.

Each route is a thin wrapper around ``simplyblock_core.peer.service``, which
``LocalPeerClient`` calls in-process, so both paths run the same code.
"""
from fastapi import APIRouter

from simplyblock_core.peer import service
from simplyblock_core.peer.protocol import PeerInfo

from ._dependencies import Cluster

api = APIRouter()


@api.get('/info', name='peer:info')
def get_info(cluster: Cluster) -> PeerInfo:
    """Identity, peer API version and capabilities of this cluster."""
    return service.info(cluster.get_id())
