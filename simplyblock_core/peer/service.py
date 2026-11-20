"""The target-side half of the peer protocol, run against the local database.

Each function answers for a cluster managed by THIS control plane. The v2
``/_peer`` router calls them on behalf of a remote control plane, and
``LocalPeerClient`` calls them directly when source and target share one
control plane, so both paths run the same code.
"""
from uuid import UUID

from simplyblock_core.db_controller import DBController
from simplyblock_core.peer.protocol import CAPABILITIES, PEER_API_VERSION, PeerInfo


def info(cluster_id: str) -> PeerInfo:
    """Identity and protocol version for *cluster_id*.

    Raises KeyError when this control plane does not manage the cluster.
    """
    cluster = DBController().get_cluster_by_id(cluster_id)
    return PeerInfo(
        cluster_id=UUID(cluster.get_id()),
        api_version=PEER_API_VERSION,
        capabilities=sorted(CAPABILITIES),
    )
