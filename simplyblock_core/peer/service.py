"""The target-side half of the peer protocol, run against the local database.

Each function answers for a cluster managed by THIS control plane. The v2
``/_peer`` router calls them on behalf of a remote control plane, and
``LocalPeerClient`` calls them directly when source and target share one
control plane, so both paths run the same code.
"""
import logging
from uuid import UUID

from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.peer.protocol import (
    CAPABILITIES,
    LANDING_PREFIX,
    PEER_API_VERSION,
    PeerInfo,
)

logger = logging.getLogger(__name__)


class NotALandingVolumeError(PreconditionError):
    """The volume a peer asked to act on is not a replication landing volume."""


def info(cluster_id: str) -> PeerInfo:
    """Identity and protocol version for *cluster_id*.

    Raises:
        KeyError: this control plane does not manage the cluster.
    """
    cluster = DBController().get_cluster_by_id(cluster_id)
    return PeerInfo(
        cluster_id=UUID(cluster.get_id()),
        api_version=PEER_API_VERSION,
        capabilities=sorted(CAPABILITIES),
    )


def discard_landing(cluster_id: str, lvol_id: str) -> None:
    """Delete the replication landing volume *lvol_id* on *cluster_id*.

    Idempotent: a volume that is already gone is success, so a source that
    retries after a lost answer gets the same result.

    Raises:
        KeyError: this control plane does not manage *cluster_id*, or the volume
            exists but belongs to another cluster.
        NotALandingVolumeError: the volume is not a landing volume. The peer API
            never deletes a volume a replication transfer did not create.
        PreconditionError: the delete itself is refused (e.g. an active migration).
    """
    # Imported here: lvol_controller pulls in most of the controllers, and the
    # peer client (which imports this module) is imported by them in turn.
    from simplyblock_core.controllers import lvol_controller

    db = DBController()
    db.get_cluster_by_id(cluster_id)
    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        logger.info("Landing volume %s on cluster %s is already gone", lvol_id, cluster_id)
        return
    if db.get_pool_by_id(lvol.pool_uuid).cluster_id != cluster_id:
        raise KeyError(f"LVol {lvol_id} is not on cluster {cluster_id}")
    if not lvol.lvol_name.startswith(LANDING_PREFIX):
        raise NotALandingVolumeError(
            f"LVol {lvol_id} ({lvol.lvol_name}) is not a replication landing volume")
    lvol_controller.delete_lvol(lvol, force_delete=True)
