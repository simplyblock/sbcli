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
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer.protocol import (
    CAPABILITIES,
    LANDING_PREFIX,
    PEER_API_VERSION,
    ChainCheck,
    ChainState,
    DescribeRequest,
    DescribeResponse,
    PeerInfo,
    SnapshotState,
)
from simplyblock_core.rpc_client import RPCException

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


def describe(cluster_id: str, request: DescribeRequest) -> DescribeResponse:
    """What the source asks about the target mid-transfer and for retention.

    Read only. Every part of *request* is optional; the response carries an
    answer for each part asked for.

    Raises:
        KeyError: this control plane does not manage *cluster_id*.
    """
    db = DBController()
    db.get_cluster_by_id(cluster_id)
    response = DescribeResponse()
    if request.landing_lvol_id:
        response.landing_leaders = _landing_leaders(db, cluster_id, request.landing_lvol_id)
    for snapshot_id in request.snapshot_ids:
        exists = _copy_on(db, cluster_id, snapshot_id) is not None
        response.snapshots.append(SnapshotState(
            snapshot_id=snapshot_id, exists=exists,
            has_dependent_clone=exists and _has_dependent_clone(db, snapshot_id)))
    for check in request.chain_checks:
        response.chains.append(ChainState(
            successor_id=check.successor_id, predecessor_id=check.predecessor_id,
            chained=_chained(db, cluster_id, check)))
    return response


def _copy_on(db, cluster_id, snapshot_id):
    """The replicated copy *snapshot_id* when it is on *cluster_id*, else None.

    A record without a cluster id counts as this cluster's: such a copy would
    otherwise read as gone, and retention would drop the source half and leave
    the copy behind for good.
    """
    try:
        snapshot = db.get_snapshot_by_id(snapshot_id)
    except KeyError:
        return None
    return snapshot if snapshot.cluster_id in (cluster_id, "") else None


def _landing_leaders(db, cluster_id, lvol_id):
    """Ids of the online members of the landing volume's lvstore reporting
    leadership (SPDK's own "lvs leadership" flag); None when the volume is gone
    or on another cluster."""
    # Imported here for the same reason as in discard_landing.
    from simplyblock_core.controllers import lvol_controller

    try:
        lvol = db.get_lvol_by_id(lvol_id)
        if db.get_pool_by_id(lvol.pool_uuid).cluster_id != cluster_id:
            return None
    except KeyError:
        return None
    leaders = []
    for node_id in dict.fromkeys([lvol.node_id, *(lvol.nodes or [])]):
        if not node_id:
            continue
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        # is_node_leader never raises: an unreachable member is reported as
        # not leading, which is what the caller acts on.
        if node.status == StorageNode.STATUS_ONLINE and lvol_controller.is_node_leader(
                node, lvol.lvs_name):
            leaders.append(node_id)
    return leaders


def _has_dependent_clone(db, snapshot_id):
    """True when a live volume is cloned from *snapshot_id*.

    A failed-over volume is a clone of the last replicated target snapshot, so
    that snapshot must outlive it. Uses the mini index (the source the snapshot
    delete path consults) and ignores volumes that are themselves going away.
    """
    return any(lvol.cloned_from_snap == snapshot_id and lvol.status != LVol.STATUS_IN_DELETION
               for lvol in db.get_mini_lvols())


def _chained(db, cluster_id, check: ChainCheck) -> bool:
    """Whether the copy *successor_id* is chained onto *predecessor_id*.

    Retention deletes a predecessor expecting SPDK to swap-merge its segments
    into the successor chained to it; without the link the delete drops them.
    The database link (``prev_snap_uuid``) is authoritative when present: it is
    written only after add_clone and convert succeeded. It is written best
    effort, so when it is absent SPDK on the successor's node is asked, which is
    the real authority.
    """
    successor = _copy_on(db, cluster_id, check.successor_id)
    if successor is None:
        return False
    if successor.prev_snap_uuid == check.predecessor_id:
        return True
    predecessor = _copy_on(db, cluster_id, check.predecessor_id)
    if predecessor is None:
        return False
    try:
        node = db.get_storage_node_by_id(successor.lvol.node_id)
        if node.status != StorageNode.STATUS_ONLINE:
            return False
        bdev = node.rpc_client().bdev_get(successor.snap_bdev)
    except (KeyError, RPCException) as e:
        logger.warning("Could not verify the chain of %s onto %s: %s",
                       check.successor_id, check.predecessor_id, e)
        return False
    driver = ((bdev or {}).get("driver_specific") or {}).get("lvol") or {}
    if driver.get("clone") and driver.get("base_snapshot") in (
            predecessor.snap_bdev, predecessor.snap_uuid):
        logger.info("Snapshot %s is chained onto %s in SPDK but the DB link is missing",
                    check.successor_id, check.predecessor_id)
        return True
    return False
