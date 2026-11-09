"""The target-side half of the peer protocol, run against the local database.

Each function answers for a cluster managed by THIS control plane. The v2
``/_peer`` router calls them on behalf of a remote control plane, and
``LocalPeerClient`` calls them directly when source and target share one
control plane, so both paths run the same code.
"""
import contextlib
import logging
from uuid import UUID

from simplyblock_core import constants
from simplyblock_core.db_controller import DBController
from simplyblock_core.exceptions import PreconditionError
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.pool import Pool
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.peer.protocol import (
    CAPABILITIES,
    LANDING_PREFIX,
    PEER_API_VERSION,
    ChainCheck,
    ChainState,
    DescribeRequest,
    DescribeResponse,
    LandingRequest,
    LandingResponse,
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


class LandingVolumeError(PreconditionError):
    """The target cannot make a landing volume: no member or pool can take it,
    or creating it failed."""


def prepare_landing(cluster_id: str, request: LandingRequest) -> LandingResponse:
    """Make the landing volume of a replicated snapshot ready on *cluster_id*.

    In order: find the member to land on, look for a copy of the same data
    already on that member's lvstore, adopt or clear a volume a previous attempt
    left under the landing name, then create one in the first hinted pool of
    this cluster. Idempotent: a repeated request adopts the volume the first
    one made.

    Raises:
        KeyError: this control plane does not manage *cluster_id*, or the node
            hint is unknown or on another cluster.
        LandingVolumeError: no member or pool can take the volume, or the
            create failed.
    """
    # Imported here for the same reason as in discard_landing.
    from simplyblock_core.controllers import lvol_controller

    db = DBController()
    db.get_cluster_by_id(cluster_id)
    pools = db.get_pools(cluster_id)
    existing = _named_landing(db, pools, request.landing_name)
    if request.create_only and existing is not None:
        return LandingResponse(state="exists", lvol_id=existing.get_id())

    node = _landing_node(db, cluster_id, request, lvol_controller)
    if not request.create_only:
        # After a relocate the destination may already hold this data: the
        # chain of the volume now on the source side came FROM here. Link to
        # it instead of shipping it back in full (its name would also collide,
        # 2026-09-29).
        counterpart = _counterpart(db, cluster_id, node, request)
        if counterpart is not None:
            return LandingResponse(state="counterpart", counterpart_snapshot_id=counterpart.get_id(),
                                   lvs_name=counterpart.lvol.lvs_name)

    if existing is not None:
        # A record wearing the derived name IS this transfer's landing volume,
        # left by an attempt that died before the source stored its id (case 6,
        # run 20260824_144226), or pre-staged for a cutover round.
        if existing.status == LVol.STATUS_ONLINE and existing.size == request.size:
            return LandingResponse(state="ready", lvol_id=existing.get_id(), adopted=True,
                                   lvs_name=existing.lvs_name)
        if existing.status == LVol.STATUS_IN_DELETION:
            return LandingResponse(state="retry",
                                   reason=f"stale landing volume {request.landing_name} still deleting")
        why = ("its size differs from the snapshot" if existing.status == LVol.STATUS_ONLINE
               else f"it is half-created ({existing.status})")
        logger.warning("Clearing landing volume %s (%s): %s", existing.get_id(),
                       request.landing_name, why)
        try:
            lvol_controller.delete_lvol(existing, force_delete=True)
        except PreconditionError as e:
            return LandingResponse(state="retry", reason=f"could not clear landing volume "
                                                         f"{request.landing_name}: {e}")
        return LandingResponse(state="retry", reason=f"cleared stale landing volume "
                                                     f"{request.landing_name} ({why})")

    pool_uuid = _pick_pool(db, cluster_id, pools, request)
    max_ns = request.max_namespace_per_subsys
    landing_class = constants.QOS_REPLICATION_CLASS_ID if _replication_class_on(cluster_id) else 0
    # internal=True: the landing copy of a transfer, never handed to a client.
    # The per-node subsystem cap is a user-admission limit; enforcing it here
    # would stop replication on a full node, exactly when the transfers that
    # let retention free slots are needed. The source volume's subsystem
    # packing is carried over, so clones taken from the copy can share one.
    lvol_id, err = lvol_controller.add_lvol_ha(
        request.landing_name, request.size, node.get_id(), request.ha_type, pool_uuid,
        internal=True, namespaced=max_ns > 1, max_namespace_per_subsys=max_ns,
        lvol_priority_class=landing_class)
    if not lvol_id:
        raise LandingVolumeError(f"Error creating remote lvol: {err}")
    return LandingResponse(state="ready", lvol_id=lvol_id, lvs_name=node.lvstore)


def _named_landing(db, pools, name):
    """The volume named *name* in one of *pools*, deleted ones included, or None."""
    for pool in pools:
        with contextlib.suppress(KeyError):
            return db.get_lvol_by_name(name, pool.get_id(), include_deleted=True)
    return None


def _landing_node(db, cluster_id, request, lvol_controller):
    """The member to create the landing volume on: the hinted one, or the
    least loaded online primary when the source did not (could not) choose.

    Raises:
        KeyError: the hinted node is unknown or on another cluster.
        LandingVolumeError: no member can take a landing volume.
    """
    if request.node_hint:
        node = db.get_storage_node_by_id(request.node_hint)
        if node.cluster_id != cluster_id:
            raise KeyError(f"Storage node {request.node_hint} is not on cluster {cluster_id}")
        return node
    node = lvol_controller._pick_replication_node(db, cluster_id, request.size)
    if node is None:
        raise LandingVolumeError(f"No storage node of cluster {cluster_id} can take a landing volume")
    return node


def _counterpart(db, cluster_id, node, request):
    """The copy of the request's data on *node*'s lvstore, or None.

    Copies share data_uuid (the finish copies it onto every replicated
    snapshot). Only a copy on the same lvstore counts: a chain is built only
    on a snapshot in the same lvstore.
    """
    lvstore = getattr(node, "lvstore", "")
    if not request.data_uuid or not lvstore:
        return None
    for cand in db.get_snapshots(cluster_id):
        if (cand.get_id() != request.snapshot_id
                and cand.data_uuid == request.data_uuid
                and cand.status != SnapShot.STATUS_IN_DELETION
                and cand.lvol and cand.lvol.lvs_name == lvstore):
            return cand
    return None


def _pick_pool(db, cluster_id, pools, request):
    """The first hinted pool of this cluster, else its first active pool.

    The source sends its hints in order of precedence. A hint naming a pool of
    another cluster is skipped, so a source's outgoing configuration never
    places an incoming copy in a third cluster's pool (lab 2026-08-19).

    Raises:
        LandingVolumeError: the cluster has no usable pool.
    """
    hints = list(request.pool_hints)
    if request.counterpart_hint:
        with contextlib.suppress(KeyError):
            hints.insert(0, db.get_snapshot_by_id(request.counterpart_hint).lvol.pool_uuid)
    own = {pool.get_id() for pool in pools}
    for hint in hints:
        if hint in own:
            return hint
    for pool in pools:
        if pool.status == Pool.STATUS_ACTIVE:
            return pool.get_id()
    raise LandingVolumeError(f"Unable to find pool on remote cluster: {cluster_id}")


def _replication_class_on(cluster_id):
    """Whether the replication QoS class exists on *cluster_id*. An unreadable
    QoS table means no: the class is an optimisation, never a reason to fail."""
    from simplyblock_core.controllers import qos_controller

    try:
        return qos_controller.replication_class_available(cluster_id)
    except KeyError as e:
        logger.warning("QoS lookup for cluster %s failed; landing in Default: %s", cluster_id, e)
        return False
