"""The target-side half of the peer protocol, run against the local database.

Each function answers for a cluster managed by THIS control plane. The v2
``/_peer`` router calls them on behalf of a remote control plane, and
``LocalPeerClient`` calls them directly when source and target share one
control plane, so both paths run the same code.
"""
import contextlib
import logging
import time
from uuid import UUID, uuid5

from simplyblock_core import constants, xfer_timing
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
    ChainRequest,
    ChainResponse,
    ChainState,
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
    try:
        lvol = db.get_lvol_by_id(lvol_id)
        if db.get_pool_by_id(lvol.pool_uuid).cluster_id != cluster_id:
            return None
    except KeyError:
        return None
    return [node.get_id() for node in lvs_leaders(db, lvol)]


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
            return _ready(existing, adopted=True)
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
    try:
        return _ready(db.get_lvol_by_id(lvol_id))
    except KeyError:
        # created but not readable back yet: the id is what matters
        return LandingResponse(state="ready", lvol_id=lvol_id, lvs_name=node.lvstore,
                               node_id=node.get_id(), pool_id=pool_uuid)


def _ready(landing, adopted=False):
    """The answer for a landing volume that can receive."""
    return LandingResponse(state="ready", lvol_id=landing.get_id(), adopted=adopted,
                           lvs_name=landing.lvs_name, top_bdev=landing.top_bdev,
                           node_id=landing.node_id, pool_id=landing.pool_uuid)


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


def lvs_leaders(db, lvol):
    """The online members of *lvol*'s lvstore that report leadership, in role
    order (primary, secondary, tertiary)."""
    from simplyblock_core.controllers import lvol_controller

    leaders = []
    for node_id in dict.fromkeys([lvol.node_id, *(getattr(lvol, "nodes", None) or [])]):
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
            leaders.append(node)
    return leaders


# (cluster id, lvstore) -> (leader id, time it was last confirmed settled).
# Process-local on purpose: an optimisation that saves the settle wait for
# volumes sent into one lvstore in quick succession; another process simply
# settles again.
_SETTLED_LEADER: dict = {}


def stable_leader(db, lvol):
    """The member that leads *lvol*'s lvstore once leadership is settled; or
    (None, why).

    Leadership is "re-established" when exactly one online member reports it,
    and the same member again REPL_LEADER_SETTLE_SEC later. A move in progress
    shows the old member, then none or two, then the new one; a transfer or
    convert sent in that window lands on a member that is about to stop being
    the leader (the hub refuses receive IO in non-leader mode, a convert on a
    non-leader silently persists nothing). The probe is SPDK's own "lvs
    leadership" -- the very flag the hub and convert check -- so a member it
    does not show as leader cannot take the transfer, whatever else is known.
    """
    first = lvs_leaders(db, lvol)
    key = (getattr(lvol, "cluster_id", ""), lvol.lvs_name)
    if len(first) != 1:
        _SETTLED_LEADER.pop(key, None)
        return None, (f"{len(first)} members of {lvol.lvs_name} report leadership"
                      + (f" ({', '.join(n.get_id()[:8] for n in first)})" if first else ""))
    # Many volumes' snapshots go into one lvstore: settling every one of them
    # would add the settle time per transfer. A leader confirmed settled
    # moments ago and seen alone again now is still settled.
    cached = _SETTLED_LEADER.get(key)
    if (cached and cached[0] == first[0].get_id()
            and time.time() - cached[1] < constants.REPL_LEADER_SETTLED_FOR_SEC):
        return first[0], ""
    time.sleep(constants.REPL_LEADER_SETTLE_SEC)
    second = lvs_leaders(db, lvol)
    if len(second) != 1 or second[0].get_id() != first[0].get_id():
        _SETTLED_LEADER.pop(key, None)
        return None, (f"leadership of {lvol.lvs_name} is moving "
                      f"({first[0].get_id()[:8]} -> "
                      f"{', '.join(n.get_id()[:8] for n in second) or 'none'})")
    _SETTLED_LEADER[key] = (second[0].get_id(), time.time())
    return second[0], ""


def converted_members(db, lvol):
    """Ids of the online members on which the landing volume *lvol* is a snapshot.

    Reads SPDK's own view (bdev driver_specific.lvol.snapshot) on every online
    member: the control plane's record cannot say whether an earlier attempt
    converted the volume on some members only. A member that cannot be asked
    is skipped; the transfer's own leader probe decides about it.
    """
    members = []
    for node_id in (getattr(lvol, "nodes", None) or [lvol.node_id]):
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            ret = node.rpc_client().bdev_get(lvol.top_bdev)
            if ret and ret.get("driver_specific", {}).get("lvol", {}).get("snapshot"):
                members.append(node.get_id())
        except Exception as e:                            # noqa: BLE001
            logger.warning("Could not read landing volume %s on %s: %s",
                           lvol.top_bdev, node.get_id(), e)
    return members


def require_leader(node, lvs_name, what):
    """True when *node* currently holds LVS leadership for *lvs_name*.

    Transfers into a hub on a non-leader fail loudly, but bdev_lvol_convert on a
    non-leader DEGRADES SILENTLY: the fork's non-leader branch marks the blob
    CLEAN and replies success without persisting anything -- the "snapshot"
    looks converted while its metadata never reached the journal. Leadership
    must therefore be verified BEFORE the operation; on False the caller
    suspends and retries rather than proceeding.
    """
    from simplyblock_core.controllers import lvol_controller

    if lvol_controller.is_node_leader(node, lvs_name):
        return True
    logger.error("Node %s is not LVS leader of %s -- refusing %s (retry)",
                 node.get_id(), lvs_name, what)
    return False


def ensure_hub(cluster_id: str, request: EnsureHubRequest) -> EnsureHubResponse:
    """Get this cluster ready to receive a transfer into the landing volume.

    Sends only to the member that leads the landing volume's lvstore once that
    leadership is settled, never to a tertiary (SPDK demuxes hub IO only on a
    primary or secondary: the node_role check in vbdev_lvol_submit_request).
    Replication only ever connects to an EXISTING, settled leader; it never
    claims, grants or forces leadership (bf8 2026-11-02: a recovery run from
    the source, during a forced shutdown of the target primary, left LVS_10
    with two leaders and corrupted metadata). Then the receiving hub is made
    ready on the leader, the volume's map id read (every write's LBA carries
    it, and the hub routes by it), and the volume checked not to be a snapshot
    already (2026-09-29: writes into a converted landing volume made the
    target LVS drop leadership and fence its ports, 14 times in 11 minutes).

    Raises:
        KeyError: this control plane does not manage *cluster_id*, or the
            landing volume is unknown or on another cluster.
    """
    from simplyblock_core.services.replication_final_step import prepare_receiving_hub

    db = DBController()
    db.get_cluster_by_id(cluster_id)
    lvol = db.get_lvol_by_id(request.landing_lvol_id)
    if db.get_pool_by_id(lvol.pool_uuid).cluster_id != cluster_id:
        raise KeyError(f"LVol {request.landing_lvol_id} is not on cluster {cluster_id}")

    leader, why = stable_leader(db, lvol)
    if leader is None:
        return EnsureHubResponse(state="unsettled",
                                 reason=f"target leadership not established ({why})")
    try:
        lvs_primary = db.get_storage_node_by_id(lvol.node_id)
    except KeyError:
        return EnsureHubResponse(state="failed", leader_node_id=leader.get_id(),
                                 reason=f"target lvstore primary {lvol.node_id} unknown")
    if leader.get_id() not in (lvs_primary.get_id(), lvs_primary.secondary_node_id):
        return EnsureHubResponse(
            state="unsettled",
            reason=(f"target lvstore {lvol.lvs_name} is led by its tertiary {leader.get_id()}, "
                    f"which cannot receive a transfer; waiting for the primary or secondary"))

    hub, err = prepare_receiving_hub(leader, lvs_primary)
    if err:
        return EnsureHubResponse(state="failed", leader_node_id=leader.get_id(),
                                 reason=f"transfer hub attach failed: {err}")
    ret = leader.rpc_client().bdev_get(lvol.top_bdev)
    try:
        map_id = ret["driver_specific"]["lvol"]["map_id"]
    except (TypeError, KeyError):
        map_id = None
    if not map_id:
        logger.error("map_id of receiving lvol %s not found on %s; not starting a transfer "
                     "that cannot land", lvol.top_bdev, lvol.node_id)
        return EnsureHubResponse(state="failed", leader_node_id=leader.get_id(), backoff=False,
                                 reason="receiving lvol map_id unavailable")
    converted_on = converted_members(db, lvol)
    if converted_on:
        return EnsureHubResponse(state="converted", leader_node_id=leader.get_id(),
                                 converted_on=converted_on)
    # The hub rejects receive IO on a non-leader ("receive io for hublvol in
    # nonleader mode"); do not start a transfer that cannot land.
    if not require_leader(leader, lvol.lvs_name, "transfer receive"):
        return EnsureHubResponse(state="not_leader", leader_node_id=leader.get_id(),
                                 reason=f"target node {leader.get_id()} not LVS leader of "
                                        f"{lvol.lvs_name}")
    return EnsureHubResponse(state="ready", leader_node_id=leader.get_id(), map_id=map_id, hub=hub,
                             pair_online=_pair_online(db, leader))


def _pair_online(db, leader):
    """Whether *leader* has no secondary (a single-node cluster: no member to
    leave holes on) or its secondary is online."""
    if not leader.secondary_node_id:
        return True
    try:
        return db.get_storage_node_by_id(leader.secondary_node_id).status == StorageNode.STATUS_ONLINE
    except KeyError:
        return False


def chain(cluster_id: str, request: ChainRequest) -> ChainResponse:
    """Chain the landing volume onto the predecessor's copy (see chain_landing).

    Raises:
        KeyError: this control plane does not manage *cluster_id*, or the
            landing volume or the leader is unknown.
    """
    db = DBController()
    db.get_cluster_by_id(cluster_id)
    return chain_landing(db, request)


def chain_landing(db, request: ChainRequest) -> ChainResponse:
    """Chain the landing volume onto the copy of the snapshot before it, on the
    leader and its secondary, BEFORE the transfer, so it can ship only the delta.

    ``bdev_lvol_transfer`` sends a blob's own cluster map and nothing else, so a
    partial transfer ships only the ranges written since the previous snapshot;
    everything outside that delta has to be on the destination already. A
    landing volume chained onto the predecessor's copy supplies it: a cluster
    the new snapshot does not own reads through to the parent, and the first
    write into one the delta touches copies the whole cluster from that same
    parent before applying the incoming range.

    The bar for "chained" is high: the copy must be in the leader's lvstore,
    and the secondary must be online and accept the chain too. The transfer
    lands on the leader and the lvstore mirrors it to the secondary; an
    unchained secondary would read zeros wherever the delta did not write. A
    member already in ``request.chained_on`` is not chained again: the same
    clone entry twice is not idempotent in SPDK. Anything short of the bar
    declines, and a full transfer -- always correct -- is sent.

    Raises:
        KeyError: the landing volume or the leader is unknown.
    """
    lvol = db.get_lvol_by_id(request.landing_lvol_id)
    chained = list(request.chained_on)
    try:
        copy = db.get_snapshot_by_id(request.predecessor_id)
    except KeyError as e:
        return ChainResponse(state="unresolvable", chained_on=chained,
                             reason=f"the predecessor's copy {request.predecessor_id} "
                                    f"cannot be resolved ({e})")
    # Any member of the copy's lvstore can chain onto it: the bdev is the
    # lvstore's, the same on primary, secondary and tertiary (bf8 2026-11-02).
    members = list(getattr(copy.lvol, "nodes", None) or [copy.lvol.node_id])
    if request.leader_node_id not in members:
        return ChainResponse(state="unresolvable", chained_on=chained,
                             reason=f"the predecessor's copy {request.predecessor_id} lives on "
                                    f"{copy.lvol.node_id}, not in the lvstore of "
                                    f"{request.leader_node_id}; cannot chain across lvstores")
    leader = db.get_storage_node_by_id(request.leader_node_id)
    secondary = None
    if leader.secondary_node_id:
        with contextlib.suppress(KeyError):
            secondary = db.get_storage_node_by_id(leader.secondary_node_id)
        if secondary is None or secondary.status != StorageNode.STATUS_ONLINE:
            return ChainResponse(state="declined", chained_on=chained,
                                 reason=f"the secondary of {leader.get_id()} is not online, "
                                        f"and a delta would leave holes on it")

    def chain_on(node, role):
        if node.get_id() in chained:
            return True
        # An answer lost after the add_clone landed leaves the member out of
        # the source's record; SPDK knows, and is asked before chaining again.
        if _reads_through(node, lvol.top_bdev, copy.snap_bdev):
            chained.append(node.get_id())
            return True
        logger.info("Pre-chaining landing volume %s onto %s on %s (%s) so the transfer can "
                    "ship only the delta", lvol.top_bdev, copy.snap_bdev, node.get_id(), role)
        try:
            ok = node.rpc_client().bdev_lvol_add_clone(lvol.top_bdev, copy.snap_bdev)
        except RPCException as e:
            logger.warning("Pre-chain of %s on %s (%s) raised %s", lvol.top_bdev,
                           node.get_id(), role, e)
            return False
        if not ok:
            logger.warning("Pre-chain of %s onto %s failed on %s (%s)", lvol.top_bdev,
                           copy.snap_bdev, node.get_id(), role)
            return False
        chained.append(node.get_id())
        return True

    # Whatever lands is reported even when the pair is not complete: the entry
    # exists on that member now, and must not be added a second time.
    if not chain_on(leader, "primary"):
        return ChainResponse(state="declined", chained_on=chained,
                             reason=f"the chain on {leader.get_id()} failed")
    if secondary is not None and not chain_on(secondary, "secondary"):
        return ChainResponse(state="declined", chained_on=chained,
                             reason=f"the chain on the secondary {secondary.get_id()} failed")
    return ChainResponse(state="chained", chained_on=chained)


def _reads_through(node, lvol_bdev, snap_bdev):
    """Whether *lvol_bdev* is already a clone of *snap_bdev* on *node*."""
    try:
        ret = node.rpc_client().bdev_get(lvol_bdev)
    except RPCException:
        return False
    if isinstance(ret, list):
        ret = ret[0] if ret else {}
    base = (((ret or {}).get("driver_specific") or {}).get("lvol") or {}).get("base_snapshot")
    return bool(base) and base == snap_bdev.split("/")[-1]


#: -ESTALE as SPDK reports it: a Linux errno, whatever this host's errno says
_SPDK_ESTALE = -116

#: Namespace of the deterministic ids of replicated copies (finish_landing).
_COPY_NAMESPACE = UUID("7d2c1a52-3f0b-4f6e-9d6a-2b1f3c0e8a11")


def copy_id_for(source_cluster_id, snapshot_id, landing_lvol_id):
    """The id of the copy one landing volume becomes: the same on every retry
    of its finish, so a repeated finish finds the record instead of writing a
    second one; a new landing volume (a new transfer) makes a new copy."""
    return str(uuid5(_COPY_NAMESPACE, f"{source_cluster_id}/{snapshot_id}/{landing_lvol_id}"))


def finish(cluster_id: str, request: FinishRequest) -> FinishResponse:
    """Make the landing volume the replicated copy (see finish_landing).

    Raises:
        KeyError: this control plane does not manage *cluster_id*.
    """
    db = DBController()
    db.get_cluster_by_id(cluster_id)
    return finish_landing(db, request)


def finish_landing(db, request: FinishRequest) -> FinishResponse:
    """After the transfer: chain, merge and convert the landing volume into the
    copy of the source snapshot, record it, and retire the landing volume.

    Re-entrant: what the source recorded from earlier answers (``chained_on``,
    ``converted_on``, ``merged``) is skipped, a merge still running answers
    ``pending``, and the copy's record id is derived from the request, so a
    finish repeated after a lost answer or a crash answers ``done`` with the
    record it wrote.
    """
    copy_id = copy_id_for(request.source_cluster_id, request.snapshot.snapshot_id,
                          request.landing_lvol_id)
    with contextlib.suppress(KeyError):
        written = db.get_snapshot_by_id(copy_id)
        return FinishResponse(state="done", copy_id=copy_id, record=written.to_dict(),
                              chained_on=request.chained_on, converted_on=request.converted_on,
                              merged=request.merged)
    try:
        landing = db.get_lvol_by_id(request.landing_lvol_id)
    except KeyError:
        return FinishResponse(state="gone",
                              reason=f"landing volume {request.landing_lvol_id} is gone")
    if landing.status == LVol.STATUS_IN_DELETION:
        return FinishResponse(state="gone",
                              reason=f"landing volume {request.landing_lvol_id} is being deleted")
    answer = FinishResponse(state="retry", chained_on=list(request.chained_on),
                            converted_on=list(request.converted_on), merged=request.merged)

    # add_clone and convert must run on the leader: a convert on a non-leader
    # reports success and persists nothing. The finish follows the settled
    # leader, not the node the transfer was sent to: the data the old leader
    # acknowledged is in the HA landing volume on every member.
    leader, why = stable_leader(db, landing)
    if leader is None:
        # Pending, not failed: the target is down, or its leadership moving,
        # which is not the transfer's fault. Counted as a failure it took the
        # doubling retry backoff (bf8 2026-11-02: the finish resumed 7 min after
        # the target was back).
        answer.state, answer.waiting_for_leader = "pending", True
        answer.reason = f"waiting for the target lvstore to have a leader ({why})"
        return answer
    answer.leader_node_id = leader.get_id()
    if request.sent_to and request.sent_to != leader.get_id():
        logger.warning("Target leadership of %s moved %s -> %s after the transfer; "
                       "finishing on the new leader", landing.lvs_name, request.sent_to,
                       leader.get_id())

    # The predecessor's copy the new copy is chained onto. Without this link
    # every copy is a standalone blob: a fail-over clone reads only the last
    # delta and zeros elsewhere, and retention cannot swap-merge segments into
    # a successor (all-zeros DR fail-over, labs 2026-08-10..14). One that exists
    # but cannot be used fails the finish (it retries) rather than building an
    # unchained copy.
    predecessor = None
    if request.predecessor_id:
        try:
            predecessor = db.get_snapshot_by_id(request.predecessor_id)
        except KeyError:
            answer.reason = (f"the predecessor's copy {request.predecessor_id} cannot be "
                             f"resolved; refusing to finish an unchained copy")
            logger.error(answer.reason)
            return answer
        members = list(getattr(predecessor.lvol, "nodes", None) or [predecessor.lvol.node_id])
        if leader.get_id() not in members:
            answer.reason = (f"the predecessor's copy {request.predecessor_id} lives on "
                             f"{predecessor.lvol.node_id}, not in the lvstore of {leader.get_id()}; "
                             f"cannot chain across lvstores")
            logger.error(answer.reason)
            return answer

    # Leadership gate BEFORE chain/convert on the leader.
    if not require_leader(leader, landing.lvs_name, "add_clone/convert"):
        answer.reason = f"{leader.get_id()} is not the LVS leader of {landing.lvs_name}"
        return answer

    if not _finish_on(answer, landing, leader, predecessor, request, "primary", required=True):
        return answer
    # A delta landed only its own blocks in pre-allocated clusters: fill the
    # rest from the base before the convert makes the volume a snapshot.
    if leader.get_id() not in answer.converted_on and not answer.merged and request.partial_base:
        merge = _merge_landing(landing, leader)
        if merge in ("pending", "retry", "restart"):
            answer.state = merge
            answer.reason = {"pending": "landing volume merging with its base",
                             "retry": "the merge with the base failed",
                             "restart": f"landing volume {landing.top_bdev} lost the record of "
                                        f"its delta; it has to be transferred again"}[merge]
            return answer
        answer.merged = True
    if not _convert_on(answer, landing, leader, "primary", required=True):
        return answer

    # The secondary; a single-node cluster's node has none.
    secondary = (db.get_storage_node_by_id(leader.secondary_node_id)
                 if leader.secondary_node_id else None)
    if secondary is not None and secondary.status == StorageNode.STATUS_ONLINE:
        if not _finish_on(answer, landing, secondary, predecessor, request, "secondary",
                          required=False):
            return answer
        if not _convert_on(answer, landing, secondary, "secondary", required=False):
            return answer

    # The landing volume is a snapshot now and will back fail-over and
    # test-failover clones: drop the replication class from it.
    if landing.lvol_priority_class == constants.QOS_REPLICATION_CLASS_ID:
        _reset_service_class(db, landing.nodes or [leader.get_id()], landing.top_bdev)

    copy = _record_copy(db, request, landing, leader, predecessor, copy_id)
    if copy is None:
        answer.reason = f"failed to record the chain back-link on {request.predecessor_id}"
        return answer
    _retire_landing(db, landing)
    answer.state, answer.copy_id, answer.record = "done", copy_id, copy.to_dict()
    return answer


def _finish_on(answer, landing, node, predecessor, request, role, required):
    """Chain the landing volume onto the predecessor's copy on *node*, unless
    that already happened (recorded, or SPDK shows it). False fails the finish."""
    if predecessor is None or node.get_id() in answer.converted_on:
        return True
    if node.get_id() in answer.chained_on:
        return True
    if _reads_through(node, landing.top_bdev, predecessor.snap_bdev):
        # chained by an earlier pass that then waited for the merge, or on the
        # secondary through the lvstore's metadata sync (bed run 5, 2026-10-24:
        # add_clone then failed "File exists" for 16 min): not idempotent.
        answer.chained_on.append(node.get_id())
        return True
    logger.info("Chaining replicated lvol %s to snap %s on %s (%s)", landing.top_bdev,
                predecessor.snap_bdev, node.get_id(), role)
    with xfer_timing.phase("chain_add_clone", snap=request.snapshot.snapshot_id,
                           lvol=landing.get_id(), node=role):
        ok = _rpc_or_none(node.rpc_client().bdev_lvol_add_clone, landing.top_bdev,
                          predecessor.snap_bdev)
    if ok:
        answer.chained_on.append(node.get_id())
        return True
    if not required and (_lacks_bdev(node, predecessor.snap_bdev)
                         or _lacks_bdev(node, landing.top_bdev)):
        # The secondary does not hold the base (-19): its view of the chain is
        # behind, and failing here only re-runs the finish until the task gives
        # up, then re-transfers into a converted landing (2026-10-02, LVS_1 site
        # A). The primary's convert made the copy; the lvstore sync repairs the
        # secondary.
        logger.warning("Secondary %s does not hold %s or %s; the chain is not repeated there",
                       node.get_id(), predecessor.snap_bdev, landing.top_bdev)
        return True
    answer.reason = f"failed to chain the replicated snapshot on the {role} {node.get_id()}"
    logger.error(answer.reason)
    return False


def _convert_on(answer, landing, node, role, required):
    """Convert the landing volume into a snapshot on *node*, unless an earlier
    pass did. False fails the finish."""
    if node.get_id() in answer.converted_on:
        logger.info("Landing volume %s is already converted on %s", landing.top_bdev, node.get_id())
        return True
    with xfer_timing.phase("chain_convert", lvol=landing.get_id(), node=role):
        ok = _rpc_or_none(node.rpc_client().bdev_lvol_convert, landing.top_bdev)
    if not ok:
        if not required and _lacks_bdev(node, landing.top_bdev):
            # The landing volume never reached the secondary; the primary's
            # convert is the copy, and the lvstore sync repairs the secondary
            # (2026-10-02, LVS_2 on B).
            logger.warning("Secondary %s does not hold %s; nothing to convert there",
                           node.get_id(), landing.top_bdev)
        else:
            answer.reason = f"failed to convert to snapshot on the {role} {node.get_id()}"
            logger.error(answer.reason)
            return False
    answer.converted_on.append(node.get_id())
    return True


def _merge_landing(landing, leader):
    """Fill the landing volume from its base after a delta transfer.

    The destination pre-allocates every cluster the delta touches and receives
    only the delta's blocks; the rest of each such cluster comes from the base
    the landing volume is chained onto, once, before the convert. The merge is
    a job on the target (minutes for a large delta): started idempotently,
    waited for briefly, then on later passes. Returns "ok", "pending", "retry"
    (a failed job is started again) or "restart" (the target lost the record
    of what arrived: transfer again into a fresh landing volume).
    """
    rpc = leader.rpc_client()
    try:
        # Look before starting: a start relaunches a failed job and answers
        # "running" before the relaunch fails again, so a landing volume that
        # lost its delta record (-ESTALE) stayed "merging" for ever (bed
        # 2026-10-26).
        st = rpc.bdev_lvol_merge_base_stat(landing.top_bdev) or {}
        if st.get("state") not in ("running", "done") and st.get("rc") != _SPDK_ESTALE:
            # The merge runs in the replication class, not in Default beside
            # client IO and the journal (bf10 2026-11-02: journal heartbeats
            # timed out and the lvstore was demoted).
            merge_class = (constants.QOS_REPLICATION_WIRE_CLASS
                           if _replication_class_on(leader.cluster_id) else None)
            st = rpc.bdev_lvol_merge_base(landing.top_bdev, priority_class=merge_class)
        deadline = time.time() + constants.REPL_MERGE_INLINE_WAIT_SEC
        while (st or {}).get("state") == "running" and time.time() < deadline:
            time.sleep(constants.REPL_MERGE_POLL_SEC)
            st = rpc.bdev_lvol_merge_base_stat(landing.top_bdev)
    except RPCException as e:
        logger.error("Merge of %s with its base on %s: %s", landing.top_bdev, leader.get_id(), e)
        return "retry"
    st = st or {}
    if st.get("state") == "running":
        return "pending"
    if st.get("state") == "done":
        logger.info("Landing volume %s merged with its base on %s: %s", landing.top_bdev,
                    leader.get_id(), st)
        xfer_timing.stamp("landing_merge", lvol=landing.get_id(), ms=st.get("ms", 0),
                          bytes=st.get("bytes", 0), clusters=st.get("clusters", 0))
        return "ok"
    if st.get("rc") == _SPDK_ESTALE:
        logger.error("%s on %s no longer knows which blocks the transfer wrote; the "
                     "transfer is repeated", landing.top_bdev, leader.get_id())
        return "restart"
    logger.error("Merge of %s with its base failed on %s: %s", landing.top_bdev,
                 leader.get_id(), st)
    return "retry"


def _record_copy(db, request, landing, leader, predecessor, copy_id):
    """Write the copy's record, linked back to the source snapshot and onto its
    predecessor's copy; the copy, or None when the back-link cannot be written."""
    source = request.snapshot
    copy = SnapShot()
    copy.uuid = copy_id
    copy.data_uuid = source.data_uuid
    copy.cluster_id = leader.cluster_id
    copy.lvol = landing
    copy.pool_uuid = landing.pool_uuid
    copy.snap_bdev = landing.top_bdev
    copy.snap_uuid = landing.lvol_uuid
    copy.size = source.size
    copy.used_size = source.used_size
    copy.snap_name = source.snap_name
    # Names are unique per cluster, and the destination can hold one of this
    # name already (the original of a chain coming back after a relocate):
    # the record must not fail AFTER the convert.
    if any(s.snap_name == copy.snap_name for s in db.get_snapshots(leader.cluster_id)):
        copy.snap_name = f"{source.snap_name}-{copy_id[:8]}"
        logger.warning("Snapshot name %s is taken on cluster %s; recording the copy as %s",
                       source.snap_name, leader.cluster_id, copy.snap_name)
    copy.blobid = landing.blobid
    copy.created_at = int(time.time())
    copy.status = SnapShot.STATUS_ONLINE
    # Consistency-group provenance travels with the copy: the fail-over
    # generation selector returns this record.
    copy.group_id = source.group_id
    copy.group_seq = source.group_seq
    if request.link == "source":
        copy.source_replicated_snap_uuid = source.snapshot_id
    elif request.link == "target":
        copy.target_replicated_snap_uuid = source.snapshot_id
    if request.link != "none" and predecessor is not None:
        # The link that lets retention delete the predecessor safely: written
        # before the copy is published, and a failure fails the finish rather
        # than publishing a copy that looks unchained.
        copy.prev_snap_uuid = predecessor.get_id()
        predecessor.next_snap_uuid = copy_id
        try:
            predecessor.write_to_db()
        except Exception as e:                            # noqa: BLE001
            logger.error("Failed to record the chain back-link on %s: %s", predecessor.get_id(), e)
            return None
    copy.write_to_db()
    return copy


def _retire_landing(db, landing):
    """Tear down the landing volume's plumbing; its blob lives on as the copy.

    The record goes regardless of the teardown: delete_lvol can raise (SPDK
    refuses to delete a bdev that is now a cloned snapshot), and a record left
    in_deletion never converges (run 20260821_205111). The subsystem and
    namespace are torn down directly, not through delete_lvol, whose async
    path stranded records an interruption left behind (runs 20260824,
    20260825_125156).
    """
    from simplyblock_core.controllers import lvol_controller

    landing.bdev_stack = []
    landing.write_to_db()
    for node_id in landing.nodes:
        try:
            node = db.get_storage_node_by_id(node_id)
            if node.status == StorageNode.STATUS_ONLINE:
                lvol_controller.delete_lvol_from_node(landing.get_id(), node_id, force=True)
        except Exception as e:                            # noqa: BLE001
            logger.error("Landing volume %s teardown on %s raised: %s; retiring the record "
                         "anyway (its bdev lives on as the converted snapshot)",
                         landing.get_id(), node_id[:8], e)
    landing.remove(db.kv_store)


def _reset_service_class(db, node_ids, bdev):
    """Best effort: give a converted landing volume the class every snapshot
    has (0); SPDK does not persist it."""
    for node_id in node_ids:
        try:
            node = db.get_storage_node_by_id(node_id)
        except KeyError:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            node.rpc_client(timeout=10).bdev_lvol_set_priority_class(bdev, 0)
        except RPCException as e:
            logger.warning("Resetting the service class of %s on %s failed (non-fatal): %s",
                           bdev, node_id, e)


def _rpc_or_none(fn, *args):
    """Call an RPC whose failure the caller handles; a raise is logged and
    reported as None so the caller's failure branch runs."""
    try:
        return fn(*args)
    except RPCException as e:
        logger.error("%s%s failed: %s", getattr(fn, "__name__", fn), args, e)
        return None


def _lacks_bdev(node, bdev_name):
    """True when *node* has no bdev *bdev_name* (an add_clone's -19)."""
    try:
        return node.rpc_client(timeout=10).bdev_get(bdev_name) is None
    except RPCException as e:
        logger.warning("Could not probe %s on %s: %s", bdev_name, node.get_id(), e)
        return False


class NotAReplicatedCopyError(PreconditionError):
    """The snapshot a peer asked to delete is not a replicated copy."""


def delete_snapshot(cluster_id: str, snapshot_id: str) -> DeleteSnapshotResponse:
    """Delete the replicated copy *snapshot_id* on *cluster_id* (retention).

    Idempotent: a copy that is already gone counts as deleted. A copy a live
    volume is cloned from (a failed-over volume) stays: its delete reaches SPDK
    as bdev_lvol_delete(sync=False) and frees the blocks there and then, so no
    later guard could save the clone.

    Raises:
        KeyError: this control plane does not manage *cluster_id*, or the
            snapshot is on another cluster.
        NotAReplicatedCopyError: the snapshot is not a replicated copy. The
            peer API never deletes a snapshot replication did not make.
    """
    from simplyblock_core.controllers import snapshot_controller

    db = DBController()
    db.get_cluster_by_id(cluster_id)
    try:
        snapshot = db.get_snapshot_by_id(snapshot_id)
    except KeyError:
        return DeleteSnapshotResponse(deleted=True, reason=f"snapshot {snapshot_id} is already gone")
    if snapshot.cluster_id not in (cluster_id, ""):
        raise KeyError(f"Snapshot {snapshot_id} is not on cluster {cluster_id}")
    if not (snapshot.source_replicated_snap_uuid or snapshot.target_replicated_snap_uuid):
        raise NotAReplicatedCopyError(f"Snapshot {snapshot_id} is not a replicated copy")
    if _has_dependent_clone(db, snapshot_id):
        return DeleteSnapshotResponse(deleted=False,
                                      reason=f"a live volume is cloned from {snapshot_id}")
    if not snapshot_controller.delete(snapshot_id):
        return DeleteSnapshotResponse(deleted=False, reason=f"the delete of {snapshot_id} failed")
    return DeleteSnapshotResponse(deleted=True)
