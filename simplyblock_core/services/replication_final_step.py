"""Cross-cluster replication cutover (the "final step").

Used by migration-commit and fail-back. Freezes source I/O, transfers the final
writable-lvol delta to the target via ``bdev_lvol_transfer_final_step`` (operation
``replicate``), links the final lvol to its predecessor snapshot on the target's
secondary/tertiary nodes, then flips ANA states so the NVMe-oF client fails over
to the target paths without an explicit disconnect/reconnect.

This is the cross-cluster analogue of the intra-cluster migration runner's
LVOL_MIGRATE cutover. Because the source and target live on *different* clusters
they never share nodes, so the source/target path sets never overlap — the ANA
choreography is the simple "no-overlap" case (target primary → optimized, other
target paths → non_optimized, all source paths → inaccessible).
"""
import uuid as uuid_lib

from simplyblock_core import constants, db_controller, utils, xfer_timing
from simplyblock_core.models.hublvol import HubLVol
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

logger = utils.get_logger(__name__)

db = db_controller.DBController()

# Final-step transfers copy only the small dirty delta; a small cluster batch
# keeps the IO-freeze window short.
_FINAL_STEP_BATCH = 2


def _get_transfer_nic(node):
    """Return (trtype, ip_address) for the preferred data interface of *node*: the
    address peers connect to."""
    trtype = "RDMA" if node.active_rdma else "TCP"
    for nic in node.data_nics:
        if nic.ip4_address:
            return trtype, nic.ip4_address
    return trtype, node.mgmt_ip


def _get_transfer_listen_ip(node):
    """The address the node's target binds on its preferred data interface, which
    is what a listener RPC names; on the pod network it is not the connect address."""
    for nic in node.data_nics:
        if nic.ip4_address:
            return nic.listen_address
    return node.mgmt_ip


# Statuses in which a member may still be serving the volume and must be
# asked to fence: a suspended or down node's SPDK keeps its listeners.
_FENCEABLE = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
              StorageNode.STATUS_DOWN)


def _lvs_members(primary_node):
    """The members of *primary_node*'s lvstore in role order, any status."""
    members = [primary_node]
    for peer_id in [primary_node.secondary_node_id, primary_node.tertiary_node_id]:
        if not peer_id:
            continue
        try:
            members.append(db.get_storage_node_by_id(peer_id))
        except KeyError:
            continue
    return members


def _online_peers(primary_node, lead=None):
    """Return the online members of *primary_node*'s lvstore other than *lead*
    (the primary by default). Membership is always read from the PRIMARY: an
    acting leader's own secondary/tertiary fields describe the lvstore IT is
    primary of, not this one."""
    lead_id = (lead or primary_node).get_id()
    return [m for m in _lvs_members(primary_node)
            if m.get_id() != lead_id and m.status == StorageNode.STATUS_ONLINE]


def _path_entry(node, lvstore):
    trtype, ip = _get_transfer_nic(node)
    return {
        'node_id': node.get_id(),
        'rpc': node.rpc_client(),
        'ip': ip,
        'listen_ip': _get_transfer_listen_ip(node),
        'trtype': trtype,
        'port': node.get_lvol_subsys_port(lvstore),
    }


def _node_paths(primary_node, lvstore, lead=None, statuses=(StorageNode.STATUS_ONLINE,)):
    """Build ordered ANA path entries for the members of *primary_node*'s lvstore.

    Each entry: {'node_id', 'rpc', 'ip', 'listen_ip', 'trtype', 'port'}. *lead* -- the
    member serving or receiving the IO, the primary by default -- is first;
    members whose status is not in *statuses* are left out (*lead* only when
    it is not in *statuses* itself).
    """
    lead = lead or primary_node
    members = [m for m in _lvs_members(primary_node) if m.get_id() != lead.get_id()]
    ordered = [lead] + members
    return [_path_entry(m, lvstore) for m in ordered if m.status in statuses]


def _flip(path, state, nqn, label, ns_id=None):
    """Set the ANA state of *nqn*'s listener on a single path (non-fatal).

    ``ns_id`` confines the change to that namespace's ANA group. Without it the
    whole subsystem moves, which is wrong when the subsystem carries other
    namespaces that are not part of this cutover — they share the client's
    controller, so their IO would follow this volume to the other cluster.
    """
    try:
        path['rpc'].nvmf_subsystem_listener_set_ana_state(
            nqn, path['listen_ip'], path['port'], trtype=path['trtype'], ana=state,
            anagrpid=ns_id)
        logger.info(f"ANA {nqn} ns {ns_id} {label} {path['listen_ip']}:{path['port']} → {state}")
    except Exception as e:
        logger.error(f"ANA flip {label} failed (non-fatal): {e}")


def fence_source_paths(src_node, src_lvstore, nqn, ns_id=None, executing=None):
    """Make EVERY source path inaccessible BEFORE the freeze/final transfer.

    Once the final delta is taken, the source must not receive IO by any means:
    a write accepted on a still-optimized source path after the delta of record
    is silently lost, and lighting the target while the source is still
    optimized creates a dual-writable window. Client IO queues on all-dark
    paths (NVMe multipath semantics) and drains to the target when it is
    enabled — the dark window is bounded by freeze + residual delta, which the
    shrink rounds keep to seconds. Peers first, the executing member last.

    Every member that may still serve is fenced, whatever the PRIMARY's state:
    skipping the whole source because the primary was offline left its peers
    optimized while the target lit up. Members that are gone are skipped --
    fail-over after a cluster failure has nothing left to fence.
    """
    if src_node is None:
        return
    paths = _node_paths(src_node, src_lvstore, lead=executing, statuses=_FENCEABLE)
    for src in paths[1:] + paths[:1]:      # peers first, executing member last
        _flip(src, "inaccessible", nqn, f"SRC-{src['node_id'][:8]}", ns_id)


def restore_source_paths(src_node, src_lvstore, nqn, ns_id=None, executing=None):
    """Failure path: re-enable the fenced source (the executing member --
    primary by default -- optimized, other members non_optimized) — the
    cutover did not happen, the source remains the authoritative copy and must
    serve again."""
    if src_node is None:
        return
    paths = _node_paths(src_node, src_lvstore, lead=executing, statuses=_FENCEABLE)
    for i, src in enumerate(paths):
        _flip(src, "optimized" if i == 0 else "non_optimized", nqn,
              f"SRC-{src['node_id'][:8]}(restore)", ns_id)


def enable_target_paths(tgt_node, tgt_lvstore, nqn, ns_id=None, receiving=None):
    """Bring the target paths live AFTER the final transfer: the receiving
    member (primary by default) optimized, other online members
    non_optimized. Queued client IO drains here."""
    tgt_paths = _node_paths(tgt_node, tgt_lvstore, lead=receiving)
    for i, tgt in enumerate(tgt_paths):
        _flip(tgt, "optimized" if i == 0 else "non_optimized", nqn,
              f"TGT-{tgt['node_id'][:8]}", ns_id)


def _transfer_hub_live(tgt_node):
    """True when the target's transfer hublvol exists in SPDK, not just in the DB.

    Both halves matter: the bdev carries the data and the subsystem is what the
    source connects to, and a restart removes both while leaving the DB record
    behind.
    """
    hub = tgt_node.transfer_hublvol
    if hub is None or not hub.bdev_name:
        return False
    try:
        rpc_client = tgt_node.rpc_client()
        if not rpc_client.bdev_get(hub.bdev_name):
            logger.info("Transfer hublvol bdev %s missing on %s (restart wipes SPDK "
                        "state); recreating", hub.bdev_name, tgt_node.get_id())
            return False
        if not rpc_client.subsystem_get(hub.nqn):
            logger.info("Transfer hublvol subsystem %s missing on %s; recreating",
                        hub.nqn, tgt_node.get_id())
            return False
    except Exception as e:
        # Unreachable target: let the caller's attach fail and retry rather than
        # recreating blindly against a node we cannot talk to.
        logger.warning("Could not verify the transfer hublvol on %s: %s",
                       tgt_node.get_id(), e)
        return True
    return True


def _peer_transfer_hub(node, lvs_primary):
    """The transfer hub *node* receives through for *lvs_primary*'s lvstore
    when *node* is a non-primary member of it. Derived, never persisted: the
    hub blob is in-memory in SPDK, so the record could only ever go stale.

    The name carries the node because the SOURCE keys its attached controller
    by it (ensure_hub_attached reuses an attachment by name): a hub named like
    the primary's would be taken for the primary's attachment, and the delta
    would keep going to the old primary.
    """
    lvs = lvs_primary.lvstore
    name = f"transferhub_{node.get_id()[:8]}"
    ident = uuid_lib.uuid5(uuid_lib.NAMESPACE_URL, f"transferhub:{lvs}:{node.get_id()}")
    return HubLVol({
        'nqn': f"{constants.CLUSTER_NQN}:{node.cluster_id}:transferhub:{lvs}:{node.get_id()}",
        'bdev_name': f"{lvs}/{name}",
        # The target lvstore's hub port: a member already listens on it for
        # that lvstore, and failover port blocks cover it with the lvstore.
        'nvmf_port': lvs_primary.hublvol.nvmf_port,
        'model_number': str(ident),
        'nguid': ident.hex,
        'hublvol_name': name,
    })


def receiving_hub(node, lvs_primary=None):
    """The hub *node* receives transfers into *lvs_primary*'s lvstore through:
    its own transfer hub when it is that lvstore's primary (or no lvstore is
    named), else its per-lvstore peer hub."""
    if lvs_primary is None or lvs_primary.get_id() == node.get_id():
        return node.transfer_hublvol
    return _peer_transfer_hub(node, lvs_primary)


def _ensure_peer_transfer_hub(node, lvs_primary):
    """Create and expose *node*'s hub on *lvs_primary*'s lvstore; (hub, error).

    SPDK demuxes hub IO by map id in the hub's OWN lvstore
    (vbdev_hublvol_submit_request), so a member receiving for an lvstore it is
    not primary of needs a hub IN that lvstore -- its regular transfer hub sits
    on the lvstore it is primary of. Idempotent: an existing hub is reused and
    expose_bdev skips what is already there, which also heals a restart.
    """
    hub = _peer_transfer_hub(node, lvs_primary)
    rpc_client = node.rpc_client()
    try:
        existing = rpc_client.bdev_get(hub.bdev_name)
        if existing:
            hub_uuid = existing.get('uuid', '') if isinstance(existing, dict) else ''
        else:
            hub_uuid = rpc_client.bdev_lvol_create_hublvol(
                lvs_primary.lvstore, name=hub.hublvol_name)
            if not hub_uuid:
                return None, (f"Failed to create transfer hub {hub.bdev_name} "
                              f"on {node.get_id()}")
            logger.info("Created transfer hub %s on %s", hub.bdev_name, node.get_id())
        node.expose_bdev(nqn=hub.nqn, bdev_name=hub.bdev_name,
                         model_number=hub.model_number, uuid=hub_uuid,
                         nguid=hub.nguid, port=hub.nvmf_port, ana_state="optimized")
    except Exception as e:                                # noqa: BLE001
        return None, f"Failed to provide transfer hub {hub.bdev_name} on {node.get_id()}: {e}"
    return hub, None


def ensure_hub_attached(src_rpc, tgt_node, lvs_primary=None):
    """Ensure the hub *tgt_node* receives through is NVMe-oF attached on the source.

    The hub lvol is the gateway the source pushes the transfer through. With
    *lvs_primary* naming a target lvstore *tgt_node* is not primary of (an
    acting secondary leader), the hub is *tgt_node*'s per-lvstore peer hub.
    Returns (hub_bdev_name, remote_bdev_name, error_string|None).
    """
    if lvs_primary is not None and lvs_primary.get_id() != tgt_node.get_id():
        hub, err = _ensure_peer_transfer_hub(tgt_node, lvs_primary)
        if err:
            return None, None, err
        return _attach_hub(src_rpc, tgt_node, hub)
    # Trusting the DB record here is what broke every fail-back into a restarted
    # node (215 attach failures, labs 2026-08-17/18): a restart wipes SPDK's
    # bdevs and subsystems while ``transfer_hublvol`` survives in the DB, so this
    # branch was skipped and the source attached to a subsystem that no longer
    # existed -- bdev_nvme_attach_controller returned -5 (EIO) and afterwards
    # "Controller ... does not exist". The HA hublvol is recreated by the restart
    # flow (recreate_hublvol); the transfer hublvol is not recreated anywhere, so
    # verify it on the TARGET and heal it here. create_transfer_hublvol is
    # idempotent: it reuses the record, recreates the bdev only when SPDK lacks
    # it, and re-exposes the subsystem.
    if not _transfer_hub_live(tgt_node):
        try:
            tgt_node.create_transfer_hublvol()
        except Exception as e:
            return None, None, f"Failed to (re)create the transfer hublvol on {tgt_node.get_id()}: {e}"
        if not _transfer_hub_live(tgt_node):
            return None, None, (f"Transfer hublvol still absent on {tgt_node.get_id()} "
                                f"after recreation")

    return _attach_hub(src_rpc, tgt_node, tgt_node.transfer_hublvol)


def _attach_hub(src_rpc, tgt_node, hub):
    """Attach *hub* (exposed on *tgt_node*) on the source; the
    (hub_bdev_name, remote_bdev_name, error) triple of ensure_hub_attached."""
    # Already attached (prior iteration or crash recovery).
    if src_rpc.bdev_get(hub.get_remote_bdev_name()):
        return hub.bdev_name, hub.get_remote_bdev_name(), None

    for iface in tgt_node.data_nics:
        ip = iface.ip4_address
        if tgt_node.active_rdma:
            if iface.trtype != "RDMA":
                continue
            trtype = "RDMA"
        else:
            if iface.trtype != "TCP":
                continue
            trtype = "TCP"

        ret = src_rpc.bdev_nvme_attach_controller(
            hub.bdev_name, hub.nqn, ip, hub.nvmf_port, trtype)
        if not ret:
            # Detach a zombie controller from a crashed attempt and retry once.
            if src_rpc.bdev_nvme_controller_list(hub.bdev_name):
                src_rpc.bdev_nvme_detach_controller(hub.bdev_name)
                ret = src_rpc.bdev_nvme_attach_controller(
                    hub.bdev_name, hub.nqn, ip, hub.nvmf_port, trtype)
            if not ret:
                return None, None, f"Failed to attach transfer hub controller to {tgt_node.get_id()}"
    return hub.bdev_name, hub.get_remote_bdev_name(), None


class CutoverUnreachable(Exception):
    """The target could not be reached for the cutover: its transfer hub did
    not attach. A target node restarting before the control plane marks it
    down looks like this, and waiting it out is the answer. A cutover that
    reached the target and failed returns (False, error) instead, and fails
    the same way on every attempt."""


def run_cutover(src_node, tgt_node, lvol, tgt_lvol_composite, tgt_map_id,
                tgt_snap_composite, operation="replicate", src_exec=None,
                tgt_exec=None):
    """Perform the cross-cluster cutover for *lvol*.

    Args:
        src_node:            source StorageNode (the current primary)
        tgt_node:            target StorageNode (the new primary)
        lvol:                the LVol being cut over (source record)
        tgt_lvol_composite:  composite name of the writable target lvol
                             (``<tgt_lvstore>/<bdev>``)
        tgt_map_id:          map_id of the target lvol (from bdev_lvol_get_lvols)
        tgt_snap_composite:  composite name on the target of the last replicated
                             snapshot (the final lvol is chained onto it)
        operation:          ``"replicate"`` for cross-cluster (metadata + data)
        src_exec:            the source member that serves the volume's IO and
                             therefore runs the freeze -- the LVS leader;
                             defaults to *src_node*
        tgt_exec:            the target member that receives the final delta
                             -- the settled LVS leader; defaults to *tgt_node*

    Returns (ok: bool, error: str|None), and raises CutoverUnreachable when the
    target's transfer hub cannot be attached. The IO-freeze window is bounded
    by the synchronous ``bdev_lvol_transfer_final_step`` call.
    """
    # The freeze must run where the IO is served, and the delta must land where
    # the target lvstore accepts receive IO: the leaders, which are the
    # primaries unless one of them is down (docs/online-migration.md, G2).
    src_exec = src_exec or src_node
    tgt_exec = tgt_exec or tgt_node
    if src_exec.get_id() != src_node.get_id() or tgt_exec.get_id() != tgt_node.get_id():
        logger.info("cutover lvol=%s: freezing on %s, receiving on %s "
                    "(primaries %s / %s)", lvol.get_id(), src_exec.get_id(),
                    tgt_exec.get_id(), src_node.get_id(), tgt_node.get_id())
    src_rpc = src_exec.rpc_client()
    src_lvol_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"

    # The gateway must be the attached NAMESPACE bdev ("<lvstore>/transferhubn1"),
    # not the controller name ("<lvstore>/transferhub") that
    # bdev_nvme_attach_controller was given -- only the former exists as a bdev.
    # Passing the controller name made every cross-cluster cutover fail with
    # ENODEV (-19) and left the volume stuck in cutover_pending forever, while
    # snapshot replication (which passes the n1 bdev) kept working. This matches
    # tasks_runner_lvol_migration, which uses the second element for the same RPC.
    with xfer_timing.phase("final_hub_attach", lvol=lvol.get_id()):
        # tgt_node is the target lvstore's primary: an acting secondary
        # receives through its own hub ON that lvstore (receiving_hub).
        _ctrl_name, hub_bdev, err = ensure_hub_attached(src_rpc, tgt_exec, tgt_node)
    if err:
        raise CutoverUnreachable(err)

    # Fence the source FIRST: from here on the source cannot take IO by any
    # means; the delta the freeze copies is definitively final.
    #
    # The client-visible freeze begins at this call and ends at
    # enable_target_paths, so freeze_total below is the number that has to fit
    # inside the client's fast_io_fail_tmo (8s in the soak).
    _freeze_started = xfer_timing.now()
    xfer_timing.stamp("freeze_begin", lvol=lvol.get_id(), nqn=lvol.nqn,
                      nsid=lvol.ns_id)
    with xfer_timing.phase("fence_source", lvol=lvol.get_id()):
        fence_source_paths(src_node, src_node.lvstore, lvol.nqn, lvol.ns_id,
                           executing=src_exec)

    logger.info(
        f"[IO-FREEZE] bdev_lvol_transfer_final_step starting: lvol={lvol.uuid} "
        f"src={src_lvol_composite} tgt_snap={tgt_snap_composite} "
        f"gateway={hub_bdev} op={operation}")
    with xfer_timing.phase("final_step_transfer", lvol=lvol.get_id(),
                           batch=_FINAL_STEP_BATCH):
        try:
            ret = src_rpc.bdev_lvol_transfer_final_step(
                src_lvol_composite, tgt_map_id, tgt_snap_composite,
                _FINAL_STEP_BATCH, hub_bdev, operation)
        except RPCException as e:
            # A raise used to skip the restore below and leave the source
            # fenced (the volume dark) through every retry (SFAM-2834). SPDK
            # may still have finished -- e.g. a read timeout on a long
            # freeze -- so ask it; only an explicit Done counts.
            logger.error(f"bdev_lvol_transfer_final_step raised for {src_lvol_composite}: {e}")
            try:
                ret = src_rpc.bdev_lvol_transfer_stat(src_lvol_composite)
            except RPCException as stat_e:
                logger.error(f"bdev_lvol_transfer_stat for {src_lvol_composite} failed: {stat_e}")
                ret = None
    # The RPC can return normally (no exception) while still reporting the
    # transfer failed -- transfer_state is one of "No process" | "In progress" |
    # "Failed" | "Done".  Checking only for None lets a "Failed" response slip
    # through to the ANA flip as if the data had moved (seen 2026-08-31,
    # lvol=1fc7911f: {"transfer_state": "Failed"} logged, then [IO-RESUME]
    # fired unconditionally, leaving the target with missing delta data).
    transfer_state = ret.get("transfer_state") if isinstance(ret, dict) else None
    if transfer_state != "Done":
        # The freeze failed with the source fenced: restore the source paths so
        # the client resumes there (nothing moved; source is still authoritative)
        # rather than leaving the volume dark until a retry succeeds.
        logger.error(
            f"bdev_lvol_transfer_final_step: transfer_state={transfer_state!r} "
            f"(expected 'Done'): {ret!r} lvol={lvol.uuid}")
        with xfer_timing.phase("restore_source_paths", lvol=lvol.get_id()):
            restore_source_paths(src_node, src_node.lvstore, lvol.nqn, lvol.ns_id,
                                 executing=src_exec)
        xfer_timing.gap("freeze_total", _freeze_started, lvol=lvol.get_id(),
                        aborted=1)
        return False, (
            f"bdev_lvol_transfer_final_step: transfer_state={transfer_state!r}"
            f" (expected 'Done'): {ret!r}"
        )
    logger.info(f"[IO-RESUME] final step Done: lvol={lvol.uuid} io now live on target")

    # Link the final lvol to its predecessor snapshot on the target peers.
    # bdev_lvol_transfer_final_step handles the receiving member internally;
    # the other online members need an explicit add_clone. Non-fatal — a
    # missing peer link self-heals on rejoin.
    for peer in _online_peers(tgt_node, lead=tgt_exec):
      with xfer_timing.phase("final_peer_add_clone", lvol=lvol.get_id(),
                             peer=peer.get_id()):
        try:
            add_clone_ok = peer.rpc_client().bdev_lvol_add_clone(tgt_lvol_composite, tgt_snap_composite)
        except RPCException as e:
            logger.warning(
                f"add_clone on peer {peer.get_id()[:8]} failed for final lvol (non-fatal): {e}")
            continue
        if not add_clone_ok:
            logger.warning(
                f"add_clone on peer {peer.get_id()[:8]} failed for final lvol (non-fatal)")

    # Light the target: queued client IO drains here.
    with xfer_timing.phase("enable_target_paths", lvol=lvol.get_id()):
        enable_target_paths(tgt_node, tgt_node.lvstore, lvol.nqn, lvol.ns_id,
                            receiving=tgt_exec)
    # The whole client-visible window, fence -> paths live. Compare against the
    # client's fast_io_fail_tmo: anything longer surfaces as IO errors, not
    # just latency.
    xfer_timing.gap("freeze_total", _freeze_started, lvol=lvol.get_id())
    return True, None
