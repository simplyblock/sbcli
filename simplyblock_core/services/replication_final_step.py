# coding=utf-8
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
from simplyblock_core import db_controller, utils
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

db = db_controller.DBController()

# Final-step transfers copy only the small dirty delta; a small cluster batch
# keeps the IO-freeze window short.
_FINAL_STEP_BATCH = 2


def _get_transfer_nic(node):
    """Return (trtype, ip_address) for the preferred data interface of *node*."""
    trtype = "RDMA" if node.active_rdma else "TCP"
    for nic in node.data_nics:
        if nic.ip4_address:
            return trtype, nic.ip4_address
    return trtype, node.mgmt_ip


def _online_peers(node):
    """Return the online secondary/tertiary nodes of *node* (HA peers)."""
    peers = []
    for peer_id in [node.secondary_node_id, node.tertiary_node_id]:
        if not peer_id:
            continue
        try:
            peer = db.get_storage_node_by_id(peer_id)
        except KeyError:
            continue
        if peer.status == StorageNode.STATUS_ONLINE:
            peers.append(peer)
    return peers


def _node_paths(primary_node, lvstore):
    """Build ordered ANA path entries for *primary_node* and its online peers.

    Each entry: {'node_id', 'rpc', 'ip', 'trtype', 'port'}. The primary is first.
    """
    def _entry(node):
        trtype, ip = _get_transfer_nic(node)
        return {
            'node_id': node.get_id(),
            'rpc': node.rpc_client(),
            'ip': ip,
            'trtype': trtype,
            'port': node.get_lvol_subsys_port(lvstore),
        }

    paths = [_entry(primary_node)]
    for peer in _online_peers(primary_node):
        paths.append(_entry(peer))
    return paths


def _flip(path, state, nqn, label):
    """Set the ANA state of *nqn*'s listener on a single path (non-fatal)."""
    try:
        path['rpc'].nvmf_subsystem_listener_set_ana_state(
            nqn, path['ip'], path['port'], trtype=path['trtype'], ana=state)
        logger.info(f"ANA {nqn} {label} {path['ip']}:{path['port']} → {state}")
    except Exception as e:
        logger.error(f"ANA flip {label} failed (non-fatal): {e}")


def flip_ana_failover(src_node, tgt_node, src_lvstore, tgt_lvstore, nqn):
    """Flip ANA so the client fails over from source to target.

    Target primary → optimized, other target paths → non_optimized, every source
    path → inaccessible. Cross-cluster: source and target never overlap, so the
    target is brought live before the source is fenced.
    """
    tgt_paths = _node_paths(tgt_node, tgt_lvstore)
    for i, tgt in enumerate(tgt_paths):
        _flip(tgt, "optimized" if i == 0 else "non_optimized", nqn,
              f"TGT-{tgt['node_id'][:8]}")

    # Source may be entirely gone (fail-over after a cluster failure); flips are
    # best-effort. Skip when the source node is unreachable to avoid blocking.
    if src_node is not None and src_node.status in [
            StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        for src in _node_paths(src_node, src_lvstore):
            _flip(src, "inaccessible", nqn, f"SRC-{src['node_id'][:8]}")


def ensure_hub_attached(src_rpc, tgt_node):
    """Ensure the target's transfer-hub lvol is NVMe-oF attached on the source.

    The hub lvol is the gateway the source pushes the final delta through.
    Returns (hub_bdev_name, remote_bdev_name, error_string|None).
    """
    if tgt_node.transfer_hublvol is None or not tgt_node.transfer_hublvol.bdev_name:
        tgt_node.create_transfer_hublvol()

    hub = tgt_node.transfer_hublvol
    # Already attached (prior iteration or crash recovery).
    if src_rpc.get_bdevs(hub.get_remote_bdev_name()):
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


def run_cutover(src_node, tgt_node, lvol, tgt_lvol_composite, tgt_map_id,
                tgt_snap_composite, operation="replicate"):
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

    Returns (ok: bool, error: str|None). The IO-freeze window is bounded by the
    synchronous ``bdev_lvol_transfer_final_step`` call.
    """
    src_rpc = src_node.rpc_client()
    src_lvol_composite = f"{src_node.lvstore}/{lvol.lvol_bdev}"

    hub_bdev, _remote, err = ensure_hub_attached(src_rpc, tgt_node)
    if err:
        return False, err

    logger.info(
        f"[IO-FREEZE] bdev_lvol_transfer_final_step starting: lvol={lvol.uuid} "
        f"src={src_lvol_composite} tgt_snap={tgt_snap_composite} op={operation}")
    ret = src_rpc.bdev_lvol_transfer_final_step(
        src_lvol_composite, tgt_map_id, tgt_snap_composite,
        _FINAL_STEP_BATCH, hub_bdev, operation)
    if ret is None:
        return False, "bdev_lvol_transfer_final_step failed"
    logger.info(f"[IO-RESUME] final step Done: lvol={lvol.uuid} io now live on target")

    # Link the final lvol to its predecessor snapshot on the target peers.
    # bdev_lvol_transfer_final_step handles the primary internally; peers need an
    # explicit add_clone. Non-fatal — a missing peer link self-heals on rejoin.
    for peer in _online_peers(tgt_node):
        if not peer.rpc_client().bdev_lvol_add_clone(tgt_lvol_composite, tgt_snap_composite):
            logger.warning(
                f"add_clone on peer {peer.get_id()[:8]} failed for final lvol (non-fatal)")

    # Flip ANA so the client fails over to the target paths.
    flip_ana_failover(src_node, tgt_node, src_node.lvstore, tgt_node.lvstore, lvol.nqn)
    return True, None
