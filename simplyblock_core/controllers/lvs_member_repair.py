"""Repair of an lvstore member whose copy keeps failing to register volumes.

bf36 R6 (2026-11-25): a tertiary that had been shut down came back, and its copy
of LVS_19 failed every volume registration for 10 hours ("Metadata page is all
zero", "Invalid blobid according to used_blobids"). The control plane alerted
(LVS_MEMBER_REGISTER_FAILING, #1715) but did nothing about it.

The repair here is the control plane's one non-disruptive reload of a member's
copy: ``bdev_lvol_update_lvstore`` (spdk_lvs_update_live with no blob), which
re-reads the blobstore's super block and its masks from disk on a non-leader.
It is the reload the guarded leaderless recovery runs before a grant
(storage_node_ops._recover_leaderless_lvs). It repairs a member whose in-memory
blobstore state went stale (a used-blobid mask it missed: "Invalid blobid
according to used_blobids"). It does NOT repair a member whose data layer
serves stale data: there the reload reads the same wrong pages. That was bf36's
case: the member's distrib placement map stopped because its journal copy was
excluded (spdk #113, ultra #250). Only a restart of the member's storage node
rebuilds that view, so a repair that does not stop the failures says so.

Safety: the member is repaired only when
  * it is ONLINE and no restart/activation/expansion owns the lvstore on it;
  * its own SPDK answers that it does NOT lead the lvstore (the reload is a
    non-leader operation; SPDK refuses it on a leader);
  * another member answers that it DOES lead the lvstore: the broken copy is
    never the only one left serving it.
Rate limits are the caller's (tasks_runner_sync_lvol_del).
"""
from simplyblock_core import constants, db_controller, storage_node_ops, utils
from simplyblock_core.controllers import replica_chain, storage_events
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

REPAIRED = "repaired"
SKIPPED = "skipped"
FAILED = "failed"


def lvstore_members(lvs_name, cluster_id, db=None):
    """The members of *lvs_name*: its primary first, then its secondary and
    tertiary (replica_chain.lvstore_replicas). [] when no node owns it."""
    db = db or db_controller.DBController()
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.lvstore == lvs_name and node.status != StorageNode.STATUS_REMOVED:
            return [node] + [n for _role, n in replica_chain.lvstore_replicas(node, db=db)]
    return []


def _preconditions(node, lvs_name, db):
    """``""`` when *node*'s copy of *lvs_name* may be reloaded now, else why not."""
    if node.status != StorageNode.STATUS_ONLINE:
        return f"node is {node.status}"
    try:
        cluster = db.get_cluster_by_id(node.cluster_id)
        if cluster.status == Cluster.STATUS_IN_ACTIVATION:
            return "cluster is in activation"
    except KeyError:
        return "cluster not found"
    if storage_node_ops.get_restart_phase(node.get_id(), lvs_name):
        return "a restart/activation/expansion owns the lvstore on this node"
    try:
        if storage_node_ops._lvs_leadership_reported(node, lvs_name):
            return "the node leads the lvstore (the reload is a non-leader operation)"
    except Exception as e:                                  # noqa: BLE001
        return f"the node does not answer whether it leads the lvstore ({e})"
    others = [m for m in lvstore_members(lvs_name, node.cluster_id, db=db)
              if m.get_id() != node.get_id() and m.status == StorageNode.STATUS_ONLINE]
    leaders, _unknown = storage_node_ops._lvs_leader_probe(others, lvs_name)
    if not leaders:
        return "no other member answers as leader of the lvstore"
    return ""


def repair_member_copy(node, lvs_name, db=None, previous_repair_at=None):
    """Reload *node*'s copy of *lvs_name* (see the module doc).

    Returns ``(outcome, detail)``: REPAIRED, SKIPPED (a safety rule held; no RPC
    sent) or FAILED. Every outcome but SKIPPED raises an event; a skip is only
    logged, since it repeats while the condition holds."""
    db = db or db_controller.DBController()
    why = _preconditions(node, lvs_name, db)
    if why:
        logger.warning("Not reloading %s on %s: %s", lvs_name, node.get_id(), why)
        return SKIPPED, why
    _event(node, lvs_name, "started", "reloading this node's copy of the lvstore "
           "(bdev_lvol_update_lvstore)", previous_repair_at)
    try:
        ok = node.rpc_client(timeout=constants.LVS_MEMBER_REPAIR_RPC_TIMEOUT_SEC,
                             retry=1).bdev_lvol_update_lvstore(lvs_name)
    except Exception as e:                                  # noqa: BLE001
        ok, detail = False, f"bdev_lvol_update_lvstore failed: {e}"
    else:
        detail = "reloaded" if ok else "bdev_lvol_update_lvstore returned False"
    _event(node, lvs_name, "done" if ok else "failed", detail, previous_repair_at)
    return (REPAIRED if ok else FAILED), detail


def _event(node, lvs_name, stage, detail, previous_repair_at):
    try:
        storage_events.snode_lvs_member_repair(node, lvs_name, stage, detail,
                                               previous_repair_at=previous_repair_at)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("Could not log the member repair event: %s", e)
