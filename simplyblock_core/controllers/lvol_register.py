"""Registering a leader's lvol on a secondary (``bdev_lvol_register``).

The secondary opens the blob the leader created and exposes it as an lvol
bdev. SPDK (spdk #98) refuses that with -EEXIST when another lvol of the
lvstore is already registered on the same blob id: a stale lvol the leader
deleted but this node never removed, whose blob id the leader then reused.
Before that check the new lvol silently shared the stale lvol's open blob
handle, and deleting either one freed the handle under the other (bed bf32,
2026-11-21: two secondaries crashed with SIGSEGV in vbdev_lvol_resv_refresh).

A refused register used to fail like any other, and the stale lvol stayed:
every later reuse of that blob id failed on this node again. Here the stale
lvol is removed and the register retried once, but only when the control plane
has no live record of it. An lvol the control plane still knows is never
deleted on a guess; the register fails as before and says why.
"""

import errno
import logging

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.rpc_client import RPCRemoteError

logger = logging.getLogger(__name__)

# A record in one of these states is the control plane's own deletion in
# progress: its lvol on a secondary is what the deletion would remove anyway.
_GOING = (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED)


def _holder_on_blob(rpc_client, lvs_name, blobid):
    """The lvol of ``lvs_name`` registered on ``blobid`` on this node, or None."""
    for lv in rpc_client.bdev_lvol_get_lvols(lvs_name) or []:
        try:
            if int(lv.get("blobid", -1)) == int(blobid):
                return lv
        except (TypeError, ValueError):
            continue
    return None


def _live_record(cluster_id, lvs_name, name, uuid, db=None):
    """A control-plane volume record that still owns the lvol ``name``/``uuid``."""
    db = db or DBController()
    for rec in db.get_lvols(cluster_id):
        if rec.lvs_name != lvs_name:
            continue
        if rec.lvol_bdev != name and rec.lvol_uuid != uuid:
            continue
        if rec.status in _GOING:
            continue
        return rec
    return None


def stale_lvol_to_remove(rpc_client, lvs_name, lvol_name, lvol_uuid, blobid, cluster_id, db=None):
    """Decide what a refused register (-EEXIST) found on ``blobid``.

    Returns ``(holder, reason, is_self)``. ``holder`` is the stale lvol (as
    reported by ``bdev_lvol_get_lvols``) that may be removed, or None with
    ``reason`` saying why nothing will be removed. ``is_self``: the lvol on
    the blob is the one being registered (a repeated register).
    """
    holder = _holder_on_blob(rpc_client, lvs_name, blobid)
    if holder is None:
        return None, f"no lvol of {lvs_name} is registered on blob {blobid} here", False
    name, uuid = holder.get("name", ""), holder.get("uuid", "")
    if name == lvol_name or (lvol_uuid and uuid == lvol_uuid):
        return None, f"{lvs_name}/{name} is this lvol, already registered", True
    if holder.get("is_snapshot"):
        # Removing a snapshot re-parents or drops its clones: never on a guess.
        return None, f"blob {blobid} is held by snapshot {lvs_name}/{name}", False
    rec = _live_record(cluster_id, lvs_name, name, uuid, db=db)
    if rec is not None:
        return None, (f"blob {blobid} is held by {lvs_name}/{name}, which the control plane "
                      f"still knows as lvol {rec.get_id()} ({rec.status})"), False
    return holder, "", False


def register_lvol(rpc_client, lvol_name, lvs_name, lvol_uuid, blobid, priority_class=0, *,
                  cluster_id, node_label="", db=None):
    """``bdev_lvol_register`` that clears a stale lvol on the same blob id.

    Same result as ``rpc_client.bdev_lvol_register``. On -EEXIST the stale
    lvol is removed (sync delete on this node, confirmed gone) and the
    register is retried once; when it may not be removed, the original error
    is raised.
    """
    try:
        return rpc_client.bdev_lvol_register(lvol_name, lvs_name, lvol_uuid, blobid, priority_class)
    except RPCRemoteError as e:
        if e.code != -errno.EEXIST:
            raise
        refused = e

    where = f" on {node_label}" if node_label else ""
    holder, reason, is_self = stale_lvol_to_remove(
        rpc_client, lvs_name, lvol_name, lvol_uuid, blobid, cluster_id, db=db)
    if holder is None:
        if is_self:
            logger.info(f"register {lvs_name}/{lvol_name}{where}: {reason}")
            return rpc_client.bdev_get(f"{lvs_name}/{lvol_name}") or True
        logger.error(f"register {lvs_name}/{lvol_name} (blob {blobid}){where} refused "
                     f"(EEXIST) and not cleared: {reason}")
        raise refused

    stale = f"{lvs_name}/{holder.get('name')}"
    logger.warning(
        f"register {lvs_name}/{lvol_name} (blob {blobid}){where} refused: stale lvol {stale} "
        f"(uuid {holder.get('uuid')}) is still registered on that blob, the leader deleted "
        f"it and reused the id. Removing it here and retrying")
    try:
        rpc_client.delete_lvol(stale, sync=True)
    except RPCRemoteError as e:
        if e.code != -errno.ENODEV:
            logger.error(f"delete of stale lvol {stale}{where} failed: {e}")
            raise refused from e
    if rpc_client.bdev_get(stale) is not None:
        logger.error(f"stale lvol {stale}{where} is still present after its delete; "
                     f"not registering {lvs_name}/{lvol_name} on blob {blobid}")
        raise refused
    return rpc_client.bdev_lvol_register(lvol_name, lvs_name, lvol_uuid, blobid, priority_class)
