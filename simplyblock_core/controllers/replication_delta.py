"""The control plane's half of a partial (delta) snapshot replication.

SPDK keeps, per volume, a bitmap of the 8 KiB blocks written since the last
snapshot, and a partial transfer sends only those blocks. The fork checks what
it can see on the node that runs the transfer (a complete bitmap, started in
the current leadership epoch, on top of the base the destination holds). Two
things it cannot see are checked here:

* the destination: the landing volume must be a clone of the copy of exactly
  the snapshot the delta was computed against (``partial_base``, the source
  predecessor's blob id);
* the other members of the lvstore: only writes the transferring node executed
  itself are in its bitmap. A member whose redirection to the leader was off,
  or one that took leadership while the transferring node still believed it
  led, executes writes the bitmap never sees. Every member counts the data
  writes it executes (``lvs_local_writes``); a delta is safe only if no member
  OTHER than the transferring node wrote while the snapshot's writes
  accumulated, i.e. between taking the predecessor and taking this snapshot.

The counts are read right before a replication snapshot is taken and right
after, and stored on it. The predecessor's "before" and this snapshot's "after"
bracket the window with margin on both ends; another member's count that
changed, any member that restarted (its count starts at zero again;
``online_since`` changes), joined or left, or a member that could not be read,
all mean a full transfer, which is always correct.
"""
import logging

from simplyblock_core.db_controller import DBController

logger = logging.getLogger(__name__)


def _members(db, lvol):
    host = db.get_storage_node_by_id(lvol.node_id)
    ids = [host.get_id(), host.secondary_node_id, host.tertiary_node_id]
    return [db.get_storage_node_by_id(nid) for nid in ids if nid]


def local_write_counts(lvol):
    """``{node_id: [count, online_since]}`` for every member of the volume's
    lvstore, or ``{}`` when any of them could not be read (an older SPDK, a
    member that is down) -- which later rules out a delta for both snapshots
    it brackets."""
    db = DBController()
    counts = {}
    try:
        for node in _members(db, lvol):
            info = node.rpc_client(timeout=5, retry=1).bdev_lvol_dirty_bitmap_info(lvol.top_bdev)
            if not isinstance(info, dict) or "lvs_local_writes" not in info:
                return {}
            counts[node.get_id()] = [int(info["lvs_local_writes"]), node.online_since]
    except Exception as e:                                      # noqa: BLE001
        logger.info("Local write counts of %s unavailable (%s); its neighbouring "
                    "snapshots replicate in full", lvol.get_id(), e)
        return {}
    return counts


def add_snapshot(lvol, name, **kwargs):
    """``snapshot_controller.add`` for a replication snapshot, bracketed by the
    members' local write counts. Same return value."""
    from simplyblock_core.controllers import snapshot_controller

    before = local_write_counts(lvol)
    snap_id, err = snapshot_controller.add(lvol.get_id(), name, **kwargs)
    if err or not snap_id:
        return snap_id, err
    after = local_write_counts(lvol)

    def put(snap):
        snap.local_writes_before = before
        snap.local_writes_after = after

    db = DBController()
    try:
        db.atomic_update(db.get_snapshot_by_id(snap_id), put)
    except Exception as e:                                      # noqa: BLE001
        # Without the record the snapshot just replicates in full.
        logger.warning("Could not record local write counts on %s: %s", snap_id, e)
    return snap_id, err


def delta_basis(prev_snap, snap, transfer_node_id):
    """``(True, "")`` if, between taking ``prev_snap`` and taking ``snap``, no
    member of the lvstore but ``transfer_node_id`` (the node whose bitmap the
    transfer uses) executed a write and none restarted, else
    ``(False, reason)``."""
    start = getattr(prev_snap, "local_writes_before", None) or {}
    end = getattr(snap, "local_writes_after", None) or {}
    if not start or not end:
        return False, "local write counts were not recorded for both snapshots"
    if set(start) != set(end):
        return False, "the lvstore's members changed between the snapshots"
    if transfer_node_id not in end:
        return False, f"the transferring node {transfer_node_id} is not a member"
    for node_id, (count, online_since) in end.items():
        count0, online_since0 = start[node_id]
        if online_since != online_since0:
            return False, f"member {node_id} restarted between the snapshots"
        if node_id != transfer_node_id and count != count0:
            return False, (f"member {node_id} executed {count - count0} write(s) the "
                           f"transferring node's bitmap does not have")
    return True, ""
