"""A spare landing volume per replicating volume, created ahead of its use.

Every replicated snapshot lands in a fresh volume on the destination
(REP_<snapshot name>), created by the replication runner at the start of the
transfer. On a loaded destination that create took 5-47 s instead of 20 ms
(bed bf13 2026-11-05: bdev_lvol_create waits for the metadata journal's home
drain behind every other blob's copy-on-write entries), and all of it was on
the critical path of the volume's lag. A converging cutover already names its
next shrink snapshot ahead and gets that round's landing volume pre-staged
(snapshot_replication._prestage_next_landing); a cadence snapshot's name is
only known when it is taken, so the spare here carries a name of its own,
REP_spare_<lvol id>, and is RENAMED to REP_<snapshot name> when adopted. The
bdev name (LVS_x/LVOL_n) is untouched by that: only the record and its name
index move, under the pool's name uniqueness.

One spare per volume: created once the current transfer is submitted, so the
NEXT cycle's landing create is already done when its snapshot arrives; deleted
when the volume stops replicating (policy detach, replication stop, volume
delete) or its cutover ends.

A spare must never outlive its volume's replication: it holds the volume's full
size on the destination, so leaked spares add up (EKS bed 2026-11-16: 42-43
spares of 10 GiB per cluster after six back-and-forth migrations, every one
keyed by a volume that no longer existed, provisioned capacity at 193%).
Three rules hold that:

* a spare is only created for a volume that still replicates and is not in its
  cutover endgame, checked again AFTER the (slow) create, so a create racing
  with the end of the replication deletes its own spare (:func:`prestage`);
* every path that ends a volume's replication discards it (:func:`discard`);
* :func:`sweep` deletes any remaining spare whose volume no longer replicates.
"""
import logging
import time
from datetime import datetime

from simplyblock_core import db_controller
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol

logger = logging.getLogger(__name__)
db = db_controller.DBController()

PREFIX = "REP_spare_"

# A spare younger than this is never swept: its create may still be settling
# and the transfer that staged it may still be running.
SWEEP_MIN_AGE_SEC = 600
# How often the snapshot monitor sweeps each cluster.
SWEEP_INTERVAL_SEC = 300


def spare_name(lvol_id):
    return f"{PREFIX}{lvol_id}"


def find(lvol_id, pool_uuid=None):
    """The volume's spare landing record, or None."""
    try:
        return db.get_lvol_by_name(spare_name(lvol_id), pool_uuid, include_deleted=True)
    except KeyError:
        return None
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not look up the spare landing volume of %s: %s", lvol_id, e)
        return None


def adopt(snapshot, rep_name, pool_uuid=None):
    """Rename the source volume's spare to *rep_name* and return it, or None.

    A spare that does not fit (not online, another size after a resize) is
    discarded; the caller then creates the landing volume as before.
    """
    lvol_id = snapshot.lvol.get_id()
    spare = find(lvol_id, pool_uuid)
    if spare is None:
        return None
    if spare.status != LVol.STATUS_ONLINE or spare.size != snapshot.size:
        logger.info("Spare landing volume %s of %s does not fit (status %s, size %s vs %s); "
                    "discarding it", spare.get_id(), lvol_id, spare.status, spare.size,
                    snapshot.size)
        if spare.status == LVol.STATUS_ONLINE:
            discard(lvol_id, pool_uuid)
        return None
    try:
        spare.lvol_name = rep_name
        spare.write_to_db()
    except Exception as e:                                # noqa: BLE001
        # the unique-name index refused (a REP_<name> appeared meanwhile) or
        # the write failed: leave the spare for the next cycle
        logger.warning("Could not adopt spare landing volume %s as %s: %s",
                       spare.get_id(), rep_name, e)
        return None
    logger.info("Adopted spare landing volume %s of %s as %s", spare.get_id(), lvol_id, rep_name)
    return spare


def _owner_replicates(lvol_id):
    """True while *lvol_id* is a live volume that replicates; False when it is
    gone, being deleted or no longer replicates; None when that could not be
    read (callers then neither create nor delete)."""
    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        return False
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not read volume %s for its spare landing volume: %s", lvol_id, e)
        return None
    if lvol.status in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED):
        return False
    return bool(lvol.do_replicate)


def _in_endgame(lvol_id, cluster_id=None):
    """True while a cutover of *lvol_id* holds its endgame (lvstore claim or
    shrink rounds), or when that could not be read. The endgame discards the
    spare, and no cadence snapshot of the volume lands while it runs.
    *cluster_id* is the volume's own cluster, looked up when not given."""
    try:
        if not cluster_id:
            cluster_id = db.get_pool_by_id(db.get_lvol_by_id(lvol_id).pool_uuid).cluster_id
        tasks = db.get_job_tasks(cluster_id)
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not read the tasks of %s for the spare of %s: %s",
                       cluster_id, lvol_id, e)
        return True
    for t in tasks:
        if t.function_name != JobSchedule.FN_REPLICATION_FINAL:
            continue
        if t.status == JobSchedule.STATUS_DONE or t.canceled:
            continue
        params = t.function_params or {}
        if params.get("lvol_id") != lvol_id:
            continue
        if params.get("cutover_lvs") or params.get("shrink_snap_id"):
            return True
    return False


def may_stage(lvol_id, cluster_id=None):
    """A spare may exist for *lvol_id*: it replicates and is not in its
    cutover endgame."""
    if _owner_replicates(lvol_id) is not True:
        return False
    return not _in_endgame(lvol_id, cluster_id)


def prestage(snapshot, remote_lv, create_landing, cluster_id=None):
    """Create the volume's spare on the destination of *remote_lv* unless one
    exists. *create_landing(name, snapshot, remote_node, pool_uuid)* is the
    runner's landing-volume factory: (lvol id, error). Best effort.

    *cluster_id* is the source volume's cluster (where its cutover task
    lives; looked up when not given). Nothing is created for a volume that no longer replicates or is in
    its endgame: a transfer still draining after a cutover (the source takes
    no new cadence snapshots, but its queued transfers run on) or after a stop
    would otherwise create a spare that nothing discards again. The create
    takes seconds on a loaded destination, so the same check runs again once
    it returns: whoever ends the replication updates the volume or the task
    first and discards after, so either that discard finds this spare or the
    check here sees the end and deletes it.
    """
    lvol_id = snapshot.lvol.get_id()
    try:
        if not may_stage(lvol_id, cluster_id):
            logger.debug("No spare landing volume for %s: it no longer replicates "
                         "or its cutover endgame runs", lvol_id)
            return None
        if find(lvol_id, remote_lv.pool_uuid) is not None:
            return None
        remote_node = db.get_storage_node_by_id(remote_lv.node_id)
        lv_id, err = create_landing(spare_name(lvol_id), snapshot, remote_node, remote_lv.pool_uuid)
        if not lv_id:
            logger.warning("Could not pre-stage a spare landing volume for %s: %s", lvol_id, err)
            return None
        if not may_stage(lvol_id, cluster_id):
            logger.info("Replication of %s ended while its spare landing volume %s was "
                        "created; deleting it", lvol_id, lv_id)
            discard(lvol_id, remote_lv.pool_uuid)
            return None
        logger.info("Pre-staged spare landing volume %s for %s on %s", lv_id, lvol_id,
                    remote_node.get_id())
        return lv_id
    except Exception as e:                                # noqa: BLE001
        logger.warning("Pre-staging a spare landing volume for %s failed: %s", lvol_id, e)
        return None


def _records(lvol_id, pool_uuid=None):
    """Every spare record of the volume: in one pool, or in all of them. A
    volume that replicated to more than one destination over its life can have
    one in each, and the unscoped name look-up refuses exactly that case
    ("Multiple values present"), which left all of them in place."""
    name = spare_name(lvol_id)
    if pool_uuid:
        spare = find(lvol_id, pool_uuid)
        return [spare] if spare is not None else []
    try:
        return [lv for lv in db.query(LVol, 'lvol_name', name) if lv.lvol_name == name]
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not look up the spare landing volumes of %s: %s", lvol_id, e)
        return []


def discard(lvol_id, pool_uuid=None):
    """Delete the volume's spare(s), if any. Best effort; returns True when one
    was deleted."""
    deleted = False
    for spare in _records(lvol_id, pool_uuid):
        if spare.status in (LVol.STATUS_IN_DELETION, LVol.STATUS_DELETED):
            continue
        from simplyblock_core.controllers import lvol_controller
        try:
            lvol_controller.delete_lvol(spare, force_delete=True)
            logger.info("Deleted spare landing volume %s of %s", spare.get_id(), lvol_id)
            deleted = True
        except Exception as e:                            # noqa: BLE001
            logger.warning("Could not delete spare landing volume %s of %s: %s",
                           spare.get_id(), lvol_id, e)
    return deleted


def _age_sec(lvol, now):
    try:
        return now - datetime.fromisoformat(lvol.create_dt).timestamp()
    except (TypeError, ValueError):
        return float("inf")         # no usable timestamp: the other guards decide


def _owners_with_pending_transfer():
    """Ids of the volumes with a replication transfer that has not settled on
    its landing volume yet: such a transfer may be adopting (renaming) the
    volume's spare right now. None when the tasks could not be read."""
    owners = set()
    try:
        for cluster in db.get_clusters():
            for t in db.get_job_tasks(cluster.get_id()):
                if t.function_name != JobSchedule.FN_SNAPSHOT_REPLICATION:
                    continue
                if t.status == JobSchedule.STATUS_DONE or t.canceled:
                    continue
                params = t.function_params or {}
                if params.get("remote_lvol_id") or not params.get("snapshot_id"):
                    continue
                try:
                    owners.add(db.get_snapshot_by_id(params["snapshot_id"]).lvol.get_id())
                except KeyError:
                    continue
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not read the replication tasks for the spare sweep: %s", e)
        return None
    return owners


def sweep(cluster_id, now=None):
    """Delete the spares on *cluster_id* whose volume no longer replicates.

    The safety net under :func:`discard`: whatever path ended a volume's
    replication without discarding its spare (or raced with its creation),
    the spare goes here. Kept: a spare whose volume still replicates (or
    cannot be read), one younger than SWEEP_MIN_AGE_SEC, and one whose volume
    has a transfer that may be adopting it right now. Returns the number of
    spares deleted.
    """
    now = time.time() if now is None else now
    try:
        candidates = [lv for lv in db.get_lvols(cluster_id)
                      if (lv.lvol_name or "").startswith(PREFIX)
                      and lv.status == LVol.STATUS_ONLINE]
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not list the volumes of %s for the spare sweep: %s",
                       cluster_id, e)
        return 0
    stale = []
    for spare in candidates:
        owner_id = spare.lvol_name[len(PREFIX):]
        if not owner_id or _owner_replicates(owner_id) is not False:
            continue
        if _age_sec(spare, now) < SWEEP_MIN_AGE_SEC:
            continue
        stale.append((spare, owner_id))
    if not stale:
        return 0
    adopting = _owners_with_pending_transfer()
    if adopting is None:
        return 0
    from simplyblock_core.controllers import lvol_controller
    deleted = 0
    for spare, owner_id in stale:
        if owner_id in adopting:
            logger.info("Spare landing volume %s of %s kept: a transfer of the volume "
                        "may be adopting it", spare.get_id(), owner_id)
            continue
        try:
            fresh = db.get_lvol_by_id(spare.get_id())
        except KeyError:
            continue
        # adopted (renamed) or already going since the listing
        if fresh.lvol_name != spare_name(owner_id) or fresh.status != LVol.STATUS_ONLINE:
            continue
        try:
            lvol_controller.delete_lvol(fresh, force_delete=True)
        except Exception as e:                            # noqa: BLE001
            logger.warning("Could not delete stale spare landing volume %s of %s: %s",
                           fresh.get_id(), owner_id, e)
            continue
        logger.warning("Deleted stale spare landing volume %s: its volume %s no longer "
                       "replicates", fresh.get_id(), owner_id)
        deleted += 1
    return deleted
