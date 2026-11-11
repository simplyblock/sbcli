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
when the volume leaves its policy or its cutover ends.
"""
import logging

from simplyblock_core import db_controller
from simplyblock_core.models.lvol_model import LVol

logger = logging.getLogger(__name__)
db = db_controller.DBController()

PREFIX = "REP_spare_"


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


def prestage(snapshot, remote_lv, create_landing):
    """Create the volume's spare on the destination of *remote_lv* unless one
    exists. *create_landing(name, snapshot, remote_node, pool_uuid)* is the
    runner's landing-volume factory: (lvol id, error). Best effort."""
    lvol_id = snapshot.lvol.get_id()
    try:
        # A transfer still draining after its volume's cutover (or after the
        # volume left its policy) must not create a spare the discard already
        # ran for: nothing would ever delete it (bed bf21 2026-11-10: 15-21
        # REP_spare_* per cluster for 8 volumes).
        if not _replicating(lvol_id):
            return None
        if find(lvol_id, remote_lv.pool_uuid) is not None:
            return None
        remote_node = db.get_storage_node_by_id(remote_lv.node_id)
        lv_id, err = create_landing(spare_name(lvol_id), snapshot, remote_node, remote_lv.pool_uuid)
        if not lv_id:
            logger.warning("Could not pre-stage a spare landing volume for %s: %s", lvol_id, err)
            return None
        if not _replicating(lvol_id):
            # stopped while the spare was created: the discard may have run
            logger.info("Volume %s stopped replicating while its spare was created; "
                        "discarding it", lvol_id)
            discard(lvol_id, remote_lv.pool_uuid)
            return None
        logger.info("Pre-staged spare landing volume %s for %s on %s", lv_id, lvol_id,
                    remote_node.get_id())
        return lv_id
    except Exception as e:                                # noqa: BLE001
        logger.warning("Pre-staging a spare landing volume for %s failed: %s", lvol_id, e)
        return None


def discard(lvol_id, pool_uuid=None):
    """Delete the volume's spare, if any. Best effort; returns True when one
    was deleted."""
    spare = find(lvol_id, pool_uuid)
    if spare is None:
        return False
    if spare.status == LVol.STATUS_IN_DELETION:
        return False
    from simplyblock_core.controllers import lvol_controller
    try:
        lvol_controller.delete_lvol(spare, force_delete=True)
        logger.info("Deleted spare landing volume %s of %s", spare.get_id(), lvol_id)
        return True
    except Exception as e:                                # noqa: BLE001
        logger.warning("Could not delete spare landing volume %s of %s: %s",
                       spare.get_id(), lvol_id, e)
        return False


def _replicating(lvol_id):
    """Whether *lvol_id* still replicates on its cadence (and so may need a
    spare). False once its cutover stopped it (do_replicate cleared), when it
    is being deleted or gone."""
    try:
        lvol = db.get_lvol_by_id(lvol_id)
    except KeyError:
        return False
    return bool(lvol.do_replicate) and lvol.status != LVol.STATUS_IN_DELETION


def sweep(cluster_id):
    """Discard the spares on *cluster_id* whose volume no longer replicates.

    A spare is named after its source volume and lives on the destination,
    so the volume's own clean-up paths (cutover end, policy detach) are the
    only ones that know about it; one they missed -- a spare created by a
    transfer that drained after them, or by a control plane without that
    check -- stayed for good (bed bf21 2026-11-10: 15-21 per cluster for 8
    volumes). Returns how many were deleted."""
    deleted = 0
    for lv in db.get_lvols(cluster_id):
        name = getattr(lv, "lvol_name", "") or ""
        if not name.startswith(PREFIX) or lv.status != LVol.STATUS_ONLINE:
            continue
        owner = name[len(PREFIX):]
        if _replicating(owner):
            continue
        logger.info("Spare landing volume %s (%s) belongs to %s, which no longer "
                    "replicates; discarding it", lv.get_id(), name, owner)
        if discard(owner, lv.pool_uuid):
            deleted += 1
    return deleted
