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
        if find(lvol_id, remote_lv.pool_uuid) is not None:
            return None
        remote_node = db.get_storage_node_by_id(remote_lv.node_id)
        lv_id, err = create_landing(spare_name(lvol_id), snapshot, remote_node, remote_lv.pool_uuid)
        if not lv_id:
            logger.warning("Could not pre-stage a spare landing volume for %s: %s", lvol_id, err)
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
