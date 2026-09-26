"""The two node-removal phases a Kubernetes drain drives on its own.

``remove_storage_node`` runs the whole removal as one call: shutdown, own
replicas, JM, hosted replicas, verify, finalize, devices. That is the right shape
for ``sbctl sn remove``, and the wrong one for an operator, which has to report
progress and fail a step at a time. Under Kubernetes the drain is the
StorageNodeOps CR, and it needs the device rebuild and the replica reallocation
as separate, restartable steps with something to poll.

Both steps are the same phase functions ``remove_storage_node`` already calls,
driven here to completion in a worker thread:

* ``_decommission_node_devices`` fails every data device and waits for the
  rebuild onto peers. It skips devices already ``failed_and_migrated``, so
  re-driving it is how it makes progress rather than something to guard against.
* ``_relocate_replicas_hosted_on`` reallocates the lvstore replica roles that
  other nodes keep on this one.

Starting either twice is a no-op: a step already running is left alone, and a
step already finished answers from the database. That is what lets a controller
which restarts mid-drain re-POST without restarting the work it is waiting for.
"""

import logging
import threading
import time
from typing import Dict, Tuple

from simplyblock_core.db_controller import DBController
from simplyblock_core.models.nvme_device import NVMeDevice


logger = logging.getLogger()

# How long a pass that reports "still migrating" waits before being re-driven.
# The device rebuild moves real data, so this is a coarse poll, not a spin.
_RETRY_INTERVAL_SEC = 10.0

# An upper bound on a single step, so a rebuild that can never finish ends as a
# failed step with a message rather than a thread that lives as long as the
# process does.
_STEP_DEADLINE_SEC = 4 * 60 * 60

#: node_id -> (thread, step_name). Guarded by ``_running_lock``.
_running: Dict[Tuple[str, str], threading.Thread] = {}
_running_lock = threading.Lock()

#: (node_id, step) -> message, for the last failure a step ended with.
_last_error: Dict[Tuple[str, str], str] = {}


def _spawn(node_id: str, step: str, drive) -> bool:
    """Start ``drive`` for one step of one node, unless it is already running.

    Returns True if this call started it, False if it was already in flight --
    both are success for the caller, because the work is under way either way.
    """
    key = (node_id, step)
    with _running_lock:
        existing = _running.get(key)
        if existing is not None and existing.is_alive():
            logger.info(f"[DRAIN] {node_id}: {step} already running, not starting a second pass")
            return False
        _last_error.pop(key, None)

        def run():
            deadline = time.time() + _STEP_DEADLINE_SEC
            try:
                while time.time() < deadline:
                    try:
                        if drive():
                            logger.info(f"[DRAIN] {node_id}: {step} complete")
                            return
                    except Exception as e:
                        # Recorded rather than raised: this runs on a worker
                        # thread nobody joins, so an exception that escapes is
                        # a step that stops with the poller none the wiser.
                        logger.exception(f"[DRAIN] {node_id}: {step} raised")
                        _last_error[key] = str(e)
                        return
                    time.sleep(_RETRY_INTERVAL_SEC)
                _last_error[key] = f"{step} did not finish within {_STEP_DEADLINE_SEC}s"
                logger.error(f"[DRAIN] {node_id}: {_last_error[key]}")
            finally:
                with _running_lock:
                    if _running.get(key) is threading.current_thread():
                        del _running[key]

        thread = threading.Thread(target=run, name=f"drain-{step}-{node_id[:8]}", daemon=True)
        _running[key] = thread
        thread.start()
        logger.info(f"[DRAIN] {node_id}: {step} started")
        return True


def _is_running(node_id: str, step: str) -> bool:
    with _running_lock:
        thread = _running.get((node_id, step))
        return thread is not None and thread.is_alive()


def start_device_decommission(node_id: str) -> bool:
    """Shut the node down, then rebuild its data devices onto its peers.

    Shutdown comes first, as it does in the CLI removal: a node being removed
    stops serving before anything is moved, and its volumes are served by their
    replicas for the rest of the drain. Both removal paths therefore mean the
    same thing by MIGRATING_DEVICES -- the node is down -- and every check that
    asks "can this node still answer?" gets one answer whichever path is
    driving. When the two disagreed, each such check had to be found the hard
    way, on a live cluster, one at a time.

    The stamp also does the job it always did: failing a device queues a rebuild
    task against every node whose distribs reference it, and the runner will not
    run one on a node that is not ONLINE, so a task queued against this node
    retries for ever while its device never reaches failed_and_migrated. The
    status is what tells the queuing side to skip this node and put its
    distribs' work on a replica peer instead.

    Idempotent: a node already shut down by an earlier pass is left alone, so
    re-POSTing the step does not try to stop it twice.
    """
    from simplyblock_core import storage_node_ops
    from simplyblock_core.models.storage_node import StorageNode

    node = DBController().get_storage_node_by_id(node_id)
    # The same predicate remove_storage_node's phase 1 uses: shut down only a
    # node that is actually still running. A node already stopped -- by the
    # operator's own ShuttingDown step, or by an earlier pass of this one, or
    # because it went offline by itself -- is left alone, so the two owners of
    # the shutdown cannot fight over it.
    if node.status in (StorageNode.STATUS_ONLINE,) + StorageNode.DRAINING_STATUSES:
        logger.info(f"[DRAIN] {node_id}: shutting the node down before the drain")
        ret = storage_node_ops.shutdown_storage_node(node_id, force=True)
        if isinstance(ret, tuple):
            ret, reason = ret
            if not ret:
                logger.error(f"[DRAIN] {node_id}: shutdown failed: {reason}")
                return False
        elif not ret:
            logger.error(f"[DRAIN] {node_id}: shutdown failed")
            return False

    # Stamped outside the branch above: the node may already be stopped -- the
    # operator has its own ShuttingDown step -- and it still has to carry the
    # status that says which half of the drain is running, or the queuing side
    # has nothing to skip on and `sbctl sn list` shows a removal that could be
    # anywhere.
    node = DBController().get_storage_node_by_id(node_id)
    if node.status != StorageNode.STATUS_MIGRATING_DEVICES:
        storage_node_ops.set_node_status(
            node_id, StorageNode.STATUS_MIGRATING_DEVICES, caused_by="drain")
        logger.info(f"[DRAIN] {node_id}: marked migrating_devices")

    def drive():
        node = DBController().get_storage_node_by_id(node_id)
        return storage_node_ops._decommission_node_devices(node)

    return _spawn(node_id, 'devices', drive)


def device_decommission_progress(node_id: str) -> dict:
    """Where the device rebuild has got to, counted from the devices themselves.

    The device records are the truth here rather than the migration tasks: a
    device reaches ``failed_and_migrated`` only once its data is rebuilt, which
    is the thing the caller is waiting for, and it stays that way across a
    control-plane restart that would lose any in-memory count.
    """
    from simplyblock_core import storage_node_ops

    node = DBController().get_storage_node_by_id(node_id)
    data_devices = [
        dev for dev in (node.nvme_devices or [])
        if dev.status != NVMeDevice.STATUS_JM
    ]
    total = len(data_devices)
    # Counted as "not pending" rather than "== failed_and_migrated" so this and
    # the removal's phase-5 skip agree by construction: they are the same
    # predicate, and a device state that one treats as done must not be one the
    # other still waits for.
    pending = storage_node_ops.data_devices_pending_migration(node)
    completed = total - len(pending)

    error = _last_error.get((node_id, 'devices'))
    running = _is_running(node_id, 'devices')

    return {
        'done': completed == total and not running,
        'total': total,
        'completed': completed,
        # A step that gave up is reported as one failure rather than a count of
        # devices: which device stalled is in the message, and the caller's only
        # decision is whether to carry on, which a single non-zero already
        # settles.
        'failed': 1 if error else 0,
        'message': error or (
            f"{completed} of {total} devices rebuilt onto peers" if total else 'no data devices'
        ),
    }


def mark_migrating_lvols(node_id: str) -> bool:
    """Record that the drain has moved on from devices to volumes.

    The CLI removal stamps both halves itself, because it runs both. The
    Kubernetes drain runs only the device half through this module -- the
    volume half is the operator creating VolumeMigration CRs -- so without
    this the node would sit in MIGRATING_DEVICES for the whole of a phase it
    finished long ago, and `sbctl sn list` would disagree with the CR about
    which step a removal is on.

    Idempotent, and never moves a node backwards: a removal that has already
    reached IN_REMOVAL or beyond keeps the status it has.
    """
    from simplyblock_core import storage_node_ops
    from simplyblock_core.models.storage_node import StorageNode

    node = DBController().get_storage_node_by_id(node_id)
    if node.status == StorageNode.STATUS_MIGRATING_LVOLS:
        return True
    if node.status in StorageNode.REMOVAL_SHUT_DOWN_STATUSES and \
            node.status != StorageNode.STATUS_MIGRATING_DEVICES:
        logger.info(f"[DRAIN] {node_id}: past the volume phase already ({node.status}); "
                    f"not stamping migrating_lvols")
        return True

    storage_node_ops.set_node_status(
        node_id, StorageNode.STATUS_MIGRATING_LVOLS, caused_by="drain")
    logger.info(f"[DRAIN] {node_id}: marked migrating_lvols")
    return True


# Replica-role reallocation deliberately has no step here.
#
# It is phase 3b of the control plane's removal, and it has a precondition the
# drain cannot meet: phase 3a frees the departing node's OWN replica slots
# first, and 3b needs those slots to have anywhere to move into. Exposed as a
# drain step it ran without 3a, so on a cluster whose replica slots are all
# occupied it found no free slot, walked the ring of occupants and refused on a
# cycle -- and the step retried that for four hours (2026-09-26, a 7-node FTT2
# cluster where every node was the next one's secondary).
#
# The removal owns that ordering, so the drain hands the node to it: once the
# volumes are off, the node is deleted and remove_storage_node does 3a then 3b
# exactly as it always did.


def _reset_for_test() -> None:
    """Drop the in-process step state. Tests only."""
    with _running_lock:
        _running.clear()
    _last_error.clear()


__all__ = [
    'start_device_decommission',
    'mark_migrating_lvols',
    'device_decommission_progress',
]
