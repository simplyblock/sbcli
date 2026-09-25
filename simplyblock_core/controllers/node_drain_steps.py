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
    """Fail this node's data devices and rebuild them onto its peers."""
    from simplyblock_core import storage_node_ops

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
    node = DBController().get_storage_node_by_id(node_id)
    data_devices = [
        dev for dev in (node.nvme_devices or [])
        if dev.status != NVMeDevice.STATUS_JM
    ]
    total = len(data_devices)
    completed = sum(
        1 for dev in data_devices
        if dev.status == NVMeDevice.STATUS_FAILED_AND_MIGRATED
    )

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


def start_replica_reshuffle(node_id: str) -> bool:
    """Reallocate the replica roles other nodes still hold on this one."""
    from simplyblock_core import storage_node_ops

    def drive():
        node = DBController().get_storage_node_by_id(node_id)
        return storage_node_ops._relocate_replicas_hosted_on(node)

    return _spawn(node_id, 'reshuffle', drive)


def replica_reshuffle_progress(node_id: str) -> dict:
    """How many nodes still name this one as their secondary or tertiary.

    Counting the back-references rather than the relocations means the answer is
    "is anything still pointing here", which is the question the caller actually
    has before it deletes the node -- and it is the same answer whether a role
    was moved, was never there, or was cleaned up by something else.
    """
    db = DBController()
    node = db.get_storage_node_by_id(node_id)
    cluster_id = node.cluster_id

    holders = [
        peer for peer in db.get_storage_nodes_by_cluster_id(cluster_id)
        if peer.get_id() != node_id
        and node_id in (peer.secondary_node_id, peer.tertiary_node_id)
    ]

    error = _last_error.get((node_id, 'reshuffle'))
    running = _is_running(node_id, 'reshuffle')

    return {
        'done': not holders and not running,
        # Total is what still had to move when asked, so completed counts down
        # to it; there is no stored "before" to measure against, and inventing
        # one would go stale the moment anything else touched the layout.
        'total': len(holders),
        'completed': 0 if holders else 1,
        'failed': 1 if error else 0,
        'message': error or (
            f"{len(holders)} node(s) still hold a replica role here: "
            + ', '.join(peer.get_id() for peer in holders)
            if holders else 'no replica roles remain on this node'
        ),
    }


def _reset_for_test() -> None:
    """Drop the in-process step state. Tests only."""
    with _running_lock:
        _running.clear()
    _last_error.clear()


__all__ = [
    'start_device_decommission',
    'device_decommission_progress',
    'start_replica_reshuffle',
    'replica_reshuffle_progress',
]
