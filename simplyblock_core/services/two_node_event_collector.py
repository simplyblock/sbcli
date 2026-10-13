"""Two-node HA event collector (docs/design/two-node-arbitration.md, section 4).

One thread per storage node of every cluster with ``two_node_arbitration`` on,
long-polling ``jc_wait_events`` and queuing each event for the arbiter
(``arbitration.events.queue_event``) as an ``ArbitrationEvent`` in FDB. The
connection is always opened by the control plane; nodes never call back.
"""

import threading
import time

from simplyblock_core import constants, db_controller, utils
from simplyblock_core.arbitration import events as arbitration_events
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.rpc_client import RPCException

utils.init_sentry_sdk()
logger = utils.get_logger(__name__)

db = db_controller.DBController()

#: Backoff on a transport error: starts here, doubles, caps at BACKOFF_MAX_S.
BACKOFF_MIN_S = 0.1
BACKOFF_MAX_S = 5.0

#: Synthetic event carrying a full jc_ha_status, queued on resync.
STATUS_RESYNC = "ha_resync"


def _resync(client, cluster_id: str, node_id: str, reported_instance: str, reason: str) -> tuple[str, int]:
    """Node restart (instance change) or a dropped event (overflow): fetch the
    full ``jc_ha_status`` and queue it as one ``ha_resync`` event, so the
    arbiter's view is replaced rather than built back up one event at a time.
    Returns the ``(instance, seq)`` to continue the long-poll from.
    """
    status = client.jc_ha_status() or {}
    instance = str(status.get("instance") or reported_instance)
    seq = int(status.get("seq", 0))
    payload = {"status": STATUS_RESYNC, "instance": instance, "seq": seq,
               "reason": reason, "ha_status": status}
    arbitration_events.queue_event(
        db, cluster_id, node_id, instance, seq, payload, int(time.time() * 1000))
    logger.info(f"Node {node_id} resynced ({reason}): instance={instance} seq={seq}")
    return instance, seq


def _poll_once(client, cluster_id: str, node_id: str, instance: str, after_seq: int) -> tuple[str, int]:
    """One jc_wait_events call; queues what it returns. Raises on a transport
    error so the caller can back off and retry with the same (instance,
    after_seq) -- at-least-once delivery, contract section 3.7."""
    res = client.jc_wait_events(
        instance, after_seq,
        timeout_ms=constants.TWO_NODE_WAIT_TIMEOUT_MS,
        max_events=constants.TWO_NODE_COLLECTOR_MAX_EVENTS) or {}

    if res.get("superseded"):
        # Another waiter took over (e.g. this collector restarted elsewhere).
        return instance, after_seq

    res_instance = str(res.get("instance", ""))
    overflow = bool(res.get("overflow"))
    if res_instance != instance or overflow:
        reason = "overflow" if overflow and res_instance == instance else "instance"
        return _resync(client, cluster_id, node_id, res_instance, reason)

    events = res.get("events") or []
    received_at = int(time.time() * 1000)
    for event in events:
        seq = int(event.get("seq", 0))
        if seq <= after_seq:
            continue
        event.setdefault("instance", instance)
        arbitration_events.queue_event(db, cluster_id, node_id, instance, seq, event, received_at)
        after_seq = seq

    reported_seq = int(res.get("seq", 0))
    if reported_seq > after_seq and not events:
        # An empty answer reports the node's current seq: nothing newer is pending.
        after_seq = reported_seq
    return instance, after_seq


def _collect(node_id: str, cluster_id: str, stop: threading.Event) -> None:
    logger.info(f"Starting two-node event collector on node: {node_id}")
    instance, after_seq = "", 0
    backoff = BACKOFF_MIN_S
    while not stop.is_set():
        try:
            snode = db.get_storage_node_by_id(node_id)
        except KeyError:
            logger.info(f"Node {node_id} deleted; stopping two-node event collector")
            return
        if snode.status == StorageNode.STATUS_REMOVED:
            logger.info(f"Node {node_id} removed; stopping two-node event collector")
            return

        try:
            client = snode.rpc_client(retry=1)
            instance, after_seq = _poll_once(client, cluster_id, node_id, instance, after_seq)
            backoff = BACKOFF_MIN_S
        except RPCException as e:
            logger.warning(f"jc_wait_events failed on {node_id}: {e}; retry in {backoff:.1f}s")
            stop.wait(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_S)

    logger.info(f"Stopping two-node event collector on node: {node_id}")


#: node_id -> (thread, stop event) for every running collector.
_workers: dict[str, tuple[threading.Thread, threading.Event]] = {}


def _reconcile() -> None:
    """Start a collector thread per node of every arbitrated cluster; stop any
    whose node is gone, removed, or no longer in an arbitrated cluster."""
    want: dict[str, str] = {}
    try:
        clusters = [c for c in db.get_clusters() if c.two_node_arbitration]
    except Exception as e:
        logger.error(f"Failed to get clusters: {e}")
        return
    for cluster in clusters:
        cluster_id = cluster.get_id()
        try:
            nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
        except Exception as e:
            logger.error(f"Failed to get nodes for cluster {cluster_id}: {e}")
            continue
        for snode in nodes:
            if snode.status == StorageNode.STATUS_REMOVED or not snode.mgmt_ip or not snode.rpc_port:
                continue
            want[snode.get_id()] = cluster_id

    for node_id in [n for n in _workers if n not in want]:
        thread, stop = _workers.pop(node_id)
        stop.set()
        thread.join(timeout=constants.TWO_NODE_WAIT_TIMEOUT_MS / 1000.0 + 15)
        logger.info(f"Stopped two-node event collector for node: {node_id}")

    for node_id, cluster_id in want.items():
        existing = _workers.get(node_id)
        if existing is not None and existing[0].is_alive():
            continue
        stop = threading.Event()
        thread = threading.Thread(
            target=_collect, args=(node_id, cluster_id, stop), daemon=True,
            name=f"two-node-collector-{node_id}")
        thread.start()
        _workers[node_id] = (thread, stop)


def main() -> None:
    logger.info("Two-node event collector started")
    while True:
        _reconcile()
        time.sleep(constants.TWO_NODE_COLLECTOR_RECONCILE_S)


if __name__ == "__main__":
    main()
