# coding=utf-8
import threading
import time
from datetime import datetime

from simplyblock_core import constants, db_controller, utils, distr_controller
from simplyblock_core.controllers import events_controller, device_controller
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode


utils.init_sentry_sdk()
logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()

EVENTS_LIST = ['SPDK_BDEV_EVENT_REMOVE', "error_open", 'error_read', "error_write", "error_unmap",
               "error_write_cannot_allocate"]

# A nominally "healthy" remote NVMe controller normally earns its device a pass
# on an IO-timeout error event (the error is assumed transient). A controller can
# however stay "connected" while its IO keeps timing out (wedged/slow device,
# stuck or duplicate qpair), producing an endless timeout/reset loop with the
# device never excluded (incident 2026-06-24: storage_id 31 skipped 38x while the
# distrib kept using it). To escalate without over-reacting to a single node's
# bad path, we require a QUORUM: a majority of the online consumer nodes must
# EACH have timed out to the device >= REMOTE_IO_TIMEOUT_LIMIT times (recently)
# before the device is forced globally UNAVAILABLE.
REMOTE_IO_TIMEOUT_LIMIT = 1          # per-node IO-error reports before that node "votes" the device bad
# One ~8s IO-timeout cycle reports a batch (read/write/unmap within ~200ms);
# debounce so a single batch advances a node's count once.
REMOTE_IO_TIMEOUT_DEBOUNCE_SEC = 6.0
# Reset/validity window: a node's vote only counts toward quorum if it reported
# an IO error this recently. If a quorum is not reached while votes are live they
# expire and the accumulation resets — so sporadic, non-concurrent errors spread
# over a long period never add up to an exclusion.
REMOTE_IO_VOTE_VALIDITY_SEC = 300.0  # 5 min
_REMOTE_IO_TIMEOUT_MESSAGES = ('error_read', 'error_write', 'error_unmap')

# Two devices on the SAME home node reach IO-error quorum a few seconds apart:
# their events are processed on independent per-node collector threads, and the
# first device to trip clears its own votes on force (below). So the raw
# "does a sibling also have quorum right now" check is racy -- when device A trips
# it may not yet see device B's quorum, and once A forces+clears, B (tripping a
# moment later) can no longer see A's. To recognize the node outage regardless of
# ordering, we ALSO remember which devices were just quorum-forced: if a sibling
# was force-marked within this window, the current trip is node-wide, not a bad
# device. Must comfortably exceed the inter-sibling trip gap (observed ~2s).
SIBLING_FORCE_EVIDENCE_SEC = 30.0

# Shared across the per-node collector threads (one process). Keyed by
# (event_node_id, device_id) -> (count, last_io_error_ts).
_remote_timeout_lock = threading.Lock()
_remote_timeout_votes: dict = {}

# device_id -> wall-clock ts of the last remote-IO-quorum force. Lets a sibling
# tripping shortly after recognize a node-wide outage even though the first
# device's votes were cleared on force.
_recent_quorum_force_lock = threading.Lock()
_recent_quorum_forced: dict = {}


def _record_remote_timeout(event_node_id, device_id):
    """Debounced per-(reporting-node, device) IO-error vote counter. Also purges
    votes older than the validity window so the quorum accumulation resets once a
    device stops erroring."""
    now = time.time()
    with _remote_timeout_lock:
        cnt, tsc = _remote_timeout_votes.get((event_node_id, device_id), (0, 0.0))
        if now - tsc > REMOTE_IO_TIMEOUT_DEBOUNCE_SEC:
            cnt += 1
        _remote_timeout_votes[(event_node_id, device_id)] = (cnt, now)
        stale = [k for k, (_, t) in _remote_timeout_votes.items()
                 if now - t > REMOTE_IO_VOTE_VALIDITY_SEC]
        for k in stale:
            del _remote_timeout_votes[k]


def _remote_timeout_quorum_reached(device_id, other_nodes):
    """Quorum check over the cluster's nodes other than the device's home node.

    other_nodes: list of (node_id, is_online) for every cluster node except the
    device's home node (removed nodes already filtered out by the caller).

    A node is a VOTER if it has recently timed out to this device
    >= REMOTE_IO_TIMEOUT_LIMIT times -- even if it has since gone offline: a node
    that already decided the device is bad keeps its vote (and stays in the
    denominator). A node counts toward the quorum DENOMINATOR if it is either
    currently online or already a voter; an offline node that never reached the
    limit counts toward neither. Quorum = N // 2 + 1 over that denominator
    (always >= 1; if the denominator is 0 -- e.g. only the home node is online and
    nobody has voted -- no quorum is possible and the device is not excluded).
    """
    now = time.time()
    denom = 0
    voters = 0
    with _remote_timeout_lock:
        for node_id, is_online in other_nodes:
            cnt, tsc = _remote_timeout_votes.get((node_id, device_id), (0, 0.0))
            has_vote = cnt >= REMOTE_IO_TIMEOUT_LIMIT and now - tsc <= REMOTE_IO_VOTE_VALIDITY_SEC
            if has_vote:
                voters += 1
                denom += 1
            elif is_online:
                denom += 1
    if denom <= 0:
        return False
    quorum = denom // 2 + 1
    return voters >= quorum


def _clear_remote_timeout_votes(device_id):
    with _remote_timeout_lock:
        for key in [k for k in _remote_timeout_votes if k[1] == device_id]:
            del _remote_timeout_votes[key]


def _record_quorum_force(device_id):
    with _recent_quorum_force_lock:
        _recent_quorum_forced[device_id] = time.time()


def _recently_quorum_forced(device_id):
    """True if this device was force-marked unavailable by the remote-IO quorum
    within the recent evidence window (used as node-outage evidence for a sibling
    tripping shortly after)."""
    with _recent_quorum_force_lock:
        ts = _recent_quorum_forced.get(device_id, 0.0)
        return (time.time() - ts) <= SIBLING_FORCE_EVIDENCE_SEC


def _clear_quorum_force(device_id):
    with _recent_quorum_force_lock:
        _recent_quorum_forced.pop(device_id, None)


def _readmit_racing_forced_siblings(siblings, logger):
    """Undo a quorum-force on a sibling that tripped moments earlier: once we know
    the outage is node-wide, a sibling force-marked in the race was wrong. Only
    re-admit devices forced by the quorum (status=UNAVAILABLE, io_error=False,
    not retries_exhausted) -- a genuine local failure sets io_error and is left to
    the device-restart/removal path."""
    for d in siblings:
        if not _recently_quorum_forced(d.get_id()):
            continue
        try:
            fresh = db.get_storage_device_by_id(d.get_id())
        except KeyError:
            _clear_quorum_force(d.get_id())
            continue
        if (fresh.status == NVMeDevice.STATUS_UNAVAILABLE
                and not fresh.io_error and not fresh.retries_exhausted):
            logger.warning(
                f"Re-admitting sibling device {d.get_id()} that was quorum-forced "
                f"in the node-outage race")
            device_controller.device_set_online(d.get_id())
        _clear_quorum_force(d.get_id())


def _get_target_remote_device(node_obj, device_id):
    fresh = db.get_storage_node_by_id(node_obj.get_id())
    for rem_dev in fresh.remote_devices:
        if rem_dev.get_id() == device_id:
            return rem_dev
    return None


def _is_target_remote_controller_healthy(device_obj, event_node_obj):
    remote_dev = _get_target_remote_device(event_node_obj, device_obj.get_id())
    remote_bdev = None
    if remote_dev and remote_dev.remote_bdev:
        remote_bdev = remote_dev.remote_bdev
    else:
        remote_bdev = f"remote_{device_obj.alceml_bdev}n1"

    ctrl_name = remote_bdev[:-2] if remote_bdev.endswith("n1") else remote_bdev
    ret, err = event_node_obj.rpc_client().bdev_nvme_controller_list_2(ctrl_name)
    if not ret:
        return False

    ctrlrs = ret[0].get("ctrlrs", []) if ret else []
    if not ctrlrs:
        return False

    bad_states = {"failed", "deleting", "resetting", "reconnect_is_delayed"}
    healthy = False
    for controller in ctrlrs:
        controller_state = controller.get("state", "")
        if controller_state not in bad_states:
            healthy = True
            break

    if not healthy:
        return False

    return bool(event_node_obj.rpc_client().get_bdevs(remote_bdev))


def remove_remote_device_from_node(node_id, device_id):
    # Re-read node immediately before write to avoid overwriting concurrent changes
    # (e.g. lvstore_ports set during cluster activation)
    node = db.get_storage_node_by_id(node_id)
    updated_devices = [d for d in node.remote_devices if d.get_id() != device_id]
    if len(updated_devices) != len(node.remote_devices):
        fresh = db.get_storage_node_by_id(node_id)
        fresh.remote_devices = [d for d in fresh.remote_devices if d.get_id() != device_id]
        fresh.write_to_db()


def process_device_event(event, logger):
    if event.message in EVENTS_LIST:
        node_id = event.node_id
        storage_id = event.storage_id
        event_node_obj = db.get_storage_node_by_id(node_id)

        device_obj = None
        device_node_obj = None
        for node in db.get_storage_nodes():
            for dev in node.nvme_devices:
                if dev.cluster_device_order == storage_id:
                    device_obj = dev
                    device_node_obj = node
                    break

        if device_obj is None or device_node_obj is None:
            logger.info(f"Device not found!, storage id: {storage_id} from node: {node_id}")
            event.status = 'device_not_found'
            return

        if "timestamp" in event.object_dict:
            ev_time = event.object_dict['timestamp']
            time_delta = datetime.now() - datetime.strptime(ev_time, '%Y-%m-%dT%H:%M:%S.%fZ')
            if time_delta.total_seconds() > 8:
                if _is_target_remote_controller_healthy(device_obj, event_node_obj):
                    logger.info(f"event was fired {time_delta.total_seconds()} seconds ago, target remote controller ok, skipping")
                    event.status = f'skipping_late_by_{int(time_delta.total_seconds())}s_but_controller_ok'
                    return
                ret, err = event_node_obj.rpc_client().bdev_nvme_controller_list_2(device_obj.nvme_controller)
                if err and err['code'] == 22:
                    logger.info(f"event was fired {time_delta.total_seconds()} seconds ago, checking controller filed")
                    event.status = f'late_by_{int(time_delta.total_seconds())}s'
                else:
                    logger.info(f"event was fired {time_delta.total_seconds()} seconds ago, error checking controller: {err}, skipping")
                    event.status = f'late_by_{int(time_delta.total_seconds())}s_skipping'
                    return

        if device_obj.is_connection_in_progress_to_node(event_node_obj.get_id()):
            logger.warning("Connection attempt was found from node to device, sleeping 5 seconds")
            time.sleep(5)

        device_obj.lock_device_connection(event_node_obj.get_id())
        is_remote = device_node_obj.get_id() != event_node_obj.get_id()

        if is_remote:
            # Record a per-(reporting node, device) IO-error vote, but only while
            # the device's HOME node is ONLINE: an IO error against a device whose
            # node is offline/down is an expected node-outage effect (handled by
            # the node-status paths below), not evidence the device itself is bad.
            if (event.message in _REMOTE_IO_TIMEOUT_MESSAGES
                    and device_node_obj.status == StorageNode.STATUS_ONLINE):
                _record_remote_timeout(event_node_obj.get_id(), device_obj.get_id())

            # Quorum over all cluster nodes except the device's home node. Each
            # node carries its online flag; the helper counts a node toward the
            # denominator if it is online OR already voted, and as a voter once it
            # has reached REMOTE_IO_TIMEOUT_LIMIT recent IO errors (a vote survives
            # the voter going offline). Quorum = N//2+1.
            other_nodes = [
                (n.get_id(), n.status == StorageNode.STATUS_ONLINE)
                for n in db.get_storage_nodes_by_cluster_id(device_obj.cluster_id)
                if n.get_id() != device_node_obj.get_id() and n.status != StorageNode.STATUS_REMOVED
            ]
            if _remote_timeout_quorum_reached(device_obj.get_id(), other_nodes):
                # Node-outage vs. bad-device disambiguation. During ANY node outage
                # (graceful / forced / container_kill / host_reboot / network) the
                # peers time out on EVERY one of that node's devices in the same
                # IO-timeout cycle -- so a sibling device on the same home node also
                # reaches quorum. A genuine single-device fault trips only its own
                # device. This signal is evaluated from the SAME in-memory vote
                # store that just tripped, so it is true at the instant of the trip
                # (unlike node.status or the peers' JM-controller state, both of
                # which lag the device IO-error quorum by a second or more and left
                # the earlier guard firing 0/N times). If a sibling also reached
                # quorum, treat this as a node outage and do NOT force the device
                # globally unavailable -- the node-status paths handle exclusion and
                # the device is re-admitted when the node recovers/restarts.
                # The two siblings do NOT trip simultaneously: their events are
                # processed on independent per-node collector threads and cross the
                # quorum threshold a few seconds apart, and the first to trip clears
                # its own votes on force (below). So checking only "does a sibling
                # have quorum right now" is racy and misses the node outage in both
                # directions -- the first tripper doesn't yet see the second's
                # quorum, and once it forces+clears, the second can't see the first's
                # (this is why the guard fired 0/N while both siblings were forced).
                # We therefore also treat a sibling that was quorum-FORCED within the
                # recent window as node-outage evidence, and re-admit it: whichever
                # sibling tripped first was wrongly forced and must be undone now that
                # we know the outage is node-wide.
                siblings = [
                    d for d in device_node_obj.nvme_devices
                    if d.get_id() != device_obj.get_id()
                ]
                node_outage_sibling = next(
                    (d for d in siblings
                     if _remote_timeout_quorum_reached(d.get_id(), other_nodes)
                     or _recently_quorum_forced(d.get_id())),
                    None,
                )
                if node_outage_sibling is not None:
                    logger.warning(
                        f"IO-error quorum for storage_id {storage_id}, but sibling "
                        f"device {node_outage_sibling.get_id()} on home node "
                        f"{device_node_obj.get_id()} also reached quorum / was just "
                        f"force-marked -- treating as a NODE outage, NOT forcing "
                        f"device globally unavailable")
                    event.status = 'skipped:node_wide_io_quorum'
                    _readmit_racing_forced_siblings(siblings, logger)
                    device_obj.release_device_connection()
                    return
                # Only THIS device is failing across the cluster => the device itself
                # is bad: set it GLOBALLY unavailable, independent of what the remote
                # NVMe controller's state reports. Erasure coding compensates;
                # re-admitted on device/node restart or port_allow recovery.
                logger.warning(
                    f"IO-error quorum reached for storage_id {storage_id}; "
                    f"forcing device globally UNAVAILABLE")
                device_controller.device_set_unavailable(device_obj.get_id())
                _record_quorum_force(device_obj.get_id())
                _clear_remote_timeout_votes(device_obj.get_id())
                event.status = 'forced_unavailable:remote_io_quorum'
                device_obj.release_device_connection()
                return

            if _is_target_remote_controller_healthy(device_obj, event_node_obj):
                # No quorum and the remote controller still reports healthy => treat
                # the error as transient and skip (do not exclude).
                event.status = 'skipped:remote_controller_healthy'
                device_obj.release_device_connection()
                return
            # No quorum but the controller is NOT healthy => exclude the device
            # only in the context of THIS initiator node (per-node); the remote
            # branch below sends the UNAVAILABLE status event to this node and
            # drops it from this node's remote_devices.

        if device_obj.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                     NVMeDevice.STATUS_CANNOT_ALLOCATE]:
            logger.info(f"The device is not online, skipping. status: {device_obj.status}")
            event.status = f'skipped:dev_{device_obj.status}'
            distr_controller.send_dev_status_event(device_obj, device_obj.status, event_node_obj)
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return


        if event_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            logger.info(f"Node is not online, skipping. status: {event_node_obj.status}")
            event.status = 'skipped:node_offline'
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return

        if device_node_obj.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            logger.info(f"Node is not online, skipping. status: {device_node_obj.status}")
            event.status = f'skipped:device_node_{device_node_obj.status}'
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())
            device_obj.release_device_connection()
            return


        if device_node_obj.get_id() == event_node_obj.get_id():
            # The device's home node itself reported an IO error against its
            # local device — this is the only path that may count toward the
            # per-device flap budget. The CAUSE_LOCAL_FAILURE opt-in is
            # gated by the equality check above (event_node == device_node).
            if event.message in ['SPDK_BDEV_EVENT_REMOVE', 'error_open']:
                # Catastrophic-failure path: the local SPDK fired a REMOVE
                # (or error_open) on this device, which means either the
                # NVMe controller was destructed after timeout-driven reset
                # failures, or the underlying device was hot-removed, or
                # AER reported the namespace gone. From here we cannot tell
                # those apart — but for the flap counter all three are
                # legitimate per-device failure events from the home node,
                # so we count them. (CLI-initiated removes use the default
                # cause and are not counted.)
                logger.info(f"Removing storage id: {storage_id} from node: {node_id}")
                device_controller.device_remove(
                    device_obj.get_id(),
                    cause=device_controller.CAUSE_LOCAL_FAILURE,
                )

            elif event.message in ['error_write', 'error_unmap']:
                logger.info("Setting device to read-only")
                device_controller.device_set_read_only(
                    device_obj.get_id(),
                    cause=device_controller.CAUSE_LOCAL_FAILURE,
                )

            elif event.message == 'error_write_cannot_allocate':
                logger.info("Setting device to cannot_allocate")
                device_controller.device_set_state(
                    device_obj.get_id(),
                    NVMeDevice.STATUS_CANNOT_ALLOCATE,
                    cause=device_controller.CAUSE_LOCAL_FAILURE,
                )

            else:
                logger.info("Setting device to unavailable")
                device_controller.device_set_unavailable(
                    device_obj.get_id(),
                    cause=device_controller.CAUSE_LOCAL_FAILURE,
                )
                device_controller.device_set_io_error(device_obj.get_id(), True)
        else:
            distr_controller.send_dev_status_event(device_obj, NVMeDevice.STATUS_UNAVAILABLE, event_node_obj)
            remove_remote_device_from_node(event_node_obj.get_id(), device_obj.get_id())

        event.status = 'processed'
        device_obj.release_device_connection()


def process_lvol_event(event, logger):
    if event.message in ["error_open", 'error_read', "error_write", "error_unmap"]:
        vuid = event.object_dict['vuid']
        event.status = f'distr error {vuid}'
    else:
        logger.error(f"Unknown event message: {event.message}")
        event.status = "event_unknown"


def process_event(event, logger):
    if event.event == "device_status":
        if event.storage_id >= 0:
            process_device_event(event, logger)

        if event.vuid >= 0:
            process_lvol_event(event, logger)

    event.write_to_db(db.kv_store)


def start_event_collector_on_node(node_id):
    snode = db.get_storage_node_by_id(node_id)

    logger.info(f"Starting Distr event collector on node: {node_id}")

    client = snode.rpc_client(timeout=2, retry=2)

    try:
        while True:
            page = 1
            events_groups = {}
            events_list = []
            while True:
                try:
                    events = client.distr_status_events_discard_then_get(
                        0, constants.DISTR_EVENT_COLLECTOR_NUM_OF_EVENTS * page)
                    if events is False:
                        logger.error("No events received")
                        return

                    if events:
                        logger.info(f"Found events: {len(events)}")
                        for event_dict in events:
                            if "storage_ID" in event_dict:
                                sid = event_dict['storage_ID']
                            elif "vuid" in event_dict:
                                sid = event_dict['vuid']
                            else:
                                logger.error(f"Unknown event: {event_dict}")
                                continue

                            # Ignore type errors, this can be simplified to avoid them
                            et = event_dict['event_type']
                            msg = event_dict['status']
                            if sid not in events_groups:
                                events_groups[sid] = {et:{msg: 1}}
                            elif et not in events_groups[sid]:
                                events_groups[sid][et]: {msg: 1}  # type: ignore
                            elif msg not in events_groups[sid][et]:
                                events_groups[sid][et][msg]: 1  # type: ignore
                            else:
                                events_groups[sid][et][msg].count += 1  # type: ignore
                                continue

                            event = events_controller.log_distr_event(snode.cluster_id, snode.get_id(), event_dict)
                            logger.info(f"Processing event: {event.get_id()}")
                            process_event(event, logger)
                            events_groups[sid][et][msg] = event
                            events_list.append(event)

                        for ev in events_list:
                            if ev.count > 1 :
                                ev.write_to_db(db.kv_store)

                        logger.info(f"Discarding events: {len(events)}")
                        client.distr_status_events_discard_then_get(len(events), 0)
                        page *= 10
                    else:
                        logger.info("no events found, sleeping")
                        break
                except Exception as e:
                    logger.error(f"Failed to process distr events: {e}")
                    break

            time.sleep(constants.DISTR_EVENT_COLLECTOR_INTERVAL_SEC)
    except Exception as e:
        logger.error(e)

    logger.info(f"Stopping Distr event collector on node: {node_id}")


threads_maps: dict[str, threading.Thread] = {}

def main():
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cluster in clusters:
            cluster_id = cluster.get_id()

            nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
            for snode in nodes:
                node_id = snode.get_id()
                # logger.info(f"Checking node {snode.hostname}")
                if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                    t = threading.Thread(target=start_event_collector_on_node, args=(node_id,))
                    t.start()
                    threads_maps[node_id] = t

        time.sleep(5)


if __name__ == "__main__":
    main()
