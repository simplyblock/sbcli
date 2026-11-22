"""Multi-attach array LUN mode, phase 3: take a LUN over on a standby node.

When the node serving a multi-attach device (``NVMeDevice.serving_node()``)
fails, its peers' controllers ``remote_alceml_<uuid>`` hold I/O for
``MULTI_ATTACH_HOLD_SEC``. Within that window :func:`take_over_lun`:

1. fences the failed node from the LUN at the array -- before anything else:
   its host multipath layer may still hold queued writes that would land late
   and overwrite newer data;
2. picks a standby: the owner's LVS secondary, else its tertiary, else the
   owner itself; ONLINE and not fenced from this LUN;
3. verifies through the standby's node agent that it sees the LUN and that
   block 0 carries the alceml signature (read-only); 2 and 3 run while the
   fence is applied, nothing is built on the standby before its ack;
4. on the standby builds the base bdev, the alceml OPEN-ONLY
   (``pba_init_mode=1``; 2/3 would format the LUN and destroy its data), the
   PT bdev and the device subsystem under the same NQN, with the standby's
   controller-ID range, the same namespace UUID and the standby's listeners;
5. records the standby as the serving node (atomically; the device stays
   ONLINE, no unavailable broadcast, no cluster-map change);
6. adds the standby's export as a path of ``remote_alceml_<uuid>`` on every
   ONLINE node (the standby too: its loopback), all in parallel, where the
   held I/O resumes;
7. detaches the failed node's path from those controllers so it never
   reconnects.

The record is written before the paths are added (design order: paths, then
record): the export on the standby is what serves the LUN from then on, and
any node that misses its path in step 6 repairs it from the record
(connect_device uses the serving node). A re-run is idempotent: each step
checks what is already in place, and a LUN already served by an ONLINE node
other than the failed one only gets steps 6-7.

Never done: a takeover without a configured array plugin or without the
array's acknowledgement of the fence; an alceml create with any init mode but
open-only. When no standby can read the LUN, nothing is changed: peers keep
holding until their hold expires, and the caller marks the device
unavailable.

Warm standby (opt-in, ``Cluster.multi_attach_warm_standby``): every attached
non-serving node keeps a standby alceml of the LUN open (``standby=true``,
never exported) whose page map the data plane keeps warm
(:func:`ensure_warm_standbys`, driven by the storage-node monitor). Step 4
then reuses that instance instead of opening the LUN cold (a full page-map
load, minutes at 100 TiB): it only adds the PT bdev, subsystem, listeners and
namespace. A standby activates on its first I/O (fence by preemption, epoch
bump, final refresh), so the takeover waits for the activation after step 6,
when the peers' held I/O reaches it. If it does not activate, the failed node
is fenced at the array, the export on the standby is torn down and redone
cold (the same NQN and IPs: the paths already point there).
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid as uuid_mod
from concurrent.futures import ThreadPoolExecutor

from simplyblock_core import array, constants, multi_attach, utils
from simplyblock_core.controllers import events_controller as ec
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.events import EventObj
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode

logger = logging.getLogger()

#: bdev_alceml_create pba_init_mode that opens an existing alceml without
#: touching its data. The only mode a takeover ever uses.
ALCEML_OPEN_ONLY = 1

#: Device states a takeover acts on. A failed, removed or new device is not
#: taken over.
_TAKEOVER_STATES = (NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                    NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE)

_local_locks: dict[str, threading.Lock] = {}
# (device_id, node_id) of array fences still running after a fence by
# reservation: the node is not unfenced (reintegrated) meanwhile.
_fences_in_flight: set = set()
_fences_guard = threading.Lock()
_local_locks_guard = threading.Lock()


#: Outcomes of :func:`take_over_lun_outcome`.
TAKEN_OVER = "taken_over"          # served by a live node other than the failed one
IN_PROGRESS = "in_progress"        # another takeover of this LUN is running
NO_STANDBY = "no_standby"          # fenced, but no standby can read the LUN: hold
FAILED = "failed"                  # not taken over (no plugin, fence, export, ...)


class TakeoverError(Exception):
    """A takeover step failed; the message says which and why."""


class StandbyUnreachable(TakeoverError):
    """The standby's SPDK stopped answering during the export: the next
    standby is tried at once (no retry on it, no array fence first)."""


#: Consecutive connection errors while waiting for an activation after which
#: the standby counts as gone (the poll interval is 50 ms, each call times out
#: after 1 s).
ACTIVATION_CONNECTION_ERRORS = 2
#: Bound of one standby candidate's LUN inventory query (no retries).
STANDBY_PROBE_TIMEOUT_SEC = 3.0
#: How long a candidate that reads the LUN waits for an earlier candidate
#: (candidate order) whose probe has not answered yet.
STANDBY_ORDER_GRACE_SEC = 0.5


def _local_lock(device_id) -> threading.Lock:
    with _local_locks_guard:
        return _local_locks.setdefault(device_id, threading.Lock())


def _event(cluster_id, device, message, level=EventObj.LEVEL_INFO):
    try:
        ec.log_event_cluster(
            cluster_id=cluster_id,
            domain=ec.DOMAIN_CLUSTER,
            event=ec.EVENT_LUN_TAKEOVER,
            db_object=device,
            caused_by=ec.CAUSED_BY_MONITOR,
            message=message,
            node_id=device.get_id(),
            event_level=level,
            storage_id=device.cluster_device_order)
    except Exception as e:                                  # noqa: BLE001
        logger.error("LUN takeover event of %s not logged: %s", device.get_id(), e)


def _check_deadline(deadline, step):
    if time.monotonic() > deadline:
        raise TakeoverError(
            f"deadline of {constants.MULTI_ATTACH_TAKEOVER_DEADLINE_SEC}s passed before {step}")


def _remaining(deadline, cap):
    return max(1, min(cap, int(deadline - time.monotonic())))


def _transport(node):
    if node.active_rdma:
        return "RDMA"
    if node.active_tcp:
        return "TCP"
    return None


def _device_on_owner(owner, device_id):
    for dev in owner.nvme_devices:
        if dev.get_id() == device_id:
            return dev
    return None


def take_over_lun(device_id, failed_node_id, reason="", *, deadline_sec=None) -> bool:
    """Move the LUN of multi-attach device ``device_id`` off the failed node
    ``failed_node_id`` onto a standby (see the module docstring).

    Returns True when the device is served by an ONLINE node other than the
    failed one afterwards (taken over now, or already before), False when it
    was not taken over (the reason is logged and raised as a cluster event)
    or another takeover of it is running. Never raises.
    """
    return take_over_lun_outcome(device_id, failed_node_id, reason,
                                 deadline_sec=deadline_sec) == TAKEN_OVER


def take_over_lun_outcome(device_id, failed_node_id, reason="", *, deadline_sec=None) -> str:
    """:func:`take_over_lun`, answering one of TAKEN_OVER, IN_PROGRESS,
    NO_STANDBY or FAILED."""
    db = DBController()
    deadline = time.monotonic() + (deadline_sec if deadline_sec is not None
                                   else constants.MULTI_ATTACH_TAKEOVER_DEADLINE_SEC)
    try:
        dev = db.get_storage_device_by_id(device_id)
        owner = db.get_storage_node_by_id(dev.node_id)
        cluster = db.get_cluster_by_id(owner.cluster_id)
    except KeyError as e:
        logger.error("LUN takeover of %s: %s", device_id, e)
        return FAILED
    if not (multi_attach.is_multi_attach(cluster) and multi_attach.is_multi_attach(dev)):
        logger.error("LUN takeover of %s refused: not a multi-attach device", device_id)
        return FAILED

    local = _local_lock(device_id)
    if not local.acquire(blocking=False):
        logger.info("LUN takeover of %s already running in this process", device_id)
        return IN_PROGRESS
    token = f"{socket.gethostname()}:{os.getpid()}:{uuid_mod.uuid4().hex[:8]}"
    try:
        try:
            held, holder = db.acquire_lun_takeover_lock(
                device_id, token, constants.MULTI_ATTACH_TAKEOVER_LOCK_TTL_SEC)
        except Exception as e:                              # noqa: BLE001
            logger.error("LUN takeover of %s: lease not taken: %s", device_id, e)
            return FAILED
        if not held:
            logger.info("LUN takeover of %s already running (%s)", device_id, holder)
            return IN_PROGRESS
        try:
            return _take_over_locked(db, cluster, device_id, failed_node_id, reason, deadline)
        finally:
            try:
                db.release_lun_takeover_lock(device_id, token)
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN takeover of %s: lease not released: %s", device_id, e)
    finally:
        local.release()


def _take_over_locked(db, cluster, device_id, failed_node_id, reason, deadline) -> str:
    owner = db.get_storage_node_by_id(db.get_storage_device_by_id(device_id).node_id)
    dev = _device_on_owner(owner, device_id)
    if dev is None:
        logger.error("LUN takeover of %s: device not on its owner %s", device_id, owner.get_id())
        return FAILED
    cluster_id = owner.cluster_id
    logger.info("LUN takeover of %s (LUN %s, owner %s) from node %s: %s",
                device_id, dev.lun_wwn, owner.get_id(), failed_node_id, reason or "node lost")

    if dev.status not in _TAKEOVER_STATES:
        logger.info("LUN takeover of %s skipped: device is %s", device_id, dev.status)
        return FAILED

    nodes_by_id = {n.get_id(): n for n in db.get_storage_nodes_by_cluster_id(cluster_id)}
    serving_id = dev.serving_node()
    if serving_id != failed_node_id:
        # Re-run, or a stale trigger: only complete the paths if the LUN is
        # served by a live node.
        server = nodes_by_id.get(serving_id)
        if server is None or server.status != StorageNode.STATUS_ONLINE:
            logger.warning("LUN takeover of %s: served by %s (%s), not by %s; nothing done",
                           device_id, serving_id, getattr(server, "status", "unknown"), failed_node_id)
            return FAILED
        logger.info("LUN takeover of %s: already served by %s; completing its paths",
                    device_id, serving_id)
        failed = nodes_by_id.get(failed_node_id)
        _switch_paths(db, cluster_id, dev, server, failed, _failed_paths(failed, None, None))
        return TAKEN_OVER

    failed = nodes_by_id.get(failed_node_id)
    if failed is None:
        logger.error("LUN takeover of %s: failed node %s not found", device_id, failed_node_id)
        return FAILED
    old_ips = [ip.strip() for ip in (dev.nvmf_ip or "").split(",") if ip.strip()]
    old_port = dev.nvmf_port

    # 2 + 3 run while the fence is applied: choosing the standby only reads
    # (the node agent's LUN inventory), and nothing is built on it before
    # the array has acknowledged the fence. Every volume of a 1+0 cluster
    # waits on this LUN.
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"standby-{device_id[:8]}")
    choice = pool.submit(_choose_standby, owner, dev, failed_node_id, nodes_by_id, deadline)
    pool.shutdown(wait=False)
    by_reservation = False
    try:
        # 1. Fence -- before anything else is changed. With reservations the
        # standby's alceml fences at open (preempting the failed node's
        # reservation, step 4) and the array fence follows off the I/O path.
        by_reservation = _fence_by_reservation(cluster, dev)
        if not by_reservation:
            _fence(cluster, dev, failed)
            _record_fenced(db, owner, device_id, failed_node_id)
        _check_deadline(deadline, "choosing a standby")

        # 2 + 3. A standby that can read the LUN.
        standby, path, why = choice.result(timeout=max(1.0, deadline - time.monotonic()))
        if standby is None:
            msg = (f"LUN {dev.lun_wwn} of device {device_id} not taken over from node "
                   f"{failed_node_id}: no standby can read it ({why}); holding I/O until "
                   f"the peers' hold expires")
            logger.error(msg)
            _event(cluster_id, dev, msg, EventObj.LEVEL_CRITICAL)
            return NO_STANDBY
        # 4. Open-only export on the standby. A standby that fails and no
        # longer answers is left for the next candidate (2026-11-20 FTT2 bed:
        # the standby chosen was rebooting; the takeover failed although a
        # third node could read the LUN, and the 1+0 cluster suspended).
        tried, retried = set(), set()
        while True:
            _check_deadline(deadline, f"exporting the LUN on {standby.get_id()}")
            try:
                export = _export_on_standby(db, cluster, standby, dev, path, deadline)
                break
            except TakeoverError as e:
                sid = standby.get_id()
                gone = isinstance(e, StandbyUnreachable)
                if by_reservation and not gone:
                    # The standby answers but did not take the reservation:
                    # fence at the array. A standby that is gone fenced
                    # nothing; the next one opens with its own key and
                    # preempts, so it is tried at once.
                    logger.warning("LUN takeover of %s: fence by reservation failed (%s); "
                                   "fencing at the array", device_id, e)
                    by_reservation = False
                    _fence(cluster, dev, failed)
                    _record_fenced(db, owner, device_id, failed_node_id)
                if not gone and sid not in retried and _reachable(standby):
                    retried.add(sid)
                    continue
                tried.add(sid)
                logger.warning("LUN takeover of %s: export on standby %s failed (%s); trying "
                               "another standby", device_id, sid, e)
                _check_deadline(deadline, "choosing another standby")
                standby, path, why = _choose_standby(owner, dev, failed_node_id, nodes_by_id,
                                                     deadline, exclude=tried)
                if standby is None:
                    raise TakeoverError(f"export failed on {sorted(tried)} and no other standby "
                                        f"can read the LUN ({why}): {e}")
        if by_reservation:
            # Fenced by the reservation: recorded now, detached at the array
            # in the background (the attachment reconcile detaches it too).
            _record_fenced(db, owner, device_id, failed_node_id)
            _fence_in_background(cluster, dev, failed)
        _check_deadline(deadline, "recording the takeover")

        # 5 (design step 6). Record, atomically.
        if not _record_serving(db, owner, device_id, failed_node_id, standby, path, export):
            raise TakeoverError("the device record changed under the takeover")
    except TakeoverError as e:
        msg = f"LUN takeover of device {device_id} from node {failed_node_id} failed: {e}"
        logger.error(msg)
        _event(cluster_id, dev, msg, EventObj.LEVEL_CRITICAL)
        return FAILED
    except Exception as e:                                  # noqa: BLE001
        msg = f"LUN takeover of device {device_id} from node {failed_node_id} failed: {e}"
        logger.exception(msg)
        _event(cluster_id, dev, msg, EventObj.LEVEL_CRITICAL)
        return FAILED

    dev = _device_on_owner(db.get_storage_node_by_id(owner.get_id()), device_id) or dev
    # 6 + 7 (design steps 5 and 7). New path on every ONLINE node, old one off.
    added = _switch_paths(db, cluster_id, dev, standby, failed,
                          _failed_paths(failed, old_ips, old_port))
    if export.get("warm") and not _activate_warm_standby(
            db, cluster, owner, dev, standby, failed, path, export, deadline):
        return FAILED

    if dev.status != NVMeDevice.STATUS_ONLINE:
        # The hold had expired and the device was marked unavailable; it is
        # served again.
        from simplyblock_core.controllers import device_controller
        device_controller.device_set_state(
            device_id, NVMeDevice.STATUS_ONLINE,
            cause=device_controller.CAUSE_LUN_TAKEOVER, connect_peers=False)

    msg = (f"LUN {dev.lun_wwn} of device {device_id} taken over from node {failed_node_id} "
           f"by node {standby.get_id()} ({path}); new path on {len(added)} node(s)")
    logger.info(msg)
    _event(cluster_id, dev, msg, EventObj.LEVEL_WARN)
    return TAKEN_OVER


# ---------------------------------------------------------------- 1. fence

def _fence(cluster, dev, failed):
    # The array's own ids when the plugin created the LUN / registered the
    # host (array_controller), else the WWN / hostname as in phase 3.
    host = multi_attach.array_host_ref(failed)
    if not host:
        raise TakeoverError(f"node {failed.get_id()} has no hostname to fence it by")
    lun = multi_attach.array_lun_ref(dev)
    if not lun:
        raise TakeoverError("the device has no LUN WWN to fence")
    ok = None
    for fresh in (False, True):
        if fresh:
            # The cached connection may have gone stale (expired session).
            array.forget_plugin(cluster.get_id())
        try:
            plugin = array.cached_plugin_for_cluster(cluster)
        except array.ArrayNotConfigured:
            raise TakeoverError(
                "no array plugin is configured (cluster array_plugin); a LUN is never "
                "taken over without fencing its serving node")
        except array.ArrayError as e:
            raise TakeoverError(f"array plugin unavailable: {e}")
        try:
            ok = plugin.fence(lun, host)
            break
        except Exception as e:                              # noqa: BLE001
            if fresh:
                raise TakeoverError(f"array fence of {host} on LUN {lun} failed: {e}")
            logger.warning("LUN %s: fence of %s failed (%s); again on a new array connection",
                           lun, host, e)
    if not ok:
        raise TakeoverError(f"array did not acknowledge the fence of {host} on LUN {lun}")
    logger.info("LUN %s: node %s (%s) fenced at the array", lun, failed.get_id(), host)
    _forget_attachment(dev.get_id(), failed.get_id())


def _forget_attachment(device_id, node_id):
    """The fence detached ``node_id`` from the LUN: drop it from the
    device's ``array_attached_node_ids`` so the attachment reconcile, which
    calls the array only for nodes it does not list, presents the LUN to it
    again once the record no longer fences it. Best-effort."""
    try:
        db = DBController()
        dev = db.get_storage_device_by_id(device_id)
        owner = db.get_storage_node_by_id(dev.node_id)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("LUN fence of %s on %s: attachment record not updated: %s",
                       node_id, device_id, e)
        return

    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or node_id not in (d.array_attached_node_ids or []):
            return False
        d.array_attached_node_ids = [x for x in d.array_attached_node_ids if x != node_id]
        return True
    try:
        db.atomic_update(owner, _mut)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("LUN fence of %s on %s: attachment record not updated: %s",
                       node_id, device_id, e)


def _fence_by_reservation(cluster, dev) -> bool:
    """Whether this takeover fences by persistent reservation: the device's
    alceml instances run with keys (NVMeDevice.lun_reservations, decided
    with the array's capability when the LUN was added)."""
    return multi_attach.reservations_enabled(cluster, dev)


def _fence_in_background(cluster, dev, failed):
    """The array fence after a fence by reservation: off the I/O path, for
    good measure (the reservation already rejects the node's writes)."""
    key = (dev.get_id(), failed.get_id())

    def _run():
        try:
            _fence(cluster, dev, failed)
        except TakeoverError as e:
            logger.warning("LUN %s: array fence of %s after the reservation failed: %s; the "
                           "attachment reconcile detaches it", dev.lun_wwn, failed.get_id(), e)
        finally:
            with _fences_guard:
                _fences_in_flight.discard(key)
    with _fences_guard:
        _fences_in_flight.add(key)
    threading.Thread(target=_run, name=f"fence-{dev.get_id()[:8]}", daemon=True).start()


def fence_in_flight(device_id, node_id) -> bool:
    """Whether an array fence of ``node_id`` on the LUN of ``device_id`` is
    still running in this process."""
    with _fences_guard:
        return (device_id, node_id) in _fences_in_flight


def _wait_active(rpc, alceml, deadline):
    """Wait until ``alceml`` (created with a reservation key, not standby)
    holds the reservation and serves. Raises TakeoverError."""
    limit = min(deadline, time.monotonic() + constants.MULTI_ATTACH_ACTIVATION_WAIT_SEC)
    last = None
    unreachable = 0
    while True:
        try:
            last = rpc.alceml_get_role(alceml) or {}
            unreachable = 0
        except Exception as e:                              # noqa: BLE001
            last = {"error": str(e)}
            unreachable += 1
            if unreachable >= ACTIVATION_CONNECTION_ERRORS:
                # The standby is gone (2026-11-20 FTT2 bed: a rebooting
                # standby cost the whole wait, 5 s, of a 13 s stall).
                raise StandbyUnreachable(f"alceml {alceml}: SPDK not answering ({e})")
        if last.get("role") == "active" and last.get("state") == "active":
            logger.info("LUN takeover: %s holds the reservation (epoch %s, activation %s ms)",
                        alceml, last.get("epoch"), last.get("last_activation_ms"))
            return
        if last.get("state") == "act_failed" or time.monotonic() >= limit:
            raise TakeoverError(f"alceml {alceml} did not take the reservation: {last}")
        time.sleep(0.05)


def _record_fenced(db, owner, device_id, node_id):
    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or node_id in d.fenced_node_ids:
            return False
        d.fenced_node_ids = list(d.fenced_node_ids) + [node_id]
        return True
    db.atomic_update(owner, _mut)


# --------------------------------------------------------- 2 + 3. standby

def _choose_standby(owner, dev, failed_node_id, nodes_by_id, deadline, exclude=()):
    reasons: list = []
    eligible: list = []
    for nid in multi_attach.standby_candidates(owner, failed_node_id):
        node = nodes_by_id.get(nid)
        if node is None or nid in exclude:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            reasons.append(f"{nid}: {node.status}")
            continue
        if nid in (dev.fenced_node_ids or []):
            reasons.append(f"{nid}: fenced from this LUN")
            continue
        if _transport(node) is None:
            reasons.append(f"{nid}: no active fabric")
            continue
        eligible.append((nid, node))
    if not eligible:
        return None, None, "; ".join(reasons) or "no standby candidate"
    # Every candidate is probed at once, each call bounded and without
    # retries; the first in candidate order that reads the LUN wins. One
    # after the other, a candidate that had just lost its network held the
    # choice until the takeover deadline (2026-11-20 FTT2 bed, network cuts
    # of two nodes: the LUN went unavailable and the cluster suspended).
    pool = ThreadPoolExecutor(max_workers=len(eligible), thread_name_prefix=f"probe-{dev.get_id()[:8]}")
    probes = [(nid, node, pool.submit(_probe, node, dev, deadline)) for nid, node in eligible]
    pool.shutdown(wait=False)
    # The wait is bounded too (a probe that ignores its client timeout must
    # not hold the choice). Candidate order wins, but a candidate that already
    # reads the LUN is not kept waiting more than STANDBY_ORDER_GRACE_SEC for
    # an earlier one that does not answer.
    now = time.monotonic()
    probes_done_by = min(deadline, now + STANDBY_PROBE_TIMEOUT_SEC + 0.5)
    first_ready_at = None
    while True:
        now = time.monotonic()
        best = None
        pending_before = False
        for nid, node, fut in probes:
            if not fut.done():
                pending_before = True
                continue
            try:
                path, _why = fut.result()
            except Exception:                               # noqa: BLE001
                path = None
            if path:
                best = (nid, node, path)
                break
        if best and not pending_before:
            break
        if best:
            if first_ready_at is None:
                first_ready_at = now
            if now - first_ready_at >= STANDBY_ORDER_GRACE_SEC:
                break
        if now >= probes_done_by or all(f.done() for _n, _o, f in probes):
            break
        time.sleep(0.02)
    if best:
        nid, node, path = best
        logger.info("LUN %s: standby %s reads it at %s", dev.lun_wwn, nid, path)
        return node, path, ""
    for nid, _node, fut in probes:
        if not fut.done():
            reasons.append(f"{nid}: probe did not answer within {STANDBY_PROBE_TIMEOUT_SEC:.0f}s")
            continue
        try:
            _path, why = fut.result()
        except Exception as e:                              # noqa: BLE001
            why = f"agent query failed: {e}"
        reasons.append(f"{nid}: {why}")
    return None, None, "; ".join(reasons) or "no standby candidate"


def _reachable(node) -> bool:
    """Whether the node's SPDK answers right now (no retries)."""
    try:
        node.rpc_client(timeout=constants.MULTI_ATTACH_PROBE_TIMEOUT_SEC, retry=0).get_version()
        return True
    except Exception:                                       # noqa: BLE001
        return False


def _probe(node, dev, deadline):
    try:
        result, err = node.client(timeout=min(_remaining(deadline, 15), STANDBY_PROBE_TIMEOUT_SEC),
                                  retry=0, connect_retry=0).lun_info(dev.lun_wwn)
    except Exception as e:                                  # noqa: BLE001
        return None, f"agent query failed: {e}"
    if err:
        return None, f"agent: {err}"
    if not isinstance(result, list):
        return None, "agent returned no LUN inventory"
    return multi_attach.pick_lun_path(result, dev)


# --------------------------------------------------------------- 4. export

def _create_base_bdev(rpc, name, path, dev):
    """The base bdev over the LUN on the standby, with the block size the
    device was formatted with (NVMeDevice.aio_block_size; 0 = legacy, SPDK's
    logical block size) and checked after creation: alceml over a base bdev
    of another block size cannot read the device. The engine follows the
    standby's own path to the LUN (a device-mapper node there needs io_uring
    even if the owner reaches the LUN as a plain disk), else what the device
    records."""
    engine = utils.lblk_io_engine_for({"path": path})
    if engine != constants.LBLK_IO_ENGINE_URING:
        engine = dev.lblk_io_engine or constants.LBLK_IO_ENGINE_AIO
    utils.create_aio_bdev(rpc, name, path, block_size=int(dev.aio_block_size or 0),
                          new_device=False, verify=True, io_engine=engine)
    return True


def _alceml_open_kwargs(cluster, dev, extra) -> dict:
    """``create_alceml`` kwargs of an open-only alceml of ``dev`` (takeover,
    warm standby): never formats, the cluster's page and checksum settings,
    plus ``extra`` (multi_attach.alceml_create_kwargs)."""
    checksum_method, cache_size, cache_eviction = utils.alceml_checksum_params(cluster, dev)
    return dict(
        pba_init_mode=ALCEML_OPEN_ONLY,
        write_protection=cluster.distr_ndcs > 1,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap,
        checksum_method=checksum_method,
        cache_size=cache_size,
        cache_eviction_threshold=cache_eviction,
        force_4k_atomic=getattr(cluster, 'atomic_4k', False),
        **extra,
    )


def _alceml_role(node, alceml) -> dict:
    """``alceml_get_role`` of ``alceml`` on ``node`` (fast, no retries); {}
    when it does not answer."""
    try:
        return node.rpc_client(timeout=1, retry=0).alceml_get_role(alceml) or {}
    except Exception as e:                                  # noqa: BLE001
        logger.info("alceml_get_role of %s on %s failed: %s", alceml, node.get_id(), e)
        return {}


def _is_standby_instance(role) -> bool:
    """A standby alceml instance that can still activate on its first I/O
    (opened standby, or an active instance that was deposed)."""
    return role.get("role") == "standby" and role.get("state") != "act_failed"


def _reuse_warm_standby(rpc, standby, alceml):
    """The standby node holds ``alceml`` already: ``(warm, exists)``. A
    standby instance is reused as is (warm). One whose activation failed is
    closed, so that it is opened again active; anything else (an active
    instance) is left to the regular path."""
    role = _alceml_role(standby, alceml)
    if _is_standby_instance(role):
        logger.info("LUN takeover: reusing the warm standby %s on %s (warm %s, %s refreshes)",
                    alceml, standby.get_id(), role.get("warm"), role.get("refreshes_count"))
        return True, True
    if role.get("role") == "standby":
        logger.warning("LUN takeover: standby %s on %s failed its activation (%s); reopening it",
                       alceml, standby.get_id(), role)
        _teardown(rpc, standby.get_id(), [("alceml", alceml)])
        if rpc.bdev_get(alceml):
            raise TakeoverError(f"failed standby alceml {alceml} not closed on {standby.get_id()}")
        return False, False
    return False, True


def _export_on_standby(db, cluster, standby, dev, path, deadline, allow_warm=True) -> dict:
    if getattr(dev, "bdev_type", "aio") != "aio":
        raise TakeoverError(f"device type {dev.bdev_type!r} cannot be taken over (array LUNs are lblk)")
    rpc = standby.rpc_client(timeout=_remaining(deadline, 60))
    sid = standby.get_id()
    created = []
    try:
        base = utils.aio_bdev_name_for_serial(dev.serial_number)
        info = rpc.bdev_get(base)
        if not info:
            if not _create_base_bdev(rpc, base, path, dev):
                raise TakeoverError(f"base bdev {base} ({path}) not created on {sid}")
            created.append(("base", base))
            info = rpc.bdev_get(base)
        if not info:
            raise TakeoverError(f"base bdev {base} not found on {sid} after create")
        size = int(info.get("block_size", 0)) * int(info.get("num_blocks", 0))
        if size < int(dev.size or 0) and ("base", base) not in created:
            # A base bdev this node kept from before a LUN growth
            # (device_controller.grow_cluster_devices) still has the old size:
            # re-read the block device before giving up.
            # The host's kernel may not have seen the growth either (the
            # node was out while it ran).
            try:
                standby.client(timeout=_remaining(deadline, 30), retry=1).rescan_block_device(path)
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN takeover: block rescan of %s on %s failed: %s", path, sid, e)
            try:
                utils.rescan_lblk_bdev(rpc, base, utils.lblk_io_engine_for({"path": path}))
                info = rpc.bdev_get(base) or info
                size = int(info.get("block_size", 0)) * int(info.get("num_blocks", 0))
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN takeover: rescan of %s on %s failed: %s", base, sid, e)
        if size < int(dev.size or 0):
            raise TakeoverError(f"base bdev {base} on {sid} is {size} bytes, the device {dev.size}")

        alceml = dev.alceml_bdev or f"alceml_{dev.get_id()}"
        kwargs = multi_attach.alceml_create_kwargs(dev, standby, cluster)
        exists = rpc.bdev_get(alceml)
        warm = False
        if (exists and allow_warm and kwargs.get("reservation_key")
                and multi_attach.warm_standby_enabled(cluster, dev)):
            warm, exists = _reuse_warm_standby(rpc, standby, alceml)
        if not exists:
            ok = standby.create_alceml(alceml, base, dev.get_id(),
                                       **_alceml_open_kwargs(cluster, dev, kwargs))
            if not ok:
                raise TakeoverError(f"alceml {alceml} not opened on {sid}")
            created.append(("alceml", alceml))
        if kwargs.get("reservation_key") and not warm:
            # Opened active with a key: it preempts the old holder (fence).
            _wait_active(standby.rpc_client(timeout=1, retry=0), alceml, deadline)
            try:
                from simplyblock_core.controllers import qos_controller
                rpc.alceml_set_qos_weights(qos_controller.get_qos_weights_list(cluster.get_id()))
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN takeover: QoS weights not set on %s: %s", sid, e)

        pt = dev.pt_bdev or f"{alceml}_PT"
        pt_info = rpc.bdev_get(pt)
        if not pt_info:
            if not rpc.bdev_PT_NoExcl_create(pt, alceml):
                raise TakeoverError(f"PT bdev {pt} not created on {sid}")
            created.append(("pt", pt))
            pt_info = rpc.bdev_get(pt)
        if not pt_info:
            raise TakeoverError(f"PT bdev {pt} not found on {sid} after create")

        nqn = multi_attach.device_subsystem_nqn(standby, dev, cluster)
        subsys = rpc.subsystem_get(nqn)
        if not subsys:
            if not rpc.subsystem_create(nqn, 'sbcli-cn', dev.get_id(),
                                        **multi_attach.subsystem_create_kwargs(standby, dev, db)):
                raise TakeoverError(f"subsystem {nqn} not created on {sid}")
            created.append(("subsystem", nqn))
            subsys = {}
        listening = {(la or {}).get("traddr") for la in (subsys.get("listen_addresses") or [])}
        ips = []
        for iface in standby.data_nics:
            if not iface.ip4_address:
                continue
            if iface.ip4_address not in listening:
                rpc.listeners_create(nqn, iface.trtype, iface.listen_address, standby.nvmf_port)
            ips.append(iface.ip4_address)
        if not ips:
            raise TakeoverError(f"node {sid} has no data IP to export the LUN on")
        if not any(ns.get("name") == pt for ns in (subsys.get("namespaces") or [])):
            if not rpc.nvmf_subsystem_add_ns(nqn, pt, multi_attach.namespace_uuid(dev),
                                             nguid=multi_attach.namespace_nguid(dev)):
                raise TakeoverError(f"namespace {pt} not added to {nqn} on {sid}")
    except StandbyUnreachable:
        logger.warning("LUN takeover cleanup on %s skipped: SPDK not answering (%s)",
                       sid, [name for _, name in created])
        raise
    except Exception as e:
        if created and not _reachable(standby):
            # Its SPDK is gone: what it held went with it (each call would
            # only wait out the retries).
            logger.warning("LUN takeover cleanup on %s skipped: SPDK not answering (%s)",
                           sid, [name for _, name in created])
            raise StandbyUnreachable(f"standby {sid}: {e}") from e
        _teardown(rpc, sid, created)
        raise
    aliases = (pt_info or {}).get("aliases") or []
    return {
        "nqn": nqn,
        "ips": ips,
        "port": standby.nvmf_port,
        "base": base,
        "pt_uuid": aliases[0] if aliases else "",
        # A reused warm standby: activates on the first I/O, after the paths.
        "warm": warm,
        "alceml": alceml,
        "pt": pt,
    }


def _teardown(rpc, sid, created):
    """Undo what this attempt created on the standby, newest first, so a
    failed takeover leaves no alceml open on a LUN the node does not serve.
    Never formats anything; best-effort."""
    for kind, name in reversed(created):
        try:
            if kind == "subsystem":
                rpc.subsystem_delete(name)
            elif kind == "pt":
                rpc.bdev_PT_NoExcl_delete(name)
            elif kind == "alceml":
                rpc.bdev_alceml_delete(name)
            elif kind == "base":
                engine = utils.bdev_io_engine(rpc.bdev_get(name)) or constants.LBLK_IO_ENGINE_AIO
                utils.delete_lblk_bdev(rpc, name, engine)
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover cleanup on %s: %s %s not removed: %s", sid, kind, name, e)


def _activate_warm_standby(db, cluster, owner, dev, standby, failed, path, export, deadline) -> bool:
    """After the paths were switched to a reused warm standby: wait until the
    peers' I/O has activated it. If it does not, fence the failed node at the
    array, tear the export down and redo it cold (alceml opened active with
    the key, as without warm standby); the NQN and IPs stay, so the peers'
    paths already point there. Returns False (device set unavailable, event
    raised) only when the cold export failed as well."""
    sid, device_id, alceml = standby.get_id(), dev.get_id(), export["alceml"]
    try:
        _wait_active(standby.rpc_client(timeout=1, retry=0), alceml, deadline)
        try:
            from simplyblock_core.controllers import qos_controller
            standby.rpc_client(timeout=_remaining(deadline, 10)).alceml_set_qos_weights(
                qos_controller.get_qos_weights_list(cluster.get_id()))
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover: QoS weights not set on %s: %s", sid, e)
        return True
    except TakeoverError as e:
        msg = (f"LUN takeover of device {device_id}: warm standby {alceml} on {sid} did not "
               f"activate ({e}); fencing node {failed.get_id()} at the array and opening the LUN cold")
        logger.warning(msg)
        _event(owner.cluster_id, dev, msg, EventObj.LEVEL_WARN)
    try:
        _fence(cluster, dev, failed)
        _record_fenced(db, owner, device_id, failed.get_id())
        rpc = standby.rpc_client(timeout=_remaining(deadline, 60))
        _teardown(rpc, sid, [("alceml", alceml), ("pt", export["pt"]), ("subsystem", export["nqn"])])
        if rpc.bdev_get(alceml):
            raise TakeoverError(f"standby alceml {alceml} not closed on {sid}")
        _export_on_standby(db, cluster, standby, dev, path, deadline, allow_warm=False)
        logger.info("LUN takeover of %s: opened cold on %s after the warm standby failed",
                    device_id, sid)
        return True
    except Exception as e:                                  # noqa: BLE001
        msg = (f"LUN takeover of device {device_id} from node {failed.get_id()} failed: the warm "
               f"standby on {sid} did not activate and the cold open failed: {e}")
        logger.error(msg)
        _event(owner.cluster_id, dev, msg, EventObj.LEVEL_CRITICAL)
        # The record names the standby, so the takeover worker would not mark
        # the device lost: do it here (the regular node-loss behaviour).
        try:
            from simplyblock_core.controllers import device_controller
            device_controller.device_set_unavailable(device_id)
        except Exception as e2:                             # noqa: BLE001
            logger.error("LUN takeover of %s: device not set unavailable: %s", device_id, e2)
        return False


# --------------------------------------------------------------- 5. record

def _record_serving(db, owner, device_id, failed_node_id, standby, path, export) -> bool:
    standby_id = standby.get_id()
    ips = export["ips"]

    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or d.serving_node() not in (failed_node_id, standby_id):
            return False
        d.serving_node_id = "" if standby_id == d.node_id else standby_id
        paths = dict(d.node_paths or {})
        paths[standby_id] = {"path": path, "bdev": export["base"]}
        d.node_paths = paths
        d.nvmf_nqn = export["nqn"]
        d.nvmf_ip = ",".join(ips)
        d.nvmf_port = export["port"]
        d.nvmf_multipath = len(ips) > 1
        if export["pt_uuid"]:
            d.pt_spdk_uuid = export["pt_uuid"]
        return True

    fresh = db.atomic_update(owner, _mut)
    d = _device_on_owner(fresh, device_id) if fresh is not None else None
    return d is not None and d.serving_node() == standby_id


# ---------------------------------------------------------- 6 + 7. paths

def _failed_paths(failed, old_ips, old_port):
    """``(ips, port, trtype)`` of the failed node's export."""
    if failed is None:
        return [], None, "TCP"
    ips = list(old_ips or [])
    for iface in failed.data_nics or []:
        if iface.ip4_address and iface.ip4_address not in ips:
            ips.append(iface.ip4_address)
    return ips, old_port or failed.nvmf_port, _transport(failed) or "TCP"


def _switch_paths(db, cluster_id, dev, standby, failed, failed_paths) -> list:
    """Add the standby's export as a path of ``remote_alceml_<uuid>`` on every
    ONLINE node, then detach the failed node's path there; on all nodes in
    parallel. Returns the ids of the nodes that have the new path."""
    name = f"remote_{dev.alceml_bdev}"
    trtype = _transport(standby) or "TCP"
    new_ips = [ip.strip() for ip in (dev.nvmf_ip or "").split(",") if ip.strip()]
    dead_ips, dead_port, dead_trtype = failed_paths
    dead_ips = [ip for ip in dead_ips if ip not in new_ips]
    failed_id = failed.get_id() if failed is not None else None
    nodes = [n for n in db.get_storage_nodes_by_cluster_id(cluster_id)
             if n.status == StorageNode.STATUS_ONLINE and n.get_id() != failed_id]

    def _one(node):
        try:
            rpc = node.rpc_client(timeout=5, retry=1)
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover: no RPC client for %s: %s", node.get_id(), e)
            return False
        ok = False
        for ip in new_ips:
            try:
                if rpc.bdev_nvme_attach_controller(
                        name, dev.nvmf_nqn, ip, dev.nvmf_port, trtype, multipath="multipath",
                        **multi_attach.attach_controller_kwargs(dev)):
                    ok = True
            except Exception as e:                          # noqa: BLE001
                logger.warning("LUN takeover: path %s of %s not added on %s: %s",
                               ip, name, node.get_id(), e)
        if not ok:
            logger.error("LUN takeover: %s on %s has no path to the new export; the health "
                         "check reconnects it from the record", name, node.get_id())
            return False
        try:
            rpc.bdev_nvme_set_multipath_policy(f"{name}n1", "active_passive")
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover: multipath policy of %sn1 on %s not set: %s",
                           name, node.get_id(), e)
        for ip in dead_ips:
            try:
                rpc.bdev_nvme_detach_controller(name, traddr=ip, trsvcid=dead_port,
                                                trtype=dead_trtype)
            except Exception as e:                          # noqa: BLE001
                # Usually: the path is already gone (the hold destructed it).
                logger.info("LUN takeover: old path %s of %s on %s not detached: %s",
                            ip, name, node.get_id(), e)
        # The node's distribs may have been told the device is unavailable
        # while its old path failed; it is served again. On 1+0 a device left
        # unavailable in one node's map fails every read there (2026-11-21
        # FTT2 bed: an LVS failover read on such a node aborted its SPDK).
        try:
            from simplyblock_core import distr_controller
            distr_controller.send_dev_status_event(dev, NVMeDevice.STATUS_ONLINE, node)
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover: device status of %s not sent to %s: %s",
                           dev.get_id(), node.get_id(), e)
        return True

    if not nodes:
        return []
    # All nodes at once: the held I/O of each resumes when its path is added.
    with ThreadPoolExecutor(max_workers=min(16, len(nodes)),
                            thread_name_prefix=f"paths-{dev.get_id()[:8]}") as pool:
        results = list(pool.map(_one, nodes))
    return [n.get_id() for n, ok in zip(nodes, results) if ok]


def retire_stale_local_stack(node) -> list:
    """Retire, on a node that is back ONLINE, what it still holds of the LUNs
    another node took over while it was unreachable: its own path in each
    ``remote_alceml_<uuid>`` controller (the loopback to its now-stale export),
    then that export's subsystem, PT bdev and alceml. Nothing is formatted; the
    base bdev over the LUN is left alone (the node is fenced from the LUN).

    Needed when the node returns without a restart (a management-plane blip
    long enough to start a takeover): a restart never builds these stacks
    (``multi_attach.served_elsewhere``), but a node that never restarted still
    has them, and its own distribs would keep writing through its loopback
    into an alceml whose LUN the array has fenced. Returns the ids of the
    devices retired; best-effort and idempotent."""
    from simplyblock_core.db_controller import DBController
    db = DBController()
    node_id = node.get_id()
    try:
        rpc = node.rpc_client(timeout=10, retry=1)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("LUN retire: no RPC client for %s: %s", node_id, e)
        return []
    retired = []
    for owner in db.get_storage_nodes_by_cluster_id(node.cluster_id):
        for dev in owner.nvme_devices or []:
            if not multi_attach.is_multi_attach(dev) or not multi_attach.served_elsewhere(dev, node_id):
                continue
            # Only a node that once exported the LUN can hold a stale stack:
            # its owner, or a node it was taken over by before.
            if node_id != dev.node_id and node_id not in (dev.node_paths or {}):
                continue
            pt = f"{dev.alceml_bdev}_PT"
            try:
                has_stack = bool(rpc.bdev_get(dev.alceml_bdev)) or bool(rpc.bdev_get(pt))
            except Exception:                               # noqa: BLE001
                has_stack = False
            if not has_stack:
                continue
            if _is_kept_warm_standby(db, rpc, node, dev, pt):
                continue
            name = f"remote_{dev.alceml_bdev}"
            own_ips = [iface.ip4_address for iface in node.data_nics or [] if iface.ip4_address]
            for ip in own_ips:
                try:
                    rpc.bdev_nvme_detach_controller(name, traddr=ip, trsvcid=node.nvmf_port,
                                                    trtype=_transport(node) or "TCP")
                except Exception as e:                      # noqa: BLE001
                    logger.info("LUN retire: own path %s of %s on %s not detached: %s",
                                ip, name, node_id, e)
            nqn = multi_attach.device_subsystem_nqn(node, dev, db.get_cluster_by_id(node.cluster_id))
            _teardown(rpc, node_id, [("alceml", dev.alceml_bdev), ("pt", pt), ("subsystem", nqn)])
            logger.warning("LUN retire: %s on %s is served by %s since a takeover; its stale "
                           "local stack was removed (nothing formatted)",
                           dev.get_id(), node_id, dev.serving_node())
            retired.append(dev.get_id())
    return retired


def _is_kept_warm_standby(db, rpc, node, dev, pt) -> bool:
    """Whether what ``node`` holds of ``dev`` is only the warm standby
    ensure_warm_standbys keeps there (an unexported instance opened standby)
    on a node not fenced from the LUN: not a stale stack. Always False
    without warm standby."""
    if multi_attach.fenced_from(dev, node.get_id()):
        return False
    try:
        cluster = db.get_cluster_by_id(node.cluster_id)
        if not multi_attach.warm_standby_enabled(cluster, dev) or rpc.bdev_get(pt):
            return False
    except Exception:                                       # noqa: BLE001
        return False
    role = _alceml_role(node, dev.alceml_bdev)
    return _is_standby_instance(role) and role.get("standby_at_open") is True


def reintegrate_fenced_node(node) -> list:
    """Make a node that is back ONLINE a standby again for every LUN a
    takeover fenced it from: unfence it at the array (re-present the LUN).

    Only when nothing it held can still reach the LUN late:

    * the LUN is served by another ONLINE node (the takeover completed; a
      fenced node of a takeover still holding is never unfenced);
    * the node holds no alceml or PT bdev of the device (its stale stack was
      retired, or a restart never built it);
    * the node's host shows no disk carrying the LUN (``lun_info`` empty): the
      fence removed every path, so no host-side queue (multipath
      ``queue_if_no_path`` included) holds writes for it. A host that still
      shows the LUN keeps the fence until an operator calls
      :func:`unfence_node`.

    Returns the ids of the devices unfenced; best-effort, idempotent."""
    db = DBController()
    node_id = node.get_id()
    if node.status != StorageNode.STATUS_ONLINE:
        return []
    nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
    by_id = {n.get_id(): n for n in nodes}
    todo = [dev for owner in nodes for dev in owner.nvme_devices or []
            if multi_attach.fenced_from(dev, node_id)]
    if not todo:
        return []
    try:
        rpc = node.rpc_client(timeout=10, retry=1)
    except Exception as e:                                  # noqa: BLE001
        logger.info("LUN reintegrate: no RPC client for %s: %s", node_id, e)
        return []
    done = []
    for dev in todo:
        serving = by_id.get(dev.serving_node())
        if serving is None or serving.get_id() == node_id or serving.status != StorageNode.STATUS_ONLINE:
            continue
        if fence_in_flight(dev.get_id(), node_id):
            # Its detach is still running: an unfence now is undone by it
            # (2026-11-20 resize bed, R3: the node stayed detached for good
            # while the record listed it as a standby).
            logger.info("LUN reintegrate: array fence of %s on %s still running; stays fenced",
                        node_id, dev.lun_wwn)
            continue
        stale = _holds_stack(rpc, dev)
        if stale is None:
            # SPDK not answering (a node just lost may still read ONLINE):
            # nothing proves it holds no stack.
            logger.info("LUN reintegrate: SPDK of %s not answering; stays fenced from %s",
                        node_id, dev.lun_wwn)
            continue
        if stale:
            retire_stale_local_stack(node)
            stale = _holds_stack(rpc, dev)
            if stale is None:
                stale = True
            if stale:
                logger.info("LUN reintegrate: %s still holds a stack of %s; stays fenced",
                            node_id, dev.get_id())
                continue
        try:
            result, err = node.client(timeout=10, retry=0, connect_retry=0).lun_info(dev.lun_wwn)
        except Exception as e:                              # noqa: BLE001
            logger.info("LUN reintegrate: agent of %s not reached: %s", node_id, e)
            continue
        if err or not isinstance(result, list):
            logger.info("LUN reintegrate: LUN inventory of %s unavailable (%s)", node_id, err)
            continue
        if result:
            logger.warning("LUN reintegrate: host of %s still shows LUN %s (%s); it stays fenced "
                           "until an operator unfences it", node_id, dev.lun_wwn,
                           [e.get("device_path") for e in result if isinstance(e, dict)])
            continue
        if unfence_node(dev.get_id(), node_id):
            logger.info("LUN reintegrate: %s is a standby of LUN %s again", node_id, dev.lun_wwn)
            done.append(dev.get_id())
    return done


def _holds_stack(rpc, dev):
    """Whether the node holds an alceml or PT bdev of ``dev``; None when its
    SPDK does not answer."""
    try:
        return bool(rpc.bdev_get(dev.alceml_bdev)) or bool(rpc.bdev_get(f"{dev.alceml_bdev}_PT"))
    except Exception:                                       # noqa: BLE001
        return None


# ------------------------------------------------------------- operator

def unfence_node(device_id, node_id) -> bool:
    """Give ``node_id`` access to the LUN of ``device_id`` again and drop it
    from ``fenced_node_ids``. Called by :func:`reintegrate_fenced_node` once
    the node's host shows no disk of the LUN, else by an operator once the
    host is known to hold no queued writes for it (its multipath queues
    drained or the host rebooted). Returns True on success."""
    db = DBController()
    try:
        dev = db.get_storage_device_by_id(device_id)
        owner = db.get_storage_node_by_id(dev.node_id)
        node = db.get_storage_node_by_id(node_id)
        cluster = db.get_cluster_by_id(owner.cluster_id)
    except KeyError as e:
        logger.error("Unfence of %s on %s: %s", node_id, device_id, e)
        return False
    if dev.serving_node() == node_id:
        logger.error("Unfence of %s on %s refused: it is the serving node", node_id, device_id)
        return False

    # The record first, the array second. The attachment reconcile
    # (array_controller.reconcile_lun_attachments, health check) detaches
    # every host the record still lists as fenced: attaching first left a
    # window in which it detached the LUN again (2026-11-19 FTT1 bed, kill of
    # an owner's SPDK). Once the record is clear the reconcile attaches the
    # node itself, so a failed array call below is retried there.
    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or node_id not in d.fenced_node_ids:
            return False
        d.fenced_node_ids = [x for x in d.fenced_node_ids if x != node_id]
        return True

    db.atomic_update(owner, _mut)
    _event(owner.cluster_id, dev, f"Node {node_id} unfenced from LUN {dev.lun_wwn} of device {device_id}")
    try:
        if not array.plugin_for_cluster(cluster).unfence(
                multi_attach.array_lun_ref(dev), multi_attach.array_host_ref(node)):
            logger.warning("Unfence of %s on LUN %s not acknowledged by the array; the "
                           "attachment reconcile presents it", node_id, dev.lun_wwn)
    except Exception as e:                                  # noqa: BLE001
        logger.warning("Array unfence of %s on LUN %s failed (%s); the attachment "
                       "reconcile presents it", node_id, dev.lun_wwn, e)
    return True


# ---------------------------------------------------------- warm standby

# node id -> the device ids whose warm standby did not fit its memory budget
# when last reported (an event is raised when the set changes).
_warm_over_budget: dict[str, frozenset] = {}


def ensure_warm_standbys(node) -> dict:
    """Keep the warm standby alcemls of an ONLINE node in line with the
    record (``Cluster.multi_attach_warm_standby`` clusters only).

    For every LUN the node is a standby of (multi_attach.warm_standby_devices:
    attached, not serving, not fenced, the device ONLINE), open a standby
    alceml (``standby=true``, open-only, the reservation key, the same
    parameters as a takeover) over the node's own path to the LUN, unless one
    is open already. It is never exported: a peer's I/O would activate it.

    Close the standby alceml (and, on a node that does not own the device,
    its base bdev) of every LUN the node is no longer a standby of (fenced,
    detached, removed). Never touches the serving node's stack, an exported
    alceml (a PT bdev over it: retire_stale_local_stack handles those) or an
    active instance, nor a device whose takeover runs in this process.

    A standby holds the LUN's page map in the node's SPDK memory: one is not
    opened when the node's budget (StorageNode.minimum_sys_memory, its
    container limit) does not cover its own devices plus its standbys
    (utils.sys_memory_for_capacity); a cluster event says so instead. A
    restart sizes the budget for them (storage_node_ops).

    Returns ``{"created": [...], "removed": [...], "over_budget": [...]}``
    (device ids); best-effort and idempotent, never raises."""
    report: dict = {"created": [], "removed": [], "over_budget": []}
    try:
        _ensure_warm_standbys(node, report)
    except Exception as e:                                  # noqa: BLE001
        logger.error("Warm standbys of %s not reconciled: %s", node.get_id(), e)
    return report


def _ensure_warm_standbys(node, report):
    node_id = node.get_id()
    db = DBController()
    cluster = db.get_cluster_by_id(node.cluster_id)
    if not (multi_attach.is_multi_attach(cluster)
            and getattr(cluster, "multi_attach_warm_standby", False) is True):
        return
    nodes = db.get_storage_nodes_by_cluster_id(node.cluster_id)
    node = next((n for n in nodes if n.get_id() == node_id), node)
    if node.status != StorageNode.STATUS_ONLINE:
        return
    keep = {dev.get_id() for _, dev in multi_attach.warm_standby_devices(
        cluster, node_id, nodes, multi_attach._WARM_STANDBY_BUDGET_STATES)}
    want = multi_attach.warm_standby_devices(cluster, node_id, nodes)
    try:
        rpc = node.rpc_client(timeout=10, retry=1)
    except Exception as e:                                  # noqa: BLE001
        logger.info("Warm standbys: no RPC client for %s: %s", node_id, e)
        return

    # Close what the node is no longer a standby of.
    for owner in nodes:
        for dev in owner.nvme_devices or []:
            if (not multi_attach.is_multi_attach(dev) or dev.get_id() in keep
                    or dev.serving_node() == node_id or _local_lock(dev.get_id()).locked()):
                continue
            if _remove_warm_standby(rpc, node, dev):
                report["removed"].append(dev.get_id())

    # Open what is missing, within the node's memory budget.
    committed = sum(max(int(d.size or 0), 0) for d in node.nvme_devices or [])
    committed += sum(max(int(d.size or 0), 0) for o, d in multi_attach.devices_served_by(nodes, node_id)
                     if o.get_id() != node_id)
    missing = []
    for _owner, dev in want:
        try:
            held = bool(rpc.bdev_get(dev.alceml_bdev))
        except Exception as e:                              # noqa: BLE001
            logger.info("Warm standbys: SPDK of %s not answering: %s", node_id, e)
            return
        if held:
            committed += max(int(dev.size or 0), 0)
        else:
            missing.append(dev)
    budget = int(getattr(node, "minimum_sys_memory", 0) or 0)
    for dev in missing:
        if _local_lock(dev.get_id()).locked():
            continue
        size = max(int(dev.size or 0), 0)
        if budget and utils.sys_memory_for_capacity(committed + size) > budget:
            report["over_budget"].append(dev.get_id())
            continue
        if _create_warm_standby(rpc, cluster, node, dev):
            report["created"].append(dev.get_id())
            committed += size
    _report_over_budget(cluster, node, report["over_budget"], committed, budget)


def _create_warm_standby(rpc, cluster, node, dev) -> bool:
    """Open the standby alceml of ``dev`` on ``node``; False (logged) when the
    node cannot read the LUN or SPDK refuses it. Closes what it opened on
    failure."""
    node_id = node.get_id()
    kwargs = multi_attach.alceml_create_kwargs(dev, node, cluster)
    if not kwargs.get("reservation_key") or getattr(dev, "bdev_type", "aio") != "aio":
        return False
    path, why = _probe(node, dev, time.monotonic() + 15)
    if not path:
        logger.info("Warm standby of %s on %s not opened: %s", dev.get_id(), node_id, why)
        return False
    base = utils.aio_bdev_name_for_serial(dev.serial_number)
    created = []
    try:
        if not rpc.bdev_get(base):
            _create_base_bdev(rpc, base, path, dev)
            created.append(("base", base))
        info = rpc.bdev_get(base) or {}
        size = int(info.get("block_size", 0)) * int(info.get("num_blocks", 0))
        if size < int(dev.size or 0):
            raise TakeoverError(f"base bdev {base} is {size} bytes, the device {dev.size}")
        ok = node.create_alceml(dev.alceml_bdev, base, dev.get_id(),
                                standby=True,
                                refresh_interval_ms=constants.MULTI_ATTACH_WARM_REFRESH_MS,
                                **_alceml_open_kwargs(cluster, dev, kwargs))
        if not ok:
            raise TakeoverError(f"standby alceml {dev.alceml_bdev} not opened")
        created.append(("alceml", dev.alceml_bdev))
    except Exception as e:                                  # noqa: BLE001
        logger.warning("Warm standby of %s on %s not opened: %s", dev.get_id(), node_id, e)
        _teardown(rpc, node_id, created)
        return False
    logger.info("Warm standby of %s (LUN %s) opened on %s at %s",
                dev.get_id(), dev.lun_wwn, node_id, path)
    return True


def _remove_warm_standby(rpc, node, dev) -> bool:
    """Close a standby alceml of ``dev`` the node holds although it is no
    longer a standby of the LUN; its base bdev too unless the node owns the
    device. Only an unexported standby instance is closed."""
    alceml = dev.alceml_bdev
    try:
        if not alceml or not rpc.bdev_get(alceml):
            return False
        if rpc.bdev_get(dev.pt_bdev or f"{alceml}_PT"):
            return False
    except Exception:                                       # noqa: BLE001
        return False
    if _alceml_role(node, alceml).get("role") != "standby":
        return False
    todo = [("alceml", alceml)]
    if dev.node_id != node.get_id():
        todo.insert(0, ("base", utils.aio_bdev_name_for_serial(dev.serial_number)))
    _teardown(rpc, node.get_id(), todo)
    logger.info("Warm standby of %s closed on %s: no longer a standby of LUN %s",
                dev.get_id(), node.get_id(), dev.lun_wwn)
    return True


def _report_over_budget(cluster, node, device_ids, committed, budget):
    """A cluster event (once per change) when warm standbys of the node did
    not fit its memory budget."""
    node_id = node.get_id()
    now = frozenset(device_ids)
    if _warm_over_budget.get(node_id, frozenset()) == now:
        return
    _warm_over_budget[node_id] = now
    if not now:
        return
    msg = (f"Node {node_id}: warm standbys of {len(now)} LUN(s) ({', '.join(sorted(now))}) not "
           f"opened: its SPDK memory budget {utils.humanbytes(budget)} covers its own devices and "
           f"standbys of {utils.humanbytes(committed)} only. Restart the node to size its memory "
           f"for its standbys; a takeover of these LUNs opens them cold meanwhile.")
    logger.warning(msg)
    try:
        ec.log_event_cluster(
            cluster_id=cluster.get_id(),
            domain=ec.DOMAIN_CLUSTER,
            event=ec.EVENT_LUN_TAKEOVER,
            db_object=node,
            caused_by=ec.CAUSED_BY_MONITOR,
            message=msg,
            node_id=node_id,
            event_level=EventObj.LEVEL_WARN)
    except Exception as e:                                  # noqa: BLE001
        logger.error("Warm standby budget event of %s not logged: %s", node_id, e)
