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
   block 0 carries the alceml signature (read-only);
4. on the standby builds the base bdev, the alceml OPEN-ONLY
   (``pba_init_mode=1``; 2/3 would format the LUN and destroy its data), the
   PT bdev and the device subsystem under the same NQN, with the standby's
   controller-ID range, the same namespace UUID and the standby's listeners;
5. records the standby as the serving node (atomically; the device stays
   ONLINE, no unavailable broadcast, no cluster-map change);
6. adds the standby's export as a path of ``remote_alceml_<uuid>`` on every
   ONLINE node (the standby too: its loopback), where the held I/O resumes;
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
"""

from __future__ import annotations

import logging
import os
import socket
import threading
import time
import uuid as uuid_mod

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
_local_locks_guard = threading.Lock()


#: Outcomes of :func:`take_over_lun_outcome`.
TAKEN_OVER = "taken_over"          # served by a live node other than the failed one
IN_PROGRESS = "in_progress"        # another takeover of this LUN is running
NO_STANDBY = "no_standby"          # fenced, but no standby can read the LUN: hold
FAILED = "failed"                  # not taken over (no plugin, fence, export, ...)


class TakeoverError(Exception):
    """A takeover step failed; the message says which and why."""


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

    try:
        # 1. Fence -- before anything else.
        _fence(cluster, dev, failed)
        _record_fenced(db, owner, device_id, failed_node_id)
        _check_deadline(deadline, "choosing a standby")

        # 2 + 3. A standby that can read the LUN.
        standby, path, why = _choose_standby(owner, dev, failed_node_id, nodes_by_id, deadline)
        if standby is None:
            msg = (f"LUN {dev.lun_wwn} of device {device_id} not taken over from node "
                   f"{failed_node_id}: no standby can read it ({why}); holding I/O until "
                   f"the peers' hold expires")
            logger.error(msg)
            _event(cluster_id, dev, msg, EventObj.LEVEL_CRITICAL)
            return NO_STANDBY
        _check_deadline(deadline, f"exporting the LUN on {standby.get_id()}")

        # 4. Open-only export on the standby.
        export = _export_on_standby(db, cluster, standby, dev, path, deadline)
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
    try:
        plugin = array.plugin_for_cluster(cluster)
    except array.ArrayNotConfigured:
        raise TakeoverError(
            "no array plugin is configured (cluster array_plugin); a LUN is never "
            "taken over without fencing its serving node")
    except array.ArrayError as e:
        raise TakeoverError(f"array plugin unavailable: {e}")
    try:
        ok = plugin.fence(lun, host)
    except Exception as e:                                  # noqa: BLE001
        raise TakeoverError(f"array fence of {host} on LUN {lun} failed: {e}")
    if not ok:
        raise TakeoverError(f"array did not acknowledge the fence of {host} on LUN {lun}")
    logger.info("LUN %s: node %s (%s) fenced at the array", lun, failed.get_id(), host)


def _record_fenced(db, owner, device_id, node_id):
    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or node_id in d.fenced_node_ids:
            return False
        d.fenced_node_ids = list(d.fenced_node_ids) + [node_id]
        return True
    db.atomic_update(owner, _mut)


# --------------------------------------------------------- 2 + 3. standby

def _choose_standby(owner, dev, failed_node_id, nodes_by_id, deadline):
    reasons = []
    for nid in multi_attach.standby_candidates(owner, failed_node_id):
        node = nodes_by_id.get(nid)
        if node is None:
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
        path, why = _probe(node, dev, deadline)
        if path:
            logger.info("LUN %s: standby %s reads it at %s", dev.lun_wwn, nid, path)
            return node, path, ""
        reasons.append(f"{nid}: {why}")
        if time.monotonic() > deadline:
            reasons.append("deadline passed")
            break
    return None, None, "; ".join(reasons) or "no standby candidate"


def _probe(node, dev, deadline):
    try:
        result, err = node.client(timeout=_remaining(deadline, 15)).lun_info(dev.lun_wwn)
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


def _export_on_standby(db, cluster, standby, dev, path, deadline) -> dict:
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
        if size < int(dev.size or 0):
            raise TakeoverError(f"base bdev {base} on {sid} is {size} bytes, the device {dev.size}")

        alceml = dev.alceml_bdev or f"alceml_{dev.get_id()}"
        if not rpc.bdev_get(alceml):
            checksum_method, cache_size, cache_eviction = utils.alceml_checksum_params(cluster, dev)
            ok = standby.create_alceml(
                alceml, base, dev.get_id(),
                pba_init_mode=ALCEML_OPEN_ONLY,
                write_protection=cluster.distr_ndcs > 1,
                pba_page_size=cluster.page_size_in_blocks,
                full_page_unmap=cluster.full_page_unmap,
                checksum_method=checksum_method,
                cache_size=cache_size,
                cache_eviction_threshold=cache_eviction,
                force_4k_atomic=getattr(cluster, 'atomic_4k', False),
                **multi_attach.alceml_create_kwargs(dev),
            )
            if not ok:
                raise TakeoverError(f"alceml {alceml} not opened on {sid}")
            created.append(("alceml", alceml))
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
    except Exception:
        _teardown(rpc, sid, created)
        raise
    aliases = (pt_info or {}).get("aliases") or []
    return {
        "nqn": nqn,
        "ips": ips,
        "port": standby.nvmf_port,
        "base": base,
        "pt_uuid": aliases[0] if aliases else "",
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
    ONLINE node, then detach the failed node's path there. Returns the ids
    of the nodes that have the new path."""
    name = f"remote_{dev.alceml_bdev}"
    trtype = _transport(standby) or "TCP"
    new_ips = [ip.strip() for ip in (dev.nvmf_ip or "").split(",") if ip.strip()]
    dead_ips, dead_port, dead_trtype = failed_paths
    dead_ips = [ip for ip in dead_ips if ip not in new_ips]
    failed_id = failed.get_id() if failed is not None else None
    added = []
    for node in db.get_storage_nodes_by_cluster_id(cluster_id):
        if node.status != StorageNode.STATUS_ONLINE or node.get_id() == failed_id:
            continue
        try:
            rpc = node.rpc_client(timeout=5, retry=1)
        except Exception as e:                              # noqa: BLE001
            logger.warning("LUN takeover: no RPC client for %s: %s", node.get_id(), e)
            continue
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
            continue
        added.append(node.get_id())
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
    return added


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


# ------------------------------------------------------------- operator

def unfence_node(device_id, node_id) -> bool:
    """Give ``node_id`` access to the LUN of ``device_id`` again and drop it
    from ``fenced_node_ids``. Never called automatically: only once the
    node's host is known to hold no queued writes for the LUN (its multipath
    queues drained or the host rebooted). Returns True on success."""
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
    try:
        if not array.plugin_for_cluster(cluster).unfence(
                multi_attach.array_lun_ref(dev), multi_attach.array_host_ref(node)):
            logger.error("Unfence of %s on LUN %s not acknowledged", node_id, dev.lun_wwn)
            return False
    except Exception as e:                                  # noqa: BLE001
        logger.error("Unfence of %s on LUN %s failed: %s", node_id, dev.lun_wwn, e)
        return False

    def _mut(n):
        d = _device_on_owner(n, device_id)
        if d is None or node_id not in d.fenced_node_ids:
            return False
        d.fenced_node_ids = [x for x in d.fenced_node_ids if x != node_id]
        return True

    db.atomic_update(owner, _mut)
    _event(owner.cluster_id, dev, f"Node {node_id} unfenced from LUN {dev.lun_wwn} of device {device_id}")
    return True
