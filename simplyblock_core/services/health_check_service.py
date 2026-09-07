import concurrent.futures
import threading
import time
from datetime import datetime

from simplyblock_core import utils
from simplyblock_core.controllers import health_controller, storage_events, device_events, tasks_controller
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core import constants, db_controller, storage_node_ops


utils.init_sentry_sdk()
logger = utils.get_logger(__name__)


def set_node_health_check(snode, health_check_status):
    snode = db.get_storage_node_by_id(snode.get_id())
    if snode.health_check == health_check_status:
        return
    now = str(datetime.now())
    # Atomic compare-and-set: a plain read-modify-write here serializes the WHOLE
    # node row and clobbers a concurrent status flip (e.g. set_node_status's
    # in_shutdown->offline) — the lost-update that wedged a node in in_shutdown
    # (incident 2026-06-18). Mutate only health_check on the freshly-read row.
    outcome = {"old": None, "changed": False}

    def _mut(n):
        if n.health_check == health_check_status:
            return False
        outcome["old"] = n.health_check
        outcome["changed"] = True
        n.health_check = health_check_status
        n.updated_at = now
        return True

    snode = db.atomic_update(snode, _mut)
    # health_check_status is None when health is not applicable (node not in
    # ONLINE/DOWN). That is not a real health transition, so don't emit a
    # health-change event for it — only fire for true/false results.
    if snode is not None and outcome["changed"] and health_check_status is not None:
        storage_events.snode_health_check_change(snode, snode.health_check, outcome["old"], caused_by="monitor")


def set_device_health_check(cluster_id, device, health_check_status):
    if device.health_check == health_check_status:
        return
    nodes = db.get_storage_nodes_by_cluster_id(cluster_id)
    for node in nodes:
        if node.nvme_devices:
            for dev in node.nvme_devices:
                if dev.get_id() == device.get_id():
                    old_status = dev.health_check
                    # Atomic compare-and-set: re-read the node fresh INSIDE the
                    # FDB tx and mutate only this device's health_check, so a
                    # concurrent node.status / lvstore_ports change is preserved
                    # (a full write_to_db would clobber it — incident 2026-06-18).
                    did = device.get_id()

                    def _mut(n, did=did):
                        for fresh_dev in n.nvme_devices:
                            if fresh_dev.get_id() == did:
                                fresh_dev.health_check = health_check_status
                                return True
                        return False
                    db.atomic_update(db.get_storage_node_by_id(node.get_id()), _mut)
                    # None => health not applicable (owning node not ONLINE/DOWN);
                    # not a real health transition, so don't emit an event.
                    if health_check_status is not None:
                        device_events.device_health_check_change(
                            dev, health_check_status, old_status, caused_by="monitor")
                    return


# Per-node memo of the last remote-device sweep: node_id -> (topology_epoch,
# monotonic ts). sync_remote_devices_from_spdk pays one SPDK inventory RPC per
# call; running it on EVERY pass for EVERY node is pure idle load once the
# topology is stable (run 20260725: the sweep fired every ~40-50s per SPDK
# instance around the clock). The sweep only changes its answer when peer
# node/device topology changes, so gate it on a topology epoch with a forced
# floor so drift (manual attach, missed event) is still reconciled.
_remote_sweep_memo: dict = {}


def _peer_topology_epoch(snode, cluster_nodes):
    """Cheap fingerprint of everything sync_remote_devices_from_spdk reads:
    peer node ids+statuses and their device ids+statuses+alceml names."""
    parts = []
    for peer in cluster_nodes:
        if peer.get_id() == snode.get_id():
            continue
        parts.append(peer.get_id())
        parts.append(peer.status)
        for dev in peer.nvme_devices:
            parts.append(dev.get_id())
            parts.append(dev.status)
            parts.append(dev.alceml_bdev or "")
    return hash(tuple(parts))


def _remote_sweep_due(snode, cluster_nodes):
    epoch = _peer_topology_epoch(snode, cluster_nodes)
    memo = _remote_sweep_memo.get(snode.get_id())
    now = time.monotonic()
    if memo is not None and memo[0] == epoch and \
            (now - memo[1]) < constants.HEALTH_CHECK_REMOTE_SWEEP_FORCE_SEC:
        return False
    _remote_sweep_memo[snode.get_id()] = (epoch, now)
    return True


#: Repairs of independent objects have no reason to wait for each other. A
#: cycle used to dial every degraded controller in series, so a node with a
#: dozen degraded remote devices spent that many round-trips before its
#: hublvol was even looked at.
REPAIR_FANOUT = 8


#: owner_node_id -> {(kind, ctrl_name, device_id, target_node_id)}
#:
#: A repair skipped because its owner cannot answer must be *deferred*, not
#: dropped. On 2026-09-01 node 22f365ef logged "Multipath repair skipped ...
#: owner is in_restart" 15 times while its peers restarted, then never retried
#: once they came back: re-detection depends on the remote bdev still looking
#: present and the device still reading ONLINE, and with the controller half
#: torn down it did not. The controller stayed single-pathed -- its sibling
#: stuck in "deleting" -- until the node was restarted 20 minutes later.
#:
#: Keyed by owner, because the owner's status is what unblocks the repair.
_deferred_repairs: dict[str, set[tuple[str, str, str, str]]] = {}
_deferred_repairs_lock = threading.Lock()


def _defer_repair(owner_id, kind, ctrl_name, device_id, target_node_id):
    """Remember a repair that could not run because its owner was mid-transition."""
    if not (owner_id and kind and ctrl_name and device_id and target_node_id):
        return
    entry = (kind, ctrl_name, device_id, target_node_id)
    with _deferred_repairs_lock:
        owed = _deferred_repairs.setdefault(owner_id, set())
        if entry in owed:
            return
        owed.add(entry)
    logger.info("Deferred %s repair of %s on %s until owner %s can answer",
                kind, ctrl_name, target_node_id[:8], owner_id[:8])


def _drain_deferred_repairs(target_node_id):
    """Pop repairs owed on ``target_node_id`` whose owner can now be dialled.

    Returns ``(kind, ctrl_name, device_id)`` tuples. Device objects are
    deliberately NOT cached here and are re-resolved by the caller: a record
    held across a peer restart is exactly the stale copy that reconnects to a
    port the peer no longer listens on.
    """
    ready = []
    with _deferred_repairs_lock:
        for owner_id in list(_deferred_repairs):
            try:
                owner = db.get_storage_node_by_id(owner_id)
            except Exception:  # noqa: BLE001 - owner gone; drop what was owed
                _deferred_repairs.pop(owner_id, None)
                continue
            if owner is None:
                _deferred_repairs.pop(owner_id, None)
                continue
            if not health_controller.repairs_allowed(owner):
                continue
            remaining = set()
            for kind, ctrl_name, device_id, tgt in _deferred_repairs[owner_id]:
                if tgt == target_node_id:
                    ready.append((kind, ctrl_name, device_id))
                else:
                    remaining.add((kind, ctrl_name, device_id, tgt))
            if remaining:
                _deferred_repairs[owner_id] = remaining
            else:
                _deferred_repairs.pop(owner_id, None)
    if ready:
        logger.info("Re-driving %d deferred repair(s) on %s: owner(s) reachable again",
                    len(ready), target_node_id[:8])
    return ready


def _run_repairs_in_parallel(jobs, what):
    """Run ``(ctrl_name, device, node)`` multipath repairs concurrently."""
    if not jobs:
        return
    workers = min(REPAIR_FANOUT, len(jobs))
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(storage_node_ops.repair_multipath_controller, ctrl, dev, node): ctrl
            for ctrl, dev, node in jobs
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.warning("Multipath repair failed for %s %s: %s",
                               what, futures[future], e)


def check_node(snode):

    try:
        snode = db.get_storage_node_by_id(snode.get_id())
    except KeyError:
        return

    try:
        cluster = db.get_cluster_by_id(snode.cluster_id)
    except KeyError:
        cluster = None

    logger.info("Node: %s, status %s", snode.get_id(), snode.status)

    # Nodes that are being torn down / rebuilt or removed (in_restart,
    # in_shutdown, offline, schedulable, removed, in_creation) have transient
    # data-plane state. Don't run the check at all and mark health (node + its
    # devices) "not applicable" (None).
    if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_UNREACHABLE,
                            StorageNode.STATUS_SUSPENDED, StorageNode.STATUS_DOWN]:
        logger.info(f"Node status is: {snode.status}, health check not applicable")
        set_node_health_check(snode, None)
        for device in snode.nvme_devices:
            set_device_health_check(snode.cluster_id, device, None)
        return

    # Health is *reported* (true/false) only for ONLINE/DOWN nodes. UNREACHABLE
    # and SUSPENDED nodes still run the check pass below — it performs self-heal
    # (reconnecting remote devices, repairing multipath, recreating hublvols) —
    # but their node/device health is reported as "not applicable" (None),
    # never true/false.
    report_health = snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED]

    # Three independent probes (ping, API, RPC), each a network round-trip to a
    # node that may be timing out. Run together: in series, a node slow on all
    # three delays every repair behind it by the sum of the three.
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        f_ping = pool.submit(health_controller._check_node_ping, snode.mgmt_ip)
        f_api = pool.submit(health_controller._check_node_api, snode)
        f_rpc = pool.submit(health_controller.check_node_rpc, snode)

        def _probe(future, label):
            try:
                return future.result()
            except Exception as e:
                logger.error("Probe %s failed for %s: %s", label, snode.mgmt_ip, e)
                return False

        ping_check = _probe(f_ping, "ping")
        node_api_check = _probe(f_api, "api")
        node_rpc_check = _probe(f_rpc, "rpc")
    logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")
    logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")
    logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

    is_node_online = ping_check and node_api_check and node_rpc_check

    health_check_status = is_node_online
    if node_rpc_check:
        # The two object classes are independent: local devices, remote devices
        # and remote JMs on one side; the lvstore chain (hublvols) and its
        # subsystem ports on the other. They inspect disjoint objects and each
        # only contributes its own verdict, so they run concurrently. In series,
        # a hublvol missing a path waited behind every degraded device
        # controller on the node before anyone even looked at it.
        #
        # snode is rebound as a default argument so each group re-reads the
        # node into its own local, rather than racing on the enclosing name.
        def _group_devices(snode=snode):
            logger.info(f"Node device count: {len(snode.nvme_devices)}")
            node_devices_check = True
            node_remote_devices_check = True

            rpc_client = snode.rpc_client(timeout=3, retry=2)
            connected_devices = []
            # Dial-outs are collected while the cheap read-only inspection runs and
            # then executed as one group, rather than one at a time inside the loop.
            device_repair_jobs = []
            jm_repair_jobs = []

            for device in snode.nvme_devices:
                if device.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                    logger.info(f"Device skipped: {device.get_id()} status: {device.status}")
                    continue

                passed = True

                if device.io_error:
                    logger.info(f"Device io_error {device.get_id()}")
                    passed = False

                if device.status != NVMeDevice.STATUS_ONLINE:
                    logger.info(f"Device status {device.status}")
                    passed = False

                if snode.enable_test_device:
                    bdevs_stack = [device.nvme_bdev, device.testing_bdev, device.alceml_bdev, device.pt_bdev]
                else:
                    bdevs_stack = [device.nvme_bdev, device.alceml_bdev, device.pt_bdev]

                logger.info(f"Checking Device: {device.get_id()}, status:{device.status}")
                problems = 0
                for bdev in bdevs_stack:
                    if not bdev:
                        continue

                    if not health_controller.check_bdev(bdev, rpc_client=rpc_client):
                        problems += 1
                        passed = False

                logger.info(f"Checking Device's BDevs ... ({(len(bdevs_stack) - problems)}/{len(bdevs_stack)})")

                passed &= health_controller.check_subsystem(device.nvmf_nqn, rpc_client=rpc_client)

                set_device_health_check(snode.cluster_id, device, passed if report_health else None)
                if device.status == NVMeDevice.STATUS_ONLINE:
                    node_devices_check &= passed

            # Topology-gated: the sweep pays an SPDK inventory RPC; skip it while
            # peer topology is unchanged (forced floor keeps drift bounded).
            if _remote_sweep_due(snode, db.get_storage_nodes_by_cluster_id(snode.cluster_id)):
                if storage_node_ops.sync_remote_devices_from_spdk(snode):
                    snode = db.get_storage_node_by_id(snode.get_id())

            # Reconcile against cluster topology. node.remote_devices is rebuilt
            # as "whatever was reachable at that moment" by the restart /
            # port-allow paths, so a peer that was unreachable while this node
            # restarted (e.g. network outage) gets silently dropped from the
            # list — and the list-driven loop below can then never see, check,
            # or repair the missing connection. Gate on cluster status like the
            # remote-JM rebuild below, to stay out of activation's way.
            if cluster is not None and cluster.status in [
                    Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
                reconnected, reconcile_ok = storage_node_ops.reconnect_dropped_remote_devs(snode)
                if reconnected:
                    snode = db.get_storage_node_by_id(snode.get_id())
                node_remote_devices_check &= reconcile_ok

            logger.info(f"Node remote device: {len(snode.remote_devices)}")

            for remote_device in snode.remote_devices:
                org_dev = db.get_storage_device_by_id(remote_device.get_id())
                org_node = db.get_storage_node_by_id(remote_device.node_id)
                # Only treat a missing remote device as a fault when the owning node
                # is ONLINE/DOWN/UNREACHABLE. If the owner is mid-transition (restart,
                # shutdown, ...) the connection is expected to be gone — skip it.
                if org_dev.status == NVMeDevice.STATUS_ONLINE and health_controller._peer_connections_relevant(org_node):
                    if health_controller.check_bdev(remote_device.remote_bdev, rpc_client=rpc_client):
                        # Bdev exists but multipath may be degraded — repair missing
                        # paths, but only while the owner can actually answer a
                        # connect. _peer_connections_relevant also admits
                        # UNREACHABLE, which is fine for judging whether a missing
                        # connection is a fault and wrong for deciding to dial out.
                        if org_dev.nvmf_multipath:
                            ctrl_name = f"remote_{org_dev.alceml_bdev}" if org_dev.alceml_bdev else None
                            if ctrl_name and health_controller.repairs_allowed(org_node):
                                device_repair_jobs.append((ctrl_name, org_dev, snode))
                            elif ctrl_name:
                                _defer_repair(remote_device.node_id, "device", ctrl_name,
                                              org_dev.get_id(), snode.get_id())
                        connected_devices.append(remote_device.get_id())
                        continue

                    if not org_dev.alceml_bdev:
                        logger.error(f"device alceml bdev not found!, {org_dev.get_id()}")
                        continue

                    if not health_controller.repairs_allowed(org_node):
                        # Judged a fault above (that uses the wider relevance
                        # test), but dialling out to a node that cannot answer is
                        # pointless; the next cycle retries once it is ONLINE/DOWN.
                        logger.info(
                            "Device connect skipped for %s: owner %s is %s",
                            org_dev.get_id(), remote_device.node_id, org_node.status)
                        continue

                    try:
                        storage_node_ops.connect_device(
                            f"remote_{org_dev.alceml_bdev}", org_dev, snode)
                        connected_devices.append(org_dev.get_id())
                    except RuntimeError:
                        logger.error(f"Failed to connect to device: {org_dev.get_id()}")
                        node_remote_devices_check = False

            connected_jms = []
            if snode.jm_device and snode.jm_device.get_id():
                jm_device = snode.jm_device
                logger.info(f"Node JM: {jm_device.get_id()}")
                if rpc_client.get_bdevs(jm_device.jm_bdev):
                    logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... ok")
                    connected_jms.append(jm_device.get_id())
                else:
                    logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... not found")

            if snode.enable_ha_jm:
                logger.info(f"Node remote JMs: {len(snode.remote_jm_devices)}")
                for remote_device in snode.remote_jm_devices:
                    if remote_device.remote_bdev:
                        check = health_controller.check_bdev(remote_device.remote_bdev, rpc_client=rpc_client)
                        if check:
                            # JM bdev exists but multipath may be degraded — repair missing paths.
                            # repair_multipath_controller needs nvmf_ip / nvmf_nqn / nvmf_port
                            # which RemoteJMDevice strips. Resolve the source JMDevice on the
                            # owning node before calling — otherwise the repair raises
                            # AttributeError("'RemoteJMDevice' object has no attribute 'nvmf_ip'")
                            # every cycle and JM controllers that lose a path during NIC chaos
                            # are NEVER repaired by the health service.
                            if remote_device.nvmf_multipath:
                                ctrl_name = remote_device.remote_bdev.replace("n1", "")
                                try:
                                    src_node = db.get_storage_node_by_id(remote_device.node_id)
                                    src_jm = src_node.jm_device if src_node else None
                                    if not health_controller.repairs_allowed(src_node):
                                        logger.info(
                                            "Multipath repair skipped for JM %s: owner %s is %s",
                                            ctrl_name, remote_device.node_id,
                                            getattr(src_node, "status", "unknown"))
                                        _defer_repair(remote_device.node_id, "JM", ctrl_name,
                                                      remote_device.get_id(), snode.get_id())
                                    elif src_jm and getattr(src_jm, 'nvmf_ip', None):
                                        jm_repair_jobs.append((ctrl_name, src_jm, snode))
                                    else:
                                        logger.warning(
                                            "Multipath repair skipped for JM %s: source JMDevice unavailable",
                                            ctrl_name)
                                except Exception as e:
                                    logger.warning("Multipath repair failed for JM %s: %s", ctrl_name, e)
                            connected_jms.append(remote_device.get_id())
                        else:
                            # Only fail health when the JM's owning node is
                            # ONLINE/DOWN/UNREACHABLE. If it's mid-transition the
                            # remote JM bdev is expected to be missing.
                            try:
                                jm_owner = db.get_storage_node_by_id(remote_device.node_id)
                            except KeyError:
                                jm_owner = None
                            if health_controller._peer_connections_relevant(jm_owner):
                                node_remote_devices_check = False
                            else:
                                logger.info(
                                    "Remote JM %s missing, but owning node %s is %s — expected",
                                    remote_device.remote_bdev, remote_device.node_id,
                                    jm_owner.status if jm_owner else "not-found")

                # The expected remote-JM set is topology-derived: the node's own
                # JM quorum (jm_ids) plus the JMs of every primary this node is
                # secondary/tertiary for — the same sources _connect_to_remote_
                # jm_devs rebuilds from. Detecting only jm_ids left secondary->
                # primary JM connections that were dropped during a restart-
                # while-outage unnoticed forever.
                expected_jm_ids = {jm_id for jm_id in snode.jm_ids if jm_id}
                for sec_attr in ('lvstore_stack_secondary', 'lvstore_stack_tertiary'):
                    sec_primary_id = getattr(snode, sec_attr, None)
                    if not sec_primary_id:
                        continue
                    try:
                        org_node = db.get_storage_node_by_id(sec_primary_id)
                    except KeyError:
                        continue
                    if org_node.jm_device and org_node.jm_device.get_id():
                        expected_jm_ids.add(org_node.jm_device.get_id())
                    expected_jm_ids.update(jm_id for jm_id in org_node.jm_ids if jm_id)

                for jm_id in expected_jm_ids:
                    if jm_id not in connected_jms:
                        for nd in db.get_storage_nodes():
                            if nd.jm_device and nd.jm_device.get_id() == jm_id:
                                if health_controller._peer_connections_relevant(nd):
                                    node_remote_devices_check = False
                                else:
                                    logger.info(
                                        "JM device %s not connected, but owning node %s is %s — expected",
                                        jm_id, nd.get_id(), nd.status)
                                break

                if not node_remote_devices_check and cluster is not None and cluster.status in [
                    Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
                    remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(snode)
                    snode = db.atomic_update(
                        db.get_storage_node_by_id(snode.get_id()),
                        lambda n, rd=remote_jm_devices: setattr(n, "remote_jm_devices", rd))

            # Re-drive whatever was skipped while its owner was mid-transition
            # and is now reachable. Everything is re-resolved from the DB.
            for kind, ctrl_name, device_id in _drain_deferred_repairs(snode.get_id()):
                try:
                    if kind == "JM":
                        # RemoteJMDevice strips nvmf_ip/nqn/port, so resolve the
                        # source JMDevice on the owning node -- same rule as the
                        # inline path above.
                        for rjm in snode.remote_jm_devices:
                            if rjm.get_id() != device_id:
                                continue
                            src = db.get_storage_node_by_id(rjm.node_id)
                            src_jm = src.jm_device if src else None
                            if src_jm and getattr(src_jm, "nvmf_ip", None):
                                jm_repair_jobs.append((ctrl_name, src_jm, snode))
                            break
                    else:
                        dev = db.get_storage_device_by_id(device_id)
                        if dev is not None:
                            device_repair_jobs.append((ctrl_name, dev, snode))
                except Exception as e:  # noqa: BLE001 - one bad entry must not stall the cycle
                    logger.warning("Could not re-drive deferred repair of %s: %s",
                                   ctrl_name, e)

            _run_repairs_in_parallel(device_repair_jobs, "device")
            _run_repairs_in_parallel(jm_repair_jobs, "JM")

            return node_devices_check, node_remote_devices_check

        def _group_lvstore(snode=snode):
            lvstore_check = True
            snode = db.get_storage_node_by_id(snode.get_id())
            if snode.lvstore_status == "ready" or snode.status == StorageNode.STATUS_ONLINE or \
                    snode.lvstore_status == "failed":

                lvstore_stack = snode.lvstore_stack
                lvstore_check &= health_controller._check_node_lvstore(
                    lvstore_stack, snode, auto_fix=True)

                sec_ids_to_check = []
                if snode.secondary_node_id:
                    sec_ids_to_check.append(snode.secondary_node_id)
                if snode.tertiary_node_id:
                    sec_ids_to_check.append(snode.tertiary_node_id)

                if sec_ids_to_check:

                    lvstore_check &= health_controller._check_node_hublvol(snode)

                    for sec_id in sec_ids_to_check:
                        sec_node = db.get_storage_node_by_id(sec_id)
                        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
                            lvstore_check &= health_controller._check_node_lvstore(
                                lvstore_stack, sec_node, auto_fix=True, stack_src_node=snode)
                            # repair_paths=True on the first pass: a hublvol
                            # missing one of its two paths passes the existence
                            # check below, so nesting path repair inside the
                            # failure branch meant it never ran.
                            sec_node_check = health_controller._check_sec_node_hublvol(
                                sec_node, primary_node_id=snode.get_id(),
                                repair_paths=True)
                            if not sec_node_check:
                                if snode.status == StorageNode.STATUS_ONLINE:
                                    ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                                    if ret:
                                        lvs_info = ret[0]
                                        if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                                            jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(
                                                snode.jm_vuid)
                                            if not jc_compression_is_active:
                                                lvstore_check &= health_controller._check_sec_node_hublvol(
                                                    sec_node, auto_fix=True, primary_node_id=snode.get_id())

                lvol_port_check = False
                # if node_api_check:
                ports = [snode.get_lvol_subsys_port(snode.lvstore)]

                for sec_stack_ref in [snode.lvstore_stack_secondary, snode.lvstore_stack_tertiary]:
                    if sec_stack_ref:
                        try:
                            sec_ref_node = db.get_storage_node_by_id(sec_stack_ref)
                            if sec_ref_node and sec_ref_node.status == StorageNode.STATUS_ONLINE:
                                ports.append(sec_ref_node.get_lvol_subsys_port(sec_ref_node.lvstore))
                        except KeyError:
                            pass

                # Batched: one nvmf_get_blocked_ports fetch answers every port
                # (was one identical full-list fetch PER port — 528/min
                # cluster-wide at idle, 2026-07-21 baseline audit).
                try:
                    _port_results = health_controller.check_ports_on_node(snode, ports)
                    for port, lvol_port_check in _port_results.items():
                        logger.info(
                            f"Check: node {snode.mgmt_ip}, port: {port} ... {lvol_port_check}")
                        if not lvol_port_check and snode.status != StorageNode.STATUS_SUSPENDED:
                            tasks_controller.add_port_allow_task(snode.cluster_id, snode.get_id(), port)
                except Exception as e:
                    for port in ports:
                        health_controller._log_port_check_failure(db, snode, port, e)

            return lvstore_check

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            future_devices = pool.submit(_group_devices)
            future_lvstore = pool.submit(_group_lvstore)
            try:
                node_devices_check, node_remote_devices_check = future_devices.result()
            except Exception as e:
                logger.error("Device checks failed on %s: %s", snode.get_id(), e)
                node_devices_check = node_remote_devices_check = False
            try:
                lvstore_check = future_lvstore.result()
            except Exception as e:
                logger.error("Lvstore checks failed on %s: %s", snode.get_id(), e)
                lvstore_check = False

        health_check_status = is_node_online and node_devices_check and node_remote_devices_check and lvstore_check
    # Report true/false only for ONLINE/DOWN; UNREACHABLE/SUSPENDED ran the
    # self-heal pass above but their health stays "not applicable" (None).
    # No sleep here: loop_for_node() owns the polling cadence (30s healthy /
    # 5s recovering). A trailing sleep in this function stacked on the loop's
    # own sleep and doubled the effective cycle to ~60s, so a self-blocked
    # client port could stay undetected past the kernel's 60s nvme Connect
    # budget (incident 2026-07-02 19:49:20).
    set_node_health_check(snode, bool(health_check_status) if report_health else None)


def loop_for_node(snode):
    while True:
        try:
            # Refresh so we see status transitions since the last iteration
            # — the adaptive interval below keys off node.status.
            snode = db.get_storage_node_by_id(snode.get_id())
            check_node(snode)
        except KeyError:
            # Node was deleted from the DB; nothing to poll.
            return
        except Exception as e:
            logger.error(e)
        # Poll faster when the node isn't ONLINE so the state machine sees
        # the recovery transition as soon as it happens (recovery is time-
        # critical; healthy-node polling stays at the normal 30 s cadence).
        if snode.status == StorageNode.STATUS_ONLINE:
            time.sleep(constants.HEALTH_CHECK_INTERVAL_SEC)
        else:
            time.sleep(constants.HEALTH_CHECK_FAST_INTERVAL_SEC)


db = db_controller.DBController()
threads_maps: dict[str, threading.Thread] = {}


def main():
    logger.info("Starting health check service")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        for cluster in clusters:
            for node in db.get_storage_nodes_by_cluster_id(cluster.get_id()):
                node_id = node.get_id()
                if node_id not in threads_maps or threads_maps[node_id].is_alive() is False:
                    t = threading.Thread(target=loop_for_node, args=(node,))
                    t.start()
                    threads_maps[node_id] = t

        time.sleep(constants.HEALTH_CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    main()
