# coding=utf-8

from typing import Any
from logging import DEBUG, ERROR, INFO


from simplyblock_core import utils, distr_controller, storage_node_ops
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteDevice
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.controllers import device_controller

logger = utils.get_logger(__name__)

# A node's health is only meaningful when it is ONLINE or DOWN. In every other
# state (offline, in_restart, in_shutdown, unreachable, schedulable, removed,
# in_creation, suspended) the stack is being torn down / rebuilt or the node is
# administratively parked, so we do not compute or report a health value at all.
HEALTH_RELEVANT_NODE_STATUSES = (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN)


def _peer_connections_relevant(peer_node) -> bool:
    """Whether a connection to/from ``peer_node`` should affect health.

    Connections (remote NVMe controllers to devices, remote JMs, hublvols)
    are only expected to exist when the peer node is ONLINE, DOWN or
    UNREACHABLE. When the peer is in any other state (in_restart, in_shutdown,
    offline, schedulable, removed, in_creation, suspended) a missing connection
    is the expected consequence of that peer's teardown/rebuild and MUST NOT
    fail the health check — it is reported (logged) but not counted as a fault.
    """
    return peer_node is not None and peer_node.status in (
        StorageNode.STATUS_ONLINE,
        StorageNode.STATUS_DOWN,
        StorageNode.STATUS_UNREACHABLE,
    )


#: Per-RPC bound for the hublvol attach+connect issued from the health cycle.
#: Generous enough for a connect against a healthy peer, short enough that one
#: unresponsive peer cannot stall the checks for every other node.
HUBLVOL_REPAIR_RPC_TIMEOUT_SEC = 2.0


def repairs_allowed(node) -> bool:
    """Whether repairs may be attempted against ``node`` at all.

    Only ONLINE and DOWN qualify. A node that is unreachable or mid-transition
    cannot answer a fabric connect, so attempting one produces a guaranteed
    failure and nothing else: during the multipath soak on 2026-08-20 the
    device repair fired 1372 times in four hours, 478 of them returning
    "-5 Input/output error", every burst falling inside a NIC-down window
    (100 failures in the minute of the 16:33 outage alone).

    Note this cannot suppress every futile attempt. A node whose *data* NIC is
    down still answers on the management NIC and so remains ONLINE, which is
    precisely the soak's phase-1 scenario; that case needs a per-address
    backoff rather than a status gate.
    """
    return node is not None and node.status in (
        StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN)


def _restart_owns_lvs(primary_node) -> bool:
    """True if the restart task currently owns ``primary_node.lvstore``.

    While ``primary_node.restart_phases[lvs]`` is set (pre_block / blocked /
    post_unblock), the restart runner is the exclusive author of hublvol
    attach/detach on that LVS. The periodic health repair must stand aside
    so it doesn't issue a parallel bdev_nvme_attach_controller on the same
    subnqn — that was the class of race that produced
    "bdev_nvme_check_multipath: cntlid N are duplicated" and left the
    tertiary without a hublvol to primary.
    """
    phases = getattr(primary_node, "restart_phases", None) or {}
    return bool(phases.get(primary_node.lvstore))


def check_bdev(name, *, rpc_client=None, bdev_names=None) -> bool:
    present = (
            ((bdev_names is not None) and (name in bdev_names)) or
            (rpc_client is not None and (rpc_client.get_bdevs(name) is not None))
    )
    logger.log(INFO if present else ERROR, f"Checking bdev: {name} ... " + ('ok' if present else 'failed'))
    return present


def check_subsystem(nqn, *, rpc_client=None, nqns=None, ns_uuid=None) -> bool:
    if rpc_client:
        subsystem = rpc_client.subsystem_get(nqn)
    elif nqns:
        subsystem = nqns.get(nqn)
    else:
        raise ValueError('Either rpc_client or nqns must be passed')

    if not subsystem:
        logger.error(f"Checking subsystem {nqn} ... not found")
        return False

    logger.info(f"Checking subsystem {nqn} ... ok")

    if ns_uuid:
        for ns in subsystem['namespaces']:
            if ns['uuid'] == ns_uuid:
                namespaces = 1
                break
        else:
            namespaces = 0
    else:
        namespaces = len(subsystem['namespaces'])
    logger.log(INFO if namespaces else ERROR, f"Checking namespaces: {namespaces} ... " + ('ok' if namespaces else 'not found'))

    listeners = subsystem['listen_addresses']
    if not listeners:
        logger.error(f"Checking listener for {nqn} ... not found")
    else:
        for listener in listeners:
            logger.info(f"Checking listener {listener['traddr']}:{listener['trsvcid']} ... ok")

    return bool(listeners) and bool(namespaces)


def check_cluster(cluster_id):
    db_controller = DBController()
    st = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    data = []
    result = True
    for node in st:
        # check if node is online, unavailable, restarting
        ret = check_node(node.get_id(), with_devices=False)
        result &= ret
        print("*"*100)
        data.append({
            "Kind": "Node",
            "UUID": node.get_id(),
            "Status": "ok" if ret else "failed"
        })

        for device in node.nvme_devices:
            ret = check_device(device.get_id())
            result &= ret
            print("*" * 100)
            data.append({
                "Kind": "Device",
                "UUID": device.get_id(),
                "Status": "ok" if ret else "failed"
            })

    for lvol in db_controller.get_lvols(cluster_id):
        ret = check_lvol(lvol.get_id())
        result &= ret
        print("*" * 100)
        data.append({
            "Kind": "LVol",
            "UUID": lvol.get_id(),
            "Status": "ok" if ret else "failed"
        })
    print(utils.print_table(data))
    return result


def check_node_rpc(node, timeout=8, retry=2):
    try:
        rpc_client = node.rpc_client(timeout=timeout, retry=retry)
        ret = rpc_client.get_version()
        if ret:
            logger.debug(f"SPDK version: {ret['version']}")
            return True, True
        else:
            return True, False
    except Exception as e:
        logger.debug(e)
    return False, False


def _check_node_api(node):
    try:
        # Liveness probe: short timeout, fail fast on connect errors (a
        # rebooting host refuses connections; retrying with backoff only delays
        # detection). 90s was wildly oversized for an is_live() ping.
        snode_api = node.client(timeout=8, retry=1, connect_retry=0)
        logger.debug(f"Node API={node.api_endpoint}")
        ret, _ = snode_api.is_live()
        logger.debug(f"snode is alive: {ret}")
        if ret:
            return True
    except Exception as e:
        logger.debug(e)
    return False


def _log_port_check_failure(db_controller, snode, port, exc):
    # ECONNREFUSED / RPC errors are routine during activation and restart
    # windows. Downgrade to WARNING so a transient miss doesn't look like
    # a real outage in logs.
    try:
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        cluster_status = cluster.status if cluster else None
    except Exception:
        cluster_status = None
    unstable = (cluster_status in (Cluster.STATUS_IN_ACTIVATION, Cluster.STATUS_SUSPENDED)
                or snode.status in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN))
    log_fn = logger.warning if unstable else logger.error
    log_fn("Check node port failed for %s port %s: %s", snode.get_id(), port, exc)


def check_port_on_node(snode, port_id):
    from simplyblock_core.utils import port_block
    return not port_block.is_port_blocked(snode, port_id)


def check_ports_on_node(snode, port_ids):
    """Batch variant of :func:`check_port_on_node`: ONE
    ``nvmf_get_blocked_ports`` fetch answers every port in ``port_ids``.
    Returns ``{port: bool}`` (True = open). Falls back to per-port checks on
    legacy nodes without the RPC. Raises on fetch failure — callers treat it
    like the single-port failure path."""
    from simplyblock_core.utils import port_block
    port_ids = list(port_ids)
    if not port_ids:
        return {}
    blocked = port_block.get_blocked_ports_set(snode)
    if blocked is None:
        return {p: check_port_on_node(snode, p) for p in port_ids}
    return {p: int(p) not in blocked for p in port_ids}


def _check_node_ping(ip):
    res = utils.ping_host(ip)
    if res:
        return True
    else:
        return False


def _check_ping_from_node(ip, ifname, node):
    # Fail fast on connect errors and don't stack retries: this SnodeAPI call
    # runs in the monitor's per-node check cycle, and against a rebooting host
    # stacked retries (read timeout x3 + connect timeouts) burned ~13s, blocking
    # that node's cycle from completing and re-evaluating (incident 2026-06-25).
    #
    # Tri-state result: True/False is the node agent's own ping_ip result; None
    # means this SnodeAPI call itself timed out / errored and the result is
    # INCONCLUSIVE. A timeout here is not evidence the data NIC is down -- node
    # liveness is already confirmed by is_live() earlier in the check cycle, so
    # the agent is alive, just slow to answer this probe. We must NOT flip the
    # node DOWN on that; the caller ignores None and re-evaluates next cycle.
    # (The previous mgmt-side ICMP fallback pinged the data IP from mgmt, which
    # is typically off the data VLAN -> always False -> spurious node-down.)
    snodeapi = node.client(timeout=8, retry=1, connect_retry=0)
    try:
        ret, _ = snodeapi.ping_ip(ip, ifname)
        return bool(ret)
    except Exception as e:
        logger.error(e)
        logger.info("data-nic ping_ip timed out/errored; treating as inconclusive")
        return None


def _check_node_hublvol(node: StorageNode) -> bool:
    if not node.hublvol:
        logger.error(f"Node {node.get_id()} does not have a hublvol")
        return False

    logger.info(f"Checking Hublvol: {node.hublvol.bdev_name} on node {node.get_id()}")
    db_controller = DBController()

    passed = True
    try:
        rpc_client = node.rpc_client(timeout=8, retry=1)

        passed &= check_bdev(node.hublvol.bdev_name, rpc_client=rpc_client)
        passed &= check_subsystem(node.hublvol.nqn, rpc_client=rpc_client)

        try:
            cl = db_controller.get_cluster_by_id(node.cluster_id)
        except KeyError:
            logger.error(f"Cluster with id {node.cluster_id} not found")
            return False

        ret = rpc_client.bdev_lvol_get_lvstores(node.lvstore)
        if ret:
            logger.info(f"Checking lvstore: {node.lvstore} ... ok")
            lvs_info = ret[0]
            logger.info("lVol store Info:")
            lvs_info_dict = []
            expected: dict[str, Any] = {}
            expected["lvs leadership"] = True
            expected["lvs_primary"] = True
            expected["lvs_read_only"] = False
            expected["name"] = node.lvstore
            expected["base_bdev"] = node.raid
            expected["block_size"] = cl.blk_size
            expected["cluster_size"] = cl.page_size_in_blocks

            for k, v in lvs_info.items():
                if k in expected:
                    value = expected[k] == v
                    lvs_info_dict.append({"Key": k, "Value": v, "expected": value})
                    if value is bool and v is False:
                        passed = False
                else:
                    lvs_info_dict.append({"Key": k, "Value": v, "expected": " "})
            if not passed:
                for line in utils.print_table(lvs_info_dict).splitlines():
                    logger.info(line)

    except Exception as e:
        logger.exception(e)
        return False
    return passed


def _check_sec_node_hublvol(node: StorageNode, auto_fix=False, primary_node_id=None,
                            repair_paths=False) -> bool:
    db_controller = DBController()
    # If a specific primary is given, use it; otherwise resolve from back-references
    if not primary_node_id:
        primary_node_id = node.lvstore_stack_secondary or node.lvstore_stack_tertiary
    if not primary_node_id:
        logger.error(f"No primary node reference found on secondary node {node.get_id()}")
        return False
    try:
        primary_node = db_controller.get_storage_node_by_id(primary_node_id)
    except KeyError:
        logger.exception("Primary node not found")
        return False
    
    if not primary_node.hublvol:
        logger.error(f"Primary node {primary_node.get_id()} does not have a hublvol")
        return False
    
    logger.info(f"Checking secondary Hublvol: {primary_node.hublvol.bdev_name} on node {node.get_id()}")

    passed = True
    try:
        rpc_client = node.rpc_client(timeout=8, retry=1)

        ret = rpc_client.bdev_nvme_controller_list(primary_node.hublvol.bdev_name)
        passed = bool(ret)
        logger.info(f"Checking controller: {primary_node.hublvol.bdev_name} ... {passed}")

        is_sec2 = (node.lvstore_stack_tertiary == primary_node.get_id())

        if not passed and auto_fix and primary_node.lvstore_status == "ready" \
                and primary_node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            try:
                # Full connect: optimized path to primary + non-optimized path to sec_1 for tertiary
                failover_node = None
                if is_sec2 and primary_node.secondary_node_id:
                    try:
                        sec1 = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)
                        if sec1.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
                            failover_node = sec1
                    except KeyError:
                        pass
                sec_role = "tertiary" if is_sec2 else "secondary"
                node.connect_to_hublvol(primary_node, failover_node=failover_node, role=sec_role)
            except Exception as e:
                logger.error("Error establishing hublvol: %s", e)
            ret = rpc_client.bdev_nvme_controller_list(primary_node.hublvol.bdev_name)
            passed = bool(ret)
            logger.info(f"Checking controller: {primary_node.hublvol.bdev_name} ... {passed}")
        elif passed and is_sec2 and auto_fix and primary_node.secondary_node_id \
                and primary_node.lvstore_status == "ready":
            # Controller exists but may only have the optimized path; ensure secondary path is present
            # ret is [{..., "ctrlrs": [path1, path2, ...]}, ...] — paths are inside ctrlrs
            ctrlrs = ret[0].get("ctrlrs", []) if ret else []
            if len(ctrlrs) < 2 and not _restart_owns_lvs(primary_node):
                try:
                    sec1 = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)
                    if sec1.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
                        if node.add_hublvol_failover_path(primary_node, sec1):
                            logger.info("Added missing secondary hublvol path on tertiary %s", node.get_id())
                        else:
                            logger.warning("Failed to add missing secondary hublvol path on tertiary %s",
                                           node.get_id())
                except Exception as e:
                    logger.error("Error adding secondary hublvol path: %s", e)

        # Repair degraded multipath on hublvol controller: each NIC should
        # contribute one path. If a NIC went down and came back, the path may
        # not have been re-established. Skip entirely while the restart task
        # owns this LVS — the restart flow is the exclusive author of hublvol
        # (re)attaches during its phases, and running a concurrent repair
        # here is what created the attach-during-destroy race in the past.
        # repair_paths is deliberately separate from auto_fix. auto_fix also
        # authorises a FULL reconnect, which the caller gates behind
        # secondary-holds-leadership and JC-compression-inactive; completing a
        # missing path on an existing enabled controller carries none of that
        # risk and must run on its own cadence. Kept behind auto_fix alone,
        # this block was unreachable for the case it exists to fix: the caller
        # only escalated to auto_fix when the coarse existence check FAILED,
        # and a hublvol holding 1 of 2 paths passes that check. It fired 0
        # times in 4 hours of the 2026-08-20 soak while hublvols sat
        # single-pathed for 5-11 minutes at a stretch.
        # Repair degraded multipath on the hublvol controller: each data NIC of
        # the primary should contribute one path. If a NIC dropped and came
        # back, the path may not have been re-established.
        #
        # repair_paths is separate from auto_fix on purpose. auto_fix also
        # authorises a FULL reconnect, which the caller gates behind
        # secondary-holds-leadership and JC-compression-inactive; completing a
        # missing path on an existing enabled controller carries none of that
        # risk and has to run on its own cadence. Gated on auto_fix alone it was
        # unreachable for the very case it exists for -- the caller escalated to
        # auto_fix only once the coarse existence check FAILED, and a hublvol
        # holding 1 of 2 paths passes that check. It fired 0 times in 4 hours of
        # the 2026-08-20 soak while hublvols sat single-pathed for 5-11 minutes.
        #
        # Skipped entirely while the restart task owns this LVS: the restart
        # flow is the exclusive author of hublvol (re)attaches during its
        # phases, and a concurrent repair here is what produced the
        # attach-during-destroy race before.
        if passed and (auto_fix or repair_paths) and ret                 and not _restart_owns_lvs(primary_node)                 and repairs_allowed(node) and repairs_allowed(primary_node):
            # SPDK multipath reports one ctrlrs entry PER PATH, so the attached
            # set must be unioned across entries before comparing. Built per
            # entry (as this was), a healthy two-path controller looks like each
            # entry is missing the other path -- every cycle would have become a
            # reconcile the moment this block became reachable.
            # _collect_attached_ips holds the same rule for device controllers,
            # including the older single-entry/alternate_trids shape.
            attached_ips = storage_node_ops._collect_attached_ips(ret)
            expected_ips = set()
            for iface in primary_node.data_nics:
                if (primary_node.active_rdma and iface.trtype == "RDMA") or                    (not primary_node.active_rdma and primary_node.active_tcp
                        and iface.trtype == "TCP"):
                    expected_ips.add(iface.ip4_address)
            # Tertiary nodes connect to both primary AND secondary; include
            # secondary NIPs so a missing secondary path is detected.
            if is_sec2 and primary_node.secondary_node_id:
                try:
                    _sec1 = db_controller.get_storage_node_by_id(
                        primary_node.secondary_node_id)
                    for iface in _sec1.data_nics:
                        if (_sec1.active_rdma and iface.trtype == "RDMA") or                            (not _sec1.active_rdma and _sec1.active_tcp
                                and iface.trtype == "TCP"):
                            expected_ips.add(iface.ip4_address)
                except Exception:
                    pass
            # A duplicated address is invisible to the set comparison below --
            # (96.179, 97.9, 97.9) reads as 2-of-2 -- so it must be checked
            # separately or the node is reported healthy while carrying a
            # surplus path. That is exactly what happened on 2026-08-25: the
            # control plane saw nothing wrong for hours while the soak's path
            # verifier counted 3-of-2 and eventually gave up. Two controllers
            # on one address also give the bdev two unordered qpairs to the
            # same target, so this is a fault to surface, not cosmetics.
            # Only meaningful for a named controller: ``ret`` came from
            # bdev_nvme_controller_list(bdev_name), and an empty name makes
            # that return every controller on the node -- whose paths then
            # look mutually "duplicated".
            hub_bdev = (primary_node.hublvol.bdev_name
                        if primary_node.hublvol else "")
            duplicate_ips = (storage_node_ops.duplicate_attached_paths(ret)
                             if hub_bdev else set())
            if duplicate_ips:
                # Detecting this without acting on it is what made the
                # 2026-09-01 multipath soak unrunnable: LVS_4/hublvol carried
                # peer 98.248 three times (97.36 twice, cntlid 1002 and 1003),
                # the check logged "repair_multipath_controller will prune
                # them" every cycle for 20 minutes, and nothing ever pruned --
                # hublvol is in neither device_repair_jobs nor jm_repair_jobs,
                # so repair_multipath_controller is never called for it. The
                # soak's path gate wants a multiple of len(data_nics), saw 5,
                # and gave up with "repair is stuck, not merely slow".
                #
                # Prune here, and deliberately do NOT re-attach: the detach
                # removes every copy of the address, and the missing-path
                # branch below reconciles it back through
                # HublvolReconnectCoordinator, whose cooldown is what closes
                # the "cntlid N are duplicated" race. Re-attaching inline
                # would bypass that and could recreate the duplicate.
                tr_type = "RDMA" if primary_node.active_rdma else "TCP"
                pruned = False
                try:
                    pruned = storage_node_ops.prune_duplicate_paths(
                        rpc_client, primary_node.hublvol.bdev_name, ret,
                        primary_node.hublvol.nvmf_port, tr_type)
                except Exception:
                    logger.exception(
                        "Failed to prune duplicate hublvol path(s) %s on %s",
                        duplicate_ips, node.get_id())
                if pruned:
                    # Re-read so missing_ips below sees the pruned addresses
                    # as missing and reconciles each back exactly once.
                    ret = rpc_client.bdev_nvme_controller_list(
                        primary_node.hublvol.bdev_name) or []
                    attached_ips = storage_node_ops._collect_attached_ips(ret)
                    logger.info(
                        "Pruned duplicate hublvol path(s) %s on %s; %d path(s) "
                        "remain, reconcile will restore them",
                        duplicate_ips, node.get_id(), len(attached_ips))
                else:
                    logger.error(
                        "Hublvol %s on %s has duplicate path(s) %s that could "
                        "not be pruned -- node is NOT healthy",
                        primary_node.hublvol.bdev_name, node.get_id(),
                        duplicate_ips)
                # Unhealthy for this cycle either way: the surplus path was
                # real, and the reconcile has not completed yet.
                passed = False

            missing_ips = expected_ips - attached_ips
            if missing_ips:
                logger.info(
                    "Hublvol %s on %s missing paths: %s, reconciling via coordinator",
                    primary_node.hublvol.bdev_name, node.get_id(), missing_ips)
                try:
                    # Reconnect the TRANSPORT *and* rejoin the lvstore.
                    #
                    # This used to call HublvolReconnectCoordinator.reconcile()
                    # directly, which only issues bdev_nvme_attach_controller --
                    # its own docstring says the caller "can return immediately
                    # and proceed to bdev_lvol_connect_hublvol / port-unblock".
                    # Nothing here did, so a repaired hublvol was left with NVMe
                    # paths up but never connected to the LVS.
                    #
                    # 2026-09-01, LVS_10: node ...4424 took a hublvol remove
                    # event at 16:24:50, this path re-attached the controller at
                    # 16:25:35, and the connect RPC never followed. The node sat
                    # half-wired as secondary, so when IO reached it at 16:28:31
                    # it could not redirect and instead triggered a leadership
                    # switch ("Leadership changed due to receive new IO"). The
                    # control plane then unblocked the old primary's port at
                    # 16:28:37, the client went back to a node that was no
                    # longer leader, and its IO was failed with a generic status
                    # -> client EIO.
                    #
                    # connect_to_hublvol() is the complete operation: it still
                    # routes the attach through the same coordinator (cooldown
                    # and cntlid-duplicate protection intact, and it takes the
                    # cross-process lock itself when none is handed in), then
                    # stamps the role via bdev_lvol_set_lvs_opts and issues
                    # bdev_lvol_connect_hublvol. It is idempotent: an already
                    # attached path is skipped.
                    from simplyblock_core.utils.hublvol_reconnect import (
                        HublvolReconnectCoordinator,
                    )
                    role = "tertiary" if is_sec2 else "secondary"
                    peers = [primary_node]
                    failover_node = None
                    if is_sec2 and primary_node.secondary_node_id:
                        try:
                            sec1 = db_controller.get_storage_node_by_id(
                                primary_node.secondary_node_id)
                            if repairs_allowed(sec1):
                                failover_node = sec1
                                peers.append(sec1)
                        except Exception:
                            pass

                    # Two steps, not one. reconcile() adds the MISSING PATHS
                    # to an existing controller -- that is the repair, and it
                    # is the only thing that goes through the coordinator's
                    # cooldown / cntlid-duplicate protection.
                    coordinator = HublvolReconnectCoordinator(db_controller)
                    coordinator.reconcile(node, primary_node, peers, role=role)

                    # Then rejoin the lvstore. reconcile() deliberately stops
                    # at the transport -- its docstring says the caller "can
                    # return immediately and proceed to
                    # bdev_lvol_connect_hublvol" -- and until now nothing here
                    # did. On 2026-09-01 node ...4424 was re-attached at
                    # 16:25:35 and never connected, so it sat half-wired as
                    # secondary: when IO arrived at 16:28:31 it could not
                    # redirect and triggered a leadership switch instead,
                    # which is what led to the client EIO.
                    #
                    # connect_to_hublvol skips the attach when the remote bdev
                    # is already present, so after reconcile() it contributes
                    # only set_lvs_opts + connect_hublvol. Calling it INSTEAD
                    # of reconcile() was wrong for exactly that reason: a
                    # partially attached controller has its bdev, so the
                    # missing path never got added (caught by
                    # test_repairs_missing_primary_nic_path).
                    connected = node.connect_to_hublvol(
                        primary_node, failover_node=failover_node, role=role,
                        rpc_timeout=HUBLVOL_REPAIR_RPC_TIMEOUT_SEC,
                        lvs_node=primary_node)
                    if not connected:
                        logger.error(
                            "Hublvol %s on %s: rejoin did not complete",
                            primary_node.hublvol.bdev_name, node.get_id())
                        passed = False
                except Exception as e:
                    logger.error(
                        "Failed to reconnect hublvol on %s: %s",
                        node.get_id(), e)
                    passed = False

        passed &= check_bdev(primary_node.hublvol.get_remote_bdev_name(), rpc_client=rpc_client)
        if not passed:
            return False

        try:
            cl = db_controller.get_cluster_by_id(node.cluster_id)
        except KeyError:
            logger.error(f"Cluster with id {node.cluster_id} not found")
            return False
        ret = rpc_client.bdev_lvol_get_lvstores(primary_node.lvstore)
        if ret:
            logger.info(f"Checking lvstore: {primary_node.lvstore} ... ok")
            lvs_info = ret[0]
            logger.info("lVol store Info:")
            lvs_info_dict = []
            expected: dict [str, Any] = {}
            expected["name"] = primary_node.lvstore
            expected["lvs leadership"] = False
            expected["lvs_secondary"] = True
            expected["lvs_read_only"] = False
            expected["lvs_redirect"] = True
            expected["remote_bdev"] = primary_node.hublvol.get_remote_bdev_name()
            expected["connect_state"] = True
            expected["base_bdev"] = primary_node.raid
            expected["block_size"] = cl.blk_size
            expected["cluster_size"] = cl.page_size_in_blocks

            for k, v in lvs_info.items():

                if k in expected:
                    value = expected[k] == v
                    lvs_info_dict.append({"Key": k, "Value": v, "expected": value})
                    if value is bool and value is False:
                        passed = False

                else:
                    lvs_info_dict.append({"Key": k, "Value": v, "expected": " "})
            if not passed:
                for line in utils.print_table(lvs_info_dict).splitlines():
                    logger.info(line)
    except Exception as e:
        logger.exception(e)
        return False
    return passed


def _check_node_lvstore(
        lvstore_stack, node, auto_fix=False, node_bdev_namesss=None, stack_src_node=None) -> bool:
    db_controller = DBController()
    logger.info(f"Checking distr stack on node : {node.get_id()}")

    cluster = db_controller.get_cluster_by_id(node.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:
        auto_fix = False

    distribs_list = []
    raid = None
    bdev_lvstore = None
    for bdev in lvstore_stack:
        type = bdev['type']
        if type == "bdev_raid":
            distribs_list = bdev["distribs_list"]
            raid = bdev["name"]
        elif type == "bdev_lvstore":
            bdev_lvstore = bdev["name"]

    node_distribs_list = []
    for bdev in node.lvstore_stack:
        type = bdev['type']
        if type == "bdev_raid":
            node_distribs_list = bdev["distribs_list"]

    nodes = {}
    devices = {}
    for n in db_controller.get_storage_nodes():
        nodes[n.get_id()] = n
        for dev in n.nvme_devices:
            devices[dev.get_id()] = dev

    for distr in distribs_list:
        if node.rpc_client().get_bdevs(distr):
            logger.info(f"Checking distr bdev : {distr} ... ok")
            logger.info("Checking distr JM names:")
            if distr in node_distribs_list:
                jm_names = storage_node_ops.get_node_jm_names(node)
            elif stack_src_node:
                jm_names = storage_node_ops.get_node_jm_names(stack_src_node, remote_node=node)
            else:
                jm_names = node.jm_ids
            for jm in jm_names:
                logger.info(jm)
            logger.info("Checking Distr map ...")
            try:
                ret = node.rpc_client().distr_get_cluster_map(distr)
            except Exception as e:
                logger.info(f"Failed to get cluster map: {e}")
                return False
            if not ret:
                logger.error("Failed to get cluster map")
                return False
            else:
                results, is_passed = distr_controller.parse_distr_cluster_map(ret, nodes, devices)
                if results:
                    logger.info(f"Checking Distr map ... {is_passed}")
                    if is_passed:
                        continue

                    elif not auto_fix:
                        return False

                    else: #  is_passed is False and auto_fix is True
                        logger.info(utils.print_table(results))
                        for result in results:
                            if result['Results'] == 'failed':
                                if result['Kind'] == "Device":
                                    if result['Found Status']:
                                        dev = db_controller.get_storage_device_by_id(result['UUID'])
                                        dev_node = db_controller.get_storage_node_by_id(dev.node_id)
                                        if dev.status == NVMeDevice.STATUS_ONLINE and dev_node.status in [
                                            StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_UNREACHABLE, StorageNode.STATUS_SUSPENDED]:
                                            try:
                                                remote_bdev = storage_node_ops.connect_device(
                                                    f"remote_{dev.alceml_bdev}", dev, node)
                                                if remote_bdev:
                                                    remote_device = RemoteDevice()
                                                    remote_device.uuid = dev.uuid
                                                    remote_device.alceml_name = dev.alceml_name
                                                    remote_device.node_id = dev.node_id
                                                    remote_device.size = dev.size
                                                    remote_device.status = NVMeDevice.STATUS_ONLINE
                                                    remote_device.nvmf_multipath = dev.nvmf_multipath
                                                    remote_device.remote_bdev = remote_bdev

                                                    # Atomic: rebuild remote_devices on the freshly-read node
                                                    # inside the FDB tx so a concurrent node.status change is
                                                    # not clobbered (incident 2026-06-18).
                                                    def _mut(nn, did=dev.get_id(), rd=remote_device):
                                                        nn.remote_devices = [
                                                            r for r in nn.remote_devices if r.get_id() != did]
                                                        nn.remote_devices.append(rd)
                                                        return True

                                                    db_controller.atomic_update(
                                                        db_controller.get_storage_node_by_id(node.get_id()), _mut)
                                                    distr_controller.send_dev_status_event(dev, dev.status, node)
                                            except Exception as e:
                                                logger.error(f"Failed to connect to {dev.get_id()}: {e}")
                                        elif dev.status == NVMeDevice.STATUS_ONLINE and dev_node.status in [
                                            StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE]:
                                            logger.warning(f"Node is offline or unreachable, setting device unavailable: {dev.get_id()}")
                                            device_controller.device_set_unavailable(dev.get_id())
                                        else:
                                            distr_controller.send_dev_status_event(dev, dev.status, node)

                                if result['Kind'] == "Node":
                                    n = db_controller.get_storage_node_by_id(result['UUID'])
                                    distr_controller.send_node_status_event(n, n.status, node)

                        try:
                            ret = node.rpc_client().distr_get_cluster_map(distr)
                        except Exception as e:
                            logger.error(e)
                            return False
                        if not ret:
                            logger.error("Failed to get cluster map")
                            return False
                        else:
                            results, is_passed = distr_controller.parse_distr_cluster_map(ret, nodes, devices)
                            logger.info(f"Checking Distr map ... {is_passed}")
                            if not is_passed:
                                return False

                else:
                    logger.error("Failed to parse distr cluster map")
                    return False
        else:
            logger.info(f"Checking distr bdev : {distr} ... not found")
            return False
    if raid:
        if node.rpc_client().get_bdevs(raid):
            logger.info(f"Checking raid bdev: {raid} ... ok")
        else:
            logger.info(f"Checking raid bdev: {raid} ... not found")
            return False
    if bdev_lvstore:
        try:
            ret = node.rpc_client().bdev_lvol_get_lvstores(bdev_lvstore)
        except Exception as e:
            logger.error(e)
            return False
        if ret:
            logger.info(f"Checking lvstore: {bdev_lvstore} ... ok")
        else:
            logger.info(f"Checking lvstore: {bdev_lvstore} ... not found")
            return False
    return True

def check_node(node_id, with_devices=True):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("node not found")
        return False

    # Health is only meaningful for ONLINE or DOWN nodes. In every other state
    # (offline, in_restart, in_shutdown, unreachable, schedulable, removed,
    # in_creation, suspended) the upper stack is being torn down or rebuilt by
    # the runner (or the operator) and the data-plane state read back here —
    # distrib cluster_map on peers, lvstore_stack comparisons, remote device
    # reachability — is momentarily inconsistent with FDB. Acting on that
    # mismatch (e.g. device_set_unavailable, recreate secondary hublvol)
    # clobbers the in-progress transition, and reporting a true/false health
    # for a parked node is misleading. So we report "not applicable" and skip.
    if snode.status not in HEALTH_RELEVANT_NODE_STATUSES:
        msg = f"Node status is {snode.status}; health check is not applicable (only ONLINE/DOWN)"
        logger.info(msg)
        print(msg)
        return True

    logger.info(f"Checking node {node_id}, status: {snode.status}")

    print("*" * 100)

    # passed = True

    # 1- check node ping
    ping_check = _check_node_ping(snode.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {snode.mgmt_ip} ... {ping_check}")

    # 2- check node API
    node_api_check = _check_node_api(snode)
    logger.info(f"Check: node API {snode.mgmt_ip}:5000 ... {node_api_check}")

    # 3- check node RPC
    node_rpc_check, _ = check_node_rpc(snode)
    logger.info(f"Check: node RPC {snode.mgmt_ip}:{snode.rpc_port} ... {node_rpc_check}")

    data_nics_check = True
    for data_nic in snode.data_nics:
        if data_nic.ip4_address:
            ping_check = _check_ping_from_node(data_nic.ip4_address, ifname=data_nic.if_name, node=snode)
            logger.info(f"Check: ping ip {data_nic.ip4_address} ... {ping_check}")
            data_nics_check &= ping_check

    # Batched: collect every port first, answer them with ONE
    # nvmf_get_blocked_ports fetch (was one full-list fetch per port).
    _hc_ports = []
    for sec_attr in ['lvstore_stack_secondary', 'lvstore_stack_tertiary']:
        primary_id = getattr(snode, sec_attr, None)
        if primary_id:
            try:
                n = db_controller.get_storage_node_by_id(primary_id)
                _hc_ports.append(n.get_lvol_subsys_port(n.lvstore))
            except KeyError:
                logger.error("node not found")

    if not snode.is_secondary_node:
        _hc_ports.append(snode.get_lvol_subsys_port(snode.lvstore))

    if _hc_ports:
        try:
            for _p, lvol_port_check in check_ports_on_node(snode, _hc_ports).items():
                logger.info(f"Check: node {snode.mgmt_ip}, port: {_p} ... {lvol_port_check}")
        except Exception as e:
            for _p in _hc_ports:
                _log_port_check_failure(db_controller, snode, _p, e)

    is_node_online = ping_check and node_api_check and node_rpc_check

    logger.info(f"Results : {is_node_online}")
    print("*" * 100)

    node_devices_check = True
    node_remote_devices_check = True
    lvstore_check = True

    if not node_rpc_check:
        logger.info("Skipping devices checks because RPC check failed")
    else:
        logger.info(f"Node device count: {len(snode.nvme_devices)}")
        print("*" * 100)
        for dev in snode.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE]:
                ret = check_device(dev.get_id())
                node_devices_check &= ret
            else:
                logger.info(f"Device skipped: {dev.get_id()} status: {dev.status}")
            print("*" * 100)

        logger.info(f"Node remote device: {len(snode.remote_devices)}")
        print("*" * 100)
        rpc_client = snode.rpc_client(timeout=8, retry=1)
        for remote_device in snode.remote_devices:
            ret = check_remote_device(remote_device.get_id(), snode)
            try:
                owner = db_controller.get_storage_node_by_id(remote_device.node_id)
            except KeyError:
                owner = None
            if _peer_connections_relevant(owner):
                node_remote_devices_check &= ret
            elif not ret:
                logger.info(
                    "Remote device %s missing, but owning node %s is %s — expected, "
                    "not failing health", remote_device.get_id(), remote_device.node_id,
                    owner.status if owner else "not-found")
            print("*" * 100)

        if snode.jm_device:
            jm_device = snode.jm_device
            logger.info(f"Node JM: {jm_device.get_id()}")
            ret = check_jm_device(jm_device.get_id())
            if ret:
                logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... ok")
            else:
                logger.info(f"Checking jm bdev: {jm_device.jm_bdev} ... not found")
            node_devices_check &= ret

        if snode.enable_ha_jm:
            print("*" * 100)
            connected_jms = []
            logger.info(f"Node remote JMs: {len(snode.remote_jm_devices)}")
            for remote_device in snode.remote_jm_devices:

                name = remote_device.remote_bdev
                # Owner resolved BEFORE the probe, not after. Previously the
                # RPC went out unconditionally and the ERROR line was logged
                # before anything knew the owner was gone -- so a removed
                # node's stale entry cost one RPC per cycle and left an ERROR
                # in the log that the very next line classified as expected,
                # and never retracted. Live 2026-09-02: 1797 such hits on one
                # removed node's JM.
                try:
                    jm_owner = db_controller.get_storage_node_by_id(remote_device.node_id)
                except KeyError:
                    jm_owner = None
                owner_relevant = _peer_connections_relevant(jm_owner)
                if not owner_relevant:
                    logger.info(
                        "Remote JM %s belongs to node %s (%s); not probing and not "
                        "failing health", name, remote_device.node_id,
                        jm_owner.status if jm_owner else "not-found")
                    connected_jms.append(remote_device.get_id())
                    continue

                bdev_info = rpc_client.get_bdevs(name)
                logger.log(INFO if bdev_info else ERROR,
                           f"Checking bdev: {name} ... " + ('ok' if bdev_info else 'failed'))
                node_remote_devices_check &= bool(bdev_info)
                connected_jms.append(remote_device.get_id())

                controller_info = rpc_client.bdev_nvme_controller_list(f'remote_{remote_device.jm_bdev}')
                if controller_info:
                    addr = controller_info[0]['ctrlrs'][0]['trid']['traddr']
                    port = controller_info[0]['ctrlrs'][0]['trid']['trsvcid']
                    logger.info(f"IP Address: {addr}:{port}")

                if remote_device.nvmf_multipath:
                    if controller_info and "alternate_trids" in controller_info[0]['ctrlrs'][0]:
                        addr = controller_info[0]['ctrlrs'][0]['alternate_trids'][0]['traddr']
                        port = controller_info[0]['ctrlrs'][0]['alternate_trids'][0]['trsvcid']
                        logger.info(f"IP Address: {addr}:{port}")

                    if bdev_info:
                        logger.info(f"multipath policy: {bdev_info[0]['driver_specific']['mp_policy']}")

            for jm_id in snode.jm_ids:
                logger.info(f"Checking connection to JM device {jm_id}")
                if jm_id and jm_id not in connected_jms:
                    for nd in db_controller.get_storage_nodes():
                        if nd.jm_device and nd.jm_device.get_id() == jm_id:
                            if _peer_connections_relevant(nd):
                                node_remote_devices_check = False
                                logger.error(f"JM device {jm_id} is not connected")
                            else:
                                logger.info(
                                    "JM device %s not connected, but owning node %s is %s "
                                    "— expected, not failing health", jm_id, nd.get_id(), nd.status)
                            break

        print("*" * 100)
        if snode.lvstore_stack:
            lvstore_stack = snode.lvstore_stack
            lvstore_check &= _check_node_lvstore(lvstore_stack, snode)
            print("*" * 100)
            if snode.secondary_node_id:
                second_node_1 = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                if second_node_1.status == StorageNode.STATUS_ONLINE:
                    lvstore_check &= _check_node_lvstore(lvstore_stack, second_node_1, stack_src_node=snode)
                    print("*" * 100)
                lvstore_check &= _check_node_hublvol(snode)
                # Ensure sec_1 has its secondary hublvol exposed (same NQN, non-optimized)
                if second_node_1.status == StorageNode.STATUS_ONLINE:
                    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                    try:
                        sec1_rpc = second_node_1.rpc_client(timeout=8, retry=1)
                        if snode.hublvol and not sec1_rpc.subsystem_get(snode.hublvol.nqn):
                            logger.info("Secondary hublvol NQN missing on sec_1 %s, recreating",
                                        second_node_1.get_id())
                            second_node_1.create_secondary_hublvol(snode, cluster.nqn)
                    except Exception as e:
                        logger.error("Error checking/recreating secondary hublvol on sec_1: %s", e)
                if second_node_1.status == StorageNode.STATUS_ONLINE:
                    print("*" * 100)
                    # Pass the primary explicitly (as the tertiary callsite
                    # below does): without it the check resolves the primary
                    # from sec_1's own back-refs, and for a node that is
                    # secondary of one lvstore and tertiary of another a
                    # stale/empty lvstore_stack_secondary silently falls
                    # through to the tertiary relationship — verifying and
                    # auto-fixing the hublvol toward the WRONG primary.
                    lvstore_check &= _check_sec_node_hublvol(
                        second_node_1, auto_fix=True, primary_node_id=snode.get_id())
                # Check tertiary's hublvol paths (optimized to primary + non-optimized to sec_1)
                if snode.tertiary_node_id:
                    tert_node = db_controller.get_storage_node_by_id(snode.tertiary_node_id)
                    if tert_node and tert_node.status == StorageNode.STATUS_ONLINE:
                        print("*" * 100)
                        lvstore_check &= _check_sec_node_hublvol(
                            tert_node, auto_fix=True, primary_node_id=snode.get_id())

    return is_node_online and node_devices_check and node_remote_devices_check and lvstore_check


def check_device(device_id):
    db_controller = DBController()
    try:
        device = db_controller.get_storage_device_by_id(device_id)
    except KeyError:
        # is jm device ?
        for node in db_controller.get_storage_nodes():
            if node.jm_device and node.jm_device.get_id() == device_id:
                return check_jm_device(node.jm_device.get_id())

        logger.error("device not found")
        return False

    try:
        snode = db_controller.get_storage_node_by_id(device.node_id)
    except KeyError:
        logger.exception("node not found")
        return False

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    if device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED, NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
        logger.info(f"Skipping ,device status is {device.status}")
        return True

    passed = True
    try:
        rpc_client = snode.rpc_client()

        if snode.enable_test_device:
            bdevs_stack = [device.nvme_bdev, device.testing_bdev, device.alceml_bdev, device.pt_bdev]
        else:
            bdevs_stack = [device.nvme_bdev, device.alceml_bdev, device.pt_bdev]

        # if device.jm_bdev:
        #     bdevs_stack.append(device.jm_bdev)
        logger.info(f"Checking Device: {device_id}, status:{device.status}")
        problems = 0
        for bdev in bdevs_stack:
            if not bdev:
                continue

            if not check_bdev(bdev, rpc_client=rpc_client):
                problems += 1
                passed = False

        logger.info(f"Checking Device's BDevs ... ({(len(bdevs_stack)-problems)}/{len(bdevs_stack)})")

        passed &= check_subsystem(device.nvmf_nqn, rpc_client=rpc_client)

        # if device.status == NVMeDevice.STATUS_ONLINE:
        #     logger.info("Checking other node's connection to this device...")
        #     ret = check_remote_device(device_id)
        # if not ret:
        #         logger.warning(f"Remote device {device_id} is not accessible from other nodes")#     # passed &= ret

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")
        passed = False

    return passed


def check_remote_device(device_id, target_node=None):
    db_controller = DBController()
    try:
        device = db_controller.get_storage_device_by_id(device_id)
    except KeyError:
        logger.error("device not found")
        return False

    try:
        snode = db_controller.get_storage_node_by_id(device.node_id)
    except KeyError:
        logger.exception("node not found")
        return False

    # The device's OWNER decides whether a remote connection to it is even
    # expected. Skip the probe entirely when it is not -- same rule, and the
    # same reason, as the remote-JM loop above: a missing connection to a
    # departed owner is the expected consequence of its teardown.
    #
    # Gating only the verdict is not enough. The caller already discards the
    # result for an irrelevant owner, but it calls this function first, so the
    # two RPCs below still went out on every cycle for every surviving node.
    # For a REMOVED node's devices that never stops: each miss makes SPDK log
    # `*ERROR*: ctrlr 'remote_alceml_<uuid>' does not exist`, measured at
    # 3-15 errors/min still climbing 35 minutes after the removal that made
    # those devices failed_and_migrated (2026-09-03, devices 04fce724 /
    # b0ada39d / ddf660f5 of the removed 2vk79, probed by 9 surviving nodes).
    # Real faults then drown in a permanent error stream.
    if not _peer_connections_relevant(snode):
        logger.info(
            "Remote device %s belongs to node %s (%s); not probing and not "
            "failing health", device_id, device.node_id, snode.status)
        return True

    result = True
    if target_node:
        nodes = [target_node]
    else:
        nodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    for node in nodes:
        if node.status == StorageNode.STATUS_ONLINE:
            if node.get_id() == snode.get_id():
                continue
            logger.info(f"Checking device: {device_id}")
            rpc_client = node.rpc_client(timeout=8, retry=1)
            name = f'remote_{device.alceml_bdev}n1'
            bdev_info = rpc_client.get_bdevs(name)
            logger.log(DEBUG if bdev_info else ERROR, f"Checking bdev: {name} ... " + ('ok' if bdev_info else 'failed'))
            result &= bool(bdev_info)
            controller_info = rpc_client.bdev_nvme_controller_list(f'remote_{device.alceml_bdev}')
            if controller_info:
                addr = controller_info[0]['ctrlrs'][0]['trid']['traddr']
                port = controller_info[0]['ctrlrs'][0]['trid']['trsvcid']
                logger.info(f"IP Address: {addr}:{port}")

            if device.nvmf_multipath:
                if controller_info and "alternate_trids" in controller_info[0]['ctrlrs'][0]:
                    addr = controller_info[0]['ctrlrs'][0]['alternate_trids'][0]['traddr']
                    port = controller_info[0]['ctrlrs'][0]['alternate_trids'][0]['trsvcid']
                    logger.info(f"IP Address: {addr}:{port}")

                if bdev_info:
                    logger.info(f"multipath policy: {bdev_info[0]['driver_specific']['mp_policy']}")

    return result


def check_lvol_on_node(lvol_id, node_id, node_bdev_names=None, node_lvols_nqns=None):
    logger.info(f"Checking lvol on node: {node_id}")

    db_controller = DBController()
    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return False

    rpc_client = snode.rpc_client(timeout=8, retry=1)

    passed = True
    try:
        for bdev_info in lvol.bdev_stack:
            bdev_name = bdev_info['name']
            if bdev_info['type'] in ["bdev_lvol", "bdev_lvol_clone"]:
                bdev_name = lvol.lvol_uuid
            bdev_check = check_bdev(bdev_name, rpc_client=rpc_client)
            if not bdev_check:
                bdev_check = check_bdev(lvol.top_bdev, rpc_client=rpc_client)
            passed &= bdev_check

        passed &= check_subsystem(lvol.nqn, rpc_client=rpc_client, ns_uuid=lvol.uuid)

    except Exception as e:
        logger.error(e)
        return False

    return passed


def check_lvol(lvol_id):
    db_controller = DBController()

    try:
        lvol = db_controller.get_lvol_by_id(lvol_id)
    except KeyError as e:
        logger.error(e)
        return False

    if lvol.ha_type == 'single':
        ret = check_lvol_on_node(lvol_id, lvol.node_id)
        return ret

    elif lvol.ha_type == "ha":
        passed = True
        for nodes_id in lvol.nodes:
            node = db_controller.get_storage_node_by_id(nodes_id)
            if node.status == StorageNode.STATUS_ONLINE:
                ret = check_lvol_on_node(lvol_id, nodes_id)
                if not ret:
                    passed = False
        return passed


def check_snap(snap_id):
    db_controller = DBController()
    try:
        snap = db_controller.get_snapshot_by_id(snap_id)
    except KeyError:
        logger.error(f"snap not found: {snap_id}")
        return False

    snode = db_controller.get_storage_node_by_id(snap.lvol.node_id)
    check_primary = snode.rpc_client().get_bdevs(snap.snap_bdev)
    logger.info(f"Checking snap bdev: {snap.snap_bdev} on node: {snap.lvol.node_id} is {bool(check_primary)}")
    if snode.secondary_node_id:
        secondary_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        check_secondary = secondary_node.rpc_client().get_bdevs(snap.snap_bdev)
        logger.info(f"Checking snap bdev: {snap.snap_bdev} on node: {snode.secondary_node_id} is {bool(check_secondary)}")
        return check_primary and check_secondary
    return check_primary


def check_jm_device(device_id):
    db_controller = DBController()

    try:
        snode = device_controller.get_storage_node_by_jm_device(db_controller, device_id)
    except KeyError:
        logger.error("device not found")
        return False

    jm_device = snode.jm_device

    if snode.status in [StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"Skipping ,node status is {snode.status}")
        return True

    if jm_device.status in [NVMeDevice.STATUS_REMOVED, NVMeDevice.STATUS_FAILED]:
        logger.info(f"Skipping ,device status is {jm_device.status}")
        return True

    if snode.primary_ip != snode.mgmt_ip and jm_device.status == JMDevice.STATUS_UNAVAILABLE:
        return True

    passed = True
    try:
        rpc_client = snode.rpc_client(timeout=8, retry=2)

        passed &= check_bdev(jm_device.jm_bdev, rpc_client=rpc_client)
        if snode.enable_ha_jm:
            passed &= check_subsystem(jm_device.nvmf_nqn, rpc_client=rpc_client)

    except Exception as e:
        logger.error(f"Failed to connect to node's SPDK: {e}")
        passed = False

    return passed
