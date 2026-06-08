# coding=utf-8
import time


from simplyblock_core import db_controller, utils, storage_node_ops, distr_controller
from simplyblock_core.controllers import (
    tcp_ports_events, health_controller, tasks_controller, storage_events,
)
from simplyblock_core.fw_api_client import FirewallClient
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode

logger = utils.get_logger(__name__)

# get DB controller
db = db_controller.DBController()


# -- Hublvol gate retry policy -----------------------------------------------
#
# Per port-allow design: before unblocking the recovering primary's listener
# port, every online peer (secondary, tertiary) must have a *verified-open*
# hublvol to the primary. "Verified-open" means:
#   1) bdev_nvme_controller_list returns the controller AND
#      at least one path is in state == "enabled", AND
#   2) the namespace bdev <primary.hublvol.bdev_name>n1 is registered on
#      the peer (i.e., spdk_lvs_open_hub_bdev would not return ENODEV).
#
# A non-empty controller list alone is insufficient — during a destruct-in-
# flight window the list returns a controller object that does not have any
# usable path and the namespace bdev is gone (incident 2026-05-21 18:52:56:
# gate said True for 6be10996 / LVS_753; lvolstore open returned ENODEV 6 s
# later; primary unblocked too early and the split-brain abort followed).
#
# If verification fails we re-issue connect_to_hublvol (which drives a fresh
# bdev_nvme_attach_controller as step 1) and re-verify. Five attempts with
# exponential backoff covers a typical SPDK reconnect window without parking
# the cluster IO indefinitely:
_HUBLVOL_RETRY_DELAYS_SEC = (1, 2, 4, 8, 16)  # delays *between* attempts -> 5 attempts, ~31s ceiling
_HUBLVOL_MAX_ATTEMPTS = len(_HUBLVOL_RETRY_DELAYS_SEC)


def _hublvol_verified_open(peer_node, primary_node):
    """Strict check of the peer's hublvol to ``primary_node``.

    Returns True only if all three hold:
      - bdev_nvme_controller_list(<hublvol_bdev_name>) returns a controller
        with at least one path whose ``state == "enabled"``;
      - that enabled path is attached to one of ``primary_node``'s data-NIC
        IPs (i.e. it actually points to the primary, not to a peer);
      - get_bdevs(<hublvol_bdev_name>n1) returns the namespace bdev (the
        bdev the lvolstore will spdk_bdev_open_ext on at takeover time).

    The primary-IP requirement matters for the tertiary case: a tertiary's
    hublvol bdev_nvme is a single multipath group containing paths to BOTH
    the primary (ANA-optimized) and the secondary (ANA-non-optimized). When
    the primary's data NICs go dark in a network_outage, the primary-pointing
    controllers get destroyed by ``ctrlr_loss_timeout`` but the
    secondary-pointing controllers stay ``enabled``. The earlier
    ``any(state == "enabled")`` check would PASS spuriously, the port-allow
    gate would unblock the primary's LVS port with the tert->pri leg still
    down, and the next JC heartbeat would surface the cross-JM sync_id
    divergence built up during the outage as a writer_conflict (incident
    2026-05-22 19:31-19:32 LVS_7578: tertiary 5156755c had cntlid 1000+1001
    enabled to secondary 2333e02a but no enabled path to primary 93abb06b
    when port_allow fired ``Port allowed: 4442`` at 19:32:30.114; 2333e02a
    went online->down at 19:32:36.400).

    On any RPC error the function returns False -- we conservatively treat
    "couldn't verify" as "not open" so the port-allow gate keeps the port
    blocked instead of letting a transient management-side failure leak a
    half-open hublvol into a split-brain.
    """
    try:
        rpc = peer_node.rpc_client(timeout=5, retry=1)
        ctrlrs_resp = rpc.bdev_nvme_controller_list(primary_node.hublvol.bdev_name)
        if not ctrlrs_resp:
            return False
        ctrlrs = ctrlrs_resp[0].get("ctrlrs", []) if isinstance(ctrlrs_resp, list) else []

        primary_ips = {
            iface.ip4_address for iface in (primary_node.data_nics or [])
            if iface.ip4_address
        }
        has_enabled_primary_path = False
        for ct in ctrlrs:
            if ct.get("state") != "enabled":
                continue
            attached = {ct.get("trid", {}).get("traddr")}
            for alt in (ct.get("alternate_trids") or []):
                attached.add(alt.get("traddr"))
            if attached & primary_ips:
                has_enabled_primary_path = True
                break
        if not has_enabled_primary_path:
            return False

        ns_name = primary_node.hublvol.bdev_name + "n1"
        bdev_resp = rpc.get_bdevs(ns_name)
        return bool(bdev_resp)
    except Exception as e:
        logger.warning(
            "Hublvol verify on %s for %s raised: %s",
            peer_node.get_id()[:8], primary_node.hublvol.bdev_name, e)
        return False


def _reconnect_peer_hublvol_once(peer_node, primary_node):
    """Drive a single ``connect_to_hublvol`` from peer to primary, with
    the correct ``role`` / ``failover_node`` for tertiary-vs-secondary.

    Returns the bool that ``connect_to_hublvol`` returned (True iff all
    three internal steps -- attach + set_lvs_opts + connect_hublvol --
    succeeded).
    """
    # Determine role: peer is tertiary of primary if its tertiary back-ref
    # points at primary (the same condition health_controller._check_sec_
    # node_hublvol uses to compute is_sec2).
    is_tertiary = (peer_node.lvstore_stack_tertiary == primary_node.get_id())
    sec_role = "tertiary" if is_tertiary else "secondary"
    failover_node = None
    if is_tertiary and primary_node.secondary_node_id:
        try:
            sec1 = db.get_storage_node_by_id(primary_node.secondary_node_id)
            if sec1.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
                failover_node = sec1
        except KeyError:
            pass
    try:
        return bool(peer_node.connect_to_hublvol(
            primary_node, failover_node=failover_node, role=sec_role))
    except Exception as e:
        logger.warning(
            "connect_to_hublvol(%s -> %s, role=%s) raised: %s",
            peer_node.get_id()[:8], primary_node.get_id()[:8], sec_role, e)
        return False


def _verify_or_reconnect_peer_hublvol(peer_node, primary_node):
    """Up to _HUBLVOL_MAX_ATTEMPTS verify+reconnect attempts, with
    exponential backoff (1, 2, 4, 8, 16 s) between them. Returns True iff
    any attempt yields a verified-open hublvol from ``peer_node`` to
    ``primary_node``; False on exhaustion.

    Each attempt runs in three steps:

      1. existing ``health_controller._check_sec_node_hublvol`` with
         ``auto_fix=True``. This is the project's pre-existing check;
         keeping it in the loop means we benefit from any new heuristics
         that land there in the future.

      2. STRICT verify: ``_hublvol_verified_open`` confirms the bdev_nvme
         controller has at least one enabled path AND the namespace bdev
         ``<hublvol.bdev_name>n1`` is registered (so a subsequent
         ``spdk_bdev_open_ext`` would not return -ENODEV the way it did
         in the 2026-05-21 LVS_753 incident). This step is only meaningful
         when ``primary_node.hublvol`` carries the metadata; without it
         we trust step 1.

      3. on failure of either, force-drive a fresh ``connect_to_hublvol``
         (which re-runs ``bdev_nvme_attach_controller`` as step 1 of its
         own contract) and immediately re-verify strictly.

    On exhaustion the caller's port-allow path aborts the recovering
    node rather than risk unblocking with a half-open peer hublvol.
    """
    label = f"{peer_node.get_id()[:8]} <- {primary_node.get_id()[:8]}"
    have_metadata = bool(primary_node.hublvol)

    for attempt in range(1, _HUBLVOL_MAX_ATTEMPTS + 1):
        # Step 1: existing check (with its embedded auto_fix heuristic).
        existing_ok = False
        try:
            existing_ok = bool(health_controller._check_sec_node_hublvol(
                peer_node, auto_fix=True, primary_node_id=primary_node.get_id()))
        except Exception as e:
            logger.warning(
                "_check_sec_node_hublvol raised on attempt %d for %s: %s",
                attempt, label, e)

        # Step 2: strict verify when metadata is available.
        if existing_ok:
            if not have_metadata or _hublvol_verified_open(peer_node, primary_node):
                logger.info(
                    "Hublvol verified on attempt %d for %s", attempt, label)
                return True
            logger.info(
                "Existing check passed but strict verify failed on "
                "attempt %d for %s; driving forced reconnect",
                attempt, label)

        # Step 3: force a reconnect (only meaningful with metadata) and
        # re-verify strictly. This bypasses the ``not passed`` gate in
        # _check_sec_node_hublvol's auto_fix branch that lets a stale
        # bdev_nvme_controller_list entry suppress the actual reconnect.
        if have_metadata:
            _reconnect_peer_hublvol_once(peer_node, primary_node)
            if _hublvol_verified_open(peer_node, primary_node):
                logger.info(
                    "Hublvol verified after forced reconnect on attempt %d for %s",
                    attempt, label)
                return True

        if attempt < _HUBLVOL_MAX_ATTEMPTS:
            delay = _HUBLVOL_RETRY_DELAYS_SEC[attempt - 1]
            logger.info(
                "Hublvol verify+reconnect attempt %d/%d failed for %s; "
                "sleeping %ds before retry",
                attempt, _HUBLVOL_MAX_ATTEMPTS, label, delay)
            time.sleep(delay)

    logger.error(
        "Hublvol verify+reconnect exhausted %d attempts for %s",
        _HUBLVOL_MAX_ATTEMPTS, label)
    return False


def _abort_recovering_node(node, reason):
    """Abort port-allow: kill SPDK on the recovering node, mark OFFLINE,
    do NOT issue port_allowed. Used when one or more online peers cannot
    establish a verified-open hublvol within the retry budget.

    The rationale matches storage_node_ops._abort_and_unblock (used in
    the non-leader-restart abort path): if we cannot prove every online
    peer has a usable hublvol, letting the primary's port re-open
    creates the split-brain window we just spent retries trying to
    avoid. Killing the primary's SPDK lets the secondary (whose own
    failover path is independent of this task) take over cleanly.
    """
    logger.error(
        "Aborting recovering node %s: %s",
        node.get_id(), reason)
    try:
        storage_events.snode_restart_failed(node)
    except Exception:
        # Event emission must never block the abort itself.
        logger.exception("Failed to emit snode_restart_failed event for %s", node.get_id())
    try:
        snode_api = node.client(timeout=5, retry=5)
        snode_api.spdk_process_kill(node.rpc_port, node.cluster_id)
    except Exception:
        logger.exception("Failed to kill SPDK on %s during port-allow abort", node.get_id())
    try:
        storage_node_ops.set_node_status(
            node.get_id(), StorageNode.STATUS_OFFLINE,
            caused_by="restart_cleanup")
    except Exception:
        logger.exception(
            "Failed to mark %s OFFLINE during port-allow abort", node.get_id())


def exec_port_allow_task(task):
    # get new task object because it could be changed from cancel task
    task = db.get_task_by_id(task.uuid)

    if task.canceled:
        task.function_result = "canceled"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    try:
        node = db.get_storage_node_by_id(task.node_id)
    except KeyError:
        task.function_result = "node not found"
        task.status = JobSchedule.STATUS_DONE
        task.write_to_db(db.kv_store)
        return

    if node.status not in [StorageNode.STATUS_DOWN, StorageNode.STATUS_ONLINE]:
        msg = f"Node is {node.status}, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # check node ping
    ping_check = health_controller._check_node_ping(node.mgmt_ip)
    logger.info(f"Check: ping mgmt ip {node.mgmt_ip} ... {ping_check}")
    if not ping_check:
        time.sleep(1)
        ping_check = health_controller._check_node_ping(node.mgmt_ip)
        logger.info(f"Check 2: ping mgmt ip {node.mgmt_ip} ... {ping_check}")

    if not ping_check:
        msg = "Node ping is false, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # check node ping
    logger.info("connect to remote devices")
    # connect to remote devs
    try:
        node_bdevs = node.rpc_client().get_bdevs()
        logger.debug(node_bdevs)
        if node_bdevs:
            node_bdev_names = {}
            for b in node_bdevs:
                node_bdev_names[b['name']] = b
                for al in b['aliases']:
                    node_bdev_names[al] = b
        else:
            node_bdev_names = {}
        remote_devices = storage_node_ops._connect_to_remote_devs(node, reattach=False)
        if not remote_devices:
            msg = "Node unable to connect to remote devs, retry task"
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        else:
            # Re-read fresh before writing to avoid overwriting concurrent changes
            node = db.get_storage_node_by_id(task.node_id)
            node.remote_devices = remote_devices
            node.write_to_db()

        logger.info("connect to remote JM devices")
        remote_jm_devices = storage_node_ops._connect_to_remote_jm_devs(node)
        if not remote_jm_devices or len(remote_jm_devices) < 2:
            msg = "Node unable to connect to remote JMs, retry task"
            logger.info(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return
        else:
            # Re-read fresh before writing to avoid overwriting concurrent changes
            node = db.get_storage_node_by_id(task.node_id)
            node.remote_jm_devices = remote_jm_devices
            node.write_to_db()


    except Exception as e:
        logger.error(e)
        msg = "Error when connect to remote devs, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    # After a network outage, every distrib on the recovering node has a
    # stale view of remote devices (status_device=48 / is_device_available_read=0),
    # which causes DISTRIBD "Unable to read stripe" errors as soon as the
    # port is unblocked. Push the full cluster map now (covers all nodes'
    # devices, including our own) so the distribs have up-to-date status
    # before any IO is allowed through.
    logger.info("Sending full cluster map to recovering node")
    if not distr_controller.send_cluster_map_to_node(node):
        msg = "Failed to send cluster map to recovering node, retry task"
        logger.warning(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    logger.info("Cluster map sent; waiting 5s for JMs to connect")
    time.sleep(5)

    snode = db.get_storage_node_by_id(node.get_id())
    sec_ids = []
    if node.secondary_node_id:
        sec_ids.append(node.secondary_node_id)
    if node.tertiary_node_id:
        sec_ids.append(node.tertiary_node_id)
    for sec_id in sec_ids:
        sec_node = db.get_storage_node_by_id(sec_id)
        if sec_node and sec_node.status == StorageNode.STATUS_ONLINE:
            try:
                ret = sec_node.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
                if ret:
                    lvs_info = ret[0]
                    if "lvs leadership" in lvs_info and lvs_info['lvs leadership']:
                        jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(snode.jm_vuid)
                        retries = 10
                        while jc_compression_is_active:
                            if retries <= 0:
                                logger.warning("Timeout waiting for JC compression task to finish")
                                break
                            retries -= 1
                            logger.info(
                                f"JC compression task found on node: {sec_node.get_id()}, retrying in 60 seconds")
                            time.sleep(60)
                            jc_compression_is_active = sec_node.rpc_client().jc_compression_get_status(
                                snode.jm_vuid)
            except Exception as e:
                logger.error(e)
                return

    if node.lvstore_status == "ready":
        lvstore_check = health_controller._check_node_lvstore(node.lvstore_stack, node, auto_fix=True)
        if not lvstore_check:
            msg = "Node LVolStore check fail, retry later"
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        sec_ids = []
        if node.secondary_node_id:
            sec_ids.append(node.secondary_node_id)
        if node.tertiary_node_id:
            sec_ids.append(node.tertiary_node_id)
        if sec_ids:
            # Primary-side hublvol exposure is the precondition; if the
            # primary hasn't (re)registered its hublvol subsystem yet there's
            # nothing useful the port_allow runner can drive from the peer
            # side. Stay with suspend-and-retry for this gate -- this is a
            # primary-local recovery step, not a peer reconnect.
            primary_hublvol_check = health_controller._check_node_hublvol(node)
            if not primary_hublvol_check:
                msg = "Node hublvol check fail, retry later"
                logger.warning(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return

            # Peer hublvol gate: for each ONLINE secondary/tertiary, drive
            # ``connect_to_hublvol`` (which re-attaches the bdev_nvme
            # controller as step 1) and verify the result with the strict
            # _hublvol_verified_open check. Up to _HUBLVOL_MAX_ATTEMPTS
            # attempts with exponential backoff per peer. On exhaustion
            # the recovering node is aborted -- we will NOT issue
            # port_allowed with a half-open peer hublvol (that's exactly
            # how the 2026-05-21 18:52:56 split-brain happened).
            failing_peers = []
            for sec_id in sec_ids:
                try:
                    sec_node = db.get_storage_node_by_id(sec_id)
                except KeyError:
                    continue
                if not sec_node or sec_node.status != StorageNode.STATUS_ONLINE:
                    # Skip peers that aren't currently online -- the spec's
                    # explicit exception "secondary is not online at that
                    # time": we cannot gate on a peer that has nothing to
                    # connect with. The peer will (re)establish its
                    # hublvol via its own restart path when it comes back.
                    continue
                if not _verify_or_reconnect_peer_hublvol(sec_node, node):
                    failing_peers.append(sec_id)

            if failing_peers:
                reason = (
                    f"hublvol not verified-open on {len(failing_peers)} peer(s) "
                    f"after {_HUBLVOL_MAX_ATTEMPTS} attempts: " +
                    ", ".join(p[:8] for p in failing_peers))
                _abort_recovering_node(node, reason)
                task.function_result = (
                    f"Aborted recovering node {node.get_id()[:8]}: {reason}")
                task.status = JobSchedule.STATUS_DONE
                task.write_to_db(db.kv_store)
                return

    if task.status != JobSchedule.STATUS_RUNNING:
        task.status = JobSchedule.STATUS_RUNNING
        task.write_to_db(db.kv_store)

    try:
        # wait for lvol sync delete
        lvol_sync_del_found = tasks_controller.get_lvol_sync_del_task(task.cluster_id, task.node_id)
        while lvol_sync_del_found:
            logger.info("Lvol sync delete task found, waiting")
            time.sleep(3)
            lvol_sync_del_found = tasks_controller.get_lvol_sync_del_task(task.cluster_id, task.node_id)

        port_number = task.function_params["port_number"]

        # The previous implementation here did force-failback: if a peer
        # was the current LVS leader (because of an earlier failover from
        # `node`), it would block the peer's port, demote the peer, take
        # leadership locally on `node`, and additionally walk every
        # secondary and block + demote them too. That was wrong on two
        # counts:
        #
        #   - A writer conflict / leadership contention only ever blocks
        #     the *primary* (the JM heartbeat detects the dual-writer on
        #     the primary's lvstore and the CP forces the primary's
        #     distribs to non_leader). Secondaries are followers with
        #     bs_nonleader=true and have nothing to demote.
        #   - If a failover already succeeded and the peer is the
        #     legitimately-elected new leader, the cluster is correctly
        #     serving IO via the peer. There is no problem to solve.
        #     Blocking the new leader's port cuts client IO that was
        #     being served correctly, and the synchronous demote+take
        #     opens a fresh writer-conflict window.
        #
        # See incident 2026-05-02 (k8s_native_failover_ha-20260502-101452):
        # at 15:51:01 the JM forced worker5's LVS_4729 distribs to
        # non_leader (writer conflict). Failover transferred leadership
        # to worker1 (legitimate new primary). At 15:51:32 the
        # health-check on worker5's port 4434 failed (worker5 was DOWN)
        # and queued a port_allow. At 15:51:44.818 the runner here
        # logged "Current leader for LVS_4729 is peer 46544aff…;
        # demoting before port_allow on ad04496b…" and blocked
        # worker1's port + force-demoted worker1 — directly producing
        # client IO errors and a follow-on writer conflict.
        #
        # port_allow's correct scope is just allowing the port on the
        # recovering node. Leadership belongs to the JM heartbeat /
        # writer-conflict resolution mechanism, not to this task.

    except Exception as e:
        logger.error(e)
        return

    logger.info(f"Allow port {port_number} on node {node.get_id()}")
    fw_api = FirewallClient(snode, timeout=5, retry=2)
    port_type = "tcp"
    if node.active_rdma:
        port_type = "udp"
    fw_api.firewall_set_port(port_number, port_type, "allow", node.rpc_port)
    tcp_ports_events.port_allowed(node, port_number)

    task.function_result = f"Port {port_number} allowed on node"
    task.status = JobSchedule.STATUS_DONE
    task.write_to_db(db.kv_store)


def _main():
    logger.info("Starting Tasks runner...")
    while True:
        try:
            db.get_clusters()
        except Exception as e:
            logger.error(f"Failed to get clusters: {e}")
            time.sleep(3)
            continue
        clusters = db.get_clusters()
        if not clusters:
            logger.error("No clusters found!")
        else:
            for cl in clusters:
                if cl.status == Cluster.STATUS_IN_ACTIVATION:
                    continue
                tasks = db.get_job_tasks(cl.get_id(), reverse=False)
                for task in tasks:
                    if task.function_name == JobSchedule.FN_PORT_ALLOW:
                        if task.status != JobSchedule.STATUS_DONE:
                            # Lease gate: skip a task another live runner host owns.
                            if not tasks_controller.claim_task(task):
                                logger.info(f"Port-allow task {task.uuid} owned by another runner host; skipping")
                                continue
                            exec_port_allow_task(task)

        time.sleep(5)


if __name__ == "__main__":
    _main()
