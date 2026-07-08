# coding=utf-8
import time


from simplyblock_core import db_controller, utils, storage_node_ops, distr_controller, port_block
from simplyblock_core.controllers import (
    tcp_ports_events, health_controller, tasks_controller, storage_events, device_controller,
)
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.models.nvme_device import NVMeDevice
from simplyblock_core.models.lvol_model import LVol

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


def _reconnect_own_sec_tert_hublvols(node):
    """Reverse direction of the peer hublvol gate: (re)establish the
    RECOVERING node's own hublvol wiring for every lvstore it serves as
    secondary or tertiary.

    A network-outage recovery has no ``recreate_all_lvstores`` pass, so
    without this the node's follower-side hublvols were only repaired by
    each primary's periodic health loop (30 s cadence, gated on the
    primary being ONLINE + the peer holding leadership) — i.e. AFTER the
    port was already allowed, leaving a window where the node's redirect
    listeners serve client IO they cannot forward.

    Per lvstore this drives:

      OUTBOUND — the node's bdev_nvme controller + lvs opts + connect_hublvol
        toward the primary, via the same verify-or-reconnect retry ladder as
        the forward gate (``_verify_or_reconnect_peer_hublvol`` derives
        role / failover_node from topology, so the tertiary's dual-path
        multipath is handled identically to the secondary's single path).

      INBOUND (secondary role only) — re-expose the shared-NQN secondary
        hublvol (non_optimized, min_cntlid=1000) that the tertiary
        multipaths to, and top up the tertiary's failover path to this
        node. With the primary ONLINE the top-up is best-effort: it only
        matters at the NEXT failover, while suspending this task would
        keep the cluster degraded NOW.

      OFFLINE-PRIMARY HARD GATE (secondary role) — if the primary went
        OFFLINE while this node was partitioned, the tertiary is the
        acting leader: the tertiary->secondary hublvol path becomes a
        HARD GATE here (the tertiary must be able to redirect to this
        node before any client can land on it). The ANA promotion of this
        node's listeners is deliberately NOT done here — it runs in
        exec_port_allow_task strictly AFTER the port release, per-lvol,
        so IO drains back from the tertiary gradually. The tertiary's
        ANA is never touched (it stays permanently non_optimized).

      ACTING-LEADER FOLLOW (tertiary role) — if the primary is out and
        the secondary is the acting leader, re-wire this tertiary's
        redirect toward the secondary (attach target sec_1, LVS metadata
        from the configured primary; the takeover hublvol keeps the
        primary's NQN/port).

    The OUTBOUND leg toward a non-ONLINE primary is skipped — there is
    nothing to attach to, and that primary's own recovery path (restart
    flow or its port-allow peer gate + leadership failback) re-drives it
    when the primary returns.

    Returns ``(ok, msg)``; ``ok=False`` means the caller must suspend the
    task and retry — the port must NOT be allowed with a broken follower
    redirect.
    """
    primary_ids = []
    if node.lvstore_stack_secondary:
        primary_ids.append(node.lvstore_stack_secondary)
    if node.lvstore_stack_tertiary and node.lvstore_stack_tertiary not in primary_ids:
        primary_ids.append(node.lvstore_stack_tertiary)

    for pid in primary_ids:
        try:
            primary = db.get_storage_node_by_id(pid)
        except KeyError:
            logger.warning("Primary %s referenced by %s not found; skipping",
                           pid[:8], node.get_id()[:8])
            continue
        if primary.lvstore_status != "ready" or not primary.hublvol:
            logger.info(
                "Skipping own-hublvol reconnect toward %s (lvstore_status=%s, "
                "hublvol=%s)", pid[:8], primary.lvstore_status, bool(primary.hublvol))
            continue

        primary_online = primary.status == StorageNode.STATUS_ONLINE
        is_secondary_role = (primary.secondary_node_id == node.get_id())

        # OUTBOUND toward a live primary. When the primary is not ONLINE
        # there is nothing to attach to; that leg is re-driven by the
        # primary's own recovery (its port_allow peer gate / restart flow).
        if primary_online:
            if not _verify_or_reconnect_peer_hublvol(node, primary):
                return False, (
                    f"own hublvol to primary {pid[:8]} not verified-open after "
                    f"{_HUBLVOL_MAX_ATTEMPTS} attempts")

            # Stale-leadership convergence: if this node was the acting
            # leader when it got partitioned and the primary failed back
            # WITHOUT it (this node was in disconnected_peers: not demoted,
            # not re-committed), it comes back still claiming leadership.
            # When the hublvol controller was destroyed during the outage
            # the verify above already forced a connect_to_hublvol
            # re-commit; but after a short blip the controller survives,
            # the verify passes untouched, and the stale claim would ride
            # into a dual-leader writer conflict the moment IO returns.
            # Converge explicitly: demote (leader=False, bs_nonleadership)
            # + force_to_non_leader, re-commit the follower role, and
            # verify the claim is gone — all while the follower listener
            # is still fenced, so it is a quiet-window transition. Only
            # when the primary actually leads: if the primary is up but
            # NOT leading, this node is the legitimate acting leader and
            # demoting it would leave the LVS with zero writers (the
            # primary's own recovery performs that failback).
            node_claims = _read_lvs_leadership(node, primary.lvstore)
            if node_claims is None:
                return False, f"own leadership read for {primary.lvstore} failed"
            if node_claims:
                primary_leads = _read_lvs_leadership(primary, primary.lvstore)
                if primary_leads is None:
                    return False, f"leadership read on primary {pid[:8]} failed"
                if primary_leads:
                    logger.warning(
                        "Recovering follower %s still claims leadership of %s "
                        "while primary %s leads; demoting by re-commit",
                        node.get_id()[:8], primary.lvstore, pid[:8])
                    try:
                        node_rpc = node.rpc_client(timeout=5, retry=1)
                        node_rpc.bdev_lvol_set_leader(
                            primary.lvstore, leader=False, bs_nonleadership=True)
                        node_rpc.bdev_distrib_force_to_non_leader(primary.jm_vuid)
                    except Exception as e:
                        return False, (
                            f"stale-leader demote for {primary.lvstore} failed: {e}")
                    if not _reconnect_peer_hublvol_once(node, primary):
                        return False, (
                            f"follower re-commit after stale-leader demote failed "
                            f"for {primary.lvstore}")
                    if _read_lvs_leadership(node, primary.lvstore):
                        return False, (
                            f"stale leadership claim for {primary.lvstore} "
                            f"persists after demote")
                else:
                    logger.info(
                        "Node %s leads %s and primary %s does not — leaving "
                        "leadership in place; the primary's own recovery "
                        "performs the failback",
                        node.get_id()[:8], primary.lvstore, pid[:8])
        else:
            logger.info(
                "Primary %s is %s; skipping outbound reconnect (its own "
                "recovery re-drives that leg)", pid[:8], primary.status)

        if is_secondary_role:
            # INBOUND exposure — local to this node, so driven regardless of
            # the primary's status: with the primary out, the tertiary's
            # failover (and the ANA switch-back below) depends on this leg.
            try:
                cluster = db.get_cluster_by_id(node.cluster_id)
                rpc = node.rpc_client(timeout=5, retry=1)
                if not rpc.subsystem_list(primary.hublvol.nqn):
                    logger.info(
                        "Secondary hublvol NQN missing on recovering secondary "
                        "%s for %s; recreating", node.get_id()[:8], primary.lvstore)
                    node.create_secondary_hublvol(primary, cluster.nqn)
            except Exception as e:
                return False, f"secondary-hublvol exposure for {pid[:8]} failed: {e}"

            # tertiary -> secondary hublvol path. With the primary ONLINE this
            # is a best-effort multipath top-up (only matters at the NEXT
            # failover). With the primary OUT it is a HARD GATE: the tertiary
            # is the acting leader right now and clients may land on this
            # node the moment its port opens — the tertiary must be able to
            # redirect to it first, or a dual-writer window opens.
            tert = None
            if primary.tertiary_node_id:
                try:
                    tert = db.get_storage_node_by_id(primary.tertiary_node_id)
                except KeyError:
                    pass
            if tert and tert.status in (StorageNode.STATUS_ONLINE,
                                        StorageNode.STATUS_DOWN):
                tert_path_ok = False
                try:
                    tert_path_ok = bool(tert.add_hublvol_failover_path(primary, node))
                except Exception as e:
                    logger.warning(
                        "Error re-adding tertiary failover path on %s: %s",
                        tert.get_id()[:8], e)
                if tert_path_ok:
                    logger.info(
                        "Re-added tertiary %s failover path to recovered "
                        "secondary %s for %s",
                        tert.get_id()[:8], node.get_id()[:8], primary.lvstore)
                elif primary_online:
                    logger.warning(
                        "Failed to re-add tertiary %s failover path to "
                        "recovered secondary %s for %s (best-effort; the "
                        "health loop retries)",
                        tert.get_id()[:8], node.get_id()[:8], primary.lvstore)
                else:
                    return False, (
                        f"tertiary {tert.get_id()[:8]} -> secondary hublvol for "
                        f"{primary.lvstore} not connected; refusing to open "
                        f"the port until it is")

        elif not primary_online and primary.secondary_node_id:
            # Tertiary role with the primary out: the acting leader is the
            # secondary (sec_1). Re-wire this tertiary's redirect toward it
            # (attach target sec_1, LVS metadata from the configured primary
            # — the takeover hublvol keeps the primary's NQN/port). Without
            # this, IO landing on the tertiary's reopened listener cannot be
            # forwarded and re-promotes the tertiary against the acting
            # leader.
            sec1 = None
            try:
                sec1 = db.get_storage_node_by_id(primary.secondary_node_id)
            except KeyError:
                pass
            if sec1 and sec1.status == StorageNode.STATUS_ONLINE:
                sec1_ok = False
                try:
                    sec1_ok = bool(node.connect_to_hublvol(
                        sec1, role="tertiary", lvs_node=primary))
                except Exception as e:
                    logger.warning(
                        "connect_to_hublvol(%s -> acting leader %s for %s) "
                        "raised: %s", node.get_id()[:8], sec1.get_id()[:8],
                        primary.lvstore, e)
                if not sec1_ok:
                    return False, (
                        f"hublvol to acting leader {sec1.get_id()[:8]} for "
                        f"{primary.lvstore} not connected")
    return True, ""


_REPL_SUSPEND_MAX_ATTEMPTS = 10


def _failback_leadership_to_primary(node, current_leader, other_peers):
    """Network-outage failback for the recovering configured primary, per the
    data-plane recovery design (2026-07-07, after the 2026-07-06 failback
    incident): the management plane prepares redirection and then steps AWAY —
    it never assigns leadership and never blocks serving ports.

    Sequence (all BEFORE the recovering node's port is unblocked):

      1. ``jc_disable_replication`` on the acting leader;
      2. verified-open hublvol from the secondary AND the tertiary to the
         primary (the peer gate already drove this for ONLINE peers; the
         acting leader is re-verified here even when it has been flipped
         DOWN, since leadership can only move if it can redirect);
      3. demote the acting leader: ``bdev_lvol_set_leader(leader=False)``.
         Leadership is NOT taken on the primary by the CP — the first
         redirected IO triggers the primary's LVS update process (blob md
         reload) which then takes leadership itself. A management-forced
         ``leader=True`` skips that update and the primary serves stale
         blob metadata (extent-metadata corruption, incident 2026-07-06
         18:23 on LVS 13).

    Deliberately absent (root causes of the 2026-07-06 cascade):
      - NO port blocking of the acting leader or any peer: the LVS ports
        carry the peer hublvol/redirect and journal traffic; blocking them
        mid-IO severs redirect chains (DISTRIBD n_unavail_read) and JC
        paths (history_append n_success=0 → node abort).
      - NO in-flight drain window: with hublvols verified first, the
        demoted leader redirects its in-flight IO to the primary, whose
        promotion-on-first-IO handles it.
      - NO ANA manipulation here.

    Returns ``(ok, msg)``; the caller suspends the task on failure.
    """
    lvs_name = node.lvstore
    lvs_jm_vuid = node.jm_vuid

    # 1- quiesce, then suspend journal replication on the acting leader.
    # wait_for_jm_rep_tasks_to_finish loops internally (10x20s) and is
    # advisory (warn-and-proceed, as in recreate_lvstore); the disable is
    # the gate: False = active replication -> re-quiesce and retry HERE,
    # bounded — the wait must happen in this loop, not via task retry.
    repl_suspended = False
    for _attempt in range(_REPL_SUSPEND_MAX_ATTEMPTS):
        try:
            if not current_leader.wait_for_jm_rep_tasks_to_finish(lvs_jm_vuid):
                logger.warning(
                    "JM replication tasks still reported on acting leader %s "
                    "for jm %s (attempt %d/%d); attempting disable anyway",
                    current_leader.get_id()[:8], lvs_jm_vuid,
                    _attempt + 1, _REPL_SUSPEND_MAX_ATTEMPTS)
            if current_leader.rpc_client().jc_disable_replication(lvs_jm_vuid):
                repl_suspended = True
                break
            logger.warning(
                "jc_disable_replication reports active replication on acting "
                "leader %s (attempt %d/%d); re-quiescing",
                current_leader.get_id()[:8], _attempt + 1,
                _REPL_SUSPEND_MAX_ATTEMPTS)
        except Exception as e:
            return False, f"jc_disable_replication on acting leader failed: {e}"
    if not repl_suspended:
        return False, (
            f"could not suspend journal replication on acting leader after "
            f"{_REPL_SUSPEND_MAX_ATTEMPTS} attempts")

    # 2- every reachable follower needs a verified-open hublvol to the
    # primary before leadership can be dropped anywhere (idempotent for the
    # peers the gate already verified).
    for peer in [current_leader] + list(other_peers):
        if peer.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
            continue
        if not _verify_or_reconnect_peer_hublvol(peer, node):
            return False, (
                f"hublvol from {peer.get_id()[:8]} to the primary not "
                f"verified-open; refusing to demote the acting leader")

    # 3- demote the acting leader; the primary promotes itself (and runs
    # the LVS update) on the first redirected IO.
    try:
        current_leader.rpc_client().bdev_lvol_set_leader(lvs_name, leader=False)
    except Exception as e:
        return False, f"failed to demote acting leader: {e}"

    logger.info(
        "Acting leader %s demoted for %s; primary %s will take leadership "
        "on first redirected IO (post-update)",
        current_leader.get_id()[:8], lvs_name, node.get_id()[:8])
    return True, ""


def _read_lvs_leadership(target_node, lvs_name):
    """Read ``lvs leadership`` for ``lvs_name`` on ``target_node``.

    Returns True/False from the lvstore state (an absent lvstore reads as
    False — not a leader), or ``None`` when the RPC failed. Callers must
    treat ``None`` as "unknown" and retry, never as "not leader".
    """
    try:
        ret = target_node.rpc_client(timeout=5, retry=1).bdev_lvol_get_lvstores(lvs_name)
        if not ret or len(ret) == 0:
            return False
        return bool(ret[0].get("lvs leadership"))
    except Exception as e:
        logger.warning("Leadership read for %s on %s failed: %s",
                       lvs_name, target_node.get_id()[:8], e)
        return None


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

    # Data-NIC gate: mgmt reachability alone is not recovery — after a
    # partial partition the mgmt plane often returns first while the
    # storage network is still dark. Everything this task does next
    # (remote-dev reconnects, hublvol wiring, leadership failback, the
    # port unblock itself) rides on the data network, and a still-dark
    # data NIC would surface in the peer hublvol gate as retry-exhaustion
    # -> node ABORT (SPDK kill) — a destructive outcome for a condition
    # that just needs more waiting. Require the node itself to positively
    # confirm at least one data NIC (SnodeAPI ping_ip from the node over
    # the data interface). _check_ping_from_node is tri-state: True (up),
    # False (agent ran the ping, it failed / carrier down), None (SnodeAPI
    # timed out -> inconclusive). Unlike the monitor's DOWN-flip, which
    # merely ignores inconclusive results, recovery needs positive
    # confirmation: anything but at least one True suspends and retries.
    data_results = []
    for data_nic in node.data_nics:
        if data_nic.ip4_address:
            data_ping = health_controller._check_ping_from_node(
                data_nic.ip4_address, ifname=data_nic.if_name, node=node)
            logger.info(f"Check: ping data nic {data_nic.ip4_address} ... {data_ping}")
            data_results.append(data_ping)
    if data_results and not any(r is True for r in data_results):
        msg = "Node data NIC not confirmed reachable, retry task"
        logger.info(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

    logger.info("connect to remote devices")
    # connect to remote devs
    try:
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

    # After a network outage every distrib in the cluster is working from a
    # stale device view: the recovering node's distribs have stale REMOTE
    # state (they were partitioned), and every peer's distribs still carry
    # this node's devices as UNAVAILABLE (marked during the outage). Both
    # must be refreshed BEFORE the failback and the port unblock, or the
    # first IO fails placement/reads (2026-07-06 18:23 incident: 3 of 4
    # sids online=0, "Failed to find available location",
    # n_unavail_read > 2).
    #
    # Order (runs only now, after the inbound/outbound device + JM
    # connections with the recovering node are re-established above):
    #   1- re-admit this node's devices in FDB (CAUSE_NODE_RECOVERY passes
    #      the stale re-online guard — the node's FDB status flips ONLINE
    #      only after the unblock, by design);
    #   2- send an update of the LOCAL devices of the recovering node to
    #      ALL nodes (including the recovering node itself) — also covers
    #      devices that stayed ONLINE in FDB through the outage;
    #   3- send an update of all OTHER nodes' devices to the recovering
    #      node only (targeted events, not a full cluster map push).
    node = db.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.status in (NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_REMOVED,
                          NVMeDevice.STATUS_JM, NVMeDevice.STATUS_NEW):
            continue
        logger.info(
            f"Re-admitting device {dev.get_id()} (was {dev.status}) before "
            f"port allow on {node.get_id()}")
        if not device_controller.device_set_online(
                dev.get_id(), cause=device_controller.CAUSE_NODE_RECOVERY):
            msg = f"Device {dev.get_id()} re-admit refused, retry task"
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

    logger.info("Broadcasting local device status of recovering node to all nodes")
    node = db.get_storage_node_by_id(task.node_id)
    for dev in node.nvme_devices:
        if dev.status in (NVMeDevice.STATUS_JM, NVMeDevice.STATUS_NEW):
            continue
        try:
            distr_controller.send_dev_status_event(dev, dev.status)
        except Exception as e:
            msg = (f"Local device status broadcast for {dev.get_id()} "
                   f"failed: {e}, retry task")
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

    logger.info("Sending other nodes' device status events to recovering node")
    for cluster_node in db.get_storage_nodes_by_cluster_id(task.cluster_id):
        if cluster_node.get_id() == node.get_id():
            continue
        for dev in cluster_node.nvme_devices:
            if dev.status in (NVMeDevice.STATUS_JM, NVMeDevice.STATUS_NEW):
                continue
            try:
                distr_controller.send_dev_status_event(
                    dev, dev.status, target_node=node)
            except Exception as e:
                msg = (f"Device status event for {dev.get_id()} to recovering "
                       f"node failed: {e}, retry task")
                logger.warning(msg)
                task.function_result = msg
                task.status = JobSchedule.STATUS_SUSPENDED
                task.write_to_db(db.kv_store)
                return

    logger.info("Device events sent; waiting 5s for JMs to connect")
    time.sleep(5)

    # The recovering node's OWN follower-side hublvols — the lvstores it
    # serves as secondary or tertiary — must be wired BEFORE its port is
    # allowed: its redirect listeners for those lvstores start accepting
    # client IO the moment the port opens, and without a committed hublvol
    # they cannot forward it. Previously this direction was left to each
    # primary's periodic health loop (30s cadence) and landed only AFTER
    # the port was allowed.
    node = db.get_storage_node_by_id(task.node_id)
    own_hublvols_ok, own_msg = _reconnect_own_sec_tert_hublvols(node)
    if not own_hublvols_ok:
        msg = f"Own sec/tert hublvol reconnect failed: {own_msg}, retry task"
        logger.warning(msg)
        task.function_result = msg
        task.status = JobSchedule.STATUS_SUSPENDED
        task.write_to_db(db.kv_store)
        return

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

    except Exception as e:
        logger.error(e)
        return

    # --- Leadership failback, BEFORE the port is unblocked --------------
    #
    # History: an earlier force-failback here was removed after incident
    # 2026-05-02 (k8s_native_failover_ha-20260502-101452) — it demoted a
    # legitimately-elected new leader with NO fencing (peer hublvols were
    # not verified, IO was not drained), producing client IO errors and a
    # follow-on writer conflict. It is reinstated here WITH that fencing:
    # this block runs only after (a) every online peer's hublvol to the
    # recovering primary is verified-open (peer gate above), and (b) the
    # recovering node's own follower-side hublvols are wired (own-hublvol
    # gate above) — so the moment leadership moves, every follower can
    # redirect. Leaving leadership on the peer instead ("no problem to
    # solve") turned out wrong in practice: nothing on the no-restart
    # recovery path ever reconciles the ex-leader's state afterwards, its
    # redirect stays broken, and the monitor flips it DOWN, unable to
    # redirect IO to the primary.
    node = db.get_storage_node_by_id(task.node_id)
    if node.lvstore and node.lvstore_status == "ready" and \
            not tasks_controller.get_active_node_restart_task(task.cluster_id, task.node_id):
        failback_peers = []
        for sec_id in [node.secondary_node_id, node.tertiary_node_id]:
            if not sec_id:
                continue
            try:
                peer = db.get_storage_node_by_id(sec_id)
            except KeyError:
                continue
            if peer.status in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
                failback_peers.append(peer)

        # Identify the current leader among reachable peers. A failed
        # leadership read is NOT "peer is no leader" — mirroring
        # recreate_lvstore, we refuse to guess and retry the task.
        current_leader = None
        leadership_read_failed = None
        for peer in failback_peers:
            try:
                ret = peer.rpc_client(timeout=5, retry=2).bdev_lvol_get_lvstores(node.lvstore)
                if ret and len(ret) > 0 and ret[0].get("lvs leadership"):
                    current_leader = peer
                    logger.info("Current leader for %s is peer %s",
                                node.lvstore, peer.get_id()[:8])
                    break
            except Exception as e:
                leadership_read_failed = f"{peer.get_id()[:8]}: {e}"

        if current_leader is None and leadership_read_failed:
            msg = f"Leadership read failed on peer {leadership_read_failed}, retry task"
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

        if current_leader is not None:
            failback_ok, failback_msg = _failback_leadership_to_primary(
                node, current_leader,
                [p for p in failback_peers if p is not current_leader])
        else:
            # No peer holds leadership. The CP does NOT assign it: the
            # primary promotes itself on the first arriving IO, which also
            # runs the LVS update process first (management-forced
            # leader=True skips that update — 2026-07-06 stale-metadata
            # corruption). Nothing to converge here.
            failback_ok, failback_msg = True, ""

        if not failback_ok:
            msg = f"Leadership failback incomplete: {failback_msg}, retry task"
            logger.warning(msg)
            task.function_result = msg
            task.status = JobSchedule.STATUS_SUSPENDED
            task.write_to_db(db.kv_store)
            return

    logger.info(f"Allow port {port_number} on node {node.get_id()}")
    port_block.set_port(node, port_number, block=False, timeout=5, retry=2)
    tcp_ports_events.port_allowed(node, port_number)

    # Deferred ANA failover, AFTER the port release: a secondary recovering
    # while its primary is OFFLINE returns with non-optimized listeners
    # (trigger_ana_failover_for_node skipped the promotion because this node
    # wasn't ONLINE at the time). Promote its subsystems to optimized now —
    # deliberately post-unblock and per-lvol, so IO drains back from the
    # tertiary to this secondary gradually rather than gating the recovery.
    # The tertiary→secondary hublvol hard gate above already guaranteed the
    # tertiary can redirect here before any client lands on this node.
    if node.lvstore_stack_secondary:
        try:
            # reverse ref holds the primary's node id (str at runtime despite
            # the model's List[dict] annotation — see field-semantics note)
            ana_primary = db.get_storage_node_by_id(str(node.lvstore_stack_secondary))
        except KeyError:
            ana_primary = None
        if ana_primary and ana_primary.status == StorageNode.STATUS_OFFLINE:
            logger.info(
                "Primary %s is OFFLINE; promoting recovered secondary %s to "
                "optimized for %s lvols (post-unblock, gradual)",
                ana_primary.get_id()[:8], node.get_id()[:8], ana_primary.lvstore)
            for lvol in db.get_lvols_by_node_id(ana_primary.get_id()):
                if lvol.status not in (LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE):
                    continue
                try:
                    storage_node_ops._set_lvol_ana_on_node(lvol, node, "optimized")
                except Exception as e:
                    logger.warning(
                        "Deferred ANA promotion of %s on %s failed: %s",
                        lvol.nqn, node.get_id()[:8], e)

    # Self-heal devices that went UNAVAILABLE while this node was fenced/partitioned
    # (e.g. forced globally UNAVAILABLE by the remote-IO quorum in
    # main_distr_event_collector during the ONLINE-status-lag window at the start of
    # a network outage). port_allow runs as the LAST step of node recovery, so by
    # definition the node's ports are being unblocked and the node is healthy again
    # -- every one of its local devices must serve.
    #
    # This must NOT be gated on node.status == ONLINE: port_allow completes a couple
    # of seconds BEFORE the storage-node monitor flips the node's status to ONLINE,
    # so gating on it skipped the re-admit every time and left the device stranded
    # (UNAVAILABLE with no other recovery path -- device_monitor's auto-restart only
    # touches io_error devices) until the cluster later suspended on the phantom
    # offline-device count. Flip the node's devices back online regardless of prior
    # device state; REMOVED is the one terminal state we never resurrect.
    try:
        node = db.get_storage_node_by_id(node.get_id())
        for dev in node.nvme_devices:
            if dev.status in (NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_REMOVED):
                continue
            logger.info(
                f"Re-admitting device {dev.get_id()} (was {dev.status}) after port "
                f"allow on {node.get_id()}")
            if not device_controller.device_set_online(dev.get_id()):
                # device_set_state refuses a device ONLINE while its node is not
                # ONLINE (stale re-online guard), and port_allow usually runs a
                # couple of seconds BEFORE the monitor flips the node ONLINE. Not
                # an error path: the monitor's DOWN/UNREACHABLE -> ONLINE clear
                # re-admits the node's devices right after the flip.
                logger.warning(
                    f"Re-admit of device {dev.get_id()} refused (node "
                    f"{node.get_id()} is {node.status}, not yet ONLINE); the "
                    f"node-online clear in storage_node_monitor will re-admit it")
    except Exception as e:
        logger.error(f"Device re-admit after port allow failed: {e}")

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
