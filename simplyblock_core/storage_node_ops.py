# coding=utf- 8
import copy
import datetime
import json
import logging
import math
import platform
import socket
import subprocess

import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Optional

import threading

import time
import uuid

import docker
from docker.types import LogConfig
from pydantic import SecretStr
from tenacity import RetryError, Retrying, before_sleep_log, retry_if_exception_type, stop_after_attempt, wait_fixed

from simplyblock_core import constants, scripts, distr_controller, cluster_ops
from simplyblock_core import utils
from simplyblock_core import jm_raid
from simplyblock_core.utils import port_block
from simplyblock_core.utils import rpc_budget
from simplyblock_core.utils import hublvol_reconnect
from simplyblock_core.constants import LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID, LINUX_DRV_MASS_STORAGE_ID
from simplyblock_core.controllers import lvol_controller, storage_events, snapshot_controller, device_events, \
    device_controller, tasks_controller, health_controller, tcp_ports_events, qos_controller
from simplyblock_core.controllers.host_auth import _reapply_allowed_hosts
from simplyblock_core import db_controller as db_module
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.iface import IFace
from simplyblock_core.models.job_schedule import JobSchedule
from simplyblock_core.models.lvol_model import LVol
from simplyblock_core.models.nvme_device import NVMeDevice, JMDevice, RemoteDevice, RemoteJMDevice
from simplyblock_core.models.snapshot import SnapShot
from simplyblock_core.models.storage_node import StorageNode
from simplyblock_core.release_upgrades import jc_compression_upgrade
from simplyblock_core.models.cluster import Cluster
from simplyblock_core.prom_client import PromClient
from simplyblock_core.rpc_client import (  # noqa: F401  (RPCClient kept as a patch target for tests)
    JC_REMOVE_JM_NOT_USED, JC_REMOVE_JM_STILL_IN_USE, RPC_UNSUPPORTED, RPCClient,
    RPCErrorCode, RPCRemoteError, RPCException, namespace_matches)
from simplyblock_core.snode_client import SNodeClient, SNodeClientException
from simplyblock_core.utils import dial_backoff
from simplyblock_web import node_utils
from simplyblock_core.utils import addNvmeDevices
from simplyblock_core.utils import pull_docker_image_with_retry
import os


logger = utils.get_logger(__name__)


def kill_client_kwargs(force=False):
    """SNodeClient kwargs for the spdk_process_kill RPC.

    The kill goes to the agent ON the node being killed, so when that agent
    is not serving, patience buys nothing. SNodeClient defaults to
    Retry(total=retry, backoff_factor=1, connect=retry): with retry=10 the
    backoff alone is 0+2+4+8+16+32+64+120+120+120 = 486s (urllib3 caps each
    at 120s), so a --force shutdown against a dead agent blocked for ~9
    minutes on 2026-08-27 -- the exact case --force exists to escape.

    connect_retry=0 makes a refused connection fail at once (SNodeClient's
    own documented rationale); read retries still cover a slow-but-alive
    agent. The graceful path keeps its patience.
    """
    if force:
        return {"timeout": 10, "retry": 2, "connect_retry": 0}
    return {"timeout": 10, "retry": 10}


def ensure_spdk_stopped(client_factory, rpc_port, cluster_id):
    """Best-effort kill of an SPDK that is still running before a restart.

    A node's status can read OFFLINE while its SPDK is very much alive: the
    status write is lost when the DB is unavailable at shutdown time (FDB
    filled its disk mid-shutdown, 2026-08-27). spdk_process_start against a
    live process is a no-op, so three node_restart tasks and a --force
    restart all reported success while the container kept its original ~1h
    uptime, and the node could never recover without manual intervention.

    Kill first so a restart always bounces the process. Fast-failing and
    non-fatal: with nothing running this is a no-op, and it must never block
    or fail the restart path.

    Returns True if a kill was accepted, False otherwise.
    """
    try:
        client_factory(**kill_client_kwargs(force=True)).spdk_process_kill(
            rpc_port, cluster_id)
        logger.info("Killed a pre-existing SPDK process before restart")
        return True
    except Exception as exc:  # noqa: BLE001 - nothing to kill is the normal case
        logger.debug("No pre-existing SPDK process to kill: %s", exc)
        return False


class LVSRestartRequiredError(Exception):
    """Raised when an LVS fails to recover via ``bdev_examine`` during
    activation-mode recreate. The node's SPDK holds partial state that
    the activation path cannot safely reconcile: the caller should
    reject the (re)activation and tell the operator to restart that
    specific node before trying again.
    """

    def __init__(self, node_id, lvs_name, detail=""):
        self.node_id = node_id
        self.lvs_name = lvs_name
        self.detail = detail
        msg = (f"LVS {lvs_name} did not recover on examine on node "
               f"{node_id}")
        if detail:
            msg += f": {detail}"
        msg += ". Restart this node before continuing."
        super().__init__(msg)


def _rpc_subsystem_has_ns(rpc_client, nqn, nsid=None, bdev_name=None, uuid=None):
    """True iff the subsystem has the namespace identified by nsid/bdev_name/uuid.

    Matching is delegated to :func:`rpc_client.namespace_matches`, which
    accepts the namespace UUID as well as the bdev name — SPDK reports an
    lvol's namespace under its raw UUID rather than the ``<lvs>/<lvol>``
    alias, and a bdev_name-only comparison therefore misses it. Always pass
    ``uuid`` when the caller knows it.
    """
    try:
        subsystem = rpc_client.subsystem_get(nqn)
        if subsystem is None:
            return False
        return any(
            namespace_matches(ns, dev_name=bdev_name, nsid=nsid, uuid=uuid)
            for ns in subsystem.get('namespaces', []) or []
        )
    except Exception:
        return False


def _rpc_wait_subsystem_has_ns(rpc_client, nqn, nsid=None, bdev_name=None,
                               uuid=None, tries=10, delay=0.2):
    """:func:`_rpc_subsystem_has_ns` with a bounded poll.

    ``nvmf_subsystem_add_ns`` can report success a moment before the namespace
    is observable on the subsystem, so a single read is not enough to conclude
    "empty subsystem" and permanently skip listener creation. Polling briefly
    keeps the post-condition strict without turning a propagation delay into
    a volume that has silently lost a path.
    """
    for attempt in range(max(1, tries)):
        if _rpc_subsystem_has_ns(rpc_client, nqn, nsid=nsid,
                                 bdev_name=bdev_name, uuid=uuid):
            return True
        if attempt + 1 < tries:
            time.sleep(delay)
    return False


def _rpc_subsystem_has_listener(rpc_client, nqn, trtype, traddr, trsvcid):
    """True iff the subsystem already has a matching listener."""
    try:
        subsystem = rpc_client.subsystem_get(nqn)
        if subsystem is None:
            return False
        for la in subsystem.get('listen_addresses', []) or []:
            if (la.get('trtype', '').upper() == trtype.upper()
                    and la.get('traddr') == traddr
                    and str(la.get('trsvcid')) == str(trsvcid)):
                return True
        return False
    except Exception:
        return False


def _rpc_bdev_exists(rpc_client, name):
    """True iff a bdev with the given name is visible to SPDK."""
    try:
        ret = rpc_client.get_bdevs(name)
        return bool(ret)
    except Exception:
        return False


def _rpc_lvstore_exists(rpc_client, lvs_name):
    """True iff bdev_lvol_get_lvstores(lvs_name) returns a live lvstore."""
    try:
        ret = rpc_client.bdev_lvol_get_lvstores(lvs_name)
        return bool(ret)
    except Exception:
        return False


def _kill_spdk_until_dead(snode: StorageNode, max_attempts=3, poll_per_attempt_sec=5,
                           poll_interval=0.25):
    """Kill SPDK on `snode` and return only after it is verifiably gone.

    Per design: any abort during restart MUST kill SPDK so the next attempt
    starts from a clean process — leftover bdevs (raid0_<vuid>, lvol
    subsystems) cause "Duplicate bdev name" / "Subsystem already exists"
    failures on retry that loop the auto-restart forever.

    The previous behavior (single 5 s soft window, log warning, proceed)
    silently left zombies behind. We now retry the kill until SPDK is
    confirmed down. Bounded total wall-clock = max_attempts *
    poll_per_attempt_sec so a wedged docker daemon cannot trap the caller.
    Returns True if SPDK died, False if all attempts exhausted (caller is
    responsible for whatever comes next; the node should still be marked
    OFFLINE so it stops being treated as in_restart).
    """
    snode_api = snode.client(timeout=5, retry=5)
    # Each attempt is bounded BOTH ways, and needs both. The wall-clock deadline
    # is what keeps the documented ``max_attempts * poll_per_attempt_sec`` cap
    # when a single spdk_process_is_up blocks (unreachable node: TCP connect
    # timeout x retries). The round count is what keeps the cap when sleeping is
    # free — the integration fixtures patch this module's ``time.sleep`` to a
    # no-op, which turns a bare deadline loop into a hot spin burning the whole
    # poll_per_attempt_sec of CPU per attempt.
    rounds_per_attempt = max(1, int(poll_per_attempt_sec / poll_interval))
    for attempt in range(1, max_attempts + 1):
        try:
            snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
        except Exception as e:
            logger.warning(
                "spdk_process_kill RPC failed on %s (attempt %d/%d): %s",
                snode.get_id(), attempt, max_attempts, e,
            )

        deadline = time.time() + poll_per_attempt_sec
        for _ in range(rounds_per_attempt):
            if time.time() >= deadline:
                break
            try:
                # spdk_process_is_up returns a (result, error) tuple; unpack it.
                # Treating the raw tuple as a bool is always truthy, so the
                # kill loop would never observe SPDK as down (it would burn all
                # attempts and log a false "did NOT die" even after a clean kill).
                up, _ = snode_api.spdk_process_is_up(snode.rpc_port, snode.cluster_id)
            except Exception:
                up = False
            if not up:
                logger.info(
                    "SPDK on %s confirmed down (kill attempt %d/%d)",
                    snode.get_id(), attempt, max_attempts,
                )
                return True
            time.sleep(poll_interval)

        logger.warning(
            "SPDK on %s still up after %ds (attempt %d/%d); re-issuing kill",
            snode.get_id(), poll_per_attempt_sec, attempt, max_attempts,
        )

    logger.error(
        "SPDK on %s did NOT die after %d kill attempts (%ds total) — "
        "investigate snode_api / docker daemon health on %s",
        snode.get_id(), max_attempts,
        max_attempts * poll_per_attempt_sec, snode.mgmt_ip,
    )
    return False




def _set_lvol_ana_on_node(lvol: LVol, node: StorageNode, ana_state):
    """Set ANA state for a single lvol's listeners on a given node."""
    rpc_client = node.rpc_client(timeout=10, retry=2)
    listener_port = node.get_lvol_subsys_port(lvol.lvs_name)
    for iface in node.data_nics:
        if iface.ip4_address and (lvol.fabric == iface.trtype.lower() or (lvol.fabric == "tcp" and node.active_tcp)):
            trtype = iface.trtype if lvol.fabric == iface.trtype.lower() else "TCP"
            # Scope the flip to this volume's ANA group (group id == namespace
            # id): a subsystem can carry several namespaces whose volumes are
            # migrated, suspended or failed over independently.
            ret = rpc_client.nvmf_subsystem_listener_set_ana_state(
                lvol.nqn, iface.ip4_address, listener_port, trtype=trtype, ana=ana_state,
                anagrpid=lvol.ns_id)
            if not ret:
                logger.warning("Failed to set ANA state %s for %s on %s", ana_state, lvol.nqn, node.get_id())
            else:
                logger.info("ANA: %s ns %s on %s (%s) → %s", lvol.nqn, lvol.ns_id,
                            node.get_id(), iface.ip4_address, ana_state)


def _failover_primary_ana(primary_node: StorageNode):
    """Primary failed: promote first_sec→optimized.

    The second_sec stays at non_optimized (its permanent state).
    """
    db_ctrl = DBController()
    lvol_list = [lv for lv in db_ctrl.get_lvols_by_node_id(primary_node.get_id())
                 if lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]]

    first_sec = None
    if primary_node.secondary_node_id:
        first_sec = db_ctrl.get_storage_node_by_id(primary_node.secondary_node_id)

    # Dedupe per NAMESPACE, not per subsystem. The old per-(nqn, lvs) dedupe was
    # correct only while the flip was subsystem-wide: now that each volume's
    # state is confined to its own ANA group, skipping the other namespaces of a
    # shared subsystem would leave every volume but the first one unpromoted.
    # Records that share (nqn, lvs, ns_id) are genuine duplicates and still cost
    # only one call.
    seen_namespaces = set()
    for lvol in lvol_list:
        if (lvol.nqn, lvol.lvs_name, lvol.ns_id) in seen_namespaces:
            continue
        seen_namespaces.add((lvol.nqn, lvol.lvs_name, lvol.ns_id))
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            _set_lvol_ana_on_node(lvol, first_sec, "optimized")


def _failback_primary_ana(primary_node: StorageNode):
    """Primary restarting: demote first_sec→non_optimized.

    The second_sec is already non_optimized and never changes.
    """
    db_ctrl = DBController()
    lvol_list = [lv for lv in db_ctrl.get_lvols_by_node_id(primary_node.get_id())
                 if lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]]

    first_sec = None
    if primary_node.secondary_node_id:
        first_sec = db_ctrl.get_storage_node_by_id(primary_node.secondary_node_id)

    # Same per-namespace dedupe as _failover_primary_ana.
    seen_namespaces = set()
    for lvol in lvol_list:
        if (lvol.nqn, lvol.lvs_name, lvol.ns_id) in seen_namespaces:
            continue
        seen_namespaces.add((lvol.nqn, lvol.lvs_name, lvol.ns_id))
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            _set_lvol_ana_on_node(lvol, first_sec, "non_optimized")


def trigger_ana_failover_for_node(offline_node: StorageNode):
    """Trigger ANA failover when a node goes offline.

    Only action needed: if the offline node is a primary, promote its
    first_sec to optimized.  The second_sec is always non_optimized and
    never needs ANA state changes.
    """
    node_id = offline_node.get_id()

    if offline_node.secondary_node_id:
        logger.info("ANA failover: node %s is primary, promoting first_sec", node_id)
        try:
            _failover_primary_ana(offline_node)
        except Exception as e:
            logger.error("ANA failover for primary role of %s failed: %s", node_id, e)


def trigger_ana_failback_for_node(restarting_node: StorageNode):
    """Trigger ANA failback when a primary comes back online.

    Demote first_sec from optimized back to non_optimized.
    The second_sec is always non_optimized and never changes.
    """
    node_id = restarting_node.get_id()

    if restarting_node.secondary_node_id:
        first_sec = DBController().get_storage_node_by_id(restarting_node.secondary_node_id)
        if first_sec and first_sec.status == StorageNode.STATUS_ONLINE:
            logger.info("ANA failback: primary %s restarting, demoting first_sec", node_id)
            try:
                _failback_primary_ana(restarting_node)
            except Exception as e:
                logger.error("ANA failback for primary %s failed: %s", node_id, e)


#: Hard cap on the bdev_nvme_attach_controller RPC timeout (seconds).
#: A reachable peer replies in microseconds; anything longer is an unreachable
#: path and we prefer a fast failure so per-peer iteration stays bounded and
#: the overall connect_device budget stays ~2s across two data NICs.
_ATTACH_CONTROLLER_MAX_TIMEOUT_SEC = 1

#: Serializes the cross-node device-connection re-establishment section of a
#: node restart (connect this node to remote devices/JMs + make peers connect
#: back). Parallel suspended-cluster recovery restarts (the restart task
#: runner fans out one worker per node when the cluster is SUSPENDED) may
#: interleave everywhere EXCEPT here: the connect-back loop performs full
#: object writes of OTHER nodes' records, so two workers running it
#: concurrently lose updates. Process-local is sufficient — all parallel
#: restarts are driven by the single restart task-runner process.
_remote_connect_gate = threading.Lock()

#: Per-record successor to _remote_connect_gate for the restart connect
#: sections: what actually needs mutual exclusion is the read-compute-write
#: of ONE node record's remote_devices (the computed list is built outside
#: the FDB tx, so two concurrent writers to the same record lose updates
#: even via atomic_update). A global gate serialized ALL 32 parallel
#: suspended-recovery restarts through their most expensive section
#: (measured: dominant share of a 19-min restart phase, 2026-07-13).
#: Per-node-id locks serialize only writers of the SAME record; work on
#: distinct records proceeds concurrently. Process-local is sufficient —
#: all parallel restarts run in the single restart task-runner process.
_remote_connect_locks_guard = threading.Lock()
_remote_connect_locks: dict = {}


def _remote_connect_lock(node_id):
    with _remote_connect_locks_guard:
        return _remote_connect_locks.setdefault(node_id, threading.Lock())


#: In-process dedupe of concurrent connects of the SAME (device, node) pair.
#: Replaces the DB-backed NVMeDevice.lock_device_connection debounce inside
#: connect_device: that variant cost a node-table scan plus a whole-node
#: record write in FDB per device connect, which under parallel node restarts
#: (16 nodes × ~34 devices → 500+ concurrent connect threads) saturated FDB
#: with conflicting transactions and killed connect threads before their
#: attach RPC ever ran (2026-07-16 half-cluster restart incident). A blocking
#: process-local lock is a strictly stronger dedupe for the storm path — all
#: parallel restarts run in the single task-runner process — at zero DB cost.
_device_connect_locks_guard = threading.Lock()
_device_connect_locks: dict = {}


def _device_connect_lock(device_id, node_id):
    with _device_connect_locks_guard:
        return _device_connect_locks.setdefault((device_id, node_id), threading.Lock())

#: Serializes lvstore port allocation + persistence in create_lvstore.
#: get_next_lvstore_ports has no reservation step, so concurrent creates
#: (parallel activation Pass 1) would allocate colliding ports without this.
#: Process-local is sufficient — all Pass-1 creates run in the single
#: activation driver process.
_lvstore_port_alloc_lock = threading.Lock()

#: Per-LVS recreate locks. Recreate must be serialized only against a
#: concurrent recreate of the SAME LVS (two members of one LVS group racing the
#: port-block / hublvol-topology rewrite / peer lvstore_status writes into a
#: writer conflict). It does NOT need to be serialized across DIFFERENT LVSes
#: or node-wide. The previous single global gate serialized ALL recreates of
#: ALL LVSes on ALL parallel-restarting nodes (~60-98s/node, ~25min for a
#: 16-node failure-domain reboot). Keying the lock per LVS lets independent
#: LVSes recreate concurrently — in a single-FD reboot each LVS has only its
#: one FD-member restarting, so recreates run fully parallel. Process-local
#: suffices: all parallel restart tasks run in the one restart task-runner
#: process. Activation-mode recreates bypass this (globally blocked, serves no
#: IO — see the wrappers).
_recreate_lvstore_locks_guard = threading.Lock()
_recreate_lvstore_locks: dict = {}


def _recreate_lvstore_lock(lvs_name):
    with _recreate_lvstore_locks_guard:
        return _recreate_lvstore_locks.setdefault(lvs_name, threading.Lock())

#: Global cap on concurrently-RUNNING restart connect/reconnect worker threads.
#: See constants.RESTART_WORKER_MAX_CONCURRENCY: a whole-failure-domain reboot
#: dispatches up to 32 parallel node restarts, each fanning out per-peer /
#: per-remote-device connect threads — 100+ concurrent threads saturated the
#: single Python GIL and starved the (serialized) recreate's between-RPC work.
#: A wrapped worker holds one slot for the duration of its run; excess threads
#: block on the semaphore (no CPU) until a slot frees.
#: Global mutex over the port-block critical span (first client-port
#: nvmf_port_block -> last nvmf_port_unblock) of a recreate. Layered ON TOP
#: of the per-LVS recreate locks: pre-block phases of different LVSes stay
#: parallel; only the seconds during which a client port is actually
#: blocked are serialized cluster-wide (this runner is the only block
#: issuer). Rationale (2026-07-21 FD-reboot, 7 volumes EIO): ~6 recreates
#: ran their block windows concurrently, each window's RPC chain + FDB txns
#: queued behind the others' GIL/CPU work, stretching blocks to 6.2-8.9s —
#: past the 6.0s nvmf ack-timeout that reject-converts a blocked port and
#: kills the client's last live path. Serializing the windows (a) removes
#: sibling-window contention from the critical seconds and (b) caps the
#: blast radius to ONE port at risk at any moment. Lock order: per-LVS
#: recreate lock -> hublvol advisory locks -> THIS gate (innermost); the
#: gated span acquires no other module lock, so no cycle is possible.
#: Released in a ``finally`` by both recreate impls: the acquire has no
#: timeout, so an exception escaping while it is held wedges every later
#: restart/recreate in this process.
_port_block_window_gate = threading.Lock()

#: Window-priority drain: BEFORE a client-port-block window opens, new
#: bounded fan-out tasks are paused and the in-flight ones are drained;
#: only then is the port blocked. The window's serial RPC chain then runs
#: with exclusive CPU instead of competing with the connect-storm convoy
#: (2026-07-22 run: steady windows 1.4-1.9s but 3.6-5.2s while the first
#: windows raced the full 16-node fan-out) — and the drain cost is paid
#: OUTSIDE the client-visible outage. Deadlock-free by construction: the
#: gate holder never spawns or joins bounded workers inside the window
#: (connect sweeps are strictly pre/post-window); both the worker pause
#: (30s) and the drain (60s) are timeout-bounded so a stuck side cannot
#: freeze the other.
_window_clear = threading.Event()
_window_clear.set()
_inflight_bounded_cond = threading.Condition()
_inflight_bounded = {"n": 0}


def _open_port_block_window(label):
    """Serialize + prioritize a port-block window: take the global gate,
    pause new bounded fan-out tasks, drain the running ones, and only then
    return (the caller blocks the client port next). Returns seconds spent
    waiting (gate + drain) for the caller's logging."""
    _t0 = time.monotonic()
    _port_block_window_gate.acquire()
    _window_clear.clear()
    _deadline = time.monotonic() + 60
    with _inflight_bounded_cond:
        while _inflight_bounded["n"] > 0:
            _remaining = _deadline - time.monotonic()
            if _remaining <= 0:
                logger.warning(
                    "Opening port-block window (%s) with %d fan-out task(s) "
                    "still running — drain timed out",
                    label, _inflight_bounded["n"])
                break
            _inflight_bounded_cond.wait(min(_remaining, 1.0))
    return time.monotonic() - _t0


def _close_port_block_window():
    _window_clear.set()
    _port_block_window_gate.release()

_restart_worker_sem = threading.BoundedSemaphore(constants.RESTART_WORKER_MAX_CONCURRENCY)

#: Coordinator tier: workers that themselves SPAWN AND JOIN leaf workers
#: (e.g. _one_peer -> _connect_to_remote_devs -> _connect_device_thread).
#: A coordinator must NEVER share the leaf semaphore: with one shared pool,
#: 24 coordinators held every slot while joining leaves waiting on the same
#: semaphore -> permanent deadlock, every node stuck in_restart (2026-07-21
#: FD reboot: py-spy showed exactly 24 _one_peer holders + 469 acquire
#: waiters in TasksRunnerRestart). Two distinct tiers cannot deadlock:
#: coordinators wait only on leaves, leaves wait on nothing.
_restart_coordinator_sem = threading.BoundedSemaphore(
    constants.RESTART_COORDINATOR_MAX_CONCURRENCY)


def _bounded_thread(target, args=(), name=None, sem=None):
    """threading.Thread whose worker first acquires a global concurrency slot,
    bounding how many connect/reconnect workers run at once across all
    parallel node restarts. Drop-in for the previous bare
    ``threading.Thread(target=..., args=..., name=...)`` fan-out: the caller's
    ``start()``/``join()`` and the target's own error handling are unchanged;
    only the concurrent-run count is capped (GIL relief for recreate).

    ``sem`` selects the tier and defaults to the LEAF semaphore
    (``_restart_worker_sem``). Any target that itself spawns and joins
    bounded threads MUST pass ``sem=_restart_coordinator_sem`` — sharing the
    leaf pool between a joining parent and its children deadlocks the whole
    restart runner (2026-07-21)."""
    sem = _restart_worker_sem if sem is None else sem

    def _run(*a):
        # Window-priority drain: don't START while a port-block window is
        # open/opening (bounded wait — a stuck window must not freeze the
        # fan-out forever). Running tasks are counted so the window opener
        # can drain them BEFORE blocking the client port.
        _window_clear.wait(timeout=30)
        with sem:
            with _inflight_bounded_cond:
                _inflight_bounded["n"] += 1
            try:
                target(*a)
            finally:
                with _inflight_bounded_cond:
                    _inflight_bounded["n"] -= 1
                    _inflight_bounded_cond.notify_all()
    return threading.Thread(target=_run, args=args, name=name)


#: One repair at a time per (node, controller). health_check_service fans
#: repair_multipath_controller out over a ThreadPoolExecutor, so two workers
#: could read the same missing={ip} and both attach it -- which is how the
#: 2026-08-25 duplicate path was created. SPDK cannot catch that for us: its
#: -EEXIST guard runs before the async probe and only compares the active
#: path, so both attaches are admitted and the target issues two cntlids.
_repair_locks_guard = threading.Lock()
_repair_locks: dict = {}


def _repair_lock(key):
    with _repair_locks_guard:
        lk = _repair_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _repair_locks[key] = lk
        return lk


def _collect_attached_paths(ctrlr_list):
    """Every enabled path as an (traddr, trsvcid) tuple, REPEATS PRESERVED.

    _collect_attached_ips() returns a set, which is right for answering "what
    is missing" but blind to the opposite fault: the same address attached
    twice. On 2026-08-25 a node carried remote_jm_1e7ff71e with paths
    (96.179, 97.9, 97.9) and the set comparison read 2-of-2, so the control
    plane reported it healthy and never repaired it while the soak's path
    verifier failed on it for 900s. Duplicate detection needs the list.
    """
    paths: list[tuple[str, str]] = []
    if not ctrlr_list:
        return paths
    for entry in ctrlr_list:
        for ct in entry.get("ctrlrs", []):
            if ct.get("state") != "enabled":
                continue
            trid = ct.get("trid") or {}
            ip = trid.get("traddr")
            if ip:
                paths.append((ip, str(trid.get("trsvcid") or "")))
            for alt in ct.get("alternate_trids", []) or []:
                alt_ip = (alt or {}).get("traddr")
                if alt_ip:
                    paths.append((alt_ip, str((alt or {}).get("trsvcid") or "")))
    return paths


def duplicate_attached_paths(ctrlr_list):
    """Addresses attached more than once on one controller."""
    seen: dict[str, int] = {}
    for ip, _port in _collect_attached_paths(ctrlr_list):
        seen[ip] = seen.get(ip, 0) + 1
    return {ip for ip, n in seen.items() if n > 1}


def prune_duplicate_paths(rpc_client, name, ctrlr_list, nvmf_port, tr_type):
    """Detach every copy of any address attached more than once on ``name``.

    Returns True if anything was pruned, so the caller knows to re-read the
    controller list. Detaching is by trid and therefore removes ALL copies of
    a duplicated address -- the address becomes *missing*, and re-attaching it
    exactly once is left to the caller's own reconcile path. That split
    matters for hublvol, whose (re)attach must go through
    HublvolReconnectCoordinator: its cooldown is what closes the "cntlid N are
    duplicated" race, so a prune helper that also re-attached would risk
    recreating the very duplicate it just removed.
    """
    if not name:
        # An empty controller name makes bdev_nvme_controller_list() return
        # EVERY controller on the node, so duplicate_attached_paths() then
        # aggregates unrelated controllers and reports the same target IP
        # "duplicated" across them. Detaching on that is destructive and
        # wrong. Observed 2026-09-02 12:16 during activation: 10 false
        # duplicates on one node, and the detach ran.
        logger.warning(
            "prune_duplicate_paths called with an empty controller name; "
            "refusing to prune (the controller list would cover all "
            "controllers)")
        return False
    duplicates = duplicate_attached_paths(ctrlr_list)
    if not duplicates:
        return False
    logger.error(
        "Controller %s has duplicate path(s) %s -- pruning. Two "
        "controllers on one address serve no purpose and give the bdev "
        "two unordered qpairs to the same target.", name, duplicates)
    for dup_ip in sorted(duplicates):
        # Keep at least one other address alive: detaching by trid drops
        # EVERY controller on that address, so pruning the only address
        # would tear the bdev down instead of repairing it.
        others = {ip for ip, _p in _collect_attached_paths(ctrlr_list)} - {dup_ip}
        if not others:
            logger.warning(
                "Not pruning duplicate %s on %s: it is the only attached "
                "address, so a detach would remove the last path", dup_ip, name)
            continue
        try:
            rpc_client.bdev_nvme_detach_controller(
                name, traddr=dup_ip, trsvcid=nvmf_port, trtype=tr_type)
        except Exception as e:
            logger.error("Failed to prune duplicate path %s on %s: %s",
                         dup_ip, name, e)
            continue
    return True


def _collect_attached_ips(ctrlr_list):
    """Aggregate the set of currently-attached traddrs across every ctrlr entry.

    SPDK multipath returns one ``ctrlrs`` entry per path (each with its own
    ``trid`` and no ``alternate_trids``). Older shapes folded all paths under a
    single entry's ``alternate_trids``. We accept both: walk every entry, and
    for each enabled one merge ``trid.traddr`` plus any ``alternate_trids``.
    Disabled / resetting paths are not counted as attached.
    """
    attached: set[str] = set()
    if not ctrlr_list:
        return attached
    for entry in ctrlr_list:
        for ct in entry.get("ctrlrs", []):
            if ct.get("state") != "enabled":
                continue
            trid = ct.get("trid") or {}
            ip = trid.get("traddr")
            if ip:
                attached.add(ip)
            for alt in ct.get("alternate_trids", []) or []:
                alt_ip = (alt or {}).get("traddr")
                if alt_ip:
                    attached.add(alt_ip)
    return attached


def connect_device(name: str, device: NVMeDevice, node: StorageNode, attach_timeout: Optional[float] = None):
    """Connect snode to device

    This only performs the actual operation between both involved SPDK instances,
    no book-keeping is done here.

    The bdev_nvme_attach_controller RPC is always bounded by
    ``_ATTACH_CONTROLLER_MAX_TIMEOUT_SEC`` (1 s) with no retries. Callers may
    pass ``attach_timeout`` to shorten further (kept as-is if lower); values
    above the cap are clamped, since a reachable SPDK peer answers in µs and
    anything longer is an unreachable path we want to fail fast on.

    More sensibly this would be a member function of either StorageNode or NVMeDevice.
    """

    logger.info(f'Connecting to {name}')

    expected_ips = [ip.strip() for ip in (device.nvmf_ip or "").split(",") if ip.strip()]
    is_multipath = bool(device.nvmf_multipath) and len(expected_ips) >= 2
    rpc_client = node.rpc_client()

    # Fast path: bdev already present. Only safe for single-path devices — for
    # multipath the bdev can survive while one of its paths has been destructed
    # (the surviving path still backs the namespace). Early-returning here was
    # the silent failure mode for partial-path-loss recovery during NIC chaos:
    # the bdev_get_bdevs snapshot taken by _connect_to_remote_devs/
    # _connect_to_remote_jm_devs contains the bdev, so we used to skip the
    # attach and never restore the missing path. With multipath we always go on
    # to inspect the controller list and re-attach any missing path.
    #
    # ``bdev_names=None`` means "no snapshot": probe the single expected bdev
    # with a name-filtered bdev_get_bdevs instead. An unfiltered dump costs
    # seconds of SPDK app-thread time on large clusters (the inventory is
    # O(cluster size): remote alceml/JM controllers to every peer), so hot
    # paths must not dump the whole table to check one name.
    if not is_multipath:
        bdev_name = f"{name}n1"
        if rpc_client.get_bdevs(bdev_name):
            logger.debug(f"Already connected, bdev found in bdev_get_bdevs: {bdev_name}")
            return bdev_name

    if attach_timeout is None or attach_timeout > _ATTACH_CONTROLLER_MAX_TIMEOUT_SEC:
        attach_timeout = _ATTACH_CONTROLLER_MAX_TIMEOUT_SEC
    attach_rpc_client = node.rpc_client(timeout=attach_timeout, retry=0)

    # Dedupe concurrent connects of the same (device, node) pair with a
    # process-local lock (see _device_connect_lock for why the DB-backed
    # lock_device_connection debounce had to go). Blocking is intended:
    # the second caller waits out the first attach instead of racing it,
    # then hits the already-attached fast paths below.
    with _device_connect_lock(device.get_id(), node.get_id()):
        return _connect_device_attach(
            name, device, node, rpc_client, attach_rpc_client,
            expected_ips, is_multipath)


def _connect_device_attach(name, device, node: StorageNode, rpc_client, attach_rpc_client,
                           expected_ips, is_multipath):
    """Controller inspect + attach path of connect_device.

    Runs under the per-(device, node) connect lock taken by connect_device.
    """
    ret = rpc_client.bdev_nvme_controller_list(name)
    if ret:
        # "failed" is transient here, NOT a terminal state to act on: the
        # state string reports is_failed BEFORE resetting /
        # reconnect_is_delayed (nvme_ctrlr_get_state_str), so a controller
        # mid reset/reconnect cycle reads "failed" in the window between a
        # reset failure and its disposition. With the cluster-wide
        # bdev_nvme options (reconnect_delay_sec=1, ctrlr_loss_timeout_sec=1,
        # set at node bring-up before any attach) the module self-resolves
        # every failure to either "enabled" or destruct within ~1s — a
        # persistently parked "failed" controller (only possible with
        # reconnect_delay=0) cannot arise. The previous code issued a
        # controller detach RPC on "failed", which raced the module's own
        # destruct/reconnect machinery and — with only a fixed sleep, no
        # detach-and-wait-gone — set up the attach-during-destroy race
        # ("cntlid N are duplicated" class) on the immediate re-attach.
        # Wait transients out; on a controller that stays transient past
        # the budget, raise so the calling task suspends and retries.
        _TRANSIENT_STATES = ("failed", "resetting", "deleting", "reconnect_is_delayed")
        states: List[str] = []
        for _attempt in range(5):
            if not ret:
                # The module destructed the controller on its own; the
                # fresh-attach path below takes over.
                break
            states = [c.get("state") for c in ret[0].get("ctrlrs", [])]
            logger.info(f"Controller found: {name}, states: {states}")
            if not any(s in _TRANSIENT_STATES for s in states):
                break  # settled (enabled/disabled) — reuse/repair below
            time.sleep(2)
            # Refresh on retry so we don't loop on a stale snapshot.
            ret = rpc_client.bdev_nvme_controller_list(name) or []
        else:
            # Still transient after the full budget: something is genuinely
            # hanging (usually stuck IO). Never detach here — surface it.
            raise RuntimeError(f"Controller: {name}, status is {states}")

    db_ctrl = DBController()
    target_node = db_ctrl.get_storage_node_by_id(device.node_id)
    if target_node is not None and target_node.active_rdma:
        tr_type = "RDMA"
    elif target_node is not None and target_node.active_tcp:
        tr_type = "TCP"
    else:
        msg = "target node to connect has no active fabric."
        logger.error(msg)
        raise RuntimeError(msg)

    # nvmf_multipath is a bool on the device record; translate it into
    # the SPDK string mode here. ``True`` must mean active-active
    # (``"multipath"``), not failover — passing the bool through to
    # rpc_client.bdev_nvme_attach_controller would coerce True ->
    # ``"failover"`` (active-passive) and remote alceml/jm controllers
    # would carry all IO on a single path.
    attach_mode = "multipath" if device.nvmf_multipath else False

    final = rpc_client.bdev_nvme_controller_list(name)
    if not final:
        # Controller is fully gone — do a full multi-path attach.
        bdev_name = ""
        for ip in (expected_ips or [device.nvmf_ip]):
            # Circuit breaker per target address: dialling a peer that has
            # refused the last N connects burns this node's app-thread time
            # on connect polling for nothing -- see utils/dial_backoff.
            if not dial_backoff.allowed(ip):
                logger.debug(f"Attach to {ip} for {name} held by dial backoff")
                continue
            try:
                resp = attach_rpc_client.bdev_nvme_attach_controller(
                    name, device.nvmf_nqn, ip, device.nvmf_port, tr_type,
                    multipath=attach_mode)
                if resp:
                    dial_backoff.record_success(ip)
                else:
                    dial_backoff.record_failure(ip)
                if not bdev_name and resp and isinstance(resp, list):
                    bdev_name = resp[0]
            except Exception as e:
                dial_backoff.record_failure(ip)
                logger.warning(f"Failed to attach controller {name} via {ip}: {e}")

            if device.nvmf_multipath and bdev_name:
                rpc_client.bdev_nvme_set_multipath_policy(bdev_name, "active_active")

        if not bdev_name:
            msg = f"Bdev name not returned from controller attach for {name}"
            logger.error(msg)
            raise RuntimeError(msg)
        bdev_found = False
        for i in range(5):
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                bdev_found = True
                break
            else:
                time.sleep(1)

        if not bdev_found:
            logger.error("Bdev not found after 5 attempts")
            raise RuntimeError(f"Failed to connect to device: {device.get_id()}")

        return bdev_name

    # Controller still present. For multipath, check whether some paths went
    # away (typical after a NIC chaos burst: one path's bdev_nvme_ctrlr was
    # destructed within ctrlr_loss_timeout, the other survived and keeps the
    # bdev up). Re-attach any missing path inline; partial success is OK —
    # whatever paths come back leave the controller in a strictly better
    # state than before, and the next health cycle picks up what's left.
    bdev_name = f"{name}n1"
    if is_multipath:
        attached_ips = _collect_attached_ips(final)
        missing_ips = [ip for ip in expected_ips if ip not in attached_ips]
        if missing_ips:
            logger.info(
                "Controller %s has %d/%d paths attached, attaching missing: %s",
                name, len(attached_ips), len(expected_ips), missing_ips)
            for ip in missing_ips:
                if not dial_backoff.allowed(ip):
                    logger.debug(
                        f"Re-attach of {ip} on {name} held by dial backoff")
                    continue
                try:
                    if attach_rpc_client.bdev_nvme_attach_controller(
                            name, device.nvmf_nqn, ip, device.nvmf_port, tr_type,
                            multipath=attach_mode):
                        dial_backoff.record_success(ip)
                    else:
                        dial_backoff.record_failure(ip)
                except Exception as e:
                    dial_backoff.record_failure(ip)
                    logger.warning(
                        "Failed to re-attach path %s on controller %s: %s",
                        ip, name, e)
            # Recognize partial success — re-read the controller list and
                # report what remains missing for observability. We don't
                # raise here: a 1/2 outcome still strictly improves over the
                # incoming state and the next cycle will retry the rest.
            post = rpc_client.bdev_nvme_controller_list(name) or []
            now_attached = _collect_attached_ips(post)
            still_missing = [ip for ip in expected_ips if ip not in now_attached]
            if still_missing:
                logger.warning(
                    "Controller %s still missing paths after attach: %s (now %d/%d)",
                    name, still_missing, len(now_attached), len(expected_ips))

    if rpc_client.get_bdevs(bdev_name):
        return bdev_name
    return None


def repair_multipath_controller(name: str, device, node: StorageNode):
    """Check a multipath NVMe controller and re-attach any missing paths.

    For a multipath device the controller should have one path per data NIC.
    Walks every entry in the ``bdev_nvme_get_controllers`` response (newer
    SPDK exposes one ``ctrlrs`` entry per path; older shapes use one entry
    with ``alternate_trids``) and aggregates the set of currently-attached
    traddrs across all of them. Any expected IP that is not in that set is
    a missing path and gets re-attached.

    Partial repair is recognized: we re-read the controller state after
    attaching and report what remains missing. Returns True if *all*
    expected paths are now attached, False otherwise. The caller must pass
    a device object that carries ``nvmf_ip`` / ``nvmf_nqn`` / ``nvmf_port``
    (i.e. the source NVMeDevice / JMDevice on the target node — NOT a
    ``RemoteJMDevice``, which strips those fields).
    """
    if not getattr(device, 'nvmf_multipath', False):
        return True

    nvmf_ip = getattr(device, 'nvmf_ip', None)
    if not nvmf_ip:
        # Caller passed a remote-side view without source addressing.
        # Nothing we can do here — log so this regression is loud.
        logger.warning(
            "repair_multipath_controller called for %s with a device that "
            "has no nvmf_ip; caller must pass the source NVMeDevice/JMDevice "
            "from the target node", name)
        return False

    expected_ips = set(ip.strip() for ip in nvmf_ip.split(",") if ip.strip())
    if len(expected_ips) < 2:
        return True  # not actually multipath

    rpc_client = node.rpc_client()
    ret = rpc_client.bdev_nvme_controller_list(name)
    if not ret:
        return True  # controller gone, connect_device will handle full reconnect

    db_ctrl = DBController()
    target_node = db_ctrl.get_storage_node_by_id(device.node_id)
    if target_node is None:
        return False
    if target_node.active_rdma:
        tr_type = "RDMA"
    elif target_node.active_tcp:
        tr_type = "TCP"
    else:
        return False

    # Serialize per controller. A concurrent repair reading the same
    # missing set is how a duplicate path gets created, and the loser of the
    # race has nothing useful to add -- skip rather than queue behind it.
    lock = _repair_lock(f"{node.get_id()}:{name}")
    if not lock.acquire(blocking=False):
        logger.debug("Repair of %s already in flight; skipping this cycle", name)
        return True
    try:
        return _repair_multipath_controller_locked(
            node, name, device, rpc_client, ret, expected_ips, tr_type)
    finally:
        lock.release()


def _repair_multipath_controller_locked(node, name, device, rpc_client, ret,
                                        expected_ips, tr_type):
    # A duplicated address is a fault in its own right, and the set-based
    # comparison below cannot see it: (96.179, 97.9, 97.9) reads as 2-of-2.
    # Prune first, because the missing-path logic would otherwise report the
    # controller complete and return while it still carries the surplus path.
    if prune_duplicate_paths(rpc_client, name, ret, device.nvmf_port, tr_type):
        # Re-read: the detach removed every copy of each duplicated address,
        # so those addresses are now missing and the loop below re-attaches
        # each exactly once.
        ret = rpc_client.bdev_nvme_controller_list(name) or []

    attached_ips = _collect_attached_ips(ret)
    # An address with a live enabled path on this very controller is reachable,
    # so any dial hold on it is stale evidence and must not delay the repair of
    # a sibling path. This is the only kind of clear() the breaker accepts --
    # observed traffic, not the peer's DB status.
    for live_ip in attached_ips:
        if dial_backoff.clear(live_ip):
            logger.info(
                "Cleared stale dial hold on %s: it has a live path on %s",
                live_ip, name)
    missing_ips = expected_ips - attached_ips
    if not missing_ips:
        return True

    logger.info(
        "Controller %s has %d/%d paths attached, re-attaching missing: %s",
        name, len(attached_ips), len(expected_ips), missing_ips)
    for ip in missing_ips:
        # Circuit breaker per target address: repeated refusals earn the
        # address a hold (utils/dial_backoff). Status gates cannot cover a
        # peer whose DB record is wrong (SPDK dead, record ONLINE -- run
        # mass_create_delete_docker-20260821), and hammering a refusing
        # address wedged a healthy node's app thread there.
        if not dial_backoff.allowed(ip):
            logger.debug("Repair of path %s on %s held by dial backoff", ip, name)
            continue
        try:
            # The return value matters: a fabric connect that cannot reach the
            # address answers with an error result rather than raising, so
            # discarding it made every such failure silent apart from the
            # "still missing" line below. 478 of these went unattributed during
            # the 2026-08-20 soak, all of them "-5 Input/output error".
            if not rpc_client.bdev_nvme_attach_controller(
                    name, device.nvmf_nqn, ip, device.nvmf_port,
                    tr_type, multipath="multipath"):
                dial_backoff.record_failure(ip)
                logger.warning(
                    "Re-attach of path %s on controller %s was rejected by the "
                    "target", ip, name)
            else:
                dial_backoff.record_success(ip)
        except Exception as e:
            dial_backoff.record_failure(ip)
            logger.error("Failed to re-attach path %s on controller %s: %s", ip, name, e)

    # Re-read and recognize partial success: a 1/2 outcome is still
    # strictly better than the incoming state and the next health cycle
    # picks up the remainder. Only return False when nothing improved.
    post = rpc_client.bdev_nvme_controller_list(name) or []
    now_attached = _collect_attached_ips(post)
    still_missing = expected_ips - now_attached
    if still_missing:
        logger.warning(
            "Controller %s still missing paths after re-attach: %s (now %d/%d)",
            name, still_missing, len(now_attached), len(expected_ips))
        return len(now_attached) > len(attached_ips)
    return True


def get_next_cluster_device_order(db_controller, cluster_id):
    max_order = 0
    found = False
    for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        for dev in node.nvme_devices:
            found = True
            max_order = max(max_order, dev.cluster_device_order)
    if found:
        return max_order + 1
    return 0


def get_next_physical_device_order(snode, exclude_node_id=None):
    # exclude_node_id: skip this node when scanning. Needed when recomputing the
    # label for a node that is already persisted with a provisional value —
    # otherwise the same-mgmt_ip early-return below would just hand back that
    # node's own stale label instead of a fresh cluster-unique one.
    db_controller = DBController()
    used_labels = []
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if exclude_node_id and node.get_id() == exclude_node_id:
            continue
        if node.physical_label > 0:
            if node.mgmt_ip == snode.mgmt_ip:
                return node.physical_label
            else:
                used_labels.append(node.physical_label)

    next_label = 1
    while next_label in used_labels:
        next_label += 1
    return next_label


def _search_for_partitions(rpc_client, nvme_device):
    partitioned_devices = []
    # Node-add cold path: full dump is fine here, the node carries no lvols yet.
    bdevs = rpc_client.get_bdevs(all_bdevs=True)
    if bdevs is None:
        raise RPCException(f"get_bdevs failed on {rpc_client.host}")
    for bdev in bdevs:
        name = bdev['name']
        if name.startswith(f"{nvme_device.nvme_bdev}p"):
            new_dev = NVMeDevice(nvme_device.to_dict())
            new_dev.uuid = str(uuid.uuid4())
            new_dev.device_name = name
            new_dev.nvme_bdev = name
            new_dev.is_partition = True
            new_dev.size = bdev['block_size'] * bdev['num_blocks']
            partitioned_devices.append(new_dev)
    return partitioned_devices


def _create_jm_stack_on_raid(rpc_client, jm_nvme_bdevs, snode: StorageNode, after_restart):
    # RAID 0+1 journal layout (see simplyblock_core/jm_raid.py):
    #   1 device   -> no raid (bare device)
    #   2 devices  -> raid1 over two single-device legs  (a 2-way mirror)
    #   > 2 devices-> two ±1 balanced raid0 legs, mirrored by a top raid1
    # The top raid bdev keeps the name raid_jm_<node> so the alceml/jm stack
    # above is unchanged; the two raid0 legs are raid_jm_<node>_l{0,1}. This
    # caps journal write amplification at 2x/node instead of N-way mirroring.
    node = snode.get_id()
    plan = jm_raid.plan_topology(jm_nvme_bdevs)
    leg_bdevs = []
    leg_members = []
    if plan["level"] == jm_raid.RAID_NONE:
        raid_bdev = plan["base"]
    else:
        for i, leg in enumerate(plan["legs"]):
            if len(leg) == 1:
                leg_bdev = leg[0]  # single-drive leg: use the bare device
            else:
                leg_bdev = f"raid_jm_{node}_l{i}"
                if not rpc_client.bdev_raid_create(leg_bdev, leg, "0"):
                    logger.error(f"Failed to create JM raid0 leg {leg_bdev}")
                    return False
            leg_bdevs.append(leg_bdev)
            leg_members.append(leg)
        raid_bdev = f"raid_jm_{node}"
        if not rpc_client.bdev_raid_create(raid_bdev, leg_bdevs, "1"):
            logger.error(f"Failed to create raid_jm_{node}")
            return False

    alceml_id = snode.get_id()
    alceml_name = f"alceml_jm_{snode.get_id()}"
    nvme_bdev = raid_bdev

    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if after_restart else 3,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask,
                                    shared_placement=cluster.shared_placement,
                                    compression_thread=False,
                                    compression_cpu_mask=snode.compression_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    pt_name = ""
    subsystem_nqn = ""
    pt_spdk_uuid = ""
    ip_list = []
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False

        pt_spdk_uuid = rpc_client.get_bdevs(pt_name)[0]["aliases"][0]
        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        for iface in snode.data_nics:
            logger.info(f"adding {iface.trtype} listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
            ip_list.append(iface.ip4_address)

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = next((iface.ip4_address for iface in snode.data_nics if iface.ip4_address), "")
        multipath = False

    ret = rpc_client.get_bdevs(raid_bdev)

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': ret[0]["block_size"] * ret[0]["num_blocks"],
        'status': JMDevice.STATUS_ONLINE,
        'jm_nvme_bdev_list': jm_nvme_bdevs,
        'raid_bdev': raid_bdev,
        'jm_leg_bdevs': leg_bdevs,
        'jm_leg_members': leg_members,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'jm_bdev': jm_bdev,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': snode.nvmf_port,
        'nvmf_multipath': multipath,
        'node_id': snode.get_id(),
        'pt_bdev_uuid': pt_spdk_uuid,
    })


def _create_jm_stack_on_device(rpc_client, nvme, snode: StorageNode, after_restart):
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)
    db_controller = DBController()
    nvme_bdev = nvme.nvme_bdev
    test_name = ""
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme.nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return False
        nvme_bdev = test_name

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if after_restart else 3,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return False

    jm_bdev = f"jm_{snode.get_id()}"
    ret = rpc_client.bdev_jm_create(jm_bdev, alceml_name, jm_cpu_mask=snode.jm_cpu_mask,
                                    shared_placement=cluster.shared_placement,
                                    compression_thread=False,
                                    compression_cpu_mask=snode.compression_cpu_mask)
    if not ret:
        logger.error(f"Failed to create {jm_bdev}")
        return False

    pt_name = ""
    subsystem_nqn = ""
    pt_spdk_uuid = ""
    ip_list = []
    if snode.enable_ha_jm:
        # add pass through
        pt_name = f"{jm_bdev}_PT"
        ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
        if not ret:
            logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
            return False
        pt_spdk_uuid = rpc_client.get_bdevs(pt_name)[0]["aliases"][0]
        subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
        logger.info("creating subsystem %s", subsystem_nqn)
        ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
        logger.info(f"add {pt_name} to subsystem")
        ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
        if not ret:
            logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
            return False

        for iface in snode.data_nics:
            if iface.ip4_address:
                logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
                ip_list.append(iface.ip4_address)

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = next((iface.ip4_address for iface in snode.data_nics if iface.ip4_address), "")
        multipath = False

    return JMDevice({
        'uuid': alceml_id,
        'device_name': jm_bdev,
        'size': nvme.size,
        'status': JMDevice.STATUS_ONLINE,
        'alceml_bdev': alceml_name,
        'alceml_name': alceml_name,
        'nvme_bdev': nvme.nvme_bdev,
        "serial_number": nvme.serial_number,
        "device_data_dict": nvme.to_dict(),
        'jm_bdev': jm_bdev,
        'testing_bdev': test_name,
        'pt_bdev': pt_name,
        'nvmf_nqn': subsystem_nqn,
        'nvmf_ip': IP,
        'nvmf_port': snode.nvmf_port,
        'nvmf_multipath': multipath,
        'node_id': snode.get_id(),
        'pt_bdev_uuid': pt_spdk_uuid,
    })


def _create_storage_device_stack(rpc_client, nvme, snode: StorageNode, after_restart):
    db_controller = DBController()
    nvme_bdev = nvme.nvme_bdev
    if snode.enable_test_device:
        test_name = f"{nvme.nvme_bdev}_test"
        ret = rpc_client.bdev_passtest_create(test_name, nvme_bdev)
        if not ret:
            logger.error(f"Failed to create passtest bdev {test_name}")
            return None
        nvme_bdev = test_name
    alceml_id = nvme.get_id()
    alceml_name = device_controller.get_alceml_name(alceml_id)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    ret = snode.create_alceml(
        alceml_name, nvme_bdev, alceml_id,
        pba_init_mode=1 if (after_restart and nvme.status != NVMeDevice.STATUS_NEW) else 3,
        write_protection=cluster.distr_ndcs > 1,
        pba_page_size=cluster.page_size_in_blocks,
        full_page_unmap=cluster.full_page_unmap,
    )

    if not ret:
        logger.error(f"Failed to create alceml bdev: {alceml_name}")
        return None
    alceml_bdev = alceml_name

    # add pass through
    pt_name = f"{alceml_name}_PT"
    ret = rpc_client.bdev_PT_NoExcl_create(pt_name, alceml_bdev)
    if not ret:
        logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
        return None

    pt_spdk_uuid = rpc_client.get_bdevs(pt_name)[0]["aliases"][0]
    subsystem_nqn = snode.subsystem + ":dev:" + alceml_id
    logger.info("creating subsystem %s", subsystem_nqn)
    ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', alceml_id)
    ip_list = []
    for iface in snode.data_nics:
        if iface.ip4_address:
            logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
            ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
            ip_list.append(iface.ip4_address)

    logger.info(f"add {pt_name} to subsystem")
    ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, alceml_id)
    if not ret:
        logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
        return None

    if len(ip_list) > 1:
        IP = ",".join(ip_list)
        multipath = True
    else:
        IP = ip_list[0]
        multipath = False

    nvme.alceml_bdev = alceml_bdev
    nvme.pt_bdev = pt_name
    nvme.alceml_name = alceml_name
    nvme.nvmf_nqn = subsystem_nqn
    nvme.nvmf_ip = IP
    nvme.nvmf_port = snode.nvmf_port
    nvme.io_error = False
    nvme.nvmf_multipath = multipath
    nvme.pt_spdk_uuid = pt_spdk_uuid
    # if nvme.status != NVMeDevice.STATUS_NEW:
    #     nvme.status = NVMeDevice.STATUS_ONLINE
    return nvme


def _create_device_partitions(rpc_client, nvme, snode: StorageNode, num_partitions_per_dev, jm_percent, partition_size, nbd_index):
    nbd_device = rpc_client.nbd_start_disk(nvme.nvme_bdev, f"/dev/nbd{nbd_index}")
    time.sleep(3)
    if not nbd_device:
        logger.error("Failed to start nbd dev")
        return False
    snode_api = snode.client()
    partition_percent = 0
    if partition_size:
        partition_percent = int(partition_size * 100 / nvme.size)

    result, error = snode_api.make_gpt_partitions(nbd_device, jm_percent, num_partitions_per_dev, partition_percent)
    if error:
        logger.error("Failed to make partitions")
        logger.error(error)
        return False
    time.sleep(3)
    rpc_client.nbd_stop_disk(nbd_device)
    for i in range(10):
        if not rpc_client.nbd_get_disks(nbd_device):
            break
        time.sleep(1)
    rpc_client.bdev_nvme_detach_controller(nvme.nvme_controller)
    for i in range(10):
        if not rpc_client.bdev_nvme_controller_list(nvme.nvme_controller):
            break
        time.sleep(1)
    try:
        rpc_client.bdev_nvme_controller_attach(nvme.nvme_controller, nvme.pcie_address)
    except RPCException as e:
        logger.error('Failed to create device partitions: ' + str(e))
        return False
    time.sleep(1)
    rpc_client.bdev_examine(nvme.nvme_bdev)
    time.sleep(1)
    return True


def _prepare_cluster_devices_partitions(snode: StorageNode, devices):
    db_controller = DBController()
    new_devices = []
    devices_to_partition = []
    thread_list = []
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            new_devices.append(nvme)
            continue
        if nvme.is_partition:
            t = threading.Thread(target=_create_storage_device_stack, args=(snode.rpc_client(), nvme, snode, False,))
            thread_list.append(t)
            new_devices.append(nvme)
            t.start()
        else:
            devices_to_partition.append(nvme)
            partitioned_devices = _search_for_partitions(snode.rpc_client(), nvme)
            if len(partitioned_devices) != (1 + snode.num_partitions_per_dev):
                logger.info(f"Creating partitions for {nvme.nvme_bdev}")
                t = threading.Thread(
                    target=_create_device_partitions,
                    args=(snode.rpc_client(), nvme, snode, snode.num_partitions_per_dev,
                          snode.jm_percent, snode.partition_size, index + 1,))
                thread_list.append(t)
                t.start()

    for thread in thread_list:
        thread.join()

    thread_list = []
    for nvme in devices_to_partition:
        partitioned_devices = _search_for_partitions(snode.rpc_client(), nvme)
        if len(partitioned_devices) == (1 + snode.num_partitions_per_dev):
            logger.info("Device partitions created")
            # remove 1st partition for jm
            partitioned_devices.pop(0)
            for dev in partitioned_devices:
                t = threading.Thread(target=_create_storage_device_stack,
                                     args=(snode.rpc_client(), dev, snode, False,))
                thread_list.append(t)
                new_devices.append(dev)
                t.start()
        else:
            logger.error("Failed to create partitions")
            return False

    for thread in thread_list:
        thread.join()

    # assign device order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    for nvme in new_devices:
        if nvme.status == NVMeDevice.STATUS_ONLINE:
            if nvme.cluster_device_order < 0:
                nvme.cluster_device_order = dev_order
                dev_order += 1
        device_events.device_create(nvme)

    # create jm device
    jm_devices = []
    # Node-add cold path: full dump is fine here, the node carries no lvols yet.
    bdevs = snode.rpc_client().get_bdevs(all_bdevs=True)
    if bdevs is None:
        # None means the RPC failed (timeout / non-200), not "no bdevs".
        # Without this guard the comprehension below crashes with an opaque
        # TypeError; raise a clear, catchable error instead.
        raise RPCException(f"get_bdevs failed on node {snode.get_id()}")
    bdevs_names = [d['name'] for d in bdevs]
    for nvme in new_devices:
        if nvme.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW]:
            dev_part = f"{nvme.nvme_bdev[:-2]}p1"
            if dev_part in bdevs_names:
                if dev_part not in jm_devices:
                    jm_devices.append(dev_part)

    if jm_devices:
        jm_device = _create_jm_stack_on_raid(snode.rpc_client(), jm_devices, snode, after_restart=False)
        if not jm_device:
            logger.error("Failed to create JM device")
            return False

        snode.jm_device = jm_device

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_jm_on_dev(snode: StorageNode, devices):
    db_controller = DBController()
    if not devices:
        return True

    # Set device cluster order
    dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
    rpc_client = snode.rpc_client()
    new_devices = []
    for index, nvme in enumerate(devices):
        if nvme.status == "not_found":
            continue

        if nvme.status == NVMeDevice.STATUS_JM:
            jm_device = _create_jm_stack_on_device(rpc_client, nvme, snode, after_restart=False)
            if not jm_device:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = jm_device
            continue

        new_devices.append(nvme)
        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_READONLY]:
            logger.debug(f"Device is not online : {nvme.get_id()}, status: {nvme.status}")
        else:
            ret = _create_storage_device_stack(rpc_client, nvme, snode, after_restart=False)
            if not ret:
                logger.error("failed to create dev stack")
                return False
            if nvme.status == NVMeDevice.STATUS_ONLINE:
                if nvme.cluster_device_order < 0:
                    nvme.cluster_device_order = dev_order
                    dev_order += 1
                device_events.device_create(nvme)

    snode.nvme_devices = new_devices
    return True


def _prepare_cluster_devices_on_restart(snode: StorageNode, clear_data=False):
    db_controller = DBController()

    new_devices = []

    rpc_client = snode.rpc_client(timeout=5 * 60)

    thread_list = []
    for index, nvme in enumerate(snode.nvme_devices):
        if nvme.status == NVMeDevice.STATUS_JM:
            continue

        new_devices.append(nvme)

        if nvme.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE,
                               NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_NEW, NVMeDevice.STATUS_CANNOT_ALLOCATE]:
            logger.debug(f"Device is skipped: {nvme.get_id()}, status: {nvme.status}")
            continue

        t = threading.Thread(
            target=_create_storage_device_stack,
            args=(rpc_client, nvme, snode, not clear_data,))
        thread_list.append(t)

    for thread in thread_list:
        thread.start()

    for thread in thread_list:
        thread.join()

    snode.nvme_devices = new_devices
    snode.write_to_db()

    # prepare JM device
    jm_device = snode.jm_device
    if jm_device is None:
        return True

    if not jm_device or not jm_device.uuid:
        return True

    jm_device.status = JMDevice.STATUS_UNAVAILABLE

    if jm_device.jm_nvme_bdev_list:
        if len(jm_device.jm_nvme_bdev_list) == 1:
            ret = rpc_client.get_bdevs(jm_device.jm_nvme_bdev_list[0])
            if not ret:
                logger.error(f"BDev not found: {jm_device.jm_nvme_bdev_list[0]}")
                jm_device.status = JMDevice.STATUS_REMOVED
                return True
            ret = _create_jm_stack_on_raid(rpc_client, jm_device.jm_nvme_bdev_list, snode, after_restart=not clear_data)
            if not ret:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = ret
            snode.write_to_db()
            return True

        jm_bdevs_found = []
        for bdev_name in jm_device.jm_nvme_bdev_list:
            ret = rpc_client.get_bdevs(bdev_name)
            if ret:
                logger.info(f"JM bdev found: {bdev_name}")
                jm_bdevs_found.append(bdev_name)
            else:
                logger.error(f"JM bdev not found: {bdev_name}")

        if len(jm_bdevs_found) > 1:
            ret = _create_jm_stack_on_raid(rpc_client, jm_bdevs_found, snode, after_restart=not clear_data)
            if not ret:
                logger.error("Failed to create JM device")
                return False
            snode.jm_device = ret
            snode.write_to_db()
        else:
            logger.error("Only one jm nvme bdev found, setting jm device to removed")
            jm_device.status = JMDevice.STATUS_REMOVED
            return True

    else:
        nvme_bdev = jm_device.nvme_bdev
        if snode.enable_test_device:
            ret = rpc_client.bdev_passtest_create(jm_device.testing_bdev, jm_device.nvme_bdev)
            if not ret:
                logger.error(f"Failed to create passtest bdev {jm_device.testing_bdev}")
                return False
            nvme_bdev = jm_device.testing_bdev

        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        ret = snode.create_alceml(
            jm_device.alceml_bdev, nvme_bdev, jm_device.get_id(),
            pba_init_mode=3 if clear_data else 1,
            pba_page_size=cluster.page_size_in_blocks,
            full_page_unmap=cluster.full_page_unmap
        )

        if not ret:
            logger.error(f"Failed to create alceml bdev: {jm_device.alceml_bdev}")
            return False

        jm_bdev = f"jm_{snode.get_id()}"
        ret = rpc_client.bdev_jm_create(jm_bdev, jm_device.alceml_bdev, jm_cpu_mask=snode.jm_cpu_mask,
                                        shared_placement=cluster.shared_placement,
                                        compression_thread=False,
                                        compression_cpu_mask=snode.compression_cpu_mask)
        if not ret:
            logger.error(f"Failed to create {jm_bdev}")
            return False

        if snode.enable_ha_jm:
            # add pass through
            pt_name = f"{jm_bdev}_PT"
            ret = rpc_client.bdev_PT_NoExcl_create(pt_name, jm_bdev)
            if not ret:
                logger.error(f"Failed to create pt noexcl bdev: {pt_name}")
                return False

            pt_spdk_uuid = rpc_client.get_bdevs(pt_name)[0]["aliases"][0]
            jm_device.pt_bdev_uuid = pt_spdk_uuid
            subsystem_nqn = snode.subsystem + ":dev:" + jm_bdev
            logger.info("creating subsystem %s", subsystem_nqn)
            ret = rpc_client.subsystem_create(subsystem_nqn, 'sbcli-cn', jm_bdev)
            logger.info(f"add {pt_name} to subsystem")
            ret = rpc_client.nvmf_subsystem_add_ns(subsystem_nqn, pt_name, snode.get_id())
            if not ret:
                logger.error(f"Failed to add: {pt_name} to the subsystem: {subsystem_nqn}")
                return False

            for iface in snode.data_nics:
                if iface.ip4_address:
                    logger.info("adding listener for %s on IP %s" % (subsystem_nqn, iface.ip4_address))
                    ret = rpc_client.listeners_create(subsystem_nqn, iface.trtype, iface.ip4_address, snode.nvmf_port)
        jm_device.status = JMDevice.STATUS_ONLINE
        snode.jm_device = jm_device
        snode.write_to_db()

    return True


def _connect_device_thread(name: str, device: NVMeDevice, node: StorageNode):
    """Thread body for bulk remote-device connects: bounded retry + loud logs.

    An exception raised in a bare ``Thread(target=connect_device)`` vanishes —
    the device silently ends up missing from ``remote_devices``, its map entry
    degrades to ``unavailable``, and the first stripe read through it fails
    minutes later with no trace of the real cause (2026-07-16 half-cluster
    incident: connect threads died on FDB timeouts BEFORE the attach RPC ran).
    The attach RPC itself is cheap (µs on a reachable peer, 1s cap otherwise),
    so a transient bookkeeping/RPC failure is always worth two more attempts,
    and a terminal failure must name the device and the reason at ERROR level.

    Fast-skip (2026-07-20 FD-0 reboot): if the device's owning peer is
    known-down (its whole failure domain is rebooting), a connect cannot
    succeed yet — do a single attempt instead of the 1+2+3s backoff-retry.
    A whole-FD reboot otherwise burned ~6s of sleeps per device × many devices
    × parallel restarts, and each sleep now also pins a bounded worker slot
    (_restart_worker_sem), starving reachable connects and the recreate. A
    later restart pass / health repair reconnects the device once its owner is
    back ONLINE — coverage is unchanged, only the pointless backoff is dropped.
    """
    attempts: tuple = (1, 2, 3)
    try:
        owner = DBController().get_storage_node_by_id(device.node_id)
        if owner is not None and owner.status != StorageNode.STATUS_ONLINE:
            attempts = (1,)
    except Exception:
        # Unknown owner / DB hiccup: keep the full best-effort retry.
        pass
    last_err: Optional[Exception] = None
    for attempt in attempts:
        try:
            connect_device(name, device, node)
            return
        except Exception as e:
            last_err = e
            logger.warning(
                "connect %s -> node %s attempt %d/%d failed: %s",
                name, node.get_id(), attempt, len(attempts), e)
            if attempt != attempts[-1]:
                time.sleep(attempt)
    logger.error(
        "connect %s -> node %s failed after %d attempt(s): %s",
        name, node.get_id(), len(attempts), last_err)


def _connect_to_remote_devs(
        this_node: StorageNode, /,
        reattach: bool = True, force_connect_restarting_nodes: bool = False,
        only_node_id: Optional[str] = None
):
    """Connect ``this_node`` to remote data devices and return the refreshed
    remote-device records.

    ``only_node_id`` switches to DELTA mode: only devices owned by that node
    are (re)connected and verified; records for every other device are carried
    over from ``this_node.remote_devices`` untouched. Used by the restart flow,
    where the restarted node is the only peer whose connections changed —
    the full-inventory reconcile there cost O(peers × cluster devices)
    name-filtered get_bdevs probes per restart (~4,000 on a 32-node cluster,
    measured 2026-07-10) although the restarted node contributes 2-3 devices.
    """
    db_controller = DBController()

    rpc_client = this_node.rpc_client(timeout=30, retry=1)

    # No full bdev_get_bdevs snapshot here: on large clusters the unfiltered
    # dump takes seconds of SPDK app-thread time per call (O(cluster size)
    # inventory) and this function used to issue it 3+ times. All existence
    # checks below use name-filtered probes on the exact expected bdev name
    # (always ``remote_<alceml_bdev>n1`` — single-namespace attach).
    remote_devices = []
    existing_remote_devices = {dev.get_id(): dev for dev in this_node.remote_devices}

    allowed_node_statuses = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED]
    allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY, NVMeDevice.STATUS_CANNOT_ALLOCATE]

    if force_connect_restarting_nodes:
        allowed_node_statuses.append(StorageNode.STATUS_RESTARTING)
        allowed_dev_statuses.append(NVMeDevice.STATUS_UNAVAILABLE)

    devices_to_connect = []
    connect_threads = []
    nodes = db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id)
    # connect to remote devs
    for node_index, node in enumerate(nodes):
        if node.get_id() == this_node.get_id() or node.status not in allowed_node_statuses:
            continue
        if only_node_id and node.get_id() != only_node_id:
            continue
        logger.info(f"Connecting to node {node.get_id()}")
        for index, dev in enumerate(node.nvme_devices):

            if dev.status not in allowed_dev_statuses:
                logger.debug(f"Device is not online: {dev.get_id()}, status: {dev.status}")
                continue

            if not dev.alceml_bdev:
                raise ValueError(f"device alceml bdev not found!, {dev.get_id()}")
            devices_to_connect.append(dev)
            t = _bounded_thread(
                _connect_device_thread,
                (f"remote_{dev.alceml_bdev}", dev, this_node,))
            connect_threads.append(t)
            t.start()

    for t in connect_threads:
        t.join()

    def _find_remote_bdev(dev):
        # UUID lookup requires a populated pt_bdev_uuid. An empty string makes
        # get_bdevs return the WHOLE bdev table and ret[0] is whatever bdev
        # happens to be first (a raw local partition like nvme_1fn1) — that
        # name then gets persisted as remote_bdev for every device, poisoning
        # every cluster map and failing raid-on-distrib creation cluster-wide
        # (2026-07-10 activation regression, all deploys after SFAM-2774).
        # Devices without the new field fall back to the name-based probe.
        if dev.pt_bdev_uuid:
            ret = rpc_client.get_bdevs(dev.pt_bdev_uuid)
            if ret:
                name = ret[0]["name"]
                # A remote attach must resolve to the attached nvme bdev,
                # never to a local base bdev that shares the table.
                if name.startswith("remote_"):
                    return name
                logger.warning(
                    "pt_bdev_uuid %s of device %s resolved to non-remote bdev "
                    "%s on node %s; falling back to name probe",
                    dev.pt_bdev_uuid, dev.get_id(), name, this_node.get_id())
        expected = f"remote_{dev.alceml_bdev}n1"
        try:
            return expected if rpc_client.get_bdevs(expected) else ""
        except Exception:
            return ""

    remote_device_ids = set()
    # Shared surface-poll over the whole pending set: the old per-device
    # ``for _ in range(10): sleep(0.5)`` serialized the waits — up to 5s PER
    # not-yet-surfaced device (measured as the dominant slice of the ~67s
    # full reconcile on a 32-node/64-device cluster, 2026-07-13). One tick
    # now re-probes every still-pending device, so total wait is bounded by
    # the SLOWEST device (max ~5s), not the sum. Probes stay name-filtered —
    # no unfiltered bdev dump, which is O(all bdevs incl. lvols) of SPDK
    # app-thread time (see the comment at the top of this function).
    pending = {}
    for dev in devices_to_connect:
        remote_bdev = RemoteDevice()
        remote_bdev.uuid = dev.uuid
        remote_bdev.alceml_name = dev.alceml_name
        remote_bdev.node_id = dev.node_id
        remote_bdev.size = dev.size
        remote_bdev.status = NVMeDevice.STATUS_ONLINE
        remote_bdev.nvmf_multipath = dev.nvmf_multipath
        remote_bdev.remote_bdev = _find_remote_bdev(dev)
        pending[dev.get_id()] = (dev, remote_bdev)

    for _ in range(10):
        if all(rb.remote_bdev for _, rb in pending.values()):
            break
        time.sleep(0.5)
        for dev, remote_bdev in pending.values():
            if not remote_bdev.remote_bdev:
                remote_bdev.remote_bdev = _find_remote_bdev(dev)

    for dev, remote_bdev in pending.values():
        if not remote_bdev.remote_bdev and dev.get_id() in existing_remote_devices:
            existing_remote_device = existing_remote_devices[dev.get_id()]
            if existing_remote_device.remote_bdev and rpc_client.get_bdevs(existing_remote_device.remote_bdev):
                remote_bdev.remote_bdev = existing_remote_device.remote_bdev
        if not remote_bdev.remote_bdev:
            logger.error(f"Failed to connect to remote device {dev.alceml_name}")
            continue
        remote_devices.append(remote_bdev)
        remote_device_ids.add(dev.get_id())

    if only_node_id:
        # Delta mode: connections to every other node did not change — carry
        # the caller's existing records over verbatim instead of re-probing
        # the whole cluster inventory (the full sweep below costs one
        # get_bdevs per device per call).
        for dev_id, existing in existing_remote_devices.items():
            if dev_id not in remote_device_ids and existing.node_id != only_node_id:
                remote_devices.append(existing)
                remote_device_ids.add(dev_id)
        return remote_devices

    # Some callers overwrite node.remote_devices with this return value. Make
    # the return value authoritative for existing SPDK state, not only for the
    # connect attempts above.
    #
    # Batched probe: ONE bdev dump answers every device's presence question
    # below (previously one filtered get_bdevs RPC per device — the full
    # sweep cost O(cluster devices) round-trips; measured share of the
    # +31,710 excess get_bdevs, 2026-07-21 FD recovery). None -> fall back
    # to per-device probes.
    _sweep_bdev_names = _fetch_bdev_name_set(rpc_client)
    for node in nodes:
        if node.get_id() == this_node.get_id() or node.status not in allowed_node_statuses:
            continue
        if only_node_id and node.get_id() != only_node_id:
            # DELTA mode: records for every other node were carried over
            # untouched above — probing their bdevs anyway made each delta
            # call O(cluster devices) get_bdevs, which dominated the
            # pre-activation repair (measured 38 links/min over 1116 links,
            # ~25 min, 2026-07-13 validation run). Verify only the delta
            # owner's devices.
            continue
        for dev in node.nvme_devices:
            if dev.get_id() in remote_device_ids:
                continue
            if dev.status not in allowed_dev_statuses:
                continue
            expected_bdev = f"remote_{dev.alceml_bdev}n1"
            if _sweep_bdev_names is not None:
                if expected_bdev not in _sweep_bdev_names:
                    continue
            else:
                try:
                    if not rpc_client.get_bdevs(expected_bdev):
                        continue
                except Exception:
                    continue
            remote_bdev = RemoteDevice()
            remote_bdev.uuid = dev.uuid
            remote_bdev.alceml_name = dev.alceml_name
            remote_bdev.node_id = dev.node_id
            remote_bdev.size = dev.size
            remote_bdev.status = NVMeDevice.STATUS_ONLINE
            remote_bdev.nvmf_multipath = dev.nvmf_multipath
            remote_bdev.remote_bdev = expected_bdev
            remote_devices.append(remote_bdev)
            remote_device_ids.add(dev.get_id())

    return remote_devices


def _fetch_bdev_name_set(rpc_client):
    """One ``bdev_nvme_get_controllers`` inventory -> set of every attached
    NVMe controller name plus its namespace bdev name (``<controller>n1``),
    or ``None`` if the inventory could not be fetched (caller falls back to
    per-name probes).

    Purpose: the reconcile sweeps used to probe presence with ONE filtered
    ``get_bdevs(name)`` RPC PER DEVICE — measured +31,710 excess get_bdevs
    during the 2026-07-21 16-node FD recovery, each paying the full CP
    round-trip. One inventory per sweep answers every membership question
    locally.

    Why controllers and NOT an unfiltered ``bdev_get_bdevs``: the full bdev
    dump serializes EVERY bdev on the SPDK app thread and its size scales
    with lvol+snapshot count, not device count. Run 20260725 (3k lvols +
    18k snapshots): one dump took 18s+, starving keep-alive handling on the
    app thread -> KATO storms -> JC/JM exclusions -> node aborts. The
    controllers inventory scales with attached controllers only (~cluster
    devices + JMs) regardless of object count. Every consumer of this set
    tests ``remote_<...>n1`` namespace-bdev names of nvme-attached
    controllers, so the controller inventory answers the same question.

    Approximation: controller present => its ``n1`` namespace bdev present.
    A controller wedged without its namespace is rare and still caught by
    the exact per-device ``check_bdev`` probes in the health pass; a false
    positive here never blocks the repair path (which re-probes exact names).
    Freshness is the same TOCTOU class as the sequential per-device probes
    this replaces (a sweep was never atomic).
    """
    try:
        ret = rpc_client.bdev_nvme_controller_list()
    except Exception as e:
        logger.debug("controller inventory for batched probe failed: %s", e)
        return None
    if not ret:
        return None
    names = set()
    for c in ret:
        n = c.get("name")
        if n:
            names.add(n)
            names.add(f"{n}n1")
    return names


def _verify_online_device_coverage(snode: StorageNode, repair: bool = True):
    """Verify this node's SPDK holds a remote bdev for every data device its
    distrib cluster maps will list as reachable; optionally repair and re-check.

    Returns the sorted list of still-missing expected bdev names (empty means
    full coverage). Coverage means: for every ONLINE/DOWN peer, every data
    device in a data-bearing status has its ``remote_<alceml_bdev>n1`` bdev
    present on ``snode``. Peers in in_restart/offline are excluded on purpose —
    they are legitimately unconnected and re-linked by the health service once
    they recover.

    Why this gate exists: the node-specific cluster map degrades any device
    absent from ``remote_devices`` to ``unavailable`` (distr_controller), so a
    single silently-failed connect surfaces only minutes later — as an EIO on
    the first stripe whose surviving chunk lives on that device, deep inside
    the raid examine read of recreate_lvstore (2026-07-16 half-cluster
    incident: 16 nodes looped restart→EIO→offline for 1.5h over exactly this).
    Verifying coverage BEFORE the data path turns that into an immediate,
    named, retryable failure.
    """
    db_controller = DBController()
    rpc_client = snode.rpc_client(timeout=10, retry=1)

    # Degraded-recovery carve-out (FD-0 whole-domain reboot, 2026-07-20).
    # When the target's ENTIRE failure domain is dead and its concurrent
    # recovery is sanctioned (fd_dead_recovery_allowed), that domain's sibling
    # peers are legitimately unreachable — their remote bdevs cannot be
    # connected until they come back. Requiring them here is the circular
    # dependency that made a whole-FD reboot loop restart->abort->offline:
    # every member aborts the coverage gate waiting on its down siblings'
    # devices, kills SPDK, goes offline, retries, and never converges.
    #
    # The predicate is a hard FTT floor: it opens ONLY when >=2 domains exist,
    # NO member of the target's domain is ONLINE, and NO node outside the domain
    # is restarting/shutting down — i.e. exactly one domain is down, strictly
    # within FTT. The raid rebuilds the excluded chunks from the surviving
    # domains during examine, which is precisely what the failure-domain design
    # tolerates. This does NOT weaken the 2026-07-16 protection: if a
    # same-domain member is still ONLINE (a device silently failed while the
    # domain was serving) the predicate is False and every online peer's device
    # stays required, so the gate still aborts before the data path.
    fd_recovery = fd_dead_recovery_allowed(db_controller, snode)
    excluded_fd_devs = 0

    expected = {}  # expected remote bdev name -> owning device record
    for peer in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if peer.get_id() == snode.get_id():
            continue
        if peer.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
            continue
        if fd_recovery and peer.failure_domain == snode.failure_domain:
            # Legitimately-down sibling in the domain being recovered. Count it
            # for the log line below, but do not require its bdevs.
            excluded_fd_devs += sum(
                1 for d in peer.nvme_devices
                if d.status in (NVMeDevice.STATUS_ONLINE,
                                NVMeDevice.STATUS_READONLY,
                                NVMeDevice.STATUS_CANNOT_ALLOCATE) and d.alceml_bdev)
            continue
        for dev in peer.nvme_devices:
            if dev.status not in (NVMeDevice.STATUS_ONLINE,
                                  NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE):
                continue
            if not dev.alceml_bdev:
                continue
            expected[f"remote_{dev.alceml_bdev}n1"] = dev

    if fd_recovery and excluded_fd_devs:
        # Never a silent relaxation — degraded recovery must be visible in logs.
        logger.warning(
            "Coverage gate on %s: sanctioned dead-FD recovery (domain %s) — "
            "excluding %d device(s) owned by down same-domain peers from the "
            "required set; raid rebuilds those chunks degraded within FTT.",
            snode.get_id(), snode.failure_domain, excluded_fd_devs)

    def _probe_missing():
        # Batched probe: one dump per pass instead of one filtered
        # get_bdevs per expected device (up to ~97 per pass, two passes per
        # restart — measured share of the +31,710 excess get_bdevs,
        # 2026-07-21). Fallback keeps the old per-device semantics,
        # including "probe error counts as missing".
        names = _fetch_bdev_name_set(rpc_client)
        out = {}
        for bdev, dev in expected.items():
            if names is not None:
                if bdev not in names:
                    out[bdev] = dev
            else:
                try:
                    if not rpc_client.get_bdevs(bdev):
                        out[bdev] = dev
                except Exception:
                    out[bdev] = dev
        return out

    missing = _probe_missing()
    if missing and repair:
        logger.warning(
            "Connectivity coverage on %s: %d/%d remote device bdevs missing, "
            "re-attempting connects: %s",
            snode.get_id(), len(missing), len(expected), sorted(missing))
        repair_threads = []
        for dev in missing.values():
            t = _bounded_thread(
                _connect_device_thread,
                (f"remote_{dev.alceml_bdev}", dev, snode))
            t.start()
            repair_threads.append(t)
        for t in repair_threads:
            t.join()
        missing = _probe_missing()
        if not missing:
            # The repair attached bdevs the earlier reconcile missed. Refresh
            # the persisted remote_devices records too — the cluster map is
            # generated from node.remote_devices, not from SPDK state, so
            # without this the repaired devices still degrade to
            # `unavailable` in the maps.
            with _remote_connect_lock(snode.get_id()):
                remote_devices = _connect_to_remote_devs(snode)
                db_controller.atomic_update(
                    snode,
                    lambda n, rd=remote_devices: setattr(n, "remote_devices", rd))
    return sorted(missing)


def sync_remote_devices_from_spdk(this_node: StorageNode):
    """Persist remote data bdevs that already exist in SPDK for this node."""
    db_controller = DBController()
    rpc_client = this_node.rpc_client(timeout=5, retry=1)
    fresh_node = db_controller.get_storage_node_by_id(this_node.get_id())
    remote_by_id = {dev.get_id(): dev for dev in fresh_node.remote_devices}
    changed = False
    # Batched probe: one dump instead of one filtered get_bdevs per peer
    # device (O(cluster devices) per call). None -> per-device fallback.
    _sweep_bdev_names = _fetch_bdev_name_set(rpc_client)

    for peer in db_controller.get_storage_nodes_by_cluster_id(fresh_node.cluster_id):
        if peer.get_id() == fresh_node.get_id():
            continue
        if peer.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_RESTARTING]:
            continue
        for dev in peer.nvme_devices:
            if dev.status not in [
                NVMeDevice.STATUS_ONLINE,
                NVMeDevice.STATUS_READONLY,
                NVMeDevice.STATUS_CANNOT_ALLOCATE,
            ]:
                continue
            expected_bdev = f"remote_{dev.alceml_bdev}n1"
            if _sweep_bdev_names is not None:
                if expected_bdev not in _sweep_bdev_names:
                    continue
            elif not rpc_client.get_bdevs(expected_bdev):
                continue
            remote_dev = remote_by_id.get(dev.get_id())
            if remote_dev:
                if remote_dev.remote_bdev != expected_bdev or remote_dev.status != NVMeDevice.STATUS_ONLINE:
                    remote_dev.remote_bdev = expected_bdev
                    remote_dev.status = NVMeDevice.STATUS_ONLINE
                    changed = True
            else:
                remote_dev = RemoteDevice()
                remote_dev.uuid = dev.uuid
                remote_dev.alceml_name = dev.alceml_name
                remote_dev.node_id = dev.node_id
                remote_dev.size = dev.size
                remote_dev.status = NVMeDevice.STATUS_ONLINE
                remote_dev.nvmf_multipath = dev.nvmf_multipath
                remote_dev.remote_bdev = expected_bdev
                fresh_node.remote_devices.append(remote_dev)
                remote_by_id[dev.get_id()] = remote_dev
                changed = True

    if changed:
        fresh_node.write_to_db(db_controller.kv_store)
    return changed


def reconnect_dropped_remote_devs(this_node: StorageNode):
    """Topology-driven repair for remote data-device connections.

    ``node.remote_devices`` is rebuilt as "whatever was reachable at that
    moment" by the restart / port-allow paths (via ``_connect_to_remote_devs``),
    so a peer that is unreachable while this node restarts is silently dropped
    from the list. The health-check repair loop iterates only the persisted
    list, which makes the dropped connection invisible: it is never checked
    and never re-established. Observed after restarting a node during another
    node's network outage — the cross connections stayed down long after the
    outage ended, because the outage node recovered via port-unblock (DOWN →
    port_allow), not a restart, and therefore never fanned out reconnects to
    its peers.

    Derive the *expected* remote-device set from cluster topology instead:
    every relevant peer's usable devices. Reconnect any device missing from
    ``this_node.remote_devices`` and append it to the persisted list, so the
    regular list-driven health check covers it from the next cycle on.

    The peer gate mirrors ``health_controller._peer_connections_relevant``
    (ONLINE/DOWN/UNREACHABLE — inlined here to avoid a circular import):
    connections to those peers are expected to exist, so a failed reconnect
    counts as a fault, exactly like a failed reconnect of a listed entry.
    Peers in transitional states are skipped — a RESTARTING peer's own
    restart fans out reconnects to all online nodes when it completes.

    Returns ``(changed, all_ok)``: ``changed`` is True when entries were
    appended (callers holding a stale node object should re-read it);
    ``all_ok`` is False when at least one expected device could not be
    connected.
    """
    db_controller = DBController()

    fresh_node = db_controller.get_storage_node_by_id(this_node.get_id())
    known_ids = {dev.get_id() for dev in fresh_node.remote_devices}
    changed = False
    all_ok = True

    for peer in db_controller.get_storage_nodes_by_cluster_id(fresh_node.cluster_id):
        if peer.get_id() == fresh_node.get_id():
            continue
        if peer.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN,
                               StorageNode.STATUS_UNREACHABLE]:
            continue
        for dev in peer.nvme_devices:
            if dev.get_id() in known_ids:
                continue
            if dev.status not in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_READONLY,
                                  NVMeDevice.STATUS_CANNOT_ALLOCATE]:
                continue
            if not dev.alceml_bdev:
                logger.error(f"device alceml bdev not found!, {dev.get_id()}")
                continue
            logger.info(
                "Remote device %s on peer %s missing from remote_devices; reconnecting",
                dev.get_id(), peer.get_id())
            try:
                remote_bdev = connect_device(
                    f"remote_{dev.alceml_bdev}", dev, fresh_node)
            except Exception as e:
                logger.error(
                    "Failed to reconnect dropped remote device %s on peer %s: %s",
                    dev.get_id(), peer.get_id(), e)
                all_ok = False
                continue
            remote_dev = RemoteDevice()
            remote_dev.uuid = dev.uuid
            remote_dev.alceml_name = dev.alceml_name
            remote_dev.node_id = dev.node_id
            remote_dev.size = dev.size
            remote_dev.status = NVMeDevice.STATUS_ONLINE
            remote_dev.nvmf_multipath = dev.nvmf_multipath
            remote_dev.remote_bdev = remote_bdev or f"remote_{dev.alceml_bdev}n1"
            fresh_node.remote_devices.append(remote_dev)
            known_ids.add(dev.get_id())
            changed = True

    if changed:
        fresh_node.write_to_db(db_controller.kv_store)
    return changed, all_ok


def _peer_reachable_via_jm_quorum(target_node_id, this_node: StorageNode, peer_probe_timeout=1):
    """Check whether ``target_node`` is reachable on the data plane by asking
    other online peers about their JM quorum state.

    Each peer's ``jc_get_jm_status(jm_vuid)`` returns a dict that includes
    ``remote_jm_<peer>n1: bool``. If any online peer (other than this_node and
    target) reports the target's remote_jm as True, the target is reachable
    from at least one vantage point and we attempt the attach. If we can probe
    one or more peers and none of them report the target reachable, treat it
    as data-plane unreachable and skip the attach. If we can't probe any
    peer, default to True (don't block on missing information).
    """
    db_controller = DBController()
    remote_key = f"remote_jm_{target_node_id}n1"
    probed = False
    for peer in db_controller.get_storage_nodes_by_cluster_id(this_node.cluster_id):
        if peer.get_id() in (target_node_id, this_node.get_id()):
            continue
        if peer.status != StorageNode.STATUS_ONLINE:
            continue
        if not peer.jm_vuid:
            continue
        try:
            ret = peer.rpc_client(timeout=peer_probe_timeout, retry=0).jc_get_jm_status(peer.jm_vuid)
        except Exception as e:
            logger.debug("JM-quorum probe on %s failed: %s", peer.get_id(), e)
            continue
        if not isinstance(ret, dict):
            continue
        probed = True
        if ret.get(remote_key) is True:
            return True
    return not probed


def _connect_to_remote_jm_devs(this_node: StorageNode, jm_ids=None, only_node_id=None):
    """Connect ``this_node`` to remote JM devices and return the refreshed
    remote-JM records.

    ``only_node_id`` switches to DELTA mode: only JM devices owned by that
    node are (re)connected; records for JMs owned by other nodes are carried
    over from ``this_node.remote_jm_devices`` untouched (same rationale and
    measurement as _connect_to_remote_devs delta mode).

    Always connects under the JM owner's own natural name. A replacement JM
    picked for a removed peer (see _decommission_node_devices) used to be
    forced to answer under the removed peer's OLD name here, via
    JMDevice.override_name_on_node, so this_node's already-built distrib/
    JM-raid construct (which has that name baked in as a member) wouldn't
    need touching. That naming trick is retired now that SPDK's
    jc_replace_jm RPC can swap a live JC member by name directly:
    _decommission_node_devices connects the replacement under its own name
    and calls jc_replace_jm to update the construct in place, so nothing
    downstream needs to keep pretending the new device is named after the
    old one.
    """
    db_controller = DBController()

    rpc_client = this_node.rpc_client(timeout=30, retry=2)

    # No full bdev snapshot: connect_device probes the exact expected bdev via
    # a name-filtered query (bdev_names=None). See _connect_to_remote_devs.
    remote_devices = []
    if jm_ids:
        for jm_id in jm_ids:
            jm_dev = db_controller.get_jm_device_by_id(jm_id)
            if jm_dev:
                remote_devices.append(jm_dev)

    if this_node.jm_ids:
        for jm_id in this_node.jm_ids:
            jm_dev = db_controller.get_jm_device_by_id(jm_id)
            if jm_dev and jm_dev not in remote_devices:
                remote_devices.append(jm_dev)

    for sec_attr in ['lvstore_stack_secondary', 'lvstore_stack_tertiary']:
        sec_primary_id = getattr(this_node, sec_attr, None)
        if sec_primary_id:
            org_node = db_controller.get_storage_node_by_id(sec_primary_id)
            if org_node.jm_device and org_node.jm_device not in remote_devices:
                remote_devices.append(org_node.jm_device)
            for jm_id in org_node.jm_ids:
                jm_dev = db_controller.get_jm_device_by_id(jm_id)
                if jm_dev and jm_dev not in remote_devices:
                    remote_devices.append(jm_dev)

    logger.debug(f"remote_devices: {remote_devices}")
    allowed_node_statuses = [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_RESTARTING]
    allowed_dev_statuses = [NVMeDevice.STATUS_ONLINE]

    new_devs = []
    existing_remote_jm_devices = {dev.get_id(): dev for dev in this_node.remote_jm_devices}
    # Index JM owners once: the previous per-JM full get_storage_nodes()
    # scan cost O(JMs x nodes) FDB reads per call (called several times per
    # restart / create).
    jm_owner_by_id = {}
    for node in db_controller.get_storage_nodes():
        if node.jm_device:
            jm_owner_by_id[node.jm_device.get_id()] = node
    for jm_dev in remote_devices:
        if not jm_dev.jm_bdev:
            continue

        org_dev_node = jm_owner_by_id.get(jm_dev.get_id())
        org_dev = org_dev_node.jm_device if org_dev_node is not None else None

        if not org_dev or org_dev in new_devs or org_dev_node and org_dev_node.get_id() == this_node.get_id():
            continue

        if only_node_id and org_dev_node is not None and org_dev_node.get_id() != only_node_id:
            # Delta mode: this JM belongs to a node whose connections did not
            # change — keep the existing record (if any) without re-probing.
            existing = existing_remote_jm_devices.get(org_dev.get_id())
            if existing is not None:
                new_devs.append(existing)
            continue

        if org_dev_node is not None and org_dev_node.status not in allowed_node_statuses:
            logger.warning(f"Skipping node:{org_dev_node.get_id()} with status: {org_dev_node.status}")
            continue

        if org_dev is not None and org_dev.status not in allowed_dev_statuses:
            logger.warning(f"Skipping device:{org_dev.get_id()} with status: {org_dev.status}")
            continue

        # Quorum reachability check intentionally not gated here:
        # during cluster_activate the peers' JC quorums are still being
        # bootstrapped, so _peer_reachable_via_jm_quorum cannot answer
        # correctly for a not-yet-built group and would skip every intended
        # member of the new jm_vuid. Runtime re-attach paths (rejoin,
        # restart-task) carry their own reachability gating.

        # Always connect under org_dev's own current name -- see the
        # docstring for why no override resolution happens here anymore.
        resolved_name = org_dev.jm_bdev

        remote_device = RemoteJMDevice()
        remote_device.uuid = org_dev.uuid
        remote_device.alceml_name = org_dev.alceml_name
        remote_device.node_id = org_dev.node_id
        remote_device.size = org_dev.size
        remote_device.jm_bdev = resolved_name
        remote_device.status = NVMeDevice.STATUS_ONLINE
        remote_device.nvmf_multipath = org_dev.nvmf_multipath
        expected_bdev = f"remote_{resolved_name}n1"
        controller_name = f"remote_{resolved_name}"
        connect_failed = False
        try:
            remote_device.remote_bdev = str(connect_device(
                controller_name, org_dev, this_node,
                attach_timeout=1,
            ))
        except (RuntimeError, RPCException):
            # RPCException included: during parallel suspended-cluster
            # recovery the JM owner may be mid-restart (allowed_node_statuses
            # includes RESTARTING) with its SPDK not yet serving — a
            # connection error against it must degrade to "this JM not
            # connected" (re-established by the peer's own restart completion
            # via _reconnect_peers_to_restarted_node), NOT abort the whole
            # restart of this node (observed 2026-07-10: aborted restarts
            # looping offline<->in_restart for 10+ minutes).
            logger.error(f'Failed to connect to {org_dev.get_id()}')
            connect_failed = True
        # When the connect raised, no new attach is in flight — poll once to
        # pick up a bdev left by an earlier attach, but don't wait the full
        # 5s for one that can never appear. During whole-cluster recovery the
        # 10x0.5s wait ran per dead peer JM (~30 of them), adding minutes to
        # every restart attempt (2026-07-13).
        def _poll_for_remote_jm_bdev():
            for _ in range(1 if connect_failed else 10):
                if remote_device.remote_bdev and rpc_client.get_bdevs(remote_device.remote_bdev):
                    return
                if rpc_client.get_bdevs(expected_bdev):
                    remote_device.remote_bdev = expected_bdev
                    return
                time.sleep(0.5)
            if not remote_device.remote_bdev and org_dev.get_id() in existing_remote_jm_devices:
                existing_remote_device = existing_remote_jm_devices[org_dev.get_id()]
                if existing_remote_device.remote_bdev and rpc_client.get_bdevs(existing_remote_device.remote_bdev):
                    remote_device.remote_bdev = existing_remote_device.remote_bdev

        try:
            # Bounded retry: a transient RPC/DNS blip against this_node's
            # own proxy (the same one connect_device just hit above) is
            # given a few seconds to clear before giving up. Same
            # degrade-not-crash rationale as the connect_device catch
            # above -- only RPCException (the transport-level failure) is
            # retried; anything else propagates immediately.
            Retrying(
                stop=stop_after_attempt(3),
                wait=wait_fixed(1),
                retry=retry_if_exception_type(RPCException),
                before_sleep=before_sleep_log(logger, logging.WARNING),
            )(_poll_for_remote_jm_bdev)
        except RetryError as e:
            # Still failing after 3 attempts -- degrade to "this JM not
            # connected" instead of aborting the whole node-removal /
            # restart operation (2026-08-10 incident: this exact call
            # raised uncaught and killed a node-removal task mid phase 5,
            # leaving a peer's lvstore un-rebuilt while the task still
            # reported "done").
            logger.warning(
                f'get_bdevs kept failing while polling for {expected_bdev} '
                f'on {this_node.get_id()} after 3 attempts: {e}')
        if not remote_device.remote_bdev:
            logger.error(f"Failed to connect to remote JM device {org_dev.alceml_name}")
            continue
        new_devs.append(remote_device)

    return new_devs


def _reconnect_peers_to_restarted_node(snode: StorageNode, only_peer_ids=None):
    """Best-effort DELTA reconnect of every ONLINE peer to ``snode``'s
    devices and JM after its restart. Returns the set of peer ids whose
    reconnect FAILED (empty set = full success).

    Replaces the previous per-peer full-inventory reconcile
    (_connect_to_remote_devs without only_node_id), which issued
    O(peers × cluster devices) probes per restart — measured 3,996
    get_bdevs calls and ~40% of a 220 s restart on a 32-node cluster
    (2026-07-10). The restarted node is the only peer whose connections
    changed, so each peer reconnects to its 2-3 devices + JM only.

    ``only_peer_ids``: restrict the sweep to these peers. The restart flow
    runs this sweep twice (before recreates — connectivity is a recreate
    precondition — and in finalization); the second pass used to redo ALL
    peers, doubling the largest fan-out of the restart (2026-07-17 profile:
    the duplicate sweep was ~55% of a 3m51s restart; 2026-07-21: 2 sweeps x
    16 restarts x 16 peers = 512 coordinator jobs through the coordinator
    pool). Peer-to-device connections do not change during LVS recreation
    (recreates build distribs/raids/lvols on ``snode``, not new alceml
    devices), so the finalization pass now re-runs only the peers that
    FAILED the first pass.

    Peers are mutually independent (each worker mutates only its own
    peer's record), so they run concurrently. Reconnect stays best-effort
    per peer (retry 3x, then skip): a transient failure against a
    topologically unrelated peer must not abort the restart
    (incident 2026-06-25).
    """
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    attempted_ids = set()
    failed_ids = set()
    failed_guard = threading.Lock()

    def _one_peer(peer_id):
        for attempt in range(1, 4):
            try:
                # Fresh read per attempt to avoid overwriting concurrent changes
                node = db_controller.get_storage_node_by_id(peer_id)
                remote_devices = _connect_to_remote_devs(
                    node, force_connect_restarting_nodes=True,
                    only_node_id=snode.get_id())
                remote_jm_devices = None
                if node.enable_ha_jm:
                    remote_jm_devices = _connect_to_remote_jm_devs(
                        node, only_node_id=snode.get_id())

                # Atomic: a full-object write of the PEER's record here races
                # the peer's own flows (its restart phase transitions, status
                # writes). A stale copy written back resurrects a just-cleared
                # restart phase — the stale-phase generator behind the
                # 2026-07-10 lost-registration incidents.
                def _apply(n, rd=remote_devices, rjd=remote_jm_devices):
                    n.remote_devices = rd
                    if rjd is not None:
                        n.remote_jm_devices = rjd
                db_controller.atomic_update(node, _apply)
                return
            except (RPCException, RuntimeError) as e:
                logger.warning(
                    f"Reconnect of peer {peer_id} failed "
                    f"(attempt {attempt}/3): {e}")
                if attempt < 3:
                    time.sleep(2)
        logger.error(
            f"Skipping peer {peer_id} after 3 failed reconnect attempts; "
            f"continuing restart (peer reconnect is best-effort)")
        with failed_guard:
            failed_ids.add(peer_id)

    threads = []
    for node in snodes:
        if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
            continue
        if only_peer_ids is not None and node.get_id() not in only_peer_ids:
            continue
        attempted_ids.add(node.get_id())
        # COORDINATOR tier: _one_peer -> _connect_to_remote_devs spawns and
        # joins leaf _connect_device_thread workers. On the leaf semaphore
        # this deadlocked (holders joining waiters of their own pool).
        t = _bounded_thread(_one_peer, (node.get_id(),),
                            name=f"peer-reconnect-{node.get_id()[:8]}",
                            sem=_restart_coordinator_sem)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    return attempted_ids, failed_ids


def _refresh_cluster_maps_after_node_recovery(snode: StorageNode):
    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(snode.get_id())

    # Push a full cluster map after reconnect/restart recovery so peers do not
    # remain on stale per-device availability derived from transient reconnect state.
    distr_controller.send_cluster_map_to_node(snode)

    # Per-target maps are independent (each build is target-specific) — send
    # them in parallel. The serial loop cost n sequential O(n·d) builds +
    # 10s-timeout RPCs per recovering node, ~1024 sends across a
    # whole-cluster recovery (2026-07-13 audit).
    def _send_one(node):
        try:
            distr_controller.send_cluster_map_to_node(node)
        except Exception as e:
            logger.warning("Cluster-map push to %s failed (best-effort): %s",
                           node.get_id(), e)

    map_threads = []
    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.get_id() == snode.get_id():
            continue
        if node.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN]:
            t = _bounded_thread(_send_one, (node,),
                                name=f"map-push-{node.get_id()[:8]}")
            t.start()
            map_threads.append(t)
    for t in map_threads:
        t.join()


def ifc_is_tcp(nic):
    addrs = psutil.net_if_addrs().get(nic, [])
    for addr in addrs:
        if addr.family == socket.AF_INET:
            return True
    return False


def ifc_is_roce(nic):
    rdma_path = "/sys/class/infiniband/"
    if not os.path.exists(rdma_path):
        return False

    for rdma_dev in os.listdir(rdma_path):
        net_path = os.path.join(rdma_path, rdma_dev, "device/net")
        if os.path.exists(net_path):
            for iface in os.listdir(net_path):
                if iface == nic:
                    return True
    return False


def get_required_ha_jm_count(cluster) -> int:
    # FTT>=2 always needs 4 HA journals (can lose 2, keep >=2 quorum).
    if cluster.max_fault_tolerance >= 2:
        return 4
    # FTT=1 normally needs only 3 journals. BUT with failure domains and just
    # two domains, 3 journals must split 2-1 across the domains; losing the
    # whole domain that holds 2 drops the journal below the 2-JM quorum (you
    # "lose two out of three"). Require 4 so a 2-2 split survives a full-domain
    # loss. With >=3 domains a 1-1-1 spread of 3 would also be safe, but we keep
    # the rule simple and safe by requiring 4 whenever failure domains are on.
    if getattr(cluster, "enable_failure_domain", False):
        return 4
    return 3


def resolve_ha_jm_count(cluster, ha_jm_count) -> int:
    required_ha_jm_count = get_required_ha_jm_count(cluster)

    if ha_jm_count is None:
        return required_ha_jm_count

    if ha_jm_count < required_ha_jm_count:
        raise ValueError(
            f"ha_jm_count={ha_jm_count} is too low for max_fault_tolerance="
            f"{cluster.max_fault_tolerance}; minimum required is {required_ha_jm_count}"
        )

    return ha_jm_count


def _acquire_cluster_add_lock_blocking(db_controller, cluster_id, owner, timeout=300, poll=2):
    """Block until the per-cluster node-add mesh lock is held by ``owner``.

    Returns True once acquired, or False if ``timeout`` seconds elapse without
    acquiring (caller should fail the task so it is retried — failing here is
    cheaper than re-running the whole node-local setup). A lock abandoned by a
    crashed holder is reclaimed automatically once its heartbeat goes stale
    (constants.CLUSTER_ADD_LOCK_TTL_SEC), so the effective wait is bounded even
    if a holder died."""
    deadline = time.time() + timeout
    while True:
        won, current_owner = db_controller.acquire_cluster_add_lock(cluster_id, owner)
        if won:
            return True
        if time.time() >= deadline:
            logger.error(
                f"Timed out waiting for cluster node-add lock (held by {current_owner})")
            return False
        logger.info(f"Cluster node-add lock held by {current_owner}; waiting")
        time.sleep(poll)


def _cluster_add_lock_heartbeat(db_controller, cluster_id, owner, stop_event):
    """Refresh the node-add lock until ``stop_event`` is set, so a long mesh
    section on a large cluster isn't reclaimed out from under a live holder."""
    while not stop_event.wait(constants.CLUSTER_ADD_LOCK_HEARTBEAT_SEC):
        if not db_controller.refresh_cluster_add_lock(cluster_id, owner):
            # Lost the lock (reclaimed after a stall). Stop heartbeating; the
            # critical section will finish and its owner-scoped release is a
            # no-op against whoever holds it now.
            logger.warning("Lost cluster node-add lock heartbeat (reclaimed)")
            return


def _classify_existing_endpoint_record(db_controller, cluster_id, node_addr, ssd_pcie):
    """Classify a pre-existing storage-node record for ``node_addr`` that owns
    one of the joining node's SSDs, before an add-node proceeds.

    An add-node whose SSH/API session died can survive server-side and finish
    the registration (runs 20260712/20260715: orphaned adds completed after
    their channels were reset mid-command); the caller's retry then finds the
    record already present and must not fail permanently on it.

    Returns one of:
    - (None, None): no record for this endpoint owns any of these SSDs.
    - ("already_added", node): record is ONLINE — the earlier add completed;
      re-adding is an idempotent success, not an error.
    - ("cleanup", node): record stuck in in_creation — a dead partial add;
      kill its SPDK and delete the record, then re-add.
    - ("conflict", node): record in any other status — refuse; the operator
      must delete or restart that node explicitly.

    More than one record can match at once: a pod restart mid-onboarding
    (e.g. a transient node NotReady blip restarting the DaemonSet pod before
    the backend's own online-match safeguard applies) can leave a stale
    in_creation record behind even after a LATER add attempt for the same
    host has gone fully online (2026-08-06, gr5kf incident) — that stale
    record is never revisited by any other code path, so it must win the
    classification here regardless of which match iteration happens to find
    first: a cleanup is always needed when one exists, independent of
    whether another match is already online.
    """
    matches = [
        node
        for node in db_controller.get_storage_nodes_by_cluster_id(cluster_id)
        if node.api_endpoint == node_addr and any(ssd in node.ssd_pcie for ssd in ssd_pcie)
    ]
    for node in matches:
        if node.status == StorageNode.STATUS_IN_CREATION:
            return "cleanup", node
    for node in matches:
        if node.status == StorageNode.STATUS_ONLINE:
            return "already_added", node
    if matches:
        return "conflict", matches[0]
    return None, None


def _resolve_core_distribution(distribution, core_to_index):
    """utils.calculate_core_allocations returns a positional 9-tuple of core
    lists, not the {"app_thread_core": [...], ...} dict every consumer
    (add_node, persist_node_config's schema) actually reads -- regenerate_config
    resolves it this exact way (get_core_indexes against core_to_index) before
    it ever reaches a node_config; do the same here.
    """
    keys = (
        "app_thread_core", "jm_cpu_core", "poller_cpu_cores", "alceml_cpu_cores",
        "alceml_worker_cpu_cores", "distrib_cpu_cores", "jc_singleton_core",
        "lvol_poller_core", "compression_core",
    )
    return {
        key: utils.get_core_indexes(core_to_index, group)
        for key, group in zip(keys, distribution)
    }


def apply_cluster_vcpu_count(snode_api, node_info, nodes, vcpu_count):
    """Resize this host's isolated-core layout to the cluster's vcpu_count, in
    place on ``nodes`` and persisted to the host's on-disk config, exactly the
    way huge-page memory is persisted via persist_node_config.

    A node's CPU layout is decided once, here, at add time; nothing else ever
    touches it afterwards -- restart_storage_node only ever re-adopts
    max_subsys/hugepages_mem. add_node itself can be retried though (a prior
    attempt may have already resized and persisted this host's layout), so
    this must be a no-op when it has: skip re-fetching topology and rewriting
    the file whenever the host's current isolated-core totals, summed per
    socket, already match what vcpu_count implies for that socket.

    Returns True on success (including the no-op case) and False if the
    layout could not be resized -- add_node must then refuse the add rather
    than run SPDK on a stale, cluster-mismatched core count.
    """
    sockets_to_use = sorted({node["socket"] for node in nodes})
    if not sockets_to_use:
        return True

    # Same split as generate_core_allocation: the budget divides evenly
    # across the sockets in use, remainder to the earlier ones.
    base, remainder = divmod(vcpu_count, len(sockets_to_use))
    per_socket_budget = {
        numa_socket: base + (1 if index < remainder else 0)
        for index, numa_socket in enumerate(sockets_to_use)
    }

    changed_sockets = [
        numa_socket for numa_socket in sockets_to_use
        if sum(len(n.get("isolated") or []) for n in nodes if n["socket"] == numa_socket)
           != per_socket_budget[numa_socket]
    ]
    if not changed_sockets:
        return True

    cpu_topology = node_info.get("cpu_topology")
    if not cpu_topology:
        logger.error(
            "This cluster requires %d SPDK vCPU(s) per host, but the node did "
            "not report its CPU topology (upgrade the node agent, or re-run "
            "'sbcli sn configure'); cannot resize its core layout.", vcpu_count)
        return False
    cores_by_numa = {int(numa): cores for numa, cores in cpu_topology.items()}

    # nodes_per_socket is not passed in from anywhere -- it is however many
    # slots already share the busiest socket in this host's persisted config.
    nodes_per_socket = max(
        sum(1 for n in nodes if n["socket"] == numa_socket)
        for numa_socket in sockets_to_use
    )

    new_layout = utils.generate_core_allocation(
        cores_by_numa, sockets_to_use, nodes_per_socket, vcpu_count)

    for numa_socket in changed_sockets:
        entries = [n for n in nodes if n["socket"] == numa_socket]
        replacements = new_layout.get(numa_socket, [])
        if len(replacements) != len(entries):
            logger.error(
                "Cannot resize storage node CPUs on socket %s: expected %d "
                "node slot(s) there, computed %d for a vcpu-count of %d -- "
                "leaving its current CPU layout in place.",
                numa_socket, len(entries), len(replacements), vcpu_count)
            return False
        # Both lists follow the same order convention (generate_configs walks
        # sockets_to_use, then each socket's slots in generate_core_allocation
        # order) so position-by-position pairing is the entries' identity.
        for entry, replacement in zip(entries, replacements):
            entry["cpu_mask"] = replacement["cpu_mask"]
            entry["isolated"] = replacement["isolated"]
            entry["l-cores"] = replacement["l-cores"]
            entry["distribution"] = _resolve_core_distribution(
                replacement["distribution"], replacement["core_to_index"])
            entry["core_to_index"] = replacement["core_to_index"]

            # number_of_distribs is sized off distrib_cpu_cores at configure
            # time (generate_configs), before the host belongs to any cluster
            # -- so it reflects the host's full core count, not the budget
            # vcpu_count just clamped distrib_cpu_cores down to above. Rederive
            # it the same way regenerate_config does, or add_node persists a
            # distrib count sized for the pre-resize layout.
            number_of_distribs = 2
            number_of_distribs_cores = len(entry["distribution"]["distrib_cpu_cores"])
            if 12 >= number_of_distribs_cores > 2:
                number_of_distribs = number_of_distribs_cores
            elif number_of_distribs_cores > 12:
                number_of_distribs = 12
            entry["number_of_distribs"] = number_of_distribs

            ok, err = snode_api.persist_node_config(
                max_lvol=None, huge_page_memory=None, numa_node=numa_socket,
                ssd_list=entry.get("ssd_pcis"),
                cpu_mask=entry["cpu_mask"], isolated=entry["isolated"],
                l_cores=entry["l-cores"], distribution=entry["distribution"],
                core_to_index={str(k): v for k, v in entry["core_to_index"].items()},
                number_of_distribs=number_of_distribs)
            if not ok:
                logger.error(
                    "Failed to persist the resized CPU layout for socket %s: %s",
                    numa_socket, err)
                return False
    return True


def apply_cluster_hugepages(snode_api, node_config, req_cpu_count, max_prov):
    """Recalculate this node_config entry's huge-page memory -- and the
    small/large pool counts it is derived from -- against the real numbers
    now known, and persist the change, the same way the CPU layout resize
    in apply_cluster_vcpu_count does.

    sn configure priced this entry for the worst case: max_lvol capped at
    the product ceiling and its own default core-count heuristic, since it
    ran before the node belonged to any cluster. By the time this runs,
    node_config["max_lvol"] is already the cluster's real max_subsys (set
    above in add_node) and req_cpu_count is already the cluster's real
    vcpu_count (via apply_cluster_vcpu_count, before the per-entry loop) --
    so recomputing here against the same formula sn configure used, just fed
    the real numbers, is the accurate figure. A no-op when it already
    matches what's on file: in particular when the cluster set neither
    max_subsys nor vcpu_count, nothing about this entry's sizing has
    actually changed since configure time.

    Returns the correct huge_page_memory (already floored to max_prov) on
    success, or None if persisting a change failed.
    """
    max_lvol = int(node_config.get("max_lvol") or 0)
    number_of_alcemls = int(node_config.get("number_of_alcemls") or 0)
    number_of_distribs = int(node_config.get("number_of_distribs") or 0)
    poller_cores = (node_config.get("distribution") or {}).get("poller_cpu_cores") or []
    poller_count = len(poller_cores) or req_cpu_count

    small_pool_count, large_pool_count = utils.calculate_pool_count(
        number_of_alcemls, 2 * number_of_distribs, req_cpu_count, poller_count, max_lvol)
    huge_page_memory = max(
        utils.calculate_minimum_hp_memory(
            small_pool_count, large_pool_count, max_lvol, max_prov, req_cpu_count),
        max_prov)

    if (huge_page_memory == node_config.get("huge_page_memory")
            and small_pool_count == node_config.get("small_pool_count")
            and large_pool_count == node_config.get("large_pool_count")):
        return huge_page_memory

    node_config["huge_page_memory"] = huge_page_memory
    node_config["small_pool_count"] = small_pool_count
    node_config["large_pool_count"] = large_pool_count
    ok, err = snode_api.persist_node_config(
        max_lvol=None, huge_page_memory=huge_page_memory, numa_node=node_config.get("socket"),
        ssd_list=node_config.get("ssd_pcis"),
        small_pool_count=small_pool_count, large_pool_count=large_pool_count)
    if not ok:
        logger.error("Failed to persist the recalculated huge-page sizing: %s", err)
        return None
    return huge_page_memory


def add_node(cluster_id, node_addr, iface_name, data_nics_list,
             max_snap, spdk_image=None, spdk_debug=False,
             small_bufsize=0, large_bufsize=0,
             num_partitions_per_dev=0, jm_percent=0, enable_test_device=False,
             namespace=None, enable_ha_jm=False, cr_name=None, cr_namespace=None, cr_plural=None,
             id_device_by_nqn=False, partition_size="", ha_jm_count=None, format_4k=False,
             spdk_proxy_image=None, spdk_sys_mem=None, expansion=False, failure_domain=None):
    snode_api = SNodeClient(node_addr)
    node_info, _ = snode_api.info()
    if node_info.get("nodes_config") and node_info["nodes_config"].get("nodes"):
        nodes = node_info["nodes_config"]["nodes"]
    else:
        logger.error("Please run sbcli sn configure before adding the storage node, "
                     "If you run it and the config has been manually changed please "
                     "run 'sbcli sn configure-upgrade'")
        return False
    db_controller = DBController()
    for n in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
        if tasks_controller.get_active_lvol_migration(n.get_id()):
            msg = f"LVol migration tasks found on node: {n.get_id()}"
            logger.error(msg)
            return False

    # An activated cluster only grows through the expansion flow: a plain add
    # leaves the node outside the role rotation and the rebalance. This also
    # closes the loophole of activate -> suspend -> add-node -> activate, which
    # re-activation used to accept because a suspended cluster is not ACTIVE.
    try:
        _cluster = db_controller.get_cluster_by_id(cluster_id)
    except KeyError:
        logger.error("Cluster not found: %s", cluster_id)
        return False
    if not expansion and (_cluster.status == Cluster.STATUS_ACTIVE
                          or _cluster.activated_node_ids):
        logger.error(
            "Cluster %s has already been activated; add nodes with --expansion "
            "while the cluster is ACTIVE so roles are rotated and data is "
            "rebalanced", cluster_id)
        return False

    snode_api.set_hugepages()

    # Resize this host's core layout to the cluster's vcpu_count once, before
    # any node_config entry is consumed below, so every entry in the loop
    # already reflects it. A no-op when the host's layout already matches
    # (see apply_cluster_vcpu_count) -- in particular on a retried add_node,
    # where a prior attempt already resized and persisted it.
    cluster_vcpu_count = int(getattr(_cluster, "spdk_vcpu_count", 0) or 0)
    if cluster_vcpu_count and not apply_cluster_vcpu_count(
            snode_api, node_info, nodes, cluster_vcpu_count):
        logger.error("Refusing the add -- could not resize node %s's CPU "
                     "layout to the cluster's vcpu-count", node_addr)
        return False

    for node_config in nodes:
        logger.debug(node_config)
        kv_store = db_controller.kv_store

        try:
            cluster = db_controller.get_cluster_by_id(cluster_id)
        except KeyError:
            logger.error("Cluster not found: %s", cluster_id)
            return False

        ha_jm_count = resolve_ha_jm_count(cluster, ha_jm_count)

        # Cluster-level SPDK sizing. These live on the cluster so every node is
        # sized alike; a node adopts them when it is added and on each restart.
        cluster_max_subsys = int(getattr(cluster, "max_subsys", 0) or 0)
        cluster_hp_mem = int(getattr(cluster, "hugepages_mem", 0) or 0)
        cluster_vcpu_count = int(getattr(cluster, "spdk_vcpu_count", 0) or 0)

        if cluster_vcpu_count:
            total_cores = int(node_info.get("cpu_count") or 0)
            # Refuse rather than quietly running SPDK on fewer cores than the
            # cluster asks for. The rule (one core beyond the SPDK budget) lives
            # in utils.vcpu_requirement_met so add and restart cannot drift.
            if not utils.vcpu_requirement_met(total_cores, cluster_vcpu_count):
                logger.error(
                    "Node reports %d vCPU(s); this cluster requires %d for SPDK "
                    "plus at least one for the system (%d total). Refusing the "
                    "node -- lower the cluster vcpu-count or use a larger host.",
                    total_cores, cluster_vcpu_count, cluster_vcpu_count + 1)
                return False

        if cluster_max_subsys:
            node_config["max_lvol"] = cluster_max_subsys

        # Failure-domain id is mandatory exactly when the cluster has the
        # feature enabled (deploy-time only — clusters cannot be upgraded into
        # it, so the flag is fixed for the cluster's lifetime). 32-bit int,
        # >= 0 to activate; -1/None means unset.
        if cluster.enable_failure_domain:
            if failure_domain is None or failure_domain < 0:
                logger.error("This cluster was created with --enable-failure-domain; "
                             "--failure-domain <id> (a non-negative integer) is required "
                             "when adding a node.")
                return False
            failure_domain_id = failure_domain
        else:
            if failure_domain is not None and failure_domain >= 0:
                logger.error("--failure-domain was given but this cluster does not have the "
                             "failure-domain feature enabled. Redeploy the cluster with "
                             "--enable-failure-domain to use failure domains.")
                return False
            failure_domain_id = -1

        logger.info(f"Adding Storage node: {node_addr}")

        if not node_info:
            logger.error("SNode API is not reachable")
            return False
        logger.info(f"Node found: {node_info['hostname']}")
        # if "cluster_id" in node_info and node_info['cluster_id']:
        #     if node_info['cluster_id'] != cluster_id:
        #         logger.error(f"This node is part of another cluster: {node_info['cluster_id']}")
        #         return False
        ip_iface = utils.get_mgmt_ip(node_info, iface_name)
        mgmt_ip = ip_iface[0] if ip_iface else None

        # A host's failure domain is immutable for the lifetime of its node
        # records: re-adding / adding another slot with a different domain id
        # would be an in-place FD migration, which is not supported (remove
        # the node, restore balance, then re-add it in the target domain).
        if cluster.enable_failure_domain and mgmt_ip:
            for _n in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
                if (_n.status != StorageNode.STATUS_REMOVED
                        and _n.mgmt_ip == mgmt_ip
                        and _n.failure_domain >= 0
                        and _n.failure_domain != failure_domain_id):
                    logger.error(
                        f"Host {mgmt_ip} already belongs to failure domain "
                        f"{_n.failure_domain} (node {_n.get_id()}); adding it "
                        f"with --failure-domain {failure_domain_id} would "
                        f"migrate it between domains, which is not supported. "
                        f"Remove the node first, restore balance, then re-add "
                        f"it in the target domain.")
                    return False

        cloud_instance = node_info['cloud_instance']
        if not cloud_instance:
            # Create a static cloud instance from node info
            cloud_instance = {"id": node_info['system_id'], "type": "None", "cloud": "None",
                              "ip": mgmt_ip,
                              "public_ip": mgmt_ip}
        """"
         "cloud_instance": {
              "id": "565979732541",
              "type": "m6id.large",
              "cloud": "google",
              "ip": "10.10.10.10",
              "public_ip": "20.20.20.20",
        }
        """""
        logger.debug(utils.dump_json(cloud_instance, indent=2))
        logger.info(f"Instance id: {cloud_instance['id']}")
        logger.info(f"Instance cloud: {cloud_instance['cloud']}")
        logger.info(f"Instance type: {cloud_instance['type']}")
        logger.info(f"Instance privateIp: {cloud_instance['ip']}")
        logger.info(f"Instance public_ip: {cloud_instance['public_ip']}")

        alceml_cpu_index = 0
        alceml_worker_cpu_index = 0
        distrib_cpu_index = 0
        jc_singleton_mask = ""
        compression_cpu_mask = ""
        compression_core = None

        req_cpu_count = len(node_config.get("isolated"))

        if req_cpu_count >= 64:
            logger.error(
                f"ERROR: The provided cpu mask {req_cpu_count} has values greater than 63, which is not allowed")
            return False

        # Calculate pool count. The huge-page floor is the cluster's
        # hugepages_mem; a max_size left in an older host config is still
        # honoured so a node configured before this change keeps working.
        max_prov = cluster_hp_mem
        if not max_prov and node_config.get("max_size"):
            max_prov = int(utils.parse_size(node_config.get("max_size")))
        if max_prov < 0:
            logger.error(f"Incorrect huge-page floor value {max_prov}")
            return False

        # sn configure sized huge_page_memory for the worst case (product-
        # ceiling max_lvol, its own default core-count heuristic); recompute
        # it against the real max_lvol/req_cpu_count now that both reflect
        # the cluster, and persist only if that actually changes anything.
        minimum_hp_memory = apply_cluster_hugepages(snode_api, node_config, req_cpu_count, max_prov)
        if minimum_hp_memory is None:
            return False

        # check for memory
        if "memory_details" in node_info and node_info['memory_details']:
            memory_details = node_info['memory_details']
            logger.info("Node Memory info")
            logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
            logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
            logger.info(f"huge_total: {utils.humanbytes(memory_details['huge_total'])}")
            logger.info(f"huge_free: {utils.humanbytes(memory_details['huge_free'])}")
            logger.info(f"Set huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
        else:
            logger.error("Cannot get memory info from the instance.. Exiting")
            return False

        # Calculate minimum sys memory
        if spdk_sys_mem:
            minimum_sys_memory = int(utils.parse_size(spdk_sys_mem))
        else:
            minimum_sys_memory = node_config.get("sys_memory")
        max_lvol = node_config.get("max_lvol")
        # Clamp rather than fail: a config file generated before the cap was
        # enforced (or hand-edited) can carry a larger value, and refusing the
        # add would strand hosts that are otherwise fine. The node record then
        # states the limit that actually applies at placement time.
        if max_lvol and int(max_lvol) > constants.MAX_SUBSYSTEMS_PER_NODE:
            logger.warning(
                f"max_lvol {max_lvol} from the node config exceeds the maximum of "
                f"{constants.MAX_SUBSYSTEMS_PER_NODE} subsystems per storage node; "
                f"using {constants.MAX_SUBSYSTEMS_PER_NODE}")
            max_lvol = constants.MAX_SUBSYSTEMS_PER_NODE

        # minimum_hp_memory is the real, cluster-aware figure now (via
        # apply_cluster_hugepages above), not sn configure's worst-case
        # estimate -- so unlike that estimate, a shortfall against it is a
        # genuine one and belongs here, not a warning.
        satisfied, _ = utils.calculate_spdk_memory(
            minimum_hp_memory, minimum_sys_memory,
            memory_details['free'], memory_details['huge_total'])
        if not satisfied:
            logger.error(
                "Not enough memory on %s for max_lvol=%s, %s SPDK vCPU(s): need %s "
                "huge-page + %s system memory, have %s free + %s huge-page. Lower "
                "the cluster's max-subsys/vcpu-count or use a host with more memory.",
                node_addr, max_lvol, req_cpu_count,
                utils.humanbytes(minimum_hp_memory), utils.humanbytes(minimum_sys_memory),
                utils.humanbytes(memory_details['free']), utils.humanbytes(memory_details['huge_total']))
            return False

        ssd_pcie = node_config.get("ssd_pcis")

        if ssd_pcie:
            action, existing = _classify_existing_endpoint_record(
                db_controller, cluster_id, node_addr, ssd_pcie)
            if action == "cleanup":
                # Repeated partial attempts can leave several stale records
                # for the same endpoint; we clean one per task retry.
                logger.warning(
                    f"Node {existing.get_id()} is in_creation status with endpoint "
                    f"{node_addr}, removing and deleting it")
                # Kill the SPDK pod first and only drop the record once it's
                # confirmed dead — a failed kill followed by record removal
                # orphans the pod (holds CPU/hugepages, blocks the retry).
                # Retry; keep the record and fail the add if it won't die.
                # _kill_spdk_until_dead independently polls spdk_process_is_up
                # rather than trusting spdk_process_kill's own return value —
                # that endpoint could report success on a delete that silently
                # failed or timed out, which is exactly how a pod outlived its
                # own DB record and starved every later add on the same host
                # (worker-3, 2026-07-28).
                if not _kill_spdk_until_dead(existing):
                    logger.error(
                        f"Could not kill SPDK for stale in_creation node {existing.get_id()}; "
                        f"keeping its DB record to avoid orphaning the pod — failing add for retry")
                    return False
                storage_events.snode_delete(existing)
                existing.remove(db_controller.kv_store)
                return False
            elif action == "already_added":
                # This is one of possibly several (nodes_per_socket * sockets)
                # slots being added for this host in the SAME call — only THIS
                # entry is already provisioned. Move on to the remaining
                # entries instead of reporting the whole host done: an early
                # `return` here silently skipped every other socket/slot on
                # a fresh host and left them forever unadded (only the first
                # config entry, whichever it happened to already own, was ever
                # checked — 2026-07-22, all-4-pods-on-numa-0 incident).
                logger.info(
                    f"Node {existing.get_id()} with endpoint {node_addr} is already "
                    f"added and online (owns the same SSDs); skipping this slot "
                    f"and checking the rest")
                continue
            elif action == "conflict":
                logger.error(
                    f"A node record with endpoint {node_addr} already exists in "
                    f"status {existing.status} and owns the same SSDs "
                    f"(node: {existing.get_id()}); delete or restart it instead "
                    f"of re-adding")
                return False

        # Expansion pre-flight (cluster-wide half): the node add itself must
        # not start unless the cluster is ACTIVE, all nodes are ONLINE and no
        # migration / restart / backup task is open. The full check —
        # including deletes on the impacted donor nodes, which are only known
        # once the role moves are planned — reruns in
        # integrate_new_node_into_cluster before the rebalance executes.
        # This deliberately runs AFTER the stale-record cleanup above: a
        # ghost in_creation record left by a died prior attempt of THIS
        # endpoint would otherwise fail the all-nodes-online condition and
        # permanently block its own retry (2026-07-17, node f6308adb).
        if expansion:
            from simplyblock_core.controllers.cluster_expansion.preconditions import (
                check_expansion_preconditions, check_fd_admission_for_add)
            ok, reason = check_expansion_preconditions(cluster, db_controller)
            if not ok:
                logger.error(f"Cannot start expansion node-add: {reason}")
                return False
            # Failure-domain admission (+/-1 rule): the post-add per-domain
            # host split must stay balanced within one host. Re-checked with
            # the newcomer in the DB by integrate_new_node_into_cluster.
            ok, reason = check_fd_admission_for_add(
                cluster, db_controller, failure_domain_id,
                new_mgmt_ip=mgmt_ip)
            if not ok:
                logger.error(f"Cannot start expansion node-add: {reason}")
                return False

        fdb_connection = cluster.db_connection

        if cluster.mode == "docker":
            logger.info("Joining docker swarm...")
            cluster_docker = utils.get_docker_client(cluster_id)
            cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]
            results, err = snode_api.join_swarm(
                cluster_ip=cluster_ip,
                join_token=cluster_docker.swarm.attrs['JoinTokens']['Worker'],
                db_connection=cluster.db_connection,
                cluster_id=cluster_id)

            if not results:
                logger.error(f"Failed to Join docker swarm: {err}")
                return False
        else:
            cluster_ip = utils.get_k8s_node_ip()

        rpc_user, rpc_pass = utils.generate_rpc_user_and_pass()
        mgmt_info = utils.get_mgmt_ip(node_info, iface_name)
        if not mgmt_info:
            logger.error(f"No management interface with IP found in provided interfaces: {iface_name}")
            return False

        mgmt_ip, mgmt_iface = mgmt_info
        # Generate the node uuid up front so it can own the port reservations
        # made before the node record itself is persisted (the new node isn't
        # written to the DB until after SPDK boots, so two concurrent adds would
        # otherwise read the same "next free" port). reserve_cluster_nvmf_port
        # allocates and reserves the port atomically against other concurrent
        # node adds.
        node_uuid = str(uuid.uuid4())
        rpc_port = db_controller.reserve_cluster_nvmf_port(cluster_id, node_uuid)
        logger.info(f"mgmt interface is {mgmt_iface}")

        if not spdk_image:
            spdk_image = constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE

        if cluster.mode == "docker":
            log_config_type = utils.get_storage_node_api_log_type(mgmt_ip, '/SNodeAPI')
            if log_config_type and log_config_type != LogConfig.types.GELF:
                logger.info("SNodeAPI container found but not configured with gelf logger")
                start_storage_node_api_container(mgmt_ip, cluster_ip)
        node_socket = node_config.get("socket")

        # Idempotent re-entry. A prior add attempt for this (node, socket) may
        # have been interrupted — most commonly because the node rebooted for
        # the CPU-topology change and killed the agent serving the add, or
        # because the tasks-runner pod driving the add was itself co-located on
        # a rebooting storage node and died mid-flight. That leaves a
        # StorageNode stuck in IN_CREATION. The lease-based takeover re-runs the
        # add, so this must start from a clean slate: kill any half-started
        # SPDK and drop the stale record. Without this the retry builds a
        # DUPLICATE node (observed 2026-07-06) and the total_mem loop below
        # double-counts the orphan's hugepages. This matches on
        # api_endpoint+socket — unlike the SSD-overlap cleanup above, which
        # misses an attempt interrupted before SSD assignment (empty ssd_pcie).
        # A storage node's identity is (api_endpoint, socket, cpu_mask): with
        # nodesPerSocket>1 several nodes share the same host AND socket and are
        # distinguished only by their CPU core group (cpu_mask), so matching on
        # socket alone would wrongly conflate legitimate co-socket siblings. The
        # record is persisted only after spdk_cpu_mask is set (below), so a
        # persisted IN_CREATION orphan always carries its mask and a
        # deterministic config re-gen reproduces the same value on retry.
        incoming_cpu_mask = node_config.get("cpu_mask")
        existing_healthy = None
        for n in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
            if not (n.api_endpoint == node_addr and n.socket == node_socket
                    and n.spdk_cpu_mask == incoming_cpu_mask):
                continue
            if n.status == StorageNode.STATUS_IN_CREATION:
                logger.warning(
                    f"Found stale IN_CREATION node {n.get_id()} for {node_addr} "
                    f"socket {node_socket} from an interrupted add; cleaning up before retry")
                # Invariant: a running SPDK pod MUST keep its DB record. Kill the
                # SPDK pod FIRST and only drop the record once it's confirmed
                # dead. Previously the kill was best-effort and the record was
                # removed regardless, so a kill that failed (agent briefly
                # unreachable during the topology reboot) left the pod ORPHANED —
                # holding the node's CPU/hugepages and blocking the retry's SPDK
                # pod (Pending, node stuck in in_creation; worker-3, 2026-07-13).
                # Retry the kill; if it never succeeds, keep the record and fail
                # the add so a later attempt cleans the pair rather than orphaning.
                # _kill_spdk_until_dead independently polls spdk_process_is_up
                # rather than trusting spdk_process_kill's own return value —
                # that endpoint could report success on a delete that silently
                # failed or timed out.
                if not _kill_spdk_until_dead(n):
                    logger.error(
                        f"Could not kill SPDK for stale in_creation node {n.get_id()}; "
                        f"keeping its DB record to avoid orphaning the pod — failing add for retry")
                    return False
                try:
                    storage_events.snode_delete(n)
                except Exception:
                    logger.warning("snode_delete event failed for stale node", exc_info=True)
                n.remove(db_controller.kv_store)
            else:
                # A healthy node already occupies this (host, socket) slot, so this
                # add is a redundant DUPLICATE — e.g. the operator re-posted the add
                # after its pod was recreated (a CPU-topology reboot evicted it), or
                # an interrupted add whose first attempt has since gone ONLINE. Only
                # one storage node exists per (api_endpoint, socket, cpu_mask) — a
                # co-socket sibling (nodesPerSocket>1) has a different mask and does
                # not match here, so it is never mistaken for a duplicate; the earlier
                # guard above cleans up a prior attempt still stuck IN_CREATION, but
                # once that attempt is ONLINE it no longer matches, so a second add
                # would build a node that can never come up (the socket's cores and
                # hugepages are already owned) and strand it IN_CREATION (observed on
                # worker-1, 2026-07-10). Record it and skip to the next slot as an
                # idempotent no-op for THIS entry only.
                existing_healthy = n
        if existing_healthy is not None:
            logger.warning(
                f"Storage node {existing_healthy.get_id()} already present for {node_addr} "
                f"socket {node_socket} (status {existing_healthy.status}); skipping duplicate add "
                f"for this slot and checking the rest")
            continue

        total_mem = minimum_hp_memory
        for n in db_controller.get_storage_nodes_by_cluster_id(cluster_id):
            if n.api_endpoint == node_addr and n.socket == node_socket:
                total_mem += (n.spdk_mem + 500000000)

        logger.info("Deploying SPDK")
        results = None
        l_cores = node_config.get("l-cores")
        spdk_cpu_mask = node_config.get("cpu_mask")
        for ssd in ssd_pcie:
            if format_4k:
                snode_api.format_device_with_4k(ssd)
                snode_api.bind_device_to_spdk(ssd)
            snode_api.bind_device_to_spdk(ssd)

        if not spdk_proxy_image:
            spdk_proxy_image = cluster.container_image_prefix + constants.SIMPLY_BLOCK_DOCKER_IMAGE
        # Initial storage-MCP maxUnavailable for the first-time CPU-topology
        # reboots = the configured parallel-add count (StorageNodeSet
        # spec.maxParallelNodeAdds), read straight from the CR. cluster_activate
        # later narrows the pool to the cluster's fault tolerance.
        mcp_max_unavailable = utils.get_max_parallel_node_adds_from_cr(
            cr_name, cr_namespace, cr_plural)
        try:
            results, err = snode_api.spdk_process_start(
                l_cores, minimum_hp_memory, spdk_image, spdk_debug, cluster_ip, fdb_connection,
                namespace, mgmt_ip, rpc_port, rpc_user, rpc_pass,
                multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED,
                timeout=constants.SPDK_PROXY_TIMEOUT,
                ssd_pcie=ssd_pcie, total_mem=total_mem, system_mem=minimum_sys_memory, cluster_mode=cluster.mode,
                socket=node_socket, cluster_id=cluster_id, spdk_proxy_image=spdk_proxy_image,
                mcp_max_unavailable=mcp_max_unavailable)
            time.sleep(5)

        except Exception as e:
            logger.error(e)
            return False

        if not results:
            logger.error(f"Failed to start spdk: {err}")
            return False
        number_of_alceml_devices = node_config.get("number_of_alcemls")
        # Increase number of alcemls by one for the JM
        number_of_alceml_devices += 1
        small_pool_count = node_config.get("small_pool_count")
        large_pool_count = node_config.get("large_pool_count")

        cores, _ = snode_api.read_allowed_list()

        if len(cores) == req_cpu_count:
            new_distribution, _ = snode_api.recalculate_cores_distribution(cores, number_of_alceml_devices)
            poller_cpu_cores = new_distribution.get("poller_cpu_cores")
            alceml_cpu_cores = new_distribution.get("alceml_cpu_cores")
            distrib_cpu_cores = new_distribution.get("distrib_cpu_cores")
            alceml_worker_cpu_cores = new_distribution.get("alceml_worker_cpu_cores")
            jc_singleton_core = new_distribution.get("jc_singleton_core")
            app_thread_core = new_distribution.get("app_thread_core")
            jm_cpu_core = new_distribution.get("jm_cpu_core")
            lvol_poller_core = new_distribution.get("lvol_poller_core")
            lvol_poller_mask = utils.generate_mask(lvol_poller_core)
            compression_core = new_distribution.get("compression_core")
        else:
            poller_cpu_cores = node_config.get("distribution").get("poller_cpu_cores")
            alceml_cpu_cores = node_config.get("distribution").get("alceml_cpu_cores")
            distrib_cpu_cores = node_config.get("distribution").get("distrib_cpu_cores")
            alceml_worker_cpu_cores = node_config.get("distribution").get("alceml_worker_cpu_cores")
            jc_singleton_core = node_config.get("distribution").get("jc_singleton_core")
            app_thread_core = node_config.get("distribution").get("app_thread_core")
            jm_cpu_core = node_config.get("distribution").get("jm_cpu_core")
            lvol_poller_core =  node_config.get("distribution").get("lvol_poller_core")
            lvol_poller_mask = utils.generate_mask(lvol_poller_core)
            compression_core = node_config.get("distribution").get("compression_core")

        number_of_distribs = node_config.get("number_of_distribs")

        pollers_mask = utils.generate_mask(poller_cpu_cores)
        app_thread_mask = utils.generate_mask(app_thread_core)

        if jc_singleton_core:
            jc_singleton_mask = utils.decimal_to_hex_power_of_2(jc_singleton_core[0])
        if compression_core:
            compression_cpu_mask = utils.generate_mask(compression_core)
        jm_cpu_mask = utils.generate_mask(jm_cpu_core)


        data_nics = []

        active_tcp = False
        active_rdma = False
        fabric_tcp = cluster.fabric_tcp
        fabric_rdma = cluster.fabric_rdma
        names = data_nics_list or [mgmt_iface]
        logger.info(f"fabric_tcp is {fabric_tcp}")
        logger.info(f"fabric_rdma is {fabric_rdma}")
        logger.debug(f"Data nics ports are: {names}")
        for nic in names:
            device = node_info['network_interface'][nic]
            base_ifc_cfg = {
                'uuid': str(uuid.uuid4()),
                'if_name': nic,
                'ip4_address': device['ip'],
                'status': device['status'],
                'net_type': device['net_type'], }
            if fabric_rdma and snode_api.ifc_is_roce(nic):
                cfg = base_ifc_cfg.copy()
                cfg['trtype'] = "RDMA"
                data_nics.append(IFace(cfg))
                active_rdma = True
                if fabric_tcp and snode_api.ifc_is_tcp(nic):
                    active_tcp = True
            elif fabric_tcp and snode_api.ifc_is_tcp(nic):
                cfg = base_ifc_cfg.copy()
                cfg['trtype'] = "TCP"
                data_nics.append(IFace(cfg))
                active_tcp = True

        if not active_tcp and not active_rdma:
            logger.error("No usable storage network interface found.")
            return False

        hostname = node_info['hostname'] + f"_{rpc_port}"
        BASE_NQN = cluster.nqn.split(":")[0]
        subsystem_nqn = f"{BASE_NQN}:{hostname}"
        # creating storage node object
        snode = StorageNode()
        snode.uuid = node_uuid
        snode.status = StorageNode.STATUS_IN_CREATION
        snode.baseboard_sn = node_info['system_id']
        snode.system_uuid = node_info['system_id']
        snode.create_dt = str(datetime.datetime.now())

        snode.cloud_instance_id = cloud_instance['id']
        snode.cloud_instance_type = cloud_instance['type']
        snode.cloud_instance_public_ip = cloud_instance['public_ip']
        snode.cloud_name = cloud_instance['cloud'] or ""

        snode.namespace = namespace
        snode.cr_name = cr_name
        snode.cr_namespace = cr_namespace
        snode.cr_plural = cr_plural
        snode.ssd_pcie = ssd_pcie
        snode.hostname = hostname
        snode.host_nqn = subsystem_nqn
        snode.subsystem = subsystem_nqn
        snode.data_nics = data_nics
        snode.mgmt_ip = mgmt_ip
        snode.primary_ip = mgmt_ip
        snode.rpc_port = rpc_port
        snode.rpc_username = rpc_user
        snode.rpc_password = rpc_pass
        snode.cluster_id = cluster_id
        snode.api_endpoint = node_addr
        snode.host_secret = SecretStr(utils.generate_string(20))
        snode.ctrl_secret = SecretStr(utils.generate_string(20))
        snode.number_of_distribs = number_of_distribs
        snode.number_of_alceml_devices = number_of_alceml_devices
        snode.enable_ha_jm = enable_ha_jm
        snode.ha_jm_count = ha_jm_count
        snode.minimum_sys_memory = minimum_sys_memory
        snode.active_tcp = active_tcp
        snode.active_rdma = active_rdma
        snode.spdk_proxy_image = spdk_proxy_image
        snode.spdk_version = spdk_proxy_image.split(":")[1]

        if 'cpu_count' in node_info:
            snode.cpu = node_info['cpu_count']
        if 'cpu_hz' in node_info:
            snode.cpu_hz = node_info['cpu_hz']
        if 'memory' in node_info:
            snode.memory = node_info['memory']
        if 'hugepages' in node_info:
            snode.hugepages = node_info['hugepages']

        snode.l_cores = l_cores or ""
        snode.spdk_cpu_mask = spdk_cpu_mask or ""
        snode.spdk_mem = minimum_hp_memory
        snode.max_lvol = max_lvol
        snode.max_snap = max_snap
        snode.max_prov = max_prov
        snode.spdk_image = spdk_image or ""
        snode.spdk_debug = spdk_debug or False
        snode.write_to_db(kv_store)
        snode.app_thread_mask = app_thread_mask or ""
        snode.pollers_mask = pollers_mask or ""
        snode.lvol_poller_mask = lvol_poller_mask or ""
        snode.jm_cpu_mask = jm_cpu_mask
        snode.alceml_cpu_index = alceml_cpu_index
        snode.alceml_worker_cpu_index = alceml_worker_cpu_index
        snode.distrib_cpu_index = distrib_cpu_index
        snode.alceml_cpu_cores = alceml_cpu_cores
        snode.alceml_worker_cpu_cores = alceml_worker_cpu_cores
        snode.distrib_cpu_cores = distrib_cpu_cores
        snode.jc_singleton_mask = jc_singleton_mask or ""
        snode.compression_cpu_mask = compression_cpu_mask or ""
        snode.nvmf_port = db_controller.reserve_cluster_nvmf_port(cluster_id, node_uuid)
        snode.poller_cpu_cores = poller_cpu_cores or []
        snode.socket = node_socket
        snode.iobuf_small_pool_count = small_pool_count or 0
        snode.iobuf_large_pool_count = large_pool_count or 0
        snode.iobuf_small_bufsize = small_bufsize or 0
        snode.iobuf_large_bufsize = large_bufsize or 0
        snode.enable_test_device = enable_test_device

        if cluster.is_single_node:
            snode.physical_label = 0
        else:
            snode.physical_label = get_next_physical_device_order(snode)

        snode.failure_domain = failure_domain_id

        snode.num_partitions_per_dev = num_partitions_per_dev
        snode.jm_percent = jm_percent
        snode.id_device_by_nqn = id_device_by_nqn

        if partition_size:
            snode.partition_size = utils.parse_size(partition_size)

        rpc_client = snode.rpc_client(timeout=3 * 60, retry=10)

        # 1- set iobuf options
        try:
            if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
                    snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
                ret = rpc_client.iobuf_set_options(
                    snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
                    snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
                if not ret:
                    logger.error("Failed to set iobuf options")
                    return False
            rpc_client.bdev_set_options(0, 0, 0, 0)
            rpc_client.accel_set_options()
        except Exception as e:
            # First contact with the just-created SPDK pod (write_to_db above
            # persisted the in_creation record). If the pod never comes up —
            # most commonly stuck Pending on a host without enough free
            # CPU/hugepages/memory — left alone it strands here: both the pod
            # and this record would otherwise sit untouched for a full retry
            # cycle (the next add-node attempt's stale-record cleanup, ~9 min
            # later) while the Pending pod contributes nothing but occupies a
            # scheduling slot. Worse, if that later cleanup's kill call ever
            # fails silently, the pod outlives even that and permanently
            # starves every future add on the same host (worker-3,
            # 2026-07-28). Clean up immediately instead of waiting.
            logger.error(f"Storage node {snode.get_id()} did not come up after creation: {e}")
            # _kill_spdk_until_dead independently polls spdk_process_is_up
            # rather than trusting spdk_process_kill's own return value.
            if _kill_spdk_until_dead(snode):
                try:
                    storage_events.snode_delete(snode)
                except Exception:
                    logger.warning("snode_delete event failed for unreachable node", exc_info=True)
                snode.remove(db_controller.kv_store)
            else:
                logger.error(
                    f"Could not kill unreachable SPDK pod for {snode.get_id()}; "
                    f"keeping its DB record to avoid orphaning the pod")
            return False

        snode.write_to_db(kv_store)

        ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
        if not ret:
            logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")

        # 2- set socket implementation options
        bind_to_device = None
        if snode.data_nics and len(snode.data_nics) == 1:
            bind_to_device = snode.data_nics[0].if_name
        ret = rpc_client.sock_impl_set_options(bind_to_device)
        if not ret:
            logger.error("Failed to set optimized socket options")
            return False

        # 3- set nvme config
        if snode.pollers_mask:
            ret = rpc_client.nvmf_set_config(
                snode.pollers_mask,
                dhchap_digests=constants.DHCHAP_DIGESTS,
                dhchap_dhgroups=[constants.DHCHAP_DHGROUP],
            )
            if not ret:
                logger.error("Failed to set pollers mask")
                return False

        # 4- start spdk framework
        ret = rpc_client.framework_start_init()
        if not ret:
            logger.error("Failed to start framework")
            return False

        rpc_client.log_set_print_level("DEBUG")

        # The lvstore-create poller group is created exactly ONCE per SPDK
        # process lifetime: here, right after framework init (add-node; the
        # restart path has the same call). Nothing else may call
        # bdev_lvol_create_poller_group later — a second call with a different
        # mask (the old create_s3_bdev path did this with app_thread_mask)
        # either fails or lands the pollers on the wrong core.
        #
        # lvol_poller_mask is the single source of truth for which core this
        # runs on: calculate_core_allocations() already colocates
        # lvol_poller_core with jc_singleton_core unless the config gives it
        # its own dedicated core (>=32 vCPU, compression-thread layout), so
        # using it directly honors both cases correctly. Do NOT override it
        # with jc_singleton_mask unconditionally here — that was tried
        # (e3e8fd08) before lvol_poller_core was aligned with jc_singleton_core
        # at the source, and it silently clobbered the dedicated-core case,
        # forcing the poller group onto JC's core even when the config
        # deliberately gave it a separate one. jc_singleton_mask is only a
        # last-resort fallback for the pathological case where
        # lvol_poller_core's own reservation came up empty (cores exhausted)
        # while a JC core still exists — this RPC must still run on some core.
        poller_group_mask = snode.lvol_poller_mask or snode.jc_singleton_mask
        if poller_group_mask:
            try:
                rpc_client.bdev_lvol_create_poller_group(poller_group_mask)
            except RPCException:
                logger.error("Failed to set pollers mask")
                return False

        # 5- set app_thread cpu mask
        if snode.app_thread_mask:
            ret = rpc_client.thread_get_stats()
            app_thread_process_id = 0
            if ret.get("threads"):
                for entry in ret["threads"]:
                    if entry['name'] == 'app_thread':
                        app_thread_process_id = entry['id']
                        break

            ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
            if not ret:
                logger.error("Failed to set app thread mask")
                return False

        # 6- set nvme bdev options
        # bdev_nvme_set_options is a pure local SPDK config call; bound it at
        # 5 s so a stuck proxy can't consume the 3 min startup RPC budget.
        set_opts_rpc = snode.rpc_client(timeout=5, retry=0)
        ret = set_opts_rpc.bdev_nvme_set_options()
        if not ret:
            logger.error("Failed to set nvme options")
            return False

        qpair = cluster.qpair_count

        if not cluster.fabric_tcp and not cluster.fabric_rdma:
            logger.error("no active fabric")
            return False

        if cluster.fabric_tcp:
            ret = rpc_client.transport_create("TCP", qpair, 512 * (req_cpu_count + 1))
            if not ret:
                logger.error(f"Failed to create transport TCP with qpair: {qpair}")
                return False
        if cluster.fabric_rdma:
            ret = rpc_client.transport_create("RDMA", qpair, 512 * (req_cpu_count + 1))
            if not ret:
                logger.error(f"Failed to create transport RDMA with qpair: {qpair}")
                return False

        # 7- set jc singleton mask
        if snode.jc_singleton_mask:
            ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
            if not ret:
                logger.error("Failed to set jc singleton mask")
                return False

        # get new node info after starting spdk
        # node_info, _ = snode_api.info()

        # if not snode.ssd_pcie:
        #     snode = db_controller.get_storage_node_by_id(snode.get_id())
        #     snode.ssd_pcie = node_info['spdk_pcie_list']
        #     snode.write_to_db()
        # discover devices
        if not snode.ssd_pcie:
            node_info, _ = snode_api.info()
            ssds = node_info['spdk_pcie_list']
        else:
            ssds = snode.ssd_pcie

        nvme_devs = addNvmeDevices(rpc_client, snode, ssds)
        if nvme_devs:

            for nvme in nvme_devs:
                nvme.status = NVMeDevice.STATUS_ONLINE

            # prepare devices
            if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

                jm_device = nvme_devs[0]
                for index, nvme in enumerate(nvme_devs):
                    if nvme.size < jm_device.size:
                        jm_device = nvme
                jm_device.status = NVMeDevice.STATUS_JM

                ret = _prepare_cluster_devices_jm_on_dev(snode, nvme_devs)
            else:
                ret = _prepare_cluster_devices_partitions(snode, nvme_devs)
            if not ret:
                logger.error("Failed to prepare cluster devices")
                return False

        # set qos values if enabled
        if cluster.is_qos_set():
            logger.info("Setting Alcemls QOS weights")
            ret = rpc_client.alceml_set_qos_weights(qos_controller.get_qos_weights_list(cluster_id))
            if not ret:
                logger.error("Failed to set Alcemls QOS")
                return False

        # --- Cluster-wide mesh critical section -------------------------
        # Everything below wires this node into the cluster mesh: it connects
        # to peers' remote devices, goes ONLINE, makes every peer reverse-
        # connect to *this* node's devices (writing the peers' records), and
        # pushes the cluster map. Two concurrent adds must not interleave here:
        # the reverse-connect loop does full-object writes of *other* nodes,
        # and a correct A<->B mesh requires whoever runs second to observe the
        # first as ONLINE. So this whole block is serialized per cluster while
        # the slow node-local setup above ran in parallel. A heartbeat keeps a
        # long section on a large cluster from being reclaimed; the lock is
        # always released (finally), including on the early `continue` and the
        # reverse-connect failure path.
        lock_owner = f"{socket.gethostname()}:{os.getpid()}:{node_uuid}"
        if not _acquire_cluster_add_lock_blocking(
                db_controller, cluster_id, lock_owner,
                timeout=constants.CLUSTER_ADD_LOCK_WAIT_TIMEOUT_SEC):
            # Nothing keeps driving this registration after the failure, but
            # the record written above stays in_creation — retries and
            # watchers read that as a live in-flight add (2026-07-16 perf
            # deploy: 20-minute ghost waits per retry). Tear down the same
            # way the stale-record path does: kill this node's SPDK and drop
            # the record so a retry starts from a clean slate.
            logger.error("Could not acquire cluster node-add lock; failing for retry")
            try:
                snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
            except Exception:
                logger.warning(
                    "Failed to kill SPDK process after node-add lock timeout", exc_info=True)
            storage_events.snode_delete(snode)
            snode.remove(db_controller.kv_store)
            return False
        stop_heartbeat = threading.Event()
        hb_thread = threading.Thread(
            target=_cluster_add_lock_heartbeat,
            args=(db_controller, cluster_id, lock_owner, stop_heartbeat),
            daemon=True)
        hb_thread.start()
        try:
            # Assign the cluster-wide device ordering under the lock. Both
            # physical_label and cluster_device_order are sequential cluster-
            # wide counters (get_next_physical_device_order /
            # get_next_cluster_device_order are read-max-then-+1 over all
            # nodes). Computed in the parallel node-local section — as they were
            # via addNvmeDevices() and _prepare_cluster_devices_*() — concurrent
            # adds read the same "next free" value and collide, producing
            # DUPLICATE ids / physical labels in the distr cluster map, which
            # makes bdev_lvol_create_lvstore fail with "Input/output error" at
            # activation. Recompute them here: the lock serializes adds and this
            # node's devices are persisted (snode.write_to_db below) before the
            # lock is released, so the next add sees them and picks the next
            # free values. The provisional values assigned earlier are
            # overwritten here before they are ever persisted.
            snode.physical_label = 0 if cluster.is_single_node else get_next_physical_device_order(
                snode, exclude_node_id=snode.get_id())
            dev_order = get_next_cluster_device_order(db_controller, snode.cluster_id)
            for dev in snode.nvme_devices:
                dev.physical_label = snode.physical_label
                if dev.status == NVMeDevice.STATUS_ONLINE:
                    dev.cluster_device_order = dev_order
                    dev_order += 1

            logger.info("Connecting to remote devices")
            remote_devices = _connect_to_remote_devs(snode)
            snode.remote_devices = remote_devices

            if snode.enable_ha_jm:
                logger.info("Connecting to remote JMs")
                snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)

            snode.write_to_db(kv_store)

            # Route the IN_CREATION -> ONLINE transition through set_node_status
            # rather than a raw status write. set_node_status enforces the
            # _ALLOWED_PRE_STATUSES_FOR_ONLINE guard (OFFLINE -> ONLINE is rejected),
            # so a concurrent/stale path can no longer clobber a freshly-detected
            # OFFLINE back to ONLINE through this code -- the raw write here was the
            # node-side stale re-online hole (incident 2026-06-24: node re-marked
            # online seconds after the monitor downed it, undoing the OFFLINE and
            # forcing a duplicate offline/auto-restart cycle). set_node_status also
            # emits the status event, broadcasts to peers, and cancels stale
            # auto-restart tasks -- all previously done by hand here.
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            if not set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="monitor"):
                logger.error(
                    f"Failed to bring node {snode.get_id()} ONLINE "
                    f"(illegal transition from {snode.status})")
                return False

            logger.info("Make other nodes connect to the node devices")
            snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
            for node in snodes:
                if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                    continue
                try:
                    node.remote_devices = _connect_to_remote_devs(node)
                except RuntimeError:
                    logger.error('Failed to connect to remote devices')
                    return False
                node.write_to_db(kv_store)

            if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY,
                                      Cluster.STATUS_IN_EXPANSION]:
                logger.warning(
                    f"The cluster status is not active ({cluster.status}), adding the node without distribs and lvstore")
                continue

            logger.info("Sending cluster map add node")
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
            for node_index, node in enumerate(snodes):
                if node.status != StorageNode.STATUS_ONLINE or node.get_id() == snode.get_id():
                    continue
                ret = distr_controller.send_cluster_map_add_node(snode, node)

            # for dev in snode.nvme_devices:
            #     if dev.status == NVMeDevice.STATUS_ONLINE:
            #         device_controller.device_set_unavailable(dev.get_id())

            # logger.info("Setting node status to suspended")
            # set_node_status(snode.get_id(), StorageNode.STATUS_SUSPENDED)
            # logger.info("Done")

            logger.info("Setting node status to Active")
            set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="add_node")

            # In --expansion mode the expand-task runner triggers expansion
            # migration explicitly *after* integrate_new_node_into_cluster has
            # built the post-rotation lvstore_stack and flipped cluster status
            # back to ACTIVE. Skipping it here avoids racing the half-built
            # rotation and double-queueing.
            if not expansion:
                for dev in snode.nvme_devices:
                    if dev.status == NVMeDevice.STATUS_ONLINE:
                        tasks_controller.add_new_device_mig_task(dev.get_id())
            else:
                # Queue the integration HERE so every entry point gets it —
                # CLI, web API and the k8s node-add task runner all funnel
                # through add_node, but only clibase used to queue the
                # cluster-expand task, so CRD-driven adds completed without
                # the rebalance ever starting (2026-07-17, vm15).
                expand_task_id = tasks_controller.add_cluster_expand_task(
                    cluster.get_id(), snode.get_id())
                if expand_task_id:
                    logger.info(
                        f"expansion: queued cluster-expand task "
                        f"{expand_task_id} for {snode.get_id()}")
                else:
                    logger.warning(
                        f"expansion: a cluster-expand task is already open "
                        f"for this cluster; node {snode.get_id()} will NOT "
                        f"be integrated by it — re-add it after the current "
                        f"expansion completes")

            storage_events.snode_add(snode)

            # Legacy (non --expansion) flow only: the follow-up
            # cluster_ops.cluster_expand accepts IN_EXPANSION and flips back
            # to ACTIVE when done. In --expansion mode the status must stay
            # ACTIVE: integrate_new_node_into_cluster's preconditions require
            # it and the executor owns the IN_EXPANSION transition itself —
            # setting it here deadlocks the cluster-expand task ("cluster
            # status is in_expansion, expansion requires active").
            if not expansion:
                cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_IN_EXPANSION)
        finally:
            stop_heartbeat.set()
            db_controller.release_cluster_add_lock(cluster_id, lock_owner)
        # --- End cluster-wide mesh critical section ---------------------
    logger.info("Done")
    return "Success"


def get_number_of_online_devices(cluster_id):
    dev_count = 0
    db_controller = DBController()
    snodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    online_nodes = []
    for node in snodes:
        if node.status == node.STATUS_ONLINE:
            online_nodes.append(node)
            for dev in node.nvme_devices:
                if dev.status == dev.STATUS_ONLINE:
                    dev_count += 1


def delete_storage_node(node_id, force=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_REMOVED:
        logger.error("Node must be in removed status")
        return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        logger.error(f"Tasks found: {len(tasks)}, can not delete storage node, or use --force")
        if not force:
            return False
        for task in tasks:
            tasks_controller.cancel_task(task.uuid)
        time.sleep(1)

    snode.remove(db_controller.kv_store)

    for node in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        logger.info(f"Sending cluster map to node: {node.get_id()}")
        send_cluster_map(node.get_id())

    storage_events.snode_delete(snode)
    logger.info("done")


def remove_storage_node(node_id, force_remove=False, force_migrate=False):
    """Start the online removal of a storage node from its cluster.

    This is the inverse of cluster expansion (add_node). It validates the
    preconditions, then queues a background FN_NODE_REMOVAL task; the
    tasks_runner_node_removal service drives the multi-step, possibly
    multi-hour orchestration (see ``node_removal_orchestrate``):

        shutdown -> in_removal -> rewire LVS replicas -> remove/fail/migrate
        devices -> removed

    Preconditions (all enforced here, before anything is queued):
      * the target node is ONLINE;
      * every other (non-removed) node in the cluster is ONLINE;
      * FTT headroom allows losing this node (``_check_ftt_allows_node_removal``);
      * the node hosts NO LVols and NO snapshots (the operator migrates those
        separately, at a higher level — see the design decision for this
        feature);
      * any secondary/tertiary replica this node hosts for OTHER primaries has
        a valid host-disjoint relocation target.

    ``force_remove`` only bypasses the active-task guard (cancelling them).
    ``force_migrate`` is accepted for signature compatibility and ignored:
    LVol migration is no longer part of node removal.

    Returns the new task uuid on success, or False on a rejected precondition.
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    # Idempotent re-entry: a removal already in flight just returns its task.
    existing = tasks_controller.get_active_node_removal_task(snode.cluster_id, node_id)
    if existing:
        logger.info(f"Node removal already in progress for {node_id} (task {existing})")
        return existing

    if snode.status == StorageNode.STATUS_REMOVED:
        logger.warning(f"Node already removed: {node_id}")
        return False

    if snode.status not in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED,
                            StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_IN_REMOVAL,
                            StorageNode.STATUS_OFFLINE, StorageNode.STATUS_UNREACHABLE]:
        logger.error(
            f"Can not remove node {node_id}: (current status: {snode.status}).")
        return False

    allowed, reason = _check_ftt_allows_node_removal(node_id, db_controller)
    if not allowed:
        logger.error(f"Can not remove node {node_id}: {reason}")
        return False

    lvols = db_controller.get_lvols_by_node_id(node_id)
    if lvols:
        logger.error(
            f"Can not remove node {node_id}: {len(lvols)} LVol(s) present. "
            f"Migrate or delete them first.")
        return False

    node_snaps = [
        sn for sn in db_controller.get_snapshots()
        if sn.lvol.node_id == node_id and sn.deleted is False
    ]
    if node_snaps:
        logger.error(
            f"Can not remove node {node_id}: {len(node_snaps)} snapshot(s) present. "
            f"Remove them first.")
        return False

    tasks = tasks_controller.get_active_node_tasks(snode.cluster_id, snode.get_id())
    if tasks:
        logger.warning(f"Task found: {len(tasks)}, can not remove storage node, or use --force-remove")
        if force_remove is False:
            return False
        for task in tasks:
            tasks_controller.cancel_task(task.uuid)

    # Failure-domain admission: the post-removal per-domain host split must
    # stay within the +/-1 balance rule and keep >=2 hosts per domain.
    # Enforced only once the cluster has an HA layout to protect.
    from simplyblock_core.controllers.cluster_expansion.preconditions import (
        check_fd_admission_for_remove)
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    ok, reason = check_fd_admission_for_remove(cluster, db_controller, snode)
    if not ok:
        logger.error(f"Can not remove node {node_id}: {reason}")
        return False

    # Case-B feasibility: every replica this node hosts for another primary must
    # have somewhere host-disjoint to go. Catches e.g. 2-node clusters where the
    # tertiary cannot be re-placed without violating anti-affinity.
    feasible, reason = _check_replica_relocation_feasible(snode, db_controller)
    if not feasible:
        logger.error(f"Can not remove node {node_id}: {reason}")
        return False

    if snode.status not in [StorageNode.STATUS_PENDING_REMOVAL, StorageNode.STATUS_IN_REMOVAL,
                            StorageNode.STATUS_OFFLINE, StorageNode.STATUS_REMOVED]:
        logger.info(f"[REMOVAL] {node_id}: phase 1 — shutdown")
        ret = shutdown_storage_node(node_id, force=force_remove)
        if isinstance(ret, tuple):
            ret, reason = ret
            if not ret:
                logger.error(f"[REMOVAL] {node_id}: shutdown failed: {reason}")
                return False
        elif not ret:
            logger.error(f"[REMOVAL] {node_id}: shutdown failed")
            return False
        snode = db_controller.get_storage_node_by_id(node_id)

    if snode.status != StorageNode.STATUS_PENDING_REMOVAL:
        set_node_status(node_id, StorageNode.STATUS_PENDING_REMOVAL, caused_by="remove")

    task_id = tasks_controller.add_node_removal_task(
        snode.cluster_id, node_id, {"force_remove": force_remove})
    if not task_id:
        logger.error(f"Failed to queue node-removal task for {node_id}")
        return False
    logger.info(f"Node removal queued for {node_id}: task {task_id}")
    return task_id


def _check_replica_relocation_feasible(removed_node: StorageNode, db_controller):
    """Pre-flight Case-B check: a secondary/tertiary replica hosted on
    ``removed_node`` for some OTHER primary must have a valid relocation target.
    Returns (feasible: bool, reason: str).

    When the global placement planner applies (see
    ``_relocation_planner_inputs``) this asks the same planner phase 3b will
    use, so admission and execution can never disagree: the removal is
    refused only when no host-disjoint layout exists at all, and a layout
    that is merely not fully domain-diverse is admitted with a warning naming
    every LVS that will end up degraded. The per-role probe below stays as
    the fallback for clusters the planner declines."""
    from simplyblock_core.controllers import replica_placement

    inputs = _relocation_planner_inputs(removed_node, db_controller)
    if inputs is not None:
        surviving_ids, fd_by_node, host_by_node, label_by_node, current_layout, ftt = inputs
        try:
            plan = replica_placement.plan_diverse_layout(
                surviving_ids, fd_by_node, current_layout, ftt,
                host_by_node=host_by_node, label_by_node=label_by_node)
        except replica_placement.InfeasiblePlacement as e:
            return False, str(e)
        if not plan.full_diversity:
            logger.warning(
                f"[REMOVAL] {removed_node.get_id()}: the post-removal layout "
                f"cannot be made fully domain-diverse: "
                f"{'; '.join(plan.notes) or 'no reason recorded'}")
            for violation in plan.violations:
                logger.warning(f"[REMOVAL] {removed_node.get_id()}: {violation}")
        return True, ""

    for backref, picker in (
            ("lvstore_stack_secondary", "secondary"),
            ("lvstore_stack_tertiary", "tertiary")):
        primary_id = getattr(removed_node, backref)
        if not primary_id:
            continue
        try:
            primary = db_controller.get_storage_node_by_id(primary_id)
        except KeyError:
            # The primary is gone; nothing to relocate, just bookkeeping.
            continue
        if not _pick_replica_relocation_node(primary, removed_node, picker, db_controller):
            return False, (
                f"no host-disjoint node available to re-host the {picker} replica "
                f"of primary {primary_id} (currently on the node being removed)")
    return True, ""


def _pick_replica_relocation_node(primary, removed_node: StorageNode, role, db_controller,
                                   extra_exclude_ids=()):
    """Choose a node to re-host ``primary``'s ``role`` (secondary|tertiary)
    replica, currently on ``removed_node``. Returns a node id or None.
    Reuses the existing anti-affinity-aware placement helpers.

    ``extra_exclude_ids``: additional node ids to rule out beyond
    ``removed_node`` itself -- used by ``_relocate_replica_between``'s
    nested vacate-rotation to exclude the primary currently being spliced
    in the ENCLOSING call. Without this, the only candidate this call finds
    can be exactly that in-flight primary (structurally invalid: it can't
    simultaneously be the thing being relocated onto a node AND the target
    something else relocates onto), and the caller's own guard against that
    just gives up rather than searching further (2026-08-28 finding: a
    three-way chain -- primary A splices into an existing pairing,
    displacing B onto A's target, but B's only free candidate for the role
    being vacated turns out to be A itself, mid-relocation -- got stuck
    retrying the identical failure forever instead of looking past A).

    With failure domains enabled, prefers FULL pairwise domain diversity
    across {primary, secondary, tertiary} — not just the weaker ">=1
    cross-domain role" floor this used to settle for. That floor let the
    role NOT being relocated stay wherever it already was and placed the
    one being relocated in ANY domain at all (including the primary's own,
    or the other role's) as long as one non-leader path stayed cross-domain
    — a real, live-confirmed gap (2026-08-27: after two uneven removals
    shrank two of four domains to 2 hosts, several nodes ended up with
    secondary and tertiary sharing a domain, or a tertiary sharing the
    primary's own domain, purely because the other role already happened
    to be cross-domain when this ran). Tries, in order:
      1. a direct candidate diverse from BOTH the primary and the other
         already-assigned role;
      2. splicing into an existing pairing whose far end is also diverse
         from both (see ``_find_splice_target_for_relocation``'s
         ``avoid_domains``);
      3. the original ">=1 cross-domain" floor (direct candidate, then
         splice) if neither of the above found anything — full diversity
         genuinely isn't achievable right now (e.g. domains have shrunk
         unevenly) and refusing the relocation outright would strand the
         node-removal instead. Logged loudly so the degraded outcome is
         visible rather than silently matching the old behavior;
      4. same-domain-blind last resort (direct candidate, then splice)
         when there's no other role to be diverse from at all, or FD data
         is absent/invalid — unchanged from before.

    ``get_secondary_nodes``/``get_secondary_nodes_2`` only ever offer
    UNCLAIMED nodes (each node hosts at most one secondary/tertiary at a
    time — ``lvstore_stack_secondary``/``_tertiary`` is a single field, not
    a list). A removal frees exactly one node system-wide (whoever hosted
    ``removed_node``'s own role); if that one lands in the wrong domain —
    or nothing is free at all — the direct search has nothing else to
    offer even though a valid rearrangement exists elsewhere in the
    cluster (2026-08-07, chained-removal incident: two removals in a row
    stranded a third node's secondary with zero free cross-domain
    candidates, while an existing pairing two hops away could have
    absorbed it). Falls back to splicing ``primary`` into an already-formed
    pairing (see ``_find_splice_target_for_relocation``) — exactly the fix
    ``splice_stranded_secondary``/``splice_stranded_tertiary`` already apply
    to the identical dead end at cluster-activation time. Only returning
    None (every step above exhausted) makes
    ``_check_replica_relocation_feasible`` refuse the removal up front.
    """
    exclude_ids = [removed_node.get_id(), *extra_exclude_ids]
    if role == "secondary":
        other_id = primary.tertiary_node_id
        if primary.tertiary_node_id and primary.tertiary_node_id != removed_node.get_id():
            exclude_ids.append(primary.tertiary_node_id)
        cands = get_secondary_nodes(primary, exclude_ids=exclude_ids, removed_node=removed_node)
    else:
        other_id = primary.secondary_node_id
        exclude_mgmt_ips = []
        if primary.secondary_node_id and primary.secondary_node_id != removed_node.get_id():
            exclude_ids.append(primary.secondary_node_id)
            try:
                sec = db_controller.get_storage_node_by_id(primary.secondary_node_id)
                exclude_mgmt_ips.append(sec.mgmt_ip)
            except KeyError:
                pass
        cands = get_secondary_nodes_2(
            primary, exclude_ids=exclude_ids, exclude_mgmt_ips=exclude_mgmt_ips)

    cluster = db_controller.get_cluster_by_id(primary.cluster_id)
    fd_enabled = bool(getattr(cluster, "enable_failure_domain", False) and primary.failure_domain >= 0)

    def first_diverse(domains):
        for cand_id in cands:
            try:
                cand = db_controller.get_storage_node_by_id(cand_id)
            except KeyError:
                continue
            if cand.failure_domain >= 0 and cand.failure_domain not in domains:
                return cand_id
        return None

    if not fd_enabled:
        if cands:
            return cands[0]
        splice = _find_splice_target_for_relocation(
            primary, role, db_controller, exclude_ids=exclude_ids + [primary.get_id()])
        return splice[1] if splice else None

    other_fd = None
    if other_id and other_id != removed_node.get_id():
        try:
            other = db_controller.get_storage_node_by_id(other_id)
            if other.failure_domain >= 0:
                other_fd = other.failure_domain
        except KeyError:
            pass

    full_avoid = {primary.failure_domain}
    if other_fd is not None:
        full_avoid.add(other_fd)

    # 1. Direct candidate, fully diverse from both the primary and the
    # other already-assigned role.
    if cands:
        found = first_diverse(full_avoid)
        if found:
            return found

    # 2. Splice into an existing pairing whose far end is also fully diverse.
    splice = _find_splice_target_for_relocation(
        primary, role, db_controller, exclude_ids=exclude_ids + [primary.get_id()],
        avoid_domains=full_avoid)
    if splice:
        return splice[1]

    # 3. Full diversity isn't achievable anywhere in the cluster right now.
    # Relax to the >=1-cross-domain floor (diverse from the primary alone)
    # rather than refuse the relocation outright — only when there IS an
    # other role to have been diverse from, i.e. this is a genuine relax,
    # not silently skipping the check.
    if other_fd is not None:
        weak_avoid = {primary.failure_domain}
        found = first_diverse(weak_avoid) if cands else None
        if found:
            logger.warning(
                f"[REMOVAL] {primary.get_id()}: no candidate keeps {role} fully domain-diverse "
                f"from both the primary (domain {primary.failure_domain}) and its other "
                f"replica (domain {other_fd}); falling back to {found}, cross-domain from "
                f"the primary only")
            return found
        splice = _find_splice_target_for_relocation(
            primary, role, db_controller, exclude_ids=exclude_ids + [primary.get_id()],
            avoid_domains=weak_avoid)
        if splice:
            logger.warning(
                f"[REMOVAL] {primary.get_id()}: no fully domain-diverse splice target for "
                f"{role} either; falling back to splicing {splice[0]} -> {splice[1]}, "
                f"cross-domain from the primary only")
            return splice[1]

    # 4. Same-domain-blind last resort: no other role to be diverse from,
    # or nothing satisfies even the weaker floor.
    if cands:
        return cands[0]
    splice = _find_splice_target_for_relocation(
        primary, role, db_controller, exclude_ids=exclude_ids + [primary.get_id()])
    return splice[1] if splice else None


def _find_splice_target_for_relocation(stranded_primary, role, db_controller, exclude_ids=(),
                                        avoid_domains=frozenset()):
    """Find an already-formed pairing ``P -> X`` (``P.<field> == X``)
    elsewhere in the cluster to splice ``stranded_primary`` into:
    ``P -> stranded_primary -> X``. Read-only — callers decide whether and
    how to execute the resulting move (see ``_relocate_one_replica``).

    Generalizes ``splice_stranded_secondary``/``splice_stranded_tertiary``'s
    edge search (same scoring: prefer both ends domain-disjoint from the
    stranded node, then relax) with an ``exclude_ids`` list, so the
    node-removal repair path can rule out the node being removed and any
    other already-claimed id. Unlike the activation-time splice helpers —
    which only ever run before any physical LVS exists — this can be asked
    to splice into a pairing that already has real data on both ends;
    executing that move (not just picking the edge) is the caller's job.

    ``avoid_domains``: X (the node ``stranded_primary`` would actually end
    up hosted on) is excluded outright, not just scored down, when its
    domain is in this set. ``_pick_replica_relocation_node`` uses this to
    keep a spliced-in replacement fully diverse from BOTH the primary and
    its other already-assigned role — the plain domain-mismatch scoring
    below only ever knows about diversity from ``stranded_primary`` itself,
    which isn't enough to keep two independently-placed roles apart.

    P's OWN other role (its tertiary if ``role`` is "secondary", or vice
    versa, untouched by this splice) is also considered: repointing P's
    ``field`` from X onto ``stranded_primary`` must not put P in the exact
    state this diversity fix exists to prevent -- two of P's own roles
    sharing a domain. This is a *preference*, not a hard filter -- among
    all valid edges, one whose P stays fully diverse afterwards is always
    picked over one that doesn't (ties broken by the existing domain-
    mismatch score below), but a colliding edge is still accepted, with a
    warning, when it's the only one available. In a real multi-domain
    cluster there are usually several candidate edges, so this alone
    resolves the common case without ever refusing a repair that a less
    picky search would have found (2026-08-27 finding: splicing kc25l into
    56mg5's secondary slot collided with 56mg5's pre-existing, untouched
    tertiary in the same domain, when another edge elsewhere in the ring
    -- t74sg's -- was collision-free the whole time; the old avoid_domains-
    only check had no way to prefer it, since avoid_domains only ever
    looked at X's domain, never P's).

    Returns ``(p_id, x_id)`` or ``None`` if no valid edge exists.
    """
    field = "secondary_node_id" if role == "secondary" else "tertiary_node_id"
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(stranded_primary.cluster_id)
    all_nodes = sorted(all_nodes, key=lambda n: n.failure_domain)
    by_id = {n.get_id(): n for n in all_nodes}
    exclude = set(exclude_ids) | {stranded_primary.get_id()}

    stranded_sec = None
    if role == "tertiary" and stranded_primary.secondary_node_id:
        stranded_sec = by_id.get(stranded_primary.secondary_node_id)

    def _online(*nodes):
        return all(n.status == StorageNode.STATUS_ONLINE for n in nodes)

    def _valid_tertiary(node, node_sec, candidate):
        if candidate.get_id() == node.get_id():
            return False
        if candidate.mgmt_ip == node.mgmt_ip:
            return False
        if node_sec and candidate.mgmt_ip == node_sec.mgmt_ip:
            return False
        return True

    def _domain_mismatch_score(*nodes):
        if stranded_primary.failure_domain < 0:
            return 0
        return sum(1 for n in nodes if n.failure_domain != stranded_primary.failure_domain)

    edges = [n for n in all_nodes if getattr(n, field) and n.get_id() not in exclude]
    other_field = "tertiary_node_id" if field == "secondary_node_id" else "secondary_node_id"

    best, best_key, best_collides = None, None, False
    for p in edges:
        x_id = getattr(p, field)
        if x_id in exclude:
            continue
        x = by_id.get(x_id)
        if not x or not _online(p, x):
            continue
        if avoid_domains and x.failure_domain in avoid_domains:
            continue
        # Once spliced, P.<field> is repointed onto stranded_primary itself
        # (see _relocate_replica_between) -- P's role-target BECOMES
        # stranded_primary, exactly as fundamental an invariant as X's
        # domain above: if P's own domain matches stranded_primary's, P now
        # holds a role-target in its own domain, the most basic diversity
        # violation there is. Hard-excluded like X's domain, not merely
        # scored down -- unlike P's OTHER role below, there is no
        # legitimate "nothing better exists" case here, since this is the
        # exact same guarantee _pick_replica_relocation_node's own
        # full_avoid already enforces for X (2026-08-28 finding: this stayed
        # a soft preference from before today's diversity work and let a
        # live splice give p59j8 a tertiary target in its own domain).
        if stranded_primary.failure_domain >= 0 and p.failure_domain == stranded_primary.failure_domain:
            continue
        if role == "secondary":
            if p.mgmt_ip == stranded_primary.mgmt_ip or x.mgmt_ip == stranded_primary.mgmt_ip:
                continue
        else:
            p_sec = by_id.get(p.secondary_node_id) if p.secondary_node_id else None
            if not _valid_tertiary(p, p_sec, stranded_primary):
                continue
            if not _valid_tertiary(stranded_primary, stranded_sec, x):
                continue

        # Prefer an edge that leaves P's OWN other role (its tertiary if
        # `role` is "secondary", or vice versa -- untouched by this splice)
        # diverse from stranded_primary once P.<field> is repointed onto
        # it. A collision here degrades a node that was never part of this
        # removal at all, so it's ranked below every non-colliding edge --
        # but still accepted as a last resort rather than refused outright
        # (2026-08-27 finding: with several candidate edges usually
        # available in a real cluster, one of them is typically collision-
        # free; the old code had no way to prefer it, so an outright reject
        # here would have been more restrictive than useful).
        p_other_id = getattr(p, other_field)
        collides = False
        if p_other_id and p_other_id != stranded_primary.get_id():
            p_other = by_id.get(p_other_id)
            if (p_other and stranded_primary.failure_domain >= 0
                    and p_other.failure_domain == stranded_primary.failure_domain):
                collides = True

        score = _domain_mismatch_score(p, x)
        key = (not collides, score)
        if best_key is None or key > best_key:
            best_key, best, best_collides = key, (p.get_id(), x.get_id()), collides

    if best and best_collides:
        logger.warning(
            f"[REMOVAL] splice {best[0]} -> {stranded_primary.get_id()} -> {best[1]}: "
            f"no candidate edge leaves {best[0]}'s other role diverse from "
            f"{stranded_primary.get_id()}'s domain {stranded_primary.failure_domain}; "
            f"using it anyway as the least-bad option -- {best[0]} is now degraded "
            f"by a removal it wasn't otherwise part of")
    return best


def node_removal_orchestrate(node_id, force_remove=False):
    """Idempotent, resumable orchestration driven by tasks_runner_node_removal.

    Returns True only when the node has been fully removed (status REMOVED).
    Returns False to signal "incomplete, retry later" (e.g. device migration
    still in progress) — the runner suspends the task and revisits it.

    Every phase is guarded so a re-entry after a crash/retry skips already
    completed work.
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error(f"node_removal_orchestrate: node {node_id} not found")
        return False

    # Phase 4 (below) flips status to REMOVED *before* phase 5 (device
    # remove/fail/migrate; also re-runs the JM patch defensively) runs --
    # so "status == REMOVED" means phases 1/3a/2/3b/4 committed, NOT that
    # removal is fully done. A bare `return True` here would let a
    # transient failure inside phase 5 (e.g. an RPC error against a peer)
    # get permanently masked: the retry re-enters, hits this guard, and
    # reports "done" forever without phase 5 ever completing (2026-08-10
    # incident: a mid-phase-5 RPC error left a peer's lvstore un-rebuilt
    # while the task reported "Node removed"). Only phases 1/3a/2/3b/4 are
    # skipped below when already_removed; phase 5 always runs and is
    # itself idempotent (skips devices/JM already migrated), so resuming
    # it here is a no-op once it has genuinely finished.
    already_removed = snode.status == StorageNode.STATUS_REMOVED

    # Node removal is a recognised restart-phase owner: phase 3b relocates
    # replicas onto an ONLINE target and sets a restart phase there, which
    # get_restart_phase would otherwise judge stale and clear out from under
    # the live rebuild. Held for ONE attempt only — this returns False and is
    # retried, and a phase cannot outlive the attempt that set it.
    # Restore the CAPTURED status, not ACTIVE: the cluster is usually DEGRADED
    # here, since the node being removed has just been shut down.
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    prev_cluster_status = cluster.status
    cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_IN_SHRINK)
    try:
        if not already_removed:
            # Phase 1 — shut the node down (graceful). Skipped on re-entry.
            if snode.status in [StorageNode.STATUS_ONLINE, StorageNode.STATUS_SUSPENDED]:
                logger.info(f"[REMOVAL] {node_id}: phase 1 — shutdown")
                ret = shutdown_storage_node(node_id, force=force_remove)
                if isinstance(ret, tuple):
                    ret, reason = ret
                    if not ret:
                        logger.error(f"[REMOVAL] {node_id}: shutdown failed: {reason}")
                        return False
                elif not ret:
                    logger.error(f"[REMOVAL] {node_id}: shutdown failed")
                    return False
                snode = db_controller.get_storage_node_by_id(node_id)

            if snode.status != StorageNode.STATUS_IN_REMOVAL:
                set_node_status(node_id, StorageNode.STATUS_IN_REMOVAL, caused_by="remove")

            # Phase 3a — tear down the (empty) secondary/tertiary replicas of THIS
            # node's own primary LVS, on the peers that host them (Case A).
            # Runs BEFORE phase 2: a peer hosting THIS node's own replica runs a
            # local JC instance for it too, and that instance also references
            # this node's OWN JM by name (get_node_jm_names always includes the
            # replica's owning primary's JM, even from a secondary's local
            # construct) -- a second, independent local jm_vuid on that peer
            # using the exact same name_old that _decommission_node_jm's
            # target-gathering has no way to see (it only tracks OTHER
            # primaries via `decisions`, never this node's own hosted replica).
            # Left in place, jc_replace_jm's own multi-target safety check
            # rejects the batched call outright (-17: "does not cover all
            # jm_vuids that use name_old") because it still finds that second
            # instance live. Tearing the replica down first removes it
            # entirely, so phase 2 never has to account for it (found live
            # 2026-08-25: this node's own hosted-replica peer failed the very
            # next removal after the phase 2/3b reorder that fixed the
            # relocation-timing gap).
            # Captured BEFORE phase 3a, which clears both this node's
            # secondary/tertiary pointers and those peers' back-references.
            # These two peers are the ones left running a JC instance for THIS
            # node's own jm_vuid, and phase 2 cannot find them any other way --
            # see _decommission_node_jm's replica_peer_ids.
            replica_peer_ids = tuple(
                pid for pid in (snode.secondary_node_id, snode.tertiary_node_id) if pid)

            logger.info(f"[REMOVAL] {node_id}: phase 3a — tear down own replicas")
            if not _teardown_replicas_of_primary(snode):
                return False

            # Phase 2 — patch this node's JM out of every live JC redundancy
            # set BEFORE phase 3b can relocate any replica onto a new host.
            # See _decommission_node_jm's docstring for why the ordering
            # matters: a replica relocated while a dying JM is still listed
            # in its primary's jm_ids bakes that unreachable member into the
            # new host's construct permanently.
            logger.info(f"[REMOVAL] {node_id}: phase 2 — decommission JM")
            _decommission_node_jm(snode, replica_peer_ids=replica_peer_ids)
            snode = db_controller.get_storage_node_by_id(node_id)

            # Phase 3b — relocate replicas this node hosts for OTHER primaries (Case B).
            logger.info(f"[REMOVAL] {node_id}: phase 3b — relocate hosted replicas")
            if not _relocate_replicas_hosted_on(snode):
                return False

            # Phase 3c — prove the relocations actually landed. Every pointer
            # phase 3b writes is bookkeeping; this is the only step that asks
            # the devices. Reported, not fatal: by here the removal is
            # physically done and the node is on its way out, so failing would
            # only spin the retry loop against a state it cannot re-drive --
            # but a missing replica must never leave this function silently.
            logger.info(f"[REMOVAL] {node_id}: phase 3c — verify replica stacks")
            _verify_replica_stacks(snode.cluster_id, db_controller,
                                   context=f" after removing {node_id}")

            # Phase 4 — finalize (swarm leave, gpt cleanup) and flip to removed.
            logger.info(f"[REMOVAL] {node_id}: phase 4 — finalize")
            _finalize_node_removal(snode)
            set_node_status(node_id, StorageNode.STATUS_REMOVED, caused_by="remove")
            snode = db_controller.get_storage_node_by_id(node_id)
            # storage_events.snode_status_change(
            #     snode, StorageNode.STATUS_REMOVED, StorageNode.STATUS_IN_REMOVAL, caused_by="remove")

        # Phase 5 — remove + fail devices, then wait for failure-migration to
        # finish. Always attempted, even on resume after status already
        # flipped to REMOVED -- see the already_removed comment above.
        logger.info(f"[REMOVAL] {node_id}: phase 5 — devices remove/fail/migrate")
        if not _decommission_node_devices(snode):
            return False

        logger.info(f"[REMOVAL] {node_id}: done")
    finally:
        cluster_ops.set_cluster_status(cluster.get_id(), prev_cluster_status)
    return True


def replica_stack_violations(nodes, stack_present):
    """Nodes whose PHYSICAL replica stacks disagree with their bookkeeping.

    ``nodes`` is the set of ONLINE nodes; ``stack_present(node, lvstore)``
    reports whether ``lvstore`` is actually surfaced on that node's SPDK.

    The invariant: a node physically holds one lvstore per primary its
    back-references claim it hosts. Every forward pointer
    (``secondary_node_id`` / ``tertiary_node_id``), back-reference
    (``lvstore_stack_secondary`` / ``_tertiary``) and ``lvstore_ports`` entry
    can agree perfectly and still describe a replica that is not there --
    they are all bookkeeping, written by the same code path, and none of them
    is evidence that ``raid0_<vuid>`` + ``LVS_<vuid>`` exist on the host.

    This is the check that was missing. Its absence is why a relocation could
    delete a just-installed replica (see _relocate_replica_between's
    same-primary/other-role guard) and have the removal report success:
    nothing ever compared the claim against the device. Found live
    2026-09-01, and only by dumping bdev_lvol_get_lvstores on all ten
    survivors by hand -- two of them were down to a single real replica for
    an FTT2 lvstore, with no error logged anywhere in the removal.

    Scoped deliberately to HOSTED replicas (what the back-references claim),
    not a node's own primary lvstore: a primary can be legitimately in flux
    mid-flow, and a false alarm there would train the reader to ignore this.

    Returns a list of ``(node_id, lvstore, owner_primary_id, role)`` for each
    claimed-but-absent stack; empty means the invariant holds.
    """
    by_id = {n.get_id(): n for n in nodes}
    missing = []
    for node in nodes:
        for backref, role in (("lvstore_stack_secondary", "secondary"),
                              ("lvstore_stack_tertiary", "tertiary")):
            owner_id = getattr(node, backref, "")
            if not owner_id:
                continue
            owner = by_id.get(owner_id)
            if owner is None or not owner.lvstore:
                # Owner gone or has no lvstore -- a bookkeeping problem of a
                # different kind, and not something a stack probe can settle.
                continue
            if not stack_present(node, owner.lvstore):
                missing.append((node.get_id(), owner.lvstore, owner_id, role))
    return missing


def _verify_replica_stacks(cluster_id, db_controller, context=""):
    """Probe every online node's hosted replica stacks and log any that are
    missing. Returns the violation list (empty when the invariant holds).

    An unreachable node is NOT reported as a violation: absence of proof is
    not proof of absence, and a probe that cries wolf on a transient RPC
    error is a check people learn to skip.
    """
    nodes = [n for n in db_controller.get_storage_nodes_by_cluster_id(cluster_id)
             if n.status == StorageNode.STATUS_ONLINE]

    def stack_present(node, lvstore):
        try:
            return bool(node.rpc_client(timeout=10, retry=1).bdev_lvol_get_lvstores(lvstore))
        except Exception as e:
            logger.warning(
                f"[REMOVAL] could not probe {lvstore} on {node.get_id()} "
                f"({e}); not counting it as missing")
            return True

    violations = replica_stack_violations(nodes, stack_present)
    for node_id, lvstore, owner_id, role in violations:
        logger.error(
            f"[REMOVAL] REPLICA STACK MISSING{context}: {node_id} is recorded as "
            f"{role} of {owner_id} but {lvstore} is not present on it -- "
            f"{owner_id} is running with one fewer replica than its bookkeeping claims")
    if not violations:
        logger.info(
            f"[REMOVAL] replica-stack invariant holds{context}: every hosted "
            f"replica claimed by a back-reference is physically present")
    return violations


def _teardown_replicas_of_primary(removed_node: StorageNode):
    """Case A: the primary LVS lives on ``removed_node`` (now shut down).
    Delete its secondary/tertiary replicas from the peers that host them and
    clear the cross-reference bookkeeping. The node has no LVols (enforced at
    entry), so the replicas hold only the empty lvstore + hublvol."""
    db_controller = DBController()
    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    cluster = db_controller.get_cluster_by_id(removed_node.cluster_id)

    for field, backref in (
            ("secondary_node_id", "lvstore_stack_secondary"),
            ("tertiary_node_id", "lvstore_stack_tertiary")):
        peer_id = getattr(removed_node, field)
        if not peer_id:
            continue
        try:
            peer = db_controller.get_storage_node_by_id(peer_id)
        except KeyError:
            removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
            setattr(removed_node, field, "")
            removed_node.write_to_db()
            continue

        if peer.status == StorageNode.STATUS_ONLINE:
            _delete_replica_on_peer(peer, removed_node, cluster)
            _prune_stale_lvstore_ports(peer_id, removed_node.lvstore, db_controller)

        # Clear the peer's back-reference if it still points at us.
        peer = db_controller.get_storage_node_by_id(peer_id)
        if getattr(peer, backref) == removed_node.get_id():
            setattr(peer, backref, "")
            peer.write_to_db()

        removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
        setattr(removed_node, field, "")
        removed_node.write_to_db()

    return True


def _delete_replica_on_peer(peer, primary, cluster, destroy_lvstore=True):
    """Best-effort teardown of ``primary``'s replica lvstore (+ hublvol) on the
    online ``peer``. RPC failures are logged, not fatal: the peer is healthy and
    a lingering empty bdev is harmless, while blocking removal on it is not.

    ``destroy_lvstore``: whether the shared on-disk blobstore backing this
    lvstore may be destroyed once the local bdev stack comes down.

    - ``True`` (default -- Case A, node removal): ``primary`` IS the node
      being removed; nothing else will ever read this lvstore again once
      its own devices are decommissioned, so destroying it here is correct.
    - ``False`` (splice/relocation eviction): ``primary`` is a SURVIVING
      node whose replica is only being *moved* off ``peer`` onto a new
      host -- the lvstore itself stays alive and in use elsewhere.
      ``peer`` only ever held a non-leader examine copy, so
      ``bdev_lvol_delete_lvstore`` here would destroy the shared blobstore
      metadata out from under the still-live primary and any other
      surviving replica (2026-08-16: this is what actually corrupted
      LVS_1's on-disk metadata during a splice eviction, surfacing later
      as `bs_super_validate: unsupported version on super block` when the
      primary tried to reload it on restart -- caught only after the fact
      via log analysis, not before). Pass ``False`` here; only the local
      raid/distrib examine bdevs get hot-removed, matching
      ``teardown_non_leader_lvstore``'s existing, correct pattern for the
      identical non-leader-eviction case."""
    rpc_client = peer.rpc_client()
    lvstore = primary.lvstore
    if not lvstore:
        return
    # peer's own hublvol subsystem for this replica has no consumers -- only
    # a promoted primary's hublvol is ever attached-to -- so it sits dormant
    # and is cleaned up naturally on peer's next restart; left alone here.
    # try:
    #     nqn = peer.hublvol_nqn_for_lvstore(cluster.nqn, lvstore)
    #     if rpc_client.subsystem_get(nqn):
    #         rpc_client.subsystem_delete(nqn)
    # except RPCException as e:
    #     logger.warning(f"hublvol subsystem teardown for {lvstore} on {peer.get_id()} failed: {e}")
    # try:
    #     rpc_client.bdev_lvol_delete_hublvol(lvstore)
    # except RPCException as e:
    #     logger.warning(f"hublvol bdev teardown for {lvstore} on {peer.get_id()} failed: {e}")

    # UNLIKE the subsystem above, peer's NVMe-oF controller connecting TO
    # primary's hublvol *is* a live consumer connection (kept attached the
    # whole time peer held this replica, for fast failover) and must be
    # detached here -- mirrors teardown_non_leader_lvstore's step 2. Leaving
    # it dangling is not harmless: if peer is later re-selected to host a
    # replica of this same primary again before its next restart, the stale
    # controller can be found wedged in a non-enabled state, and the
    # reconcile's detach-and-wait-gone can then time out and abort the
    # rebuild (2026-08-14: this exact sequence took ffznh's SPDK down --
    # this connection was left behind by an earlier splice eviction and
    # ffznh was never restarted before being re-selected as 5bc9k's
    # secondary again).
    if primary.hublvol and primary.hublvol.bdev_name:
        try:
            rpc_client.bdev_nvme_detach_controller(primary.hublvol.bdev_name)
        except RPCException as e:
            logger.warning(f"hublvol controller detach for {lvstore} on {peer.get_id()} failed: {e}")
    try:
        # deepcopy: _remove_bdev_stack stamps bdev['status']; don't mutate the
        # primary's stored stack definition.
        _remove_bdev_stack(copy.deepcopy(primary.lvstore_stack), rpc_client,
                           remove_distr_only=not destroy_lvstore)
    except RPCException as e:
        logger.warning(f"replica bdev-stack teardown for {lvstore} on {peer.get_id()} failed: {e}")


def _prune_stale_lvstore_ports(node_id, lvstore, db_controller):
    """Drop ``lvstore``'s port reservation from ``node_id``'s
    ``lvstore_ports`` after its replica has been torn down there for good.

    Unlike ``teardown_non_leader_lvstore``'s donor-reconnect path (which
    deliberately keeps the entry so a returning node reuses its old ports),
    callers of this helper -- node removal, splice eviction -- know the
    replica isn't coming back to this node. A stale entry there only
    misrepresents `sn list`'s "LVS Ports" column (2026-08-12: found live via
    bdev_lvol_get_lvstores disagreeing with the DB after a node removal).
    Re-fetches fresh to avoid clobbering unrelated concurrent edits."""
    if not lvstore:
        return
    node = db_controller.get_storage_node_by_id(node_id)
    if node.lvstore_ports and lvstore in node.lvstore_ports:
        del node.lvstore_ports[lvstore]
        node.write_to_db()


def _teardown_lvol_subsystems_on_vacated_peer(peer, primary, db_controller):
    """Best-effort: delete every LVol-of-``primary``'s own NVMe-oF subsystem
    on ``peer`` after ``peer`` stops hosting ``primary``'s replica.

    ``_delete_replica_on_peer(..., destroy_lvstore=False)`` tears down
    ``peer``'s local raid/distrib bdev stack for the lvstore, which cascades
    to remove each hosted LVol's *namespace* -- but the per-LVol NVMe-oF
    *subsystem and listener* (registered separately, per lvol, via
    add_lvol_thread) are never touched by that teardown. Left behind, the
    listener keeps accepting connections in front of a now-empty subsystem
    -- exactly the "live but no path" failure test_missing_namespace_path_
    loss.py guards against, just reached by a different door: the CSI/host
    initiator's own connection to peer stays live, and since peer is no
    longer in lvol.nodes nothing ever tells it to drop that connection
    either. The volume ends up with an extra path that looks healthy but
    carries no I/O, alongside the correct new one (2026-08-18: found live
    after the lvol.nodes/wrong-port fixes above -- both corrected lvols
    still carried this stale-but-live third path to their pre-relocation
    host). RPC failures are logged, not fatal: peer is healthy and a
    lingering empty subsystem is harmless to leave for the next restart to
    clear, while blocking the relocation on it is not."""
    rpc_client = peer.rpc_client()
    for lvol in db_controller.get_lvols_by_node_id(primary.get_id()):
        try:
            rpc_client.subsystem_delete(lvol.nqn)
        except RPCException as e:
            logger.warning(
                f"subsystem teardown for lvol {lvol.get_id()} ({lvol.nqn}) "
                f"on vacated peer {peer.get_id()} failed: {e}")


def _update_lvol_nodes_for_replica_move(primary_id, old_host_id, new_host_id, db_controller):
    """Re-point every LVol hosted on ``primary_id`` from ``old_host_id`` to
    ``new_host_id`` in its own ``nodes`` list, once that primary's
    secondary/tertiary replica has been relocated between the two hosts.

    ``lvol.nodes`` is what the CSI/host initiator actually connects to for
    multipath failover -- a separate record from the storage-node-level
    ``secondary_node_id``/``tertiary_node_id`` bookkeeping the relocation
    itself already updates. Leaving the old host listed here strands every
    LVol hosted on ``primary_id`` on a single path (the primary only) once
    the old host is gone/unreachable, with nothing ever repointing it --
    mirrors cluster_expansion/executor.py's identical fix for the
    expansion-rebalancing case. (2026-08-18: found live during node
    removal -- a splice-relocated secondary left a still-online lvol's
    ``nodes`` naming the just-removed node; the CSI initiator never
    reconnected to the actual new secondary.)

    Safe to call redundantly (e.g. on a retry after an earlier attempt
    already applied it): each lvol is only rewritten if ``old_host_id`` is
    still present in its ``nodes``."""
    for lvol in db_controller.get_lvols_by_node_id(primary_id):
        nodes = list(lvol.nodes or [])
        if old_host_id in nodes:
            lvol.nodes = [new_host_id if n == old_host_id else n for n in nodes]
            lvol.write_to_db()


def _relocate_replicas_hosted_on(removed_node: StorageNode):
    """Case B: ``removed_node`` holds a secondary and/or tertiary replica for
    other primaries. Re-host each on a fresh, anti-affinity-valid node so the
    owning primary keeps its fault tolerance after this node leaves.

    With failure domains enabled this goes through the global planner
    (``replica_placement``), which solves the whole post-removal layout at
    once instead of re-homing each stranded replica in isolation -- see
    ``_plan_driven_relocation``. The per-replica greedy path below is the
    fallback for the cases the planner deliberately does not take
    (FD disabled, dedicated secondary nodes, a peer that is not ONLINE)."""
    db_controller = DBController()

    handled = _plan_driven_relocation(removed_node, db_controller)
    if handled is not None:
        return handled

    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    if removed_node.lvstore_stack_secondary:
        if not _relocate_one_replica(removed_node, removed_node.lvstore_stack_secondary, "secondary"):
            return False

    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    if removed_node.lvstore_stack_tertiary:
        if not _relocate_one_replica(removed_node, removed_node.lvstore_stack_tertiary, "tertiary"):
            return False

    return True


def _relocation_planner_inputs(removed_node: StorageNode, db_controller):
    """Gather the pure inputs the global placement planner needs, or ``None``
    when this cluster is not a case the planner handles.

    Returns ``(surviving_ids, fd_by_node, host_by_node, label_by_node,
    current_layout, ftt)``. ``current_layout`` is read straight off the
    primaries' ``secondary_node_id``/``tertiary_node_id`` pointers, including
    the ones that still name ``removed_node`` -- the planner treats a holder
    outside the surviving set as "no host", and the diff then carries
    ``removed_node`` as the move's origin so the existing mover can tear the
    old copy down and clear its back-reference exactly as before.

    Declines (returns ``None``) when:

    * failure domains are off -- there is no diversity invariant to solve
      for, and the long-standing greedy path already handles host-disjoint
      re-homing;
    * any surviving node has no failure domain set -- a partial domain map
      cannot be reasoned about, only guessed at;
    * the cluster has dedicated secondary nodes (``is_secondary_node``),
      which may host more than one replica each and so break the
      one-slot-per-node permutation model the planner is built on;
    * any surviving node is not ONLINE -- the planner would happily place a
      replica on a node that cannot build it.
    """
    from simplyblock_core.controllers import replica_placement

    cluster = db_controller.get_cluster_by_id(removed_node.cluster_id)
    if not getattr(cluster, "enable_failure_domain", False):
        return None

    ftt = cluster.max_fault_tolerance if cluster.max_fault_tolerance in (1, 2) else 1
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(removed_node.cluster_id)
    survivors = [
        n for n in all_nodes
        if n.get_id() != removed_node.get_id() and n.status != StorageNode.STATUS_REMOVED
    ]
    if not survivors:
        return None
    if any(n.is_secondary_node for n in survivors):
        return None
    if any(n.status != StorageNode.STATUS_ONLINE for n in survivors):
        return None
    if any(n.failure_domain < 0 for n in survivors):
        return None

    surviving_ids = [n.get_id() for n in survivors]
    fd_by_node = {n.get_id(): n.failure_domain for n in survivors}
    host_by_node = {n.get_id(): n.mgmt_ip for n in survivors}
    label_by_node = {n.get_id(): n.physical_label for n in survivors}
    current_layout = {
        n.get_id(): replica_placement.Placement(
            n.secondary_node_id, n.tertiary_node_id if ftt >= 2 else "")
        for n in survivors
    }
    return surviving_ids, fd_by_node, host_by_node, label_by_node, current_layout, ftt


def _plan_driven_relocation(removed_node: StorageNode, db_controller):
    """Phase 3b via the global placement planner.

    Returns True when the planned relocations were applied, False when one of
    them failed (the caller retries the whole phase), or ``None`` when the
    planner does not apply to this cluster and the caller should fall back to
    the per-replica greedy path.

    Why this replaces the per-replica path under failure domains: the greedy
    path answers "where does THIS stranded replica go" and can only ever move
    the replica in front of it, so the one repair that a shrinking cluster
    most often needs -- swapping two replicas that are both already placed --
    is not expressible in it at all. It compensates with splices into third
    parties' pairings and, when even that fails, by relaxing the invariant to
    the weaker ">=1 cross-domain role" floor with a warning. Repeated over
    several removals (the reported 4-domain x 3-host case, one host removed
    per domain) those relaxations accumulate into a layout with secondaries
    and tertiaries sharing domains, even though a fully diverse layout
    existed at every step. ``replica_placement`` computes that layout
    directly, as a min-cost perfect matching, and returns the provably
    smallest set of rebuilds that reaches it -- so no splice heuristic, no
    collateral-damage repair hop, and no silent relaxation is needed.

    The move ORDER matters and is part of the plan: every move lands on a
    slot the planner has already proved is free at that point, so
    ``_relocate_replica_between``'s recursive vacate never has to run here.
    A rotation cycle -- which that recursion cannot resolve at all, it hits
    its own cycle backstop -- is broken up front by the planner into an extra
    hop through the one slot the removal frees.
    """
    from simplyblock_core.controllers import replica_placement

    inputs = _relocation_planner_inputs(removed_node, db_controller)
    if inputs is None:
        return None
    surviving_ids, fd_by_node, host_by_node, label_by_node, current_layout, ftt = inputs

    try:
        plan = replica_placement.plan_diverse_layout(
            surviving_ids, fd_by_node, current_layout, ftt,
            host_by_node=host_by_node, label_by_node=label_by_node)
        moves = replica_placement.plan_moves(
            current_layout, plan.layout, surviving_ids, ftt)
    except replica_placement.InfeasiblePlacement as e:
        logger.warning(
            f"[REMOVAL] {removed_node.get_id()}: global replica placement is "
            f"unusable ({e}); falling back to per-replica relocation")
        return None

    logger.info(
        f"[REMOVAL] {removed_node.get_id()}: replica placement plan -- "
        f"{replica_placement.describe_plan(plan, moves)}")
    for violation in plan.violations:
        logger.warning(f"[REMOVAL] {removed_node.get_id()}: {violation}")

    for move in moves:
        # A role with no current host was hosted on the node being removed:
        # name it as the origin so the mover re-points the LVols and clears
        # its back-reference, exactly as _relocate_one_replica used to.
        old_host_id = move.from_node_id or removed_node.get_id()
        if old_host_id == move.to_node_id:
            continue
        logger.info(
            f"[REMOVAL] {removed_node.get_id()}: move {move.role} of "
            f"{move.lvs_primary_node_id}: {old_host_id} -> {move.to_node_id}"
            f"{' (scratch hop)' if move.scratch else ''}")
        if not _relocate_replica_between(
                move.lvs_primary_node_id, old_host_id, move.to_node_id,
                move.role, db_controller):
            logger.error(
                f"[REMOVAL] {removed_node.get_id()}: planned move of "
                f"{move.lvs_primary_node_id}'s {move.role} from {old_host_id} "
                f"to {move.to_node_id} failed; will retry the phase")
            return False

    _clear_replica_backref(removed_node, "lvstore_stack_secondary")
    _clear_replica_backref(removed_node, "lvstore_stack_tertiary")
    return True


def _relocate_one_replica(removed_node: StorageNode, primary_id, role):
    """Re-host ``primary_id``'s ``role`` replica off ``removed_node``.

    Idempotent: the back-reference on ``removed_node`` is cleared only AFTER the
    replica is successfully rebuilt on the new node, so a retry resumes cleanly.
    """
    db_controller = DBController()
    field = "secondary_node_id" if role == "secondary" else "tertiary_node_id"
    backref = "lvstore_stack_secondary" if role == "secondary" else "lvstore_stack_tertiary"

    try:
        primary = db_controller.get_storage_node_by_id(primary_id)
    except KeyError:
        # Primary gone — nothing to rebuild, just drop the stale back-reference.
        _clear_replica_backref(removed_node, backref)
        return True

    # Choose (or recover) the relocation target. If the primary's pointer still
    # names the removed node we must pick a fresh one and commit the forward
    # bookkeeping; on a retry it already names the new node, so reuse it.
    new_id = getattr(primary, field)
    if not new_id or new_id == removed_node.get_id():
        new_id = _pick_replica_relocation_node(primary, removed_node, role, db_controller)
        if not new_id:
            logger.error(
                f"[REMOVAL] no relocation target for {role} replica of {primary_id}")
            return False

        new_node = db_controller.get_storage_node_by_id(new_id)
        occupant_id = getattr(new_node, backref)
        if occupant_id and occupant_id not in (primary_id, removed_node.get_id()):
            # _pick_replica_relocation_node fell back to a splice candidate:
            # new_id is currently busy hosting occupant_id's replica. Evict
            # that occupant onto `primary` (the node whose replica we're
            # relocating) before claiming new_id for primary's own role —
            # see _find_splice_target_for_relocation's docstring.
            if not _relocate_replica_between(occupant_id, new_id, primary_id, role, db_controller):
                logger.error(
                    f"[REMOVAL] failed to splice {primary_id} into the pairing "
                    f"occupying {new_id} (occupant {occupant_id})")
                return False
            _repair_occupants_other_role_after_splice(occupant_id, primary_id, role, db_controller)

        primary = db_controller.get_storage_node_by_id(primary_id)
        setattr(primary, field, new_id)
        primary.write_to_db()
        new_node = db_controller.get_storage_node_by_id(new_id)
        setattr(new_node, backref, primary.get_id())
        new_node.write_to_db()

    new_node = db_controller.get_storage_node_by_id(new_id)
    primary = db_controller.get_storage_node_by_id(primary_id)

    # Build the replica on the new node. The primary is online and remains the
    # leader, so recreate_lvstore_on_non_leader wires distribs/raid/lvstore,
    # role + ANA, and the hublvol connection exactly as the restart path does.
    ret = recreate_lvstore_on_non_leader(new_node, primary, primary)
    if not ret:
        logger.error(
            f"[REMOVAL] failed to rebuild {role} replica of {primary_id} on {new_id}, will retry")
        return False

    # Re-point every LVol hosted on primary before dropping removed_node's
    # side of the relationship -- see _update_lvol_nodes_for_replica_move's
    # docstring. Unconditional (not gated on "did we just build it above")
    # so a retry that resumes past the build-skip branch still catches up
    # if an earlier attempt crashed between the build and this step.
    _update_lvol_nodes_for_replica_move(primary_id, removed_node.get_id(), new_id, db_controller)

    _clear_replica_backref(removed_node, backref)
    return True


def _repair_occupants_other_role_after_splice(occupant_id, primary_id, role, db_controller):
    """After a splice repoints ``occupant``'s ``role`` replica onto
    ``primary``, check whether ``occupant``'s OTHER, untouched role (its
    tertiary if ``role`` is "secondary", or vice versa) now shares a domain
    with ``primary`` -- and if so, relocate THAT role too, reusing the same
    picker (``_pick_replica_relocation_node``) and mover
    (``_relocate_replica_between``) already used for the splice itself.

    A splice edge is chosen to protect the node actually being relocated
    (``primary``) and, since the diversity fix, to prefer one where
    ``occupant`` also stays diverse -- but a colliding edge is still
    accepted as a last resort when no clean one exists (see
    ``_find_splice_target_for_relocation``'s docstring). This closes that
    gap ACTIVELY instead of just warning about it: by the time this runs,
    ``occupant.<role>`` already points at ``primary``, so calling the same
    picker for occupant's other role automatically avoids both occupant's
    own domain and primary's domain -- no extra plumbing needed. Because
    the picker itself tries a direct candidate before a further splice,
    this one call transparently covers both "a free replacement exists"
    and "occupant's other role itself needs splicing into a further
    pairing" -- the same machinery, one hop further out.

    Best-effort and never blocks the outer splice, which has already
    succeeded by the time this runs: if no replacement is found, or the
    relocation itself fails, occupant is left with the collision and a
    warning is logged rather than the removal being failed over it.

    Not itself recursive beyond this one hop -- if repairing occupant's
    other role creates a NEW collision for some third node, that is not
    chased further.
    """
    try:
        primary = db_controller.get_storage_node_by_id(primary_id)
        occupant = db_controller.get_storage_node_by_id(occupant_id)
    except KeyError:
        return
    if primary.failure_domain < 0 or occupant.failure_domain < 0:
        return
    cluster = db_controller.get_cluster_by_id(primary.cluster_id)
    if not getattr(cluster, "enable_failure_domain", False):
        return

    other_role = "tertiary" if role == "secondary" else "secondary"
    other_field = "tertiary_node_id" if role == "secondary" else "secondary_node_id"
    other_target_id = getattr(occupant, other_field)
    if not other_target_id or other_target_id == primary_id:
        return
    try:
        other_target = db_controller.get_storage_node_by_id(other_target_id)
    except KeyError:
        return
    if other_target.failure_domain != primary.failure_domain:
        return  # no collision -- occupant is already fine, nothing to do

    replacement = _pick_replica_relocation_node(occupant, other_target, other_role, db_controller)
    if not replacement or replacement == other_target_id:
        logger.warning(
            f"[REMOVAL] splice: no replacement found to move {occupant_id}'s "
            f"{other_role} off {other_target_id} (domain {other_target.failure_domain}) "
            f"after splicing it onto {primary_id}'s domain {primary.failure_domain}; "
            f"{occupant_id} is left with a domain-diversity gap")
        return
    if not _relocate_replica_between(occupant_id, other_target_id, replacement, other_role, db_controller):
        logger.warning(
            f"[REMOVAL] splice: failed to move {occupant_id}'s {other_role} off "
            f"{other_target_id} onto {replacement}; {occupant_id} is left with a "
            f"domain-diversity gap")


def _relocate_replica_between(occupant_primary_id, old_host_id, new_host_id, role, db_controller, _seen=None):
    """Physically move ``occupant_primary_id``'s ``role`` replica off
    ``old_host_id`` onto ``new_host_id``, updating its forward pointer AND
    ``new_host_id``'s back-reference.

    Used by the splice fallback in ``_relocate_one_replica``: before an
    already-busy node can be claimed for the primary being relocated, its
    current occupant must move onto that primary's node instead (see
    ``_find_splice_target_for_relocation``'s docstring for why an
    already-formed pairing, not an idle node, is what's available).

    ``new_host_id`` itself may ALREADY be hosting a different primary's
    replica via this same single-value ``lvstore_stack_secondary`` /
    ``lvstore_stack_tertiary`` slot: every node in a full ring hosts exactly
    one other node's replica before any removal starts, and that
    relationship has nothing to do with whichever edge the splice search
    happened to pick. (2026-08-12 incident: a splice claimed a node whose
    slot already held an unrelated pre-existing occupant. The physical
    build succeeded -- SPDK doesn't mind hosting a second lvstore -- but the
    slot could not record it, silently untracking that replica for any
    future failover, and leaving `sn list`'s "LVS Ports" column short by
    one entry.) When the slot is occupied, the existing occupant is
    relocated first -- recursively, via this same function -- onto a fresh
    target, before ``occupant_primary``'s replica claims the freed slot.
    This is a rotation, not a retry loop: ``_seen`` accumulates visited
    ``new_host_id``s across the recursion purely as a cycle backstop against
    a topology bug; the rotation itself is always finite, since each hop
    heads toward the one slot the original removal freed.

    Create-before-destroy: the new replica is built on ``new_host_id``
    BEFORE the old one on ``old_host_id`` is torn down, so
    ``occupant_primary`` never has zero surviving copies -- critical on
    FTT1 (no tertiary): a cluster only tolerates one node down at a time,
    and that budget belongs to the node actually being removed, not to
    whatever healthy node this splice happens to touch. (2026-08-07
    incident: the old destroy-then-build order tore down the occupant's
    only copy up front; a hublvol attach failure on the rebuild then
    retried for minutes with that copy already gone.) A raised exception
    from the rebuild is treated the same as a returned False -- both leave
    the old copy untouched and safe to retry.

    Idempotent and retry-safe: "already built" is read from the occupant's
    forward pointer, so a retry after a confirmed build skips straight to
    the teardown check without re-running the rebuild. The teardown itself
    is guarded separately by ``old_host``'s own back-reference (not the
    occupant's forward pointer), so a crash between the two commits still
    resumes the teardown on the next pass instead of leaking a stale
    replica on ``old_host`` forever.

    Returns True if ``occupant_primary_id`` no longer exists — nothing left
    to relocate.
    """
    field = "secondary_node_id" if role == "secondary" else "tertiary_node_id"
    backref = "lvstore_stack_secondary" if role == "secondary" else "lvstore_stack_tertiary"
    seen = _seen if _seen is not None else set()
    try:
        occupant_primary = db_controller.get_storage_node_by_id(occupant_primary_id)
    except KeyError:
        return True

    if getattr(occupant_primary, field) != new_host_id:
        new_host = db_controller.get_storage_node_by_id(new_host_id)

        existing_occupant_id = getattr(new_host, backref)
        if existing_occupant_id and existing_occupant_id != occupant_primary_id:
            if new_host_id in seen:
                logger.error(
                    f"[REMOVAL] splice: cycle detected while vacating "
                    f"{new_host_id}'s existing {role} occupant "
                    f"{existing_occupant_id} to make room for "
                    f"{occupant_primary_id}; refusing")
                return False
            seen.add(new_host_id)
            try:
                existing_occupant = db_controller.get_storage_node_by_id(existing_occupant_id)
            except KeyError:
                existing_occupant = None
            vacate_target = (
                _pick_replica_relocation_node(
                    existing_occupant, new_host, role, db_controller,
                    # occupant_primary_id is itself mid-relocation onto
                    # new_host_id in THIS call -- it can't simultaneously be
                    # the target existing_occupant vacates onto. Excluding
                    # it upfront lets the picker search past it instead of
                    # dead-ending on the one candidate that's structurally
                    # invalid (see _pick_replica_relocation_node's
                    # extra_exclude_ids docstring).
                    extra_exclude_ids=(occupant_primary_id,))
                if existing_occupant else None)
            if not vacate_target or vacate_target == occupant_primary_id:
                logger.error(
                    f"[REMOVAL] splice: no relocation target to vacate "
                    f"{new_host_id}'s existing {role} occupant "
                    f"{existing_occupant_id}; cannot free the slot for "
                    f"{occupant_primary_id}")
                return False
            if not _relocate_replica_between(
                    existing_occupant_id, new_host_id, vacate_target, role,
                    db_controller, _seen=seen):
                logger.error(
                    f"[REMOVAL] splice: failed to vacate {new_host_id}'s "
                    f"existing {role} occupant {existing_occupant_id} onto "
                    f"{vacate_target}")
                return False
            new_host = db_controller.get_storage_node_by_id(new_host_id)

        try:
            built = recreate_lvstore_on_non_leader(new_host, occupant_primary, occupant_primary)
        except Exception as e:
            logger.error(
                f"[REMOVAL] splice: failed to build {role} replica of "
                f"{occupant_primary_id} on {new_host_id}, old copy on "
                f"{old_host_id} left untouched: {e}")
            return False
        if not built:
            return False
        occupant_primary = db_controller.get_storage_node_by_id(occupant_primary_id)
        setattr(occupant_primary, field, new_host_id)
        occupant_primary.write_to_db()

        # Record the new host's side of the relationship too. Re-fetch: the
        # build above (recreate_lvstore_on_non_leader) persists its own
        # lvstore_ports snapshot for new_host, derived only from its
        # (now-vacated) lvstore_stack_secondary/_tertiary -- it has no way to
        # know about occupant_primary, so this entry has to be added here.
        new_host = db_controller.get_storage_node_by_id(new_host_id)
        setattr(new_host, backref, occupant_primary_id)
        if not new_host.lvstore_ports:
            new_host.lvstore_ports = {}
        new_host.lvstore_ports[occupant_primary.lvstore] = {
            "lvol_subsys_port": occupant_primary.lvol_subsys_port,
            "hublvol_port": occupant_primary.hublvol.nvmf_port if occupant_primary.hublvol else 0,
        }
        new_host.write_to_db()

    # Re-point every LVol hosted on occupant_primary before the old_host
    # teardown below -- see _update_lvol_nodes_for_replica_move's docstring.
    # Unconditional (outside the "if not already built" guard above) so a
    # retry that skips straight past that guard still catches up if an
    # earlier attempt crashed between the build and this step.
    _update_lvol_nodes_for_replica_move(occupant_primary_id, old_host_id, new_host_id, db_controller)

    old_host = db_controller.get_storage_node_by_id(old_host_id)
    if getattr(old_host, backref) == occupant_primary_id:
        cluster = db_controller.get_cluster_by_id(occupant_primary.cluster_id)
        # A node's secondary replica and its tertiary replica OF THE SAME
        # PRIMARY are not two resources -- they are ONE physical stack
        # (raid0_<vuid> + LVS_<vuid>, keyed by the primary's lvstore, not by
        # the role). Vacating one role therefore must not tear that stack
        # down while the other role still needs it: _delete_replica_on_peer
        # ends in _remove_bdev_stack(remove_distr_only=True) ->
        # bdev_raid_delete(raid0_<vuid>), which hot-removes the lvstore from
        # this node outright.
        #
        # The planner (controllers/replica_placement.py) routinely emits both
        # roles of one primary in a single removal -- that is the whole point
        # of solving the layout globally, and order_moves proves each move
        # lands on a free SLOT. Slots are the wrong granularity here: for a
        # given primary, this node's secondary slot and tertiary slot map to
        # the same stack. Found live 2026-09-01 on the 12-node/4-domain FTT2
        # cluster, on the FIRST two removals and in the identical shape both
        # times -- "secondary: <removed> -> X" promoted X (already holding
        # that primary as tertiary), then "tertiary: X -> Y" deleted X's
        # raid ~1s after Y's was built:
        #   14:50:26 nq2mm bdev_raid_create raid0_45
        #   14:50:27 fvgtl bdev_raid_delete raid0_45   <- the new secondary
        # leaving pq8h9/LVS_45 and 9s25f/LVS_1 recorded as FTT2 while
        # physically down to a single replica (their tertiary), with the
        # recorded secondary holding nothing but a stranded hublvol
        # controller. Silent: every forward pointer, back-reference and
        # lvstore_ports entry still said the replica was there.
        other_backref = ("lvstore_stack_tertiary" if backref == "lvstore_stack_secondary"
                         else "lvstore_stack_secondary")
        still_hosts_other_role = getattr(old_host, other_backref) == occupant_primary_id
        if old_host.status == StorageNode.STATUS_ONLINE and not still_hosts_other_role:
            # occupant_primary survives this relocation (only its host is
            # moving) -- must NOT destroy the shared lvstore, only vacate
            # old_host's local examine copy. See _delete_replica_on_peer's
            # destroy_lvstore docstring.
            _delete_replica_on_peer(old_host, occupant_primary, cluster,
                                    destroy_lvstore=False)
            _teardown_lvol_subsystems_on_vacated_peer(old_host, occupant_primary, db_controller)
            _prune_stale_lvstore_ports(old_host_id, occupant_primary.lvstore, db_controller)
        elif still_hosts_other_role:
            # Drop only the back-reference below. The stack, its subsystems
            # and its lvstore_ports entry all stay -- they belong to the role
            # old_host still holds for this same primary.
            logger.info(
                f"[REMOVAL] {old_host_id} keeps {occupant_primary_id}'s "
                f"{occupant_primary.lvstore} stack: it still hosts that primary as "
                f"{'tertiary' if role == 'secondary' else 'secondary'}; "
                f"clearing the {role} back-reference only")
        old_host = db_controller.get_storage_node_by_id(old_host_id)
        setattr(old_host, backref, "")
        old_host.write_to_db()
    return True


def _clear_replica_backref(removed_node: StorageNode, backref):
    db_controller = DBController()
    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    if getattr(removed_node, backref):
        setattr(removed_node, backref, "")
        removed_node.write_to_db()


def _release_jm_from_jc(node, name_old) -> bool:
    """Hand ``name_old`` back to JC so its bdev can then be deleted.

    JC holds an open descriptor and IO channel on every JM it knows about;
    ``jc_remove_jm`` closes them and drops the JM's JC context, and only after
    that may the bdev be deleted. Deleting it first is what leaves JC naming a
    bdev that no longer exists.

    Returns True when the bdev is safe to delete. The interesting codes:

    * ``-13`` "not used by JC" -- ALREADY RELEASED, and the normal answer after
      a successful jc_replace_jm: measured live 2026-09-02 on spdk R26.3, a
      replace that swaps the JM out of every vuid on the node also drops it
      from JC, so the follow-up finds nothing left to do. Success, not failure.
    * ``-22`` "still in use by one or more jm_vuids" -- some vuid still
      references it, including one the control plane may be unable to
      enumerate (a vuid whose primary is already removed appears in no
      `decisions` entry and under no back-reference). Do NOT delete.
    * ``RPC_UNSUPPORTED`` -- build predates the RPC (spdk main-latest as of
      2026-09-02 does not expose it, R26.3-latest does). Fall through to the
      historical behaviour rather than stranding every superseded controller.
    """
    try:
        ret = node.rpc_client().jc_remove_jm(name_old)
    except RPCRemoteError as re:
        if re.code == JC_REMOVE_JM_NOT_USED:
            logger.info(
                f"[REMOVAL] {node.get_id()}: {name_old} already released "
                f"(jc_remove_jm -13); safe to delete the bdev")
            return True
        if re.code == JC_REMOVE_JM_STILL_IN_USE:
            logger.error(
                f"[REMOVAL] {node.get_id()}: jc_remove_jm refused {name_old} "
                f"(-22: still used by one or more jm_vuids) -- a jm_vuid this removal "
                f"could not enumerate still references the departing node's JM "
                f"(typically the removed node's OWN lvstore vuid, whose primary is gone "
                f"so it appears in no decision and under no back-reference). Leaving the "
                f"bdev in place: deleting it now would leave JC pointing at nothing")
            return False
        logger.error(
            f"[REMOVAL] {node.get_id()}: jc_remove_jm({name_old}) failed ({re.code}): "
            f"{re}; leaving the bdev in place")
        return False
    except Exception as e:
        logger.error(
            f"[REMOVAL] {node.get_id()}: jc_remove_jm({name_old}) raised: {e}; "
            f"leaving the bdev in place")
        return False

    if ret == RPC_UNSUPPORTED:
        logger.warning(
            f"[REMOVAL] {node.get_id()}: jc_remove_jm unsupported on this SPDK build; "
            f"deleting {name_old}'s controller with JC's descriptor possibly still open "
            f"(pre-existing behaviour)")
    else:
        logger.info(f"[REMOVAL] {node.get_id()}: jc_remove_jm released {name_old}")
    return True


def _drop_superseded_jm_bdev(node, name_old, removed_jm_id) -> None:
    """Detach the controller behind ``name_old`` and drop its bookkeeping.

    Only call once ``_release_jm_from_jc`` has confirmed JC is done with it.
    """
    controller = name_old[:-2] if name_old.endswith("n1") else name_old
    try:
        node.rpc_client().bdev_nvme_detach_controller(controller)
    except Exception as de:
        logger.warning(
            f"Failed to detach superseded controller {controller} on {node.get_id()}: {de}")
    node.remote_jm_devices = [
        rd for rd in (node.remote_jm_devices or []) if rd.uuid != removed_jm_id]


def _decommission_node_jm(removed_node: StorageNode, replica_peer_ids=()) -> None:
    """Patch every live JC group that referenced ``removed_node``'s JM out of
    its redundancy set, replacing it with a freshly picked candidate.

    ``replica_peer_ids``: the peers that hosted ``removed_node``'s OWN lvstore
    replica (its secondary and tertiary), captured by the caller BEFORE phase
    3a -- which clears both those pointers and the peers' back-references, so
    by the time this runs nothing in the DB records who they were. They matter
    because each of them still runs a local JC instance for ``removed_node``'s
    own jm_vuid, and that instance references the dying JM by name. It is
    reachable through neither source Pass 2 consults (its primary is
    ``removed_node``, which is not in ``live_nodes`` and therefore in no
    ``decisions`` entry; and its back-reference has been cleared), so without
    this the batched jc_replace_jm cannot cover it and jc_replace_jm's own -17
    check rejects the whole call. Reproduced live 2026-09-02: the removal's
    only failing peer was the one hosting the removed node's own lvstore.

    Called TWICE by design, both idempotent (guarded by the JM device's own
    status, set below): early, as node_removal_orchestrate's own phase 2 --
    AFTER phase 3a tears down removed_node's own hosted replicas but BEFORE
    phase 3b relocates any replica hosted ON removed_node -- and again from
    _decommission_node_devices's phase 5, as a defensive no-op for tasks
    resuming from before this function existed as a separate phase.

    Running this before phase 3b matters: relocating a hosted primary's
    replica onto a new host builds that host's JC group construct fresh via
    get_node_jm_names(), which bakes in whatever the primary's CURRENT
    jm_ids says -- unconditionally, by name, regardless of whether the
    underlying connection ever succeeds. If jm_ids still listed
    removed_node's JM at that moment (because this hadn't patched it out
    yet), the new host's construct permanently references a member it can
    never reach (removed_node is already shut down by phase 1) -- and there
    is no live connection to hand jc_replace_jm as name_old afterwards to
    fix it (found live 2026-08-25: a node relocated onto during the SAME
    removal that killed one of its new group's members could never be
    patched, no matter how many times phase 5 retried). Patching jm_ids
    first means every relocation that follows already sees the corrected
    membership from its very first build.

    Running this AFTER phase 3a matters too, the other direction: any peer
    hosting removed_node's OWN secondary/tertiary replica runs a local JC
    instance for THAT replica as well, and get_node_jm_names() always
    includes the replica's owning primary's (removed_node's) own JM by name
    in that instance's construct too -- a second, independent local jm_vuid
    on that peer sharing the exact same name_old as whatever THIS function
    is trying to patch there, one this function's target-gathering has no
    way to see (it only tracks OTHER primaries via `decisions`, never
    removed_node's own hosted replica). Left in place, jc_replace_jm's own
    multi-target safety check rejects the batched call outright (-17: "does
    not cover all jm_vuids that use name_old"), because it still finds that
    second instance live. Phase 3a tearing the replica down first removes it
    entirely, so this function never has to account for it (found live
    2026-08-25, the very next removal after the phase-2/3b fix above:
    removed_node's own hosted-replica peer failed this way).
    """
    db_controller = DBController()
    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())

    if (removed_node.jm_device and removed_node.jm_device.get_id()
            and removed_node.jm_device.status in (JMDevice.STATUS_ONLINE, JMDevice.STATUS_UNAVAILABLE)):
        logger.info(f"[REMOVAL] {removed_node.get_id()}: removing JM device")
        device_controller.remove_jm_device(removed_node.jm_device.get_id(), force=True)
        removed_jm_id = removed_node.jm_device.get_id()
        removed_fd = removed_node.failure_domain

        # get_storage_nodes_by_cluster_id returns every node regardless of
        # status, including ones already REMOVED. An already-removed node
        # can still carry the just-removed node's JM id in its own stale
        # jm_ids (never cleared on ITS removal) -- without this guard we'd
        # try to "fix" that dead node's JM connections using its own
        # rpc_client, which points at a pod that no longer exists and can
        # never resolve/connect (2026-08-11 incident: a prior removal's
        # leftover jm_ids on 7b8hf sent a later removal's phase 5 chasing
        # a permanently-dead hostname).
        # ...and removed_node itself, by identity. Its status here is
        # IN_REMOVAL, not REMOVED, so the status filter alone lets it through
        # (and the function is called twice, so its status differs between
        # calls -- identity is the only stable guard). Phase 1 has already
        # shut it down, but phase 3b has NOT yet relocated the replicas it
        # hosts for other primaries, so its lvstore_stack_secondary/_tertiary
        # still point at live primaries. Pass 2 therefore picked it up as a
        # patch target for a hosted primary's vuid and went looking for the
        # dying JM's bdev name in its own remote_jm_devices -- where a node's
        # OWN JM never appears. Found live 2026-09-03 removing s25dl:
        # "no recorded bdev name for removed JM 601dae11...,
        #  affected targets=[(1, 'a91a2d46...')]". Harmless only because the
        # missing name short-circuited the call; with a name recorded it would
        # have issued jc_replace_jm at the dead pod this filter exists to
        # avoid. IN_REMOVAL is listed too, on the same grounds as REMOVED:
        # such a node is down and its rpc_client cannot resolve.
        live_nodes = [n for n in db_controller.get_storage_nodes_by_cluster_id(removed_node.cluster_id)
                      if n.status not in (StorageNode.STATUS_REMOVED,
                                          StorageNode.STATUS_IN_REMOVAL)
                      and n.get_id() != removed_node.get_id()]

        def _pick_replacement(primary):
            # get_sorted_ha_jms ranks candidates by host-disjoint (hard) +
            # failure-domain balance (best-effort) -- it has no notion of
            # "primary already holds this candidate", so filter that here.
            candidates = get_sorted_ha_jms(primary)
            if removed_fd >= 0 and candidates:
                # Prefer a replacement from the SAME failure domain the
                # removed node was in -- keeps primary's domain distribution
                # identical to what it was before the removal (the least-
                # disruptive choice) instead of reshuffling it. Stable sort:
                # within the "matches" and "doesn't match" groups,
                # get_sorted_ha_jms' own ranking is preserved.
                def _owner_fd(jid):
                    owner_jm = db_controller.get_jm_device_by_id(jid)
                    owner = (db_controller.get_storage_node_by_id(owner_jm.node_id)
                             if owner_jm else None)
                    return owner.failure_domain if owner else -1
                candidates = sorted(candidates, key=lambda jid: _owner_fd(jid) != removed_fd)
            return next((c for c in candidates if c not in primary.jm_ids), None)

        # Pass 1: one authoritative replacement decision per PRIMARY whose
        # OWN redundancy set (jm_ids) references the dead JM. The redundancy
        # set is shared identically across every host that runs a local JC
        # instance for this jm_vuid (the primary itself, plus any secondary/
        # tertiary hosting it) -- see get_node_jm_names -- so every one of
        # them must apply this SAME decision, not pick independently.
        decisions = {}   # primary_node_id -> replacement jm_id, or None
        for primary in live_nodes:
            if primary.jm_ids and removed_jm_id in primary.jm_ids:
                decisions[primary.get_id()] = _pick_replacement(primary)

        # removed_node's OWN vuid needs a replacement too: its JC instance
        # survives on whichever peers hosted its replica (see
        # replica_peer_ids), still naming the dying JM. We are not keeping that
        # vuid useful -- its lvstore is gone -- only getting the dead JM out of
        # it, so name_old ends up referenced by nothing and jc_remove_jm can
        # release it instead of refusing with -22.
        #
        # Deliberately NOT stored in `decisions`. Every `decisions` entry means
        # "this primary's OWN redundancy set lists the dead JM", and Pass 2
        # relies on that: it keys the node's own-vuid target off membership,
        # and both jm_ids.remove() calls below assume it. Storing removed_node
        # there made Pass 2 treat it as a normal consumer and then remove an id
        # its jm_ids never held -- ValueError, phase 2 aborted mid-flight, and
        # NO peer got its jc_replace_jm at all (found live 2026-09-02: strictly
        # worse than the gap it was meant to close). removed_node is now also
        # excluded from live_nodes outright, so Pass 1 and Pass 2 cannot reach
        # it by any route; this stays as the statement of intent for the entry
        # Pass 1 would otherwise be tempted to add back.
        # removed_node's OWN lvstore group lives on as a "leftover" on its
        # secondary and tertiary: no live primary, no back-reference, and by
        # phase 2 no lvstore, raid or distribs either. It deliberately gets no
        # decision and never becomes a replace target -- see the note in Pass 2.
        logger.info(
            f"[REMOVAL] {removed_node.get_id()}: own lvstore vuid {removed_node.jm_vuid} "
            f"leftover on replica peers {list(replica_peer_ids)}; not a replace target")

        # Pass 2: a single storage node can run more than one local JC
        # instance against the removed JM's bdev at once -- its own
        # redundancy set, plus one instance per primary it hosts as
        # secondary/tertiary (found live 2026-08-24: a consumer's jc_replace_jm
        # collided with a bdev already claimed by exactly this kind of
        # hosted-replica JC membership). jc_replace_jm now requires every
        # local jm_vuid using name_old to be covered in ONE call (-17
        # otherwise), so for each node, gather every jm_vuid it needs to
        # patch and issue exactly one call.
        for node in live_nodes:
            targets = []  # (jm_vuid, owner_primary, replacement_jm_id)
            if node.get_id() in decisions:
                targets.append((node.jm_vuid, node, decisions[node.get_id()]))
            for backref in (node.lvstore_stack_secondary, node.lvstore_stack_tertiary):
                if backref and backref in decisions:
                    hosted_primary = db_controller.get_storage_node_by_id(backref)
                    targets.append((hosted_primary.jm_vuid, hosted_primary, decisions[backref]))
            # Replace and remove are MUTUALLY EXCLUSIVE on a node -- per the
            # SPDK team (2026-09-02), and forced by the two RPCs' own rules:
            #
            #   * jc_replace_jm must cover EVERY local vuid using name_old or
            #     it rejects the batch (-17). So if any surviving group uses the
            #     dead JM, the leftover group must be in that same call too --
            #     it cannot be left out and handled separately.
            #   * jc_remove_jm refuses while any vuid still uses the JM (-22).
            #     So it is only available when nothing else holds it, i.e. when
            #     the leftover group is the sole user.
            #
            # Hence: other groups present -> replace all of them plus the
            # leftover, and never call remove. Only the leftover -> remove
            # alone, and never call replace. Whether the node is secondary or
            # tertiary does not enter into it beyond determining whether it
            # carries a leftover group at all.
            # removed_node's own lvstore group is NEVER a replace target. Its
            # lvstore is being destroyed, so there is nothing to keep redundant
            # and no reason to burn a spare JM on it -- jc_replace_jm is only
            # for groups that keep running. The batch therefore covers the
            # surviving groups and nothing else, even on the secondary and
            # tertiary, which are the only nodes that carry the leftover at all.
            carries_removed_lvs = node.get_id() in replica_peer_ids

            if not targets:
                if not carries_removed_lvs:
                    # No surviving group here uses the dying JM, and this node
                    # never carried removed_node's lvstore either -- so no JC
                    # operation applies: no jc_remove_jm, no detach. Nothing on
                    # this node references the JM.
                    #
                    # The one thing still done is reconciling the DB record.
                    # remote_jm_devices is derived from three sources (an
                    # explicit jm_ids list, the node's own jm_ids, and the JM of
                    # whichever primary it hosts -- see
                    # _connect_to_remote_jm_devs); a record that none of them
                    # justifies any more is not inert, because it is the lookup
                    # that later removals use to find jc_replace_jm's name_old.
                    # A splice reshuffle can leave one behind (2026-08-14
                    # incident: a peer reachable only through the hosted-primary
                    # path, own jm_ids clean, entry never re-derived). Refresh
                    # only when such a record is actually present.
                    if any(d.uuid == removed_jm_id for d in (node.remote_jm_devices or [])):
                        node.remote_jm_devices = _connect_to_remote_jm_devs(node, node.jm_ids)
                        node.write_to_db()
                    continue

                # Secondary or tertiary, and no surviving group uses the JM:
                # removed_node's own lvstore group is the sole remaining user,
                # and jc_remove_jm is the call for it. That group was never a
                # replace target -- its lvstore is being destroyed -- so this
                # is the only place the JM gets released on this node.
                #
                # -22 would mean some group still holds it after all; the bdev
                # then stays, deliberately, and the error names why.
                old_remote_dev = next(
                    (rd for rd in (node.remote_jm_devices or []) if rd.uuid == removed_jm_id),
                    None)
                if old_remote_dev:
                    stale_bdev = old_remote_dev.remote_bdev
                    if not stale_bdev:
                        # No recorded bdev name: nothing to release and nothing
                        # to detach, so just drop the unreachable entry.
                        node.remote_jm_devices = _connect_to_remote_jm_devs(node, node.jm_ids)
                    elif _release_jm_from_jc(node, stale_bdev):
                        _drop_superseded_jm_bdev(node, stale_bdev, removed_jm_id)
                    # else: release refused. Leave BOTH the bdev and its
                    # remote_jm_devices entry alone -- dropping the entry while
                    # the bdev is still present and still held by JC is the
                    # bookkeeping-vs-reality split this sequence exists to
                    # avoid. Keep describing what is actually there.
                    node.write_to_db()
                continue

            # Capture the exact bdev name node's JC currently has live for
            # this slot BEFORE any bookkeeping changes below -- this is
            # jc_replace_jm's ``name_old``. One physical bdev serves every
            # jm_vuid target on this node, so one lookup covers them all.
            old_remote_dev = next(
                (rd for rd in (node.remote_jm_devices or []) if rd.uuid == removed_jm_id), None)
            name_old = old_remote_dev.remote_bdev if old_remote_dev else None

            replaced = False
            if name_old and all(new_jm_id is not None for _, _, new_jm_id in targets):
                # Snapshotted so it can be restored below on failure --
                # _connect_to_remote_jm_devs' delta mode needs node.remote_
                # jm_devices updated as we go (each subsequent candidate's
                # connect call carries over the previous one's untouched
                # entries), but if the batched call ultimately fails and we
                # detach whatever we just connected, that bookkeeping must
                # not keep claiming a connection that no longer exists.
                original_remote_jm_devices = list(node.remote_jm_devices or [])
                replacements = []
                connected_controllers = []  # (controller_name, pre_existing)
                connect_ok = True
                for jm_vuid, _owner, new_jm_id in targets:
                    d = db_controller.get_jm_device_by_id(new_jm_id)

                    if d.node_id == node.get_id():
                        # The picked candidate is node's OWN local JM --
                        # nothing to connect, it's already live under its
                        # natural (non-"remote_") name. _connect_to_remote_
                        # jm_devs deliberately skips self-connections (see
                        # its "org_dev_node.get_id() == this_node.get_id()"
                        # guard), so routing this case through it always
                        # raised "failed to connect" and left the slot
                        # permanently short (found live 2026-08-25: a node
                        # picked as its own hosted-primary's replacement
                        # could never actually be installed). Reference it
                        # the same way get_node_jm_names does for a local
                        # member: the plain jm_bdev name.
                        replacements.append({"jm_vuid": jm_vuid, "name_new": d.jm_bdev})
                        continue

                    controller_name = f"remote_{d.jm_bdev}"
                    expected_bdev = f"{controller_name}n1"
                    # Recorded BEFORE our own connect call below, so a
                    # failure's cleanup only ever detaches a connection THIS
                    # call made -- never one already serving some other
                    # legitimate purpose (e.g. a hosted-replica JC membership).
                    try:
                        pre_existing = bool(node.rpc_client().get_bdevs(expected_bdev))
                    except Exception:
                        pre_existing = False
                    try:
                        # Connect the candidate under its own name first --
                        # jc_replace_jm hands off to an already-live bdev,
                        # it doesn't create the connection itself.
                        connected = _connect_to_remote_jm_devs(
                            node, jm_ids=[new_jm_id], only_node_id=d.node_id)
                        new_remote_dev = next(
                            (rd for rd in connected if rd.uuid == new_jm_id), None)
                        if not new_remote_dev or not new_remote_dev.remote_bdev:
                            raise RPCException(
                                f"failed to connect replacement JM device {new_jm_id}")
                        node.remote_jm_devices = connected
                    except Exception as e:
                        logger.error(
                            f"[REMOVAL] {node.get_id()}: failed to connect replacement "
                            f"candidate {new_jm_id} for jm_vuid {jm_vuid}: {e}")
                        connect_ok = False
                        break
                    replacements.append({"jm_vuid": jm_vuid, "name_new": new_remote_dev.remote_bdev})
                    connected_controllers.append((controller_name, pre_existing))

                if connect_ok:
                    try:
                        node.rpc_client(timeout=30, retry=2).jc_replace_jm(
                            name_old=name_old, replacements=replacements)
                        logger.info(
                            f"[REMOVAL] {node.get_id()}: jc_replace_jm {name_old} replaced for "
                            f"{replacements} replacing removed JM {removed_jm_id}")
                        for _jm_vuid, owner_primary, new_jm_id in targets:
                            if owner_primary.get_id() == node.get_id():
                                # Membership-conditional: a target can exist for
                                # a vuid whose OWNER is not this node, and an
                                # unguarded remove() throws ValueError and
                                # aborts the whole phase mid-node, leaving every
                                # peer after it unpatched (found live
                                # 2026-09-02).
                                if removed_jm_id in node.jm_ids:
                                    node.jm_ids.remove(removed_jm_id)
                                if new_jm_id not in node.jm_ids:
                                    node.jm_ids.append(new_jm_id)
                        replaced = True
                    except Exception as e:
                        logger.error(
                            f"[REMOVAL] {node.get_id()}: jc_replace_jm failed replacing "
                            f"{name_old} ({replacements}): {e}")

                if replaced:
                    # jc_replace_jm only swaps the live membership pointer, it
                    # never tears down the bdev/controller it swapped AWAY
                    # from, and _connect_to_remote_jm_devs' delta mode above
                    # only ever carries a different-owner entry over
                    # untouched, never drops it. Left alone this dangles
                    # forever, pointing at a node that no longer exists:
                    # sbctl cluster check walks remote_jm_devices and flags
                    # it as a failed bdev probe, and the underlying NVMe-oF
                    # session itself was observed staying connected for
                    # ~15+ minutes until some unrelated SPDK-side dead-peer
                    # timeout eventually noticed (found live 2026-08-25).
                    # Clean up both sides here instead of waiting for that.
                    #
                    # No release here, on any node. jc_replace_jm and
                    # jc_remove_jm are alternatives, never a sequence:
                    #
                    #   * this branch means at least one SURVIVING group used
                    #     the JM and has just been repointed. removed_node's
                    #     own lvstore group is never in that batch -- it is
                    #     being destroyed, not repaired, so a replacement
                    #     member would buy nothing.
                    #   * the release belongs to the other case only: a node
                    #     where NO surviving group used the JM, handled in the
                    #     no-targets branch above.
                    #
                    # Measured live 2026-09-02 on spdk R26.3: after any
                    # successful replace jc_remove_jm answers -13 ("not used
                    # by JC") on every node, so calling it here would be a
                    # guaranteed no-op.
                    _drop_superseded_jm_bdev(node, name_old, removed_jm_id)

                if not replaced:
                    for controller_name, pre_existing in connected_controllers:
                        if not pre_existing:
                            try:
                                node.rpc_client().bdev_nvme_detach_controller(controller_name)
                            except Exception as de:
                                logger.warning(
                                    f"Failed to detach unused controller "
                                    f"{controller_name} on {node.get_id()}: {de}")
                    # Whatever got connected along the way is either
                    # unwound above (fresh, now detached) or was already
                    # legitimately live before this attempt -- either way
                    # node.remote_jm_devices must not end up claiming a
                    # connection that this failed attempt just tore down.
                    node.remote_jm_devices = original_remote_jm_devices

            if not replaced:
                # Every jm_vuid target on this node either had no candidate,
                # or the connect/replace itself failed -- leave the
                # redundancy slot honestly short rather than claim a
                # replacement that isn't actually live in JC. Nothing
                # currently revisits this automatically; it stays a visible
                # gap until a future removal/reconnect cycle retries it.
                if not name_old:
                    # This single boolean (no entry in remote_jm_devices for
                    # removed_jm_id) can't by itself distinguish WHY: the
                    # physical connection may simply never have been made
                    # for this node (e.g. it picked up this jm_vuid via a
                    # relocation/splice whose soft-reconnect prelude never
                    # ran for it -- see _recreate_lvstore_on_non_leader_impl,
                    # 2026-08-25), or a connection existed and was dropped by
                    # something else entirely. jm_ids (checked in Pass 1/2
                    # above) already confirms this node's OWN redundancy
                    # membership does reference removed_jm_id -- that part
                    # is not in question. Log the actual remote_jm_devices
                    # state so a live occurrence is diagnosable without
                    # re-deriving it from scratch (2026-08-28 finding): an
                    # entirely empty list points at "never connected in the
                    # first place"; a non-empty list missing only this uuid
                    # points at something explicitly removing/skipping it.
                    current_remote_uuids = [rd.uuid for rd in (node.remote_jm_devices or [])]
                    logger.error(
                        f"[REMOVAL] {node.get_id()}: no recorded bdev name for removed "
                        f"JM {removed_jm_id}; cannot call jc_replace_jm -- "
                        f"node.jm_ids={node.jm_ids}, "
                        f"remote_jm_devices uuids={current_remote_uuids} "
                        f"({'never connected to any remote JM' if not current_remote_uuids else 'connected to other JM(s) but not this one'}), "
                        f"affected targets={[(jm_vuid, owner_primary.get_id()) for jm_vuid, owner_primary, _ in targets]}")
                elif any(new_jm_id is None for _, _, new_jm_id in targets):
                    logger.error(
                        f"[REMOVAL] {node.get_id()}: no replacement candidate for jm_vuid(s) "
                        f"{[jm_vuid for jm_vuid, _, new_jm_id in targets if new_jm_id is None]}")
                if node.get_id() in decisions and removed_jm_id in node.jm_ids:
                    # Guarded for the same reason as the success path above: an
                    # unguarded remove() here threw ValueError and aborted
                    # phase 2 before any peer was patched (found live
                    # 2026-09-02).
                    node.jm_ids.remove(removed_jm_id)
            node.write_to_db()


def _decommission_node_devices(removed_node: StorageNode):
    """Remove, fail and migrate every data device on ``removed_node``.

    Drives each device ONLINE/UNAVAILABLE -> REMOVED -> FAILED (which queues the
    failure-migration tasks on the surviving online nodes), then waits for them
    all to reach FAILED_AND_MIGRATED. Returns True only once every data device
    is migrated; False means "still migrating, retry later".

    Also (re)runs _decommission_node_jm -- see its own docstring for why this
    is a defensive no-op here on any task that already ran it as phase 2."""
    db_controller = DBController()
    _decommission_node_jm(removed_node)

    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    for dev in removed_node.nvme_devices:
        if dev.status in (NVMeDevice.STATUS_JM, NVMeDevice.STATUS_FAILED_AND_MIGRATED):
            continue
        if dev.status in (NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_UNAVAILABLE):
            # force=True tolerates the dead local SPDK (it was killed at shutdown);
            # the meaningful work — disconnect from peers + DB state — still runs.
            # device_controller.device_remove(dev.get_id(), force=True)
            device_controller.device_set_state(dev.get_id(), NVMeDevice.STATUS_REMOVED)

        fresh = db_controller.get_storage_device_by_id(dev.get_id())
        if fresh.status == NVMeDevice.STATUS_REMOVED:
            device_controller.device_set_failed(dev.get_id())

    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    for dev in removed_node.nvme_devices:
        if dev.status in (NVMeDevice.STATUS_JM, NVMeDevice.STATUS_FAILED_AND_MIGRATED):
            continue
        logger.info(
            f"[REMOVAL] {removed_node.get_id()}: device {dev.get_id()} "
            f"status={dev.status}, migration not complete"
        )

    return True


def _finalize_node_removal(removed_node: StorageNode):
    """Best-effort host cleanup before the node is flipped to REMOVED: leave the
    docker swarm and wipe GPT partitions. Mirrors the tail of the legacy
    offline-removal path."""
    db_controller = DBController()
    removed_node = db_controller.get_storage_node_by_id(removed_node.get_id())
    cluster = db_controller.get_cluster_by_id(removed_node.cluster_id)

    # Case A/B (_teardown_replicas_of_primary, _relocate_replicas_hosted_on)
    # already cleared this node's forward/back-reference fields as they
    # relocated each side of the relationship elsewhere. lvstore_ports is
    # the one piece of bookkeeping neither touches -- it isn't part of any
    # relocation, just a port-reuse cache for THIS node's own restarts (see
    # recreate_lvstore_on_non_leader) -- and by the time this function runs
    # there won't be one: the node is about to flip to REMOVED for good.
    # Left uncleared, `sn list`'s "LVS Ports" column keeps showing entries
    # for a node with no SPDK process left to back them (2026-08-13, found
    # live after a removal).
    if removed_node.lvstore_ports:
        removed_node.lvstore_ports = {}
        removed_node.write_to_db()

    if cluster.mode == "docker":
        logger.info("Leaving swarm...")
        try:
            cluster_docker = utils.get_docker_client(removed_node.cluster_id)
            for node in cluster_docker.nodes.list():
                if node.attrs["Status"] and removed_node.mgmt_ip in node.attrs["Status"]["Addr"]:
                    node.remove(force=True)
        except Exception:
            pass

    try:
        if health_controller._check_node_api(removed_node):
            logger.info("Stopping SPDK container")
            snode_api = removed_node.client(timeout=20)
            snode_api.spdk_process_kill(removed_node.rpc_port, removed_node.cluster_id)
            snode_api.leave_swarm()
            pci_address = []
            for dev in removed_node.nvme_devices:
                if dev.pcie_address not in pci_address:
                    ret = snode_api.delete_dev_gpt_partitions(dev.pcie_address)
                    logger.debug(ret)
                    pci_address.append(dev.pcie_address)
    except Exception as e:
        logger.exception(e)

    logger.info("done")


def restart_storage_node(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None, set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0,
        force=False, node_address=None, reattach_volume=False, clear_data=False, new_ssd_pcie=[],
        force_lvol_recreate=False, spdk_proxy_image=None, current_restart_task_id=None):
    """Wrapper that guarantees the node is reset to OFFLINE if the restart
    fails after THIS call set the RESTARTING status. Without this, any
    ``return False`` inside the inner logic leaves the node pinned in
    STATUS_RESTARTING, which blocks all future restart attempts.

    The cleanup is gated on pre-call status. The earlier version of this
    wrapper unconditionally wrote OFFLINE whenever the post-call status was
    RESTARTING, which corrupted concurrent in-flight restarts: a CLI retry
    bails fast in `_restart_storage_node_impl` (status != OFFLINE → return
    False) without acquiring the lock, but the wrapper would still see
    RESTARTING (held by the auto-restart task) and clobber it with OFFLINE.
    Peers then saw the node as OFFLINE while the running restart was still
    progressing, and `health_controller` flipped that node's local devices
    to UNAVAILABLE — leaving them stuck once the restart completed because
    the device-online block in the impl had already executed earlier.

    Pre-status of RESTARTING or IN_SHUTDOWN means another caller owns the
    transition; we must not clean up after them. Any other pre-status means
    the only way post-call status can be RESTARTING is that THIS call's
    `try_set_node_restarting` acquired the lock and a subsequent step
    failed — that's the case the cleanup is for."""
    # Refuse an over-cap max_lvol before touching node status: a restart is
    # the one path that can raise an existing node's limit, and the impl
    # applies it (snode.max_lvol = max_lvol) without any ceiling of its own.
    if max_lvol and max_lvol > constants.MAX_SUBSYSTEMS_PER_NODE:
        logger.error(f"max_lvol {max_lvol} exceeds the maximum of "
                     f"{constants.MAX_SUBSYSTEMS_PER_NODE} subsystems per storage node")
        return False

    db_ctrl = DBController()
    pre_status = None
    _snode_pre = None
    try:
        _snode_pre = db_ctrl.get_storage_node_by_id(node_id)
        pre_status = _snode_pre.status
    except Exception:
        logger.warning(f"Could not read pre-call status for {node_id}; "
                       f"skipping orphan-RESTARTING cleanup as a precaution")

    # Transferable ownership: ensure a persistent NODE_RESTART task exists,
    # claim its lease for this host, and heartbeat it while this process
    # drives the restart. If this process dies mid-restart (pod evicted while
    # its host drains, CLI killed), the lease goes stale within
    # TASK_LEASE_TTL_SEC and a live tasks-runner claims the task and resumes
    # the restart — instead of the node staying orphaned in RESTARTING and
    # deadlocking node drains (2026-07-04 MCO rollout incident). On success
    # the ONLINE transition auto-cancels the task. Pre-status RESTARTING /
    # IN_SHUTDOWN means another caller owns the transition — don't touch
    # its task, and don't add our own.
    #
    # Only self-create when the caller did not already hand us a task id:
    # the task runner (tasks_runner_restart) already owns a NODE_RESTART
    # task when it drives this restart and passes it in via
    # current_restart_task_id — creating a second one here would be
    # redundant and would fight the runner's own task for the lease.
    # Per-node restart claim token: identifies THIS driver (CLI process or
    # task-runner thread) for the cross-actor mutual exclusion enforced in
    # try_set_node_restarting's FDB tx. The heartbeat below keeps the claim
    # fresh while the (potentially minutes-long) restart runs; it lands only
    # once the impl has actually acquired the claim (owner-matched CAS is a
    # no-op before that). Released in the finally block on every exit path.
    _claim_token = _new_restart_claim_token()
    _claim_hb_stop = threading.Event()

    def _claim_heartbeat():
        while not _claim_hb_stop.wait(constants.RESTART_CLAIM_HEARTBEAT_SEC):
            try:
                db_ctrl.refresh_node_restart_claim(node_id, _claim_token)
            except Exception as hb_e:
                logger.debug(f"Restart claim heartbeat failed for {node_id}: {hb_e}")
    threading.Thread(target=_claim_heartbeat, daemon=True).start()

    _hb_stop = threading.Event()
    _owned_task_id = None
    if current_restart_task_id is None and pre_status not in (
            StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN, None):
        try:
            from simplyblock_core.controllers import tasks_controller
            # Reuse the node read for pre_status above — pre_status is only
            # non-None (and thus inside this block) when that read succeeded,
            # so _snode_pre is populated. Re-fetching here would be a wasted
            # FDB round-trip and, more subtly, breaks callers/tests that count
            # get_storage_node_by_id calls against the wrapper's contract.
            _task_id = tasks_controller.ensure_node_restart_task(_snode_pre)
            _hb_task = db_ctrl.get_task_by_id(_task_id) if _task_id else None
            if _hb_task and tasks_controller.claim_task(_hb_task):
                # We hold this task's lease: it is OUR ownership token, not a
                # competing restart. Passed to the impl so its active-task
                # guard does not abort on the very task this call created
                # (observed 2026-07-17: every manual `sn restart` failed
                # inline with "Restart task found: <own task>" and degraded
                # into waiting for the task runner). Only set when the claim
                # succeeded — if another live host owns the lease, the guard
                # must keep rejecting us.
                _owned_task_id = _task_id
                def _lease_heartbeat():
                    while not _hb_stop.wait(constants.TASK_LEASE_HEARTBEAT_SEC):
                        try:
                            if not tasks_controller.refresh_task_lease(_hb_task):
                                # Lost the lease (another host took over) —
                                # stop heartbeating; the node-status lock
                                # still serializes the actual restart work.
                                return
                        except Exception as hb_e:
                            logger.debug(f"Restart lease heartbeat failed for {node_id}: {hb_e}")
                threading.Thread(target=_lease_heartbeat, daemon=True).start()
        except Exception as e:
            logger.warning(f"Could not set up transferable restart ownership for {node_id}: {e}")

    result = False
    try:
        result = _restart_storage_node_impl(
            node_id, max_lvol=max_lvol, max_snap=max_snap, max_prov=max_prov,
            spdk_image=spdk_image, set_spdk_debug=set_spdk_debug,
            small_bufsize=small_bufsize, large_bufsize=large_bufsize,
            force=force, node_address=node_address, reattach_volume=reattach_volume,
            clear_data=clear_data, new_ssd_pcie=new_ssd_pcie,
            force_lvol_recreate=force_lvol_recreate, spdk_proxy_image=spdk_proxy_image,
            current_restart_task_id=current_restart_task_id or _owned_task_id,
            restart_claim_token=_claim_token)
    except Exception:
        # exc_info so the traceback is captured: without it a failing restart
        # only logs this one line, leaving the actual raise point (e.g. a
        # remote-JM/device connect timing out when a same-failure-domain peer
        # is also down) undiagnosable from the logs.
        logger.error("restart_storage_node raised unexpectedly", exc_info=True)
    finally:
        _hb_stop.set()
        # Trust the DB. If the impl raised after the ONLINE write was
        # already committed, the node IS factually online — peers see
        # ONLINE, IO is being served — and the only thing that "failed"
        # was a post-flip side-effect that bubbled an exception. Treating
        # that as failure caused the iteration-77 hang where the script
        # spent 8 minutes retrying restarts of an already-online node.
        try:
            post_node = db_ctrl.get_storage_node_by_id(node_id)
            if not result and post_node.status == StorageNode.STATUS_ONLINE:
                logger.warning(
                    f"Restart of {node_id} returned False but DB shows ONLINE; "
                    f"trusting the DB and treating as success."
                )
                result = True
            elif (not result and pre_status not in (StorageNode.STATUS_RESTARTING,
                                                    StorageNode.STATUS_IN_SHUTDOWN,
                                                    None)
                    and post_node.restart_claim_owner == _claim_token):
                # We owned the lock — proven by the claim token, not inferred
                # from pre_status: "pre_status was OFFLINE" also holds when
                # the impl was REFUSED before acquisition (peer gate, claim
                # gate, entry checks). Running this cleanup then killed the
                # SPDK container of whichever OTHER actor was legitimately
                # mid-restart (2026-08-06 soak iter-50: a refused CLI
                # attempt's cleanup destroyed the task runner's 14-second-old
                # container). With the claim check, cleanup runs only when
                # THIS call's try_set_node_restarting acquisition committed
                # and the impl failed after it. Reset to OFFLINE regardless
                # of current status — a failed restart can leave RESTARTING,
                # but it can also leave intermediate states; OFFLINE is the
                # only safe wedge-free landing for the next retry.
                logger.warning(
                    f"Restart of {node_id} failed (post-status={post_node.status}); "
                    f"resetting to OFFLINE to unblock future attempts"
                )

                # Abort contract: SPDK MUST be killed on every failed
                # restart that owned the lock, so the next attempt starts
                # from a clean process. Without this, _restart_storage_node_impl
                # has ~20 different `return False` paths (per-device setup,
                # examine, subsystem create, listener add, remote-dev
                # connect, etc.) that all leave SPDK running with whatever
                # bdevs the impl already set up — causing the next attempt
                # to fail on "Duplicate bdev name for manual examine:
                # raid0_<vuid>" / "Subsystem NQN ... already exists" and
                # loop forever (incident 2026-05-10, b278fd62 restart
                # attempts 1–3). Routing every owned-lock failure through
                # _kill_spdk_until_dead closes those gaps in one place.
                # Idempotent: a fast no-op when SPDK was never started in
                # this attempt. Inner abort paths (recreate_lvstore's
                # _abort_restart_and_unblock, restart_storage_node's
                # _abort_restart) emit the snode_restart_failed event
                # already; the wrapper does NOT re-emit it to avoid
                # duplicate events and to avoid the FDB write that
                # `snode_restart_failed` performs unconditionally (which
                # would raise SystemExit through base_model.write_to_db
                # on hosts without FDB — the wrapper must not depend on
                # FDB liveness for cleanup correctness).
                try:
                    _kill_spdk_until_dead(post_node)
                except Exception as kill_exc:
                    logger.error(
                        f"Restart cleanup: kill SPDK on {node_id} raised: {kill_exc}"
                    )

                # Force the OFFLINE write — bypass the state-machine guard
                # in set_node_status (which only restricts ONLINE writes
                # anyway, but we use a direct write here to avoid any
                # second-order effects from the helper).
                post_node.status = StorageNode.STATUS_OFFLINE
                post_node.updated_at = str(datetime.datetime.now(datetime.timezone.utc))
                post_node.online_since = ""
                # This would disable adding further node restart tasks.
                # if this restart was because of a restart task, then the same task would continue,
                # but if the restart because of manual restart, then no task restart would be created.
                post_node.auto_restart_disabled = True
                post_node.write_to_db(db_ctrl.kv_store)
                storage_events.snode_status_change(
                    post_node, StorageNode.STATUS_OFFLINE, post_node.status,
                    caused_by="restart_cleanup")
                distr_controller.send_node_status_event(post_node, StorageNode.STATUS_OFFLINE)

                # Failback compensation. The restart impl demotes this primary's
                # first_sec to non_optimized (trigger_ana_failback_for_node) in
                # anticipation of the primary resuming leadership. Since the
                # restart FAILED and the node is now OFFLINE, that demotion would
                # otherwise leave the LVS with NO optimized path — the
                # 2026-06-03 LVS_8720 zero-leader outage, where the primary's
                # SPDK was killed mid-restart just after the surviving secondary
                # had been handed leadership back. Re-promote the secondary so it
                # serves IO again. Idempotent; a no-op for non-primary nodes or
                # an offline first_sec.
                try:
                    trigger_ana_failover_for_node(post_node)
                except Exception as ana_exc:
                    logger.error(
                        f"Restart cleanup: re-promoting secondary (ANA failover) "
                        f"for {node_id} raised: {ana_exc}"
                    )
            elif not result:
                # Failed WITHOUT holding the claim: this call was refused
                # before acquisition (peer gate, claim held by another live
                # driver, entry checks). Nothing here is ours to clean —
                # killing SPDK or flipping OFFLINE would sabotage whichever
                # actor legitimately owns the node's transition.
                logger.info(
                    f"Restart of {node_id} failed without acquiring the "
                    f"restart claim (holder: "
                    f"{post_node.restart_claim_owner or 'none'}); skipping "
                    f"SPDK/status cleanup — not this call's transition")
        except Exception as cleanup_exc:
            logger.error(f"Failed to reset node {node_id} after failed restart: {cleanup_exc}")
        finally:
            _claim_hb_stop.set()
            try:
                db_ctrl.release_node_restart_claim(node_id, _claim_token)
            except Exception as rel_exc:
                logger.debug(f"Restart claim release for {node_id} failed: {rel_exc}")
    return result


def fd_dead_recovery_allowed(db_controller, snode: StorageNode) -> bool:
    """Same-failure-domain parallel restart carve-out, reinstated with a hard gate.

    Concurrent node restarts outside a drained SUSPENDED cluster are sanctioned
    in exactly one situation: the target node's ENTIRE failure domain is dead
    (no member ONLINE). Restarting its members concurrently cannot reduce
    served availability — every member is already out of service — and the
    surviving domains' IO is protected by the connect-storm fixes (targeted
    device-connect updates, bounded connect retries, coverage gate) plus
    _remote_connect_gate serializing the cross-node reconnect phase.

    Hard gates, all on a fresh read:
    - the cluster has >= 2 distinct failure domains among its nodes (with a
      single/unset domain this predicate would degenerate to "whole cluster
      down", which is the drained-suspension path's job);
    - no member of the target's domain is ONLINE;
    - no node OUTSIDE the target's domain is RESTARTING / IN_SHUTDOWN
      (cross-domain concurrency stays forbidden).

    The 2026-07-16 contract violation was parallel in_restart while the same
    domain was still SERVING (DEGRADED cluster, domain partially online); the
    no-ONLINE-member gate makes that state ineligible by construction.

    Evaluated OUTSIDE the restart-guard FDB tx by design: the guard tx must
    stay a single-row point read/write — fat node rows plus concurrent
    acquisitions drove the FDB 1031 timeout storms (2026-07-16/17). Same
    pattern as suspend_drain_complete, which is also read outside the tx.
    The residual race (a domain member flipping ONLINE between this check and
    lock acquisition) is bounded: the restarting set is already offline, and
    reconnects are serialized by _remote_connect_gate. Any error → strict.
    """
    try:
        nodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
        if len({n.failure_domain for n in nodes}) < 2:
            return False
        for n in nodes:
            if n.get_id() == snode.get_id():
                continue
            if n.failure_domain == snode.failure_domain:
                if n.status == StorageNode.STATUS_ONLINE:
                    return False
            elif n.status in (StorageNode.STATUS_RESTARTING,
                              StorageNode.STATUS_IN_SHUTDOWN):
                return False
        return True
    except Exception as e:
        logger.warning(
            "fd_dead_recovery predicate failed for %s, staying strict: %s",
            snode.get_id(), e)
        return False


def _new_restart_claim_token():
    """Unique per-driver token for the per-node restart claim. Hostname+pid
    identify the driving process; the uuid suffix disambiguates threads and
    pid reuse. Distinct tokens for the CLI and the task-runner service even
    when both run on the mgmt host — which is exactly the case the
    hostname-keyed task lease cannot distinguish."""
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _restart_storage_node_impl(
        node_id, max_lvol=0, max_snap=0, max_prov=0,
        spdk_image=None, set_spdk_debug=None,
        small_bufsize=0, large_bufsize=0,
        force=False, node_address=None, reattach_volume=False, clear_data=False, new_ssd_pcie=[],
        force_lvol_recreate=False, spdk_proxy_image=None, current_restart_task_id=None,
        restart_claim_token=""):
    db_controller = DBController()
    logger.info("Restarting storage node")
    if not restart_claim_token:
        # Direct callers (tests, legacy paths) that bypass the wrapper still
        # get a claim; without the wrapper there is no heartbeat, so a hung
        # direct call becomes takeover-able after RESTART_CLAIM_TTL_SEC.
        restart_claim_token = _new_restart_claim_token()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_OFFLINE and force is False:
        logger.error(f"Node must be offline: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_REMOVED:
        logger.error(f"Can not restart removed node: {node_id}")
        return False

    if snode.status == StorageNode.STATUS_RESTARTING:
        logger.error(f"Node is in restart: {node_id}")
        if force is False:
            return False
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status == Cluster.STATUS_IN_ACTIVATION:
        logger.error("Cluster is in activation status, can not restart node")
        return False

    # Guard: atomically check no peer is restarting/shutting down and set RESTARTING.
    # Uses a single FDB transaction to prevent TOCTOU race conditions.
    #
    # current_restart_task_id: either the task-runner's own NODE_RESTART task
    # (passed in when it drives this restart) or the restart_storage_node
    # wrapper's self-created ownership token for a caller with no task
    # context (manual `sn restart`) — either way, that task is THIS restart,
    # not a competing one. Aborting on it made every manual restart fail
    # against its own token (2026-07-17). A task owned by anyone else still
    # blocks; get_active_node_restart_task returns the bare task uuid, so
    # callers must pass the bare uuid here too (not JobSchedule.get_id()'s
    # composite cluster/date/uuid key) or this comparison never matches.
    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id and task_id != current_restart_task_id:
        logger.error(f"Restart task found: {task_id}, can not restart storage node")
        if force is False:
            return False

    logger.info("Pre-restart check: FDB transaction to verify no peer in restart/shutdown")
    # Suspended-cluster recovery: once the drain has completed
    # (suspend_drain_complete certifies every non operator-stopped node went
    # OFFLINE), no client IO is flowing and concurrent node restarts cannot
    # violate FTT. Skip the peer-exclusion predicate so the restart task
    # runner can fan restarts out in parallel; the _remote_connect_gate below
    # still serializes the cross-node connection re-establishment. Online
    # clusters — and operator-caused suspensions, which never drain and whose
    # survivors still serve IO — keep strict one-restart-at-a-time semantics.
    allow_concurrent_peers = (cluster.status == Cluster.STATUS_SUSPENDED
                              and cluster.suspend_drain_complete)
    # Second sanctioned relaxation: the target's whole failure domain is dead
    # (fd_dead_recovery_allowed — no domain member ONLINE, no cross-domain
    # restart in flight). The former same_fd_of carve-out that allowed
    # same-domain peers to restart in parallel while the domain was still
    # SERVING (2026-07-16 violation) remains removed; this gate only opens
    # once the domain serves nothing, so recovery of a fully-rebooted domain
    # fans out instead of paying 16 x single-restart serially (2026-07-17).
    if not allow_concurrent_peers:
        allow_concurrent_peers = fd_dead_recovery_allowed(db_controller, snode)
    acquired, reason = db_controller.try_set_node_restarting(
        snode.cluster_id, node_id, allow_concurrent_peers=allow_concurrent_peers,
        claim_owner=restart_claim_token)
    if not acquired:
        logger.error(f"Cannot restart {node_id}: {reason}")
        return False
    snode = db_controller.get_storage_node_by_id(node_id)

    if  node_address == snode.api_endpoint:
        node_address = None

    if node_address:
        logger.info(f"Restarting on new node with ip: {node_address}")
        snode_api = SNodeClient(node_address, timeout=5 * 60, retry=3)
        node_info, _ = snode_api.info()
        if not node_info:
            logger.error("Failed to get node info!")
            return False
        snode.api_endpoint = node_address
        snode.mgmt_ip = utils.resolve_address(node_address)
        data_nics = []
        for nic in snode.data_nics:
            if_name = nic["if_name"]
            device = node_info['network_interface'][if_name]
            data_nics.append(
                IFace({
                    'uuid': str(uuid.uuid4()),
                    'if_name': if_name,
                    'ip4_address': device['ip'],
                    'status': device['status'],
                    'net_type': device['net_type']}))
        snode.data_nics = data_nics
        snode.hostname = node_info['hostname']

        if snode.num_partitions_per_dev == 0 and reattach_volume:
            new_cloud_instance_id = node_info['cloud_instance']['id']
            detached_volumes = node_utils.detach_ebs_volumes(snode.cloud_instance_id)
            if not detached_volumes:
                logger.error("No volumes with matching tags were detached.")
                return False

            attached_volumes = node_utils.attach_ebs_volumes(new_cloud_instance_id, detached_volumes)
            if not attached_volumes:
                logger.error("Failed to attach volumes.")
                return False

            snode.cloud_instance_id = new_cloud_instance_id
            known_sn = [dev.serial_number for dev in snode.nvme_devices]
            if snode.jm_device and 'serial_number' in snode.jm_device.device_data_dict:
                known_sn.append(snode.jm_device.device_data_dict['serial_number'])

            node_info, _ = snode_api.info()
            for dev in node_info['nvme_devices']:
                if dev['serial_number'] in known_sn:
                    snode_api.bind_device_to_spdk(dev['address'])

    # Pre-flight: if the node agent (SNodeAPI) is unreachable, fail this restart
    # attempt fast and let the task runner reschedule, instead of wedging in the
    # upcoming info()/spdk_process_start RPC retry+backoff (incident 2026-06-03
    # LVS_8720: spdk_process_start against an unreachable vm202 agent retried for
    # ~8 minutes, holding the restart task and a peer-port block the whole time).
    # An unreachable agent means the host cannot host SPDK anyway, so there is
    # nothing to start here.
    from simplyblock_core.controllers import health_controller
    if not health_controller._check_node_api(snode):
        logger.error(
            "Node agent for %s is unreachable; aborting this restart attempt "
            "(task runner will retry)", snode.get_id())
        return False

    active_tcp = False
    active_rdma = False
    fabric_tcp = cluster.fabric_tcp
    fabric_rdma = cluster.fabric_rdma
    snode_api = snode.client(timeout=5 * 60, retry=3)
    for nic in snode.data_nics:
        if fabric_rdma and snode_api.ifc_is_roce(nic["if_name"]):
            nic.trtype = "RDMA"
            active_rdma = True
            if fabric_tcp and snode_api.ifc_is_tcp(nic["if_name"]):
                active_tcp = True
        elif fabric_tcp and snode_api.ifc_is_tcp(nic["if_name"]):
            nic.trtype = "TCP"
            active_tcp = True
    snode.active_tcp = active_tcp
    snode.active_rdma = active_rdma

    logger.info(f"Restarting Storage node: {snode.mgmt_ip}")
    node_info, _ = snode_api.info()
    logger.debug(f"Node info: {node_info}")

    logger.info("Restarting SPDK")

    # Cluster-level SPDK sizing is adopted here: a restart is exactly when a
    # node is meant to pick up a changed cluster setting. An explicit argument
    # (internal callers only -- the CLI no longer offers one) still wins.
    cluster_max_subsys = int(getattr(cluster, "max_subsys", 0) or 0)
    cluster_hp_mem = int(getattr(cluster, "hugepages_mem", 0) or 0)
    cluster_vcpu_count = int(getattr(cluster, "spdk_vcpu_count", 0) or 0)
    if not max_lvol and cluster_max_subsys:
        max_lvol = cluster_max_subsys
    if not max_prov and cluster_hp_mem:
        max_prov = cluster_hp_mem

    if max_lvol and max_lvol != snode.max_lvol:
        # Occupancy guard: never adopt a subsystem limit below what the node
        # already serves. Restart recreates every existing subsystem
        # record-driven, so a smaller max_lvol would size huge pages and
        # iobuf pools for fewer subsystems than actually come back
        # (undersized SPDK). Typical trigger: `cluster update --max-subsys`
        # on an upgraded cluster whose legacy nodes carry larger, divergent
        # per-node values from before the setting became cluster-wide.
        current_subsys = lvol_controller.count_lvol_subsystems(snode)
        if max_lvol < current_subsys:
            logger.error(
                f"Refusing max_lvol {max_lvol} on node {snode.get_id()}: the "
                f"node already serves {current_subsys} subsystems; the lowest "
                f"adoptable value is {current_subsys}. Existing subsystems "
                f"are never torn down by a limit change.")
            return False

    if not utils.vcpu_requirement_met(snode.cpu, cluster_vcpu_count):
        # Same rule as add-node: refuse rather than run SPDK on fewer cores
        # than the cluster asks for, and keep one core for the system.
        logger.error(
            "Node %s has %s vCPU(s); this cluster requires %d for SPDK plus at "
            "least one for the system. Refusing the restart.",
            node_id, snode.cpu, cluster_vcpu_count)
        return False

    lvol_changed = bool(max_lvol) and max_lvol != snode.max_lvol
    if lvol_changed:
        snode.max_lvol = max_lvol
    if max_snap:
        snode.max_snap = max_snap

    if not snode.l_cores:
        if node_info.get("nodes_config") and node_info["nodes_config"].get("nodes"):
            nodes = node_info["nodes_config"]["nodes"]
            for node in nodes:
                if node['cpu_mask'] == snode.spdk_cpu_mask:
                    snode.l_cores = node['l-cores']
                    break

    if max_prov > 0:
        try:
            max_prov = int(utils.parse_size(max_prov))
            snode.max_prov = max_prov
        except Exception as e:
            logger.debug(e)
            logger.error(f"Invalid max_prov value: {max_prov}")
            return False
    else:
        max_prov = snode.max_prov

    if spdk_image:
        snode.spdk_image = spdk_image

    # Calculate minimum huge page memory
    minimum_hp_memory = utils.calculate_minimum_hp_memory(snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
                                                          snode.max_lvol,
                                                          max_prov,
                                                          len(utils.hexa_to_cpu_list(snode.spdk_cpu_mask)))

    minimum_hp_memory = max(minimum_hp_memory, max_prov)

    # check for memory
    if "memory_details" in node_info and node_info['memory_details']:
        memory_details = node_info['memory_details']
        logger.info("Node Memory info")
        logger.info(f"Total: {utils.humanbytes(memory_details['total'])}")
        logger.info(f"Free: {utils.humanbytes(memory_details['free'])}")
        logger.info(f"Minimum required huge pages memory is : {utils.humanbytes(minimum_hp_memory)}")
    else:
        logger.error("Cannot get memory info from the instance.. Exiting")
        return False

    # Calculate minimum sys memory
    # minimum_sys_memory = utils.calculate_minimum_sys_memory(snode.max_prov, memory_details['total'])
    # minimum_sys_memory = snode.minimum_sys_memory
    # satisfied, spdk_mem = utils.calculate_spdk_memory(minimum_hp_memory,
    #                                                  minimum_sys_memory,
    #                                                  int(memory_details['free']),
    #                                                  int(memory_details['huge_total']))
    # if not satisfied:
    #    logger.error(
    #        f"Not enough memory for the provided max_lvo: {snode.max_lvol}, max_snap: {snode.max_snap}, max_prov: {utils.humanbytes(snode.max_prov)}.. Exiting")
    minimum_sys_memory = snode.minimum_sys_memory or 0
    snode.spdk_mem = minimum_hp_memory

    spdk_debug = snode.spdk_debug
    if set_spdk_debug:
        spdk_debug = True
        snode.spdk_debug = spdk_debug

    if minimum_sys_memory:
        snode.minimum_sys_memory = minimum_sys_memory

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)

    if cluster.mode == "docker":
        cluster_docker = utils.get_docker_client(snode.cluster_id)
        cluster_ip = cluster_docker.info()["Swarm"]["NodeAddr"]

    else:
        cluster_ip = utils.get_k8s_node_ip()

    total_mem = minimum_hp_memory
    for n in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if n.api_endpoint == snode.api_endpoint and n.socket == snode.socket and n.uuid != snode.uuid:
            total_mem += (n.spdk_mem + 500000000)

    if spdk_proxy_image:
        snode.spdk_proxy_image = spdk_proxy_image
        snode.spdk_version = spdk_proxy_image.split(":")[1]

    if not snode.spdk_proxy_image:
        snode.spdk_proxy_image = cluster.container_image_prefix + constants.SIMPLY_BLOCK_DOCKER_IMAGE

    results = None
    try:
        if new_ssd_pcie and type(new_ssd_pcie) is list:
            for new_ssd in new_ssd_pcie:
                if new_ssd not in snode.ssd_pcie:
                    try:
                        snode_api.bind_device_to_spdk(new_ssd)
                    except Exception as e:
                        logger.error(e)
                    snode.ssd_pcie.append(new_ssd)

        fdb_connection = cluster.db_connection
        if lvol_changed:
            snode_api.persist_node_config(snode.max_lvol, minimum_hp_memory, snode.socket, snode.ssd_pcie)
        snode_api.set_hugepages()
        # A restart must actually bounce SPDK: see ensure_spdk_stopped.
        # snode.api_endpoint is already rewritten to node_address above when
        # restarting onto a new host, so snode.client() targets the right one.
        ensure_spdk_stopped(
            snode.client, snode.rpc_port, snode.cluster_id)
        results, err = snode_api.spdk_process_start(
            snode.l_cores, snode.spdk_mem, snode.spdk_image, spdk_debug, cluster_ip, fdb_connection,
            snode.namespace, snode.mgmt_ip, snode.rpc_port, snode.rpc_username, snode.rpc_password,
            multi_threading_enabled=constants.SPDK_PROXY_MULTI_THREADING_ENABLED, timeout=constants.SPDK_PROXY_TIMEOUT,
            ssd_pcie=snode.ssd_pcie, total_mem=total_mem, system_mem=minimum_sys_memory, cluster_mode=cluster.mode,
            socket=snode.socket, cluster_id=snode.cluster_id,
            spdk_proxy_image=snode.spdk_proxy_image)

    except Exception as e:
        logger.error(e)
        return False
    req_cpu_count = len(utils.hexa_to_cpu_list(snode.spdk_cpu_mask))

    cores, _ = snode_api.read_allowed_list()
    logger.info(f"read_allowed list is {cores}")

    # alceml_cpu_cores/distrib_cpu_cores/poller_cpu_cores and every mask below
    # are l-core INDICES (0..req_cpu_count-1), not physical core ids -- SPDK
    # addresses reactors by their position in -l, and that role-to-index split
    # was decided once, at add time (see apply_cluster_vcpu_count/add_node),
    # using whatever allocation policy existed then. Restart must not
    # re-derive it: calling recalculate_cores_distribution here re-ran
    # calculate_core_allocations fresh on every restart, so upgrading the
    # node agent to a build with a changed allocation policy silently
    # re-pinned an already-provisioned node's roles the next time it merely
    # restarted -- not a deliberate re-provisioning action.
    #
    # What legitimately can go stale across a restart is which *physical*
    # core sits at each index -- the OS/k8s CPU manager can hand back a
    # different specific set (same count) than before. That's all this
    # refreshes: the index@physical_core pairing in l_cores, nothing else --
    # reassign_l_cores_for_restart keeps every role's index set (hence its
    # size and any sharing with other roles) exactly as decided at add time,
    # only choosing which fresh physical core fills each index, preferring
    # to keep distrib/poller/alceml's own cores mutual hyperthread siblings.
    if len(cores) == req_cpu_count:
        prior_physical_cores = {int(pair.split("@")[1]) for pair in snode.l_cores.split(",") if pair}
        if set(cores) == prior_physical_cores:
            # Identical cpuset -- nothing to reassign; leave l_cores exactly
            # as it was rather than have reassign_l_cores_for_restart pick an
            # arbitrary (if equally valid) sibling pairing that only churns
            # which physical core each role lands on for no operational gain.
            logger.info(f"Node {node_id}: cpuset unchanged, keeping existing l_cores")
        else:
            placement = utils.reassign_l_cores_for_restart(
                cores, snode.distrib_cpu_cores, snode.poller_cpu_cores, snode.alceml_cpu_cores)
            snode.l_cores = utils.generate_l_cores(placement)
    else:
        logger.warning(
            "Node %s: read_allowed_list returned %d core(s), expected %d -- "
            "leaving l_cores as-is", node_id, len(cores), req_cpu_count)

    if not results:
        logger.error(f"Failed to start spdk: {err}")
        return False
    time.sleep(5)

    if small_bufsize:
        snode.iobuf_small_bufsize = small_bufsize
    if large_bufsize:
        snode.iobuf_large_bufsize = large_bufsize

    snode.write_to_db(db_controller.kv_store)

    rpc_client = snode.rpc_client(timeout=10 * 60, retry=10)

    # 1- set iobuf options
    if (snode.iobuf_small_pool_count or snode.iobuf_large_pool_count or
            snode.iobuf_small_bufsize or snode.iobuf_large_bufsize):
        ret = rpc_client.iobuf_set_options(
            snode.iobuf_small_pool_count, snode.iobuf_large_pool_count,
            snode.iobuf_small_bufsize, snode.iobuf_large_bufsize)
        if not ret:
            logger.error("Failed to set iobuf options")
            return False
    rpc_client.bdev_set_options(0, 0, 0, 0)
    rpc_client.accel_set_options()

    # 2- set socket implementation options
    bind_to_device = None
    if snode.data_nics and len(snode.data_nics) == 1:
        bind_to_device = snode.data_nics[0].if_name
    ret = rpc_client.sock_impl_set_options(bind_to_device)
    if not ret:
        logger.error("Failed socket implement set options")
        return False

    ret = rpc_client.nvmf_set_max_subsystems(constants.NVMF_MAX_SUBSYSTEMS)
    if not ret:
        logger.warning(f"Failed to set nvmf max subsystems {constants.NVMF_MAX_SUBSYSTEMS}")

    # 3- set nvme config
    if snode.pollers_mask:
        ret = rpc_client.nvmf_set_config(
            snode.pollers_mask,
            dhchap_digests=constants.DHCHAP_DIGESTS,
            dhchap_dhgroups=[constants.DHCHAP_DHGROUP],
        )
        if not ret:
            logger.error("Failed to set pollers mask")
            return False

    # 4- start spdk framework
    ret = rpc_client.framework_start_init()
    if not ret:
        logger.error("Failed to start framework")
        return False

    rpc_client.log_set_print_level("DEBUG")

    # ONCE per SPDK process lifetime. lvol_poller_mask is the single source
    # of truth for which core this runs on (already colocated with
    # jc_singleton_core unless the config gave it its own dedicated core);
    # jc_singleton_mask is only a last-resort fallback if that reservation
    # came up empty. See the add-node twin of this call for the full
    # rationale.
    poller_group_mask = snode.lvol_poller_mask or snode.jc_singleton_mask
    if poller_group_mask:
        try:
            rpc_client.bdev_lvol_create_poller_group(poller_group_mask)
        except RPCException:
            logger.error("Failed to set pollers mask")
            return False

    # 5- set app_thread cpu mask
    if snode.app_thread_mask:
        ret = rpc_client.thread_get_stats()
        app_thread_process_id = 0
        if ret.get("threads"):
            for entry in ret["threads"]:
                if entry['name'] == 'app_thread':
                    app_thread_process_id = entry['id']
                    break

        ret = rpc_client.thread_set_cpumask(app_thread_process_id, snode.app_thread_mask)
        if not ret:
            logger.error("Failed to set app thread mask")
            return False

    # 6- set nvme bdev options
    # bdev_nvme_set_options is a pure local SPDK config call; bound it at
    # 5 s so a stuck proxy can't consume the 10 min restart RPC budget.
    set_opts_rpc = snode.rpc_client(timeout=5, retry=0)
    ret = set_opts_rpc.bdev_nvme_set_options()
    if not ret:
        logger.error("Failed to set nvme options")
        return False

    qpair = cluster.qpair_count
    if cluster.fabric_tcp:
        ret = rpc_client.transport_create("TCP", qpair, 512 * (req_cpu_count + 1))
        if not ret:
            logger.error(f"Failed to create transport TCP with qpair: {qpair}")
            return False
    if cluster.fabric_rdma:
        ret = rpc_client.transport_create("RDMA", qpair, 512 * (req_cpu_count + 1))
        if not ret:
            logger.error(f"Failed to create transport RDMA with qpair: {qpair}")
            return False

    # 7- set jc singleton mask
    if snode.jc_singleton_mask:
        ret = rpc_client.jc_set_hint_lcpu_mask(snode.jc_singleton_mask)
        if not ret:
            logger.error("Failed to set jc singleton mask")
            return False

    node_info, _ = snode_api.info()
    if not snode.ssd_pcie:
        ssds = node_info['spdk_pcie_list']
    else:
        ssds = []
        for ssd in snode.ssd_pcie:
            if ssd in node_info['spdk_pcie_list']:
                ssds.append(ssd)

    nvme_devs = addNvmeDevices(rpc_client, snode, ssds)
    if not nvme_devs:
        logger.error("No NVMe devices was found!")
        return False

    logger.info(f"Devices found: {len(nvme_devs)}")
    logger.debug(nvme_devs)

    logger.info(f"Devices in db: {len(snode.nvme_devices)}")
    logger.debug(snode.nvme_devices)

    new_devices = []
    active_devices = []
    removed_devices = []
    known_devices_sn = []
    devices_sn_dict = {d.serial_number: d for d in nvme_devs}
    for db_dev in snode.nvme_devices:
        known_devices_sn.append(db_dev.serial_number)
        if db_dev.status in [NVMeDevice.STATUS_FAILED_AND_MIGRATED, NVMeDevice.STATUS_FAILED,
                             NVMeDevice.STATUS_REMOVED]:
            removed_devices.append(db_dev)
            continue
        if db_dev.serial_number in devices_sn_dict.keys():
            logger.info(f"Device found: {db_dev.get_id()}, status {db_dev.status}")
            found_dev = devices_sn_dict[db_dev.serial_number]
            if not db_dev.is_partition and not found_dev.is_partition:
                db_dev.device_name = found_dev.device_name
                db_dev.nvme_bdev = found_dev.nvme_bdev
                db_dev.nvme_controller = found_dev.nvme_controller
                db_dev.pcie_address = found_dev.pcie_address

            # if db_dev.status in [ NVMeDevice.STATUS_ONLINE]:
            #     db_dev.status = NVMeDevice.STATUS_UNAVAILABLE
            active_devices.append(db_dev)
        else:
            logger.info(f"Device not found: {db_dev.get_id()}")
            if db_dev.status == NVMeDevice.STATUS_NEW:
                snode.nvme_devices.remove(db_dev)
            else:
                db_dev.status = NVMeDevice.STATUS_REMOVED
                removed_devices.append(db_dev)

    jm_dev_sn = ""
    if snode.jm_device and "serial_number" in snode.jm_device.device_data_dict:
        jm_dev_sn = snode.jm_device.device_data_dict['serial_number']
        known_devices_sn.append(jm_dev_sn)

    for dev in nvme_devs:
        if dev.serial_number == jm_dev_sn:
            logger.info(f"JM device found: {snode.jm_device.get_id()}")
            snode.jm_device.nvme_bdev = dev.nvme_bdev

        elif dev.serial_number not in known_devices_sn:
            logger.info(f"New device found: {dev.get_id()}")
            dev.status = NVMeDevice.STATUS_NEW
            new_devices.append(dev)
            snode.nvme_devices.append(dev)

    snode.write_to_db(db_controller.kv_store)
    if node_address and len(new_devices) > 0:
        # prepare devices on new node
        if snode.num_partitions_per_dev == 0 or snode.jm_percent == 0:

            jm_device = snode.nvme_devices[0]
            for index, nvme in enumerate(snode.nvme_devices):
                if nvme.status in [NVMeDevice.STATUS_ONLINE, NVMeDevice.STATUS_NEW] and nvme.size < jm_device.size:
                    jm_device = nvme
            jm_device.status = NVMeDevice.STATUS_JM

            if snode.jm_device and snode.jm_device.get_id():
                jm_device.uuid = snode.jm_device.get_id()

            ret = _prepare_cluster_devices_jm_on_dev(snode, snode.nvme_devices)
        else:
            ret = _prepare_cluster_devices_partitions(snode, snode.nvme_devices)
        if not ret:
            logger.error("Failed to prepare cluster devices")
            # return False
    else:
        ret = _prepare_cluster_devices_on_restart(snode, clear_data=clear_data)
        if not ret:
            logger.error("Failed to prepare cluster devices")
            return False

    snode.write_to_db()

    # set qos values if enabled
    if cluster.is_qos_set():
        logger.info("Setting Alcemls QOS weights")
        ret = rpc_client.alceml_set_qos_weights(qos_controller.get_qos_weights_list(snode.cluster_id))
        if not ret:
            logger.error("Failed to set Alcemls QOS")
            return False

    logger.info("Connecting to remote devices")
    # Locked per-record: this section full-object-writes THIS node's record;
    # concurrent peers' connect-back loops write the same record (as their
    # peer) under the same per-node lock, so the two never interleave. Other
    # nodes' restarts no longer serialize behind us (see _remote_connect_lock).
    with _remote_connect_lock(snode.get_id()):
        # Device and JM reconciles touch disjoint bdev sets — run them
        # concurrently (they were back-to-back, ~sum of two RPC-bound
        # phases per restart).
        jm_result: dict = {}

        def _jm_reconcile():
            try:
                jm_result["devices"] = _connect_to_remote_jm_devs(snode)
            except Exception as e:
                jm_result["error"] = e

        jm_thread = None
        if snode.enable_ha_jm:
            jm_thread = threading.Thread(target=_jm_reconcile,
                                         name=f"jm-reconcile-{snode.get_id()[:8]}")
            jm_thread.start()
        try:
            snode.remote_devices = _connect_to_remote_devs(snode)
        except RuntimeError:
            logger.error('Failed to connect to remote devices')
            if jm_thread:
                jm_thread.join()
            return False
        if jm_thread:
            jm_thread.join()
            if "error" in jm_result:
                logger.error(f"Failed to connect to remote JM devices: {jm_result['error']}")
            elif "devices" in jm_result:
                snode.remote_jm_devices = jm_result["devices"]
        snode.lvstore_status = ""
        snode.write_to_db(db_controller.kv_store)

    snode = db_controller.get_storage_node_by_id(snode.get_id())
    for db_dev in snode.nvme_devices:
        if db_dev.status in [NVMeDevice.STATUS_UNAVAILABLE, NVMeDevice.STATUS_ONLINE,
                             NVMeDevice.STATUS_CANNOT_ALLOCATE, NVMeDevice.STATUS_READONLY]:
            db_dev.status = NVMeDevice.STATUS_ONLINE
            if db_dev.previous_status and db_dev.previous_status == NVMeDevice.STATUS_CANNOT_ALLOCATE:
                records = db_controller.get_device_capacity(db_dev, 1)
                if records and records[0].size_util == 100:
                    db_dev.status = NVMeDevice.STATUS_CANNOT_ALLOCATE
            db_dev.health_check = True
            device_events.device_restarted(db_dev)
    snode.write_to_db(db_controller.kv_store)

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status not in [Cluster.STATUS_ACTIVE, Cluster.STATUS_DEGRADED, Cluster.STATUS_READONLY]:

        # make other nodes connect to the new devices
        # Per-peer locks + parallel workers replace the old global gate +
        # serial loop: each worker re-reads its peer FRESH inside that peer's
        # lock (the computed remote_devices list is built outside the FDB tx,
        # so lock-then-read is what makes the write safe), and per-peer
        # failure is best-effort — a struggling peer must not fail this
        # node's restart (its links are re-covered by the peer's own health
        # fixups and the pre-activation connectivity repair).
        logger.info("Make other nodes connect to the node devices")
        snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)

        def _connect_back_one_peer(peer_id):
            try:
                with _remote_connect_lock(peer_id):
                    peer = db_controller.get_storage_node_by_id(peer_id)
                    if peer.status != StorageNode.STATUS_ONLINE:
                        return
                    remote_devices = _connect_to_remote_devs(
                        peer, reattach=True, force_connect_restarting_nodes=True,
                        only_node_id=snode.get_id())
                    # Atomic: never full-object-write a PEER's record — a stale
                    # copy resurrects a just-cleared restart phase on that peer
                    # (2026-07-10 stale-phase generator).
                    db_controller.atomic_update(
                        peer, lambda n, rd=remote_devices: setattr(n, "remote_devices", rd))
            except Exception as e:
                logger.error(
                    f"Peer {peer_id} connect-back to {snode.get_id()} "
                    f"failed (best-effort): {e}")

        connect_back_threads = []
        for node in snodes:
            if node.get_id() == snode.get_id() or node.status != StorageNode.STATUS_ONLINE:
                continue
            t = threading.Thread(
                target=_connect_back_one_peer, args=(node.get_id(),),
                name=f"connect-back-{node.get_id()[:8]}")
            t.start()
            connect_back_threads.append(t)
        for t in connect_back_threads:
            t.join()

        logger.info("Sending device status event")
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        for db_dev in snode.nvme_devices:
            distr_controller.send_dev_status_event(db_dev, db_dev.status)

        if snode.jm_device and snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # ANA failback: demote secondaries BEFORE port unblock/online
        try:
            trigger_ana_failback_for_node(snode)
        except Exception as ana_e:
            logger.error("ANA failback during restart of %s failed: %s", snode.get_id(), ana_e)

        logger.info("Cluster is not ready yet")
        logger.info("Setting node status to Online")
        if not set_node_status(node_id, StorageNode.STATUS_ONLINE, caused_by="restart"):
            # FSM rejected the final flip — typically because a racing
            # monitor/healthcheck/task already clobbered the RESTARTING
            # lock with OFFLINE. The wrapper's finally block will pick
            # up the False return and run its cleanup; without this
            # propagation the impl would silently return True with the
            # node stranded (incident 2026-05-20).
            logger.error(
                f"Restart impl: final ONLINE write rejected for {node_id}; "
                f"treating restart as failed"
            )
            return False
        _refresh_cluster_maps_after_node_recovery(snode)

        online_devices_list = []
        for dev in snode.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE,
                              NVMeDevice.STATUS_CANNOT_ALLOCATE,
                              NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                online_devices_list.append(dev.get_id())
        if online_devices_list:
            logger.info(f"Starting migration task for node {snode.get_id()}")
            tasks_controller.add_device_mig_task_for_node(snode.get_id())

        return True

    else:
        snode = db_controller.get_storage_node_by_id(snode.get_id())

        # Remote device connectivity is node-level and must be established before
        # any LVS recreation consumes remote alceml bdevs in distrib maps/stacks.
        logger.info("Make other nodes connect to the node devices")
        peer_swept_ids, peer_reconnect_failed = \
            _reconnect_peers_to_restarted_node(snode)

        # === LVS Recreation: clear sequential structure per design ===
        # No recursion. Process primary, secondary, tertiary LVS in order.
        # Before each, perform disconnect checks on the other two nodes.

        def _abort_restart(reason):
            """Kill SPDK and set offline on fatal error.

            Contract: any abort during restart kills SPDK reliably (verified
            down) before returning, so the next restart attempt starts from
            a clean SPDK process. The previous implementation issued a
            single fire-and-forget ``spdk_process_kill`` and proceeded —
            which left zombie SPDK behind when docker-rm took >5 s,
            causing the next attempt to fail with "Duplicate bdev name for
            manual examine: raid0_<vuid>" and loop forever.
            """
            logger.error(f"Restart abort: {reason}")
            storage_events.snode_restart_failed(snode)
            _kill_spdk_until_dead(snode)
            set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE,
                            caused_by="restart_cleanup")

        # Connectivity-coverage gate: recreate consumes remote alceml bdevs
        # through the distrib data path, and any online peer device absent
        # from this node's SPDK shows up as `unavailable` in the node's
        # cluster maps — the failure then surfaces only as an EIO on the raid
        # examine read, minutes into the attempt. Verify (and repair once)
        # BEFORE entering the data path; abort with the explicit missing list
        # if coverage stays incomplete so the retry starts from a clean SPDK.
        try:
            missing_remote_bdevs = _verify_online_device_coverage(snode, repair=True)
        except Exception as e:
            missing_remote_bdevs = [f"coverage check failed: {e}"]
        if missing_remote_bdevs:
            _set_lvstore_status_atomic(snode.get_id(), "failed", db_controller)
            _abort_restart(
                "connectivity coverage incomplete before LVS recreation; "
                f"missing remote bdevs: {missing_remote_bdevs}")
            return False

        try:
            ret = recreate_all_lvstores(snode, force=force_lvol_recreate)
        except Exception as e:
            logger.error(e)
            _abort_restart(f"LVS recreation failed: {e}")
            return False
        if not ret:
            # Restart abort path. recreate_all_lvstores returning False is
            # ALSO a restart abort and must honor the same kill+offline
            # contract — otherwise SPDK keeps running with the partial
            # bdev stack from this attempt (e.g. raid0_<vuid> created via
            # auto-examine) and the next retry fails on "Duplicate bdev
            # name". 10:58:11 in the AWS soak run hit exactly this gap.
            _set_lvstore_status_atomic(snode.get_id(), "failed", db_controller)
            _abort_restart("recreate_all_lvstores returned False")
            return False

        # === Phase 10: Finalization — post all LVS recreation ===

        # Create S3 bdev for backup support (only if backup is configured)
        if cluster.backup_config:
            from simplyblock_core.controllers import backup_controller
            logger.info("Creating S3 bdev on restarted node")
            try:
                backup_controller.create_s3_bdev(snode, cluster.backup_config)
            except Exception as e:
                logger.exception(str(e))
                return False

        # Finalization peer sweep, DEDUPED (2026-07-17 profile: the wholesale
        # second sweep was ~55% of restart time; 2026-07-21: 512 coordinator
        # jobs across a 16-node recovery). Peer-to-device connections do not
        # change during LVS recreation, so re-sweep only:
        #   (a) peers that FAILED the pre-recreate sweep, and
        #   (b) peers that came ONLINE since it ran — their own restart's
        #       connect-back skipped this node (it was RESTARTING then), so
        #       the wholesale second sweep was their only prompt link-up;
        #       keep exactly that coverage without redoing succeeded peers.
        _retry_ids = set(peer_reconnect_failed)
        for _p in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
            if (_p.get_id() != snode.get_id()
                    and _p.status == StorageNode.STATUS_ONLINE
                    and _p.get_id() not in peer_swept_ids):
                _retry_ids.add(_p.get_id())
        if _retry_ids:
            logger.info(
                "Finalization peer re-sweep for %d peer(s) "
                "(failed pass-1 or newly online): %s",
                len(_retry_ids), sorted(p[:8] for p in _retry_ids))
            _reconnect_peers_to_restarted_node(
                snode, only_peer_ids=_retry_ids)

        if snode.jm_device and snode.jm_device.status in [JMDevice.STATUS_UNAVAILABLE, JMDevice.STATUS_ONLINE]:
            device_controller.set_jm_device_state(snode.jm_device.get_id(), JMDevice.STATUS_ONLINE)

        # ANA failback: demote secondaries BEFORE port unblock/online
        try:
            trigger_ana_failback_for_node(snode)
        except Exception as ana_e:
            logger.error("ANA failback during restart of %s failed: %s", snode.get_id(), ana_e)

        # Start data migration
        online_devices_list = []
        for dev in snode.nvme_devices:
            if dev.status in [NVMeDevice.STATUS_ONLINE,
                              NVMeDevice.STATUS_CANNOT_ALLOCATE,
                              NVMeDevice.STATUS_FAILED_AND_MIGRATED]:
                online_devices_list.append(dev.get_id())
        if online_devices_list:
            logger.info(f"Starting migration task for node {snode.get_id()}")
            tasks_controller.add_device_mig_task_for_node(snode.get_id())

        logger.info("Setting node status to Online")
        if not set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE, caused_by="restart"):
            # See twin call site above (single-leader restart path) for
            # the full rationale — final ONLINE rejection must propagate
            # so the wrapper's finally cleanup runs and the CLI reports
            # a real failure instead of silently lying.
            logger.error(
                f"Restart impl (non-leader): final ONLINE write rejected for "
                f"{snode.get_id()}; treating restart as failed"
            )
            return False

        logger.info("Sending device status event")
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        for db_dev in snode.nvme_devices:
            distr_controller.send_dev_status_event(db_dev, db_dev.status)

        _refresh_cluster_maps_after_node_recovery(snode)

        lvol_list = db_controller.get_lvols_by_node_id(snode.get_id())
        logger.info(f"Found {len(lvol_list)} lvols")

        return True


def _format_lvstore_ports(node: StorageNode):
    """Format per-lvstore ports for display."""
    if not node.lvstore_ports:
        return "-"
    parts = []
    for lvs_name, ports in node.lvstore_ports.items():
        lp = ports.get("lvol_subsys_port", "-")
        hp = ports.get("hublvol_port", "-")
        parts.append(f"{lvs_name}(L:{lp},H:{hp})")
    return " ".join(parts)


def list_storage_nodes(cluster_id=None):
    db_controller = DBController()
    if cluster_id:
        nodes = db_controller.get_storage_nodes_by_cluster_id(cluster_id)
    else:
        nodes = db_controller.get_storage_nodes()
    data = []
    all_lvols = db_controller.get_mini_lvols()
    # Only surface the failure-domain column when the feature is actually in use.
    show_failure_domain = any(node.failure_domain >= 0 for node in nodes)
    for node in nodes:
        logger.debug(node)
        logger.debug("*" * 20)
        total_devices = len(node.nvme_devices)
        online_devices = 0

        for dev in node.nvme_devices:
            if dev.status == NVMeDevice.STATUS_ONLINE:
                online_devices += 1
        lvs = [lv for lv in all_lvols if lv.node_id == node.get_id()]
        row = {
            "UUID": node.uuid,
            "Hostname": node.hostname,
            "Management IP": node.mgmt_ip,
            "Dev": f"{total_devices}/{online_devices}",
            "LVols": f"{len(lvs)}",
            "Status": node.status,
            # Health is only meaningful for ONLINE/DOWN nodes; otherwise N/A.
            "Health": node.health_check if node.status in (
                StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED) else "-",
            "Up time": utils.strfdelta(uptime) if (uptime := node.uptime()) is not None else "",
            "CPU": f"{len(utils.hexa_to_cpu_list(node.spdk_cpu_mask))}",
            "MEM": utils.humanbytes(node.spdk_mem),
            "SPDK P": node.rpc_port,
            "LVOL P": node.lvol_subsys_port,
            "DEV P": node.nvmf_port,
            "HUB P": node.hublvol.nvmf_port if node.hublvol else "-",
            "LVS Ports": _format_lvstore_ports(node),
            # "Cloud ID": node.cloud_instance_id,
            # "JM VUID": node.jm_vuid,
            # "Ext IP": node.cloud_instance_public_ip,
            "Secondary node ID": node.secondary_node_id,

        }
        if show_failure_domain:
            row["Failure Domain"] = node.failure_domain if node.failure_domain >= 0 else "-"
        data.append(row)

    return data


def list_storage_devices(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    storage_devices = []
    bdev_devices = []
    jm_devices = []
    remote_devices = []
    for device in snode.nvme_devices:
        logger.debug(device)
        logger.debug("*" * 20)
        storage_devices.append({
            "UUID": device.uuid,
            "StorgeID": device.cluster_device_order,
            "Name": device.alceml_name,
            "Size": utils.humanbytes(device.size),
            "Serial Number": device.serial_number,
            "PCIe": device.pcie_address,
            "Status": device.status,
            "IO Err": device.io_error,
            # Device health is only meaningful when its node is ONLINE/DOWN.
            "Health": device.health_check if snode.status in (
                StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED) else "-"
        })

    for bdev in snode.lvstore_stack:
        if bdev['type'] != "bdev_distr":
            continue
        logger.debug("*" * 20)
        distrib_params = bdev['params']
        bdev_devices.append({
            "VUID": distrib_params['vuid'],
            "Name": distrib_params['name'],
            "Size": utils.humanbytes(distrib_params['num_blocks'] * distrib_params['block_size']),
            "Block Size": distrib_params['block_size'],
            "Num Blocks": distrib_params['num_blocks'],
            "NDCS": f"{distrib_params['ndcs']}",
            "NPCS": f"{distrib_params['npcs']}",
            "Chunk": distrib_params['chunk_size'],
            "Page Size": distrib_params['pba_page_size'],
            "JM_VUID": distrib_params['jm_vuid'],
        })

    if snode.jm_device and snode.jm_device.get_id():
        jm_devices.append({
            "UUID": snode.jm_device.uuid,
            "Name": snode.jm_device.alceml_name,
            "Size": utils.humanbytes(snode.jm_device.size),
            "Status": snode.jm_device.status,
            "IO Err": snode.jm_device.io_error,
            "Health": snode.jm_device.health_check if snode.status in (
                StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN, StorageNode.STATUS_SUSPENDED) else "-"
        })

    for remote_device in snode.remote_devices:
        logger.debug(remote_device)
        logger.debug("*" * 20)
        name = remote_device.alceml_name

        remote_devices.append({
            "UUID": remote_device.uuid,
            "Name": name,
            "Size": utils.humanbytes(remote_device.size),
            "Node ID": remote_device.node_id,
            "Status": remote_device.status,
        })

    for remote_jm_device in snode.remote_jm_devices:
        logger.debug(remote_jm_device)
        logger.debug("*" * 20)
        remote_devices.append({
            "UUID": remote_jm_device.uuid,
            "Name": remote_jm_device.remote_bdev,
            "Size": utils.humanbytes(remote_jm_device.size),
            "Node ID": remote_jm_device.node_id,
            "Status": remote_jm_device.status,
        })

    data: dict[str, List[Any]] = {
        "Storage Devices": storage_devices,
        "JM Devices": jm_devices,
        "Remote Devices": remote_devices,
    }
    if bdev_devices:
        data["Distrib Block Devices"] = bdev_devices

    return data


def _check_ftt_allows_node_removal(node_id, db_controller):
    """Check whether FTT constraints allow removing (suspend/shutdown) a node.

    Returns (allowed: bool, reason: str).
    """
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return False, "Node not found"

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    snodes = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)

    if cluster.ha_type != "ha":
        return True, ""

    npcs = cluster.distr_npcs  # parity chunk count (1 or 2)
    ndcs = cluster.distr_ndcs  # data chunk count
    ft = cluster.max_fault_tolerance  # declared fault tolerance level

    # Count total active nodes (excluding in_creation and removed)
    total_active_nodes = sum(
        1 for node in snodes
        if node.status not in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]
    )

    # Block suspend/shutdown during rebalancing based on node headroom.
    # A cluster needs ndcs+npcs nodes minimum. During rebalancing:
    #   - With exactly ndcs+npcs nodes: no shutdowns allowed (no headroom)
    #   - With ndcs+npcs+1 nodes: one shutdown allowed (one spare)
    #   - With ndcs+npcs+2+ nodes: two shutdowns allowed, etc.
    # The number of allowed shutdowns during rebalancing is:
    #   total_active_nodes - (ndcs + npcs)
    # This must be greater than the number of already-not-online nodes.
    if cluster.is_re_balancing:
        not_online_already = sum(
            1 for node in snodes
            if node.get_id() != node_id
            and node.status != StorageNode.STATUS_ONLINE
            and node.status not in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]
        )
        headroom = total_active_nodes - (ndcs + npcs)
        if headroom <= not_online_already:
            return False, (
                f"Cluster is rebalancing with {total_active_nodes} active nodes "
                f"({not_online_already} already not online, "
                f"need >{ndcs + npcs} for ndcs={ndcs}, npcs={npcs}). "
                f"Wait for rebalancing to complete before removing a node."
            )

    # Count nodes that are not online (excluding the node being removed,
    # and excluding nodes in creation or already removed).
    not_online_nodes = []
    for node in snodes:
        if node.get_id() == node_id:
            continue
        if node.status in [StorageNode.STATUS_IN_CREATION, StorageNode.STATUS_REMOVED]:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            not_online_nodes.append(node)

    # Check for journal replication in progress on any online node.
    # A node with active journal replication counts as one additional not-online node.
    jm_replication_active = False
    for node in snodes:
        if node.get_id() == node_id:
            continue
        if node.status != StorageNode.STATUS_ONLINE:
            continue
        try:
            lvstores = node.rpc_client(timeout=5, retry=1).bdev_lvol_get_lvstores(node.lvstore)
            if lvstores:
                ret = node.rpc_client(timeout=5, retry=1).jc_get_jm_status(node.jm_vuid)
                for jm in ret:
                    if ret[jm] is False:
                        jm_replication_active = True
                        break
        except Exception:
            pass
        if jm_replication_active:
            break

    not_online_count = len(not_online_nodes)
    if jm_replication_active:
        not_online_count += 1

    fd_on = cluster.enable_failure_domain and snode.failure_domain >= 0
    blocked_by_capacity = False
    capacity_reason = ""

    if fd_on:
        # Placement spreads a stripe's ndcs+npcs chunks as evenly as possible
        # across the domains that actually exist. With fewer domains than
        # chunks, at least one domain holds ceil((ndcs+npcs)/domains) chunks
        # -- that many nodes can go down within a SINGLE domain for free
        # (mirrors that domain's worst-case chunk contribution going down
        # outright, already priced in), but once a domain hits that many
        # down, it has maxed its contribution to the npcs risk budget and a
        # DIFFERENT domain can only add a node if the combined risk across
        # every affected domain (each capped at chunks_per_domain) still
        # leaves room. This reduces to the familiar "up to npcs whole domains
        # are free" rule when there are >= ndcs+npcs domains (chunks_per_domain
        # == 1). See the analogous fdDrainGate in nodedrain_controller.go.
        domains_available = len({n.failure_domain for n in snodes if n.failure_domain >= 0})
        domains_needed = ndcs + npcs
        chunks_per_domain = -(-domains_needed // domains_available) if domains_available > 0 else domains_needed

        domain_down_counts: dict[int, int] = {}
        for node in not_online_nodes:
            if node.failure_domain >= 0:
                domain_down_counts[node.failure_domain] = domain_down_counts.get(node.failure_domain, 0) + 1

        my_domain = snode.failure_domain
        my_domain_down = domain_down_counts.get(my_domain, 0)

        if my_domain_down < chunks_per_domain:
            current_risk = sum(min(c, chunks_per_domain) for c in domain_down_counts.values())
            # jm_replication_active can't be attributed to a specific domain
            # (the probe only says "some online node's journal is behind"),
            # so treat it conservatively as always adding a fresh risk unit.
            if jm_replication_active:
                current_risk += 1
            if current_risk + 1 > npcs:
                blocked_by_capacity = True
                capacity_reason = (
                    f"FTT={ft} (npcs={npcs}): cannot remove node in failure domain {my_domain}; "
                    f"{current_risk}/{npcs} failure-domain risk budget already committed "
                    f"({domains_available} domain(s) available, {chunks_per_domain} chunk(s)/domain worst case)"
                    f"{' (including in-progress journal replication)' if jm_replication_active else ''}"
                )
        # else: this domain already holds >= chunks_per_domain down nodes --
        # it has maxed its contribution to the risk budget, so one more node
        # in the SAME domain adds no additional risk.
    elif npcs == 1:
        # FTT=1: no room at all if anything is already not online or journal replicating
        if not_online_count > 0:
            blocked_by_capacity = True
            capacity_reason = (
                f"FTT=1 (npcs=1): cannot remove node, cluster already has "
                f"{len(not_online_nodes)} not-online node(s)"
                f"{' and journal replication in progress' if jm_replication_active else ''}"
            )

    elif npcs == 2:
        if ft >= 2:
            # FTT=2: room for one not-online node, block if already have one+
            if not_online_count >= 2:
                blocked_by_capacity = True
                capacity_reason = (
                    f"FTT=2 (npcs=2): cannot remove node, cluster already has "
                    f"{len(not_online_nodes)} not-online node(s)"
                    f"{' and journal replication in progress' if jm_replication_active else ''}"
                )
        else:
            # npcs=2, ft=1: like FTT=2 for capacity, but additionally
            # cannot remove both primary and its secondary (checked below).
            if not_online_count >= 2:
                blocked_by_capacity = True
                capacity_reason = (
                    f"npcs=2/ft=1: cannot remove node, cluster already has "
                    f"{len(not_online_nodes)} not-online node(s)"
                    f"{' and journal replication in progress' if jm_replication_active else ''}"
                )

    if blocked_by_capacity:
        return False, capacity_reason

    if npcs == 2 and ft == 1:
        # npcs=2, ft=1: beyond the capacity cap above, cannot remove both a
        # primary and its own secondary/tertiary at once -- a per-relationship
        # constraint, orthogonal to failure domains.
        for not_online_node in not_online_nodes:
            # Is any not-online node the secondary of the node we're removing?
            if snode.secondary_node_id == not_online_node.get_id():
                return False, (
                    f"npcs=2/ft=1: cannot remove node {node_id}, "
                    f"its secondary {not_online_node.get_id()} is not online "
                    f"(status: {not_online_node.status})"
                )
            if snode.tertiary_node_id == not_online_node.get_id():
                return False, (
                    f"npcs=2/ft=1: cannot remove node {node_id}, "
                    f"its secondary {not_online_node.get_id()} is not online "
                    f"(status: {not_online_node.status})"
                )

        # Is the node we're removing a secondary of any not-online primary?
        for not_online_node in not_online_nodes:
            if not_online_node.secondary_node_id == node_id:
                return False, (
                    f"npcs=2/ft=1: cannot remove node {node_id}, "
                    f"it is secondary of not-online primary {not_online_node.get_id()} "
                    f"(status: {not_online_node.status})"
                )
            if not_online_node.tertiary_node_id == node_id:
                return False, (
                    f"npcs=2/ft=1: cannot remove node {node_id}, "
                    f"it is secondary of not-online primary {not_online_node.get_id()} "
                    f"(status: {not_online_node.status})"
                )

    return True, ""


def _allow_shutdown_with_migration_tasks(snode: StorageNode, db_controller):
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    return (
        cluster.ha_type == "ha"
        and cluster.max_fault_tolerance >= 2
        and cluster.distr_npcs >= 2
    )


# Peer statuses we still try to talk to during a graceful shutdown's
# Loop 1 (device-unavailable broadcast) and Loop 2 (detach remote ctrlrs).
# A peer in any other status is either gone (offline/removed) or in a
# state where the RPC would be meaningless (the peer's own shutdown).
_PEER_RECONNECT_ELIGIBLE_STATUSES = (
    StorageNode.STATUS_ONLINE,
    StorageNode.STATUS_DOWN,
    StorageNode.STATUS_RESTARTING,
)


def _target_is_reconnect_eligible(target_node: StorageNode):
    """True iff a remote ctrlr attach toward ``target_node`` should proceed.

    Any service that calls bdev_nvme_attach_controller toward a peer must
    consult this gate first. A target in in_shutdown / offline / unreachable
    is either dying or already dead; a fresh attach would either fail or
    silently make the local node a competing writer for an LVS the target
    is no longer serving.
    """
    if target_node is None:
        return False
    return target_node.status in _PEER_RECONNECT_ELIGIBLE_STATUSES


def _detach_remote_controllers_from_peers(snode: StorageNode, db_controller):
    """Loop 2 of graceful shutdown.

    For every peer in {online, down, in_restart}, detach the remote
    controllers on that peer that reference ``snode`` — i.e. its
    remote_alceml_<dev-uuid> and remote_jm_<node-uuid> controllers.
    bdev_nvme_detach_controller cancels the SPDK auto-reconnect poller
    on the peer in one shot, so the peer's SPDK can never reattach to
    the dying node behind our back.

    Per-peer work is sequential (avoid issuing concurrent detach RPCs to
    one SPDK); fan-out across peers is parallel. Every RPC is wrapped in
    try/except — silent on failure including: controller already absent
    (peer detached on its own), peer in_restart hasn't created the
    controller yet, peer unreachable / timeout. None of these can block
    the kill in step 4.
    """
    shutting_down_id = snode.get_id()
    all_peers = db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id)
    peers = [
        p for p in all_peers
        if p.get_id() != shutting_down_id
        and p.status in _PEER_RECONNECT_ELIGIBLE_STATUSES
    ]

    if not peers:
        return 0

    detached = [0]
    detached_lock = threading.Lock()

    def _detach_one_peer(peer):
        try:
            rpc_client = peer.rpc_client(timeout=5, retry=1)
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(
                "detach: could not build rpc_client for peer %s: %s",
                peer.get_id(), e)
            return

        ctrl_names = []
        for rem_dev in (peer.remote_devices or []):
            if rem_dev.node_id != shutting_down_id:
                continue
            bdev_name = rem_dev.remote_bdev or ""
            if bdev_name.endswith("n1"):
                ctrl_names.append(bdev_name[:-2])

        for rem_jm in (peer.remote_jm_devices or []):
            if rem_jm.node_id != shutting_down_id:
                continue
            bdev_name = rem_jm.remote_bdev or ""
            if bdev_name.endswith("n1"):
                ctrl_names.append(bdev_name[:-2])

        if not ctrl_names:
            return

        local_count = 0
        for ctrl_name in ctrl_names:
            try:
                rpc_client.bdev_nvme_detach_controller(ctrl_name)
                local_count += 1
            except Exception as e:
                logger.info(
                    "detach: peer %s ctrlr %s detach failed (best-effort, "
                    "shutdown continues): %s",
                    peer.get_id(), ctrl_name, e)
        if local_count:
            with detached_lock:
                detached[0] += local_count

    threads = []
    for peer in peers:
        t = threading.Thread(target=_detach_one_peer, args=(peer,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join(timeout=15)
    return detached[0]


def check_node_shutdown_preconditions(node_id, force=False, current_restart_task_id=None):
    """Read-only validation of everything that can refuse a graceful node
    shutdown. Returns (allowed, reason).

    current_restart_task_id: bare task uuid (NOT JobSchedule.get_id()'s
    composite key) of the caller's own NODE_RESTART task, when the caller IS
    that task's cleanup shutdown. Exempts the task from the restart-task
    guard below — it is this shutdown's driver, not a competing restart.

    Exists so API endpoints can evaluate the guards SYNCHRONOUSLY and return
    an actionable error (409) to the caller. Previously these checks only ran
    inside shutdown_storage_node in the endpoint's fire-and-forget background
    thread: the API had already answered 202, so a refusal (e.g. active
    migration tasks) was invisible to the caller — the k8s operator polled
    for the node to go offline forever and node drains stalled even after
    the blocking condition cleared (2026-07-06 MCO rollout incident).

    With force=True the refusals downgrade to warnings and the shutdown is
    allowed, mirroring shutdown_storage_node's historical behavior.
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return False, f"Storage node not found: {node_id}"

    # Per-node restart claim: a FRESH claim on the target itself means a
    # live driver is mid-transition on this very node RIGHT NOW. Shutting it
    # down would yank the SPDK container out from under that driver
    # (2026-08-06 soak iter-50: the restart task runner's cleanup shutdown
    # destroyed a manual CLI restart's container seconds before it finished).
    # Deliberately NOT overridable with force and NOT exempted by
    # current_restart_task_id: both actors share the one NODE_RESTART task,
    # so the task id cannot discriminate them — the claim token is the only
    # identity that can. A dead driver's claim expires within
    # RESTART_CLAIM_TTL_SEC; waiting that out is the sanctioned takeover.
    if snode.status in (StorageNode.STATUS_RESTARTING, StorageNode.STATUS_IN_SHUTDOWN):
        _claim_holder = db_module.restart_claim_active(snode)
        if _claim_holder:
            reason = (f"Node {node_id} is {snode.status} under a live restart "
                      f"claim ({_claim_holder}); shutdown refused until the "
                      f"claim is released or expires")
            logger.error(reason)
            return False, reason

    # Guard: no concurrent shutdown + restart (design: mutual exclusion)
    for peer in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if peer.get_id() != node_id and peer.status == StorageNode.STATUS_RESTARTING:
            reason = (f"Node {peer.get_id()} is restarting in this cluster, "
                      f"cannot shutdown {node_id} concurrently")
            if force is False:
                logger.error(reason)
                return False, reason
            logger.warning("%s — proceeding with force", reason)
        if peer.get_id() != node_id and peer.status == StorageNode.STATUS_IN_SHUTDOWN:
            reason = (f"Node {peer.get_id()} is already shutting down in this cluster, "
                      f"cannot shutdown {node_id} concurrently")
            if force is False:
                logger.error(reason)
                return False, reason
            logger.warning("%s — proceeding with force", reason)

    task_id = tasks_controller.get_active_node_restart_task(snode.cluster_id, snode.get_id())
    if task_id and task_id != current_restart_task_id:
        reason = f"Restart task found: {task_id}, can not shutdown storage node"
        if force is False:
            logger.error(reason)
            return False, reason
        logger.warning("%s — proceeding with force", reason)

    # Only DATA-MOVEMENT tasks may block a shutdown, which is what this check
    # has always claimed to be about. get_active_node_tasks returns every
    # non-done task on the node, so any durable, re-drivable work counted too:
    # a replicating cluster has a snapshot_replication task in flight almost
    # every minute, which made graceful shutdown impossible (lab run 18 —
    # "Migration task found: 2" was one running replication task). Replication
    # and sync-delete work survives the outage and resumes on return; a
    # migration in progress does not.
    migration_fns = {
        JobSchedule.FN_DEV_MIG,
        JobSchedule.FN_FAILED_DEV_MIG,
        JobSchedule.FN_NEW_DEV_MIG,
        JobSchedule.FN_LVOL_MIG,
        JobSchedule.FN_LVOL_BATCH_MIG,
    }
    tasks = [t for t in tasks_controller.get_active_node_tasks(
        snode.cluster_id, snode.get_id()) if t.function_name in migration_fns]
    if tasks:
        blocking = ", ".join(sorted({t.function_name for t in tasks}))
        if not force and _allow_shutdown_with_migration_tasks(snode, db_controller):
            logger.warning(
                "Migration task found: %s (%s), proceeding with shutdown because FTT=2 allows node outage",
                len(tasks), blocking,
            )
        elif force:
            logger.warning(
                "Migration task found: %s (%s), proceeding with forced shutdown",
                len(tasks), blocking,
            )
        else:
            reason = (f"Migration task found: {len(tasks)} ({blocking}), "
                      f"can not shutdown storage node or use --force")
            logger.error(reason)
            return False, reason

    if snode.status not in (
            StorageNode.STATUS_ONLINE,
            StorageNode.STATUS_SUSPENDED,
            StorageNode.STATUS_DOWN,
    ):
        if force:
            logger.warning(
                "Node status is %s, proceeding with force", snode.status)
        else:
            reason = (f"Node is in {snode.status} state; only online/suspended/down "
                      f"can be gracefully shut down. Use --force.")
            logger.error(reason)
            return False, reason

    return True, ""


def shutdown_storage_node(node_id, force=False, keep_auto_restart=False,
                          current_restart_task_id=None):
    """Gracefully terminate a storage node.

    current_restart_task_id: bare uuid of the caller's own NODE_RESTART task
    when this shutdown is that task's cleanup step (see
    check_node_shutdown_preconditions).

    keep_auto_restart=True is used by the suspend-recovery auto-shutdown
    (storage_node_monitor): it brings the node down WITHOUT marking it
    auto_restart_disabled, so once the whole cluster has drained to offline the
    existing auto-restart brings it back. A genuine operator `sn shutdown`
    (CLI/API) leaves the default (False) and stays stopped until an explicit
    restart.

    Flow (graceful, force=False):
      1. FTT / concurrency guards, set node status to in_shutdown.
      2. Cancel in-flight migration tasks for this node.
      3. Loop 1: broadcast device-unavailable events to peers in
         {online, down, in_restart} via device_set_unavailable() /
         set_jm_device_state() — these already fan out
         distr_controller.send_dev_status_event(...) under the hood,
         so peers update their cluster maps and DISTRIB stops routing
         IO toward this node's devices.
      4. Loop 2: on the same peers, detach the remote_alceml /
         remote_jm controllers that point at this node. Detach
         cancels SPDK's auto-reconnect poller in one shot, so peers
         cannot reattach after we kill our SPDK.
      5. spdk_process_kill — hard SIGKILL of the SPDK container.
      6. Set status to offline + trigger_ana_failover_for_node.

    No suspension phase. Earlier revisions blocked sec/tert lvstore
    ports on the dying node first ("suspend") to drain host IO before
    kill. That fence is iptables-only and cannot stop SPDK's lvol
    layer from resubmitting failed-redirect IO as if it were new host
    IO — which races with the surviving sec/tert peer's auto-promotion
    and produces a writer conflict. Removing the suspension step
    removes the surface where that race lives; the only benefit
    suspension provided over a hard kill — letting peers cleanly tear
    down their remote_alceml / remote_jm controllers — is now provided
    by Loop 2.

    Forced (force=True) still skips Loops 1+2 and goes straight to
    kill (matches the existing --force semantics: terminate immediately
    and accept that peers discover the loss through TCP errors).
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)

    # Expansion lock: while the cluster is IN_EXPANSION the role rebalance
    # is re-wiring sec/tert stacks across nodes — losing any node mid-move
    # leaves half-applied topology. Shutdowns are disabled until the
    # expansion completes (or its task is cancelled, which restores the
    # previous cluster status). force keeps its usual escape-hatch
    # semantics for emergencies.
    try:
        _cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    except KeyError:
        _cluster = None
    if _cluster is not None and _cluster.status == Cluster.STATUS_IN_EXPANSION:
        logger.error(
            f"Cluster {snode.cluster_id} is in expansion; node shutdown is "
            f"disabled until the expansion completes")
        if force is False:
            return False
        logger.warning("Proceeding with forced shutdown DURING expansion — "
                       "the role rebalance may abort and require a resume")

    # NOTE: shutdown does not consult _check_ftt_allows_node_removal.
    # Removal and shutdown are different operations: removing a node
    # permanently changes the cluster's storage budget, while shutting one
    # down is a transient state that the cluster is meant to absorb under
    # its FTT contract. Conflating the two was added in commit fbdffea3
    # (2026-03-28) and caused soak/operator workflows to wait for
    # rebalancing to drain — the wrong policy for an operation whose
    # whole point is to disrupt the cluster on purpose. The web API
    # layer (simplyblock_web/api/v{1,2}/storage_node.py) still gates on
    # this for its own non-force shutdown endpoint, where the policy
    # decision belongs.

    allowed, _reason = check_node_shutdown_preconditions(
        node_id, force=force, current_restart_task_id=current_restart_task_id)
    if not allowed:
        return False

    for n in db_controller.get_storage_nodes_by_cluster_id(snode.cluster_id):
        if tasks_controller.get_active_lvol_migration(n.get_id()):
            msg = f"LVol migration tasks found on node: {n.get_id()}"
            logger.error(msg)
            return False

    # Step 1: mark the node in_shutdown. set_node_status fans out a
    # node_status event to peers so their cluster maps see "this node
    # is going away" before we touch any device state.
    logger.info("Shutting down node")
    set_node_status(node_id, StorageNode.STATUS_IN_SHUTDOWN)
    snode = db_controller.get_storage_node_by_id(node_id)

    # Mark this as a deliberate stop so the monitor's auto-restart leaves it
    # alone. We set it here — as soon as the intent is committed — rather than
    # at the final OFFLINE flip, so an interrupted/forced shutdown that never
    # reaches a clean OFFLINE is still protected from being auto-restarted.
    # Cleared in set_node_status() when the node deliberately returns ONLINE.
    #
    # Exception: a suspend-recovery auto-shutdown (keep_auto_restart=True) must
    # NOT set this — the whole point is to drain the cluster offline and then
    # let auto-restart bring these nodes back. Only operator-initiated shutdowns
    # stay disabled.
    if not keep_auto_restart:
        snode.auto_restart_disabled = True
        snode.write_to_db(db_controller.kv_store)
        # The flag alone is not enough: it is enforced at ENQUEUE time only
        # (tasks_controller.add_node_to_auto_restart, "the single chokepoint
        # for every auto-restart queue path"). tasks_runner_restart never
        # consults it. So an FN_NODE_RESTART row queued BEFORE the flag was
        # set survives, executes unconditionally, and its ONLINE transition
        # clears the flag again -- the deliberate-stop intent destroyed by the
        # very task it was meant to prevent. Live 2026-09-03: a cluster
        # graceful-shutdown left s7457 and zdgtb ONLINE because two such rows
        # fired seconds after the sweep passed them.
        #
        # Reap them here, mirroring what set_node_status already does on the
        # opposite transition (ONLINE cancels obsolete restart rows). This
        # also gives the runner its dequeue-side check for free -- it already
        # honours task.canceled -- without a new field and without breaking
        # ensure_node_restart_task, which deliberately bypasses the flag
        # because an explicit `sn restart` is the operator intervention the
        # flag is waiting for. A restart queued AFTER this point is exactly
        # that, and is left alone.
        #
        # current_restart_task_id is excluded: the restart runner drives this
        # very function as its kill step, so cancelling its task here would
        # abort the restart that is doing the shutting down.
        tasks_controller.cancel_pending_node_restart_tasks(
            snode.cluster_id, node_id,
            exclude_task_id=current_restart_task_id,
            reason="node deliberately shut down")

    # Step 2: cancel migration tasks while controllers are still up.
    pending_tasks = db_controller.get_job_tasks(snode.cluster_id)
    for task in pending_tasks:
        if task.node_id != node_id or task.status == JobSchedule.STATUS_DONE:
            continue
        if task.function_name in [
            JobSchedule.FN_DEV_MIG,
            JobSchedule.FN_NEW_DEV_MIG,
        ]:
            task.canceled = True
            task.write_to_db(db_controller.kv_store)

    if not force:
        # Step 3 (Loop 1): broadcast device-unavailable events. The
        # underlying device_set_unavailable() / set_jm_device_state()
        # helpers call distr_controller.send_dev_status_event() which
        # already fans out to all peers and skips offline/removed.
        if snode.jm_device and snode.jm_device.status != JMDevice.STATUS_REMOVED:
            logger.info("Loop 1: setting JM unavailable on peers")
            try:
                device_controller.set_jm_device_state(
                    snode.jm_device.get_id(), JMDevice.STATUS_UNAVAILABLE)
            except Exception as e:
                logger.warning(
                    "Loop 1: set_jm_device_state failed (continuing): %s", e)

        logger.info(
            "Loop 1: marking %d nvme device(s) unavailable on peers",
            len(snode.nvme_devices))
        for dev in snode.nvme_devices:
            if dev.status not in [
                NVMeDevice.STATUS_UNAVAILABLE,
                NVMeDevice.STATUS_ONLINE,
                NVMeDevice.STATUS_CANNOT_ALLOCATE,
                NVMeDevice.STATUS_READONLY,
            ]:
                continue
            try:
                # Default cause (CAUSE_OTHER): a node-driven shutdown
                # must not count against the per-device flap budget.
                device_controller.device_set_unavailable(dev.get_id())
            except Exception as e:
                logger.warning(
                    "Loop 1: device_set_unavailable(%s) failed (continuing): %s",
                    dev.get_id(), e)

        # Step 4 (Loop 2): detach remote_alceml / remote_jm controllers
        # on every peer still capable of receiving an RPC. Detach (vs.
        # disconnect) removes the per-ctrlr reconnect poller so the
        # peer's SPDK cannot reattach after we kill our SPDK below.
        snode = db_controller.get_storage_node_by_id(node_id)
        logger.info("Loop 2: detaching remote controllers on peers")
        try:
            count = _detach_remote_controllers_from_peers(snode, db_controller)
            logger.info("Loop 2: detached %d controller(s) total", count)
        except Exception as e:
            logger.warning(
                "Loop 2: peer-side detach pass raised %s (continuing to kill)",
                e)

        if snode.hublvol:
            # Disconnect hublvol from secondary
            if snode.secondary_node_id:
                sec_node = db_controller.get_storage_node_by_id(snode.secondary_node_id)
                if sec_node.status == StorageNode.STATUS_ONLINE:
                    logger.info("Disconnecting hublvol from %s", sec_node.get_id())
                    try:
                        sec_node.rpc_client().bdev_nvme_detach_controller(snode.hublvol.bdev_name)
                    except Exception as e:
                        logger.warning("Disconnecting hublvol failed: %s", e)

            # Disconnect hublvol from tertiary
            if snode.tertiary_node_id:
                ter_node = db_controller.get_storage_node_by_id(snode.tertiary_node_id)
                if ter_node.status == StorageNode.STATUS_ONLINE:
                    logger.info("Disconnecting hublvol from %s", ter_node.get_id())
                    try:
                        ter_node.rpc_client().bdev_nvme_detach_controller(snode.hublvol.bdev_name)
                    except Exception as e:
                        logger.warning("Disconnecting hublvol failed: %s", e)


    # Step 5: hard-kill SPDK. Same code path as the existing --force
    # shutdown — peers see the TCP drop and host multipath retries on
    # surviving paths. Any IO inside SPDK at this instant is lost;
    # that's also true for --force today and is the design contract for
    # kill.
    logger.info("Stopping SPDK")
    try:
        snode.client(**kill_client_kwargs(force)).spdk_process_kill(
            snode.rpc_port, snode.cluster_id)
    except SNodeClientException:
        logger.error('Failed to kill SPDK')
        return False
    pci_address = []
    for dev in snode.nvme_devices:
        if dev.pcie_address not in pci_address:
            try:
                ret = snode.client(timeout=30, retry=1).bind_device_to_nvme(dev.pcie_address)
                logger.debug(ret)
                pci_address.append(dev.pcie_address)
            except Exception as e:
                logger.debug(e)

    # Step 6: status → offline + ANA failover bookkeeping.
    logger.info("Setting node status to offline")
    if not set_node_status(node_id, StorageNode.STATUS_OFFLINE):
        # The FSM refused the flip — typically the record reads RESTARTING,
        # i.e. a restart transition owns this node. SPDK is already killed at
        # this point, but the shutdown has NOT fully committed; reporting
        # success anyway let a restart proceed on top of a half-committed
        # shutdown (2026-07-29 double restart). Fail the shutdown and let the
        # caller retry once the conflicting transition ends (or
        # _reset_if_transient / the orphan watchdog reconciles the record).
        logger.error(
            "Node shutdown incomplete for %s: OFFLINE transition was refused",
            node_id)
        return False

    snode = db_controller.get_storage_node_by_id(node_id)
    try:
        trigger_ana_failover_for_node(snode)
    except Exception as ana_e:
        logger.error("ANA failover during shutdown of %s failed: %s", node_id, ana_e)

    logger.info("Done")
    return True


def suspend_storage_node(node_id: str, force=False):
    """
    Suspends a storage node by changing its status to suspended if the node is in an online state.

    This would exclude this node from lvol host selection, see: lvol_controller._get_next_3_nodes

    Parameters
    ----------
    node_id : int
        The unique identifier of the storage node to be suspended.
    force : bool, optional
        A flag to indicate whether to forcibly suspend the node. Currently unused, defaults to False.

    Returns
    -------
    bool
        Returns True if the node was successfully suspended, or False if the operation failed.
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Node is not in online state")
        return False

    logger.info("Setting node status to suspended")
    set_node_status(snode.get_id(), StorageNode.STATUS_SUSPENDED)

    snode = db_controller.get_storage_node_by_id(node_id)
    snode.auto_restart_disabled = True
    snode.write_to_db(db_controller.kv_store)

    logger.info("Done")
    return True


def resume_storage_node(node_id):
    """
    Resumes a storage node currently in a suspended state.

    This function sets the node status to online.

    Parameters:
    node_id: int
        The unique identifier of the storage node to resume.

    Returns:
    bool
        True if the storage node was successfully resumed, False otherwise.
    """
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("This storage node is not part of the cluster")
        return False

    logger.info("Node found: %s in state: %s", snode.hostname, snode.status)
    if snode.status != StorageNode.STATUS_SUSPENDED:
        logger.error("Node is not in suspended state")
        return False

    logger.info("Setting node status to online")
    set_node_status(snode.get_id(), StorageNode.STATUS_ONLINE)
    snode = db_controller.get_storage_node_by_id(node_id)
    snode.auto_restart_disabled = False
    snode.write_to_db(db_controller.kv_store)

    logger.info("Done")
    return True


def get_node_capacity(node_id, history, records_count=20, parse_sizes=True):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Storage node Not found")
        return

    cap_stats_keys = [
        "date",
        "size_total",
        "size_prov",
        "size_used",
        "size_free",
        "size_util",
        "size_prov_util",
    ]
    prom_client = PromClient(node.cluster_id)
    records = prom_client.get_node_metrics(node_id, cap_stats_keys, history)
    new_records = utils.process_records(records, records_count, keys=cap_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Absolut": utils.humanbytes(record['size_total']),
            "Provisioned": utils.humanbytes(record['size_prov']),
            "Used": utils.humanbytes(record['size_used']),
            "Free": utils.humanbytes(record['size_free']),
            "Util %": f"{record['size_util']}%",
            "Prov Util %": f"{record['size_prov_util']}%",
        })
    return out


def get_node_iostats_history(node_id, history, records_count=20, parse_sizes=True, with_sizes=False):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False
    io_stats_keys = [
        "date",
        "read_bytes",
        "read_bytes_ps",
        "read_io_ps",
        "read_io",
        "read_latency_ps",
        "write_bytes",
        "write_bytes_ps",
        "write_io",
        "write_io_ps",
        "write_latency_ps",
    ]

    if with_sizes:
        io_stats_keys.extend(
            [
                "size_total",
                "size_prov",
                "size_used",
                "size_free",
                "size_util",
                "size_prov_util",
                "read_latency_ticks",
                "record_duration",
                "record_end_time",
                "record_start_time",
                "unmap_bytes",
                "unmap_bytes_ps",
                "unmap_io",
                "unmap_io_ps",
                "unmap_latency_ps",
                "unmap_latency_ticks",
                "write_bytes_ps",
                "write_latency_ticks",
            ]
        )
    prom_client = PromClient(node.cluster_id)
    records = prom_client.get_node_metrics(node_id, io_stats_keys, history)
    # combine records
    new_records = utils.process_records(records, records_count, keys=io_stats_keys)

    if not parse_sizes:
        return new_records

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(record['date'])),
            "Read speed": utils.humanbytes(record['read_bytes_ps']),
            "Read IOPS": record["read_io_ps"],
            "Read lat": record["read_latency_ps"],
            "Write speed": utils.humanbytes(record["write_bytes_ps"]),
            "Write IOPS": record["write_io_ps"],
            "Write lat": record["write_latency_ps"],
        })
    return out


def get_node_ports(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    out = []
    for nic in node.data_nics:
        out.append({
            "ID": nic.get_id(),
            "Device name": nic.if_name,
            "Address": nic.ip4_address,
            "Net type": nic.trtype,
            "Status": nic.status,
        })
    return utils.print_table(out)


def get_node_port_iostats(port_id, history=None, records_count=20):
    db_controller = DBController()
    nodes = db_controller.get_storage_nodes()
    nd = None
    port = None
    for node in nodes:
        for nic in node.data_nics:
            if nic.get_id() == port_id:
                port = nic
                nd = node
                break

    if port is None or nd is None:
        logger.error("Port not found")
        return False

    if history:
        records_number = utils.parse_history_param(history)
        if not records_number:
            logger.error(f"Error parsing history string: {history}")
            return False
    else:
        records_number = 20

    records = db_controller.get_port_stats(nd.get_id(), port.get_id(), limit=records_number)
    new_records = utils.process_records(records, records_count)

    out = []
    for record in new_records:
        out.append({
            "Date": time.strftime("%H:%M:%S, %d/%m/%Y", time.gmtime(record['date'])),
            "out_speed": utils.humanbytes(record['out_speed']),
            "in_speed": utils.humanbytes(record['in_speed']),
            "bytes_sent": utils.humanbytes(record['bytes_sent']),
            "bytes_received": utils.humanbytes(record['bytes_received']),
        })
    return utils.print_table(out)


def upgrade_automated_deployment_config():
    try:
        new_config = utils.load_config(constants.NODES_CONFIG_FILE)
        if not utils.validate_config(new_config, True):
            return False
        origin_config = utils.load_config(f"{constants.NODES_CONFIG_FILE}_read_only")
        updated_config = utils.regenerate_config(new_config, origin_config)
        if not updated_config or not updated_config.get("nodes"):
            return False
        utils.store_config_file(updated_config, constants.NODES_CONFIG_FILE, create_read_only_file=True)
        # Set Huge page memory
        huge_page_memory_dict: dict = {}
        for node_config in updated_config["nodes"]:
            numa = node_config["socket"]
            huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
        for numa, huge_page_memory in huge_page_memory_dict.items():
            num_pages = huge_page_memory // (2048 * 1024)
            utils.set_hugepages_if_needed(numa, num_pages)
        logger.info("Config regenerated successfully")
        return True
    except FileNotFoundError:
        logger.error("Error: Config file not found!")
        return False
    except json.JSONDecodeError:
        logger.error("Error: Config file is not valid JSON!")
        return False


def generate_automated_deployment_config(max_lvol, max_prov, sockets_to_use, nodes_per_socket, pci_allowed, pci_blocked,
                                         vcpu_count=0, force=False, device_model="", size_range="", nvme_names=None, k8s=False,
                                         calculate_hp_only=False, number_of_devices=0):
    # Reject an over-cap max_lvol here rather than only in the CLI: this is the
    # single entry point shared by `sn configure` and the k8s node-configure
    # job, and the value it writes into NODES_CONFIG_FILE becomes the node's
    # max_lvol at add time. Above the cap it also sizes huge pages for
    # subsystems the node can never serve (placement caps at
    # MAX_SUBSYSTEMS_PER_NODE).
    if int(max_lvol or 0) > constants.MAX_SUBSYSTEMS_PER_NODE:
        logger.error(f"max_lvol {max_lvol} exceeds the maximum of "
                     f"{constants.MAX_SUBSYSTEMS_PER_NODE} subsystems per storage node")
        return False
    if calculate_hp_only:
        minimum_hp_memory = utils.calculate_hp_only(max_lvol, number_of_devices, sockets_to_use, nodes_per_socket, vcpu_count)
        hp_number = math.ceil(minimum_hp_memory / 2)
        logger.info(f"The required number of huge pages on this host is: {hp_number} ({minimum_hp_memory} MB)")
        return True
    else:
        # we need minimum of 6 VPCs. RAM 4GB min. Plus 0.2% of the storage.
        total_cores = os.cpu_count() or 0
        if total_cores < 6:
            raise ValueError("Error: Not enough CPU cores to deploy storage node. Minimum 6 cores required.")

        # load vfio_pci and uio_pci_generic
        utils.load_kernel_module("vfio_pci")
        utils.load_kernel_module("uio_pci_generic")

        nodes_config, system_info = utils.generate_configs(max_lvol, max_prov, sockets_to_use, nodes_per_socket,
                                                           pci_allowed, pci_blocked, vcpu_count, force=force,
                                                           device_model=device_model, size_range=size_range, nvme_names=nvme_names)
        if not nodes_config or not nodes_config.get("nodes"):
            return False
        utils.store_config_file(nodes_config, constants.NODES_CONFIG_FILE, create_read_only_file=True)
        if system_info:
            utils.store_config_file(system_info, constants.SYSTEM_INFO_FILE)
        huge_page_memory_dict: dict = {}

        # Set Huge page memory
        for node_config in nodes_config["nodes"]:
            numa = node_config["socket"]
            huge_page_memory_dict[numa] = huge_page_memory_dict.get(numa, 0) + node_config["huge_page_memory"]
        if not k8s:
            utils.create_rpc_socket_mount()
        # for numa, huge_page_memory in huge_page_memory_dict.items():
        #    num_pages = huge_page_memory // (2048 * 1024)
        #    utils.set_hugepages_if_needed(numa, num_pages)
    return True


def deploy(ifname, isolate_cores=False):
    if not ifname:
        ifname = "eth0"

    dev_ip = utils.get_iface_ip(ifname)
    if not dev_ip:
        logger.error(f"Error getting interface ip: {ifname}")
        return False
    try:
        nodes_config = utils.load_config(constants.NODES_CONFIG_FILE)
        logger.info("Config loaded successfully.")
    except FileNotFoundError:
        logger.error("Error: Config file not found!")
        return False
    except json.JSONDecodeError:
        logger.error("Error: Config file is not valid JSON!")
        return False
    all_isolated_cores = utils.validate_config(nodes_config)
    if not all_isolated_cores:
        return False
    logger.info("Config Validated successfully.")

    logger.info("NVMe SSD devices found on node:")
    for line in subprocess.check_output(["lspci", "-Dnn"], text=True).splitlines():
        if f"[{LINUX_DRV_MASS_STORAGE_ID:02x}{LINUX_DRV_MASS_STORAGE_NVME_TYPE_ID:02x}]".lower() in line.lower():
            logger.info(line.strip())

    logger.info("Installing dependencies...")
    scripts.install_deps(mode="docker")

    logger.info(f"Node IP: {dev_ip}")
    scripts.configure_docker(dev_ip)

    start_storage_node_api_container(dev_ip)

    if isolate_cores:
        utils.generate_realtime_variables_file(all_isolated_cores)
        utils.run_tuned()
        arch = platform.machine().lower()
        if "arm" in arch or "aarch64" in arch:
            utils.run_grubby(all_isolated_cores)
    return f"{dev_ip}:5000"


def start_storage_node_api_container(node_ip, cluster_ip=None):
    node_docker = docker.DockerClient(base_url=f"tcp://{node_ip}:2375", version="auto", timeout=60 * 5)
    # node_docker = docker.DockerClient(base_url='unix://var/run/docker.sock', version="auto", timeout=60 * 5)
    logger.info(f"Pulling image {constants.SIMPLY_BLOCK_DOCKER_IMAGE}")
    pull_docker_image_with_retry(node_docker, constants.SIMPLY_BLOCK_DOCKER_IMAGE)

    logger.info("Recreating SNodeAPI container")

    # create the api container
    utils.remove_container(node_docker, '/SNodeAPI')

    if cluster_ip is not None:
        log_config = LogConfig(type=LogConfig.types.GELF, config={"gelf-address": f"tcp://{cluster_ip}:12202"})
    else:
        log_config = LogConfig(type=LogConfig.types.JOURNALD)

    node_docker.containers.run(
        constants.SIMPLY_BLOCK_DOCKER_IMAGE,
        "sudo -E python3 simplyblock_web/node_webapp.py storage_node",
        detach=True,
        privileged=True,
        name="SNodeAPI",
        network_mode="host",
        log_config=log_config,
        volumes=[
            '/etc/simplyblock:/etc/simplyblock',
            '/etc/foundationdb:/etc/foundationdb',
            '/var/tmp:/var/tmp',
            '/var/run:/var/run',
            '/dev:/dev',
            '/lib/modules/:/lib/modules/',
            '/sys:/sys',
            # Bind-mount the SPDK ramdisk so the spdk_process_is_up endpoint
            # can probe SPDK's JSON-RPC Unix socket directly at
            # /mnt/ramdisk/spdk_<port>/spdk.sock. Without this, the endpoint
            # has to fall through to dockerd, which can stall for 60-80s
            # during post-outage Swarm reconciliation (incident 2026-04-24).
            '/mnt/ramdisk:/mnt/ramdisk',
            '/var/run/simplyblock:/var/run/simplyblock'],
        restart_policy={"Name": "always"},
        environment=[
            f"DOCKER_IP={node_ip}",
            "WITHOUT_CLOUD_INFO=True",
            "SIMPLYBLOCK_LOG_LEVEL=DEBUG",
        ]
    )
    logger.info(f"Pulling image {constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE}")
    pull_docker_image_with_retry(node_docker, constants.SIMPLY_BLOCK_SPDK_ULTRA_IMAGE)
    return True


def deploy_cleaner():
    scripts.deploy_cleaner()


def clean_devices(config_path, format=True, force=False, format_4k=False):
    utils.clean_devices(config_path, format=format, force=force, format_4k=format_4k)


def get_host_secret(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    return node.host_secret.get_secret_value()


def get_ctrl_secret(node_id):
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("node not found")
        return False

    return node.ctrl_secret.get_secret_value()


def get_info(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    node_info, _ = snode.client().info()
    return node_info


def get_spdk_info(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    rpc_client = snode.rpc_client()
    ret = rpc_client.ultra21_util_get_malloc_stats()
    if not ret:
        logger.error(f"Failed to get SPDK info for node {node_id}")
        return False
    data = []
    for key in ret.keys():
        data.append({
            "Key": key,
            "Value": ret[key],
            "Parsed": utils.humanbytes(ret[key])
        })
    return utils.print_table(data)


def get(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    return snode.get_clean_dict()


# States from which a node may legally transition INTO STATUS_ONLINE.
# Going online is the most consequential write in the state machine: it
# tells peers the node is serving IO. The transient "active operation
# in progress" predecessors are obvious:
#   - RESTARTING : restart impl finished, ready to commit ONLINE.
#   - IN_CREATION: add_node finished provisioning the new node.
#   - SUSPENDED  : resume_storage_node lifting the suspension.
# UNREACHABLE / DOWN are also legal: the monitor's check_node tail
# only flips them when *every* health probe (ping, SnodeAPI,
# spdk_process, RPC, port_check) just passed. SPDK is alive and the
# listener is reachable — the node is in fact serving. Without this,
# transient mgmt-plane blips and port flaps strand the node forever
# (lab incident 2026-05-06).
_ALLOWED_PRE_STATUSES_FOR_ONLINE = (
    StorageNode.STATUS_RESTARTING,
    StorageNode.STATUS_IN_CREATION,
    StorageNode.STATUS_SUSPENDED,
    StorageNode.STATUS_UNREACHABLE,
    StorageNode.STATUS_DOWN,
)

# Callers permitted to flip a node out of STATUS_RESTARTING to STATUS_OFFLINE.
# The only legitimate cleanup path is the restart wrapper's finally block —
# everything else (monitors, health service, task runners) must respect the
# in-progress restart and leave the lock alone. The wrapper itself uses a
# direct DB write (storage_node_ops.py:2373) and bypasses this helper, so
# this whitelist is for callers that still route through set_node_status
# (e.g. tasks_runner_restart._reset_if_transient which tags itself).
_ALLOWED_CAUSED_BY_RESTARTING_TO_OFFLINE = (
    "restart_cleanup",
)


def set_node_status(node_id, status, caused_by="monitor"):
    """Write a status transition for the node. Pure bookkeeping: emits
    the event, broadcasts to peers, and (on ONLINE) cancels any pending
    auto-restart tasks for this node. Does NOT do peer connects, hublvol
    wiring, or device-event broadcasts — those are the caller's job
    (the restart impl, resume_storage_node, etc. all already do them
    before calling this function)."""
    from simplyblock_core.controllers import tasks_controller

    db_controller = DBController()
    snode = db_controller.get_storage_node_by_id(node_id)
    if snode is None:
        logger.error(f"set_node_status: node {node_id} not found")
        return False

    now = str(datetime.datetime.now(datetime.timezone.utc))
    # verdict communicates the (single, committed) outcome of the mutator out
    # of the transaction so the irreversible work — event emission, peer
    # broadcast, task cancellation, error logging — happens exactly once,
    # AFTER commit. The mutator itself must stay side-effect-free because
    # fdb.transactional replays it on write conflicts.
    outcome: dict = {"verdict": None, "old_status": None, "from": None}

    def _mutate(n):
        if n.status == status:
            outcome["verdict"] = "noop"
            return False
        if status == StorageNode.STATUS_ONLINE and n.status not in _ALLOWED_PRE_STATUSES_FOR_ONLINE:
            # Hard reject: ONLINE may only be reached from RESTARTING (restart
            # path), IN_CREATION (add_node path), or SUSPENDED (resume path).
            # Other paths must route through one of those states first.
            outcome["verdict"] = "reject_online"
            outcome["from"] = n.status
            return False
        if (status == StorageNode.STATUS_OFFLINE
                and n.status == StorageNode.STATUS_RESTARTING
                and caused_by not in _ALLOWED_CAUSED_BY_RESTARTING_TO_OFFLINE):
            # Symmetric to the ONLINE guard above: RESTARTING is the restart
            # impl's exclusive lock. Anything else clobbering it to OFFLINE
            # mid-flight (HealthCheck, StorageNodeMonitor, MainDistrEventCollector,
            # auto-restart task races) strands the node — the impl's later
            # set_node_status(ONLINE, caused_by="restart") then hits the
            # OFFLINE → ONLINE rejection above, returns False, and the CLI
            # exits silently with the node parked in OFFLINE forever.
            # Observed: incident 2026-05-20 iter 57 (forced restart of
            # 5110e910 stuck offline for 16 min until soak gave up).
            outcome["verdict"] = "reject_offline"
            outcome["from"] = n.status
            return False

        outcome["verdict"] = "changed"
        outcome["old_status"] = n.status
        n.status = status
        n.updated_at = now
        if status == StorageNode.STATUS_ONLINE:
            n.online_since = now
            # The node is back ONLINE — necessarily via a deliberate restart
            # while auto-restart was blocked, or via the normal restart path.
            # Either way a prior deliberate-shutdown marker no longer applies;
            # clear it so future genuine failures auto-restart this node again.
            n.auto_restart_disabled = False
        else:
            n.online_since = ""
        # Stamp/clear the DOWN entry time so get_next_cluster_status can apply a
        # grace window: a transient DOWN (self-healing writer conflict) must not
        # tip the cluster into suspend, but a sustained DOWN still must.
        if status == StorageNode.STATUS_DOWN:
            n.down_since = now
        else:
            n.down_since = ""
        # Stamp/clear the IN_SHUTDOWN entry time so the monitor can reconcile a
        # node stranded in in_shutdown (e.g. a lost-update reverting the offline
        # flip, or a crashed shutdown) back to OFFLINE after a grace window.
        if status == StorageNode.STATUS_IN_SHUTDOWN:
            n.shutdown_since = now
        else:
            n.shutdown_since = ""
        return True

    # Atomic compare-and-set: the guard checks above are evaluated against the
    # FRESH row inside the transaction, and the write can no longer clobber a
    # concurrent update from another service (HealthCheck/Monitor/restart task)
    # — the lost-update race this function's incident comments document.
    snode = db_controller.atomic_update(snode, _mutate)
    if snode is None:
        logger.error(f"set_node_status: node {node_id} disappeared during update")
        return False

    verdict = outcome["verdict"]
    if verdict == "noop":
        return True
    if verdict == "reject_online":
        logger.error(
            f"Refusing illegal status transition for {node_id}: "
            f"{outcome['from']} -> ONLINE. Only {_ALLOWED_PRE_STATUSES_FOR_ONLINE} -> ONLINE is allowed."
        )
        return False
    if verdict == "reject_offline":
        logger.error(
            f"Refusing illegal status transition for {node_id}: "
            f"{outcome['from']} -> OFFLINE from caused_by={caused_by!r}. "
            f"Only {_ALLOWED_CAUSED_BY_RESTARTING_TO_OFFLINE} may flip "
            f"a RESTARTING node to OFFLINE."
        )
        return False

    storage_events.snode_status_change(snode, snode.status, outcome["old_status"], caused_by=caused_by)
    distr_controller.send_node_status_event(snode, status)

    if status == StorageNode.STATUS_ONLINE:
        # The node is back online; obsolete auto-restart tasks must not
        # linger in the queue, or the dedup guard in
        # _validate_new_task_node_restart blocks every subsequent restart
        # attempt until the task runner happens to pick the orphan up.
        try:
            tasks_controller.cancel_pending_node_restart_tasks(snode.cluster_id, node_id)
        except Exception as e:
            logger.error(f"Failed to cancel pending node_restart tasks for {node_id}: {e}")

    return True


def _set_restart_phase(snode: StorageNode, lvs_name, phase, db_controller):
    """Persist the restart phase for a given LVS to FDB.

    Other services check this to gate sync deletes and create/clone/
    resize/snapshot registrations. All non-empty phases are treated as
    "restart in progress" — operations during any of them are queued and
    applied after the phase is cleared:

    - pre_block     : restart task has claimed the LVS but hasn't blocked
                      the client port yet. The primary-side operation can
                      still run; however the restarting peer's SPDK state
                      is about to be torn down and rebuilt, so fanning
                      the operation out to it now would be lost. Queue it.
    - blocked       : client port blocked, examine + hublvol wiring in
                      flight. Queue.
    - post_unblock  : port unblocked, but the subsystem re-registration
                      loop is still running on the restarting node — an
                      nvmf_subsystem_add_ns for a concurrently-created
                      lvol would race a subsystem_create in the restart
                      flow. Queue until the phase is cleared.
    - ""            : not in restart.

    When transitioning out of any non-empty phase to a phase that implies
    "the restart task is done with the queue", the queue is drained: from
    BLOCKED → POST_UNBLOCK (once the rebuild owns the node) and from
    POST_UNBLOCK → "" (once the per-lvol subsystem re-registration has
    finished). Operations are applied in FIFO order.
    """
    node_id = snode.get_id()
    snode = db_controller.get_storage_node_by_id(node_id)
    old_phase = snode.restart_phases.get(lvs_name, "") if snode.restart_phases else ""

    # Atomic: a full-object write here is a lost-update hazard in BOTH
    # directions — it can clobber concurrent updates to other node fields,
    # and a concurrent full-object writer holding a stale copy can
    # resurrect a phase this call just cleared. A resurrected phase is
    # catastrophic: check_non_leader_for_operation queues every subsequent
    # create/delete/resize registration for this LVS into a drain queue
    # that no future transition ever drains (incident 2026-07-10: lvol
    # cef09c39's tertiary subsystem was never created — the volume ran on
    # 2/3 paths until a dual outage within FTT killed all IO).
    def _mutate(fresh):
        if not fresh.restart_phases:
            fresh.restart_phases = {}
        if phase:
            fresh.restart_phases[lvs_name] = phase
        else:
            fresh.restart_phases.pop(lvs_name, None)

    db_controller.atomic_update(snode, _mutate)
    logger.info("Restart phase for %s on %s: %s", lvs_name, node_id[:8], phase or "cleared")

    # Drain queued operations whenever the phase advances past a queue-gating
    # state. Two drain points, both drain the same FIFO queue:
    #   1. BLOCKED → POST_UNBLOCK: rebuild is done enough that RPCs won't
    #      race the examine. Drain so ops queued during pre_block+blocked
    #      can execute before clients resume.
    #   2. POST_UNBLOCK → "": subsystem re-registration has also finished.
    #      Drain any ops that arrived between the previous drain and now
    #      (e.g. a create submitted during post_unblock) so they don't
    #      hit a partially-initialized node.
    # The queue is popped on drain so a second drain on an empty queue is
    # a no-op.
    if old_phase == StorageNode.RESTART_PHASE_BLOCKED and phase == StorageNode.RESTART_PHASE_POST_UNBLOCK:
        drain_restart_queue(node_id, lvs_name)
    elif old_phase == StorageNode.RESTART_PHASE_POST_UNBLOCK and phase == "":
        drain_restart_queue(node_id, lvs_name)



def _set_lvstore_status_atomic(node_id, value, db_controller):
    """Set a node's lvstore_status via atomic_update. Full-object writes of
    node records race concurrent flows (parallel activation/restart workers)
    AND phase transitions on the same record — a stale copy written back
    resurrects a just-cleared restart phase (stale-phase generator behind
    the 2026-07-10 lost-registration incidents)."""
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return
    db_controller.atomic_update(
        node, lambda n, v=value: setattr(n, "lvstore_status", v))


def get_restart_phase(node_id, lvs_name):
    """Get the current restart phase for a node/LVS. Used by other services.

    Returns the phase string, or "" if not in restart.

    Self-heals stale phases: a phase is only meaningful while some flow
    actually OWNS the LVS state — a node restart (node status RESTARTING;
    covers both task-driven and API-driven restarts, which flip the status
    via try_set_node_restarting before any phase is set), a cluster
    activation, or an expansion move (cluster IN_ACTIVATION /
    IN_EXPANSION). Outside those, a non-empty phase is a leaked or
    resurrected leftover — e.g. a concurrent full-object node write
    undoing a just-committed clear (incident 2026-07-10: phase for
    LVS_6 on the tertiary read non-empty 12 minutes after its restart
    logged "cleared"; every subsequent lvol registration for that LVS
    was queued into a drain queue that no future phase transition would
    ever drain, so the volume's tertiary subsystem was never created and
    a dual outage within FTT killed all IO paths). Returning such a
    phase converts one lost write into a permanent operation black hole,
    so clear it (atomically) and report "not in restart" instead.
    """
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
        phase = node.restart_phases.get(lvs_name, "") if node.restart_phases else ""
        if not phase:
            return ""
        if node.status == StorageNode.STATUS_RESTARTING:
            return phase
        try:
            cluster = db_controller.get_cluster_by_id(node.cluster_id)
            if cluster.status in Cluster.TOPOLOGY_OWNED_STATUSES:
                return phase
        except KeyError:
            pass
        # WARNING, not ERROR: this is a successful self-repair on an
        # otherwise-successful operation. CLI/test harnesses treat "ERROR:"
        # in a create's output as "the create did not commit" and retry —
        # an ERROR here made the soak loop on 'LVol name must be unique'
        # after a create that actually succeeded (2026-07-10 20:22 run).
        logger.warning(
            "Stale restart phase %r for %s on %s: node is %s, no restart/"
            "activation/expansion/shrink owns this LVS — clearing it so "
            "operations proceed instead of queueing into a dead drain queue",
            phase, lvs_name, node_id[:8], node.status)

        def _clear(fresh):
            if fresh.restart_phases:
                fresh.restart_phases.pop(lvs_name, None)

        db_controller.atomic_update(node, _clear)
        return ""
    except (KeyError, Exception):
        return ""


def wait_or_delay_for_restart_gate(node_id, lvs_name, timeout=30):
    """Gate for sync deletes and create/clone/resize registrations.

    Any non-empty restart phase (pre_block / blocked / post_unblock)
    returns ``"delay"`` — the caller must queue the op via
    :func:`queue_for_restart_drain`. The queue drains automatically on
    the ``BLOCKED → POST_UNBLOCK`` and ``POST_UNBLOCK → ""`` transitions
    in :func:`_set_restart_phase`, so the op lands on the rebuilt node in
    FIFO order after per-lvol subsystem re-registration has completed.

    Why all three non-empty phases delay:

    - ``pre_block``: restart task has claimed the LVS. The node's SPDK
      state is about to be torn down / rebuilt; applying a metadata op
      now would be lost by the rebuild.
    - ``blocked``: client port blocked, examine in flight. Applying a
      create/delete now can race examine's read of the primary's
      blobstore.
    - ``post_unblock``: client port unblocked but the per-lvol subsystem
      re-registration loop on the restarting node is still running.
      ``nvmf_subsystem_add_ns`` from a mgmt-side create would race the
      restart's own ``subsystem_create``.

    Normal (healthy) case: phase is empty → returns ``"proceed"``
    immediately. Operations execute in ms.
    """
    phase = get_restart_phase(node_id, lvs_name)
    if phase in (StorageNode.RESTART_PHASE_PRE_BLOCK,
                 StorageNode.RESTART_PHASE_BLOCKED,
                 StorageNode.RESTART_PHASE_POST_UNBLOCK):
        return "delay"
    return "proceed"


# Per-node ordered queue for operations delayed during port block.
# Key: (node_id, lvs_name), Value: list of (callable, description) in FIFO order.
_restart_op_queues: dict[tuple[str, str], list[tuple]] = {}
_restart_op_queues_lock = threading.Lock()


def queue_for_restart_drain(node_id, lvs_name, operation_fn, description=""):
    """Queue an operation for execution after port unblock.

    WARNING — per-process and volatile: this queue is a module-level dict,
    so it exists separately in every process, is drained only by the
    process that performs the phase transitions, and dies with the
    process. An op queued by the webappapi while the restart runner owns
    the restart is unrecoverable (incident 2026-07-10). Lvol
    create/delete/resize deferrals therefore use DB-backed tasks instead
    (``tasks_controller.add_lvol_sync_op_task`` /
    ``add_lvol_sync_del_task``). Only snapshot flows still queue here —
    do NOT add new callers; use a durable task.

    Called when wait_or_delay_for_restart_gate returns "delay".
    Operations are appended in order and will be drained sequentially
    by drain_restart_queue() after phase transitions to post_unblock.

    Args:
        node_id: target node
        lvs_name: LVS being restarted
        operation_fn: callable() that performs the actual RPC
        description: human-readable description for logging
    """
    key = (node_id, lvs_name)
    with _restart_op_queues_lock:
        if key not in _restart_op_queues:
            _restart_op_queues[key] = []
        _restart_op_queues[key].append((operation_fn, description))
    logger.info("Queued operation for post-unblock drain on %s/%s: %s",
                node_id[:8], lvs_name, description)


def drain_restart_queue(node_id, lvs_name):
    """Drain all queued operations for a node/LVS after port unblock.

    Called by the restart code after phase transitions to post_unblock.
    Executes operations in strict FIFO order, single-threaded.
    """
    key = (node_id, lvs_name)
    with _restart_op_queues_lock:
        queue = _restart_op_queues.pop(key, [])

    if not queue:
        return

    logger.info("Draining %d queued operations for %s/%s", len(queue), node_id[:8], lvs_name)
    for operation_fn, description in queue:
        try:
            logger.info("Executing queued operation: %s", description)
            operation_fn()
        except Exception as e:
            logger.error("Queued operation failed (%s): %s", description, e)


def _is_node_rpc_responsive(node: StorageNode, lvs_name, timeout=5, retry=2):
    """Check if a node's RPC interface is responsive.

    Returns True if RPC succeeds, False if it fails/times out.
    RPC is considered failing if it returns an error code or times out
    beyond the defined retries.
    """
    try:
        rpc = node.rpc_client(timeout=timeout, retry=retry)
        ret = rpc.bdev_lvol_get_lvstores(lvs_name)
        return ret is not None
    except Exception:
        return False


def _is_fabric_connected(node: StorageNode, lvs_peer_ids=None):
    """Check if a node's fabric is connected (JM quorum says NOT disconnected)."""
    return not _check_peer_disconnected(node, lvs_peer_ids=lvs_peer_ids)


def _count_fabric_disconnected_nodes(all_nodes, lvs_peer_ids=None):
    """Count how many nodes have disconnected fabric."""
    count = 0
    for n in all_nodes:
        if _check_peer_disconnected(n, lvs_peer_ids=lvs_peer_ids):
            count += 1
    return count


def _leadership_moving_tasks_active(cluster_id, node_ids):
    """True when a port-allow or restart task is active on any LVS member.

    Those flows own leadership movement (the restart flow's fenced
    demote->grant handoff; the port-allow failback demotes the acting leader
    and lets the primary self-promote); a concurrent recovery grant fights
    them — run 20260725 21:18-21:22: take-leadership grants on the primary
    vs port-allow demotes on the acting leader, flapping LVS_1 leadership
    for minutes; 2026-07-30 LVS_9: a recovery grant seated the secondary as
    writer moments before the primary's restart handoff. When the task
    state cannot be read, err on NOT granting."""
    try:
        db = DBController()
        for task in db.get_job_tasks(cluster_id):
            if task.function_name not in (JobSchedule.FN_PORT_ALLOW,
                                          JobSchedule.FN_NODE_RESTART):
                continue
            if task.node_id in node_ids and \
                    task.status != JobSchedule.STATUS_DONE and not task.canceled:
                return True
    except Exception as e:
        logger.warning("Cannot verify port-allow/restart task state for "
                       "leaderless recovery (%s) — refusing to grant", e)
        return True
    return False


def _taker_jm_quorum_ok(taker):
    """True when the prospective leadership taker's JC reports at least the
    JM write quorum (2) ready. Granting leadership to a primary whose JMs are
    excluded is pointless and harmful: it self-demotes on the next quorum
    check and the grant/demote cycle flaps (run 20260725: LVS_1 primary
    self-demoted at 20:46/20:55 on JM quorum loss, every subsequent grant
    lasted seconds)."""
    if not taker.jm_vuid:
        return False
    try:
        st = taker.rpc_client(timeout=5, retry=1).jc_get_jm_status(taker.jm_vuid)
    except Exception as e:
        logger.warning("jc_get_jm_status on %s failed: %s — refusing to grant "
                       "leadership", taker.get_id()[:8], e)
        return False
    if not st:
        return False
    ready = sum(1 for v in st.values() if v)
    if ready < 2:
        logger.warning("taker %s has only %d ready JM(s) — refusing to grant "
                       "leadership", taker.get_id()[:8], ready)
        return False
    return True


def _recover_leaderless_lvs(cluster_id, all_nodes, lvs_name, preferred_taker):
    """Recovery for a leaderless-but-healthy LVS.

    Leadership placement is otherwise the restart/creation/activation flows'
    job; this recovery runs only when an object operation needs a leader and
    none exists. It never flips the leadership flag blind: the 2026-07-06
    LVS_13 incident showed a bare ``set_leader(True)`` skips the primary's
    blob-md reload and can serve stale metadata, and run 20260725 showed
    unguarded grants flapping against the port-allow handoff.

    Sequence, single-flight cluster-wide:
      0. FDB test-and-set lock keyed ``takeleader/<lvs>``; deliberately never
         released, so the lock TTL (LVSTORE_MUTATION_LOCK_TTL_SEC) doubles as
         the recovery cooldown across all processes.
      1. Verify/repair follower->primary hublvols (redirect path); bounded
         wait for the primary's self-promotion via redirected IO.
      2. Still leaderless (control-plane-only workloads never generate the
         redirected IO that triggers self-promotion), and only if no
         port-allow/restart task owns leadership movement on any member and
         the taker's JM quorum is intact: ``bdev_lvol_update_lvstore`` on the
         taker — an explicit blob-md reload from disk, the same update the
         IO-driven promotion performs — and, only after the reload succeeded,
         ``set_leader(True)``; then verify the grant took.

    Returns the confirmed leader node or None (callers fail fast; the
    no-leader negative cache bounds re-entry)."""
    from simplyblock_core.controllers.lvol_controller import is_node_leader

    db = DBController()
    owner = f"{socket.gethostname()}-{os.getpid()}-{threading.get_ident()}"
    won, holder = db.acquire_lvstore_lock(
        cluster_id, f"takeleader/{lvs_name}", owner)
    if not won:
        logger.warning(
            "leaderless recovery for %s already ran/running within the last "
            "%ss (holder %s) — failing fast", lvs_name,
            constants.LVSTORE_MUTATION_LOCK_TTL_SEC, holder)
        return None

    taker = preferred_taker
    member_ids = [n.get_id() for n in all_nodes]

    # 1- repair the redirect paths so the primary CAN self-promote.
    for peer in all_nodes:
        if peer.get_id() == taker.get_id():
            continue
        if peer.status not in (StorageNode.STATUS_ONLINE, StorageNode.STATUS_DOWN):
            continue
        try:
            health_controller._check_sec_node_hublvol(
                peer, auto_fix=True, primary_node_id=taker.get_id())
        except Exception as e:
            logger.warning("hublvol verify/repair %s -> %s failed: %s",
                           peer.get_id()[:8], taker.get_id()[:8], e)

    # Bounded wait: with hublvols open, the first redirected IO triggers the
    # primary's LVS update and self-promotion (the sanctioned path).
    for _ in range(5):
        for node in all_nodes:
            try:
                if is_node_leader(node, lvs_name):
                    logger.info("Leadership for %s restored on %s (self-"
                                "promotion)", lvs_name, node.get_id()[:8])
                    return node
            except Exception:
                continue
        time.sleep(1)

    # 2- reload-then-grant, guarded. The explicit bdev_lvol_update_lvstore
    # replays the same blob-md reload the IO-driven promotion does, so the
    # subsequent set_leader cannot serve stale metadata.
    if _leadership_moving_tasks_active(cluster_id, member_ids):
        logger.warning("leaderless recovery for %s: port-allow/restart task "
                       "active on an LVS member — leaving leadership movement "
                       "to it", lvs_name)
        return None
    if not _taker_jm_quorum_ok(taker):
        return None
    logger.warning(
        "LVS %s still leaderless after hublvol repair and no handoff task is "
        "active — reloading lvstore metadata on %s before granting leadership",
        lvs_name, taker.get_id())
    try:
        if not taker.rpc_client(timeout=10, retry=1).bdev_lvol_update_lvstore(
                lvs_name):
            logger.error("bdev_lvol_update_lvstore on %s for %s returned "
                         "False — refusing to grant leadership on top of "
                         "un-reloaded metadata", taker.get_id(), lvs_name)
            return None
    except Exception as e:
        logger.error("bdev_lvol_update_lvstore on %s for %s failed: %s — "
                     "refusing to grant leadership", taker.get_id(),
                     lvs_name, e)
        return None
    try:
        taker.rpc_client(timeout=5, retry=2).bdev_lvol_set_leader(
            lvs_name, leader=True)
    except Exception as e:
        logger.error("take-leadership RPC on %s for %s failed: %s",
                     taker.get_id(), lvs_name, e)
        return None
    for _ in range(5):
        try:
            if is_node_leader(taker, lvs_name):
                logger.info("Leadership for %s restored on %s (metadata "
                            "reloaded first)", lvs_name, taker.get_id())
                return taker
        except Exception:
            pass
        time.sleep(1)
    logger.error("take-leadership on %s for %s did not take effect — "
                 "refusing to route", taker.get_id(), lvs_name)
    return None


def find_leader_with_failover(all_nodes, lvs_name):
    """Detect the current leader and failover if needed — with a no-leader
    fail-fast gate.

    If a full detection pass recently (< NO_LEADER_TTL_SEC) concluded the LVS
    has no confirmable leader, return (None, []) immediately without probing:
    the caller must fail the operation until a leader is re-established. This
    caps the probe/recovery machinery at one full pass per TTL window per
    process — under a mass-create workload against a leaderless LVS, running
    the pass per request stormed every LVS member with several
    bdev_lvol_get_lvstores per second for hours (run 20260712-231123).
    """
    from simplyblock_core.utils.ttl_cache import no_leader_cache, NO_LEADER_TTL_SEC

    cluster_id = all_nodes[0].cluster_id if all_nodes else ""
    cache_key = (cluster_id, lvs_name)
    if no_leader_cache.get(cache_key, NO_LEADER_TTL_SEC):
        logger.warning(
            "LVS %s was confirmed leaderless less than %ss ago — failing fast "
            "without re-probing; operations are rejected until a leader is "
            "re-established", lvs_name, NO_LEADER_TTL_SEC)
        return None, []

    leader, non_leaders = _find_leader_with_failover_impl(all_nodes, lvs_name)
    if leader is None:
        no_leader_cache.put(cache_key, True)
    else:
        no_leader_cache.invalidate(cache_key)
    return leader, non_leaders


def _find_leader_with_failover_impl(all_nodes, lvs_name):
    """Single full leader-detection/recovery pass.

    0. Cached fast path: if a leader for this lvstore was confirmed within
       LEADER_TTL_SEC, probe ONLY that node (one RPC). Leadership rarely moves,
       so this replaces the 3-node scan on the hot create paths; the probe
       itself is a fresh confirmation, so a moved leadership simply misses and
       falls through to the full scan below.
    1. Try each node as leader via bdev_lvol_get_lvstores (leadership field).
       A node that answers leadership=True is CONFIRMED (the query is itself a
       successful RPC to that node) → return it directly. A confirmed leader is
       never failed over for being slow, only for genuinely not answering.
    2. If no node admitted leadership (every probe returned False or raised),
       attempt a failover — but only ever return a node whose leadership is
       re-confirmed via is_node_leader:
       - Guessed leader's fabric down → promote a non-leader, confirm, return it.
       - Guessed leader's fabric healthy → force a leadership change to a
         responsive non-leader, then VERIFY the change took effect before
         returning. Reject (None) if leadership did not settle.

    Returns:
        (leader_node, non_leader_nodes) or (None, []) if no confirmable leader.
    """
    from simplyblock_core.controllers.lvol_controller import is_node_leader
    from simplyblock_core.utils.ttl_cache import leader_cache, LEADER_TTL_SEC

    leader = None
    leader_confirmed = False
    non_leaders = []

    cluster_id = all_nodes[0].cluster_id if all_nodes else ""
    cache_key = (cluster_id, lvs_name)
    cached_id = leader_cache.get(cache_key, LEADER_TTL_SEC)
    if cached_id:
        cached_node = next((n for n in all_nodes if n.get_id() == cached_id), None)
        if cached_node is not None:
            try:
                if is_node_leader(cached_node, lvs_name):
                    leader_cache.put(cache_key, cached_id)
                    return cached_node, [n for n in all_nodes
                                         if n.get_id() != cached_id]
            except Exception:
                pass
        # Cached leader moved or stopped answering — do the full scan.
        leader_cache.invalidate(cache_key)

    # Find current leader
    for node in all_nodes:
        try:
            if is_node_leader(node, lvs_name):
                leader = node
                # is_node_leader() is itself a successful RPC to the node that
                # returned leadership=True. The node is therefore reachable and
                # authoritative about its own leadership.
                leader_confirmed = True
                break
        except Exception:
            continue

    if leader is None:
        # No leader found via RPC — find first fabric-connected node
        for node in all_nodes:
            if _is_fabric_connected(node):
                leader = node
                break
        if leader is None:
            return None, []

    non_leaders = [n for n in all_nodes if n.get_id() != leader.get_id()]

    # If the leader answered the leadership query above, it is reachable and
    # authoritative — return it directly. Do NOT re-probe with the tighter
    # responsiveness timeout and force a failover: under high concurrency the
    # SPDK reactor mgmt queue backs up and the confirmed leader's RPC can
    # exceed the short probe timeout while the node is perfectly healthy.
    # Treating that as "leader down" forces leadership onto a non-leader and
    # routes the operation to the wrong node (split routing + retry storm).
    # The responsiveness probe / forced failover below is only meaningful when
    # leadership could NOT be confirmed by RPC (the fabric-connected fallback,
    # i.e. the leader's mgmt RPC genuinely failed so is_node_leader raised).
    if leader_confirmed:
        leader_cache.put(cache_key, leader.get_id())
        return leader, non_leaders

    # Unconfirmed-leader fallback: `leader` is only a fabric-connected guess —
    # no node admitted leadership above. RPC-responsiveness does NOT make a node
    # the leader, so a tight responsiveness probe here can route the operation to
    # a non-leader. Require an actual leadership confirmation before returning;
    # otherwise fall through to the failover logic.
    try:
        if is_node_leader(leader, lvs_name):
            leader_cache.put(cache_key, leader.get_id())
            return leader, non_leaders
    except Exception:
        pass

    # Leaderless-but-healthy recovery: the guessed leader answers RPC fine yet
    # reports leadership=False — and so did every other candidate in the scan.
    # This is not "leader down": the LVS has NO leader at all (e.g. a forced
    # restart proceeded after its take-leadership step failed — soak
    # 2026-07-10 21:52, LVS_7: primary+sec+tert all leadership=False, all
    # healthy). The forced-handoff below cannot repair it: the signal only
    # asks a leader to DROP leadership, nobody holds it, and the JC never
    # elects while the primary is alive. Recovery is delegated to
    # _recover_leaderless_lvs: hublvol repair + bounded self-promotion wait,
    # then a guarded reload-then-grant (bdev_lvol_update_lvstore before
    # set_leader, so the grant never serves stale blob metadata — incident
    # 2026-07-06 LVS_13). The previous unguarded per-call set_leader(True)
    # here fired from every API worker at once and fought the port-allow
    # handoff (run 20260725), and a bare grant raced the restart handoff
    # into a writer conflict (2026-07-30 LVS_9) — hence the single-flight
    # lock, the moving-task guard, and the mandatory metadata reload.
    if _is_node_rpc_responsive(leader, lvs_name):
        # Last-moment sweep: abort the recovery if anyone became leader meanwhile.
        for node in all_nodes:
            try:
                if is_node_leader(node, lvs_name):
                    leader_cache.put(cache_key, node.get_id())
                    return node, [n for n in all_nodes
                                  if n.get_id() != node.get_id()]
            except Exception:
                continue
        # Prefer the configured primary of this LVS as the taker; fall back
        # to the responsive guess.
        taker = next((n for n in all_nodes if n.lvstore == lvs_name), leader)
        if taker.get_id() != leader.get_id() and not (
                _is_fabric_connected(taker)
                and _is_node_rpc_responsive(taker, lvs_name)):
            taker = leader
        recovered = _recover_leaderless_lvs(cluster_id, all_nodes, lvs_name, taker)
        if recovered is not None:
            leader_cache.put(cache_key, recovered.get_id())
            return recovered, [n for n in all_nodes
                               if n.get_id() != recovered.get_id()]
        return None, []

    # Leader unconfirmed — check if fabric is healthy
    if not _is_fabric_connected(leader):
        # Fabric disconnected — leader truly down, find new leader
        for nl in non_leaders:
            if _is_fabric_connected(nl) and _is_node_rpc_responsive(nl, lvs_name):
                # Promotion is driven by the JM heartbeat; confirm the peer has
                # actually taken leadership before routing to it.
                try:
                    if not is_node_leader(nl, lvs_name):
                        continue
                except Exception:
                    continue
                logger.info("Leader %s fabric disconnected, failed over to %s",
                            leader.get_id(), nl.get_id())
                new_non_leaders = [n for n in all_nodes if n.get_id() != nl.get_id()]
                leader_cache.put(cache_key, nl.get_id())
                return nl, new_non_leaders
        return None, []

    # Leader fabric healthy but RPC failing — force leadership change
    # Need at least one non-leader with healthy fabric
    failover_target = None
    for nl in non_leaders:
        if _is_fabric_connected(nl) and _is_node_rpc_responsive(nl, lvs_name):
            failover_target = nl
            break

    if failover_target is None:
        logger.error("Leader %s RPC failing, fabric healthy, but no non-leader available for failover",
                     leader.get_id())
        return None, []

    # Force leadership change via fabric signal: send bdev_lvol_set_lvs_signal
    # FROM failover_target through the fabric TO the leader (whose mgmt is down
    # but data plane is healthy). The signal tells the leader's SPDK to drop
    # leadership for this LVS.
    try:
        rpc = failover_target.rpc_client(timeout=5, retry=2)
        rpc.bdev_lvol_set_lvs_signal(lvs_name)
        time.sleep(2)
        logger.info("Sent bdev_lvol_set_lvs_signal(%s) from %s to leader %s via fabric",
                    lvs_name, failover_target.get_id(), leader.get_id())
    except Exception as e:
        logger.error("Failed to send fabric signal for leadership change: %s", e)
        return None, []

    # Verify the forced leadership change actually took effect before routing.
    # The signal is best-effort: if the old leader was merely slow (not down)
    # and reclaims leadership, failover_target never becomes leader and routing
    # to it would land the operation on a non-leader. Confirm with a short
    # retry; reject if leadership did not settle on failover_target.
    leadership_confirmed = False
    for _ in range(3):
        try:
            if is_node_leader(failover_target, lvs_name):
                leadership_confirmed = True
                break
        except Exception:
            pass
        time.sleep(1)
    if not leadership_confirmed:
        logger.error("Forced leadership change to %s for %s did not take effect — "
                     "refusing to route to a non-leader",
                     failover_target.get_id(), lvs_name)
        return None, []

    new_non_leaders = [n for n in all_nodes if n.get_id() != failover_target.get_id()]
    leader_cache.put(cache_key, failover_target.get_id())
    return failover_target, new_non_leaders


def check_non_leader_for_operation(node_id, lvs_name, operation_type="create",
                                    leader_op_completed=False, all_nodes=None,
                                    wait_for_restart=0):
    """Check a non-leader node's readiness for a sync operation.

    Args:
        node_id: the non-leader node to check
        lvs_name: the LVS name
        operation_type: "create" (create/clone/resize) or "delete"
        leader_op_completed: True if the operation was already executed on leader
        all_nodes: all nodes in the LVS group (for FTT check)

    Returns:
        "proceed" — execute now
        "skip" — disconnected, skip
        "reject" — unreachable+fabric healthy; reject entire operation
        "queue" — restart port blocked OR need to queue for retry
        "kill_and_wait" — kill node and wait for restart (FTT allows)
    """
    db_controller = DBController()
    try:
        node = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return "skip"

    # 1. Check disconnect state (JM quorum)
    lvs_peer_ids = [sid for sid in [node.secondary_node_id, node.tertiary_node_id] if sid]
    if _check_peer_disconnected(node, lvs_peer_ids=lvs_peer_ids):
        return "skip"

    # 2. Check restart phase — any non-empty phase means the restart task
    # owns the node's LVS state and the operation must be queued for the
    # post-rebuild drain. "skip" (the old pre_block behaviour) is incorrect
    # because the primary-side op runs unaffected by the LVS port block
    # (its mgmt RPC goes to port 8085, not 4436), so a pre_block skip can
    # lose a create/delete on the restarting node. See
    # _set_restart_phase for the drain timing.
    _restart_phases = (StorageNode.RESTART_PHASE_PRE_BLOCK,
                       StorageNode.RESTART_PHASE_BLOCKED,
                       StorageNode.RESTART_PHASE_POST_UNBLOCK)
    phase = get_restart_phase(node_id, lvs_name)
    if phase in _restart_phases and wait_for_restart > 0:
        # Caller holds the chain lock and asked to wait this one out. A
        # restart is bounded and self-clearing, so waiting keeps the whole
        # multi-node sequence under one lock instead of handing the leg to
        # a task runner that will execute it later, in another process,
        # with no chain lock at all. Bounded: see
        # DEFERRED_LEG_RESTART_WAIT_SEC -- a wedged restart must not pin
        # the chain.
        deadline = time.time() + wait_for_restart
        while phase in _restart_phases and time.time() < deadline:
            time.sleep(2)
            phase = get_restart_phase(node_id, lvs_name)
        if phase in _restart_phases:
            logger.info(
                "Non-leader %s still in restart phase %s after %ss; "
                "deferring the leg durably", node_id[:8], phase,
                wait_for_restart)
            return "queue"
        logger.info("Non-leader %s left its restart phase; proceeding "
                    "under the held lock", node_id[:8])
    elif phase in _restart_phases:
        return "queue"

    # 3. Fabric is connected — check RPC responsiveness
    if _is_node_rpc_responsive(node, lvs_name):
        return "proceed"

    # 4. RPC failing but fabric connected
    logger.warning("Non-leader %s RPC failing but fabric connected", node_id[:8])

    # Check FTT — can we tolerate this node being unresponsive?
    if all_nodes:
        cluster = db_controller.get_cluster_by_id(node.cluster_id)
        max_ft = getattr(cluster, 'max_fault_tolerance', 1)
        disconnected_count = _count_fabric_disconnected_nodes(all_nodes, lvs_peer_ids)
        if disconnected_count + 1 > max_ft:
            # FTT would be violated — cannot proceed or kill
            if not leader_op_completed:
                logger.warning("Non-leader %s RPC failing, FTT would be violated "
                              "(disconnected=%d, max_ft=%d) — rejecting before leader op",
                              node_id[:8], disconnected_count, max_ft)
                return "reject"
            logger.warning("Cannot kill node %s: would violate FTT (disconnected=%d, max_ft=%d)",
                          node_id[:8], disconnected_count, max_ft)
            return "queue"

        if not leader_op_completed:
            # FTT allows — queue the registration for this non-leader and
            # let the leader operation proceed. The non-leader's
            # registration will be retried once it becomes RPC-responsive.
            logger.info("Non-leader %s RPC failing but FTT tolerates it "
                       "(disconnected=%d, max_ft=%d) — queueing, leader op can proceed",
                       node_id[:8], disconnected_count, max_ft)
            return "queue"

        # AFTER leader operation: FTT allows — kill node, wait for restart
        logger.info("Killing node %s (FTT allows: disconnected=%d, max_ft=%d)",
                    node_id[:8], disconnected_count, max_ft)
        return "kill_and_wait"

    # No all_nodes provided — safe default: queue
    return "queue"


def execute_on_leader_with_failover(all_nodes, lvs_name, operation_fn,
                                    known_leader=None):
    """Execute an operation on the current leader with failover support.

    1. Find leader (with failover if needed) — skipped when the caller passes
       ``known_leader`` from a just-completed find_leader_with_failover, so the
       leadership scan is not paid twice per operation. Failure handling below
       still re-detects, so a leadership flip between the caller's detect and
       the operation is covered by the retry, exactly as before.
    2. Execute operation_fn(leader_node)
    3. If operation fails, re-check leadership and retry on new leader
    4. Return (success, leader_node, result)

    Args:
        all_nodes: list of all StorageNode objects in the LVS group
        lvs_name: LVS name
        operation_fn: callable(leader_node) → result. Returns None/False on failure.
        known_leader: leader StorageNode already confirmed by the caller.

    Returns:
        (True, leader_node, result) on success
        (False, None, error_msg) on failure
    """
    if known_leader is not None:
        leader = known_leader
    else:
        leader, _ = find_leader_with_failover(all_nodes, lvs_name)
    if leader is None:
        return False, None, "No leader available"

    # Execute on leader
    try:
        result = operation_fn(leader)
        if result is not None and result is not False:
            return True, leader, result
    except Exception as e:
        logger.warning("Operation failed on leader %s: %s — re-checking leadership",
                      leader.get_id(), e)

    # Operation failed — re-check leadership
    new_leader, _ = find_leader_with_failover(all_nodes, lvs_name)
    if new_leader is None:
        return False, None, "Operation failed and no leader available"

    if new_leader.get_id() == leader.get_id():
        # Same leader, operation truly failed
        return False, leader, "Operation failed on leader"

    # Leadership changed — retry on new leader
    logger.info("Leadership changed from %s to %s, retrying operation",
               leader.get_id(), new_leader.get_id())
    try:
        result = operation_fn(new_leader)
        if result is not None and result is not False:
            return True, new_leader, result
        return False, new_leader, "Operation failed on new leader"
    except Exception as e:
        return False, new_leader, f"Operation failed on new leader: {e}"


#: Mgmt statuses that make a peer unusable as a routing, port-block, or
#: failover target, without consulting the data plane at all. Each one means
#: mgmt has already observed the peer leaving the cluster and is not expecting
#: it back, so the peer's mgmt API and SPDK are gone (or going).
#:
#: IN_SHUTDOWN / RESTARTING are deliberately absent: those are transient states
#: the runner owns, and preempting another node's leadership during its own
#: restart would be incorrect.
#:
#: PENDING_REMOVAL is also deliberately absent. node_removal_orchestrate sets
#: it *before* phase 1 shuts the node down, so the node is still up and serving
#: then; treating it as gone would skip a port-block it still needs.
_PEER_DISCONNECTED_STATUSES = (
    StorageNode.STATUS_OFFLINE,
    StorageNode.STATUS_REMOVED,
    StorageNode.STATUS_UNREACHABLE,
    StorageNode.STATUS_IN_REMOVAL,
)


def _check_peer_disconnected(peer_node: StorageNode, lvs_peer_ids=None):
    """Check if a peer node should be treated as disconnected for the purpose
    of routing (takeover vs. non-leader path) and peer-port-block decisions.

    Returns True if peer is disconnected (should be skipped), False otherwise.

    Two signals, first match wins:

      1. Mgmt ground truth (FDB status). If FDB already says the peer is in
         one of ``_PEER_DISCONNECTED_STATUSES``, trust it immediately — mgmt
         has observed the peer leaving the cluster. Attempting to port-block
         such a peer's mgmt API will only hit ECONNREFUSED and, after 5×
         retries, abort the entire restart with a misleading "LVStore
         recovery failed" event.

         IN_REMOVAL is in that list on the same grounds as OFFLINE: phase 1 of
         node_removal_orchestrate has already killed the node's SPDK and mgmt
         API by the time the status is set, and unlike IN_SHUTDOWN it is never
         coming back. Without it this check falls through to the JM-quorum
         path, which votes "connected" on `0/0 peers report disconnected`
         exactly when peers have already torn down their controllers for the
         departing node — the abstain-from-all case described below. Callers
         then route to, or pre-warm a failover path towards, a node the
         control plane is in the middle of deleting (live 2026-09-03: a
         tertiary spent its deferred hublvol attach on the node being removed,
         "Failed to add deferred hublvol failover path to <removed> …
         for LVS_21").

      2. Data-plane JM quorum (legacy path). Only reached if mgmt says
         the peer is in an "alive" state. Useful to detect fabric
         partitions where mgmt is still reachable but the data plane
         isn't — the quorum reads NVMe controller state on surviving
         peers (see storage_node_monitor::_count_data_plane_votes).
    """
    from simplyblock_core.services.storage_node_monitor import is_node_data_plane_disconnected_quorum

    # Refresh from FDB before reading peer_node.status. Callers commonly
    # build a sec_nodes list at the top of recreate_lvstore (line ~5223)
    # and then run this check seconds later. If the peer's status flipped
    # to OFFLINE in that window (e.g. monitor's set_node_offline after a
    # container_kill), the cached object's .status is still ONLINE and
    # the FDB-status short-circuit below silently misses. The function
    # then falls through to JM-quorum, which itself can vote "connected"
    # when peers have already torn down their NVMe controllers for the
    # dead peer's JM (`0/0 peers report disconnected` — abstain from all).
    # The caller proceeds to port-block the peer via its mgmt firewall API
    # and hits ECONNREFUSED, aborting the entire restart with a misleading
    # "LVStore recovery failed" event. Lab incident 2026-05-06 iter 2.
    db_ctrl = DBController()
    try:
        peer_node = db_ctrl.get_storage_node_by_id(peer_node.get_id())
    except KeyError:
        # Peer has been fully removed from the cluster — definitely disconnected.
        return True

    if peer_node.status in _PEER_DISCONNECTED_STATUSES:
        logger.info("Peer %s mgmt status is %s — treating as disconnected",
                    peer_node.get_id(), peer_node.status)
        return True

    # The NVMe-ctrlr quorum sweep costs ~10 RPCs across the cluster and is
    # re-run for every peer on every create/clone/snapshot. Its verdict is
    # connectivity state that moves on node-failure timescales, so cache it
    # briefly per process; the FDB-status short-circuit above stays uncached
    # and still catches mgmt-observed transitions immediately. A stale
    # "connected" is bounded by the TTL and by the operation itself failing
    # and re-checking; a stale "disconnected" only delays inclusion of a
    # just-recovered peer by the same window.
    from simplyblock_core.utils.ttl_cache import quorum_verdict_cache, QUORUM_VERDICT_TTL_SEC
    verdict = quorum_verdict_cache.get_or_compute(
        (peer_node.get_id(), tuple(lvs_peer_ids or ())), QUORUM_VERDICT_TTL_SEC,
        lambda: is_node_data_plane_disconnected_quorum(peer_node, lvs_peer_ids=lvs_peer_ids))
    if verdict:
        logger.info("Peer %s is data-plane disconnected (NVMe-ctrlr quorum confirmed), will skip",
                     peer_node.get_id())
        return True

    logger.info("Peer %s is data-plane connected (NVMe-ctrlr quorum check)", peer_node.get_id())
    return False


def _check_hublvol_connected(snode: StorageNode, peer_node):
    """Method 2: Check if the hublvol to peer_node is still connected from snode.

    Per design: used as fallback when RPCs fail/timeout after the quorum check
    said the node was connected.
    - If hublvol IS connected: only management plane unreachable
    - If hublvol is NOT connected: node truly disconnected from fabric

    Returns True if hublvol is connected, False if disconnected.
    """
    try:
        rpc_client = snode.rpc_client(timeout=5, retry=1)
        if peer_node.hublvol and peer_node.hublvol.bdev_name:
            remote_bdev = f"{peer_node.hublvol.bdev_name}n1"
            bdevs = rpc_client.get_bdevs(remote_bdev)
            if bdevs:
                logger.info("HubLVol to %s is still connected from %s",
                            peer_node.get_id(), snode.get_id())
                return True
        logger.info("HubLVol to %s is NOT connected from %s",
                    peer_node.get_id(), snode.get_id())
        return False
    except Exception as e:
        logger.warning("Failed to check hublvol connection to %s: %s", peer_node.get_id(), e)
        return False


def _handle_rpc_failure_on_peer(snode: StorageNode, peer_node, lvs_jm_vuid, lvs_name=None):
    """Handle RPC failure to a peer during restart, per design decision tree.

    Called when RPCs to a previously-connected peer fail/timeout.

    Per design:
    Step 1: Check if hublvol to this node is still connected
      - If NOT connected → node is fabric-disconnected, skip it
      - If connected → only mgmt plane unreachable, go to step 2
    Step 2: Check if unreachable node is leader
      - If NOT leader → skip that node
      - If IS leader → send ``bdev_lvol_set_lvs_signal`` from snode through
        the fabric to the peer. This tells the peer's SPDK to drop
        leadership for the given LVS. Only relevant when the peer's data
        plane is healthy (hublvol connected). Wait 2 seconds for the
        signal to take effect, then continue.

    Returns:
        "skip" - node can be safely skipped
        "leader_dropped" - leadership was dropped via fabric, can continue
        "abort" - must abort restart (fabric connected but signal failed)
    """
    if not _check_hublvol_connected(snode, peer_node):
        logger.info("Peer %s hublvol disconnected after RPC failure, skipping", peer_node.get_id())
        return "skip"

    # Hublvol is connected — only mgmt plane is down, data plane healthy.
    # Send a fabric-level signal FROM snode TO the peer to drop leadership.
    if not lvs_name:
        logger.error("_handle_rpc_failure_on_peer: lvs_name required for fabric signal")
        return "abort"
    try:
        rpc_client = snode.rpc_client(timeout=5, retry=1)
        ret = rpc_client.bdev_lvol_set_lvs_signal(lvs_name)
        if ret:
            logger.info("Sent bdev_lvol_set_lvs_signal(%s) from %s to peer %s via fabric, waiting 2s",
                        lvs_name, snode.get_id(), peer_node.get_id())
            time.sleep(2)
            return "leader_dropped"
        else:
            logger.info("bdev_lvol_set_lvs_signal(%s) returned False — peer %s may not be leader, skipping",
                        lvs_name, peer_node.get_id())
            return "skip"
    except Exception as e:
        logger.error("Failed to send fabric signal to peer %s for LVS %s: %s — aborting restart",
                     peer_node.get_id(), lvs_name, e)
        return "abort"


def recreate_lvstore_on_non_leader(snode, leader_node, primary_node, activation_mode=False, force=False):
    """Per-LVS-locked wrapper: serialize recreate of ``primary_node.lvstore``
    only against a concurrent recreate of the SAME LVS. Activation-mode
    (globally blocked, serves no IO) bypasses the lock — see recreate_all_lvstores."""
    if activation_mode:
        return _recreate_lvstore_on_non_leader_impl(
            snode, leader_node, primary_node, activation_mode=True, force=force)
    with _recreate_lvstore_lock(primary_node.lvstore):
        return _recreate_lvstore_on_non_leader_impl(
            snode, leader_node, primary_node, activation_mode=False, force=force)


def _recreate_lvstore_on_non_leader_impl(snode: StorageNode, leader_node, primary_node, activation_mode=False, force=False):
    """Recreate a non-leader LVS on snode.

    Per design: runs for secondary when primary is online, or for tertiary always.
    While snode examines its raid, the current leader must be quiesced:
    block the leader's port only, demote its lvs leadership, drain inflight
    IO, then examine. Non-leader peers (siblings) are never port-blocked.

    During the port-blocked window, all RPCs to the leader use timeout=0.2s
    with no retries. Any RPC failure in this window triggers an abort: kill
    the restarting SPDK, set node offline, unblock the leader port, raise.

    Args:
        snode: the restarting node (RPCs are executed here)
        leader_node: whoever currently leads this LVS
        primary_node: the original primary (for lvol list, lvstore name, etc.)
        activation_mode: when True, skip all peer operations (port blocking,
            hublvol creation/connection, leader demotion).  Used during
            cluster_activate() where not all LVS are ready yet.
    """
    db_controller = DBController()
    snode_rpc_client = snode.rpc_client()

    # Soft prelude: reconnect any missing remote devices + remote JMs before
    # touching the LVS stack. Both helpers iterate existing bdevs internally
    # and no-op on controllers that are already attached, so this is safe to
    # run every time -- unconditionally, not just in activation_mode.
    # Previously gated behind activation_mode (only cluster_activate() paid
    # this cost); _relocate_one_replica's call into this function always
    # passes activation_mode=False, so a node freshly taking over a
    # relocated secondary/tertiary replica never got its connections to the
    # primary's redundancy-set peers established here, leaving it silently
    # missing whichever peer it had no other prior reason to already be
    # connected to (found live 2026-08-25: a replaced node's JM stayed
    # unreachable on the new host because of exactly this gap).
    try:
        fresh_remote_devs = _connect_to_remote_devs(snode, reattach=False)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        snode.remote_devices = fresh_remote_devs or snode.remote_devices
        snode.write_to_db()
    except Exception as e:
        logger.warning("Soft reconnect of remote devices failed on %s: %s",
                       snode.get_id(), e)
    try:
        fresh_remote_jms = _connect_to_remote_jm_devs(snode)
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        snode.remote_jm_devices = fresh_remote_jms or snode.remote_jm_devices
        snode.write_to_db()
    except Exception as e:
        logger.warning("Soft reconnect of remote JMs failed on %s: %s",
                       snode.get_id(), e)

    # Ensure snode has per-lvstore ports from primary
    lvstore_ports = {}
    if snode.lvstore:
        lvstore_ports[snode.lvstore] = {
            "lvol_subsys_port": snode.lvol_subsys_port,
            "hublvol_port": snode.hublvol.nvmf_port if snode.hublvol else 0,
        }
    if snode.lvstore_stack_secondary:
        nd = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
        lvstore_ports[nd.lvstore] = {
            "lvol_subsys_port": nd.lvol_subsys_port,
            "hublvol_port": nd.hublvol.nvmf_port if nd.hublvol else 0,
        }
    if snode.lvstore_stack_tertiary:
        nd = db_controller.get_storage_node_by_id(snode.lvstore_stack_tertiary)
        lvstore_ports[nd.lvstore] = {
            "lvol_subsys_port": nd.lvol_subsys_port,
            "hublvol_port": nd.hublvol.nvmf_port if nd.hublvol else 0,
        }
    snode.lvstore_ports = lvstore_ports
    snode.write_to_db()

    lvol_list = []
    for lv in db_controller.get_lvols_by_node_id(primary_node.get_id()):
        if lv.status not in [LVol.STATUS_IN_DELETION, LVol.STATUS_IN_CREATION]:
            lvol_list.append(lv)

    # Probe whether the raid already exists BEFORE step 1 (re)builds the stack.
    # On a real restart SPDK has just started and the raid (superblock=False)
    # cannot persist, so this is False and step 1 freshly builds it. It is only
    # True on an activation retry where a prior pass already created AND examined
    # the raid (SPDK then rejects re-examine). This distinguishes the normal
    # restart path (just examine) from the convergence trap (drop + re-create).
    raid_preexisted = _rpc_bdev_exists(snode_rpc_client, primary_node.raid)

    ### 1- create distribs and raid
    # Set restart phase: pre_block — sync deletes and registrations can still complete.
    # IMPORTANT: every exit path after this point MUST clear the phase (either by
    # reaching the normal clear at the end, or via the except/finally below).
    # A stale pre_block causes check_non_leader_for_operation to return "skip"
    # for this LVS indefinitely, silently blocking all new volume subsystem
    # creation on this node.
    _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_PRE_BLOCK, db_controller)

    ret, err = _create_bdev_stack(snode, primary_node.lvstore_stack, primary_node=primary_node)
    if err:
        logger.error(f"Failed to recreate non-leader lvstore on node {snode.get_id()}")
        logger.error(err)
        _set_restart_phase(snode, primary_node.lvstore, "", db_controller)
        # The replica stack (distribs + raid) did NOT come up — e.g. the raid
        # build returns -EIO because a peer node's devices are unreachable
        # (a concurrent/dual outage: the partner node is still offline). This
        # node therefore does NOT hold a usable replica of this LVS. Marking
        # it "ready" here is a phantom success: the node goes online without
        # the replica, every later leader restart fails to wire this peer's
        # hublvol (set_lvs_opts -> -19 "No such device") and the leader loops
        # restart->offline->in_restart forever (soak 2026-06-19, LVS_7613 on
        # tertiary after the partner dac5725c was force-shut-down). Mark it
        # "failed" and propagate so the restart is retried instead — once the
        # missing peer returns, the retry rebuilds the replica cleanly.
        _set_lvstore_status_atomic(primary_node.get_id(), "failed", db_controller)
        return False

    # Expansion/activate (activation_mode=True) skips the port-blocked
    # retry block below, so it establishes the hublvol here. A normal
    # restart (activation_mode=False) connects in that block instead —
    # connecting here too would double the hublvol attach.
    if activation_mode:
        try:
            # Role from topology, never a default: this call used to pass
            # no role and connect_to_hublvol defaulted to "secondary", so
            # an activation-mode TERTIARY recreate stamped role=secondary
            # onto its LVS — a duplicate secondary role next to the real
            # secondary (mass_create_delete_k8s 2026-07-14 12:02:37,
            # LVS_11 on worker-1) until a later topology-correct call
            # happened to repair it. Every LVS must hold a unique role
            # per node at all times.
            activation_role = ("tertiary"
                               if primary_node.tertiary_node_id == snode.get_id()
                               else "secondary")
            snode.connect_to_hublvol(primary_node, failover_node=None,
                                     role=activation_role)
        except Exception as e:
            logger.error("Error establishing hublvol: %s", e)
            # return False

    # Resume JC compression for this LVS group on the restarting node —
    # unless a release upgrade holds all resumes until `cluster
    # upgrade-complete` (release-upgrade guard, remove with the
    # jc_compression_upgrade plugin).
    if jc_compression_upgrade.resume_is_held(DBController().get_cluster_by_id(snode.cluster_id)):
        logger.info("JC compression resume held: cluster upgrade in progress")
    else:
        ret, err = snode.rpc_client().jc_suspend_compression(jm_vuid=primary_node.jm_vuid, suspend=False)
        if not ret:
            logger.info("Failed to resume JC compression adding task...")
            tasks_controller.add_jc_comp_resume_task(
                snode.cluster_id, snode.get_id(), jm_vuid=primary_node.jm_vuid)

    ### 2- create lvols nvmf subsystems (idempotent: skip existing)
    is_tertiary = (primary_node.tertiary_node_id == snode.get_id())
    min_cntlid = 2000 if is_tertiary else 1000
    for lvol in lvol_list:
        allow_any = not bool(lvol.allowed_hosts)
        if snode_rpc_client.subsystem_get(lvol.nqn):
            logger.info("subsystem %s already exists on %s, skipping create",
                        lvol.nqn, snode.get_id())
        else:
            logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
            snode_rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
                                              max_namespaces=lvol.max_namespace_per_subsys,
                                              allow_any_host=allow_any)
        if lvol.allowed_hosts:
            _reapply_allowed_hosts(lvol, snode, snode_rpc_client)

    leader_lvs_port = primary_node.get_lvol_subsys_port(primary_node.lvstore)

    logger.info(f"[RESTART] Non-leader for {primary_node.lvstore} on {snode.get_id()[:8]}, "
                f"leader={leader_node.get_id()[:8]}, is_tert={is_tertiary}")

    # Set restart phase: blocked — sync deletes and registrations must be delayed until post_unblock
    _port_block_t0 = time.monotonic()
    _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_BLOCKED, db_controller)

    # Resolve the secondary node for tertiary→secondary hublvol fallback
    secondary_node = None
    if primary_node.secondary_node_id and primary_node.secondary_node_id != snode.get_id():
        secondary_node = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)

    leader_port_blocked = False

    # Pre-acquire the hublvol advisory lock OUTSIDE the port-block window.
    # The acquire is an FDB transaction (avg 858ms/std 487 measured INSIDE
    # blocked windows, 2026-07-21 n=11) — paying it before the leader port
    # is blocked keeps the client-visible outage window short. Ownership:
    # released after the unblock/print below, in _abort_and_unblock on
    # aborts, and by the 60s lock TTL on any other escape. Failure to
    # pre-acquire is non-fatal: connect_to_hublvol then locks internally
    # (the pre-fix behavior).
    _hub_lock_holder = {"lock": None}
    if not activation_mode:
        try:
            from simplyblock_core.utils.hublvol_reconnect import (
                HublvolReconnectCoordinator,
            )
            _hub_lock_holder["lock"] = HublvolReconnectCoordinator(
                db_controller).acquire_lock(snode.get_id(), primary_node.lvstore)
        except Exception as _hl_e:
            logger.warning(
                "Pre-acquire of hublvol lock (%s, %s) failed — reconcile "
                "will lock in-window: %s",
                snode.get_id(), primary_node.lvstore, _hl_e)

        # #1 pre-stage: the secondary-hublvol NVMf subsystem + listeners have
        # no lvstore dependency (params come from the primary's persisted
        # hublvol metadata) — create them BEFORE the block window so the
        # in-window create_secondary_hublvol/expose reduces to probe+add_ns
        # (was ~4 RPCs x ~50ms inside every window, 2026-07-21). Idempotent;
        # failure is non-fatal (expose creates in-window as before).
        if not is_tertiary and primary_node.hublvol:
            try:
                _cluster_pre = db_controller.get_cluster_by_id(snode.cluster_id)
                snode.prestage_hublvol_subsystem(
                    nqn=StorageNode.hublvol_nqn_for_lvstore(
                        _cluster_pre.nqn, primary_node.lvstore),
                    model_number=primary_node.hublvol.model_number,
                    port=primary_node.hublvol.nvmf_port,
                    ana_state="non_optimized",
                    min_cntlid=1000,
                )
            except Exception as _ps_e:
                logger.warning(
                    "Hublvol subsystem pre-stage failed on %s for %s "
                    "(in-window expose will create it): %s",
                    snode.get_id(), primary_node.lvstore, _ps_e)

        # Pre-block controller ATTACH to the leader's live hublvol: the
        # attach is inert until bdev_lvol_connect_hublvol (issued in-window)
        # registers the redirect, so only set_lvs_opts + connect remain in
        # the blocked span. Leader's hublvol subsystem+namespace are fully
        # live (it is serving), so the n1 bdev appears immediately.
        # Non-fatal: any failure falls back to the in-window attach path.
        if primary_node.hublvol:
            try:
                snode.connect_to_hublvol(
                    leader_node, failover_node=None,
                    role=("tertiary" if is_tertiary else "secondary"),
                    rpc_timeout=2.0, lvs_node=primary_node,
                    coordinator_lock=_hub_lock_holder.get("lock"),
                    attach_only=True)
            except Exception as _pa_e:
                logger.warning(
                    "Pre-block hublvol attach on %s for %s failed "
                    "(in-window attach will retry): %s",
                    snode.get_id(), primary_node.lvstore, _pa_e)

    def _release_hub_lock():
        lk = _hub_lock_holder.pop("lock", None)
        if lk is not None:
            if getattr(lk, "pending_stamp", False):
                # Deferred success stamp (#2): paid here, post-unblock,
                # instead of inside the port-block window.
                try:
                    lk.stamp_attach()
                except Exception as _st_e:
                    logger.warning("Deferred hublvol stamp failed: %s", _st_e)
            lk.release()

    # Global port-block window gate (see _port_block_window_gate): at most
    # one LVS's client port is blocked at any moment across the runner.
    _gate_state = {"held": False}

    # True client-outage accounting for the leader port (may block/unblock
    # several times across the attach-retry loop). The phase print at the
    # end brackets the whole phase and overstates the real outage ~2x
    # (2026-07-21); the 6s nvmf ack-timeout reject applies to each interval
    # measured here.
    _leader_block = {"t0": None, "max": 0.0}

    # #3: port deny/allow event emission (FDB+graylog write, ~190ms measured
    # in-window) is deferred to after the unblock; the block-span logs carry
    # the true timing, the events remain complete for audit.
    _deferred_port_events: list = []

    def _flush_port_events():
        for _kind, _n, _p in _deferred_port_events:
            try:
                if _kind == "deny":
                    tcp_ports_events.port_deny(_n, _p)
                else:
                    tcp_ports_events.port_allowed(_n, _p)
            except Exception as _ev_e:
                logger.warning("Deferred port event emit failed: %s", _ev_e)
        del _deferred_port_events[:]

    def _mark_leader_blocked():
        _leader_block["t0"] = time.monotonic()

    def _mark_leader_unblocked():
        if _leader_block["t0"] is None:
            return
        _d = time.monotonic() - _leader_block["t0"]
        _leader_block["t0"] = None
        _leader_block["max"] = max(float(_leader_block["max"] or 0.0), _d)
        logger.info(
            "[RESTART] Leader client port %s (%s) was blocked %.3fs "
            "(reject threshold 6s)",
            leader_lvs_port, primary_node.lvstore, _d)

    def _acquire_block_gate():
        _waited = _open_port_block_window(primary_node.lvstore)
        _gate_state["held"] = True
        if _waited > 0.5:
            logger.info("[RESTART] Waited %.3fs for port-block window "
                        "(gate + fan-out drain) (%s)",
                        _waited, primary_node.lvstore)

    def _release_block_gate():
        if _gate_state["held"]:
            _gate_state["held"] = False
            _close_port_block_window()

    def _abort_and_unblock(reason):
        """Abort restart: kill SPDK on snode, set offline, unblock leader port, raise."""
        logger.error("Aborting non-leader restart on %s for %s: %s",
                     snode.get_id(), primary_node.lvstore, reason)
        _release_hub_lock()
        try:
            storage_events.snode_restart_failed(snode)
            snode_api = snode.client(timeout=5, retry=5)
            snode_api.spdk_process_kill(snode.rpc_port, snode.cluster_id)
        except Exception as ke:
            logger.error("Failed to kill SPDK during abort: %s", ke)
        set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE,
                        caused_by="restart_cleanup")
        if leader_port_blocked:
            try:
                port_block.set_port(leader_node, leader_lvs_port, block=False, timeout=0.5, retry=2)
                _deferred_port_events.append(("allow", leader_node, leader_lvs_port))
                _mark_leader_unblocked()
            except Exception as ue:
                logger.error("Failed to unblock leader port during abort: %s", ue)
        _release_block_gate()
        _flush_port_events()
        _set_restart_phase(snode, primary_node.lvstore, "", db_controller)
        raise Exception(f"Abort non-leader restart: {reason}")

    # Quorum check on the current leader ONLY. Use a peer list that excludes the
    # restarting node (snode) — snode's JM is expected to be disconnected on peers
    # during restart, so including it would cause false negatives.
    lvs_peer_ids_excl_snode = [sid for sid in [primary_node.secondary_node_id, primary_node.tertiary_node_id]
                               if sid and sid != snode.get_id()]
    leader_has_quorum = not _check_peer_disconnected(leader_node, lvs_peer_ids=lvs_peer_ids_excl_snode)

    # #4: compute the examine-idempotency probes BEFORE the block window —
    # they only read snode's own fresh SPDK (raid built at ###1 pre-block;
    # nothing else mutates it until the examine below), so probing here
    # gives the identical answer without paying two in-window round-trips.
    raid_already = _rpc_bdev_exists(snode_rpc_client, primary_node.raid)
    lvstore_already = _rpc_lvstore_exists(snode_rpc_client, primary_node.lvstore)

    try:
        if not activation_mode:
            # Serialize the client-port outage span across all concurrent
            # recreates (covers the initial block, the attach-retry re-blocks,
            # and the final unblock below).
            _acquire_block_gate()

        if not activation_mode and leader_has_quorum:
            ### 3- block leader port ONLY (no siblings)
            # Blocking the leader's LVS port is what quiesces its IO so this
            # restarting node can safely examine its raid0 without a write
            # racing into a half-reconstructed lvstore. Silently skipping the
            # block (as we used to do on ConnectionRefused) lets the leader
            # keep serving reads/writes while we examine — which has produced
            # CRC mismatches and lvol drops on the restarting peer. So retry,
            # and if it still can't land, abort the restart unless force=True.
            #
            # Budget: 3 attempts × rpc_client(timeout=3, retry=1) × 1s sleep
            # between attempts → worst-case ~15s abort. Previously 5× ×
            # (timeout=5, retry=5) × 2s = ~140s, which made every iteration
            # against a dead-mgmt leader stall the restart task for minutes.
            # The FDB-status short-circuit in _check_peer_disconnected should
            # already route such peers to the takeover path before we reach
            # here; keeping a short local budget protects against stragglers.
            last_err = None
            attempts = 3
            for attempt in range(1, attempts + 1):
                try:
                    port_block.set_port(leader_node, leader_lvs_port, block=True, timeout=3, retry=1)
                    _deferred_port_events.append(("deny", leader_node, leader_lvs_port))
                    leader_port_blocked = True
                    _mark_leader_blocked()
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    logger.warning(
                        "Port-block attempt %d/%d failed for leader %s on %s: %s",
                        attempt, attempts, leader_node.get_id(), primary_node.lvstore, e)
                    if attempt < attempts:
                        time.sleep(1)
            if not leader_port_blocked:
                msg = (f"Failed to block leader {leader_node.get_id()} port "
                       f"{leader_lvs_port} after {attempts} attempts for "
                       f"{primary_node.lvstore}: {last_err}")
                if force:
                    logger.warning(
                        "%s — force=True: proceeding without leader port block; "
                        "this allows leader-vs-restarter writes to race during "
                        "examine and can corrupt the rebuilt lvstore", msg)
                else:
                    _abort_and_unblock(msg)

        if not activation_mode and leader_port_blocked:
            # Fixed quiesce window instead of draining distrib-inflight — see
            # constants.NON_LEADER_BLOCK_QUIESCE_SEC for the full rationale
            # (the inflight counter is polluted by migration mover IO on this
            # node class, so a drain loop never settles; client IO admitted
            # before the block settles in ms). Migration IO does not touch
            # lvstore metadata, so a brief fixed wait is sufficient for the
            # secondary's examine to see a consistent superblock.
            time.sleep(constants.NON_LEADER_BLOCK_QUIESCE_SEC)

        elif not activation_mode and not leader_has_quorum:
            logger.info("Leader %s has no quorum for %s, skipping port block",
                        leader_node.get_id(), primary_node.lvstore)

        ### 4- examine (idempotent: skip only when raid AND lvstore already surfaced)
        # #4: probes were computed pre-block (see above the block section) —
        # nothing touches snode's fresh SPDK between there and here, and each
        # in-window RPC costs a full CP round-trip.
        if raid_already and lvstore_already:
            logger.info(
                "Raid %s and lvstore %s already present on %s; skipping examine",
                primary_node.raid, primary_node.lvstore, snode.get_id())
        else:
            if raid_already and not lvstore_already and raid_preexisted:
                # Convergence trap (activation retry only): the raid was created
                # AND examined on a prior pass and the lvstore module did not
                # surface it. SPDK rejects re-examine of an already-examined
                # bdev with "Duplicate bdev name for manual examine", so a
                # plain bdev_examine here is a silent no-op that loops the
                # activation retry forever. Drop the raid and re-create via
                # _create_bdev_stack (idempotent) so the next examine is
                # against a freshly-registered raid.
                logger.info(
                    "Raid %s present but lvstore %s did not surface on %s; "
                    "dropping raid for clean re-examine",
                    primary_node.raid, primary_node.lvstore, snode.get_id())
                try:
                    snode_rpc_client.bdev_raid_delete(primary_node.raid)
                except Exception as e:
                    logger.warning(
                        "bdev_raid_delete(%s) raised: %s — proceeding to "
                        "_create_bdev_stack which is idempotent",
                        primary_node.raid, e)
                ret, err = _create_bdev_stack(snode, primary_node.lvstore_stack,
                                              primary_node=primary_node)
                if not ret:
                    logger.error(
                        "Failed to rebuild bdev stack on %s after raid drop: %s",
                        snode.get_id(), err)
            elif raid_already and not lvstore_already:
                # Normal restart: the raid was freshly built this pass in step 1
                # and has never been examined, so the first-time bdev_examine below
                # surfaces the lvstore. Dropping+recreating it here would be pure
                # churn inside the (minimized) port-block window — the duplicate
                # bdev_raid_create observed 2026-06-12 (LVS_5199).
                logger.info(
                    "Raid %s freshly built this pass on %s; examining without drop",
                    primary_node.raid, snode.get_id())

            # Examine is required whenever the lvstore isn't surfaced — whether
            # the raid was freshly created by _create_bdev_stack (normal restart
            # path) or pre-existing with stale state (activation retry).
            snode_rpc_client.bdev_examine(primary_node.raid)

            ### 5- wait for examine
            ret = snode_rpc_client.bdev_wait_for_examine()
            if not ret:
                logger.warning("Failed to examine bdevs on non-leader node")

            # After examine, the lvstore MUST be present. If it isn't, SPDK
            # failed to rediscover the lvstore from its persisted metadata
            # (e.g. partial stack components left over, corrupt on-disk state).
            # During activation we can't safely recover — signal the caller
            # to reject the activation and ask for a restart of this node.
            if activation_mode and not _rpc_lvstore_exists(snode_rpc_client, primary_node.lvstore):
                raise LVSRestartRequiredError(
                    snode.get_id(), primary_node.lvstore,
                    detail=f"raid={primary_node.raid} present but lvstore did not recover"
                    if raid_already else "examine did not produce lvstore")

        # Verify that examine actually rediscovered the lvstore and every lvol
        # the FDB expects to be present on this node. Mirrors the check in
        # recreate_lvstore() for the primary path. If an lvol blob did not
        # become durable on this peer's shard of raid0 before it was torn down
        # (e.g. the blob was committed on the primary/tertiary quorum but this
        # node missed the write window due to a simultaneous force-shutdown),
        # the examine won't surface it. Continuing would leave the lvol
        # subsystem bound without a namespace on this node — present on
        # primary/tertiary, missing here — and the divergence would never be
        # reconciled because there is no FDB↔SPDK lvol-set reconcile loop.
        if not activation_mode:
            if not snode_rpc_client.bdev_lvol_get_lvstores(primary_node.lvstore):
                logger.error(
                    "Failed to recover lvstore %s on %s after examine",
                    primary_node.lvstore, snode.get_id())
                if not force:
                    _abort_and_unblock(
                        f"lvstore {primary_node.lvstore} did not recover after examine "
                        f"on non-leader {snode.get_id()}")

        # Per-lvol recovery verification — DEFERRED to after the port unblock
        # (2026-07-22, user decision): the probes cost 60-230ms of blocked-window
        # time and feed only the abort decision; a post-unblock abort kills SPDK
        # (crash-equivalent, handled by failover) instead of a clean in-window
        # abort. Runs before ### 9 so no subsystem binds a missing blob.
        # Per-lvol name-filtered probes, NOT one unfiltered dump (the dump costs
        # seconds of SPDK app-thread time on large clusters).
        def _deferred_lvol_verify():
            if activation_mode:
                return

            def _lvol_bdev_registered(lv):
                for candidate in (lv.lvol_uuid, f"{lv.lvs_name}/{lv.lvol_bdev}"):
                    try:
                        if snode_rpc_client.get_bdevs(candidate):
                            return True
                    except Exception:
                        pass
                return False

            missing_lvols = []
            for lv in lvol_list:
                if _lvol_bdev_registered(lv):
                    continue
                missing_lvols.append(lv)

            if missing_lvols:
                missing_repr = ", ".join(
                    f"{lv.lvs_name}/{lv.lvol_bdev}(uuid={lv.lvol_uuid[:8]})"
                    for lv in missing_lvols)
                logger.error(
                    "Expected lvol bdevs missing on %s for %s after examine: %s",
                    snode.get_id(), primary_node.lvstore, missing_repr)
                if not force:
                    _abort_and_unblock(
                        f"Expected lvols not registered on {snode.get_id()} after "
                        f"examine of {primary_node.raid}: {missing_repr}. "
                        f"Re-run restart with force=True to proceed anyway "
                        f"(this peer will not serve these lvols).")
                else:
                    logger.warning(
                        "force=True: proceeding with %d missing lvol(s) on %s for %s; "
                        "these lvols will not be served by this peer",
                        len(missing_lvols), snode.get_id(), primary_node.lvstore)

        # bdev_examine brings the LVS back with its metadata-persisted role
        # (primary). Leaving it as primary makes SPDK reject a later
        # bdev_lvol_connect_hublvol with "-22 nonsecondary node".
        sec_role = "tertiary" if is_tertiary else "secondary"
        if not snode_rpc_client.bdev_lvol_set_lvs_opts(
                primary_node.lvstore,
                groupid=primary_node.jm_vuid,
                subsystem_port=primary_node.get_lvol_subsys_port(primary_node.lvstore),
                hublvol_port=primary_node.get_hublvol_port(primary_node.lvstore),
                role=sec_role,
        ):
            logger.error("bdev_lvol_set_lvs_opts(%s) failed for %s on %s",
                         sec_role, primary_node.lvstore, snode.get_id())

        # Track the deferred failover-path attach so we can run it AFTER the
        # leader port is unblocked. The in-freeze attach below uses a single
        # path only; the second path (if any) is reconciled out-of-band so the
        # 3 s INTER_ATTACH_SLEEP_SEC inside the coordinator never sits inside
        # the IO-impact window.
        deferred_failover_target = None
        deferred_failover_via = None

        if not activation_mode:
            ### 6- create hublvol on secondary (non-leader) for multipath failover
            # Secondary creates its own hublvol so the tertiary can use it as a failover path.
            if not is_tertiary:
                try:
                    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                    snode.create_secondary_hublvol(leader_node, cluster.nqn)
                    logger.info("Created secondary hublvol on restarting node %s for %s",
                                snode.get_id(), primary_node.lvstore)
                except Exception as e:
                    logger.error("Error creating secondary hublvol on restarting node: %s", e)

            ### 7- single-path hublvol attach inside the freeze
            # Pre-flight reachability up front: the second path must NEVER be
            # attempted inside the leader-port-block window, and the single
            # attached path must target a known-alive peer. If the original
            # leader is offline (no quorum) we attach directly to the secondary
            # — the dead leader's IP is not even tried.
            attach_target = None
            try:
                if is_tertiary:
                    secondary_alive = (secondary_node and not _check_peer_disconnected(
                        secondary_node, lvs_peer_ids=lvs_peer_ids_excl_snode))
                    # leader_has_quorum was computed earlier (line ~4722). When
                    # the leader has lost quorum, attaching to it would burn up
                    # to fast_io_fail_timeout_sec (5s) inside the freeze.
                    if leader_has_quorum:
                        sync_target = leader_node
                        if (secondary_alive and secondary_node is not None
                                and secondary_node.get_id() != leader_node.get_id()):
                            deferred_failover_target = secondary_node
                            deferred_failover_via = leader_node
                    elif secondary_alive and secondary_node is not None and secondary_node.hublvol:
                        logger.info("Leader %s offline (no quorum); tertiary %s "
                                    "connecting directly to secondary %s hublvol for %s",
                                    leader_node.get_id(), snode.get_id(),
                                    secondary_node.get_id(), primary_node.lvstore)
                        sync_target = secondary_node
                        # No deferred path: there is no live alternative peer
                        # to add as a failover. Once the original leader comes
                        # back online, its periodic hublvol reconciliation will
                        # add it as a path.
                    else:
                        sync_target = None
                        logger.error(
                            "Tertiary %s rejoin %s: no reachable hublvol target "
                            "(leader=%s alive=%s, secondary alive=%s)",
                            snode.get_id(), primary_node.lvstore,
                            leader_node.get_id(), leader_has_quorum, secondary_alive)
                    attach_target = sync_target  # may be None
                else:
                    # Secondary: connect to leader (primary) hublvol — single path,
                    # no deferred failover (secondaries don't carry one).
                    attach_target = leader_node
            except Exception as e:
                logger.error("Error determining hublvol attach target: %s", e)
                attach_target = None

            # Attach with retries. Each call is bounded by ``rpc_timeout=1.0``
            # so the port-block window cannot be held open indefinitely. If
            # the proxy is still busy when the RPC times out the controller
            # may be partially attached in SPDK — but the CP has no proof,
            # and unblocking the leader on that ambiguous state has been
            # observed to produce writer-conflicts seconds later (incident
            # 2026-05-21 18:04:01: tertiary e16a rejoining LVS_9651 with
            # leader=00e7, 200 ms RPC timeout exhausted while SPDK was past
            # ``set_num_queues_done`` but pre-namespace; no path attached;
            # restart unblocked the leader anyway → writer_conflict on
            # ``jm_vuid=9651`` at 18:04:03.520, 00e7 marked ``down``).
            # Between attempts the leader port is unblocked for 5 s so the
            # client side has time to reconnect its NVMe controller (which
            # may have been disconnected during the prior block) and push
            # IO again, then we re-block for the next attempt. The gap is
            # not held under port-block. On exhaustion, ``_abort_and_unblock``
            # kills snode's SPDK, marks it offline, and restores the leader
            # port — the task runner will retry.
            # ``lvs_node=primary_node`` is preserved so LVS metadata
            # (lvstore name, jm_vuid, port, hublvol NQN/bdev) comes from
            # the configured primary of the LVS being recreated, not from
            # ``attach_target``; when the configured primary is offline
            # and ``attach_target`` is a peer that took over leadership,
            # that peer's own hublvol points at its OWN primary-LVS, which
            # is the wrong LVS for our connection (incident 2026-05-02
            # 15:53:42: tertiary worker1 via acting-leader worker5 for
            # LVS_6207 set groupid=4729 — worker5's own primary).
            if attach_target is not None:
                ATTACH_MAX_ATTEMPTS = 3
                ATTACH_RETRY_GAP_SEC = 5
                ok = False
                last_err = None
                for attempt in range(1, ATTACH_MAX_ATTEMPTS + 1):
                    try:
                        ok = snode.connect_to_hublvol(
                            attach_target, failover_node=None,
                            role=sec_role, rpc_timeout=1.0,
                            lvs_node=primary_node,
                            coordinator_lock=_hub_lock_holder.get("lock"))
                        last_err = None
                    except Exception as e:
                        ok = False
                        last_err = e
                        logger.error(
                            "connect_to_hublvol attempt %d/%d on %s for %s raised: %s",
                            attempt, ATTACH_MAX_ATTEMPTS,
                            snode.get_id(), primary_node.lvstore, e)
                    if ok or attempt >= ATTACH_MAX_ATTEMPTS:
                        break
                    logger.warning(
                        "connect_to_hublvol attempt %d/%d on %s for %s failed; "
                        "unblock leader, wait %ds, re-block and retry",
                        attempt, ATTACH_MAX_ATTEMPTS, snode.get_id(),
                        primary_node.lvstore, ATTACH_RETRY_GAP_SEC)
                    if leader_port_blocked:
                        try:
                            port_block.set_port(leader_node, leader_lvs_port, block=False, timeout=3, retry=1)
                            _deferred_port_events.append(("allow", leader_node, leader_lvs_port))
                            leader_port_blocked = False
                            _mark_leader_unblocked()
                        except Exception as ue:
                            logger.warning(
                                "Unblock leader %s during attach-retry gap "
                                "failed: %s", leader_node.get_id(), ue)
                    time.sleep(ATTACH_RETRY_GAP_SEC)
                    try:
                        port_block.set_port(leader_node, leader_lvs_port, block=True, timeout=3, retry=1)
                        _deferred_port_events.append(("deny", leader_node, leader_lvs_port))
                        leader_port_blocked = True
                        _mark_leader_blocked()
                    except Exception as be:
                        _abort_and_unblock(
                            f"Re-block leader {leader_node.get_id()} port "
                            f"{leader_lvs_port} after retry gap failed for "
                            f"{primary_node.lvstore}: {be}")
                if not ok:
                    _abort_and_unblock(
                        f"connect_to_hublvol failed for {primary_node.lvstore} "
                        f"after {ATTACH_MAX_ATTEMPTS} attempts"
                        + (f": {last_err}" if last_err else ""))

            ### 8- unblock leader port
            # If we blocked it, we MUST unblock — a stuck-blocked leader can't
            # serve client IO on that LVS. Retry until it lands; schedule a
            # port_allow task as a fallback if we still can't reach the leader
            # after our attempts so another retry loop keeps trying.
            if leader_port_blocked:
                unblocked = False
                attempts = 3
                for attempt in range(1, attempts + 1):
                    try:
                        port_block.set_port(leader_node, leader_lvs_port, block=False, timeout=3, retry=1)
                        _deferred_port_events.append(("allow", leader_node, leader_lvs_port))
                        unblocked = True
                        _mark_leader_unblocked()
                        break
                    except Exception as e:
                        logger.warning(
                            "Port-unblock attempt %d/%d failed for leader %s on %s: %s",
                            attempt, attempts, leader_node.get_id(), primary_node.lvstore, e)
                        if attempt < attempts:
                            time.sleep(1)
                if not unblocked:
                    logger.error(
                        "Failed to unblock leader %s port %s for %s after %d attempts; "
                        "scheduling port_allow task",
                        leader_node.get_id(), leader_lvs_port, primary_node.lvstore, attempts)
                    try:
                        tasks_controller.add_port_allow_task(
                            leader_node.cluster_id, leader_node.get_id(), leader_lvs_port)
                    except Exception as sched_exc:
                        logger.error("Failed to schedule port_allow fallback: %s", sched_exc)
                leader_port_blocked = False

        # Set restart phase: post_unblock — delayed sync deletes and registrations can now proceed
        _release_block_gate()
        _flush_port_events()
        _release_hub_lock()
        _deferred_lvol_verify()
        _set_restart_phase(snode, primary_node.lvstore, StorageNode.RESTART_PHASE_POST_UNBLOCK, db_controller)
        if _leader_block["max"]:
            logger.info("[RESTART] Longest client-port block for %s: %.3fs "
                        "(reject threshold 6s)",
                        primary_node.lvstore, _leader_block["max"])
        logger.info("[RESTART] Port-block phase for %s on %s took %.3fs "
                    "(phase span incl. post-unblock work; see per-port lines "
                    "for true outage)",
                    primary_node.lvstore, snode.get_id()[:8], time.monotonic() - _port_block_t0)

        ### 8b- deferred failover-path attach (tertiary only, leader was alive)
        # The in-freeze attach above used a single path. Now that the leader
        # port is unblocked and IO is flowing again, top up the second path on
        # the multipath hublvol controller so a future primary loss has an
        # immediate failover. The coordinator's INTER_ATTACH_SLEEP_SEC (3 s)
        # cost lives here, OUTSIDE the IO-impact window — it doesn't sit inside
        # the leader-port-block freeze any more, so client IO is unaffected.
        if deferred_failover_target is not None and deferred_failover_via is not None:
            try:
                if snode.add_hublvol_failover_path(deferred_failover_via, deferred_failover_target):
                    logger.info("Added deferred hublvol failover path to %s (via %s) on %s for %s",
                                deferred_failover_target.get_id(),
                                deferred_failover_via.get_id(),
                                snode.get_id(), primary_node.lvstore)
                else:
                    logger.warning("Failed to add deferred hublvol failover path to %s on %s for %s",
                                   deferred_failover_target.get_id(),
                                   snode.get_id(), primary_node.lvstore)
            except Exception as e:
                logger.error("Error adding deferred hublvol failover path on %s: %s",
                             snode.get_id(), e)

        ### 9- add lvols to subsystems (non_optimized for non-leader; INACCESSIBLE
        # during (re)activation so no client IO flows before hublvol redirects are
        # connected and leadership settles — cluster_activate sets the correct ANA
        # in a dedicated pass before flipping the cluster to ACTIVE).
        non_leader_ana_state = "inaccessible" if activation_mode else "non_optimized"
        executor = ThreadPoolExecutor(max_workers=50)
        for lvol in lvol_list:
            executor.submit(add_lvol_thread, lvol, snode, lvol_ana_state=non_leader_ana_state)
        executor.shutdown(wait=True)

        if not activation_mode:
            ### 10- add non-optimized path on tertiary to newly-restarted secondary's hublvol
            if not is_tertiary and primary_node.tertiary_node_id and leader_node.hublvol:
                tert_id = primary_node.tertiary_node_id
                if tert_id != snode.get_id() and tert_id != leader_node.get_id():
                    tert_node = db_controller.get_storage_node_by_id(tert_id)
                    if tert_node and not _check_peer_disconnected(tert_node, lvs_peer_ids=lvs_peer_ids_excl_snode):
                        try:
                            if tert_node.add_hublvol_failover_path(leader_node, snode):
                                logger.info("Added secondary %s hublvol path on tertiary %s for %s",
                                            snode.get_id(), tert_node.get_id(), primary_node.lvstore)
                            else:
                                logger.warning(
                                    "Failed to add secondary %s hublvol path on tertiary %s for %s",
                                    snode.get_id(), tert_node.get_id(), primary_node.lvstore)
                        except Exception as e:
                            logger.error("Error adding secondary hublvol path on tertiary: %s", e)

        # Clear restart phase for this LVS
        _set_restart_phase(snode, primary_node.lvstore, "", db_controller)

        _set_lvstore_status_atomic(primary_node.get_id(), "ready", db_controller)

        return True
    finally:
        # Idempotent; the in-flow release above is the normal path.
        _release_block_gate()
        # An unbounded fence is bad; a fence budget leaking onto this thread's
        # later work would be worse. Every exit path clears it.
        rpc_budget.clear_budget()
        try:
            # this function never arms the gate; just release it
            hublvol_reconnect.clear_defer_gate()
        except Exception:
            pass


def _release_lvs_subsys_port_on_peers(lvs_node, exclude_node_id, db_controller):
    """Best-effort release of an LVS subsystem port on every replica peer.

    recreate_lvstore / recreate_lvstore_on_non_leader block the LVS port on
    the surviving leader (and other peers) while a restarting node rebuilds
    its lvstore, and release it only via their internal abort/success paths.
    A RAW RPC exception mid-rebuild (e.g. the restarting node's SPDK going
    unreachable) unwinds PAST those release points, leaving a peer's client
    IO blocked for the entire failed restart and its retries — the
    2026-06-03 LVS_8720 incident, where vm203 (the sole surviving leader)
    stayed port-blocked for 10m12s. Calling this on any recreate failure
    guarantees the port is reopened. Idempotent: 'allow' is a no-op when the
    port is not blocked.
    """
    try:
        port = lvs_node.get_lvol_subsys_port(lvs_node.lvstore)
    except Exception as e:
        logger.error("Defensive unblock: could not resolve LVS port for %s: %s",
                     lvs_node.get_id(), e)
        return
    peer_ids = {pid for pid in (lvs_node.get_id(),
                                lvs_node.secondary_node_id,
                                lvs_node.tertiary_node_id)
                if pid and pid != exclude_node_id}
    for pid in peer_ids:
        try:
            peer = db_controller.get_storage_node_by_id(pid)
            if not peer or peer.status != StorageNode.STATUS_ONLINE:
                continue
            port_block.set_port(peer, port, block=False, timeout=0.5, retry=2)
            tcp_ports_events.port_allowed(peer, port)
            logger.info("Defensive unblock: allowed LVS port %s on peer %s after "
                        "failed recreate of %s", port, pid, lvs_node.lvstore)
        except Exception as e:
            logger.error("Defensive unblock of LVS port %s on %s failed: %s",
                         port, pid, e)


def _restore_peer_lvstore_status_ready(node_id, db_controller):
    """Clear an ``in_creation`` lvstore_status leaked onto a peer primary by a
    FAILED replica-rebuild phase (restart Step 2/3 sets it up front; only the
    success path restores it). The marker is a window flag, not state — while
    it lingers, the storage-node monitor skips ALL checks of that peer
    (check_node's in_creation skip), so a peer whose SPDK dies inside the
    window stays 'online/health True' forever and every dependent recovery
    keeps routing to a dead node (incident 2026-07-07 13:52: d277d436 SPDK
    segfault mid-window -> zombie-online for 1.5h+, 7 failed restarts of the
    node that set the marker)."""
    try:
        fresh = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        return
    if fresh.lvstore_status == "in_creation":
        logger.warning(
            "Restoring lvstore_status of peer %s to 'ready' after failed "
            "replica-rebuild phase (was left 'in_creation')", node_id)
        db_controller.atomic_update(
            fresh, lambda n: setattr(n, "lvstore_status", "ready"))


def recreate_all_lvstores(snode: StorageNode, force=False):
    """Recreate all LVS stacks on a restarting node: primary, secondary, tertiary.

    This is the dispatch logic extracted from restart_storage_node() so it can
    be called independently (e.g. from tests) without the SPDK init preamble.

    Serialization is PER-LVS, not node-wide: each per-role recreate
    (primary/secondary/tertiary) takes ``_recreate_lvstore_lock(lvs_name)``
    inside its wrapper, so two members of the SAME LVS group cannot race the
    port-block / hublvol rewrite / peer lvstore_status writes, while DIFFERENT
    LVSes recreate concurrently. (Previously a single global gate serialized
    ALL recreates of ALL LVSes across ALL parallel-restarting nodes — ~60-98s
    per node, the dominant cost of a whole-FD reboot recovery.) Everything
    before recreate (SPDK bring-up, device/JM connects) already runs parallel.
    Activation-mode recreates keep their own orchestration (suspended cluster,
    ports globally blocked) and bypass the per-LVS lock.
    """
    return _recreate_all_lvstores_serial(snode, force=force)


def _recreate_all_lvstores_serial(snode: StorageNode, force=False):
    db_controller = DBController()

    # --- Step 1: Primary LVS ---
    logger.info("=== Phase: Primary LVS recreation ===")
    try:
        ret = recreate_lvstore(snode, force=force)
    except Exception:
        # A raw RPC exception (e.g. the restarting node's SPDK going
        # unreachable mid-rebuild) unwinds past recreate_lvstore's internal
        # abort/unblock, leaving the surviving leader's LVS port blocked for
        # the whole failed restart (incident 2026-06-03 LVS_8720). Release it.
        _release_lvs_subsys_port_on_peers(snode, snode.get_id(), db_controller)
        raise
    snode = db_controller.get_storage_node_by_id(snode.get_id())
    if not ret:
        logger.error("Failed to recreate primary lvstore")
        _release_lvs_subsys_port_on_peers(snode, snode.get_id(), db_controller)
        return False

    # Track non-leader (secondary/tertiary) recreate outcomes. A failed
    # replica rebuild must fail the whole node restart so the restart task
    # runner retries it — NOT be swallowed, which would bring the node online
    # holding a phantom (absent) replica and wedge the LVS leader in a
    # restart loop. See recreate_lvstore_on_non_leader's "failed" path.
    non_leader_ok = True

    # --- Step 2: Secondary LVS ---
    if snode.lvstore_stack_secondary:
        logger.info("=== Phase: Secondary LVS recreation ===")
        secondary_primary_node = None
        try:
            secondary_primary_node = db_controller.get_storage_node_by_id(snode.lvstore_stack_secondary)
            secondary_primary_node.lvstore_status = "in_creation"
            secondary_primary_node.write_to_db()

            sec_lvs_peer_ids = [sid for sid in [secondary_primary_node.secondary_node_id,
                                                 secondary_primary_node.tertiary_node_id] if sid]
            primary_disconnected = _check_peer_disconnected(secondary_primary_node, lvs_peer_ids=sec_lvs_peer_ids)

            if primary_disconnected:
                logger.info("Primary %s disconnected — %s taking leadership for %s",
                            secondary_primary_node.get_id(), snode.get_id(), secondary_primary_node.lvstore)
                ret = recreate_lvstore(snode, force=force, lvs_primary=secondary_primary_node)
            else:
                leader_node = secondary_primary_node
                logger.info("Non-leader for %s on %s (leader=%s)",
                            secondary_primary_node.lvstore, snode.get_id(), leader_node.get_id())
                ret = recreate_lvstore_on_non_leader(snode, leader_node, secondary_primary_node, force=force)
            if not ret:
                non_leader_ok = False
                logger.error(f"Failed to recreate secondary LVS {secondary_primary_node.lvstore}")
                _restore_peer_lvstore_status_ready(
                    secondary_primary_node.get_id(), db_controller)
        except Exception as e:
            non_leader_ok = False
            logger.error("Secondary LVS recreation failed: %s", e)
            if secondary_primary_node is not None:
                _release_lvs_subsys_port_on_peers(
                    secondary_primary_node, snode.get_id(), db_controller)
                _restore_peer_lvstore_status_ready(
                    secondary_primary_node.get_id(), db_controller)

    # --- Step 3: Tertiary LVS ---
    if snode.lvstore_stack_tertiary:
        logger.info("=== Phase: Tertiary LVS recreation ===")
        tertiary_primary_node = None
        try:
            tertiary_primary_node = db_controller.get_storage_node_by_id(snode.lvstore_stack_tertiary)
            tertiary_primary_node.lvstore_status = "in_creation"
            tertiary_primary_node.write_to_db()

            tert_lvs_peer_ids = [sid for sid in [tertiary_primary_node.secondary_node_id,
                                                  tertiary_primary_node.tertiary_node_id] if sid]
            primary_disconnected = _check_peer_disconnected(tertiary_primary_node, lvs_peer_ids=tert_lvs_peer_ids)

            if primary_disconnected:
                sec_id = tertiary_primary_node.secondary_node_id
                sec_disconnected = True
                if sec_id and sec_id != snode.get_id():
                    sec_node_check = db_controller.get_storage_node_by_id(sec_id)
                    sec_disconnected = _check_peer_disconnected(sec_node_check, lvs_peer_ids=tert_lvs_peer_ids)

                if not sec_disconnected and sec_id:
                    leader_node = db_controller.get_storage_node_by_id(sec_id)
                    logger.info("Primary disconnected, secondary %s is leader for %s, "
                                "tertiary %s connects as non-leader",
                                leader_node.get_id(), tertiary_primary_node.lvstore, snode.get_id())
                    ret = recreate_lvstore_on_non_leader(snode, leader_node, tertiary_primary_node, force=force)
                else:
                    logger.warning("Both primary and secondary disconnected for tertiary LVS %s, skipping",
                                   tertiary_primary_node.lvstore)
                    ret = True
            else:
                leader_node = tertiary_primary_node
                logger.info("Non-leader (tertiary) for %s on %s (leader=%s)",
                            tertiary_primary_node.lvstore, snode.get_id(), leader_node.get_id())
                ret = recreate_lvstore_on_non_leader(snode, leader_node, tertiary_primary_node, force=force)
            if not ret:
                non_leader_ok = False
                logger.error(f"Failed to recreate tertiary LVS {tertiary_primary_node.lvstore}")
                _restore_peer_lvstore_status_ready(
                    tertiary_primary_node.get_id(), db_controller)
        except Exception as e:
            non_leader_ok = False
            logger.error("Tertiary LVS recreation failed: %s", e)
            if tertiary_primary_node is not None:
                _release_lvs_subsys_port_on_peers(
                    tertiary_primary_node, snode.get_id(), db_controller)
                _restore_peer_lvstore_status_ready(
                    tertiary_primary_node.get_id(), db_controller)

    # Fail the restart if any non-leader replica did not come up, so the
    # restart task runner retries (the node must not go online advertising a
    # replica it does not actually hold).
    return non_leader_ok


def recreate_lvstore(snode: StorageNode, force=False, lvs_primary=None, activation_mode=False):
    """Per-LVS-locked wrapper: serialize recreate of this LVS only against a
    concurrent recreate of the SAME LVS. The LVS is ``lvs_primary.lvstore``
    (secondary taking leadership) or ``snode.lvstore`` (own primary).
    Activation-mode (globally blocked, serves no IO) bypasses the lock."""
    lvs_name = lvs_primary.lvstore if lvs_primary is not None else snode.lvstore
    if activation_mode:
        return _recreate_lvstore_impl(
            snode, force=force, lvs_primary=lvs_primary, activation_mode=True)
    with _recreate_lvstore_lock(lvs_name):
        return _recreate_lvstore_impl(
            snode, force=force, lvs_primary=lvs_primary, activation_mode=False)


def _recreate_lvstore_impl(snode: StorageNode, force=False, lvs_primary=None, activation_mode=False):
    """Recreate LVStore as leader.

    Per design: runs for snode's own primary LVS, and also when snode
    takes over leadership from an offline primary (lvs_primary is set).

    Args:
        snode: the restarting node (RPCs are executed here)
        force: force recreation even on validation failure
        lvs_primary: when set, the original primary node (now offline)
            whose LVS this node is taking over.  When None, snode is the
            primary for its own LVS.
        activation_mode: when True, skip all peer operations (port blocking,
            hublvol creation/connection, leader demotion).  Used during
            cluster_activate() where peer LVS may not exist yet.  Hublvol
            setup is done in a separate pass after all lvstores are up.
    """
    db_controller = DBController()

    # --- LVS context: who owns the metadata for this lvstore? ---
    is_takeover = lvs_primary is not None
    lvs_node = lvs_primary if is_takeover else snode
    lvs_name = lvs_node.lvstore
    lvs_jm_vuid = lvs_node.jm_vuid
    lvs_raid = lvs_node.raid

    lvs_node.lvstore_status = "in_creation"
    lvs_node.write_to_db()

    if activation_mode:
        # Soft prelude: reconnect any missing remote devices + remote JMs
        # so the recreate path doesn't stumble on stale/absent controllers.
        # Both helpers iterate existing bdevs internally and no-op on
        # controllers that are already attached, so this is safe to call
        # every activation pass.
        try:
            fresh_remote_devs = _connect_to_remote_devs(snode, reattach=False)
            snode = db_controller.get_storage_node_by_id(snode.get_id())
            snode.remote_devices = fresh_remote_devs or snode.remote_devices
            snode.write_to_db()
        except Exception as e:
            logger.warning("Soft reconnect of remote devices failed on %s: %s",
                           snode.get_id(), e)

    if not is_takeover:
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode)
        snode.write_to_db()

    # Gather peer nodes for this LVS, EXCLUDING snode itself
    sec_nodes = []
    lvs_all_peer_ids = [sid for sid in [lvs_node.secondary_node_id, lvs_node.tertiary_node_id] if sid]
    # Peer list for quorum checks: exclude snode (restarting node) since its JM
    # is expected to be disconnected on peers during restart.
    lvs_peer_ids = [sid for sid in lvs_all_peer_ids if sid != snode.get_id()]
    for sec_id in lvs_all_peer_ids:
        if sec_id != snode.get_id():
            sec = db_controller.get_storage_node_by_id(sec_id)
            if sec:
                sec_nodes.append(sec)

    # Per design: determine peer connectivity via disconnect state, NOT node status.
    # Method 1: JM quorum check for each peer.
    disconnected_peers = set()
    if activation_mode:
        # During activation peer LVS may not exist yet; skip all peer checks.
        current_leader = None
    else:
        for sec_node in sec_nodes:
            if _check_peer_disconnected(sec_node, lvs_peer_ids=lvs_peer_ids):
                disconnected_peers.add(sec_node.get_id())

        # Identify the current leader among connected peers.
        # Uses bdev_lvol_get_lvstores which returns "lvs leadership" field.
        # Compression and replication checks run only against the current leader.
        current_leader = None
        for sec_node in sec_nodes:
            if sec_node.get_id() in disconnected_peers:
                continue
            try:
                sec_rpc = sec_node.rpc_client(timeout=5, retry=2)
                ret = sec_rpc.bdev_lvol_get_lvstores(lvs_name)
                if ret and len(ret) > 0 and ret[0].get("lvs leadership"):
                    current_leader = sec_node
                    logger.info("Current leader for %s is %s", lvs_name, sec_node.get_id())
                    break
            except Exception as e:
                # Cannot tell "peer down" from "peer mgmt slow" at this stage:
                # snode has no peer-hublvol controller bdevs yet, so any
                # hublvol-presence check from snode would always say
                # "disconnected" and silently drop the leader. Abort and let
                # the next restart attempt re-evaluate peer state via the
                # data-plane check earlier in this function.
                raise Exception(
                    f"Abort restart: leader detection RPC to peer {sec_node.get_id()} failed: {e}")

        # Check compression and replication only on the current leader
        if current_leader:
            try:
                jc_compression_is_active = current_leader.rpc_client().jc_compression_get_status(lvs_jm_vuid)
                retries = 10
                while jc_compression_is_active:
                    if retries <= 0:
                        logger.warning("Timeout waiting for JC compression task to finish on leader %s",
                                       current_leader.get_id())
                        break
                    retries -= 1
                    logger.info(f"JC compression active on leader {current_leader.get_id()}, retrying in 60 seconds")
                    time.sleep(60)
                    # Poll the SAME jm_vuid as the first read above — the LVS
                    # being recovered (lvs_jm_vuid). Was previously
                    # current_leader.jm_vuid, which is the leader-node's own
                    # configured-primary LVS jm_vuid (a different LVS), so the
                    # poll watched the wrong subsystem and could either exit
                    # too early (false clear) or block here for the full
                    # 10×60 s when the leader's own primary LVS happened to
                    # be compressing. Incident 2026-05-06: 70850783 was
                    # acting leader of LVS_4450 *and* configured primary of
                    # LVS_5676; jm_vuid=5676 stayed active → 5 min hang.
                    jc_compression_is_active = current_leader.rpc_client().jc_compression_get_status(
                        lvs_jm_vuid)
            except Exception as e:
                raise Exception(
                    f"Abort restart: jc_compression check on leader {current_leader.get_id()} failed: {e}")

    # Probe whether the raid already exists BEFORE step 1 (re)builds the stack.
    # On a real restart SPDK has just started and the raid (superblock=False)
    # cannot persist, so this is False and step 1 freshly builds it. It is only
    # True on an activation retry where a prior pass already created AND examined
    # the raid (SPDK then rejects re-examine). This distinguishes the normal
    # restart path (just examine) from the convergence trap (drop + re-create).
    raid_preexisted = _rpc_bdev_exists(snode.rpc_client(), lvs_raid)

    ### 1- create distribs and raid
    _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_PRE_BLOCK, db_controller)

    if is_takeover:
        ret, err = _create_bdev_stack(snode, lvs_node.lvstore_stack, primary_node=lvs_node)
    else:
        ret, err = _create_bdev_stack(snode, [])

    if err:
        logger.error(f"Failed to recreate lvstore on node {snode.get_id()}")
        logger.error(err)
        _set_restart_phase(snode, lvs_name, "", db_controller)
        return False

    rpc_client = snode.rpc_client()

    lvol_list = []
    for lv in db_controller.get_lvols_by_node_id(lvs_node.get_id()):
        if lv.status == LVol.STATUS_IN_DELETION:
            if not is_takeover:
                lv.deletion_status = ''
                lv.write_to_db()
        elif lv.status in [LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE]:
            if lv.deletion_status == '':
                lvol_list.append(lv)

    # During (re)activation, bring client-facing listeners up INACCESSIBLE so no
    # client IO can flow before the hublvol redirects exist and leadership is
    # settled (Pass 3 of cluster_activate). Surfacing them optimized here opens a
    # window where this node serves writes with no redirect to its peers -> a
    # dual-write / writer-conflict against a peer that is also mid-activation.
    # cluster_activate sets the correct ANA state (optimized for primary,
    # non_optimized for secondary/tertiary) in a dedicated pass before it flips
    # the cluster to ACTIVE.
    lvol_ana_state = "inaccessible" if activation_mode else "optimized"

    ### 2- create lvols nvmf subsystems (idempotent: probe SPDK first; mirrors
    ### the pattern in recreate_lvstore_on_non_leader so a re-activation that
    ### finds the subsystem already present from a prior partial pass does not
    ### emit "Subsystem NQN ... already exists" / "Unable to create subsystem".
    created_subsystems = []
    for lvol in lvol_list:
        if lvol.nqn in created_subsystems:
            continue
        allow_any = not bool(lvol.allowed_hosts)
        if rpc_client.subsystem_get(lvol.nqn) is not None:
            logger.info("subsystem %s already exists on %s, skipping create",
                        lvol.nqn, snode.get_id())
            created_subsystems.append(lvol.nqn)
        else:
            logger.info("creating subsystem %s (allow_any_host=%s)", lvol.nqn, allow_any)
            ret = rpc_client.subsystem_create(lvol.nqn, lvol.ha_type, lvol.uuid, 1,
                                              max_namespaces=lvol.max_namespace_per_subsys,
                                              allow_any_host=allow_any)
            if ret:
                created_subsystems.append(lvol.nqn)
        if lvol.allowed_hosts:
            _reapply_allowed_hosts(lvol, snode, rpc_client)

    # ANA failback only when the original primary is coming back (not takeover)
    if not is_takeover and lvs_node.secondary_node_id and lvol_list:
        _failback_primary_ana(snode)

    snode_lvs_port = lvs_node.get_lvol_subsys_port(lvs_name)

    # Phase transition: blocked — sync deletes and registrations must be delayed
    _port_block_t0 = time.monotonic()
    _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_BLOCKED, db_controller)

    # Peers whose LVS port is currently blocked. Client IO to any peer on
    # snode_lvs_port is rejected until that peer is removed from the list.
    # Every blocked peer MUST be unblocked — either per-peer after its
    # connect_to_hublvol succeeds, or en bloc on abort.
    blocked_peers: list = []
    # True client-outage accounting: per-peer monotonic stamp at successful
    # block, duration logged at unblock. The phase print below brackets the
    # whole BLOCKED->POST_UNBLOCK phase (incl. work after the last unblock)
    # and overstates the real outage ~2x (2026-07-21: printed 11-21s vs
    # 6.2-8.9s true block->unblock from spdk logs). The 6s nvmf ack-timeout
    # reject applies to THIS number, per port.
    _block_started: dict = {}
    #: Fired once every fenced port is released, so deferred redundant-path
    #: hublvol attaches run outside the client-visible window.
    _defer_gate_event = threading.Event()
    _block_longest = {"sec": 0.0}

    # #3: port deny/allow event emission (FDB+graylog write, ~190ms measured
    # in-window) is deferred to after the unblock; the per-port block-span
    # logs carry the true timing, the events remain complete for audit.
    _deferred_port_events: list = []

    def _flush_port_events():
        for _kind, _n, _p in _deferred_port_events:
            try:
                if _kind == "deny":
                    tcp_ports_events.port_deny(_n, _p)
                else:
                    tcp_ports_events.port_allowed(_n, _p)
            except Exception as _ev_e:
                logger.warning("Deferred port event emit failed: %s", _ev_e)
        del _deferred_port_events[:]

    def _warn_if_unblocking_a_non_leader(peer):
        """Read-only: warn when a peer is being returned to service demoted.

        Leadership can move WHILE the fence is held. On 2026-09-01 it did: the
        fence went on LVS_10's primary at 16:28:25, the secondary took
        leadership at 16:28:31, and the control plane unblocked the old primary
        at 16:28:37 regardless. The client reconnected within milliseconds to a
        node that was no longer leader, that node could not redirect, and the
        IO came back as a generic INTERNAL DEVICE ERROR -- which
        nvme-multipath does not retry on another path. Client EIO, fio rc=4.

        Deliberately does NOT repair here. Wiring a hublvol inside the fence is
        exactly the kind of extra in-window work that made the fence 12.2s in
        the first place, and on the abort path it would delay the release we
        want immediate. One bounded probe (0.5s via the ambient fence budget)
        buys the diagnosis; the fix belongs to the post-unblock repair, and to
        keeping the fence short enough that leadership does not move inside it.
        """
        try:
            ret = peer.rpc_client().bdev_lvol_get_lvstores(lvs_name)
        except Exception:
            return                      # never let a probe delay the release
        if not ret or ret[0].get("lvs leadership"):
            return
        logger.error(
            "[RESTART] Unblocking %s for %s while it is NO LONGER leader -- "
            "the client will reconnect to a demoted node; if its hublvol "
            "redirect path is missing, its next IO is failed outright. Fence "
            "held %.3fs.",
            peer.get_id()[:8], lvs_name, _fence_elapsed())

    def _unblock_peer_port(peer):
        """Remove the port block for snode_lvs_port on peer and drop
        the peer from blocked_peers. Safe to call if peer is not currently
        blocked (no-op). Tolerates RPC failure — logs and continues so
        other peers can still be unblocked."""
        if peer in blocked_peers:
            _warn_if_unblocking_a_non_leader(peer)
        try:
            port_block.set_port(peer, snode_lvs_port, block=False, timeout=0.5, retry=2)
            _deferred_port_events.append(("allow", peer, snode_lvs_port))
            _t0 = _block_started.pop(peer.get_id(), None)
            if _t0 is not None:
                _d = time.monotonic() - _t0
                _block_longest["sec"] = max(_block_longest["sec"], _d)
                logger.info(
                    "[RESTART] Client port %s on %s was blocked %.3fs "
                    "(reject threshold 6s)",
                    snode_lvs_port, peer.get_id()[:8], _d)
        except Exception as ue:
            logger.error("Failed to unblock port %s on %s: %s",
                         snode_lvs_port, peer.get_id(), ue)
        finally:
            try:
                blocked_peers.remove(peer)
            except ValueError:
                pass
            if not blocked_peers:
                # Fence over: normal work must not inherit the 0.5s budget,
                # and the deferred hublvol attaches may now run.
                rpc_budget.clear_budget()
                _defer_gate_event.set()

    def _kill_app():
        """Kill SPDK on snode and mark OFFLINE before peer ports unblock.

        Holding the peer port blocks during this wait is intentional:
        unblocking before SPDK is confirmed dead lets a residual primary
        on snode race the acting-leader and produce a writer conflict.

        Implemented via the module-level :func:`_kill_spdk_until_dead`
        helper so the same hardened kill logic is used by every abort
        path (recreate_lvstore aborts here; restart_storage_node aborts
        in `_abort_restart`). On total kill failure we still mark the
        node OFFLINE so it stops being treated as in_restart by the
        cluster, and so peer ports get released by the caller.
        """
        storage_events.snode_restart_failed(snode)
        _kill_spdk_until_dead(snode)
        set_node_status(snode.get_id(), StorageNode.STATUS_OFFLINE,
                        caused_by="restart_cleanup")

    # Pre-acquired hublvol advisory locks, one per peer that will
    # connect_to_hublvol inside the blocked window (key: peer id). The
    # acquire is an FDB transaction (avg 858ms/std 487 measured INSIDE
    # blocked windows, 2026-07-21 n=11) — paying it before the peer ports
    # are blocked keeps the client-visible outage short. Released after the
    # unblock/print below, on aborts, and by the 60s lock TTL on any other
    # escape. Pre-acquire failure is non-fatal: connect_to_hublvol then
    # locks internally (the pre-fix behavior).
    _hub_locks: dict = {}

    # Deferred persistence of hublvol/transfer_hublvol metadata mutated
    # in-window with defer_db_write=True. Persisted ATOMICALLY (field-only
    # update) post-unblock — a full-object write here is both an in-window
    # FDB round-trip and a stale-write hazard (2026-07-21 resurrection).
    _deferred_node_persist = {"needed": False}

    def _persist_deferred_node_fields():
        if not _deferred_node_persist.pop("needed", False):
            return
        try:
            def _apply_hub_fields(n, h=snode.hublvol,
                                  t=snode.transfer_hublvol):
                n.hublvol = h
                n.transfer_hublvol = t
            db_controller.atomic_update(snode, _apply_hub_fields)
        except Exception as _pe:
            logger.error("Deferred hublvol persist failed for %s: %s",
                         snode.get_id(), _pe)

    def _release_hub_locks():
        for _pid in list(_hub_locks):
            lk = _hub_locks.pop(_pid, None)
            if lk is not None:
                if getattr(lk, "pending_stamp", False):
                    # Deferred success stamp (#2): paid here, post-unblock,
                    # instead of inside the port-block window.
                    try:
                        lk.stamp_attach()
                    except Exception as _st_e:
                        logger.warning("Deferred hublvol stamp failed: %s", _st_e)
                lk.release()

    # Global port-block window gate (see _port_block_window_gate): at most
    # one LVS's client port is blocked at any moment across the runner.
    _gate_state = {"held": False}

    def _acquire_block_gate():
        _waited = _open_port_block_window(lvs_name)
        _gate_state["held"] = True
        if _waited > 0.5:
            logger.info("[RESTART] Waited %.3fs for port-block window "
                        "(gate + fan-out drain) (%s)",
                        _waited, lvs_name)

    def _release_block_gate():
        if _gate_state["held"]:
            _gate_state["held"] = False
            _close_port_block_window()

    def _abort_restart_and_unblock(reason):
        """Abort: kill SPDK, set offline, unblock every blocked peer, raise."""
        logger.error("Aborting recreate_lvstore on %s for %s: %s",
                     snode.get_id(), lvs_name, reason)
        # Release the fences FIRST. Every fenced peer has its client
        # listener blocked and cannot answer keep-alives, so each extra
        # second risks clients (KATO 4s) dropping that path. _kill_app()
        # used to run first and cost ~1.2s of spdk_process_kill +
        # spdk_process_is_up polling before any peer was released
        # (2026-08-31: unblock landed at 13:33:29.13, one second after the
        # client had already failed IO at 13:33:28). Killing our own SPDK
        # can wait; a fenced healthy peer cannot.
        for peer in list(blocked_peers):
            _unblock_peer_port(peer)
        _persist_deferred_node_fields()
        _release_hub_locks()
        _kill_app()
        _release_block_gate()
        _flush_port_events()
        raise Exception(f"Abort restart: {reason}")

    def _fence_elapsed():
        """Seconds since the first peer port was fenced (0.0 if none is)."""
        if not _block_started:
            return 0.0
        return time.monotonic() - min(_block_started.values())

    def _check_fence_deadline(where):
        """Release and abort if the fence has run to FENCE_DEADLINE_SEC.

        Called between steps and inside the in-window wait loops. The fence
        must be lifted by us before SPDK converts the block to reject at
        ack_timeout * 4 (8s): the conversion quiesces every qpair on the port,
        so the client loses the path rather than merely waiting for it.
        """
        if not _block_started:
            return
        elapsed = _fence_elapsed()
        if elapsed >= constants.FENCE_DEADLINE_SEC:
            _abort_restart_and_unblock(
                f"port fence held {elapsed:.3f}s at {where}, over the "
                f"{constants.FENCE_DEADLINE_SEC}s deadline (reject threshold 8s)")

    def _fenced(method, *args, budget=None, **kwargs):
        """Run one RPC while a peer's client port is fenced.

        ``method`` is the RPCClient method name. The client is built per call
        so its timeout can be clamped to the time left on the fence -- a fixed
        per-call timeout is not enough on its own, since a 6s call started late
        in the window would still overrun the deadline.

        Any failure, a timeout above all, releases the fence and aborts the
        restart. The task runner re-queues an aborted restart; a quiesced
        client path is not recoverable.
        """
        budget = constants.FENCE_RPC_TIMEOUT_SEC if budget is None else budget
        timeout = budget
        if _block_started:
            remaining = constants.FENCE_DEADLINE_SEC - _fence_elapsed()
            if remaining <= 0:
                _check_fence_deadline(method)
            timeout = min(budget, remaining)
        try:
            client = snode.rpc_client(timeout=timeout,
                                      retry=constants.FENCE_RPC_RETRY)
            return getattr(client, method)(*args, **kwargs)
        except Exception as e:
            _abort_restart_and_unblock(
                f"{method} failed inside the port-fence window "
                f"(budget {timeout:.2f}s, fence {_fence_elapsed():.3f}s): {e}")

    # #4: compute the examine-idempotency probes BEFORE the block window —
    # they only read snode's own fresh SPDK (raid built at ###1 pre-block),
    # so probing here gives the identical answer without paying two
    # in-window round-trips.
    raid_already = _rpc_bdev_exists(rpc_client, lvs_raid)
    lvstore_already = _rpc_lvstore_exists(rpc_client, lvs_name)

    try:
        if not activation_mode:
            try:
                from simplyblock_core.utils.hublvol_reconnect import (
                    HublvolReconnectCoordinator,
                )
                _hub_coord = HublvolReconnectCoordinator(db_controller)
                for _peer in sec_nodes:
                    if _peer.get_id() in disconnected_peers:
                        continue
                    _hub_locks[_peer.get_id()] = _hub_coord.acquire_lock(
                        _peer.get_id(), lvs_node.lvstore)
            except Exception as _hl_e:
                logger.warning(
                    "Pre-acquire of hublvol locks for %s failed — reconcile "
                    "will lock in-window: %s", lvs_name, _hl_e)

            # #1 pre-stage: NVMf subsystems + listeners for the hublvols wired
            # inside the window have no lvstore/bdev dependency — create them
            # here so the in-window expose calls reduce to probe+add_ns. All
            # params come from persisted hublvol metadata; every call is
            # idempotent and failure is non-fatal (in-window expose creates as
            # before).
            try:
                if lvs_node.hublvol:
                    _cluster_pre = db_controller.get_cluster_by_id(snode.cluster_id)
                    # snode's own (leader) hublvol subsystem
                    snode.prestage_hublvol_subsystem(
                        nqn=lvs_node.hublvol.nqn,
                        model_number=lvs_node.hublvol.model_number,
                        port=lvs_node.hublvol.nvmf_port,
                        ana_state="optimized",
                    )
                    # transferhub subsystem (only when its metadata is persisted;
                    # otherwise create_transfer_hublvol mints a fresh model
                    # number in-window and must own the create)
                    if snode.transfer_hublvol and snode.transfer_hublvol.nqn:
                        snode.prestage_hublvol_subsystem(
                            nqn=snode.transfer_hublvol.nqn,
                            model_number=snode.transfer_hublvol.model_number,
                            port=snode.transfer_hublvol.nvmf_port,
                            ana_state="optimized",
                        )
                    # sec_1's shared-NQN secondary hublvol subsystem
                    _sec1_pre = next(
                        (p for p in sec_nodes
                         if p.get_id() == lvs_node.secondary_node_id
                         and p.get_id() not in disconnected_peers), None)
                    if _sec1_pre is not None:
                        _sec1_pre.prestage_hublvol_subsystem(
                            nqn=StorageNode.hublvol_nqn_for_lvstore(
                                _cluster_pre.nqn, lvs_node.lvstore),
                            model_number=lvs_node.hublvol.model_number,
                            port=lvs_node.hublvol.nvmf_port,
                            ana_state="non_optimized",
                            min_cntlid=1000,
                        )
            except Exception as _ps_e:
                logger.warning(
                    "Hublvol subsystem pre-stage failed for %s "
                    "(in-window expose will create it): %s", lvs_name, _ps_e)

            # Pre-block controller ATTACH from every connected peer to snode's
            # pre-staged hublvol subsystem. The subsystem is still NAMESPACE-LESS
            # (the hublvol bdev exists only after the in-window examine): the
            # controller attaches empty and the peer's n1 bdev surfaces via AER
            # once the in-window add_ns runs — connect_to_hublvol's n1-wait
            # covers that. The attach is inert until the in-window
            # bdev_lvol_connect_hublvol registers the redirect. Non-fatal per
            # peer: failure falls back to the in-window attach.
            if lvs_node.hublvol:
                for _peer in sec_nodes:
                    if _peer.get_id() in disconnected_peers:
                        continue
                    if _peer.get_id() == lvs_node.secondary_node_id:
                        _pre_role = "secondary"
                    elif _peer.get_id() == lvs_node.tertiary_node_id:
                        _pre_role = "tertiary"
                    else:
                        continue
                    try:
                        _peer.connect_to_hublvol(
                            snode, failover_node=None, role=_pre_role,
                            rpc_timeout=1.0, lvs_node=lvs_node,
                            coordinator_lock=_hub_locks.get(_peer.get_id()),
                            attach_only=True)
                    except Exception as _pa_e:
                        logger.warning(
                            "Pre-block hublvol attach on %s for %s failed "
                            "(in-window attach will retry): %s",
                            _peer.get_id(), lvs_name, _pa_e)

            # Serialize the client-port outage span across all concurrent
            # recreates. Acquired AFTER the hublvol advisory locks (fixed lock
            # order: per-LVS recreate lock -> hublvol locks -> window gate).
            _acquire_block_gate()
            ### 3- block LVS port on every connected peer (leader + non-leaders),
            # then suspend the leader's journal replication before the flap.
            #
            # Per attempt against the current leader:
            #   a. wait for any in-flight JM replication task to finish (loops
            #      internally) so we don't block mid-replication;
            #   b. mark the leader in_creation and block its LVS port;
            #   c. jc_disable_replication(jm_vuid):
            #        True  -> no active replication; it is now suspended (~12s) ->
            #                 proceed with the drain + leadership drop below.
            #        False -> active replication present -> unblock the leader port
            #                 and retry the whole sequence (re-wait, re-block,
            #                 re-disable).
            #
            # Without blocking the tertiary too, client IO can leak to it during the
            # leader flap: tertiary's LVOL listener stays open and serves writes
            # whose hublvol redirect target is mid-transition, producing
            # writer_conflict events on the journal. Non-leader peers are blocked
            # once, after the leader's replication is confirmed suspended. Each peer
            # stays blocked until its connect_to_hublvol succeeds in ### 8b.
            if current_leader and current_leader.get_id() not in disconnected_peers:
                _REPL_SUSPEND_MAX_ATTEMPTS = 10
                replication_suspended = False
                for _attempt in range(_REPL_SUSPEND_MAX_ATTEMPTS):
                    # a. ensure no active replication on the leader (loops internally)
                    try:
                        ret = current_leader.wait_for_jm_rep_tasks_to_finish(lvs_jm_vuid)
                        if not ret:
                            msg = f"JM replication task found on leader {current_leader.get_id()} for jm {lvs_jm_vuid}"
                            logger.error(msg)
                            storage_events.jm_repl_tasks_found(current_leader, lvs_jm_vuid)
                    except Exception as e:
                        raise Exception(
                            f"Abort restart: replication-wait on leader {current_leader.get_id()} failed: {e}")

                    # b. block the leader's LVS port
                    try:
                        # Field-scoped and transactional. A plain
                        # `leader.lvstore_status = X; leader.write_to_db()`
                        # serialises the WHOLE record from a copy read at
                        # the top of this function -- before the peer went
                        # down -- so it silently restores status=online over
                        # the monitor's offline write, and emits no
                        # STATUS_CHANGE event because it never goes through
                        # the status path.
                        #
                        # 2026-08-31: that resurrected "online" for a node
                        # whose SPDK was dead (it started at 12:02:31, the
                        # record read online from 11:52 to 12:02). So
                        # _check_peer_disconnected could never observe
                        # offline, the port-block below was issued to a dead
                        # node, and the restart aborted -- and every retry
                        # re-resurrected it: 5 aborts over 15 minutes, with
                        # the peer reported online in sn list throughout.
                        # Deliberately does NOT rebind current_leader: the
                        # rest of this flow keeps the object it was called
                        # with, so behaviour is unchanged. Not clobbering the
                        # record is enough to break the retry loop --
                        # _check_peer_disconnected re-reads from FDB on the
                        # next attempt, so it now sees offline and skips the
                        # port-block instead of failing on it forever.
                        db_controller.atomic_update(
                            current_leader,
                            lambda x: setattr(x, "lvstore_status", "in_creation"))
                        port_block.set_port(current_leader, snode_lvs_port, block=True, timeout=0.5, retry=1)
                        _deferred_port_events.append(("deny", current_leader, snode_lvs_port))
                        blocked_peers.append(current_leader)
                        _block_started[current_leader.get_id()] = time.monotonic()
                        # From here until the last port is released, every
                        # rpc_client() built on this thread -- including the
                        # ones inside hublvol/bdev-stack helpers we do not own
                        # -- is bounded. Cleared in _unblock_peer_port once
                        # blocked_peers empties, and in the outer finally.
                        rpc_budget.set_budget(constants.FENCE_RPC_TIMEOUT_SEC,
                                              constants.FENCE_RPC_RETRY)
                        # Redundant hublvol paths must land AFTER the fence,
                        # not 3s after the foreground attach (which is still
                        # inside it -- 2026-09-01 16:28:27.739, mid-block).
                        hublvol_reconnect.set_defer_gate(_defer_gate_event)
                    except Exception as e:
                        # Failing to port-block the current leader means we cannot
                        # safely promote snode: the old leader may still be serving
                        # IO, and a parallel leader on snode would produce a writer
                        # conflict (observed 2026-04-25, LVS_6609 incident).
                        # _check_hublvol_connected from snode is meaningless here —
                        # snode hasn't reconnected to peer hublvols yet — so we
                        # cannot use it to discriminate "peer gone" from "peer slow".
                        # Abort the attempt; the task runner will retry.
                        _abort_restart_and_unblock(
                            f"Failed to port-block leader {current_leader.get_id()}: {e}")

                    repl_disabled = False
                    # c. suspend journal replication while the port is blocked
                    try:
                        # Bounded: this runs with the leader's port already
                        # fenced, and a bare rpc_client() inherits
                        # RPCClient(timeout=180, retry=3) -> ~726s worst case
                        # with backoff. The leader answers this in ~11ms; if
                        # it has just died, the fence must not be held for
                        # minutes (client KATO is 4s). Failure here is
                        # already handled: repl_disabled stays False, the
                        # port is unblocked and the suspend loop retries.
                        repl_disabled = current_leader.rpc_client(
                            timeout=0.5, retry=1).jc_disable_replication(lvs_jm_vuid)
                    except RPCRemoteError as e:
                        if e.code == RPCErrorCode.method_not_found:
                            try:
                                logger.warning("Failed to disable replication on leader, trying other method")
                                ret = current_leader.rpc_client(
                                    timeout=0.5, retry=1).jc_get_jm_status(lvs_jm_vuid)
                                repl_disabled = True
                                for jm in ret:
                                    if ret[jm] is False:  # jm is not ready (has active replication task)
                                        repl_disabled = False
                                        break
                            except Exception as ex:
                                _abort_restart_and_unblock(
                                    f"jc_get_jm_status on leader {current_leader.get_id()} failed: {ex}")
                        else:
                            _abort_restart_and_unblock(
                                f"jc_disable_replication on leader {current_leader.get_id()} failed: {e}")
                    except RPCException as e:
                        _abort_restart_and_unblock(
                            f"jc_disable_replication on leader {current_leader.get_id()} failed: {e}")

                    if repl_disabled:
                        replication_suspended = True
                        break

                    # Active replication still present: unblock the leader port and
                    # retry the full sequence from the replication wait.
                    logger.warning(
                        "jc_disable_replication reports active replication on leader %s "
                        "(attempt %d/%d); unblocking and retrying",
                        current_leader.get_id(), _attempt + 1, _REPL_SUSPEND_MAX_ATTEMPTS)
                    _unblock_peer_port(current_leader)

                if not replication_suspended:
                    _abort_restart_and_unblock(
                        f"Could not suspend journal replication on leader "
                        f"{current_leader.get_id()} after {_REPL_SUSPEND_MAX_ATTEMPTS} attempts")

            # Also block non-leader peers (tertiary). The leader's demote+drain
            # below is leader-specific; non-leaders just need the port shut so
            # IO can't leak to them during the flap.
            for sec_node in sec_nodes:
                if sec_node is current_leader:
                    continue
                if sec_node.get_id() in disconnected_peers:
                    continue
                if sec_node in blocked_peers:
                    continue
                try:
                    port_block.set_port(sec_node, snode_lvs_port, block=True, timeout=0.5, retry=1)
                    _deferred_port_events.append(("deny", sec_node, snode_lvs_port))
                    blocked_peers.append(sec_node)
                    _block_started[sec_node.get_id()] = time.monotonic()
                    rpc_budget.set_budget(constants.FENCE_RPC_TIMEOUT_SEC,
                                          constants.FENCE_RPC_RETRY)
                except Exception as e:
                    # Same rationale as the leader port-block: cannot safely
                    # decide "peer gone" vs "peer slow" before snode has
                    # reconnected to peer hublvols. A non-leader peer left
                    # serving on snode_lvs_port during the leader flap can
                    # accept client IO whose hublvol redirect is mid-transition,
                    # producing a writer conflict.
                    _abort_restart_and_unblock(
                        f"Failed to port-block non-leader peer {sec_node.get_id()}: {e}")

            if current_leader and current_leader in blocked_peers:
                # --- Inside port-blocked window: timeout=0.2s, retry=0, abort on failure ---
                leader_rpc = current_leader.rpc_client(timeout=0.2, retry=0)

                ### 4- drain in-flight IO BEFORE dropping leadership
                #
                # If we drop leadership while IO is still in distrib, those
                # in-flight IOs land on a non-leader lvstore and either get
                # redirected via the hub bdev (which may not be open yet on
                # the new follower) or aborted — both produce client-visible
                # IO errors and qpair tear-downs.  Concrete example: incident
                # 2026-05-02 (k8s_native_failover_ha-20260502-101452), worker1.
                # 123 state-9 IOs were in flight on its distribs at the moment
                # set_leader=False fired; the open of LVS_4729/hublvoln1
                # returned ENODEV; nvmf_tcp_qpair_set_recv_state floods and
                # disconnects followed ~1.6 s later.
                #
                # The drain runs while the leader's lvol port is iptables-
                # blocked, so we must not hold this open indefinitely.  The
                # earlier fixed 0.5 s sleep was a workaround put in place
                # after the original 10 s drain regression — but that
                # regression was on the recreate_lvstore_on_non_leader path,
                # where the blocked node is the configured primary and runs
                # data migration (which never pauses on port block, hence the
                # poll never settled).  *This* path blocks `current_leader`,
                # which is a secondary or tertiary that became acting leader
                # while the configured primary was out — and migration never
                # runs on a secondary/tertiary, so the inflight counter
                # genuinely drains.
                #
                # Bound at _DRAIN_BOUND_SEC anyway: a slow JM/distrib
                # completion shouldn't be allowed to hold the leader's port
                # blocked beyond client max_latency.  On timeout we proceed
                # with the drop and accept the same residual class of error
                # this is trying to prevent — but bounded.
                _DRAIN_BOUND_SEC = 2.0
                _DRAIN_POLL_SEC = 0.05
                deadline = time.time() + _DRAIN_BOUND_SEC
                drained = False
                while time.time() < deadline:
                    _check_fence_deadline("inflight drain")
                    try:
                        still_inflight = leader_rpc.bdev_distrib_check_inflight_io(lvs_jm_vuid)
                    except Exception as e:
                        logger.warning(
                            "bdev_distrib_check_inflight_io poll failed for %s on %s: %s",
                            lvs_name, current_leader.get_id(), e)
                        break
                    if not still_inflight:
                        drained = True
                        break
                    time.sleep(_DRAIN_POLL_SEC)
                if not drained:
                    # Continuing with the leadership drop while IO is still in
                    # the distrib pipeline produces exactly the failure this
                    # drain is meant to prevent (in-flight IO hitting a
                    # non-leader lvstore at the moment of transition: hub-bdev
                    # redirect failures, qpair tear-downs, client IO errors).
                    # Abort cleanly: _abort_restart_and_unblock kills the
                    # recovering node's SPDK, sets it OFFLINE, and unblocks
                    # every peer port we just blocked above. The restart task
                    # runner re-queues from there; on the next attempt the
                    # cluster may have settled enough for drain to complete
                    # within the bound.
                    _abort_restart_and_unblock(
                        f"Inflight IO did not drain on acting-leader "
                        f"{current_leader.get_id()} within {_DRAIN_BOUND_SEC}s; "
                        f"refusing to drop leadership against a non-empty distrib "
                        f"pipeline")

                ### 5- drop leadership on current leader (drain complete)
                try:
                    leader_rpc.bdev_lvol_set_leader(lvs_name, leader=False, bs_nonleadership=True)
                    leader_rpc.bdev_distrib_force_to_non_leader(lvs_jm_vuid)
                except Exception as e:
                    _abort_restart_and_unblock(f"Failed to demote leader {current_leader.get_id()}: {e}")

            if disconnected_peers:
                logger.info(f"Peers disconnected {disconnected_peers}, forcing journal replication on node: {snode.get_id()}")
                _fenced("jc_explicit_synchronization", lvs_jm_vuid)

        ### 5- examine (idempotent: skip only when raid AND lvstore already surfaced)
        # #4: raid/lvstore probes were computed pre-block (see above the block
        # section) — they read snode's own fresh SPDK only, and
        # force_to_non_leader does not affect bdev/lvstore presence.
        _fenced("bdev_distrib_force_to_non_leader", lvs_jm_vuid)
        if raid_already and lvstore_already:
            logger.info(
                "Raid %s and lvstore %s already present on %s; skipping examine",
                lvs_raid, lvs_name, snode.get_id())
        else:
            if raid_already and not lvstore_already and raid_preexisted:
                # Raid pre-existed this pass and the lvstore module never surfaced
                # it on this SPDK process (a prior activation pass examined the
                # raid and the lvstore-side examine failed/was incomplete).
                # SPDK rejects re-examine of an already-examined bdev with
                # "Duplicate bdev name for manual examine: <raid>", so calling
                # bdev_examine again is a no-op that leaves the lvstore
                # missing forever and burns the activation retry loop.
                #
                # Drop the raid so the underlying distribs are reusable, then
                # re-create it via _create_bdev_stack (which is itself
                # idempotent — it skips bdevs already present and only creates
                # what's missing). The fresh bdev_examine below now runs
                # against a newly-registered raid and the lvstore module gets
                # a real chance to surface.
                logger.info(
                    "Raid %s present but lvstore %s did not surface on %s; "
                    "dropping raid for clean re-examine",
                    lvs_raid, lvs_name, snode.get_id())
                try:
                    _fenced("bdev_raid_delete", lvs_raid)
                except Exception as e:
                    logger.warning(
                        "bdev_raid_delete(%s) raised: %s — proceeding to "
                        "_create_bdev_stack which is idempotent", lvs_raid, e)
                stack = lvs_node.lvstore_stack if is_takeover else None
                if is_takeover:
                    ret, err = _create_bdev_stack(snode, stack, primary_node=lvs_node)
                else:
                    ret, err = _create_bdev_stack(snode, [])
                if not ret:
                    logger.error(
                        "Failed to rebuild bdev stack on %s after raid drop: %s",
                        snode.get_id(), err)
                    # Fall through; bdev_examine below will surface what we have.
            elif raid_already and not lvstore_already:
                # Normal restart: the raid was freshly built this pass in step 1
                # and has never been examined, so the first-time bdev_examine below
                # surfaces the lvstore. Dropping+recreating it here would be pure
                # churn inside the (minimized) port-block window — the duplicate
                # bdev_raid_create observed 2026-06-12 (LVS_5199).
                logger.info(
                    "Raid %s freshly built this pass on %s; examining without drop",
                    lvs_raid, snode.get_id())

            # Examine is required whenever the lvstore isn't surfaced — whether
            # the raid was freshly created by _create_bdev_stack (normal restart
            # path) or pre-existing with stale state (activation retry). The
            # previous "raid_already → skip examine" shortcut broke the normal
            # restart path: _create_bdev_stack leaves the raid in place but does
            # not examine it, so the lvstore never surfaces and the subsequent
            # bdev_lvol_get_lvstores validation fails every time.
            _fenced("bdev_examine", lvs_raid)

            ### 6- wait for examine
            _fenced("bdev_wait_for_examine",
                    budget=constants.FENCE_WAIT_EXAMINE_TIMEOUT_SEC)

        # Validate lvstore recovery
        ret = _fenced("bdev_lvol_get_lvstores", lvs_name)
        if not ret:
            logger.error(f"Failed to recover lvstore: {lvs_name} on node: {snode.get_id()}")
            if activation_mode:
                # In activation we can't safely patch partial on-disk state.
                # Tell the caller to restart this node before continuing.
                raise LVSRestartRequiredError(
                    snode.get_id(), lvs_name,
                    detail=f"raid={lvs_raid} present but lvstore did not recover"
                    if raid_already else "examine did not produce lvstore")
            if not force:
                _abort_restart_and_unblock("Failed to recover lvstore")

        # Validate all bdev recovery — DEFERRED to after the port unblock
        # (2026-07-22, user decision): the per-lvol probes cost 60-230ms of
        # blocked-window time and their only consumer is the abort decision. An
        # abort after the unblock kills SPDK outright, which to the cluster is a
        # node crash — a state the failover machinery already handles — so the
        # rare missing-blob case trades a clean in-window abort for a
        # crash-equivalent one, and every restart stops paying the probes inside
        # the client-visible outage. Runs before ### 9 so no subsystem is bound
        # to a missing blob. Per-lvol name-filtered probes, NOT one unfiltered
        # dump (the dump costs seconds of SPDK app-thread time at scale).
        def _deferred_lvol_verify():
            for lv in lvol_list:
                bdev_name = lv.lvol_uuid
                passed = health_controller.check_bdev(bdev_name, rpc_client=rpc_client)
                if not passed:
                    logger.error(f"Failed to recover BDev: {bdev_name} on node: {snode.get_id()}")
                    if not force:
                        _abort_restart_and_unblock("Failed to recover lvstore")

        ### 7- take leadership
        # Derive the kernel-side role from snode's topology relative to lvs_node.
        # On takeover snode is acting as leader, but its kernel role must still
        # reflect topology so the peer view of the original primary stays
        # coherent. Hardcoding role="primary" caused the LVS_9060 follow-on
        # incident (2026-04-25 11:28:50 run): when the original primary later
        # rejoins, peers disagree on who the primary is and a writer conflict
        # follows.
        if snode.get_id() == lvs_node.get_id():
            snode_lvs_role = "primary"
        elif snode.get_id() == lvs_node.secondary_node_id:
            snode_lvs_role = "secondary"
        elif snode.get_id() == lvs_node.tertiary_node_id:
            snode_lvs_role = "tertiary"
        else:
            _abort_restart_and_unblock(
                f"snode {snode.get_id()} is not a registered peer of "
                f"lvstore {lvs_name} (lvs_node={lvs_node.get_id()})")
        ret = _fenced(
            "bdev_lvol_set_lvs_opts",
            lvs_name,
            groupid=lvs_jm_vuid,
            subsystem_port=lvs_node.get_lvol_subsys_port(lvs_name),
            hublvol_port=lvs_node.get_hublvol_port(lvs_name),
            role=snode_lvs_role,
        )
        ret = _fenced("bdev_lvol_set_leader", lvs_name, leader=True)
        leader_restored = False
        for _ in range(10):
            # 10 x 0.2s of sleep plus 10 RPCs is a large slice of the fence
            # budget on its own; give up the fence rather than the deadline.
            _check_fence_deadline("leader-restore poll")
            try:
                ret = _fenced("bdev_lvol_get_lvstores", lvs_name)
                if ret and len(ret) > 0 and ret[0].get("lvs leadership"):
                    leader_restored = True
                    break
            except Exception:
                pass
            time.sleep(0.2)
        if not leader_restored:
            logger.error("Failed to restore leadership for %s on node %s", lvs_name, snode.get_id())
            if not force:
                _abort_restart_and_unblock(f"Failed to restore leadership for {lvs_name}")

        if not activation_mode:
            ### 8- create hublvol and expose via subsystem with listeners
            if sec_nodes:
                if is_takeover:
                    try:
                        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                        snode.adopt_hublvol(lvs_node, cluster.nqn)
                        logger.info("Adopted hublvol on new leader %s for %s", snode.get_id(), lvs_name)
                    except Exception as e:
                        logger.error("Error adopting hublvol on new leader: %s", e)
                        _abort_restart_and_unblock(f"adopt_hublvol on new leader failed: {e}")
                else:
                    try:
                        if not snode.recreate_hublvol():
                            _abort_restart_and_unblock(
                                f"recreate_hublvol returned False on {snode.get_id()}")
                    except RPCException as e:
                        logger.error("Error creating hublvol: %s", e)
                        _abort_restart_and_unblock(f"recreate_hublvol raised: {e}")
                    try:
                        # defer_db_write: the full-object node write (~150ms FDB
                        # round-trip caught in-window by the [NODE-WRITE]
                        # tripwire) is persisted atomically post-unblock below.
                        snode.create_transfer_hublvol(defer_db_write=True)
                        _deferred_node_persist["needed"] = True
                    except RPCException as e:
                        logger.error("Error creating transfer hublvol: %s", e)

            ### 8b- connect peers to hublvol WITHIN port-blocked window
            # The old leader must be set to secondary role (via set_lvs_opts + connect_hublvol)
            # BEFORE we unblock its port.  Otherwise new IO can arrive and trigger
            # spdk_lvs_trigger_leadership_switch, re-promoting the old leader and
            # causing a writer conflict.
            cluster = db_controller.get_cluster_by_id(snode.cluster_id)

            # Identify the topological secondary owner (sec_1) of this LVS by
            # looking at lvs_node, NOT by sec_nodes ordering. The previous
            # index-based code (sec_nodes[0]) routed sec_1 work to whichever
            # peer happened to be first after disconnected_peers filtering —
            # which on the LVS_9060 takeover (2026-04-25 11:28:50) wasn't even
            # the right LVS, since create_secondary_hublvol read the lvstore
            # name off snode.lvstore (snode's own primary, not the LVS being
            # taken over).
            sec1_id = lvs_node.secondary_node_id
            sec1_node = next((s for s in sec_nodes if s.get_id() == sec1_id), None)
            sec1_online = bool(sec1_node and sec1_node.get_id() not in disconnected_peers)

            # Create the sec_1 hublvol only if sec_1 is a peer (not snode itself)
            # and it's online. When snode IS the topological sec_1 (secondary
            # owner taking leadership), there is no separate node to expose
            # the secondary hublvol on — the leader's primary hublvol on snode
            # is the only path until the original primary returns.
            if sec1_online and sec1_node is not None:
                try:
                    sec1_node.create_secondary_hublvol(lvs_node, cluster.nqn)
                except Exception as e:
                    logger.error("Error creating secondary hublvol on sec_1: %s", e)
                    _abort_restart_and_unblock(
                        f"create_secondary_hublvol on {sec1_node.get_id()} raised: {e}")

            # Track tertiary→secondary failover-path attaches to run AFTER the
            # peer port unblock — keeping the in-freeze attach single-path with
            # a 0.2 s RPC budget and pushing the second-path INTER_ATTACH_SLEEP
            # outside the IO-impact window. ``deferred_tertiary_paths`` holds
            # ``(tert_node, primary_node, sec1_node)`` tuples to apply later.
            deferred_tertiary_paths = []

            for sec_node in sec_nodes:
                if sec_node.get_id() in disconnected_peers:
                    continue
                # Role and failover are determined by topology, not by index.
                # An index-based assignment (sec_nodes[0] -> 'secondary',
                # rest -> 'tertiary') breaks when the original primary is
                # filtered out via disconnected_peers and shifts the
                # remaining peers up one slot.
                if sec_node.get_id() == lvs_node.secondary_node_id:
                    sec_role = "secondary"
                elif sec_node.get_id() == lvs_node.tertiary_node_id:
                    sec_role = "tertiary"
                    # Defer the tertiary→secondary path; in-freeze attach is
                    # single-path against the (returning) primary only.
                    if sec1_online:
                        deferred_tertiary_paths.append((sec_node, snode, sec1_node))
                else:
                    logger.warning(
                        "Skipping hublvol connect for %s: not a registered "
                        "peer of %s (lvs_node=%s)",
                        sec_node.get_id(), lvs_name, lvs_node.get_id())
                    continue
                try:
                    # Single-path attach against ``snode`` (the leader). The
                    # secondary failover for tertiary is appended in a
                    # post-unblock pass via ``add_hublvol_failover_path``.
                    #
                    # Pass lvs_node=lvs_node so LVS metadata (lvstore name,
                    # jm_vuid, port, hublvol NQN/bdev) comes from the
                    # configured primary of the LVS being taken over, *not*
                    # from snode — when this is a takeover (lvs_primary set,
                    # configured primary offline), snode.hublvol points at
                    # snode's OWN primary-LVS, which is the wrong LVS for
                    # this connection. Without it, the call sets up the
                    # wrong LVS on the peer, the LVS being taken over is
                    # never wired up, and the subsequent peer-port unblock
                    # opens the tertiary path to a still-unconfigured LVS —
                    # any client IO arriving on the still-open existing
                    # connection triggers spdk_lvs_trigger_leadership_switch
                    # on the peer and produces a dual-leader writer
                    # conflict. (incident 2026-05-21 05:38:14 k8s_native_
                    # resilient_failover-20260520-231822, LVS_270 takeover
                    # by worker-4: tertiary worker-1 was wired up as
                    # tertiary of LVS_9915 instead of LVS_270, port 4432
                    # was unblocked, worker-1 re-promoted on next client
                    # write, writer conflict on worker-4.)
                    ok = sec_node.connect_to_hublvol(snode, failover_node=None, role=sec_role,
                                                     rpc_timeout=0.2, lvs_node=lvs_node,
                                                     coordinator_lock=_hub_locks.get(sec_node.get_id()))
                except Exception as e:
                    logger.error("Error establishing hublvol on %s: %s", sec_node.get_id(), e)
                    _abort_restart_and_unblock(
                        f"connect_to_hublvol on {sec_node.get_id()} raised: {e}")
                if not ok:
                    _abort_restart_and_unblock(
                        f"connect_to_hublvol returned False on {sec_node.get_id()} ({sec_role})")

                ### 8c- unblock this peer's port only after its hublvol is connected
                if sec_node in blocked_peers:
                    _unblock_peer_port(sec_node)

            # Every peer port is unblocked — end of the client-visible outage
            # span. Release the window gate BEFORE the lvol-attach pass below
            # so the next recreate's block window can start while lvols attach.
            _release_block_gate()
            _flush_port_events()
            _persist_deferred_node_fields()

        _deferred_lvol_verify()

        ### 9- add lvols to subsystems
        executor = ThreadPoolExecutor(max_workers=50)
        for lvol in lvol_list:
            executor.submit(add_lvol_thread, lvol, snode, lvol_ana_state)
        executor.shutdown(wait=True)

        # Phase transition: post_unblock — delayed sync deletes and registrations can now proceed
        _release_block_gate()
        _flush_port_events()
        _persist_deferred_node_fields()
        _release_hub_locks()
        _set_restart_phase(snode, lvs_name, StorageNode.RESTART_PHASE_POST_UNBLOCK, db_controller)
        if _block_longest["sec"]:
            logger.info("[RESTART] Longest client-port block for %s: %.3fs "
                        "(reject threshold 6s)", lvs_name, _block_longest["sec"])
        logger.info("[RESTART] Port-block phase for %s on %s took %.3fs "
                    "(phase span incl. post-unblock work; see per-port lines "
                    "for true outage)",
                    lvs_name, snode.get_id()[:8], time.monotonic() - _port_block_t0)

        ### 10b- deferred tertiary→secondary hublvol failover paths
        # The in-freeze attach above used a single path (tertiary → primary).
        # Now that every peer's port is unblocked and IO is flowing again,
        # top up the multipath controller on each tertiary so a future primary
        # loss has an immediate failover. The coordinator's
        # INTER_ATTACH_SLEEP_SEC (3 s) cost lives here, OUTSIDE the IO-impact
        # window — it doesn't sit inside the leader-port-block freeze any more.
        if not activation_mode and deferred_tertiary_paths:
            for tert_node, primary_node, sec1_failover in deferred_tertiary_paths:
                if sec1_failover is None:
                    # Only appended when ``sec1_online`` was True (meaning
                    # ``sec1_node`` was non-None at the time), so this branch
                    # should be unreachable in practice — guard for mypy.
                    continue
                try:
                    if tert_node.add_hublvol_failover_path(primary_node, sec1_failover):
                        logger.info("Added deferred secondary %s hublvol path on tertiary %s for %s",
                                    sec1_failover.get_id(), tert_node.get_id(), lvs_name)
                    else:
                        logger.warning("Failed to add deferred secondary %s hublvol path on tertiary %s for %s",
                                       sec1_failover.get_id(), tert_node.get_id(), lvs_name)
                except Exception as e:
                    logger.error("Error adding deferred hublvol failover path on tertiary %s: %s",
                                 tert_node.get_id(), e)

        if not activation_mode:
            ### 11- demote old leader's subsystems to non_optimized (async)
            # Per design: after restarting node takes leadership, the old leader must
            # start demoting all its lvol subsystems to non_optimized.
            for sec_node in sec_nodes:
                if sec_node.get_id() in disconnected_peers:
                    continue
                try:
                    sec_rpc = sec_node.rpc_client(timeout=10, retry=2)
                    for lvol in lvol_list:
                        listener_port = sec_node.get_lvol_subsys_port(lvol.lvs_name)
                        for iface in sec_node.data_nics:
                            if iface.ip4_address:
                                tr_type = "RDMA" if sec_node.active_rdma and iface.trtype == "RDMA" else "TCP"
                                sec_rpc.listeners_create(
                                    lvol.nqn, tr_type, iface.ip4_address, listener_port,
                                    ana_state="non_optimized")
                    logger.info("Demoted subsystems to non_optimized on old leader %s", sec_node.get_id())
                except Exception as e:
                    logger.warning("Failed to demote subsystems on %s: %s", sec_node.get_id(), e)

            ### finish
            for sec_node in sec_nodes:
                if sec_node.get_id() not in disconnected_peers:
                    _set_lvstore_status_atomic(sec_node.get_id(), "ready", db_controller)

        # Clear restart phase for this LVS
        _set_restart_phase(snode, lvs_name, "", db_controller)

        _set_lvstore_status_atomic(lvs_node.get_id(), "ready", db_controller)

        # reset snapshot delete status (only for own primary LVS)
        if not is_takeover:
            for snap in db_controller.get_snapshots_by_node_id(snode.get_id()):
                if snap.status == SnapShot.STATUS_IN_DELETION:
                    snap.deletion_status = ''
                    snap.write_to_db()

        return True
    finally:
        # Idempotent; the in-flow release above is the normal path.
        _release_block_gate()
        # An unbounded fence is bad; a fence budget leaking onto this thread's
        # later work would be worse. Every exit path clears it.
        rpc_budget.clear_budget()
        try:
            _defer_gate_event.set()
            hublvol_reconnect.clear_defer_gate()
        except Exception:
            pass


def add_lvol_thread(lvol, snode: StorageNode, lvol_ana_state="optimized"):
    db_controller = DBController()

    # Refuse to (re)register an lvol that is being torn down: the delete
    # flow removes the namespace BEFORE the async blob delete, so an add_ns
    # here re-exposes a deleted blob to clients (incident 2026-07-14: reads
    # on the resurrected namespace returned INTERNAL DEVICE ERROR and
    # flapped the whole cluster). Callers hold stale objects — check fresh.
    try:
        if db_controller.get_lvol_by_id(lvol.get_id()).status == LVol.STATUS_IN_DELETION:
            msg = f"LVol {lvol.get_id()} is in_deletion, skipping registration"
            logger.info(msg)
            return False, msg
    except KeyError:
        msg = f"LVol {lvol.get_id()} no longer exists, skipping registration"
        logger.info(msg)
        return False, msg

    rpc_client = snode.rpc_client(timeout=10, retry=2)

    pool = db_controller.get_pool_by_id(lvol.pool_uuid)
    if pool.has_qos():
        lvol_controller.connect_lvol_to_pool(lvol.uuid, snode.get_id())

    if "crypto" in lvol.lvol_type:
        cluster = db_controller.get_cluster_by_id(snode.cluster_id)
        if not lvol_controller._create_crypto_lvol(rpc_client, lvol, cluster):
            msg = f"Failed to create crypto lvol on node {snode.get_id()}"
            logger.error(msg)
            return False, msg

    # Add NS to subsystem (idempotent: skip if already bound with matching NSID).
    if _rpc_subsystem_has_ns(rpc_client, lvol.nqn, nsid=lvol.ns_id,
                             bdev_name=lvol.top_bdev, uuid=lvol.uuid):
        logger.info("Namespace nsid=%s already on subsystem %s, skipping add_ns",
                    lvol.ns_id, lvol.nqn)
    else:
        logger.info("Add BDev to subsystem " + f"{lvol.vuid:016X}")
        if not rpc_client.nvmf_subsystem_add_ns(
                lvol.nqn, lvol.top_bdev, lvol.uuid, lvol.guid, nsid=lvol.ns_id):
            # An add_ns error is not by itself a reason to abandon the whole
            # registration. What matters for the client is whether the
            # namespace is on the subsystem now — it may already have been,
            # which is exactly how SPDK answers a duplicate add: -32602
            # "Invalid parameters" because the nsid is taken. Treating the
            # error as terminal returns before the listener loop below and
            # costs the volume a PATH, not just a namespace, and the repair
            # in lvol_monitor then re-fails identically on every cycle
            # (soak 2026-08-11: 20 such failures across two recovered nodes,
            # the dominant cause of the namespace-without-listener state;
            # the add_ns idempotency probe should absorb the duplicate case,
            # so reaching here means either a real failure or a probe miss).
            # Re-read the subsystem and only give up if the namespace is
            # genuinely absent.
            if _rpc_wait_subsystem_has_ns(rpc_client, lvol.nqn, nsid=lvol.ns_id,
                                          bdev_name=lvol.top_bdev, uuid=lvol.uuid):
                logger.warning(
                    "add_ns for nsid=%s (%s) on %s reported failure but the "
                    "namespace is present; continuing to listener setup",
                    lvol.ns_id, lvol.top_bdev, lvol.nqn)
            else:
                msg = (f"Failed to add namespace nsid={lvol.ns_id} ({lvol.top_bdev}) "
                       f"to {lvol.nqn} on {snode.get_id()}")
                logger.error(msg)
                return False, msg

    # Post-condition before publishing a listener: the subsystem MUST have the
    # namespace. A listener in front of an empty subsystem accepts connections
    # and establishes qpairs, but its namespace never joins the client's
    # multipath head — the path is silently absent, no controller reset ever
    # fires, and `nvme connect` refuses to repair it ("already connected").
    # Incident 2026-08-09: 19 such subsystems on one node, one of which cost a
    # volume all of its I/O. Both ways of arriving here are covered — an add_ns
    # that failed (above) and an idempotency check that wrongly reported the
    # namespace present (the skip branch above; observed for 4 of those 19).
    #
    # The check must identify the namespace by UUID as well as bdev name, and
    # must tolerate a short propagation delay. Getting either wrong inverts the
    # incident it guards against: soak 2026-08-11 saw this post-condition read
    # a present namespace as absent (SPDK reports it under the lvol's raw UUID,
    # not the <lvs>/<lvol> alias) and skip listener creation on both recovered
    # nodes, leaving namespace-without-listener on 4 of 6 volumes — a silent
    # path loss the control plane never flagged, re-refused by the lvol-monitor
    # repair loop on every cycle.
    if not _rpc_wait_subsystem_has_ns(rpc_client, lvol.nqn, nsid=lvol.ns_id,
                                      bdev_name=lvol.top_bdev, uuid=lvol.uuid):
        msg = (f"Subsystem {lvol.nqn} on {snode.get_id()} has no namespace "
               f"nsid={lvol.ns_id} ({lvol.top_bdev}, uuid={lvol.uuid}) after "
               f"registration; refusing to add a listener for an empty subsystem")
        logger.error(msg)
        return False, msg

    # Use per-lvstore port for this lvol's lvstore. get_lvol_subsys_port()'s
    # fallback to snode.lvol_subsys_port is only correct for lvol.lvs_name ==
    # snode.lvstore (this node's OWN primary, which legitimately has no
    # lvstore_ports entry -- it uses the plain node-level port). For any
    # OTHER lvs_name, a missing entry means the relocation that assigned
    # snode this non-leader role hasn't finished committing lvstore_ports
    # yet -- snode here can be a stale, caller-held object (same hazard as
    # the in_deletion check above). Silently falling back would register
    # the listener on snode's OWN leader port instead of lvol.lvs_name's
    # real one (2026-08-18: raced a node-removal relocation live, leaving
    # two lvols' secondaries listening on the wrong port indefinitely, with
    # nothing to ever revisit or correct it). Re-fetch once and refuse
    # rather than guess; the next lvol_monitor repair cycle retries.
    if lvol.lvs_name != snode.lvstore and lvol.lvs_name not in snode.lvstore_ports:
        snode = db_controller.get_storage_node_by_id(snode.get_id())
        if lvol.lvs_name not in snode.lvstore_ports:
            msg = (f"{snode.get_id()} has no lvstore_ports entry for "
                   f"{lvol.lvs_name} yet; refusing to add a listener for "
                   f"{lvol.nqn} on a guessed port")
            logger.warning(msg)
            return False, msg
    listener_port = snode.get_lvol_subsys_port(lvol.lvs_name)
    for iface in snode.data_nics:
        if iface.ip4_address and lvol.fabric == iface.trtype.lower():
            tr = iface.trtype
        elif iface.ip4_address and lvol.fabric == "tcp" and snode.active_tcp:
            tr = "TCP"
        else:
            continue
        if _rpc_subsystem_has_listener(rpc_client, lvol.nqn, tr, iface.ip4_address, listener_port):
            logger.info("Listener %s %s:%s already on %s, skipping",
                        tr, iface.ip4_address, listener_port, lvol.nqn)
            continue
        logger.info("adding listener for %s on IP %s (%s)", lvol.nqn, iface.ip4_address, tr)
        rpc_client.listeners_create(
            lvol.nqn, tr, iface.ip4_address, listener_port, ana_state=lvol_ana_state)

    # Guarded CAS instead of read-modify-write: a delete can land between the
    # entry guard and this point, and an unconditional full-object write both
    # clobbers concurrent field updates and resurrects an in_deletion record
    # to online (the 2026-07-14 leak: 1393 ghost "online" lvols post-run).
    def _set_online(x):
        if x.status == LVol.STATUS_IN_DELETION:
            return False
        x.status = LVol.STATUS_ONLINE
        x.io_error = False
        x.health_check = True
        return True

    try:
        lvol_obj = db_controller.get_lvol_by_id(lvol.get_id())
    except KeyError:
        msg = f"LVol {lvol.get_id()} deleted during registration"
        logger.info(msg)
        return False, msg
    updated = db_controller.atomic_update(lvol_obj, _set_online)
    if updated is None or updated.status == LVol.STATUS_IN_DELETION:
        # The delete flow's remove_ns ran before (or while) we re-registered —
        # undo our add so a deleted blob is never left exposed to clients.
        try:
            rpc_client.nvmf_subsystem_remove_ns(lvol.nqn, lvol.ns_id)
        except Exception as e:
            logger.warning("Failed to undo ns registration for deleted lvol %s: %s",
                           lvol.get_id(), e)
        msg = f"LVol {lvol.get_id()} entered deletion during registration"
        logger.info(msg)
        return False, msg
    # set QOS
    if lvol.rw_ios_per_sec or lvol.rw_mbytes_per_sec or lvol.r_mbytes_per_sec or lvol.w_mbytes_per_sec:
        lvol_controller.set_lvol(lvol.uuid, lvol.rw_ios_per_sec, lvol.rw_mbytes_per_sec,
                                 lvol.r_mbytes_per_sec, lvol.w_mbytes_per_sec)
    return True, None


def repair_lvol_registration_on_non_leader(lvol, sec_node: StorageNode, secondary_index):
    """(Re)apply an lvol's fabric registration on an ONLINE non-leader
    (secondary/tertiary): create the missing nvmf subsystem if absent
    (cntlid range mirrors the restart flow: sec1 1000, sec2/tert 2000,
    allowed hosts reapplied) and run the idempotent ns+listener
    registration (``add_lvol_thread``, non_optimized ANA).

    Shared by the lvol monitor's self-heal and the FN_LVOL_SYNC_OP task
    runner — both exist because a create-time registration can be lost
    (incident 2026-07-10: the tertiary's registration was parked behind a
    stale restart phase in a per-process in-memory queue and never ran).

    Returns ``(ok, err)``.
    """
    from simplyblock_core.controllers.host_auth import _reapply_allowed_hosts

    # Never touch an LVS whose state is owned by a restart / activation /
    # expansion — the owning flow re-registers lvols itself.
    if get_restart_phase(sec_node.get_id(), lvol.lvs_name):
        return False, (f"LVS {lvol.lvs_name} on {sec_node.get_id()} is owned "
                       f"by a restart/activation/expansion")

    # Repair only volumes that are actually supposed to be registered. Both
    # callers hand in lvol objects that can be minutes stale (the monitor's
    # cycle-start snapshot, a queued sync task) — a "missing" registration on
    # an in_deletion lvol is the delete flow's own remove_ns, and repairing
    # it re-exposes an async-deleted blob to clients (incident 2026-07-14).
    try:
        lvol = DBController().get_lvol_by_id(lvol.get_id())
    except KeyError:
        return False, f"LVol {lvol.get_id()} no longer exists"
    if lvol.status not in (LVol.STATUS_ONLINE, LVol.STATUS_OFFLINE):
        return False, (f"LVol {lvol.get_id()} status is {lvol.status}, "
                       f"not repairing registration")

    rpc_client = sec_node.rpc_client(timeout=10, retry=2)
    if rpc_client.subsystem_get(lvol.nqn) is None:
        min_cntlid = lvol_controller.lvol_min_cntlid(secondary_index + 1)
        allow_any = not bool(lvol.allowed_hosts)
        logger.warning(
            "Repairing missing subsystem %s on non-leader %s (lvol %s)",
            lvol.nqn, sec_node.get_id(), lvol.get_id())
        rpc_client.subsystem_create(
            lvol.nqn, lvol.ha_type, lvol.uuid, min_cntlid,
            max_namespaces=lvol.max_namespace_per_subsys,
            allow_any_host=allow_any)
        if lvol.allowed_hosts:
            _reapply_allowed_hosts(lvol, sec_node, rpc_client)

    return add_lvol_thread(lvol, sec_node, lvol_ana_state="non_optimized")


def get_sorted_ha_jms(current_node: StorageNode):
    """Select the remote HA journal members for ``current_node``.

    The full HA journal set is ``ha_jm_count`` members: the node's own local JM
    plus ``ha_jm_count - 1`` remote JMs returned here. Selection honors these
    dimensions, in priority order:

      0. Locality (best-effort) — reserve ONE remote copy in the current
         node's OWN failure domain, when the FD-balance cap (below) allows a
         domain to hold 2 members (local + this one). A same-domain copy is
         cheap to reach — lower round-trip than any cross-domain member —
         which matters for whatever in the write path benefits from a fast
         quorum member. Skipped outright when the cap only allows 1 per
         domain (many domains relative to ha_jm_count): even-spread already
         wins in that regime, nothing to reserve.
      1. Host-disjoint (hard) — never two journal copies on one physical host.
      2. Failure-domain balance (best-effort) — spread the REMAINING copies
         across as many domains as possible and never let one domain hold
         more than a quorum-safe cap, so losing a whole domain still leaves
         >= 2 journals (the JC quorum). With 2 domains and 4 journals the
         result is 2-2; with N>=4 domains it is one per domain (plus the
         local-domain reservation from step 0). The local JM counts toward
         its own domain's tally.
      3. Physical-label distinct (best-effort) — prefer journals on distinct
         physical labels (a coarser grouping than host).

    Best-effort means each constraint is relaxed in turn only when it cannot be
    satisfied, rather than failing placement.
    """
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(current_node.cluster_id)
    jm_count = {}
    jm_dev_to_mgmt_ip = {}
    jm_dev_to_fd = {}
    jm_dev_to_label = {}

    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if node.get_id() == current_node.get_id():  # pass
            continue

        if node.jm_device and node.jm_device.status == JMDevice.STATUS_ONLINE and node.jm_device.get_id():
            jm_count[node.jm_device.get_id()] = 0
            jm_dev_to_mgmt_ip[node.jm_device.get_id()] = node.mgmt_ip
            jm_dev_to_fd[node.jm_device.get_id()] = node.failure_domain  # int, -1 if unset
            jm_dev_to_label[node.jm_device.get_id()] = node.physical_label

    for node in db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id):
        if node.get_id() == current_node.get_id():  # pass
            continue
        if not node.jm_ids:
            continue
        for rem_jm_id in node.jm_ids:
            if rem_jm_id in jm_count:
                jm_count[rem_jm_id] += 1

    # Least-used JMs first (load balancing); ties broken in the greedy pick.
    jm_count = dict(sorted(jm_count.items(), key=lambda x: x[1]))
    total_jms = current_node.ha_jm_count
    target = total_jms - 1
    fd_enabled = cluster.enable_failure_domain

    # Per-domain cap so that losing any single domain keeps >= 2 journals.
    # Distinct domains across the candidate JMs plus the current node's own.
    all_fds = {fd for fd in jm_dev_to_fd.values() if fd >= 0}
    if current_node.failure_domain >= 0:
        all_fds.add(current_node.failure_domain)
    num_fds = len(all_fds)
    if fd_enabled and num_fds > 1:
        even_cap = math.ceil(total_jms / num_fds)   # spread as evenly as possible
        quorum_cap = total_jms - 2                   # keep >= 2 after losing one domain
        per_fd_cap = max(1, min(even_cap, quorum_cap))
    else:
        per_fd_cap = total_jms  # no domain constraint

    selected: list[str] = []
    used_ips = {current_node.mgmt_ip}
    used_labels = {current_node.physical_label} if current_node.physical_label > 0 else set()
    fd_count = {}
    if current_node.failure_domain >= 0:
        fd_count[current_node.failure_domain] = 1  # the local JM occupies its domain

    def _pick_same_fd_as_local(enforce_label):
        # One-shot reservation (not a loop): take the least-used JM in the
        # local node's own domain, if the cap leaves room for a second
        # member there and nothing has claimed that reservation yet.
        if (current_node.failure_domain < 0 or per_fd_cap < 2
                or len(selected) >= target
                or fd_count.get(current_node.failure_domain, 0) >= 2):
            return
        best = None
        for jm_id, cnt in jm_count.items():
            if not jm_id or jm_id in selected:
                continue
            ip = jm_dev_to_mgmt_ip[jm_id]
            if ip in used_ips:                            # host-disjoint (hard)
                continue
            if jm_dev_to_fd.get(jm_id, -1) != current_node.failure_domain:
                continue
            label = jm_dev_to_label.get(jm_id, 0)
            if enforce_label and label > 0 and label in used_labels:
                continue
            if best is None or cnt < best[0]:
                best = (cnt, jm_id, ip, label)
        if best is None:
            return
        _, jm_id, ip, label = best
        selected.append(jm_id)
        used_ips.add(ip)
        fd_count[current_node.failure_domain] = fd_count.get(current_node.failure_domain, 0) + 1
        if label > 0:
            used_labels.add(label)

    def _pick(enforce_fd_cap, enforce_label):
        # Greedy: repeatedly take the eligible JM that lands in the currently
        # emptiest domain (maximizes spread), breaking ties by least usage.
        while len(selected) < target:
            best = None
            for jm_id, cnt in jm_count.items():
                if not jm_id or jm_id in selected:
                    continue
                ip = jm_dev_to_mgmt_ip[jm_id]
                if ip in used_ips:                       # host-disjoint (hard)
                    continue
                fd = jm_dev_to_fd.get(jm_id, -1)
                label = jm_dev_to_label.get(jm_id, 0)
                if fd_enabled and enforce_fd_cap and fd >= 0 and fd_count.get(fd, 0) >= per_fd_cap:
                    continue
                if enforce_label and label > 0 and label in used_labels:
                    continue
                score = (fd_count.get(fd, 0) if fd >= 0 else 0, cnt)
                if best is None or score < best[0]:
                    best = (score, jm_id, ip, fd, label)
            if best is None:
                return
            _, jm_id, ip, fd, label = best
            selected.append(jm_id)
            used_ips.add(ip)
            if fd >= 0:
                fd_count[fd] = fd_count.get(fd, 0) + 1
            if label > 0:
                used_labels.add(label)

    if fd_enabled:
        _pick_same_fd_as_local(enforce_label=True)
        _pick_same_fd_as_local(enforce_label=False)
        _pick(enforce_fd_cap=True, enforce_label=True)
        _pick(enforce_fd_cap=True, enforce_label=False)   # relax label, keep domain cap
        if len(selected) < target:
            logger.warning(
                "Could only place %d/%d HA journal copies within the failure-"
                "domain quorum cap for node %s; relaxing to host-disjoint "
                "placement for the remaining copies.", len(selected), target,
                current_node.get_id())
            _pick(enforce_fd_cap=False, enforce_label=False)  # last resort
    else:
        _pick(enforce_fd_cap=False, enforce_label=True)       # still honor labels
        _pick(enforce_fd_cap=False, enforce_label=False)
    return selected[:target]


def get_node_jm_names(current_node: StorageNode, remote_node=None):
    jm_list = []
    if current_node.jm_device:
        if remote_node:
            jm_list.append(f"remote_{current_node.jm_device.jm_bdev}n1")
        else:
            jm_list.append(current_node.jm_device.jm_bdev)
    else:
        jm_list.append("JM_LOCAL")

    if current_node.enable_ha_jm:
        for jm_id in current_node.jm_ids:
            if not jm_id:
                continue

            if remote_node:
                if remote_node.jm_device.get_id() == jm_id:
                    jm_list.append(remote_node.jm_device.jm_bdev)
                    continue

            jm_dev = DBController().get_jm_device_by_id(jm_id)
            jm_list.append(f"remote_{jm_dev.jm_bdev}n1")

    return jm_list[:current_node.ha_jm_count]


def get_secondary_nodes(current_node: StorageNode, exclude_ids=None, removed_node=None):
    if exclude_ids is None:
        exclude_ids = []
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(current_node.cluster_id)
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id)
    # Group by failure domain (stable sort, preserves DB order within each
    # domain) before scanning candidates. The "first valid candidate after my
    # own position" logic below skips same-domain nodes as forbidden, so on an
    # arbitrary/interleaved node order it can still land back on a same-domain
    # pick once every other domain's nodes are already claimed -- purely an
    # artifact of iteration order, not availability (verified by simulation:
    # ~1 in 5 arbitrary orderings produces an avoidable same-domain pick even
    # when a fully domain-disjoint assignment exists). Grouping first removes
    # that sensitivity: every node's forward scan cleanly skips past the rest
    # of its own domain into the next one. A no-op when FD is disabled (all
    # nodes share the same failure_domain, so the sort is order-preserving).
    all_nodes = sorted(all_nodes, key=lambda n: n.failure_domain)
    if len(all_nodes) == 2:
        for node in all_nodes:
            if node.get_id() != current_node.get_id() and node.get_id() not in exclude_ids:
                return [node.get_id()]

    def _candidates(forbidden_fds, forbidden_labels):
        nodes = []
        nod_found = False
        for node in all_nodes:
            if node.get_id() == current_node.get_id() or node.get_id() in exclude_ids:
                if node.get_id() == current_node.get_id():
                    nod_found = True
                continue
            elif node.status == StorageNode.STATUS_ONLINE and node.mgmt_ip != current_node.mgmt_ip:
                # elif node.status == StorageNode.STATUS_ONLINE :
                # Domain 0 is a valid id, so guard on >= 0 rather than truthiness.
                if forbidden_fds and node.failure_domain >= 0 and node.failure_domain in forbidden_fds:
                    continue
                # Physical-label anti-affinity (best-effort): label 0 == unset.
                if forbidden_labels and node.physical_label > 0 and node.physical_label in forbidden_labels:
                    continue
                if node.is_secondary_node:
                    nodes.append(node.get_id())

                elif not node.lvstore_stack_secondary:
                    nodes.append(node.get_id())
                    if nod_found:
                        return [node.get_id()]

                elif removed_node and node.get_id() == removed_node.secondary_node_id:
                    nodes.append(node.get_id())

        return nodes

    # Anti-affinity is best-effort and honored on BOTH failure domain and
    # physical label, in that priority order: prefer a secondary that differs
    # in domain AND label from the primary; then relax the label; then relax
    # the domain; finally host-disjoint only.
    fd_on = cluster.enable_failure_domain and current_node.failure_domain >= 0
    forbidden_fds = {current_node.failure_domain} if fd_on else None
    forbidden_labels = {current_node.physical_label} if current_node.physical_label > 0 else None

    for f_fds, f_labels in ((forbidden_fds, forbidden_labels),
                            (forbidden_fds, None),
                            (None, forbidden_labels),
                            (None, None)):
        result = _candidates(f_fds, f_labels)
        if result:
            if fd_on and f_fds is None:
                logger.warning(
                    "No failure-domain-disjoint secondary available for node %s; "
                    "falling back to host-disjoint placement.", current_node.get_id())
            return result
    return []


def splice_stranded_secondary(stranded_node) -> bool:
    """Fold a node get_secondary_nodes() could not place into the pairing
    graph already built by the in-progress cluster_activate() pass.

    get_secondary_nodes() walks primaries in order, greedily picking the most
    domain/host-disjoint unclaimed candidate for each. That greedy walk has no
    mechanism to guarantee the resulting secondary_node_id/lvstore_stack_secondary
    edges close a cycle spanning every online node: it can close a cycle over a
    strict subset and leave the remaining node(s) with zero unclaimed
    candidates, even though a perfect pairing trivially exists whenever there
    are 2+ online nodes (observed 2026-08-03: 12 nodes across 3 failure
    domains formed an 11-node cycle, stranding the 12th and aborting
    activation).

    Rather than reworking the greedy walk into a global matching solver, this
    repairs the one failure mode it has: pick any already-formed edge P->X
    (P.secondary_node_id == X.get_id()) and splice the stranded node in
    between, P->stranded->X. This always succeeds as long as at least one
    edge already exists (guaranteed once 2+ pairings have been made this
    activation pass) and turns the cycle that edge belongs to into one that
    also covers the stranded node, without disturbing any other node. Prefers
    an edge where both P and X differ from the stranded node's failure domain
    (falling back to a host-disjoint-only edge), mirroring get_secondary_nodes'
    own anti-affinity tiering.
    """
    db_controller = DBController()
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(stranded_node.cluster_id)
    # Deterministic tie-breaking among equally domain-scored edges -- see
    # get_secondary_nodes for why this sort matters.
    all_nodes = sorted(all_nodes, key=lambda n: n.failure_domain)
    edges = [n for n in all_nodes if n.secondary_node_id and n.get_id() != stranded_node.get_id()]

    def _host_disjoint(p, x):
        return p.mgmt_ip != stranded_node.mgmt_ip and x.mgmt_ip != stranded_node.mgmt_ip

    def _domain_mismatch_score(p, x):
        if stranded_node.failure_domain < 0:
            return 0
        return sum(1 for n in (p, x) if n.failure_domain != stranded_node.failure_domain)

    best = None
    best_score = -1
    for p in edges:
        x = db_controller.get_storage_node_by_id(p.secondary_node_id)
        if not x or x.get_id() == stranded_node.get_id() or not _host_disjoint(p, x):
            continue
        score = _domain_mismatch_score(p, x)
        if score > best_score:
            best_score, best = score, (p, x)

    if best is None:
        return False

    p, x = best
    logger.warning(
        "get_secondary_nodes found no candidate for node %s; splicing it into "
        "the existing pairing %s -> %s (domain-mismatch score %d/2).",
        stranded_node.get_id(), p.get_id(), x.get_id(), best_score)

    p = db_controller.get_storage_node_by_id(p.get_id())
    p.secondary_node_id = stranded_node.get_id()
    p.write_to_db()

    stranded_node = db_controller.get_storage_node_by_id(stranded_node.get_id())
    stranded_node.lvstore_stack_secondary = p.get_id()
    stranded_node.secondary_node_id = x.get_id()
    stranded_node.write_to_db()

    x = db_controller.get_storage_node_by_id(x.get_id())
    x.lvstore_stack_secondary = stranded_node.get_id()
    x.write_to_db()

    return True


def get_secondary_nodes_2(current_node: StorageNode, exclude_ids=None, exclude_mgmt_ips=None,
                          exclude_failure_domains=None, exclude_physical_labels=None):
    """Get candidate nodes for second secondary assignment (dual fault tolerance).
    Unlike get_secondary_nodes, this checks lvstore_stack_tertiary instead of
    lvstore_stack_secondary, since nodes that already serve as first secondary
    for another primary are still eligible as second secondary.

    The tertiary must be host-disjoint from both the primary (current_node) and
    the already-picked first secondary, otherwise a single host outage would
    take out two of the four HA journal members and violate the cluster's
    fault-tolerance guarantee. Caller passes the secondary's mgmt_ip via
    exclude_mgmt_ips to enforce this.

    When the cluster has failure domains enabled, the tertiary is additionally
    preferred to be in a different failure domain than both the primary and the
    first secondary (caller passes the secondary's tag via
    exclude_failure_domains). This is best-effort: if no domain-disjoint
    candidate exists, placement falls back to host-disjoint only.
    """
    if exclude_ids is None:
        exclude_ids = []
    forbidden_ips = {current_node.mgmt_ip}
    if exclude_mgmt_ips:
        forbidden_ips.update(exclude_mgmt_ips)
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(current_node.cluster_id)
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(current_node.cluster_id)
    # See get_secondary_nodes for why this sort matters: it removes the
    # pairing algorithm's sensitivity to arbitrary/interleaved node order.
    all_nodes = sorted(all_nodes, key=lambda n: n.failure_domain)
    if len(all_nodes) == 2:
        for node in all_nodes:
            if node.get_id() != current_node.get_id() and node.get_id() not in exclude_ids:
                return [node.get_id()]

    def _candidates(forbidden_fds, forbidden_labels):
        nodes = []
        nod_found = False
        for node in all_nodes:
            if node.get_id() == current_node.get_id() or node.get_id() in exclude_ids:
                if node.get_id() == current_node.get_id():
                    nod_found = True
                continue
            elif node.status == StorageNode.STATUS_ONLINE and node.mgmt_ip not in forbidden_ips:
                # Domain 0 is a valid id, so guard on >= 0 rather than truthiness.
                if forbidden_fds and node.failure_domain >= 0 and node.failure_domain in forbidden_fds:
                    continue
                # Physical-label anti-affinity (best-effort): label 0 == unset.
                if forbidden_labels and node.physical_label > 0 and node.physical_label in forbidden_labels:
                    continue
                if node.is_secondary_node:
                    nodes.append(node.get_id())

                elif not node.lvstore_stack_tertiary:
                    nodes.append(node.get_id())
                    if nod_found:
                        return [node.get_id()]

        return nodes

    # Best-effort anti-affinity on BOTH failure domain and physical label,
    # relative to the primary AND the already-picked first secondary (passed by
    # the caller via exclude_failure_domains / exclude_physical_labels). Domain
    # takes priority over label; both are relaxed in turn before falling back to
    # host-disjoint only.
    fd_on = cluster.enable_failure_domain and current_node.failure_domain >= 0
    forbidden_fds = None
    if fd_on:
        forbidden_fds = {current_node.failure_domain}
        if exclude_failure_domains:
            forbidden_fds.update(fd for fd in exclude_failure_domains if fd is not None and fd >= 0)
    forbidden_labels = None
    if current_node.physical_label > 0 or exclude_physical_labels:
        forbidden_labels = set()
        if current_node.physical_label > 0:
            forbidden_labels.add(current_node.physical_label)
        if exclude_physical_labels:
            forbidden_labels.update(lbl for lbl in exclude_physical_labels if lbl and lbl > 0)

    for f_fds, f_labels in ((forbidden_fds, forbidden_labels),
                            (forbidden_fds, None),
                            (None, forbidden_labels),
                            (None, None)):
        result = _candidates(f_fds, f_labels)
        if result:
            if fd_on and f_fds is None:
                logger.warning(
                    "No failure-domain-disjoint tertiary available for node %s; "
                    "falling back to host-disjoint placement.", current_node.get_id())
            return result
    return []


def splice_stranded_tertiary(stranded_node) -> bool:
    """Tertiary-assignment counterpart to splice_stranded_secondary.

    get_secondary_nodes_2()'s greedy walk has the identical dead-end risk as
    get_secondary_nodes(): it can close a tertiary-pairing cycle over a
    subset of online nodes and strand the rest, even though a valid
    assignment exists — this can surface on any cluster with
    max_fault_tolerance >= 2 (e.g. a 2+2 layout), the same way
    splice_stranded_secondary's bug surfaced on the plain secondary pass.

    Splices the stranded node into an already-formed tertiary edge P->X
    (P.tertiary_node_id == X.get_id()), same idea as the secondary case:
    P->stranded->X. The extra wrinkle here is that a tertiary must be
    host-disjoint from BOTH a primary and that primary's OWN secondary (a
    single host outage must not take out two of the four HA journal members)
    — so splicing changes what "valid" means on both sides of the edge, and
    each side is re-checked against the other's current secondary_node_id,
    not just against each other.
    """
    db_controller = DBController()
    all_nodes = db_controller.get_storage_nodes_by_cluster_id(stranded_node.cluster_id)
    # Deterministic tie-breaking among equally domain-scored edges -- see
    # get_secondary_nodes for why this sort matters.
    all_nodes = sorted(all_nodes, key=lambda n: n.failure_domain)
    by_id = {n.get_id(): n for n in all_nodes}
    stranded_sec = by_id.get(stranded_node.secondary_node_id) if stranded_node.secondary_node_id else None

    def _valid_tertiary(primary, primary_sec, candidate):
        if candidate.get_id() == primary.get_id():
            return False
        if candidate.mgmt_ip == primary.mgmt_ip:
            return False
        if primary_sec and candidate.mgmt_ip == primary_sec.mgmt_ip:
            return False
        return True

    def _domain_mismatch_score(*nodes):
        if stranded_node.failure_domain < 0:
            return 0
        return sum(1 for n in nodes if n.failure_domain != stranded_node.failure_domain)

    edges = [n for n in all_nodes if n.tertiary_node_id and n.get_id() != stranded_node.get_id()]

    best = None
    best_score = -1
    for p in edges:
        x = by_id.get(p.tertiary_node_id)
        if not x or x.get_id() == stranded_node.get_id():
            continue
        p_sec = by_id.get(p.secondary_node_id) if p.secondary_node_id else None
        if not _valid_tertiary(p, p_sec, stranded_node):
            continue
        if not _valid_tertiary(stranded_node, stranded_sec, x):
            continue
        score = _domain_mismatch_score(p, x)
        if score > best_score:
            best_score, best = score, (p, x)

    if best is None:
        return False

    p, x = best
    logger.warning(
        "get_secondary_nodes_2 found no candidate for node %s; splicing it into "
        "the existing tertiary pairing %s -> %s (domain-mismatch score %d/2).",
        stranded_node.get_id(), p.get_id(), x.get_id(), best_score)

    p = db_controller.get_storage_node_by_id(p.get_id())
    p.tertiary_node_id = stranded_node.get_id()
    p.write_to_db()

    stranded_node = db_controller.get_storage_node_by_id(stranded_node.get_id())
    stranded_node.lvstore_stack_tertiary = p.get_id()
    stranded_node.tertiary_node_id = x.get_id()
    stranded_node.write_to_db()

    x = db_controller.get_storage_node_by_id(x.get_id())
    x.lvstore_stack_tertiary = stranded_node.get_id()
    x.write_to_db()

    return True


def create_lvstore(snode: StorageNode, ndcs, npcs, distr_bs, distr_chunk_bs, page_size_in_blocks, max_size):
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    lvstore_stack: List[dict] = []
    distrib_list = []
    distrib_vuids = []
    # Fixed size per distrib, reported up to the raid0/lvstore layer,
    # regardless of cluster raw capacity (max_size) or number_of_distribs.
    size = constants.DISTRIB_SIZE_BYTES
    distr_page_size = page_size_in_blocks
    # distr_page_size = (ndcs + npcs) * page_size_in_blocks
    # cluster_sz = ndcs * page_size_in_blocks
    cluster_sz = page_size_in_blocks * constants.LVOL_CLUSTER_RATIO
    strip_size_kb = int((ndcs + npcs) * 2048)
    strip_size_kb = utils.nearest_upper_power_of_2(strip_size_kb)
    jm_vuid = 1
    jm_ids = []
    if snode.enable_ha_jm:
        jm_vuid = utils.get_random_vuid()
        jm_ids = get_sorted_ha_jms(snode)
        logger.debug(f"online_jms: {str(jm_ids)}")
        snode.remote_jm_devices = _connect_to_remote_jm_devs(snode, jm_ids)
        snode.jm_ids = jm_ids
        snode.jm_vuid = jm_vuid
        snode.write_to_db()

    write_protection = False
    if ndcs > 1:
        write_protection = True
    # Which generation of write protection to create these distribs with. New
    # clusters are v2 from the start; a cluster upgraded from a release without
    # v2 stays on v1 until `sbctl cluster switch-write-protection` has run the
    # runtime RPC on every node (see cluster.write_protection_v2). Persisted on
    # the stack entry like the other create flags, and re-normalised against
    # the cluster on every replay by apply_write_protection_mode.
    wp_key = ("write_protection_v2" if cluster.write_protection_v2
              else "write_protection")
    for _ in range(snode.number_of_distribs):
        distrib_vuid = utils.get_random_vuid()
        while distrib_vuid in distrib_vuids:
            distrib_vuid = utils.get_random_vuid()

        distrib_name = f"distrib_{distrib_vuid}"
        distrib_params = {
            "name": distrib_name,
            "jm_vuid": jm_vuid,
            "vuid": distrib_vuid,
            "ndcs": ndcs,
            "npcs": npcs,
            "num_blocks": size // distr_bs,
            "block_size": distr_bs,
            "chunk_size": distr_chunk_bs,
            "pba_page_size": distr_page_size,
            wp_key: write_protection,
        }
        # Per-chunk placement is a cluster-wide opt-in. Persist it on each
        # stack entry so subsequent restarts re-create the bdev with the
        # same flag without having to re-fetch the cluster setting.
        if cluster.shared_placement:
            distrib_params["shared_placement"] = True
        # Failure-domain placement is activated implicitly via the per-node
        # failure_domain id (>= 0) in the distrib cluster map sent by
        # distr_send_cluster_map / distr_add_nodes (see get_distr_cluster_map);
        # there is no separate bdev_distrib_create flag.
        lvstore_stack.extend(
            [
                {
                    "type": "bdev_distr",
                    "name": distrib_name,
                    "params": distrib_params,
                }
            ]
        )
        distrib_list.append(distrib_name)
        distrib_vuids.append(distrib_vuid)

    if len(distrib_list) == 1:
        raid_device = distrib_list[0]
    else:
        raid_device = f"raid0_{jm_vuid}"
        lvstore_stack.append(
            {
                "type": "bdev_raid",
                "name": raid_device,
                "params": {
                    "name": raid_device,
                    "raid_level": "0",
                    "base_bdevs": distrib_list,
                    "strip_size_kb": strip_size_kb
                },
                "distribs_list": distrib_list,
                "jm_ids": jm_ids,
                "jm_vuid": jm_vuid,
            }
        )

    lvs_name = f"LVS_{jm_vuid}"
    lvstore_stack.append(
        {
            "type": "bdev_lvstore",
            "name": lvs_name,
            "params": {
                "name": lvs_name,
                "bdev_name": raid_device,
                "cluster_sz": cluster_sz,
                "clear_method": "none",
                "num_md_pages_per_cluster_ratio": 1,
            }
        }
    )

    snode.lvstore = lvs_name
    snode.lvstore_stack = lvstore_stack
    snode.raid = raid_device
    # Allocate the lvstore ports and persist them under one lock:
    # get_next_lvstore_ports is a read-allocate with no reservation, so two
    # concurrent create_lvstore calls (parallel activation Pass 1) would pick
    # the same ports. The lock spans allocation -> write_to_db so the next
    # allocator's used-port scan already sees these ports taken.
    with _lvstore_port_alloc_lock:
        lvol_subsys_port, hublvol_port = utils.get_next_lvstore_ports(snode.cluster_id)
        snode.lvol_subsys_port = lvol_subsys_port
        # Re-read lvstore_ports from DB to preserve ports propagated by other
        # nodes' create_lvstore calls (the in-memory snode may be stale).
        fresh = db_controller.get_storage_node_by_id(snode.get_id())
        snode.lvstore_ports = fresh.lvstore_ports if fresh.lvstore_ports else {}
        snode.lvstore_ports[lvs_name] = {
            "lvol_subsys_port": lvol_subsys_port,
            "hublvol_port": hublvol_port,
        }
        snode.lvstore_status = "in_creation"
        snode.write_to_db()

    ret, err = _create_bdev_stack(snode, lvstore_stack)
    if err:
        logger.error(f"Failed to create lvstore on node {snode.get_id()}")
        logger.error(err)
        return False

    rpc_client = snode.rpc_client()
    ret = rpc_client.bdev_lvol_set_lvs_opts(
        snode.lvstore,
        groupid=snode.jm_vuid,
        subsystem_port=snode.get_lvol_subsys_port(snode.lvstore),
        hublvol_port=snode.get_hublvol_port(snode.lvstore),
        role="primary"
    )
    ret = rpc_client.bdev_lvol_set_leader(snode.lvstore, leader=True)

    secondary_ids = []
    if snode.secondary_node_id:
        secondary_ids.append(snode.secondary_node_id)
    if snode.tertiary_node_id:
        secondary_ids.append(snode.tertiary_node_id)

    for sec_node_id in secondary_ids:
        sec_node = db_controller.get_storage_node_by_id(sec_node_id)

        # Propagate per-lvstore ports to secondary node
        if not sec_node.lvstore_ports:
            sec_node.lvstore_ports = {}
        sec_node.lvstore_ports[lvs_name] = snode.lvstore_ports[lvs_name].copy()

        # creating lvstore on secondary
        sec_node.remote_jm_devices = _connect_to_remote_jm_devs(sec_node)
        sec_node.write_to_db()
        ret, err = _create_bdev_stack(sec_node, lvstore_stack, primary_node=snode)
        if err:
            logger.error(f"Failed to create lvstore on node {sec_node.get_id()}")
            logger.error(err)
            return False

        # sending to the other node (sec_node) with the primary group jm_vuid (snode.jm_vuid)
        # (release-upgrade guard: held until `cluster upgrade-complete`,
        # remove with the jc_compression_upgrade plugin)
        if jc_compression_upgrade.resume_is_held(DBController().get_cluster_by_id(sec_node.cluster_id)):
            logger.info("JC compression resume held: cluster upgrade in progress")
        else:
            ret, err = sec_node.rpc_client().jc_suspend_compression(jm_vuid=snode.jm_vuid, suspend=False)
            if not ret:
                logger.info("Failed to resume JC compression adding task...")
                tasks_controller.add_jc_comp_resume_task(sec_node.cluster_id, sec_node.get_id(), jm_vuid=snode.jm_vuid)

        sec_rpc_client = sec_node.rpc_client()
        sec_rpc_client.bdev_examine(snode.raid)
        sec_rpc_client.bdev_wait_for_examine()

        sec_node.write_to_db()

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    try:
        snode.create_hublvol(cluster_nqn=cluster.nqn)
    except RPCException as e:
        logger.error("Error establishing hublvol: %s", e)
        # return False

    try:
        snode.create_transfer_hublvol()
    except RPCException as e:
        logger.error("Error creating transfer hublvol: %s", e)

    if secondary_ids:
        # Create secondary hublvol on sec_1 so tertiary can multipath.
        # sec_1 is the CONFIGURED secondary — never secondary_ids[0], which
        # is the tertiary whenever secondary_node_id is unset (e.g. demoted
        # after a failover) and would get the secondary hublvol created on
        # the wrong node.
        sec1 = None
        if snode.secondary_node_id:
            sec1 = db_controller.get_storage_node_by_id(snode.secondary_node_id)
        if sec1 and sec1.status == StorageNode.STATUS_ONLINE:
            try:
                cluster = db_controller.get_cluster_by_id(snode.cluster_id)
                sec1.create_secondary_hublvol(snode, cluster.nqn)
            except Exception as e:
                logger.error("Error creating secondary hublvol on sec_1: %s", e)

        for sec_node_id in secondary_ids:
            sec_node = db_controller.get_storage_node_by_id(sec_node_id)
            if sec_node.status == StorageNode.STATUS_ONLINE:
                try:
                    # Brief settle beat; connect_to_hublvol retries via the
                    # reconnect coordinator, the old 1s was serial latency.
                    time.sleep(0.2)
                    # Role and failover from topology, never list position:
                    # with secondary_node_id unset the tertiary sits at
                    # index 0 and an index rule marks it "secondary" — a
                    # duplicate secondary role (same class as the 2026-05-21
                    # takeover fix in recreate_lvstore; recurred in
                    # mass_create_delete_k8s 2026-07-14). Tertiary gets
                    # multipath failover to sec_1.
                    is_tert = sec_node_id == snode.tertiary_node_id
                    failover_node = sec1 if is_tert and sec1 and sec1.status == StorageNode.STATUS_ONLINE else None
                    sec_role = "tertiary" if is_tert else "secondary"
                    sec_node.connect_to_hublvol(snode, failover_node=failover_node, role=sec_role)
                except Exception as e:
                    logger.error("Error establishing hublvol: %s", e)
                    # return False

    storage_events.node_ports_changed(snode)
    return True



# Seconds to wait before the single retry of a failed distrib (re)creation,
# giving the control plane time to reconcile a peer that went offline mid-restart
# so the rebuilt cluster map no longer references its devices as online.
_DISTR_RECREATE_RETRY_DELAY_SEC = 5


def apply_write_protection_mode(params, use_v2):
    """Normalise the write-protection generation in one distrib param dict.

    A distrib carries write protection under one of two mutually exclusive
    keys, ``write_protection`` (v1) or ``write_protection_v2``. Which one is
    correct is a property of the CLUSTER, not of the stored params: the params
    are persisted on the node's lvstore_stack at create time and replayed at
    every restart, so a stack written before the cluster switched to v2 still
    says v1 -- and replaying it verbatim would re-create the bdev on the old
    generation while every other distrib in the cluster is on the new one.

    So the stored value answers only "is write protection on at all?" (it is
    off for ndcs == 1) and the cluster flag answers "under which key?".

    Mutates and returns ``params``.
    """
    enabled = bool(params.get("write_protection") or
                   params.get("write_protection_v2"))
    params.pop("write_protection", None)
    params.pop("write_protection_v2", None)
    if enabled:
        params["write_protection_v2" if use_v2 else "write_protection"] = True
    return params



def _create_bdev_stack(snode: StorageNode, lvstore_stack=None, primary_node=None):
    # Per-distrib creation outcome, keyed by bdev name. Threads write their own
    # key (distinct keys -> GIL-safe), the main loop reads after join.
    distr_results: dict = {}

    def _create_distr(snode: StorageNode, name, params):
        # If a peer node goes offline at the exact moment a distrib is
        # (re)created, the cluster map can be briefly stale -- it still flags
        # that peer's devices as online -- and bdev_distrib_create (or the
        # subsequent map push) fails. That is transient: once the control plane
        # marks the departed devices offline, a freshly built map succeeds. So
        # try once more (send_cluster_map_to_distr rebuilds the map from the
        # current DB view each call) before giving up. A failure that survives
        # the retry is recorded so the caller aborts the restart -- the standard
        # unrecoverable-error path -- instead of completing on a broken distrib.
        for attempt in range(2):
            if attempt > 0:
                # Give the control plane a moment to reconcile the departed
                # node, then clear any half-created distrib before retrying.
                time.sleep(_DISTR_RECREATE_RETRY_DELAY_SEC)
                try:
                    rpc_client.bdev_distrib_delete(name)
                except Exception:
                    pass
            try:
                rpc_client.bdev_distrib_create(**params)
                if distr_controller.send_cluster_map_to_distr(snode, name):
                    distr_results[name] = True
                    return
                logger.error(
                    "Failed to send cluster map to distrib %s (attempt %d/2)",
                    name, attempt + 1)
            except Exception as e:
                logger.error(
                    "Failed to create bdev distrib %s (attempt %d/2): %s",
                    name, attempt + 1, e)
        distr_results[name] = False

    def _distr_failures():
        return [n for n, ok in distr_results.items() if not ok]

    rpc_client = snode.rpc_client()
    db_controller = DBController()
    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    created_bdevs: list = []
    if not lvstore_stack:
        # Restart case
        stack = snode.lvstore_stack
    else:
        stack = lvstore_stack

    # Per-name filtered probes instead of one unfiltered bdev_get_bdevs dump:
    # the stack holds ~10 names while the full dump is O(cluster size) and
    # costs seconds of SPDK app-thread time on large clusters.
    def _stack_bdev_exists(bdev_name):
        try:
            return bool(rpc_client.get_bdevs(bdev_name))
        except Exception:
            return False

    thread_list = []
    for bdev in stack:
        type = bdev['type']
        name = bdev['name']
        params = bdev['params']
        if _stack_bdev_exists(name):
            continue

        elif type == "bdev_distr":
            if primary_node:
                params['jm_names'] = get_node_jm_names(primary_node, remote_node=snode)
            else:
                params['jm_names'] = get_node_jm_names(snode)

            if snode.distrib_cpu_cores:
                distrib_cpu_mask = utils.decimal_to_hex_power_of_2(snode.distrib_cpu_cores[snode.distrib_cpu_index])
                params['distrib_cpu_mask'] = distrib_cpu_mask
                snode.distrib_cpu_index = (snode.distrib_cpu_index + 1) % len(snode.distrib_cpu_cores)

            params['full_page_unmap'] = cluster.full_page_unmap
            # The stack may have been written before this cluster switched
            # write-protection generation; replay it under whichever key the
            # cluster is on now, never the one that happens to be stored.
            apply_write_protection_mode(params, cluster.write_protection_v2)
            t = threading.Thread(target=_create_distr, args=(snode, name, params,))
            thread_list.append(t)
            t.start()
            ret = True

        elif type == "bdev_lvstore" and lvstore_stack and not primary_node:
                ret = rpc_client.create_lvstore(**params)

        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_create(**params)

        elif type == "bdev_raid":
            if thread_list:
                for t in thread_list:
                    t.join()
            # Never assemble the raid on top of distribs that failed to
            # (re)create after the retry -- that is the unrecoverable case the
            # restart must abort on.
            failed = _distr_failures()
            if failed:
                if created_bdevs:
                    _remove_bdev_stack(created_bdevs[::-1], rpc_client)
                return False, f"Failed to (re)create distrib(s) after retry: {failed}"
            distribs_list = bdev["distribs_list"]
            strip_size_kb = params["strip_size_kb"]
            ret = rpc_client.bdev_raid_create(name, distribs_list, strip_size_kb=strip_size_kb)

        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue

        if ret:
            bdev['status'] = "created"
            created_bdevs.insert(0, bdev)
        else:
            if created_bdevs:
                # rollback
                _remove_bdev_stack(created_bdevs[::-1], rpc_client)
            return False, f"Failed to create BDev: {name}"

    if thread_list:
        for t in thread_list:
            t.join()
    # Catch distrib failures for stacks without a trailing raid (the raid
    # branch checks before assembling its raid; this covers everything else).
    failed = _distr_failures()
    if failed:
        if created_bdevs:
            _remove_bdev_stack(created_bdevs[::-1], rpc_client)
        return False, f"Failed to (re)create distrib(s) after retry: {failed}"
    return True, None


def _remove_bdev_stack(bdev_stack, rpc_client, remove_distr_only=False):
    for bdev in reversed(bdev_stack):
        if 'status' in bdev and bdev['status'] == 'deleted':
            continue
        type = bdev['type']
        name = bdev['name']
        if type == "bdev_distr":
            ret = rpc_client.bdev_distrib_delete(name)
        elif type == "bdev_raid":
            ret = rpc_client.bdev_raid_delete(name)
        elif type == "bdev_lvstore":
            if remove_distr_only:
                # Non-leader teardown: bdev_lvol_delete_lvstore destroys the
                # blobstore metadata on the shared backing storage — data loss
                # for every replica. Deleting the raid below hot-removes the
                # examined lvstore bdev from this node without touching disk.
                continue
            ret = rpc_client.bdev_lvol_delete_lvstore(name)
        elif type == "bdev_ptnonexcl":
            ret = rpc_client.bdev_PT_NoExcl_delete(name)
        else:
            logger.debug(f"Unknown BDev type: {type}")
            continue
        if not ret:
            logger.error(f"Failed to delete BDev {name}")

        bdev['status'] = 'deleted'
        # time.sleep(1)


def recreate_lvstore_on_sec(snode: StorageNode):
    """Build (or rebuild) the non-leader LVS stack on ``snode`` for every
    primary that currently points at it as secondary or tertiary.

    Iterates by DB query — so callers that want to drive a *specific*
    new role onto ``snode`` set the back-references first
    (``snode.lvstore_stack_secondary`` / ``lvstore_stack_tertiary`` plus
    the primary's ``secondary_node_id`` / ``tertiary_node_id``) and let
    this function pick them up. Idempotent on roles ``snode`` already
    serves: re-running for an existing peer just reapplies the stack
    via :func:`recreate_lvstore_on_non_leader`, the same path used by
    every node restart.

    Parameters
    ----------
    snode:
        The non-leader (recipient / holder) node where the stack will be
        built. RPCs target this node's SPDK process.

    Returns
    -------
    bool
        True iff every primary's stack came up successfully. False if
        any one failed — the orchestrator treats that as a fatal
        executor error and aborts.

    Notes
    -----
    The expansion executor calls this after updating DB pointers for
    a newly-assigned role; by then ``get_primary_storage_nodes_by_secondary_node_id``
    returns exactly the new primary plus any pre-existing peers that
    haven't moved. Re-running for unchanged peers is the cost of
    delegating discovery to the DB rather than threading a per-call
    primary argument — the production restart path makes the same
    trade-off.
    """
    db_controller = DBController()
    primaries = db_controller.get_primary_storage_nodes_by_secondary_node_id(
        snode.get_id())
    if not primaries:
        logger.info(
            f"recreate_lvstore_on_sec: no primaries point at "
            f"{snode.get_id()} — nothing to do")
        return True

    overall_ok = True
    for primary in primaries:
        try:
            ok = recreate_lvstore_on_non_leader(
                snode, leader_node=primary, primary_node=primary)
        except Exception as e:
            logger.exception(
                f"recreate_lvstore_on_sec: recreate failed for "
                f"primary {primary.get_id()} on {snode.get_id()}: {e}")
            overall_ok = False
            continue
        if ok is False:
            logger.error(
                f"recreate_lvstore_on_sec: recreate returned False for "
                f"primary {primary.get_id()} on {snode.get_id()}")
            overall_ok = False
    return overall_ok


def teardown_non_leader_lvstore(donor_node: StorageNode, primary_node: StorageNode, slot=None):
    """Tear down a non-leader (secondary or tertiary) LVStore stack on
    ``donor_node`` for the LVS owned by ``primary_node``, in-place.

    This is the inverse of ``recreate_lvstore_on_sec`` for a single
    (primary, donor) pair and is used by the single-node-expansion orchestrator
    when re-homing a sec/tert role from one node to another.

    Pre-conditions (caller's responsibility — not enforced here):
      * A replacement holder for the same role has already been created
        elsewhere and is in sync (so the LVS still meets FTT after this
        teardown).
      * IO quiescing / port-blocking is *not* performed here. Callers needing
        coordinated leadership transitions must do it before calling this
        function; this helper only removes the donor-side bdevs/subsystems.

    Slot discovery
    --------------
    Two modes:

    * ``slot=None`` (default): the helper auto-detects the slot from
      ``primary_node.secondary_node_id`` / ``tertiary_node_id``. Use this
      when the primary's pointer has not yet been moved away from the
      donor.
    * ``slot="secondary"`` or ``slot="tertiary"``: the caller asserts
      which slot the donor previously occupied. Used by the expansion
      executor, which flips the primary's pointer to the recipient
      *before* tearing down the donor (so the discovered pointer no
      longer matches).

    Steps performed:
      1. Delete per-lvol nvmf subsystems on the donor for every lvol owned
         by ``primary_node``.
      2. Remove the donor's bdev stack starting from the raid0 that backs
         the LVS, then the distribs below (+ ptnonexcl), via
         ``_remove_bdev_stack(remove_distr_only=True)`` using
         ``primary_node.lvstore_stack`` as the structural template. The
         lvstore bdev itself is NEVER deleted: it was only *examined* on the
         donor, and ``bdev_lvol_delete_lvstore`` would destroy the shared
         on-disk blobstore metadata — data loss for every replica. Deleting
         the raid hot-removes the examined lvstore bdev cleanly.
      3. Detach the hublvol nvme controller on the donor (best-effort —
         may already be gone if the controller was never attached).
      4. Clear the corresponding back-reference field
         (``lvstore_stack_secondary`` or ``lvstore_stack_tertiary``) on
         the donor and persist.

    What this does NOT do (orchestrator's responsibility):
      * Update ``primary_node.secondary_node_id`` / ``tertiary_node_id``
        pointers — the orchestrator knows the new holder and will
        overwrite there.
      * Reconfigure the sibling sec/tert (e.g., when sec_1 is being torn
        down, sec_2's multipath controller still references the donor and
        must be re-attached separately).
      * Remove ``donor_node.lvstore_ports[primary.lvstore]`` — left in place
        so a subsequent re-add to the same donor reuses the same ports.

    Returns
    -------
    bool
        True if all donor-side cleanup completed; False if the donor was
        not actually a sec/tert for this primary (no-op refused) or the
        bdev stack delete returned an error.
    """
    if slot in ("secondary", "tertiary"):
        sec_attr = f"lvstore_stack_{slot}"
    elif slot is None:
        if primary_node.secondary_node_id == donor_node.get_id():
            sec_attr = 'lvstore_stack_secondary'
        elif primary_node.tertiary_node_id == donor_node.get_id():
            sec_attr = 'lvstore_stack_tertiary'
        else:
            logger.error(
                f"teardown_non_leader_lvstore: donor {donor_node.get_id()} "
                f"is not secondary nor tertiary for primary "
                f"{primary_node.get_id()}; refusing")
            return False
    else:
        raise ValueError(
            f"teardown_non_leader_lvstore: slot must be None, "
            f"'secondary', or 'tertiary', got {slot!r}")

    db_controller = DBController()
    rpc_client = donor_node.rpc_client()

    # 1. Delete per-lvol subsystems on the donor.
    for lvol in db_controller.get_lvols_by_node_id(primary_node.get_id()):
        if lvol.status == LVol.STATUS_IN_DELETION:
            continue
        try:
            rpc_client.subsystem_delete(lvol.nqn)
        except Exception as e:
            logger.warning(
                f"teardown_non_leader_lvstore: subsystem_delete({lvol.nqn}) "
                f"on {donor_node.get_id()} raised {e}; continuing")

    # 2. Best-effort: detach the hublvol nvme controller.
    if primary_node.hublvol and primary_node.hublvol.bdev_name:
        try:
            rpc_client.bdev_nvme_detach_controller(
                primary_node.hublvol.bdev_name)
        except Exception as e:
            logger.debug(
                f"teardown_non_leader_lvstore: hublvol detach raised {e} "
                f"(likely already detached)")

    # 3. Remove the bdev stack. The donor instantiated the stack from
    #    primary_node.lvstore_stack; we use the same list as the structural
    #    template so _remove_bdev_stack walks it in the right order.
    #    remove_distr_only=True: the lvstore itself must NEVER be deleted on
    #    a non-leader — it was only examined here, and bdev_lvol_delete_lvstore
    #    wipes the blobstore metadata on the shared backing storage (data loss
    #    for all replicas). Deleting the raid hot-removes the lvstore bdev.
    if primary_node.lvstore_stack:
        # _remove_bdev_stack mutates 'status' fields — work on a shallow copy
        # of the dicts so we don't accidentally persist 'deleted' markers
        # back into primary_node.lvstore_stack on subsequent writes.
        stack_copy = [dict(b) for b in primary_node.lvstore_stack]
        _remove_bdev_stack(stack_copy, rpc_client, remove_distr_only=True)


    # 4. Clear the back-reference on the donor and persist. Re-fetch so we
    #    don't clobber unrelated concurrent edits to the donor record.
    fresh_donor = db_controller.get_storage_node_by_id(donor_node.get_id())
    setattr(fresh_donor, sec_attr, "")
    if fresh_donor.lvstore_ports and primary_node.lvstore in fresh_donor.lvstore_ports:
        del fresh_donor.lvstore_ports[primary_node.lvstore]
    fresh_donor.write_to_db()

    logger.info(
        f"teardown_non_leader_lvstore: tore down {sec_attr} for primary "
        f"{primary_node.get_id()} on donor {donor_node.get_id()}")
    return True


def reattach_sibling_failover(sibling_node: StorageNode, primary_node: StorageNode,
                              old_failover_node, new_failover_node):
    """Surgically reconfigure a sibling secondary's NVMe-oF multipath group
    so its failover path points at the new sec_1 holder instead of the old
    one.

    Used by the single-node-expansion executor when ``primary_node``'s
    sec_1 is re-homed from ``old_failover_node`` to ``new_failover_node``.
    The sec_2 node (``sibling_node``) currently has an NVMe controller
    attached to ``primary_node``'s hublvol bdev with paths
    ``{primary, old_failover}``; after this call the paths are
    ``{primary, new_failover}``.

    The two RPCs are issued in additive-then-subtractive order so that the
    sibling never has fewer than the prior path count: the new failover
    path is attached first, then the old one is removed. If the additive
    step fails on every NIC the function raises (the operator must
    intervene); if only the subtractive cleanup fails it logs and returns
    success — the dead path will be inert once the donor's stack is torn
    down.
    """
    if primary_node.hublvol is None or not primary_node.hublvol.bdev_name:
        logger.debug(
            "reattach_sibling_failover: primary %s has no hublvol; nothing to do",
            primary_node.get_id())
        return

    bdev_name = primary_node.hublvol.bdev_name
    nqn = primary_node.hublvol.nqn
    port = primary_node.hublvol.nvmf_port
    rpc_client = sibling_node.rpc_client()

    def _tr_type_for(node, iface):
        if node.active_rdma and iface.trtype == "RDMA":
            return "RDMA"
        if not node.active_rdma and node.active_tcp and iface.trtype == "TCP":
            return "TCP"
        return None

    # Add new failover path(s).
    new_attached = 0
    for iface in new_failover_node.data_nics:
        tr_type = _tr_type_for(new_failover_node, iface)
        if tr_type is None:
            continue
        try:
            ret = rpc_client.bdev_nvme_attach_controller(
                bdev_name, nqn, iface.ip4_address, port, tr_type,
                multipath="multipath")
            if ret:
                new_attached += 1
        except Exception as e:
            logger.warning(
                f"reattach_sibling_failover: attach new failover path "
                f"{iface.ip4_address}:{port} on {sibling_node.get_id()} "
                f"raised {e}")

    if new_attached == 0:
        raise RuntimeError(
            f"reattach_sibling_failover: failed to attach any new failover "
            f"path for primary {primary_node.get_id()} on sibling "
            f"{sibling_node.get_id()}")

    # This is the one hublvol attach that does not flow through
    # HublvolReconnectCoordinator, so assert the policy here too rather
    # than waiting for the next reconcile to converge it.
    from simplyblock_core.utils.hublvol_reconnect import (
        ensure_hublvol_active_active,
    )
    ensure_hublvol_active_active(
        rpc_client, bdev_name, sibling_node.get_id(), "sec_2")

    # Remove old failover path(s). Best-effort: the dead path is also
    # naturally inert once the donor's stack is torn down.
    for iface in old_failover_node.data_nics:
        tr_type = _tr_type_for(old_failover_node, iface)
        if tr_type is None:
            continue
        try:
            rpc_client.bdev_nvme_remove_trid(
                bdev_name, iface.ip4_address, port, trtype=tr_type)
        except Exception as e:
            logger.warning(
                f"reattach_sibling_failover: remove old failover path "
                f"{iface.ip4_address}:{port} on {sibling_node.get_id()} "
                f"raised {e} (path will be inert after donor teardown)")

    logger.info(
        f"reattach_sibling_failover: sibling {sibling_node.get_id()} for "
        f"LVS@{primary_node.get_id()} repointed failover "
        f"{old_failover_node.get_id()} -> {new_failover_node.get_id()}")


def send_cluster_map(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    logger.info("Sending cluster map")
    return distr_controller.send_cluster_map_to_node(snode)


def get_cluster_map(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    distribs_list = []
    nodes = [snode]

    if snode.secondary_node_id:
        try:
            nodes.append(db_controller.get_storage_node_by_id(snode.secondary_node_id))
        except KeyError:
            pass

    for bdev in snode.lvstore_stack:
        type = bdev['type']
        if type == "bdev_raid":
            distribs_list.extend(bdev["distribs_list"])

    for node in nodes:
        logger.info(f"getting cluster map from node: {node.get_id()}")
        rpc_client = node.rpc_client()
        for distr in distribs_list:
            ret = rpc_client.distr_get_cluster_map(distr)
            if not ret:
                logger.error(f"Failed to get distr cluster map: {distr}")
                return False
            logger.debug(ret)
            print("*" * 100)
            print(distr)
            results, is_passed = distr_controller.parse_distr_cluster_map(ret)
            print(utils.print_table(results))
    return True


def make_sec_new_primary(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("snode not found")
        return False

    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_NEW:
            device_controller.add_device(dev.get_id(), add_migration_task=False)

    time.sleep(5)
    for dev in snode.nvme_devices:
        if dev.status == NVMeDevice.STATUS_REMOVED:
            device_controller.device_set_failed(dev.get_id())

    snode = db_controller.get_storage_node_by_id(node_id)
    snode.primary_ip = snode.mgmt_ip
    snode.write_to_db(db_controller.kv_store)

    for lvol in db_controller.get_lvols_by_node_id(node_id):
        lvol.hostname = snode.hostname
        lvol.write_to_db()

    return True


def dump_lvstore(node_id):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if not snode.lvstore:
        logger.error("Storage node does not have lvstore")
        return False

    rpc_client = snode.rpc_client(timeout=120)
    logger.info(f"Dumping lvstore data on node: {snode.get_id()}")
    file_name = f"LVS_dump_{snode.hostname}_{snode.lvstore}_{str(datetime.datetime.now().isoformat())}.txt"
    file_path = f"/etc/simplyblock/{file_name}"
    ret = rpc_client.bdev_lvs_dump(snode.lvstore, file_path)
    if not ret:
        logger.warning("faild to dump lvstore data")
    #     return False

    logger.info(f"LVS dump file will be here: {file_path}")
    return True


def set_value(node_id, attr, value):
    db_controller = DBController()

    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.exception("Can not find storage node")
        return False

    if attr in snode.get_attrs_map():
        try:
            value = snode.get_attrs_map()[attr]['type'](value)
            logger.info(f"Setting {attr} to {value}")
            setattr(snode, attr, value)
            snode.write_to_db()
        except Exception:
            pass

    return True


def safe_delete_bdev(name, node_id):
    # On primary node
    #./ rpc.py bdev_lvol_delete lvsname / name
    # check the statue code of the following command it must be 0
    #./ rpc.py bdev_lvol_get_lvol_delete_status lvsname / name
    # #./ rpc.py bdev_lvol_delete lvsname / name - s

    # On secondary:
    #./ rpc.py bdev_lvol_delete lvsname / name - s

    db_controller = DBController()
    primary_node = db_controller.get_storage_node_by_id(node_id)
    secondary_node = db_controller.get_storage_node_by_id(primary_node.secondary_node_id)
    bdev_name = f"{primary_node.lvstore}/{name}"
    logger.info(f"deleting from primary: {bdev_name}")
    ret, _ = primary_node.rpc_client().delete_lvol(bdev_name)
    if not ret:
        logger.error(f"Failed to delete bdev: {bdev_name} from node: {primary_node.get_id()}")
        return False

    time.sleep(1)

    while True:
        try:
            ret = primary_node.rpc_client().bdev_lvol_get_lvol_delete_status(bdev_name)
        except Exception as e:
            logger.error(e)
            return False

        if ret == 1:  # Async lvol deletion is in progress or queued
            logger.info(f"deletion in progress: {bdev_name}")
            time.sleep(1)

        elif ret == 0 or ret == 2:  # Lvol may have already been deleted (not found) or delete completed
            ret, _ = primary_node.rpc_client().delete_lvol(bdev_name, sync=True)
            if not ret:
                logger.error(f"Failed to delete bdev: {bdev_name} from node: {primary_node.get_id()}")
                return False

            logger.info(f"deletion completed on primary: {bdev_name}")
            logger.info(f"deleting from secondary: {bdev_name}")
            ret, _ = secondary_node.rpc_client().delete_lvol(bdev_name, sync=True)
            if not ret:
                logger.error(f"Failed to delete bdev: {bdev_name} from node: {secondary_node.get_id()}")
                return False
            else:
                logger.info(f"deletion completed on secondary: {bdev_name}")
            return True
        else:
            logger.error(f"failed to delete bdev: {bdev_name}, status code: {ret}")
            return False


def auto_repair(node_id, validate_only=False, force_remove_inconsistent=False, force_remove_worng_ref=False):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Storage node is not online")
        return False

    cluster = db_controller.get_cluster_by_id(snode.cluster_id)
    if cluster.status not in [Cluster.STATUS_DEGRADED, Cluster.STATUS_ACTIVE]:
        logger.error("Cluster is not in degraded or active state")
        return False

    ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
    if not ret:
        logger.error("Failed to get LVol info")
        return False
    lvs_info = ret[0]
    if "uuid" in lvs_info and lvs_info['uuid']:
        lvs_uuid =  lvs_info['uuid']
    else:
        logger.error("Failed to get lvstore uuid")
        return False

    # get the lvstore uuid
    # ./spdk/scripts/rpc.py -s /mnt/ramdisk/spdk_8080/spdk.sock bdev_lvs_dump_tree  --uuid=1dc5fb34-5ff6-4be6-ab46-eb9f006f5d47 > out_8080.json
    lvstore_dump = snode.rpc_client().bdev_lvs_dump_tree(lvs_uuid)

    # #sbctl sn list-lvols d4577fa7-545f-4506-b127-7e81fc3a6e34 --json > lvols_8080.json
    # with open('lvols_8082.json', 'r') as file:
    #     lvols = json.load(file)
    lvols = lvol_controller.list_by_node(node_id)

    # #sbctl sn list-snapshots d4577fa7-545f-4506-b127-7e81fc3a6e34 --json > snaps_8080.json
    # with open('snaps_8082.json', 'r') as file:
    #     snaps = json.load(file)
    snaps = snapshot_controller.list_snapshots(node_id=node_id)

    out_blobid_dict = {}
    lvols_blobid_dict = {}
    snaps_blobid_dict = {}
    diff_list = []
    diff_lvol_dict = {}
    diff_snap_dict = {}
    diff_clone_dict = {}
    manual_del = {}
    inconsistent_dict = {}
    mgmt_diff_dict = {}


    for dump in lvstore_dump["lvols"]:
        out_blobid_dict[dump["blobid"]] = {"uuid": dump["uuid"], "name": dump["name"], "ref": dump["ref"]}

    for lvol in lvols:
        lvols_blobid_dict[lvol["BlobID"]] = {"uuid": lvol["BDdev UUID"], "name": lvol["BDev"]}
    for snap in snaps:
        snaps_blobid_dict[snap["BlobID"]] = {"uuid": snap["BDdev UUID"], "name": snap["BDev"]}

    out_blobid_dict_keys = list(out_blobid_dict.keys())
    lvols_blobid_dict_keys = list(lvols_blobid_dict.keys())
    snaps_blobid_dict_keys = list(snaps_blobid_dict.keys())

    for blob in out_blobid_dict_keys:
        if blob not in (lvols_blobid_dict_keys + snaps_blobid_dict_keys):
            if out_blobid_dict[blob]["name"] == "hublvol":
                continue
            else:
                # all blob ID in spdk but not in mgmt
                diff_list.append(blob)
        else:
            if blob  in lvols_blobid_dict_keys:
                if out_blobid_dict[blob]["name"] != lvols_blobid_dict[blob]["name"] or out_blobid_dict[blob]["uuid"] != lvols_blobid_dict[blob]["uuid"]:
                    inconsistent_dict[blob] = out_blobid_dict[blob]
                    inconsistent_dict[blob]["type"] = "lvol|clone"
            if blob in snaps_blobid_dict_keys:
                if out_blobid_dict[blob]["name"] != snaps_blobid_dict[blob]["name"] or out_blobid_dict[blob]["uuid"] != snaps_blobid_dict[blob]["uuid"]:
                    inconsistent_dict[blob] = out_blobid_dict[blob]
                    inconsistent_dict[blob]["type"] = "snap"


    for blob in lvols_blobid_dict_keys:
        if blob not in out_blobid_dict_keys:
            # All blob in mgmt but not in SPDK
            mgmt_diff_dict[blob] = lvols_blobid_dict[blob]
            mgmt_diff_dict[blob]["type"] = "lvol|clone"

    for blob in snaps_blobid_dict_keys:
        if blob not in out_blobid_dict_keys:
            # All blob in mgmt but not in SPDK
            mgmt_diff_dict[blob] = snaps_blobid_dict[blob]
            mgmt_diff_dict[blob]["type"] = "snap"

    print(f"All diff count is: {len(diff_list)}")
    print(f"All mgmt diff count is: {len(mgmt_diff_dict.keys())}")

    for blob in diff_list:
        if "LVOL" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] !=1:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_lvol_dict[blob] = out_blobid_dict[blob]
        elif "SNAP" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] != 2:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_snap_dict[blob] = out_blobid_dict[blob]
        elif "CLN" in out_blobid_dict[blob]["name"]:
            if out_blobid_dict[blob]["ref"] !=1:
                manual_del[blob] = out_blobid_dict[blob]
            else:
                diff_clone_dict[blob] = out_blobid_dict[blob]

    if not validate_only:
        cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_IN_ACTIVATION)
        time.sleep(3)

    print(f"safe lvols to be deleted count is {len(diff_lvol_dict.keys())}")
    print(f"safe snaps to be deleted count is {len(diff_snap_dict.keys())}")
    print(f"safe clone to be deleted count is {len(diff_clone_dict.keys())}")
    print(f"manual bdevs to be deleted count is {len(manual_del.keys())}")
    print(f"inconsistent bdevs to be checked count is {len(inconsistent_dict.keys())}")
    print("#########################################")
    print("Safe lvols to be deleted:")
    for blob, value in diff_lvol_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Safe snaps to be deleted:")
    for blob, value in diff_snap_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Safe clones to be deleted:")
    for blob, value in diff_clone_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Manual bdeves to be deleted that have wrong ref number:")
    for blob, value in manual_del.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only and force_remove_worng_ref:
            safe_delete_bdev(value['name'], node_id)
    print("#########################################")
    print("Inconsistent bdeves to be checked:")
    for blob, value in inconsistent_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['ref']}")
        if not validate_only and force_remove_inconsistent:
            safe_delete_bdev(value['name'], node_id)

    if not validate_only:
        cluster_ops.set_cluster_status(cluster.get_id(), Cluster.STATUS_ACTIVE)

    print("#########################################")
    print("All mgmt bdeves to be checked:")
    print(mgmt_diff_dict)
    for blob, value in mgmt_diff_dict.items():
        print(f"{blob}, {value['uuid']}, {value['name']}, {value['type']}")

    if validate_only:
        return not(diff_lvol_dict or diff_snap_dict or diff_clone_dict or manual_del or inconsistent_dict or mgmt_diff_dict)

    return True


def lvs_dump_tree(node_id):
    db_controller = DBController()
    try:
        snode = db_controller.get_storage_node_by_id(node_id)
    except KeyError:
        logger.error("Can not find storage node")
        return False

    if snode.status != StorageNode.STATUS_ONLINE:
        logger.error("Storage node is not online")
        return False

    ret = snode.rpc_client().bdev_lvol_get_lvstores(snode.lvstore)
    if not ret:
        logger.error("Failed to get LVol info")
        return False
    lvs_info = ret[0]
    if "uuid" in lvs_info and lvs_info['uuid']:
        lvs_uuid =  lvs_info['uuid']
    else:
        logger.error("Failed to get lvstore uuid")
        return False

    ret = snode.rpc_client().bdev_lvs_dump_tree(lvs_uuid)
    if not ret:
        logger.error("Failed to dump lvstore tree")
        return False

    return ret
