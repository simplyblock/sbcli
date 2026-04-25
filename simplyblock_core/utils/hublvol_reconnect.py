# coding=utf-8
"""Cross-process coordinator for hublvol NVMe-oF (re)attach.

All ``bdev_nvme_attach_controller`` / ``bdev_nvme_detach_controller`` calls
that target a hublvol subsystem (NQN of the form
``…:hublvol:LVS_xxxx``) must flow through :class:`HublvolReconnectCoordinator`.
That includes initial connect on secondary/tertiary startup, the restart
runner's hublvol wiring, the health service's periodic path repair, and the
storage-node-ops failover helpers.

The coordinator enforces three properties:

1. **Single serialized worker per ``(node_id, lvstore)`` subsystem.** An
   FDB-backed advisory lock with a TTL is held for the whole observe/detach/
   attach sequence. This is cross-process: different control-plane services
   (``TasksRunnerRestart``, ``HealthCheck``, ``StorageNodeMonitor``, SNodeAPI)
   cannot overlap on the same hublvol.

2. **Minimum cooldown between attach attempts.** After any completed
   attach/detach sequence the coordinator refuses to run another one within
   ``cooldown_sec``; a caller that arrives inside the window either observes
   an already-``enabled`` controller (no-op) or waits out the remaining
   cooldown before acting. This prevents the ``cntlid N are duplicated``
   race where a second ``bdev_nvme_attach_controller`` lands while SPDK is
   still driving ``nvme_ctrlr_destruct_poll_async`` on the prior controller.

3. **Detach-and-wait-gone before re-attach on a non-enabled controller.**
   Because SPDK's detach runs the final destroy asynchronously, the
   coordinator polls ``bdev_nvme_controller_list`` until the name is absent
   before issuing the next attach.

This is an in-repo coordinator — the SPDK target itself is not modified.
If a future refactor moves this logic into ``spdk_proxy`` / SNodeAPI as a
per-subnqn worker queue, the call sites here remain the same.
"""
import json
import logging
import threading
import time
import uuid

import fdb  # type: ignore[import-not-found]


logger = logging.getLogger(__name__)

# Per-key in-process lock used when no FDB kv_store is available (tests,
# early bootstrap). In production kv_store is always populated and the
# FDB advisory lock does the real cross-process serialization.
_process_local_locks: "dict[bytes, threading.Lock]" = {}
_process_local_locks_guard = threading.Lock()
_process_local_state: "dict[bytes, dict]" = {}


def _get_process_lock(key):
    with _process_local_locks_guard:
        lk = _process_local_locks.get(key)
        if lk is None:
            lk = threading.Lock()
            _process_local_locks[key] = lk
        return lk


#: Minimum wall-clock interval (seconds) between successive attach/detach
#: sequences on the same ``(node, lvstore)`` hublvol. Chosen > SPDK's own
#: ctrlr initialization settling time so a second arrival sees the first
#: attach's controller in its terminal ``enabled`` state, not an
#: intermediate one.
DEFAULT_COOLDOWN_SEC = 5.0

#: Max time to wait for a controller to transition out of an intermediate
#: state (``resetting`` / ``connecting``) before we force a teardown.
#: Sized against ``ctrlr_loss_timeout_sec`` below plus a small margin.
DEFAULT_TRANSIENT_WAIT_SEC = 12.0

#: Max time to wait for ``bdev_nvme_controller_list`` to report the
#: controller absent after a detach. SPDK's destroy is asynchronous, so
#: issuing a new attach before this completes is what produces
#: ``bdev_nvme_check_multipath: cntlid N are duplicated``.
DEFAULT_DETACH_WAIT_SEC = 10.0

#: How long the FDB advisory lock lives if the holder crashes without
#: releasing. Must be >> typical reconcile runtime (including detach-wait).
#: Other callers block on the lock, not on the SPDK RPC, so this only bounds
#: dead-holder recovery.
DEFAULT_LOCK_TTL_SEC = 60

#: NVMe-oF ctrlr timeout params handed to ``bdev_nvme_attach_controller``
#: for hublvol controllers. The SPDK defaults (``ctrlr_loss_timeout`` ≈ 1s)
#: turn short peer blips into a destroy→reattach, which is exactly the
#: condition under which two callers can race on the same subnqn. Giving
#: the controller reset window enough room to absorb a short blip keeps
#: the controller alive and removes the opportunity for a race.
HUBLVOL_CTRLR_LOSS_TIMEOUT_SEC = 10
HUBLVOL_RECONNECT_DELAY_SEC = 2
HUBLVOL_FAST_IO_FAIL_TIMEOUT_SEC = 5


class HublvolReconnectError(Exception):
    """Raised when the coordinator gives up on a reconcile."""


def _lock_key(node_id, lvstore):
    return f"hublvol_lock/{node_id}/{lvstore}".encode()


def _now():
    return time.time()


def _try_acquire_tx(tr, key, token, ttl_sec):
    raw = tr.get(key).wait()
    now = _now()
    if raw.present():
        state = json.loads(bytes(raw).decode())
        if state.get("expires_at", 0) > now and state.get("token") != token:
            return False, state.get("last_attach_at", 0.0)
        last_attach_at = state.get("last_attach_at", 0.0)
    else:
        last_attach_at = 0.0
    new_state = {
        "token": token,
        "expires_at": now + ttl_sec,
        "last_attach_at": last_attach_at,
    }
    tr[key] = json.dumps(new_state).encode()
    return True, last_attach_at


def _stamp_attach_tx(tr, key, token, attach_at):
    raw = tr.get(key).wait()
    if not raw.present():
        return False
    state = json.loads(bytes(raw).decode())
    if state.get("token") != token:
        # Lock was stolen (TTL expired and another holder took over); don't
        # overwrite a stranger's state.
        return False
    state["last_attach_at"] = attach_at
    tr[key] = json.dumps(state).encode()
    return True


def _release_tx(tr, key, token):
    raw = tr.get(key).wait()
    if not raw.present():
        return
    state = json.loads(bytes(raw).decode())
    if state.get("token") != token:
        return  # not ours anymore
    # Keep last_attach_at across releases so future cooldown checks work;
    # drop token + expires_at so the next caller can acquire immediately.
    del state["token"]
    state["expires_at"] = 0
    tr[key] = json.dumps(state).encode()


def _run_txn(kv_store, fn, *args):
    """Apply ``fn`` as an FDB transaction via ``fdb.transactional(fn)(...)``.

    Wrapping at call time mirrors the pattern used elsewhere (see
    ``DBController._acquire_backup_chain_locks_tx``); decorating at
    module import would force ``fdb.api_version()`` to have been called
    at import time, which is not guaranteed for short-lived tooling or
    test processes.
    """
    return fdb.transactional(fn)(kv_store, *args)


class _HublvolLock:
    """Context manager around the hublvol advisory lock.

    Cross-process semantics come from an FDB-backed record when ``kv_store``
    is available. If ``kv_store`` is None (unit tests, early bootstrap,
    or environments without FDB) the class falls back to a per-key
    ``threading.Lock`` plus a module-level ``last_attach_at`` map so the
    coordinator's single-writer invariant still holds within the process.

    Exposes ``last_attach_at`` (read when the lock was acquired) and
    ``stamp_attach()`` (to be called on a successful attach so subsequent
    callers can honor the cooldown).
    """

    def __init__(self, kv_store, node_id, lvstore,
                 ttl_sec=DEFAULT_LOCK_TTL_SEC,
                 acquire_timeout_sec=30.0):
        self._kv = kv_store
        self._key = _lock_key(node_id, lvstore)
        self._ttl = ttl_sec
        self._acquire_timeout = acquire_timeout_sec
        self._token = uuid.uuid4().hex
        self._process_lock = None  # set in __enter__ when kv_store is None
        self.last_attach_at = 0.0

    def __enter__(self):
        if self._kv is None:
            return self._enter_process_local()
        deadline = time.monotonic() + self._acquire_timeout
        while True:
            acquired, last_attach = _run_txn(
                self._kv, _try_acquire_tx,
                self._key, self._token, self._ttl)
            if acquired:
                self.last_attach_at = last_attach
                return self
            if time.monotonic() >= deadline:
                raise HublvolReconnectError(
                    f"timed out acquiring hublvol lock {self._key!r}")
            time.sleep(0.1)

    def _enter_process_local(self):
        self._process_lock = _get_process_lock(self._key)
        if not self._process_lock.acquire(timeout=self._acquire_timeout):
            raise HublvolReconnectError(
                f"timed out acquiring in-process hublvol lock {self._key!r}")
        state = _process_local_state.get(self._key) or {}
        self.last_attach_at = state.get("last_attach_at", 0.0)
        return self

    def stamp_attach(self, attach_at=None):
        attach_at = _now() if attach_at is None else attach_at
        if self._kv is None:
            _process_local_state[self._key] = {"last_attach_at": attach_at}
        else:
            _run_txn(self._kv, _stamp_attach_tx,
                     self._key, self._token, attach_at)
        self.last_attach_at = attach_at

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._kv is None:
                if self._process_lock is not None:
                    self._process_lock.release()
            else:
                _run_txn(self._kv, _release_tx, self._key, self._token)
        except Exception as e:  # pragma: no cover
            # Release must never raise from __exit__; TTL will recover.
            logger.warning("Failed to release hublvol lock %s: %s", self._key, e)
        return False


def _ctrlrs_from_list(rpc, ctrl_name):
    """Return the list of controller paths for ``ctrl_name`` or ``[]``."""
    ret = rpc.bdev_nvme_controller_list(ctrl_name)
    if not ret:
        return []
    # ret is ``[{'name': ctrl_name, 'ctrlrs': [...], ...}]``
    return ret[0].get("ctrlrs", []) if ret else []


def _detach_and_wait_gone(rpc, ctrl_name, timeout_sec=DEFAULT_DETACH_WAIT_SEC):
    """Detach the controller and poll until SPDK reports it absent.

    Returns True on clean teardown, False on timeout. Swallows detach
    errors (the controller may already be gone / partially destroyed).
    """
    try:
        rpc.bdev_nvme_detach_controller(ctrl_name)
    except Exception as e:
        logger.debug("detach %s raised (may already be gone): %s", ctrl_name, e)
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not _ctrlrs_from_list(rpc, ctrl_name):
            return True
        time.sleep(0.1)
    return False


def _wait_for_settled(rpc, ctrl_name, timeout_sec=DEFAULT_TRANSIENT_WAIT_SEC):
    """Wait for any transient (``resetting``/``connecting``) paths to settle.

    Returns the final ctrlr list. Does not tear down — the caller decides
    whether the settled state is acceptable or needs a detach.
    """
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        ctrlrs = _ctrlrs_from_list(rpc, ctrl_name)
        if not ctrlrs:
            return []
        if all(c.get("state") not in ("resetting", "connecting") for c in ctrlrs):
            return ctrlrs
        time.sleep(0.2)
    return _ctrlrs_from_list(rpc, ctrl_name)


def _attached_ips(ctrlrs):
    ips = set()
    for ct in ctrlrs:
        trid = ct.get("trid") or {}
        if trid.get("traddr"):
            ips.add(trid["traddr"])
        for alt in ct.get("alternate_trids", []) or []:
            if alt.get("traddr"):
                ips.add(alt["traddr"])
    return ips


def _expected_ips_for_peer(peer):
    """IPs to attach for ``peer``, honoring the peer's active transport flag."""
    ips = []
    for iface in peer.data_nics or []:
        if peer.active_rdma and iface.trtype == "RDMA":
            ips.append((iface.ip4_address, "RDMA"))
        elif not peer.active_rdma and peer.active_tcp and iface.trtype == "TCP":
            ips.append((iface.ip4_address, "TCP"))
    return ips


def _do_attach(rpc, ctrl_name, nqn, ip, port, trtype,
               multipath, attach_timeout_sec=1):
    """Single ``bdev_nvme_attach_controller`` with hublvol timeout tuning."""
    return rpc.bdev_nvme_attach_controller(
        ctrl_name, nqn, ip, port, trtype,
        multipath=multipath,
        ctrlr_loss_timeout_sec=HUBLVOL_CTRLR_LOSS_TIMEOUT_SEC,
        reconnect_delay_sec=HUBLVOL_RECONNECT_DELAY_SEC,
        fast_io_fail_timeout_sec=HUBLVOL_FAST_IO_FAIL_TIMEOUT_SEC,
    )


class HublvolReconnectCoordinator:
    """Single entry point for (re)attaching a hublvol NVMe-oF controller.

    Usage:

        coord = HublvolReconnectCoordinator(db_controller)
        coord.reconcile(node, primary_node, peer_nodes, role="tertiary")

    ``node``         — the node the attach is happening on (the secondary/
                       tertiary / fast-path caller).
    ``primary_node`` — the node whose hublvol subsystem we're attaching
                       to. Must have ``.hublvol.bdev_name``, ``.hublvol.nqn``
                       and ``.hublvol.nvmf_port`` populated.
    ``peer_nodes``   — nodes whose data-NIC IPs are expected paths on the
                       controller. Typically ``[primary_node]`` for the
                       secondary role, ``[primary_node, secondary_node]``
                       for the tertiary role.
    ``role``         — informational; only used for logging.
    """

    def __init__(self, db_controller,
                 cooldown_sec=DEFAULT_COOLDOWN_SEC,
                 lock_ttl_sec=DEFAULT_LOCK_TTL_SEC):
        self._db = db_controller
        self._cooldown = cooldown_sec
        self._lock_ttl = lock_ttl_sec

    def reconcile(self, node, primary_node, peer_nodes, role="secondary"):
        """Observe, then converge, the hublvol controller state.

        Returns True if the controller ends up with at least one
        ``enabled`` path covering every ``peer_node``, False otherwise.
        """
        if primary_node.hublvol is None:
            raise ValueError(
                f"primary node {primary_node.get_id()} has no hublvol")
        ctrl_name = primary_node.hublvol.bdev_name
        nqn = primary_node.hublvol.nqn
        port = primary_node.hublvol.nvmf_port

        expected = {}
        for peer in peer_nodes:
            for ip, trtype in _expected_ips_for_peer(peer):
                expected.setdefault(ip, trtype)
        if not expected:
            raise ValueError(
                f"no data-NIC IPs to attach for hublvol {ctrl_name} on "
                f"{node.get_id()}")

        with _HublvolLock(self._db.kv_store, node.get_id(),
                          primary_node.lvstore,
                          ttl_sec=self._lock_ttl) as lock:
            rpc = node.rpc_client()

            # 1. Cooldown: coalesce a second arrival inside the window.
            since = _now() - lock.last_attach_at
            if since < self._cooldown:
                ctrlrs = _ctrlrs_from_list(rpc, ctrl_name)
                if ctrlrs and all(c.get("state") == "enabled" for c in ctrlrs):
                    attached = _attached_ips(ctrlrs)
                    if set(expected).issubset(attached):
                        # Another caller just converged us; nothing to do.
                        return True
                # Otherwise sleep out the remaining cooldown so SPDK has
                # time to settle the prior attach before we poke it again.
                remaining = self._cooldown - since
                logger.debug(
                    "hublvol %s on %s inside cooldown (%.2fs left), "
                    "sleeping before reconcile",
                    ctrl_name, node.get_id(), remaining)
                time.sleep(remaining)

            # 2. Settle any transient state; don't tear down while SPDK is
            #    mid-reset — the reset may yet succeed.
            ctrlrs = _wait_for_settled(rpc, ctrl_name)

            # 3. Decide whether to tear down before re-attach.
            if ctrlrs and any(c.get("state") != "enabled" for c in ctrlrs):
                logger.info(
                    "hublvol %s on %s has non-enabled path(s): %s; "
                    "detaching before re-attach",
                    ctrl_name, node.get_id(),
                    [c.get("state") for c in ctrlrs])
                if not _detach_and_wait_gone(rpc, ctrl_name):
                    logger.error(
                        "hublvol %s on %s: detach-wait-gone timed out; "
                        "aborting reconcile",
                        ctrl_name, node.get_id())
                    return False
                ctrlrs = []

            # 4. Act.
            if not ctrlrs:
                ok = self._fresh_multipath_attach(
                    rpc, ctrl_name, nqn, port, expected, node, role)
            else:
                # Already enabled — top up any missing peer paths. Adding
                # a path to an existing enabled controller is the intended
                # multipath extension and does not race with destroys.
                ok = self._add_missing_paths(
                    rpc, ctrl_name, nqn, port, expected, ctrlrs,
                    node, role)

            if ok:
                lock.stamp_attach()
            return ok

    def _fresh_multipath_attach(self, rpc, ctrl_name, nqn, port, expected,
                                node, role):
        any_attached = False
        for ip, trtype in expected.items():
            try:
                ret = _do_attach(rpc, ctrl_name, nqn, ip, port, trtype,
                                 multipath="multipath")
                if ret:
                    any_attached = True
                    logger.info(
                        "hublvol %s on %s (%s): attached path %s",
                        ctrl_name, node.get_id(), role, ip)
                else:
                    logger.warning(
                        "hublvol %s on %s: attach returned falsy for %s",
                        ctrl_name, node.get_id(), ip)
            except Exception as e:
                logger.warning(
                    "hublvol %s on %s: attach path %s raised: %s",
                    ctrl_name, node.get_id(), ip, e)
        if not any_attached:
            logger.error(
                "hublvol %s on %s: no path attached (expected=%s)",
                ctrl_name, node.get_id(), list(expected))
            return False
        # Verify the controller really came up.
        ctrlrs = _wait_for_settled(rpc, ctrl_name)
        return bool(ctrlrs) and any(c.get("state") == "enabled" for c in ctrlrs)

    def _add_missing_paths(self, rpc, ctrl_name, nqn, port, expected,
                           ctrlrs, node, role):
        attached = _attached_ips(ctrlrs)
        missing = {ip: trtype for ip, trtype in expected.items()
                   if ip not in attached}
        if not missing:
            return True
        logger.info(
            "hublvol %s on %s (%s): %d/%d paths present, adding %s",
            ctrl_name, node.get_id(), role,
            len(attached), len(expected), list(missing))
        added_any = False
        for ip, trtype in missing.items():
            try:
                if _do_attach(rpc, ctrl_name, nqn, ip, port, trtype,
                              multipath="multipath"):
                    added_any = True
            except Exception as e:
                logger.warning(
                    "hublvol %s on %s: add path %s raised: %s",
                    ctrl_name, node.get_id(), ip, e)
        return added_any or not missing
