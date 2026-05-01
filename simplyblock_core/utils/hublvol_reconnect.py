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

#: Mandatory sleep between successive attach RPCs against the same
#: ``ctrl_name`` within a single reconcile (e.g. multipath path-by-path
#: attach). Even when the prior attach reports its controller as enabled
#: in ``bdev_nvme_controller_list``, SPDK may still be finalising
#: per-controller state for a short while. A small extra gap absorbs
#: that finalisation window. Combined with ``_ensure_attach_ready``
#: this prevents the back-to-back attach race that produces
#: ``cntlid N are duplicated`` (LVS_5918 incident, 2026-04-25 12:47:18).
INTER_ATTACH_SLEEP_SEC = 3.0

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
               multipath, attach_timeout_sec=1, rpc_timeout=None):
    """Single ``bdev_nvme_attach_controller`` with hublvol timeout tuning.

    ``rpc_timeout`` (seconds) bounds the underlying SPDK RPC HTTP call.
    Used by the LVS rejoin to keep a single in-freeze attach under a hard
    sub-second budget; falls through to the rpc client's default when None.
    """
    return rpc.bdev_nvme_attach_controller(
        ctrl_name, nqn, ip, port, trtype,
        multipath=multipath,
        ctrlr_loss_timeout_sec=HUBLVOL_CTRLR_LOSS_TIMEOUT_SEC,
        reconnect_delay_sec=HUBLVOL_RECONNECT_DELAY_SEC,
        fast_io_fail_timeout_sec=HUBLVOL_FAST_IO_FAIL_TIMEOUT_SEC,
        request_timeout=rpc_timeout,
    )


def _ensure_attach_ready(rpc, ctrl_name, ip):
    """Inspect ``ctrl_name``'s current state and decide what to do for the
    next attach of ``ip`` (a single path).

    Returns one of:

      ``"skip"``   — controller exists, all paths are ``enabled``, and ``ip``
                     is already among the attached paths. Caller must NOT
                     issue another attach.
      ``"attach"`` — caller may proceed with ``bdev_nvme_attach_controller``
                     for this ``ip``: either the controller is absent (fresh
                     create) or it is enabled and missing this path
                     (multipath path-add).
      ``"failed"`` — controller is hung in a non-enabled terminal state and
                     could not be torn down. Caller must abort.

    Behavior matches the contract requested in the LVS_5918 follow-up:

      - enabled + path present  -> skip (no-op)
      - enabled + path missing  -> attach (path-add)
      - in transient state      -> wait for terminal state, then re-decide
      - absent                  -> attach (fresh create)
      - hung in non-enabled     -> detach-and-wait-gone, then attach
    """
    ctrlrs = _ctrlrs_from_list(rpc, ctrl_name)

    if not ctrlrs:
        return "attach"

    if all(c.get("state") == "enabled" for c in ctrlrs):
        return "skip" if ip in _attached_ips(ctrlrs) else "attach"

    # Some path is in resetting/connecting/other transient — wait for the
    # controller to converge to a terminal state before deciding.
    ctrlrs = _wait_for_settled(rpc, ctrl_name)
    if not ctrlrs:
        return "attach"
    if all(c.get("state") == "enabled" for c in ctrlrs):
        return "skip" if ip in _attached_ips(ctrlrs) else "attach"

    # Hangs in a terminal non-enabled state (e.g. failed reset, disconnected
    # without destroy). Tear down for a clean retry.
    logger.warning(
        "hublvol %s: controller hung in non-enabled state %s; "
        "detaching for clean retry",
        ctrl_name, [c.get("state") for c in ctrlrs])
    if not _detach_and_wait_gone(rpc, ctrl_name):
        return "failed"
    return "attach"


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

    def reconcile(self, node, primary_node, peer_nodes, role="secondary",
                  rpc_timeout=None):
        """Observe, then converge, the hublvol controller state.

        Returns True if the controller ends up with at least one
        ``enabled`` path covering every ``peer_node``, False otherwise.

        ``rpc_timeout`` (seconds) bounds each underlying SPDK
        ``bdev_nvme_attach_controller`` HTTP RPC. The LVS rejoin uses a
        sub-second value so a single in-freeze attach must land fast or
        abort fast. ``None`` (default) leaves the rpc client's own
        timeout in place — appropriate for post-freeze background
        reconciliation.
        """
        if primary_node.hublvol is None:
            raise ValueError(
                f"primary node {primary_node.get_id()} has no hublvol")
        ctrl_name = primary_node.hublvol.bdev_name
        nqn = primary_node.hublvol.nqn
        port = primary_node.hublvol.nvmf_port

        expected: dict[str, str] = {}
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
                    rpc, ctrl_name, nqn, port, expected, node, role,
                    rpc_timeout=rpc_timeout)
            else:
                # Already enabled — top up any missing peer paths. Adding
                # a path to an existing enabled controller is the intended
                # multipath extension and does not race with destroys.
                ok = self._add_missing_paths(
                    rpc, ctrl_name, nqn, port, expected, ctrlrs,
                    node, role, rpc_timeout=rpc_timeout)

            if ok:
                lock.stamp_attach()
            return ok

    def _fresh_multipath_attach(self, rpc, ctrl_name, nqn, port, expected,
                                node, role, rpc_timeout=None):
        return self._attach_paths_safely(
            rpc, ctrl_name, nqn, port, expected, node, role,
            verify_at_end=True, rpc_timeout=rpc_timeout)

    def _add_missing_paths(self, rpc, ctrl_name, nqn, port, expected,
                           ctrlrs, node, role, rpc_timeout=None):
        attached = _attached_ips(ctrlrs)
        missing = {ip: trtype for ip, trtype in expected.items()
                   if ip not in attached}
        if not missing:
            return True
        logger.info(
            "hublvol %s on %s (%s): %d/%d paths present, adding %s",
            ctrl_name, node.get_id(), role,
            len(attached), len(expected), list(missing))
        return self._attach_paths_safely(
            rpc, ctrl_name, nqn, port, missing, node, role,
            verify_at_end=False, rpc_timeout=rpc_timeout)

    def _attach_paths_safely(self, rpc, ctrl_name, nqn, port, paths,
                             node, role, verify_at_end, rpc_timeout=None):
        """Drive ``bdev_nvme_attach_controller`` for each ``(ip, trtype)`` in
        ``paths`` against ``ctrl_name``, applying:

          1. State-aware gating: before each attach, consult
             ``_ensure_attach_ready`` so an already-enabled path is a no-op
             and a hung controller is detached for a clean retry.
          2. A mandatory inter-attach sleep so SPDK has a window to
             finalise per-controller state between back-to-back path
             attaches in a single multipath fan-out. This protects against
             the race that produces ``cntlid N are duplicated`` even when
             the previous attach has reported the controller as registered.

        Foreground/background split: as soon as one path is in the desired
        enabled state, the remaining redundant paths are completed
        asynchronously in a daemon thread that still honours
        ``INTER_ATTACH_SLEEP_SEC`` (so the cntlid-duplicated race is still
        avoided), and we return ``True`` immediately. The caller — typically
        the failback flow on a non-leader replica — can then proceed to
        ``bdev_lvol_set_lvs_opts`` / ``bdev_lvol_connect_hublvol`` and the
        port-unblock signal without waiting for redundant-path setup.

        If the very first path fails, we fall through synchronously to the
        next path (still respecting the inter-attach sleep). Only when
        every path tried in the foreground fails do we return ``False``
        and abort the restart.

        Returns True if at least one path is in the desired enabled state
        after the loop. With ``verify_at_end=True`` (fresh create case), a
        final ``_wait_for_settled`` confirms that path is actually up.
        """
        last_attach_at = 0.0
        # Always use ``"multipath"``: SPDK cannot widen a non-multipath
        # controller to multipath after the fact (bdev_nvme.c:5849 returns
        # -EINVAL), so even a single-path attach (secondary's hublvol, or
        # a tertiary's first-path attach with the failover deferred) must
        # start in multipath mode — otherwise the deferred failover-path
        # add would need a detach+reattach, which reopens the
        # ``cntlid duplicated`` race the coordinator was built to close.
        attach_mode = "multipath"
        paths_list = list(paths.items())
        for i, (ip, trtype) in enumerate(paths_list):
            now = time.monotonic()
            since = now - last_attach_at
            if last_attach_at and since < INTER_ATTACH_SLEEP_SEC:
                time.sleep(INTER_ATTACH_SLEEP_SEC - since)

            decision = _ensure_attach_ready(rpc, ctrl_name, ip)
            if decision == "failed":
                logger.error(
                    "hublvol %s on %s (%s): cannot prepare controller for "
                    "path %s, trying next",
                    ctrl_name, node.get_id(), role, ip)
                last_attach_at = time.monotonic()
                continue
            if decision == "skip":
                logger.debug(
                    "hublvol %s on %s (%s): path %s already enabled, skip",
                    ctrl_name, node.get_id(), role, ip)
                if verify_at_end:
                    ctrlrs = _wait_for_settled(rpc, ctrl_name)
                    ok = bool(ctrlrs) and any(
                        c.get("state") == "enabled" for c in ctrlrs)
                else:
                    ok = True
                if ok:
                    self._defer_remaining_attaches(
                        rpc, ctrl_name, nqn, port, paths_list[i + 1:],
                        attach_mode, last_attach_at, node, role, rpc_timeout)
                return ok

            try:
                ret = _do_attach(rpc, ctrl_name, nqn, ip, port, trtype,
                                 multipath=attach_mode,
                                 rpc_timeout=rpc_timeout)
                last_attach_at = time.monotonic()
                if ret:
                    remaining = paths_list[i + 1:]
                    logger.info(
                        "hublvol %s on %s (%s): attached path %s; deferring "
                        "%d redundant path(s) to background",
                        ctrl_name, node.get_id(), role, ip, len(remaining))
                    if verify_at_end:
                        ctrlrs = _wait_for_settled(rpc, ctrl_name)
                        ok = bool(ctrlrs) and any(
                            c.get("state") == "enabled" for c in ctrlrs)
                    else:
                        ok = True
                    # Defer redundant paths AFTER verify_at_end so the
                    # background thread doesn't race with the verify on
                    # the same controller-list RPC.
                    if ok:
                        self._defer_remaining_attaches(
                            rpc, ctrl_name, nqn, port, remaining,
                            attach_mode, last_attach_at, node, role,
                            rpc_timeout)
                    return ok
                logger.warning(
                    "hublvol %s on %s: attach returned falsy for %s, "
                    "trying next path",
                    ctrl_name, node.get_id(), ip)
            except Exception as e:
                last_attach_at = time.monotonic()
                logger.warning(
                    "hublvol %s on %s: attach path %s raised: %s, "
                    "trying next path",
                    ctrl_name, node.get_id(), ip, e)

        logger.error(
            "hublvol %s on %s: no path attached (expected=%s)",
            ctrl_name, node.get_id(), [p[0] for p in paths_list])
        return False

    def _defer_remaining_attaches(self, rpc, ctrl_name, nqn, port,
                                  remaining, attach_mode, last_attach_at,
                                  node, role, rpc_timeout):
        """Run redundant-path attaches in a daemon thread.

        Called once the foreground loop has secured at least one path. The
        remaining paths still respect ``INTER_ATTACH_SLEEP_SEC`` (relative
        to the last foreground attach) so the cntlid-duplicated race stays
        closed, but the wait happens off the failback critical path. The
        caller can return immediately and proceed to
        ``bdev_lvol_connect_hublvol`` / port-unblock.

        The background thread logs success/failure per path and never
        propagates exceptions.
        """
        if not remaining:
            return

        node_id = node.get_id()

        def _worker():
            local_last = last_attach_at
            for ip, trtype in remaining:
                now = time.monotonic()
                since = now - local_last
                if local_last and since < INTER_ATTACH_SLEEP_SEC:
                    time.sleep(INTER_ATTACH_SLEEP_SEC - since)

                try:
                    decision = _ensure_attach_ready(rpc, ctrl_name, ip)
                except Exception as e:
                    local_last = time.monotonic()
                    logger.warning(
                        "hublvol %s on %s (%s) bg: ensure_attach_ready "
                        "raised for path %s: %s",
                        ctrl_name, node_id, role, ip, e)
                    continue

                if decision == "failed":
                    local_last = time.monotonic()
                    logger.warning(
                        "hublvol %s on %s (%s) bg: cannot prepare path %s",
                        ctrl_name, node_id, role, ip)
                    continue
                if decision == "skip":
                    logger.debug(
                        "hublvol %s on %s (%s) bg: path %s already enabled",
                        ctrl_name, node_id, role, ip)
                    continue

                try:
                    ret = _do_attach(rpc, ctrl_name, nqn, ip, port, trtype,
                                     multipath=attach_mode,
                                     rpc_timeout=rpc_timeout)
                    local_last = time.monotonic()
                    if ret:
                        logger.info(
                            "hublvol %s on %s (%s) bg: attached redundant "
                            "path %s",
                            ctrl_name, node_id, role, ip)
                    else:
                        logger.warning(
                            "hublvol %s on %s (%s) bg: attach returned "
                            "falsy for %s",
                            ctrl_name, node_id, role, ip)
                except Exception as e:
                    local_last = time.monotonic()
                    logger.warning(
                        "hublvol %s on %s (%s) bg: attach path %s raised: %s",
                        ctrl_name, node_id, role, ip, e)

        threading.Thread(
            target=_worker,
            name=f"hublvol-bg-attach-{ctrl_name}",
            daemon=True,
        ).start()
