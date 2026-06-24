# coding=utf-8
"""
hub_controller_manager.py — lifecycle manager for migration hub NVMe-oF controllers.

Goal
----
A migration hub controller (mighub*n1 bdev on the source SPDK) connects source to
target's dedicated transfer-hub subsystem.  Two problems arise when controllers are
detached naively:

  1. Concurrent migrations: migration A finishes and detaches the controller while
     migration B (to the same target) is still running and still needs it.

  2. Deleting-state race: bdev_nvme_detach_controller triggers an async NVMe
     disconnect handshake.  If a new attach arrives before the handshake completes
     the controller is in 'deleting' state and the attach fails.

This module solves both:

  • Reference counting — controller is only detached when ALL migrations using it
    have finished (refcount == 0) AND it has been idle for IDLE_TIMEOUT seconds.

  • Cooldown enforcement — a minimum DETACH_COOLDOWN gap is maintained between
    the moment a detach RPC is issued and any subsequent re-attach attempt.  If
    acquire() is called during the cooldown window it returns a transient error so
    the task runner retries on the next loop iteration (~3 s) until the gap passes.

Public API (called by tasks_runner_lvol_migration)
--------------------------------------------------
  hub_manager.acquire(src_node_id, src_rpc, tgt_node, trtype)
      → attach (or reuse) the controller, increment refcount
      → returns (ctrl_name, hub_bdev, error)

  hub_manager.release(src_node_id, tgt_node_id)
      → decrement refcount, start idle timer — does NOT detach

  hub_manager.detach_now(src_node_id, tgt_node_id, src_rpc=None)
      → immediate detach (failure / cancel path), records detach timestamp
"""

import threading
import time

from simplyblock_core import utils

logger = utils.get_logger(__name__)


class _HubEntry:
    __slots__ = ('ctrl_name', 'hub_bdev', 'src_rpc', 'tgt_node',
                 'refcount', 'last_release')

    def __init__(self, ctrl_name: str, hub_bdev: str, src_rpc, tgt_node):
        self.ctrl_name    = ctrl_name
        self.hub_bdev     = hub_bdev    # e.g. "mighubXXXXXXXXn1"
        self.src_rpc      = src_rpc     # refreshed on every acquire — used by GC
        self.tgt_node     = tgt_node
        self.refcount     = 1
        self.last_release = 0.0         # monotonic ts when refcount last hit 0


class HubControllerManager:
    """
    Thread-safe lifecycle manager for migration hub NVMe-oF controllers.

    One instance (``hub_manager``) is created at module level and shared by
    the entire migration task runner process.
    """

    # Minimum seconds between issuing a detach RPC and allowing any re-attach.
    # Must be long enough for the NVMe TCP disconnect handshake to complete.
    DETACH_COOLDOWN = 10

    # Seconds with refcount == 0 before the GC triggers a detach.
    # Setting this well above DETACH_COOLDOWN means back-to-back migrations
    # typically reuse the existing controller without going through detach+reattach.
    IDLE_TIMEOUT = 60

    # GC sweep period.
    GC_INTERVAL = 15

    def __init__(self):
        self._lock = threading.Lock()
        self._entries: dict = {}     # (src_node_id, tgt_node_id) → _HubEntry
        self._detach_ts: dict = {}   # (src_node_id, tgt_node_id) → monotonic ts of last detach
        self._gc_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def acquire(self, src_node_id: str, src_rpc, tgt_node, trtype: str):
        """
        Ensure the hub controller from *src_node_id* to *tgt_node* is attached.

        Returns (ctrl_name, hub_bdev, error).  On transient cooldown the error
        string starts with "HUB_COOLDOWN:" so callers can log it distinctly.
        """
        key = (src_node_id, tgt_node.get_id())

        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                try:
                    if src_rpc.get_bdevs(entry.hub_bdev):
                        entry.refcount += 1
                        entry.src_rpc = src_rpc
                        logger.info(
                            f"[HubMgr] reusing {entry.ctrl_name} "
                            f"src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]} "
                            f"refcount={entry.refcount}"
                        )
                        return entry.ctrl_name, entry.hub_bdev, None
                except Exception:
                    pass
                logger.info(f"[HubMgr] stale entry for {entry.ctrl_name}; re-attaching")
                del self._entries[key]

            # Enforce cooldown before any fresh attach
            cooldown_remaining = self._cooldown_remaining(key)
            if cooldown_remaining > 0:
                msg = (
                    f"HUB_COOLDOWN: {cooldown_remaining:.1f}s remaining after last detach "
                    f"(src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]})"
                )
                logger.info(f"[HubMgr] {msg}")
                return None, None, msg

        # Attach outside the lock so we don't block the GC thread
        ctrl_name, hub_bdev, err = self._attach(src_rpc, tgt_node, trtype)
        if err:
            return None, None, err

        with self._lock:
            # Another thread may have attached concurrently — prefer the winner
            existing = self._entries.get(key)
            if existing is not None:
                try:
                    if src_rpc.get_bdevs(existing.hub_bdev):
                        existing.refcount += 1
                        existing.src_rpc = src_rpc
                        try:
                            src_rpc.bdev_nvme_detach_controller(ctrl_name)
                        except Exception:
                            pass
                        return existing.ctrl_name, existing.hub_bdev, None
                except Exception:
                    pass

            entry = _HubEntry(ctrl_name, hub_bdev, src_rpc, tgt_node)
            self._entries[key] = entry
            self._ensure_gc_running()

        logger.info(
            f"[HubMgr] attached {ctrl_name} "
            f"src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]}"
        )
        return ctrl_name, hub_bdev, None

    def release(self, src_node_id: str, tgt_node_id: str):
        """
        Signal that one migration using this hub has finished.

        Decrements refcount and starts the idle timer.  Does NOT detach —
        the GC thread handles detach after IDLE_TIMEOUT seconds.
        """
        key = (src_node_id, tgt_node_id)
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return
            entry.refcount = max(0, entry.refcount - 1)
            if entry.refcount == 0:
                entry.last_release = time.monotonic()
                logger.info(
                    f"[HubMgr] released {entry.ctrl_name} "
                    f"src={src_node_id[:8]} tgt={tgt_node_id[:8]} — "
                    f"idle timer started (detach in ~{self.IDLE_TIMEOUT}s if not reused)"
                )

    def detach_now(self, src_node_id: str, tgt_node_id: str, src_rpc=None):
        """
        Immediately detach the hub controller (failure / cancel path).

        Records the detach timestamp so subsequent acquire() calls observe
        the DETACH_COOLDOWN gap.
        """
        key = (src_node_id, tgt_node_id)
        with self._lock:
            entry = self._entries.pop(key, None)
        if entry is None:
            return
        rpc = src_rpc or entry.src_rpc
        self._do_detach(key, entry, rpc, reason="detach_now")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _cooldown_remaining(self, key) -> float:
        """Return seconds remaining in the post-detach cooldown (0 if none)."""
        ts = self._detach_ts.get(key, 0)
        if ts == 0:
            return 0.0
        elapsed = time.monotonic() - ts
        return max(0.0, self.DETACH_COOLDOWN - elapsed)

    def _do_detach(self, key, entry: _HubEntry, rpc, reason: str):
        """Issue the detach RPC and record the detach timestamp."""
        try:
            if rpc.get_bdevs(entry.hub_bdev):
                rpc.bdev_nvme_detach_controller(entry.ctrl_name)
                logger.info(f"[HubMgr] detached {entry.ctrl_name} ({reason})")
            else:
                logger.info(
                    f"[HubMgr] {entry.ctrl_name} bdev already gone ({reason}); skipping detach"
                )
        except Exception as e:
            logger.warning(f"[HubMgr] detach {entry.ctrl_name} ({reason}) non-fatal: {e}")
        finally:
            # Record timestamp regardless of whether the RPC succeeded — the
            # controller state is unknown so enforce the cooldown either way.
            with self._lock:
                self._detach_ts[key] = time.monotonic()

    @staticmethod
    def _attach(src_rpc, tgt_node, trtype: str):
        """
        Attach a hub controller on *src_rpc* to *tgt_node*'s migration hub subsystem.

        Returns (ctrl_name, hub_bdev, error).
        """
        if tgt_node.transfer_hublvol is None or not tgt_node.transfer_hublvol.bdev_name:
            tgt_node.create_transfer_hublvol()

        # Already attached (crash recovery or concurrent acquire)
        if src_rpc.get_bdevs(tgt_node.transfer_hublvol.get_remote_bdev_name()):
            return (
                tgt_node.transfer_hublvol.bdev_name,
                tgt_node.transfer_hublvol.get_remote_bdev_name(),
                None,
            )

        attached = False
        for iface in tgt_node.data_nics:
            ip = iface.ip4_address
            if tgt_node.active_rdma:
                if iface.trtype != "RDMA":
                    continue
                nic_trtype = "RDMA"
            else:
                if iface.trtype != "TCP":
                    continue
                nic_trtype = "TCP"

            ret = src_rpc.bdev_nvme_attach_controller(
                tgt_node.transfer_hublvol.bdev_name,
                tgt_node.transfer_hublvol.nqn,
                ip,
                tgt_node.transfer_hublvol.nvmf_port,
                nic_trtype,
            )
            if not ret:
                if src_rpc.bdev_nvme_controller_list(tgt_node.transfer_hublvol.bdev_name):
                    logger.info("[HubMgr] zombie controller found; detaching and reattaching")
                    src_rpc.bdev_nvme_detach_controller(tgt_node.transfer_hublvol.bdev_name)
                    try:
                        tgt_node.create_transfer_hublvol()
                    except Exception as e:
                        logger.warning(f"[HubMgr] hub subsystem re-create (non-fatal): {e}")
                    ret = src_rpc.bdev_nvme_attach_controller(
                        tgt_node.transfer_hublvol.bdev_name,
                        tgt_node.transfer_hublvol.nqn,
                        ip,
                        tgt_node.transfer_hublvol.nvmf_port,
                        nic_trtype,
                    )
                if not ret:
                    return None, None, (
                        f"Failed to attach migration hub controller to {tgt_node.get_id()}"
                    )
            attached = True
            break

        if not attached:
            return None, None, f"No suitable NIC found on target node {tgt_node.get_id()}"

        return (
            tgt_node.transfer_hublvol.bdev_name,
            tgt_node.transfer_hublvol.get_remote_bdev_name(),
            None,
        )

    def _ensure_gc_running(self):
        """Start the GC daemon thread if not already running. Must be called under self._lock."""
        if self._gc_thread is None or not self._gc_thread.is_alive():
            self._gc_thread = threading.Thread(
                target=self._gc_loop, name="HubMgrGC", daemon=True
            )
            self._gc_thread.start()

    def _gc_loop(self):
        logger.info("[HubMgr] GC thread started")
        while True:
            time.sleep(self.GC_INTERVAL)
            now = time.monotonic()

            with self._lock:
                to_evict = [
                    key
                    for key, e in self._entries.items()
                    if e.refcount == 0 and (now - e.last_release) >= self.IDLE_TIMEOUT
                ]
                evicted = [(key, self._entries.pop(key)) for key in to_evict]

                # Prune stale detach timestamps older than DETACH_COOLDOWN — no
                # longer needed once the cooldown window has passed.
                stale = [k for k, ts in self._detach_ts.items()
                         if (now - ts) > self.DETACH_COOLDOWN * 2]
                for k in stale:
                    del self._detach_ts[k]

                if not self._entries:
                    logger.info("[HubMgr] GC thread exiting (no active entries)")
                    self._gc_thread = None
                    break   # restarted on next acquire()

            for key, entry in evicted:
                logger.info(
                    f"[HubMgr] GC evicting {entry.ctrl_name} "
                    f"(idle {now - entry.last_release:.0f}s >= {self.IDLE_TIMEOUT}s)"
                )
                self._do_detach(key, entry, entry.src_rpc, "gc-idle")


# Module-level singleton — imported and used by tasks_runner_lvol_migration
hub_manager = HubControllerManager()
