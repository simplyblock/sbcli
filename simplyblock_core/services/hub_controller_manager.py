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

  • Activity-based lifetime — the controller stays alive as long as any migration
    calls acquire() within IDLE_TIMEOUT seconds.  No reference counting needed;
    each acquire() simply refreshes the last-used timestamp.

  • Cooldown enforcement — a minimum DETACH_COOLDOWN gap is maintained between
    the moment a detach RPC is issued and any subsequent re-attach attempt.  If
    acquire() is called during the cooldown window it returns a transient error so
    the task runner retries on the next loop iteration (~3 s) until the gap passes.

Public API (called by tasks_runner_lvol_migration)
--------------------------------------------------
  hub_manager.acquire(src_node_id, src_rpc, tgt_node, trtype)
      → attach (or reuse) the controller, refresh the idle timer
      → returns (ctrl_name, hub_bdev, error)

  hub_manager.release(src_node_id, tgt_node_id)
      → no-op kept for call-site compatibility; idle timer is driven by acquire()

  hub_manager.detach_now(src_node_id, tgt_node_id, src_rpc=None)
      → immediate detach (failure / cancel path), records detach timestamp
"""

import threading
import time

from simplyblock_core import utils

logger = utils.get_logger(__name__)


class _HubEntry:
    __slots__ = ('ctrl_name', 'hub_bdev', 'src_rpc', 'tgt_node', 'last_used')

    def __init__(self, ctrl_name: str, hub_bdev: str, src_rpc, tgt_node):
        self.ctrl_name = ctrl_name
        self.hub_bdev  = hub_bdev    # e.g. "mighubXXXXXXXXn1"
        self.src_rpc   = src_rpc     # refreshed on every acquire — used by GC
        self.tgt_node  = tgt_node
        self.last_used = time.monotonic()


class HubControllerManager:
    """
    Thread-safe lifecycle manager for migration hub NVMe-oF controllers.

    One instance (``hub_manager``) is created at module level and shared by
    the entire migration task runner process.
    """

    # Minimum seconds between issuing a detach RPC and allowing any re-attach.
    # Must be long enough for the NVMe TCP disconnect handshake to complete.
    DETACH_COOLDOWN = 10

    # Seconds since the last acquire() before the GC triggers a detach.
    # Refreshed on every acquire() so concurrent migrations naturally keep
    # the controller alive without any reference counting.
    IDLE_TIMEOUT = 300  # 5 minutes

    # GC sweep period.
    GC_INTERVAL = 30

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

        Refreshes the idle timer so the controller stays alive as long as
        migrations are actively using it.

        Returns (ctrl_name, hub_bdev, error).  On transient cooldown the error
        string starts with "HUB_COOLDOWN:" so callers can log it distinctly.
        """
        key = (src_node_id, tgt_node.get_id())

        with self._lock:
            entry = self._entries.get(key)
            if entry is not None:
                try:
                    if src_rpc.get_bdevs(entry.hub_bdev):
                        entry.last_used = time.monotonic()
                        entry.src_rpc = src_rpc
                        logger.info(
                            f"[HubMgr] reusing {entry.ctrl_name} "
                            f"src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]}"
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
                        existing.last_used = time.monotonic()
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
                    if (now - e.last_used) >= self.IDLE_TIMEOUT
                ]
                evicted = [(key, self._entries.pop(key)) for key in to_evict]

                # Prune stale detach timestamps older than DETACH_COOLDOWN.
                stale = [k for k, ts in self._detach_ts.items()
                         if (now - ts) > self.DETACH_COOLDOWN * 2]
                for k in stale:
                    del self._detach_ts[k]

                should_exit = not self._entries

            for key, entry in evicted:
                logger.info(
                    f"[HubMgr] GC evicting {entry.ctrl_name} "
                    f"(idle {now - entry.last_used:.0f}s >= {self.IDLE_TIMEOUT}s)"
                )
                self._do_detach(key, entry, entry.src_rpc, "gc-idle")

            if should_exit:
                with self._lock:
                    self._gc_thread = None
                logger.info("[HubMgr] GC thread exiting (no active entries)")
                break   # restarted on next acquire()


# Module-level singleton — imported and used by tasks_runner_lvol_migration
hub_manager = HubControllerManager()
