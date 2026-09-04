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
    This cooldown is enforced via a DB-backed record (HubDetachCooldown), NOT local
    process memory: TasksRunnerLVolMigration and TasksRunnerBatchMigration are two
    separate processes, each running its own HubControllerManager instance, and a
    solo migration + a batch migration can both touch the same (src, tgt) hub at
    the same time. A purely in-memory cooldown would only be honored by whichever
    process happened to perform the detach, letting the other one re-attach inside
    the disconnect-handshake window it never saw.

Instantiation
-------------
There is deliberately no module-level singleton here. Each task-runner process
(tasks_runner_lvol_migration.py, tasks_runner_batch_migration.py) constructs its
own HubControllerManager once, near the top of the module, right next to that
process's own DBController instance — see the `hub_manager = HubControllerManager(db)`
line in each. Do not add one here: this module used to construct one at import
time, which meant simply importing this module (even just to reuse an unrelated
helper) silently created another live manager sharing no state with the real one.

Public API (called by the task runners)
----------------------------------------
  hub_manager.acquire(src_node_id, src_rpc, tgt_node, trtype)
      → attach (or reuse) the controller, refresh the idle timer
      → returns (ctrl_name, hub_bdev, error)

  hub_manager.detach_now(src_node_id, tgt_node_id, src_rpc=None)
      → immediate detach (failure / cancel path), records detach timestamp
"""

import threading
import time
from typing import Optional

from simplyblock_core import utils
from simplyblock_core.models.hub_cooldown import HubDetachCooldown
from simplyblock_core.rpc_client import RPCException

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

    Constructed explicitly, once, by each task-runner process (see this
    module's docstring) — never a module-level singleton. The `_entries`
    idle-controller cache below is process-local by design (each process
    only needs to know what IT has attached), but the detach cooldown is
    persisted via `HubDetachCooldown` (models/hub_cooldown.py) so it is
    honored across every process that might race an attach against it.
    """

    # Minimum seconds between issuing a detach RPC and allowing any re-attach.
    # Must be long enough for the NVMe TCP disconnect handshake to complete.
    DETACH_COOLDOWN = 10

    # Seconds since the last acquire() before the GC triggers a detach.
    # Refreshed on every acquire() so concurrent migrations naturally keep
    # the controller alive without any reference counting.
    IDLE_TIMEOUT = 1200  # 20 minutes

    # GC sweep period.
    GC_INTERVAL = 30

    def __init__(self, db_controller=None):
        if db_controller is None:
            from simplyblock_core.db_controller import DBController
            db_controller = DBController()
        self._db = db_controller
        self._lock = threading.Lock()
        self._entries: dict = {}     # (src_node_id, tgt_node_id) → _HubEntry
        # Per-(src, tgt) lock, held across the (possibly slow) attach RPC so
        # concurrent acquire() calls for the SAME cold key serialize onto a
        # single attach instead of racing -- see acquire()'s docstring for
        # the incident this replaced. Keyed the same as _entries; created
        # lazily and never removed (bounded by the number of distinct
        # (src, tgt) pairs a process ever touches, i.e. node-pair count).
        self._attach_locks: dict = {}
        self._gc_thread: Optional[threading.Thread] = None

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

        Concurrency: the controller name (``tgt_node.transfer_hublvol.bdev_name``)
        is fixed per target node -- every caller attaching to the same (src, tgt)
        pair gets the identical name, there is no such thing as "my own"
        controller to tell apart from anyone else's. A cold key (no cached
        entry yet) therefore serializes on ``_attach_locks[key]`` across this
        whole check-attach-store sequence, so only ONE caller ever issues the
        attach RPC; every other concurrent caller for the same key simply
        blocks and then reuses the resulting entry. This replaced an earlier
        version that raced the attach outside any lock and had the losing
        caller "clean up its duplicate" by detaching `ctrl_name` -- since that
        name is shared, the loser was actually detaching the WINNER's live,
        already-in-use controller out from under whichever sibling caller got
        there first (incident 2026-08-31: a 5-member batch group's transfers
        collapsed massively when this fired mid-transfer). Detaching a hub is
        now something ONLY the GC thread's idle-timeout sweep ever does; no
        caller-facing code path issues bdev_nvme_detach_controller itself.
        """
        key = (src_node_id, tgt_node.get_id())

        entry = self._try_reuse(key, src_rpc)
        if entry is not None:
            return entry.ctrl_name, entry.hub_bdev, None

        # Cold (or just-invalidated) key: serialize the attach per-key so
        # concurrent callers for this SAME pair wait for one attach instead of
        # racing their own. Different keys never contend with each other.
        attach_lock = self._get_attach_lock(key)
        with attach_lock:
            # Re-check: whoever held the lock before us may have just attached.
            entry = self._try_reuse(key, src_rpc)
            if entry is not None:
                return entry.ctrl_name, entry.hub_bdev, None

            cooldown_remaining = self._cooldown_remaining(key)
            if cooldown_remaining > 0:
                msg = (
                    f"HUB_COOLDOWN: {cooldown_remaining:.1f}s remaining after last detach "
                    f"(src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]})"
                )
                logger.info(f"[HubMgr] {msg}")
                return None, None, msg

            ctrl_name, hub_bdev, err = self._attach(src_rpc, tgt_node, trtype)
            if err:
                return None, None, err

            entry = _HubEntry(ctrl_name, hub_bdev, src_rpc, tgt_node)
            with self._lock:
                self._entries[key] = entry
                self._ensure_gc_running()

        logger.info(
            f"[HubMgr] attached {ctrl_name} "
            f"src={src_node_id[:8]} tgt={tgt_node.get_id()[:8]}"
        )
        return ctrl_name, hub_bdev, None

    def _try_reuse(self, key, src_rpc) -> Optional["_HubEntry"]:
        """Return the cached entry for *key* if it's still live, refreshing its
        idle timer. Drops (and returns None for) a stale entry that no longer
        validates -- the caller then falls through to a fresh attach."""
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
        try:
            if src_rpc.get_bdevs(entry.hub_bdev):
                with self._lock:
                    entry.last_used = time.monotonic()
                    entry.src_rpc = src_rpc
                logger.info(
                    f"[HubMgr] reusing {entry.ctrl_name} "
                    f"src={key[0][:8]} tgt={key[1][:8]}"
                )
                return entry
        except RPCException:
            logger.exception(
                f"[HubMgr] failed to validate cached hub entry {entry.ctrl_name}; "
                "treating as stale and re-attaching"
            )
        logger.info(f"[HubMgr] stale entry for {entry.ctrl_name}; re-attaching")
        with self._lock:
            if self._entries.get(key) is entry:
                del self._entries[key]
        return None

    def _get_attach_lock(self, key) -> threading.Lock:
        with self._lock:
            return self._attach_locks.setdefault(key, threading.Lock())

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

    @staticmethod
    def _pair_key(key) -> str:
        return f"{key[0]}:{key[1]}"

    def _cooldown_remaining(self, key) -> float:
        """Return seconds remaining in the post-detach cooldown (0 if none).

        Reads the DB-backed HubDetachCooldown record rather than local state
        — the most recent detach for this (src, tgt) pair may have been
        issued by the OTHER task-runner process's HubControllerManager.
        """
        try:
            records = HubDetachCooldown().read_from_db(self._db.kv_store, id=self._pair_key(key))
        except Exception:
            logger.exception("[HubMgr] failed to read detach-cooldown record; assuming no cooldown")
            return 0.0
        if not records:
            return 0.0
        elapsed = time.time() - records[0].detach_ts
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
            # Wall-clock time.time(), not time.monotonic(): this record is
            # read by whichever process's HubControllerManager calls acquire()
            # next, which may not be this one.
            try:
                record = HubDetachCooldown()
                record.pair_key = self._pair_key(key)
                record.detach_ts = time.time()
                record.write_to_db(self._db.kv_store)
            except Exception:
                logger.exception(
                    f"[HubMgr] failed to persist detach-cooldown record for {self._pair_key(key)}"
                )

    @staticmethod
    def _attach(src_rpc, tgt_node, trtype: str):
        """
        Attach a hub controller on *src_rpc* to *tgt_node*'s migration hub subsystem.

        Returns (ctrl_name, hub_bdev, error).
        """
        hub_missing = tgt_node.transfer_hublvol is None or not tgt_node.transfer_hublvol.bdev_name
        if not hub_missing:
            # A transfer_hublvol DB record from an earlier migration can outlive
            # a node restart or cluster reactivation — SPDK's own bdev/subsystem
            # state is not persisted across that, only the DB record is. Trusting
            # the cached record alone leads every future attach attempt to fail
            # with "Invalid subsystem" against a hub that no longer exists on the
            # target. Verify liveness against the target's own SPDK before
            # skipping (re)creation — same subsystem_get-based check
            # _ensure_target_nvmf_state uses for the equivalent lvol-subsystem case.
            try:
                hub_missing = not tgt_node.rpc_client().subsystem_get(tgt_node.transfer_hublvol.nqn)
            except Exception as e:
                logger.warning(
                    f"[HubMgr] could not verify hub subsystem on {tgt_node.get_id()}, "
                    f"assuming missing: {e}"
                )
                hub_missing = True

        if hub_missing:
            # create_transfer_hublvol() itself re-checks live existence, creates
            # the bdev + subsystem/listener only if actually missing, and persists
            # the (possibly new) bdev_name/uuid back to the DB record.
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

                # No local detach-timestamp dict to prune anymore — that state
                # now lives in the DB-backed HubDetachCooldown record, one row
                # per (src, tgt) pair, which is simply overwritten on the next
                # detach rather than accumulating.

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
