"""
Mass-creation/deletion stress test for lvols, snapshots, and clones.

Tests system behavior under high object counts across different
namespace-to-subsystem ratios. All lvols are pinned to a single
storage node. Exercises the full lifecycle:
  1. Mass-create lvols (parallel, 20 threads, until limit)
  2. FIO on 10% of lvols (connect, format, run)
  3. Create 50 snapshots per lvol
  4. Delete lvols (free subsystem slots for clones — snapshots
     survive parent lvol deletion and remain valid for cloning)
  5. Mass-create clones from orphaned snapshots (until limit)
  6. FIO on 10% of clones (connect, format, run)
  7. Mass-delete all clones
  8. Mass-delete all snapshots

Lvols are deleted before clones are created to free subsystem slots,
since both lvols and clones occupy subsystem slots. Lvols can be
deleted even when they have snapshots — the snapshots become orphaned
but remain valid for cloning.

Four ratio configurations (NxM = N namespaces per subsystem × M subsystems):
  3000x1  — 3000 ns in 1 subsystem  = 3000 lvols
  300x10  — 300 ns in 10 subsystems = 3000 lvols
  30x100  — 30 ns in 100 subsystems = 3000 lvols
  1x500   — 1 ns in 500 subsystems  = 500 standalone lvols
            (if 500 subsystems doesn't fit, use 300x10 as largest test)

Docker and K8s variants for each ratio.

Invocation:
  # Docker
  python3 stress.py --testname MassCreateDelete_1x500_Docker --ndcs 2 --npcs 2

  # K8s
  python3 stress.py --testname MassCreateDelete_30x100_K8s --ndcs 2 --npcs 2 --run_k8s True
"""

from __future__ import annotations

import json as _json
import math
import os
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from logger_config import setup_logger
from utils.common_utils import sleep_n_sec

logger = setup_logger(__name__)


def _rand_seq(length: int = 8) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits,
                                  k=length - 1))
    return first + rest


# ─────────────────────────────────────────────────────────────────────────────
#  Shared mixin: config, 8-phase orchestration, batch helpers, summary
# ─────────────────────────────────────────────────────────────────────────────

class _MassCreateDeleteMixin:
    """Shared orchestration for mass-creation/deletion stress test."""

    # ── Scale (overridden per ratio class) ─────────────────────────────────
    # NxM = N namespaces per subsystem × M subsystems
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000
    LVOL_SIZE = "1G"
    PVC_SIZE = "1Gi"

    # ── Snapshot / clone ───────────────────────────────────────────────────
    SNAPSHOTS_PER_LVOL = 1
    FIO_SAMPLE_PERCENT = 10
    FIO_SAMPLE_MAX = 50             # absolute cap on sampled volumes

    # ── FIO (lightweight) ──────────────────────────────────────────────────
    FIO_IODEPTH = 1
    FIO_NUMJOBS = 1
    FIO_RUNTIME = 60            # 1 min per sampled volume
    FIO_SIZE = "800M"

    # ── Parallelism ────────────────────────────────────────────────────────
    MAX_WORKERS = 10
    BATCH_SIZE = 50
    SNAPSHOT_BATCH_SIZE = 200
    CLONE_MAX_WORKERS = 10
    DELETE_MAX_WORKERS = 10
    PARALLEL_PARENTS = 10       # concurrent parent subsystem child creation
    MAX_FAILURES = 500
    FIO_CONCURRENT = 20         # concurrent FIO job creations per batch
    FIO_BATCH_PAUSE = 30        # seconds between FIO job creation batches
    FIO_PHASE_TIMEOUT = 7200    # 2 hours max for FIO job creation phase

    # ── Phase timeouts (seconds) ───────────────────────────────────────────
    SNAPSHOT_PHASE_TIMEOUT = 14400   # 4 hours
    CLONE_PHASE_TIMEOUT = 7200       # 2 hours
    DELETE_PHASE_TIMEOUT = 3600      # 1 hour

    # ── Persistent retry mode ─────────────────────────────────────────────
    # Subclasses set PERSISTENT_RETRY = True to retry failed items until
    # all expected entities are created or a terminal error is hit.
    PERSISTENT_RETRY = False
    PERSISTENT_PHASE_TIMEOUT = 14400   # 4 hours per creation phase
    PERSISTENT_BACKOFF_SEC = 10        # sleep between retry rounds

    # ── Internal state ─────────────────────────────────────────────────────
    _lvol_registry: dict
    _snapshot_registry: dict
    _clone_registry: dict
    _fio_lvol_sample: set
    _fio_clone_sample: set
    _phase_durations: dict
    _metrics: dict

    def _init_mixin_state(self):
        self._lvol_registry = {}      # name -> {id, parent_name}
        self._snapshot_registry = {}  # snap_name -> {snap_id, parent_lvol}
        self._clone_registry = {}     # clone_name -> {clone_id, snap_name}
        self._fio_lvol_sample = set()
        self._fio_clone_sample = set()
        self._phase_durations = {}
        self._fio_lvol_threads = []
        self._fio_clone_threads = []
        self._soft_failures = []      # accumulate phase-level validation warnings
        self._metrics = {
            "lvols_created": 0,
            "snapshots_created": 0,
            "clones_created": 0,
            "lvols_deleted": 0,
            "snapshots_deleted": 0,
            "clones_deleted": 0,
            "fio_lvol_started": 0,
            "fio_clone_started": 0,
            "fio_lvol_failures": 0,
            "fio_clone_failures": 0,
        }
        self._retry_round_counts = {}   # phase_name -> round_count

    # ── Error detection (from continuous_parallel_lvol_snapshot_clone.py) ──

    def _is_max_lvols_error(self, exc):
        text = str(exc).lower()
        return (
            "max lvols reached" in text
            or "max_lvols" in text
            or "too many subsystems" in text
            or "max subsystems reached" in text
        )

    def _is_bdev_error(self, exc):
        return "failed to create bdev" in str(exc).lower()

    def _is_sync_deletion_error(self, exc):
        return "lvol sync deletion found" in str(exc).lower()

    def _is_capacity_error(self, exc):
        """Detect terminal capacity errors — the system cannot create more."""
        text = str(exc).lower()
        return (
            "no space" in text
            or "enospc" in text
            or "pool max size has reached" in text
            or "pool max lvol size" in text
            or "exceeded the max number of lvol" in text
        )

    def _is_terminal_error(self, exc):
        """Return True if retrying is pointless (limit or capacity hit)."""
        return self._is_max_lvols_error(exc) or self._is_capacity_error(exc)

    # ── Bulk verify / soft validation ────────────────────────────────────────

    def _bulk_verify_created(self, names, list_fn, label, timeout=600):
        """Poll list_fn() until all names appear or timeout.

        Returns (verified_count, name_to_id_dict) where name_to_id_dict
        maps verified names to their IDs from the list response.
        """
        deadline = time.monotonic() + timeout
        remaining = set(names)
        resolved = {}  # name -> id

        while remaining and time.monotonic() < deadline:
            try:
                current = list_fn()  # {name: id}
            except Exception as exc:
                self.logger.warning(
                    f"[{label}] list call failed: {exc}, retrying..."
                )
                sleep_n_sec(10)
                continue

            for name in list(remaining):
                if name in current:
                    resolved[name] = current[name]
                    remaining.discard(name)

            if remaining:
                self.logger.info(
                    f"[{label}] {len(resolved)} verified, "
                    f"{len(remaining)} remaining"
                )
                sleep_n_sec(10)

        if remaining:
            self.logger.warning(
                f"[{label}] {len(remaining)} items not found "
                f"after {timeout}s"
            )
        else:
            self.logger.info(
                f"[{label}] All {len(resolved)} items verified"
            )
        return len(resolved), resolved

    def _check_count(self, actual, expected, label, hard_min_pct=0.50):
        """Validate count against hard minimum and soft tolerance.

        If actual < hard_min_pct of expected, raises RuntimeError to stop
        the test immediately — proceeding with <50% of resources is
        meaningless and wastes compute.

        If actual < 90% of expected (but above hard min), logs a warning
        and appends to _soft_failures for end-of-test reporting.
        """
        if expected > 0 and actual < expected * hard_min_pct:
            raise RuntimeError(
                f"[{label}] only {actual}/{expected} created "
                f"({actual / expected:.1%}) — below "
                f"{hard_min_pct:.0%} hard minimum, aborting test"
            )
        tolerance = 0.10
        if expected > 0 and actual < expected * (1 - tolerance):
            msg = (
                f"[{label}] only {actual}/{expected} created "
                f"(below 10% tolerance)"
            )
            self.logger.warning(msg)
            self._soft_failures.append(msg)

    # ── Batch execution (from large_scale_lvol_stress.py) ──────────────────

    def _batch_exec(self, items, task_fn, op_name: str,
                    per_item_timeout: int = 600,
                    max_workers: int = None,
                    max_failures: int = None,
                    batch_size: int = None,
                    stop_on_max_lvols: bool = False):
        """Execute task_fn(item) for each item using ThreadPoolExecutor.

        Returns (success_count, failure_count).
        If stop_on_max_lvols is True, stops on "max lvols reached" error.
        Applies exponential backoff between batches when failure rate > 50%.
        """
        total = len(items)
        success = 0
        failures = 0
        workers = max_workers or self.MAX_WORKERS
        bs = batch_size or self.BATCH_SIZE
        max_fail = max_failures if max_failures is not None else self.MAX_FAILURES
        hit_limit = False
        consecutive_high_fail = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch_start in range(0, total, bs):
                if failures >= max_fail or hit_limit:
                    break

                batch = items[batch_start:batch_start + bs]
                futures = {}
                for item in batch:
                    f = executor.submit(task_fn, item)
                    futures[f] = item

                batch_ok = 0
                batch_fail = 0
                for f in as_completed(futures):
                    try:
                        f.result(timeout=per_item_timeout)
                        success += 1
                        batch_ok += 1
                    except Exception as exc:
                        if stop_on_max_lvols and self._is_max_lvols_error(exc):
                            hit_limit = True
                            self.logger.info(
                                f"[{op_name}] Max lvols reached after "
                                f"{success} successes"
                            )
                            break
                        failures += 1
                        batch_fail += 1
                        self.logger.error(
                            f"[{op_name}] Failed ({failures}/"
                            f"{max_fail} max): {exc}"
                        )

                done = min(batch_start + len(batch), total)
                self.logger.info(
                    f"[{op_name}] progress: {done}/{total} "
                    f"(ok={success} fail={failures})"
                )

                # Backoff: if >50% of this batch failed, wait before
                # next batch to let SPDK recover from transient errors
                batch_total = batch_ok + batch_fail
                if batch_total > 0 and batch_fail > batch_total * 0.5:
                    consecutive_high_fail += 1
                    backoff = min(10 * consecutive_high_fail, 60)
                    self.logger.info(
                        f"[{op_name}] High failure rate in batch "
                        f"({batch_fail}/{batch_total}) — backing off "
                        f"{backoff}s before next batch"
                    )
                    sleep_n_sec(backoff)
                else:
                    consecutive_high_fail = 0

        return success, failures

    def _batch_exec_persistent(self, items, task_fn, op_name: str,
                               per_item_timeout: int = 600,
                               max_workers: int = None,
                               batch_size: int = None,
                               phase_timeout: int = None):
        """Retry failed items until all succeed or a terminal error is hit.

        Returns (success_count, remaining_count) — compatible with
        _batch_exec's 2-tuple return.

        Terminal errors (_is_terminal_error) abort immediately.
        Phase timeout (wall-clock) acts as upper bound.
        Between retry rounds, sleeps PERSISTENT_BACKOFF_SEC seconds.
        """
        workers = max_workers or self.MAX_WORKERS
        bs = batch_size or self.BATCH_SIZE
        deadline = time.monotonic() + (
            phase_timeout or self.PERSISTENT_PHASE_TIMEOUT
        )
        remaining = list(items)
        total_ok = 0
        total_items = len(items)
        round_num = 0
        terminal_hit = False

        while remaining and time.monotonic() < deadline and not terminal_hit:
            round_num += 1
            self.logger.info(
                f"[{op_name}] Persistent round {round_num}: "
                f"{len(remaining)} items remaining, "
                f"{total_ok}/{total_items} succeeded so far"
            )

            failed_items = []

            for batch_start in range(0, len(remaining), bs):
                if terminal_hit or time.monotonic() >= deadline:
                    # Carry forward un-attempted items
                    failed_items.extend(remaining[batch_start:])
                    break

                batch = remaining[batch_start:batch_start + bs]
                futures = {}
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    for item in batch:
                        f = pool.submit(task_fn, item)
                        futures[f] = item

                    batch_ok = 0
                    batch_fail = 0
                    for f in as_completed(futures):
                        item = futures[f]
                        try:
                            f.result(timeout=per_item_timeout)
                            total_ok += 1
                            batch_ok += 1
                        except Exception as exc:
                            if self._is_terminal_error(exc):
                                terminal_hit = True
                                self.logger.info(
                                    f"[{op_name}] Terminal error after "
                                    f"{total_ok} successes: {exc}"
                                )
                                break
                            failed_items.append(item)
                            batch_fail += 1

                done = min(batch_start + len(batch), len(remaining))
                self.logger.info(
                    f"[{op_name}] Round {round_num} progress: "
                    f"{done}/{len(remaining)} "
                    f"(batch ok={batch_ok} fail={batch_fail}, "
                    f"cumulative ok={total_ok}/{total_items})"
                )

                if terminal_hit:
                    break

            remaining = failed_items

            if remaining and not terminal_hit and time.monotonic() < deadline:
                self.logger.info(
                    f"[{op_name}] Round {round_num} done: "
                    f"{len(remaining)} items to retry. "
                    f"Backing off {self.PERSISTENT_BACKOFF_SEC}s"
                )
                sleep_n_sec(self.PERSISTENT_BACKOFF_SEC)

        self._retry_round_counts[op_name] = round_num

        if remaining:
            self.logger.warning(
                f"[{op_name}] Persistent exec ended with "
                f"{len(remaining)} items still failed "
                f"(terminal={terminal_hit}, rounds={round_num})"
            )
        else:
            self.logger.info(
                f"[{op_name}] All {total_items} items succeeded "
                f"after {round_num} round(s)"
            )

        return total_ok, len(remaining)

    # ── 8-phase orchestrator ───────────────────────────────────────────────

    def _run_mass_create_delete_test(self):
        total = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        self._init_mixin_state()
        self.logger.info(
            f"=== Starting {self.__class__.__name__}: "
            f"{self.NUM_SUBSYSTEMS} subsystems x "
            f"{self.NS_PER_SUBSYSTEM} ns/sub = {total} lvols | "
            f"snapshots_per_lvol={self.SNAPSHOTS_PER_LVOL} | "
            f"fio_sample={self.FIO_SAMPLE_PERCENT}% ==="
        )

        try:
            # Phase 1: Mass-create lvols
            t0 = time.time()
            self._phase_1_create_lvols()
            self._phase_durations["1_create_lvols"] = round(time.time() - t0, 1)
            self._metrics["lvols_created"] = len(self._lvol_registry)
            self.logger.info(
                f"[Phase 1] Done: {len(self._lvol_registry)} lvols created "
                f"in {self._phase_durations['1_create_lvols']}s"
            )

            if not self._lvol_registry:
                raise RuntimeError("No lvols created — cannot proceed")

            # Phase 2: FIO on 10% of lvols
            t0 = time.time()
            self._phase_2_fio_on_lvols()
            self._phase_durations["2_fio_lvols"] = round(time.time() - t0, 1)
            self.logger.info(
                f"[Phase 2] FIO started on "
                f"{self._metrics['fio_lvol_started']} lvols"
            )

            # Phase 3: Create snapshots
            t0 = time.time()
            self._phase_3_create_snapshots()
            self._phase_durations["3_create_snapshots"] = round(
                time.time() - t0, 1
            )
            submitted_snaps = len(self._snapshot_registry)
            self.logger.info(
                f"[Phase 3] kubectl apply done: {submitted_snaps} "
                f"snapshot objects submitted "
                f"in {self._phase_durations['3_create_snapshots']}s"
            )

            # Verify snapshots are actually readyToUse — prunes registry
            # to only contain verified snapshots
            if hasattr(self, '_verify_snapshots_ready'):
                self._verify_snapshots_ready()

            verified_snaps = len(self._snapshot_registry)
            self._metrics["snapshots_created"] = verified_snaps
            self.logger.info(
                f"[Phase 3] Verified: {verified_snaps}/{submitted_snaps} "
                f"snapshots readyToUse"
            )
            self._check_count(
                verified_snaps,
                total * self.SNAPSHOTS_PER_LVOL,
                "snapshots",
            )

            # Phase 4: Delete lvols to free subsystem slots for clones.
            # Lvols can be deleted even with snapshots — orphaned
            # snapshots remain valid for cloning.
            t0 = time.time()
            self._phase_4_delete_lvols()
            self._phase_durations["4_delete_lvols"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 4] Lvols deleted "
                f"in {self._phase_durations['4_delete_lvols']}s"
            )

            # Phase 5: Mass-create clones from (orphaned) snapshots
            t0 = time.time()
            self._phase_5_create_clones()
            self._phase_durations["5_create_clones"] = round(
                time.time() - t0, 1
            )
            self._metrics["clones_created"] = len(self._clone_registry)
            self.logger.info(
                f"[Phase 5] Done: {len(self._clone_registry)} clones "
                f"in {self._phase_durations['5_create_clones']}s"
            )
            self._check_count(
                len(self._clone_registry), total, "clones",
            )

            # Phase 6: FIO on 10% of clones
            t0 = time.time()
            self._phase_6_fio_on_clones()
            self._phase_durations["6_fio_clones"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 6] FIO started on "
                f"{self._metrics['fio_clone_started']} clones"
            )

            # Phase 7: Mass-delete all clones
            t0 = time.time()
            self._phase_7_delete_clones()
            self._phase_durations["7_delete_clones"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 7] Clones deleted "
                f"in {self._phase_durations['7_delete_clones']}s"
            )

            # Phase 8: Mass-delete all snapshots
            t0 = time.time()
            self._phase_8_delete_snapshots()
            self._phase_durations["8_delete_snapshots"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 8] Snapshots deleted "
                f"in {self._phase_durations['8_delete_snapshots']}s"
            )

        finally:
            try:
                self.collect_management_details(suffix="_pre_delete")
            except Exception as exc:
                self.logger.warning(
                    f"Failed to collect management details before cleanup: {exc}"
                )
            t0 = time.time()
            self._phase_cleanup()
            self._phase_durations["cleanup"] = round(time.time() - t0, 1)
            self._print_summary()
            self._write_monitoring_json()

        # Soft validation: fail the test if any phase was below ±10% tolerance
        if self._soft_failures:
            for f in self._soft_failures:
                self.logger.error(f"SOFT FAILURE: {f}")
            raise AssertionError(
                f"Test had {len(self._soft_failures)} soft failures: "
                + "; ".join(self._soft_failures)
            )

    # ── Abstract phase methods (subclasses implement) ──────────────────────

    def _phase_1_create_lvols(self):
        raise NotImplementedError

    def _phase_2_fio_on_lvols(self):
        raise NotImplementedError

    def _phase_3_create_snapshots(self):
        raise NotImplementedError

    def _phase_4_delete_lvols(self):
        raise NotImplementedError

    def _phase_5_create_clones(self):
        raise NotImplementedError

    def _phase_6_fio_on_clones(self):
        raise NotImplementedError

    def _phase_7_delete_clones(self):
        raise NotImplementedError

    def _phase_8_delete_snapshots(self):
        raise NotImplementedError

    def _phase_cleanup(self):
        raise NotImplementedError

    # ── Summary ────────────────────────────────────────────────────────────

    def _print_summary(self):
        total = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        self.logger.info("=" * 65)
        self.logger.info("  MASS CREATE/DELETE STRESS TEST — SUMMARY")
        self.logger.info("=" * 65)
        self.logger.info(f"  Config:          {self.NUM_SUBSYSTEMS} subsys x "
                         f"{self.NS_PER_SUBSYSTEM} ns/sub = {total} target")
        self.logger.info(f"  Lvols created:   {self._metrics['lvols_created']}")
        self.logger.info(f"  Snaps created:   "
                         f"{self._metrics['snapshots_created']}")
        self.logger.info(f"  Clones created:  {self._metrics['clones_created']}")
        self.logger.info(f"  FIO lvol sample: "
                         f"{self._metrics['fio_lvol_started']}")
        self.logger.info(f"  FIO clone sample:"
                         f" {self._metrics['fio_clone_started']}")
        if self.PERSISTENT_RETRY:
            self.logger.info("  Mode:            PERSISTENT RETRY")
        for phase, dur in self._phase_durations.items():
            self.logger.info(f"  Phase {phase:25s}: {dur}s")
        if self._retry_round_counts:
            self.logger.info("  --- Retry rounds ---")
            for phase, rounds in self._retry_round_counts.items():
                self.logger.info(f"    {phase:25s}: {rounds} round(s)")
        total_dur = sum(self._phase_durations.values())
        self.logger.info(f"  Total duration:  {total_dur:.1f}s")
        self.logger.info("=" * 65)

    def _write_monitoring_json(self):
        phases = []
        for name, dur in self._phase_durations.items():
            phases.append({"name": name, "duration_sec": dur, "status": "ok"})

        total_dur = sum(self._phase_durations.values())
        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed",
            "config": {
                "num_subsystems": self.NUM_SUBSYSTEMS,
                "ns_per_subsystem": self.NS_PER_SUBSYSTEM,
                "snapshots_per_lvol": self.SNAPSHOTS_PER_LVOL,
                "fio_sample_percent": self.FIO_SAMPLE_PERCENT,
            },
            "phases": phases,
            "metrics": self._metrics,
            "summary": {
                "total_duration_sec": round(total_dur, 2),
            },
        }
        if self.PERSISTENT_RETRY:
            report["persistent_retry"] = True
            report["retry_rounds"] = dict(self._retry_round_counts)

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "mass_create_delete_timing.json"
        with open(out_path, "w") as f:
            _json.dump(report, f, indent=2)
        self.logger.info(f"Monitoring JSON written to {out_path}")


# ─────────────────────────────────────────────────────────────────────────────
#  Docker variant — sbcli API + NVMe connect + SSH FIO
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.lvol_ha_stress_fio import TestLvolHACluster  # noqa: E402
from utils.ssh_utils import get_parent_device  # noqa: E402


class _MassCreateDeleteDocker(_MassCreateDeleteMixin, TestLvolHACluster):
    """Docker base: sbcli API + NVMe connect + SSH FIO."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mass_create_delete_docker"
        self.sn_nodes: list[str] = []
        # parent_name -> {id, node_id, client, ctrl_dev, nqn, devices}
        self._parent_registry: dict[str, dict] = {}
        # child_name -> {id, parent_name, device}
        self._child_registry: dict[str, dict] = {}
        # device_path -> {name, client, mount, parent_name}
        self._device_registry: dict[str, dict] = {}
        self._connected_lvols: dict[str, dict] = {}
        # Lock to prevent races when connecting parent for multiple children
        self._parent_connect_lock = threading.Lock()
        # Tracks fallback devices picked by _fallback_fio_from_lsblk to avoid reuse
        self._fallback_devices_used: set[str] = set()

    # ── NVMe namespace helpers ─────────────────────────────────────────────

    def _rescan_nvme_namespaces(self, node: str, ctrl_dev: str):
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"nvme ns-rescan {ctrl} 2>/dev/null || true\""
        self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)

    # ── FIO fallback helpers ─────────────────────────────────────────────

    def _fallback_fio_from_lsblk(self, client: str) -> tuple[str, str]:
        """Pick a random unmounted NVMe block device on *client* via lsblk.

        Returns (client, device).  Raises RuntimeError if none available.
        """
        cmd = "lsblk -dpno NAME,TYPE,MOUNTPOINT"
        out, _ = self.ssh_obj.exec_command(node=client, command=cmd)
        candidates = []
        for line in (out or "").splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            name, dtype = parts[0], parts[1]
            # If there's a mountpoint column it means the device is mounted
            mountpoint = parts[2] if len(parts) > 2 else ""
            if (
                dtype == "disk"
                and name.startswith("/dev/nvme")
                and not mountpoint
                and name not in self._fallback_devices_used
            ):
                candidates.append(name)
        if not candidates:
            raise RuntimeError(
                f"No available NVMe devices on {client} for fallback FIO"
            )
        device = random.choice(candidates)
        self._fallback_devices_used.add(device)
        self.logger.info(
            f"[FIO fallback] Using {device} on {client} (lsblk discovery)"
        )
        return client, device

    def _run_fallback_fio(self, client: str, device: str, label: str):
        """Format, mount, and start FIO on a fallback device.

        Returns the FIO thread (already started).
        """
        self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
        mount_path = f"/mnt/mcd_fallback_{label}"
        self.ssh_obj.mount_path(
            node=client, device=device, mount_path=mount_path
        )
        log_file = f"/tmp/fio_fallback_{label}.log"
        randseed = random.randint(1, 2**63)
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client, None, mount_path, log_file),
            kwargs={
                "size": self.FIO_SIZE,
                "name": f"mcd_fallback_{label}_fio",
                "rw": "randrw",
                "bs": "4K",
                "iodepth": self.FIO_IODEPTH,
                "numjobs": self.FIO_NUMJOBS,
                "time_based": True,
                "runtime": self.FIO_RUNTIME,
                "randseed": randseed,
            },
        )
        fio_thread.start()
        return fio_thread

    # ── run() ──────────────────────────────────────────────────────────────

    def run(self):
        actual_pool = self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        if actual_pool and actual_pool != self.pool_name:
            self.pool_name = actual_pool
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
        self._run_mass_create_delete_test()

    # ── Phase 1: Mass-create lvols ─────────────────────────────────────────

    def _phase_1_create_lvols(self):
        total = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        self.logger.info(
            f"=== Phase 1: Create lvols "
            f"({self.NUM_SUBSYSTEMS} x {self.NS_PER_SUBSYSTEM} = "
            f"{total}) ==="
        )

        if self.NS_PER_SUBSYSTEM == 1:
            self._create_standalone_lvols()
        else:
            self._create_namespaced_lvols()

    def _create_standalone_lvols(self):
        """Create standalone lvols (one subsystem each).

        Fire-first: create all via API, then bulk-verify + populate IDs.
        """
        names = [
            f"mcd-{_rand_seq(6)}-{i:04d}"
            for i in range(self.NUM_SUBSYSTEMS)
        ]
        items = [{"name": n, "idx": i} for i, n in enumerate(names)]
        if self.PERSISTENT_RETRY:
            ok, fail = self._batch_exec_persistent(
                items, self._fire_create_standalone, "create_lvols",
            )
        else:
            ok, fail = self._batch_exec(
                items, self._fire_create_standalone, "create_lvols",
                stop_on_max_lvols=True,
            )

        # Bulk-verify + populate IDs
        self.logger.info(
            f"[Phase 1] Bulk-verifying {len(names)} standalone lvols"
        )
        verified, id_map = self._bulk_verify_created(
            names, self.sbcli_utils.list_lvols, "verify_standalone",
            timeout=600,
        )
        for name in names:
            if name in id_map:
                self._lvol_registry[name] = {
                    "id": id_map[name], "parent_name": None,
                }

        self._check_count(
            verified, self.NUM_SUBSYSTEMS, "Phase 1 standalone lvols"
        )

    def _fire_create_standalone(self, params: dict):
        """Fire add_lvol for a standalone lvol. No ID fetch — bulk
        verify populates IDs after all are fired."""
        name = params["name"]
        idx = params["idx"]
        host_id = self.sn_nodes[0] if self.sn_nodes else None
        bdev_retries = 0
        sync_retries = 0

        for attempt in range(10):
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=name,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    host_id=host_id,
                    retry=1,
                )
                break
            except Exception as e:
                if self._is_max_lvols_error(e):
                    raise
                if self._is_bdev_error(e) and bdev_retries < 3:
                    bdev_retries += 1
                    name = f"mcd-{_rand_seq(6)}-{idx:04d}"
                    sleep_n_sec(2)
                    continue
                if self._is_sync_deletion_error(e) and sync_retries < 5:
                    sync_retries += 1
                    sleep_n_sec(15)
                    continue
                if attempt < 9:
                    sleep_n_sec(5)
                    continue
                raise

    def _create_namespaced_lvols(self):
        """Create parent lvols + children via namespace grouping.

        Fire-first approach: create all parents, bulk-verify, then
        fire all children across all parents, bulk-verify again.
        No NVMe connect during creation — that happens on-demand in
        Phase 2 for the FIO sample only.
        """
        parent_names = [
            f"mcd-par-{_rand_seq(6)}-{i:03d}"
            for i in range(self.NUM_SUBSYSTEMS)
        ]

        # Sub-phase 1a: Fire parent creation (API-only)
        self.logger.info(
            f"[Phase 1a] Creating {len(parent_names)} parent lvols"
        )
        parent_items = [{"name": n} for n in parent_names]
        if self.PERSISTENT_RETRY:
            ok, fail = self._batch_exec_persistent(
                parent_items, self._create_parent, "create_parents",
            )
        else:
            ok, fail = self._batch_exec(
                parent_items, self._create_parent, "create_parents",
                stop_on_max_lvols=True,
            )
        # Filter to successfully created parents
        parent_names = [n for n in parent_names if n in self._parent_registry]

        if not parent_names:
            return

        # Bulk-verify parents + populate IDs
        self.logger.info(
            f"[Phase 1a] Bulk-verifying {len(parent_names)} parents"
        )
        verified, id_map = self._bulk_verify_created(
            parent_names, self.sbcli_utils.list_lvols, "verify_parents",
            timeout=300,
        )
        for pn in parent_names:
            if pn in id_map:
                self._parent_registry[pn]["id"] = id_map[pn]
                self._lvol_registry[pn] = {
                    "id": id_map[pn], "parent_name": None,
                }

        self._check_count(
            verified, len(parent_names), "Phase 1a parents"
        )

        # Sub-phase 1b: Fire ALL children across ALL parents (flat parallel)
        children_per_parent = self.NS_PER_SUBSYSTEM - 1
        if children_per_parent <= 0:
            return

        child_tasks = []
        for pn in parent_names:
            if pn not in id_map:
                continue
            for ns_idx in range(1, children_per_parent + 1):
                cname = (
                    f"mcd-ch-{_rand_seq(5)}-{pn[-3:]}-{ns_idx:03d}"
                )
                child_tasks.append({
                    "name": cname,
                    "parent_name": pn,
                })

        self.logger.info(
            f"[Phase 1b] Creating {len(child_tasks)} children "
            f"across {len(parent_names)} parents"
        )
        if self.PERSISTENT_RETRY:
            ok, fail = self._batch_exec_persistent(
                child_tasks, self._fire_create_child, "create_children",
            )
        else:
            ok, fail = self._batch_exec(
                child_tasks, self._fire_create_child, "create_children",
                stop_on_max_lvols=True,
                max_failures=self.MAX_FAILURES,
            )

        # Bulk-verify children + populate IDs
        child_names = [t["name"] for t in child_tasks]
        self.logger.info(
            f"[Phase 1b] Bulk-verifying {len(child_names)} children"
        )
        c_verified, c_id_map = self._bulk_verify_created(
            child_names, self.sbcli_utils.list_lvols, "verify_children",
            timeout=600,
        )
        for ct in child_tasks:
            cname = ct["name"]
            pn = ct["parent_name"]
            if cname in c_id_map:
                self._lvol_registry[cname] = {
                    "id": c_id_map[cname], "parent_name": pn,
                }
                self._child_registry[cname] = {
                    "id": c_id_map[cname], "parent_name": pn,
                    "device": None,
                }

        expected_children = len(parent_names) * children_per_parent
        self._check_count(
            c_verified, expected_children, "Phase 1b children"
        )

    def _create_parent(self, params: dict):
        """Fire add_lvol for a parent. ID populated later by bulk verify."""
        name = params["name"]
        host_id = self.sn_nodes[0] if self.sn_nodes else None
        self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
            max_namespace_per_subsys=self.NS_PER_SUBSYSTEM,
            host_id=host_id,
            retry=3,
        )
        # Register parent stub — ID filled in by bulk verify
        self._parent_registry[name] = {
            "id": None, "node_id": None,
            "client": None, "ctrl_dev": None, "nqn": None, "devices": [],
        }

    def _connect_parent(self, parent_name: str):
        pinfo = self._parent_registry[parent_name]
        connect_ls = self.sbcli_utils.get_lvol_connect_str(
            lvol_name=parent_name
        )
        if not connect_ls:
            raise RuntimeError(f"{parent_name}: no connect strings")

        client = pinfo["client"]

        for cmd in connect_ls:
            self.ssh_obj.exec_command(node=client, command=cmd)
        sleep_n_sec(3)

        device = self.ssh_obj.get_lvol_vs_device(
            node=client, lvol_id=pinfo["id"]
        )
        if not device:
            for _ in range(120):
                sleep_n_sec(30)
                device = self.ssh_obj.get_lvol_vs_device(
                    node=client, lvol_id=pinfo["id"]
                )
                if device:
                    break

        if not device:
            raise RuntimeError(f"{parent_name}: device not found")

        ctrl_dev = get_parent_device(device)
        nqn = None
        try:
            cmd = (
                f"bash -lc \"nvme list-subsys {device} 2>/dev/null "
                f"| grep -oP 'nqn\\S+' | head -1 || true\""
            )
            out, _ = self.ssh_obj.exec_command(node=client, command=cmd)
            nqn = (out or "").strip() or None
        except Exception:
            pass

        pinfo["ctrl_dev"] = ctrl_dev
        pinfo["nqn"] = nqn
        pinfo["devices"] = [device]
        self._connected_lvols[parent_name] = {
            "client": client, "device": device, "ctrl_dev": ctrl_dev,
        }

    def _fire_create_child(self, params: dict):
        """Fire add_lvol(namespace=True) for a child. No ID fetch, no
        device discovery. ID populated later by bulk verify."""
        name = params["name"]
        host_id = self.sn_nodes[0] if self.sn_nodes else None
        self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
            namespace=True,
            host_id=host_id,
            retry=3,
        )

    # ── Phase 2: FIO on 10% of lvols ──────────────────────────────────────

    def _phase_2_fio_on_lvols(self):
        sample_size = min(
            max(1, math.ceil(
                len(self._lvol_registry) * self.FIO_SAMPLE_PERCENT / 100
            )),
            self.FIO_SAMPLE_MAX,
        )
        sample = random.sample(
            list(self._lvol_registry.keys()),
            min(sample_size, len(self._lvol_registry)),
        )
        self._fio_lvol_sample = set(sample)
        self.logger.info(
            f"=== Phase 2: FIO on {len(sample)} lvols "
            f"({self.FIO_SAMPLE_PERCENT}% of "
            f"{len(self._lvol_registry)}) ==="
        )

        phase_start = time.time()
        phase_timeout = 3600  # 1 hour
        last_progress_count = 0
        last_progress_time = phase_start
        stall_timeout = 600  # abort if no new success in 10 min

        for i, lvol_name in enumerate(sample):
            elapsed = time.time() - phase_start
            if elapsed > phase_timeout:
                self.logger.error(
                    f"[Phase 2] Aborting — phase timeout {phase_timeout}s "
                    f"exceeded after {self._metrics['fio_lvol_started']}/"
                    f"{len(sample)} lvols started"
                )
                break

            current_ok = self._metrics["fio_lvol_started"]
            if current_ok > last_progress_count:
                last_progress_count = current_ok
                last_progress_time = time.time()
            elif time.time() - last_progress_time > stall_timeout:
                self.logger.error(
                    f"[Phase 2] Aborting — no progress for "
                    f"{stall_timeout}s, stuck at "
                    f"{current_ok}/{len(sample)} lvols"
                )
                break

            try:
                self._connect_format_fio_lvol(lvol_name)
                self._metrics["fio_lvol_started"] += 1
            except Exception as exc:
                self.logger.warning(
                    f"[Phase 2] Normal FIO setup failed for {lvol_name}: "
                    f"{exc}; trying lsblk fallback"
                )
                try:
                    client = self.fio_node[
                        hash(lvol_name) % len(self.fio_node)
                    ]
                    fb_client, fb_device = self._fallback_fio_from_lsblk(
                        client
                    )
                    fio_thread = self._run_fallback_fio(
                        fb_client, fb_device, f"lvol_{i}"
                    )
                    self._fio_lvol_threads.append(fio_thread)
                    self._metrics["fio_lvol_started"] += 1
                    self.logger.info(
                        f"[Phase 2] Fallback FIO started on {fb_device} "
                        f"(for {lvol_name})"
                    )
                except Exception as fb_exc:
                    self.logger.error(
                        f"[Phase 2] Fallback also failed for "
                        f"{lvol_name}: {fb_exc}"
                    )
                    self._metrics["fio_lvol_failures"] += 1

            if (i + 1) % 10 == 0:
                self.logger.info(
                    f"[Phase 2] Progress: {i + 1}/{len(sample)} "
                    f"attempted, {self._metrics['fio_lvol_started']} ok, "
                    f"{self._metrics['fio_lvol_failures']} failed"
                )

    def _connect_format_fio_lvol(self, lvol_name: str):
        info = self._lvol_registry[lvol_name]

        # Already connected?
        if lvol_name in self._connected_lvols:
            cl = self._connected_lvols[lvol_name]
            client, device = cl["client"], cl["device"]
        elif info.get("parent_name") and lvol_name in self._child_registry:
            # Child namespace — connect parent first, then discover child
            client, device = self._connect_child_for_fio(lvol_name)
        else:
            # Standalone lvol — on-demand NVMe connect
            client, device = self._connect_standalone_for_fio(lvol_name)

        # Format
        self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
        mount_path = f"/mnt/mcd_{lvol_name}"
        self.ssh_obj.mount_path(
            node=client, device=device, mount_path=mount_path
        )

        # Start FIO
        log_file = f"/tmp/fio_{lvol_name}.log"
        randseed = random.randint(1, 2**63)
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client, None, mount_path, log_file),
            kwargs={
                "size": self.FIO_SIZE,
                "name": f"mcd_{lvol_name}_fio",
                "rw": "randrw",
                "bs": "4K",
                "iodepth": self.FIO_IODEPTH,
                "numjobs": self.FIO_NUMJOBS,
                "time_based": True,
                "runtime": self.FIO_RUNTIME,
                "randseed": randseed,
            },
        )
        fio_thread.start()
        self._fio_lvol_threads.append(fio_thread)

    def _connect_standalone_for_fio(self, lvol_name: str):
        """On-demand NVMe connect for a standalone lvol. Returns (client, device)."""
        info = self._lvol_registry[lvol_name]
        lvol_id = info["id"]
        client = self.fio_node[hash(lvol_name) % len(self.fio_node)]

        connect_cmds = self.sbcli_utils.get_lvol_connect_str(
            lvol_name=lvol_name
        )
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(node=client, command=cmd)
        sleep_n_sec(3)

        device = None
        for _ in range(120):
            device = self.ssh_obj.get_lvol_vs_device(
                node=client, lvol_id=lvol_id
            )
            if device:
                break
            sleep_n_sec(30)
        if not device:
            raise RuntimeError(f"{lvol_name}: device not found")

        self._connected_lvols[lvol_name] = {
            "client": client, "device": device,
        }
        return client, device

    def _find_parent_by_nqn(self, nqn: str):
        """Return (parent_name, pinfo) for the parent whose connected NQN
        matches *nqn*, or (None, None) if no match."""
        for pname, pinfo in self._parent_registry.items():
            if pinfo.get("nqn") == nqn:
                return pname, pinfo
        return None, None

    def _connect_child_for_fio(self, child_name: str):
        """On-demand NVMe connect for a child namespace.

        Uses the child's connect API to discover the real parent
        subsystem NQN (the control plane decides placement, so the
        test's internal parent_name mapping may not match reality).
        Connects that subsystem if needed, rescans, and discovers
        the child device.
        Returns (client, device).
        """
        info = self._lvol_registry[child_name]
        child_id = info["id"]
        parent_name = info["parent_name"]
        pinfo = self._parent_registry.get(parent_name, {})

        # Assign a client to the parent if not already set
        with self._parent_connect_lock:
            if not pinfo.get("client"):
                idx = list(self._parent_registry.keys()).index(parent_name)
                pinfo["client"] = self.fio_node[
                    idx % len(self.fio_node)
                ]

        client = pinfo["client"]

        # Ask the control plane for the child's actual connect info.
        # This returns the parent subsystem NQN that the child lives in
        # (children are namespaces, not separate NVMe targets).
        connect_data = self.sbcli_utils.get_request(
            api_url=f"/lvol/connect/{child_id}"
        )
        connect_results = connect_data.get("results", [])
        if not connect_results:
            raise RuntimeError(
                f"{child_name}: connect API returned no results"
            )

        real_nqn = connect_results[0].get("nqn")
        child_ns_id = connect_results[0].get("ns_id")
        if child_ns_id is not None:
            child_ns_id = int(child_ns_id)

        if not real_nqn:
            raise RuntimeError(
                f"{child_name}: connect API returned no NQN"
            )

        # Find which parent (if any) already owns this NQN on the client.
        # The test's parent_name mapping may be wrong because add_lvol
        # with namespaced=True lets the control plane auto-place.
        with self._parent_connect_lock:
            actual_parent, actual_pinfo = self._find_parent_by_nqn(real_nqn)

            if actual_pinfo and actual_pinfo.get("ctrl_dev"):
                # Subsystem already connected
                ctrl_dev = actual_pinfo["ctrl_dev"]
            else:
                # Need to connect this subsystem.  Build connect commands
                # from the API response.
                connect_cmds = []
                for entry in connect_results:
                    if "connect" in entry:
                        connect_cmds.append(entry["connect"])
                    else:
                        connect_cmds.append(
                            f"sudo nvme connect "
                            f"--reconnect-delay={entry.get('reconnect_delay', 2)} "
                            f"--ctrl-loss-tmo={entry.get('ctrl_loss_tmo', 3600)} "
                            f"--fast_io_fail_tmo={entry.get('fast_io_fail_tmo', 1)} "
                            f"--nr-io-queues={entry.get('nr_io_queues', 3)} "
                            f"--keep-alive-tmo={entry.get('keep_alive_tmo', 4)} "
                            f"--transport={entry.get('transport', 'tcp')} "
                            f"--traddr={entry.get('ip')} "
                            f"--trsvcid={entry.get('port')} "
                            f"--nqn={real_nqn}"
                        )

                for cmd in connect_cmds:
                    self.ssh_obj.exec_command(node=client, command=cmd)
                sleep_n_sec(3)

                # Discover the controller device for this subsystem
                parent_uuid = real_nqn.split(":lvol:")[-1]
                device = self.ssh_obj.get_lvol_vs_device(
                    node=client, lvol_id=parent_uuid
                )
                if not device:
                    for _ in range(10):
                        sleep_n_sec(5)
                        device = self.ssh_obj.get_lvol_vs_device(
                            node=client, lvol_id=parent_uuid
                        )
                        if device:
                            break
                if not device:
                    raise RuntimeError(
                        f"{child_name}: parent subsystem {real_nqn} "
                        f"device not found after connect"
                    )

                ctrl_dev = get_parent_device(device)

                # Update whichever parent registry entry matches, or
                # the one the test assigned.
                target_pinfo = actual_pinfo if actual_pinfo else pinfo
                target_pinfo["ctrl_dev"] = ctrl_dev
                target_pinfo["nqn"] = real_nqn
                target_pinfo["devices"] = [device]

        if not ctrl_dev:
            raise RuntimeError(
                f"{child_name}: no ctrl_dev after connect"
            )

        # Rescan and find child device by NQN + ns_id
        self._rescan_nvme_namespaces(client, ctrl_dev)
        sleep_n_sec(5)

        device = None
        for attempt in range(120):
            device = self.ssh_obj.get_lvol_vs_device(
                node=client, lvol_id=child_id,
                nqn=real_nqn, ns_id=child_ns_id,
            )
            if device:
                break
            self._rescan_nvme_namespaces(client, ctrl_dev)
            sleep_n_sec(30)
        if not device:
            raise RuntimeError(f"{child_name}: device not found after rescan")

        self._connected_lvols[child_name] = {
            "client": client, "device": device,
        }
        self._child_registry[child_name]["device"] = device
        return client, device

    # ── Phase 3: Create snapshots ──────────────────────────────────────────

    def _phase_3_create_snapshots(self):
        snap_items = []
        # Round-robin: create snapshot round 0 for all lvols, then round 1
        # for all lvols, etc.  This spreads blobstore metadata load across
        # lvols instead of stacking all snapshots on one lvol before moving
        # to the next (which causes O(n) degradation on a single LVS).
        lvol_entries = [
            (name, info["id"]) for name, info in self._lvol_registry.items()
        ]
        for s in range(self.SNAPSHOTS_PER_LVOL):
            for lvol_name, lvol_id in lvol_entries:
                snap_name = f"snap-{lvol_name[-8:]}-{s:03d}"
                snap_items.append({
                    "lvol_id": lvol_id,
                    "snap_name": snap_name,
                    "lvol_name": lvol_name,
                })

        expected_snaps = len(snap_items)
        self.logger.info(
            f"=== Phase 3: Create {expected_snaps} snapshots "
            f"({len(self._lvol_registry)} lvols x "
            f"{self.SNAPSHOTS_PER_LVOL}) ==="
        )

        # Fire all snapshot creation calls.
        if self.PERSISTENT_RETRY:
            self.logger.info(
                f"[Phase 3] Persistent retry mode: will retry until "
                f"all {expected_snaps} snapshots created or terminal error"
            )
            ok, fail = self._batch_exec_persistent(
                snap_items, self._fire_create_snapshot, "create_snapshots",
                batch_size=self.SNAPSHOT_BATCH_SIZE,
                phase_timeout=self.SNAPSHOT_PHASE_TIMEOUT,
            )
        else:
            # Scale max_failures to item count — with 50 snaps/lvol, the
            # default MAX_FAILURES=500 is exhausted by just 10 bad lvols.
            # Allow up to 10% of total items to fail before aborting.
            snap_max_failures = max(self.MAX_FAILURES, expected_snaps // 10)
            self.logger.info(
                f"[Phase 3] max_failures for snapshots: {snap_max_failures}"
            )
            ok, fail = self._batch_exec(
                snap_items, self._fire_create_snapshot, "create_snapshots",
                batch_size=self.SNAPSHOT_BATCH_SIZE,
                stop_on_max_lvols=True,
                max_failures=snap_max_failures,
            )

        # Bulk-verify snapshots + populate IDs
        snap_names = [s["snap_name"] for s in snap_items]
        self.logger.info(
            f"[Phase 3] Bulk-verifying {len(snap_names)} snapshots"
        )
        verified, snap_id_map = self._bulk_verify_created(
            snap_names, self.sbcli_utils.list_snapshots,
            "verify_snapshots", timeout=600,
        )
        # Build lvol_name lookup for snap_items
        snap_to_lvol = {s["snap_name"]: s["lvol_name"] for s in snap_items}
        for sn, sid in snap_id_map.items():
            self._snapshot_registry[sn] = {
                "snap_id": sid,
                "parent_lvol": snap_to_lvol.get(sn),
            }

        self._check_count(verified, expected_snaps, "Phase 3 snapshots")

    def _fire_create_snapshot(self, params: dict):
        """Fire add_snapshot only. No ID fetch — bulk verify resolves IDs.

        Retries on sync-deletion errors (lvol temporarily in cleanup state)
        with backoff, similar to _fire_create_standalone.
        """
        lvol_id = params["lvol_id"]
        snap_name = params["snap_name"]
        sync_retries = 0

        for attempt in range(6):
            try:
                self.sbcli_utils.add_snapshot(
                    lvol_id=lvol_id, snapshot_name=snap_name, retry=3
                )
                return
            except Exception as e:
                if self._is_sync_deletion_error(e) and sync_retries < 5:
                    sync_retries += 1
                    sleep_n_sec(15)
                    continue
                if attempt < 5:
                    sleep_n_sec(5)
                    continue
                raise

    # ── Phase 4: Delete lvols (free subsystem slots for clones) ──────────

    def _phase_4_delete_lvols(self):
        # Kill lvol FIO (started in Phase 2, left running)
        # Include fio_node for fallback FIO processes
        kill_clients = set(
            c.get("client") for c in self._connected_lvols.values()
            if c.get("client")
        ) | set(self.fio_node)
        for client in kill_clients:
            try:
                self.ssh_obj.exec_command(
                    node=client,
                    command="sudo pkill -9 -f 'fio.*mcd_' 2>/dev/null || true",
                )
            except Exception:
                pass
        for t in self._fio_lvol_threads:
            t.join(timeout=30)

        lvol_names = list(self._lvol_registry.keys())
        self.logger.info(
            f"=== Phase 4: Delete {len(lvol_names)} lvols "
            f"(freeing subsystem slots for clones) ==="
        )

        # Fire-and-forget: issue DELETE for all lvols without polling
        ok, fail = self._batch_exec(
            lvol_names,
            self._fire_delete_lvol,
            "delete_lvols_fire",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(lvol_names),
        )
        self.logger.info(
            f"[Phase 4] DELETE issued for {ok} lvols "
            f"({fail} failed to issue)"
        )

        # Wait for lvols to actually disappear
        self._wait_lvols_deleted(lvol_names, "lvols")
        self._metrics["lvols_deleted"] = ok

    # ── Phase 5: Mass-create clones from snapshots ─────────────────────

    def _phase_5_create_clones(self):
        if not self._snapshot_registry:
            self.logger.info("[Phase 5] No snapshots — skipping clone phase")
            return

        # Resolve any missing snapshot IDs via bulk list
        missing = [
            sn for sn, si in self._snapshot_registry.items()
            if not si.get("snap_id")
        ]
        if missing:
            self.logger.info(
                f"[Phase 5] Resolving {len(missing)} missing snap IDs"
            )
            try:
                all_snaps = self.sbcli_utils.list_snapshots()
                for sn in missing:
                    if sn in all_snaps:
                        self._snapshot_registry[sn]["snap_id"] = all_snaps[sn]
            except Exception as exc:
                self.logger.warning(
                    f"[Phase 5] Could not list snapshots: {exc}"
                )

        snap_list = [
            (sn, si["snap_id"])
            for sn, si in self._snapshot_registry.items()
            if si.get("snap_id")
        ]
        if not snap_list:
            self.logger.info("[Phase 5] No valid snapshot IDs — skipping")
            return

        self.logger.info(
            f"=== Phase 5: Create clones from {len(snap_list)} snapshots "
            f"(until subsystem limit) ==="
        )

        # Fire-first: create clones in batches until limit hit
        clone_names_fired = []
        clone_idx = [0]
        hit_limit = [False]
        lock = threading.Lock()

        def _fire_create_clone(_):
            if hit_limit[0]:
                return
            snap_name, snap_id = random.choice(snap_list)
            with lock:
                idx = clone_idx[0]
                clone_idx[0] += 1
            clone_name = f"clone-{_rand_seq(5)}-{idx:06d}"
            try:
                self.sbcli_utils.add_clone(
                    snapshot_id=snap_id, clone_name=clone_name, retry=3
                )
                with lock:
                    clone_names_fired.append(
                        (clone_name, snap_name)
                    )
            except Exception as e:
                if self._is_max_lvols_error(e):
                    hit_limit[0] = True
                    self.logger.info(
                        f"[Phase 5] Max lvols reached at clone #{idx}"
                    )
                    return
                raise

        # Submit clones in batches until limit
        deadline = time.time() + self.CLONE_PHASE_TIMEOUT
        batch_num = 0
        while not hit_limit[0] and time.time() < deadline:
            batch = list(range(self.BATCH_SIZE))
            ok, fail = self._batch_exec(
                batch,
                _fire_create_clone,
                f"create_clones_b{batch_num}",
                max_workers=self.CLONE_MAX_WORKERS,
                stop_on_max_lvols=False,
                max_failures=self.BATCH_SIZE,
            )
            batch_num += 1
            if fail >= self.BATCH_SIZE:
                break
            self.logger.info(
                f"[Phase 5] {len(clone_names_fired)} clones fired so far"
            )

        # Bulk-verify clones + populate IDs
        if clone_names_fired:
            all_clone_names = [cn for cn, _ in clone_names_fired]
            snap_lookup = {cn: sn for cn, sn in clone_names_fired}
            self.logger.info(
                f"[Phase 5] Bulk-verifying {len(all_clone_names)} clones"
            )
            verified, id_map = self._bulk_verify_created(
                all_clone_names, self.sbcli_utils.list_lvols,
                "verify_clones", timeout=600,
            )
            for cn, cid in id_map.items():
                self._clone_registry[cn] = {
                    "clone_id": cid,
                    "snap_name": snap_lookup.get(cn),
                }
            self.logger.info(
                f"[Phase 5] {len(self._clone_registry)} clones verified"
            )

    # ── Phase 6: FIO on 10% of clones ─────────────────────────────────────

    def _phase_6_fio_on_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 6] No clones — skipping")
            return

        sample_size = min(
            max(1, math.ceil(
                len(self._clone_registry) * self.FIO_SAMPLE_PERCENT / 100
            )),
            self.FIO_SAMPLE_MAX,
        )
        sample = random.sample(
            list(self._clone_registry.keys()),
            min(sample_size, len(self._clone_registry)),
        )
        self._fio_clone_sample = set(sample)
        self.logger.info(
            f"=== Phase 6: FIO on {len(sample)} clones ==="
        )

        for i, clone_name in enumerate(sample):
            try:
                self._connect_format_fio_clone(clone_name)
                self._metrics["fio_clone_started"] += 1
            except Exception as exc:
                self.logger.warning(
                    f"[Phase 6] Normal FIO setup failed for {clone_name}: "
                    f"{exc}; trying lsblk fallback"
                )
                try:
                    client = self.fio_node[
                        hash(clone_name) % len(self.fio_node)
                    ]
                    fb_client, fb_device = self._fallback_fio_from_lsblk(
                        client
                    )
                    fio_thread = self._run_fallback_fio(
                        fb_client, fb_device, f"clone_{i}"
                    )
                    self._fio_clone_threads.append(fio_thread)
                    self._metrics["fio_clone_started"] += 1
                    self.logger.info(
                        f"[Phase 6] Fallback FIO started on {fb_device} "
                        f"(for {clone_name})"
                    )
                except Exception as fb_exc:
                    self.logger.error(
                        f"[Phase 6] Fallback also failed for "
                        f"{clone_name}: {fb_exc}"
                    )
                    self._metrics["fio_clone_failures"] += 1

        # Wait for FIO to finish
        self.logger.info(
            f"[Phase 6] Waiting for {len(self._fio_clone_threads)} "
            f"FIO threads to finish (timeout={self.FIO_RUNTIME + 120}s)"
        )
        for t in self._fio_clone_threads:
            t.join(timeout=self.FIO_RUNTIME + 120)

    def _connect_format_fio_clone(self, clone_name: str):
        clone_id = self._clone_registry[clone_name].get("clone_id")
        client = self.fio_node[hash(clone_name) % len(self.fio_node)]

        connect_cmds = self.sbcli_utils.get_lvol_connect_str(
            lvol_name=clone_name
        )
        for cmd in connect_cmds:
            self.ssh_obj.exec_command(node=client, command=cmd)
        sleep_n_sec(3)

        device = None
        for _ in range(120):
            device = self.ssh_obj.get_lvol_vs_device(
                node=client, lvol_id=clone_id
            )
            if device:
                break
            sleep_n_sec(30)
        if not device:
            raise RuntimeError(f"{clone_name}: device not found")

        self._connected_lvols[clone_name] = {
            "client": client, "device": device,
        }

        # Format the clone (parent may not have been formatted)
        self.ssh_obj.format_disk(node=client, device=device, fs_type="ext4")
        mount_path = f"/mnt/mcd_clone_{clone_name}"
        self.ssh_obj.mount_path(
            node=client, device=device, mount_path=mount_path
        )

        log_file = f"/tmp/fio_clone_{clone_name}.log"
        randseed = random.randint(1, 2**63)
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(client, None, mount_path, log_file),
            kwargs={
                "size": self.FIO_SIZE,
                "name": f"mcd_clone_{clone_name}_fio",
                "rw": "randrw",
                "bs": "4K",
                "iodepth": self.FIO_IODEPTH,
                "numjobs": self.FIO_NUMJOBS,
                "time_based": True,
                "runtime": self.FIO_RUNTIME,
                "randseed": randseed,
            },
        )
        fio_thread.start()
        self._fio_clone_threads.append(fio_thread)

    # ── Phase 7: Mass-delete clones ────────────────────────────────────────

    def _phase_7_delete_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 7] No clones — skipping")
            return

        self.logger.info(
            f"=== Phase 7: Delete {len(self._clone_registry)} clones ==="
        )

        # Kill clone FIO (include fio_node for fallback FIO processes)
        kill_clients = set(
            c.get("client") for c in self._connected_lvols.values()
            if c.get("client")
        ) | set(self.fio_node)
        for client in kill_clients:
            try:
                self.ssh_obj.exec_command(
                    node=client,
                    command="sudo pkill -9 -f 'fio.*mcd_.*clone_' "
                            "2>/dev/null || true",
                )
            except Exception:
                pass
        for t in self._fio_clone_threads:
            t.join(timeout=30)

        clone_names = list(self._clone_registry.keys())

        # Fire-and-forget: issue DELETE for all clones without polling
        ok, fail = self._batch_exec(
            clone_names,
            self._fire_delete_lvol,
            "delete_clones_fire",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(clone_names),
        )
        self.logger.info(
            f"[Phase 7] DELETE issued for {ok} clones "
            f"({fail} failed to issue)"
        )

        # Wait for clones to actually disappear
        self._wait_lvols_deleted(clone_names, "clones")
        self._metrics["clones_deleted"] = ok

    def _fire_delete_lvol(self, lvol_name: str):
        """Issue DELETE request for an lvol without polling for completion.

        Uses cached lvol ID from _lvol_registry or _clone_registry to avoid
        calling list_lvols(). This is critical at scale (3000+ lvols) where
        20 threads each polling list_lvols() every 5s would flood the API.
        """
        # Look up cached ID (lvol_registry uses "id", clone_registry uses "clone_id")
        info = self._lvol_registry.get(lvol_name)
        if info:
            lvol_id = info.get("id")
        else:
            cinfo = self._clone_registry.get(lvol_name)
            lvol_id = cinfo.get("clone_id") if cinfo else None

        if not lvol_id:
            # Fallback: fetch ID (single GET, not list)
            try:
                lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            except Exception:
                pass

        if not lvol_id:
            self.logger.warning(
                f"[fire_delete] {lvol_name}: no ID found, skipping"
            )
            return

        try:
            self.sbcli_utils.delete_request(
                api_url=f"/lvol/{lvol_id}",
                treat_404_as_success=True,
            )
        except Exception as exc:
            self.logger.warning(
                f"[fire_delete] {lvol_name} ({lvol_id}): {exc}"
            )

    def _wait_lvols_deleted(
        self, names: list, label: str, timeout: int = 1800
    ):
        """Wait for lvols/clones to disappear from the API.

        Polls list_lvols() periodically (single call, not per-lvol) every
        30s until all named items are gone or timeout is reached.
        Re-issues DELETE for any stuck items every 60s.
        """
        deadline = time.monotonic() + timeout
        remaining = set(names)
        re_delete_interval = 60  # re-issue DELETE every 60s
        last_re_delete = time.monotonic()

        self.logger.info(
            f"[{label}] Waiting for {len(remaining)} items to be deleted "
            f"(timeout={timeout}s)"
        )

        while remaining and time.monotonic() < deadline:
            try:
                current_lvols = self.sbcli_utils.list_lvols()
            except Exception as exc:
                self.logger.warning(
                    f"[{label}] list_lvols failed: {exc}, retrying..."
                )
                sleep_n_sec(10)
                continue

            still_present = remaining & set(current_lvols.keys())
            just_deleted = remaining - still_present
            if just_deleted:
                self.logger.info(
                    f"[{label}] {len(just_deleted)} more deleted, "
                    f"{len(still_present)} remaining"
                )
            remaining = still_present

            if not remaining:
                break

            # Re-issue DELETE for stuck items every re_delete_interval
            now = time.monotonic()
            if now - last_re_delete >= re_delete_interval:
                self.logger.info(
                    f"[{label}] Re-issuing DELETE for "
                    f"{len(remaining)} stuck items"
                )
                for name in list(remaining)[:100]:  # cap to avoid flood
                    lvol_id = current_lvols.get(name)
                    if lvol_id:
                        try:
                            self.sbcli_utils.delete_request(
                                api_url=f"/lvol/{lvol_id}",
                                treat_404_as_success=True,
                            )
                        except Exception:
                            pass
                last_re_delete = now

            sleep_n_sec(30)

        if remaining:
            self.logger.warning(
                f"[{label}] Timeout: {len(remaining)} items still exist "
                f"after {timeout}s. Proceeding anyway."
            )
            self._metrics[f"{label}_delete_timeout_remaining"] = len(
                remaining
            )
        else:
            self.logger.info(f"[{label}] All items deleted successfully")

    # ── Phase 8: Mass-delete snapshots ─────────────────────────────────────

    def _phase_8_delete_snapshots(self):
        if not self._snapshot_registry:
            self.logger.info("[Phase 8] No snapshots — skipping")
            return

        self.logger.info(
            f"=== Phase 8: Delete {len(self._snapshot_registry)} "
            f"snapshots ==="
        )

        snap_names = list(self._snapshot_registry.keys())
        ok, fail = self._batch_exec(
            snap_names,
            self._delete_single_snapshot,
            "delete_snapshots",
            max_workers=self.DELETE_MAX_WORKERS,
            batch_size=self.SNAPSHOT_BATCH_SIZE,
            max_failures=len(snap_names),
        )
        self._metrics["snapshots_deleted"] = ok

    def _delete_single_snapshot(self, snap_name: str):
        try:
            self.sbcli_utils.delete_snapshot(
                snap_name=snap_name, skip_error=True, max_attempt=30
            )
        except Exception as exc:
            self.logger.warning(f"[delete_snap] {snap_name}: {exc}")

    # ── Cleanup safety net ─────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Cleanup ===")
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_clones: {exc}")
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_snapshots: {exc}")
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_lvols: {exc}")
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_storage_pools: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  K8s variant — PVC + VolumeSnapshot + FIO K8s Jobs
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.continuous_k8s_native_failover import (  # noqa: E402
    K8sNativeFailoverTest,
)


class _MassCreateDeleteK8s(_MassCreateDeleteMixin, K8sNativeFailoverTest):
    """K8s base: PVC + VolumeSnapshot + FIO K8s Jobs."""

    STORAGE_CLASS_NAME = "mcd-sc"
    SNAPSHOT_CLASS_NAME = "mcd-snapshotclass"

    # ── Batched fire-and-monitor config ─────────────────────────────────
    CREATE_CONCURRENT = 20      # concurrent fire-and-forget per batch
    CREATE_BATCH_PAUSE = 30     # seconds between creation batches
    DELETE_CONCURRENT = 20      # concurrent deletes per batch
    DELETE_BATCH_PAUSE = 30     # seconds between deletion batches
    MONITOR_INTERVAL = 30       # seconds between background count polls
    BOUND_WAIT_TIMEOUT = 1800   # max seconds to wait for PVCs to bind

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mass_create_delete_k8s"
        self.fio_num_jobs = self.FIO_NUMJOBS
        self._pvc_registry: dict[str, dict] = {}    # pvc_name -> {lvol_name}
        self._snap_pvc_map: dict[str, str] = {}     # vs_name -> pvc_name
        self._clone_pvc_registry: dict[str, dict] = {}  # clone_pvc -> {vs_name}
        self._fio_jobs: dict[str, str] = {}          # job_name -> pvc/clone
        self._time_series: dict[str, list] = {}      # phase -> [(elapsed_s, count)]

    # ── run() ──────────────────────────────────────────────────────────────

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

        actual_pool = self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        if actual_pool and actual_pool != self.pool_name:
            self.pool_name = actual_pool

        cluster_id = self.cluster_id or os.environ.get("CLUSTER_ID", "")
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
            max_namespace_per_subsys=self.NS_PER_SUBSYSTEM,
        )
        self.k8s_utils.create_volume_snapshot_class(
            name=self.SNAPSHOT_CLASS_NAME,
        )

        self._run_mass_create_delete_test()

    # ── Fire-and-monitor helpers ──────────────────────────────────────────

    def _start_monitor(self, label, count_fn, interval=30):
        """Start a daemon thread that polls count_fn every *interval* seconds.

        Returns (stop_event, time_series) where time_series is a list of
        (elapsed_seconds, count) tuples populated by the background thread.
        """
        stop = threading.Event()
        series: list[tuple[float, int]] = []
        t0 = time.time()

        def _poll():
            while not stop.is_set():
                try:
                    count = count_fn()
                    elapsed = round(time.time() - t0)
                    series.append((elapsed, count))
                    self.logger.info(
                        f"[{label} monitor] count={count} at +{elapsed}s"
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[{label} monitor] query failed: {exc}"
                    )
                stop.wait(interval)

        threading.Thread(
            target=_poll, daemon=True, name=f"monitor-{label}",
        ).start()
        return stop, series

    def _count_bound_pvcs(self, prefix: str) -> int:
        """Count PVCs in Bound state whose names start with *prefix*."""
        ns = self.k8s_utils.namespace
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pvc -n {ns} --no-headers 2>/dev/null "
            f"| grep '^{prefix}' | grep -c ' Bound ' || echo 0",
            supress_logs=True,
        )
        try:
            return int(out.strip())
        except ValueError:
            return 0

    def _count_pvcs_by_prefix(self, prefix: str) -> int:
        """Count PVCs matching *prefix* (any state)."""
        ns = self.k8s_utils.namespace
        out, _ = self.k8s_utils._exec_kubectl(
            f"kubectl get pvc -n {ns} --no-headers 2>/dev/null "
            f"| grep -c '^{prefix}' || echo 0",
            supress_logs=True,
        )
        try:
            return int(out.strip())
        except ValueError:
            return 0

    def _fire_in_batches(self, items, fire_fn, label,
                         concurrent=20, pause=30, phase_timeout=14400,
                         stop_on_capacity=False):
        """Fire items in concurrent batches (fire-and-forget).

        Fires *concurrent* items at a time, waits *pause* seconds
        between batches.

        When *stop_on_capacity* is True, stops early once a full batch
        of capacity/terminal errors is detected (the system has hit its
        limit and further attempts are pointless).

        Returns (fired_items, error_count, capacity_errors).
        """
        deadline = time.time() + phase_timeout
        fired_items = []
        errors = 0
        capacity_errors = 0

        for batch_start in range(0, len(items), concurrent):
            if time.time() >= deadline:
                self.logger.warning(
                    f"[{label}] Phase timeout after "
                    f"{len(fired_items)}/{len(items)} fired"
                )
                break

            batch = items[batch_start:batch_start + concurrent]
            batch_capacity_errors = 0
            with ThreadPoolExecutor(max_workers=concurrent) as pool:
                futures = {pool.submit(fire_fn, item): item for item in batch}
                for f in as_completed(futures, timeout=120):
                    item = futures[f]
                    try:
                        f.result(timeout=60)
                        fired_items.append(item)
                    except Exception as exc:
                        errors += 1
                        if self._is_terminal_error(exc):
                            capacity_errors += 1
                            batch_capacity_errors += 1
                            self.logger.warning(
                                f"[{label}] Capacity/limit error: {exc}"
                            )
                        else:
                            self.logger.error(
                                f"[{label}] Fire failed: {exc}"
                            )

            done = min(batch_start + len(batch), len(items))
            self.logger.info(
                f"[{label}] Fired {done}/{len(items)} "
                f"(ok={len(fired_items)}, errors={errors}, "
                f"capacity={capacity_errors})"
            )

            # If an entire batch hit capacity errors, stop — system is full
            if (stop_on_capacity and batch_capacity_errors > 0
                    and batch_capacity_errors == len(batch)):
                self.logger.info(
                    f"[{label}] Full batch hit capacity limit — "
                    f"stopping after {len(fired_items)} successes"
                )
                break

            if batch_start + concurrent < len(items):
                time.sleep(pause)

        return fired_items, errors, capacity_errors

    def _bulk_wait_pvcs_bound(self, pvc_names, label="PVCs",
                               timeout=1800, poll_interval=30):
        """Wait for PVCs to reach Bound state via bulk kubectl query.

        Returns the set of PVC names that became Bound.
        """
        ns = self.k8s_utils.namespace
        deadline = time.time() + timeout
        target = set(pvc_names)
        bound = set()

        while time.time() < deadline and len(bound) < len(target):
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get pvc -n {ns} --no-headers "
                    f"-o custom-columns=NAME:.metadata.name,"
                    f"STATUS:.status.phase "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
                for line in (out or "").strip().splitlines():
                    parts = line.split()
                    if (len(parts) >= 2 and parts[1] == "Bound"
                            and parts[0] in target):
                        bound.add(parts[0])
            except Exception as exc:
                self.logger.warning(
                    f"[{label}] Bulk PVC query failed: {exc}"
                )

            pending = len(target) - len(bound)
            if pending > 0:
                self.logger.info(
                    f"[{label}] Bound wait: {len(bound)}/{len(target)} "
                    f"Bound, {pending} pending"
                )
                time.sleep(poll_interval)

        self.logger.info(
            f"[{label}] Bound wait done: {len(bound)}/{len(target)} Bound"
        )
        return bound

    def _log_time_series(self, label, series):
        """Log time-series data from a monitor."""
        if not series:
            return
        self.logger.info(f"[{label}] Time series ({len(series)} samples):")
        for elapsed, count in series:
            self.logger.info(f"  +{elapsed:>6}s: {count}")

    # ── Overflow / capacity error verification ────────────────────────────

    _CAPACITY_PATTERNS = [
        "max subsystems reached",
        "too many subsystems",
        "pool max size has reached",
        "pool max lvol size",
        "no space",
        "exceeded the max number of lvol",
    ]

    def _check_unbound_pvc_events(self, unbound_names, label, sample=10):
        """Check K8s events + web API logs for capacity errors on unbound PVCs.

        Samples up to *sample* unbound PVCs, inspects their K8s events,
        and greps the web API log on the mgmt node for matching capacity
        error strings.  Logs a summary indicating whether the system
        correctly rejected provisioning beyond its limit or if there is
        an unexpected failure.
        """
        if not unbound_names:
            return

        self.logger.info(
            f"[{label}] {len(unbound_names)} PVCs stayed Pending — "
            f"checking K8s events and web API logs for capacity errors"
        )
        capacity_confirmed = 0
        unknown_failures = 0

        # Check K8s events on a sample of unbound PVCs
        for pvc_name in unbound_names[:sample]:
            try:
                ns = self.k8s_utils.namespace
                events_out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get events -n {ns} "
                    f"--field-selector involvedObject.name={pvc_name} "
                    f"--sort-by=.lastTimestamp "
                    f"--request-timeout=30s 2>/dev/null || true",
                    supress_logs=True,
                )
                events_text = (events_out or "").lower()
                is_capacity = any(
                    pat in events_text for pat in self._CAPACITY_PATTERNS
                )
                if is_capacity:
                    capacity_confirmed += 1
                else:
                    unknown_failures += 1
                    self.logger.warning(
                        f"[{label}] Unbound PVC {pvc_name} — no "
                        f"capacity error in K8s events:\n"
                        f"{(events_out or '(none)').strip()}"
                    )
            except Exception:
                unknown_failures += 1

        # Also check web API logs on mgmt node for capacity errors
        try:
            mgmt_ip = self.mgmt_nodes[0]
            log_cmd = (
                "docker logs simplyblock-webapi 2>&1 | "
                "grep -iE 'max subsystems reached|pool max size|"
                "too many subsystems|no space' | tail -5"
            )
            log_out, _ = self.ssh_obj.exec_command(
                node=mgmt_ip, command=log_cmd, timeout=30,
            )
            if log_out and log_out.strip():
                self.logger.info(
                    f"[{label}] Web API capacity errors:\n"
                    f"{log_out.strip()}"
                )
                capacity_confirmed += 1
        except Exception as exc:
            self.logger.debug(
                f"[{label}] Could not check web API logs: {exc}"
            )

        sampled = min(len(unbound_names), sample)
        self.logger.info(
            f"[{label}] Overflow verification: sampled {sampled} "
            f"unbound PVCs + web API logs — "
            f"{capacity_confirmed} confirmed capacity error, "
            f"{unknown_failures} unknown/missing"
        )
        if capacity_confirmed > 0:
            self.logger.info(
                f"[{label}] System correctly refused provisioning "
                f"beyond capacity limit"
            )
        if unknown_failures > 0 and capacity_confirmed == 0:
            self.logger.warning(
                f"[{label}] {unknown_failures} unbound PVCs with no "
                f"recognizable capacity error — may indicate a bug "
                f"or timeout rather than overflow"
            )

    def _check_unready_snapshot_events(self, unready_names, label, sample=10):
        """Check K8s events + web API logs for errors on unready snapshots."""
        if not unready_names:
            return

        self.logger.info(
            f"[{label}] {len(unready_names)} snapshots not readyToUse — "
            f"checking K8s events and web API logs"
        )
        capacity_confirmed = 0
        unknown_failures = 0

        for vs_name in unready_names[:sample]:
            try:
                ns = self.k8s_utils.namespace
                events_out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get events -n {ns} "
                    f"--field-selector involvedObject.name={vs_name} "
                    f"--sort-by=.lastTimestamp "
                    f"--request-timeout=30s 2>/dev/null || true",
                    supress_logs=True,
                )
                events_text = (events_out or "").lower()
                is_capacity = any(
                    pat in events_text for pat in self._CAPACITY_PATTERNS
                )
                if is_capacity:
                    capacity_confirmed += 1
                else:
                    unknown_failures += 1
                    self.logger.warning(
                        f"[{label}] Unready snapshot {vs_name} — no "
                        f"capacity error in K8s events:\n"
                        f"{(events_out or '(none)').strip()}"
                    )
            except Exception:
                unknown_failures += 1

        # Check web API logs
        try:
            mgmt_ip = self.mgmt_nodes[0]
            log_cmd = (
                "docker logs simplyblock-webapi 2>&1 | "
                "grep -iE 'max subsystems reached|pool max size|"
                "too many subsystems|no space' | tail -5"
            )
            log_out, _ = self.ssh_obj.exec_command(
                node=mgmt_ip, command=log_cmd, timeout=30,
            )
            if log_out and log_out.strip():
                self.logger.info(
                    f"[{label}] Web API capacity errors:\n"
                    f"{log_out.strip()}"
                )
                capacity_confirmed += 1
        except Exception as exc:
            self.logger.debug(
                f"[{label}] Could not check web API logs: {exc}"
            )

        sampled = min(len(unready_names), sample)
        self.logger.info(
            f"[{label}] Snapshot failure check: sampled {sampled} "
            f"unready snapshots + web API logs — "
            f"{capacity_confirmed} confirmed capacity error, "
            f"{unknown_failures} unknown/missing"
        )
        if capacity_confirmed > 0:
            self.logger.info(
                f"[{label}] System correctly refused snapshot creation "
                f"beyond capacity limit"
            )
        if unknown_failures > 0 and capacity_confirmed == 0:
            self.logger.warning(
                f"[{label}] {unknown_failures} unready snapshots with no "
                f"recognizable capacity error — may indicate a bug "
                f"or timeout rather than a limit"
            )

    # ── Phase 1: Create PVCs ──────────────────────────────────────────────

    def _phase_1_create_lvols(self):
        total = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        self.logger.info(
            f"=== Phase 1: Create {total} PVCs "
            f"(batch={self.CREATE_CONCURRENT}, "
            f"pause={self.CREATE_BATCH_PAUSE}s) ==="
        )

        pvc_names = [
            f"mcd-pvc-{_rand_seq(5)}-{i:04d}" for i in range(total)
        ]

        # Start background monitor — counts Bound PVCs every MONITOR_INTERVAL
        stop_mon, series = self._start_monitor(
            "Phase 1",
            lambda: self._count_bound_pvcs("mcd-pvc"),
            self.MONITOR_INTERVAL,
        )

        try:
            # Fire all PVCs (fire-and-forget kubectl apply, no per-PVC wait)
            fired_items, errors, _ = self._fire_in_batches(
                pvc_names,
                lambda name: self.k8s_utils.create_pvc(
                    name, self.PVC_SIZE, self.STORAGE_CLASS_NAME,
                ),
                "Phase 1",
                concurrent=self.CREATE_CONCURRENT,
                pause=self.CREATE_BATCH_PAUSE,
                phase_timeout=self.PERSISTENT_PHASE_TIMEOUT,
            )

            # Bulk wait for PVCs to reach Bound
            self.logger.info(
                f"[Phase 1] {len(fired_items)} PVCs fired, "
                f"waiting for Bound..."
            )
            bound_names = self._bulk_wait_pvcs_bound(
                fired_items, label="Phase 1",
                timeout=self.BOUND_WAIT_TIMEOUT,
            )
        finally:
            stop_mon.set()

        self._time_series["phase_1_pvcs"] = series
        self._log_time_series("Phase 1", series)

        # Report any unbound PVCs and check for capacity errors
        unbound = len(fired_items) - len(bound_names)
        self.logger.info(
            f"[Phase 1] PVC creation summary: target={total}, "
            f"fired={len(fired_items)}, apply_errors={errors}, "
            f"bound={len(bound_names)}, unbound={unbound}"
        )
        if unbound > 0:
            self._check_unbound_pvc_events(
                [n for n in fired_items if n not in bound_names],
                "Phase 1",
            )

        # Populate registries with only Bound PVCs
        for pvc_name in bound_names:
            self._pvc_registry[pvc_name] = {"bound": True}
            self._lvol_registry[pvc_name] = {
                "id": pvc_name, "parent_name": None,
            }

    # ── Phase 2: FIO on 10% of PVCs ──────────────────────────────────────

    def _phase_2_fio_on_lvols(self):
        if not self._pvc_registry:
            return

        sample_size = min(
            max(1, math.ceil(
                len(self._pvc_registry) * self.FIO_SAMPLE_PERCENT / 100
            )),
            self.FIO_SAMPLE_MAX,
        )
        sample = random.sample(
            list(self._pvc_registry.keys()),
            min(sample_size, len(self._pvc_registry)),
        )
        self._fio_lvol_sample = set(sample)
        self.logger.info(
            f"=== Phase 2: FIO on {len(sample)} PVCs ==="
        )

        for pvc_name in sample:
            try:
                job_name = f"fio-mcd-{_rand_seq(6)}"
                cm_name = f"fio-cm-mcd-{_rand_seq(6)}"
                fio_cfg = self._build_simple_fio_config()
                self.k8s_utils.create_fio_job(
                    job_name=job_name,
                    pvc_name=pvc_name,
                    configmap_name=cm_name,
                    fio_config=fio_cfg,
                )
                self._fio_jobs[job_name] = pvc_name
                self._metrics["fio_lvol_started"] += 1
            except Exception as exc:
                self.logger.error(
                    f"[Phase 2] FIO Job failed for {pvc_name}: {exc}"
                )
                self._metrics["fio_lvol_failures"] += 1

    def _build_simple_fio_config(self):
        return (
            "[global]\n"
            f"size={self.FIO_SIZE}\n"
            "ioengine=libaio\n"
            "direct=1\n"
            f"iodepth={self.FIO_IODEPTH}\n"
            f"numjobs={self.FIO_NUMJOBS}\n"
            "rw=randrw\n"
            "bs=4k\n"
            "time_based=1\n"
            f"runtime={self.FIO_RUNTIME}\n"
            "verify=md5\n"
            "verify_fatal=1\n"
            "group_reporting=1\n"
            "\n"
            "[mcd_fio]\n"
            "directory=/spdkvol\n"
            "nrfiles=4\n"
        )

    # ── Phase 3: Create VolumeSnapshots ───────────────────────────────────

    def _phase_3_create_snapshots(self):
        snap_items = []
        # Round-robin: create snapshot round 0 for all PVCs, then round 1, etc.
        pvc_names = list(self._pvc_registry.keys())
        for s in range(self.SNAPSHOTS_PER_LVOL):
            for pvc_name in pvc_names:
                vs_name = f"vs-{pvc_name[-8:]}-{s:03d}"
                snap_items.append({
                    "vs_name": vs_name,
                    "pvc_name": pvc_name,
                })

        expected_snaps = len(snap_items)
        self.logger.info(
            f"=== Phase 3: Create {expected_snaps} VolumeSnapshots ==="
        )

        if self.PERSISTENT_RETRY:
            self.logger.info(
                f"[Phase 3] Persistent retry mode: will retry until "
                f"all {expected_snaps} snapshots created or terminal error"
            )
            ok, fail = self._batch_exec_persistent(
                snap_items, self._create_single_vs, "create_snapshots",
                batch_size=self.SNAPSHOT_BATCH_SIZE,
                phase_timeout=self.SNAPSHOT_PHASE_TIMEOUT,
            )
        else:
            snap_max_failures = max(self.MAX_FAILURES, expected_snaps // 10)
            ok, fail = self._batch_exec(
                snap_items, self._create_single_vs, "create_snapshots",
                batch_size=self.SNAPSHOT_BATCH_SIZE,
                stop_on_max_lvols=True,
                max_failures=snap_max_failures,
            )

    def _create_single_vs(self, params: dict):
        vs_name = params["vs_name"]
        pvc_name = params["pvc_name"]
        self.k8s_utils.create_volume_snapshot(
            vs_name, pvc_name, self.SNAPSHOT_CLASS_NAME
        )
        self._snapshot_registry[vs_name] = {
            "snap_id": vs_name,
            "parent_lvol": pvc_name,
        }
        self._snap_pvc_map[vs_name] = pvc_name

    def _verify_snapshots_ready(self, timeout: int = 900,
                                poll_interval: int = 30):
        """Verify snapshots actually reach readyToUse=true after fire-and-forget creation.

        Uses bulk kubectl queries to count ready snapshots across
        the entire registry, polling until all are ready or timeout.
        Prunes _snapshot_registry to only contain verified-ready snapshots
        so downstream phases (clone creation) work with real data.
        """
        if not self._snapshot_registry:
            return

        submitted = len(self._snapshot_registry)
        ns = self.k8s_utils.namespace
        self.logger.info(
            f"[Phase 3] Verifying {submitted} snapshots reach "
            f"readyToUse=true (timeout={timeout}s, poll={poll_interval}s)"
        )

        deadline = time.time() + timeout
        ready_names = set()

        while time.time() < deadline:
            # Bulk query: get all snapshots that are readyToUse=true
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get volumesnapshot -n {ns} "
                    f"-o jsonpath='{{range .items[?(@.status.readyToUse==true)]}}{{.metadata.name}}{{\"\\n\"}}{{end}}' "
                    f"2>/dev/null || true",
                    supress_logs=True,
                )
                current_ready = set()
                for line in (out or "").strip().splitlines():
                    name = line.strip()
                    if name and name in self._snapshot_registry:
                        current_ready.add(name)
                ready_names = current_ready
            except Exception as exc:
                self.logger.warning(
                    f"[Phase 3] Bulk snapshot query failed: {exc}"
                )

            self.logger.info(
                f"[Phase 3] Snapshot readiness: {len(ready_names)}/{submitted} "
                f"readyToUse=true"
            )

            if len(ready_names) >= submitted:
                break

            # Check if progress is stalling — if no new snapshots became
            # ready in the last poll, check snapshot-controller health
            time.sleep(poll_interval)

        # Prune registry to only verified-ready snapshots
        not_ready = set(self._snapshot_registry.keys()) - ready_names
        if not_ready:
            for name in not_ready:
                del self._snapshot_registry[name]
                self._snap_pvc_map.pop(name, None)

            pct = len(ready_names) / submitted * 100
            msg = (
                f"[Phase 3] {len(ready_names)}/{submitted} snapshots "
                f"verified readyToUse ({pct:.1f}%). "
                f"Pruned {len(not_ready)} unready snapshots from registry."
            )
            if len(ready_names) == 0:
                raise RuntimeError(
                    f"{msg} snapshot-controller may be down, aborting"
                )
            self.logger.warning(msg)
            self._soft_failures.append(msg)

            # Check why unready snapshots failed — K8s events + web API
            self._check_unready_snapshot_events(
                list(not_ready), "Phase 3"
            )
        else:
            self.logger.info(
                f"[Phase 3] All {len(ready_names)} snapshots verified "
                f"readyToUse=true"
            )

    # ── Phase 4: Delete PVCs (free subsystem slots for clones) ──────────

    def _phase_4_delete_lvols(self):
        # Kill FIO jobs (started in Phase 2, left running)
        for job_name in list(self._fio_jobs.keys()):
            try:
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete job {job_name} "
                    f"-n {self.k8s_utils.namespace} "
                    f"--force --grace-period=0 2>/dev/null || true"
                )
            except Exception:
                pass
        sleep_n_sec(10)

        pvc_names = list(self._pvc_registry.keys())
        initial_count = len(pvc_names)
        self.logger.info(
            f"=== Phase 4: Delete {initial_count} PVCs "
            f"(batch={self.DELETE_CONCURRENT}, "
            f"pause={self.DELETE_BATCH_PAUSE}s) ==="
        )

        # Start background monitor — tracks remaining PVCs
        stop_mon, series = self._start_monitor(
            "Phase 4",
            lambda: self._count_pvcs_by_prefix("mcd-pvc"),
            self.MONITOR_INTERVAL,
        )

        try:
            fired_items, errors, _ = self._fire_in_batches(
                pvc_names,
                self._delete_single_pvc,
                "Phase 4",
                concurrent=self.DELETE_CONCURRENT,
                pause=self.DELETE_BATCH_PAUSE,
                phase_timeout=self.DELETE_PHASE_TIMEOUT,
            )
        finally:
            stop_mon.set()

        self._time_series["phase_4_delete_pvcs"] = series
        self._log_time_series("Phase 4", series)
        self._metrics["lvols_deleted"] = len(fired_items)

    # ── Phase 5: Create clone PVCs from VolumeSnapshots ───────────────────

    def _phase_5_create_clones(self):
        if not self._snapshot_registry:
            self.logger.info("[Phase 5] No snapshots — skipping")
            return

        snap_list = list(self._snapshot_registry.keys())
        # Create more clones than cluster capacity to verify the system
        # handles overflow gracefully (proper errors, no corruption).
        cluster_capacity = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        overflow = max(10, cluster_capacity // 5)  # 20% extra past the limit
        target_clones = cluster_capacity + overflow
        self.logger.info(
            f"=== Phase 5: Create {target_clones} clones from "
            f"{len(snap_list)} snapshots "
            f"(capacity={cluster_capacity}, overflow={overflow}, "
            f"batch={self.CREATE_CONCURRENT}, "
            f"pause={self.CREATE_BATCH_PAUSE}s) ==="
        )

        clone_items = []
        for idx in range(target_clones):
            vs_name = snap_list[idx % len(snap_list)]
            clone_pvc = f"clone-pvc-{_rand_seq(5)}-{idx:06d}"
            clone_items.append({"clone_pvc": clone_pvc, "vs_name": vs_name})

        # Start background monitor — counts Bound clone PVCs
        stop_mon, series = self._start_monitor(
            "Phase 5",
            lambda: self._count_bound_pvcs("clone-pvc"),
            self.MONITOR_INTERVAL,
        )

        try:
            # Fire clone PVCs past capacity — kubectl apply always
            # succeeds; overflow is detected as PVCs stuck in Pending.
            fired_items, errors, _ = self._fire_in_batches(
                clone_items,
                lambda params: self.k8s_utils.create_clone_pvc(
                    params["clone_pvc"], self.PVC_SIZE,
                    self.STORAGE_CLASS_NAME, params["vs_name"],
                ),
                "Phase 5",
                concurrent=self.CREATE_CONCURRENT,
                pause=self.CREATE_BATCH_PAUSE,
                phase_timeout=self.CLONE_PHASE_TIMEOUT,
            )

            # Bulk wait for clone PVCs to reach Bound
            fired_names = [p["clone_pvc"] for p in fired_items]
            self.logger.info(
                f"[Phase 5] {len(fired_names)} clone PVCs fired "
                f"({errors} create errors), waiting for Bound..."
            )
            bound_names = self._bulk_wait_pvcs_bound(
                fired_names, label="Phase 5",
                timeout=self.BOUND_WAIT_TIMEOUT,
            )
        finally:
            stop_mon.set()

        self._time_series["phase_5_clones"] = series
        self._log_time_series("Phase 5", series)

        # Report overflow behavior — in K8s, capacity overflow shows up
        # as PVCs stuck in Pending (kubectl apply always succeeds, the
        # CSI driver rejects provisioning asynchronously).
        unbound = len(fired_items) - len(bound_names)
        self.logger.info(
            f"[Phase 5] Clone creation summary: "
            f"target={target_clones} (capacity={cluster_capacity} + "
            f"overflow={overflow}), fired={len(fired_items)}, "
            f"apply_errors={errors}, bound={len(bound_names)}, "
            f"unbound={unbound}"
        )
        if unbound > 0:
            unbound_names = [
                p["clone_pvc"] for p in fired_items
                if p["clone_pvc"] not in bound_names
            ]
            self._check_unbound_pvc_events(unbound_names, "Phase 5")

        # Build lookup from fired items for registry population
        name_to_snap = {
            p["clone_pvc"]: p["vs_name"] for p in fired_items
        }
        for clone_pvc in bound_names:
            vs_name = name_to_snap.get(clone_pvc, "")
            self._clone_registry[clone_pvc] = {
                "clone_id": clone_pvc, "snap_name": vs_name,
            }
            self._clone_pvc_registry[clone_pvc] = {"vs_name": vs_name}

    # ── Phase 6: FIO on 10% of clone PVCs ─────────────────────────────────

    def _phase_6_fio_on_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 6] No clones — skipping")
            return

        sample_size = min(
            max(1, math.ceil(
                len(self._clone_registry) * self.FIO_SAMPLE_PERCENT / 100
            )),
            self.FIO_SAMPLE_MAX,
        )
        sample = random.sample(
            list(self._clone_registry.keys()),
            min(sample_size, len(self._clone_registry)),
        )
        self._fio_clone_sample = set(sample)
        self.logger.info(
            f"=== Phase 6: FIO on {len(sample)} clone PVCs "
            f"(concurrent={self.FIO_CONCURRENT}, "
            f"pause={self.FIO_BATCH_PAUSE}s) ==="
        )

        clone_fio_jobs = {}
        deadline = time.monotonic() + self.FIO_PHASE_TIMEOUT
        concurrent = self.FIO_CONCURRENT
        batch_pause = self.FIO_BATCH_PAUSE

        for batch_start in range(0, len(sample), concurrent):
            if time.monotonic() >= deadline:
                self.logger.warning(
                    f"[Phase 6] Phase timeout ({self.FIO_PHASE_TIMEOUT}s) "
                    f"reached after {len(clone_fio_jobs)} FIO jobs created "
                    f"(of {len(sample)})"
                )
                break

            batch = sample[batch_start:batch_start + concurrent]
            batch_ok = 0
            batch_fail = 0

            with ThreadPoolExecutor(max_workers=concurrent) as pool:
                futures = {}
                for clone_pvc in batch:
                    job_name = f"fio-clone-{_rand_seq(6)}"
                    cm_name = f"fio-cm-clone-{_rand_seq(6)}"
                    fio_cfg = self._build_simple_fio_config()
                    f = pool.submit(
                        self.k8s_utils.create_fio_job,
                        job_name=job_name,
                        pvc_name=clone_pvc,
                        configmap_name=cm_name,
                        fio_config=fio_cfg,
                    )
                    futures[f] = (job_name, clone_pvc)

                for f in as_completed(futures, timeout=120):
                    job_name, clone_pvc = futures[f]
                    try:
                        f.result(timeout=60)
                        clone_fio_jobs[job_name] = clone_pvc
                        self._metrics["fio_clone_started"] += 1
                        batch_ok += 1
                    except Exception as exc:
                        self.logger.error(
                            f"[Phase 6] FIO Job failed for "
                            f"{clone_pvc}: {exc}"
                        )
                        self._metrics["fio_clone_failures"] += 1
                        batch_fail += 1

            total_done = batch_start + len(batch)
            self.logger.info(
                f"[Phase 6] FIO batch progress: {total_done}/{len(sample)} "
                f"(batch ok={batch_ok} fail={batch_fail}, "
                f"cumulative started={self._metrics['fio_clone_started']})"
            )

            # Pause between batches to avoid overwhelming etcd
            if batch_start + concurrent < len(sample):
                time.sleep(batch_pause)

        # Wait for FIO jobs
        self.logger.info(
            f"[Phase 6] Waiting for {len(clone_fio_jobs)} FIO jobs"
        )
        timeout = self.FIO_RUNTIME + 300
        for job_name in clone_fio_jobs:
            try:
                self.k8s_utils.wait_job_complete(job_name, timeout=timeout)
            except Exception as exc:
                self.logger.warning(
                    f"[Phase 6] FIO job {job_name} wait failed: {exc}"
                )

    # ── Phase 7: Delete clone PVCs ────────────────────────────────────────

    def _phase_7_delete_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 7] No clones — skipping")
            return

        clone_names = list(self._clone_registry.keys())
        self.logger.info(
            f"=== Phase 7: Delete {len(clone_names)} clone PVCs "
            f"(batch={self.DELETE_CONCURRENT}, "
            f"pause={self.DELETE_BATCH_PAUSE}s) ==="
        )

        # Start background monitor — tracks remaining clone PVCs
        stop_mon, series = self._start_monitor(
            "Phase 7",
            lambda: self._count_pvcs_by_prefix("clone-pvc"),
            self.MONITOR_INTERVAL,
        )

        try:
            fired_items, errors, _ = self._fire_in_batches(
                clone_names,
                self._delete_single_pvc,
                "Phase 7",
                concurrent=self.DELETE_CONCURRENT,
                pause=self.DELETE_BATCH_PAUSE,
                phase_timeout=self.DELETE_PHASE_TIMEOUT,
            )
        finally:
            stop_mon.set()

        self._time_series["phase_7_delete_clones"] = series
        self._log_time_series("Phase 7", series)
        self._metrics["clones_deleted"] = len(fired_items)

    # ── Phase 8: Delete VolumeSnapshots ───────────────────────────────────

    def _phase_8_delete_snapshots(self):
        if not self._snapshot_registry:
            self.logger.info("[Phase 8] No snapshots — skipping")
            return

        self.logger.info(
            f"=== Phase 8: Delete {len(self._snapshot_registry)} "
            f"VolumeSnapshots ==="
        )

        vs_names = list(self._snapshot_registry.keys())
        ok, fail = self._batch_exec(
            vs_names,
            self._delete_single_vs,
            "delete_snapshots",
            max_workers=self.DELETE_MAX_WORKERS,
            batch_size=self.SNAPSHOT_BATCH_SIZE,
            max_failures=len(vs_names),
        )
        self._metrics["snapshots_deleted"] = ok

    def _delete_single_vs(self, vs_name: str):
        ns = self.k8s_utils.namespace
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete volumesnapshot {vs_name} -n {ns} "
                f"--ignore-not-found=true 2>/dev/null || true"
            )
        except Exception as exc:
            self.logger.warning(f"[delete_vs] {vs_name}: {exc}")

    def _delete_single_pvc(self, pvc_name: str):
        try:
            self.k8s_utils.delete_pvc(pvc_name)
        except Exception as exc:
            self.logger.warning(f"[delete_pvc] {pvc_name}: {exc}")

    # ── Cleanup safety net ─────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Cleanup ===")
        ns = self.k8s_utils.namespace
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete jobs -n {ns} "
                f"-l app=fio-benchmark --force --grace-period=0 "
                f"2>/dev/null || true"
            )
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_clones: {exc}")
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete volumesnapshots --all -n {ns} "
                f"2>/dev/null || true"
            )
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_snapshots: {exc}")
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_lvols: {exc}")
        try:
            self.k8s_utils._exec_kubectl(
                f"kubectl delete pvc --all -n {ns} "
                f"2>/dev/null || true"
            )
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] delete_all_storage_pools: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
#  Concrete classes: 4 ratios × 2 modes = 8 test classes
# ─────────────────────────────────────────────────────────────────────────────

# Docker variants
#
# Naming: NxM = N namespaces per subsystem × M subsystems.
# All lvols pinned to a single storage node.
# If 500 subsystems doesn't fit, use 300x10 as the largest test.

class MassCreateDelete_1x500_Docker(_MassCreateDeleteDocker):
    """1 ns/sub × 500 subsystems = 500 standalone lvols."""
    NUM_SUBSYSTEMS = 500
    NS_PER_SUBSYSTEM = 1


class MassCreateDelete_30x100_Docker(_MassCreateDeleteDocker):
    """30 ns/sub × 100 subsystems = 3000 lvols."""
    NUM_SUBSYSTEMS = 100
    NS_PER_SUBSYSTEM = 30


class MassCreateDelete_300x10_Docker(_MassCreateDeleteDocker):
    """300 ns/sub × 10 subsystems = 3000 lvols."""
    NUM_SUBSYSTEMS = 10
    NS_PER_SUBSYSTEM = 300


class MassCreateDelete_500x1_Docker(_MassCreateDeleteDocker):
    """500 ns/sub × 1 subsystem = 500 lvols (1 parent + 499 children)."""
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 500


class MassCreateDelete_3000x1_Docker(_MassCreateDeleteDocker):
    """3000 ns/sub × 1 subsystem = 3000 lvols."""
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000


# K8s variants

class MassCreateDelete_1x500_K8s(_MassCreateDeleteK8s):
    """1 ns/sub × 500 subsystems = 500 PVCs."""
    NUM_SUBSYSTEMS = 500
    NS_PER_SUBSYSTEM = 1


class MassCreateDelete_30x100_K8s(_MassCreateDeleteK8s):
    """30 ns/sub × 100 subsystems = 3000 PVCs."""
    NUM_SUBSYSTEMS = 100
    NS_PER_SUBSYSTEM = 30


class MassCreateDelete_300x10_K8s(_MassCreateDeleteK8s):
    """300 ns/sub × 10 subsystems = 3000 PVCs."""
    NUM_SUBSYSTEMS = 10
    NS_PER_SUBSYSTEM = 300


class MassCreateDelete_500x1_K8s(_MassCreateDeleteK8s):
    """500 ns/sub × 1 subsystem = 500 PVCs (1 parent + 499 children)."""
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 500


class MassCreateDelete_3000x1_K8s(_MassCreateDeleteK8s):
    """3000 ns/sub × 1 subsystem = 3000 PVCs."""
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000


# ─────────────────────────────────────────────────────────────────────────────
#  Persistent retry variants: retry failed items until all expected entities
#  are created or a terminal error (capacity / subsystem limit) is hit.
#  Same 8 phases, same ratios, same parallelism (20 threads).
# ─────────────────────────────────────────────────────────────────────────────

# Docker persistent variants

class MassCreateDeletePersistent_1x500_Docker(_MassCreateDeleteDocker):
    """1 ns/sub × 500 subsystems = 500 standalone lvols (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 500
    NS_PER_SUBSYSTEM = 1


class MassCreateDeletePersistent_30x100_Docker(_MassCreateDeleteDocker):
    """30 ns/sub × 100 subsystems = 3000 lvols (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 100
    NS_PER_SUBSYSTEM = 30


class MassCreateDeletePersistent_300x10_Docker(_MassCreateDeleteDocker):
    """300 ns/sub × 10 subsystems = 3000 lvols (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 10
    NS_PER_SUBSYSTEM = 300


class MassCreateDeletePersistent_500x1_Docker(_MassCreateDeleteDocker):
    """500 ns/sub × 1 subsystem = 500 lvols, 1 parent + 499 children (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 500


class MassCreateDeletePersistent_3000x1_Docker(_MassCreateDeleteDocker):
    """3000 ns/sub × 1 subsystem = 3000 lvols (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000


# K8s persistent variants

class MassCreateDeletePersistent_1x500_K8s(_MassCreateDeleteK8s):
    """1 ns/sub × 500 subsystems = 500 PVCs (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 500
    NS_PER_SUBSYSTEM = 1


class MassCreateDeletePersistent_30x100_K8s(_MassCreateDeleteK8s):
    """30 ns/sub × 100 subsystems = 3000 PVCs (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 100
    NS_PER_SUBSYSTEM = 30


class MassCreateDeletePersistent_300x10_K8s(_MassCreateDeleteK8s):
    """300 ns/sub × 10 subsystems = 3000 PVCs (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 10
    NS_PER_SUBSYSTEM = 300


class MassCreateDeletePersistent_500x1_K8s(_MassCreateDeleteK8s):
    """500 ns/sub × 1 subsystem = 500 PVCs, 1 parent + 499 children (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 500


class MassCreateDeletePersistent_3000x1_K8s(_MassCreateDeleteK8s):
    """3000 ns/sub × 1 subsystem = 3000 PVCs (persistent retry)."""
    PERSISTENT_RETRY = True
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000
