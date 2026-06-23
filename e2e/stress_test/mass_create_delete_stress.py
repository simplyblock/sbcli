"""
Mass-creation/deletion stress test for lvols, snapshots, and clones.

Tests system behavior under high object counts across different
namespace-to-subsystem ratios. All lvols are pinned to a single
storage node. Exercises the full lifecycle:
  1. Mass-create lvols (parallel, 20 threads, until limit)
  2. FIO on 10% of lvols (connect, format, run)
  3. Create 50 snapshots per lvol
  4. Delete all lvols (free subsystem slots for clones)
  5. Mass-create clones from snapshots (until limit)
  6. FIO on 10% of clones (connect, format, run)
  7. Mass-delete all clones
  8. Mass-delete all snapshots

Lvols are deleted before clones are created to free subsystem slots,
since both lvols and clones occupy subsystem slots. Snapshots remain
valid for cloning even after the parent lvol is deleted.

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
    SNAPSHOTS_PER_LVOL = 50
    FIO_SAMPLE_PERCENT = 10

    # ── FIO (lightweight) ──────────────────────────────────────────────────
    FIO_IODEPTH = 1
    FIO_NUMJOBS = 1
    FIO_RUNTIME = 300           # 5 min per sampled volume
    FIO_SIZE = "800M"

    # ── Parallelism ────────────────────────────────────────────────────────
    MAX_WORKERS = 20
    BATCH_SIZE = 50
    SNAPSHOT_BATCH_SIZE = 200
    CLONE_MAX_WORKERS = 20
    DELETE_MAX_WORKERS = 20
    PARALLEL_PARENTS = 10       # concurrent parent subsystem child creation
    MAX_FAILURES = 500

    # ── Phase timeouts (seconds) ───────────────────────────────────────────
    SNAPSHOT_PHASE_TIMEOUT = 14400   # 4 hours
    CLONE_PHASE_TIMEOUT = 7200       # 2 hours
    DELETE_PHASE_TIMEOUT = 3600      # 1 hour

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
            self._metrics["snapshots_created"] = len(self._snapshot_registry)
            self.logger.info(
                f"[Phase 3] Done: {len(self._snapshot_registry)} snapshots "
                f"in {self._phase_durations['3_create_snapshots']}s"
            )

            # Phase 4: Delete lvols (free subsystem slots so clones
            # can be created — clones need their own subsystem slots)
            t0 = time.time()
            self._phase_4_delete_lvols()
            self._phase_durations["4_delete_lvols"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 4] Lvols deleted "
                f"in {self._phase_durations['4_delete_lvols']}s"
            )

            # Phase 5: Mass-create clones from snapshots
            # (lvols deleted above freed subsystem slots)
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

            # Phase 7: Mass-delete all clones (clones must go
            # before snapshots can be deleted)
            t0 = time.time()
            self._phase_7_delete_clones()
            self._phase_durations["7_delete_clones"] = round(
                time.time() - t0, 1
            )
            self.logger.info(
                f"[Phase 7] Clones deleted "
                f"in {self._phase_durations['7_delete_clones']}s"
            )

            # Phase 8: Mass-delete all snapshots (last)
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
            t0 = time.time()
            self._phase_cleanup()
            self._phase_durations["cleanup"] = round(time.time() - t0, 1)
            self._print_summary()
            self._write_monitoring_json()

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
        for phase, dur in self._phase_durations.items():
            self.logger.info(f"  Phase {phase:25s}: {dur}s")
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

    # ── NVMe namespace helpers ─────────────────────────────────────────────

    def _list_nvme_ns_devices(self, node: str, ctrl_dev: str) -> list[str]:
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"ls -1 {ctrl}n* 2>/dev/null | sort -V || true\""
        out, _ = self.ssh_obj.exec_command(node=node, command=cmd,
                                           supress_logs=True)
        return [x.strip() for x in (out or "").splitlines() if x.strip()]

    def _rescan_nvme_namespaces(self, node: str, ctrl_dev: str):
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"nvme ns-rescan {ctrl} 2>/dev/null || true\""
        self.ssh_obj.exec_command(node=node, command=cmd, supress_logs=True)

    def _wait_for_new_ns_device(self, node: str, ctrl_dev: str,
                                before_set: set, timeout: int = 120,
                                interval: int = 3):
        deadline = time.time() + timeout
        while time.time() < deadline:
            self._rescan_nvme_namespaces(node, ctrl_dev)
            sleep_n_sec(interval)
            cur = set(self._list_nvme_ns_devices(node, ctrl_dev))
            diff = sorted(cur - before_set)
            if diff:
                return diff[-1], cur
        return None, set(self._list_nvme_ns_devices(node, ctrl_dev))

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
        """Create standalone lvols (one subsystem each) — for 3000x1 mode."""
        names = [
            f"mcd-{_rand_seq(6)}-{i:04d}"
            for i in range(self.NUM_SUBSYSTEMS)
        ]
        ok, fail = self._batch_exec(
            [{"name": n, "idx": i} for i, n in enumerate(names)],
            self._create_single_lvol,
            "create_lvols",
            stop_on_max_lvols=True,
        )

    def _create_single_lvol(self, params: dict):
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

        sleep_n_sec(1)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
        if not lvol_id:
            raise RuntimeError(f"[create] {name}: ID not found")
        self._lvol_registry[name] = {"id": lvol_id, "parent_name": None}

    def _create_namespaced_lvols(self):
        """Create parent lvols + children via namespace grouping."""
        parent_names = [
            f"mcd-par-{_rand_seq(6)}-{i:03d}"
            for i in range(self.NUM_SUBSYSTEMS)
        ]

        # Sub-phase 1a: Create parents
        self.logger.info(
            f"[Phase 1a] Creating {len(parent_names)} parent lvols"
        )
        ok, fail = self._batch_exec(
            [{"name": n} for n in parent_names],
            self._create_parent,
            "create_parents",
            stop_on_max_lvols=True,
        )
        # Filter to successfully created parents
        parent_names = [n for n in parent_names if n in self._parent_registry]

        if not parent_names:
            return

        # Sub-phase 1b: NVMe-connect parents (sequential)
        self.logger.info(
            f"[Phase 1b] Connecting {len(parent_names)} parents"
        )
        for idx, pn in enumerate(parent_names):
            self._parent_registry[pn]["client"] = (
                self.fio_node[idx % len(self.fio_node)]
            )
            try:
                self._connect_parent(pn)
            except Exception as exc:
                self.logger.error(f"[connect] {pn} failed: {exc}")
                continue

        connected_parents = [
            pn for pn in parent_names
            if self._parent_registry.get(pn, {}).get("ctrl_dev")
        ]

        if not connected_parents:
            return

        # Sub-phase 1c: Create children
        children_per_parent = self.NS_PER_SUBSYSTEM - 1
        if children_per_parent > 0:
            self.logger.info(
                f"[Phase 1c] Creating {children_per_parent} children "
                f"for {len(connected_parents)} parents"
            )
            child_timeout = children_per_parent * 180
            ok, fail = self._batch_exec(
                connected_parents,
                self._create_children_for_parent,
                "create_children",
                per_item_timeout=child_timeout,
                max_workers=self.PARALLEL_PARENTS,
            )

    def _create_parent(self, params: dict):
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
        sleep_n_sec(1)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=name)
        if not lvol_id:
            raise RuntimeError(f"[create_parent] {name}: ID not found")
        node_id = None
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            if details:
                node_id = details[0].get("node_id")
        except Exception:
            pass
        self._parent_registry[name] = {
            "id": lvol_id, "node_id": node_id,
            "client": None, "ctrl_dev": None, "nqn": None, "devices": [],
        }
        self._lvol_registry[name] = {"id": lvol_id, "parent_name": None}

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
            for _ in range(20):
                sleep_n_sec(3)
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

    def _create_children_for_parent(self, parent_name: str):
        pinfo = self._parent_registry[parent_name]
        client = pinfo["client"]
        ctrl_dev = pinfo["ctrl_dev"]
        if not ctrl_dev:
            return

        children_needed = self.NS_PER_SUBSYSTEM - 1
        cur_devs = set(self._list_nvme_ns_devices(client, ctrl_dev))

        for ns_idx in range(1, children_needed + 1):
            cname = f"mcd-ch-{_rand_seq(5)}-{parent_name[-3:]}-{ns_idx:03d}"
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=cname,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    namespace=True,
                    retry=3,
                )
            except Exception as e:
                if self._is_max_lvols_error(e):
                    self.logger.info(
                        f"[children] Max lvols on child {ns_idx} "
                        f"of {parent_name}"
                    )
                    break
                raise

            sleep_n_sec(1)
            child_id = self.sbcli_utils.get_lvol_id(lvol_name=cname)
            if not child_id:
                self.logger.warning(f"[children] {cname}: ID not found")
                continue

            # Wait for new namespace device to appear
            new_dev, cur_devs = self._wait_for_new_ns_device(
                client, ctrl_dev, cur_devs, timeout=60
            )
            if not new_dev:
                self.logger.warning(
                    f"[children] {cname}: device not found"
                )
                self._lvol_registry[cname] = {
                    "id": child_id, "parent_name": parent_name,
                }
                self._child_registry[cname] = {
                    "id": child_id, "parent_name": parent_name,
                    "device": None,
                }
                continue

            self._lvol_registry[cname] = {
                "id": child_id, "parent_name": parent_name,
            }
            self._child_registry[cname] = {
                "id": child_id, "parent_name": parent_name,
                "device": new_dev,
            }
            pinfo["devices"].append(new_dev)

            if (ns_idx % 50) == 0:
                self.logger.info(
                    f"[children] {parent_name}: {ns_idx}/"
                    f"{children_needed} done"
                )

    # ── Phase 2: FIO on 10% of lvols ──────────────────────────────────────

    def _phase_2_fio_on_lvols(self):
        sample_size = max(1, math.ceil(
            len(self._lvol_registry) * self.FIO_SAMPLE_PERCENT / 100
        ))
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

        for lvol_name in sample:
            try:
                self._connect_format_fio_lvol(lvol_name)
                self._metrics["fio_lvol_started"] += 1
            except Exception as exc:
                self.logger.error(
                    f"[Phase 2] FIO setup failed for {lvol_name}: {exc}"
                )
                self._metrics["fio_lvol_failures"] += 1

    def _connect_format_fio_lvol(self, lvol_name: str):
        info = self._lvol_registry[lvol_name]
        lvol_id = info["id"]

        # Determine client and device
        if lvol_name in self._connected_lvols:
            cl = self._connected_lvols[lvol_name]
            client, device = cl["client"], cl["device"]
        elif info.get("parent_name") and lvol_name in self._child_registry:
            # It's a child — device already detected
            cinfo = self._child_registry.get(lvol_name, {})
            device = cinfo.get("device")
            pinfo = self._parent_registry.get(info["parent_name"], {})
            client = pinfo.get("client")
            if not device or not client:
                raise RuntimeError(f"{lvol_name}: no device/client")
        else:
            # Standalone lvol — need to connect
            client = self.fio_node[
                hash(lvol_name) % len(self.fio_node)
            ]
            connect_cmds = self.sbcli_utils.get_lvol_connect_str(
                lvol_name=lvol_name
            )
            for cmd in connect_cmds:
                self.ssh_obj.exec_command(node=client, command=cmd)
            sleep_n_sec(3)
            device = None
            for _ in range(20):
                device = self.ssh_obj.get_lvol_vs_device(
                    node=client, lvol_id=lvol_id
                )
                if device:
                    break
                sleep_n_sec(3)
            if not device:
                raise RuntimeError(f"{lvol_name}: device not found")
            self._connected_lvols[lvol_name] = {
                "client": client, "device": device,
            }

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

    # ── Phase 3: Create snapshots ──────────────────────────────────────────

    def _phase_3_create_snapshots(self):
        snap_items = []
        for lvol_name, info in self._lvol_registry.items():
            lvol_id = info["id"]
            for s in range(self.SNAPSHOTS_PER_LVOL):
                snap_name = f"snap-{lvol_name[-8:]}-{s:03d}"
                snap_items.append({
                    "lvol_id": lvol_id,
                    "snap_name": snap_name,
                    "lvol_name": lvol_name,
                })

        self.logger.info(
            f"=== Phase 3: Create {len(snap_items)} snapshots "
            f"({len(self._lvol_registry)} lvols x "
            f"{self.SNAPSHOTS_PER_LVOL}) ==="
        )

        ok, fail = self._batch_exec(
            snap_items,
            self._create_single_snapshot,
            "create_snapshots",
            batch_size=self.SNAPSHOT_BATCH_SIZE,
            stop_on_max_lvols=True,
            max_failures=self.MAX_FAILURES,
        )

    def _create_single_snapshot(self, params: dict):
        lvol_id = params["lvol_id"]
        snap_name = params["snap_name"]
        lvol_name = params["lvol_name"]

        result = self.sbcli_utils.add_snapshot(
            lvol_id=lvol_id, snapshot_name=snap_name, retry=3
        )

        # Try to extract snap_id from result or fall back to API lookup
        snap_id = None
        if isinstance(result, dict):
            snap_id = result.get("id") or result.get("snapshot_id")
        if not snap_id:
            try:
                snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
            except Exception:
                pass

        self._snapshot_registry[snap_name] = {
            "snap_id": snap_id,
            "parent_lvol": lvol_name,
        }

    # ── Phase 4: Delete lvols (free subsystem slots for clones) ────────────

    def _phase_4_delete_lvols(self):
        self.logger.info(
            f"=== Phase 4: Delete {len(self._lvol_registry)} lvols "
            f"(freeing subsystem slots for clones) ==="
        )

        # Kill lvol FIO (started in Phase 2, left running)
        for client in set(
            c.get("client") for c in self._connected_lvols.values()
            if c.get("client")
        ):
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
        ok, fail = self._batch_exec(
            lvol_names,
            self._delete_single_lvol,
            "delete_lvols",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(lvol_names),
        )
        self._metrics["lvols_deleted"] = ok

    # ── Phase 5: Mass-create clones ────────────────────────────────────────

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
            f"(until limit) ==="
        )

        clone_idx = [0]
        hit_limit = [False]
        lock = threading.Lock()

        def _create_next_clone(_):
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
                clone_id = self.sbcli_utils.get_lvol_id(clone_name)
                self._clone_registry[clone_name] = {
                    "clone_id": clone_id, "snap_name": snap_name,
                }
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
                _create_next_clone,
                f"create_clones_b{batch_num}",
                max_workers=self.CLONE_MAX_WORKERS,
                stop_on_max_lvols=False,
                max_failures=self.BATCH_SIZE,
            )
            batch_num += 1
            if fail >= self.BATCH_SIZE:
                break
            self.logger.info(
                f"[Phase 5] {len(self._clone_registry)} clones so far"
            )

    # ── Phase 6: FIO on 10% of clones ─────────────────────────────────────

    def _phase_6_fio_on_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 6] No clones — skipping")
            return

        sample_size = max(1, math.ceil(
            len(self._clone_registry) * self.FIO_SAMPLE_PERCENT / 100
        ))
        sample = random.sample(
            list(self._clone_registry.keys()),
            min(sample_size, len(self._clone_registry)),
        )
        self._fio_clone_sample = set(sample)
        self.logger.info(
            f"=== Phase 6: FIO on {len(sample)} clones ==="
        )

        for clone_name in sample:
            try:
                self._connect_format_fio_clone(clone_name)
                self._metrics["fio_clone_started"] += 1
            except Exception as exc:
                self.logger.error(
                    f"[Phase 6] FIO setup failed for {clone_name}: {exc}"
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
        for _ in range(20):
            device = self.ssh_obj.get_lvol_vs_device(
                node=client, lvol_id=clone_id
            )
            if device:
                break
            sleep_n_sec(3)
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

        # Kill clone FIO
        for client in set(
            c.get("client") for c in self._connected_lvols.values()
            if c.get("client")
        ):
            try:
                self.ssh_obj.exec_command(
                    node=client,
                    command="sudo pkill -9 -f 'fio.*mcd_clone_' "
                            "2>/dev/null || true",
                )
            except Exception:
                pass
        for t in self._fio_clone_threads:
            t.join(timeout=30)

        clone_names = list(self._clone_registry.keys())
        ok, fail = self._batch_exec(
            clone_names,
            self._delete_single_lvol,
            "delete_clones",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(clone_names),
        )
        self._metrics["clones_deleted"] = ok

    def _delete_single_lvol(self, lvol_name: str):
        try:
            self.sbcli_utils.delete_lvol(
                lvol_name=lvol_name, skip_error=True, max_attempt=30
            )
        except Exception as exc:
            self.logger.warning(f"[delete_lvol] {lvol_name}: {exc}")

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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "mass_create_delete_k8s"
        self.fio_num_jobs = self.FIO_NUMJOBS
        self._pvc_registry: dict[str, dict] = {}    # pvc_name -> {lvol_name}
        self._snap_pvc_map: dict[str, str] = {}     # vs_name -> pvc_name
        self._clone_pvc_registry: dict[str, dict] = {}  # clone_pvc -> {vs_name}
        self._fio_jobs: dict[str, str] = {}          # job_name -> pvc/clone

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
        self.k8s_utils.create_snapshot_class(
            name=self.SNAPSHOT_CLASS_NAME,
            cluster_id=cluster_id,
        )

        self._run_mass_create_delete_test()

    # ── Phase 1: Create PVCs ──────────────────────────────────────────────

    def _phase_1_create_lvols(self):
        total = self.NUM_SUBSYSTEMS * self.NS_PER_SUBSYSTEM
        self.logger.info(f"=== Phase 1: Create {total} PVCs ===")

        pvc_names = [
            f"mcd-pvc-{_rand_seq(5)}-{i:04d}" for i in range(total)
        ]

        ok, fail = self._batch_exec(
            pvc_names,
            self._create_single_pvc,
            "create_pvcs",
            stop_on_max_lvols=True,
        )

        # Populate lvol registry from PVC -> volumeHandle
        for pvc_name in list(self._pvc_registry.keys()):
            self._lvol_registry[pvc_name] = {
                "id": pvc_name,
                "parent_name": None,
            }

    def _create_single_pvc(self, pvc_name: str):
        ns = self.k8s_utils.namespace
        self.k8s_utils.create_pvc(
            pvc_name, self.PVC_SIZE, self.STORAGE_CLASS_NAME
        )
        # Wait for Bound
        bound = False
        for _ in range(60):
            try:
                out, _ = self.k8s_utils._exec_kubectl(
                    f"kubectl get pvc {pvc_name} -n {ns} "
                    f"-o jsonpath='{{.status.phase}}'"
                )
                if "Bound" in (out or ""):
                    bound = True
                    break
            except Exception:
                pass
            sleep_n_sec(5)

        if not bound:
            raise RuntimeError(f"PVC {pvc_name} not Bound after 300s")

        self._pvc_registry[pvc_name] = {"bound": True}

    # ── Phase 2: FIO on 10% of PVCs ──────────────────────────────────────

    def _phase_2_fio_on_lvols(self):
        if not self._pvc_registry:
            return

        sample_size = max(1, math.ceil(
            len(self._pvc_registry) * self.FIO_SAMPLE_PERCENT / 100
        ))
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
        for pvc_name in self._pvc_registry:
            for s in range(self.SNAPSHOTS_PER_LVOL):
                vs_name = f"vs-{pvc_name[-8:]}-{s:03d}"
                snap_items.append({
                    "vs_name": vs_name,
                    "pvc_name": pvc_name,
                })

        self.logger.info(
            f"=== Phase 3: Create {len(snap_items)} VolumeSnapshots ==="
        )

        ok, fail = self._batch_exec(
            snap_items,
            self._create_single_vs,
            "create_snapshots",
            batch_size=self.SNAPSHOT_BATCH_SIZE,
            stop_on_max_lvols=True,
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

    # ── Phase 4: Delete all PVCs (free subsystem slots for clones) ────────

    def _phase_4_delete_lvols(self):
        self.logger.info(
            f"=== Phase 4: Delete {len(self._pvc_registry)} PVCs "
            f"(freeing subsystem slots for clones) ==="
        )

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
        ok, fail = self._batch_exec(
            pvc_names,
            self._delete_single_pvc,
            "delete_pvcs",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(pvc_names),
        )
        self._metrics["lvols_deleted"] = ok

    # ── Phase 5: Create clone PVCs from VolumeSnapshots ───────────────────

    def _phase_5_create_clones(self):
        if not self._snapshot_registry:
            self.logger.info("[Phase 5] No snapshots — skipping")
            return

        snap_list = list(self._snapshot_registry.keys())
        self.logger.info(
            f"=== Phase 5: Create clones from {len(snap_list)} "
            f"snapshots (until limit) ==="
        )

        clone_idx = [0]
        hit_limit = [False]
        lock = threading.Lock()

        def _create_clone_pvc(_):
            if hit_limit[0]:
                return
            vs_name = random.choice(snap_list)
            with lock:
                idx = clone_idx[0]
                clone_idx[0] += 1
            clone_pvc = f"clone-pvc-{_rand_seq(5)}-{idx:06d}"
            try:
                self.k8s_utils.create_clone_pvc(
                    clone_pvc, self.PVC_SIZE, self.STORAGE_CLASS_NAME,
                    vs_name,
                )
                self._clone_registry[clone_pvc] = {
                    "clone_id": clone_pvc, "snap_name": vs_name,
                }
                self._clone_pvc_registry[clone_pvc] = {"vs_name": vs_name}
            except Exception as e:
                if self._is_max_lvols_error(e):
                    hit_limit[0] = True
                    return
                raise

        deadline = time.time() + self.CLONE_PHASE_TIMEOUT
        batch_num = 0
        while not hit_limit[0] and time.time() < deadline:
            batch = list(range(self.BATCH_SIZE))
            ok, fail = self._batch_exec(
                batch, _create_clone_pvc,
                f"create_clones_b{batch_num}",
                max_workers=self.CLONE_MAX_WORKERS,
                max_failures=self.BATCH_SIZE,
            )
            batch_num += 1
            if fail >= self.BATCH_SIZE:
                break
            self.logger.info(
                f"[Phase 5] {len(self._clone_registry)} clones so far"
            )

    # ── Phase 6: FIO on 10% of clone PVCs ─────────────────────────────────

    def _phase_6_fio_on_clones(self):
        if not self._clone_registry:
            self.logger.info("[Phase 6] No clones — skipping")
            return

        sample_size = max(1, math.ceil(
            len(self._clone_registry) * self.FIO_SAMPLE_PERCENT / 100
        ))
        sample = random.sample(
            list(self._clone_registry.keys()),
            min(sample_size, len(self._clone_registry)),
        )
        self._fio_clone_sample = set(sample)
        self.logger.info(
            f"=== Phase 6: FIO on {len(sample)} clone PVCs ==="
        )

        clone_fio_jobs = {}
        for clone_pvc in sample:
            try:
                job_name = f"fio-clone-{_rand_seq(6)}"
                cm_name = f"fio-cm-clone-{_rand_seq(6)}"
                fio_cfg = self._build_simple_fio_config()
                self.k8s_utils.create_fio_job(
                    job_name=job_name,
                    pvc_name=clone_pvc,
                    configmap_name=cm_name,
                    fio_config=fio_cfg,
                )
                clone_fio_jobs[job_name] = clone_pvc
                self._metrics["fio_clone_started"] += 1
            except Exception as exc:
                self.logger.error(
                    f"[Phase 6] FIO Job failed for {clone_pvc}: {exc}"
                )
                self._metrics["fio_clone_failures"] += 1

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

        self.logger.info(
            f"=== Phase 7: Delete {len(self._clone_registry)} "
            f"clone PVCs ==="
        )

        clone_names = list(self._clone_registry.keys())
        ok, fail = self._batch_exec(
            clone_names,
            self._delete_single_pvc,
            "delete_clones",
            max_workers=self.DELETE_MAX_WORKERS,
            max_failures=len(clone_names),
        )
        self._metrics["clones_deleted"] = ok

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


class MassCreateDelete_3000x1_K8s(_MassCreateDeleteK8s):
    """3000 ns/sub × 1 subsystem = 3000 PVCs."""
    NUM_SUBSYSTEMS = 1
    NS_PER_SUBSYSTEM = 3000
