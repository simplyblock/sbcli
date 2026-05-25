"""
Parallel Namespace LVol Stress Test (Docker + K8s)

Creates 300 parent lvols each with 6 namespace partitions (1800 total),
takes 2 snapshots per lvol (3600 total), clones 1 picked snapshot 1500 times,
then deletes everything in parallel — with verified deletion.  Repeats for
NUM_ITERATIONS cycles to measure latency degradation over time.

Two variants:
  - TestParallelNamespaceLvolDocker: sbcli API (add_lvol with namespace=)
  - TestParallelNamespaceLvolK8s:   K8s PVC / StorageClass with max_namespace_per_subsys

Every operation is timed end-to-end (API call → resource Bound/visible or
resource confirmed gone for deletes).  Results are written to a JSON timing
report and 5 PNG graphs.
"""

import json
import os
import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec

try:
    import requests
except Exception:
    requests = None


def _rand_seq(n: int = 8) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


# ═══════════════════════════════════════════════════════════════════════════════
#  Base class — shared logic for Docker and K8s variants
# ═══════════════════════════════════════════════════════════════════════════════

class _ParallelNamespaceLvolBase(TestClusterBase):
    """Shared phased stress test: create → snapshot → clone → delete × N."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ── Scale ──────────────────────────────────────────────────────────
        self.NUM_PARENTS = 300
        self.NAMESPACES_PER_PARENT = 100     # max_namespace_per_subsys
        self.CHILDREN_PER_PARENT = 5         # 300 × 5 = 1500 children
        self.SNAPSHOTS_PER_LVOL = 2          # per parent + 1 random child
        self.NUM_CLONES = 1500               # from 1 picked snapshot
        self.NUM_ITERATIONS = 20

        # ── Sizing ─────────────────────────────────────────────────────────
        self.LVOL_SIZE = "1G"
        self.PVC_SIZE = "1Gi"

        # ── Concurrency ───────────────────────────────────────────────────
        self.MAX_WORKERS_CREATE = 20
        self.MAX_WORKERS_DELETE = 30
        self.BATCH_SIZE = 50
        self.TASK_TIMEOUT = 300

        # ── Retry ─────────────────────────────────────────────────────────
        self.RETRY_MAX = 10
        self.RETRY_INTERVAL = 5

        # ── Thread-safe state ─────────────────────────────────────────────
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # parent_name -> {id, children: [child_name], snapshots: [snap_name]}
        self._parent_registry = {}
        # child_name  -> {id, parent_name}
        self._child_registry = {}
        # snap_name   -> {snap_id, lvol_name, clones: [clone_name]}
        self._snap_registry = {}
        # clone_name  -> {id, snap_name}
        self._clone_registry = {}

        # ── Timing samples ────────────────────────────────────────────────
        self._timing_samples = []   # list of dicts
        self._iteration_timings = []  # per-iteration phase durations
        self._current_iteration = 0

        # ── Metrics ───────────────────────────────────────────────────────
        self._metrics = {
            "start_ts": None,
            "end_ts": None,
            "counts": {k: 0 for k in [
                "parents_created", "children_created", "snapshots_created",
                "clones_created", "parents_deleted", "children_deleted",
                "snapshots_deleted", "clones_deleted",
            ]},
            "attempts": {k: 0 for k in [
                "create_parent", "create_child", "create_snapshot",
                "create_clone", "delete_clone", "delete_snapshot",
                "delete_child", "delete_parent",
            ]},
            "failures": {k: 0 for k in [
                "create_parent", "create_child", "create_snapshot",
                "create_clone", "delete_clone", "delete_snapshot",
                "delete_child", "delete_parent",
            ]},
            "failure_info": None,
        }

    # ── Metrics helpers ───────────────────────────────────────────────────

    def _inc(self, bucket: str, key: str, n: int = 1):
        with self._lock:
            self._metrics[bucket][key] += n

    def _set_failure(self, op: str, exc: Exception, details: str = ""):
        with self._lock:
            if self._metrics["failure_info"] is None:
                self._metrics["failure_info"] = {
                    "op": op, "exc": repr(exc),
                    "when": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "details": details,
                }

    def _snapshot_inventory(self) -> dict:
        with self._lock:
            lvols = len(self._parent_registry) + len(self._child_registry)
            snaps = len(self._snap_registry)
            clones = len(self._clone_registry)
            return {
                "lvols": lvols, "snapshots": snaps,
                "clones": clones, "total": lvols + snaps + clones,
            }

    def _record_timing(self, op: str, name: str, elapsed: float, inventory: dict):
        with self._lock:
            self._timing_samples.append({
                "iteration": self._current_iteration,
                "op": op,
                "name": name,
                "elapsed_sec": round(elapsed, 4),
                "inventory": inventory,
                "timestamp": time.time(),
            })

    # ── API error helpers (reused from existing parallel test) ────────────

    def _extract_api_error(self, e: Exception) -> dict:
        info = {"type": type(e).__name__, "msg": str(e)}
        if requests is not None:
            try:
                if isinstance(e, requests.exceptions.HTTPError):
                    resp = getattr(e, "response", None)
                    if resp is not None:
                        info["status_code"] = getattr(resp, "status_code", None)
                        try:
                            info["text"] = resp.text
                        except Exception:
                            pass
            except Exception:
                pass
        resp = getattr(e, "response", None)
        if resp is not None:
            info["status_code"] = getattr(resp, "status_code", None)
            try:
                info["text"] = resp.text
            except Exception:
                pass
        return info

    def _is_max_lvols_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "max lvols reached" in text or "max lvols reached" in msg

    def _is_bdev_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "failed to create bdev" in text or "failed to create bdev" in msg

    def _is_sync_deletion_error(self, api_err: dict) -> bool:
        text = (api_err.get("text") or "").lower()
        msg = (api_err.get("msg") or "").lower()
        return "lvol sync deletion found" in text or "lvol sync deletion found" in msg

    def _api_retry(self, op: str, fn, ctx: dict = None):
        """Call fn() with retry.  Returns fn() result on success."""
        ctx = ctx or {}
        for attempt in range(1, self.RETRY_MAX + 1):
            try:
                return fn()
            except Exception as e:
                api_err = self._extract_api_error(e)
                if self._is_max_lvols_error(api_err):
                    self._inc("failures", op)
                    self.logger.warning(f"[max_lvols] op={op} ctx={ctx}")
                    raise
                if attempt < self.RETRY_MAX:
                    self.logger.warning(
                        f"[retry] op={op} attempt {attempt}/{self.RETRY_MAX} "
                        f"failed: {e}; retrying in {self.RETRY_INTERVAL}s"
                    )
                    sleep_n_sec(self.RETRY_INTERVAL)
                else:
                    self._inc("failures", op)
                    self._set_failure(op, e, f"failed after {self.RETRY_MAX} attempts")
                    raise

    # ── Wait helpers ──────────────────────────────────────────────────────

    def _wait_lvol_id(self, lvol_name: str, timeout: int = 300,
                      interval: int = 10) -> str:
        sleep_n_sec(3)
        start = time.time()
        while time.time() - start < timeout:
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            if lvol_id:
                return lvol_id
            sleep_n_sec(interval)
        raise TimeoutError(f"LVOL id not visible for {lvol_name} after {timeout}s")

    def _wait_snapshot_id(self, snap_name: str, timeout: int = 300,
                          interval: int = 10) -> str:
        sleep_n_sec(3)
        start = time.time()
        while time.time() - start < timeout:
            snap_id = self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
            if snap_id:
                return snap_id
            sleep_n_sec(interval)
        raise TimeoutError(f"Snapshot id not visible for {snap_name} after {timeout}s")

    def _wait_lvol_gone(self, lvol_name: str, timeout: int = 120) -> float:
        """Poll until lvol is gone.  Returns elapsed seconds."""
        start = time.time()
        while time.time() - start < timeout:
            if not self.sbcli_utils.get_lvol_id(lvol_name=lvol_name):
                return time.time() - start
            sleep_n_sec(2)
        self.logger.warning(f"lvol {lvol_name} still exists after {timeout}s")
        return time.time() - start

    def _wait_snapshot_gone(self, snap_name: str, timeout: int = 120) -> float:
        """Poll until snapshot is gone.  Returns elapsed seconds."""
        start = time.time()
        while time.time() - start < timeout:
            if not self.sbcli_utils.get_snapshot_id(snap_name=snap_name):
                return time.time() - start
            sleep_n_sec(2)
        self.logger.warning(f"snapshot {snap_name} still exists after {timeout}s")
        return time.time() - start

    # ── Batch parallel execution ──────────────────────────────────────────

    def _batch_parallel(self, items, task_fn, max_workers: int, op_name: str):
        """Execute task_fn(item) for each item using ThreadPoolExecutor.

        Submits in BATCH_SIZE chunks, harvests between batches.
        Returns (success_count, failure_count).
        """
        total = len(items)
        success = 0
        failures = 0

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            for batch_start in range(0, total, self.BATCH_SIZE):
                if self._stop_event.is_set():
                    break
                batch = items[batch_start:batch_start + self.BATCH_SIZE]
                futures = {}
                for item in batch:
                    if self._stop_event.is_set():
                        break
                    f = ex.submit(task_fn, item)
                    futures[f] = item

                for f in as_completed(futures):
                    try:
                        f.result(timeout=self.TASK_TIMEOUT)
                        success += 1
                    except Exception as exc:
                        failures += 1
                        self.logger.error(
                            f"[{op_name}] Failed for {futures[f]}: {exc}"
                        )

                done = batch_start + len(batch)
                self.logger.info(
                    f"[{op_name}] progress: {done}/{total}  "
                    f"(ok={success} fail={failures})"
                )

        return success, failures

    # ── Phase orchestration ───────────────────────────────────────────────

    def _run_phase(self, name: str, fn):
        if self._stop_event.is_set():
            self.logger.warning(f"[{name}] Skipping — prior failure")
            return
        self.logger.info(f"=== Phase: {name} ===")
        start = time.time()
        try:
            fn()
        except Exception as e:
            self.logger.error(f"[{name}] Phase failed: {e}")
            self._set_failure(name, e, f"Phase {name} failed")
        finally:
            dur = time.time() - start
            self.logger.info(f"=== Phase {name} done in {dur:.1f}s ===")
            return dur  # used for iteration timing

    def _clear_registries(self):
        with self._lock:
            self._parent_registry.clear()
            self._child_registry.clear()
            self._snap_registry.clear()
            self._clone_registry.clear()

    # ── Abstract-like methods (subclasses override) ───────────────────────

    def _phase_setup(self):
        raise NotImplementedError

    def _phase_cleanup(self):
        raise NotImplementedError

    def _create_parent_impl(self, params: dict):
        raise NotImplementedError

    def _create_child_impl(self, params: dict):
        raise NotImplementedError

    def _create_snapshot_impl(self, params: dict):
        raise NotImplementedError

    def _create_clone_impl(self, params: dict):
        raise NotImplementedError

    def _delete_clone_impl(self, clone_name: str):
        raise NotImplementedError

    def _delete_snapshot_impl(self, snap_name: str):
        raise NotImplementedError

    def _delete_child_impl(self, child_name: str):
        raise NotImplementedError

    def _delete_parent_impl(self, parent_name: str):
        raise NotImplementedError

    # ── Timed wrappers (called by _batch_parallel) ───────────────────────

    def _timed_create_parent(self, params: dict):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._create_parent_impl(params)
        elapsed = time.time() - t0
        self._record_timing("create_parent", params["name"], elapsed, inv)

    def _timed_create_child(self, params: dict):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._create_child_impl(params)
        elapsed = time.time() - t0
        self._record_timing("create_child", params["name"], elapsed, inv)

    def _timed_create_snapshot(self, params: dict):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._create_snapshot_impl(params)
        elapsed = time.time() - t0
        self._record_timing("create_snapshot", params["name"], elapsed, inv)

    def _timed_create_clone(self, params: dict):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._create_clone_impl(params)
        elapsed = time.time() - t0
        self._record_timing("create_clone", params["name"], elapsed, inv)

    def _timed_delete_clone(self, clone_name: str):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._delete_clone_impl(clone_name)
        elapsed = time.time() - t0
        self._record_timing("delete_clone", clone_name, elapsed, inv)

    def _timed_delete_snapshot(self, snap_name: str):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._delete_snapshot_impl(snap_name)
        elapsed = time.time() - t0
        self._record_timing("delete_snapshot", snap_name, elapsed, inv)

    def _timed_delete_child(self, child_name: str):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._delete_child_impl(child_name)
        elapsed = time.time() - t0
        self._record_timing("delete_child", child_name, elapsed, inv)

    def _timed_delete_parent(self, parent_name: str):
        inv = self._snapshot_inventory()
        t0 = time.time()
        self._delete_parent_impl(parent_name)
        elapsed = time.time() - t0
        self._record_timing("delete_parent", parent_name, elapsed, inv)

    # ── Phase implementations ─────────────────────────────────────────────

    def _phase_create_parents(self):
        items = []
        for i in range(self.NUM_PARENTS):
            name = f"ns-par-{_rand_seq(6)}-{i:04d}"
            items.append({"name": name, "idx": i})
        self._batch_parallel(
            items, self._timed_create_parent,
            self.MAX_WORKERS_CREATE, "create_parents",
        )

    def _phase_create_children(self):
        """Create CHILDREN_PER_PARENT child namespace lvols per parent."""
        items = []
        with self._lock:
            parents = list(self._parent_registry.items())
        for parent_name, pinfo in parents:
            parent_id = pinfo["id"]
            for c in range(self.CHILDREN_PER_PARENT):
                child_name = f"ns-ch-{_rand_seq(6)}-{parent_name[-4:]}-{c}"
                items.append({
                    "name": child_name,
                    "parent_name": parent_name,
                    "parent_id": parent_id,
                })
        self._batch_parallel(
            items, self._timed_create_child,
            self.MAX_WORKERS_CREATE, "create_children",
        )

    def _phase_create_snapshots(self):
        """Create SNAPSHOTS_PER_LVOL snapshots for each parent + 1 random child."""
        items = []
        with self._lock:
            # All parents
            snap_lvols = []
            for pname, pinfo in self._parent_registry.items():
                snap_lvols.append((pname, pinfo["id"]))
            # Pick 1 random child (if any)
            child_names = list(self._child_registry.keys())
            if child_names:
                chosen_child = random.choice(child_names)
                cinfo = self._child_registry[chosen_child]
                snap_lvols.append((chosen_child, cinfo["id"]))
                self.logger.info(
                    f"[create_snapshots] Also snapshotting child: {chosen_child}"
                )

        for lvol_name, lvol_id in snap_lvols:
            for s in range(self.SNAPSHOTS_PER_LVOL):
                snap_name = f"snap-{_rand_seq(6)}-{lvol_name[-8:]}-{s}"
                items.append({
                    "name": snap_name,
                    "lvol_name": lvol_name,
                    "lvol_id": lvol_id,
                })
        self.logger.info(
            f"[create_snapshots] Creating {len(items)} snapshots "
            f"({len(snap_lvols)} lvols × {self.SNAPSHOTS_PER_LVOL})"
        )
        self._batch_parallel(
            items, self._timed_create_snapshot,
            self.MAX_WORKERS_CREATE, "create_snapshots",
        )

    def _phase_create_clones(self):
        """Pick 1 random snapshot and create NUM_CLONES clones from it."""
        with self._lock:
            snap_names = list(self._snap_registry.keys())
        if not snap_names:
            self.logger.warning("[create_clones] No snapshots available!")
            return
        chosen_snap = random.choice(snap_names)
        with self._lock:
            snap_id = self._snap_registry[chosen_snap]["snap_id"]
        self.logger.info(
            f"[create_clones] Chosen snapshot: {chosen_snap} (id={snap_id})"
        )
        items = []
        for i in range(self.NUM_CLONES):
            clone_name = f"cln-{_rand_seq(6)}-{i:04d}"
            items.append({
                "name": clone_name,
                "snap_name": chosen_snap,
                "snap_id": snap_id,
            })
        self._batch_parallel(
            items, self._timed_create_clone,
            self.MAX_WORKERS_CREATE, "create_clones",
        )

    def _phase_delete_all(self):
        """Delete: clones → snapshots → children → parents (ordered)."""
        # Step 1: clones
        with self._lock:
            clone_names = list(self._clone_registry.keys())
        if clone_names:
            self.logger.info(f"[delete_all] Deleting {len(clone_names)} clones")
            self._batch_parallel(
                clone_names, self._timed_delete_clone,
                self.MAX_WORKERS_DELETE, "delete_clones",
            )

        # Step 2: snapshots
        with self._lock:
            snap_names = list(self._snap_registry.keys())
        if snap_names:
            self.logger.info(f"[delete_all] Deleting {len(snap_names)} snapshots")
            self._batch_parallel(
                snap_names, self._timed_delete_snapshot,
                self.MAX_WORKERS_DELETE, "delete_snapshots",
            )

        # Step 3: children
        with self._lock:
            child_names = list(self._child_registry.keys())
        if child_names:
            self.logger.info(f"[delete_all] Deleting {len(child_names)} children")
            self._batch_parallel(
                child_names, self._timed_delete_child,
                self.MAX_WORKERS_DELETE, "delete_children",
            )

        # Step 4: parents
        with self._lock:
            parent_names = list(self._parent_registry.keys())
        if parent_names:
            self.logger.info(f"[delete_all] Deleting {len(parent_names)} parents")
            self._batch_parallel(
                parent_names, self._timed_delete_parent,
                self.MAX_WORKERS_DELETE, "delete_parents",
            )

    # ── Reporting ─────────────────────────────────────────────────────────

    def _get_log_dir(self) -> str:
        """Return the directory for timing/graph output."""
        d = getattr(self, "docker_logs_path", None)
        if not d:
            d = os.path.join(self.nfs_log_base, self.test_name)
        os.makedirs(d, exist_ok=True)
        return d

    def _write_timing_report(self):
        out_dir = self._get_log_dir()
        report = {
            "config": {
                "NUM_PARENTS": self.NUM_PARENTS,
                "NAMESPACES_PER_PARENT": self.NAMESPACES_PER_PARENT,
                "CHILDREN_PER_PARENT": self.CHILDREN_PER_PARENT,
                "SNAPSHOTS_PER_LVOL": self.SNAPSHOTS_PER_LVOL,
                "NUM_CLONES": self.NUM_CLONES,
                "NUM_ITERATIONS": self.NUM_ITERATIONS,
            },
            "iterations": self._iteration_timings,
            "samples": self._timing_samples,
            "metrics": self._metrics,
        }
        path = os.path.join(out_dir, "namespace_stress_timings.json")
        try:
            with open(path, "w") as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"Wrote timing report to {path}")
        except Exception as exc:
            self.logger.warning(f"Could not write timing report: {exc}")

    def _generate_graphs(self):
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            self.logger.warning(
                "matplotlib not available; skipping graph generation"
            )
            return

        out_dir = self._get_log_dir()
        samples = self._timing_samples

        if not samples:
            self.logger.warning("No timing samples; skipping graphs")
            return

        # ── 1. Latency vs inventory (scatter) ────────────────────────────
        try:
            op_types = sorted(set(s["op"] for s in samples))
            colors = plt.cm.tab10.colors
            fig, ax = plt.subplots(figsize=(14, 8))
            for i, op in enumerate(op_types):
                pts = [s for s in samples if s["op"] == op]
                x = [p["inventory"]["total"] for p in pts]
                y = [p["elapsed_sec"] for p in pts]
                ax.scatter(x, y, label=op, alpha=0.4, s=8,
                           color=colors[i % len(colors)])
            ax.set_xlabel("Total inventory count")
            ax.set_ylabel("Latency (sec)")
            ax.set_title("Operation Latency vs Inventory Size")
            ax.legend(fontsize=7)
            fig.tight_layout()
            fig.savefig(os.path.join(out_dir, "latency_vs_inventory.png"), dpi=150)
            plt.close(fig)
            self.logger.info("Generated latency_vs_inventory.png")
        except Exception as exc:
            self.logger.warning(f"Graph 1 failed: {exc}")

        # ── 2. Latency per iteration (box plot) ──────────────────────────
        try:
            create_ops = [
                "create_parent", "create_child",
                "create_snapshot", "create_clone",
            ]
            iterations = sorted(set(s["iteration"] for s in samples))
            fig, ax = plt.subplots(figsize=(14, 8))
            positions = []
            labels = []
            data_groups = []
            for it in iterations:
                for op in create_ops:
                    vals = [
                        s["elapsed_sec"] for s in samples
                        if s["iteration"] == it and s["op"] == op
                    ]
                    if vals:
                        data_groups.append(vals)
                        positions.append(
                            it * (len(create_ops) + 1)
                            + create_ops.index(op)
                        )
                        labels.append(f"i{it}_{op.split('_')[-1]}")
            if data_groups:
                bp = ax.boxplot(data_groups, positions=positions, widths=0.6,
                                patch_artist=True, showfliers=False)
                for j, patch in enumerate(bp["boxes"]):
                    c_idx = j % len(create_ops)
                    patch.set_facecolor(colors[c_idx % len(colors)])
                ax.set_xlabel("Iteration / Operation")
                ax.set_ylabel("Latency (sec)")
                ax.set_title("Create Latency per Iteration")
                ax.set_xticks(positions[::len(create_ops)])
                ax.set_xticklabels(
                    [f"iter {it}" for it in iterations],
                    rotation=45, fontsize=7,
                )
            fig.tight_layout()
            fig.savefig(os.path.join(out_dir, "latency_per_iteration.png"),
                        dpi=150)
            plt.close(fig)
            self.logger.info("Generated latency_per_iteration.png")
        except Exception as exc:
            self.logger.warning(f"Graph 2 failed: {exc}")

        # ── 3. Phase duration per iteration (stacked bar) ────────────────
        try:
            phase_names = [
                "create_parents", "create_children",
                "create_snapshots", "create_clones", "delete_all",
            ]
            fig, ax = plt.subplots(figsize=(12, 6))
            x_pos = list(range(len(self._iteration_timings)))
            bottom = [0.0] * len(x_pos)
            for pi, pname in enumerate(phase_names):
                vals = [
                    it_info.get("phase_durations_sec", {}).get(pname, 0)
                    for it_info in self._iteration_timings
                ]
                ax.bar(x_pos, vals, bottom=bottom, label=pname,
                       color=colors[pi % len(colors)])
                bottom = [b + v for b, v in zip(bottom, vals)]
            ax.set_xlabel("Iteration")
            ax.set_ylabel("Duration (sec)")
            ax.set_title("Phase Duration per Iteration")
            ax.legend(fontsize=7)
            ax.set_xticks(x_pos)
            ax.set_xticklabels([str(i + 1) for i in x_pos])
            fig.tight_layout()
            fig.savefig(
                os.path.join(out_dir, "phase_duration_per_iteration.png"),
                dpi=150,
            )
            plt.close(fig)
            self.logger.info("Generated phase_duration_per_iteration.png")
        except Exception as exc:
            self.logger.warning(f"Graph 3 failed: {exc}")

        # ── 4. Clone latency vs clone index (per iteration) ──────────────
        try:
            fig, ax = plt.subplots(figsize=(14, 8))
            for it in iterations:
                clone_samples = sorted(
                    [s for s in samples
                     if s["iteration"] == it and s["op"] == "create_clone"],
                    key=lambda s: s["timestamp"],
                )
                if clone_samples:
                    ax.plot(
                        range(len(clone_samples)),
                        [s["elapsed_sec"] for s in clone_samples],
                        label=f"iter {it}", alpha=0.7, linewidth=0.8,
                    )
            ax.set_xlabel("Clone index (creation order)")
            ax.set_ylabel("Latency (sec)")
            ax.set_title("Clone Creation Latency vs Clone Count")
            ax.legend(fontsize=7)
            fig.tight_layout()
            fig.savefig(
                os.path.join(out_dir, "clone_latency_vs_clone_count.png"),
                dpi=150,
            )
            plt.close(fig)
            self.logger.info("Generated clone_latency_vs_clone_count.png")
        except Exception as exc:
            self.logger.warning(f"Graph 4 failed: {exc}")

        # ── 5. Delete latency vs remaining inventory ─────────────────────
        try:
            delete_ops = [
                "delete_clone", "delete_snapshot",
                "delete_child", "delete_parent",
            ]
            fig, ax = plt.subplots(figsize=(14, 8))
            for i, op in enumerate(delete_ops):
                pts = [s for s in samples if s["op"] == op]
                if pts:
                    x = [p["inventory"]["total"] for p in pts]
                    y = [p["elapsed_sec"] for p in pts]
                    ax.scatter(x, y, label=op, alpha=0.4, s=8,
                               color=colors[i % len(colors)])
            ax.set_xlabel("Remaining inventory at delete time")
            ax.set_ylabel("Delete latency (sec)")
            ax.set_title("Delete Latency vs Remaining Inventory")
            ax.legend(fontsize=7)
            fig.tight_layout()
            fig.savefig(
                os.path.join(out_dir, "delete_latency_vs_remaining.png"),
                dpi=150,
            )
            plt.close(fig)
            self.logger.info("Generated delete_latency_vs_remaining.png")
        except Exception as exc:
            self.logger.warning(f"Graph 5 failed: {exc}")

    def _print_summary(self):
        self.logger.info("=" * 60)
        self.logger.info("  PARALLEL NAMESPACE LVOL STRESS — SUMMARY")
        self.logger.info("=" * 60)
        c = self._metrics["counts"]
        for k, v in c.items():
            self.logger.info(f"  {k}: {v}")
        a = self._metrics["attempts"]
        f = self._metrics["failures"]
        self.logger.info("  --- attempts / failures ---")
        for k in a:
            self.logger.info(f"  {k}: attempts={a[k]}  failures={f.get(k, 0)}")
        self.logger.info(f"  timing_samples: {len(self._timing_samples)}")
        self.logger.info(f"  iterations_completed: {len(self._iteration_timings)}")
        if self._metrics["failure_info"]:
            self.logger.info(f"  FIRST FAILURE: {self._metrics['failure_info']}")
        self.logger.info("=" * 60)

    # ── Main entry point ──────────────────────────────────────────────────

    def run(self):
        self.logger.info(
            f"=== Starting {self.__class__.__name__} "
            f"({self.NUM_ITERATIONS} iterations) ==="
        )
        self._metrics["start_ts"] = time.time()

        try:
            self._phase_setup()

            for iteration in range(1, self.NUM_ITERATIONS + 1):
                if self._stop_event.is_set():
                    self.logger.warning(
                        f"Stopping at iteration {iteration} due to prior failure"
                    )
                    break

                self._current_iteration = iteration
                self.logger.info(
                    f"\n{'='*60}\n"
                    f"  ITERATION {iteration}/{self.NUM_ITERATIONS}\n"
                    f"{'='*60}"
                )

                phase_durations = {}
                for phase_name, phase_fn in [
                    ("create_parents", self._phase_create_parents),
                    ("create_children", self._phase_create_children),
                    ("create_snapshots", self._phase_create_snapshots),
                    ("create_clones", self._phase_create_clones),
                    ("delete_all", self._phase_delete_all),
                ]:
                    dur = self._run_phase(phase_name, phase_fn)
                    phase_durations[phase_name] = round(dur or 0, 2)

                self._iteration_timings.append({
                    "iteration": iteration,
                    "phase_durations_sec": phase_durations,
                })
                self._clear_registries()

        finally:
            self._metrics["end_ts"] = time.time()
            self._print_summary()
            self._write_timing_report()
            self._generate_graphs()
            try:
                self._phase_cleanup()
            except Exception as exc:
                self.logger.warning(f"Cleanup failed: {exc}")

        if self._metrics["failure_info"]:
            raise Exception(
                f"Test failed: {self._metrics['failure_info']}"
            )


# ═══════════════════════════════════════════════════════════════════════════════
#  Docker variant — sbcli API
# ═══════════════════════════════════════════════════════════════════════════════

class TestParallelNamespaceLvolDocker(_ParallelNamespaceLvolBase):
    """Parallel namespace lvol stress via sbcli REST API (Docker / bare-metal)."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "parallel_namespace_lvol_docker"

    # ── Setup / Cleanup ───────────────────────────────────────────────────

    def _phase_setup(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        sleep_n_sec(2)

    def _phase_cleanup(self):
        self.logger.info("[cleanup] Bulk delete safety net")
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

    # ── Create implementations ────────────────────────────────────────────

    def _create_parent_impl(self, params: dict):
        name = params["name"]
        self._inc("attempts", "create_parent")
        self._api_retry("create_parent", lambda: self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
            max_namespace_per_subsys=self.NAMESPACES_PER_PARENT,
            retry=1,
        ), ctx={"name": name})
        lvol_id = self._wait_lvol_id(name)
        with self._lock:
            self._parent_registry[name] = {
                "id": lvol_id, "children": [], "snapshots": [],
            }
            self._metrics["counts"]["parents_created"] += 1
        self._inc("attempts", "create_parent", 0)  # already counted
        self.logger.info(f"[create_parent] {name} -> {lvol_id}")

    def _create_child_impl(self, params: dict):
        name = params["name"]
        parent_name = params["parent_name"]
        parent_id = params["parent_id"]
        self._inc("attempts", "create_child")
        self._api_retry("create_child", lambda: self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
            namespace=parent_id,
            retry=1,
        ), ctx={"name": name, "parent": parent_name})
        child_id = self._wait_lvol_id(name)
        with self._lock:
            self._child_registry[name] = {
                "id": child_id, "parent_name": parent_name,
            }
            if parent_name in self._parent_registry:
                self._parent_registry[parent_name]["children"].append(name)
            self._metrics["counts"]["children_created"] += 1
        self.logger.info(f"[create_child] {name} -> {child_id} (parent={parent_name})")

    def _create_snapshot_impl(self, params: dict):
        snap_name = params["name"]
        lvol_name = params["lvol_name"]
        lvol_id = params["lvol_id"]
        self._inc("attempts", "create_snapshot")
        self._api_retry("create_snapshot", lambda: self.sbcli_utils.add_snapshot(
            lvol_id=lvol_id,
            snapshot_name=snap_name,
            retry=1,
        ), ctx={"snap_name": snap_name, "lvol": lvol_name})
        snap_id = self._wait_snapshot_id(snap_name)
        with self._lock:
            self._snap_registry[snap_name] = {
                "snap_id": snap_id,
                "lvol_name": lvol_name,
                "clones": [],
            }
            # Link to parent or child
            if lvol_name in self._parent_registry:
                self._parent_registry[lvol_name]["snapshots"].append(snap_name)
            self._metrics["counts"]["snapshots_created"] += 1
        self.logger.info(f"[create_snapshot] {snap_name} -> {snap_id} (lvol={lvol_name})")

    def _create_clone_impl(self, params: dict):
        clone_name = params["name"]
        snap_name = params["snap_name"]
        snap_id = params["snap_id"]
        self._inc("attempts", "create_clone")
        self._api_retry("create_clone", lambda: self.sbcli_utils.add_clone(
            snapshot_id=snap_id,
            clone_name=clone_name,
            retry=1,
        ), ctx={"clone": clone_name, "snap": snap_name})
        clone_id = self._wait_lvol_id(clone_name)
        with self._lock:
            self._clone_registry[clone_name] = {
                "id": clone_id, "snap_name": snap_name,
            }
            if snap_name in self._snap_registry:
                self._snap_registry[snap_name]["clones"].append(clone_name)
            self._metrics["counts"]["clones_created"] += 1
        self.logger.info(f"[create_clone] {clone_name} -> {clone_id}")

    # ── Delete implementations (with verification) ────────────────────────

    def _delete_clone_impl(self, clone_name: str):
        self._inc("attempts", "delete_clone")
        try:
            self._api_retry("delete_clone", lambda: self.sbcli_utils.delete_lvol(
                lvol_name=clone_name, skip_error=False,
            ))
        except Exception:
            # delete_lvol already waits for removal internally
            pass
        # Verify gone
        self._wait_lvol_gone(clone_name)
        with self._lock:
            self._clone_registry.pop(clone_name, None)
            self._metrics["counts"]["clones_deleted"] += 1

    def _delete_snapshot_impl(self, snap_name: str):
        self._inc("attempts", "delete_snapshot")
        try:
            self._api_retry("delete_snapshot", lambda: self.sbcli_utils.delete_snapshot(
                snap_name=snap_name, skip_error=False,
            ))
        except Exception:
            pass
        self._wait_snapshot_gone(snap_name)
        with self._lock:
            self._snap_registry.pop(snap_name, None)
            self._metrics["counts"]["snapshots_deleted"] += 1

    def _delete_child_impl(self, child_name: str):
        self._inc("attempts", "delete_child")
        try:
            self._api_retry("delete_child", lambda: self.sbcli_utils.delete_lvol(
                lvol_name=child_name, skip_error=False,
            ))
        except Exception:
            pass
        self._wait_lvol_gone(child_name)
        with self._lock:
            self._child_registry.pop(child_name, None)
            self._metrics["counts"]["children_deleted"] += 1

    def _delete_parent_impl(self, parent_name: str):
        self._inc("attempts", "delete_parent")
        try:
            self._api_retry("delete_parent", lambda: self.sbcli_utils.delete_lvol(
                lvol_name=parent_name, skip_error=False,
            ))
        except Exception:
            pass
        self._wait_lvol_gone(parent_name)
        with self._lock:
            self._parent_registry.pop(parent_name, None)
            self._metrics["counts"]["parents_deleted"] += 1


# ═══════════════════════════════════════════════════════════════════════════════
#  K8s variant — PVC / StorageClass / VolumeSnapshot
# ═══════════════════════════════════════════════════════════════════════════════

class TestParallelNamespaceLvolK8s(_ParallelNamespaceLvolBase):
    """Parallel namespace lvol stress via K8s PVC + CSI driver.

    The StorageClass is created with max_namespace_per_subsys=NAMESPACES_PER_PARENT.
    The CSI driver groups PVCs into subsystems automatically (every N PVCs share
    one subsystem).  There is no explicit parent/child distinction at the K8s level.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "parallel_namespace_lvol_k8s"
        self.STORAGE_CLASS_NAME = "simplyblock-ns-stress-sc"
        self.SNAPSHOT_CLASS_NAME = "simplyblock-csi-snapshotclass"
        self.k8s_utils = None

    # ── K8s helpers ───────────────────────────────────────────────────────

    def _init_k8s_utils(self):
        if self.k8s_utils is not None:
            return
        from utils.k8s_utils import K8sUtils
        _mnodes_raw = os.environ.get("MNODES", os.environ.get("K3S_MNODES", ""))
        _mgmt_node = _mnodes_raw.split()[0] if _mnodes_raw.split() else ""
        self.k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=_mgmt_node)

    def _wait_pvc_gone(self, pvc_name: str, timeout: int = 120) -> float:
        start = time.time()
        ns = self.k8s_utils.namespace
        while time.time() - start < timeout:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get pvc {pvc_name} -n {ns} -o name 2>/dev/null || true",
                supress_logs=True,
            )
            if not out.strip():
                return time.time() - start
            sleep_n_sec(2)
        self.logger.warning(f"PVC {pvc_name} still exists after {timeout}s")
        return time.time() - start

    def _wait_snapshot_k8s_gone(self, snap_name: str, timeout: int = 120) -> float:
        start = time.time()
        ns = self.k8s_utils.namespace
        while time.time() - start < timeout:
            out, _ = self.k8s_utils._exec_kubectl(
                f"kubectl get volumesnapshot {snap_name} -n {ns} "
                f"-o name 2>/dev/null || true",
                supress_logs=True,
            )
            if not out.strip():
                return time.time() - start
            sleep_n_sec(2)
        self.logger.warning(f"VolumeSnapshot {snap_name} still exists after {timeout}s")
        return time.time() - start

    # ── Setup / Cleanup ───────────────────────────────────────────────────

    def _phase_setup(self):
        self._init_k8s_utils()
        # Create pool via sbcli
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        sleep_n_sec(2)

        # Create StorageClass with namespace support
        cluster_id = self.cluster_id or os.environ.get("CLUSTER_ID", "")
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
            max_namespace_per_subsys=self.NAMESPACES_PER_PARENT,
        )
        self.k8s_utils.create_volume_snapshot_class(
            name=self.SNAPSHOT_CLASS_NAME,
        )

    def _phase_cleanup(self):
        self.logger.info("[cleanup] K8s bulk cleanup")
        ns = self.k8s_utils.namespace if self.k8s_utils else "default"
        if self.k8s_utils:
            # Delete all PVCs with our label
            try:
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete pvc -l test=ns-stress -n {ns} "
                    f"--wait=false --ignore-not-found 2>/dev/null || true"
                )
            except Exception:
                pass
            # Delete all volume snapshots
            try:
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete volumesnapshot -l test=ns-stress -n {ns} "
                    f"--wait=false --ignore-not-found 2>/dev/null || true"
                )
            except Exception:
                pass
            # Delete StorageClass
            try:
                self.k8s_utils._exec_kubectl(
                    f"kubectl delete storageclass {self.STORAGE_CLASS_NAME} "
                    f"--ignore-not-found 2>/dev/null || true"
                )
            except Exception:
                pass
        # Bulk sbcli cleanup
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_lvols()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

    # ── Phase overrides ───────────────────────────────────────────────────

    def _phase_create_parents(self):
        """In K8s, create ALL PVCs (NUM_PARENTS × NAMESPACES_PER_PARENT).
        CSI driver groups into subsystems automatically."""
        total = self.NUM_PARENTS * self.NAMESPACES_PER_PARENT
        items = []
        for i in range(total):
            pvc_name = f"ns-pvc-{_rand_seq(6)}-{i:04d}"
            items.append({"name": pvc_name, "idx": i})
        self._batch_parallel(
            items, self._timed_create_parent,
            self.MAX_WORKERS_CREATE, "create_pvcs",
        )

    def _phase_create_children(self):
        """No-op in K8s — CSI groups namespaces automatically."""
        self.logger.info(
            "[K8s] Children phase is no-op; CSI driver groups "
            "PVCs into subsystems automatically"
        )

    # ── Create implementations ────────────────────────────────────────────

    def _create_parent_impl(self, params: dict):
        name = params["name"]
        self._inc("attempts", "create_parent")
        ns = self.k8s_utils.namespace
        # Create PVC with label for easy cleanup
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {name}\n"
            f"  labels:\n"
            f"    test: ns-stress\n"
            f"spec:\n"
            f"  accessModes:\n"
            f"    - ReadWriteOnce\n"
            f"  storageClassName: {self.STORAGE_CLASS_NAME}\n"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {self.PVC_SIZE}\n"
        )
        self.k8s_utils.apply_yaml(yaml_content, namespace=ns)
        if not self.k8s_utils.wait_pvc_bound(name, timeout=300, namespace=ns):
            raise TimeoutError(f"PVC {name} not Bound within 300s")
        with self._lock:
            self._parent_registry[name] = {
                "id": name, "children": [], "snapshots": [],
            }
            self._metrics["counts"]["parents_created"] += 1
        self.logger.info(f"[create_pvc] {name} Bound")

    def _create_child_impl(self, params: dict):
        """No-op in K8s."""
        pass

    def _create_snapshot_impl(self, params: dict):
        snap_name = params["name"]
        lvol_name = params["lvol_name"]
        self._inc("attempts", "create_snapshot")
        ns = self.k8s_utils.namespace
        # Create VolumeSnapshot with label
        yaml_content = (
            f"apiVersion: snapshot.storage.k8s.io/v1\n"
            f"kind: VolumeSnapshot\n"
            f"metadata:\n"
            f"  name: {snap_name}\n"
            f"  labels:\n"
            f"    test: ns-stress\n"
            f"spec:\n"
            f"  volumeSnapshotClassName: {self.SNAPSHOT_CLASS_NAME}\n"
            f"  source:\n"
            f"    persistentVolumeClaimName: {lvol_name}\n"
        )
        self.k8s_utils.apply_yaml(yaml_content, namespace=ns)
        if not self.k8s_utils.wait_volume_snapshot_ready(
            snap_name, timeout=300, namespace=ns
        ):
            raise TimeoutError(f"VolumeSnapshot {snap_name} not ready within 300s")
        with self._lock:
            self._snap_registry[snap_name] = {
                "snap_id": snap_name,
                "lvol_name": lvol_name,
                "clones": [],
            }
            if lvol_name in self._parent_registry:
                self._parent_registry[lvol_name]["snapshots"].append(snap_name)
            self._metrics["counts"]["snapshots_created"] += 1
        self.logger.info(f"[create_snapshot] {snap_name} ready (pvc={lvol_name})")

    def _create_clone_impl(self, params: dict):
        clone_name = params["name"]
        snap_name = params["snap_name"]
        self._inc("attempts", "create_clone")
        ns = self.k8s_utils.namespace
        # Clone PVC from VolumeSnapshot with label
        yaml_content = (
            f"apiVersion: v1\n"
            f"kind: PersistentVolumeClaim\n"
            f"metadata:\n"
            f"  name: {clone_name}\n"
            f"  labels:\n"
            f"    test: ns-stress\n"
            f"spec:\n"
            f"  accessModes:\n"
            f"    - ReadWriteOnce\n"
            f"  storageClassName: {self.STORAGE_CLASS_NAME}\n"
            f"  resources:\n"
            f"    requests:\n"
            f"      storage: {self.PVC_SIZE}\n"
            f"  dataSource:\n"
            f"    name: {snap_name}\n"
            f"    kind: VolumeSnapshot\n"
            f"    apiGroup: snapshot.storage.k8s.io\n"
        )
        self.k8s_utils.apply_yaml(yaml_content, namespace=ns)
        if not self.k8s_utils.wait_pvc_bound(clone_name, timeout=300, namespace=ns):
            raise TimeoutError(f"Clone PVC {clone_name} not Bound within 300s")
        with self._lock:
            self._clone_registry[clone_name] = {
                "id": clone_name, "snap_name": snap_name,
            }
            if snap_name in self._snap_registry:
                self._snap_registry[snap_name]["clones"].append(clone_name)
            self._metrics["counts"]["clones_created"] += 1
        self.logger.info(f"[create_clone] {clone_name} Bound (snap={snap_name})")

    # ── Delete implementations (with verification) ────────────────────────

    def _delete_clone_impl(self, clone_name: str):
        self._inc("attempts", "delete_clone")
        ns = self.k8s_utils.namespace
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pvc {clone_name} -n {ns} "
            f"--ignore-not-found --wait=false 2>/dev/null || true"
        )
        self._wait_pvc_gone(clone_name)
        with self._lock:
            self._clone_registry.pop(clone_name, None)
            self._metrics["counts"]["clones_deleted"] += 1

    def _delete_snapshot_impl(self, snap_name: str):
        self._inc("attempts", "delete_snapshot")
        ns = self.k8s_utils.namespace
        self.k8s_utils._exec_kubectl(
            f"kubectl delete volumesnapshot {snap_name} -n {ns} "
            f"--ignore-not-found --wait=false 2>/dev/null || true"
        )
        self._wait_snapshot_k8s_gone(snap_name)
        with self._lock:
            self._snap_registry.pop(snap_name, None)
            self._metrics["counts"]["snapshots_deleted"] += 1

    def _delete_child_impl(self, child_name: str):
        """No-op in K8s — no separate children."""
        pass

    def _delete_parent_impl(self, parent_name: str):
        self._inc("attempts", "delete_parent")
        ns = self.k8s_utils.namespace
        self.k8s_utils._exec_kubectl(
            f"kubectl delete pvc {parent_name} -n {ns} "
            f"--ignore-not-found --wait=false 2>/dev/null || true"
        )
        self._wait_pvc_gone(parent_name)
        with self._lock:
            self._parent_registry.pop(parent_name, None)
            self._metrics["counts"]["parents_deleted"] += 1
