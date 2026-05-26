"""
Parallel Namespace LVol Stress Test (Docker + K8s)

Creates 100 parent lvols each with 50 namespace children (5100 total lvols),
writes 10 MB data to each parent, takes 2 snapshots per parent (+ 1 random
child), clones 1 picked snapshot 1500 times, verifies everything, then deletes
in parallel — with verified deletion.  Repeats for NUM_ITERATIONS cycles to
measure latency degradation over time.

**Sequential per-parent flow**: for each parent, all 50 children are created
and verified before moving to the next parent.  Any failure aborts the test.

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
        self.NUM_PARENTS = 50
        self.NAMESPACES_PER_PARENT = 51      # max_namespace_per_subsys (parent + 50 children)
        self.CHILDREN_PER_PARENT = 50        # 50 × 50 = 2500 children
        self.SNAPSHOTS_PER_LVOL = 2          # per parent + 1 random child
        self.NUM_CLONES = 1500               # from 1 picked snapshot
        self.NUM_ITERATIONS = 1

        # ── Sizing ─────────────────────────────────────────────────────────
        self.LVOL_SIZE = "1G"
        self.PVC_SIZE = "1Gi"

        # ── Concurrency ───────────────────────────────────────────────────
        self.MAX_WORKERS_CREATE = 20
        self.MAX_WORKERS_DELETE = 30
        self.BATCH_SIZE = 50
        self.TASK_TIMEOUT = 300
        self.PARALLEL_PARENTS = 5            # concurrent parents during child creation
        self.CLONE_BATCH_SIZE = 250          # clone creation batch size for stats

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
        self._batch_timings = []    # batch-level summaries for graphs
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

    def _log_op_stats(self, op: str, batch_label: str = "",
                      batch_elapsed: float = 0, count: int = 0):
        """Log avg/p50/p95 stats for a given op in the current iteration."""
        with self._lock:
            samples = [
                s["elapsed_sec"] for s in self._timing_samples
                if s["iteration"] == self._current_iteration and s["op"] == op
            ]
        if not samples:
            return
        samples_sorted = sorted(samples)
        n = len(samples_sorted)
        avg = sum(samples_sorted) / n
        p50 = samples_sorted[n // 2]
        p95 = samples_sorted[min(int(n * 0.95), n - 1)]
        mn, mx = samples_sorted[0], samples_sorted[-1]
        tag = f" ({batch_label})" if batch_label else ""
        self.logger.info(
            f"[{op}]{tag}: {count or n} ops in {batch_elapsed:.1f}s — "
            f"avg={avg:.2f}s p50={p50:.2f}s p95={p95:.2f}s "
            f"min={mn:.2f}s max={mx:.2f}s"
        )
        with self._lock:
            self._batch_timings.append({
                "iteration": self._current_iteration,
                "op": op,
                "batch_label": batch_label,
                "batch_elapsed": round(batch_elapsed, 2),
                "count": count or n,
                "avg": round(avg, 4),
                "p50": round(p50, 4),
                "p95": round(p95, 4),
                "min": round(mn, 4),
                "max": round(mx, 4),
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

    # ── Verification helpers ──────────────────────────────────────────────

    def _verify_all_lvols_exist(self):
        """Verify all registered parents and children exist in lvol list."""
        all_lvols = self.sbcli_utils.list_lvols()
        missing = []
        with self._lock:
            for name in self._parent_registry:
                if name not in all_lvols:
                    missing.append(("parent", name))
            for name in self._child_registry:
                if name not in all_lvols:
                    missing.append(("child", name))
        if missing:
            raise RuntimeError(
                f"[verify_lvols] {len(missing)} lvols missing from API: "
                f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
            )
        total = len(self._parent_registry) + len(self._child_registry)
        self.logger.info(f"[verify_lvols] All {total} lvols confirmed in API")

    def _verify_all_snapshots_exist(self):
        """Verify all registered snapshots exist in snapshot list."""
        all_snaps = self.sbcli_utils.list_snapshots()
        missing = []
        with self._lock:
            for name in self._snap_registry:
                if name not in all_snaps:
                    missing.append(name)
        if missing:
            raise RuntimeError(
                f"[verify_snapshots] {len(missing)} snapshots missing: "
                f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
            )
        self.logger.info(
            f"[verify_snapshots] All {len(self._snap_registry)} snapshots "
            f"confirmed in API"
        )

    def _verify_all_clones_exist(self):
        """Verify all registered clones exist in lvol list."""
        all_lvols = self.sbcli_utils.list_lvols()
        missing = []
        with self._lock:
            for name in self._clone_registry:
                if name not in all_lvols:
                    missing.append(name)
        if missing:
            raise RuntimeError(
                f"[verify_clones] {len(missing)} clones missing from API: "
                f"{missing[:10]}{'...' if len(missing) > 10 else ''}"
            )
        self.logger.info(
            f"[verify_clones] All {len(self._clone_registry)} clones "
            f"confirmed in API"
        )

    def _verify_nodes_healthy(self):
        """Verify all storage nodes are online and healthy."""
        nodes_data = self.sbcli_utils.get_storage_nodes()
        unhealthy = []
        for node in nodes_data.get("results", []):
            node_id = node.get("id", "?")
            hostname = node.get("hostname", "?")
            status = node.get("status", "unknown")
            health = node.get("health_check", None)
            if status != "online" or health is not True:
                unhealthy.append(
                    f"{hostname}(id={node_id}, status={status}, "
                    f"health={health})"
                )
        if unhealthy:
            raise RuntimeError(
                f"[verify_nodes] Unhealthy nodes: {', '.join(unhealthy)}"
            )
        total = len(nodes_data.get("results", []))
        self.logger.info(
            f"[verify_nodes] All {total} storage nodes online and healthy"
        )

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
            self._stop_event.set()
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

    def _phase_create_subsystems(self):
        """Sequential per-parent: create parent + children + verify."""
        raise NotImplementedError

    def _phase_write_data(self):
        """Write 10 MB to each parent lvol before snapshotting."""
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

    def _phase_verify_cleanup(self):
        """Verify all test resources are gone before next iteration."""
        all_lvols = self.sbcli_utils.list_lvols()
        if all_lvols:
            self.logger.warning(
                f"[verify_cleanup] {len(all_lvols)} lvols still present "
                f"— retrying cleanup"
            )
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
            sleep_n_sec(10)
            remaining = self.sbcli_utils.list_lvols()
            if remaining:
                raise RuntimeError(
                    f"Cleanup verification failed: "
                    f"{len(remaining)} lvols still exist"
                )
        self.logger.info("[verify_cleanup] All resources confirmed deleted")

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
        snap_t0 = time.time()
        _ok, fail = self._batch_parallel(
            items, self._timed_create_snapshot,
            self.MAX_WORKERS_CREATE, "create_snapshots",
        )
        snap_elapsed = time.time() - snap_t0
        self._log_op_stats(
            "create_snapshot", batch_label="all snapshots",
            batch_elapsed=snap_elapsed,
        )
        if fail > 0:
            raise RuntimeError(
                f"[create_snapshots] {fail}/{len(items)} snapshots failed"
            )

    def _phase_create_clones(self):
        """Pick 1 random snapshot and create NUM_CLONES clones in batches."""
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
        all_items = []
        for i in range(self.NUM_CLONES):
            clone_name = f"cln-{_rand_seq(6)}-{i:04d}"
            all_items.append({
                "name": clone_name,
                "snap_name": chosen_snap,
                "snap_id": snap_id,
            })

        total_batches = (
            (len(all_items) + self.CLONE_BATCH_SIZE - 1)
            // self.CLONE_BATCH_SIZE
        )
        overall_t0 = time.time()

        for batch_idx in range(0, len(all_items), self.CLONE_BATCH_SIZE):
            batch = all_items[batch_idx:batch_idx + self.CLONE_BATCH_SIZE]
            batch_num = batch_idx // self.CLONE_BATCH_SIZE + 1
            self.logger.info(
                f"[create_clones] Batch {batch_num}/{total_batches}: "
                f"{len(batch)} clones"
            )
            batch_t0 = time.time()
            _ok, batch_fail = self._batch_parallel(
                batch, self._timed_create_clone,
                self.MAX_WORKERS_CREATE,
                f"create_clones_b{batch_num}",
            )
            batch_elapsed = time.time() - batch_t0
            if batch_fail > 0:
                raise RuntimeError(
                    f"[create_clones] Batch {batch_num}: "
                    f"{batch_fail}/{len(batch)} clones failed"
                )
            # Per-batch stats (only for clones created in this batch)
            with self._lock:
                batch_samples = [
                    s["elapsed_sec"] for s in self._timing_samples
                    if (s["iteration"] == self._current_iteration
                        and s["op"] == "create_clone"
                        and s["timestamp"] >= batch_t0)
                ]
            if batch_samples:
                bs = sorted(batch_samples)
                n = len(bs)
                self.logger.info(
                    f"[create_clones] Batch {batch_num} stats: "
                    f"{n} ops in {batch_elapsed:.1f}s — "
                    f"avg={sum(bs)/n:.2f}s "
                    f"p50={bs[n//2]:.2f}s "
                    f"p95={bs[min(int(n*0.95), n-1)]:.2f}s "
                    f"min={bs[0]:.2f}s max={bs[-1]:.2f}s"
                )
                with self._lock:
                    self._batch_timings.append({
                        "iteration": self._current_iteration,
                        "op": "create_clone",
                        "batch_label": f"batch {batch_num}/{total_batches}",
                        "batch_elapsed": round(batch_elapsed, 2),
                        "count": n,
                        "avg": round(sum(bs) / n, 4),
                        "p50": round(bs[n // 2], 4),
                        "p95": round(bs[min(int(n * 0.95), n - 1)], 4),
                        "min": round(bs[0], 4),
                        "max": round(bs[-1], 4),
                    })

        overall_elapsed = time.time() - overall_t0
        self._log_op_stats(
            "create_clone", batch_label="all clones",
            batch_elapsed=overall_elapsed,
        )

    def _phase_delete_all(self):
        """Delete: clones → snapshots → children → parents (ordered)."""
        total_failures = 0

        # Step 1: clones
        with self._lock:
            clone_names = list(self._clone_registry.keys())
        if clone_names:
            self.logger.info(f"[delete_all] Deleting {len(clone_names)} clones")
            t0 = time.time()
            _ok, fail = self._batch_parallel(
                clone_names, self._timed_delete_clone,
                self.MAX_WORKERS_DELETE, "delete_clones",
            )
            self._log_op_stats(
                "delete_clone", batch_label="all clones",
                batch_elapsed=time.time() - t0, count=len(clone_names),
            )
            if fail > 0:
                self.logger.warning(
                    f"[delete_all] {fail}/{len(clone_names)} clone "
                    f"deletions failed"
                )
                total_failures += fail

        # Step 2: snapshots
        with self._lock:
            snap_names = list(self._snap_registry.keys())
        if snap_names:
            self.logger.info(f"[delete_all] Deleting {len(snap_names)} snapshots")
            t0 = time.time()
            _ok, fail = self._batch_parallel(
                snap_names, self._timed_delete_snapshot,
                self.MAX_WORKERS_DELETE, "delete_snapshots",
            )
            self._log_op_stats(
                "delete_snapshot", batch_label="all snapshots",
                batch_elapsed=time.time() - t0, count=len(snap_names),
            )
            if fail > 0:
                self.logger.warning(
                    f"[delete_all] {fail}/{len(snap_names)} snapshot "
                    f"deletions failed"
                )
                total_failures += fail

        # Step 3: children
        with self._lock:
            child_names = list(self._child_registry.keys())
        if child_names:
            self.logger.info(f"[delete_all] Deleting {len(child_names)} children")
            t0 = time.time()
            _ok, fail = self._batch_parallel(
                child_names, self._timed_delete_child,
                self.MAX_WORKERS_DELETE, "delete_children",
            )
            self._log_op_stats(
                "delete_child", batch_label="all children",
                batch_elapsed=time.time() - t0, count=len(child_names),
            )
            if fail > 0:
                self.logger.warning(
                    f"[delete_all] {fail}/{len(child_names)} child "
                    f"deletions failed"
                )
                total_failures += fail

        # Step 4: parents
        with self._lock:
            parent_names = list(self._parent_registry.keys())
        if parent_names:
            self.logger.info(f"[delete_all] Deleting {len(parent_names)} parents")
            t0 = time.time()
            _ok, fail = self._batch_parallel(
                parent_names, self._timed_delete_parent,
                self.MAX_WORKERS_DELETE, "delete_parents",
            )
            self._log_op_stats(
                "delete_parent", batch_label="all parents",
                batch_elapsed=time.time() - t0, count=len(parent_names),
            )
            if fail > 0:
                self.logger.warning(
                    f"[delete_all] {fail}/{len(parent_names)} parent "
                    f"deletions failed"
                )
                total_failures += fail

        if total_failures > 0:
            self.logger.warning(
                f"[delete_all] Total: {total_failures} deletion failures — "
                f"verify_cleanup phase will retry"
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
            "batch_timings": self._batch_timings,
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
                "create_subsystems", "write_data",
                "create_snapshots", "create_clones", "delete_all",
                "verify_cleanup",
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

        # ── 6. Batch timing stats (bar chart) ────────────────────────────
        try:
            bt = self._batch_timings
            if bt:
                clone_batches = [
                    b for b in bt
                    if b["op"] == "create_clone"
                    and b["batch_label"].startswith("batch ")
                ]
                if clone_batches:
                    fig, ax = plt.subplots(figsize=(14, 8))
                    labels = [b["batch_label"] for b in clone_batches]
                    avgs = [b["avg"] for b in clone_batches]
                    p50s = [b["p50"] for b in clone_batches]
                    p95s = [b["p95"] for b in clone_batches]
                    x = range(len(labels))
                    width = 0.25
                    ax.bar(
                        [i - width for i in x], avgs, width,
                        label="avg", color=colors[0],
                    )
                    ax.bar(x, p50s, width, label="p50", color=colors[1])
                    ax.bar(
                        [i + width for i in x], p95s, width,
                        label="p95", color=colors[2],
                    )
                    ax.set_xlabel("Clone Batch")
                    ax.set_ylabel("Latency (sec)")
                    ax.set_title("Clone Creation — Per-Batch Latency Stats")
                    ax.set_xticks(list(x))
                    ax.set_xticklabels(labels, rotation=45, fontsize=7)
                    ax.legend(fontsize=7)
                    fig.tight_layout()
                    fig.savefig(
                        os.path.join(
                            out_dir, "clone_batch_latency_stats.png"
                        ),
                        dpi=150,
                    )
                    plt.close(fig)
                    self.logger.info(
                        "Generated clone_batch_latency_stats.png"
                    )
        except Exception as exc:
            self.logger.warning(f"Graph 6 failed: {exc}")

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
                    ("create_subsystems", self._phase_create_subsystems),
                    ("verify_lvols", self._verify_all_lvols_exist),
                    ("verify_nodes_healthy", self._verify_nodes_healthy),
                    ("write_data", self._phase_write_data),
                    ("create_snapshots", self._phase_create_snapshots),
                    ("verify_snapshots", self._verify_all_snapshots_exist),
                    ("create_clones", self._phase_create_clones),
                    ("verify_clones", self._verify_all_clones_exist),
                    ("verify_nodes_final", self._verify_nodes_healthy),
                    ("delete_all", self._phase_delete_all),
                    ("verify_cleanup", self._phase_verify_cleanup),
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
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        if actual_pool and actual_pool != self.pool_name:
            self.logger.info(
                f"[setup] Pool name changed: {self.pool_name} -> {actual_pool}"
            )
            self.pool_name = actual_pool
        sleep_n_sec(2)

    def _phase_cleanup(self):
        self.logger.info("[cleanup] Bulk delete safety net (ns-* only)")
        # Delete only test resources by prefix, not all lvols
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            all_lvols = self.sbcli_utils.list_lvols()
            test_lvols = [
                name for name in all_lvols
                if name.startswith("ns-") or name.startswith("cln-")
                or name.startswith("snap-")
            ]
            self.logger.info(
                f"[cleanup] Deleting {len(test_lvols)}/{len(all_lvols)} "
                f"test lvols"
            )
            for lv_name in test_lvols:
                try:
                    self.sbcli_utils.delete_lvol(
                        lvol_name=lv_name, skip_error=True
                    )
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

    # ── Two-phase subsystem creation: parents then parallel children ────

    def _phase_create_subsystems(self):
        """Sub-phase 1: create all parents sequentially.
        Sub-phase 2: create children for PARALLEL_PARENTS parents concurrently."""
        total_expected = self.NUM_PARENTS * (1 + self.CHILDREN_PER_PARENT)
        self.logger.info(
            f"[create_subsystems] {self.NUM_PARENTS} parents × "
            f"(1 + {self.CHILDREN_PER_PARENT} children) = "
            f"{total_expected} lvols (parallel={self.PARALLEL_PARENTS})"
        )

        # ── Sub-phase 1: Create all parents (sequential) ────────────
        self.logger.info(
            f"[create_subsystems][sub1] Creating {self.NUM_PARENTS} parents "
            f"(sequential)"
        )
        parent_names = []
        for i in range(self.NUM_PARENTS):
            parent_name = f"ns-par-{_rand_seq(6)}-{i:04d}"
            self.logger.info(
                f"[create_subsystems][sub1] Parent {i+1}/"
                f"{self.NUM_PARENTS}: {parent_name}"
            )
            t0 = time.time()
            self._create_parent(parent_name)
            self._record_timing(
                "create_parent", parent_name,
                time.time() - t0, self._snapshot_inventory(),
            )
            parent_names.append(parent_name)

        self.logger.info(
            f"[create_subsystems][sub1] All {len(parent_names)} parents created"
        )

        # ── Sub-phase 2: Create children (PARALLEL_PARENTS concurrent) ──
        self.logger.info(
            f"[create_subsystems][sub2] Creating children for "
            f"{len(parent_names)} parents "
            f"(parallel, workers={self.PARALLEL_PARENTS})"
        )
        children_t0 = time.time()
        _ok, fail = self._batch_parallel(
            parent_names,
            self._create_children_for_parent_docker,
            self.PARALLEL_PARENTS,
            "create_children",
        )
        children_elapsed = time.time() - children_t0
        if fail > 0:
            raise RuntimeError(
                f"[create_subsystems][sub2] {fail} parent child-creation "
                f"batches failed"
            )
        self._log_op_stats(
            "create_child", batch_label="all children",
            batch_elapsed=children_elapsed,
        )

        # ── Verify total lvol count ──────────────────────────────────
        all_lvols = self.sbcli_utils.list_lvols()
        if len(all_lvols) < total_expected:
            self.logger.warning(
                f"[create_subsystems] lvol count {len(all_lvols)} < "
                f"expected {total_expected}"
            )

        self.logger.info(
            f"[create_subsystems] Done: {len(self._parent_registry)} parents, "
            f"{len(self._child_registry)} children"
        )

    def _create_parent(self, name: str):
        """Create a single parent lvol + register. Raises on failure."""
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
        node_id = None
        try:
            details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
            if details:
                node_id = details[0].get("node_id")
        except Exception as ex:
            self.logger.warning(
                f"[create_parent] {name}: could not get node_id: {ex}"
            )
        self._parent_registry[name] = {
            "id": lvol_id, "node_id": node_id,
            "children": [], "snapshots": [],
        }
        self._inc("counts", "parents_created")
        self.logger.info(
            f"[create_parent] {name} -> {lvol_id} (node={node_id})"
        )

    def _create_child(self, name: str, parent_name: str,
                      parent_id: str, parent_node_id: str):
        """Create a single child namespace lvol. Raises on failure."""
        self._inc("attempts", "create_child")
        self._api_retry("create_child", lambda: self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            distr_ndcs=self.ndcs,
            distr_npcs=self.npcs,
            distr_bs=self.bs,
            distr_chunk_bs=self.chunk_bs,
            host_id=parent_node_id,
            namespace=parent_id,
            retry=1,
        ), ctx={"name": name, "parent": parent_name})
        child_id = self._wait_lvol_id(name)
        self._child_registry[name] = {
            "id": child_id, "parent_name": parent_name,
        }
        self._parent_registry[parent_name]["children"].append(name)
        self._inc("counts", "children_created")
        self.logger.info(
            f"[create_child] {name} -> {child_id} (parent={parent_name})"
        )

    def _create_children_for_parent_docker(self, parent_name: str):
        """Create all children for one parent sequentially.

        Called from _batch_parallel with PARALLEL_PARENTS concurrency.
        Children within a parent must be sequential for device detection."""
        pinfo = self._parent_registry.get(parent_name)
        if not pinfo:
            raise RuntimeError(f"{parent_name}: not in registry")
        parent_id = pinfo["id"]
        parent_node_id = pinfo.get("node_id")

        for c in range(self.CHILDREN_PER_PARENT):
            child_name = (
                f"ns-ch-{_rand_seq(6)}-{parent_name[-4:]}-{c:02d}"
            )
            t0 = time.time()
            self._create_child(
                child_name, parent_name, parent_id, parent_node_id,
            )
            self._record_timing(
                "create_child", child_name,
                time.time() - t0, self._snapshot_inventory(),
            )

        # Verify all lvols for this parent are in API
        all_lvols = self.sbcli_utils.list_lvols()
        expected = [parent_name] + [
            cn for cn, ci in self._child_registry.items()
            if ci["parent_name"] == parent_name
        ]
        missing = [n for n in expected if n not in all_lvols]
        if missing:
            raise RuntimeError(
                f"Parent {parent_name}: {len(missing)} lvols missing "
                f"from API after creation: {missing}"
            )
        self.logger.info(
            f"[create_children] {parent_name}: "
            f"{self.CHILDREN_PER_PARENT} children verified"
        )

    # ── Write data to parent lvols ───────────────────────────────────────

    def _phase_write_data(self):
        """NVMe-connect to each parent, write 10 MB, disconnect."""
        client = self.fio_node[0]
        parents = list(self._parent_registry.items())
        self.logger.info(
            f"[write_data] Writing 10 MB to {len(parents)} parent lvols "
            f"from client {client}"
        )

        for idx, (pname, pinfo) in enumerate(parents):
            try:
                self._write_data_to_lvol(client, pname, pinfo["id"])
                self.logger.info(
                    f"[write_data] {idx+1}/{len(parents)} {pname} OK"
                )
            except Exception as exc:
                raise RuntimeError(
                    f"[write_data] Failed to write data to {pname}: {exc}"
                )

        self.logger.info(f"[write_data] Done: {len(parents)} lvols written")

    def _write_data_to_lvol(self, client: str, lvol_name: str, lvol_id: str):
        """Connect, write 10 MB raw data, disconnect for a single lvol."""
        connect_strs = self.sbcli_utils.get_lvol_connect_str(lvol_name)
        if not connect_strs:
            raise RuntimeError(f"No connect strings for {lvol_name}")

        # Get NQN from connect string for later disconnect
        nqn = None
        for cs in connect_strs:
            for part in cs.split():
                if part.startswith("--nqn="):
                    nqn = part.split("=", 1)[1]
                    break
            if nqn:
                break

        # NVMe connect
        for cs in connect_strs:
            self.ssh_obj.exec_command(client, cs)
        sleep_n_sec(3)

        # Discover the device — find NVMe device matching this NQN
        out, _ = self.ssh_obj.exec_command(
            client,
            "sudo nvme list-subsys -o json 2>/dev/null || echo '[]'",
            supress_logs=True,
        )
        import json as _json
        device = None
        try:
            subsys_data = _json.loads(out)
            if isinstance(subsys_data, list) and subsys_data:
                subsys_data = subsys_data[0]
            for ss in subsys_data.get("Subsystems", []):
                if ss.get("NQN") == nqn:
                    for path in ss.get("Paths", []):
                        dev_name = path.get("Name")
                        if dev_name:
                            device = f"/dev/{dev_name}"
                            break
                    break
        except Exception:
            pass

        if not device:
            # Fallback: use nvme list and find newest device
            out2, _ = self.ssh_obj.exec_command(
                client,
                "lsblk -dn -o NAME,TYPE | grep disk | grep nvme | "
                "tail -1 | awk '{print $1}'",
                supress_logs=True,
            )
            dev_name = out2.strip()
            if dev_name:
                device = f"/dev/{dev_name}"

        if not device:
            raise RuntimeError(
                f"Could not find NVMe device for {lvol_name} (nqn={nqn})"
            )

        # Write 10 MB of data
        self.ssh_obj.exec_command(
            client,
            f"sudo dd if=/dev/urandom of={device} bs=1M count=10 "
            f"oflag=direct 2>/dev/null",
        )

        # NVMe disconnect
        if nqn:
            self.ssh_obj.exec_command(
                client, f"sudo nvme disconnect -n {nqn}",
            )

    # ── Create implementations ────────────────────────────────────────────

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
        actual_pool = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        if actual_pool and actual_pool != self.pool_name:
            self.logger.info(
                f"[setup] Pool name changed: {self.pool_name} -> {actual_pool}"
            )
            self.pool_name = actual_pool
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
        # Targeted sbcli cleanup — only test resources
        try:
            self.sbcli_utils.delete_all_clones()
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_snapshots()
        except Exception:
            pass
        try:
            all_lvols = self.sbcli_utils.list_lvols()
            test_lvols = [
                name for name in all_lvols
                if name.startswith("ns-") or name.startswith("cln-")
                or name.startswith("snap-")
            ]
            self.logger.info(
                f"[cleanup] Deleting {len(test_lvols)}/{len(all_lvols)} "
                f"test lvols"
            )
            for lv_name in test_lvols:
                try:
                    self.sbcli_utils.delete_lvol(
                        lvol_name=lv_name, skip_error=True
                    )
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self.sbcli_utils.delete_all_storage_pools()
        except Exception:
            pass

    def _phase_verify_cleanup(self):
        """K8s override: also verify no test PVCs remain."""
        ns = self.k8s_utils.namespace if self.k8s_utils else "default"
        # Check K8s PVCs with test label
        if self.k8s_utils:
            try:
                output = self.k8s_utils._exec_kubectl(
                    f"kubectl get pvc -l test=ns-stress -n {ns} "
                    f"--no-headers 2>/dev/null || true"
                )
                if output and output.strip():
                    lines = [
                        l for l in output.strip().split("\n") if l.strip()
                    ]
                    self.logger.warning(
                        f"[verify_cleanup] {len(lines)} test PVCs still "
                        f"present — force deleting"
                    )
                    self.k8s_utils._exec_kubectl(
                        f"kubectl delete pvc -l test=ns-stress -n {ns} "
                        f"--wait=false --ignore-not-found 2>/dev/null || true"
                    )
                    sleep_n_sec(10)
            except Exception:
                pass
        # Delegate to base for sbcli-level verification
        super()._phase_verify_cleanup()

    # ── Two-phase subsystem creation: parents then parallel children ────

    def _phase_create_subsystems(self):
        """Sub-phase 1: create all parent PVCs sequentially.
        Sub-phase 2: create children for PARALLEL_PARENTS subsystems
        concurrently."""
        pvcs_per_subsys = 1 + self.CHILDREN_PER_PARENT
        total = self.NUM_PARENTS * pvcs_per_subsys
        self.logger.info(
            f"[create_subsystems] {self.NUM_PARENTS} subsystems × "
            f"{pvcs_per_subsys} PVCs = {total} total "
            f"(parallel={self.PARALLEL_PARENTS})"
        )

        # ── Sub-phase 1: Create all parent PVCs (sequential) ────────
        self.logger.info(
            f"[create_subsystems][sub1] Creating {self.NUM_PARENTS} parent "
            f"PVCs (sequential)"
        )
        parent_names = []
        for i in range(self.NUM_PARENTS):
            parent_name = f"ns-pvc-{_rand_seq(6)}-{i:04d}"
            self.logger.info(
                f"[create_subsystems][sub1] Parent {i+1}/"
                f"{self.NUM_PARENTS}: {parent_name}"
            )
            t0 = time.time()
            self._create_pvc(parent_name)
            self._record_timing(
                "create_parent", parent_name,
                time.time() - t0, self._snapshot_inventory(),
            )
            self._parent_registry[parent_name] = {
                "id": parent_name,
                "children": [],
                "snapshots": [],
                "start_child_idx": i * pvcs_per_subsys + 1,
            }
            self._inc("counts", "parents_created")
            parent_names.append(parent_name)

        self.logger.info(
            f"[create_subsystems][sub1] All {len(parent_names)} parents "
            f"created"
        )

        # ── Sub-phase 2: Create child PVCs (PARALLEL_PARENTS concurrent) ─
        self.logger.info(
            f"[create_subsystems][sub2] Creating children for "
            f"{len(parent_names)} subsystems "
            f"(parallel, workers={self.PARALLEL_PARENTS})"
        )
        children_t0 = time.time()
        _ok, fail = self._batch_parallel(
            parent_names,
            self._create_children_for_subsystem_k8s,
            self.PARALLEL_PARENTS,
            "create_children",
        )
        children_elapsed = time.time() - children_t0
        if fail > 0:
            raise RuntimeError(
                f"[create_subsystems][sub2] {fail} subsystem child-creation "
                f"batches failed"
            )
        self._log_op_stats(
            "create_child", batch_label="all children",
            batch_elapsed=children_elapsed,
        )

        # ── Bulk verify ──────────────────────────────────────────────
        all_lvols = self.sbcli_utils.list_lvols()
        if len(all_lvols) < total:
            self.logger.warning(
                f"[create_subsystems] lvol count {len(all_lvols)} < "
                f"expected {total}"
            )

        self.logger.info(
            f"[create_subsystems] Done: {len(self._parent_registry)} "
            f"parents, {len(self._child_registry)} children"
        )

    def _create_children_for_subsystem_k8s(self, parent_name: str):
        """Create all child PVCs for one subsystem sequentially.

        Called from _batch_parallel with PARALLEL_PARENTS concurrency.
        PVCs within a subsystem must be sequential for CSI grouping."""
        pinfo = self._parent_registry.get(parent_name)
        if not pinfo:
            raise RuntimeError(f"{parent_name}: not in registry")
        start_idx = pinfo.get("start_child_idx", 0)

        for c in range(self.CHILDREN_PER_PARENT):
            child_idx = start_idx + c
            child_name = f"ns-pvc-{_rand_seq(6)}-{child_idx:04d}"
            t0 = time.time()
            self._create_pvc(child_name)
            self._record_timing(
                "create_child", child_name,
                time.time() - t0, self._snapshot_inventory(),
            )
            self._child_registry[child_name] = {
                "id": child_name, "parent_name": parent_name,
            }
            with self._lock:
                self._parent_registry[parent_name]["children"].append(
                    child_name
                )
            self._inc("counts", "children_created")

        self.logger.info(
            f"[create_children] {parent_name}: "
            f"{self.CHILDREN_PER_PARENT} child PVCs created"
        )

    def _create_pvc(self, name: str):
        """Create a single PVC with label and wait for Bound."""
        ns = self.k8s_utils.namespace
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

    # ── Write data to parent PVCs ────────────────────────────────────────

    def _phase_write_data(self):
        """Create one-shot Jobs that write 10 MB to each parent PVC."""
        parents = list(self._parent_registry.keys())
        self.logger.info(
            f"[write_data] Writing 10 MB to {len(parents)} parent PVCs "
            f"via K8s Jobs"
        )
        ns = self.k8s_utils.namespace

        for idx, pvc_name in enumerate(parents):
            job_name = f"write-{pvc_name[:40]}-{_rand_seq(4)}"
            yaml_content = (
                f"apiVersion: batch/v1\n"
                f"kind: Job\n"
                f"metadata:\n"
                f"  name: {job_name}\n"
                f"  labels:\n"
                f"    test: ns-stress\n"
                f"    purpose: write-data\n"
                f"spec:\n"
                f"  backoffLimit: 0\n"
                f"  template:\n"
                f"    spec:\n"
                f"      restartPolicy: Never\n"
                f"      containers:\n"
                f"      - name: writer\n"
                f"        image: alpine\n"
                f"        command:\n"
                f"        - sh\n"
                f"        - -c\n"
                f"        - dd if=/dev/urandom of=/data/testfile "
                f"bs=1M count=10 2>/dev/null\n"
                f"        volumeMounts:\n"
                f"        - name: vol\n"
                f"          mountPath: /data\n"
                f"      volumes:\n"
                f"      - name: vol\n"
                f"        persistentVolumeClaim:\n"
                f"          claimName: {pvc_name}\n"
            )
            self.k8s_utils.apply_yaml(yaml_content, namespace=ns)
            result = self.k8s_utils.wait_job_complete(
                job_name, timeout=120, namespace=ns,
            )
            if result != "succeeded":
                raise RuntimeError(
                    f"[write_data] Job {job_name} for PVC {pvc_name} "
                    f"ended with: {result}"
                )
            # Clean up the job
            self.k8s_utils.delete_resource("job", job_name, namespace=ns)
            self.logger.info(
                f"[write_data] {idx+1}/{len(parents)} {pvc_name} OK"
            )

        self.logger.info(f"[write_data] Done: {len(parents)} PVCs written")

    # ── Create implementations ────────────────────────────────────────────

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
