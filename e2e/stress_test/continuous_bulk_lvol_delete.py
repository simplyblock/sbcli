"""
Bulk lvol create-delete stress test.

Creates a large batch of lvols (50 × 100G) with FIO running, waits 10 minutes,
then deletes them one-by-one verifying each deletion succeeds.  Runs 5 iterations.

Three modes:
  Docker  (BulkLvolDeleteDocker)  — sbcli API + NVMe connect + SSH FIO
  K8s     (BulkLvolDeleteK8s)     — PVC + FIO K8s Jobs OR Client SSH FIO

Invocation:
  # Docker
  python3 stress.py --testname BulkLvolDeleteDocker --ndcs 2 --npcs 2

  # K8s without client (FIO Jobs)
  python3 stress.py --testname BulkLvolDeleteK8s --ndcs 2 --npcs 2 --run_k8s True

  # K8s with client (SSH FIO)
  CLIENT_IP="10.0.0.5" python3 stress.py --testname BulkLvolDeleteK8s --ndcs 2 --npcs 2 --run_k8s True
"""

from __future__ import annotations

import random
import string
import threading
import time

from logger_config import setup_logger
from utils.common_utils import sleep_n_sec

logger = setup_logger(__name__)


def _rand_seq(length: int) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


# ─────────────────────────────────────────────────────────────────────────────
# Shared mixin: iteration loop, summary, cleanup contract
# ─────────────────────────────────────────────────────────────────────────────

class _BulkDeleteMixin:
    """Shared iteration logic: create batch → wait → delete one-by-one."""

    NUM_LVOLS = 50
    LVOL_SIZE = "100G"
    PVC_SIZE = "100Gi"
    FIO_SIZE = "14G"             # per job; 5 jobs × 14G = 70G ≈ 70% of 100G lvol
    FIO_NUMJOBS = 5
    WAIT_AFTER_CREATE = 600      # 10 minutes
    NUM_ITERATIONS = 5
    DELETE_INTERVAL = 5          # seconds between sequential deletes
    FIO_RUNTIME = 2000           # seconds per FIO job

    CORE_DUMP_DIR = "/etc/simplyblock"
    CORE_DUMP_PATTERN = "core*.zst"

    # ── Core dump helpers (used by graceful and hot-delete) ───────────

    def _get_storage_node_ips(self):
        """Get management IPs of all storage nodes."""
        nodes = self.sbcli_utils.get_storage_nodes()
        ips = []
        for n in nodes.get("results", []):
            ip = n.get("mgmt_ip") or n.get("ip")
            if ip:
                ips.append(ip)
        return ips

    def _snapshot_core_dumps(self, node_ips):
        """Return {ip: set(core_dump_paths)} for all storage nodes."""
        snapshots = {}
        for ip in node_ips:
            try:
                cmd = (
                    f"find {self.CORE_DUMP_DIR} -maxdepth 1 "
                    f"-name '{self.CORE_DUMP_PATTERN}' "
                    f"-type f 2>/dev/null || true"
                )
                output, _ = self.ssh_obj.exec_command(ip, cmd)
                files = set()
                if output and output.strip():
                    files = {
                        f.strip() for f in output.strip().split("\n")
                        if f.strip()
                    }
                snapshots[ip] = files
            except Exception as exc:
                self.logger.warning(
                    f"Could not snapshot core dumps on {ip}: {exc}"
                )
                snapshots[ip] = set()
        return snapshots

    def _check_new_core_dumps(self, node_ips, before_snapshots,
                              iteration, resource_name):
        """Check for new core dumps after a deletion.

        Returns True if new core dumps were found.
        Updates *before_snapshots* in-place so dumps are not re-reported.
        """
        found_new = False
        for ip in node_ips:
            try:
                cmd = (
                    f"find {self.CORE_DUMP_DIR} -maxdepth 1 "
                    f"-name '{self.CORE_DUMP_PATTERN}' "
                    f"-type f 2>/dev/null || true"
                )
                output, _ = self.ssh_obj.exec_command(ip, cmd)
                current = set()
                if output and output.strip():
                    current = {
                        f.strip() for f in output.strip().split("\n")
                        if f.strip()
                    }
                new_dumps = current - before_snapshots.get(ip, set())
                if new_dumps:
                    found_new = True
                    self.logger.error(
                        f"[coredump {iteration}] NEW CORE DUMPS on {ip} "
                        f"after deleting {resource_name}: {new_dumps}"
                    )
                    # Also grab dmesg tail for context
                    try:
                        dmesg_out, _ = self.ssh_obj.exec_command(
                            ip,
                            "dmesg | grep -i 'segfault\\|core dump\\|panic' "
                            "| tail -20 || true",
                        )
                        if dmesg_out and dmesg_out.strip():
                            self.logger.error(
                                f"[coredump {iteration}] dmesg on {ip}: "
                                f"{dmesg_out.strip()}"
                            )
                    except Exception:
                        pass
                    before_snapshots[ip] = current
            except Exception as exc:
                self.logger.warning(
                    f"Could not check core dumps on {ip}: {exc}"
                )
        if not found_new:
            self.logger.info(
                f"[coredump {iteration}] No new core dumps after "
                f"deleting {resource_name}"
            )
        return found_new

    @staticmethod
    def _extract_nqn_from_connect_str(connect_str):
        """Extract NQN from an nvme connect command string."""
        for part in connect_str.split():
            if part.startswith("--nqn="):
                return part.split("=", 1)[1]
        # Handle "-n <nqn>" or "--nqn <nqn>" form
        parts = connect_str.split()
        for i, part in enumerate(parts):
            if part in ("-n", "--nqn") and i + 1 < len(parts):
                return parts[i + 1]
        return None

    def _wait_lvol_deleted(self, lvol_name, timeout=300):
        """Poll until lvol is no longer listed. Returns True if deleted."""
        for _ in range(timeout // 5):
            remaining = self.sbcli_utils.list_lvols()
            if lvol_name not in remaining:
                return True
            sleep_n_sec(5)
        self.logger.warning(
            f"Lvol {lvol_name} still present after {timeout}s"
        )
        return False

    def _run_bulk_iterations(self):
        results = []
        for iteration in range(1, self.NUM_ITERATIONS + 1):
            self.logger.info(
                f"=== Bulk Delete Iteration {iteration}/{self.NUM_ITERATIONS} ==="
            )

            names = self._bulk_create(iteration)
            self.logger.info(
                f"Created {len(names)} resources.  "
                f"Waiting {self.WAIT_AFTER_CREATE}s with FIO running..."
            )
            sleep_n_sec(self.WAIT_AFTER_CREATE)

            t_del = time.time()
            result = self._bulk_delete_sequential(iteration, names)
            result["delete_duration"] = time.time() - t_del
            results.append(result)
            self.logger.info(
                f"Iteration {iteration} done: "
                f"created={result['created']} deleted={result['deleted']} "
                f"failed={result['failed']} stale={result['stale']} "
                f"delete_time={result['delete_duration']:.1f}s"
            )

        self._bulk_cleanup()
        self._print_bulk_summary(results)
        self._write_monitoring_json(results)
        self._generate_bulk_charts(results)

        total_failed = sum(r["failed"] + r["stale"] for r in results)
        total_core_dumps = sum(
            r.get("core_dumps_detected", 0) for r in results
        )

        if total_core_dumps > 0:
            raise RuntimeError(
                f"Bulk delete test detected {total_core_dumps} core dumps "
                f"on storage nodes across {self.NUM_ITERATIONS} iterations"
            )

        if total_failed > 0:
            raise RuntimeError(
                f"Bulk delete test had {total_failed} total failures across "
                f"{self.NUM_ITERATIONS} iterations"
            )

    # Subclasses MUST implement:
    #   _bulk_create(iteration) -> list[str]
    #   _bulk_delete_sequential(iteration, names) -> dict
    #   _bulk_cleanup()

    def _print_bulk_summary(self, results):
        self.logger.info("=== Bulk Lvol Delete Test Summary ===")
        self.logger.info(
            f"{'Iter':>4} | {'Created':>7} | {'Deleted':>7} | "
            f"{'Failed':>6} | {'Stale':>5}"
        )
        for r in results:
            self.logger.info(
                f"{r['iteration']:>4} | {r['created']:>7} | {r['deleted']:>7} | "
                f"{r['failed']:>6} | {r['stale']:>5}"
            )
        total_f = sum(r["failed"] for r in results)
        total_s = sum(r["stale"] for r in results)
        self.logger.info(f"Total failures: {total_f}  Total stale: {total_s}")

    def _write_monitoring_json(self, results):
        """Write standardised timing JSON for monitoring suite aggregation."""
        import json as _json
        from datetime import datetime, timezone
        from pathlib import Path

        phases = []
        total_core_dumps = 0
        for r in results:
            cd = r.get("core_dumps_detected", 0)
            total_core_dumps += cd
            per_lvol = r.get("per_lvol_times", [])
            avg_delete = 0
            if per_lvol:
                avg_delete = round(
                    sum(t["delete_sec"] for t in per_lvol) / len(per_lvol), 3
                )
            phases.append({
                "name": f"iteration_{r['iteration']}",
                "duration_sec": round(r.get("delete_duration", 0), 2),
                "status": "ok" if r["failed"] + r["stale"] == 0 else "degraded",
                "details": {
                    "created": r["created"],
                    "deleted": r["deleted"],
                    "failed": r["failed"],
                    "stale": r["stale"],
                    "core_dumps_detected": cd,
                    "avg_delete_sec": avg_delete,
                    "per_lvol_times": per_lvol,
                },
            })

        total_duration = sum(r.get("delete_duration", 0) for r in results)

        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed" if all(
                r["failed"] + r["stale"] == 0 for r in results
            ) and total_core_dumps == 0 else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "batch_size": self.NUM_LVOLS,
                "num_iterations": self.NUM_ITERATIONS,
                "lvol_size": self.LVOL_SIZE,
                "fio_size": self.FIO_SIZE,
                "fio_numjobs": self.FIO_NUMJOBS,
            },
            "phases": phases,
            "summary": {
                "total_duration_sec": round(total_duration, 2),
                "key_metric": round(total_duration, 2),
                "key_metric_label": "total_delete_duration_sec",
                "total_core_dumps": total_core_dumps,
            },
        }

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "bulk_delete_timing.json"
        with open(out_path, "w") as f:
            _json.dump(report, f, indent=2)
        self.logger.info(f"Monitoring JSON written to {out_path}")

    def _generate_bulk_charts(self, results):
        """Generate per-lvol and per-iteration timing charts."""
        from pathlib import Path

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)

        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:
            self.logger.warning("matplotlib not available — skipping charts")
            return

        class_name = self.__class__.__name__

        # ── Chart 1: Per-lvol delete time (line chart, one line per iteration) ──
        try:
            fig, ax = plt.subplots(figsize=(14, 6))
            has_data = False
            for r in results:
                per_lvol = r.get("per_lvol_times", [])
                if not per_lvol:
                    continue
                has_data = True
                xs = [t["index"] for t in per_lvol]
                ys = [t["delete_sec"] for t in per_lvol]
                ax.plot(xs, ys, marker=".", markersize=3, linewidth=1,
                        label=f"Iter {r['iteration']}", alpha=0.8)

            if has_data:
                ax.set_xlabel("Lvol Index (within batch)")
                ax.set_ylabel("Delete Time (seconds)")
                ax.set_title(f"{class_name} — Per-Lvol Delete Time")
                ax.legend(fontsize=8)
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                path = out_dir / "per_lvol_delete_time.png"
                fig.savefig(str(path), dpi=150)
                self.logger.info(f"Chart saved: {path}")
            plt.close(fig)
        except Exception as exc:
            self.logger.warning(f"Per-lvol chart failed: {exc}")

        # ── Chart 2: Per-iteration summary (bar chart) ──
        try:
            iters = [r["iteration"] for r in results]
            avgs = []
            maxs = []
            mins = []
            totals = []
            for r in results:
                per_lvol = r.get("per_lvol_times", [])
                times = [t["delete_sec"] for t in per_lvol] if per_lvol else [0]
                avgs.append(sum(times) / len(times) if times else 0)
                maxs.append(max(times) if times else 0)
                mins.append(min(times) if times else 0)
                totals.append(r.get("delete_duration", 0))

            fig, axes = plt.subplots(1, 2, figsize=(14, 5))

            # Left: avg/min/max per iteration
            x = range(len(iters))
            width = 0.25
            axes[0].bar([i - width for i in x], mins, width,
                        label="Min", color="#2ecc71", alpha=0.8)
            axes[0].bar(list(x), avgs, width,
                        label="Avg", color="#3498db", alpha=0.8)
            axes[0].bar([i + width for i in x], maxs, width,
                        label="Max", color="#e74c3c", alpha=0.8)
            for i, v in enumerate(avgs):
                axes[0].text(i, v + max(maxs) * 0.02, f"{v:.1f}s",
                             ha="center", fontsize=7)
            axes[0].set_xticks(list(x))
            axes[0].set_xticklabels([f"Iter {i}" for i in iters])
            axes[0].set_ylabel("Delete Time per Lvol (seconds)")
            axes[0].set_title("Per-Iteration: Min / Avg / Max Delete Time")
            axes[0].legend(fontsize=8)
            axes[0].grid(True, axis="y", alpha=0.3)

            # Right: total batch delete duration
            axes[1].bar(list(x), totals, color="#9b59b6", alpha=0.8)
            for i, v in enumerate(totals):
                axes[1].text(i, v + max(totals) * 0.02, f"{v:.0f}s",
                             ha="center", fontsize=8)
            axes[1].set_xticks(list(x))
            axes[1].set_xticklabels([f"Iter {i}" for i in iters])
            axes[1].set_ylabel("Total Batch Delete Duration (seconds)")
            axes[1].set_title("Per-Iteration: Total Delete Duration")
            axes[1].grid(True, axis="y", alpha=0.3)

            plt.suptitle(f"{class_name} — Iteration Summary", fontsize=12)
            plt.tight_layout()
            path = out_dir / "iteration_summary.png"
            fig.savefig(str(path), dpi=150)
            self.logger.info(f"Chart saved: {path}")
            plt.close(fig)
        except Exception as exc:
            self.logger.warning(f"Iteration summary chart failed: {exc}")

        # ── Chart 3: Delete time distribution (histogram) ──
        try:
            all_times = []
            for r in results:
                per_lvol = r.get("per_lvol_times", [])
                all_times.extend(t["delete_sec"] for t in per_lvol)

            if all_times:
                fig, ax = plt.subplots(figsize=(10, 5))
                ax.hist(all_times, bins=min(50, max(10, len(all_times) // 5)),
                        color="#3498db", edgecolor="white", alpha=0.8)
                ax.axvline(sum(all_times) / len(all_times), color="#e74c3c",
                           linestyle="--", linewidth=1.5,
                           label=f"Mean: {sum(all_times)/len(all_times):.1f}s")
                ax.set_xlabel("Delete Time (seconds)")
                ax.set_ylabel("Count")
                ax.set_title(
                    f"{class_name} — Delete Time Distribution "
                    f"(n={len(all_times)})"
                )
                ax.legend()
                ax.grid(True, axis="y", alpha=0.3)
                plt.tight_layout()
                path = out_dir / "delete_time_distribution.png"
                fig.savefig(str(path), dpi=150)
                self.logger.info(f"Chart saved: {path}")
                plt.close(fig)
        except Exception as exc:
            self.logger.warning(f"Distribution chart failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Docker variant
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.lvol_ha_stress_fio import TestLvolHACluster  # noqa: E402


class BulkLvolDeleteDocker(_BulkDeleteMixin, TestLvolHACluster):
    """
    Docker-mode bulk create+FIO → sequential delete stress test.

    Inherits from TestLvolHACluster for sbcli_utils, ssh_obj, NVMe connect,
    FIO thread management, pool/node setup.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_delete_docker"
        self.total_lvols = self.NUM_LVOLS
        self.lvol_size = self.LVOL_SIZE
        self.fio_size = self.FIO_SIZE
        self.sn_nodes = []
        self.node_vs_lvol = {}
        self.lvol_mount_details = {}
        self.fio_threads = []
        self._run_id = _rand_seq(8)

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])

        self._run_bulk_iterations()

    # ── Create ────────────────────────────────────────────────────────────

    def _bulk_create(self, iteration):
        names = []
        for i in range(self.NUM_LVOLS):
            lvol_name = f"bulk-{self._run_id}-i{iteration}-{i:03d}"
            self.logger.info(
                f"[create {iteration}] Creating lvol {lvol_name} "
                f"({i+1}/{self.NUM_LVOLS})"
            )

            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=self.LVOL_SIZE,
                    distr_ndcs=self.ndcs,
                    distr_npcs=self.npcs,
                    distr_bs=self.bs,
                    distr_chunk_bs=self.chunk_bs,
                    retry=3,
                )
            except Exception as exc:
                self.logger.error(
                    f"[create {iteration}] add_lvol failed for {lvol_name}: {exc}"
                )
                continue

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            if not lvol_id:
                self.logger.error(
                    f"[create {iteration}] {lvol_name} not visible after add_lvol"
                )
                continue

            # Pick a client node for NVMe connect + FIO
            client_node = random.choice(self.fio_node)
            fs_type = random.choice(["ext4", "xfs"])

            # Get NVMe connect strings
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            # Snapshot devices before connect
            initial_devices = self.ssh_obj.get_devices(node=client_node)

            # Connect all paths
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(
                    node=client_node, command=connect_str
                )
                if error:
                    self.logger.warning(
                        f"[create {iteration}] NVMe connect warning for "
                        f"{lvol_name}: {error}"
                    )

            sleep_n_sec(3)

            # Detect new device
            final_devices = self.ssh_obj.get_devices(node=client_node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                self.logger.error(
                    f"[create {iteration}] {lvol_name} did not connect — "
                    f"no new device found"
                )
                continue

            # Format and mount
            self.ssh_obj.format_disk(
                node=client_node, device=lvol_device, fs_type=fs_type
            )
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(
                node=client_node, device=lvol_device, mount_path=mount_point
            )

            sleep_n_sec(5)

            # Clean old FIO files
            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])

            # Start FIO
            log_file = f"{self.log_path}/{lvol_name}.log"
            iolog_file = f"{self.log_path}/{lvol_name}_fio_iolog"
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_point, log_file),
                kwargs={
                    "size": self.FIO_SIZE,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": self.FIO_NUMJOBS,
                    "time_based": True,
                    "runtime": self.FIO_RUNTIME,
                    "log_avg_msec": 1000,
                    "iolog_file": iolog_file,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

            # Track
            self.lvol_mount_details[lvol_name] = {
                "ID": lvol_id,
                "Command": connect_ls,
                "Mount": mount_point,
                "Device": lvol_device,
                "FS": fs_type,
                "Log": log_file,
                "Client": client_node,
                "snapshots": [],
                "iolog_base_path": iolog_file,
            }

            # Track node → lvol mapping
            lvol_node_id = None
            try:
                details = self.sbcli_utils.get_lvol_details(lvol_id)
                if details:
                    lvol_node_id = details[0].get("node_id")
            except Exception:
                pass
            if lvol_node_id:
                self.node_vs_lvol.setdefault(lvol_node_id, []).append(lvol_name)

            names.append(lvol_name)
            self.logger.info(
                f"[create {iteration}] {lvol_name} → {lvol_device} on "
                f"{client_node}, FIO started"
            )

        return names

    # ── Delete (sequential, one-by-one) ──────────────────────────────────

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0
        core_dump_count = 0
        per_lvol_times = []

        # Snapshot existing core dumps before starting deletions
        storage_ips = self._get_storage_node_ips()
        core_snapshots = self._snapshot_core_dumps(storage_ips)

        for idx, lvol_name in enumerate(names):
            self.logger.info(
                f"[delete {iteration}] Deleting {lvol_name} "
                f"({idx+1}/{len(names)})"
            )
            details = self.lvol_mount_details.get(lvol_name, {})
            client = details.get("Client", self.fio_node[0] if self.fio_node else None)

            # 1. Kill FIO
            if client:
                self.ssh_obj.find_process_name(
                    client, f"{lvol_name}_fio", return_pid=False
                )
                fio_pids = self.ssh_obj.find_process_name(
                    client, f"{lvol_name}_fio", return_pid=True
                )
                for pid in fio_pids:
                    self.ssh_obj.kill_processes(client, pid=pid)
                # Wait for FIO to stop
                for attempt in range(30):
                    fio_pids = self.ssh_obj.find_process_name(
                        client, f"{lvol_name}_fio", return_pid=True
                    )
                    if len(fio_pids) <= 2:
                        break
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(client, pid=pid)
                    sleep_n_sec(10)

            sleep_n_sec(5)

            # 2. Unmount
            if client and details.get("Mount"):
                try:
                    self.ssh_obj.unmount_path(client, details["Mount"])
                    self.ssh_obj.remove_dir(client, dir_path=details["Mount"])
                except Exception as exc:
                    self.logger.warning(
                        f"[delete {iteration}] Unmount failed for {lvol_name}: {exc}"
                    )

            # 3. Disconnect NVMe
            if client and details.get("Command"):
                nqns_disconnected = set()
                for connect_str in details["Command"]:
                    nqn = self._extract_nqn_from_connect_str(connect_str)
                    if nqn and nqn not in nqns_disconnected:
                        try:
                            self.ssh_obj.disconnect_nvme(
                                node=client, nqn_grep=nqn
                            )
                            nqns_disconnected.add(nqn)
                        except Exception as exc:
                            self.logger.warning(
                                f"[delete {iteration}] NVMe disconnect "
                                f"failed for {lvol_name}: {exc}"
                            )

            # 4. Delete lvol
            t_del_start = time.time()
            result = self.sbcli_utils.delete_lvol(
                lvol_name, max_attempt=120, skip_error=True
            )
            if result:
                deleted += 1
                self.logger.info(
                    f"[delete {iteration}] {lvol_name} deleted successfully"
                )
                # Wait for lvol to be actually deleted
                self._wait_lvol_deleted(lvol_name, timeout=300)
            else:
                failed += 1
                self.logger.error(
                    f"[delete {iteration}] {lvol_name} FAILED to delete"
                )
            delete_sec = round(time.time() - t_del_start, 3)
            per_lvol_times.append({
                "index": idx,
                "name": lvol_name,
                "delete_sec": delete_sec,
                "ok": bool(result),
            })
            self.logger.info(
                f"[delete {iteration}] {lvol_name} delete took {delete_sec:.1f}s"
            )

            # 5. Check core dumps 20s after delete
            sleep_n_sec(20)
            if self._check_new_core_dumps(
                storage_ips, core_snapshots, iteration, lvol_name
            ):
                core_dump_count += 1

            # 6. Clean up tracking
            self.lvol_mount_details.pop(lvol_name, None)
            for _, lvols in self.node_vs_lvol.items():
                if lvol_name in lvols:
                    lvols.remove(lvol_name)
                    break

            # Clean FIO log files
            if client:
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/local-{lvol_name}_fio*"]
                )
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/{lvol_name}_fio_iolog*"]
                )

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale lvols remain for this iteration
        remaining = self.sbcli_utils.list_lvols()
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale = [n for n in remaining if n.startswith(prefix)]
        if stale:
            self.logger.error(
                f"[verify {iteration}] {len(stale)} lvols still present: {stale}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": len(stale),
            "core_dumps_detected": core_dump_count,
            "per_lvol_times": per_lvol_times,
        }

    # ── Cleanup ──────────────────────────────────────────────────────────

    def _bulk_cleanup(self):
        self.logger.info("[cleanup] Running safety-net cleanup...")
        try:
            self.sbcli_utils.delete_all_clones()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] Safety-net cleanup failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# K8s variant
# ─────────────────────────────────────────────────────────────────────────────

from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest  # noqa: E402


class BulkLvolDeleteK8s(_BulkDeleteMixin, K8sNativeFailoverTest):
    """
    K8s-mode bulk create+FIO → sequential delete stress test.

    Inherits from K8sNativeFailoverTest for k8s_utils, PVC creation,
    FIO Job management, client SSH FIO, full setup()/teardown().

    Works in two sub-modes:
      - FIO as K8s Jobs (default, no CLIENT_IP)
      - FIO via SSH on external clients (CLIENT_IP env set)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_delete_k8s"
        self.pvc_size = self.PVC_SIZE
        self.fio_size = self.FIO_SIZE
        self.fio_num_jobs = self.FIO_NUMJOBS
        self.FIO_RUNTIME = _BulkDeleteMixin.FIO_RUNTIME
        self._run_id = _rand_seq(8)

    def run(self):
        # Discover storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

        # Create pool + StorageClass
        pool_test = self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.pool_name = self.pool_name if pool_test == self.pool_name else pool_test

        cluster_id = self.cluster_id or ""
        self.k8s_utils.create_storage_class(
            name=self.STORAGE_CLASS_NAME,
            cluster_id=cluster_id,
            pool_name=self.pool_name,
            ndcs=self.ndcs,
            npcs=self.npcs,
        )

        self._run_bulk_iterations()

    # ── Create ────────────────────────────────────────────────────────────

    def _bulk_create(self, iteration):
        names = []
        old_lvol_ids = set()

        for i in range(self.NUM_LVOLS):
            pvc_name = f"bulk-{self._run_id}-i{iteration}-{i:03d}"
            self.logger.info(
                f"[create {iteration}] Creating PVC {pvc_name} "
                f"({i+1}/{self.NUM_LVOLS})"
            )

            # Snapshot lvol IDs before PVC creation (for client mode mapping)
            if self.use_client_fio:
                old_lvol_ids = self._snapshot_lvol_ids()

            try:
                self.k8s_utils.create_pvc(
                    pvc_name, self.PVC_SIZE, self.STORAGE_CLASS_NAME,
                )
                self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
            except Exception as exc:
                self.logger.error(
                    f"[create {iteration}] PVC creation failed for "
                    f"{pvc_name}: {exc}"
                )
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
                continue

            sleep_n_sec(5)

            if self.use_client_fio:
                # ── Client SSH FIO path ──
                lvol_info = self._find_new_lvol(old_lvol_ids)
                if not lvol_info:
                    self.logger.warning(
                        f"[create {iteration}] Could not map PVC {pvc_name} "
                        f"to lvol — skipping"
                    )
                    continue
                lvol_name, lvol_id = lvol_info

                node_id = None
                try:
                    details = self.sbcli_utils.get_lvol_details(lvol_id)
                    if details:
                        node_id = details[0].get("node_id")
                except Exception:
                    pass

                client = self.fio_node[i % len(self.fio_node)]
                fs_type = random.choice(["ext4", "xfs"])

                try:
                    device, failed_cmds = self._connect_lvol_on_client(
                        lvol_name, client
                    )
                except Exception as exc:
                    self.logger.error(
                        f"[create {iteration}] NVMe connect failed for "
                        f"{pvc_name}/{lvol_name}: {exc}"
                    )
                    continue

                self.ssh_obj.format_disk(
                    node=client, device=device, fs_type=fs_type
                )
                mount_point = f"{self.mount_path}/{pvc_name}"
                self.ssh_obj.mount_path(
                    node=client, device=device, mount_path=mount_point
                )
                sleep_n_sec(5)

                log_file = f"{self.log_path}/{pvc_name}.log"
                self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])

                bs = f"{2 ** random.randint(2, 7)}K"
                self._start_client_fio(
                    pvc_name, client, mount_point, log_file, bs=bs
                )

                self.pvc_details[pvc_name] = {
                    "job_name": None,
                    "configmap_name": None,
                    "snapshots": [],
                    "node_id": node_id,
                    "lvol_name": lvol_name,
                    "lvol_id": lvol_id,
                    "device": device,
                    "mount_path": mount_point,
                    "client": client,
                    "log_file": log_file,
                    "fs_type": fs_type,
                    "storage_class": self.STORAGE_CLASS_NAME,
                }
                self.lvol_mount_details[lvol_name] = {
                    "ID": lvol_id,
                    "Mount": mount_point,
                    "Device": device,
                    "FS": fs_type,
                    "Log": log_file,
                    "Client": client,
                    "pvc_name": pvc_name,
                    "snapshots": [],
                }

                self.logger.info(
                    f"[create {iteration}] PVC {pvc_name} → lvol {lvol_name} "
                    f"connected on {client}, FIO started"
                )
            else:
                # ── K8s Job FIO path ──
                job_name = f"fio-{pvc_name}"
                cm_name = f"fiocfg-{pvc_name}"

                node_id = self._get_pvc_node_id(pvc_name)
                avoid = (
                    self._get_k8s_node_for_storage_node(node_id)
                    if node_id
                    else None
                )

                fio_config, warmup_config = self._build_fio_config(pvc_name)
                try:
                    self.k8s_utils.create_fio_job(
                        job_name, pvc_name, cm_name, fio_config,
                        image=self.FIO_IMAGE,
                        avoid_node=avoid,
                        warmup_config=warmup_config,
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[create {iteration}] FIO Job failed for "
                        f"{pvc_name}: {exc}"
                    )

                self.pvc_details[pvc_name] = {
                    "job_name": job_name,
                    "configmap_name": cm_name,
                    "snapshots": [],
                    "node_id": node_id,
                    "storage_class": self.STORAGE_CLASS_NAME,
                }

                self.logger.info(
                    f"[create {iteration}] PVC {pvc_name} node={node_id} "
                    f"FIO Job={job_name}"
                )

            if node_id:
                self.node_vs_pvc.setdefault(node_id, []).append(pvc_name)
            names.append(pvc_name)
            sleep_n_sec(3)

        return names

    # ── Delete (sequential, one-by-one) ──────────────────────────────────

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0
        core_dump_count = 0
        per_lvol_times = []

        # Snapshot existing core dumps before starting deletions
        storage_ips = self._get_storage_node_ips()
        core_snapshots = self._snapshot_core_dumps(storage_ips)

        for idx, pvc_name in enumerate(names):
            self.logger.info(
                f"[delete {iteration}] Deleting PVC {pvc_name} "
                f"({idx+1}/{len(names)})"
            )
            pvc_info = self.pvc_details.get(pvc_name, {})

            # 1. Stop FIO
            if self.use_client_fio:
                client = pvc_info.get("client")
                if client:
                    self._kill_fio_on_client(pvc_name, client)
                    sleep_n_sec(5)
                    # Unmount
                    try:
                        if pvc_info.get("mount_path"):
                            self.ssh_obj.unmount_path(
                                client, pvc_info["mount_path"]
                            )
                            self.ssh_obj.remove_dir(
                                client, dir_path=pvc_info["mount_path"]
                            )
                    except Exception as exc:
                        self.logger.warning(
                            f"[delete {iteration}] Unmount failed for "
                            f"{pvc_name}: {exc}"
                        )
                    # Disconnect NVMe
                    if pvc_info.get("lvol_name"):
                        self._disconnect_lvol_on_client(
                            pvc_info["lvol_name"], client
                        )
                    self.lvol_mount_details.pop(
                        pvc_info.get("lvol_name"), None
                    )
            else:
                # Delete FIO Job + ConfigMap
                try:
                    if pvc_info.get("job_name"):
                        self.k8s_utils.delete_job(pvc_info["job_name"])
                    if pvc_info.get("configmap_name"):
                        self.k8s_utils.delete_configmap(
                            pvc_info["configmap_name"]
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"[delete {iteration}] Job cleanup failed for "
                        f"{pvc_name}: {exc}"
                    )

            # 2. Delete PVC
            t_del_start = time.time()
            del_ok = False
            try:
                self.k8s_utils.delete_pvc(pvc_name)
                del_ok = True
                deleted += 1
                self.logger.info(
                    f"[delete {iteration}] {pvc_name} deleted successfully"
                )
                # Wait for PVC to be actually gone
                for _ in range(60):
                    out, _ = self.k8s_utils._exec_kubectl(
                        f"get pvc {pvc_name} -o name 2>/dev/null || true"
                    )
                    if not out or not out.strip() or "NotFound" in out:
                        break
                    sleep_n_sec(5)
            except Exception as exc:
                failed += 1
                self.logger.error(
                    f"[delete {iteration}] {pvc_name} FAILED to delete: {exc}"
                )
            delete_sec = round(time.time() - t_del_start, 3)
            per_lvol_times.append({
                "index": idx,
                "name": pvc_name,
                "delete_sec": delete_sec,
                "ok": del_ok,
            })
            self.logger.info(
                f"[delete {iteration}] {pvc_name} delete took {delete_sec:.1f}s"
            )

            # 3. Check core dumps 20s after delete
            sleep_n_sec(20)
            if self._check_new_core_dumps(
                storage_ips, core_snapshots, iteration, pvc_name
            ):
                core_dump_count += 1

            # 4. Clean tracking
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            self.pvc_details.pop(pvc_name, None)

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale PVCs remain for this iteration
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale_count = 0
        try:
            output, _ = self.k8s_utils._exec_kubectl("get pvc -o name")
            if output:
                all_pvcs = output.strip().split("\n")
                stale = [p for p in all_pvcs if prefix in p]
                stale_count = len(stale)
                if stale:
                    self.logger.error(
                        f"[verify {iteration}] {stale_count} PVCs still "
                        f"present: {stale}"
                    )
        except Exception as exc:
            self.logger.warning(
                f"[verify {iteration}] Could not verify stale PVCs: {exc}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": stale_count,
            "core_dumps_detected": core_dump_count,
            "per_lvol_times": per_lvol_times,
        }

    # ── Cleanup ──────────────────────────────────────────────────────────

    def _bulk_cleanup(self):
        self.logger.info("[cleanup] Running safety-net cleanup...")
        try:
            # K8s resources
            prefix = f"bulk-{self._run_id}"
            output = self.k8s_utils._exec_kubectl("get pvc -o name")
            if output:
                for line in output.strip().split("\n"):
                    if prefix in line:
                        pvc_name = line.replace("persistentvolumeclaim/", "")
                        try:
                            job_name = f"fio-{pvc_name}"
                            cm_name = f"fiocfg-{pvc_name}"
                            self.k8s_utils.delete_job(job_name)
                            self.k8s_utils.delete_configmap(cm_name)
                        except Exception:
                            pass
                        try:
                            self.k8s_utils.delete_pvc(pvc_name)
                        except Exception as exc:
                            self.logger.warning(f"[cleanup] Failed to delete PVC {pvc_name}: {exc}")
        except Exception as exc:
            self.logger.warning(f"[cleanup] K8s cleanup failed: {exc}")

        try:
            self.sbcli_utils.delete_all_clones()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_snapshots()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_lvols()
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as exc:
            self.logger.warning(f"[cleanup] sbcli cleanup failed: {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Hot-delete variants: delete lvol WHILE FIO is running, then cleanup
# ─────────────────────────────────────────────────────────────────────────────

class _BulkHotDeleteMixin:
    """Hot-delete variant: delete lvol/PVC → disconnect NVMe → stop FIO → unmount.

    Core dump methods, _write_monitoring_json, and _extract_nqn_from_connect_str
    are inherited from _BulkDeleteMixin.
    """


class BulkLvolHotDeleteDocker(_BulkHotDeleteMixin, BulkLvolDeleteDocker):
    """
    Hot-delete Docker: delete lvol WHILE FIO is running.

    Order per lvol: delete → disconnect NVMe → stop FIO → unmount.
    Checks for core dumps on all storage nodes after each deletion.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_hot_delete_docker"

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0
        core_dump_count = 0
        per_lvol_times = []

        # Snapshot existing core dumps before starting deletions
        storage_ips = self._get_storage_node_ips()
        core_snapshots = self._snapshot_core_dumps(storage_ips)

        for idx, lvol_name in enumerate(names):
            self.logger.info(
                f"[hot-delete {iteration}] Deleting {lvol_name} "
                f"({idx+1}/{len(names)}) — FIO still running"
            )
            details = self.lvol_mount_details.get(lvol_name, {})
            client = details.get(
                "Client",
                self.fio_node[0] if self.fio_node else None,
            )

            # 1. DELETE LVOL (while FIO is still running!)
            t_del_start = time.time()
            result = self.sbcli_utils.delete_lvol(
                lvol_name, max_attempt=120, skip_error=True
            )
            if result:
                deleted += 1
                self.logger.info(
                    f"[hot-delete {iteration}] {lvol_name} deleted"
                )
                # Wait for lvol to be actually deleted
                self._wait_lvol_deleted(lvol_name, timeout=300)
            else:
                failed += 1
                self.logger.error(
                    f"[hot-delete {iteration}] {lvol_name} FAILED to delete"
                )
            delete_sec = round(time.time() - t_del_start, 3)
            per_lvol_times.append({
                "index": idx,
                "name": lvol_name,
                "delete_sec": delete_sec,
                "ok": bool(result),
            })
            self.logger.info(
                f"[hot-delete {iteration}] {lvol_name} delete took "
                f"{delete_sec:.1f}s"
            )

            # 2. DISCONNECT NVMe (using NQN cached from connect strings)
            if client and details.get("Command"):
                nqns_disconnected = set()
                for connect_str in details["Command"]:
                    nqn = self._extract_nqn_from_connect_str(connect_str)
                    if nqn and nqn not in nqns_disconnected:
                        try:
                            self.ssh_obj.disconnect_nvme(
                                node=client, nqn_grep=nqn
                            )
                            nqns_disconnected.add(nqn)
                        except Exception as exc:
                            self.logger.warning(
                                f"[hot-delete {iteration}] NVMe disconnect "
                                f"failed for {lvol_name}: {exc}"
                            )

            sleep_n_sec(3)

            # 3. STOP FIO
            if client:
                fio_pids = self.ssh_obj.find_process_name(
                    client, f"{lvol_name}_fio", return_pid=True
                )
                for pid in fio_pids:
                    self.ssh_obj.kill_processes(client, pid=pid)
                for attempt in range(30):
                    fio_pids = self.ssh_obj.find_process_name(
                        client, f"{lvol_name}_fio", return_pid=True
                    )
                    if len(fio_pids) <= 2:
                        break
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(client, pid=pid)
                    sleep_n_sec(10)

            # 4. UNMOUNT
            if client and details.get("Mount"):
                try:
                    self.ssh_obj.unmount_path(client, details["Mount"])
                    self.ssh_obj.remove_dir(
                        client, dir_path=details["Mount"]
                    )
                except Exception as exc:
                    self.logger.warning(
                        f"[hot-delete {iteration}] Unmount failed for "
                        f"{lvol_name}: {exc}"
                    )

            # 5. CHECK CORE DUMPS on storage nodes (after unmount)
            if self._check_new_core_dumps(
                storage_ips, core_snapshots, iteration, lvol_name
            ):
                core_dump_count += 1

            # Clean up tracking
            self.lvol_mount_details.pop(lvol_name, None)
            for _, lvols in self.node_vs_lvol.items():
                if lvol_name in lvols:
                    lvols.remove(lvol_name)
                    break

            if client:
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/local-{lvol_name}_fio*"]
                )
                self.ssh_obj.delete_files(
                    client, [f"{self.log_path}/{lvol_name}_fio_iolog*"]
                )

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale lvols remain
        remaining = self.sbcli_utils.list_lvols()
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale = [n for n in remaining if n.startswith(prefix)]
        if stale:
            self.logger.error(
                f"[verify {iteration}] {len(stale)} lvols still present: "
                f"{stale}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": len(stale),
            "core_dumps_detected": core_dump_count,
            "per_lvol_times": per_lvol_times,
        }


class BulkLvolHotDeleteK8s(_BulkHotDeleteMixin, BulkLvolDeleteK8s):
    """
    Hot-delete K8s: delete lvol WHILE FIO is running.

    For client-SSH FIO: delete lvol via sbcli → disconnect NVMe → stop FIO → unmount → delete PVC.
    For K8s-Job FIO: delete lvol via sbcli → delete FIO Job → delete PVC.
    Checks for core dumps on all storage nodes after each deletion.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "bulk_lvol_hot_delete_k8s"

    def _bulk_create(self, iteration):
        """Override to cache NQN and lvol_id for all modes (needed for hot-delete)."""
        names = super()._bulk_create(iteration)

        # For K8s Job mode, resolve and cache lvol_id + NQN per PVC
        if not self.use_client_fio:
            for pvc_name in names:
                pvc_info = self.pvc_details.get(pvc_name, {})
                if pvc_info.get("lvol_id"):
                    continue  # already set (client mode)
                try:
                    lvol_id = self.k8s_utils.get_pvc_volume_handle(pvc_name)
                    if lvol_id:
                        pvc_info["lvol_id"] = lvol_id
                        details = self.sbcli_utils.get_lvol_details(lvol_id)
                        if details:
                            pvc_info["nqn"] = details[0].get("nqn", "")
                            if not pvc_info.get("lvol_name"):
                                pvc_info["lvol_name"] = details[0].get(
                                    "lvol_name", ""
                                )
                except Exception as exc:
                    self.logger.warning(
                        f"[create {iteration}] Could not cache lvol info "
                        f"for PVC {pvc_name}: {exc}"
                    )
        else:
            # Client mode: cache NQN from lvol details
            for pvc_name in names:
                pvc_info = self.pvc_details.get(pvc_name, {})
                lvol_id = pvc_info.get("lvol_id")
                if lvol_id and not pvc_info.get("nqn"):
                    try:
                        details = self.sbcli_utils.get_lvol_details(lvol_id)
                        if details:
                            pvc_info["nqn"] = details[0].get("nqn", "")
                    except Exception:
                        pass

        return names

    def _bulk_delete_sequential(self, iteration, names):
        deleted = 0
        failed = 0
        core_dump_count = 0
        per_lvol_times = []

        # Snapshot existing core dumps before starting deletions
        storage_ips = self._get_storage_node_ips()
        core_snapshots = self._snapshot_core_dumps(storage_ips)

        for idx, pvc_name in enumerate(names):
            self.logger.info(
                f"[hot-delete {iteration}] Deleting PVC {pvc_name} "
                f"({idx+1}/{len(names)}) — FIO still running"
            )
            pvc_info = self.pvc_details.get(pvc_name, {})
            lvol_id = pvc_info.get("lvol_id", "")
            lvol_name = pvc_info.get("lvol_name", "")
            cached_nqn = pvc_info.get("nqn", "")

            # 1. DELETE LVOL via sbcli (bypasses K8s PV protection,
            #    causes NVMe device to disappear while FIO is running)
            t_del_start = time.time()
            delete_ok = False
            if lvol_id:
                try:
                    self.sbcli_utils.delete_lvol(
                        lvol_name or lvol_id,
                        max_attempt=120,
                        skip_error=True,
                    )
                    delete_ok = True
                    self.logger.info(
                        f"[hot-delete {iteration}] Lvol {lvol_id} deleted "
                        f"(PVC {pvc_name})"
                    )
                    # Wait for lvol to be actually deleted
                    if lvol_name:
                        self._wait_lvol_deleted(lvol_name, timeout=300)
                except Exception as exc:
                    self.logger.error(
                        f"[hot-delete {iteration}] Lvol delete failed for "
                        f"{pvc_name}: {exc}"
                    )
            delete_sec = round(time.time() - t_del_start, 3)
            per_lvol_times.append({
                "index": idx,
                "name": pvc_name,
                "delete_sec": delete_sec,
                "ok": delete_ok,
            })
            self.logger.info(
                f"[hot-delete {iteration}] {pvc_name} delete took "
                f"{delete_sec:.1f}s"
            )

            # 2. DISCONNECT NVMe + STOP FIO
            if self.use_client_fio:
                client = pvc_info.get("client")
                if client:
                    # 2a. DISCONNECT NVMe first (using cached NQN)
                    if cached_nqn:
                        try:
                            self.ssh_obj.disconnect_nvme(
                                node=client, nqn_grep=cached_nqn
                            )
                        except Exception as exc:
                            self.logger.warning(
                                f"[hot-delete {iteration}] NVMe disconnect "
                                f"failed for {pvc_name}: {exc}"
                            )

                    sleep_n_sec(3)

                    # 2b. STOP FIO
                    self._kill_fio_on_client(pvc_name, client)
                    sleep_n_sec(5)

                    # 3. UNMOUNT
                    if pvc_info.get("mount_path"):
                        try:
                            self.ssh_obj.unmount_path(
                                client, pvc_info["mount_path"]
                            )
                            self.ssh_obj.remove_dir(
                                client, dir_path=pvc_info["mount_path"]
                            )
                        except Exception as exc:
                            self.logger.warning(
                                f"[hot-delete {iteration}] Unmount failed "
                                f"for {pvc_name}: {exc}"
                            )

                self.lvol_mount_details.pop(lvol_name, None)
            else:
                # K8s Job mode: delete the FIO Job + ConfigMap
                try:
                    if pvc_info.get("job_name"):
                        self.k8s_utils.delete_job(pvc_info["job_name"])
                    if pvc_info.get("configmap_name"):
                        self.k8s_utils.delete_configmap(
                            pvc_info["configmap_name"]
                        )
                except Exception as exc:
                    self.logger.warning(
                        f"[hot-delete {iteration}] Job cleanup failed for "
                        f"{pvc_name}: {exc}"
                    )

            # 5. DELETE PVC (orphaned — lvol already gone)
            try:
                self.k8s_utils.delete_pvc(pvc_name)
                deleted += 1
                self.logger.info(
                    f"[hot-delete {iteration}] PVC {pvc_name} deleted"
                )
            except Exception as exc:
                if delete_ok:
                    deleted += 1
                    self.logger.warning(
                        f"[hot-delete {iteration}] PVC {pvc_name} delete "
                        f"failed (lvol already removed): {exc}"
                    )
                else:
                    failed += 1
                    self.logger.error(
                        f"[hot-delete {iteration}] {pvc_name} FAILED: {exc}"
                    )

            # 6. CHECK CORE DUMPS on storage nodes (after all cleanup)
            if self._check_new_core_dumps(
                storage_ips, core_snapshots, iteration, pvc_name
            ):
                core_dump_count += 1

            # Clean tracking
            node_id = pvc_info.get("node_id")
            if node_id and node_id in self.node_vs_pvc:
                if pvc_name in self.node_vs_pvc[node_id]:
                    self.node_vs_pvc[node_id].remove(pvc_name)
            self.pvc_details.pop(pvc_name, None)

            sleep_n_sec(self.DELETE_INTERVAL)

        # Verify no stale PVCs remain
        prefix = f"bulk-{self._run_id}-i{iteration}-"
        stale_count = 0
        try:
            output, _ = self.k8s_utils._exec_kubectl("get pvc -o name")
            if output:
                all_pvcs = output.strip().split("\n")
                stale = [p for p in all_pvcs if prefix in p]
                stale_count = len(stale)
                if stale:
                    self.logger.error(
                        f"[verify {iteration}] {stale_count} PVCs still "
                        f"present: {stale}"
                    )
        except Exception as exc:
            self.logger.warning(
                f"[verify {iteration}] Could not verify stale PVCs: {exc}"
            )

        return {
            "iteration": iteration,
            "created": len(names),
            "deleted": deleted,
            "failed": failed,
            "stale": stale_count,
            "core_dumps_detected": core_dump_count,
            "per_lvol_times": per_lvol_times,
        }
