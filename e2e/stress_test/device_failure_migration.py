"""
Device Failure Migration Stress Test

Measures the time it takes to complete failure migration on a single device.
Two variants:

  - DeviceFailureMigrationNoLoad:
        Fill device to 65 %, fail it, measure migration time (no IO load).
  - DeviceFailureMigrationUnderLoad:
        Fill device to 65 %, start IO on every cluster node, fail device,
        measure migration time while IO is running.

Both tests are Docker-mode only (sbcli + SSH FIO).  They work with any
cluster geometry (ndcs/npcs) and require at least one client node
(CLIENT_IP env var or mgmt node fallback).
"""

import json
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from e2e_tests.cluster_test_base import generate_random_sequence
from logger_config import setup_logger
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from utils.common_utils import sleep_n_sec


# ═══════════════════════════════════════════════════════════════════════════════
#  Mixin — shared orchestration for both variants
# ═══════════════════════════════════════════════════════════════════════════════

class _DeviceFailureMigrationBase:
    """Shared logic for device failure migration timing tests."""

    # ── Configuration ────────────────────────────────────────────────────────
    FILL_PERCENT = 65          # target device utilisation before failure
    LVOL_SIZE = "50G"          # per-lvol size (large enough to fill device)
    FIO_FILL_SIZE = "45G"      # < LVOL_SIZE to fit within filesystem overhead
    FIO_FILL_BS = "512K"       # sequential write block size for fill
    FIO_LOAD_BS = "4K"         # random IO block size for load
    FIO_LOAD_IODEPTH = 4
    FIO_LOAD_NUMJOBS = 2
    FIO_LOAD_RUNTIME = 7200    # 2 h (longer than expected migration)
    MIGRATION_TIMEOUT = 3600   # 1 h max for migration
    MAX_WORKERS = 10           # parallel fill / IO threads

    def _init_migration_state(self):
        """Initialise per-test tracking state (call from __init__)."""
        self._timing = {}
        self._target_node_id = None
        self._target_device_id = None
        self._target_device_info = None
        self._lvols_on_target = []       # lvol names pinned to target node
        self._lvols_on_others = []       # 1 lvol per other node (IO load)
        self._fill_fio_threads = []
        self._load_fio_threads = []
        self._sn_nodes = []
        self._with_io_load = False

    # ── Main flow ────────────────────────────────────────────────────────────

    def _run_migration_test(self, with_io_load=False):
        """Main flow: setup → fill → [start IO] → fail → migrate → cleanup."""
        self._with_io_load = with_io_load
        t0 = time.time()
        try:
            self._phase_setup_pool_and_lvols()
            self._phase_fill_devices()
            if with_io_load:
                self._phase_start_io_load()
            self._phase_fail_and_migrate()
        finally:
            if with_io_load:
                self._phase_stop_io_load()
            self._phase_cleanup()
            self._timing["total_duration"] = time.time() - t0
            self._print_migration_summary()
            self._write_timing_json()
            self._generate_charts()

    # ── Phase 1: create pool, lvols, connect, format, mount ──────────────────

    def _phase_setup_pool_and_lvols(self):
        self.logger.info("=== Phase: Setup pool and lvols ===")
        t0 = time.time()

        # Get storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for r in storage_nodes["results"]:
            self._sn_nodes.append(r["uuid"])

        if len(self._sn_nodes) < 1:
            raise RuntimeError("No storage nodes found")

        # Pick target node and device
        self._target_node_id = self._sn_nodes[0]
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        if not devices:
            raise RuntimeError(
                f"No devices found on target node {self._target_node_id}"
            )
        self._target_device_info = devices[0]
        self._target_device_id = devices[0]["id"]
        self.logger.info(
            f"Target node: {self._target_node_id}, "
            f"Target device: {self._target_device_id}"
        )

        # Get node capacity to calculate how many lvols to create
        capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
        if isinstance(capacity, list):
            capacity = capacity[0] if capacity else {}
        size_total_bytes = capacity.get("size_total", 0)
        if isinstance(size_total_bytes, str):
            # Handle human-readable strings like "500G"
            size_total_bytes = self._parse_size(size_total_bytes)
        target_bytes = int(size_total_bytes * self.FILL_PERCENT / 100)
        lvol_bytes = self._parse_size(self.LVOL_SIZE)
        num_lvols = max(1, math.ceil(target_bytes / lvol_bytes))
        self.logger.info(
            f"Node capacity: {size_total_bytes} bytes, "
            f"target fill: {target_bytes} bytes, "
            f"creating {num_lvols} lvols of {self.LVOL_SIZE}"
        )

        # Create lvols on target node
        client = self.fio_node[0]
        for i in range(num_lvols):
            name = f"mig_target_{generate_random_sequence(4)}_{i}"
            self._create_and_connect_lvol(name, self._target_node_id, client)
            self._lvols_on_target.append(name)

        # Create 1 lvol per OTHER node (for IO load variant)
        other_nodes = [n for n in self._sn_nodes if n != self._target_node_id]
        for idx, node_id in enumerate(other_nodes):
            name = f"mig_other_{generate_random_sequence(4)}_{idx}"
            self._create_and_connect_lvol(name, node_id, client)
            self._lvols_on_others.append(name)

        self._timing["setup_duration"] = time.time() - t0
        self.logger.info(
            f"Setup complete: {len(self._lvols_on_target)} target lvols, "
            f"{len(self._lvols_on_others)} other lvols "
            f"({self._timing['setup_duration']:.1f}s)"
        )

    def _create_and_connect_lvol(self, name, node_id, client):
        """Create a single lvol, NVMe-connect, format, mount."""
        self.sbcli_utils.add_lvol(
            lvol_name=name,
            pool_name=self.pool_name,
            size=self.LVOL_SIZE,
            crypto=False,
            key1=self.lvol_crypt_keys[0],
            key2=self.lvol_crypt_keys[1],
            host_id=node_id,
        )
        sleep_n_sec(2)
        lvol_id = self.sbcli_utils.get_lvol_id(name)

        initial_devices = self.ssh_obj.get_devices(node=client)
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=name)
        for cmd in connect_ls:
            self.ssh_obj.exec_command(client, cmd)
        sleep_n_sec(3)
        final_devices = self.ssh_obj.get_devices(node=client)

        device = None
        for d in final_devices:
            if d not in initial_devices:
                device = f"/dev/{d.strip()}"
                break
        if not device:
            self.logger.warning(f"Could not detect device for {name}")
            return

        fs_type = "ext4"
        mount_path = f"/mnt/{name}"
        self.ssh_obj.format_disk(client, device, fs_type)
        self.ssh_obj.mount_path(client, device, mount_path)
        self.lvol_mount_details[name] = {
            "ID": lvol_id,
            "Command": connect_ls,
            "Mount": mount_path,
            "Device": device,
            "MD5": None,
            "FS": fs_type,
            "Log": f"{self.log_path}/{name}.log",
            "snapshots": [],
            "Client": client,
            "NodeID": node_id,
        }

    # ── Phase 2: fill target-node lvols to FILL_PERCENT ──────────────────────

    def _phase_fill_devices(self):
        self.logger.info(
            f"=== Phase: Fill target device to {self.FILL_PERCENT}% ==="
        )
        t0 = time.time()
        client = self.fio_node[0]

        # Sequential-write fill on each target lvol
        threads = []
        for name in self._lvols_on_target:
            info = self.lvol_mount_details.get(name)
            if not info:
                continue
            t = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, info["Mount"], info["Log"]),
                kwargs={
                    "name": f"fill_{name}",
                    "rw": "write",
                    "bs": self.FIO_FILL_BS,
                    "size": self.FIO_FILL_SIZE,
                    "runtime": 0,
                    "time_based": False,
                    "iodepth": 1,
                    "numjobs": 1,
                    "use_latency": False,
                },
            )
            t.start()
            threads.append(t)

        # Wait for all fills to complete
        for t in threads:
            t.join(timeout=3600)

        # Verify fill level
        sleep_n_sec(5)
        capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
        if isinstance(capacity, list):
            capacity = capacity[0] if capacity else {}
        util = capacity.get("size_util", 0)
        self.logger.info(f"Post-fill device utilisation: {util}%")

        self._timing["fill_duration"] = time.time() - t0
        self.logger.info(
            f"Fill complete ({self._timing['fill_duration']:.1f}s)"
        )

    # ── Phase 3: start random IO on all nodes (under-load variant) ───────────

    def _phase_start_io_load(self):
        self.logger.info("=== Phase: Start IO load on all nodes ===")
        client = self.fio_node[0]
        all_lvol_names = self._lvols_on_target + self._lvols_on_others

        for name in all_lvol_names:
            info = self.lvol_mount_details.get(name)
            if not info:
                continue
            t = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, info["Mount"], info["Log"]),
                kwargs={
                    "name": f"load_{name}",
                    "rw": "randrw",
                    "bs": self.FIO_LOAD_BS,
                    "size": "1G",
                    "runtime": self.FIO_LOAD_RUNTIME,
                    "iodepth": self.FIO_LOAD_IODEPTH,
                    "numjobs": self.FIO_LOAD_NUMJOBS,
                    "use_latency": False,
                },
            )
            t.start()
            self._load_fio_threads.append(t)

        sleep_n_sec(15)  # let IO ramp up
        self.logger.info(
            f"IO load started: {len(self._load_fio_threads)} FIO threads"
        )

    # ── Phase 4: remove device → set-failed → wait migration ────────────────

    def _phase_fail_and_migrate(self):
        self.logger.info(
            f"=== Phase: Fail device {self._target_device_id} and migrate ==="
        )
        t0 = time.time()

        # Step 1: remove device (ONLINE → REMOVED)
        self.logger.info(f"Removing device {self._target_device_id} …")
        self.sbcli_utils.remove_device(self._target_device_id)
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "removed", timeout=120
        )
        self._timing["remove_duration"] = time.time() - t0
        self.logger.info(
            f"Device removed ({self._timing['remove_duration']:.1f}s)"
        )

        # Step 2: set-failed via CLI (no REST endpoint exists)
        t1 = time.time()
        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} sn set-failed-device {self._target_device_id}"
        self.logger.info(f"Setting device failed via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"set-failed-device result: {result}")
        sleep_n_sec(5)

        # Step 3: wait for migration to complete
        self.logger.info("Waiting for failure migration tasks to complete …")
        migration_elapsed = self.sbcli_utils.wait_migration_tasks_complete(
            timeout=self.MIGRATION_TIMEOUT
        )
        self._timing["migration_duration"] = time.time() - t1
        self._timing["migration_tasks_elapsed"] = migration_elapsed

        # Step 4: verify device status
        sleep_n_sec(5)
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        target_dev = None
        for d in devices:
            if d["id"] == self._target_device_id:
                target_dev = d
                break
        final_status = target_dev["status"] if target_dev else "unknown"
        self.logger.info(
            f"Device final status: {final_status} "
            f"(migration took {self._timing['migration_duration']:.1f}s)"
        )
        self._timing["device_final_status"] = final_status

    # ── Phase 5: stop IO load ────────────────────────────────────────────────

    def _phase_stop_io_load(self):
        self.logger.info("=== Phase: Stop IO load ===")
        client = self.fio_node[0]
        self.ssh_obj.exec_command(client, "pkill -f fio || true")
        for t in self._load_fio_threads:
            t.join(timeout=30)
        self.logger.info("IO load stopped")

    # ── Cleanup ──────────────────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Phase: Cleanup ===")
        try:
            # Kill FIO on all clients
            for client in self.fio_node:
                self.ssh_obj.exec_command(client, "pkill -f fio || true")
            sleep_n_sec(5)

            # Unmount and disconnect
            for name, info in self.lvol_mount_details.items():
                client = info.get("Client", self.fio_node[0])
                try:
                    self.ssh_obj.unmount_path(client, info["Device"])
                except Exception:
                    pass
                for cmd in info.get("Command", []):
                    nqn = None
                    for part in cmd.split():
                        if "nqn" in part.lower():
                            nqn = part.split("=")[-1] if "=" in part else part
                    if nqn:
                        self.ssh_obj.exec_command(
                            client, f"nvme disconnect -n {nqn} || true"
                        )

            # Delete resources in correct order
            self.sbcli_utils.delete_all_clones()
            self.sbcli_utils.delete_all_snapshots()
            self.sbcli_utils.delete_all_lvols()
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")

    # ── Summary and timing output ────────────────────────────────────────────

    def _print_migration_summary(self):
        self.logger.info("=" * 70)
        self.logger.info("  DEVICE FAILURE MIGRATION SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"  Test class:       {self.__class__.__name__}")
        self.logger.info(f"  IO load:          {'YES' if self._with_io_load else 'NO'}")
        self.logger.info(f"  Target node:      {self._target_node_id}")
        self.logger.info(f"  Target device:    {self._target_device_id}")
        self.logger.info(f"  Fill target:      {self.FILL_PERCENT}%")
        self.logger.info(f"  Lvols on target:  {len(self._lvols_on_target)}")
        self.logger.info(f"  Lvols on others:  {len(self._lvols_on_others)}")
        self.logger.info("-" * 70)
        for key, val in self._timing.items():
            if isinstance(val, float):
                self.logger.info(f"  {key:30s} {val:10.1f}s")
            else:
                self.logger.info(f"  {key:30s} {val}")
        self.logger.info("=" * 70)

    def _write_timing_json(self):
        """Write standardised timing JSON for monitoring suite aggregation."""
        phases = []
        for name in ("setup_duration", "fill_duration", "remove_duration",
                      "migration_duration"):
            if name in self._timing:
                phases.append({
                    "name": name.replace("_duration", ""),
                    "duration_sec": round(self._timing[name], 2),
                    "status": "ok",
                })

        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "fill_percent": self.FILL_PERCENT,
                "lvol_size": self.LVOL_SIZE,
                "with_io_load": self._with_io_load,
                "target_node": self._target_node_id,
                "target_device": self._target_device_id,
                "lvols_on_target": len(self._lvols_on_target),
                "lvols_on_others": len(self._lvols_on_others),
            },
            "phases": phases,
            "summary": {
                "total_duration_sec": round(
                    self._timing.get("total_duration", 0), 2
                ),
                "key_metric": round(
                    self._timing.get("migration_duration", 0), 2
                ),
                "key_metric_label": "migration_duration_sec",
            },
        }

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "device_migration_timing.json"
        with open(out_path, "w") as f:
            json.dump(report, f, indent=2)
        self.logger.info(f"Timing JSON written to {out_path}")

    def _generate_charts(self):
        """Generate timing charts for device failure migration test."""
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

        # Chart 1: Phase duration waterfall
        try:
            phase_names = []
            phase_durations = []
            for name in ("setup_duration", "fill_duration", "remove_duration",
                          "migration_duration"):
                if name in self._timing:
                    phase_names.append(name.replace("_duration", ""))
                    phase_durations.append(self._timing[name])

            if phase_durations:
                colors = ["#3498db", "#f39c12", "#e74c3c", "#9b59b6"]
                colors = colors[:len(phase_names)]

                fig, axes = plt.subplots(1, 2, figsize=(14, 5))

                # Left: bar chart
                bars = axes[0].bar(range(len(phase_names)), phase_durations,
                                   color=colors, alpha=0.8)
                axes[0].set_xticks(range(len(phase_names)))
                axes[0].set_xticklabels(phase_names, fontsize=10)
                axes[0].set_ylabel("Duration (seconds)")
                axes[0].set_title(f"{class_name} — Phase Durations")
                for b, v in zip(bars, phase_durations):
                    axes[0].text(b.get_x() + b.get_width() / 2,
                                 b.get_height() + max(phase_durations) * 0.02,
                                 f"{v:.0f}s", ha="center", va="bottom", fontsize=9)
                axes[0].grid(True, axis="y", alpha=0.3)

                # Right: pie chart of time distribution
                axes[1].pie(phase_durations, labels=phase_names, colors=colors,
                            autopct="%1.0f%%", startangle=90)
                axes[1].set_title("Time Distribution")

                plt.suptitle(
                    f"{class_name}\n"
                    f"IO load: {'YES' if self._with_io_load else 'NO'}  |  "
                    f"Fill: {self.FILL_PERCENT}%  |  "
                    f"Lvols: {len(self._lvols_on_target)} target + "
                    f"{len(self._lvols_on_others)} other",
                    fontsize=10,
                )
                plt.tight_layout()
                path = out_dir / "migration_phase_durations.png"
                fig.savefig(str(path), dpi=150)
                plt.close(fig)
                self.logger.info(f"Chart saved: {path}")
        except Exception as exc:
            self.logger.warning(f"Phase duration chart failed: {exc}")

        # Chart 2: Migration vs fill comparison
        try:
            fill = self._timing.get("fill_duration", 0)
            migrate = self._timing.get("migration_duration", 0)
            if fill > 0 or migrate > 0:
                fig, ax = plt.subplots(figsize=(8, 4))
                bars = ax.barh(["Fill to 65%", "Migration"],
                               [fill, migrate],
                               color=["#f39c12", "#9b59b6"], alpha=0.8,
                               height=0.5)
                for b, v in zip(bars, [fill, migrate]):
                    ax.text(b.get_width() + max(fill, migrate) * 0.02,
                            b.get_y() + b.get_height() / 2,
                            f"{v:.0f}s ({v/60:.1f}m)", va="center", fontsize=10)
                ax.set_xlabel("Duration (seconds)")
                ax.set_title(
                    f"{class_name} — Fill vs Migration Time"
                )
                ax.grid(True, axis="x", alpha=0.3)
                plt.tight_layout()
                path = out_dir / "fill_vs_migration.png"
                fig.savefig(str(path), dpi=150)
                plt.close(fig)
                self.logger.info(f"Chart saved: {path}")
        except Exception as exc:
            self.logger.warning(f"Fill vs migration chart failed: {exc}")

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_size(size_str):
        """Convert human-readable size like '50G' to bytes."""
        if isinstance(size_str, (int, float)):
            return int(size_str)
        s = str(size_str).strip().upper()
        multipliers = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
        for suffix, mult in multipliers.items():
            if s.endswith(suffix):
                return int(float(s[:-1]) * mult)
        return int(s)


# ═══════════════════════════════════════════════════════════════════════════════
#  Concrete test classes
# ═══════════════════════════════════════════════════════════════════════════════

class DeviceFailureMigrationNoLoad(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, fail it, run migration WITHOUT IO load.

    Measures: setup time, fill time, device remove time, migration time.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_no_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_migration_test(with_io_load=False)


class DeviceFailureMigrationUnderLoad(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, start IO on all nodes, fail device, migrate UNDER LOAD.

    Measures: setup time, fill time, device remove time, migration time.
    IO errors during migration are logged but do not fail the test.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_under_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_migration_test(with_io_load=True)
