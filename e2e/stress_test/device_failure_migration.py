"""
Device Failure Migration Stress Test

Measures the time it takes to complete failure migration on a single device.

Variants:

  Docker (sbcli + SSH FIO):
  - DeviceFailureMigrationNoLoadDocker          — API removal, no IO load
  - DeviceFailureMigrationUnderLoadDocker       — API removal, IO load running
  - DeviceFailureMigrationPCIeNoLoadDocker      — PCIe sysfs removal, no IO load
  - DeviceFailureMigrationPCIeUnderLoadDocker   — PCIe sysfs removal, IO load running

  K8s-native (PVC + FIO K8s Jobs):
  - DeviceFailureMigrationNoLoadK8s       — API removal, no IO load
  - DeviceFailureMigrationUnderLoadK8s    — API removal, IO load running
  - DeviceFailureMigrationPCIeNoLoadK8s   — PCIe sysfs removal, no IO load
  - DeviceFailureMigrationPCIeUnderLoadK8s— PCIe sysfs removal, IO load running

  PCIe restart (no migration — restart device after PCI rescan):
  - DevicePCIeRestartNoLoadDocker             — PCIe unplug + restart, no IO load
  - DevicePCIeRestartUnderLoadDocker          — PCIe unplug + restart, IO load running
  - DevicePCIeRestartNoLoadK8s               — PCIe unplug + restart, no IO load (K8s)
  - DevicePCIeRestartUnderLoadK8s            — PCIe unplug + restart, IO load running (K8s)

  Device add after bootstrap (PCIe disabled before bootstrap, then added):
  - DeviceAddAfterBootstrapDocker            — PCI rescan + sn restart + add-device, no IO load
  - DeviceAddAfterBootstrapUnderLoadDocker   — PCI rescan + sn restart + add-device, 128K randwrite IO load

Failure modes:
  - "api"  : Logical removal via REST API + set-failed-device CLI
  - "pcie" : Physical removal via /sys/bus/pci/devices/<addr>/remove

Restart mode (PCIe only):
  - PCIe unplug → PCI rescan → restart-device CLI (no set-failed, no migration)

All tests work with any cluster geometry (ndcs/npcs) and require at least
one storage node with a device.

Invocation:
  # Docker
  python3 stress.py --testname DeviceFailureMigrationNoLoadDocker --ndcs 2 --npcs 2
  python3 stress.py --testname DeviceFailureMigrationPCIeNoLoad --ndcs 2 --npcs 2

  # K8s
  python3 stress.py --testname DeviceFailureMigrationNoLoadK8s --ndcs 2 --npcs 2 --run_k8s True
  python3 stress.py --testname DeviceFailureMigrationPCIeUnderLoadK8s --ndcs 2 --npcs 2 --run_k8s True
"""

import json
import math
import os
import random
import string
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from e2e_tests.cluster_test_base import generate_random_sequence
from logger_config import setup_logger
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from utils.common_utils import sleep_n_sec


def _rand_seq(length: int = 8) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


def _fmt_bytes(b):
    """Format bytes as human-readable GiB string."""
    if b is None or b == "—":
        return "—"
    try:
        val = float(b)
    except (TypeError, ValueError):
        return str(b)
    return f"{val / (1024**3):.1f} GiB"


# ═══════════════════════════════════════════════════════════════════════════════
#  Mixin — shared orchestration for all variants
# ═══════════════════════════════════════════════════════════════════════════════

class _DeviceFailureMigrationBase:
    """Shared logic for device failure migration timing tests."""

    # ── Configuration ────────────────────────────────────────────────────────
    FILL_PERCENT = 65          # target device utilisation before failure
    LVOL_SIZE = "50G"          # per-lvol size (large enough to fill device)
    FIO_FILL_SIZE = "30G"      # < LVOL_SIZE to fit within filesystem overhead
    FIO_FILL_BS = "512K"       # sequential write block size for fill
    FIO_LOAD_BS = "128K"       # random write block size for bandwidth measurement
    FIO_LOAD_IODEPTH = 1       # must be 1 when verify=md5 to avoid false errors
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
        self._failure_mode = "api"
        self._pre_migration_checksums = {}  # {lvol_name: {filepath: md5}}
        self._pre_existing_failed_devices = set()  # device IDs already failed before test
        self._cached_capacity = {}  # {node_id: {status, cap, devices}}

    # ── Main flow ────────────────────────────────────────────────────────────

    def _run_migration_test(self, with_io_load=False, failure_mode="api"):
        """Main flow for device failure migration timing tests.

        NoLoad:  setup → fail device → migrate → validate → recover → cleanup
                 (no fill, no IO — measures raw migration time on empty device)
        UnderLoad: setup → fill → start FIO (128K randwrite, verify=md5) → fail device →
                   migrate → collect bandwidth → wait FIO → recover → cleanup
        """
        self._with_io_load = with_io_load
        self._failure_mode = failure_mode
        self._test_passed = False
        t0 = time.time()
        try:
            self._phase_setup_pool_and_lvols()
            if with_io_load:
                self._phase_fill_devices()
                self._log_device_fill_level()
                self._phase_start_io_load()
            if failure_mode == "pcie":
                self._phase_fail_and_migrate_pcie()
            else:
                self._phase_fail_and_migrate()
            self._phase_validate()
            if with_io_load:
                self._phase_restart_non_target_node()
                # Wait for FIO to finish naturally — do NOT kill it
                self._phase_wait_fio_completion()
                self._phase_validate_fio()
            self._test_passed = True
        finally:
            # Collect IO stats BEFORE stopping FIO so the cluster API
            # still has active IO counters to report.
            if with_io_load:
                try:
                    self._phase_collect_cluster_io_stats()
                except Exception as e:
                    self.logger.warning(f"IO stats collection failed: {e}")
            if with_io_load:
                try:
                    self._phase_stop_io_load()
                except Exception as e:
                    self.logger.warning(f"Stop IO load failed: {e}")
            if not self._test_passed:
                try:
                    self._phase_recover_device()
                except Exception as e:
                    self.logger.warning(f"Device recovery failed: {e}")
            try:
                self._phase_post_migration_health_check()
            except Exception as e:
                self.logger.warning(f"Post-migration health check failed: {e}")
            try:
                self.collect_management_details(suffix="_pre_cleanup")
            except Exception as e:
                self.logger.warning(f"collect_management_details failed: {e}")
            try:
                self._capture_cluster_capacity()
            except Exception as e:
                self.logger.warning(f"Capacity capture failed: {e}")
            try:
                self._phase_cleanup()
            except Exception as e:
                self.logger.warning(f"Cleanup failed: {e}")
            self._timing["total_duration"] = time.time() - t0
            self._print_migration_summary()
            self._write_timing_json()
            self._write_test_summary_md()
            self._generate_charts()

        self.logger.info("TEST CASE PASSED !!!")

    def _run_restart_test(self, with_io_load=False):
        """PCIe unplug → rescan → restart-device → verify cluster healthy.

        Unlike _run_migration_test, this does NOT set-failed the device or
        trigger failure migration.  Instead it rescans the PCI bus to make the
        physical device visible again, then calls ``sbctl sn restart-device``
        so the control plane re-attaches it.

        NoLoad:  setup → PCIe unplug → rescan → restart-device → validate → cleanup
                 (no fill, no IO — measures raw restart time on empty device)
        UnderLoad: setup → fill → start FIO (128K randwrite) → PCIe unplug →
                   rescan → restart-device → validate → collect bandwidth → cleanup
        """
        self._with_io_load = with_io_load
        self._failure_mode = "pcie"
        self._test_passed = False
        t0 = time.time()
        try:
            self._phase_setup_pool_and_lvols()
            if with_io_load:
                self._phase_fill_devices()
                self._log_device_fill_level()
                self._phase_start_io_load()
            self._phase_pcie_remove_and_restart()
            self._phase_validate_restart()
            if with_io_load:
                self._phase_restart_non_target_node()
                self._phase_wait_fio_completion()
                self._phase_validate_fio()
            self._test_passed = True
        finally:
            # Collect IO stats BEFORE stopping FIO so the cluster API
            # still has active IO counters to report.
            if with_io_load:
                try:
                    self._phase_collect_cluster_io_stats()
                except Exception as e:
                    self.logger.warning(f"IO stats collection failed: {e}")
            if with_io_load:
                try:
                    self._phase_stop_io_load()
                except Exception as e:
                    self.logger.warning(f"Stop IO load failed: {e}")
            # No device recovery needed — restart-device already brought it back
            try:
                self._phase_post_migration_health_check()
            except Exception as e:
                self.logger.warning(f"Post-migration health check failed: {e}")
            try:
                self.collect_management_details(suffix="_pre_cleanup")
            except Exception as e:
                self.logger.warning(f"collect_management_details failed: {e}")
            try:
                self._capture_cluster_capacity()
            except Exception as e:
                self.logger.warning(f"Capacity capture failed: {e}")
            try:
                self._phase_cleanup()
            except Exception as e:
                self.logger.warning(f"Cleanup failed: {e}")
            self._timing["total_duration"] = time.time() - t0
            self._print_migration_summary()
            self._write_timing_json()
            self._write_test_summary_md()
            self._generate_charts()

        self.logger.info("TEST CASE PASSED !!!")

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
        # Record devices already in a non-online state from previous runs —
        # these will be ignored throughout the test (validation, recovery, etc.)
        for d in devices:
            if d.get("status") != "online":
                self._pre_existing_failed_devices.add(d["id"])
        if self._pre_existing_failed_devices:
            self.logger.info(
                f"Pre-existing non-online devices (will be ignored): "
                f"{self._pre_existing_failed_devices}"
            )

        # Filter for online devices only
        online_devices = [d for d in devices if d.get("status") == "online"]
        if not online_devices:
            raise RuntimeError(
                f"No online devices found on target node {self._target_node_id}. "
                f"Device statuses: {[d.get('status') for d in devices]}"
            )
        self._target_device_info = online_devices[0]
        self._target_device_id = online_devices[0]["id"]
        self.logger.info(
            f"Target node: {self._target_node_id}, "
            f"Target device: {self._target_device_id} "
            f"(selected from {len(online_devices)} online / {len(devices)} total devices)"
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

        # Fail fast if we'd exceed the node's max_lvol limit
        node_details = self.sbcli_utils.get_storage_node_details(
            self._target_node_id
        )
        max_lvol = node_details[0].get("max_lvol", 0) if node_details else 0
        other_nodes = [n for n in self._sn_nodes if n != self._target_node_id]
        total_needed = num_lvols + len(other_nodes)
        if max_lvol > 0 and total_needed > max_lvol:
            raise RuntimeError(
                f"Need {total_needed} lvols ({num_lvols} target + "
                f"{len(other_nodes)} other-node) but node max_lvol={max_lvol}. "
                f"Recreate cluster with a higher max_lvol limit."
            )

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

        # Wait for FIO launch threads to return (they return after verifying
        # FIO is running in tmux, but FIO itself is still writing)
        for t in threads:
            t.join(timeout=60)

        # Wait for actual FIO processes to finish on the remote node
        self.logger.info("Waiting for FIO fill processes to complete on remote node ...")
        self.common_utils.manage_fio_threads(
            node=client, threads=[], timeout=7200
        )

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

    # ── Phase 2b: compute pre-migration checksums (no-load variant) ─────────

    def _phase_compute_checksums(self):
        """Compute MD5 checksums of all files on target lvols before migration."""
        self.logger.info("=== Phase: Compute pre-migration checksums ===")
        client = self.fio_node[0]
        self._pre_migration_checksums = {}

        for name in self._lvols_on_target:
            info = self.lvol_mount_details.get(name)
            if not info:
                continue
            mount = info["Mount"]
            try:
                files = self.ssh_obj.find_files(client, mount)
                if files:
                    checksums = self.ssh_obj.generate_checksums(client, files)
                    self._pre_migration_checksums[name] = checksums
                    self.logger.info(
                        f"Captured {len(checksums)} file checksums for {name}"
                    )
                else:
                    self.logger.warning(f"No files found on {mount} for checksum")
            except Exception as exc:
                self.logger.warning(f"Checksum capture failed for {name}: {exc}")

        self.logger.info(
            f"Pre-migration checksums captured for "
            f"{len(self._pre_migration_checksums)} lvols"
        )

    def _phase_verify_checksums(self):
        """Verify MD5 checksums of target lvols match pre-migration values."""
        self.logger.info("=== Verifying post-migration data integrity ===")
        client = self.fio_node[0]
        mismatches = 0

        for name, expected_checksums in self._pre_migration_checksums.items():
            info = self.lvol_mount_details.get(name)
            if not info:
                continue
            mount = info["Mount"]
            try:
                files = self.ssh_obj.find_files(client, mount)
                self.ssh_obj.verify_checksums(
                    client, files, expected_checksums,
                    message=(
                        f"Data integrity check failed for lvol {name} "
                        f"after device migration"
                    ),
                )
                self.logger.info(f"Checksums verified for {name}: OK")
            except ValueError as exc:
                self.logger.error(f"Checksum MISMATCH for {name}: {exc}")
                mismatches += 1
            except Exception as exc:
                self.logger.error(
                    f"Checksum verification error for {name}: {exc}"
                )
                mismatches += 1

        assert mismatches == 0, (
            f"Data integrity check failed: {mismatches} lvol(s) had "
            f"checksum mismatches after migration"
        )
        self.logger.info(
            "All post-migration checksums verified — data integrity OK"
        )

    def _phase_validate_fio(self):
        """Check FIO logs for errors after migration (under-load variant).

        IO errors on lvols hosted on the failed device are expected and
        logged as warnings.  IO errors on lvols hosted on OTHER devices
        are logged as errors.
        """
        self.logger.info("=== Verifying FIO logs for errors ===")
        client = self.fio_node[0]
        fail_words = ["error", "fail", "interrupt", "terminate"]
        target_errors = []
        other_errors = []

        all_names = self._lvols_on_target + self._lvols_on_others
        for name in all_names:
            info = self.lvol_mount_details.get(name)
            if not info or not info.get("Log"):
                continue
            try:
                log_data = self.ssh_obj.exec_command(
                    client, f"cat {info['Log']} 2>/dev/null || true"
                )
                if not log_data:
                    self.logger.warning(f"Empty or missing FIO log for {name}")
                    continue
                log_lower = log_data.lower() if isinstance(log_data, str) else str(log_data).lower()
                found = [w for w in fail_words if w in log_lower]
                if found:
                    msg = f"{name}: FIO log contains {found}"
                    if name in self._lvols_on_target:
                        target_errors.append(msg)
                        self.logger.warning(
                            f"[expected] FIO error on failed-device lvol {name}: {found}"
                        )
                    else:
                        other_errors.append(msg)
                        self.logger.error(
                            f"FIO error on non-target lvol {name}: {found}"
                        )
                else:
                    self.logger.info(f"FIO log for {name}: no errors")
            except Exception as exc:
                self.logger.warning(f"Could not read FIO log for {name}: {exc}")

        if target_errors:
            self.logger.warning(
                f"{len(target_errors)} FIO error(s) on target-device lvols "
                f"(expected during device migration)"
            )
        if other_errors:
            self.logger.error(
                f"{len(other_errors)} FIO error(s) on non-target lvols: "
                f"{other_errors}"
            )

    # ── Phase: wait for FIO to complete naturally ──────────────────────────

    def _phase_wait_fio_completion(self):
        """Wait for FIO processes to finish naturally (do NOT kill them).

        Uses ``common_utils.manage_fio_threads`` to poll for active FIO
        processes on the client node until none remain.
        """
        self.logger.info("=== Phase: Waiting for FIO to complete naturally ===")
        client = self.fio_node[0]
        t0 = time.time()
        timeout = self.FIO_LOAD_RUNTIME + 300  # runtime + buffer

        self.common_utils.manage_fio_threads(
            node=client, threads=[], timeout=timeout
        )

        self._timing["fio_completion_duration"] = time.time() - t0
        self.logger.info(
            f"All FIO processes completed "
            f"({self._timing['fio_completion_duration']:.1f}s)"
        )

    # ── Phase 3: start random IO on all nodes (under-load variant) ───────────

    def _phase_start_io_load(self):
        self.logger.info(
            f"=== Phase: Start IO load on all nodes "
            f"(bs={self.FIO_LOAD_BS}, rw=randwrite, verify=md5) ==="
        )
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
                    "rw": "randwrite",
                    "bs": self.FIO_LOAD_BS,
                    "size": "1G",
                    "runtime": self.FIO_LOAD_RUNTIME,
                    "iodepth": self.FIO_LOAD_IODEPTH,
                    "numjobs": self.FIO_LOAD_NUMJOBS,
                    "use_latency": False,
                    "verify": "md5",
                },
            )
            t.start()
            self._load_fio_threads.append(t)

        sleep_n_sec(15)  # let IO ramp up
        self.logger.info(
            f"IO load started: {len(self._load_fio_threads)} FIO threads"
        )

    # ── Phase 4a: API removal -> set-failed -> wait migration ────────────────

    def _phase_fail_and_migrate(self):
        self.logger.info(
            f"=== Phase: Fail device {self._target_device_id} via API and migrate ==="
        )
        t0 = time.time()

        # Step 1: remove device (ONLINE -> REMOVED)
        self.logger.info(f"Removing device {self._target_device_id} ...")
        self.sbcli_utils.remove_device(self._target_device_id)
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "removed", timeout=120,
            device_id=self._target_device_id,
        )
        self._timing["remove_duration"] = time.time() - t0
        self.logger.info(
            f"Device removed ({self._timing['remove_duration']:.1f}s)"
        )

        # Step 2: set-failed via CLI (no REST endpoint exists)
        t1 = time.time()
        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} -d sn set-failed-device {self._target_device_id}"
        self.logger.info(f"Setting device failed via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"set-failed-device result: {result}")
        sleep_n_sec(5)

        # Step 3: wait for migration to complete
        self._wait_migration_and_verify(t1)

    # ── Phase 4b: PCIe sysfs removal -> set-failed -> wait migration ─────────

    def _phase_fail_and_migrate_pcie(self):
        self.logger.info(
            f"=== Phase: Fail device {self._target_device_id} via PCIe and migrate ==="
        )
        t0 = time.time()

        # Step 1: Get node IP and PCIe address
        node_details = self.sbcli_utils.get_storage_node_details(
            self._target_node_id
        )
        node_ip = node_details[0]["mgmt_ip"]
        pcie_addr = self._target_device_info.get("pcie_address", "")
        if not pcie_addr:
            raise RuntimeError(
                f"No pcie_address found for device {self._target_device_id}"
            )
        self.logger.info(
            f"PCIe hot-unplug: device {self._target_device_id} "
            f"at {pcie_addr} on {node_ip}"
        )

        # Step 2: PCIe hot-unplug via sysfs
        self.ssh_obj.exec_command(
            node=node_ip,
            command=f"echo 1 | sudo tee /sys/bus/pci/devices/{pcie_addr}/remove"
        )
        self.logger.info("PCIe device removed via sysfs")
        sleep_n_sec(10)

        # Step 3: Wait for control plane to detect device loss.
        # The control plane may auto-remove the device (unavailable → removed)
        # before we poll, so accept both states.
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, ["unavailable", "removed"], timeout=120,
            device_id=self._target_device_id,
        )
        self._timing["remove_duration"] = time.time() - t0

        # Check which state we landed in
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        actual_status = "unknown"
        for d in devices:
            if d["id"] == self._target_device_id:
                actual_status = d["status"]
                break
        self.logger.info(
            f"Device detected as {actual_status} "
            f"({self._timing['remove_duration']:.1f}s)"
        )

        # Step 4: Logical remove + set-failed to trigger migration.
        # If control plane already auto-removed, skip the remove step.
        t1 = time.time()
        if actual_status != "removed":
            self.sbcli_utils.remove_device(self._target_device_id)
            self.sbcli_utils.wait_for_device_status(
                self._target_node_id, "removed", timeout=120,
                device_id=self._target_device_id,
            )
        else:
            self.logger.info(
                "Device already in 'removed' state (auto-removed by control plane), "
                "skipping manual remove"
            )

        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} -d sn set-failed-device {self._target_device_id}"
        self.logger.info(f"Setting device failed via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"set-failed-device result: {result}")
        sleep_n_sec(5)

        # Step 5: wait for migration to complete
        self._wait_migration_and_verify(t1)

        # Step 6: Rescan PCI bus to bring device back (for future tests)
        self.logger.info("Rescanning PCI bus to restore device ...")
        self.ssh_obj.exec_command(
            node=node_ip,
            command="echo 1 | sudo tee /sys/bus/pci/rescan"
        )
        sleep_n_sec(10)
        self.logger.info("PCI bus rescan complete")

    # ── Phase 4c: PCIe remove → rescan → restart-device ─────────────────────

    def _phase_pcie_remove_and_restart(self):
        """PCIe hot-unplug, wait for detection, rescan PCI bus, restart device.

        This is the "restart" variant — no set-failed, no failure migration.
        The device is physically unplugged, the control plane detects the loss,
        then we rescan PCI to make it visible again and call restart-device.
        """
        self.logger.info(
            f"=== Phase: PCIe remove + restart device {self._target_device_id} ==="
        )
        t0 = time.time()

        # Step 1: Get node IP and PCIe address
        node_details = self.sbcli_utils.get_storage_node_details(
            self._target_node_id
        )
        node_ip = node_details[0]["mgmt_ip"]
        pcie_addr = self._target_device_info.get("pcie_address", "")
        if not pcie_addr:
            raise RuntimeError(
                f"No pcie_address found for device {self._target_device_id}"
            )
        self.logger.info(
            f"PCIe hot-unplug: device {self._target_device_id} "
            f"at {pcie_addr} on {node_ip}"
        )

        # Step 2: PCIe hot-unplug via sysfs
        self.ssh_obj.exec_command(
            node=node_ip,
            command=f"echo 1 | sudo tee /sys/bus/pci/devices/{pcie_addr}/remove"
        )
        self.logger.info("PCIe device removed via sysfs")
        sleep_n_sec(10)

        # Step 3: Wait for control plane to detect device loss
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, ["unavailable", "removed"], timeout=120,
            device_id=self._target_device_id,
        )
        self._timing["remove_duration"] = time.time() - t0
        self.logger.info(
            f"Device detected as unavailable/removed "
            f"({self._timing['remove_duration']:.1f}s)"
        )

        # Step 4: Rescan PCI bus to make the physical device visible again
        self.logger.info("Rescanning PCI bus to restore physical device ...")
        self.ssh_obj.exec_command(
            node=node_ip,
            command="echo 1 | sudo tee /sys/bus/pci/rescan"
        )
        sleep_n_sec(15)
        self.logger.info("PCI bus rescan complete")

        # Step 5: Restart device via CLI
        t1 = time.time()
        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} -d sn restart-device {self._target_device_id}"
        self.logger.info(f"Restarting device via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"restart-device result: {result}")

        # Step 6: Wait for device to come back online
        self.logger.info("Waiting for device to come back online ...")
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "online", timeout=600,
            device_id=self._target_device_id,
        )
        self._timing["restart_duration"] = time.time() - t1
        self.logger.info(
            f"Device back online ({self._timing['restart_duration']:.1f}s)"
        )

        # Step 7: Wait for any migration tasks that may have been triggered
        self.logger.info("Checking for any active migration tasks ...")
        try:
            migration_elapsed = self.sbcli_utils.wait_migration_tasks_complete(
                timeout=self.MIGRATION_TIMEOUT
            )
            self._timing["migration_duration"] = migration_elapsed
            self.logger.info(
                f"Migration tasks complete ({migration_elapsed:.1f}s)"
            )
        except TimeoutError:
            raise
        except Exception as exc:
            self.logger.info(f"No migration tasks or already done: {exc}")
            self._timing["migration_duration"] = 0

    def _phase_validate_restart(self):
        """Validate after device restart: device online, nodes healthy, cluster active."""
        self.logger.info("=== Phase: Validate restart results ===")

        # 1. Target device should be back online
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        target_dev = None
        for d in devices:
            if d["id"] == self._target_device_id:
                target_dev = d
                break
        assert target_dev is not None, (
            f"Target device {self._target_device_id} not found"
        )
        assert target_dev["status"] == "online", (
            f"Device {self._target_device_id} expected online, "
            f"got {target_dev['status']}"
        )
        self.logger.info(
            f"Device {self._target_device_id} status: {target_dev['status']}"
        )

        # 2. All storage nodes should be online and healthy
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for node in storage_nodes["results"]:
            assert node["status"] == "online", (
                f"Node {node['id']} is not online (status={node['status']})"
            )
            assert node["health_check"], (
                f"Node {node['id']} health check failed"
            )
        self.logger.info(
            f"All {len(storage_nodes['results'])} storage nodes online and healthy"
        )

        # 3. All other devices on target node should be online
        for d in devices:
            if d["id"] == self._target_device_id:
                continue
            if d["id"] in self._pre_existing_failed_devices:
                continue
            assert d["status"] == "online", (
                f"Non-target device {d['id']} expected online, "
                f"got {d['status']}"
            )
        self.logger.info("All non-target devices remain online")

        # 4. Cluster should be active
        cluster_details = self.sbcli_utils.get_cluster_details(
            self.sbcli_utils.cluster_id
        )
        if cluster_details:
            cl = cluster_details[0] if isinstance(cluster_details, list) else cluster_details
            cluster_status = cl.get("status", "unknown")
            self.logger.info(f"Cluster status: {cluster_status}")
            assert cluster_status == "active", (
                f"Cluster expected active, got {cluster_status}"
            )

    # ── Shared migration wait + verify ───────────────────────────────────────

    def _wait_migration_and_verify(self, t_start):
        """Wait for migration tasks and verify final device status.

        Tries the REST-based ``wait_migration_tasks_complete`` first.
        If the API is unavailable (404 etc.), falls back to polling
        ``sbctl cluster list-tasks`` via CLI.
        """
        self.logger.info("Waiting for failure migration tasks to complete ...")
        try:
            migration_elapsed = self.sbcli_utils.wait_migration_tasks_complete(
                timeout=self.MIGRATION_TIMEOUT
            )
        except TimeoutError:
            raise
        except Exception as exc:
            self.logger.warning(
                f"REST migration wait failed ({exc}), falling back to CLI"
            )
            migration_elapsed = self._wait_migration_cli_fallback()

        self._timing["failed_device_migration_duration"] = time.time() - t_start
        self._timing["migration_tasks_elapsed"] = migration_elapsed

        # Verify device status
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
            f"(failed device migration took {self._timing['failed_device_migration_duration']:.1f}s)"
        )
        self._timing["device_final_status"] = final_status

    # ── Post-migration health check ─────────────────────────────────────────

    def _phase_post_migration_health_check(self):
        """Check all node statuses every 10s for 5 minutes after migration.

        Logs status at each interval.  Runs in the finally block so it
        always executes.  Does not assert — just records results.
        """
        self.logger.info("=== Phase: Post-migration health check (5m, 10s intervals) ===")
        check_interval = 10
        check_duration = 300  # 5 minutes
        t0 = time.time()
        all_healthy = True
        unhealthy_records = []

        while time.time() - t0 < check_duration:
            try:
                storage_nodes = self.sbcli_utils.get_storage_nodes()
                statuses = {}
                for node in storage_nodes.get("results", []):
                    nid = node.get("id", "?")
                    status = node.get("status", "?")
                    health = node.get("health_check", "?")
                    statuses[nid] = {"status": status, "health": health}
                    if status != "online":
                        all_healthy = False
                        unhealthy_records.append({
                            "time": round(time.time() - t0, 1),
                            "node": nid,
                            "status": status,
                            "health": health,
                        })

                elapsed = time.time() - t0
                status_str = "  ".join(
                    f"{nid[:8]}={s['status']}(health={s['health']})"
                    for nid, s in statuses.items()
                )
                self.logger.info(
                    f"  [{elapsed:5.0f}s] {status_str}"
                )
            except Exception as exc:
                self.logger.warning(f"  Health check poll error: {exc}")

            sleep_n_sec(check_interval)

        self._timing["post_migration_all_healthy"] = all_healthy
        if unhealthy_records:
            self._timing["post_migration_unhealthy_events"] = len(unhealthy_records)
            self.logger.warning(
                f"Post-migration health check: {len(unhealthy_records)} unhealthy "
                f"event(s) in {check_duration}s"
            )
            for rec in unhealthy_records:
                self.logger.warning(
                    f"  t={rec['time']}s node={rec['node'][:8]} "
                    f"status={rec['status']} health={rec['health']}"
                )
        else:
            self.logger.info(
                "Post-migration health check: all nodes healthy for "
                f"{check_duration}s"
            )

    # ── Phase: restart non-target node under IO load ────────────────────────

    def _phase_restart_non_target_node(self):
        """Restart a non-target storage node while IO is active.

        Validates that the cluster is truly healthy post-migration by
        restarting a node that does NOT hold the faulty device and
        verifying IO continues without interruption.
        """
        self.logger.info(
            "=== Phase: Restart non-target node under IO load ==="
        )
        t0 = time.time()
        restart_node_id = self._pick_non_target_restart_node()
        if not restart_node_id:
            self.logger.warning(
                "No non-target node candidate found — skipping restart validation"
            )
            self._timing["non_target_restart_result"] = "skipped_no_candidate"
            return

        self._timing["non_target_restart_node"] = restart_node_id
        self.logger.info(f"Selected non-target node for restart: {restart_node_id}")

        try:
            # Log pre-restart FIO state
            pre_fio = self._count_active_fio()
            self.logger.info(f"Active FIO processes before restart: {pre_fio}")

            # Shutdown the node first
            self.logger.info(f"Shutting down node {restart_node_id} ...")
            deadline = time.time() + 300
            while True:
                try:
                    self.sbcli_utils.shutdown_node(
                        node_uuid=restart_node_id, force=False
                    )
                except Exception as e:
                    self.logger.warning(
                        f"shutdown_node raised (may already be shutting down): {e}"
                    )
                sleep_n_sec(20)

                self.logger.info(
                    f"Waiting for node {restart_node_id} to go offline ..."
                )
                self.sbcli_utils.wait_for_storage_node_status(
                    restart_node_id, "offline", timeout=600
                )
                node_detail = self.sbcli_utils.get_storage_node_details(
                    restart_node_id
                )
                if node_detail[0]["status"] == "offline":
                    self.logger.info(f"Node {restart_node_id} is offline")
                    break
                if time.time() > deadline:
                    raise RuntimeError(
                        f"Node {restart_node_id} did not reach offline "
                        f"within 300s"
                    )

            # Restart the node
            sleep_n_sec(60)
            self.logger.info(f"Restarting node {restart_node_id} ...")
            self.sbcli_utils.restart_node(restart_node_id)

            # Wait for node to come back online
            self.logger.info(
                f"Waiting for node {restart_node_id} to come back online ..."
            )
            self.sbcli_utils.wait_for_storage_node_status(
                restart_node_id, "online", timeout=600
            )
            self.logger.info(f"Node {restart_node_id} is online")

            # Wait for health check to pass
            self.logger.info(
                f"Waiting for node {restart_node_id} health check ..."
            )
            self.sbcli_utils.wait_for_health_status(
                restart_node_id, True, timeout=300
            )
            self.logger.info(f"Node {restart_node_id} health check passed")

            # Stabilization period
            sleep_n_sec(30)

            # Verify all nodes are online and healthy
            storage_nodes = self.sbcli_utils.get_storage_nodes()
            for node in storage_nodes.get("results", []):
                nid = node.get("id", "?")
                status = node.get("status", "?")
                health = node.get("health_check", False)
                self.logger.info(
                    f"  Node {nid[:8]}: status={status} health={health}"
                )
                if status != "online":
                    raise RuntimeError(
                        f"Node {nid} has status '{status}' after "
                        f"non-target restart (expected online)"
                    )

            # Verify FIO is still running
            self._verify_fio_still_running()

            self._timing["non_target_restart_duration"] = time.time() - t0
            self._timing["non_target_restart_result"] = "passed"
            self.logger.info(
                f"Non-target node restart validation PASSED "
                f"({self._timing['non_target_restart_duration']:.1f}s)"
            )
        except Exception as exc:
            self._timing["non_target_restart_duration"] = time.time() - t0
            self._timing["non_target_restart_result"] = f"failed: {exc}"
            self.logger.error(
                f"Non-target node restart validation FAILED: {exc}"
            )
            raise

    def _pick_non_target_restart_node(self):
        """Pick a non-target storage node to restart.

        Prefers a node that has IO running on it (from ``_lvols_on_others``).
        Returns the node UUID or None if no candidate found.
        """
        candidates = set()

        # Docker: check lvol_mount_details for NodeID
        if hasattr(self, "lvol_mount_details"):
            for name in self._lvols_on_others:
                info = self.lvol_mount_details.get(name, {})
                nid = info.get("NodeID")
                if nid and nid != self._target_node_id:
                    candidates.add(nid)

        # K8s: check _pvc_details for node_id
        if hasattr(self, "_pvc_details"):
            for name in self._lvols_on_others:
                info = self._pvc_details.get(name, {})
                nid = info.get("node_id")
                if nid and nid != self._target_node_id:
                    candidates.add(nid)

        # Fallback: any non-target node
        if not candidates:
            candidates = {
                n for n in self._sn_nodes if n != self._target_node_id
            }

        if not candidates:
            return None

        return sorted(candidates)[0]

    def _count_active_fio(self):
        """Count active FIO processes on the client node (Docker variant)."""
        try:
            client = self.fio_node[0]
            process = self.ssh_obj.find_process_name(
                node=client, process_name="fio --name"
            )
            process_fio = [
                p for p in process
                if "grep" not in p and not p.startswith("kworker")
            ]
            return len(process_fio)
        except Exception as exc:
            self.logger.warning(f"Could not count FIO processes: {exc}")
            return -1

    def _verify_fio_still_running(self):
        """Verify FIO processes are still active after non-target node restart."""
        fio_count = self._count_active_fio()
        self.logger.info(f"Active FIO processes after restart: {fio_count}")
        if fio_count == 0:
            raise RuntimeError(
                "FIO processes are no longer running after non-target node "
                "restart — IO was interrupted during storage node restart"
            )
        if fio_count < 0:
            self.logger.warning(
                "Could not verify FIO process count — skipping check"
            )

    # ── Phase 5: stop IO load ────────────────────────────────────────────────

    def _phase_validate(self):
        """Validate migration results: device migrated, nodes healthy, data intact."""
        self.logger.info("=== Phase: Validate migration results ===")

        # 1. Device should be in a migrated/failed state
        final_status = self._timing.get("device_final_status", "unknown")
        assert final_status in ("failed_and_migrated", "failed"), (
            f"Device {self._target_device_id} has unexpected final status: "
            f"{final_status} (expected failed_and_migrated or failed)"
        )
        self.logger.info(
            f"Device {self._target_device_id} status: {final_status}"
        )

        # 2. All storage nodes should still be online and healthy
        #    Retry for up to 5 minutes — health_check can be transiently
        #    False due to a race between HealthCheck and migration tasks
        #    updating device statuses in the distrib cluster maps.
        node_check_deadline = time.time() + 300
        node_check_interval = 15
        while True:
            storage_nodes = self.sbcli_utils.get_storage_nodes()
            unhealthy = []
            for node in storage_nodes["results"]:
                if node["status"] != "online" or not node["health_check"]:
                    unhealthy.append(
                        f"{node['id']} (status={node['status']}, "
                        f"health_check={node['health_check']})"
                    )
            if not unhealthy:
                break
            if time.time() >= node_check_deadline:
                assert False, (
                    "Nodes still not healthy after 5 minutes: "
                    + "; ".join(unhealthy)
                )
            self.logger.warning(
                f"Unhealthy nodes, retrying in {node_check_interval}s: "
                + "; ".join(unhealthy)
            )
            time.sleep(node_check_interval)
        self.logger.info(
            f"All {len(storage_nodes['results'])} storage nodes online and healthy"
        )

        # 3. Other devices on target node should still be online
        #    (skip the target device and any pre-existing failed devices)
        #    Phase A: poll API for up to 5 minutes
        #    Phase B: if API still reports not-online, cross-check with CLI
        #             for up to 10 more minutes before failing
        api_timeout = 300
        cli_timeout = 600
        poll_interval = 15
        deadline = time.time() + api_timeout
        not_online = {}

        # Phase A: API polling
        while True:
            not_online = {}
            devices = self.sbcli_utils.get_device_details(self._target_node_id)
            for d in devices:
                if d["id"] == self._target_device_id:
                    continue
                if d["id"] in self._pre_existing_failed_devices:
                    continue
                if d.get("status") != "online":
                    not_online[d["id"]] = d.get("status")
            if not not_online:
                break
            if time.time() >= deadline:
                break
            self.logger.info(
                f"Waiting for non-target devices to come online (API): "
                f"{not_online} ({int(deadline - time.time())}s remaining)"
            )
            sleep_n_sec(poll_interval)

        if not not_online:
            self.logger.info("All non-target devices remain online")
        else:
            # Phase B: API says not-online — cross-check with CLI
            self.logger.warning(
                f"API reports non-target devices not online after "
                f"{api_timeout}s: {not_online}. "
                f"Cross-checking with CLI for up to {cli_timeout}s ..."
            )
            mgmt_ip = self.mgmt_nodes[0]
            cli_deadline = time.time() + cli_timeout

            while not_online and time.time() < cli_deadline:
                # Re-check API first in case it caught up
                devices = self.sbcli_utils.get_device_details(
                    self._target_node_id
                )
                api_resolved = []
                for d in devices:
                    if d["id"] in not_online and d.get("status") == "online":
                        api_resolved.append(d["id"])
                for dev_id in api_resolved:
                    self.logger.info(
                        f"Device {dev_id} now online in API"
                    )
                    del not_online[dev_id]

                if not not_online:
                    break

                # Cross-check with CLI: sbctl sn list-devices <sn_id>
                # Output is a pipe-delimited table:
                # | UUID | StorgeID | Name | Size | Serial | PCIe | Status | IO Err | Health |
                cmd = (
                    f"{self.base_cmd} -d sn list-devices "
                    f"{self._target_node_id}"
                )
                try:
                    result = self.ssh_obj.exec_command(mgmt_ip, cmd)
                    cli_output = result[0] if isinstance(result, tuple) else str(result)
                    self.logger.info(f"CLI list-devices output:\n{cli_output}")
                except Exception as exc:
                    self.logger.warning(f"CLI list-devices failed: {exc}")
                    sleep_n_sec(poll_interval)
                    continue

                # Parse CLI table — find rows matching device UUID and
                # extract the Status column (7th pipe-delimited field)
                cli_resolved = []
                for dev_id in list(not_online.keys()):
                    for line in cli_output.splitlines():
                        if dev_id not in line:
                            continue
                        cols = [c.strip() for c in line.split("|")]
                        # cols[0] is empty (before first |), cols[1]=UUID,
                        # ... cols[7]=Status for the Storage Devices table
                        cli_status = None
                        for i, col in enumerate(cols):
                            if col == dev_id and i + 6 < len(cols):
                                cli_status = cols[i + 6]
                                break
                        if cli_status and cli_status.lower() == "online":
                            self.logger.warning(
                                f"Device {dev_id}: CLI shows 'online' but "
                                f"API reports '{not_online[dev_id]}' — "
                                f"possible API staleness"
                            )
                            cli_resolved.append(dev_id)
                        elif cli_status:
                            self.logger.info(
                                f"Device {dev_id}: CLI status='{cli_status}', "
                                f"API status='{not_online[dev_id]}'"
                            )
                        break

                for dev_id in cli_resolved:
                    del not_online[dev_id]

                if not not_online:
                    break

                self.logger.info(
                    f"Devices still not online: {not_online} "
                    f"({int(cli_deadline - time.time())}s remaining)"
                )
                sleep_n_sec(poll_interval)

            if not_online:
                for dev_id, status in not_online.items():
                    self.logger.error(
                        f"Non-target device {dev_id} on target node has "
                        f"unexpected status: {status} after "
                        f"{api_timeout + cli_timeout}s (API + CLI check)"
                    )
                assert not not_online, (
                    f"Non-target devices not online after "
                    f"{api_timeout + cli_timeout}s: {not_online}"
                )
            else:
                self.logger.info(
                    "All non-target devices online (resolved via CLI fallback)"
                )

        # 4. Data integrity checks (NoLoad skipped — no fill/checksum on empty device;
        #    UnderLoad integrity is checked via FIO verify=md5 during IO)

    def _phase_stop_io_load(self):
        """Kill remaining FIO processes (failure path only).

        On the success path, FIO completes naturally via
        ``_phase_wait_fio_completion``.  This method runs in the
        ``finally`` block to ensure cleanup if the test failed early.
        """
        self.logger.info("=== Phase: Stop IO load (cleanup) ===")
        client = self.fio_node[0]
        self.ssh_obj.exec_command(client, "pkill -f fio || true")
        for t in self._load_fio_threads:
            t.join(timeout=30)
        self.logger.info("IO load stopped")

    # ── Logging helpers ──────────────────────────────────────────────────────

    def _log_device_fill_level(self):
        """Query and log device utilisation on the target node."""
        try:
            capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
            if isinstance(capacity, list):
                capacity = capacity[0] if capacity else {}
            size_total = capacity.get("size_total", 0)
            size_used = capacity.get("size_used", 0)
            size_util = capacity.get("size_util", 0)
            self._timing["device_fill_pct"] = size_util
            self._timing["device_size_total_bytes"] = size_total
            self._timing["device_size_used_bytes"] = size_used
            self.logger.info(
                f"Device fill level on node {self._target_node_id}: "
                f"{size_util}% used "
                f"({size_used} / {size_total} bytes)"
            )
        except Exception as exc:
            self.logger.warning(f"Could not query device fill level: {exc}")

    def _capture_cluster_capacity(self):
        """Snapshot node and device capacity for use in the summary.

        Must be called BEFORE ``_phase_cleanup()`` deletes pools/lvols,
        otherwise the capacity API may return errors or zeroes.
        """
        self.logger.info("Capturing cluster capacity snapshot for summary ...")
        for node_id in self._sn_nodes:
            entry = {"status": "?", "cap": {}, "devices": []}
            try:
                details = self.sbcli_utils.get_storage_node_details(node_id)
                entry["status"] = details[0].get("status", "?") if details else "?"
            except Exception as exc:
                self.logger.warning(
                    f"Could not get node details for {node_id}: {exc}"
                )
            try:
                cap = self.sbcli_utils.get_node_capacity(node_id)
                if isinstance(cap, list):
                    cap = cap[0] if cap else {}
                entry["cap"] = cap
            except Exception as exc:
                self.logger.warning(
                    f"Could not get node capacity for {node_id}: {exc}"
                )
            try:
                devices = self.sbcli_utils.get_device_details(node_id)
                for dev in devices:
                    dev_entry = {
                        "id": dev.get("id", "?"),
                        "status": dev.get("status", "?"),
                        "cap": {},
                    }
                    try:
                        dcap = self.sbcli_utils.get_device_capacity(dev_entry["id"])
                        if isinstance(dcap, list):
                            dcap = dcap[0] if dcap else {}
                        dev_entry["cap"] = dcap
                    except Exception:
                        pass
                    entry["devices"].append(dev_entry)
            except Exception as exc:
                self.logger.warning(
                    f"Could not get device details for {node_id}: {exc}"
                )
            self._cached_capacity[node_id] = entry
        self.logger.info(
            f"Captured capacity for {len(self._cached_capacity)} nodes"
        )

    def _build_capacity_md_lines(self):
        """Return markdown lines for Per-Node and Per-Device Capacity tables.

        Uses data from ``_cached_capacity`` (populated by
        ``_capture_cluster_capacity``).  Falls back to live API calls if
        the cache is empty (e.g. capture was skipped).
        """
        lines = []

        # ── Per-Node Capacity ──
        lines.append("## Per-Node Capacity")
        lines.append("| Node | Status | Size Total | Size Used | Utilisation |")
        lines.append("|------|--------|-----------|-----------|-------------|")
        for node_id in self._sn_nodes:
            cached = self._cached_capacity.get(node_id)
            if cached:
                status = cached.get("status", "?")
                cap = cached.get("cap", {})
            else:
                # Fallback: live query (may fail post-cleanup)
                try:
                    details = self.sbcli_utils.get_storage_node_details(node_id)
                    status = details[0].get("status", "?") if details else "?"
                    cap = self.sbcli_utils.get_node_capacity(node_id)
                    if isinstance(cap, list):
                        cap = cap[0] if cap else {}
                except Exception as exc:
                    lines.append(f"| `{node_id}` | error | — | — | — |")
                    self.logger.warning(
                        f"Could not get capacity for node {node_id}: {exc}"
                    )
                    continue
            s_total = cap.get("size_total", 0)
            s_used = cap.get("size_used", 0)
            s_util = cap.get("size_util", 0)
            lines.append(
                f"| `{node_id}` | {status} | "
                f"{_fmt_bytes(s_total)} | {_fmt_bytes(s_used)} | {s_util}% |"
            )
        lines.append("")

        # ── Per-Device Capacity ──
        lines.append("## Per-Device Capacity")
        lines.append("| Node | Device | Status | Size Total | Size Used | Utilisation |")
        lines.append("|------|--------|--------|-----------|-----------|-------------|")
        for node_id in self._sn_nodes:
            cached = self._cached_capacity.get(node_id)
            if cached and cached.get("devices"):
                for dev in cached["devices"]:
                    dev_id = dev.get("id", "?")
                    dev_status = dev.get("status", "?")
                    dcap = dev.get("cap", {})
                    ds_total = dcap.get("size_total", 0)
                    ds_used = dcap.get("size_used", 0)
                    ds_util = dcap.get("size_util", 0)
                    lines.append(
                        f"| `{node_id}` | `{dev_id}` | {dev_status} | "
                        f"{_fmt_bytes(ds_total)} | {_fmt_bytes(ds_used)} | {ds_util}% |"
                    )
            else:
                # Fallback: live query
                try:
                    devices = self.sbcli_utils.get_device_details(node_id)
                    for dev in devices:
                        dev_id = dev.get("id", "?")
                        dev_status = dev.get("status", "?")
                        try:
                            dcap = self.sbcli_utils.get_device_capacity(dev_id)
                            if isinstance(dcap, list):
                                dcap = dcap[0] if dcap else {}
                            ds_total = dcap.get("size_total", 0)
                            ds_used = dcap.get("size_used", 0)
                            ds_util = dcap.get("size_util", 0)
                            lines.append(
                                f"| `{node_id}` | `{dev_id}` | {dev_status} | "
                                f"{_fmt_bytes(ds_total)} | {_fmt_bytes(ds_used)} | "
                                f"{ds_util}% |"
                            )
                        except Exception:
                            lines.append(
                                f"| `{node_id}` | `{dev_id}` | {dev_status} "
                                f"| — | — | — |"
                            )
                except Exception as exc:
                    lines.append(f"| `{node_id}` | — | error | — | — | — |")
                    self.logger.warning(
                        f"Could not get devices for node {node_id}: {exc}"
                    )
        lines.append("")

        return lines

    def _phase_collect_cluster_io_stats(self):
        """Collect cluster-wide IO stats from the API and compute averages."""
        self.logger.info("=== Phase: Collect cluster IO stats ===")
        try:
            cluster_id = self.sbcli_utils.cluster_id
            # Calculate time duration covering the test run (round up to next minute + 1m buffer)
            total_elapsed = self._timing.get("total_duration", 0)
            if total_elapsed <= 0:
                # Estimate from known phase durations
                total_elapsed = sum(
                    self._timing.get(k, 0) for k in (
                        "setup_duration", "fill_duration", "remove_duration",
                        "failed_device_migration_duration",
                        "new_device_migration_duration",
                        "restart_duration", "migration_duration",
                        "fio_completion_duration",
                    )
                )
            minutes = max(5, int(total_elapsed / 60) + 2)  # at least 5m, +2m buffer
            time_duration = f"{minutes}m"
            self.logger.info(
                f"Fetching cluster IO stats for {cluster_id} "
                f"over last {time_duration}"
            )

            io_stats = self.sbcli_utils.get_io_stats(cluster_id, time_duration)
            self.logger.info(f"Got {len(io_stats) if io_stats else 0} IO stat records")

            if not io_stats:
                self.logger.warning("No cluster IO stats returned from API")
                return

            # Compute averages across all records
            write_bps_vals = []
            read_bps_vals = []
            write_iops_vals = []
            read_iops_vals = []
            write_lat_vals = []
            read_lat_vals = []

            for stat in io_stats:
                w_bps = stat.get("write_bytes_ps", 0)
                r_bps = stat.get("read_bytes_ps", 0)
                w_iops = stat.get("write_io_ps", 0)
                r_iops = stat.get("read_io_ps", 0)
                w_lat = stat.get("write_latency_ps", 0)
                r_lat = stat.get("read_latency_ps", 0)
                if w_bps or r_bps:  # skip records with zero IO
                    write_bps_vals.append(w_bps)
                    read_bps_vals.append(r_bps)
                    write_iops_vals.append(w_iops)
                    read_iops_vals.append(r_iops)
                    write_lat_vals.append(w_lat)
                    read_lat_vals.append(r_lat)

            n = len(write_bps_vals)
            if n == 0:
                self.logger.warning("All cluster IO stat records have zero IO")
                return

            avg_write_bps = sum(write_bps_vals) / n
            avg_read_bps = sum(read_bps_vals) / n
            avg_write_iops = sum(write_iops_vals) / n
            avg_read_iops = sum(read_iops_vals) / n
            avg_write_lat = sum(write_lat_vals) / n
            avg_read_lat = sum(read_lat_vals) / n

            self._timing["cluster_io_records"] = n
            self._timing["avg_write_bytes_ps"] = round(avg_write_bps, 1)
            self._timing["avg_read_bytes_ps"] = round(avg_read_bps, 1)
            self._timing["avg_write_iops"] = round(avg_write_iops, 1)
            self._timing["avg_read_iops"] = round(avg_read_iops, 1)
            self._timing["avg_write_latency_ps"] = round(avg_write_lat, 3)
            self._timing["avg_read_latency_ps"] = round(avg_read_lat, 3)

            self.logger.info(
                f"Cluster IO stats ({n} records): "
                f"avg write={avg_write_bps/(1024*1024):.1f} MiB/s, "
                f"avg read={avg_read_bps/(1024*1024):.1f} MiB/s, "
                f"avg write IOPS={avg_write_iops:.0f}, "
                f"avg read IOPS={avg_read_iops:.0f}, "
                f"avg write latency={avg_write_lat:.3f}us, "
                f"avg read latency={avg_read_lat:.3f}us"
            )
        except Exception as exc:
            self.logger.warning(f"Could not collect cluster IO stats: {exc}")

    # ── Phase: recover failed device ─────────────────────────────────────────

    def _phase_recover_device(self):
        """Create a new device from the failed one and add it back.

        Runs in the finally block so it executes even if the test fails.

        Steps:
          0. (PCIe mode) Rescan PCI bus so the physical device is visible again
          1. ``sbctl sn new-device-from-failed <failed_device_id>`` → new device ID
          2. ``sbctl sn add-device <new_device_id>``
          3. Wait for ``new_device_migration`` tasks to complete
        """
        if not self._target_device_id:
            return
        self.logger.info(
            f"=== Phase: Recover device {self._target_device_id} ==="
        )

        # Step 0: For PCIe mode, always rescan PCI bus to restore the
        # physical device (in case the test crashed after hot-unplug
        # but before the in-test rescan)
        if self._failure_mode == "pcie" and self._target_node_id:
            try:
                node_details = self.sbcli_utils.get_storage_node_details(
                    self._target_node_id
                )
                node_ip = node_details[0]["mgmt_ip"]
                self.logger.info(
                    f"Rescanning PCI bus on {node_ip} to ensure device is visible"
                )
                self.ssh_obj.exec_command(
                    node=node_ip,
                    command="echo 1 | sudo tee /sys/bus/pci/rescan"
                )
                sleep_n_sec(15)
                self.logger.info("PCI bus rescan complete")
            except Exception as exc:
                self.logger.warning(f"PCI bus rescan failed: {exc}")

        mgmt_ip = self.mgmt_nodes[0]

        # Step 0b: If the test crashed before set-failed-device was called,
        # the device may still be in 'removed' state.  Drive it through
        # set-failed → migration → failed_and_migrated so that
        # new-device-from-failed will succeed.
        try:
            devices = self.sbcli_utils.get_device_details(self._target_node_id)
            dev_status = "unknown"
            for d in devices:
                if d["id"] == self._target_device_id:
                    dev_status = d["status"]
                    break
            self.logger.info(
                f"Device {self._target_device_id} current status: {dev_status}"
            )
            if dev_status == "removed":
                self.logger.info(
                    "Device still in 'removed' state — running set-failed-device "
                    "and waiting for migration before recovery"
                )
                cmd = (
                    f"{self.base_cmd} -d sn set-failed-device "
                    f"{self._target_device_id}"
                )
                self.ssh_obj.exec_command(mgmt_ip, cmd)
                sleep_n_sec(5)
                try:
                    self.sbcli_utils.wait_migration_tasks_complete(
                        timeout=self.MIGRATION_TIMEOUT
                    )
                except Exception as mig_exc:
                    self.logger.warning(
                        f"Migration wait during recovery failed: {mig_exc}"
                    )
            elif dev_status == "unavailable":
                self.logger.info(
                    "Device still in 'unavailable' state — running remove + "
                    "set-failed-device and waiting for migration before recovery"
                )
                self.sbcli_utils.remove_device(self._target_device_id)
                self.sbcli_utils.wait_for_device_status(
                    self._target_node_id, "removed", timeout=120,
                    device_id=self._target_device_id,
                )
                cmd = (
                    f"{self.base_cmd} -d sn set-failed-device "
                    f"{self._target_device_id}"
                )
                self.ssh_obj.exec_command(mgmt_ip, cmd)
                sleep_n_sec(5)
                try:
                    self.sbcli_utils.wait_migration_tasks_complete(
                        timeout=self.MIGRATION_TIMEOUT
                    )
                except Exception as mig_exc:
                    self.logger.warning(
                        f"Migration wait during recovery failed: {mig_exc}"
                    )
        except Exception as exc:
            self.logger.warning(
                f"Pre-recovery device status check/fix failed: {exc}"
            )

        # Step 1: create new device from failed device
        try:
            cmd = (
                f"{self.base_cmd} -d sn new-device-from-failed "
                f"{self._target_device_id}"
            )
            self.logger.info(f"Creating new device from failed: {cmd}")
            result = self.ssh_obj.exec_command(mgmt_ip, cmd)
            result_str = result[0] if isinstance(result, tuple) else str(result)
            result_str = result_str.strip()
            self.logger.info(f"new-device-from-failed result: {result_str}")

            # Check for "already added back" — device was recovered previously
            if "already added back from failed" in result_str.lower():
                self.logger.info(
                    "Device was already recovered from a previous run, "
                    "skipping add-device step"
                )
                return

            # Check for other errors in output
            if "error" in result_str.lower() and "new device id:" not in result_str.lower():
                self.logger.error(
                    f"new-device-from-failed returned error: {result_str}"
                )
                return

            # The last line of successful output is the bare UUID
            # e.g. "5ab70b74-c8c5-4e24-b76e-dd64bdcfa39d"
            new_device_id = result_str.strip().split("\n")[-1].strip()
            # Validate it looks like a UUID (8-4-4-4-12 hex)
            import re
            if not re.match(
                r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
                new_device_id
            ):
                self.logger.error(
                    f"Could not parse valid device UUID from output. "
                    f"Got: '{new_device_id}', full output: {result_str}"
                )
                return
            self.logger.info(f"New device ID: {new_device_id}")
        except Exception as exc:
            self.logger.error(f"new-device-from-failed failed: {exc}")
            return

        # Step 2: add the new device
        try:
            cmd = f"{self.base_cmd} -d sn add-device {new_device_id}"
            self.logger.info(f"Adding new device: {cmd}")
            result = self.ssh_obj.exec_command(mgmt_ip, cmd)
            self.logger.info(f"add-device result: {result}")
            sleep_n_sec(5)
        except Exception as exc:
            self.logger.error(f"add-device failed: {exc}")
            return

        # Step 3: wait for new_device_migration tasks to complete
        t_new_mig = time.time()
        try:
            self._wait_new_device_migration(
                new_device_id, timeout=self.MIGRATION_TIMEOUT
            )
            self._timing["new_device_migration_duration"] = time.time() - t_new_mig
            self.logger.info(
                f"Device recovery complete — new device {new_device_id} online "
                f"({self._timing['new_device_migration_duration']:.1f}s)"
            )
        except Exception as exc:
            self._timing["new_device_migration_duration"] = time.time() - t_new_mig
            self.logger.warning(
                f"new_device_migration did not complete: {exc}"
            )

    def _wait_new_device_migration(self, new_device_id, timeout=3600):
        """Wait for all new_device_migration tasks for the target node to finish.

        The migration tasks use ``NodeID:<node-uuid>`` as ``target_id``,
        not the device UUID, so we filter by ``self._target_node_id``.
        We also wait up to 120 s for the first task to appear (the
        control-plane may need a few seconds to create migration tasks
        after ``add-device``).
        """
        node_id = self._target_node_id
        self.logger.info(
            f"Waiting for new_device_migration tasks for device "
            f"{new_device_id} (node {node_id}) ..."
        )
        start = time.time()
        seen_any = False
        grace_period = 120  # seconds to wait for tasks to appear
        while time.time() - start < timeout:
            try:
                tasks = self.sbcli_utils.list_migration_tasks(
                    self.sbcli_utils.cluster_id
                )
                matching = [
                    t for t in tasks.get("results", [])
                    if t.get("function_name") == "new_device_migration"
                    and node_id in str(t.get("target_id", ""))
                ]
                active = [
                    t for t in matching
                    if t.get("status") not in ("done", "cancelled", "error")
                ]
                if matching:
                    seen_any = True
                if seen_any and not active:
                    elapsed = time.time() - start
                    self.logger.info(
                        f"All new_device_migration tasks complete "
                        f"({len(matching)} total) in {elapsed:.1f}s"
                    )
                    return elapsed
                if not seen_any and time.time() - start > grace_period:
                    elapsed = time.time() - start
                    self.logger.warning(
                        f"No new_device_migration tasks appeared "
                        f"after {grace_period}s — returning"
                    )
                    return elapsed
                if active:
                    self.logger.info(
                        f"Waiting for {len(active)}/{len(matching)} "
                        f"new_device_migration task(s) ..."
                    )
                else:
                    self.logger.info(
                        "Waiting for new_device_migration tasks to "
                        "appear ..."
                    )
            except Exception as exc:
                self.logger.warning(
                    f"Error checking migration tasks: {exc}"
                )
            sleep_n_sec(10)
        self.logger.warning(
            f"new_device_migration not complete after {timeout}s"
        )

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
        self.logger.info(f"  Failure mode:     {self._failure_mode}")
        self.logger.info(f"  IO load:          {'YES' if self._with_io_load else 'NO (empty device)'}")
        self.logger.info(f"  FIO params:       bs={self.FIO_LOAD_BS} rw=randwrite verify=md5" if self._with_io_load else "  FIO params:       N/A")
        self.logger.info(f"  Target node:      {self._target_node_id}")
        self.logger.info(f"  Target device:    {self._target_device_id}")
        self.logger.info(f"  Lvols on target:  {len(self._lvols_on_target)}")
        self.logger.info(f"  Lvols on others:  {len(self._lvols_on_others)}")
        self.logger.info(f"  Result:           {'PASSED' if self._test_passed else 'FAILED'}")
        self.logger.info("-" * 70)
        # Key metrics
        fill_pct = self._timing.get("device_fill_pct", "0 (empty)")
        self.logger.info(f"  {'device_fill_pct':30s} {fill_pct}")
        failed_mig = self._timing.get("failed_device_migration_duration", 0)
        if isinstance(failed_mig, (int, float)) and failed_mig > 0:
            self.logger.info(f"  {'failed_device_migration':30s} {failed_mig:10.1f}s ({failed_mig/60:.1f}m)")
        new_mig = self._timing.get("new_device_migration_duration", 0)
        if isinstance(new_mig, (int, float)) and new_mig > 0:
            self.logger.info(f"  {'new_device_migration':30s} {new_mig:10.1f}s ({new_mig/60:.1f}m)")
        restart = self._timing.get("restart_duration", 0)
        if isinstance(restart, (int, float)) and restart > 0:
            self.logger.info(f"  {'restart_time':30s} {restart:10.1f}s ({restart/60:.1f}m)")
        nt_restart = self._timing.get("non_target_restart_duration", 0)
        if isinstance(nt_restart, (int, float)) and nt_restart > 0:
            self.logger.info(f"  {'non_target_restart':30s} {nt_restart:10.1f}s ({nt_restart/60:.1f}m)")
            nt_node = self._timing.get("non_target_restart_node", "?")
            nt_result = self._timing.get("non_target_restart_result", "?")
            self.logger.info(f"  {'non_target_restart_node':30s} {nt_node}")
            self.logger.info(f"  {'non_target_restart_result':30s} {nt_result}")
        if self._with_io_load:
            avg_w = self._timing.get("avg_write_bytes_ps", 0)
            avg_r = self._timing.get("avg_read_bytes_ps", 0)
            avg_wiops = self._timing.get("avg_write_iops", 0)
            avg_riops = self._timing.get("avg_read_iops", 0)
            self.logger.info(f"  {'avg_write_bw':30s} {avg_w/(1024*1024):10.1f} MiB/s")
            self.logger.info(f"  {'avg_read_bw':30s} {avg_r/(1024*1024):10.1f} MiB/s")
            self.logger.info(f"  {'avg_write_iops':30s} {avg_wiops:10.0f}")
            self.logger.info(f"  {'avg_read_iops':30s} {avg_riops:10.0f}")
        self.logger.info("-" * 70)
        # All timing entries
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
                      "failed_device_migration_duration",
                      "new_device_migration_duration",
                      "restart_duration",
                      "non_target_restart_duration"):
            if name in self._timing:
                phases.append({
                    "name": name.replace("_duration", ""),
                    "duration_sec": round(self._timing[name], 2),
                    "status": "ok",
                })

        report = {
            "test_class": self.__class__.__name__,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "passed" if self._test_passed else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "lvol_size": self.LVOL_SIZE,
                "with_io_load": self._with_io_load,
                "failure_mode": self._failure_mode,
                "fio_load_bs": self.FIO_LOAD_BS if self._with_io_load else None,
                "fio_load_rw": "randwrite" if self._with_io_load else None,
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
                    self._timing.get("failed_device_migration_duration",
                                     self._timing.get("restart_duration", 0)), 2
                ),
                "key_metric_label": "migration_duration_sec",
                "device_fill_pct": self._timing.get("device_fill_pct", 0),
            },
        }
        # Non-target restart details
        if "non_target_restart_duration" in self._timing:
            report["summary"]["non_target_restart_duration"] = round(
                self._timing["non_target_restart_duration"], 2
            )
            report["summary"]["non_target_restart_node"] = self._timing.get(
                "non_target_restart_node", ""
            )
            report["summary"]["non_target_restart_result"] = self._timing.get(
                "non_target_restart_result", ""
            )
        if self._with_io_load:
            report["summary"]["avg_write_bytes_ps"] = self._timing.get(
                "avg_write_bytes_ps", 0
            )
            report["summary"]["avg_read_bytes_ps"] = self._timing.get(
                "avg_read_bytes_ps", 0
            )
            report["summary"]["avg_write_iops"] = self._timing.get(
                "avg_write_iops", 0
            )
            report["summary"]["avg_read_iops"] = self._timing.get(
                "avg_read_iops", 0
            )
            report["summary"]["cluster_io_records"] = self._timing.get(
                "cluster_io_records", 0
            )

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
                          "failed_device_migration_duration",
                          "new_device_migration_duration",
                          "restart_duration",
                          "non_target_restart_duration"):
                if name in self._timing:
                    phase_names.append(name.replace("_duration", ""))
                    phase_durations.append(self._timing[name])

            if phase_durations:
                colors = ["#3498db", "#f39c12", "#e74c3c", "#9b59b6",
                          "#2ecc71", "#1abc9c", "#e67e22"]
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
                    f"Failure: {self._failure_mode}  |  "
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
            failed_mig = self._timing.get("failed_device_migration_duration", 0)
            new_mig = self._timing.get("new_device_migration_duration", 0)
            labels = []
            values = []
            colors = []
            if fill > 0:
                labels.append(f"Fill to {self.FILL_PERCENT}%")
                values.append(fill)
                colors.append("#f39c12")
            if failed_mig > 0:
                labels.append("Failed device migration")
                values.append(failed_mig)
                colors.append("#9b59b6")
            if new_mig > 0:
                labels.append("New device migration")
                values.append(new_mig)
                colors.append("#2ecc71")
            if values:
                fig, ax = plt.subplots(figsize=(8, 4))
                bars = ax.barh(labels, values,
                               color=colors, alpha=0.8,
                               height=0.5)
                max_val = max(values) if values else 1
                for b, v in zip(bars, values):
                    ax.text(b.get_width() + max_val * 0.02,
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

    # ── Test summary markdown ─────────────────────────────────────────────────

    def _write_test_summary_md(self):
        """Write a markdown summary with cluster/device details to logs/test_summary.md.

        Also appends to GITHUB_STEP_SUMMARY so it appears in the GitHub
        Actions run summary regardless of which workflow runs the test.
        """
        try:
            lines = self._build_summary_md_lines()
            summary_text = "\n".join(lines) + "\n"
            out_dir = Path("logs")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "test_summary.md"
            out_path.write_text(summary_text)
            self.logger.info(f"Test summary markdown written to {out_path}")

            # Append to GitHub Actions step summary if available
            gh_summary = os.environ.get("GITHUB_STEP_SUMMARY", "")
            if gh_summary:
                try:
                    with open(gh_summary, "a") as f:
                        f.write("\n" + summary_text)
                    self.logger.info("Test summary appended to GITHUB_STEP_SUMMARY")
                except Exception as gh_exc:
                    self.logger.warning(
                        f"Failed to write to GITHUB_STEP_SUMMARY: {gh_exc}"
                    )
        except Exception as exc:
            self.logger.warning(f"Failed to write test summary markdown: {exc}")

    def _build_summary_md_lines(self):
        """Build markdown lines for the test summary."""
        lines = []
        lines.append("# Device Migration Test Summary\n")

        # ── Test Info ──
        lines.append("## Test Info")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| Test class | `{self.__class__.__name__}` |")
        lines.append(f"| Failure mode | {self._failure_mode} |")
        if self._with_io_load:
            lines.append(f"| IO load | YES (bs={self.FIO_LOAD_BS}, rw=randwrite, verify=md5) |")
        else:
            lines.append("| IO load | NO (empty device, no fill) |")
        lines.append(f"| Result | **{'PASSED' if self._test_passed else 'FAILED'}** |")
        lines.append("")

        # ── Key Metrics ──
        lines.append("## Key Metrics")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        failed_mig = self._timing.get("failed_device_migration_duration", 0)
        if isinstance(failed_mig, (int, float)) and failed_mig > 0:
            lines.append(f"| Failed device migration | {failed_mig:.1f}s ({failed_mig/60:.1f}m) |")
        new_mig = self._timing.get("new_device_migration_duration", 0)
        if isinstance(new_mig, (int, float)) and new_mig > 0:
            lines.append(f"| New device migration | {new_mig:.1f}s ({new_mig/60:.1f}m) |")
        restart = self._timing.get("restart_duration", 0)
        if isinstance(restart, (int, float)) and restart > 0:
            lines.append(f"| Restart time | {restart:.1f}s ({restart/60:.1f}m) |")
        fill_pct = self._timing.get("device_fill_pct", 0)
        lines.append(f"| Device fill level | {fill_pct}% |")
        if self._with_io_load:
            avg_w = self._timing.get("avg_write_bytes_ps", 0)
            avg_r = self._timing.get("avg_read_bytes_ps", 0)
            avg_wiops = self._timing.get("avg_write_iops", 0)
            avg_riops = self._timing.get("avg_read_iops", 0)
            n_records = self._timing.get("cluster_io_records", 0)
            lines.append(f"| Avg write bandwidth | {avg_w/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg read bandwidth | {avg_r/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg write IOPS | {avg_wiops:.0f} |")
            lines.append(f"| Avg read IOPS | {avg_riops:.0f} |")
            lines.append(f"| IO stat records | {n_records} |")
        lines.append("")

        # ── Lvol Distribution ──
        lines.append("## Lvol Distribution")
        lines.append("| Node | # Lvols | Role |")
        lines.append("|------|---------|------|")
        other_nodes = [n for n in self._sn_nodes if n != self._target_node_id]
        lines.append(f"| `{self._target_node_id}` | {len(self._lvols_on_target)} | target |")
        others_per_node = max(1, len(self._lvols_on_others)) // max(1, len(other_nodes)) if other_nodes else 0
        for node_id in other_nodes:
            lines.append(f"| `{node_id}` | {others_per_node} | other |")
        lines.append("")

        lines.extend(self._build_capacity_md_lines())

        # ── Post-Migration Health ──
        health_ok = self._timing.get("post_migration_all_healthy", None)
        if health_ok is not None:
            lines.append("## Post-Migration Health (5m check)")
            icon = "PASS" if health_ok else "FAIL"
            lines.append(f"| Result | **{icon}** |")
            unhealthy = self._timing.get("post_migration_unhealthy_events", 0)
            if unhealthy:
                lines.append(f"| Unhealthy events | {unhealthy} |")
            lines.append("")

        # ── Non-Target Node Restart Validation ──
        nt_dur = self._timing.get("non_target_restart_duration")
        if nt_dur is not None:
            lines.append("## Non-Target Node Restart Validation")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            nt_node = self._timing.get("non_target_restart_node", "N/A")
            nt_result = self._timing.get("non_target_restart_result", "N/A")
            lines.append(f"| Restarted node | `{nt_node}` |")
            lines.append(f"| Duration | {nt_dur:.1f}s ({nt_dur/60:.1f}m) |")
            lines.append(f"| Result | **{nt_result}** |")
            lines.append("")

        # ── Phase Timings ──
        lines.append("## Phase Timings")
        lines.append("| Phase | Duration |")
        lines.append("|-------|----------|")
        for name in ("setup_duration", "fill_duration", "remove_duration",
                      "failed_device_migration_duration",
                      "new_device_migration_duration",
                      "restart_duration", "non_target_restart_duration",
                      "migration_duration",
                      "fio_completion_duration", "total_duration"):
            if name in self._timing:
                val = self._timing[name]
                label = name.replace("_duration", "").replace("_", " ")
                if isinstance(val, (int, float)):
                    lines.append(f"| {label} | {val:.1f}s ({val/60:.1f}m) |")
        lines.append("")

        return lines

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
#  Docker concrete test classes (sbcli + SSH FIO)
# ═══════════════════════════════════════════════════════════════════════════════

class DeviceFailureMigrationNoLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, fail it via API, run migration WITHOUT IO load.

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


class DeviceFailureMigrationUnderLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, start IO on all nodes, fail device via API, migrate UNDER LOAD.

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


class DeviceFailureMigrationPCIeNoLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, remove via PCIe sysfs, run migration WITHOUT IO load.

    Uses physical PCIe hot-unplug (/sys/bus/pci/devices/<addr>/remove) instead
    of the control-plane API.  After migration, rescans PCI bus to restore device.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_pcie_no_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_migration_test(with_io_load=False, failure_mode="pcie")


class DeviceFailureMigrationPCIeUnderLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, start IO, remove via PCIe sysfs, migrate UNDER LOAD.

    Uses physical PCIe hot-unplug (/sys/bus/pci/devices/<addr>/remove) instead
    of the control-plane API.  After migration, rescans PCI bus to restore device.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_pcie_under_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_migration_test(with_io_load=True, failure_mode="pcie")


class DevicePCIeRestartNoLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, PCIe hot-unplug, rescan, restart device, verify — NO IO load.

    Unlike the migration variants, this does NOT set-failed the device.
    Instead it rescans the PCI bus and calls restart-device to bring it back.
    Verifies device comes online, cluster healthy, no data loss.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_pcie_restart_no_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_restart_test(with_io_load=False)


class DevicePCIeRestartUnderLoadDocker(_DeviceFailureMigrationBase, TestLvolHACluster):
    """Fill device to 65 %, start IO, PCIe hot-unplug, rescan, restart device, verify — UNDER LOAD.

    Unlike the migration variants, this does NOT set-failed the device.
    Instead it rescans the PCI bus and calls restart-device to bring it back.
    Verifies device comes online, cluster healthy, FIO completed without errors.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_pcie_restart_under_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_restart_test(with_io_load=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  Device Add After Bootstrap — Docker (sbcli + SSH FIO)
# ═══════════════════════════════════════════════════════════════════════════════

class _DeviceAddAfterBootstrapBase:
    """Shared logic for "add device after bootstrap" tests.

    Scenario: cluster was bootstrapped with one node having a PCIe device
    disabled beforehand.  The test rescans PCI, restarts the node with the
    new PCIe address (so SPDK discovers the device), then calls
    ``sbctl sn add-device`` to bring it online and trigger rebalancing.
    """

    # ── Configuration ─────────────────────────────────────────────────────
    LVOL_SIZE = "50G"
    FIO_FILL_SIZE = "30G"
    FIO_FILL_BS = "512K"
    FIO_LOAD_BS = "128K"         # 128K random write for bandwidth measurement
    FIO_LOAD_IODEPTH = 1         # must be 1 when verify=md5
    FIO_LOAD_NUMJOBS = 2
    FIO_LOAD_RUNTIME = 7200      # 2 h ceiling
    MIGRATION_TIMEOUT = 3600     # 1 h max for add-device migration
    NUM_LVOLS = 10               # lvols to create on existing capacity
    DEFAULT_PCIE_ADDR = "0000:00:05.0"

    def _init_add_device_state(self):
        """Initialise per-test tracking state (call from __init__)."""
        self._timing = {}
        self._target_node_id = None
        self._target_node_ip = None
        self._disabled_pcie_addr = None
        self._new_device_id = None
        self._lvol_names = []
        self._load_fio_threads = []
        self._sn_nodes = []
        self._test_passed = False
        self._with_io_load = False

    # ── Target node detection ─────────────────────────────────────────────

    def _find_target_node(self):
        """Find the storage node that has fewer devices than the others.

        Checks ``DISABLED_PCIE_NODE_IP`` env var first; falls back to
        comparing device counts across all storage nodes.
        """
        env_ip = os.environ.get("DISABLED_PCIE_NODE_IP", "").strip()
        if env_ip:
            self.logger.info(
                f"DISABLED_PCIE_NODE_IP env var set: {env_ip}"
            )
            for node_id in self._sn_nodes:
                details = self.sbcli_utils.get_storage_node_details(node_id)
                if details and details[0].get("mgmt_ip") == env_ip:
                    self.logger.info(
                        f"Matched env IP {env_ip} to node {node_id}"
                    )
                    return node_id
            self.logger.warning(
                f"Could not match DISABLED_PCIE_NODE_IP={env_ip} to any "
                f"storage node — falling back to auto-detect"
            )

        # Fallback: find the node with fewer devices
        device_counts = {}
        for node_id in self._sn_nodes:
            devices = self.sbcli_utils.get_device_details(node_id)
            device_counts[node_id] = len(devices)
            self.logger.info(
                f"Node {node_id}: {len(devices)} device(s)"
            )

        if not device_counts:
            raise RuntimeError("No storage nodes found")

        max_count = max(device_counts.values())
        for node_id, count in device_counts.items():
            if count < max_count:
                self.logger.info(
                    f"Auto-detected target node {node_id} with "
                    f"{count} device(s) (others have {max_count})"
                )
                return node_id

        raise RuntimeError(
            "All nodes have the same device count — "
            "no disabled device detected. "
            f"Counts: {device_counts}"
        )

    def _get_disabled_pcie_addr(self):
        """Get the PCIe address of the disabled device."""
        env_addr = os.environ.get("DISABLED_PCIE_ADDR", "").strip()
        if env_addr:
            self.logger.info(f"DISABLED_PCIE_ADDR env var: {env_addr}")
            return env_addr
        self.logger.info(
            f"DISABLED_PCIE_ADDR not set — using default: "
            f"{self.DEFAULT_PCIE_ADDR}"
        )
        return self.DEFAULT_PCIE_ADDR

    # ── Main test flow ────────────────────────────────────────────────────

    def _run_add_device_test(self, with_io_load=False):
        """Main flow for the add-device-after-bootstrap test.

        NoLoad:  setup → PCI rescan → sn restart --ssd-pcie → add-device →
                 wait migration → validate (no fill, no IO — raw migration time)
        UnderLoad: setup → fill → log fill level → start FIO (128K randwrite) →
                   PCI rescan → sn restart → add-device → wait migration →
                   validate → stop FIO → collect bandwidth
        """
        self._with_io_load = with_io_load
        self._test_passed = False
        t0 = time.time()
        try:
            self._phase_discover_target()
            self._phase_setup_pool_and_lvols()
            if with_io_load:
                self._phase_fill_lvols()
                self._log_device_fill_level()
                self._phase_start_io_load()
            self._phase_rescan_and_restart()
            self._phase_add_new_device()
            self._phase_validate_cluster()
            if with_io_load:
                self._phase_restart_non_target_node()
                self._phase_stop_io_load()
            self._test_passed = True
        finally:
            # Collect IO stats BEFORE killing FIO so the cluster API
            # still has active IO counters to report.
            if with_io_load:
                try:
                    self._phase_collect_cluster_io_stats()
                except Exception as e:
                    self.logger.warning(f"IO stats collection failed: {e}")
            if with_io_load:
                try:
                    self._phase_kill_fio()
                except Exception as e:
                    self.logger.warning(f"Kill FIO failed: {e}")
            try:
                self._phase_post_migration_health_check()
            except Exception as e:
                self.logger.warning(f"Post-migration health check failed: {e}")
            try:
                self.collect_management_details(suffix="_pre_cleanup")
            except Exception as e:
                self.logger.warning(
                    f"collect_management_details failed: {e}"
                )
            try:
                self._capture_cluster_capacity()
            except Exception as e:
                self.logger.warning(f"Capacity capture failed: {e}")
            try:
                self._phase_cleanup()
            except Exception as e:
                self.logger.warning(f"Cleanup failed: {e}")
            self._timing["total_duration"] = time.time() - t0
            self._print_add_device_summary()
            self._write_timing_json()
            self._write_test_summary_md()

        self.logger.info("TEST CASE PASSED !!!")

    # ── Phase: discover target node ───────────────────────────────────────

    def _phase_discover_target(self):
        self.logger.info("=== Phase: Discover target node ===")
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for r in storage_nodes["results"]:
            self._sn_nodes.append(r["uuid"])

        if len(self._sn_nodes) < 2:
            raise RuntimeError(
                f"Need at least 2 storage nodes, found {len(self._sn_nodes)}"
            )

        self._target_node_id = self._find_target_node()
        node_details = self.sbcli_utils.get_storage_node_details(
            self._target_node_id
        )
        self._target_node_ip = node_details[0]["mgmt_ip"]
        self._disabled_pcie_addr = self._get_disabled_pcie_addr()

        # Verify target node has exactly 1 device
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        online_devices = [d for d in devices if d.get("status") == "online"]
        if len(online_devices) != 1:
            raise RuntimeError(
                f"Expected target node {self._target_node_id} to have "
                f"exactly 1 online device, but found {len(online_devices)}: "
                f"{[d['id'] for d in online_devices]}"
            )
        self.logger.info(
            f"Target node: {self._target_node_id} "
            f"(IP: {self._target_node_ip}), "
            f"1 online device: {online_devices[0]['id']}, "
            f"disabled PCIe: {self._disabled_pcie_addr}"
        )

    # ── Phase: create pool and lvols ──────────────────────────────────────

    def _phase_setup_pool_and_lvols(self):
        self.logger.info("=== Phase: Setup pool and lvols ===")
        t0 = time.time()
        client = self.fio_node[0]

        for i in range(self.NUM_LVOLS):
            name = f"add_dev_{_rand_seq(4)}_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=name,
                pool_name=self.pool_name,
                size=self.LVOL_SIZE,
                crypto=False,
            )
            connect_info = self.sbcli_utils.get_lvol_connect_str(name)
            device = self.ssh_obj.get_nvme_device(client, connect_info)
            mount = f"/mnt/add_dev_{i}"
            self.ssh_obj.exec_command(client, f"mkdir -p {mount}")
            self.ssh_obj.format_and_mount(client, device, mount)
            log_path = f"/tmp/fio_add_dev_{i}.log"
            self.lvol_mount_details[name] = {
                "Device": device,
                "Mount": mount,
                "Log": log_path,
                "Command": connect_info if isinstance(connect_info, list) else [connect_info],
                "Client": client,
            }
            self._lvol_names.append(name)

        self._timing["setup_duration"] = time.time() - t0
        self.logger.info(
            f"Setup complete: {len(self._lvol_names)} lvols "
            f"({self._timing['setup_duration']:.1f}s)"
        )

    # ── Phase: fill lvols with data ───────────────────────────────────────

    def _phase_fill_lvols(self):
        self.logger.info("=== Phase: Fill lvols with data ===")
        t0 = time.time()
        client = self.fio_node[0]
        threads = []

        for name in self._lvol_names:
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

        for t in threads:
            t.join(timeout=60)

        # Wait for FIO fill to complete
        self.common_utils.manage_fio_threads(
            node=client, threads=[], timeout=3600
        )

        self._timing["fill_duration"] = time.time() - t0
        self.logger.info(
            f"Fill complete ({self._timing['fill_duration']:.1f}s)"
        )

    # ── Phase: start IO load (under-load variant) ─────────────────────────

    def _phase_start_io_load(self):
        self.logger.info("=== Phase: Start IO load (128K randwrite) ===")
        client = self.fio_node[0]

        for name in self._lvol_names:
            info = self.lvol_mount_details.get(name)
            if not info:
                continue
            t = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, info["Mount"], info["Log"]),
                kwargs={
                    "name": f"load_{name}",
                    "rw": "randwrite",
                    "bs": self.FIO_LOAD_BS,
                    "size": "1G",
                    "runtime": self.FIO_LOAD_RUNTIME,
                    "iodepth": self.FIO_LOAD_IODEPTH,
                    "numjobs": self.FIO_LOAD_NUMJOBS,
                    "use_latency": False,
                    "verify": "md5",
                },
            )
            t.start()
            self._load_fio_threads.append(t)

        sleep_n_sec(15)
        self.logger.info(
            f"IO load started: {len(self._load_fio_threads)} FIO threads "
            f"(bs={self.FIO_LOAD_BS}, rw=randwrite, verify=md5)"
        )

    # ── Logging helpers ────────────────────────────────────────────────────

    def _log_device_fill_level(self):
        """Query and log device utilisation on the target node."""
        try:
            capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
            if isinstance(capacity, list):
                capacity = capacity[0] if capacity else {}
            size_total = capacity.get("size_total", 0)
            size_used = capacity.get("size_used", 0)
            size_util = capacity.get("size_util", 0)
            self._timing["device_fill_pct"] = size_util
            self._timing["device_size_total_bytes"] = size_total
            self._timing["device_size_used_bytes"] = size_used
            self.logger.info(
                f"Device fill level on node {self._target_node_id}: "
                f"{size_util}% used "
                f"({size_used} / {size_total} bytes)"
            )
        except Exception as exc:
            self.logger.warning(f"Could not query device fill level: {exc}")

    # ── Phase: PCI rescan + node restart with new PCIe ────────────────────

    def _phase_rescan_and_restart(self):
        self.logger.info(
            f"=== Phase: PCI rescan + restart node "
            f"{self._target_node_id} with --ssd-pcie "
            f"{self._disabled_pcie_addr} ==="
        )
        t0 = time.time()

        # Record existing device IDs before restart
        existing_devices = {
            d["id"]
            for d in self.sbcli_utils.get_device_details(self._target_node_id)
        }
        self.logger.info(
            f"Existing devices on target node: {existing_devices}"
        )

        # PCI rescan to make the removed PCIe device visible to OS
        self.logger.info(
            f"PCI rescan on {self._target_node_ip} ..."
        )
        self.ssh_obj.exec_command(
            self._target_node_ip,
            "echo 1 | sudo tee /sys/bus/pci/rescan"
        )
        sleep_n_sec(10)
        self.logger.info("PCI rescan complete")

        # Restart node with new PCIe address
        mgmt_ip = self.mgmt_nodes[0]
        cmd = (
            f"{self.base_cmd} -d sn restart "
            f"{self._target_node_id} "
            f"--ssd-pcie {self._disabled_pcie_addr}"
        )
        self.logger.info(f"Restarting node via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"sn restart result: {result}")

        # Wait for node to come back online
        self.logger.info("Waiting for node to come back online ...")
        self.sbcli_utils.wait_for_storage_node_status(
            self._target_node_id, "online", timeout=600
        )
        self._timing["restart_duration"] = time.time() - t0
        self.logger.info(
            f"Node back online ({self._timing['restart_duration']:.1f}s)"
        )

        # Discover new device
        sleep_n_sec(10)  # let device registration settle
        current_devices = {
            d["id"]
            for d in self.sbcli_utils.get_device_details(self._target_node_id)
        }
        new_devices = current_devices - existing_devices
        self.logger.info(
            f"Devices after restart: {current_devices}, "
            f"new device(s): {new_devices}"
        )
        if len(new_devices) != 1:
            raise RuntimeError(
                f"Expected 1 new device after restart, "
                f"got {len(new_devices)}: {new_devices}"
            )
        self._new_device_id = new_devices.pop()
        self.logger.info(f"New device discovered: {self._new_device_id}")

    # ── Phase: add new device and wait for migration ──────────────────────

    def _phase_add_new_device(self):
        self.logger.info(
            f"=== Phase: Add device {self._new_device_id} ==="
        )
        t0 = time.time()

        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} -d sn add-device {self._new_device_id}"
        self.logger.info(f"Adding device via CLI: {cmd}")
        result = self.ssh_obj.exec_command(mgmt_ip, cmd)
        self.logger.info(f"add-device result: {result}")
        sleep_n_sec(5)

        # Wait for device to come online
        self.logger.info("Waiting for new device to come online ...")
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "online", timeout=600,
            device_id=self._new_device_id,
        )

        # Wait for any new_device_migration tasks to complete
        self.logger.info("Waiting for new_device_migration tasks ...")
        try:
            self._wait_new_device_migration(
                self._new_device_id, timeout=self.MIGRATION_TIMEOUT
            )
        except TimeoutError:
            self.logger.warning(
                "new_device_migration did not complete within timeout"
            )

        self._timing["migration_duration"] = time.time() - t0
        self.logger.info(
            f"Add-device + migration complete "
            f"({self._timing['migration_duration']:.1f}s)"
        )

    def _wait_new_device_migration(self, new_device_id, timeout=3600):
        """Wait for all new_device_migration tasks for the target node to finish.

        The migration tasks use ``NodeID:<node-uuid>`` as ``target_id``,
        not the device UUID, so we filter by ``self._target_node_id``.
        We also wait up to 120 s for the first task to appear (the
        control-plane may need a few seconds to create migration tasks
        after ``add-device``).
        """
        node_id = self._target_node_id
        self.logger.info(
            f"Waiting for new_device_migration tasks for device "
            f"{new_device_id} (node {node_id}) ..."
        )
        start = time.time()
        seen_any = False
        grace_period = 120  # seconds to wait for tasks to appear
        while time.time() - start < timeout:
            try:
                tasks = self.sbcli_utils.list_migration_tasks(
                    self.sbcli_utils.cluster_id
                )
                matching = [
                    t for t in tasks.get("results", [])
                    if t.get("function_name") == "new_device_migration"
                    and node_id in str(t.get("target_id", ""))
                ]
                active = [
                    t for t in matching
                    if t.get("status") not in ("done", "cancelled", "error")
                ]
                if matching:
                    seen_any = True
                if seen_any and not active:
                    elapsed = time.time() - start
                    self.logger.info(
                        f"All new_device_migration tasks complete "
                        f"({len(matching)} total) in {elapsed:.1f}s"
                    )
                    return elapsed
                if not seen_any and time.time() - start > grace_period:
                    elapsed = time.time() - start
                    self.logger.warning(
                        f"No new_device_migration tasks appeared "
                        f"after {grace_period}s — returning"
                    )
                    return elapsed
                if active:
                    self.logger.info(
                        f"Waiting for {len(active)}/{len(matching)} "
                        f"new_device_migration task(s) ..."
                    )
                else:
                    self.logger.info(
                        "Waiting for new_device_migration tasks to "
                        "appear ..."
                    )
            except Exception as exc:
                self.logger.warning(
                    f"Error checking migration tasks: {exc}"
                )
            sleep_n_sec(15)
        raise TimeoutError(
            f"new_device_migration tasks for {new_device_id} did not "
            f"complete within {timeout}s"
        )

    # ── Phase: validate cluster health ────────────────────────────────────

    def _phase_validate_cluster(self):
        self.logger.info("=== Phase: Validate cluster health ===")

        # 1. All storage nodes online and healthy
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for node in storage_nodes["results"]:
            assert node["status"] == "online", (
                f"Node {node['id']} is not online (status={node['status']})"
            )
            assert node["health_check"], (
                f"Node {node['id']} health check failed"
            )
        self.logger.info(
            f"All {len(storage_nodes['results'])} storage nodes "
            f"online and healthy"
        )

        # 2. Target node now has 2 online devices
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        online_devices = [d for d in devices if d.get("status") == "online"]
        assert len(online_devices) == 2, (
            f"Expected 2 online devices on target node, "
            f"got {len(online_devices)}: "
            f"{[(d['id'], d.get('status')) for d in devices]}"
        )
        self.logger.info(
            f"Target node {self._target_node_id} has "
            f"{len(online_devices)} online devices — OK"
        )

        # 3. New device is online
        new_dev = next(
            (d for d in devices if d["id"] == self._new_device_id), None
        )
        assert new_dev is not None, (
            f"New device {self._new_device_id} not found in device list"
        )
        assert new_dev.get("status") == "online", (
            f"New device {self._new_device_id} status is "
            f"{new_dev.get('status')}, expected online"
        )
        self.logger.info(
            f"New device {self._new_device_id} is online — OK"
        )

    # ── Post-migration health check ─────────────────────────────────────────

    def _phase_post_migration_health_check(self):
        """Check all node statuses every 10s for 5 minutes after migration."""
        self.logger.info("=== Phase: Post-migration health check (5m, 10s intervals) ===")
        check_interval = 10
        check_duration = 300  # 5 minutes
        t0 = time.time()
        all_healthy = True
        unhealthy_records = []

        while time.time() - t0 < check_duration:
            try:
                storage_nodes = self.sbcli_utils.get_storage_nodes()
                statuses = {}
                for node in storage_nodes.get("results", []):
                    nid = node.get("id", "?")
                    status = node.get("status", "?")
                    health = node.get("health_check", "?")
                    statuses[nid] = {"status": status, "health": health}
                    if status != "online":
                        all_healthy = False
                        unhealthy_records.append({
                            "time": round(time.time() - t0, 1),
                            "node": nid,
                            "status": status,
                            "health": health,
                        })

                elapsed = time.time() - t0
                status_str = "  ".join(
                    f"{nid[:8]}={s['status']}(health={s['health']})"
                    for nid, s in statuses.items()
                )
                self.logger.info(
                    f"  [{elapsed:5.0f}s] {status_str}"
                )
            except Exception as exc:
                self.logger.warning(f"  Health check poll error: {exc}")

            sleep_n_sec(check_interval)

        self._timing["post_migration_all_healthy"] = all_healthy
        if unhealthy_records:
            self._timing["post_migration_unhealthy_events"] = len(unhealthy_records)
            self.logger.warning(
                f"Post-migration health check: {len(unhealthy_records)} unhealthy "
                f"event(s) in {check_duration}s"
            )
            for rec in unhealthy_records:
                self.logger.warning(
                    f"  t={rec['time']}s node={rec['node'][:8]} "
                    f"status={rec['status']} health={rec['health']}"
                )
        else:
            self.logger.info(
                "Post-migration health check: all nodes healthy for "
                f"{check_duration}s"
            )

    # ── Phase: stop IO load and collect bandwidth ─────────────────────────

    def _phase_stop_io_load(self):
        """Signal FIO to stop gracefully and wait for threads."""
        self.logger.info("=== Phase: Stop IO load ===")
        client = self.fio_node[0]
        # Send SIGTERM to fio for graceful shutdown
        self.ssh_obj.exec_command(client, "pkill -TERM -f fio || true")
        for t in self._load_fio_threads:
            t.join(timeout=120)
        self.logger.info("IO load stopped")

    def _phase_kill_fio(self):
        """Force-kill FIO (finally block cleanup)."""
        try:
            client = self.fio_node[0]
            self.ssh_obj.exec_command(client, "pkill -9 -f fio || true")
            for t in self._load_fio_threads:
                t.join(timeout=30)
        except Exception:
            pass

    def _phase_collect_cluster_io_stats(self):
        """Collect cluster-wide IO stats from the API and compute averages."""
        self.logger.info("=== Phase: Collect cluster IO stats ===")
        try:
            cluster_id = self.sbcli_utils.cluster_id
            total_elapsed = self._timing.get("total_duration", 0)
            if total_elapsed <= 0:
                total_elapsed = sum(
                    self._timing.get(k, 0) for k in (
                        "setup_duration", "fill_duration",
                        "restart_duration", "migration_duration",
                    )
                )
            minutes = max(5, int(total_elapsed / 60) + 2)
            time_duration = f"{minutes}m"
            self.logger.info(
                f"Fetching cluster IO stats for {cluster_id} "
                f"over last {time_duration}"
            )

            io_stats = self.sbcli_utils.get_io_stats(cluster_id, time_duration)
            self.logger.info(f"Got {len(io_stats) if io_stats else 0} IO stat records")

            if not io_stats:
                self.logger.warning("No cluster IO stats returned from API")
                return

            write_bps_vals = []
            read_bps_vals = []
            write_iops_vals = []
            read_iops_vals = []
            write_lat_vals = []
            read_lat_vals = []

            for stat in io_stats:
                w_bps = stat.get("write_bytes_ps", 0)
                r_bps = stat.get("read_bytes_ps", 0)
                w_iops = stat.get("write_io_ps", 0)
                r_iops = stat.get("read_io_ps", 0)
                w_lat = stat.get("write_latency_ps", 0)
                r_lat = stat.get("read_latency_ps", 0)
                if w_bps or r_bps:
                    write_bps_vals.append(w_bps)
                    read_bps_vals.append(r_bps)
                    write_iops_vals.append(w_iops)
                    read_iops_vals.append(r_iops)
                    write_lat_vals.append(w_lat)
                    read_lat_vals.append(r_lat)

            n = len(write_bps_vals)
            if n == 0:
                self.logger.warning("All cluster IO stat records have zero IO")
                return

            avg_write_bps = sum(write_bps_vals) / n
            avg_read_bps = sum(read_bps_vals) / n
            avg_write_iops = sum(write_iops_vals) / n
            avg_read_iops = sum(read_iops_vals) / n
            avg_write_lat = sum(write_lat_vals) / n
            avg_read_lat = sum(read_lat_vals) / n

            self._timing["cluster_io_records"] = n
            self._timing["avg_write_bytes_ps"] = round(avg_write_bps, 1)
            self._timing["avg_read_bytes_ps"] = round(avg_read_bps, 1)
            self._timing["avg_write_iops"] = round(avg_write_iops, 1)
            self._timing["avg_read_iops"] = round(avg_read_iops, 1)
            self._timing["avg_write_latency_ps"] = round(avg_write_lat, 3)
            self._timing["avg_read_latency_ps"] = round(avg_read_lat, 3)

            self.logger.info(
                f"Cluster IO stats ({n} records): "
                f"avg write={avg_write_bps/(1024*1024):.1f} MiB/s, "
                f"avg read={avg_read_bps/(1024*1024):.1f} MiB/s, "
                f"avg write IOPS={avg_write_iops:.0f}, "
                f"avg read IOPS={avg_read_iops:.0f}"
            )
        except Exception as exc:
            self.logger.warning(f"Could not collect cluster IO stats: {exc}")

    # ── Cleanup ───────────────────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Phase: Cleanup ===")
        try:
            client = self.fio_node[0]
            self.ssh_obj.exec_command(client, "pkill -f fio || true")
            sleep_n_sec(5)

            for name, info in self.lvol_mount_details.items():
                cl = info.get("Client", self.fio_node[0])
                try:
                    self.ssh_obj.unmount_path(cl, info["Device"])
                except Exception:
                    pass
                for cmd in info.get("Command", []):
                    nqn = None
                    for part in cmd.split():
                        if "nqn" in part.lower():
                            nqn = part.split("=")[-1] if "=" in part else part
                    if nqn:
                        self.ssh_obj.exec_command(
                            cl, f"nvme disconnect -n {nqn} || true"
                        )

            self.sbcli_utils.delete_all_clones()
            self.sbcli_utils.delete_all_snapshots()
            self.sbcli_utils.delete_all_lvols()
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")

    # ── Summary and timing ────────────────────────────────────────────────

    def _print_add_device_summary(self):
        self.logger.info("=" * 70)
        self.logger.info("  DEVICE ADD AFTER BOOTSTRAP SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"  Test class:       {self.__class__.__name__}")
        self.logger.info(f"  IO load:          {'YES' if self._with_io_load else 'NO (empty device)'}")
        self.logger.info(f"  FIO params:       bs={self.FIO_LOAD_BS} rw=randwrite verify=md5" if self._with_io_load else "  FIO params:       N/A")
        self.logger.info(f"  Target node:      {self._target_node_id}")
        self.logger.info(f"  Target node IP:   {self._target_node_ip}")
        self.logger.info(f"  Disabled PCIe:    {self._disabled_pcie_addr}")
        self.logger.info(f"  New device:       {self._new_device_id}")
        self.logger.info(f"  Lvols created:    {len(self._lvol_names)}")
        self.logger.info(f"  Result:           {'PASSED' if self._test_passed else 'FAILED'}")
        self.logger.info("-" * 70)
        # Key metrics
        fill_pct = self._timing.get("device_fill_pct", "0 (empty)")
        self.logger.info(f"  {'device_fill_pct':30s} {fill_pct}")
        mig = self._timing.get("migration_duration", 0)
        if isinstance(mig, (int, float)):
            self.logger.info(f"  {'migration_time':30s} {mig:10.1f}s ({mig/60:.1f}m)")
        restart = self._timing.get("restart_duration", 0)
        if isinstance(restart, (int, float)):
            self.logger.info(f"  {'restart_time':30s} {restart:10.1f}s ({restart/60:.1f}m)")
        if self._with_io_load:
            avg_w = self._timing.get("avg_write_bytes_ps", 0)
            avg_r = self._timing.get("avg_read_bytes_ps", 0)
            avg_wiops = self._timing.get("avg_write_iops", 0)
            avg_riops = self._timing.get("avg_read_iops", 0)
            self.logger.info(f"  {'avg_write_bw':30s} {avg_w/(1024*1024):10.1f} MiB/s")
            self.logger.info(f"  {'avg_read_bw':30s} {avg_r/(1024*1024):10.1f} MiB/s")
            self.logger.info(f"  {'avg_write_iops':30s} {avg_wiops:10.0f}")
            self.logger.info(f"  {'avg_read_iops':30s} {avg_riops:10.0f}")
        self.logger.info("-" * 70)
        # All timing entries
        for key, val in self._timing.items():
            if isinstance(val, float):
                self.logger.info(f"  {key:30s} {val:10.1f}s")
            else:
                self.logger.info(f"  {key:30s} {val}")
        self.logger.info("=" * 70)

    def _write_timing_json(self):
        """Write standardised timing JSON for monitoring suite aggregation."""
        phases = []
        for name in ("setup_duration", "fill_duration", "restart_duration",
                      "non_target_restart_duration",
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
            "status": "passed" if self._test_passed else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "with_io_load": self._with_io_load,
                "target_node": self._target_node_id,
                "disabled_pcie_addr": self._disabled_pcie_addr,
                "new_device_id": self._new_device_id,
                "num_lvols": len(self._lvol_names),
                "fio_load_bs": self.FIO_LOAD_BS if self._with_io_load else None,
                "fio_load_rw": "randwrite" if self._with_io_load else None,
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
                "device_fill_pct": self._timing.get("device_fill_pct", 0),
            },
        }
        # Non-target restart details
        if "non_target_restart_duration" in self._timing:
            report["summary"]["non_target_restart_duration"] = round(
                self._timing["non_target_restart_duration"], 2
            )
            report["summary"]["non_target_restart_node"] = self._timing.get(
                "non_target_restart_node", ""
            )
            report["summary"]["non_target_restart_result"] = self._timing.get(
                "non_target_restart_result", ""
            )
        if self._with_io_load:
            report["summary"]["avg_write_bytes_ps"] = self._timing.get(
                "avg_write_bytes_ps", 0
            )
            report["summary"]["avg_read_bytes_ps"] = self._timing.get(
                "avg_read_bytes_ps", 0
            )
            report["summary"]["avg_write_iops"] = self._timing.get(
                "avg_write_iops", 0
            )
            report["summary"]["avg_read_iops"] = self._timing.get(
                "avg_read_iops", 0
            )
            report["summary"]["cluster_io_records"] = self._timing.get(
                "cluster_io_records", 0
            )

        out_dir = Path("logs")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "device_add_after_bootstrap_timing.json"
        with open(out_path, "w") as f:
            json.dump(report, f, indent=2)
        self.logger.info(f"Timing JSON written to {out_path}")

    def _write_test_summary_md(self):
        """Write a markdown summary with cluster/device details to logs/test_summary.md.

        Also appends to GITHUB_STEP_SUMMARY so it appears in the GitHub
        Actions run summary regardless of which workflow runs the test.
        """
        try:
            lines = self._build_add_device_summary_md_lines()
            summary_text = "\n".join(lines) + "\n"
            out_dir = Path("logs")
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "test_summary.md"
            out_path.write_text(summary_text)
            self.logger.info(f"Test summary markdown written to {out_path}")

            # Append to GitHub Actions step summary if available
            gh_summary = os.environ.get("GITHUB_STEP_SUMMARY", "")
            if gh_summary:
                try:
                    with open(gh_summary, "a") as f:
                        f.write("\n" + summary_text)
                    self.logger.info("Test summary appended to GITHUB_STEP_SUMMARY")
                except Exception as gh_exc:
                    self.logger.warning(
                        f"Failed to write to GITHUB_STEP_SUMMARY: {gh_exc}"
                    )
        except Exception as exc:
            self.logger.warning(f"Failed to write test summary markdown: {exc}")

    def _build_add_device_summary_md_lines(self):
        """Build markdown lines for the add-device test summary."""
        lines = []
        lines.append("# Device Add After Bootstrap Test Summary\n")

        # ── Test Info ──
        lines.append("## Test Info")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| Test class | `{self.__class__.__name__}` |")
        if self._with_io_load:
            lines.append(f"| IO load | YES (bs={self.FIO_LOAD_BS}, rw=randwrite, verify=md5) |")
        else:
            lines.append("| IO load | NO (empty device, no fill) |")
        lines.append(f"| Target node | `{self._target_node_id}` |")
        lines.append(f"| Target node IP | {self._target_node_ip} |")
        lines.append(f"| Disabled PCIe | {self._disabled_pcie_addr} |")
        lines.append(f"| New device | `{self._new_device_id}` |")
        lines.append(f"| Result | **{'PASSED' if self._test_passed else 'FAILED'}** |")
        lines.append("")

        # ── Key Metrics ──
        lines.append("## Key Metrics")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        restart = self._timing.get("restart_duration", 0)
        if isinstance(restart, (int, float)):
            lines.append(f"| Restart time | {restart:.1f}s ({restart/60:.1f}m) |")
        mig = self._timing.get("migration_duration", 0)
        if isinstance(mig, (int, float)):
            lines.append(f"| Migration time | {mig:.1f}s ({mig/60:.1f}m) |")
        fill_pct = self._timing.get("device_fill_pct", 0)
        lines.append(f"| Device fill level | {fill_pct}% |")
        if self._with_io_load:
            avg_w = self._timing.get("avg_write_bytes_ps", 0)
            avg_r = self._timing.get("avg_read_bytes_ps", 0)
            avg_wiops = self._timing.get("avg_write_iops", 0)
            avg_riops = self._timing.get("avg_read_iops", 0)
            n_records = self._timing.get("cluster_io_records", 0)
            lines.append(f"| Avg write bandwidth | {avg_w/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg read bandwidth | {avg_r/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg write IOPS | {avg_wiops:.0f} |")
            lines.append(f"| Avg read IOPS | {avg_riops:.0f} |")
            lines.append(f"| IO stat records | {n_records} |")
        lines.append("")

        # ── Lvol Distribution ──
        lines.append("## Lvol Distribution")
        lines.append("| Node | # Lvols | Role |")
        lines.append("|------|---------|------|")
        for node_id in self._sn_nodes:
            if node_id == self._target_node_id:
                lines.append(f"| `{node_id}` | {len(self._lvol_names)} | target |")
            else:
                lines.append(f"| `{node_id}` | 0 | other |")
        lines.append("")

        lines.extend(self._build_capacity_md_lines())

        # ── Post-Migration Health ──
        health_ok = self._timing.get("post_migration_all_healthy", None)
        if health_ok is not None:
            lines.append("## Post-Migration Health (5m check)")
            icon = "PASS" if health_ok else "FAIL"
            lines.append(f"| Result | **{icon}** |")
            unhealthy = self._timing.get("post_migration_unhealthy_events", 0)
            if unhealthy:
                lines.append(f"| Unhealthy events | {unhealthy} |")
            lines.append("")

        # ── Non-Target Node Restart Validation ──
        nt_dur = self._timing.get("non_target_restart_duration")
        if nt_dur is not None:
            lines.append("## Non-Target Node Restart Validation")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            nt_node = self._timing.get("non_target_restart_node", "N/A")
            nt_result = self._timing.get("non_target_restart_result", "N/A")
            lines.append(f"| Restarted node | `{nt_node}` |")
            lines.append(f"| Duration | {nt_dur:.1f}s ({nt_dur/60:.1f}m) |")
            lines.append(f"| Result | **{nt_result}** |")
            lines.append("")

        # ── Phase Timings ──
        lines.append("## Phase Timings")
        lines.append("| Phase | Duration |")
        lines.append("|-------|----------|")
        for name in ("setup_duration", "fill_duration", "restart_duration",
                      "non_target_restart_duration",
                      "migration_duration", "total_duration"):
            if name in self._timing:
                val = self._timing[name]
                label = name.replace("_duration", "").replace("_", " ")
                if isinstance(val, (int, float)):
                    lines.append(f"| {label} | {val:.1f}s ({val/60:.1f}m) |")
        lines.append("")

        return lines


class DeviceAddAfterBootstrapDocker(_DeviceAddAfterBootstrapBase, TestLvolHACluster):
    """Add a new PCIe device after cluster bootstrap — WITHOUT IO load.

    The cluster is bootstrapped with one storage node having 1 PCIe device
    disabled.  This test rescans PCI, restarts the node to discover the
    device, then adds it via ``sbctl sn add-device`` and waits for
    rebalancing to complete.

    Measures: setup time, fill time, restart time, migration time.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_add_device_state()
        self.test_name = "device_add_after_bootstrap_no_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_add_device_test(with_io_load=False)


class DeviceAddAfterBootstrapUnderLoadDocker(_DeviceAddAfterBootstrapBase, TestLvolHACluster):
    """Add a new PCIe device after cluster bootstrap — UNDER IO LOAD.

    Same as DeviceAddAfterBootstrapDocker but runs concurrent FIO
    (128K randwrite, iodepth=1, verify=md5) during the restart + add-device
    + migration phases.  Reports write bandwidth alongside migration time.

    Measures: setup time, fill time, restart time, migration time,
              average write bandwidth (KiB/s).
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_add_device_state()
        self.test_name = "device_add_after_bootstrap_under_load"

    def run(self):
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self._run_add_device_test(with_io_load=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  K8s-native concrete test classes (PVC + FIO K8s Jobs)
# ═══════════════════════════════════════════════════════════════════════════════

from stress_test.continuous_k8s_native_failover import K8sNativeFailoverTest  # noqa: E402


class _DeviceFailureMigrationK8s(_DeviceFailureMigrationBase):
    """K8s-native overrides for setup, fill, IO load, and cleanup phases.

    Uses PVCs for storage provisioning and K8s FIO Jobs for workload
    generation instead of sbcli + SSH.

    The device failure and migration phases are identical to Docker
    (they operate at the control-plane / sysfs level, not the data path).
    """

    # K8s-specific sizing
    K8S_PVC_SIZE = "50Gi"
    K8S_FIO_FILL_SIZE = "45G"
    K8S_FIO_LOAD_SIZE = "1G"

    def _init_migration_state(self):
        super()._init_migration_state()
        self._pvc_details = {}     # pvc_name -> {job_name, configmap_name, node_id}
        self._fill_jobs = []       # (job_name, configmap_name) for fill FIO jobs
        self._load_jobs = []       # (job_name, configmap_name) for load FIO jobs

    # ── Phase 1 override: PVC-based setup ────────────────────────────────────

    def _phase_setup_pool_and_lvols(self):
        self.logger.info("=== Phase: Setup pool and PVCs (K8s) ===")
        t0 = time.time()

        # Get storage nodes
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for r in storage_nodes["results"]:
            self._sn_nodes.append(r["uuid"])
            self.node_vs_pvc[r["uuid"]] = []

        if len(self._sn_nodes) < 1:
            raise RuntimeError("No storage nodes found")

        # Pick target node and device
        self._target_node_id = self._sn_nodes[0]
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        if not devices:
            raise RuntimeError(
                f"No devices found on target node {self._target_node_id}"
            )
        # Record devices already in a non-online state from previous runs —
        # these will be ignored throughout the test (validation, recovery, etc.)
        for d in devices:
            if d.get("status") != "online":
                self._pre_existing_failed_devices.add(d["id"])
        if self._pre_existing_failed_devices:
            self.logger.info(
                f"Pre-existing non-online devices (will be ignored): "
                f"{self._pre_existing_failed_devices}"
            )

        # Filter for online devices only
        online_devices = [d for d in devices if d.get("status") == "online"]
        if not online_devices:
            raise RuntimeError(
                f"No online devices found on target node {self._target_node_id}. "
                f"Device statuses: {[d.get('status') for d in devices]}"
            )
        self._target_device_info = online_devices[0]
        self._target_device_id = online_devices[0]["id"]
        self.logger.info(
            f"Target node: {self._target_node_id}, "
            f"Target device: {self._target_device_id} "
            f"(selected from {len(online_devices)} online / {len(devices)} total devices)"
        )

        # Get node capacity to calculate how many PVCs to create
        capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
        if isinstance(capacity, list):
            capacity = capacity[0] if capacity else {}
        size_total_bytes = capacity.get("size_total", 0)
        if isinstance(size_total_bytes, str):
            size_total_bytes = self._parse_size(size_total_bytes)
        target_bytes = int(size_total_bytes * self.FILL_PERCENT / 100)
        lvol_bytes = self._parse_size(self.LVOL_SIZE)
        num_lvols = max(1, math.ceil(target_bytes / lvol_bytes))
        self.logger.info(
            f"Node capacity: {size_total_bytes} bytes, "
            f"target fill: {target_bytes} bytes, "
            f"creating {num_lvols} PVCs of {self.K8S_PVC_SIZE}"
        )

        # Create PVCs pinned to target node
        for i in range(num_lvols):
            pvc_name = f"mig-target-{_rand_seq(4)}-{i}"
            self._create_pvc(pvc_name, self._target_node_id)
            self._lvols_on_target.append(pvc_name)

        # Create 1 PVC per OTHER node (for IO load variant)
        other_nodes = [n for n in self._sn_nodes if n != self._target_node_id]
        for idx, node_id in enumerate(other_nodes):
            pvc_name = f"mig-other-{_rand_seq(4)}-{idx}"
            self._create_pvc(pvc_name, node_id)
            self._lvols_on_others.append(pvc_name)

        self._timing["setup_duration"] = time.time() - t0
        self.logger.info(
            f"Setup complete: {len(self._lvols_on_target)} target PVCs, "
            f"{len(self._lvols_on_others)} other PVCs "
            f"({self._timing['setup_duration']:.1f}s)"
        )

    def _create_pvc(self, pvc_name, node_id):
        """Create a PVC pinned to a specific storage node."""
        self.k8s_utils.create_pvc(
            pvc_name, self.K8S_PVC_SIZE, self.STORAGE_CLASS_NAME,
            node_id=node_id,
        )
        self.k8s_utils.wait_pvc_bound(pvc_name, timeout=300)
        sleep_n_sec(2)

        node_id_actual = self._get_pvc_node_id(pvc_name) or node_id
        self._pvc_details[pvc_name] = {
            "job_name": None,
            "configmap_name": None,
            "node_id": node_id_actual,
        }
        self.node_vs_pvc.setdefault(node_id_actual, []).append(pvc_name)
        self.logger.info(f"PVC {pvc_name} created and bound (node={node_id_actual})")

    # ── Phase 2 override: fill via K8s FIO Jobs ──────────────────────────────

    def _phase_fill_devices(self):
        self.logger.info(
            f"=== Phase: Fill target device to {self.FILL_PERCENT}% (K8s FIO Jobs) ==="
        )
        t0 = time.time()

        # Create fill FIO jobs for target PVCs
        for pvc_name in self._lvols_on_target:
            job_name = f"fio-fill-{pvc_name}"
            cm_name = f"fiocfg-fill-{pvc_name}"
            run_id = _rand_seq(6)

            fio_config = (
                f"[global]\n"
                f"name=fill-{pvc_name}\n"
                f"filename_format=/spdkvol/fio-fill-{run_id}.$jobnum\n"
                f"rw=write\n"
                f"bs={self.FIO_FILL_BS}\n"
                f"iodepth=1\n"
                f"direct=1\n"
                f"ioengine=libaio\n"
                f"size={self.K8S_FIO_FILL_SIZE}\n"
                f"numjobs=1\n"
                f"group_reporting\n"
                f"\n"
                f"[job1]\n"
            )

            try:
                self.k8s_utils.create_fio_job(
                    job_name, pvc_name, cm_name, fio_config,
                    image=self.FIO_IMAGE,
                )
                self._fill_jobs.append((job_name, cm_name))
                self.logger.info(f"Fill FIO job {job_name} created for {pvc_name}")
            except Exception as exc:
                self.logger.error(f"Fill FIO job failed for {pvc_name}: {exc}")

        # Wait for fill jobs to complete
        self.logger.info(f"Waiting for {len(self._fill_jobs)} fill jobs to complete ...")
        for job_name, _ in self._fill_jobs:
            try:
                self.k8s_utils.wait_job_complete(job_name, timeout=3600)
                self.logger.info(f"Fill job {job_name} completed")
            except Exception as exc:
                self.logger.warning(f"Fill job {job_name} did not complete: {exc}")

        # Verify fill level
        sleep_n_sec(5)
        capacity = self.sbcli_utils.get_node_capacity(self._target_node_id)
        if isinstance(capacity, list):
            capacity = capacity[0] if capacity else {}
        util = capacity.get("size_util", 0)
        self.logger.info(f"Post-fill device utilisation: {util}%")

        # Cleanup fill jobs
        for job_name, cm_name in self._fill_jobs:
            try:
                self.k8s_utils.delete_resource("job", job_name)
                self.k8s_utils.delete_resource("configmap", cm_name)
            except Exception:
                pass

        self._timing["fill_duration"] = time.time() - t0
        self.logger.info(
            f"Fill complete ({self._timing['fill_duration']:.1f}s)"
        )

    # ── Phase 3 override: IO load via K8s FIO Jobs ───────────────────────────

    def _phase_start_io_load(self):
        self.logger.info("=== Phase: Start IO load on all nodes (K8s FIO Jobs) ===")
        all_pvc_names = self._lvols_on_target + self._lvols_on_others

        for pvc_name in all_pvc_names:
            job_name = f"fio-load-{pvc_name}"
            cm_name = f"fiocfg-load-{pvc_name}"
            run_id = _rand_seq(6)

            fio_config = (
                f"[global]\n"
                f"name=load-{pvc_name}\n"
                f"filename_format=/spdkvol/fio-load-{run_id}.$jobnum\n"
                f"rw=randwrite\n"
                f"bs={self.FIO_LOAD_BS}\n"
                f"iodepth={self.FIO_LOAD_IODEPTH}\n"
                f"direct=1\n"
                f"ioengine=libaio\n"
                f"size={self.K8S_FIO_LOAD_SIZE}\n"
                f"numjobs={self.FIO_LOAD_NUMJOBS}\n"
                f"time_based\n"
                f"runtime={self.FIO_LOAD_RUNTIME}\n"
                f"verify=md5\n"
                f"verify_dump=1\n"
                f"verify_fatal=1\n"
                f"verify_backlog=4096\n"
                f"group_reporting\n"
                f"\n"
                f"[job1]\n"
            )

            try:
                node_id = self._pvc_details.get(pvc_name, {}).get("node_id")
                avoid = (
                    self._get_k8s_node_for_storage_node(node_id)
                    if node_id else None
                )
                self.k8s_utils.create_fio_job(
                    job_name, pvc_name, cm_name, fio_config,
                    image=self.FIO_IMAGE,
                    avoid_node=avoid,
                )
                self._load_jobs.append((job_name, cm_name))
                self._pvc_details[pvc_name]["job_name"] = job_name
                self._pvc_details[pvc_name]["configmap_name"] = cm_name
                self.logger.info(f"Load FIO job {job_name} created for {pvc_name}")
            except Exception as exc:
                self.logger.error(f"Load FIO job failed for {pvc_name}: {exc}")

        sleep_n_sec(15)  # let IO ramp up
        self.logger.info(
            f"IO load started: {len(self._load_jobs)} FIO jobs"
        )

    # ── Phase 2b override: checksums via K8s utility pods ───────────────────

    def _phase_compute_checksums(self):
        """Compute MD5 checksums via utility pods on target PVCs."""
        self.logger.info("=== Phase: Compute pre-migration checksums (K8s) ===")
        self._pre_migration_checksums = {}
        self._checksum_utility_pods = []

        for pvc_name in self._lvols_on_target:
            pod_name = f"cksum-pre-{pvc_name}"
            try:
                self.k8s_utils.create_utility_pod(pod_name, pvc_name)
                self._checksum_utility_pods.append(pod_name)
                self.k8s_utils.wait_pod_running(pod_name)
                files = self.k8s_utils.find_files_in_pvc(pod_name)
                if files:
                    checksums = self.k8s_utils.generate_checksums_in_pvc(
                        pod_name, files
                    )
                    self._pre_migration_checksums[pvc_name] = checksums
                    self.logger.info(
                        f"Captured {len(checksums)} file checksums for {pvc_name}"
                    )
                else:
                    self.logger.warning(
                        f"No files found in PVC {pvc_name} for checksum"
                    )
            except Exception as exc:
                self.logger.warning(
                    f"Checksum capture failed for {pvc_name}: {exc}"
                )
            finally:
                try:
                    self.k8s_utils.delete_pod(pod_name)
                except Exception:
                    pass

        self.logger.info(
            f"Pre-migration checksums captured for "
            f"{len(self._pre_migration_checksums)} PVCs"
        )

    def _phase_verify_checksums(self):
        """Verify MD5 checksums via utility pods on target PVCs."""
        self.logger.info("=== Verifying post-migration data integrity (K8s) ===")
        mismatches = 0

        for pvc_name, expected in self._pre_migration_checksums.items():
            pod_name = f"cksum-post-{pvc_name}"
            try:
                self.k8s_utils.create_utility_pod(pod_name, pvc_name)
                self.k8s_utils.wait_pod_running(pod_name)
                actual = self.k8s_utils.generate_checksums_in_pvc(
                    pod_name,
                    self.k8s_utils.find_files_in_pvc(pod_name),
                )
                # Compare by filename (basename)
                expected_by_name = {
                    os.path.basename(k): v for k, v in expected.items()
                }
                actual_by_name = {
                    os.path.basename(k): v for k, v in actual.items()
                }
                for fname, cksum in expected_by_name.items():
                    if fname not in actual_by_name:
                        self.logger.error(
                            f"File {fname} missing in PVC {pvc_name} after migration"
                        )
                        mismatches += 1
                    elif actual_by_name[fname] != cksum:
                        self.logger.error(
                            f"Checksum MISMATCH for {fname} in {pvc_name}: "
                            f"expected {cksum}, got {actual_by_name[fname]}"
                        )
                        mismatches += 1
                    else:
                        self.logger.info(f"Checksum OK: {fname} in {pvc_name}")
            except Exception as exc:
                self.logger.error(
                    f"Checksum verification error for {pvc_name}: {exc}"
                )
                mismatches += 1
            finally:
                try:
                    self.k8s_utils.delete_pod(pod_name)
                except Exception:
                    pass

        assert mismatches == 0, (
            f"Data integrity check failed: {mismatches} file(s) had "
            f"checksum mismatches after migration"
        )
        self.logger.info(
            "All post-migration checksums verified — data integrity OK"
        )

    def _phase_validate_fio(self):
        """Check FIO K8s Job status and pod logs for errors."""
        self.logger.info("=== Verifying FIO jobs for errors (K8s) ===")
        target_errors = []
        other_errors = []

        for job_name, _ in self._load_jobs:
            # Determine if this job is on a target or other PVC
            pvc_name = job_name.replace("fio-load-", "", 1)
            is_target = pvc_name in self._lvols_on_target
            try:
                pod_name = self.k8s_utils.get_job_pod_name(job_name)
                if not pod_name:
                    self.logger.warning(
                        f"Could not find pod for FIO job {job_name}"
                    )
                    continue
                logs = self.k8s_utils.get_pod_logs(pod_name, tail=500)
                fail_words = ["error", "fail", "interrupt", "terminate"]
                logs_lower = logs.lower() if logs else ""
                found = [w for w in fail_words if w in logs_lower]
                if found:
                    msg = f"{job_name} ({pvc_name}): pod logs contain {found}"
                    if is_target:
                        target_errors.append(msg)
                        self.logger.warning(
                            f"[expected] FIO error on failed-device PVC "
                            f"{pvc_name}: {found}"
                        )
                    else:
                        other_errors.append(msg)
                        self.logger.error(
                            f"FIO error on non-target PVC {pvc_name}: {found}"
                        )
                else:
                    self.logger.info(f"FIO job {job_name}: no errors")
            except Exception as exc:
                self.logger.warning(
                    f"Could not check FIO job {job_name}: {exc}"
                )

        if target_errors:
            self.logger.warning(
                f"{len(target_errors)} FIO error(s) on target-device PVCs "
                f"(expected during device migration)"
            )
        if other_errors:
            self.logger.error(
                f"{len(other_errors)} FIO error(s) on non-target PVCs: "
                f"{other_errors}"
            )

    # ── Phase: wait for FIO to complete naturally (K8s) ─────────────────────

    def _phase_wait_fio_completion(self):
        """Wait for FIO K8s Jobs to complete naturally."""
        self.logger.info(
            "=== Phase: Waiting for FIO K8s Jobs to complete naturally ==="
        )
        t0 = time.time()
        fio_timeout = self.FIO_LOAD_RUNTIME + 300

        for job_name, _ in self._load_jobs:
            try:
                status = self.k8s_utils.wait_job_complete(
                    job_name, timeout=fio_timeout
                )
                self.logger.info(
                    f"FIO job {job_name} completed: {status}"
                )
            except Exception as exc:
                self.logger.warning(
                    f"FIO job {job_name} did not complete: {exc}"
                )

        elapsed = time.time() - t0
        self._timing["fio_completion_duration"] = elapsed
        self.logger.info(
            f"All FIO jobs finished ({elapsed:.1f}s)"
        )

    # ── K8s overrides for non-target restart FIO verification ───────────────

    def _count_active_fio(self):
        """Count active FIO K8s Jobs."""
        active = 0
        for job_name, _ in self._load_jobs:
            try:
                out = self.k8s_utils._exec_kubectl(
                    f"get job {job_name} "
                    f"-o jsonpath='{{.status.active}}'"
                )
                n = int(out.strip() or "0")
                active += n
            except Exception:
                pass
        return active

    def _verify_fio_still_running(self):
        """Verify FIO K8s Jobs are still active after non-target node restart.

        Raises RuntimeError if any job failed.  Jobs that completed
        naturally (succeeded) are acceptable — FIO may have finished
        within its runtime window.
        """
        active = 0
        succeeded = 0
        failed = 0
        for job_name, _ in self._load_jobs:
            try:
                out = self.k8s_utils._exec_kubectl(
                    f"get job {job_name} "
                    f"-o jsonpath='{{.status.active}} {{.status.succeeded}} {{.status.failed}}'"
                )
                parts = (out.strip() or "0 0 0").split()
                a = int(parts[0]) if len(parts) > 0 and parts[0] else 0
                s = int(parts[1]) if len(parts) > 1 and parts[1] else 0
                f = int(parts[2]) if len(parts) > 2 and parts[2] else 0
                active += a
                succeeded += s
                failed += f
            except Exception as exc:
                self.logger.warning(
                    f"Could not check job {job_name}: {exc}"
                )

        self.logger.info(
            f"FIO jobs after restart: active={active} "
            f"succeeded={succeeded} failed={failed}"
        )
        if failed > 0:
            raise RuntimeError(
                f"{failed} FIO K8s Job(s) failed after non-target node "
                f"restart — IO was interrupted during storage node restart"
            )
        if active == 0 and succeeded == 0:
            raise RuntimeError(
                "No active or succeeded FIO K8s Jobs found after "
                "non-target node restart"
            )

    # ── Phase 5 override: stop IO load (K8s) ─────────────────────────────────

    def _phase_stop_io_load(self):
        """Delete remaining FIO jobs (failure path only)."""
        self.logger.info("=== Phase: Stop IO load (K8s cleanup) ===")
        for job_name, cm_name in self._load_jobs:
            try:
                self.k8s_utils.delete_resource("job", job_name)
                self.k8s_utils.delete_resource("configmap", cm_name)
            except Exception:
                pass
        self.logger.info("IO load stopped (K8s jobs deleted)")

    # ── Cleanup override (K8s) ───────────────────────────────────────────────

    def _phase_cleanup(self):
        self.logger.info("=== Phase: Cleanup (K8s) ===")
        try:
            # Delete all FIO jobs and configmaps
            for job_name, cm_name in self._fill_jobs + self._load_jobs:
                try:
                    self.k8s_utils.delete_resource("job", job_name)
                    self.k8s_utils.delete_resource("configmap", cm_name)
                except Exception:
                    pass

            # Delete PVCs
            all_pvcs = self._lvols_on_target + self._lvols_on_others
            for pvc_name in all_pvcs:
                try:
                    self.k8s_utils.delete_pvc(pvc_name)
                except Exception:
                    pass
            sleep_n_sec(10)

            # Delete storage pool
            self.sbcli_utils.delete_all_storage_pools()
        except Exception as e:
            self.logger.error(f"Cleanup error: {e}")

    # ── Override: lvol distribution uses node_vs_pvc ──────────────────────────

    def _build_summary_md_lines(self):
        """Build markdown lines — uses node_vs_pvc for accurate K8s PVC counts."""
        lines = []
        lines.append("# Device Migration Test Summary (K8s)\n")

        # ── Test Info ──
        lines.append("## Test Info")
        lines.append("| Field | Value |")
        lines.append("|-------|-------|")
        lines.append(f"| Test class | `{self.__class__.__name__}` |")
        lines.append(f"| Failure mode | {self._failure_mode} |")
        if self._with_io_load:
            lines.append(f"| IO load | YES (bs={self.FIO_LOAD_BS}, rw=randwrite, verify=md5) |")
        else:
            lines.append("| IO load | NO (empty device, no fill) |")
        lines.append(f"| Result | **{'PASSED' if self._test_passed else 'FAILED'}** |")
        lines.append("")

        # ── Key Metrics ──
        lines.append("## Key Metrics")
        lines.append("| Metric | Value |")
        lines.append("|--------|-------|")
        failed_mig = self._timing.get("failed_device_migration_duration", 0)
        if isinstance(failed_mig, (int, float)) and failed_mig > 0:
            lines.append(f"| Failed device migration | {failed_mig:.1f}s ({failed_mig/60:.1f}m) |")
        new_mig = self._timing.get("new_device_migration_duration", 0)
        if isinstance(new_mig, (int, float)) and new_mig > 0:
            lines.append(f"| New device migration | {new_mig:.1f}s ({new_mig/60:.1f}m) |")
        restart = self._timing.get("restart_duration", 0)
        if isinstance(restart, (int, float)) and restart > 0:
            lines.append(f"| Restart time | {restart:.1f}s ({restart/60:.1f}m) |")
        fill_pct = self._timing.get("device_fill_pct", 0)
        lines.append(f"| Device fill level | {fill_pct}% |")
        if self._with_io_load:
            avg_w = self._timing.get("avg_write_bytes_ps", 0)
            avg_r = self._timing.get("avg_read_bytes_ps", 0)
            avg_wiops = self._timing.get("avg_write_iops", 0)
            avg_riops = self._timing.get("avg_read_iops", 0)
            n_records = self._timing.get("cluster_io_records", 0)
            lines.append(f"| Avg write bandwidth | {avg_w/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg read bandwidth | {avg_r/(1024*1024):.1f} MiB/s |")
            lines.append(f"| Avg write IOPS | {avg_wiops:.0f} |")
            lines.append(f"| Avg read IOPS | {avg_riops:.0f} |")
            lines.append(f"| IO stat records | {n_records} |")
        lines.append("")

        # ── Lvol Distribution (K8s: uses node_vs_pvc) ──
        lines.append("## Lvol Distribution")
        lines.append("| Node | # PVCs | Role |")
        lines.append("|------|--------|------|")
        for node_id in self._sn_nodes:
            pvcs = self.node_vs_pvc.get(node_id, [])
            role = "target" if node_id == self._target_node_id else "other"
            lines.append(f"| `{node_id}` | {len(pvcs)} | {role} |")
        lines.append("")

        lines.extend(self._build_capacity_md_lines())

        # ── Post-Migration Health ──
        health_ok = self._timing.get("post_migration_all_healthy", None)
        if health_ok is not None:
            lines.append("## Post-Migration Health (5m check)")
            icon = "PASS" if health_ok else "FAIL"
            lines.append(f"| Result | **{icon}** |")
            unhealthy = self._timing.get("post_migration_unhealthy_events", 0)
            if unhealthy:
                lines.append(f"| Unhealthy events | {unhealthy} |")
            lines.append("")

        # ── Non-Target Node Restart Validation ──
        nt_dur = self._timing.get("non_target_restart_duration")
        if nt_dur is not None:
            lines.append("## Non-Target Node Restart Validation")
            lines.append("| Field | Value |")
            lines.append("|-------|-------|")
            nt_node = self._timing.get("non_target_restart_node", "N/A")
            nt_result = self._timing.get("non_target_restart_result", "N/A")
            lines.append(f"| Restarted node | `{nt_node}` |")
            lines.append(f"| Duration | {nt_dur:.1f}s ({nt_dur/60:.1f}m) |")
            lines.append(f"| Result | **{nt_result}** |")
            lines.append("")

        # ── Phase Timings ──
        lines.append("## Phase Timings")
        lines.append("| Phase | Duration |")
        lines.append("|-------|----------|")
        for name in ("setup_duration", "fill_duration", "remove_duration",
                      "failed_device_migration_duration",
                      "new_device_migration_duration",
                      "restart_duration", "non_target_restart_duration",
                      "migration_duration",
                      "fio_completion_duration", "total_duration"):
            if name in self._timing:
                val = self._timing[name]
                label = name.replace("_duration", "").replace("_", " ")
                if isinstance(val, (int, float)):
                    lines.append(f"| {label} | {val:.1f}s ({val/60:.1f}m) |")
        lines.append("")

        return lines


# ── K8s concrete classes ─────────────────────────────────────────────────────

class DeviceFailureMigrationNoLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, fail via API, run migration WITHOUT IO load."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_no_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_migration_test(with_io_load=False, failure_mode="api")


class DeviceFailureMigrationUnderLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, start IO, fail via API, migrate UNDER LOAD."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_under_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_migration_test(with_io_load=True, failure_mode="api")


class DeviceFailureMigrationPCIeNoLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, remove via PCIe sysfs, migrate WITHOUT IO load."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_pcie_no_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_migration_test(with_io_load=False, failure_mode="pcie")


class DeviceFailureMigrationPCIeUnderLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, start IO, remove via PCIe sysfs, migrate UNDER LOAD."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_failure_migration_pcie_under_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_migration_test(with_io_load=True, failure_mode="pcie")


class DevicePCIeRestartNoLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, PCIe hot-unplug, rescan, restart device — NO IO load."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_pcie_restart_no_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_restart_test(with_io_load=False)


class DevicePCIeRestartUnderLoadK8s(_DeviceFailureMigrationK8s, K8sNativeFailoverTest):
    """K8s-native: fill device to 65 %, start IO, PCIe hot-unplug, rescan, restart device — UNDER LOAD."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self._init_migration_state()
        self.test_name = "device_pcie_restart_under_load_k8s"

    def run(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes["results"]:
            self.sn_nodes.append(result["uuid"])
            self.node_vs_pvc[result["uuid"]] = []

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
        self._run_restart_test(with_io_load=True)
