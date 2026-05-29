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

Failure modes:
  - "api"  : Logical removal via REST API + set-failed-device CLI
  - "pcie" : Physical removal via /sys/bus/pci/devices/<addr>/remove

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


# ═══════════════════════════════════════════════════════════════════════════════
#  Mixin — shared orchestration for all variants
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
        self._failure_mode = "api"
        self._pre_migration_checksums = {}  # {lvol_name: {filepath: md5}}

    # ── Main flow ────────────────────────────────────────────────────────────

    def _run_migration_test(self, with_io_load=False, failure_mode="api"):
        """Main flow: setup -> fill -> [checksum] -> [start IO] -> fail -> migrate -> validate -> recover -> cleanup.

        NoLoad:  fill → md5sum → fail device → migrate → verify md5 + FIO fill logs → recover device → cleanup
        UnderLoad: fill → start FIO (verify=md5) → fail device → migrate → check FIO OK → wait FIO complete → recover → cleanup
        """
        self._with_io_load = with_io_load
        self._failure_mode = failure_mode
        self._test_passed = False
        t0 = time.time()
        try:
            self._phase_setup_pool_and_lvols()
            self._phase_fill_devices()
            if not with_io_load:
                self._phase_compute_checksums()
            if with_io_load:
                self._phase_start_io_load()
            if failure_mode == "pcie":
                self._phase_fail_and_migrate_pcie()
            else:
                self._phase_fail_and_migrate()
            self._phase_validate()
            if with_io_load:
                # Wait for FIO to finish naturally — do NOT kill it
                self._phase_wait_fio_completion()
                self._phase_validate_fio()
            self._test_passed = True
        finally:
            if with_io_load:
                self._phase_stop_io_load()  # kill FIO only if still running (failure path)
            self._phase_recover_device()
            self._phase_cleanup()
            self._timing["total_duration"] = time.time() - t0
            self._print_migration_summary()
            self._write_timing_json()
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
        # Filter for online devices only — old failed_and_migrated devices
        # remain in the list after recovery and must be skipped
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

        # Wait for FIO launch threads to return (they return after verifying
        # FIO is running in tmux, but FIO itself is still writing)
        for t in threads:
            t.join(timeout=60)

        # Wait for actual FIO processes to finish on the remote node
        self.logger.info("Waiting for FIO fill processes to complete on remote node ...")
        self.common_utils.manage_fio_threads(
            node=client, threads=[], timeout=3600
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
        cmd = f"{self.base_cmd} sn set-failed-device {self._target_device_id}"
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

        # Step 3: Wait for control plane to detect device loss
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "unavailable", timeout=120,
            device_id=self._target_device_id,
        )
        self._timing["remove_duration"] = time.time() - t0
        self.logger.info(
            f"Device detected as unavailable ({self._timing['remove_duration']:.1f}s)"
        )

        # Step 4: Logical remove + set-failed to trigger migration
        t1 = time.time()
        self.sbcli_utils.remove_device(self._target_device_id)
        self.sbcli_utils.wait_for_device_status(
            self._target_node_id, "removed", timeout=120,
            device_id=self._target_device_id,
        )

        mgmt_ip = self.mgmt_nodes[0]
        cmd = f"{self.base_cmd} sn set-failed-device {self._target_device_id}"
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

        self._timing["migration_duration"] = time.time() - t_start
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
            f"(migration took {self._timing['migration_duration']:.1f}s)"
        )
        self._timing["device_final_status"] = final_status

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

        # 3. Other devices on target node should still be online
        devices = self.sbcli_utils.get_device_details(self._target_node_id)
        for d in devices:
            if d["id"] == self._target_device_id:
                continue
            assert d["status"] == "online", (
                f"Non-target device {d['id']} on target node has "
                f"unexpected status: {d['status']}"
            )
        self.logger.info("All non-target devices remain online")

        # 4. Data integrity checks (NoLoad only — UnderLoad is checked after FIO completes)
        if not self._with_io_load:
            self._phase_verify_checksums()

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

    # ── Phase: recover failed device ─────────────────────────────────────────

    def _phase_recover_device(self):
        """Create a new device from the failed one and add it back.

        Runs in the finally block so it executes even if the test fails.

        Steps:
          1. ``sbctl sn new-device-from-failed <failed_device_id>`` → new device ID
          2. ``sbctl sn add-device <new_device_id>``
          3. Wait for ``new_device_migration`` tasks to complete
        """
        if not self._target_device_id:
            return
        self.logger.info(
            f"=== Phase: Recover device {self._target_device_id} ==="
        )
        mgmt_ip = self.mgmt_nodes[0]

        # Step 1: create new device from failed device
        try:
            cmd = (
                f"{self.base_cmd} sn new-device-from-failed "
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
        try:
            self._wait_new_device_migration(
                new_device_id, timeout=self.MIGRATION_TIMEOUT
            )
            self.logger.info(
                f"Device recovery complete — new device {new_device_id} online"
            )
        except Exception as exc:
            self.logger.warning(
                f"new_device_migration did not complete: {exc}"
            )

    def _wait_new_device_migration(self, new_device_id, timeout=3600):
        """Wait for all new_device_migration tasks for *new_device_id* to finish."""
        self.logger.info(
            f"Waiting for new_device_migration tasks for {new_device_id} ..."
        )
        start = time.time()
        while time.time() - start < timeout:
            try:
                tasks = self.sbcli_utils.list_migration_tasks(
                    self.sbcli_utils.cluster_id
                )
                active = [
                    t for t in tasks.get("results", [])
                    if t.get("function_name") == "new_device_migration"
                    and new_device_id in str(t.get("target_id", ""))
                    and t.get("status") not in ("done", "cancelled", "error")
                ]
                if not active:
                    elapsed = time.time() - start
                    self.logger.info(
                        f"All new_device_migration tasks complete "
                        f"in {elapsed:.1f}s"
                    )
                    return elapsed
                self.logger.info(
                    f"Waiting for {len(active)} new_device_migration "
                    f"task(s) ..."
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
        self.logger.info(f"  IO load:          {'YES' if self._with_io_load else 'NO'}")
        self.logger.info(f"  Target node:      {self._target_node_id}")
        self.logger.info(f"  Target device:    {self._target_device_id}")
        self.logger.info(f"  Fill target:      {self.FILL_PERCENT}%")
        self.logger.info(f"  Lvols on target:  {len(self._lvols_on_target)}")
        self.logger.info(f"  Lvols on others:  {len(self._lvols_on_others)}")
        self.logger.info(f"  Result:           {'PASSED' if self._test_passed else 'FAILED'}")
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
            "status": "passed" if self._test_passed else "failed",
            "geometry": {"ndcs": self.ndcs, "npcs": self.npcs},
            "config": {
                "fill_percent": self.FILL_PERCENT,
                "lvol_size": self.LVOL_SIZE,
                "with_io_load": self._with_io_load,
                "failure_mode": self._failure_mode,
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
        # Filter for online devices only — old failed_and_migrated devices
        # remain in the list after recovery and must be skipped
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
                f"rw=randrw\n"
                f"rwmixread=50\n"
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
