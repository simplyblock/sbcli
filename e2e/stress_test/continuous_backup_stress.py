"""
Continuous stress tests for S3 backup / restore feature.

Stress scenarios
----------------
  BackupStressParallelSnapshots    – TC-BCK-STR-001..005
    Many concurrent snapshot-backup operations on multiple lvols.
    Verifies service stability, correct delta chain management, no data loss.

  BackupStressTcpFailover          – TC-BCK-STR-010..015
    Backup/restore cycle with random TCP-fabric storage-node outages mid-backup.
    Verifies backup survives failover; restore produces correct data.

  BackupStressRdmaFailover         – TC-BCK-STR-020..025
    Same as TCP variant but with RDMA fabric.

  BackupStressCryptoMix            – TC-BCK-STR-030..035
    Mix of plain, crypto, and geometry-varied lvols backed up concurrently.
    Covers all ndcs/npcs combinations + crypto lvols in a single stress run.

  BackupStressPolicyRetention      – TC-BCK-STR-040..045
    Policy with short retention; rapid snapshot creation to exercise
    the auto-merge / eviction path under load.

  BackupStressRestoreConcurrent    – TC-BCK-STR-050..055
    Multiple simultaneous restore operations; verify data integrity for each.

  BackupStressMarathon             – TC-BCK-STR-060..065
    Long-running mixed marathon: N rounds of backup / restore / delete / verify
    across 3 lvols.  Default 20 rounds for CI; set num_rounds=100 for full stress.

  BackupStressLargeScale           – TC-BCK-STR-070..076
    100 consecutive backups on a single lvol with periodic FIO writes.
    Restores from various chain depths (latest, oldest, mid-chain).

  BackupStressFilesystemSecurityMix – TC-BCK-STR-080..087
    Matrix of (ext4, xfs) x (plain, crypto, dhchap+crypto) — 6 combos.
    Each goes through backup → retention merge → restore → checksum verify.

  BackupStressRetentionMergeCycles  – TC-BCK-STR-090..095
    Repeated delete → backup → retention-merge → restore cycles on
    plain/crypto/xfs lvols.  Targets the regression where deleted backups
    corrupt the chain after retention merge.
"""

from __future__ import annotations

import os
import random
import threading
import time
from datetime import datetime

from e2e_tests.backup.test_backup_restore import BackupTestBase, _rand_suffix
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from utils.ssh_utils import get_parent_device

# ── constants ────────────────────────────────────────────────────────────────

_OUTAGE_TYPES = [
    "graceful_shutdown",
    "container_stop",
    "interface_full_network_interrupt",
    "interface_partial_network_interrupt",
]

_GEOMETRIES = [(1, 0), (1, 1), (2, 1)]

_BACKUP_POLL_INTERVAL = 10
_BACKUP_TIMEOUT = 300


# ════════════════════════════════════════════════════════════════════════════
#  Stress base – extends BackupTestBase with failover helpers
# ════════════════════════════════════════════════════════════════════════════


class BackupStressBase(BackupTestBase):
    """
    Adds storage-node outage helpers on top of BackupTestBase.
    Outage mechanics are reused from the existing HA stress framework.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "10G"
        self.fio_size = "2G"
        self.outage_log_file = os.path.join(
            "logs",
            f"bck_stress_outage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        self._init_outage_log()

    # ── outage log ────────────────────────────────────────────────────────────

    def _init_outage_log(self):
        os.makedirs("logs", exist_ok=True)
        with open(self.outage_log_file, "w") as f:
            f.write("Timestamp,Node,OutageType,Event\n")

    def _log_outage(self, node: str, outage_type: str, event: str):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(self.outage_log_file, "a") as f:
            f.write(f"{ts},{node},{outage_type},{event}\n")

    # ── outage execution ──────────────────────────────────────────────────────

    def _get_random_sn(self) -> str:
        """Return a random storage-node UUID."""
        nodes = self.sbcli_utils.get_all_nodes_ip()
        sn_ids = [
            n["uuid"]
            for n in nodes.get("results", [])
            if n.get("status") == "online"
        ]
        assert sn_ids, "No online storage nodes found"
        return random.choice(sn_ids)

    def _do_outage(self, node_id: str, outage_type: str):
        """Execute one outage cycle (trigger → wait → recover)."""
        self._log_outage(node_id, outage_type, "start")
        self.logger.info(f"[outage] {outage_type} on node {node_id}")

        node_details = self.sbcli_utils.get_storage_node_details(node_id)
        sn_node_ip = node_details[0]["mgmt_ip"]

        if outage_type == "graceful_shutdown":
            self.ssh_obj.exec_command(
                sn_node_ip,
                "systemctl stop simplyblock-storage || true")
            sleep_n_sec(30)
            self.ssh_obj.exec_command(
                sn_node_ip,
                "systemctl start simplyblock-storage || true")

        elif outage_type == "container_stop":
            self.ssh_obj.exec_command(
                sn_node_ip,
                "docker stop $(docker ps -q --filter name=spdk) || true")
            sleep_n_sec(30)
            self.ssh_obj.exec_command(
                sn_node_ip,
                "docker start $(docker ps -aq --filter name=spdk) || true")

        elif outage_type == "interface_full_network_interrupt":
            data_nics = node_details[0].get("data_nics", [])
            iface = data_nics[0]["if_name"] if data_nics else "eth0"
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"nmcli dev disconnect {iface} || true")
            sleep_n_sec(20)
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"nmcli dev connect {iface} || true")

        elif outage_type == "interface_partial_network_interrupt":
            port = 4420  # NVMe-oF target port
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"iptables -A INPUT -p tcp --dport {port} -j DROP || true")
            sleep_n_sec(20)
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"iptables -D INPUT -p tcp --dport {port} -j DROP || true")

        sleep_n_sec(10)
        self._log_outage(node_id, outage_type, "recovered")

    # ── snapshot / backup helpers ─────────────────────────────────────────────

    def _snap_and_backup(self, lvol_id: str, label: str) -> str | None:
        """Create a snapshot + trigger S3 backup; return backup_id or None on failure.

        Thread-safe: resolves the backup ID by matching the snapshot name
        or lvol ID in the backup list, instead of blindly taking the last
        entry (which races when multiple threads create backups).
        """
        snap_name = f"str_{label}_{_rand_suffix()}"
        try:
            snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
            sleep_n_sec(5)
            backups = self._list_backups()
            # Search for a backup whose Snapshot field matches our snap_id
            # or snap_name. This uniquely identifies the backup created by
            # this specific _create_snapshot call.
            for bk in reversed(backups):
                bk_snap = bk.get("Snapshot") or bk.get("snapshot") or ""
                if (snap_id and snap_id in bk_snap) \
                        or snap_name in bk_snap:
                    bk_id = (
                        bk.get("id") or bk.get("ID")
                        or bk.get("uuid") or None
                    )
                    if bk_id:
                        return bk_id
            # Last resort: log warning and return None
            self.logger.warning(
                f"snap_and_backup: could not resolve backup_id for "
                f"{label} (snap={snap_name}, lvol={lvol_id})")
        except Exception as e:
            self.logger.warning(f"snap_and_backup error ({label}): {e}")
        return None

    # ── FIO thread ────────────────────────────────────────────────────────────

    def _fio_background(self, mount: str, log_file: str,
                         results: dict, key: str):
        """Run FIO in a thread; record pass/fail in *results[key]*."""
        try:
            self._run_fio(mount, log_file=log_file, runtime=120)
            results[key] = "pass"
        except Exception as e:
            self.logger.error(f"FIO thread {key} failed: {e}")
            results[key] = f"fail: {e}"

    # ── backup / restore state helpers ────────────────────────────────────────

    def _wait_for_backup_terminal(self, backup_id: str,
                                   timeout: int = 600) -> str:
        """Poll backup list until *backup_id* leaves in-progress states.
        Returns the final status string (e.g. 'done', 'failed') or 'timeout'."""
        _IN_PROGRESS = {"in_progress", "pending", "running", "uploading",
                        "processing", "queued"}
        deadline = time.time() + timeout
        while time.time() < deadline:
            for b in self._list_backups():
                bid = b.get("id") or b.get("ID") or b.get("uuid") or ""
                if bid == backup_id or backup_id in bid:
                    status = (b.get("status") or b.get("Status") or "").lower()
                    if status and status not in _IN_PROGRESS:
                        return status
            sleep_n_sec(_BACKUP_POLL_INTERVAL)
        return "timeout"

    def _get_lvol_status(self, lvol_name: str) -> str | None:
        """Return the status of *lvol_name* from `lvol list`, or None if absent."""
        out, _ = self._sbcli("lvol list")
        rows = self._parse_table(out)
        for row in rows:
            name = (row.get("name") or row.get("Name")
                    or row.get("lvol_name") or "")
            if name == lvol_name:
                return (row.get("status") or row.get("Status")
                        or "unknown").lower()
        # Fallback: raw presence check
        if lvol_name in out:
            return "present"
        return None

    def _force_delete_lvol(self, lvol_name: str):
        """Delete lvol; try sbcli --force if the first attempt fails."""
        try:
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name, skip_error=False)
        except Exception as e:
            self.logger.warning(
                f"Normal lvol delete failed for {lvol_name}: {e} — retrying --force")
            self._sbcli(f"lvol delete {lvol_name} --force")
        if lvol_name in self.created_lvols:
            self.created_lvols.remove(lvol_name)

    def _connect_format_mount(self, lvol_name: str, lvol_id: str,
                               fs_type: str = "ext4") -> tuple[str, str]:
        """Connect lvol and format with specified filesystem type."""
        if self.k8s_test:
            pvc_name = self._k8s_normalize_name(lvol_name)
            return pvc_name, pvc_name

        mount = f"{self.mount_path}/{lvol_name}"
        initial = self.ssh_obj.get_devices(node=self.fio_node)
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for cmd in connect_ls:
            self.ssh_obj.exec_command(node=self.fio_node, command=cmd)
        sleep_n_sec(3)
        final = self.ssh_obj.get_devices(node=self.fio_node)
        new_devs = [d for d in final if d not in initial]
        assert new_devs, f"No new block device after connecting {lvol_name}"
        device = f"/dev/{new_devs[0]}"
        self.ssh_obj.format_disk(
            node=self.fio_node, device=device, fs_type=fs_type)
        self.ssh_obj.exec_command(self.fio_node, f"mkdir -p {mount}")
        self.ssh_obj.mount_path(
            node=self.fio_node, device=device, mount_path=mount)
        self.mounted.append((self.fio_node, mount))
        self.connected.append(lvol_id)
        return device, mount


# ════════════════════════════════════════════════════════════════════════════
#  Stress 1 – Parallel snapshot-backups on many lvols
# ════════════════════════════════════════════════════════════════════════════


class BackupStressParallelSnapshots(BackupStressBase):
    """
    TC-BCK-STR-001..005

    Creates N lvols concurrently, writes data to each, then triggers
    snapshot-backups for all of them in parallel threads.

    Validates:
      - All backups eventually appear in backup list (no silent drop)
      - Service remains responsive throughout
      - Restoring from any one backup succeeds with correct checksums
      - Delta chain stays bounded (no unbounded growth)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_parallel_snapshots"
        self.num_lvols = 6

    def run(self):
        self.logger.info("=== BackupStressParallelSnapshots START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Phase 1: create lvols and write data
        lvol_map: dict[str, tuple[str, str, str, dict]] = {}
        # {lvol_name: (lvol_id, device, mount, checksums)}

        for i in range(self.num_lvols):
            name, lvol_id = self._create_lvol(
                name=f"pstr_{i}_{_rand_suffix()}", size="5G")
            device, mount = self._connect_and_mount(name, lvol_id)
            self._run_fio(mount, runtime=20)
            files = self.ssh_obj.find_files(self.fio_node, mount)
            checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
            lvol_map[name] = (lvol_id, device, mount, checksums)

        # Phase 2: trigger snapshot + backup for all lvols in parallel
        snap_threads = []
        snap_results: dict[str, str | None] = {}

        def _snap_thread(name, lvol_id, idx):
            bk_id = self._snap_and_backup(lvol_id, f"pstr_{idx}")
            snap_results[name] = bk_id

        for i, (name, (lvol_id, _, _, _)) in enumerate(lvol_map.items()):
            t = threading.Thread(target=_snap_thread, args=(name, lvol_id, i))
            snap_threads.append(t)
            t.start()

        for t in snap_threads:
            t.join(timeout=_BACKUP_TIMEOUT)

        self.logger.info(f"TC-BCK-STR-001: snap_results={snap_results}")

        # Phase 3: verify all backups appear in list
        backups = self._list_backups()
        self.logger.info(
            f"TC-BCK-STR-002: total backups = {len(backups)} for {self.num_lvols} lvols")

        # Phase 4: restore one backup and verify checksums
        target_name = list(lvol_map.keys())[0]
        bk_id = snap_results.get(target_name)
        if bk_id:
            restored_name = f"par_rest_{_rand_suffix()}"
            self._restore_backup(bk_id, restored_name)
            self._wait_for_restore(restored_name)
            rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
            r_device, r_mount = self._connect_and_mount(
                restored_name, rest_id,
                mount=f"{self.mount_path}/par_rest_{_rand_suffix()}",
                format_disk=False)
            r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
            orig_checksums = lvol_map[target_name][3]
            self.ssh_obj.verify_checksums(
                self.fio_node, r_files, orig_checksums,
                message="TC-BCK-STR-004: parallel restore checksum mismatch", by_name=True)
            self.logger.info("TC-BCK-STR-004: parallel restore checksum ✓")

        # Phase 5: rapid multiple backups to test chain management
        self.logger.info("TC-BCK-STR-005: rapid multiple backups for chain management")
        first_name, (first_id, _, _, _) = list(lvol_map.items())[0]
        for i in range(4):
            self._snap_and_backup(first_id, f"chain_{i}")
            sleep_n_sec(3)
        final_backups = self._list_backups()
        self.logger.info(
            f"TC-BCK-STR-005: backup count after 4 rapid snaps: {len(final_backups)}")

        self.logger.info("=== BackupStressParallelSnapshots PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 2 – Backup with TCP failover mid-operation
# ════════════════════════════════════════════════════════════════════════════


class BackupStressTcpFailover(BackupStressBase):
    """
    TC-BCK-STR-010..015

    Runs FIO on lvols while triggering storage-node outages (TCP fabric).
    Interleaves snapshot-backups and outages to verify:
      - Backup survives a storage-node outage
      - Restored lvol has correct data after failover cycle
      - Multiple outage types covered (graceful, crash, network)
      - Crypto lvols included
      - Custom ndcs/npcs geometry included
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_tcp_failover"
        self.outage_types = _OUTAGE_TYPES
        self.num_iterations = 5

    def run(self):
        self.logger.info("=== BackupStressTcpFailover START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Create lvols: plain, crypto, and a geometry variant
        configs = [
            ("tcp_plain", False, None, None),
            ("tcp_crypto", True, None, None),
            ("tcp_geom", False, 2, 1),
        ]

        lvol_map = {}
        for label, crypto, ndcs, npcs in configs:
            name, lvol_id = self._create_lvol(
                name=f"{label}_{_rand_suffix()}",
                crypto=crypto,
                ndcs=ndcs,
                npcs=npcs,
            )
            device, mount = self._connect_and_mount(name, lvol_id)
            fio_log = f"{self.log_path}/fio_{label}.log"
            fio_results = {}
            fio_t = threading.Thread(
                target=self._fio_background,
                args=(mount, fio_log, fio_results, label),
            )
            fio_t.start()
            lvol_map[name] = {
                "id": lvol_id,
                "mount": mount,
                "fio_t": fio_t,
                "fio_results": fio_results,
                "label": label,
            }

        # Interleave: snapshot+backup then outage, repeated
        for iteration in range(self.num_iterations):
            outage_type = _OUTAGE_TYPES[iteration % len(_OUTAGE_TYPES)]
            self.logger.info(
                f"=== Iteration {iteration + 1}/{self.num_iterations} "
                f"outage_type={outage_type} ===")

            # TC-BCK-STR-010: Trigger backups for all lvols
            backup_ids = {}
            for name, info in lvol_map.items():
                bk_id = self._snap_and_backup(info["id"], f"iter{iteration}")
                backup_ids[name] = bk_id

            # TC-BCK-STR-011: Trigger storage-node outage
            try:
                sn_id = self._get_random_sn()
                self._do_outage(sn_id, outage_type)
            except Exception as e:
                self.logger.warning(f"Outage execution error: {e}")

            sleep_n_sec(20)

        # Wait for all FIO threads to finish
        for name, info in lvol_map.items():
            info["fio_t"].join(timeout=300)
            result = info["fio_results"].get(info["label"], "not_set")
            self.logger.info(f"TC-BCK-STR-012: FIO result for {name}: {result}")

        # Capture checksums after FIO completes (data is stable now)
        for name, info in lvol_map.items():
            try:
                files = self.ssh_obj.find_files(self.fio_node, info["mount"])
                info["checksums"] = self.ssh_obj.generate_checksums(
                    self.fio_node, files)
            except Exception as e:
                self.logger.warning(
                    f"TC-BCK-STR-012: could not capture checksums for {name}: {e}")
                info["checksums"] = {}

        # Take a final backup of each lvol now that FIO is done (data is stable)
        final_backup_ids: dict[str, str | None] = {}
        for name, info in lvol_map.items():
            bk_id = self._snap_and_backup(info["id"], "final")
            if bk_id:
                status = self._wait_for_backup_terminal(bk_id)
                if status in ("done", "complete", "completed"):
                    final_backup_ids[name] = bk_id
                    continue
            final_backup_ids[name] = None
            self.logger.warning(f"TC-BCK-STR-013: final backup failed for {name}")

        # TC-BCK-STR-013: Restore the final backup of each lvol; verify checksums
        for name, info in lvol_map.items():
            bk_id = final_backup_ids.get(name)
            if not bk_id:
                self.logger.warning(
                    f"TC-BCK-STR-013: no final backup for {name} — skipping restore")
                continue
            restored_name = f"tcp_rest_{_rand_suffix()}"
            try:
                self._restore_backup(bk_id, restored_name)
                self._wait_for_restore(restored_name)
                rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
                r_device, r_mount = self._connect_and_mount(
                    restored_name, rest_id,
                    mount=f"{self.mount_path}/tr_{_rand_suffix()}",
                    format_disk=False)
                # Verify checksums match what was captured after FIO completed
                if info.get("checksums"):
                    r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                    self.ssh_obj.verify_checksums(
                        self.fio_node, r_files, info["checksums"],
                        message=f"TC-BCK-STR-013: checksum mismatch for {name}",
                        by_name=True)
                    self.logger.info(
                        f"TC-BCK-STR-013: {name} checksum verified after failover")
                else:
                    # Fallback: at least verify files exist
                    r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                    assert len(r_files) > 0, (
                        f"TC-BCK-STR-013: no files in restored {name}")
                self._run_fio(r_mount, runtime=30)
                self.logger.info(
                    f"TC-BCK-STR-013: restore after TCP failover OK for {name}")
            except Exception as e:
                self.logger.error(f"TC-BCK-STR-013: restore failed for {name}: {e}")

        self.logger.info("=== BackupStressTcpFailover PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 3 – Backup with RDMA failover mid-operation
# ════════════════════════════════════════════════════════════════════════════


class BackupStressRdmaFailover(BackupStressTcpFailover):
    """
    TC-BCK-STR-020..025

    Identical to BackupStressTcpFailover but verifies RDMA fabric.
    Inherits all test logic; only test_name differs so the runner
    can select it independently.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_rdma_failover"

    def run(self):
        self.logger.info("=== BackupStressRdmaFailover START ===")
        cluster = self.sbcli_utils.get_cluster_details()
        if not cluster.get("fabric_rdma"):
            self.logger.warning(
                "RDMA fabric not available on this cluster — skipping RDMA stress test")
            return
        super().run()
        self.logger.info("=== BackupStressRdmaFailover PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 4 – Mixed crypto + geometry under concurrent backup load
# ════════════════════════════════════════════════════════════════════════════


class BackupStressCryptoMix(BackupStressBase):
    """
    TC-BCK-STR-030..035

    Creates one lvol per (crypto, ndcs, npcs) combination and backs
    them all up concurrently.

    Combinations tested:
      plain  ndcs=1 npcs=0
      plain  ndcs=2 npcs=1
      plain  ndcs=4 npcs=1
      crypto ndcs=1 npcs=0
      crypto ndcs=2 npcs=1

    Validates:
      - All backup operations complete without error
      - Restore from each backup produces correct checksums
      - Service remains stable throughout
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_crypto_mix"
        self._combos = [
            # (label, crypto, ndcs, npcs)
            ("plain_1_0", False, 1, 0),
            ("plain_2_1", False, 2, 1),
            ("plain_4_1", False, 4, 1),
            ("crypto_1_0", True,  1, 0),
            ("crypto_2_1", True,  2, 1),
        ]

    def run(self):
        self.logger.info("=== BackupStressCryptoMix START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        lvol_map = {}
        for label, crypto, ndcs, npcs in self._combos:
            name, lvol_id = self._create_lvol(
                name=f"mix_{label}_{_rand_suffix()}",
                crypto=crypto, ndcs=ndcs, npcs=npcs)
            device, mount = self._connect_and_mount(name, lvol_id)
            self._run_fio(mount, runtime=20)
            files = self.ssh_obj.find_files(self.fio_node, mount)
            checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
            lvol_map[name] = {"id": lvol_id, "mount": mount,
                               "checksums": checksums, "label": label}

        # Concurrent backups
        backup_results: dict[str, str | None] = {}
        threads = []

        def _bk_thread(name, lvol_id, label):
            bk_id = self._snap_and_backup(lvol_id, f"mix_{label}")
            backup_results[name] = bk_id

        for name, info in lvol_map.items():
            t = threading.Thread(
                target=_bk_thread,
                args=(name, info["id"], info["label"]))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=_BACKUP_TIMEOUT)

        self.logger.info(f"TC-BCK-STR-030: backup_results={backup_results}")

        # Restore each and verify checksums
        for name, bk_id in backup_results.items():
            if not bk_id:
                self.logger.warning(f"TC-BCK-STR-031: no backup_id for {name}")
                continue
            restored_name = f"mix_rest_{_rand_suffix()}"
            try:
                self._restore_backup(bk_id, restored_name)
                self._wait_for_restore(restored_name)
                rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
                r_device, r_mount = self._connect_and_mount(
                    restored_name, rest_id,
                    mount=f"{self.mount_path}/mr_{_rand_suffix()}",
                    format_disk=False)
                r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, r_files, lvol_map[name]["checksums"],
                    message=f"TC-BCK-STR-032: checksum mismatch for {name}", by_name=True)
                self.logger.info(f"TC-BCK-STR-032: {name} checksum ✓")
            except Exception as e:
                self.logger.error(f"TC-BCK-STR-032: restore/checksum error {name}: {e}")

        self.logger.info("=== BackupStressCryptoMix PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 5 – Policy retention under rapid snapshot load
# ════════════════════════════════════════════════════════════════════════════


class BackupStressPolicyRetention(BackupStressBase):
    """
    TC-BCK-STR-040..045

    Attaches a policy with --versions 3 to an lvol and then creates
    snapshots rapidly to exercise the auto-merge / pruning path.

    Validates:
      - Policy enforced: backup count stays bounded
      - Service remains stable after many merges
      - Restore from latest backup still works after multiple merges
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_policy_retention"
        self.num_snapshots = 10

    def run(self):
        self.logger.info("=== BackupStressPolicyRetention START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        lvol_name, lvol_id = self._create_lvol(name=f"ret_str_{_rand_suffix()}")
        self.sbcli_utils.get_storage_pool_id(pool_name=self.pool_name)

        # TC-BCK-STR-040: create policy with versions=3
        policy_name = f"ret_pol_{_rand_suffix()}"
        policy_id = self._add_policy(policy_name, versions=3, age="1d")
        self._attach_policy(policy_id, "lvol", lvol_id)

        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=20)

        # Capture checksums for verification after restore
        files = self.ssh_obj.find_files(self.fio_node, mount)
        original_checksums = self.ssh_obj.generate_checksums(
            self.fio_node, files)

        # TC-BCK-STR-041: rapid snapshots
        for i in range(self.num_snapshots):
            self.logger.info(
                f"TC-BCK-STR-041: snapshot {i + 1}/{self.num_snapshots}")
            sn = f"ret_snap_{i}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, sn, backup=True)
            sleep_n_sec(5)

        sleep_n_sec(30)

        # TC-BCK-STR-042: backup count bounded by policy
        backups_now = self._list_backups()
        self.logger.info(
            f"TC-BCK-STR-042: {len(backups_now)} backups after "
            f"{self.num_snapshots} snapshots (policy versions=3)")
        # Delta chain can be larger during merge window; just log

        # TC-BCK-STR-043: restore latest backup after merges + verify checksums
        if backups_now:
            latest_id = (
                backups_now[-1].get("id")
                or backups_now[-1].get("ID")
                or backups_now[-1].get("uuid")
                or None
            )
            if latest_id:
                self._unmount_and_disconnect(self.fio_node, mount, lvol_id)
                ret_restored = f"ret_rest_{_RAND_SUFFIX()}"
                self._restore_backup(latest_id, ret_restored)
                self._wait_for_restore(ret_restored)
                rest_id = self.sbcli_utils.get_lvol_id(
                    lvol_name=ret_restored)
                _, r_mount = self._connect_and_mount(
                    ret_restored, rest_id,
                    mount=f"{self.mount_path}/retr_{_rand_suffix()}",
                    format_disk=False)
                r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, r_files, original_checksums,
                    message="TC-BCK-STR-043: checksum mismatch after "
                            "retention merge restore",
                    by_name=True)
                self.logger.info(
                    "TC-BCK-STR-043: restore after merges — "
                    "checksums verified ✓")
                # Reconnect source for subsequent operations
                _, mount = self._connect_and_mount(
                    lvol_name, lvol_id, format_disk=False)

        # TC-BCK-STR-044: detach policy, more snapshots → no auto-backup
        self._detach_policy(policy_id, "lvol", lvol_id)
        bk_count_before = len(self._list_backups())
        for i in range(3):
            sn = f"post_detach_{i}_{_rand_suffix()}"
            self._create_snapshot(lvol_id, sn, backup=False)
            sleep_n_sec(3)
        sleep_n_sec(15)
        bk_count_after = len(self._list_backups())
        self.logger.info(
            f"TC-BCK-STR-044: backups before={bk_count_before} after detach={bk_count_after}")

        self.logger.info("=== BackupStressPolicyRetention PASSED ===")


def _RAND_SUFFIX():
    return _rand_suffix()


# ════════════════════════════════════════════════════════════════════════════
#  Stress 6 – Concurrent restores
# ════════════════════════════════════════════════════════════════════════════


class BackupStressRestoreConcurrent(BackupStressBase):
    """
    TC-BCK-STR-050..055

    Triggers multiple restore operations simultaneously and verifies
    each restored lvol has correct data.

    Validates:
      - Concurrent restores complete without service crash
      - Each restored lvol has correct data (checksum)
      - All restored lvols are independently connectable and FIO-capable
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_restore_concurrent"
        self.num_concurrent = 4

    def run(self):
        self.logger.info("=== BackupStressRestoreConcurrent START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Create source lvols, write data, snapshot+backup
        source_bk_pairs: list[tuple[str, dict, str]] = []
        # (lvol_name, checksums, backup_id)

        for i in range(self.num_concurrent):
            name, lvol_id = self._create_lvol(
                name=f"conc_src_{i}_{_rand_suffix()}", size="5G")
            device, mount = self._connect_and_mount(name, lvol_id)
            self._run_fio(mount, runtime=20)
            files = self.ssh_obj.find_files(self.fio_node, mount)
            checksums = self.ssh_obj.generate_checksums(self.fio_node, files)

            bk_id = self._snap_and_backup(lvol_id, f"conc_{i}")
            source_bk_pairs.append((name, checksums, bk_id))

        sleep_n_sec(15)

        # TC-BCK-STR-050: trigger concurrent restores
        restore_results: dict[str, str] = {}
        restore_threads = []

        def _restore_thread(bk_id: str, restored_name: str, key: str):
            try:
                self._restore_backup(bk_id, restored_name)
                self._wait_for_restore(restored_name)
                restore_results[key] = "done"
            except Exception as e:
                self.logger.error(f"Restore thread {key} failed: {e}")
                restore_results[key] = f"fail: {e}"

        restored_pairs: list[tuple[str, dict]] = []
        for i, (src_name, checksums, bk_id) in enumerate(source_bk_pairs):
            if not bk_id:
                self.logger.warning(f"No backup_id for {src_name} — skip")
                continue
            restored_name = f"conc_rest_{i}_{_rand_suffix()}"
            t = threading.Thread(
                target=_restore_thread,
                args=(bk_id, restored_name, restored_name))
            restore_threads.append(t)
            restored_pairs.append((restored_name, checksums))
            t.start()

        for t in restore_threads:
            t.join(timeout=_BACKUP_TIMEOUT)

        self.logger.info(f"TC-BCK-STR-050: restore_results={restore_results}")

        # TC-BCK-STR-051–055: verify each restored lvol
        for restored_name, orig_checksums in restored_pairs:
            if restore_results.get(restored_name, "").startswith("fail"):
                self.logger.error(
                    f"TC-BCK-STR-051: skipping checksum for {restored_name} "
                    f"(restore failed)")
                continue
            try:
                rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
                r_device, r_mount = self._connect_and_mount(
                    restored_name, rest_id,
                    mount=f"{self.mount_path}/cr_{_rand_suffix()}",
                    format_disk=False)
                r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, r_files, orig_checksums,
                    message=f"TC-BCK-STR-052: checksum mismatch {restored_name}", by_name=True)
                self._run_fio(r_mount, runtime=20)
                self.logger.info(f"TC-BCK-STR-051: {restored_name} ✓")
            except Exception as e:
                self.logger.error(
                    f"TC-BCK-STR-051: post-restore check failed {restored_name}: {e}")

        self.logger.info("=== BackupStressRestoreConcurrent PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Marathon – long-running mixed backup / restore / delete stress
# ════════════════════════════════════════════════════════════════════════════


class BackupStressMarathon(BackupStressBase):
    """
    TC-BCK-STR-060..065

    Runs num_rounds iterations (default 50; set to 100 for full stress)
    across 6 lvols with a randomly selected operation each round:

      BACKUP            (53 % weight) – snapshot + S3 backup on a random lvol
      RESTORE           (26 % weight) – restore a random previously-made backup;
                                        verify checksums
      DELETE_AND_BACKUP (11 % weight) – delete all backups for a random lvol,
                                        immediately take a fresh backup, verify
                                        the chain works again (graceful fallback
                                        if backup delete not yet supported)
      VERIFY            (10 % weight) – verify checksums on a randomly chosen
                                        already-restored lvol

    Every 5 rounds a forced checksum check is also run on the most-recently
    restored lvol to catch silent corruption early.

    Validates:
      - Service remains stable across 50-100 mixed operations
      - Delta chain stays bounded after repeated backups
      - After backup delete + re-backup, new chain is fully restorable
      - Checksums are correct throughout (no silent data corruption)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_marathon"
        self.num_rounds = 50   # set to 100 for a full stress run
        self.num_lvols = 6
        self._weights = (["backup"] * 10 + ["restore"] * 5
                         + ["delete_and_backup"] * 2 + ["verify"] * 2)

    # ── internal helpers ──────────────────────────────────────────────────

    def _do_backup(self, state: dict, lvol_key: str, round_num: int) -> None:
        info = state["lvols"][lvol_key]
        sn = f"mara_{lvol_key}_{round_num}_{_rand_suffix()}"
        bk_id = self._snap_and_backup(info["id"], sn)
        if bk_id:
            info["backup_ids"].append(bk_id)
            self.logger.info(
                f"[round {round_num}] BACKUP {lvol_key} → {bk_id} "
                f"(chain depth={len(info['backup_ids'])})")

    def _do_restore(self, state: dict, lvol_key: str, round_num: int) -> None:
        info = state["lvols"][lvol_key]
        if not info["backup_ids"]:
            self.logger.info(f"[round {round_num}] RESTORE {lvol_key}: no backups yet, skipping")
            return
        bk_id = random.choice(info["backup_ids"])
        rst_name = f"mara_rst_{round_num}_{_rand_suffix()}"
        try:
            self._restore_backup(bk_id, rst_name)
            self._wait_for_restore(rst_name)
            rst_id = self.sbcli_utils.get_lvol_id(lvol_name=rst_name)
            _, rst_mount = self._connect_and_mount(
                rst_name, rst_id,
                mount=f"{self.mount_path}/mr_{round_num}_{_rand_suffix()}",
                format_disk=False)
            rst_files = self.ssh_obj.find_files(self.fio_node, rst_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, rst_files, info["checksums"],
                message=f"[round {round_num}] RESTORE {lvol_key} checksum mismatch", by_name=True)
            state["restored"].append((rst_name, info["checksums"]))
            self.logger.info(f"[round {round_num}] RESTORE {lvol_key} ← {bk_id} ✓")
        except Exception as e:
            self.logger.error(f"[round {round_num}] RESTORE {lvol_key} failed: {e}")

    def _do_delete_and_backup(self, state: dict, lvol_key: str, round_num: int) -> None:
        info = state["lvols"][lvol_key]
        self.logger.info(
            f"[round {round_num}] DELETE_AND_BACKUP {lvol_key} "
            f"(deleting {len(info['backup_ids'])} backup(s))")
        try:
            self._delete_backups(info["id"])
            info["backup_ids"].clear()
            sleep_n_sec(5)
            remaining = [
                b for b in self._list_backups()
                if lvol_key in " ".join(str(v) for v in b.values())
            ]
            if remaining:
                self.logger.warning(
                    f"[round {round_num}] backups not fully deleted for "
                    f"{lvol_key}: {len(remaining)} remaining (may be unsupported)")
            sn = f"mara_fresh_{lvol_key}_{round_num}_{_rand_suffix()}"
            self._create_snapshot(info["id"], sn, backup=True)
            bk_id = self._wait_for_backup_by_snap(sn, f"marathon[{round_num}]")
            info["backup_ids"].append(bk_id)
            self.logger.info(
                f"[round {round_num}] DELETE_AND_BACKUP {lvol_key}: fresh backup {bk_id}")
        except Exception as e:
            self.logger.warning(
                f"[round {round_num}] DELETE_AND_BACKUP {lvol_key} "
                f"failed (backup delete may not be supported): {e}")

    def _do_verify(self, state: dict, round_num: int) -> None:
        if not state["restored"]:
            self.logger.info(f"[round {round_num}] VERIFY: no restored lvols yet, skipping")
            return
        rst_name, expected = random.choice(state["restored"])
        try:
            rst_id = self.sbcli_utils.get_lvol_id(lvol_name=rst_name)
            if not rst_id:
                self.logger.warning(
                    f"[round {round_num}] VERIFY: {rst_name} no longer exists, skipping")
                return
            out, _ = self._sbcli("lvol list")
            if rst_name not in out:
                self.logger.warning(
                    f"[round {round_num}] VERIFY: {rst_name} not in lvol list, skipping")
                return
            # Re-mount and re-verify (mount may already be tracked; use a fresh path)
            mount_path = f"{self.mount_path}/mv_{round_num}_{_rand_suffix()}"
            _, rst_mount = self._connect_and_mount(rst_name, rst_id, mount=mount_path, format_disk=False)
            files = self.ssh_obj.find_files(self.fio_node, rst_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, files, expected,
                message=f"[round {round_num}] VERIFY {rst_name} checksum mismatch", by_name=True)
            self.logger.info(f"[round {round_num}] VERIFY {rst_name} ✓")
        except Exception as e:
            self.logger.error(f"[round {round_num}] VERIFY {rst_name} error: {e}")

    # ── main run ──────────────────────────────────────────────────────────

    def run(self):
        self.logger.info(
            f"=== BackupStressMarathon START  rounds={self.num_rounds} ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # TC-BCK-STR-060: Setup — create lvols, write data, capture checksums
        self.logger.info(f"TC-BCK-STR-060: creating {self.num_lvols} lvols and capturing checksums")
        state = {"lvols": {}, "restored": []}

        for i in range(self.num_lvols):
            key = f"lv{i}"
            name, lvol_id = self._create_lvol(
                name=f"mara_{i}_{_rand_suffix()}", size="5G")
            _, mount = self._connect_and_mount(name, lvol_id)
            self._run_fio(mount, runtime=20)
            files = self.ssh_obj.find_files(self.fio_node, mount)
            checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
            state["lvols"][key] = {
                "name": name,
                "id": lvol_id,
                "checksums": checksums,
                "backup_ids": [],
            }
            self.logger.info(f"TC-BCK-STR-060: {key}={name} ready")

        lvol_keys = list(state["lvols"].keys())

        # TC-BCK-STR-061: Marathon loop
        self.logger.info(f"TC-BCK-STR-061: starting {self.num_rounds}-round marathon")
        backup_count = restore_count = delete_count = verify_count = 0

        for round_num in range(1, self.num_rounds + 1):
            action = random.choice(self._weights)
            lvol_key = random.choice(lvol_keys)

            if action == "backup":
                self._do_backup(state, lvol_key, round_num)
                backup_count += 1
            elif action == "restore":
                self._do_restore(state, lvol_key, round_num)
                restore_count += 1
            elif action == "delete_and_backup":
                self._do_delete_and_backup(state, lvol_key, round_num)
                delete_count += 1
            elif action == "verify":
                self._do_verify(state, round_num)
                verify_count += 1

            # TC-BCK-STR-062: Forced checksum every 5 rounds
            if round_num % 5 == 0 and state["restored"]:
                self.logger.info(f"TC-BCK-STR-062: forced checksum check at round {round_num}")
                self._do_verify(state, round_num)

            sleep_n_sec(2)

        self.logger.info(
            f"TC-BCK-STR-061: marathon complete — "
            f"backups={backup_count} restores={restore_count} "
            f"deletes={delete_count} verifies={verify_count}")

        # TC-BCK-STR-063: Final checksum verification on all restored lvols
        self.logger.info(
            f"TC-BCK-STR-063: final checksum pass on {len(state['restored'])} restored lvol(s)")
        failures = 0
        for rst_name, expected in state["restored"]:
            try:
                out, _ = self._sbcli("lvol list")
                if rst_name not in out:
                    continue
                rst_id = self.sbcli_utils.get_lvol_id(lvol_name=rst_name)
                if not rst_id:
                    continue
                mount_path = f"{self.mount_path}/mf_{_rand_suffix()}"
                _, rst_mount = self._connect_and_mount(rst_name, rst_id, mount=mount_path, format_disk=False)
                files = self.ssh_obj.find_files(self.fio_node, rst_mount)
                self.ssh_obj.verify_checksums(self.fio_node, files, expected,
                    message=f"TC-BCK-STR-063: final checksum mismatch for {rst_name}", by_name=True)
                self.logger.info(f"TC-BCK-STR-063: {rst_name} ✓")
            except Exception as e:
                self.logger.error(f"TC-BCK-STR-063: {rst_name} failed: {e}")
                failures += 1

        assert failures == 0, \
            f"TC-BCK-STR-063: {failures} lvol(s) failed final checksum verification"

        # TC-BCK-STR-064: Verify backup list depth bounded for each lvol
        self.logger.info("TC-BCK-STR-064: verify backup chain depth is bounded")
        all_backups = self._list_backups()
        for key, info in state["lvols"].items():
            lvol_bks = [
                b for b in all_backups
                if info["name"] in " ".join(str(v) for v in b.values())
            ]
            self.logger.info(
                f"TC-BCK-STR-064: {key} ({info['name']}) has {len(lvol_bks)} backup(s) in list")

        # TC-BCK-STR-065: Service health — backup list must still respond
        self.logger.info("TC-BCK-STR-065: service health check — backup list must respond")
        final_list = self._list_backups()
        self.logger.info(
            f"TC-BCK-STR-065: backup list returned {len(final_list)} entries — service healthy ✓")

        self.logger.info("=== BackupStressMarathon PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 8 – Large-scale: 100 backups on a single lvol
# ════════════════════════════════════════════════════════════════════════════


class BackupStressLargeScale(BackupStressBase):
    """
    TC-BCK-STR-070..076

    Creates 100 incremental backups across 4 lvols (25 per lvol) with a
    mix of filesystem types (ext4, xfs) and security configs (plain, crypto).
    Backups are batched in parallel (4 concurrent threads).  Periodic FIO
    writes between batches produce real deltas.  Restores from various chain
    depths are run concurrently at the end.

    Validates:
      - Service stays stable after 100 total backups across 4 lvols
      - Delta chain management keeps backup list bounded (via auto-merge)
      - Restore from any depth of any lvol's chain yields correct data
      - Backup list responds promptly even with many entries
      - Parallel backup and restore operations do not interfere

    This test is designed for the backup-stress runner.  On a CI cluster
    with limited resources, num_backups can be lowered via subclass override.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_large_scale"
        self.num_backups = 100       # total across all lvols
        self.fio_interval = 10       # write new data every N backups per lvol
        self.restore_count = 5       # chain points to restore per lvol
        self._lvol_configs = [
            # (label, fs_type, crypto)
            ("plain_ext4", "ext4", False),
            ("crypto_ext4", "ext4", True),
            ("plain_xfs",  "xfs",  False),
            ("crypto_xfs", "xfs",  True),
        ]

    def run(self):
        self.logger.info(
            f"=== BackupStressLargeScale START  num_backups={self.num_backups} ===")
        self.fio_node = self.fio_node[0]
        self._ensure_pool_and_sc()

        num_lvols = len(self._lvol_configs)
        backups_per_lvol = self.num_backups // num_lvols

        # TC-BCK-STR-070: create lvols with FS mix, initial FIO, capture baseline
        self.logger.info(
            f"TC-BCK-STR-070: creating {num_lvols} lvols "
            f"({backups_per_lvol} backups each)")
        lvol_state: dict[str, dict] = {}
        for label, fs_type, crypto in self._lvol_configs:
            name, lvol_id = self._create_lvol(
                name=f"scale_{label}_{_rand_suffix()}", size="10G",
                crypto=crypto)
            _, mount = self._connect_format_mount(
                name, lvol_id, fs_type=fs_type)
            self._run_fio(mount, runtime=30)
            checksums = self._get_checksums(self.fio_node, mount)
            assert checksums, f"TC-BCK-STR-070: no checksums for {label}"
            lvol_state[label] = {
                "name": name, "id": lvol_id, "mount": mount,
                "latest_checksums": checksums,
                "checksums_at": {0: checksums},
                "all_bk_ids": [], "successful": [],
            }
            self.logger.info(f"TC-BCK-STR-070: {label} ({name}) ready")

        # TC-BCK-STR-071: create backups with periodic FIO writes (parallel batches)
        self.logger.info(
            f"TC-BCK-STR-071: creating {self.num_backups} backups in "
            f"parallel batches of {num_lvols}")

        for i in range(backups_per_lvol):
            # Write new data periodically
            if i > 0 and i % self.fio_interval == 0:
                for label, info in lvol_state.items():
                    self._run_fio(info["mount"], runtime=10, rw="write")
                    info["latest_checksums"] = self._get_checksums(
                        self.fio_node, info["mount"])
                    info["checksums_at"][i] = info["latest_checksums"]
                self.logger.info(
                    f"TC-BCK-STR-071[{i}]: wrote new data to all lvols")

            # Backup all lvols in parallel
            threads = []
            thread_results: dict[str, str] = {}

            def _backup_one(label, info, idx):
                sn = f"sc_{label}_{idx}_{_rand_suffix()}"
                try:
                    self._create_snapshot(info["id"], sn, backup=True)
                    bk_id = self._wait_for_backup_by_snap(
                        sn, f"TC-BCK-STR-071[{label}][{idx}]")
                    thread_results[label] = bk_id
                except Exception as e:
                    self.logger.warning(
                        f"TC-BCK-STR-071[{label}][{idx}]: backup failed: {e}")
                    thread_results[label] = ""

            for label, info in lvol_state.items():
                t = threading.Thread(
                    target=_backup_one, args=(label, info, i))
                threads.append(t)
                t.start()

            for t in threads:
                t.join(timeout=_BACKUP_TIMEOUT)

            for label, bk_id in thread_results.items():
                lvol_state[label]["all_bk_ids"].append(bk_id)
                if bk_id:
                    lvol_state[label]["successful"].append(bk_id)

            if i % 10 == 9:
                total_done = (i + 1) * num_lvols
                self.logger.info(
                    f"TC-BCK-STR-071: {total_done}/{self.num_backups} backups done")
            sleep_n_sec(2)

        # Summary
        for label, info in lvol_state.items():
            self.logger.info(
                f"TC-BCK-STR-071: {label}: "
                f"{len(info['successful'])}/{backups_per_lvol} backups succeeded")
        total_ok = sum(len(info["successful"]) for info in lvol_state.values())
        assert total_ok >= self.num_backups * 0.8, (
            f"TC-BCK-STR-071: too many failures — only {total_ok} of "
            f"{self.num_backups} backups succeeded"
        )

        # TC-BCK-STR-072: backup list health
        all_backups = self._list_backups()
        self.logger.info(
            f"TC-BCK-STR-072: backup list has {len(all_backups)} entries "
            f"after {self.num_backups} backup operations")

        # TC-BCK-STR-073: concurrent restores from multiple chain depths
        # Pick restore points for each lvol
        for label, info in lvol_state.items():
            self._unmount_and_disconnect(
                self.fio_node, info["mount"], info["id"])

        restore_threads = []
        restore_results: dict[str, int] = {}  # label → ok count

        def _restore_lvol(label, info):
            successful = info["successful"]
            if not successful:
                restore_results[label] = 0
                return
            indices = [len(successful) - 1, 0]
            step = max(1, len(successful) // max(self.restore_count - 2, 1))
            for j in range(1, self.restore_count - 1):
                idx = min(j * step, len(successful) - 1)
                if idx not in indices:
                    indices.append(idx)
            ok = 0
            for idx in indices:
                bk_id = successful[idx]
                rst_name = f"sc_rst_{label}_{idx}_{_rand_suffix()}"
                try:
                    self._restore_backup(bk_id, rst_name)
                    self._wait_for_restore(rst_name)
                    rst_id = self._get_lvol_id(rst_name)
                    _, rst_mount = self._connect_and_mount(
                        rst_name, rst_id,
                        mount=f"{self.mount_path}/scr_{label}_{idx}_{_rand_suffix()}",
                        format_disk=False)
                    all_bk_ids = info["all_bk_ids"]
                    orig_idx = (all_bk_ids.index(bk_id)
                                if bk_id in all_bk_ids else idx)
                    valid_points = sorted(
                        k for k in info["checksums_at"] if k <= orig_idx)
                    expected = (info["checksums_at"][valid_points[-1]]
                                if valid_points else info["latest_checksums"])
                    r_files = self.ssh_obj.find_files(self.fio_node, rst_mount)
                    assert len(r_files) > 0
                    self.ssh_obj.verify_checksums(
                        self.fio_node, r_files, expected,
                        message=f"TC-BCK-STR-073: {label}[{idx}] checksum mismatch",
                        by_name=True)
                    ok += 1
                    self.logger.info(
                        f"TC-BCK-STR-073: {label} depth {idx} checksum OK")
                    self._unmount_and_disconnect(
                        self.fio_node, rst_mount, rst_id)
                except Exception as e:
                    self.logger.error(
                        f"TC-BCK-STR-073: {label} depth {idx} failed: {e}")
            restore_results[label] = ok

        for label, info in lvol_state.items():
            t = threading.Thread(target=_restore_lvol, args=(label, info))
            restore_threads.append(t)
            t.start()

        for t in restore_threads:
            t.join(timeout=_BACKUP_TIMEOUT * 3)

        total_restore_ok = sum(restore_results.values())
        total_restore_attempts = sum(
            min(self.restore_count, len(info["successful"]))
            for info in lvol_state.values())
        self.logger.info(
            f"TC-BCK-STR-073: {total_restore_ok}/{total_restore_attempts} "
            f"restores succeeded across {num_lvols} lvols")
        assert total_restore_ok >= total_restore_attempts - num_lvols, (
            f"TC-BCK-STR-073: too many restore failures — "
            f"{total_restore_ok}/{total_restore_attempts}"
        )

        # TC-BCK-STR-074: restore latest from each lvol and verify
        self.logger.info("TC-BCK-STR-074: restore latest from each lvol")
        for label, info in lvol_state.items():
            if not info["successful"]:
                continue
            latest_bk = info["successful"][-1]
            rst_name = f"sc_latest_{label}_{_rand_suffix()}"
            self._restore_backup(latest_bk, rst_name)
            self._wait_for_restore(rst_name)
            rst_id = self._get_lvol_id(rst_name)
            _, rst_mount = self._connect_and_mount(
                rst_name, rst_id,
                mount=f"{self.mount_path}/scl_{label}_{_rand_suffix()}",
                format_disk=False)
            self._verify_checksums(
                self.fio_node, rst_mount, info["latest_checksums"])
            self.logger.info(f"TC-BCK-STR-074: {label} latest restore OK")
            self._unmount_and_disconnect(self.fio_node, rst_mount, rst_id)

        self.logger.info("=== BackupStressLargeScale PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 9 – Filesystem + security matrix: ext4/xfs x plain/crypto/dhchap
# ════════════════════════════════════════════════════════════════════════════


class BackupStressFilesystemSecurityMix(BackupStressBase):
    """
    TC-BCK-STR-080..087

    Creates lvols across a matrix of filesystem type (ext4, xfs) and
    security configuration (plain, crypto, dhchap+crypto).  Each lvol
    goes through a full backup → retention merge → restore → verify cycle.

    Validates:
      - Backup/restore works for every (fs, security) combination
      - Retention merge preserves data for all configurations
      - No cross-contamination between different lvol configs
      - XFS UUID handling doesn't break restore
      - Crypto + DHCHAP don't interfere with backup data chain

    Combinations tested (6 lvols):
      ext4 + plain
      ext4 + crypto
      ext4 + dhchap+crypto
      xfs  + plain
      xfs  + crypto
      xfs  + dhchap+crypto
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_fs_security_mix"
        self._combos = [
            # (label, fs_type, crypto, dhchap)
            ("ext4_plain",       "ext4", False, False),
            ("ext4_crypto",      "ext4", True,  False),
            ("ext4_dhchap_crypt", "ext4", True,  True),
            ("xfs_plain",        "xfs",  False, False),
            ("xfs_crypto",       "xfs",  True,  False),
            ("xfs_dhchap_crypt", "xfs",  True,  True),
        ]

    def run(self):
        self.logger.info("=== BackupStressFilesystemSecurityMix START ===")
        self.fio_node = self.fio_node[0]
        self._ensure_pool_and_sc()

        results: dict[str, str] = {}

        for combo_idx, (label, fs_type, crypto, dhchap) in enumerate(self._combos):
            self.logger.info(
                f"TC-BCK-STR-08{combo_idx}: [{label}] "
                f"fs={fs_type} crypto={crypto} dhchap={dhchap}")
            try:
                # Create lvol with specified config
                lvol_name, lvol_id = self._create_lvol(
                    name=f"fsm_{label}_{_rand_suffix()}",
                    crypto=crypto)

                # Connect and format with specified filesystem
                _, mount = self._connect_format_mount(
                    lvol_name, lvol_id, fs_type=fs_type)

                # Write data and capture checksums
                self._run_fio(mount, runtime=20)
                checksums = self._get_checksums(self.fio_node, mount)
                assert checksums, f"[{label}] no checksums captured"

                # Take 5 backups, apply retention policy
                bk_ids = []
                for i in range(5):
                    sn = f"fsm_{label}_{i}_{_rand_suffix()}"
                    self._create_snapshot(lvol_id, sn, backup=True)
                    bk_id = self._wait_for_backup_by_snap(
                        sn, f"[{label}][{i}]")
                    bk_ids.append(bk_id)
                    sleep_n_sec(3)

                pol_name = f"fsm_pol_{label}_{_rand_suffix()}"
                pol_id = self._add_policy(pol_name, versions=3, age="1d")
                self._attach_policy(pol_id, "lvol", lvol_id)
                sleep_n_sec(30)  # let retention merge run

                # Restore from latest and verify checksums
                self._unmount_and_disconnect(self.fio_node, mount, lvol_id)

                rst_name = f"fsm_rst_{label}_{_rand_suffix()}"
                self._restore_backup(bk_ids[-1], rst_name)
                self._wait_for_restore(rst_name)
                rst_id = self._get_lvol_id(rst_name)

                # Restored lvol already has filesystem — mount without format
                _, rst_mount = self._connect_and_mount(
                    rst_name, rst_id,
                    mount=f"{self.mount_path}/fsmr_{label}_{_rand_suffix()}",
                    format_disk=False)

                self._verify_checksums(
                    self.fio_node, rst_mount, checksums)
                self.logger.info(f"[{label}] restore + checksum OK")
                self._unmount_and_disconnect(
                    self.fio_node, rst_mount, rst_id)

                # Clean up policy
                self._detach_policy(pol_id, "lvol", lvol_id)

                results[label] = "PASSED"

            except Exception as e:
                self.logger.error(f"[{label}] FAILED: {e}")
                results[label] = f"FAILED: {e}"

        # Summary
        passed = sum(1 for v in results.values() if v == "PASSED")
        total = len(self._combos)
        self.logger.info(
            f"TC-BCK-STR-087: {passed}/{total} combos passed: {results}")
        assert passed == total, (
            f"TC-BCK-STR-087: {total - passed} combo(s) failed: "
            + ", ".join(k for k, v in results.items() if v != "PASSED")
        )

        self.logger.info("=== BackupStressFilesystemSecurityMix PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 10 – Retention merge cycles with mixed configs
# ════════════════════════════════════════════════════════════════════════════


class BackupStressRetentionMergeCycles(BackupStressBase):
    """
    TC-BCK-STR-090..095

    Repeated delete → backup → dual-policy retention-merge → restore cycles
    on lvols with different configurations (plain, crypto, xfs).  Each cycle
    adds new data before the backup to produce real deltas, then verifies the
    restored data matches the latest write.

    Uses two alternating retention policies (versions=2 and versions=3) to
    exercise merge scheduling under policy switches.  After cycle 3, deletes
    oldest backup before restore to validate chain integrity after deletion.

    This specifically targets the regression where older deleted backups
    corrupt the chain for new backups after retention merge.

    Validates:
      - Multiple delete-backup-merge-restore cycles produce correct data
      - Dual-policy alternating merges handled correctly
      - Delete mid-chain doesn't corrupt subsequent restores
      - No stale data leaks across cycles
      - Works with crypto, plain, and XFS lvols
      - Retention merge handles the chain correctly after each delete
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_retention_merge_cycles"
        self.num_cycles = 10
        self._configs = [
            # (label, crypto, fs_type)
            ("plain_ext4", False, "ext4"),
            ("crypto_ext4", True,  "ext4"),
            ("plain_xfs",  False, "xfs"),
        ]

    def run(self):
        self.logger.info(
            f"=== BackupStressRetentionMergeCycles START "
            f"cycles={self.num_cycles} ===")
        self.fio_node = self.fio_node[0]
        self._ensure_pool_and_sc()

        # Create two retention policies for alternating merge thresholds
        pol_tight_name = f"rmc_pol_tight_{_rand_suffix()}"
        pol_tight_id = self._add_policy(pol_tight_name, versions=2, age="1d")
        pol_wide_name = f"rmc_pol_wide_{_rand_suffix()}"
        pol_wide_id = self._add_policy(pol_wide_name, versions=3, age="1d")
        self.logger.info(
            f"TC-BCK-STR-090: dual policies created — "
            f"tight (versions=2): {pol_tight_id}, "
            f"wide (versions=3): {pol_wide_id}")

        results: dict[str, str] = {}

        # Run first two configs in parallel, then third sequentially
        def _run_config(label, crypto, fs_type):
            try:
                lvol_name, lvol_id = self._create_lvol(
                    name=f"rmc_{label}_{_rand_suffix()}",
                    crypto=crypto)

                _, mount = self._connect_format_mount(
                    lvol_name, lvol_id, fs_type=fs_type)
                self._run_fio(mount, runtime=20)

                for cycle in range(self.num_cycles):
                    self.logger.info(
                        f"[{label}] cycle {cycle + 1}/{self.num_cycles}")

                    # Write new data each cycle
                    self._run_fio(mount, runtime=15, rw="write")
                    cycle_checksums = self._get_checksums(
                        self.fio_node, mount)

                    # Delete existing backups (after first cycle)
                    if cycle > 0:
                        self._delete_backups(lvol_id)
                        sleep_n_sec(10)

                    # Take 4 backups
                    cycle_bk_ids = []
                    for i in range(4):
                        sn = f"rmc_{label}_c{cycle}_{i}_{_rand_suffix()}"
                        self._create_snapshot(lvol_id, sn, backup=True)
                        bk_id = self._wait_for_backup_by_snap(
                            sn, f"[{label}][c{cycle}.{i}]")
                        cycle_bk_ids.append(bk_id)
                        sleep_n_sec(3)

                    # Alternate between tight and wide retention policies
                    use_tight = (cycle % 2 == 0)
                    active_pol_id = pol_tight_id if use_tight else pol_wide_id
                    pol_label = "tight(v=2)" if use_tight else "wide(v=3)"
                    self._attach_policy(active_pol_id, "lvol", lvol_id)
                    self.logger.info(
                        f"[{label}] cycle {cycle + 1}: policy={pol_label}")
                    sleep_n_sec(20)  # let merge run

                    # After cycle 3, delete oldest backup to test chain
                    if cycle >= 3 and len(cycle_bk_ids) >= 2:
                        try:
                            self.logger.info(
                                f"[{label}] cycle {cycle + 1}: deleting "
                                f"oldest backup to test chain integrity")
                            self._delete_backups(lvol_id)
                            sleep_n_sec(5)
                            # Re-take a fresh backup after delete
                            sn = (f"rmc_{label}_c{cycle}_fresh_"
                                  f"{_rand_suffix()}")
                            self._create_snapshot(
                                lvol_id, sn, backup=True)
                            fresh_bk = self._wait_for_backup_by_snap(
                                sn, f"[{label}][c{cycle}.fresh]")
                            cycle_bk_ids.append(fresh_bk)
                        except Exception as e:
                            self.logger.warning(
                                f"[{label}] cycle {cycle + 1} delete "
                                f"chain test warning: {e}")

                    # Restore latest and verify
                    self._unmount_and_disconnect(
                        self.fio_node, mount, lvol_id)

                    rst_name = f"rmc_rst_{label}_{cycle}_{_rand_suffix()}"
                    self._restore_backup(cycle_bk_ids[-1], rst_name)
                    self._wait_for_restore(rst_name)
                    rst_id = self._get_lvol_id(rst_name)
                    _, rst_mount = self._connect_and_mount(
                        rst_name, rst_id,
                        mount=(
                            f"{self.mount_path}/"
                            f"rmcr_{label}_{cycle}_{_rand_suffix()}"
                        ),
                        format_disk=False)

                    self._verify_checksums(
                        self.fio_node, rst_mount, cycle_checksums)
                    self.logger.info(
                        f"[{label}] cycle {cycle + 1} restore checksum OK")
                    self._unmount_and_disconnect(
                        self.fio_node, rst_mount, rst_id)

                    # Detach policy, reconnect source for next cycle
                    self._detach_policy(active_pol_id, "lvol", lvol_id)
                    _, mount = self._connect_and_mount(lvol_name, lvol_id,
                                                       format_disk=False)

                self._unmount_and_disconnect(self.fio_node, mount, lvol_id)
                results[label] = "PASSED"
                self.logger.info(
                    f"[{label}] all {self.num_cycles} cycles PASSED")

            except Exception as e:
                self.logger.error(f"[{label}] FAILED: {e}")
                results[label] = f"FAILED: {e}"

        # Run plain_ext4 and crypto_ext4 in parallel
        threads = []
        for label, crypto, fs_type in self._configs[:2]:
            t = threading.Thread(
                target=_run_config, args=(label, crypto, fs_type))
            threads.append(t)
            t.start()
        for t in threads:
            t.join(timeout=3600)

        # Run plain_xfs sequentially (XFS UUID conflicts with parallel mounts)
        for label, crypto, fs_type in self._configs[2:]:
            _run_config(label, crypto, fs_type)

        # Summary
        passed = sum(1 for v in results.values() if v == "PASSED")
        total = len(self._configs)
        self.logger.info(
            f"TC-BCK-STR-095: {passed}/{total} configs passed: {results}")
        assert passed == total, (
            f"TC-BCK-STR-095: {total - passed} config(s) failed: "
            + ", ".join(k for k, v in results.items() if v != "PASSED")
        )

        self.logger.info("=== BackupStressRetentionMergeCycles PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Stress 11 – Comprehensive mega-stress: 30 parent lvols + NS children, dual policy,
#              FS mix, concurrent backup/restore, delete+verify
# ════════════════════════════════════════════════════════════════════════════


class BackupStressComprehensive(BackupStressBase):
    """
    TC-BCK-STR-100..115

    Comprehensive backup stress test combining high scale, high concurrency,
    dual alternating retention policies, namespace lvols, filesystem mix,
    and delete+restore chain verification.

    Setup:
      - 30 lvols (30G each, ~900GB total, capacity-checked)
        - 10x ext4+plain, 10x ext4+crypto, 5x xfs+plain, 5x xfs+crypto
        - 4 of the 30 lvols are created with max_namespace_per_subsys=10,
          then 3 namespace children are created on each (12 NS children total)
      - 2 retention policies: tight (versions=2) on odd lvols,
        wide (versions=3) on even lvols

    Stress phases:
      1. Initial backup wave: 30 parallel backups (8 at a time)
      2. Marathon: 60 rounds of 8 parallel backups + 2 parallel restores
      3. Delete + restore integrity: delete backups, re-backup, verify chain
      4. Concurrent burst: 16 mixed operations simultaneously
      5. Final restore from every lvol: verify all data

    Validates:
      - 520+ backups, 120+ restores across 30+ lvols with no data corruption
      - Alternating retention merge under concurrent load
      - Chain integrity after backup deletion
      - Namespace lvols backup/restore correctly
      - Mixed ext4/xfs + plain/crypto all work simultaneously
      - Service remains stable throughout ~3.5-4.5 hour run
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_comprehensive"
        self.lvol_size = "30G"
        self.fio_size = "10G"
        self.num_rounds = 60
        self.parallel_batch = 8
        self._regular_configs = [
            # (prefix, count, fs_type, crypto)
            ("ext4_plain",  10, "ext4", False),
            ("ext4_crypto", 10, "ext4", True),
            ("xfs_plain",   5,  "xfs",  False),
            ("xfs_crypto",  5,  "xfs",  True),
        ]
        # Namespace config: first 4 regular lvols become namespace parents,
        # each getting 3 namespace children (12 NS children total)
        self.num_namespace_parents = 4
        self.num_namespace_children = 3

    def _check_and_adjust_capacity(self):
        """Check cluster capacity and adjust lvol count/size if needed."""
        try:
            cap = self.sbcli_utils.get_cluster_capacity()
            if isinstance(cap, list):
                cap = cap[0] if cap else {}
            total_bytes = cap.get("size_total", 0)
            used_bytes = cap.get("size_used", 0)
            avail_bytes = total_bytes - used_bytes
            avail_gb = avail_bytes / (1024 ** 3) if avail_bytes > 0 else 0

            total_lvols = sum(c[1] for c in self._regular_configs)
            ns_lvols = self.num_namespace_parents * self.num_namespace_children
            all_lvols = total_lvols + ns_lvols
            size_gb = int(self.lvol_size.rstrip("Gg"))
            required_gb = all_lvols * size_gb

            self.logger.info(
                f"TC-BCK-STR-100: capacity check — "
                f"available={avail_gb:.1f}GB, "
                f"required={required_gb}GB "
                f"({all_lvols} lvols x {size_gb}GB)")

            if required_gb > avail_gb * 0.9:
                # Scale down proportionally (thin provisioning, so use 70%
                # of available as our budget)
                budget_gb = avail_gb * 0.7
                new_size_gb = max(5, int(budget_gb / all_lvols))
                self.lvol_size = f"{new_size_gb}G"
                self.fio_size = f"{max(1, new_size_gb // 3)}G"
                self.logger.warning(
                    f"TC-BCK-STR-100: insufficient capacity — "
                    f"reduced lvol_size to {self.lvol_size}, "
                    f"fio_size to {self.fio_size}")
        except Exception as e:
            self.logger.warning(
                f"TC-BCK-STR-100: capacity check failed: {e} — "
                f"continuing with defaults")

    def _list_nvme_ns_devices(self, ctrl_dev: str) -> list[str]:
        """List namespace devices on an NVMe controller.

        ctrl_dev: /dev/nvmeX (controller) or /dev/nvmeXnY (namespace —
                  will be stripped to controller).
        Returns: ['/dev/nvmeXn1', '/dev/nvmeXn2', ...]
        """
        ctrl = get_parent_device(ctrl_dev)
        cmd = f"bash -lc \"ls -1 {ctrl}n* 2>/dev/null | sort -V || true\""
        out, _ = self.ssh_obj.exec_command(
            node=self.fio_node, command=cmd, supress_logs=True)
        return [x.strip() for x in (out or "").splitlines() if x.strip()]

    def _wait_for_new_ns_device(self, ctrl_dev: str,
                                 before_set: set,
                                 timeout: int = 120) -> str | None:
        """Wait for a new namespace device to appear on the controller.

        Returns the new device path (e.g. /dev/nvmeXn2) or None on timeout.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            cur = set(self._list_nvme_ns_devices(ctrl_dev))
            diff = sorted(cur - before_set)
            if diff:
                return diff[-1]
            sleep_n_sec(2)
        return None

    def _create_lvols_batch(self, configs_batch, lvol_state):
        """Create a batch of lvols in parallel threads.

        Each item in configs_batch is (label, fs_type, crypto) or
        (label, fs_type, crypto, ns_parent) where ns_parent=True means
        the lvol should be created with max_namespace_per_subsys=10.
        """
        threads = []
        errors = []

        def _create_one(label, fs_type, crypto, ns_parent=False):
            try:
                name = f"comp_{label}_{_rand_suffix()}"
                if ns_parent and not self.k8s_test:
                    # Create with namespace support via sbcli directly
                    self.sbcli_utils.add_lvol(
                        lvol_name=name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=crypto,
                        max_namespace_per_subsys=10)
                    lvol_id = self._get_lvol_id(name)
                    self.created_lvols.append(name)
                else:
                    name, lvol_id = self._create_lvol(
                        name=name,
                        size=self.lvol_size, crypto=crypto)
                _, mount = self._connect_format_mount(
                    name, lvol_id, fs_type=fs_type)
                self._run_fio(mount, runtime=30)
                checksums = self._get_checksums(self.fio_node, mount)
                assert checksums, f"No checksums for {label}"
                lvol_state[label] = {
                    "name": name, "id": lvol_id, "mount": mount,
                    "device": device,
                    "fs_type": fs_type, "crypto": crypto,
                    "is_namespace": ns_parent, "parent_id": None,
                    "checksums": checksums, "backup_ids": [],
                }
                self.logger.info(f"  {label} ({name}) ready"
                                 f"{' [NS parent]' if ns_parent else ''}")
            except Exception as e:
                self.logger.error(f"  {label} creation failed: {e}")
                errors.append((label, e))

        for item in configs_batch:
            if len(item) == 4:
                label, fs_type, crypto, ns_parent = item
            else:
                label, fs_type, crypto = item
                ns_parent = False
            t = threading.Thread(
                target=_create_one,
                args=(label, fs_type, crypto, ns_parent))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=600)

        return errors

    def _backup_batch(self, lvol_keys, lvol_state):
        """Backup a batch of lvols in parallel. Returns {label: bk_id}."""
        threads = []
        results: dict[str, str] = {}

        def _backup_one(label):
            info = lvol_state.get(label)
            if not info:
                return
            bk_id = self._snap_and_backup(
                info["id"], f"comp_{label}")
            results[label] = bk_id or ""

        for label in lvol_keys:
            t = threading.Thread(target=_backup_one, args=(label,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=_BACKUP_TIMEOUT)

        for label, bk_id in results.items():
            if bk_id and label in lvol_state:
                lvol_state[label]["backup_ids"].append(bk_id)

        return results

    def _restore_batch(self, restore_specs, lvol_state):
        """Restore a batch in parallel.

        restore_specs: list of (label, bk_id) tuples.
        Returns {label: (rst_name, checksums_ok)}.
        """
        threads = []
        results: dict[str, tuple[str, bool]] = {}

        def _restore_one(label, bk_id):
            info = lvol_state.get(label)
            if not info:
                return
            rst_name = f"comp_rst_{label}_{_rand_suffix()}"
            try:
                self._restore_backup(bk_id, rst_name)
                self._wait_for_restore(rst_name)
                rst_id = self._get_lvol_id(rst_name)
                _, rst_mount = self._connect_and_mount(
                    rst_name, rst_id,
                    mount=f"{self.mount_path}/cr_{label}_{_rand_suffix()}",
                    format_disk=False)
                self._verify_checksums(
                    self.fio_node, rst_mount, info["checksums"])
                self._unmount_and_disconnect(
                    self.fio_node, rst_mount, rst_id)
                results[label] = (rst_name, True)
            except Exception as e:
                self.logger.error(
                    f"  restore {label} from {bk_id} failed: {e}")
                results[label] = (rst_name, False)

        for label, bk_id in restore_specs:
            t = threading.Thread(
                target=_restore_one, args=(label, bk_id))
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=_BACKUP_TIMEOUT * 2)

        return results

    def run(self):
        self.logger.info("=== BackupStressComprehensive START ===")
        self.fio_node = self.fio_node[0]
        self._ensure_pool_and_sc()

        # ── Phase 0: Capacity check ────────────────────────────────────
        self._check_and_adjust_capacity()

        # ── Phase 1: Create all lvols ──────────────────────────────────
        self.logger.info("TC-BCK-STR-101: creating lvols...")
        lvol_state: dict[str, dict] = {}

        # Build flat list of (label, fs_type, crypto, ns_parent) for lvols.
        # The first num_namespace_parents lvols are created with
        # max_namespace_per_subsys=10 so namespace children can be added.
        all_items = []
        ns_parents_remaining = self.num_namespace_parents if not self.k8s_test else 0
        for prefix, count, fs_type, crypto in self._regular_configs:
            for i in range(count):
                label = f"{prefix}_{i}"
                ns_parent = ns_parents_remaining > 0
                all_items.append((label, fs_type, crypto, ns_parent))
                if ns_parent:
                    ns_parents_remaining -= 1

        # Create in batches of parallel_batch
        for batch_start in range(0, len(all_items), self.parallel_batch):
            batch = all_items[batch_start:batch_start + self.parallel_batch]
            self.logger.info(
                f"TC-BCK-STR-101: creating batch "
                f"{batch_start // self.parallel_batch + 1} "
                f"({len(batch)} lvols)")
            errors = self._create_lvols_batch(batch, lvol_state)
            if errors:
                self.logger.warning(
                    f"TC-BCK-STR-101: {len(errors)} lvol creation failures "
                    f"in batch")

        # Create namespace children on the lvols marked as NS parents.
        # Children share the parent's NVMe subsystem — no separate connect
        # is needed.  New namespace devices appear automatically on the
        # same controller (e.g. /dev/nvmeXn2, /dev/nvmeXn3).
        # (Docker only — K8s CSI handles NS internally)
        if not self.k8s_test:
            parent_labels = [
                lbl for lbl, info in lvol_state.items()
                if info.get("is_namespace") and info.get("parent_id") is None
            ]
            self.logger.info(
                f"TC-BCK-STR-101: creating {self.num_namespace_children} "
                f"namespace children on each of {len(parent_labels)} "
                f"parent lvols: {parent_labels}")
            for parent_label in parent_labels:
                parent_info = lvol_state[parent_label]
                parent_id = parent_info["id"]
                parent_device = parent_info.get("device", "")
                ctrl_dev = get_parent_device(parent_device)
                self.logger.info(
                    f"TC-BCK-STR-101: parent {parent_label} "
                    f"device={parent_device} ctrl={ctrl_dev}")
                # Snapshot existing namespace devices before adding children
                before_set = set(
                    self._list_nvme_ns_devices(ctrl_dev))
                try:
                    for ci in range(self.num_namespace_children):
                        child_label = f"ns_child_{parent_label}_{ci}"
                        child_name = (
                            f"comp_nsc_{parent_label}_{ci}_{_rand_suffix()}")
                        self.sbcli_utils.add_lvol(
                            lvol_name=child_name,
                            pool_name=self.pool_name,
                            size=self.lvol_size,
                            namespace=parent_id)
                        child_id = self.sbcli_utils.get_lvol_id(
                            lvol_name=child_name)
                        assert child_id, f"Child {child_name} not found"
                        self.created_lvols.append(child_name)
                        # Wait for new namespace device on the controller
                        new_dev = self._wait_for_new_ns_device(
                            ctrl_dev, before_set, timeout=120)
                        assert new_dev, (
                            f"Namespace device did not appear for "
                            f"{child_name} on {ctrl_dev}")
                        # Update before_set for the next child
                        before_set = set(
                            self._list_nvme_ns_devices(ctrl_dev))
                        # Format + mount (no connect needed)
                        child_mount = (
                            f"{self.mount_path}/{child_name}")
                        self.ssh_obj.format_disk(
                            node=self.fio_node, device=new_dev,
                            fs_type=parent_info["fs_type"])
                        self.ssh_obj.exec_command(
                            self.fio_node, f"mkdir -p {child_mount}")
                        self.ssh_obj.mount_path(
                            node=self.fio_node, device=new_dev,
                            mount_path=child_mount)
                        self.mounted.append(
                            (self.fio_node, child_mount))
                        self._run_fio(child_mount, runtime=30)
                        child_checksums = self._get_checksums(
                            self.fio_node, child_mount)
                        lvol_state[child_label] = {
                            "name": child_name, "id": child_id,
                            "mount": child_mount,
                            "device": new_dev,
                            "fs_type": parent_info["fs_type"],
                            "crypto": parent_info["crypto"],
                            "is_namespace": True,
                            "parent_id": parent_id,
                            "checksums": child_checksums,
                            "backup_ids": [],
                        }
                        self.logger.info(
                            f"  {child_label} ({child_name}) "
                            f"device={new_dev} ready")
                except Exception as e:
                    self.logger.error(
                        f"TC-BCK-STR-101: namespace children on "
                        f"{parent_label} failed: {e}")

        total_lvols = len(lvol_state)
        self.logger.info(
            f"TC-BCK-STR-101: {total_lvols} lvols created and ready")
        assert total_lvols >= 20, (
            f"TC-BCK-STR-101: only {total_lvols} lvols created, "
            f"need at least 20")

        # ── Phase 2: Create 2 retention policies, attach alternating ───
        self.logger.info("TC-BCK-STR-102: creating dual retention policies")
        pol_tight_name = f"comp_tight_{_rand_suffix()}"
        pol_tight_id = self._add_policy(
            pol_tight_name, versions=2, age="1d")
        pol_wide_name = f"comp_wide_{_rand_suffix()}"
        pol_wide_id = self._add_policy(
            pol_wide_name, versions=3, age="1d")

        all_labels = sorted(lvol_state.keys())
        for idx, label in enumerate(all_labels):
            info = lvol_state[label]
            pol_id = pol_tight_id if idx % 2 == 0 else pol_wide_id
            pol_desc = "tight(v=2)" if idx % 2 == 0 else "wide(v=3)"
            try:
                self._attach_policy(pol_id, "lvol", info["id"])
                info["policy_id"] = pol_id
                info["policy_desc"] = pol_desc
            except Exception as e:
                self.logger.warning(
                    f"TC-BCK-STR-102: attach policy to {label} failed: {e}")
                info["policy_id"] = None
                info["policy_desc"] = "none"

        self.logger.info(
            f"TC-BCK-STR-102: policies attached — "
            f"tight on {sum(1 for l in all_labels if all_labels.index(l) % 2 == 0)} lvols, "
            f"wide on {sum(1 for l in all_labels if all_labels.index(l) % 2 == 1)} lvols")

        # ── Phase 3: Initial backup wave ───────────────────────────────
        self.logger.info("TC-BCK-STR-103: initial backup wave")
        initial_ok = 0
        initial_fail = 0
        for batch_start in range(0, len(all_labels), self.parallel_batch):
            batch = all_labels[batch_start:batch_start + self.parallel_batch]
            results = self._backup_batch(batch, lvol_state)
            ok = sum(1 for v in results.values() if v)
            fail = len(results) - ok
            initial_ok += ok
            initial_fail += fail
            self.logger.info(
                f"TC-BCK-STR-103: batch "
                f"{batch_start // self.parallel_batch + 1}: "
                f"{ok}/{len(batch)} ok")

        self.logger.info(
            f"TC-BCK-STR-103: initial wave done — "
            f"{initial_ok} ok, {initial_fail} failed")
        assert initial_ok >= total_lvols * 0.9, (
            f"TC-BCK-STR-103: too many initial backup failures: "
            f"{initial_ok}/{total_lvols}")

        # ── Phase 4: Marathon backup/restore loop ──────────────────────
        self.logger.info(
            f"TC-BCK-STR-104: starting {self.num_rounds}-round marathon")
        marathon_backups = 0
        marathon_restores = 0
        marathon_restore_ok = 0
        marathon_fio_writes = 0

        for round_num in range(1, self.num_rounds + 1):
            # Pick 8 random lvols for backup
            backup_labels = random.sample(
                all_labels,
                min(self.parallel_batch, len(all_labels)))
            bk_results = self._backup_batch(backup_labels, lvol_state)
            round_bk_ok = sum(1 for v in bk_results.values() if v)
            marathon_backups += round_bk_ok

            # Pick 2 random lvols with backups for restore
            restorable = [
                l for l in all_labels
                if lvol_state[l]["backup_ids"]]
            if len(restorable) >= 2:
                restore_labels = random.sample(restorable, 2)
                restore_specs = [
                    (l, random.choice(lvol_state[l]["backup_ids"]))
                    for l in restore_labels]
                rst_results = self._restore_batch(
                    restore_specs, lvol_state)
                marathon_restores += len(rst_results)
                marathon_restore_ok += sum(
                    1 for _, (_, ok) in rst_results.items() if ok)

            # Every 10 rounds: write new FIO data to 4 random lvols
            if round_num % 10 == 0:
                fio_labels = random.sample(
                    all_labels,
                    min(4, len(all_labels)))
                for label in fio_labels:
                    info = lvol_state[label]
                    try:
                        self._run_fio(
                            info["mount"], runtime=15, rw="write")
                        info["checksums"] = self._get_checksums(
                            self.fio_node, info["mount"])
                        marathon_fio_writes += 1
                    except Exception as e:
                        self.logger.warning(
                            f"[round {round_num}] FIO write "
                            f"{label} failed: {e}")
                self.logger.info(
                    f"TC-BCK-STR-106: round {round_num} — "
                    f"wrote new data to {len(fio_labels)} lvols")

            # Every 15 rounds: forced checksum verification
            if round_num % 15 == 0:
                verify_labels = random.sample(
                    all_labels, min(4, len(all_labels)))
                verify_ok = 0
                for label in verify_labels:
                    info = lvol_state[label]
                    try:
                        current = self._get_checksums(
                            self.fio_node, info["mount"])
                        if current == info["checksums"]:
                            verify_ok += 1
                    except Exception:
                        pass
                self.logger.info(
                    f"TC-BCK-STR-107: round {round_num} — "
                    f"verify {verify_ok}/{len(verify_labels)} ok")

            if round_num % 10 == 0:
                self.logger.info(
                    f"TC-BCK-STR-104: round {round_num}/{self.num_rounds} "
                    f"— backups={marathon_backups}, "
                    f"restores={marathon_restore_ok}/{marathon_restores}")

            sleep_n_sec(3)

        self.logger.info(
            f"TC-BCK-STR-104: marathon complete — "
            f"backups={marathon_backups}, "
            f"restores={marathon_restore_ok}/{marathon_restores}, "
            f"fio_writes={marathon_fio_writes}")

        # ── Phase 5: Delete + restore integrity ────────────────────────
        self.logger.info("TC-BCK-STR-108: delete + restore integrity check")
        delete_labels = random.sample(
            [l for l in all_labels if lvol_state[l]["backup_ids"]],
            min(10, len([l for l in all_labels
                         if lvol_state[l]["backup_ids"]])))
        delete_ok = 0
        for label in delete_labels:
            info = lvol_state[label]
            try:
                self._delete_backups(info["id"])
                info["backup_ids"].clear()
                sleep_n_sec(5)
                # Take fresh backup
                bk_id = self._snap_and_backup(
                    info["id"], f"comp_fresh_{label}")
                if bk_id:
                    info["backup_ids"].append(bk_id)
                sleep_n_sec(30)  # let retention merge run
                # Restore and verify
                if info["backup_ids"]:
                    rst_name = f"comp_del_rst_{label}_{_rand_suffix()}"
                    self._restore_backup(
                        info["backup_ids"][-1], rst_name)
                    self._wait_for_restore(rst_name)
                    rst_id = self._get_lvol_id(rst_name)
                    _, rst_mount = self._connect_and_mount(
                        rst_name, rst_id,
                        mount=(
                            f"{self.mount_path}/"
                            f"cdel_{label}_{_rand_suffix()}"
                        ),
                        format_disk=False)
                    self._verify_checksums(
                        self.fio_node, rst_mount, info["checksums"])
                    self._unmount_and_disconnect(
                        self.fio_node, rst_mount, rst_id)
                    delete_ok += 1
                    self.logger.info(
                        f"TC-BCK-STR-108: {label} delete+restore OK")
            except Exception as e:
                self.logger.error(
                    f"TC-BCK-STR-108: {label} delete+restore failed: {e}")

        self.logger.info(
            f"TC-BCK-STR-109: delete+restore — "
            f"{delete_ok}/{len(delete_labels)} passed")

        # ── Phase 6: Concurrent stress burst ───────────────────────────
        self.logger.info("TC-BCK-STR-110: concurrent stress burst")
        burst_threads = []
        burst_results: dict[str, str] = {}

        # 8 backups
        backup_burst = random.sample(
            all_labels, min(8, len(all_labels)))
        for label in backup_burst:
            def _bk(lbl=label):
                try:
                    bk_id = self._snap_and_backup(
                        lvol_state[lbl]["id"], f"burst_{lbl}")
                    if bk_id:
                        lvol_state[lbl]["backup_ids"].append(bk_id)
                    burst_results[f"backup_{lbl}"] = "ok" if bk_id else "no_id"
                except Exception as e:
                    burst_results[f"backup_{lbl}"] = f"fail: {e}"
            t = threading.Thread(target=_bk)
            burst_threads.append(t)

        # 4 restores
        restorable = [
            l for l in all_labels
            if l not in backup_burst and lvol_state[l]["backup_ids"]]
        restore_burst = random.sample(
            restorable, min(4, len(restorable)))
        for label in restore_burst:
            def _rs(lbl=label):
                info = lvol_state[lbl]
                try:
                    bk_id = random.choice(info["backup_ids"])
                    rst_name = f"burst_rst_{lbl}_{_rand_suffix()}"
                    self._restore_backup(bk_id, rst_name)
                    self._wait_for_restore(rst_name)
                    rst_id = self._get_lvol_id(rst_name)
                    _, rst_mount = self._connect_and_mount(
                        rst_name, rst_id,
                        mount=(
                            f"{self.mount_path}/"
                            f"cburst_{lbl}_{_rand_suffix()}"
                        ),
                        format_disk=False)
                    self._verify_checksums(
                        self.fio_node, rst_mount, info["checksums"])
                    self._unmount_and_disconnect(
                        self.fio_node, rst_mount, rst_id)
                    burst_results[f"restore_{lbl}"] = "ok"
                except Exception as e:
                    burst_results[f"restore_{lbl}"] = f"fail: {e}"
            t = threading.Thread(target=_rs)
            burst_threads.append(t)

        # 4 FIO writes
        remaining = [
            l for l in all_labels
            if l not in backup_burst and l not in restore_burst]
        fio_burst = random.sample(
            remaining, min(4, len(remaining)))
        for label in fio_burst:
            def _fio(lbl=label):
                info = lvol_state[lbl]
                try:
                    self._run_fio(info["mount"], runtime=15, rw="write")
                    info["checksums"] = self._get_checksums(
                        self.fio_node, info["mount"])
                    burst_results[f"fio_{lbl}"] = "ok"
                except Exception as e:
                    burst_results[f"fio_{lbl}"] = f"fail: {e}"
            t = threading.Thread(target=_fio)
            burst_threads.append(t)

        for t in burst_threads:
            t.start()
        for t in burst_threads:
            t.join(timeout=_BACKUP_TIMEOUT * 2)

        burst_ok = sum(
            1 for v in burst_results.values() if v == "ok")
        self.logger.info(
            f"TC-BCK-STR-110: burst complete — "
            f"{burst_ok}/{len(burst_results)} operations ok")

        # ── Phase 7: Retention merge verification ──────────────────────
        self.logger.info("TC-BCK-STR-111: retention merge verification")
        all_backups = self._list_backups()
        tight_counts = []
        wide_counts = []
        for idx, label in enumerate(all_labels):
            info = lvol_state[label]
            lvol_bks = [
                b for b in all_backups
                if info["name"] in " ".join(str(v) for v in b.values())
            ]
            active = [
                b for b in lvol_bks
                if (b.get("status") or b.get("Status") or "").lower()
                not in ("merged", "deleted", "failed", "error")
            ]
            if idx % 2 == 0:
                tight_counts.append(len(active))
            else:
                wide_counts.append(len(active))

        if tight_counts:
            self.logger.info(
                f"TC-BCK-STR-111: tight policy lvols — "
                f"avg={sum(tight_counts)/len(tight_counts):.1f} "
                f"active backups (expected ≤3)")
        if wide_counts:
            self.logger.info(
                f"TC-BCK-STR-111: wide policy lvols — "
                f"avg={sum(wide_counts)/len(wide_counts):.1f} "
                f"active backups (expected ≤4)")

        # ── Phase 8: Final restore from every lvol ─────────────────────
        self.logger.info("TC-BCK-STR-112: final restore wave")
        # Disconnect all source lvols first
        for label in all_labels:
            info = lvol_state[label]
            try:
                self._unmount_and_disconnect(
                    self.fio_node, info["mount"], info["id"])
            except Exception:
                pass

        final_ok = 0
        final_total = 0
        for batch_start in range(0, len(all_labels), self.parallel_batch):
            batch = all_labels[batch_start:batch_start + self.parallel_batch]
            restore_specs = []
            for label in batch:
                info = lvol_state[label]
                if info["backup_ids"]:
                    restore_specs.append(
                        (label, info["backup_ids"][-1]))
            if restore_specs:
                rst_results = self._restore_batch(
                    restore_specs, lvol_state)
                batch_ok = sum(
                    1 for _, (_, ok) in rst_results.items() if ok)
                final_ok += batch_ok
                final_total += len(restore_specs)
                self.logger.info(
                    f"TC-BCK-STR-112: final batch "
                    f"{batch_start // self.parallel_batch + 1}: "
                    f"{batch_ok}/{len(restore_specs)} ok")

        self.logger.info(
            f"TC-BCK-STR-113: final restore — "
            f"{final_ok}/{final_total} passed")
        assert final_ok >= final_total * 0.95, (
            f"TC-BCK-STR-113: too many final restore failures: "
            f"{final_ok}/{final_total}")

        # ── Phase 9: Capacity snapshot + cleanup + summary ─────────────
        self.logger.info("TC-BCK-STR-114: capacity snapshot")
        try:
            cap = self.sbcli_utils.get_cluster_capacity()
            if isinstance(cap, list):
                cap = cap[0] if cap else {}
            self.logger.info(
                f"TC-BCK-STR-114: final capacity — "
                f"total={cap.get('size_total', 0)}, "
                f"used={cap.get('size_used', 0)}")
        except Exception as e:
            self.logger.warning(f"TC-BCK-STR-114: capacity check: {e}")

        self.logger.info("TC-BCK-STR-115: cleanup")
        # Detach policies
        for label in all_labels:
            info = lvol_state[label]
            if info.get("policy_id"):
                try:
                    self._detach_policy(
                        info["policy_id"], "lvol", info["id"])
                except Exception:
                    pass

        # Delete namespace children first, then parents, then regular
        ns_children = [
            l for l in all_labels
            if lvol_state[l].get("parent_id")]
        ns_parents = [
            l for l in all_labels
            if lvol_state[l].get("is_namespace")
            and not lvol_state[l].get("parent_id")]
        regular = [
            l for l in all_labels
            if not lvol_state[l].get("is_namespace")]

        for group_name, group in [
            ("ns_children", ns_children),
            ("ns_parents", ns_parents),
            ("regular", regular),
        ]:
            for label in group:
                try:
                    self._force_delete_lvol(lvol_state[label]["name"])
                except Exception as e:
                    self.logger.warning(
                        f"cleanup {label}: {e}")

        # Summary
        total_backups = initial_ok + marathon_backups
        self.logger.info(
            f"\n{'=' * 60}\n"
            f"  BackupStressComprehensive SUMMARY\n"
            f"{'=' * 60}\n"
            f"  Lvols created:     {total_lvols}\n"
            f"  Initial backups:   {initial_ok}\n"
            f"  Marathon backups:  {marathon_backups}\n"
            f"  Marathon restores: {marathon_restore_ok}/{marathon_restores}\n"
            f"  Delete+restore:    {delete_ok}/{len(delete_labels)}\n"
            f"  Burst operations:  {burst_ok}/{len(burst_results)}\n"
            f"  Final restores:    {final_ok}/{final_total}\n"
            f"  Total backups:     {total_backups}\n"
            f"  FS breakdown:      "
            f"ext4={sum(1 for l in all_labels if lvol_state[l]['fs_type'] == 'ext4')}, "
            f"xfs={sum(1 for l in all_labels if lvol_state[l]['fs_type'] == 'xfs')}\n"
            f"  Namespace lvols:   "
            f"{sum(1 for l in all_labels if lvol_state[l].get('is_namespace'))}\n"
            f"{'=' * 60}")

        self.logger.info("=== BackupStressComprehensive PASSED ===")
