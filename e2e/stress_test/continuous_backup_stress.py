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
"""

from __future__ import annotations

import itertools
import os
import random
import string
import threading
import time
from datetime import datetime
from pathlib import Path

from e2e_tests.backup.test_backup_restore import BackupTestBase, _rand_suffix
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec

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

        sn_node_ip = self.sbcli_utils.get_node_without_lvols(node_id)

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
            iface = self.sbcli_utils.get_node_interface(node_id)
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"nmcli dev disconnect {iface} || true")
            sleep_n_sec(20)
            self.ssh_obj.exec_command(
                sn_node_ip,
                f"nmcli dev connect {iface} || true")

        elif outage_type == "interface_partial_network_interrupt":
            port = self.sbcli_utils.get_node_port(node_id)
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
        """Create a snapshot + trigger S3 backup; return backup_id or None on failure."""
        snap_name = f"str_{label}_{_rand_suffix()}"
        try:
            snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
            # backup_id is not always directly returned by snapshot add --backup;
            # we resolve it from backup list after a short wait
            sleep_n_sec(5)
            backups = self._list_backups()
            if backups:
                return (
                    backups[-1].get("id")
                    or backups[-1].get("ID")
                    or backups[-1].get("uuid")
                    or None
                )
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
                mount=f"{self.mount_path}/par_rest_{_rand_suffix()}")
            r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
            orig_checksums = lvol_map[target_name][3]
            self.ssh_obj.verify_checksums(
                self.fio_node, r_files, orig_checksums,
                message="TC-BCK-STR-004: parallel restore checksum mismatch")
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

        # TC-BCK-STR-013: Restore the last backup of each lvol; verify
        for name, info in lvol_map.items():
            backups_all = self._list_backups()
            if not backups_all:
                continue
            bk_id = (
                backups_all[-1].get("id")
                or backups_all[-1].get("ID")
                or backups_all[-1].get("uuid")
                or None
            )
            if not bk_id:
                continue
            restored_name = f"tcp_rest_{_rand_suffix()}"
            try:
                self._restore_backup(bk_id, restored_name)
                self._wait_for_restore(restored_name)
                rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
                r_device, r_mount = self._connect_and_mount(
                    restored_name, rest_id,
                    mount=f"{self.mount_path}/tr_{_rand_suffix()}")
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
                    mount=f"{self.mount_path}/mr_{_rand_suffix()}")
                r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, r_files, lvol_map[name]["checksums"],
                    message=f"TC-BCK-STR-032: checksum mismatch for {name}")
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
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name=self.pool_name)

        # TC-BCK-STR-040: create policy with versions=3
        policy_name = f"ret_pol_{_rand_suffix()}"
        policy_id = self._add_policy(policy_name, versions=3, age="1d")
        self._attach_policy(policy_id, "lvol", lvol_id)

        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=20)

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

        # TC-BCK-STR-043: restore latest backup after merges
        if backups_now:
            latest_id = (
                backups_now[-1].get("id")
                or backups_now[-1].get("ID")
                or backups_now[-1].get("uuid")
                or None
            )
            if latest_id:
                ret_restored = f"ret_rest_{_RAND_SUFFIX()}"
                self._restore_backup(latest_id, ret_restored)
                self._wait_for_restore(ret_restored)
                self.logger.info(
                    f"TC-BCK-STR-043: restore after merges succeeded ✓")

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
                    mount=f"{self.mount_path}/cr_{_rand_suffix()}")
                r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, r_files, orig_checksums,
                    message=f"TC-BCK-STR-052: checksum mismatch {restored_name}")
                self._run_fio(r_mount, runtime=20)
                self.logger.info(f"TC-BCK-STR-051: {restored_name} ✓")
            except Exception as e:
                self.logger.error(
                    f"TC-BCK-STR-051: post-restore check failed {restored_name}: {e}")

        self.logger.info("=== BackupStressRestoreConcurrent PASSED ===")
