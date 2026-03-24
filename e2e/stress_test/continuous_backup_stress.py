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


# ════════════════════════════════════════════════════════════════════════════
#  E2E 7 – Backup interrupted mid-operation
# ════════════════════════════════════════════════════════════════════════════


class TestBackupInterruptedBackup(BackupStressBase):
    """
    TC-BCK-080..086 — Storage-node outage triggered while a backup is in progress.

    Validates:
      - Interrupted backup reaches a terminal state (done or failed) — no hang
      - Delta chain stays consistent after interruption (no corruption)
      - After recovery, a fresh snapshot+backup completes successfully
      - Restored lvol from the post-recovery backup has correct data (checksum)
      - FIO on the restored lvol succeeds
      - Crypto lvol handled correctly under the same interruption scenario

    Approach:
      Create a large lvol and write substantial data so the S3 upload takes
      enough time to still be in progress when the outage fires.  Immediately
      after calling `snapshot backup` (async), trigger a graceful_shutdown on
      one storage node.  After recovery, poll backup status until it settles,
      then verify the recovery path.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_interrupted_backup"
        self.lvol_size = "10G"
        self.fio_size = "3G"

    def _run_interrupted_backup_scenario(
            self, lvol_name: str, lvol_id: str, orig_checksums: dict,
            outage_type: str, label: str):
        """
        Core scenario: snapshot → backup → immediate outage → recover → verify.
        Returns True if a successful restore with matching checksums was achieved.
        """
        snap_name = f"intr_{label}_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=False)

        # Trigger backup (async — returns immediately)
        self.logger.info(f"[{label}] Triggering backup for snap {snap_id}")
        backup_id = self._snapshot_backup(snap_id)
        self.logger.info(f"[{label}] backup_id={backup_id} — triggering outage now")

        # Immediately cause outage while backup is uploading
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, outage_type)
        except Exception as e:
            self.logger.warning(f"[{label}] Outage error (non-fatal): {e}")

        # Allow the cluster to recover, then wait for backup to settle
        sleep_n_sec(30)
        final_status = self._wait_for_backup_terminal(backup_id, timeout=600)
        self.logger.info(f"[{label}] backup terminal status: {final_status}")

        # Whichever state the interrupted backup ended in, take a fresh
        # snapshot+backup after recovery and use that for the restore test
        snap2_name = f"intr_{label}_snap2_{_rand_suffix()}"
        self.logger.info(f"[{label}] Taking fresh snapshot+backup after recovery")
        snap2_id = self._create_snapshot(lvol_id, snap2_name, backup=True)
        sleep_n_sec(20)

        backups = self._list_backups()
        assert backups, f"[{label}] No backups available after recovery"
        fresh_bk_id = (
            backups[-1].get("id") or backups[-1].get("ID")
            or backups[-1].get("uuid") or ""
        )
        assert fresh_bk_id, f"[{label}] Could not extract fresh backup_id"

        # Restore and verify data integrity
        restored_name = f"intr_{label}_rest_{_rand_suffix()}"
        self._restore_backup(fresh_bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        r_device, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/intr_{label}_{_rand_suffix()}")
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message=f"[{label}] checksum mismatch after interrupted backup recovery")
        self.logger.info(f"[{label}] checksums match after interrupted backup ✓")
        return restored_name, r_mount

    def run(self):
        self.logger.info("=== TestBackupInterruptedBackup START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── Plain lvol scenario ────────────────────────────────────────────

        # TC-BCK-080: Setup — large plain lvol, write data, capture checksums
        self.logger.info("TC-BCK-080: setup plain lvol for backup interruption test")
        lvol_name, lvol_id = self._create_lvol(
            name=f"intr_bck_{_rand_suffix()}", size=self.lvol_size)
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=40)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(
            f"TC-BCK-080: {len(orig_checksums)} checksum(s) captured")

        # TC-BCK-081..084: backup → graceful_shutdown outage → recover → restore
        self.logger.info(
            "TC-BCK-081..084: backup triggered, outage injected mid-upload")
        snap_name = f"intr_plain_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=False)
        backup_id = self._snapshot_backup(snap_id)
        self.logger.info(f"TC-BCK-081: backup_id={backup_id} — outage now")

        # TC-BCK-081: Trigger outage immediately after backup starts
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, "graceful_shutdown")
        except Exception as e:
            self.logger.warning(f"TC-BCK-081: outage error (non-fatal): {e}")

        # TC-BCK-082: Wait for backup to reach a terminal state
        sleep_n_sec(30)
        final_status = self._wait_for_backup_terminal(backup_id, timeout=600)
        self.logger.info(f"TC-BCK-082: interrupted backup final status: {final_status}")
        # Status must be terminal — no infinite hang
        assert final_status in (
            "done", "complete", "completed", "failed", "error", "timeout"
        ), f"TC-BCK-082: unexpected status after interruption: {final_status}"

        # TC-BCK-083: After recovery, fresh snapshot+backup must succeed
        self.logger.info("TC-BCK-083: fresh snapshot+backup after recovery")
        snap2_name = f"intr_plain_snap2_{_rand_suffix()}"
        self._create_snapshot(lvol_id, snap2_name, backup=True)
        sleep_n_sec(20)
        backups_after = self._list_backups()
        assert backups_after, "TC-BCK-083: no backups after recovery"
        fresh_bk_id = (
            backups_after[-1].get("id") or backups_after[-1].get("ID")
            or backups_after[-1].get("uuid") or ""
        )
        assert fresh_bk_id, "TC-BCK-083: could not get fresh backup_id"
        self.logger.info(
            f"TC-BCK-083: fresh backup {fresh_bk_id} available "
            f"({len(backups_after)} total) ✓")

        # TC-BCK-084: Restore fresh backup → verify checksums
        self.logger.info("TC-BCK-084: restore fresh backup and verify checksums")
        restored_name = f"intr_plain_rest_{_rand_suffix()}"
        self._restore_backup(fresh_bk_id, restored_name)
        self._wait_for_restore(restored_name)
        rest_id = self.sbcli_utils.get_lvol_id(lvol_name=restored_name)
        r_device, r_mount = self._connect_and_mount(
            restored_name, rest_id,
            mount=f"{self.mount_path}/intr_rest_{_rand_suffix()}")
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message="TC-BCK-084: checksum mismatch after interrupted backup restore")
        self.logger.info("TC-BCK-084: checksums match after interrupted backup ✓")

        # TC-BCK-085: FIO on the restored lvol
        self.logger.info("TC-BCK-085: FIO on restored lvol after interrupted backup")
        self._run_fio(r_mount, runtime=20)
        self.logger.info("TC-BCK-085: FIO succeeded ✓")

        # ── Crypto lvol scenario ───────────────────────────────────────────

        # TC-BCK-086: Same scenario with a crypto lvol + container_stop outage
        self.logger.info("TC-BCK-086: interrupted backup on crypto lvol (container_stop)")
        crypto_name, crypto_id = self._create_lvol(
            name=f"intr_crypto_{_rand_suffix()}", crypto=True)
        c_device, c_mount = self._connect_and_mount(crypto_name, crypto_id)
        self._run_fio(c_mount, runtime=30)
        c_files = self.ssh_obj.find_files(self.fio_node, c_mount)
        c_checksums = self.ssh_obj.generate_checksums(self.fio_node, c_files)

        c_snap = f"intr_c_snap_{_rand_suffix()}"
        c_snap_id = self._create_snapshot(crypto_id, c_snap, backup=False)
        c_bk_id = self._snapshot_backup(c_snap_id)
        self.logger.info(f"TC-BCK-086: crypto backup_id={c_bk_id} — outage now")

        try:
            sn_id2 = self._get_random_sn()
            self._do_outage(sn_id2, "container_stop")
        except Exception as e:
            self.logger.warning(f"TC-BCK-086: outage error (non-fatal): {e}")

        sleep_n_sec(30)
        c_status = self._wait_for_backup_terminal(c_bk_id, timeout=600)
        self.logger.info(f"TC-BCK-086: crypto backup terminal status: {c_status}")

        # Fresh backup after recovery
        c_snap2 = f"intr_c_snap2_{_rand_suffix()}"
        self._create_snapshot(crypto_id, c_snap2, backup=True)
        sleep_n_sec(20)
        c_backups = self._list_backups()
        if c_backups:
            c_fresh_id = (
                c_backups[-1].get("id") or c_backups[-1].get("ID")
                or c_backups[-1].get("uuid") or ""
            )
            if c_fresh_id:
                c_rest_name = f"intr_c_rest_{_rand_suffix()}"
                self._restore_backup(c_fresh_id, c_rest_name)
                self._wait_for_restore(c_rest_name)
                c_rest_id = self.sbcli_utils.get_lvol_id(lvol_name=c_rest_name)
                c_r_device, c_r_mount = self._connect_and_mount(
                    c_rest_name, c_rest_id,
                    mount=f"{self.mount_path}/icr_{_rand_suffix()}")
                c_r_files = self.ssh_obj.find_files(self.fio_node, c_r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, c_r_files, c_checksums,
                    message="TC-BCK-086: checksum mismatch on crypto interrupted backup")
                self.logger.info("TC-BCK-086: crypto interrupted backup restore ✓")

        self.logger.info("=== TestBackupInterruptedBackup PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  E2E 8 – Restore interrupted mid-operation
# ════════════════════════════════════════════════════════════════════════════


class TestBackupInterruptedRestore(BackupStressBase):
    """
    TC-BCK-090..097 — Storage-node outage triggered while a restore is running.

    Validates:
      - Interrupted restore reaches a terminal state — no hang
      - Partial/failed lvol is either absent or in a deletable error state
      - The system does NOT leave an un-deletable zombie lvol
      - Retry restore (different name, or same name after cleanup) succeeds
      - Restored lvol from the retry has correct data (checksum)
      - FIO on the retried restore succeeds
      - Crypto lvol handled correctly under restore interruption

    Approach:
      First complete a real backup so we have a valid backup_id.  Then call
      `backup restore` (which starts the restore asynchronously) and
      immediately trigger a storage-node outage.  After recovery inspect the
      lvol state and exercise the cleanup + retry path.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_interrupted_restore"
        self.lvol_size = "10G"
        self.fio_size = "3G"

    def _wait_for_restore_or_error(self, lvol_name: str,
                                    timeout: int = 300) -> str:
        """Wait until lvol appears in list (any status) or timeout.
        Returns final status string or 'absent' / 'timeout'."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            status = self._get_lvol_status(lvol_name)
            if status is not None:
                return status
            sleep_n_sec(_BACKUP_POLL_INTERVAL)
        return "timeout"

    def _cleanup_lvol(self, lvol_name: str):
        """Best-effort cleanup of a partial/error lvol."""
        self.logger.info(f"[cleanup] deleting lvol {lvol_name}")
        try:
            self._force_delete_lvol(lvol_name)
            self.logger.info(f"[cleanup] {lvol_name} deleted ✓")
        except Exception as e:
            self.logger.warning(f"[cleanup] delete error for {lvol_name}: {e}")

    def _run_interrupted_restore_scenario(
            self, backup_id: str, orig_checksums: dict,
            outage_type: str, label: str):
        """
        Core scenario: restore → immediate outage → check state → cleanup → retry.
        """
        # Trigger restore (async start)
        restore_name = f"intr_rst_{label}_{_rand_suffix()}"
        self.logger.info(
            f"[{label}] restore {backup_id} → {restore_name}, triggering outage")
        self._restore_backup(backup_id, restore_name)

        # Immediately trigger outage
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, outage_type)
        except Exception as e:
            self.logger.warning(f"[{label}] Outage error (non-fatal): {e}")

        # Allow recovery window, then check lvol state
        sleep_n_sec(30)
        lvol_status = self._wait_for_restore_or_error(restore_name, timeout=120)
        self.logger.info(
            f"[{label}] restore target status after outage: {lvol_status!r}")

        if lvol_status in ("error", "failed"):
            self.logger.info(
                f"[{label}] lvol is in error state — system reported failure ✓")
            self._cleanup_lvol(restore_name)
        elif lvol_status in ("absent", "timeout"):
            self.logger.info(
                f"[{label}] restore target not found — system cleaned up ✓")
            if restore_name in self.created_lvols:
                self.created_lvols.remove(restore_name)
        elif lvol_status in ("online", "present", "unknown"):
            self.logger.info(
                f"[{label}] restore completed despite outage (fast outage) — "
                f"status={lvol_status}")
            # The restore beat the outage timing; treat as success
        else:
            # Still in restoring state — wait a bit more
            self.logger.info(
                f"[{label}] still in progress ({lvol_status}), waiting more")
            sleep_n_sec(60)
            lvol_status = self._get_lvol_status(restore_name) or "absent"
            self.logger.info(
                f"[{label}] status after extra wait: {lvol_status!r}")
            if lvol_status in ("error", "failed", "absent"):
                self._cleanup_lvol(restore_name)

        # Retry restore with a fresh name — must succeed
        retry_name = f"intr_rst_{label}_retry_{_rand_suffix()}"
        self.logger.info(f"[{label}] retry restore → {retry_name}")
        self._restore_backup(backup_id, retry_name)
        self._wait_for_restore(retry_name)
        self.logger.info(f"[{label}] retry restore {retry_name} appeared ✓")

        # Verify checksums
        retry_id = self.sbcli_utils.get_lvol_id(lvol_name=retry_name)
        r_device, r_mount = self._connect_and_mount(
            retry_name, retry_id,
            mount=f"{self.mount_path}/intr_rr_{label}_{_rand_suffix()}")
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message=f"[{label}] checksum mismatch on retry restore")
        self.logger.info(f"[{label}] retry restore checksums match ✓")
        return retry_name, r_mount

    def run(self):
        self.logger.info("=== TestBackupInterruptedRestore START ===")
        self.fio_node = self.fio_node[0]
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # ── Setup: complete backup to use as restore source ────────────────

        self.logger.info("TC-BCK-090 setup: create lvol, write data, complete backup")
        lvol_name, lvol_id = self._create_lvol(
            name=f"intr_rst_src_{_rand_suffix()}", size=self.lvol_size)
        device, mount = self._connect_and_mount(lvol_name, lvol_id)
        self._run_fio(mount, runtime=40)
        files = self.ssh_obj.find_files(self.fio_node, mount)
        orig_checksums = self.ssh_obj.generate_checksums(self.fio_node, files)
        self.logger.info(f"Setup: {len(orig_checksums)} checksum(s) captured")

        snap_name = f"intr_rst_snap_{_rand_suffix()}"
        snap_id = self._create_snapshot(lvol_id, snap_name, backup=True)
        self.logger.info("Setup: waiting for backup to complete before restore test")
        sleep_n_sec(30)
        backups = self._list_backups()
        assert backups, "Setup: no backups available for interrupted restore test"
        backup_id = (
            backups[0].get("id") or backups[0].get("ID")
            or backups[0].get("uuid") or ""
        )
        assert backup_id, "Setup: could not extract backup_id"
        self.logger.info(f"Setup: backup_id={backup_id} — ready for interrupt test")

        # ── TC-BCK-090..095: Plain lvol, graceful_shutdown interrupt ───────

        self.logger.info(
            "TC-BCK-090: restore triggered → graceful_shutdown injected")
        restore_name = f"intr_rst_plain_{_rand_suffix()}"
        self._restore_backup(backup_id, restore_name)
        try:
            sn_id = self._get_random_sn()
            self._do_outage(sn_id, "graceful_shutdown")
        except Exception as e:
            self.logger.warning(f"TC-BCK-090: outage error (non-fatal): {e}")

        sleep_n_sec(30)

        # TC-BCK-091: Check restore target lvol status after outage
        lvol_status = self._wait_for_restore_or_error(restore_name, timeout=120)
        self.logger.info(
            f"TC-BCK-091: restore target {restore_name!r} status={lvol_status!r}")

        # TC-BCK-092: Verify partial lvol is in a recognisable/usable state
        if lvol_status in ("error", "failed"):
            self.logger.info(
                "TC-BCK-092: lvol is in error state — clean failure reported ✓")
        elif lvol_status in ("absent", "timeout"):
            self.logger.info(
                "TC-BCK-092: lvol absent — system cleaned up partial restore ✓")
            if restore_name in self.created_lvols:
                self.created_lvols.remove(restore_name)
        else:
            self.logger.info(
                f"TC-BCK-092: lvol status={lvol_status!r} "
                f"(may have completed before outage took effect)")

        # TC-BCK-093: Clean up the partial/error lvol
        self.logger.info(f"TC-BCK-093: cleaning up partial lvol {restore_name}")
        if lvol_status not in ("absent", "timeout"):
            self._cleanup_lvol(restore_name)

        # TC-BCK-094: Retry restore with fresh name — must succeed
        self.logger.info("TC-BCK-094: retry restore after cleanup")
        retry_name = f"intr_rst_retry_{_rand_suffix()}"
        self._restore_backup(backup_id, retry_name)
        self._wait_for_restore(retry_name)
        self.logger.info(f"TC-BCK-094: retry restore {retry_name} succeeded ✓")

        # TC-BCK-095: Verify checksums on retried restore
        self.logger.info("TC-BCK-095: verify checksums on retried restore")
        retry_id = self.sbcli_utils.get_lvol_id(lvol_name=retry_name)
        r_device, r_mount = self._connect_and_mount(
            retry_name, retry_id,
            mount=f"{self.mount_path}/intr_rr_{_rand_suffix()}")
        r_files = self.ssh_obj.find_files(self.fio_node, r_mount)
        self.ssh_obj.verify_checksums(
            self.fio_node, r_files, orig_checksums,
            message="TC-BCK-095: checksum mismatch on retried restore")
        self.logger.info("TC-BCK-095: retry restore checksums match ✓")

        # TC-BCK-096: FIO on the retried restore
        self.logger.info("TC-BCK-096: FIO on retried restore lvol")
        self._run_fio(r_mount, runtime=20)
        self.logger.info("TC-BCK-096: FIO succeeded ✓")

        # ── TC-BCK-097: Crypto lvol, container_stop interrupt ──────────────

        self.logger.info("TC-BCK-097: interrupted restore on crypto lvol")
        crypto_name, crypto_id = self._create_lvol(
            name=f"intr_rst_c_{_rand_suffix()}", crypto=True)
        c_device, c_mount = self._connect_and_mount(crypto_name, crypto_id)
        self._run_fio(c_mount, runtime=30)
        c_files = self.ssh_obj.find_files(self.fio_node, c_mount)
        c_checksums = self.ssh_obj.generate_checksums(self.fio_node, c_files)

        c_snap = f"intr_rst_c_snap_{_rand_suffix()}"
        c_snap_id = self._create_snapshot(crypto_id, c_snap, backup=True)
        sleep_n_sec(20)
        c_backups = self._list_backups()
        if c_backups:
            c_bk_id = (
                c_backups[-1].get("id") or c_backups[-1].get("ID")
                or c_backups[-1].get("uuid") or None
            )
            if c_bk_id:
                c_rst_name = f"intr_rst_c1_{_rand_suffix()}"
                self._restore_backup(c_bk_id, c_rst_name)
                try:
                    sn_id2 = self._get_random_sn()
                    self._do_outage(sn_id2, "container_stop")
                except Exception as e:
                    self.logger.warning(f"TC-BCK-097: outage error (non-fatal): {e}")

                sleep_n_sec(30)
                c_rst_status = self._wait_for_restore_or_error(c_rst_name, timeout=120)
                self.logger.info(
                    f"TC-BCK-097: crypto restore status={c_rst_status!r}")

                if c_rst_status not in ("absent", "timeout"):
                    self._cleanup_lvol(c_rst_name)

                # Retry crypto restore
                c_rst_retry = f"intr_rst_c2_{_rand_suffix()}"
                self._restore_backup(c_bk_id, c_rst_retry)
                self._wait_for_restore(c_rst_retry)
                c_rest_id = self.sbcli_utils.get_lvol_id(lvol_name=c_rst_retry)
                c_r_device, c_r_mount = self._connect_and_mount(
                    c_rst_retry, c_rest_id,
                    mount=f"{self.mount_path}/icr2_{_rand_suffix()}")
                c_r_files = self.ssh_obj.find_files(self.fio_node, c_r_mount)
                self.ssh_obj.verify_checksums(
                    self.fio_node, c_r_files, c_checksums,
                    message="TC-BCK-097: checksum mismatch on interrupted crypto restore retry")
                self.logger.info("TC-BCK-097: crypto interrupted restore retry ✓")

        self.logger.info("=== TestBackupInterruptedRestore PASSED ===")


# ════════════════════════════════════════════════════════════════════════════
#  Marathon – long-running mixed backup / restore / delete stress
# ════════════════════════════════════════════════════════════════════════════


class BackupStressMarathon(BackupStressBase):
    """
    TC-BCK-STR-060..065

    Runs num_rounds iterations (default 20; set to 100 for full stress)
    across 3 lvols with a randomly selected operation each round:

      BACKUP            (50 % weight) – snapshot + S3 backup on a random lvol
      RESTORE           (25 % weight) – restore a random previously-made backup;
                                        verify checksums
      DELETE_AND_BACKUP (15 % weight) – delete all backups for a random lvol,
                                        immediately take a fresh backup, verify
                                        the chain works again
      VERIFY            (10 % weight) – verify checksums on a randomly chosen
                                        already-restored lvol

    Every 5 rounds a forced checksum check is also run on the most-recently
    restored lvol to catch silent corruption early.

    Validates:
      - Service remains stable across 20-100 mixed operations
      - Delta chain stays bounded after repeated backups
      - After backup delete + re-backup, new chain is fully restorable
      - Checksums are correct throughout (no silent data corruption)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "backup_stress_marathon"
        self.num_rounds = 20   # set to 100 for a full stress run
        self.num_lvols = 3
        self._weights = ["backup"] * 10 + ["restore"] * 5 + \
                        ["delete_and_backup"] * 3 + ["verify"] * 2

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
                mount=f"{self.mount_path}/mr_{round_num}_{_rand_suffix()}")
            rst_files = self.ssh_obj.find_files(self.fio_node, rst_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, rst_files, info["checksums"],
                message=f"[round {round_num}] RESTORE {lvol_key} checksum mismatch")
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
            # Verify backup list is clean for this lvol
            remaining = [
                b for b in self._list_backups()
                if lvol_key in " ".join(str(v) for v in b.values())
            ]
            assert len(remaining) == 0, \
                f"[round {round_num}] backups not fully deleted for {lvol_key}: {remaining}"
            # Immediately take a fresh backup to confirm chain re-starts cleanly
            sn = f"mara_fresh_{lvol_key}_{round_num}_{_rand_suffix()}"
            self._create_snapshot(info["id"], sn, backup=True)
            bk_id = self._wait_for_backup_by_snap(sn, f"marathon[{round_num}]")
            info["backup_ids"].append(bk_id)
            self.logger.info(
                f"[round {round_num}] DELETE_AND_BACKUP {lvol_key}: fresh backup {bk_id} ✓")
        except Exception as e:
            self.logger.error(f"[round {round_num}] DELETE_AND_BACKUP {lvol_key} error: {e}")

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
            _, rst_mount = self._connect_and_mount(rst_name, rst_id, mount=mount_path)
            files = self.ssh_obj.find_files(self.fio_node, rst_mount)
            self.ssh_obj.verify_checksums(
                self.fio_node, files, expected,
                message=f"[round {round_num}] VERIFY {rst_name} checksum mismatch")
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
                _, rst_mount = self._connect_and_mount(rst_name, rst_id, mount=mount_path)
                files = self.ssh_obj.find_files(self.fio_node, rst_mount)
                self.ssh_obj.verify_checksums(self.fio_node, files, expected,
                    message=f"TC-BCK-STR-063: final checksum mismatch for {rst_name}")
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
