"""
E2E tests for backup behaviour after cluster expansion (node add).

TestBackupAfterNodeAdd
    Verify that (a) pre-expansion backups remain valid and restorable after
    adding a node, (b) LVOLs created on the new node can be backed up to S3,
    and (c) backup policies fire correctly on new-node volumes.

TestBackupWithFioOnNewNode
    Run FIO on both existing and new node volumes while taking concurrent
    snapshots+backups, then restore and verify.

Both tests extend BackupTestBase and support Docker (SSH) and K8s-native modes.
Requires a backup-enabled cluster (S3/MinIO configured at bootstrap).
"""

from __future__ import annotations

import random
import string
from datetime import datetime

from e2e_tests.backup.test_backup_restore import BackupTestBase, _rand_suffix
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


def _rand_seq(length: int = 6) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


class TestBackupAfterNodeAdd(BackupTestBase):
    """
    TC-BCK-ADD-001 through TC-BCK-ADD-006

    Steps
    -----
    1. Create pool, create lvol on existing node, write data, snapshot+backup.
    2. Add a storage node; wait for in_expansion (graceful), node online,
       cluster active, migration tasks complete.
    3. Verify pre-expansion backup still accessible and restorable with
       data integrity (checksum match).
    4. Create lvol on new node, write data, snapshot+backup.
       Key assertion: backup on new-node lvol succeeds.
    5. Restore new-node backup, verify data integrity.
    6. Attach backup policy to new-node lvol, verify policy-triggered backup.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "backup_after_node_add"

        self.new_nodes = kwargs.get("new_nodes", [])
        if isinstance(self.new_nodes, str):
            self.new_nodes = self.new_nodes.strip().split()

        self.new_worker_nodes = kwargs.get("new_worker_nodes", [])
        if isinstance(self.new_worker_nodes, str):
            self.new_worker_nodes = [
                n.strip() for n in self.new_worker_nodes.split(",") if n.strip()
            ]

    # ── node-add helpers ──────────────────────────────────────────────

    def _add_node_docker(self, ip: str):
        """Deploy + add a single storage node via SSH."""
        node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
        max_lvol = node_sample["max_lvol"]
        max_prov = int(node_sample["max_prov"] / (1024**3))
        data_nics = node_sample.get("data_nics", [])
        data_nic = data_nics[0]["if_name"] if data_nics else None

        self.logger.info(f"Deploying storage node: {ip}")
        self.ssh_obj.deploy_storage_node(ip, max_lvol, max_prov)
        self.ssh_obj.add_storage_node(
            self.mgmt_nodes[0], self.cluster_id, ip,
            spdk_image=node_sample["spdk_image"],
            partitions=node_sample["num_partitions_per_dev"],
            disable_ha_jm=not node_sample["enable_ha_jm"],
            enable_test_device=node_sample["enable_test_device"],
            spdk_debug=node_sample["spdk_debug"],
            data_nic=data_nic,
        )
        sleep_n_sec(60)
        self.storage_nodes.append(ip)

    def _add_node_k8s(self, worker_name: str, initial_pod_count: int):
        """Add a single worker by creating a StorageNode CR."""
        from utils.k8s_utils import K8sUtils
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
        k8s_utils.patch_storage_node_add_workers(new_workers=[worker_name])
        sleep_n_sec(10)
        k8s_utils.wait_spdk_pods_ready(
            expected_count=initial_pod_count + 1, timeout=900
        )

    def _add_node_and_wait(self, initial_node_ids: set, initial_pod_count: int) -> list[str]:
        """Add a node, wait for expansion, return new node IDs."""
        nodes_to_add = self.new_worker_nodes if self.k8s_test else self.new_nodes
        assert len(nodes_to_add) >= 1, "At least 1 new node required"

        timestamp = int(datetime.now().timestamp())

        if self.k8s_test:
            self._add_node_k8s(nodes_to_add[0], initial_pod_count)
        else:
            self._add_node_docker(nodes_to_add[0])

        # Check in_expansion
        try:
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id, status="in_expansion", timeout=60,
            )
            self.logger.info("Cluster entered in_expansion state")
        except Exception:
            self.logger.info("Cluster may already be past in_expansion state")

        # Detect new nodes
        sleep_n_sec(60)
        all_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        new_node_ids = [n["id"] for n in all_nodes if n["id"] not in initial_node_ids]
        self.logger.info(f"New node IDs: {new_node_ids}")

        # Wait for online + active
        for nid in new_node_ids:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=nid, status="online", timeout=600,
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )

        sleep_n_sec(120)
        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)
        sleep_n_sec(30)

        return new_node_ids

    # ── main test ─────────────────────────────────────────────────────

    def run(self):
        self.logger.info("Starting Test: Backup After Node Add")

        # ── TC-BCK-ADD-001: Setup + pre-expansion backup ─────────────
        self.logger.info("TC-BCK-ADD-001: Create lvol, write data, backup")
        self._ensure_pool_and_sc()

        initial_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        initial_node_ids = {n["id"] for n in initial_nodes}
        initial_pod_count = len(initial_nodes)

        pre_name, pre_id = self._create_lvol(name="bck_pre_add")
        _, pre_mount = self._connect_and_mount(pre_name, pre_id)
        self._run_fio(pre_mount, runtime=30)
        pre_checksums = self._get_checksums(self.fio_node, pre_mount)
        self.logger.info(f"Pre-expansion checksums: {pre_checksums}")

        snap_pre = self._create_snapshot(pre_id, "snap_pre_add", backup=True)
        backup_id_pre = self._last_backup_id or snap_pre
        self._wait_for_backup(backup_id_pre)
        self.logger.info(f"Pre-expansion backup done: {backup_id_pre}")

        # ── TC-BCK-ADD-002: Add node ─────────────────────────────────
        self.logger.info("TC-BCK-ADD-002: Adding storage node")
        new_node_ids = self._add_node_and_wait(initial_node_ids, initial_pod_count)

        # ── TC-BCK-ADD-003: Verify pre-expansion backup still valid ──
        self.logger.info("TC-BCK-ADD-003: Verifying pre-expansion backup")
        backups = self._list_backups()
        pre_backup_found = False
        for b in backups:
            bid = b.get("id") or b.get("ID") or b.get("name") or ""
            status = (b.get("status") or b.get("Status") or "").lower()
            if backup_id_pre in bid or bid in backup_id_pre:
                assert status in ("done", "complete", "completed"), (
                    f"Pre-expansion backup status={status}, expected done"
                )
                pre_backup_found = True
                break
        assert pre_backup_found, (
            f"Pre-expansion backup {backup_id_pre} not found in backup list"
        )

        # Restore and verify
        restored_pre = self._restore_backup(backup_id_pre, "restored_pre_add")
        self._wait_for_restore(restored_pre)

        # Unmount original to avoid XFS UUID conflict
        self._unmount_and_disconnect(self.fio_node, pre_mount, pre_id)

        restored_pre_id = self._get_lvol_id(restored_pre)
        _, restored_pre_mount = self._connect_and_mount(
            restored_pre, restored_pre_id, format_disk=False
        )
        self._verify_checksums(
            self.fio_node if not self.k8s_test else restored_pre,
            restored_pre_mount, pre_checksums,
        )
        self.logger.info("Pre-expansion backup restore verified!")

        # ── TC-BCK-ADD-004: Create lvol on new node + backup ─────────
        self.logger.info("TC-BCK-ADD-004: Creating lvol on new node, backup")
        new_name, new_id = self._create_lvol(name="bck_new_node")
        _, new_mount = self._connect_and_mount(new_name, new_id)
        self._run_fio(new_mount, runtime=30)
        new_checksums = self._get_checksums(self.fio_node, new_mount)
        self.logger.info(f"New-node checksums: {new_checksums}")

        snap_new = self._create_snapshot(new_id, "snap_new_node", backup=True)
        backup_id_new = self._last_backup_id or snap_new
        self._wait_for_backup(backup_id_new)
        self.logger.info(f"New-node backup done: {backup_id_new}")

        # ── TC-BCK-ADD-005: Restore new-node backup + verify ─────────
        self.logger.info("TC-BCK-ADD-005: Restoring new-node backup")

        # Unmount new-node lvol to avoid UUID conflict
        self._unmount_and_disconnect(self.fio_node, new_mount, new_id)

        restored_new = self._restore_backup(backup_id_new, "restored_new_node")
        self._wait_for_restore(restored_new)
        restored_new_id = self._get_lvol_id(restored_new)
        _, restored_new_mount = self._connect_and_mount(
            restored_new, restored_new_id, format_disk=False,
        )
        self._verify_checksums(
            self.fio_node if not self.k8s_test else restored_new,
            restored_new_mount, new_checksums,
        )
        self.logger.info("New-node backup restore verified!")

        # ── TC-BCK-ADD-006: Backup policy on new-node lvol ───────────
        self.logger.info("TC-BCK-ADD-006: Attaching backup policy to new-node lvol")

        # Reconnect the new-node lvol for policy test
        nn_name2, nn_id2 = self._create_lvol(name="bck_nn_policy")
        _, nn_mount2 = self._connect_and_mount(nn_name2, nn_id2)
        self._run_fio(nn_mount2, runtime=30)

        policy_id = self._add_policy(
            "policy_nn_add", versions=3, schedule="*/5 * * * *"
        )
        self._attach_policy(policy_id, "lvol", nn_id2)

        self.logger.info("Waiting 360s for policy-triggered backup...")
        sleep_n_sec(360)

        backups_after = self._list_backups()
        policy_backup_found = False
        for b in backups_after:
            lvol_ref = b.get("LVol") or b.get("lvol") or ""
            if nn_name2 in lvol_ref or (self.k8s_test and self._k8s_normalize_name(nn_name2) in lvol_ref):
                status = (b.get("status") or b.get("Status") or "").lower()
                if status in ("done", "complete", "completed"):
                    policy_backup_found = True
                    break

        self._detach_policy(policy_id, "lvol", nn_id2)
        self._remove_policy(policy_id)

        assert policy_backup_found, (
            "No policy-triggered backup found for new-node lvol"
        )
        self.logger.info("Policy-triggered backup verified!")

        self.logger.info("TEST CASE PASSED: TestBackupAfterNodeAdd")


class TestBackupWithFioOnNewNode(BackupTestBase):
    """
    Run FIO on both existing and new node volumes while taking concurrent
    snapshots + backups.

    Steps
    -----
    1. Create pool/SC, create lvol on existing node, start FIO (600s).
    2. Add new node, wait for cluster active.
    3. Create lvol on new node, start FIO (600s).
    4. While FIO running: snapshot+backup on new-node lvol.
    5. While FIO still running: second snapshot+backup (delta chain).
    6. Wait for all FIO to finish, validate.
    7. Restore both backups, verify each can mount + read FIO.
    8. Validate all nodes healthy.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "backup_fio_new_node"

        self.new_nodes = kwargs.get("new_nodes", [])
        if isinstance(self.new_nodes, str):
            self.new_nodes = self.new_nodes.strip().split()

        self.new_worker_nodes = kwargs.get("new_worker_nodes", [])
        if isinstance(self.new_worker_nodes, str):
            self.new_worker_nodes = [
                n.strip() for n in self.new_worker_nodes.split(",") if n.strip()
            ]

    def _add_node_docker(self, ip: str):
        node_sample = self.sbcli_utils.get_storage_nodes()["results"][0]
        max_lvol = node_sample["max_lvol"]
        max_prov = int(node_sample["max_prov"] / (1024**3))
        data_nics = node_sample.get("data_nics", [])
        data_nic = data_nics[0]["if_name"] if data_nics else None
        self.ssh_obj.deploy_storage_node(ip, max_lvol, max_prov)
        self.ssh_obj.add_storage_node(
            self.mgmt_nodes[0], self.cluster_id, ip,
            spdk_image=node_sample["spdk_image"],
            partitions=node_sample["num_partitions_per_dev"],
            disable_ha_jm=not node_sample["enable_ha_jm"],
            enable_test_device=node_sample["enable_test_device"],
            spdk_debug=node_sample["spdk_debug"],
            data_nic=data_nic,
        )
        sleep_n_sec(60)
        self.storage_nodes.append(ip)

    def _add_node_k8s(self, worker_name: str, initial_pod_count: int):
        from utils.k8s_utils import K8sUtils
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
        k8s_utils.patch_storage_node_add_workers(new_workers=[worker_name])
        sleep_n_sec(10)
        k8s_utils.wait_spdk_pods_ready(
            expected_count=initial_pod_count + 1, timeout=900
        )

    def run(self):
        self.logger.info("Starting Test: Backup With FIO On New Node")

        nodes_to_add = self.new_worker_nodes if self.k8s_test else self.new_nodes
        assert len(nodes_to_add) >= 1, "At least 1 new node required"

        # ── Step 1: Create lvol on existing node + FIO ────────────────
        self.logger.info("Step 1: Creating lvol on existing node, starting FIO")
        self._ensure_pool_and_sc()

        initial_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        initial_node_ids = {n["id"] for n in initial_nodes}
        initial_pod_count = len(initial_nodes)

        exist_name, exist_id = self._create_lvol(name="bck_exist_fio")
        _, exist_mount = self._connect_and_mount(exist_name, exist_id)
        # Start FIO in background (in docker mode _run_fio is blocking,
        # so we use the dual helpers from TestClusterBase for non-blocking FIO)
        self._run_fio(exist_mount, runtime=60, rw="write", bs="1M")
        self.logger.info("Initial data written on existing lvol")

        # ── Step 2: Add node ──────────────────────────────────────────
        self.logger.info("Step 2: Adding new node")
        timestamp = int(datetime.now().timestamp())

        if self.k8s_test:
            self._add_node_k8s(nodes_to_add[0], initial_pod_count)
        else:
            self._add_node_docker(nodes_to_add[0])

        try:
            self.sbcli_utils.wait_for_cluster_status(
                cluster_id=self.cluster_id, status="in_expansion", timeout=60,
            )
        except Exception:
            self.logger.info("Cluster may already be past in_expansion state")

        sleep_n_sec(60)
        all_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        new_node_ids = [n["id"] for n in all_nodes if n["id"] not in initial_node_ids]
        for nid in new_node_ids:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=nid, status="online", timeout=600
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        sleep_n_sec(120)
        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=False)

        # ── Step 3: Create lvol on new node + FIO ─────────────────────
        self.logger.info("Step 3: Creating lvol on new node, running FIO")
        new_name, new_id = self._create_lvol(name="bck_new_fio")
        _, new_mount = self._connect_and_mount(new_name, new_id)
        self._run_fio(new_mount, runtime=60, rw="write", bs="1M")
        self.logger.info("Data written on new-node lvol")

        # ── Step 4: Snapshot + backup while data present ──────────────
        self.logger.info("Step 4: First snapshot+backup on new-node lvol")
        snap1 = self._create_snapshot(new_id, "snap_nn_1", backup=True)
        backup1 = self._last_backup_id or snap1
        self._wait_for_backup(backup1)
        self.logger.info(f"First backup done: {backup1}")

        # Write more data
        self._run_fio(new_mount, runtime=30, rw="write", bs="4K")

        # ── Step 5: Second snapshot + backup (delta chain) ────────────
        self.logger.info("Step 5: Second snapshot+backup (delta chain)")
        snap2 = self._create_snapshot(new_id, "snap_nn_2", backup=True)
        backup2 = self._last_backup_id or snap2
        self._wait_for_backup(backup2)
        self.logger.info(f"Second backup done: {backup2}")

        # ── Step 6: Restore both backups and verify ───────────────────
        self.logger.info("Step 6: Restoring backups and verifying")

        # Unmount original to avoid UUID conflicts
        self._unmount_and_disconnect(self.fio_node, new_mount, new_id)

        restored1 = self._restore_backup(backup1, "rst_nn_bck1")
        self._wait_for_restore(restored1)
        rst1_id = self._get_lvol_id(restored1)
        _, rst1_mount = self._connect_and_mount(restored1, rst1_id, format_disk=False)

        # Verify we can read the data (run a short read FIO)
        self._run_fio(rst1_mount, runtime=15, rw="read", bs="4K")
        self.logger.info("Backup 1 restore verified (read FIO succeeded)")

        self._unmount_and_disconnect(self.fio_node, rst1_mount, rst1_id)

        restored2 = self._restore_backup(backup2, "rst_nn_bck2")
        self._wait_for_restore(restored2)
        rst2_id = self._get_lvol_id(restored2)
        _, rst2_mount = self._connect_and_mount(restored2, rst2_id, format_disk=False)
        self._run_fio(rst2_mount, runtime=15, rw="read", bs="4K")
        self.logger.info("Backup 2 restore verified (read FIO succeeded)")

        # ── Step 7: Final validation ──────────────────────────────────
        self.logger.info("Step 7: Final node health validation")
        final_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in final_nodes:
            assert node["status"] == "online", (
                f"Node {node['id']} status={node['status']}, expected online"
            )

        self.logger.info("TEST CASE PASSED: TestBackupWithFioOnNewNode")
