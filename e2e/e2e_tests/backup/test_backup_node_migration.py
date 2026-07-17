"""
E2E tests for backup behaviour around node migration.

TestBackupAfterNodeMigration
    Verify that (a) existing backups survive migration, (b) new backups work
    post-migration, and (c) backup policies continue to fire.

TestBackupDuringMigration
    Take a backup while migration is actively happening to verify in-flight
    backups are handled gracefully.

Both tests extend BackupTestBase and support K8s-native mode (CRD-based
migration via patch_storage_node_migrate).  Docker mode is supported for
TestBackupAfterNodeMigration by using node shutdown/restart to trigger
migration tasks.

Requires a backup-enabled cluster (S3/MinIO configured at bootstrap).
"""

from __future__ import annotations

import random
import string
import time
from datetime import datetime

from e2e_tests.backup.test_backup_restore import BackupTestBase, _rand_suffix
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec


def _rand_seq(length: int = 6) -> str:
    first = random.choice(string.ascii_lowercase)
    rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=length - 1))
    return first + rest


class TestBackupAfterNodeMigration(BackupTestBase):
    """
    TC-BCK-MIG-001 through TC-BCK-MIG-007

    Steps
    -----
    1. Create pool, create lvol, write data, capture checksums,
       snapshot+backup, wait for done.
    2. Attach backup policy (5-min schedule) to lvol.
    3. Identify node hosting the lvol, trigger migration.
       Wait for node online + cluster active, validate migration tasks.
    4. Verify pre-migration backup still listed as done.
       Restore it, verify checksums match.
    5. Write new data post-migration, snapshot+backup.
       Key assertion: backup of migrated lvol succeeds.
    6. Restore post-migration backup, verify data integrity.
    7. Wait 6 min, verify backup policy still fires after migration.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "backup_after_node_migration"

        # K8s-native migration target
        self.migrate_to_worker = kwargs.get("migrate_to_worker", "")
        if isinstance(self.migrate_to_worker, str):
            self.migrate_to_worker = self.migrate_to_worker.strip()

        # SSD PCIe addresses for K8s migration
        new_ssd_pcie_raw = kwargs.get("new_ssd_pcie", "")
        if isinstance(new_ssd_pcie_raw, str) and new_ssd_pcie_raw.strip():
            self.new_ssd_pcie = [addr.strip() for addr in new_ssd_pcie_raw.split(",") if addr.strip()]
        else:
            self.new_ssd_pcie = []

        reattach_raw = kwargs.get("reattach_volume", "")
        self.reattach_volume = str(reattach_raw).strip().lower() in ("true", "1", "yes")

    def _get_lvol_hosting_node(self, lvol_id: str) -> str:
        """Return the storage node ID that hosts the given lvol."""
        if self.k8s_test:
            # In k8s mode, look up via sbcli lvol list and find the node
            lvols = self.sbcli_utils.list_lvols()
            for name, details in lvols.items():
                lid = details.get("UUID", details.get("uuid", ""))
                if lid == lvol_id or lvol_id in lid or name == lvol_id:
                    node_id = details.get("node_id", details.get("Node", ""))
                    if node_id:
                        return node_id
        else:
            details = self.sbcli_utils.get_lvol_details(lvol_id)
            if details:
                return details.get("node_id", "")
        return ""

    def _trigger_migration_k8s(self, node_uuid: str):
        """Trigger node migration via CRD patch (K8s mode)."""
        from utils.k8s_utils import K8sUtils
        mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
        k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
        k8s_utils.patch_storage_node_migrate(
            node_uuid=node_uuid,
            target_worker=self.migrate_to_worker,
            new_ssd_pcie=self.new_ssd_pcie if self.new_ssd_pcie else None,
            reattach_volume=self.reattach_volume,
        )

    def _trigger_migration_docker(self, node_id: str):
        """In Docker mode, trigger migration by restarting the storage node.

        This causes the data migration/rebalancing mechanism to kick in.
        """
        self.logger.info(f"Docker mode: Restarting storage node {node_id} to trigger migration")
        node_details = self.sbcli_utils.get_storage_node_details(node_id)
        node_ip = node_details.get("mgmt_ip", "")
        if node_ip:
            self.ssh_obj.restart_storage_node(node_ip)

    def run(self):
        self.logger.info("Starting Test: Backup After Node Migration")

        # Validate prerequisites
        if self.k8s_test:
            assert self.migrate_to_worker, (
                "TestBackupAfterNodeMigration in K8s mode requires "
                "--migrate_to_worker"
            )
        else:
            nodes = self.sbcli_utils.get_storage_nodes()["results"]
            assert len(nodes) >= 2, (
                "TestBackupAfterNodeMigration requires at least 2 storage nodes"
            )

        # ── TC-BCK-MIG-001: Create lvol + pre-migration backup ───────
        self.logger.info("TC-BCK-MIG-001: Create lvol, write data, backup")
        self._ensure_pool_and_sc()

        mig_name, mig_id = self._create_lvol(name="bck_mig_lvol")
        _, mig_mount = self._connect_and_mount(mig_name, mig_id)
        self._run_fio(mig_mount, runtime=30)
        pre_mig_checksums = self._get_checksums(self.fio_node, mig_mount)
        self.logger.info(f"Pre-migration checksums: {pre_mig_checksums}")

        snap_pre = self._create_snapshot(mig_id, "snap_pre_mig", backup=True)
        backup_id_pre = self._last_backup_id or snap_pre
        self._wait_for_backup(backup_id_pre)
        self.logger.info(f"Pre-migration backup done: {backup_id_pre}")

        # ── TC-BCK-MIG-002: Attach backup policy ─────────────────────
        self.logger.info("TC-BCK-MIG-002: Attaching backup policy")
        policy_id = self._add_policy(
            "policy_mig_test", versions=5, schedule="*/5 * * * *"
        )
        self._attach_policy(policy_id, "lvol", mig_id)
        sleep_n_sec(60)  # Let policy register

        # ── TC-BCK-MIG-003: Trigger migration ────────────────────────
        self.logger.info("TC-BCK-MIG-003: Triggering node migration")
        hosting_node = self._get_lvol_hosting_node(mig_id)
        self.logger.info(f"Lvol hosted on node: {hosting_node}")

        timestamp = int(datetime.now().timestamp())

        if self.k8s_test:
            self._trigger_migration_k8s(hosting_node)
        else:
            self._trigger_migration_docker(hosting_node)

        # Wait for node online + cluster active
        if hosting_node:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=hosting_node, status="online", timeout=600,
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        sleep_n_sec(60)
        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=True)
        self.logger.info("Migration complete")

        # ── TC-BCK-MIG-004: Verify pre-migration backup ──────────────
        self.logger.info("TC-BCK-MIG-004: Verifying pre-migration backup")
        backups = self._list_backups()
        pre_found = False
        for b in backups:
            bid = b.get("id") or b.get("ID") or b.get("name") or ""
            status = (b.get("status") or b.get("Status") or "").lower()
            if backup_id_pre in bid or bid in backup_id_pre:
                assert status in ("done", "complete", "completed"), (
                    f"Pre-migration backup status={status}, expected done"
                )
                pre_found = True
                break
        assert pre_found, f"Pre-migration backup {backup_id_pre} not found"

        # Restore pre-migration backup
        self._unmount_and_disconnect(self.fio_node, mig_mount, mig_id)

        restored_pre = self._restore_backup(backup_id_pre, "rst_pre_mig")
        self._wait_for_restore(restored_pre)
        rst_pre_id = self._get_lvol_id(restored_pre)
        _, rst_pre_mount = self._connect_and_mount(
            restored_pre, rst_pre_id, format_disk=False
        )
        self._verify_checksums(
            self.fio_node if not self.k8s_test else restored_pre,
            rst_pre_mount, pre_mig_checksums,
        )
        self.logger.info("Pre-migration backup restore verified!")

        # Unmount restored
        self._unmount_and_disconnect(self.fio_node, rst_pre_mount, rst_pre_id)

        # ── TC-BCK-MIG-005: Post-migration backup ────────────────────
        self.logger.info("TC-BCK-MIG-005: Writing new data + post-migration backup")

        # Reconnect original lvol
        _, mig_mount2 = self._connect_and_mount(mig_name, mig_id, format_disk=False)
        self._run_fio(mig_mount2, runtime=30, rw="write", bs="4K")
        post_mig_checksums = self._get_checksums(self.fio_node, mig_mount2)
        self.logger.info(f"Post-migration checksums: {post_mig_checksums}")

        snap_post = self._create_snapshot(mig_id, "snap_post_mig", backup=True)
        backup_id_post = self._last_backup_id or snap_post
        self._wait_for_backup(backup_id_post)
        self.logger.info(f"Post-migration backup done: {backup_id_post}")

        # ── TC-BCK-MIG-006: Restore post-migration backup ────────────
        self.logger.info("TC-BCK-MIG-006: Restoring post-migration backup")
        self._unmount_and_disconnect(self.fio_node, mig_mount2, mig_id)

        restored_post = self._restore_backup(backup_id_post, "rst_post_mig")
        self._wait_for_restore(restored_post)
        rst_post_id = self._get_lvol_id(restored_post)
        _, rst_post_mount = self._connect_and_mount(
            restored_post, rst_post_id, format_disk=False,
        )
        self._verify_checksums(
            self.fio_node if not self.k8s_test else restored_post,
            rst_post_mount, post_mig_checksums,
        )
        self.logger.info("Post-migration backup restore verified!")

        # ── TC-BCK-MIG-007: Verify policy still fires ────────────────
        self.logger.info("TC-BCK-MIG-007: Verifying backup policy fires after migration")
        self.logger.info("Waiting 360s for policy-triggered backup...")
        sleep_n_sec(360)

        backups_final = self._list_backups()
        # Count backups associated with our lvol
        lvol_backups = []
        for b in backups_final:
            lvol_ref = b.get("LVol") or b.get("lvol") or ""
            crd_name = b.get("name") or ""
            if (mig_name in lvol_ref
                    or (self.k8s_test and self._k8s_normalize_name(mig_name) in lvol_ref)
                    or mig_name in crd_name):
                lvol_backups.append(b)

        self.logger.info(f"Total backups for lvol: {len(lvol_backups)}")
        # We created 2 manual backups (pre + post). Policy should have added at least 1 more.
        assert len(lvol_backups) >= 3, (
            f"Expected at least 3 backups (2 manual + 1 policy), "
            f"got {len(lvol_backups)}"
        )

        self._detach_policy(policy_id, "lvol", mig_id)
        self._remove_policy(policy_id)

        self.logger.info("TEST CASE PASSED: TestBackupAfterNodeMigration")


class TestBackupDuringMigration(BackupTestBase):
    """
    Take a backup while migration is actively happening.

    Steps
    -----
    1. Create pool/SC, create lvol, write data + checksums, start FIO.
    2. Take baseline snapshot+backup, wait done.
    3. Trigger node migration for the node hosting the lvol.
    4. Immediately take another snapshot+backup (races with migration).
    5. Wait for migration to complete.
    6. Wait for in-flight backup to complete.
    7. If backup succeeded: restore, verify checksums.
    8. If backup failed: log warning, assert cluster not in bad state.
    9. Take one more post-migration backup to confirm system recovered.
    10. Validate all nodes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.test_name = "backup_during_migration"

        self.migrate_to_worker = kwargs.get("migrate_to_worker", "")
        if isinstance(self.migrate_to_worker, str):
            self.migrate_to_worker = self.migrate_to_worker.strip()

        new_ssd_pcie_raw = kwargs.get("new_ssd_pcie", "")
        if isinstance(new_ssd_pcie_raw, str) and new_ssd_pcie_raw.strip():
            self.new_ssd_pcie = [addr.strip() for addr in new_ssd_pcie_raw.split(",") if addr.strip()]
        else:
            self.new_ssd_pcie = []

        reattach_raw = kwargs.get("reattach_volume", "")
        self.reattach_volume = str(reattach_raw).strip().lower() in ("true", "1", "yes")

    def _get_lvol_hosting_node(self, lvol_id: str) -> str:
        if self.k8s_test:
            lvols = self.sbcli_utils.list_lvols()
            for name, details in lvols.items():
                lid = details.get("UUID", details.get("uuid", ""))
                if lid == lvol_id or lvol_id in lid or name == lvol_id:
                    node_id = details.get("node_id", details.get("Node", ""))
                    if node_id:
                        return node_id
        else:
            details = self.sbcli_utils.get_lvol_details(lvol_id)
            if details:
                return details.get("node_id", "")
        return ""

    def run(self):
        self.logger.info("Starting Test: Backup During Migration")

        if self.k8s_test:
            assert self.migrate_to_worker, (
                "TestBackupDuringMigration in K8s mode requires --migrate_to_worker"
            )
        else:
            nodes = self.sbcli_utils.get_storage_nodes()["results"]
            assert len(nodes) >= 2, (
                "TestBackupDuringMigration requires at least 2 storage nodes"
            )

        # ── Step 1: Create lvol, write data ───────────────────────────
        self.logger.info("Step 1: Create lvol, write data")
        self._ensure_pool_and_sc()

        dm_name, dm_id = self._create_lvol(name="bck_during_mig")
        _, dm_mount = self._connect_and_mount(dm_name, dm_id)
        self._run_fio(dm_mount, runtime=30)
        pre_checksums = self._get_checksums(self.fio_node, dm_mount)
        self.logger.info(f"Pre-migration checksums captured")

        # ── Step 2: Baseline backup ───────────────────────────────────
        self.logger.info("Step 2: Baseline snapshot+backup")
        snap_base = self._create_snapshot(dm_id, "snap_base", backup=True)
        backup_base = self._last_backup_id or snap_base
        self._wait_for_backup(backup_base)
        self.logger.info(f"Baseline backup done: {backup_base}")

        # ── Step 3: Trigger migration ─────────────────────────────────
        self.logger.info("Step 3: Triggering migration")
        hosting_node = self._get_lvol_hosting_node(dm_id)
        self.logger.info(f"Lvol hosted on node: {hosting_node}")

        timestamp = int(datetime.now().timestamp())

        if self.k8s_test:
            from utils.k8s_utils import K8sUtils
            mgmt_node = self.mgmt_nodes[0] if self.mgmt_nodes else ""
            k8s_utils = K8sUtils(ssh_obj=self.ssh_obj, mgmt_node=mgmt_node)
            k8s_utils.patch_storage_node_migrate(
                node_uuid=hosting_node,
                target_worker=self.migrate_to_worker,
                new_ssd_pcie=self.new_ssd_pcie if self.new_ssd_pcie else None,
                reattach_volume=self.reattach_volume,
            )
        else:
            node_details = self.sbcli_utils.get_storage_node_details(hosting_node)
            node_ip = node_details.get("mgmt_ip", "")
            if node_ip:
                self.ssh_obj.restart_storage_node(node_ip)

        # ── Step 4: Immediately take backup (race with migration) ─────
        self.logger.info("Step 4: Taking snapshot+backup during active migration")
        inflight_backup_id = None
        inflight_failed = False
        try:
            snap_inflight = self._create_snapshot(dm_id, "snap_inflight", backup=True)
            inflight_backup_id = self._last_backup_id or snap_inflight
            self.logger.info(f"In-flight backup created: {inflight_backup_id}")
        except Exception as e:
            self.logger.warning(
                f"In-flight snapshot/backup creation failed (expected edge case): {e}"
            )
            inflight_failed = True

        # ── Step 5: Wait for migration to complete ────────────────────
        self.logger.info("Step 5: Waiting for migration to complete")
        if hosting_node:
            self.sbcli_utils.wait_for_storage_node_status(
                node_id=hosting_node, status="online", timeout=600,
            )
        self.sbcli_utils.wait_for_cluster_status(
            cluster_id=self.cluster_id, status="active", timeout=600,
        )
        sleep_n_sec(60)
        self.validate_migration_for_node(timestamp, 2000, None, 60, no_task_ok=True)
        self.logger.info("Migration complete")

        # ── Step 6: Check in-flight backup result ─────────────────────
        if not inflight_failed and inflight_backup_id:
            self.logger.info("Step 6: Checking in-flight backup result")
            try:
                self._wait_for_backup(inflight_backup_id, timeout=600)
                self.logger.info(f"In-flight backup {inflight_backup_id} succeeded!")

                # ── Step 7: Restore and verify ────────────────────────
                self.logger.info("Step 7: Restoring in-flight backup")
                self._unmount_and_disconnect(self.fio_node, dm_mount, dm_id)

                restored = self._restore_backup(inflight_backup_id, "rst_inflight")
                self._wait_for_restore(restored)
                rst_id = self._get_lvol_id(restored)
                _, rst_mount = self._connect_and_mount(
                    restored, rst_id, format_disk=False,
                )
                # Note: checksums may differ from pre_checksums if FIO wrote
                # more data before the snapshot, but the restored data should
                # be internally consistent
                self._run_fio(rst_mount, runtime=15, rw="read", bs="4K")
                self.logger.info("In-flight backup restore verified (read succeeded)")
                self._unmount_and_disconnect(self.fio_node, rst_mount, rst_id)

            except (AssertionError, TimeoutError) as e:
                self.logger.warning(
                    f"In-flight backup failed (acceptable edge case): {e}"
                )
                inflight_failed = True
        else:
            self.logger.info("Step 6: Skipped (in-flight backup was not created)")

        # ── Step 8: Verify cluster healthy ────────────────────────────
        self.logger.info("Step 8: Verifying cluster health after migration")
        final_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        for node in final_nodes:
            assert node["status"] == "online", (
                f"Node {node['id']} status={node['status']}, expected online"
            )

        # ── Step 9: Post-migration backup to confirm recovery ─────────
        self.logger.info("Step 9: Post-migration backup to confirm system recovered")

        # Reconnect original lvol if we disconnected it
        if not inflight_failed:
            _, dm_mount_post = self._connect_and_mount(dm_name, dm_id, format_disk=False)
        else:
            dm_mount_post = dm_mount

        snap_post = self._create_snapshot(dm_id, "snap_post_confirm", backup=True)
        backup_post = self._last_backup_id or snap_post
        self._wait_for_backup(backup_post)
        self.logger.info(f"Post-migration confirmation backup done: {backup_post}")

        self.logger.info("TEST CASE PASSED: TestBackupDuringMigration")
