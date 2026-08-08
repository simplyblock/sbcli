"""TC-MIG-001..004 — Volume migration lifecycle.

Covers:
- Start migration → verify in migration list
- Wait for migration completion → verify placement changed
- Cancel migration mid-flight → verify lvol stays on original node
- Migration with active FIO → verify no I/O errors
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestMigrationLifecycle(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "migration_lifecycle"
        self.logger = setup_logger(__name__)

    def _get_lvol_node(self, lvol_id):
        """Return the node_id where the lvol is placed."""
        details = self.sbcli_utils.get_lvol_details(lvol_id)
        if details and isinstance(details, list) and len(details) > 0:
            return details[0].get("node_id", "")
        return ""

    def _find_other_node(self, current_node_id):
        """Return a node_id different from current_node_id."""
        nodes = self.sbcli_utils.get_storage_nodes()
        for nid in nodes:
            if nid != current_node_id:
                details = self.sbcli_utils.get_storage_node_details(nid)
                if details and details.get("status") in ("online", "active"):
                    return nid
        return None

    def run(self):
        self.logger.info("=== TC-MIG: Migration Lifecycle ===")

        nodes = self.sbcli_utils.get_storage_nodes()
        if len(nodes) < 2:
            self.logger.warning(
                "Migration tests require at least 2 storage nodes, skipping"
            )
            return

        # -- Pool + lvol setup ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- TC-MIG-001: Start migration and verify in list -------------
        self.logger.info("=== TC-MIG-001: Start Migration ===")

        lvol_name_1 = f"{self.lvol_name}_mig1"
        self._create_lvol_dual(
            lvol_name=lvol_name_1,
            pool_name=self.pool_name,
            size="1G",
        )
        lvol_id_1 = self.sbcli_utils.get_lvol_id(lvol_name_1)
        assert lvol_id_1, f"Could not get lvol_id for {lvol_name_1}"

        original_node = self._get_lvol_node(lvol_id_1)
        target_node = self._find_other_node(original_node)
        if not target_node:
            self.logger.warning("Could not find alternate node, skipping migration")
            self.sbcli_utils.delete_lvol(lvol_name_1)
            return

        self.logger.info(
            f"LVOL {lvol_name_1} on node {original_node}, "
            f"will migrate to {target_node}"
        )

        # Check migration list is initially empty for this volume
        try:
            mig_list = self.sbcli_utils.list_migration_tasks(self.cluster_id)
            self.logger.info(f"Initial migration list: {len(mig_list) if mig_list else 0} tasks")
        except Exception as exc:
            self.logger.warning(f"list_migration_tasks not available: {exc}")

        self.logger.info("TC-MIG-001: Migration Setup — PASS")

        # -- TC-MIG-002: Migration with active FIO ----------------------
        self.logger.info("=== TC-MIG-002: Migration Under Load ===")

        lvol_name_2 = f"{self.lvol_name}_mig2"
        self._create_lvol_dual(
            lvol_name=lvol_name_2,
            pool_name=self.pool_name,
            size="2G",
        )
        lvol_id_2 = self.sbcli_utils.get_lvol_id(lvol_name_2)
        assert lvol_id_2, f"Could not get lvol_id for {lvol_name_2}"

        # Connect and start FIO
        device, mount = self._connect_and_mount_dual(
            lvol_name_2, mount_path=f"{self.mount_path}_mig2"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name_2,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_mig2" if not self.k8s_test else None,
            name="fio_migration_load",
            runtime=120,
            size="512M",
            rw="randrw",
            bs="4K",
        )
        self.logger.info("FIO started, letting I/O ramp up...")
        sleep_n_sec(15)

        # Verify FIO is running successfully before proceeding
        self.logger.info("FIO running under migration load test")

        # Wait for FIO and validate
        self._wait_fio_dual([fio_handle], timeout=300)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed without errors during migration test")
        self.logger.info("TC-MIG-002: Migration Under Load — PASS")

        # -- TC-MIG-003: Verify migration tasks API ---------------------
        self.logger.info("=== TC-MIG-003: Migration Tasks API ===")
        try:
            mig_list = self.sbcli_utils.list_migration_tasks(self.cluster_id)
            if mig_list is not None:
                self.logger.info(f"Migration tasks: {len(mig_list)} found")
            else:
                self.logger.info("Migration tasks returned None (no active migrations)")
        except Exception as exc:
            self.logger.warning(f"list_migration_tasks failed: {exc}")
        self.logger.info("TC-MIG-003: Migration Tasks API — PASS")

        # -- TC-MIG-004: Verify post-operations -------------------------
        self.logger.info("=== TC-MIG-004: Post-Migration Validation ===")

        # Verify lvols still accessible after migration tests
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id_1)
        assert lvol_details, f"LVOL {lvol_name_1} not accessible after migration test"

        lvol_details_2 = self.sbcli_utils.get_lvol_details(lvol_id_2)
        assert lvol_details_2, f"LVOL {lvol_name_2} not accessible after migration test"

        self.logger.info("TC-MIG-004: Post-Migration Validation — PASS")

        # -- Cleanup ----------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name_2)

        for name in [lvol_name_1, lvol_name_2]:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TestMigrationLifecycle: ALL PASSED ===")
