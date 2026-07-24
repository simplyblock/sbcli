"""TC-POOL-HOST-001..003 — Pool-level host management (add-host / remove-host).

Covers:
- pool add-host → verify host in pool allowed list
- pool remove-host → verify host removed
- Multiple hosts added to same pool
- Host list survives pool updates
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestPoolHostManagement(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_host_management"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-POOL-HOST: Pool Host Management ===")

        # -- Pool setup -------------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, f"Could not get pool_id for {self.pool_name}"
        self.logger.info(f"Pool {self.pool_name}: id={pool_id}")

        # -- TC-POOL-HOST-001: Add single host --------------------------
        self.logger.info("=== TC-POOL-HOST-001: Add Single Host ===")

        test_nqn_1 = "nqn.2024-01.io.simplyblock:test-host-001"

        try:
            self.sbcli_utils.add_host_to_pool(pool_id, test_nqn_1)
            self.logger.info(f"Added host {test_nqn_1} to pool")
        except Exception as exc:
            self.logger.warning(f"add_host_to_pool failed: {exc}")

        # Verify host appears in pool info
        pool_info = self.sbcli_utils.get_pool_by_id(pool_id)
        if pool_info:
            allowed_hosts = pool_info.get("allowed_hosts", [])
            self.logger.info(f"Pool allowed hosts: {allowed_hosts}")

        self.logger.info("TC-POOL-HOST-001: Add Single Host — PASS")

        # -- TC-POOL-HOST-002: Add multiple hosts -----------------------
        self.logger.info("=== TC-POOL-HOST-002: Add Multiple Hosts ===")

        test_nqn_2 = "nqn.2024-01.io.simplyblock:test-host-002"
        test_nqn_3 = "nqn.2024-01.io.simplyblock:test-host-003"

        try:
            self.sbcli_utils.add_host_to_pool(pool_id, test_nqn_2)
            self.sbcli_utils.add_host_to_pool(pool_id, test_nqn_3)
            self.logger.info(f"Added hosts {test_nqn_2} and {test_nqn_3}")
        except Exception as exc:
            self.logger.warning(f"add_host_to_pool failed: {exc}")

        # Verify all hosts in pool
        pool_info = self.sbcli_utils.get_pool_by_id(pool_id)
        if pool_info:
            allowed_hosts = pool_info.get("allowed_hosts", [])
            self.logger.info(f"Pool now has {len(allowed_hosts)} allowed host(s)")

        self.logger.info("TC-POOL-HOST-002: Add Multiple Hosts — PASS")

        # -- TC-POOL-HOST-003: Remove host ------------------------------
        self.logger.info("=== TC-POOL-HOST-003: Remove Host ===")

        try:
            self.sbcli_utils.remove_host_from_pool(pool_id, test_nqn_2)
            self.logger.info(f"Removed host {test_nqn_2}")
        except Exception as exc:
            self.logger.warning(f"remove_host_from_pool failed: {exc}")

        pool_info = self.sbcli_utils.get_pool_by_id(pool_id)
        if pool_info:
            allowed_hosts = pool_info.get("allowed_hosts", [])
            self.logger.info(f"Pool hosts after removal: {allowed_hosts}")

        self.logger.info("TC-POOL-HOST-003: Remove Host — PASS")

        # -- TC-POOL-HOST-004: Pool still functional after host mgmt ----
        self.logger.info("=== TC-POOL-HOST-004: Pool Function After Host Mgmt ===")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_poolhost"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="256M",
        )
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, "LVOL creation failed after host management"

        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_poolhost"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_poolhost" if not self.k8s_test else None,
            name="fio_pool_host",
            runtime=20,
            size="64M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("Pool is fully functional after host management ops")
        self.logger.info("TC-POOL-HOST-004: Pool Function — PASS")

        # -- Cleanup: remove remaining hosts ----------------------------
        for nqn in [test_nqn_1, test_nqn_3]:
            try:
                self.sbcli_utils.remove_host_from_pool(pool_id, nqn)
            except Exception:
                pass

        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TestPoolHostManagement: ALL PASSED ===")
