# DEPRECATED / UNCERTAIN — Shared-placement is an advanced feature whose
# availability in CI is unclear.  This test is commented out in
# get_all_tests() and should not be included until the feature is confirmed.
"""TC-FD-003 — Shared Placement (UNCERTAIN).

UNCERTAIN: The ``cluster set-shared-placement`` command may not be enabled or
supported in all environments.  This test attempts to toggle shared placement,
creates lvols with a distributed RAID configuration, and logs the resulting
placement information.  It is commented out in get_all_tests().
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestSharedPlacement(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "shared_placement"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-FD-003: Shared Placement (UNCERTAIN) ===")

        # ── Try to enable shared placement ───────────────────────────
        if not self.k8s_test and self.mgmt_nodes:
            mgmt = self.mgmt_nodes[0]
            try:
                result = self.ssh_obj.exec_command(
                    mgmt,
                    f"{self.base_cmd} cluster set-shared-placement {self.cluster_id}",
                )
                self.logger.info(f"set-shared-placement result: {result}")
            except Exception as exc:
                self.logger.warning(
                    f"set-shared-placement is not available or failed: {exc}. "
                    "Continuing with default placement."
                )
        else:
            self.logger.info(
                "Skipping set-shared-placement (K8s mode or no mgmt nodes)"
            )

        # ── Pool create / verify ─────────────────────────────────────
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Create lvols with distributed RAID config ────────────────
        lvol_names = []
        for i in range(3):
            name = f"{self.lvol_name}_sp{i}"
            try:
                self._create_lvol_dual(
                    lvol_name=name,
                    pool_name=self.pool_name,
                    size="2G",
                    ndcs=self.ndcs,
                    npcs=self.npcs,
                )
                lvol_names.append(name)
            except Exception as exc:
                self.logger.warning(
                    f"Failed to create lvol {name} with distributed RAID "
                    f"config (ndcs={self.ndcs}, npcs={self.npcs}): {exc}"
                )
        self.logger.info(f"Created {len(lvol_names)} lvols: {lvol_names}")

        # ── Get lvol details and log placement info ──────────────────
        lvols = self.sbcli_utils.list_lvols()
        for name in lvol_names:
            try:
                lvol_id = lvols.get(name)
                if lvol_id:
                    details = self.sbcli_utils.get_lvol_details(lvol_id)
                    self.logger.info(f"Placement info for {name}: {details}")
                else:
                    self.logger.warning(f"LVOL {name} not found in lvol list")
            except Exception as exc:
                self.logger.warning(f"Could not get details for {name}: {exc}")

        # ── Cleanup ──────────────────────────────────────────────────
        for name in lvol_names:
            try:
                if not self.k8s_test:
                    self._disconnect_and_cleanup_dual(name)
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup error for {name}: {exc}")
        sleep_n_sec(5)

        self.logger.info("=== TC-FD-003: Shared Placement — PASS ===")
