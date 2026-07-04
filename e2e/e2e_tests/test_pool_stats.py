"""TC-POOL-004 -- Pool capacity and IO statistics under load.

Covers:
- Create pool, lvol, connect, mount, and run FIO
- Query pool capacity via CLI
- Query pool IO stats via CLI
- Log results and validate FIO completion
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolStats(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_stats"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-POOL-004: Pool Statistics ===")

        mgmt_node = self.mgmt_nodes[0]

        # ── Step 1: Create pool, lvol, connect, mount, start FIO ─────
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_pool_stats"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="5G",
        )
        self.logger.info(f"LVOL {lvol_name} created")

        mount_path = f"{self.mount_path}_pool_stats"
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=mount_path
        )
        self.logger.info(
            f"Connected {lvol_name} -> device={device}, mount={mount}"
        )

        fio_log = (
            f"{self.log_path}/fio_pool_stats.log"
            if not self.k8s_test
            else None
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=fio_log,
            name="fio_pool_stats",
            runtime=60,
            size="1G",
        )
        self.logger.info("FIO started (60s runtime)")

        # ── Step 2: Get pool ID ──────────────────────────────────────
        pool_id = self.sbcli_utils.get_storage_pool_id(self.pool_name)
        assert pool_id, (
            f"Could not retrieve pool ID for {self.pool_name}"
        )
        self.logger.info(f"Pool ID: {pool_id}")

        # ── Step 3: Query pool capacity ──────────────────────────────
        self.logger.info("Querying pool capacity...")
        try:
            if not self.k8s_test:
                cmd = (
                    f"{self.base_cmd} pool get-capacity {pool_id}"
                )
                output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
                self.logger.info(f"Pool capacity output:\n{output}")
                if error and error.strip():
                    self.logger.warning(f"Pool capacity stderr: {error}")
            else:
                # In K8s mode, try via sbcli_utils if method exists
                if hasattr(self.sbcli_utils, "get_pool_capacity"):
                    cap = self.sbcli_utils.get_pool_capacity(pool_id)
                    self.logger.info(f"Pool capacity: {cap}")
                else:
                    self.logger.info(
                        "get_pool_capacity not available in K8s mode; skipping"
                    )
        except Exception as exc:
            self.logger.warning(f"Pool capacity query failed: {exc}")

        # ── Step 4: Query pool IO stats ──────────────────────────────
        self.logger.info("Querying pool IO stats...")
        try:
            if not self.k8s_test:
                cmd = (
                    f"{self.base_cmd} pool get-io-stats {pool_id}"
                )
                output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
                self.logger.info(f"Pool IO stats output:\n{output}")
                if error and error.strip():
                    self.logger.warning(f"Pool IO stats stderr: {error}")
            else:
                if hasattr(self.sbcli_utils, "get_pool_io_stats"):
                    stats = self.sbcli_utils.get_pool_io_stats(pool_id)
                    self.logger.info(f"Pool IO stats: {stats}")
                else:
                    self.logger.info(
                        "get_pool_io_stats not available in K8s mode; skipping"
                    )
        except Exception as exc:
            self.logger.warning(f"Pool IO stats query failed: {exc}")

        # ── Step 5: Wait for FIO, validate ───────────────────────────
        self.logger.info("Waiting for FIO to complete...")
        self._wait_fio_dual([fio_handle], timeout=300)
        self._validate_fio_dual(fio_handle, log_path=fio_log)
        self.logger.info("FIO completed and validated")

        # ── Step 6: Cleanup ──────────────────────────────────────────
        self.logger.info("Starting cleanup...")

        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)

        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete lvol {lvol_name}: {exc}")

        sleep_n_sec(5)

        self.logger.info("=== TC-POOL-004: Pool Statistics — PASS ===")
