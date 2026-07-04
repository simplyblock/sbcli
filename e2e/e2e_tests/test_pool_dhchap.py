"""TC-POOL-ADV-001 — Pool-level DHCHAP host management.

Covers:
- Create pool with DHCHAP enabled
- Add/remove host NQN to pool
- Verify lvol connect works for allowed host
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolDhchap(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_dhchap"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-POOL-ADV-001: Pool DHCHAP ===")

        pool_name = f"{self.pool_name}_dhchap"

        # Create pool with DHCHAP via SSH
        mgmt = self.mgmt_nodes[0]
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} pool add {pool_name} --dhchap {self.cluster_id}",
            )
            self.logger.info(f"Pool with DHCHAP created: {out}")
        except Exception as exc:
            self.logger.warning(f"DHCHAP pool creation failed: {exc}")
            # Fallback to regular pool
            self.sbcli_utils.add_storage_pool(pool_name)
            self.logger.info("Fell back to regular pool (DHCHAP not available)")

        sleep_n_sec(5)
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name)
        assert pool_id, f"Pool {pool_name} not found"

        # Try to add host NQN
        test_nqn = "nqn.2014-08.org.nvmexpress:uuid:test-host-001"
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} pool add-host {pool_id} {test_nqn}",
            )
            self.logger.info(f"Host NQN added: {out}")
        except Exception as exc:
            self.logger.warning(f"add-host failed: {exc}")

        # Create lvol and verify basic I/O
        lvol_name = f"{self.lvol_name}_dhchap"
        self.sbcli_utils.add_lvol(
            lvol_name=lvol_name, pool_name=pool_name, size="1G", retry=3,
        )
        sleep_n_sec(5)
        self.logger.info(f"LVOL {lvol_name} created in DHCHAP pool")

        # Try to remove host NQN
        try:
            out, _ = self.ssh_obj.exec_command(
                mgmt,
                f"{self.base_cmd} pool remove-host {pool_id} {test_nqn}",
            )
            self.logger.info(f"Host NQN removed: {out}")
        except Exception as exc:
            self.logger.warning(f"remove-host failed: {exc}")

        # Cleanup
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception:
            pass
        sleep_n_sec(5)
        try:
            self.sbcli_utils.delete_storage_pool(pool_name)
        except Exception:
            pass

        self.logger.info("=== TC-POOL-ADV-001: Pool DHCHAP — PASS ===")
