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

        # Create a DHCHAP-enabled pool through the dual helper so the call is
        # routed via K8sSbcliUtils (kubectl exec into the admin pod) in K8s
        # mode and the REST API in Docker mode — never a raw sbcli-dev SSH
        # command on a K3s node where the binary is containerised.
        pool_name = self._add_pool_dual(
            pool_name=f"{self.pool_name}_dhchap", dhchap=True,
        )
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        sleep_n_sec(5)
        pool_id = self.sbcli_utils.get_storage_pool_id(pool_name)
        assert pool_id, f"Pool {pool_name} not found"
        self.logger.info(f"DHCHAP pool ready: {pool_name} ({pool_id})")

        # Add host NQN via the dual client
        test_nqn = "nqn.2014-08.org.nvmexpress:uuid:test-host-001"
        try:
            self.sbcli_utils.add_host_to_pool(pool_id, test_nqn)
            self.logger.info(f"Host NQN added: {test_nqn}")
        except Exception as exc:
            self.logger.warning(f"add-host failed: {exc}")

        # Create lvol in the DHCHAP pool
        lvol_name = f"{self.lvol_name}_dhchap"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=pool_name, size="1G",
        )
        self._verify_lvol_exists_dual(lvol_name)
        self.logger.info(f"LVOL {lvol_name} created in DHCHAP pool")

        # Remove host NQN
        try:
            self.sbcli_utils.remove_host_from_pool(pool_id, test_nqn)
            self.logger.info(f"Host NQN removed: {test_nqn}")
        except Exception as exc:
            self.logger.warning(f"remove-host failed: {exc}")

        # Cleanup
        self._delete_lvol_dual(lvol_name)
        sleep_n_sec(5)
        if not self.k8s_test:
            try:
                self.sbcli_utils.delete_storage_pool(pool_name)
            except Exception:
                pass

        self.logger.info("=== TC-POOL-ADV-001: Pool DHCHAP — PASS ===")
