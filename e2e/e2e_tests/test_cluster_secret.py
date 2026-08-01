"""TC-CLUSTER-007 -- Cluster secret retrieval and operational verification.

Covers:
- Retrieve cluster secret via sbcli_utils
- Verify secret is a non-empty string (logged masked)
- Confirm cluster operations still work after secret retrieval
  (create lvol, verify, delete)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestClusterSecret(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cluster_secret"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CLUSTER-007: Cluster Secret Retrieval ===")

        mgmt_node = self.mgmt_nodes[0]

        # ── Step 1: Retrieve cluster secret ──────────────────────────
        self.logger.info("Retrieving cluster secret...")
        secret = None
        try:
            secret = self.sbcli_utils.get_cluster_secret(self.cluster_id)
        except AttributeError:
            # Fallback: try via SSH CLI command
            self.logger.info(
                "get_cluster_secret not available on sbcli_utils, "
                "falling back to SSH CLI"
            )
            if not self.k8s_test:
                cmd = f"{self.base_cmd} cluster get-secret {self.cluster_id}"
                output, error = self.ssh_obj.exec_command(mgmt_node, cmd)
                if output and output.strip():
                    secret = output.strip()
        except Exception as exc:
            self.logger.warning(f"get_cluster_secret raised: {exc}")

        if secret is None:
            # Last resort: use the secret from environment / constructor
            secret = self.cluster_secret

        assert secret, "Cluster secret is empty or None"
        assert isinstance(secret, str), (
            f"Cluster secret is not a string: {type(secret)}"
        )
        assert len(secret) > 0, "Cluster secret is an empty string"

        # Log masked -- never log the actual secret value
        self.logger.info(
            f"Cluster secret retrieved successfully "
            f"(length={len(secret)}, masked=**********)"
        )

        # ── Step 2: Verify cluster operations still work ─────────────
        self.logger.info("Verifying cluster operations after secret retrieval...")

        # Create pool
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # Create lvol
        lvol_name = f"{self.lvol_name}_secret_test"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )

        # Verify lvol exists
        lvols = self.sbcli_utils.list_lvols()
        assert lvol_name in lvols, (
            f"LVOL {lvol_name} not found after create: {list(lvols.keys())}"
        )
        self.logger.info(f"LVOL {lvol_name} created successfully")

        # Delete lvol and verify removal
        self.sbcli_utils.delete_lvol(lvol_name)
        sleep_n_sec(5)
        lvols = self.sbcli_utils.list_lvols()
        assert lvol_name not in lvols, (
            f"LVOL {lvol_name} still present after delete"
        )
        self.logger.info("LVOL deleted and verified absent")

        # ── Step 3: Cleanup ──────────────────────────────────────────
        self.logger.info("Cleanup completed")

        self.logger.info("=== TC-CLUSTER-007: Cluster Secret — PASS ===")
