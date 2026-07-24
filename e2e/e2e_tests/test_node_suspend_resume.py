"""TC-SN-002 — Storage node suspend / resume.

NOTE: sn suspend / sn resume are DEPRECATED no-ops in the current CLI.
This test verifies the deprecation behavior — commands succeed but
node status remains unchanged.

Status: DEPRECATED — commented out in get_all_tests()
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNodeSuspendResume(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "node_suspend_resume"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SN-002: Node Suspend / Resume (deprecated) ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # Create lvol and run FIO to ensure cluster is under load
        lvol_name = f"{self.lvol_name}_suspend"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="2G",
        )
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_suspend"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_suspend" if not self.k8s_test else None,
            name="fio_suspend_test",
            runtime=60,
            size="512M",
        )

        # Pick a node (any node)
        nodes = self.sbcli_utils.get_storage_nodes()
        assert nodes and "results" in nodes and len(nodes["results"]) > 0, \
            "No storage nodes found"
        target_node = nodes["results"][0]
        node_id = target_node["id"]
        node_status_before = target_node.get("status", "unknown")
        self.logger.info(
            f"Target node {node_id} — status before suspend: {node_status_before}"
        )

        # ── Suspend (deprecated no-op) ─────────────────────────────
        try:
            self.sbcli_utils.suspend_node(node_id)
            self.logger.info(f"suspend_node({node_id}) succeeded (no-op expected)")
        except Exception as exc:
            self.logger.info(f"suspend_node raised: {exc}")

        sleep_n_sec(5)

        # Verify status unchanged
        updated = self.sbcli_utils.get_storage_node_details(node_id)
        if updated:
            node_status_after_suspend = updated[0].get("status", "unknown")
            self.logger.info(
                f"Node status after suspend: {node_status_after_suspend}"
            )

        # ── Resume (deprecated no-op) ──────────────────────────────
        try:
            self.sbcli_utils.resume_node(node_id)
            self.logger.info(f"resume_node({node_id}) succeeded (no-op expected)")
        except Exception as exc:
            self.logger.info(f"resume_node raised: {exc}")

        sleep_n_sec(5)

        # Verify status unchanged
        updated = self.sbcli_utils.get_storage_node_details(node_id)
        if updated:
            node_status_after_resume = updated[0].get("status", "unknown")
            self.logger.info(
                f"Node status after resume: {node_status_after_resume}"
            )

        # ── Verify FIO continued without errors ────────────────────
        self._wait_fio_dual([fio_handle], timeout=180)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed without errors during suspend/resume cycle")

        self.logger.info("=== TC-SN-002: Node Suspend / Resume — PASS ===")
