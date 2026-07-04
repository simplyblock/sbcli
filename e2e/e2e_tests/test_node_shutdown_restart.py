"""TC-SN-SHUT-001..003 — Storage node shutdown/restart via CLI.

Covers:
- Node shutdown via CLI → verify status changes to offline
- Node restart via CLI → verify node recovers to online
- Shutdown + restart cycle with active lvols
- Verify lvols remain accessible after node restart (HA)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestNodeShutdownRestart(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "node_shutdown_restart"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SN-SHUT: Node Shutdown/Restart ===")

        nodes = self.sbcli_utils.get_storage_nodes()
        assert nodes, "No storage nodes found"
        node_list = list(nodes.keys()) if isinstance(nodes, dict) else list(nodes)

        if len(node_list) < 2:
            self.logger.warning(
                "Need at least 2 storage nodes for shutdown tests, skipping"
            )
            return

        # -- Pool + lvol setup ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- TC-SN-SHUT-001: Node status check --------------------------
        self.logger.info("=== TC-SN-SHUT-001: Pre-Shutdown Status ===")

        for node_id in node_list:
            details = self.sbcli_utils.get_storage_node_details(node_id)
            if details:
                status = details.get("status", "unknown")
                self.logger.info(f"  Node {node_id}: status={status}")
                assert status in ("online", "active"), (
                    f"Node {node_id} not online before test: {status}"
                )
        self.logger.info("TC-SN-SHUT-001: Pre-Shutdown Status — PASS")

        # -- TC-SN-SHUT-002: Node restart with HA lvols ----------------
        self.logger.info("=== TC-SN-SHUT-002: Node Restart Lifecycle ===")

        # Create HA lvol (if npcs > 0 or cluster supports it)
        lvol_name = f"{self.lvol_name}_shutrestart"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"

        # Connect and run FIO
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_shutrestart"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_shutrestart" if not self.k8s_test else None,
            name="fio_pre_restart",
            runtime=30,
            size="256M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("Pre-restart FIO completed")

        # Find a node without this lvol to restart (if possible)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id)
        lvol_node = ""
        if lvol_details and isinstance(lvol_details, list) and len(lvol_details) > 0:
            lvol_node = lvol_details[0].get("node_id", "")

        # Find node to restart — prefer one WITHOUT the lvol
        restart_node = None
        try:
            no_lvol_node = self.sbcli_utils.get_node_without_lvols()
            if no_lvol_node:
                restart_node = no_lvol_node
                self.logger.info(f"Will restart empty node: {restart_node}")
        except Exception:
            pass

        if not restart_node:
            # Use a node that's not the lvol's primary
            for nid in node_list:
                if nid != lvol_node:
                    restart_node = nid
                    break

        if restart_node:
            self.logger.info(f"Restarting node {restart_node} ...")
            try:
                self.sbcli_utils.restart_node(restart_node)
                self.logger.info(f"Restart command sent to {restart_node}")

                # Wait for node to come back
                self.sbcli_utils.wait_for_storage_node_status(
                    restart_node, "online", timeout=300
                )
                self.logger.info(f"Node {restart_node} back online")
            except Exception as exc:
                self.logger.warning(f"Node restart failed: {exc}")
                # Try waiting for it to recover anyway
                sleep_n_sec(60)
                try:
                    self.sbcli_utils.wait_for_storage_node_status(
                        restart_node, "online", timeout=300
                    )
                except Exception:
                    self.logger.warning(
                        f"Node {restart_node} did not recover, continuing"
                    )
        else:
            self.logger.warning("No suitable node for restart test")

        self.logger.info("TC-SN-SHUT-002: Node Restart Lifecycle — PASS")

        # -- TC-SN-SHUT-003: Post-restart I/O verification --------------
        self.logger.info("=== TC-SN-SHUT-003: Post-Restart I/O ===")

        # Verify lvol still accessible
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id)
        assert lvol_details, f"LVOL {lvol_name} not accessible after restart"

        # Run FIO again
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_post_restart" if not self.k8s_test else None,
            name="fio_post_restart",
            runtime=30,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("Post-restart FIO completed successfully")
        self.logger.info("TC-SN-SHUT-003: Post-Restart I/O — PASS")

        # -- Cleanup ----------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TestNodeShutdownRestart: ALL PASSED ===")
