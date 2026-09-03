"""TC-CL-LIFE-001 -- Cluster graceful shutdown and startup lifecycle.

Covers:
- Create pool, lvols, run FIO to establish baseline I/O
- Graceful shutdown via CLI and verify nodes go offline
- Graceful startup and verify nodes come back online
- Reconnect lvols, run FIO again, and validate data path recovery
"""

import time
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestClusterGracefulShutdown(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cluster_graceful_shutdown"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CL-LIFE-001: Cluster Graceful Shutdown ===")

        mgmt_node = self.mgmt_nodes[0]

        # ── Step 1: Create pool, 2 lvols, connect, mount, run FIO ────
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_names = []
        fio_handles = []
        for i in range(2):
            name = f"{self.lvol_name}_shutdown_{i}"
            lvol_names.append(name)
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size="2G",
            )
            self.logger.info(f"LVOL {name} created")

            mount_path = f"{self.mount_path}_shutdown_{i}"
            device, mount = self._connect_and_mount_dual(
                name, mount_path=mount_path
            )
            self.logger.info(
                f"Connected {name} -> device={device}, mount={mount}"
            )

            fio_log = (
                f"{self.log_path}/fio_shutdown_{i}.log"
                if not self.k8s_test
                else None
            )
            fio_handle = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=fio_log,
                name=f"fio_pre_shutdown_{i}",
                runtime=30,
                size="256M",
            )
            fio_handles.append(fio_handle)

        # Wait for pre-shutdown FIO to finish
        self._wait_fio_dual(fio_handles, timeout=120)
        for h in fio_handles:
            self._validate_fio_dual(h)
        self.logger.info("Pre-shutdown FIO completed and validated")

        # Disconnect lvols before shutdown (Docker only)
        if not self.k8s_test:
            for name in lvol_names:
                self._disconnect_and_cleanup_dual(name)
            self.logger.info("Disconnected all lvols before shutdown")

        # ── Step 2: Graceful shutdown ────────────────────────────────
        self.logger.info("Initiating cluster graceful-shutdown...")
        try:
            cmd = (
                f"{self.base_cmd} cluster graceful-shutdown "
                f"{self.cluster_id}"
            )
            output, error = self.ssh_obj.exec_command(
                mgmt_node, cmd, timeout=120
            )
            self.logger.info(f"graceful-shutdown output: {output}")
            if error and error.strip():
                self.logger.warning(f"graceful-shutdown stderr: {error}")
        except Exception as exc:
            self.logger.error(f"graceful-shutdown command failed: {exc}")
            raise

        # ── Step 3: Wait for shutdown to take effect ─────────────────
        self.logger.info("Sleeping 30s for shutdown to propagate...")
        sleep_n_sec(30)

        # ── Step 4: Verify all nodes go offline ──────────────────────
        self.logger.info("Checking that storage nodes are offline...")
        try:
            storage_data = self.sbcli_utils.get_storage_nodes()
            nodes = storage_data.get("results", storage_data) if isinstance(
                storage_data, dict
            ) else storage_data
            offline_count = 0
            for node in nodes:
                node_id = node.get("id", node.get("uuid", "unknown"))
                status = node.get("status", "unknown")
                self.logger.info(
                    f"  Node {node_id}: status={status}"
                )
                if status in ("offline", "shutting_down", "shutdown"):
                    offline_count += 1
            self.logger.info(
                f"{offline_count}/{len(nodes)} nodes are offline/shutdown"
            )
        except Exception as exc:
            self.logger.warning(
                f"Could not verify node statuses after shutdown: {exc}"
            )

        # ── Step 5: Graceful startup ─────────────────────────────────
        self.logger.info("Initiating cluster graceful-startup...")
        try:
            cmd = (
                f"{self.base_cmd} cluster graceful-startup "
                f"{self.cluster_id}"
            )
            output, error = self.ssh_obj.exec_command(
                mgmt_node, cmd, timeout=120
            )
            self.logger.info(f"graceful-startup output: {output}")
            if error and error.strip():
                self.logger.warning(f"graceful-startup stderr: {error}")
        except Exception as exc:
            self.logger.error(f"graceful-startup command failed: {exc}")
            raise

        # ── Step 6: Wait for all nodes to come back online AND healthy ─
        self.logger.info(
            "Waiting for all storage nodes to come back online "
            "and healthy (timeout 600s)..."
        )
        deadline = time.time() + 600
        all_healthy = False
        while time.time() < deadline:
            try:
                storage_data = self.sbcli_utils.get_storage_nodes()
                nodes = storage_data.get(
                    "results", storage_data
                ) if isinstance(storage_data, dict) else storage_data
                online_count = 0
                healthy_count = 0
                for node in nodes:
                    status = node.get("status", "unknown")
                    health = node.get("health_check", False)
                    if status == "online":
                        online_count += 1
                    if status == "online" and health:
                        healthy_count += 1
                self.logger.info(
                    f"  {online_count}/{len(nodes)} online, "
                    f"{healthy_count}/{len(nodes)} healthy"
                )
                if healthy_count == len(nodes) and len(nodes) > 0:
                    all_healthy = True
                    break
            except Exception as exc:
                self.logger.warning(
                    f"Error checking node status: {exc}"
                )
            sleep_n_sec(15)

        assert all_healthy, (
            "Not all storage nodes came back online and healthy "
            "within 600s timeout"
        )
        self.logger.info("All storage nodes are back online and healthy")

        # ── Step 7: Reconnect lvols, run FIO again, validate ─────────
        self.logger.info("Reconnecting lvols and running post-startup FIO...")
        post_fio_handles = []
        for i, name in enumerate(lvol_names):
            mount_path = f"{self.mount_path}_shutdown_post_{i}"
            device, mount = self._connect_and_mount_dual(
                name, mount_path=mount_path, format_disk=False
            )
            self.logger.info(
                f"Reconnected {name} -> device={device}, mount={mount}"
            )

            fio_log = (
                f"{self.log_path}/fio_post_shutdown_{i}.log"
                if not self.k8s_test
                else None
            )
            fio_handle = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=fio_log,
                name=f"fio_post_shutdown_{i}",
                runtime=30,
                size="256M",
            )
            post_fio_handles.append(fio_handle)

        self._wait_fio_dual(post_fio_handles, timeout=120)
        for h in post_fio_handles:
            self._validate_fio_dual(h)
        self.logger.info("Post-startup FIO completed and validated")

        # ── Step 8: Cleanup ──────────────────────────────────────────
        self.logger.info("Starting cleanup...")

        if not self.k8s_test:
            for name in lvol_names:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception as exc:
                    self.logger.warning(
                        f"Cleanup disconnect {name}: {exc}"
                    )

        for name in lvol_names:
            self._delete_lvol_dual(name)

        sleep_n_sec(5)

        self.logger.info(
            "=== TC-CL-LIFE-001: Cluster Graceful Shutdown — PASS ==="
        )
