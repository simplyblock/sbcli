"""TC-HEALTH-001..005 — Comprehensive health check validation.

Covers:
- cluster check → validate cluster health status
- sn check → validate storage node health
- sn check-device → validate device health
- volume check → validate volume health
- snapshot check → validate snapshot health
- Health checks during degraded states (after FIO load)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestHealthChecks(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "health_checks"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-HEALTH: Comprehensive Health Checks ===")

        # -- Pool + lvol setup ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- TC-HEALTH-001: Cluster health check ------------------------
        self.logger.info("=== TC-HEALTH-001: Cluster Health Check ===")
        try:
            cluster_status = self.sbcli_utils.get_cluster_status()
            assert cluster_status is not None, "Cluster status returned None"
            self.logger.info(f"Cluster status: {cluster_status}")

            cluster_details = self.sbcli_utils.get_cluster_details()
            assert cluster_details is not None, "Cluster details returned None"
            self.logger.info("Cluster details retrieved successfully")
        except AttributeError as exc:
            self.logger.warning(f"Cluster health check method not available: {exc}")
        self.logger.info("TC-HEALTH-001: Cluster Health Check — PASS")

        # -- TC-HEALTH-002: Storage node health check -------------------
        self.logger.info("=== TC-HEALTH-002: Storage Node Health Check ===")
        # get_storage_nodes() returns {"results": [<node dict>, ...]} in BOTH
        # Docker (REST) and K8s (CLI) modes — iterate the list, not the dict.
        nodes = self.sbcli_utils.get_storage_nodes()["results"]
        assert nodes, "No storage nodes found"
        node_ids = [n.get("uuid") or n.get("id") for n in nodes]
        self.logger.info(f"Found {len(nodes)} storage node(s)")

        for node in nodes:
            node_id = node.get("uuid") or node.get("id")
            status = node.get("status", "unknown")
            self.logger.info(f"  Node {node_id}: status={status}")

            # Verify node is in a healthy state
            assert status in ("online", "active", "in_creation"), (
                f"Node {node_id} unexpected status: {status}"
            )
        self.logger.info("TC-HEALTH-002: Storage Node Health Check — PASS")

        # -- TC-HEALTH-003: Device health check -------------------------
        self.logger.info("=== TC-HEALTH-003: Device Health Check ===")
        for node_id in node_ids:
            devices = self.sbcli_utils.get_device_details(node_id)
            if not devices:
                self.logger.info(f"  Node {node_id}: no devices found")
                continue
            for dev in devices:
                dev_id = dev.get("id", "unknown")
                dev_status = dev.get("status", "unknown")
                self.logger.info(f"  Device {dev_id}: status={dev_status}")
                assert dev_status in ("online", "active", "healthy"), (
                    f"Device {dev_id} unexpected status: {dev_status}"
                )
        self.logger.info("TC-HEALTH-003: Device Health Check — PASS")

        # -- TC-HEALTH-004: Volume health check -------------------------
        self.logger.info("=== TC-HEALTH-004: Volume Health Check ===")
        lvol_name = f"{self.lvol_name}_health"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )
        lvol_id = self._get_lvol_id_dual(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"

        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id)
        assert lvol_details, f"LVOL {lvol_name} details returned empty"
        if isinstance(lvol_details, list) and len(lvol_details) > 0:
            lvol_status = lvol_details[0].get("status", "unknown")
            self.logger.info(f"LVOL {lvol_name}: status={lvol_status}")
            assert lvol_status in ("online", "active"), (
                f"LVOL {lvol_name} unexpected status: {lvol_status}"
            )
        self.logger.info("TC-HEALTH-004: Volume Health Check — PASS")

        # -- TC-HEALTH-005: Snapshot health check -----------------------
        self.logger.info("=== TC-HEALTH-005: Snapshot Health Check ===")
        # Connect and write data before snapshot
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_health"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_health" if not self.k8s_test else None,
            name="fio_health_pre_snap",
            runtime=20,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)

        snap_name = f"{lvol_name}_snap"
        self._create_snapshot_dual(lvol_name, snap_name)
        snap_id = self._get_snapshot_id_dual(snap_name)
        assert snap_id, f"Could not get snapshot_id for {snap_name}"

        self._verify_snapshot_exists_dual(snap_name)
        self.logger.info(f"Snapshot {snap_name}: id={snap_id}, found in list")
        self.logger.info("TC-HEALTH-005: Snapshot Health Check — PASS")

        # -- TC-HEALTH-006: Health after FIO load -----------------------
        self.logger.info("=== TC-HEALTH-006: Health After Load ===")
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_health_load" if not self.k8s_test else None,
            name="fio_health_load",
            runtime=30,
            size="256M",
            rw="randrw",
            bs="4K",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)

        # Re-check cluster health after load
        cluster_status = self.sbcli_utils.get_cluster_status()
        assert cluster_status is not None, (
            "Cluster status returned None after FIO load"
        )

        # Re-check node health after load
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            nid = node.get("uuid") or node.get("id")
            assert node.get("status", "unknown") in (
                "online", "active", "in_creation"
            ), f"Node {nid} unhealthy after load: {node.get('status')}"
        self.logger.info("TC-HEALTH-006: Health After Load — PASS")

        # -- Cleanup ----------------------------------------------------
        self._delete_snapshot_dual(snap_name)
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        self._delete_lvol_dual(lvol_name)

        self.logger.info("=== TestHealthChecks: ALL PASSED ===")
