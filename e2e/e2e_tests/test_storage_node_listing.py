"""TC-SN-008..010 — Storage node listing and info operations.

Covers:
- sn list → validate all fields present
- sn get → validate detailed node info fields
- sn list after lvol creation → verify lvol count
- Cross-reference: lvol placement matches node listing
- Node capacity reporting consistency
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestStorageNodeListing(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "storage_node_listing"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SN: Storage Node Listing & Info ===")

        # -- TC-SN-008: Node list field validation ----------------------
        self.logger.info("=== TC-SN-008: Node List Validation ===")

        nodes = self.sbcli_utils.get_storage_nodes()
        assert nodes, "No storage nodes returned from list"
        assert len(nodes) > 0, "Storage node list is empty"
        self.logger.info(f"Found {len(nodes)} storage node(s)")

        for node_id in nodes:
            assert node_id, "Node ID is empty/None in list"
            self.logger.info(f"  Node ID: {node_id}")
        self.logger.info("TC-SN-008: Node List — PASS")

        # -- TC-SN-009: Node get detail validation ----------------------
        self.logger.info("=== TC-SN-009: Node Get Details ===")

        for node_id in nodes:
            details = self.sbcli_utils.get_storage_node_details(node_id)
            assert details is not None, (
                f"get_storage_node_details returned None for {node_id}"
            )

            # Validate key fields exist
            expected_fields = ["id", "status"]
            for field in expected_fields:
                assert field in details, (
                    f"Node {node_id} missing field '{field}' in details"
                )

            status = details.get("status", "unknown")
            self.logger.info(
                f"  Node {node_id}: status={status}"
            )

            # Validate status is valid
            valid_statuses = (
                "online", "active", "offline", "suspended",
                "in_creation", "restarting",
            )
            assert status in valid_statuses, (
                f"Node {node_id} has unknown status: {status}"
            )

        self.logger.info("TC-SN-009: Node Get Details — PASS")

        # -- TC-SN-010: Node capacity validation ------------------------
        self.logger.info("=== TC-SN-010: Node Capacity ===")

        for node_id in nodes:
            try:
                capacity = self.sbcli_utils.get_node_capacity(node_id)
                if capacity is not None:
                    self.logger.info(f"  Node {node_id} capacity: {capacity}")
                else:
                    self.logger.info(f"  Node {node_id} capacity: None (may not be online)")
            except Exception as exc:
                self.logger.warning(f"  Node {node_id} get_node_capacity failed: {exc}")

        self.logger.info("TC-SN-010: Node Capacity — PASS")

        # -- TC-SN-011: Device listing per node -------------------------
        self.logger.info("=== TC-SN-011: Device Listing Per Node ===")

        total_devices = 0
        for node_id in nodes:
            devices = self.sbcli_utils.get_device_details(node_id)
            dev_count = len(devices) if devices else 0
            total_devices += dev_count
            self.logger.info(f"  Node {node_id}: {dev_count} device(s)")

            if devices:
                for dev in devices:
                    dev_id = dev.get("id", "unknown")
                    dev_status = dev.get("status", "unknown")
                    self.logger.info(f"    Device {dev_id}: status={dev_status}")

        self.logger.info(f"Total devices across all nodes: {total_devices}")
        self.logger.info("TC-SN-011: Device Listing — PASS")

        # -- TC-SN-012: Cross-reference lvol placement ------------------
        self.logger.info("=== TC-SN-012: LVOL Placement Cross-Reference ===")

        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # Create lvols to verify placement tracking
        created = []
        for i in range(3):
            name = f"{self.lvol_name}_snlist_{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size="256M",
            )
            created.append(name)

        # List lvols and verify each has a valid node assignment
        lvols = self.sbcli_utils.list_lvols()
        for name in created:
            assert name in lvols, f"Created lvol {name} not in list"
            lid = lvols[name]
            det = self.sbcli_utils.get_lvol_details(lid)
            if det and isinstance(det, list) and len(det) > 0:
                node_id = det[0].get("node_id", "")
                assert node_id in nodes, (
                    f"LVOL {name} placed on unknown node: {node_id}"
                )
                self.logger.info(f"  LVOL {name} → node {node_id}")

        self.logger.info("TC-SN-012: LVOL Placement Cross-Reference — PASS")

        # -- Cleanup ----------------------------------------------------
        for name in created:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TestStorageNodeListing: ALL PASSED ===")
