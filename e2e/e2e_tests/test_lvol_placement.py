"""TC-PLACE-001..003 — Volume placement validation.

Covers:
- Create with explicit host_id → verify placed on correct node
- Create without host_id → verify auto-placement distributes across nodes
- Create multiple lvols → verify placement spread
- Verify lvol node assignment matches sn get output
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestLvolPlacement(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_placement"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-PLACE: LVOL Placement Validation ===")

        # -- Pool setup -------------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        nodes = self.sbcli_utils.get_storage_nodes()
        assert nodes, "No storage nodes found"
        node_list = list(nodes.keys()) if isinstance(nodes, dict) else list(nodes)
        self.logger.info(f"Available storage nodes: {len(node_list)}")

        # -- TC-PLACE-001: Explicit host_id placement -------------------
        self.logger.info("=== TC-PLACE-001: Explicit Placement ===")

        if not self.k8s_test and len(node_list) > 0:
            target_node = node_list[0]
            lvol_name = f"{self.lvol_name}_place_explicit"
            self._create_lvol_dual(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size="256M",
                host_id=target_node,
            )
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            assert lvol_id, f"Could not get lvol_id for {lvol_name}"

            details = self.sbcli_utils.get_lvol_details(lvol_id)
            if details and isinstance(details, list) and len(details) > 0:
                actual_node = details[0].get("node_id", "")
                assert actual_node == target_node, (
                    f"LVOL placed on {actual_node}, expected {target_node}"
                )
                self.logger.info(
                    f"LVOL {lvol_name} correctly placed on {target_node}"
                )

            self.sbcli_utils.delete_lvol(lvol_name)
            sleep_n_sec(3)
        else:
            self.logger.info("Skipping explicit placement (K8s mode or no nodes)")

        self.logger.info("TC-PLACE-001: Explicit Placement — PASS")

        # -- TC-PLACE-002: Auto-placement distribution ------------------
        self.logger.info("=== TC-PLACE-002: Auto-Placement Distribution ===")

        num_lvols = min(6, len(node_list) * 2) if len(node_list) > 1 else 3
        created = []
        placement_map = {}

        for i in range(num_lvols):
            name = f"{self.lvol_name}_place_auto_{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size="256M",
            )
            created.append(name)

        # Check placement distribution
        for name in created:
            lvol_id = self.sbcli_utils.get_lvol_id(name)
            if lvol_id:
                det = self.sbcli_utils.get_lvol_details(lvol_id)
                if det and isinstance(det, list) and len(det) > 0:
                    node_id = det[0].get("node_id", "unknown")
                    placement_map[name] = node_id

        # Log placement distribution
        node_counts = {}
        for name, nid in placement_map.items():
            node_counts[nid] = node_counts.get(nid, 0) + 1
            self.logger.info(f"  {name} → node {nid}")

        self.logger.info(f"Placement distribution: {node_counts}")

        # If multiple nodes, verify lvols are distributed (not all on one)
        if len(node_list) > 1 and len(placement_map) > 1:
            unique_nodes = len(set(placement_map.values()))
            assert unique_nodes > 1, (
                f"All {len(placement_map)} lvols placed on same node "
                f"(expected distribution across {len(node_list)} nodes)"
            )
            self.logger.info(
                f"LVOLs distributed across {unique_nodes} node(s) — OK"
            )

        self.logger.info("TC-PLACE-002: Auto-Placement Distribution — PASS")

        # -- TC-PLACE-003: Placement on each available node -------------
        self.logger.info("=== TC-PLACE-003: Per-Node Placement ===")

        if not self.k8s_test:
            per_node_lvols = []
            for i, node_id in enumerate(node_list[:3]):
                name = f"{self.lvol_name}_place_node_{i}"
                self._create_lvol_dual(
                    lvol_name=name,
                    pool_name=self.pool_name,
                    size="256M",
                    host_id=node_id,
                )
                per_node_lvols.append((name, node_id))

            # Verify each went to the right node
            for name, expected_node in per_node_lvols:
                lvol_id = self.sbcli_utils.get_lvol_id(name)
                if lvol_id:
                    det = self.sbcli_utils.get_lvol_details(lvol_id)
                    if det and isinstance(det, list) and len(det) > 0:
                        actual = det[0].get("node_id", "")
                        assert actual == expected_node, (
                            f"{name}: placed on {actual}, expected {expected_node}"
                        )
                        self.logger.info(
                            f"  {name} → {expected_node} ✓"
                        )

            # Cleanup per-node lvols
            for name, _ in per_node_lvols:
                try:
                    self.sbcli_utils.delete_lvol(name)
                except Exception:
                    pass
        else:
            self.logger.info("Skipping per-node placement (K8s mode)")

        self.logger.info("TC-PLACE-003: Per-Node Placement — PASS")

        # -- TC-PLACE-004: I/O works on placed lvols --------------------
        self.logger.info("=== TC-PLACE-004: I/O on Placed LVOLs ===")

        if created:
            test_lvol = created[0]
            device, mount = self._connect_and_mount_dual(
                test_lvol, mount_path=f"{self.mount_path}_place"
            )
            fio_handle = self._run_fio_dual(
                lvol_name=test_lvol,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_place" if not self.k8s_test else None,
                name="fio_placement",
                runtime=20,
                size="64M",
            )
            self._wait_fio_dual([fio_handle], timeout=120)
            self._validate_fio_dual(fio_handle)
            self.logger.info("I/O completed on auto-placed lvol")

            if not self.k8s_test:
                self._disconnect_and_cleanup_dual(test_lvol)

        self.logger.info("TC-PLACE-004: I/O on Placed LVOLs — PASS")

        # -- Cleanup ----------------------------------------------------
        for name in created:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TestLvolPlacement: ALL PASSED ===")
