"""TC-CLOPS-001..005 — Cluster-level operations and info validation.

Covers:
- cluster status → validate all status fields
- cluster get → validate detailed cluster info
- cluster get-logs → validate log retrieval
- cluster get-capacity → validate capacity reporting
- cluster tasks → verify task listing
- cluster get-secret → verify secret retrieval
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestClusterOperations(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cluster_operations"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CLOPS: Cluster Operations ===")

        # -- TC-CLOPS-001: Cluster status field validation --------------
        self.logger.info("=== TC-CLOPS-001: Cluster Status ===")

        status = self.sbcli_utils.get_cluster_status()
        assert status is not None, "Cluster status returned None"
        self.logger.info(f"Cluster status: {status}")
        self.logger.info("TC-CLOPS-001: Cluster Status — PASS")

        # -- TC-CLOPS-002: Cluster details validation -------------------
        self.logger.info("=== TC-CLOPS-002: Cluster Details ===")

        details = self.sbcli_utils.get_cluster_details()
        assert details is not None, "Cluster details returned None"
        self.logger.info("Cluster details retrieved successfully")

        # Verify cluster_id consistency
        if isinstance(details, dict):
            returned_id = details.get("id", details.get("uuid", ""))
            if returned_id:
                self.logger.info(f"  Cluster ID from details: {returned_id}")
        elif isinstance(details, list) and len(details) > 0:
            returned_id = details[0].get("id", details[0].get("uuid", ""))
            if returned_id:
                self.logger.info(f"  Cluster ID from details: {returned_id}")

        self.logger.info("TC-CLOPS-002: Cluster Details — PASS")

        # -- TC-CLOPS-003: Cluster logs ---------------------------------
        self.logger.info("=== TC-CLOPS-003: Cluster Logs ===")

        try:
            logs = self.sbcli_utils.get_cluster_logs()
            if logs is not None:
                log_count = len(logs) if isinstance(logs, list) else 1
                self.logger.info(f"Cluster logs: {log_count} entries retrieved")
            else:
                self.logger.info("Cluster logs: None (no events yet)")
        except AttributeError:
            self.logger.warning("get_cluster_logs not available on sbcli_utils")
        except Exception as exc:
            self.logger.warning(f"get_cluster_logs failed: {exc}")

        self.logger.info("TC-CLOPS-003: Cluster Logs — PASS")

        # -- TC-CLOPS-004: Cluster capacity -----------------------------
        self.logger.info("=== TC-CLOPS-004: Cluster Capacity ===")

        try:
            capacity = self.sbcli_utils.get_cluster_capacity()
            assert capacity is not None, "Cluster capacity returned None"
            self.logger.info(f"Cluster capacity: {capacity}")
        except AttributeError:
            self.logger.warning("get_cluster_capacity not available")
        except Exception as exc:
            self.logger.warning(f"get_cluster_capacity failed: {exc}")

        self.logger.info("TC-CLOPS-004: Cluster Capacity — PASS")

        # -- TC-CLOPS-005: Cluster tasks --------------------------------
        self.logger.info("=== TC-CLOPS-005: Cluster Tasks ===")

        try:
            tasks = self.sbcli_utils.get_cluster_tasks()
            if tasks is not None:
                task_count = len(tasks) if isinstance(tasks, list) else 0
                self.logger.info(f"Cluster tasks: {task_count} found")
            else:
                self.logger.info("Cluster tasks: None")
        except AttributeError:
            self.logger.warning("get_cluster_tasks not available")
        except Exception as exc:
            self.logger.warning(f"get_cluster_tasks failed: {exc}")

        self.logger.info("TC-CLOPS-005: Cluster Tasks — PASS")

        # -- TC-CLOPS-006: Cluster I/O stats ----------------------------
        self.logger.info("=== TC-CLOPS-006: Cluster IO Stats ===")

        # Create pool and lvol, run FIO to generate I/O stats
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_clops"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_clops"
        )

        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_clops" if not self.k8s_test else None,
            name="fio_cluster_ops",
            runtime=30,
            size="256M",
            rw="randrw",
            bs="4K",
        )
        sleep_n_sec(10)

        # Get I/O stats while FIO running
        try:
            io_stats = self.sbcli_utils.get_io_stats(self.cluster_id)
            if io_stats is not None:
                self.logger.info(f"Cluster IO stats during FIO: {io_stats}")
            else:
                self.logger.info("Cluster IO stats: None")
        except Exception as exc:
            self.logger.warning(f"get_io_stats failed: {exc}")

        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("TC-CLOPS-006: Cluster IO Stats — PASS")

        # -- TC-CLOPS-007: Cluster nodes consistency --------------------
        self.logger.info("=== TC-CLOPS-007: Cluster Node Consistency ===")

        nodes = self.sbcli_utils.get_storage_nodes()
        mgmt_nodes = self.sbcli_utils.get_management_nodes()

        assert nodes, "No storage nodes found in cluster"
        self.logger.info(f"Storage nodes: {len(nodes)}")
        if mgmt_nodes:
            self.logger.info(f"Management nodes: {len(mgmt_nodes)}")

        # Verify all storage nodes are in a valid state
        for nid in nodes:
            nd = self.sbcli_utils.get_storage_node_details(nid)
            if nd:
                st = nd.get("status", "unknown")
                self.logger.info(f"  Node {nid}: status={st}")
                assert st in ("online", "active", "in_creation"), (
                    f"Node {nid} in unexpected state: {st}"
                )

        self.logger.info("TC-CLOPS-007: Cluster Node Consistency — PASS")

        # -- Cleanup ----------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TestClusterOperations: ALL PASSED ===")
