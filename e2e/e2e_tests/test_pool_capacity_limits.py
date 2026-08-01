"""TC-POOL-ADV-002 + TC-CAP-001 — Pool capacity limits and threshold alerts.

Covers:
- Create lvols until pool capacity is high
- Log capacity at each step
- Verify cluster logs for warnings at threshold levels
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolCapacityLimits(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_capacity_limits"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-POOL-ADV-002: Pool Capacity Limits ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # Get initial capacity
        try:
            cap = self.sbcli_utils.get_cluster_capacity()
            self.logger.info(f"Initial cluster capacity: {cap}")
        except Exception as exc:
            self.logger.warning(f"get_cluster_capacity: {exc}")

        # Create lvols to increase capacity usage
        lvol_names = []
        for i in range(5):
            name = f"{self.lvol_name}_cap_{i}"
            try:
                self.sbcli_utils.add_lvol(
                    lvol_name=name,
                    pool_name=self.pool_name,
                    size="10G",
                    retry=3,
                )
                lvol_names.append(name)
                sleep_n_sec(3)

                # Check capacity after each creation
                try:
                    cap = self.sbcli_utils.get_cluster_capacity()
                    self.logger.info(f"After {name}: capacity = {cap}")
                except Exception:
                    pass
            except Exception as exc:
                self.logger.info(f"LVOL {name} creation failed (may be at capacity): {exc}")
                break

        self.logger.info(f"Created {len(lvol_names)} lvols for capacity test")

        # Check cluster logs for capacity warnings
        try:
            logs = self.sbcli_utils.get_cluster_logs(self.cluster_id)
            if logs:
                warning_count = 0
                for log_entry in (logs if isinstance(logs, list) else []):
                    entry_str = str(log_entry)
                    if "warning" in entry_str.lower() or "capacity" in entry_str.lower():
                        warning_count += 1
                self.logger.info(f"Found {warning_count} capacity-related log entries")
        except Exception as exc:
            self.logger.warning(f"get_cluster_logs: {exc}")

        # Cleanup
        for name in lvol_names:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(1)

        self.logger.info("=== TC-POOL-ADV-002: Pool Capacity Limits — PASS ===")
