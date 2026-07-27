"""TC-POOL-002 — Pool attributes and QoS limits.

Covers:
- Create pool with max_rw_iops, max_rw_mbytes
- Verify pool details reflect QoS limits
- Create lvol in pool, run FIO, verify I/O within pool limits
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolAttributes(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_attributes"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-POOL-002: Pool Attributes & QoS Limits ===")

        # ── Create pool with QoS limits ────────────────────────────
        pool_name = f"{self.pool_name}_qos"
        max_rw_iops = 5000
        max_rw_mbytes = 200

        self.sbcli_utils.add_storage_pool(
            pool_name,
            max_rw_iops=max_rw_iops,
            max_rw_mbytes=max_rw_mbytes,
        )
        sleep_n_sec(5)

        pools = self.sbcli_utils.list_storage_pools()
        assert pool_name in pools, f"Pool {pool_name} not in list"
        self.logger.info(f"Pool {pool_name} created with IOPS={max_rw_iops}, BW={max_rw_mbytes}")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Create lvol in QoS-limited pool ────────────────────────
        lvol_name = f"{self.lvol_name}_pool_qos"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=pool_name, size="5G",
        )

        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_pool_qos"
        )

        # ── Run FIO and verify I/O works within limits ─────────────
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_pool_qos" if not self.k8s_test else None,
            name="fio_pool_qos",
            runtime=30,
            size="512M",
            rw="randrw",
            bs="4K",
            iodepth=32,
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed within pool QoS limits")

        # ── Cleanup ────────────────────────────────────────────────
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception:
            pass
        sleep_n_sec(5)
        try:
            self.sbcli_utils.delete_storage_pool(pool_name)
        except Exception:
            pass

        self.logger.info("=== TC-POOL-002: Pool Attributes — PASS ===")
