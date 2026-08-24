"""TC-POOL-005 — Pool negative / error-handling cases.

Covers:
- Duplicate pool name
- Delete pool with active lvols
- Delete non-existent pool
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolNegativeCases(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_negative_cases"
        self.logger = setup_logger(__name__)

    def _expect_failure(self, operation, fn, *args, **kwargs):
        try:
            fn(*args, **kwargs)
            self.logger.error(f"[{operation}] Expected failure but succeeded")
            return False
        except Exception as exc:
            self.logger.info(f"[{operation}] Correctly failed: {exc}")
            return True

    def run(self):
        self.logger.info("=== TC-POOL-005: Pool Negative Cases ===")
        failures = []

        # ── 1. Create pool → success ──────────────────────────────
        pool_name = f"{self.pool_name}_neg"
        self.sbcli_utils.add_storage_pool(pool_name)
        sleep_n_sec(5)
        pools = self.sbcli_utils.list_storage_pools()
        assert pool_name in pools, f"Pool {pool_name} not created"
        self.logger.info(f"Pool {pool_name} created")

        # ── 2. Duplicate pool name ─────────────────────────────────
        if not self._expect_failure(
            "duplicate_pool",
            self.sbcli_utils.add_storage_pool,
            pool_name,
        ):
            failures.append("duplicate_pool: should have failed")

        # ── 3. Delete pool with active lvol ────────────────────────
        lvol_name = f"{self.lvol_name}_pool_neg"
        self.sbcli_utils.add_lvol(
            lvol_name=lvol_name,
            pool_name=pool_name,
            size="1G",
            retry=3,
        )
        sleep_n_sec(5)

        if not self._expect_failure(
            "delete_pool_with_lvol",
            self.sbcli_utils.delete_storage_pool,
            pool_name,
        ):
            failures.append("delete_pool_with_lvol: should have failed")

        # ── 4. Delete lvol → then delete empty pool → success ─────
        self.sbcli_utils.delete_lvol(lvol_name)
        sleep_n_sec(5)
        self.sbcli_utils.delete_storage_pool(pool_name)
        sleep_n_sec(5)
        pools = self.sbcli_utils.list_storage_pools()
        assert pool_name not in pools, f"Pool {pool_name} still present after delete"
        self.logger.info("Empty pool deleted successfully")

        # ── 5. Delete non-existent pool ────────────────────────────
        if not self._expect_failure(
            "delete_nonexistent_pool",
            self.sbcli_utils.delete_storage_pool,
            "pool-does-not-exist-99999",
        ):
            self.logger.warning("delete_nonexistent_pool: did not fail — may be idempotent")

        if failures:
            raise AssertionError(
                f"TC-POOL-005 had {len(failures)} unexpected passes: "
                + "; ".join(failures)
            )

        self.logger.info("=== TC-POOL-005: Pool Negative Cases — PASS ===")
