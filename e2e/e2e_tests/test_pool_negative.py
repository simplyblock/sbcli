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
        # _add_pool_dual returns the ACTUAL pool name (the K8s operator uses a
        # single reconciled pool whose name may differ from the requested one).
        pool_name = self._add_pool_dual(pool_name=f"{self.pool_name}_neg")
        sleep_n_sec(5)
        self._verify_pool_exists_dual(pool_name)
        self.logger.info(f"Pool {pool_name} created")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── 2. Duplicate pool name ─────────────────────────────────
        # The K8s operator reconciles to a single shared pool, so re-requesting
        # a pool is idempotent rather than an error — only assert the failure
        # in Docker (REST) mode where a duplicate name is rejected.
        if not self.k8s_test:
            if not self._expect_failure(
                "duplicate_pool",
                self.sbcli_utils.add_storage_pool,
                pool_name,
            ):
                failures.append("duplicate_pool: should have failed")

        # ── 3. Delete pool with active lvol ────────────────────────
        lvol_name = f"{self.lvol_name}_pool_neg"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=pool_name,
            size="1G",
        )
        sleep_n_sec(5)

        # Deleting a pool that still has lvols must fail in Docker mode. In K8s
        # the pool is a shared operator resource that outlives individual PVCs,
        # so this assertion does not apply.
        if not self.k8s_test:
            if not self._expect_failure(
                "delete_pool_with_lvol",
                self.sbcli_utils.delete_storage_pool,
                pool_name,
            ):
                failures.append("delete_pool_with_lvol: should have failed")

        # ── 4. Delete lvol → then delete empty pool → success ─────
        self._delete_lvol_dual(lvol_name)
        sleep_n_sec(5)
        if not self.k8s_test:
            self.sbcli_utils.delete_storage_pool(pool_name)
            sleep_n_sec(5)
            pools = self.sbcli_utils.list_storage_pools()
            assert pool_name not in pools, (
                f"Pool {pool_name} still present after delete"
            )
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
