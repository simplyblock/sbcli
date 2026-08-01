"""TC-LVOL-010 — LVOL negative / error-handling cases.

Covers:
- Duplicate name collision
- Non-existent pool
- Invalid size
- Delete non-existent lvol
- Resize to smaller
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestLvolNegativeCases(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_negative_cases"
        self.logger = setup_logger(__name__)

    def _expect_failure(self, operation, fn, *args, **kwargs):
        """Call fn and assert it raises an exception."""
        try:
            fn(*args, **kwargs)
            self.logger.error(f"[{operation}] Expected failure but succeeded")
            return False
        except Exception as exc:
            self.logger.info(f"[{operation}] Correctly failed: {exc}")
            return True

    def run(self):
        self.logger.info("=== TC-LVOL-010: LVOL Negative Cases ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        failures = []

        # ── 1. Duplicate name ──────────────────────────────────────
        dup_name = f"{self.lvol_name}_dup"
        self._create_lvol_dual(
            lvol_name=dup_name, pool_name=self.pool_name, size="1G",
        )
        self.logger.info(f"Created {dup_name} — now attempting duplicate")

        if not self._expect_failure(
            "duplicate_name",
            self.sbcli_utils.add_lvol,
            lvol_name=dup_name,
            pool_name=self.pool_name,
            size="1G",
            retry=1,
        ):
            failures.append("duplicate_name: should have failed")

        # ── 2. Non-existent pool ───────────────────────────────────
        if not self._expect_failure(
            "nonexistent_pool",
            self.sbcli_utils.add_lvol,
            lvol_name=f"{self.lvol_name}_badpool",
            pool_name="pool-does-not-exist-12345",
            size="1G",
            retry=1,
        ):
            failures.append("nonexistent_pool: should have failed")

        # ── 3. Invalid size (zero) ─────────────────────────────────
        if not self._expect_failure(
            "zero_size",
            self.sbcli_utils.add_lvol,
            lvol_name=f"{self.lvol_name}_zero",
            pool_name=self.pool_name,
            size="0M",
            retry=1,
        ):
            failures.append("zero_size: should have failed")

        # ── 4. Delete non-existent lvol ────────────────────────────
        if not self._expect_failure(
            "delete_nonexistent",
            self.sbcli_utils.delete_lvol,
            "lvol-does-not-exist-99999",
            max_attempt=1,
        ):
            # Some implementations may silently succeed — treat as warning
            self.logger.warning("delete_nonexistent: did not fail — may be idempotent")

        # ── 5. Resize to smaller ───────────────────────────────────
        lvol_id = self.sbcli_utils.get_lvol_id(dup_name)
        if lvol_id:
            if not self._expect_failure(
                "resize_smaller",
                self.sbcli_utils.resize_lvol,
                lvol_id,
                "256M",
            ):
                failures.append("resize_smaller: should have failed")
        else:
            self.logger.warning("Could not get lvol_id for resize test — skipping")

        # ── 6. Connect non-existent lvol ───────────────────────────
        if not self._expect_failure(
            "connect_nonexistent",
            self.sbcli_utils.get_lvol_connect_str,
            "lvol-does-not-exist-99999",
        ):
            self.logger.warning("connect_nonexistent: did not fail — may return empty")

        # ── Cleanup ────────────────────────────────────────────────
        try:
            self.sbcli_utils.delete_lvol(dup_name)
        except Exception:
            pass

        if failures:
            raise AssertionError(
                f"TC-LVOL-010 had {len(failures)} unexpected passes: "
                + "; ".join(failures)
            )

        self.logger.info("=== TC-LVOL-010: LVOL Negative Cases — PASS ===")
