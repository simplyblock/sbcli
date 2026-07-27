"""Scenario 4.10 — Cross-resource negative / error handling suite.

Covers:
- LVOL: invalid size, resize to 0
- Pool: disable non-existent
- Snapshot: snapshot of deleted lvol
- Node: restart already-online node
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestCrossResourceNegative(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cross_resource_negative"
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
        self.logger.info("=== Scenario 4.10: Cross-Resource Negative Cases ===")
        failures = []

        self._add_pool_dual(pool_name=self.pool_name)

        # ── LVOL: invalid size (negative) ──────────────────────────
        if not self._expect_failure(
            "lvol_negative_size",
            self.sbcli_utils.add_lvol,
            lvol_name=f"{self.lvol_name}_negsize",
            pool_name=self.pool_name,
            size="-1G",
            retry=1,
        ):
            failures.append("lvol_negative_size: should have failed")

        # ── LVOL: create, then resize to 0 ─────────────────────────
        resize_lvol = f"{self.lvol_name}_resize0"
        self.sbcli_utils.add_lvol(
            lvol_name=resize_lvol,
            pool_name=self.pool_name,
            size="1G",
            retry=3,
        )
        sleep_n_sec(5)
        lvol_id = self.sbcli_utils.get_lvol_id(resize_lvol)
        if lvol_id:
            if not self._expect_failure(
                "resize_to_zero",
                self.sbcli_utils.resize_lvol,
                lvol_id, "0M",
            ):
                failures.append("resize_to_zero: should have failed")

        # ── Pool: disable non-existent ─────────────────────────────
        if not self._expect_failure(
            "disable_nonexistent_pool",
            self.sbcli_utils.disable_storage_pool,
            "pool-does-not-exist-99999",
        ):
            self.logger.warning("disable_nonexistent_pool: did not fail")

        # ── Snapshot: snapshot of deleted lvol ──────────────────────
        deleted_lvol = f"{self.lvol_name}_todelete"
        self.sbcli_utils.add_lvol(
            lvol_name=deleted_lvol,
            pool_name=self.pool_name,
            size="1G",
            retry=3,
        )
        sleep_n_sec(5)
        del_lvol_id = self.sbcli_utils.get_lvol_id(deleted_lvol)
        self.sbcli_utils.delete_lvol(deleted_lvol)
        sleep_n_sec(10)

        if del_lvol_id:
            if not self._expect_failure(
                "snapshot_deleted_lvol",
                self.sbcli_utils.add_snapshot,
                del_lvol_id, "snap_deleted", 1,
            ):
                failures.append("snapshot_deleted_lvol: should have failed")

        # ── Node: restart already-online node ──────────────────────
        nodes = self.sbcli_utils.get_storage_nodes()
        if nodes and "results" in nodes and len(nodes["results"]) > 0:
            online_node = None
            for n in nodes["results"]:
                if n.get("status") == "online":
                    online_node = n
                    break
            if online_node:
                # restart requires OFFLINE state — this should fail
                if not self._expect_failure(
                    "restart_online_node",
                    self.sbcli_utils.restart_node,
                    online_node["id"],
                ):
                    self.logger.warning(
                        "restart_online_node: did not fail — may auto-cycle"
                    )

        # ── Cleanup ────────────────────────────────────────────────
        try:
            self.sbcli_utils.delete_lvol(resize_lvol)
        except Exception:
            pass

        if failures:
            raise AssertionError(
                f"Scenario 4.10 had {len(failures)} unexpected passes: "
                + "; ".join(failures)
            )

        self.logger.info("=== Scenario 4.10: Cross-Resource Negative — PASS ===")
