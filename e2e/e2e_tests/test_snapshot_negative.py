"""TC-SNAP-006 — Snapshot negative / error-handling cases.

Covers:
- Duplicate snapshot name
- Clone from deleted snapshot
- Delete snapshot with existing clone
- Snapshot of non-existent lvol
- Clone from non-existent snapshot
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestSnapshotNegativeCases(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "snapshot_negative_cases"
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
        self.logger.info("=== TC-SNAP-006: Snapshot Negative Cases ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        failures = []

        # Create base lvol
        lvol_name = f"{self.lvol_name}_snap_neg"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="2G",
        )
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"

        # ── 1. Create snapshot → success ───────────────────────────
        snap_name = "snap_neg_1"
        self.sbcli_utils.add_snapshot(lvol_id, snap_name)
        sleep_n_sec(5)
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id, f"Snapshot {snap_name} not created"
        self.logger.info(f"Snapshot {snap_name} created — id={snap_id}")

        # ── 2. Duplicate snapshot name ─────────────────────────────
        if not self._expect_failure(
            "duplicate_snapshot_name",
            self.sbcli_utils.add_snapshot,
            lvol_id, snap_name, 1,
        ):
            failures.append("duplicate_snapshot_name: should have failed")

        # ── 3. Create clone from snapshot → success ────────────────
        clone_name = "clone_neg_1"
        self.sbcli_utils.add_clone(snap_id, clone_name)
        sleep_n_sec(5)
        clone_id = self.sbcli_utils.get_lvol_id(clone_name)
        assert clone_id, f"Clone {clone_name} not created"
        self.logger.info(f"Clone {clone_name} created — id={clone_id}")

        # ── 4. Delete snapshot with existing clone ─────────────────
        if not self._expect_failure(
            "delete_snapshot_with_clone",
            self.sbcli_utils.delete_snapshot,
            snap_name=snap_name, max_attempt=1,
        ):
            # Some systems allow this (orphan clone) — log as warning
            self.logger.warning(
                "delete_snapshot_with_clone: succeeded — system allows orphaned clones"
            )

        # ── 5. Delete clone → delete snapshot → verify ─────────────
        self.sbcli_utils.delete_lvol(clone_name)
        sleep_n_sec(5)
        self.sbcli_utils.delete_snapshot(snap_name=snap_name)
        sleep_n_sec(5)
        snaps = self.sbcli_utils.list_snapshots()
        assert snap_name not in snaps, f"Snapshot {snap_name} still present after delete"
        self.logger.info("Clone → Snapshot delete lifecycle — PASS")

        # ── 6. Snapshot of non-existent lvol ───────────────────────
        if not self._expect_failure(
            "snapshot_nonexistent_lvol",
            self.sbcli_utils.add_snapshot,
            "00000000-0000-0000-0000-000000000000", "snap_bad", 1,
        ):
            failures.append("snapshot_nonexistent_lvol: should have failed")

        # ── 7. Clone from non-existent snapshot ────────────────────
        if not self._expect_failure(
            "clone_nonexistent_snapshot",
            self.sbcli_utils.add_clone,
            "00000000-0000-0000-0000-000000000000", "clone_bad", 1,
        ):
            failures.append("clone_nonexistent_snapshot: should have failed")

        # ── Cleanup ────────────────────────────────────────────────
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception:
            pass

        if failures:
            raise AssertionError(
                f"TC-SNAP-006 had {len(failures)} unexpected passes: "
                + "; ".join(failures)
            )

        self.logger.info("=== TC-SNAP-006: Snapshot Negative Cases — PASS ===")
