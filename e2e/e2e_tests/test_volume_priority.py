# DEPRECATED / UNCERTAIN — Priority class enforcement mechanism is unclear;
# this test is commented out in get_all_tests().  It creates multiple lvols
# and runs simultaneous FIO as a placeholder until the priority-class API is
# available.
"""TC-VOL-ADV-005 — Volume Priority Class (UNCERTAIN).

UNCERTAIN: The priority-class API does not exist yet.  This test creates
three lvols with default settings, runs FIO on all of them simultaneously,
and logs that proper priority-class testing is pending API support.  It is
commented out in get_all_tests() and should not be included in CI until the
feature lands.
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestVolumePriority(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "volume_priority"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-VOL-ADV-005: Volume Priority Class (UNCERTAIN) ===")

        # ── Pool create / verify ─────────────────────────────────────
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Create 3 lvols with different sizes ──────────────────────
        lvol_names = []
        sizes = ["1G", "2G", "5G"]
        for i, sz in enumerate(sizes):
            name = f"{self.lvol_name}_prio{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size=sz,
            )
            lvol_names.append(name)
        self.logger.info(f"Created {len(lvol_names)} lvols: {lvol_names}")

        # ── Connect all 3, mount, run FIO simultaneously ────────────
        fio_handles = []
        for i, name in enumerate(lvol_names):
            mount = f"{self.mount_path}_prio{i}"
            device, mount_point = self._connect_and_mount_dual(
                name, mount_path=mount
            )
            self.logger.info(f"Connected {name} -> device={device}, mount={mount_point}")

            handle = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount_point if not self.k8s_test else None,
                log_path=f"{self.log_path}_prio{i}" if not self.k8s_test else None,
                name=f"fio_prio_{i}",
                runtime=60,
                size="256M",
            )
            fio_handles.append(handle)

        # ── Wait for all FIO, validate each ──────────────────────────
        self._wait_fio_dual(fio_handles, timeout=300)
        for handle in fio_handles:
            self._validate_fio_dual(handle)
        self.logger.info("All 3 FIO runs completed and validated")

        # ── Log that priority-class testing is pending ───────────────
        self.logger.info(
            "NOTE: Priority-class enforcement could not be tested — "
            "the priority-class API is not yet available.  This test only "
            "validates that multiple lvols can run FIO simultaneously."
        )

        # ── Cleanup ──────────────────────────────────────────────────
        for name in lvol_names:
            try:
                if not self.k8s_test:
                    self._disconnect_and_cleanup_dual(name)
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup error for {name}: {exc}")
        sleep_n_sec(5)

        self.logger.info("=== TC-VOL-ADV-005: Volume Priority — PASS (partial) ===")
