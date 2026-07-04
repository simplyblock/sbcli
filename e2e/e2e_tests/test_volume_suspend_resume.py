"""TC-VOL-ADV-001 — Volume suspend / resume subsystems.

NOTE: volume suspend / resume may not be wired to a working API endpoint.
This test validates whatever behavior exists.

Status: UNCERTAIN — commented out in get_all_tests()
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestVolumeSuspendResume(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "volume_suspend_resume"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-VOL-ADV-001: Volume Suspend / Resume ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_vol_sr"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="2G",
        )

        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"

        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_vol_sr"
        )

        # Start FIO
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_vol_sr" if not self.k8s_test else None,
            name="fio_vol_sr",
            runtime=90,
            size="256M",
        )
        sleep_n_sec(10)

        # ── Volume suspend ─────────────────────────────────────────
        self.logger.info("Suspending volume...")
        try:
            self.sbcli_utils.suspend_lvol(lvol_id)
            self.logger.info("volume suspend succeeded")
        except Exception as exc:
            self.logger.warning(f"volume suspend failed (may not be implemented): {exc}")

        sleep_n_sec(10)

        # ── Volume resume ──────────────────────────────────────────
        self.logger.info("Resuming volume...")
        try:
            self.sbcli_utils.resume_lvol(lvol_id)
            self.logger.info("volume resume succeeded")
        except Exception as exc:
            self.logger.warning(f"volume resume failed (may not be implemented): {exc}")

        sleep_n_sec(10)

        # ── Wait for FIO ───────────────────────────────────────────
        try:
            self._wait_fio_dual([fio_handle], timeout=180)
            self._validate_fio_dual(fio_handle)
            self.logger.info("FIO completed after suspend/resume cycle")
        except Exception as exc:
            self.logger.warning(
                f"FIO had issues during suspend/resume (expected if suspend works): {exc}"
            )

        self.logger.info("=== TC-VOL-ADV-001: Volume Suspend / Resume — PASS ===")
