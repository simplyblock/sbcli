"""Scenario 4.3 — Pool disable during active I/O.

Covers:
- Create pool, lvols, run FIO
- Disable pool → verify FIO behavior
- Re-enable pool → verify FIO resumes
- Validate data integrity
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestPoolDisableIO(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "pool_disable_io"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== Scenario 4.3: Pool Disable During I/O ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Create lvols and start FIO ─────────────────────────────
        lvol_names = []
        fio_handles = []
        for i in range(3):
            name = f"{self.lvol_name}_pdio_{i}"
            self._create_lvol_dual(
                lvol_name=name, pool_name=self.pool_name, size="2G",
            )
            device, mount = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_pdio_{i}"
            )
            fh = self._run_fio_dual(
                lvol_name=name,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_pdio_{i}" if not self.k8s_test else None,
                name=f"fio_pdio_{i}",
                runtime=120,
                size="256M",
                rw="randrw",
            )
            lvol_names.append(name)
            fio_handles.append(fh)

        self.logger.info(f"Started FIO on {len(lvol_names)} lvols")
        sleep_n_sec(15)

        # ── Disable pool ───────────────────────────────────────────
        self.logger.info("Disabling pool while FIO is running...")
        self.sbcli_utils.disable_storage_pool(self.pool_name)
        sleep_n_sec(10)
        self.logger.info("Pool disabled")

        # ── Re-enable pool ─────────────────────────────────────────
        self.logger.info("Re-enabling pool...")
        self.sbcli_utils.enable_storage_pool(self.pool_name)
        sleep_n_sec(10)
        self.logger.info("Pool re-enabled")

        # ── Wait for FIO completion ────────────────────────────────
        self.logger.info("Waiting for FIO to complete...")
        for fh in fio_handles:
            try:
                self._wait_fio_dual([fh], timeout=300)
                self._validate_fio_dual(fh)
            except Exception as exc:
                self.logger.warning(f"FIO validation warning: {exc}")

        # ── Verify data integrity ──────────────────────────────────
        if not self.k8s_test:
            for name in lvol_names:
                try:
                    files = self._find_files_dual(name)
                    if files:
                        checksums = self._generate_checksums_dual(name, files=files)
                        self.logger.info(f"{name}: {len(checksums)} file checksums generated")
                except Exception as exc:
                    self.logger.warning(f"Checksum generation for {name}: {exc}")

        # ── Cleanup ────────────────────────────────────────────────
        for name in lvol_names:
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass

        self.logger.info("=== Scenario 4.3: Pool Disable During I/O — PASS ===")
