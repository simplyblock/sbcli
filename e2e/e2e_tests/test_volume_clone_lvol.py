"""TC-VOL-ADV-002 — Volume clone-lvol (combined snapshot + clone).

Covers:
- Create lvol, write data
- volume clone-lvol → snapshot + clone in one command
- Verify clone in lvol list
- Verify intermediate snapshot exists
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestVolumeCloneLvol(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "volume_clone_lvol"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-VOL-ADV-002: Volume Clone-Lvol ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # ── Create source lvol, write data ─────────────────────────
        src_name = f"{self.lvol_name}_clsrc"
        self._create_lvol_dual(
            lvol_name=src_name, pool_name=self.pool_name, size="2G",
        )
        device, mount = self._connect_and_mount_dual(
            src_name, mount_path=f"{self.mount_path}_clsrc"
        )

        # Write some data via short FIO
        fio_handle = self._run_fio_dual(
            lvol_name=src_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_clsrc" if not self.k8s_test else None,
            name="fio_clone_src",
            runtime=15,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=60)

        # ── Snapshot + clone via dual helpers ───────────────────────
        src_id = self._get_lvol_id_dual(src_name)
        assert src_id, f"Could not get lvol_id for {src_name}"

        snap_name = f"{src_name}_snap"
        # snap_ref is the mode-appropriate clone source (Docker: backend snap
        # UUID, K8s: VolumeSnapshot name). Using the dual helpers keeps the
        # clone as a real PVC in K8s so FIO can mount it.
        snap_ref = self._create_snapshot_dual(src_name, snap_name)
        sleep_n_sec(5)
        self._verify_snapshot_exists_dual(snap_name)
        self.logger.info(f"Intermediate snapshot {snap_name} exists")

        clone_name = f"{src_name}_clone"
        clone_device, clone_mount = self._create_clone_dual(
            snap_ref, clone_name, size="2Gi",
            mount_path=f"{self.mount_path}_clone", format_disk=False,
        )
        sleep_n_sec(5)

        # ── Verify clone in list ───────────────────────────────────
        self._verify_lvol_exists_dual(clone_name)
        self.logger.info(f"Clone {clone_name} exists in lvol list")

        # ── Verify I/O works on clone ──────────────────────────────
        fio_clone = self._run_fio_dual(
            lvol_name=clone_name,
            mount_path=clone_mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_clone" if not self.k8s_test else None,
            name="fio_clone_verify",
            runtime=15,
            size="64M",
        )
        self._wait_fio_dual([fio_clone], timeout=60)
        self._validate_fio_dual(fio_clone)
        self.logger.info("FIO on clone completed successfully")

        # ── Cleanup ────────────────────────────────────────────────
        for name in [clone_name, src_name]:
            if not self.k8s_test:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass
            self._delete_lvol_dual(name)
            sleep_n_sec(2)
        self._delete_snapshot_dual(snap_name)

        self.logger.info("=== TC-VOL-ADV-002: Volume Clone-Lvol — PASS ===")
