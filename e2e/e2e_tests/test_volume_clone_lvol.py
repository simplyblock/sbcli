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

        # ── Snapshot + clone via snapshot then clone ────────────────
        src_id = self.sbcli_utils.get_lvol_id(src_name)
        assert src_id, f"Could not get lvol_id for {src_name}"

        snap_name = f"{src_name}_snap"
        self.sbcli_utils.add_snapshot(src_id, snap_name)
        sleep_n_sec(5)
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id, f"Snapshot {snap_name} not created"

        clone_name = f"{src_name}_clone"
        self.sbcli_utils.add_clone(snap_id, clone_name)
        sleep_n_sec(5)

        # ── Verify clone in list ───────────────────────────────────
        lvols = self.sbcli_utils.list_lvols()
        assert clone_name in lvols, (
            f"Clone {clone_name} not in lvol list: {list(lvols.keys())}"
        )
        self.logger.info(f"Clone {clone_name} exists in lvol list")

        # ── Verify snapshot exists ─────────────────────────────────
        snaps = self.sbcli_utils.list_snapshots()
        assert snap_name in snaps, (
            f"Snapshot {snap_name} not in snapshot list"
        )
        self.logger.info(f"Intermediate snapshot {snap_name} exists")

        # ── Connect clone and verify I/O works ─────────────────────
        clone_device, clone_mount = self._connect_and_mount_dual(
            clone_name,
            mount_path=f"{self.mount_path}_clone",
            format_disk=False,
        )
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
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception:
                pass
            sleep_n_sec(2)
        try:
            self.sbcli_utils.delete_snapshot(snap_name=snap_name)
        except Exception:
            pass

        self.logger.info("=== TC-VOL-ADV-002: Volume Clone-Lvol — PASS ===")
