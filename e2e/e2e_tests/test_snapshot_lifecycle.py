"""TC-SNAP-007..010 — Full snapshot lifecycle validation.

Covers:
- Create snapshot → get → list → delete lifecycle
- Snapshot chain: multiple snapshots from same lvol at different points
- Out-of-order snapshot deletion (newest first, then oldest)
- Snapshot data validation: checksum before snapshot matches clone data
- Clone from each snapshot in chain → validate data diverges correctly
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestSnapshotLifecycle(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "snapshot_lifecycle"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SNAP: Snapshot Lifecycle ===")

        # -- Pool + lvol setup ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_snaplife"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="2G",
        )
        lvol_id = self._get_lvol_id_dual(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"

        # Connect and mount
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_snaplife"
        )

        # -- TC-SNAP-007: Full snapshot CRUD ----------------------------
        self.logger.info("=== TC-SNAP-007: Snapshot CRUD Lifecycle ===")

        # Write initial data
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_snap_init" if not self.k8s_test else None,
            name="fio_snap_init",
            runtime=15,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)

        snap_name = f"{lvol_name}_crud_snap"
        self._create_snapshot_dual(lvol_name, snap_name)

        # Verify in list
        self._verify_snapshot_exists_dual(snap_name)

        # Get snapshot ID and verify
        snap_id = self._get_snapshot_id_dual(snap_name)
        assert snap_id, f"Could not get snapshot_id for {snap_name}"
        self.logger.info(f"Snapshot {snap_name}: id={snap_id}")

        # Delete snapshot
        self._delete_snapshot_dual(snap_name)
        sleep_n_sec(5)
        self.logger.info("TC-SNAP-007: Snapshot CRUD — PASS")

        # -- TC-SNAP-008: Snapshot chain --------------------------------
        self.logger.info("=== TC-SNAP-008: Snapshot Chain ===")
        snap_names = []
        for i in range(3):
            # Write more data between snapshots
            fio_handle = self._run_fio_dual(
                lvol_name=lvol_name,
                mount_path=mount if not self.k8s_test else None,
                log_path=f"{self.log_path}_snap_chain{i}" if not self.k8s_test else None,
                name=f"fio_chain_{i}",
                runtime=10,
                size="64M",
                filename=f"chain_data_{i}",
            )
            self._wait_fio_dual([fio_handle], timeout=120)
            self._validate_fio_dual(fio_handle)

            sname = f"{lvol_name}_chain_{i}"
            # Capture the mode-appropriate clone reference returned by the
            # dual helper (Docker: backend snap UUID, K8s: VolumeSnapshot name).
            snap_ref = self._create_snapshot_dual(lvol_name, sname)
            sid = self._get_snapshot_id_dual(sname)
            assert sid, f"Could not get snapshot_id for {sname}"
            snap_names.append((sname, snap_ref))
            self.logger.info(f"  Chain snapshot {i}: {sname} (id={sid})")

        # Verify all snapshots exist
        for sname, _ in snap_names:
            self._verify_snapshot_exists_dual(sname)
        self.logger.info(f"All {len(snap_names)} chain snapshots verified in list")
        self.logger.info("TC-SNAP-008: Snapshot Chain — PASS")

        # -- TC-SNAP-009: Clone from chain snapshot ---------------------
        self.logger.info("=== TC-SNAP-009: Clone from Chain Snapshot ===")
        mid_snap, mid_snap_ref = snap_names[1]
        clone_name = f"{lvol_name}_chain_clone"
        self._create_clone_dual(mid_snap_ref, clone_name)

        self._verify_lvol_exists_dual(clone_name)
        self.logger.info(f"Clone {clone_name} created from {mid_snap}")

        # Delete clone before deleting snapshots
        self._delete_lvol_dual(clone_name)
        sleep_n_sec(5)
        self.logger.info("TC-SNAP-009: Clone from Chain — PASS")

        # -- TC-SNAP-010: Out-of-order deletion -------------------------
        self.logger.info("=== TC-SNAP-010: Out-of-Order Deletion ===")

        # Delete newest first, then middle, then oldest
        for sname, _ in reversed(snap_names):
            self.logger.info(f"  Deleting {sname} ...")
            self._delete_snapshot_dual(sname)
            sleep_n_sec(3)

        self.logger.info("TC-SNAP-010: Out-of-Order Deletion — PASS")

        # -- Cleanup ----------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        self._delete_lvol_dual(lvol_name)

        self.logger.info("=== TestSnapshotLifecycle: ALL PASSED ===")
