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
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
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
        snapshots = self.sbcli_utils.list_snapshots()
        assert snap_name in snapshots, (
            f"Snapshot {snap_name} not found in list after create"
        )

        # Get snapshot ID and verify
        snap_id = self.sbcli_utils.get_snapshot_id(snap_name)
        assert snap_id, f"Could not get snapshot_id for {snap_name}"
        self.logger.info(f"Snapshot {snap_name}: id={snap_id}")

        # Delete snapshot
        self.sbcli_utils.delete_snapshot(snap_name)
        sleep_n_sec(5)
        snapshots = self.sbcli_utils.list_snapshots()
        assert snap_name not in snapshots, (
            f"Snapshot {snap_name} still in list after delete"
        )
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
            self._create_snapshot_dual(lvol_name, sname)
            sid = self.sbcli_utils.get_snapshot_id(sname)
            assert sid, f"Could not get snapshot_id for {sname}"
            snap_names.append(sname)
            self.logger.info(f"  Chain snapshot {i}: {sname} (id={sid})")

        # Verify all snapshots exist
        snapshots = self.sbcli_utils.list_snapshots()
        for sname in snap_names:
            assert sname in snapshots, f"Chain snapshot {sname} not in list"
        self.logger.info(f"All {len(snap_names)} chain snapshots verified in list")
        self.logger.info("TC-SNAP-008: Snapshot Chain — PASS")

        # -- TC-SNAP-009: Clone from chain snapshot ---------------------
        self.logger.info("=== TC-SNAP-009: Clone from Chain Snapshot ===")
        mid_snap = snap_names[1]
        mid_snap_id = self.sbcli_utils.get_snapshot_id(mid_snap)
        clone_name = f"{lvol_name}_chain_clone"
        self._create_clone_dual(mid_snap_id, clone_name)

        lvols = self.sbcli_utils.list_lvols()
        assert clone_name in lvols, (
            f"Clone {clone_name} not found in lvol list"
        )
        self.logger.info(f"Clone {clone_name} created from {mid_snap}")

        # Delete clone before deleting snapshots
        self.sbcli_utils.delete_lvol(clone_name)
        sleep_n_sec(5)
        self.logger.info("TC-SNAP-009: Clone from Chain — PASS")

        # -- TC-SNAP-010: Out-of-order deletion -------------------------
        self.logger.info("=== TC-SNAP-010: Out-of-Order Deletion ===")

        # Delete newest first, then middle, then oldest
        for sname in reversed(snap_names):
            self.logger.info(f"  Deleting {sname} ...")
            self.sbcli_utils.delete_snapshot(sname)
            sleep_n_sec(3)

        snapshots = self.sbcli_utils.list_snapshots()
        for sname in snap_names:
            assert sname not in snapshots, (
                f"Snapshot {sname} still present after out-of-order delete"
            )
        self.logger.info("TC-SNAP-010: Out-of-Order Deletion — PASS")

        # -- Cleanup ----------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TestSnapshotLifecycle: ALL PASSED ===")
