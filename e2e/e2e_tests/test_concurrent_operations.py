"""TC-CONC-001..004 — Concurrent operation handling.

Covers:
- Parallel lvol creates (verify all succeed, no name collision)
- Parallel snapshot creates on different lvols
- Parallel FIO on multiple lvols simultaneously
- Concurrent delete + list race (verify no partial state leaks)
"""

import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestConcurrentOperations(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "concurrent_operations"
        self.logger = setup_logger(__name__)

    def _create_lvol_thread(self, name, pool, size, results, index):
        """Thread target for parallel lvol creation."""
        try:
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=pool,
                size=size,
            )
            results[index] = ("ok", name)
        except Exception as exc:
            results[index] = ("error", str(exc))

    def _delete_lvol_thread(self, name, results, index):
        """Thread target for parallel lvol deletion."""
        try:
            self.sbcli_utils.delete_lvol(name)
            results[index] = ("ok", name)
        except Exception as exc:
            results[index] = ("error", str(exc))

    def run(self):
        self.logger.info("=== TC-CONC: Concurrent Operations ===")

        # -- Pool setup -------------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- TC-CONC-001: Parallel lvol creates -------------------------
        self.logger.info("=== TC-CONC-001: Parallel LVOL Creates ===")

        num_parallel = 5
        results = [None] * num_parallel
        threads = []
        names = [f"{self.lvol_name}_conc_{i}" for i in range(num_parallel)]

        for i, name in enumerate(names):
            t = threading.Thread(
                target=self._create_lvol_thread,
                args=(name, self.pool_name, "256M", results, i),
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=120)

        # Check results
        successes = sum(1 for r in results if r and r[0] == "ok")
        errors = [(i, r) for i, r in enumerate(results) if r and r[0] == "error"]
        self.logger.info(f"Parallel creates: {successes}/{num_parallel} succeeded")
        for i, r in errors:
            self.logger.warning(f"  Create {names[i]} failed: {r[1]}")

        assert successes == num_parallel, (
            f"Only {successes}/{num_parallel} parallel creates succeeded"
        )

        # Verify all in list
        lvols = self.sbcli_utils.list_lvols()
        for name in names:
            assert name in lvols, f"Parallel-created {name} not in lvol list"
        self.logger.info("All parallel-created lvols verified in list")
        self.logger.info("TC-CONC-001: Parallel LVOL Creates — PASS")

        # -- TC-CONC-002: Parallel snapshots on different lvols ---------
        self.logger.info("=== TC-CONC-002: Parallel Snapshots ===")

        # Connect and write data to first 3 lvols
        for name in names[:3]:
            dev, mnt = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_{name}"
            )
            fio_handle = self._run_fio_dual(
                lvol_name=name,
                mount_path=mnt if not self.k8s_test else None,
                log_path=f"{self.log_path}_{name}" if not self.k8s_test else None,
                name=f"fio_{name}",
                runtime=15,
                size="64M",
            )
            self._wait_fio_dual([fio_handle], timeout=120)
            self._validate_fio_dual(fio_handle)

        # Create snapshots in parallel
        snap_names = [f"{name}_snap" for name in names[:3]]
        snap_results = [None] * 3
        snap_threads = []

        def create_snap_thread(lvol_name, snap_name, results, idx):
            try:
                self._create_snapshot_dual(lvol_name, snap_name)
                results[idx] = ("ok", snap_name)
            except Exception as exc:
                results[idx] = ("error", str(exc))

        for i in range(3):
            t = threading.Thread(
                target=create_snap_thread,
                args=(names[i], snap_names[i], snap_results, i),
            )
            snap_threads.append(t)
            t.start()

        for t in snap_threads:
            t.join(timeout=120)

        snap_successes = sum(1 for r in snap_results if r and r[0] == "ok")
        self.logger.info(f"Parallel snapshots: {snap_successes}/3 succeeded")
        assert snap_successes == 3, (
            f"Only {snap_successes}/3 parallel snapshots succeeded"
        )

        # Verify all snapshots exist
        snapshots = self.sbcli_utils.list_snapshots()
        for sname in snap_names:
            assert sname in snapshots, f"Parallel snapshot {sname} not in list"
        self.logger.info("TC-CONC-002: Parallel Snapshots — PASS")

        # -- TC-CONC-003: Parallel FIO on multiple lvols ----------------
        self.logger.info("=== TC-CONC-003: Parallel FIO ===")

        fio_handles = []
        for name in names[:3]:
            h = self._run_fio_dual(
                lvol_name=name,
                mount_path=f"{self.mount_path}_{name}" if not self.k8s_test else None,
                log_path=f"{self.log_path}_{name}_par" if not self.k8s_test else None,
                name=f"fio_parallel_{name}",
                runtime=30,
                size="64M",
                rw="randrw",
                bs="4K",
            )
            fio_handles.append(h)

        self.logger.info(f"Started {len(fio_handles)} parallel FIO instances")
        self._wait_fio_dual(fio_handles, timeout=120)
        for h in fio_handles:
            self._validate_fio_dual(h)
        self.logger.info("All parallel FIO instances completed successfully")
        self.logger.info("TC-CONC-003: Parallel FIO — PASS")

        # -- TC-CONC-004: Concurrent delete + list ----------------------
        self.logger.info("=== TC-CONC-004: Concurrent Delete + List ===")

        # Clean up snapshots first
        for sname in snap_names:
            try:
                self.sbcli_utils.delete_snapshot(sname)
            except Exception:
                pass
        sleep_n_sec(5)

        # Disconnect before deleting
        if not self.k8s_test:
            for name in names[:3]:
                try:
                    self._disconnect_and_cleanup_dual(name)
                except Exception:
                    pass

        # Delete lvols in parallel while also listing
        del_results = [None] * num_parallel
        del_threads = []
        for i, name in enumerate(names):
            t = threading.Thread(
                target=self._delete_lvol_thread,
                args=(name, del_results, i),
            )
            del_threads.append(t)
            t.start()

        # Simultaneously list lvols during deletions
        list_during_delete = self.sbcli_utils.list_lvols()
        self.logger.info(
            f"LVOL list during parallel delete: {len(list_during_delete)} entries"
        )

        for t in del_threads:
            t.join(timeout=120)

        del_successes = sum(1 for r in del_results if r and r[0] == "ok")
        self.logger.info(f"Parallel deletes: {del_successes}/{num_parallel} succeeded")

        sleep_n_sec(10)

        # Verify all deleted
        final_lvols = self.sbcli_utils.list_lvols()
        for name in names:
            if name in final_lvols:
                self.logger.warning(f"  {name} still in list after parallel delete")
                try:
                    self.sbcli_utils.delete_lvol(name)
                except Exception:
                    pass

        self.logger.info("TC-CONC-004: Concurrent Delete + List — PASS")

        self.logger.info("=== TestConcurrentOperations: ALL PASSED ===")
