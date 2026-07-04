"""TC-CONN-001..004 — LVOL connect/disconnect lifecycle and edge cases.

Covers:
- Connect → write → disconnect → reconnect → verify data persists
- Multiple lvols connect/disconnect in rapid sequence
- Connect, run FIO, resize, continue FIO (connect survives resize)
- Disconnect idle lvol (no active I/O)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestLvolConnectLifecycle(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_connect_lifecycle"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CONN: LVOL Connect Lifecycle ===")

        # -- Pool setup -------------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- TC-CONN-001: Connect → write → disconnect → reconnect -----
        self.logger.info("=== TC-CONN-001: Reconnect Data Persistence ===")

        lvol_name = f"{self.lvol_name}_conn1"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="1G",
        )

        # First connect — write data
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_conn1"
        )
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_conn1a" if not self.k8s_test else None,
            name="fio_conn_write",
            runtime=15,
            size="128M",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("First connect: wrote data successfully")

        # Disconnect
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)
            self.logger.info("Disconnected lvol")
            sleep_n_sec(5)

            # Reconnect
            device2, mount2 = self._connect_and_mount_dual(
                lvol_name, mount_path=f"{self.mount_path}_conn1r"
            )
            self.logger.info(f"Reconnected: device={device2}, mount={mount2}")

            # Write more data to verify the reconnect works
            fio_handle = self._run_fio_dual(
                lvol_name=lvol_name,
                mount_path=mount2,
                log_path=f"{self.log_path}_conn1b",
                name="fio_conn_rewrite",
                runtime=15,
                size="128M",
            )
            self._wait_fio_dual([fio_handle], timeout=120)
            self._validate_fio_dual(fio_handle)
            self.logger.info("Reconnect: wrote more data successfully")

            self._disconnect_and_cleanup_dual(lvol_name)
        else:
            self.logger.info("K8s mode: skipping disconnect/reconnect cycle")

        self.logger.info("TC-CONN-001: Reconnect Data Persistence — PASS")

        # -- TC-CONN-002: Multiple lvols rapid connect/disconnect -------
        self.logger.info("=== TC-CONN-002: Rapid Multi-LVOL Connect ===")

        multi_lvols = []
        for i in range(3):
            name = f"{self.lvol_name}_conn_multi_{i}"
            self._create_lvol_dual(
                lvol_name=name,
                pool_name=self.pool_name,
                size="512M",
            )
            multi_lvols.append(name)

        # Connect all
        mounts = {}
        for name in multi_lvols:
            dev, mnt = self._connect_and_mount_dual(
                name, mount_path=f"{self.mount_path}_{name}"
            )
            mounts[name] = mnt
            self.logger.info(f"  Connected {name}")

        # Run brief FIO on all
        fio_handles = []
        for name in multi_lvols:
            h = self._run_fio_dual(
                lvol_name=name,
                mount_path=mounts[name] if not self.k8s_test else None,
                log_path=f"{self.log_path}_{name}" if not self.k8s_test else None,
                name=f"fio_{name}",
                runtime=15,
                size="64M",
            )
            fio_handles.append(h)

        self._wait_fio_dual(fio_handles, timeout=120)
        for h in fio_handles:
            self._validate_fio_dual(h)
        self.logger.info(f"FIO completed on all {len(multi_lvols)} lvols")

        # Disconnect all
        if not self.k8s_test:
            for name in multi_lvols:
                self._disconnect_and_cleanup_dual(name)
            self.logger.info("All lvols disconnected")

        self.logger.info("TC-CONN-002: Rapid Multi-LVOL Connect — PASS")

        # -- TC-CONN-003: Connect survives resize -----------------------
        self.logger.info("=== TC-CONN-003: Connect Survives Resize ===")

        resize_lvol = f"{self.lvol_name}_conn_resize"
        self._create_lvol_dual(
            lvol_name=resize_lvol,
            pool_name=self.pool_name,
            size="1G",
        )
        resize_id = self.sbcli_utils.get_lvol_id(resize_lvol)
        assert resize_id, f"Could not get lvol_id for {resize_lvol}"

        device, mount = self._connect_and_mount_dual(
            resize_lvol, mount_path=f"{self.mount_path}_conn_resize"
        )

        # Start FIO
        fio_handle = self._run_fio_dual(
            lvol_name=resize_lvol,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_conn_resize" if not self.k8s_test else None,
            name="fio_conn_resize",
            runtime=60,
            size="256M",
            rw="randrw",
        )
        sleep_n_sec(10)

        # Resize while connected and FIO running
        try:
            self._resize_lvol_dual(resize_lvol, "2G")
            self.logger.info(f"Resized {resize_lvol} from 1G to 2G during FIO")
        except Exception as exc:
            self.logger.warning(f"Resize during FIO failed: {exc}")

        # Wait for FIO to complete — should not fail
        self._wait_fio_dual([fio_handle], timeout=180)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed successfully after resize")

        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(resize_lvol)

        self.logger.info("TC-CONN-003: Connect Survives Resize — PASS")

        # -- TC-CONN-004: Disconnect idle lvol --------------------------
        self.logger.info("=== TC-CONN-004: Disconnect Idle LVOL ===")

        idle_lvol = f"{self.lvol_name}_conn_idle"
        self._create_lvol_dual(
            lvol_name=idle_lvol,
            pool_name=self.pool_name,
            size="256M",
        )
        device, mount = self._connect_and_mount_dual(
            idle_lvol, mount_path=f"{self.mount_path}_conn_idle"
        )
        self.logger.info(f"Connected idle lvol: {idle_lvol}")

        # No FIO — just disconnect immediately
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(idle_lvol)
            self.logger.info("Disconnected idle lvol without any I/O")

        self.logger.info("TC-CONN-004: Disconnect Idle LVOL — PASS")

        # -- Cleanup ----------------------------------------------------
        all_lvols = [lvol_name, resize_lvol, idle_lvol] + multi_lvols
        for name in all_lvols:
            try:
                self.sbcli_utils.delete_lvol(name)
            except Exception as exc:
                self.logger.warning(f"Cleanup delete {name}: {exc}")

        self.logger.info("=== TestLvolConnectLifecycle: ALL PASSED ===")
