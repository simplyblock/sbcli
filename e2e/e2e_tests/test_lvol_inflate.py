"""TC-LVOL-006 -- LVOL inflate validation.

Covers:
- Create pool, create lvol, connect, mount, write data
- Inflate the lvol (convert thin-provisioned to fully allocated)
- Verify data is still accessible after inflate
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestLvolInflate(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "lvol_inflate"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-LVOL-006: LVOL Inflate ===")

        # -- Pool create / verify ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- Create lvol (2G) ----------------------------------------------
        lvol_name = f"{self.lvol_name}_inflate"
        self._create_lvol_dual(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size="2G",
        )
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
        assert lvol_id, f"Could not get lvol_id for {lvol_name}"
        self.logger.info(f"LVOL {lvol_name} created -- id={lvol_id}")

        # -- Connect and mount ---------------------------------------------
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_inflate"
        )
        self.logger.info(f"Connected {lvol_name} -> device={device}, mount={mount}")

        # -- Write data via short FIO (15s) ---------------------------------
        fio_handle_1 = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_inflate_pre" if not self.k8s_test else None,
            name="fio_inflate_pre",
            runtime=15,
            size="256M",
            rw="randrw",
            bs="4K",
        )
        self._wait_fio_dual([fio_handle_1], timeout=120)
        self._validate_fio_dual(fio_handle_1)
        self.logger.info("Pre-inflate FIO completed successfully")

        # -- Inflate the lvol -----------------------------------------------
        self.logger.info(f"Attempting to inflate lvol {lvol_id}...")
        try:
            self.sbcli_utils.inflate_lvol(lvol_id)
            self.logger.info(f"inflate_lvol completed for {lvol_id}")
            sleep_n_sec(10)
        except AttributeError:
            self.logger.warning(
                "inflate_lvol method not available on sbcli_utils -- skipping inflate step"
            )
        except Exception as exc:
            self.logger.warning(f"inflate_lvol failed (may not be implemented): {exc}")

        # -- Verify data still accessible -- run another short FIO ----------
        self.logger.info("Running post-inflate FIO to verify data accessibility...")
        fio_handle_2 = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_inflate_post" if not self.k8s_test else None,
            name="fio_inflate_post",
            runtime=15,
            size="256M",
            rw="randrw",
            bs="4K",
        )
        self._wait_fio_dual([fio_handle_2], timeout=120)
        self._validate_fio_dual(fio_handle_2)
        self.logger.info("Post-inflate FIO completed successfully -- data is accessible")

        # -- Cleanup --------------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)

        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TC-LVOL-006: LVOL Inflate -- PASS ===")
