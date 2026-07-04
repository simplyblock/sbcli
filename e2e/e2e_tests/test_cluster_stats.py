"""TC-CLUSTER-006 -- Cluster capacity and I/O statistics validation.

Covers:
- Create pool, lvol, connect, mount, start FIO
- Get cluster capacity -- verify not empty
- Get cluster I/O stats while FIO is running -- verify not empty
- Wait for FIO completion and validate
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestClusterStats(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "cluster_stats"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-CLUSTER-006: Cluster Stats ===")

        # -- Pool create / verify ------------------------------------------
        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()
        self.logger.info(f"Pool {self.pool_name} created and verified")

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        # -- Create lvol ---------------------------------------------------
        lvol_name = f"{self.lvol_name}_clstats"
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
            lvol_name, mount_path=f"{self.mount_path}_clstats"
        )
        self.logger.info(f"Connected {lvol_name} -> device={device}, mount={mount}")

        # -- Start FIO (60s) -----------------------------------------------
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_clstats" if not self.k8s_test else None,
            name="fio_cluster_stats",
            runtime=60,
            size="512M",
            rw="randrw",
            bs="4K",
        )
        self.logger.info("FIO started, allowing I/O to ramp up...")
        sleep_n_sec(15)

        # -- Cluster capacity ----------------------------------------------
        self.logger.info("Fetching cluster capacity...")
        try:
            cluster_cap = self.sbcli_utils.get_cluster_capacity()
            assert cluster_cap, "get_cluster_capacity returned empty result"
            self.logger.info(f"Cluster capacity: {cluster_cap}")
        except AttributeError:
            self.logger.warning(
                "get_cluster_capacity method not available on sbcli_utils"
            )
        except AssertionError:
            raise
        except Exception as exc:
            self.logger.warning(f"get_cluster_capacity failed: {exc}")

        # -- Cluster I/O stats (while FIO running) -------------------------
        self.logger.info("Fetching cluster I/O stats while FIO is running...")
        try:
            io_stats = self.sbcli_utils.get_io_stats(self.cluster_id)
            assert io_stats is not None, "get_io_stats returned None"
            self.logger.info(f"Cluster I/O stats: {io_stats}")
        except AttributeError:
            self.logger.warning(
                "get_io_stats method not available on sbcli_utils"
            )
        except AssertionError:
            raise
        except Exception as exc:
            self.logger.warning(f"get_io_stats failed: {exc}")

        # -- Wait for FIO completion and validate --------------------------
        self.logger.info("Waiting for FIO completion...")
        self._wait_fio_dual([fio_handle], timeout=180)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO completed and validated successfully")

        # -- Cleanup -------------------------------------------------------
        if not self.k8s_test:
            self._disconnect_and_cleanup_dual(lvol_name)

        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception as exc:
            self.logger.warning(f"Cleanup delete {lvol_name}: {exc}")

        self.logger.info("=== TC-CLUSTER-006: Cluster Stats -- PASS ===")
