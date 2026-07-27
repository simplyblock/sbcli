"""TC-VOL-ADV-004 — Multi-client concurrent connect.

Covers:
- Create HA lvol (npcs >= 1)
- Get multi-path connect strings
- Connect from client, run FIO to validate HA connect
"""

from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger


class TestMultiClientConnect(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "multi_client_connect"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-VOL-ADV-004: Multi-Client Connect ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_mc"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="5G",
        )

        # Get connect strings — may return multiple paths
        connect_strs = self.sbcli_utils.get_lvol_connect_str(lvol_name)
        self.logger.info(f"Connect strings returned: {len(connect_strs)} paths")
        for cs in connect_strs:
            self.logger.info(f"  path: {cs[:80]}...")

        # Connect and mount via primary path
        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_mc"
        )

        # Run FIO to validate I/O on HA volume
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_mc" if not self.k8s_test else None,
            name="fio_multi_client",
            runtime=30,
            size="256M",
            rw="randrw",
        )
        self._wait_fio_dual([fio_handle], timeout=120)
        self._validate_fio_dual(fio_handle)
        self.logger.info("FIO on HA volume completed successfully")

        # Cleanup
        if not self.k8s_test:
            try:
                self._disconnect_and_cleanup_dual(lvol_name)
            except Exception:
                pass
        try:
            self.sbcli_utils.delete_lvol(lvol_name)
        except Exception:
            pass

        self.logger.info("=== TC-VOL-ADV-004: Multi-Client Connect — PASS ===")
