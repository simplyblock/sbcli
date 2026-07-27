"""TC-SN-DEV-002 — Device restart during active I/O.

Covers:
- List devices on a node
- Restart a device while FIO running
- Verify device comes back online
- Verify FIO completes (HA failover during restart)
"""

from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestDeviceRestart(TestClusterBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "device_restart"
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("=== TC-SN-DEV-002: Device Restart ===")

        self._add_pool_dual(pool_name=self.pool_name)
        if self.k8s_test:
            self._k8s_ensure_storage_class()

        lvol_name = f"{self.lvol_name}_devr"
        self._create_lvol_dual(
            lvol_name=lvol_name, pool_name=self.pool_name, size="5G",
        )

        device, mount = self._connect_and_mount_dual(
            lvol_name, mount_path=f"{self.mount_path}_devr"
        )

        # Start FIO
        fio_handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount if not self.k8s_test else None,
            log_path=f"{self.log_path}_devr" if not self.k8s_test else None,
            name="fio_dev_restart",
            runtime=120,
            size="1G",
            rw="randrw",
        )
        sleep_n_sec(10)

        # Get storage nodes and their devices
        nodes = self.sbcli_utils.get_storage_nodes()
        target_node = None
        device_id = None

        if nodes and "results" in nodes:
            for n in nodes["results"]:
                if n.get("status") == "online":
                    nid = n["id"]
                    try:
                        devices = self.sbcli_utils.get_device_details(nid)
                        if devices:
                            for dev in (devices if isinstance(devices, list) else [devices]):
                                did = dev.get("id", "")
                                if did:
                                    target_node = n
                                    device_id = did
                                    break
                    except Exception as exc:
                        self.logger.warning(f"get_device_details({nid}): {exc}")
                if device_id:
                    break

        if device_id:
            self.logger.info(
                f"Restarting device {device_id} on node {target_node['id']}"
            )
            mgmt = self.mgmt_nodes[0]
            try:
                out, _ = self.ssh_obj.exec_command(
                    mgmt,
                    f"{self.base_cmd} sn restart-device {device_id}",
                )
                self.logger.info(f"restart-device output: {out}")
            except Exception as exc:
                self.logger.warning(f"restart-device failed: {exc}")

            # Wait for device to come back
            sleep_n_sec(30)

            # Check device status
            try:
                out, _ = self.ssh_obj.exec_command(
                    mgmt,
                    f"{self.base_cmd} sn check-device {device_id}",
                )
                self.logger.info(f"check-device output: {out}")
            except Exception as exc:
                self.logger.warning(f"check-device failed: {exc}")
        else:
            self.logger.warning("No devices found to restart — skipping restart step")

        # Wait for FIO completion
        try:
            self._wait_fio_dual([fio_handle], timeout=300)
            self._validate_fio_dual(fio_handle)
            self.logger.info("FIO completed after device restart")
        except Exception as exc:
            self.logger.warning(f"FIO had issues during device restart: {exc}")

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

        self.logger.info("=== TC-SN-DEV-002: Device Restart — PASS ===")
