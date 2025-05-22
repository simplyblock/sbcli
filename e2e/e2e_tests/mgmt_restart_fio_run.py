from datetime import datetime
from pathlib import Path
import threading
import random
from e2e_tests.cluster_test_base import TestClusterBase, generate_random_sequence
from utils.common_utils import sleep_n_sec
from utils import proxmox


class TestMgmtNodeReboot(TestClusterBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.test_name = "restart_mgmt_node"
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"

    def run(self):
        fio_threads = []
        self.logger.info("Starting test to restart management node during active FIO...")

        self.sbcli_utils.add_storage_pool(self.pool_name)
        lvol_details = {}
        node = random.choice(self.fio_node)

        for i, _ in enumerate(self.storage_nodes):
            node_uuid = self.sbcli_utils.get_node_without_lvols()
            lvol_name = f"lvl_{generate_random_sequence(4)}_{i}"
            self.sbcli_utils.add_lvol(lvol_name, self.pool_name, size="5G", host_id=node_uuid)

            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(node, cmd)

            device = self.ssh_obj.get_lvol_vs_device(node, lvol_id)
            mount_path = f"{self.mount_base}/{lvol_name}"
            log_path = f"{self.log_base}/{lvol_name}.log"
            self.ssh_obj.format_disk(node, device)
            self.ssh_obj.mount_path(node, device, mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(node, None, mount_path, log_path),
                kwargs={
                    "size": "500M",
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 600,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)

            sleep_n_sec(5)

            lvol_details[lvol_name] = {
                "ID": lvol_id,
                "Mount": mount_path,
                "Log": log_path,
                "Clone": {
                    "ID": None,
                    "Snapshot": None,
                    "Log": None,
                    "Mount": None,
                }
            }

            snapshot_name = f"snap_{lvol_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], lvol_id, snapshot_name)
            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)
            clone_name = f"clone_{lvol_name}"
            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)

            clone_id = self.sbcli_utils.get_lvol_id(clone_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(node, cmd)

            device = self.ssh_obj.get_lvol_vs_device(node, clone_id)
            cl_mount = f"{self.mount_base}/{clone_name}"
            cl_log = f"{self.log_base}/{clone_name}.log"
            self.ssh_obj.format_disk(node, device)
            self.ssh_obj.mount_path(node, device, cl_mount)

            lvol_details[lvol_name]["Clone"].update({
                "ID": clone_id,
                "Snapshot": snapshot_name,
                "Log": cl_log,
                "Mount": cl_mount
            })

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(node, None, cl_mount, cl_log),
                kwargs={
                    "size": "500M",
                    "name": f"{clone_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 600,
                },
            )
            fio_thread.start()
            fio_threads.append(fio_thread)

        # Step 2: Random wait time before reboot
        mode = random.choice(["1m", "5m", "10m", "after_fio"])
        self.logger.info(f"Reboot mode selected: {mode}")

        # Step: Reboot mgmt node
        proxmox_id, vm_id = proxmox.get_proxmox(self.mgmt_nodes[0])
        self.logger.info(f"Rebooting VM {vm_id} on proxmox {proxmox_id}")
        try:
            proxmox.stop_vm(proxmox_id, vm_id)
        except Exception as e:
            raise e

        if mode == "after_fio":
            self.logger.info("Waiting for all FIO threads to complete before reboot...")
            self.common_utils.manage_fio_threads(node, fio_threads, timeout=1000)
        else:
            wait_map = {"1m": 60, "5m": 300, "10m": 600}
            sleep_n_sec(wait_map[mode])

        
        proxmox.start_vm(proxmox_id, vm_id, 600)

        # Step: Wait for WebAppAPI container
        self.logger.info("Waiting for app_WebAppAPI container...")
        for _ in range(60):
            containers = self.ssh_obj.get_running_containers(self.mgmt_nodes[0])
            if any("app_WebAppAPI" in c for c in containers):
                self.logger.info("WebAppAPI is online.")
                break
            sleep_n_sec(10)
        else:
            raise TimeoutError("app_WebAppAPI did not come online.")
        
        sleep_n_sec(100)

        # Step: Cluster status and health
        self.sbcli_utils.wait_for_cluster_status(status="active", timeout=600)
        for node in self.sbcli_utils.get_storage_nodes()["results"]:
            self.sbcli_utils.wait_for_storage_node_status(node_id=node["uuid"], status="online", timeout=300)
            self.sbcli_utils.wait_for_health_status(node_id=node["uuid"], status=True, timeout=300)

        # Step: Wait for FIO (if not already done)
        if mode != "after_fio":
            self.logger.info("Waiting for FIO to complete post reboot...")
            self.common_utils.manage_fio_threads(node, fio_threads, timeout=1000)

        # Step: FIO validation
        for lvol_name, lvol_detail in lvol_details.items():
            self.logger.info(f"Validating FIO logs for {lvol_name} and clone...")
            self.common_utils.validate_fio_test(node=node, log_file=lvol_detail["Log"])
            self.common_utils.validate_fio_test(node=node, log_file=lvol_detail["Clone"]["Log"])

        self.logger.info("TEST CASE PASSED !!!")