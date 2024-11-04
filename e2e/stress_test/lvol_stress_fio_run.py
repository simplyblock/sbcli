import time
import os
import random
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

class TestStressLvolClusterFioRun(TestClusterBase):
    """
    This script performs the following operations:
    1. Creat 80 lvols 50GB lvols
    2. Run mutliple types of fio with different block size and params
    3. Let them complete.
    4. Validate no errors
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "50G"
        self.fio_size = "40G"
        self.mount_path = "/mnt"
        self.num_iterations = 80
        self.lvol_name = "test_lvl"
        self.lvols_disks_mount = {}

    def run(self):
        """Performs each step of the test case"""
        self.logger.info("Inside run function")

        # Create Storage Pool
        self.logger.info(f"Creating pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        for i in range(1, self.num_iterations + 1):
            fs_type = random.choice(["ext4", "xfs"])

            # Modify lvol name to include fs_type
            lvol_name = f"{self.lvol_name}_{fs_type}_{i}_ll"
            self.logger.info(f"Iteration {i} of {self.num_iterations}")
            self.logger.info(f"Creating logical volume: {lvol_name}")
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} present in list of lvols post add: {lvols}"
            
            self.lvols_disks_mount[lvol_name] = {
                "device": None,
                "mount_path": None,
                "log": None
            }
            connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            
            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            self.lvols_disks_mount[lvol_name]["device"] = lvol_device
            self.lvols_disks_mount[lvol_name]["log"] = f"{lvol_name}_log.txt"

            trim = random.choice([0,0,1])
            if not trim:
                self.logger.info(f"Formatting device: {lvol_device} with filesystem: {fs_type}")
                self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=lvol_device, fs_type=fs_type)
                mount_point = f"{self.mount_path}/{lvol_name}"
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=lvol_device, mount_path=mount_point)
                self.lvols_disks_mount[lvol_name]["mount_path"] = mount_point

        fio_threads = []
        fio_started = 0
        for lvol_name in list(self.lvols_disks_mount.keys()):
            block_sizes_kb = [4, 8, 16, 32, 64, 128, 256]
            fio_started += 1
            
            mount = self.lvols_disks_mount[lvol_name]["mount_path"]
            device = self.lvols_disks_mount[lvol_name]["device"]
            log_file = self.lvols_disks_mount[lvol_name]["log"]
            if mount:
                fio_workload = random.choice(["randrw", "read", "write"])
                fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                              args=(self.mgmt_nodes[0], None, mount, log_file),
                                              kwargs={"name": f"fio_run_{lvol_name}",
                                                      "rw": fio_workload,
                                                      "size": self.fio_size,
                                                      "runtime": 7200,
                                                      "nrfiles": 3,
                                                      "bs": f"{random.choice(block_sizes_kb)}K",
                                                      "time_based": True,
                                                      "debug": self.fio_debug})
            else:
                fio_workload = random.choice(["randrw","write", "trimwrite"])
                fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                              args=(self.mgmt_nodes[0], device, None, log_file),
                                              kwargs={"name": f"fio_run_{lvol_name}",
                                                      "rw": fio_workload,
                                                      "size": self.fio_size,
                                                      "runtime": 7200,
                                                      "nrfiles": 3,
                                                      "bs": f"{random.choice(block_sizes_kb)}K",
                                                      "time_based": True,
                                                      "debug": self.fio_debug})

            fio_thread.start()
            fio_threads.append(fio_thread)
            sleep_n_sec(10)
        sleep_n_sec(10)
        self.logger.info(f"Total fio started: {fio_started}")
        
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread],
                                             timeout=20000)
        for lvol_name in list(self.lvols_disks_mount.keys()):
            log_file = self.lvols_disks_mount[lvol_name]["log"]
            self.common_utils.validate_fio_test(
                node=self.mgmt_nodes[0],
                log_file=log_file
            )

        self.logger.info(f"Total fio started: {fio_started}")

        self.logger.info("Stress test execution completed")
