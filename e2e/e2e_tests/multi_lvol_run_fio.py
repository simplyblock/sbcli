import time
import random
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

class TestManyLvolSameNode(TestClusterBase):
    """
    This script performs the following operations:

    Rerun step 1-5 40 times

    1. Creates a storage pool and logical volumes iteratively.
    2. Connects, formats, and mounts the logical volumes.
    3. Runs FIO workloads on the mounted logical volumes.
    4. Measures the time taken for connecting, formatting, and running FIO.
    5. Cleans up by unmounting, disconnecting, and deleting logical volumes and the pool.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "20G"
        self.mount_path = "/mnt"
        self.num_iterations = 40
        self.format_timings = []
        self.connect_timings = []
        self.fio_run_timings = []

    def run(self):
        """Performs each step of the test case"""
        self.logger.info("Inside run function")

        # Create Storage Pool
        self.logger.info(f"Creating pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        node_id = self.sbcli_utils.get_node_without_lvols()

        for i in range(1, self.num_iterations + 1):
            fs_type = random.choice(["ext4", "xfs"])

            # Modify lvol name to include fs_type
            lvol_name = f"{self.lvol_name}_{fs_type}_{i}_ll"
            self.logger.info(f"Iteration {i} of {self.num_iterations}")
            self.logger.info(f"Creating logical volume: {lvol_name} with configuration 2+1")
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                # distr_ndcs=2,
                # distr_npcs=1,
                host_id=node_id
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} present in list of lvols post add: {lvols}"
            self.mount_and_run_fio(lvol_name, fs_type)
            self.cleanup()

        self.logger.info("Script execution completed")

        # Print Timings
        self.print_timings()

        self.logger.info("CLEANUP COMPLETE")

    def mount_and_run_fio(self, lvol_name, fs_type):
        """Mounts the logical volume and runs FIO workload"""
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        # Connect LVOL
        start_time = time.time()
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)
        end_time = time.time()

        time_taken = end_time - start_time
        self.connect_timings.append(f"{lvol_name} - {time_taken} seconds")

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        lvol_device = None
        for device in final_devices:
            if device not in initial_devices:
                lvol_device = f"/dev/{device.strip()}"
                break

        # Format LVOL
        self.format_fs(lvol_device, fs_type)

        # Mount and Run FIO
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=lvol_device, mount_path=mount_point)

        start_time = time.time()
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, mount_point),
                                      kwargs={"name": "fio_run_1GiB",
                                              "size": "1GiB",
                                              "runtime": 100,
                                              "nrfiles": 5,
                                              "bs": "4K-128K",
                                              "debug": self.fio_debug})
        fio_thread.start()
        sleep_n_sec(8)
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread],
                                             timeout=400)
        end_time = time.time()

        time_taken = end_time - start_time
        self.fio_run_timings.append(f"{lvol_name} - {mount_point} - {time_taken} seconds")

    def format_fs(self, device, fs_type):
        """Formats the device with the specified filesystem type"""
        self.logger.info(f"Formatting device: {device} with filesystem: {fs_type}")
        
        start_time = time.time()
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=device, fs_type=fs_type)
        end_time = time.time()

        time_taken = end_time - start_time
        self.format_timings.append(f"{device} - {time_taken} seconds")

    def cleanup(self):
        """Cleans up by unmounting, disconnecting, and deleting logical volumes and the pool"""
        self.logger.info("Starting cleanup process")
        self.unmount_all()
        self.remove_mount_dirs()
        self.disconnect_lvols()

    def print_timings(self):
        """Prints the timings for connect, format, and fio operations"""
        self.logger.info("Printing timings for connect operations")
        for timing in self.connect_timings:
            self.logger.info(timing)

        self.logger.info("Printing timings for mkfs operations")
        for timing in self.format_timings:
            self.logger.info(timing)

        self.logger.info("Printing timings for fio operations")
        for timing in self.fio_run_timings:
            self.logger.info(timing)
