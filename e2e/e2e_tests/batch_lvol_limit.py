import time
import random
import threading
from utils.common_utils import sleep_n_sec
from e2e_tests.cluster_test_base import TestClusterBase
from logger_config import setup_logger
import traceback

class TestBatchLVOLsLimit(TestClusterBase):
    """
    This script performs the following operations:

    1. Creates logical volumes in batches of 25 until a crash or failure occurs.
    2. Connects, formats, and mounts each logical volume.
    3. Runs FIO workloads to create a test file on each logical volume.
    4. Disconnects all the logical volumes and repeats until the node limit is reached.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "10G"
        self.mount_path = "/mnt"
        self.batch_size = 25
        self.fs_types = ["ext4", "xfs"]
        self.lvol_details = {}

    def run(self):
        """Executes the batch creation and testing of logical volumes."""

        self.logger.info(f"Creating storage pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        self.logger.info("Starting the batch creation of logical volumes.")
        batch_count = 0
        while True:
            try:
                self.logger.info(f"Starting batch {batch_count + 1}")
                self.create_and_test_lvol_batch(batch_count)
                batch_count += 1
            except Exception as e:
                self.logger.error(f"Error occurred: {e}")
                traceback.print_exc()
                break

        self.logger.info("Finished running all batches or stopped due to an error.")
        self.cleanup()

    def create_and_test_lvol_batch(self, batch_count):
        """Creates and tests a batch of logical volumes."""
        lvol_vs_fstype = {}
        for i in range(1, self.batch_size + 1):
            fs_type = random.choice(self.fs_types)
            lvol_name = f"{self.lvol_name}_{fs_type}_batch{batch_count + 1}_lvol{i}"
            self.logger.info(f"Creating logical volume {lvol_name} with filesystem {fs_type}")
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                # distr_ndcs=2,
                # distr_npcs=1
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} present in list of lvols post add: {lvols}"
            lvol_vs_fstype[lvol_name] = fs_type

        for lvol_name, fs_type in lvol_vs_fstype.items():
            self.connect_and_test_lvol(lvol_name, fs_type)

        for lvol_name, lvol_detail in self.lvol_details.items():
            self.logger.info(f"Unmounting disk {lvol_detail['disk']}")
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                      device=lvol_detail["disk"])
            self.logger.info(f"Removing directory {lvol_detail['mount']}")
            self.ssh_obj.remove_dir(node=self.mgmt_nodes[0],
                                    dir_path=lvol_detail['mount'])
            self.disconnect_lvol(lvol_detail['id'])

    def connect_and_test_lvol(self, lvol_name, fs_type):
        """Connects the LVOL, formats, mounts, and runs FIO."""
        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
        self.lvol_details[lvol_name] = {
            "id": lvol_id,
            "disk": None,
            "mount": None
        }

        # Connect LVOL
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)
        
        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        lvol_device = None
        for device in final_devices:
            if device not in initial_devices:
                lvol_device = f"/dev/{device.strip()}"
                break

        # Format LVOL
        self.logger.info(f"Formatting device {lvol_device} with {fs_type} filesystem")
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=lvol_device, fs_type=fs_type)

        # Mount and Run FIO
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=lvol_device, mount_path=mount_point)

        self.lvol_details[lvol_name]["disk"] = lvol_device
        self.lvol_details[lvol_name]["mount"] = mount_point

        self.logger.info(f"Running FIO workload on {mount_point}")
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, mount_point),
                                      kwargs={"name": f"fio_{lvol_name}",
                                              "size": "1GiB",
                                              "runtime": 100,
                                              "nrfiles": 1,
                                              "bs": "256K",
                                              "debug": self.fio_debug})
        fio_thread.start()
        
        sleep_n_sec(8)
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=[fio_thread],
                                             timeout=400)
        fio_thread.join()
        self.logger.info(f"Finished FIO for {lvol_name}.")

    def disconnect_lvol(self, lvol_device):
        """Disconnects the logical volume."""
        nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0], nqn_filter=lvol_device)
        for nqn in nqn_lvol:
            self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
            self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=nqn)

    def cleanup(self):
        """Cleans up by unmounting, disconnecting, and deleting all logical volumes."""
        self.logger.info("Starting cleanup process.")
        self.unmount_all()
        self.remove_mount_dirs()
        self.disconnect_lvols()