import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
import traceback

class TestSnapshotBatchCloneLVOLs(TestClusterBase):
    """
    This script performs the following operations:

    1. Creates a storage pool and a single logical volume (LVOL).
    2. Iterates on creating snapshots and clones in batches of 25.
    3. Connects, mounts, lists files, creates a test file on each clone, and runs FIO.
    4. Disconnects all clones and continues until a crash or node limit is reached.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "80G"
        self.mount_path = "/mnt"
        self.snapshot_name_prefix = "snapshot"
        self.clone_name_prefix = "clone"
        self.batch_size = 25
        self.fs_type = "ext4"
        self.pool_name = "test_pool"
        self.clone_details = {}

    def run(self):
        """Executes the creation of storage pool, snapshots, and clones in batches."""
        self.logger.info("Starting the snapshot and clone creation process.")

        # Step 1: Create a storage pool
        self.logger.info(f"Creating storage pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Step 2: Create an initial LVOL
        lvol_name = self.lvol_name
        self.logger.info(f"Creating initial logical volume {lvol_name}")
        self.sbcli_utils.add_lvol(
            lvol_name=lvol_name,
            pool_name=self.pool_name,
            size=self.lvol_size,
            # distr_ndcs=2,
            # distr_npcs=1
        )

        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)

        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        disk_use = None
        self.logger.info("Initial vs final disk:")
        self.logger.info(f"Initial: {initial_devices}")
        self.logger.info(f"Final: {final_devices}")
        for device in final_devices:
            if device not in initial_devices:
                self.logger.info(f"Using disk: /dev/{device.strip()}")
                disk_use = f"/dev/{device.strip()}"
                break
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=disk_use)
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                 device=disk_use)
        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                device=disk_use,
                                mount_path=f"{self.mount_path}/{self.lvol_name}")
        
        self.logger.info(f"Running FIO workload to create a test file on clone {self.mount_path}/{self.lvol_name}")
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, f"{self.mount_path}/{self.lvol_name}"),
                                      kwargs={"name": f"fio_{lvol_name}",
                                              "size": "1GiB",
                                              "runtime": 100,
                                              "nrfiles": 1,
                                              "bs": "256K",
                                              "debug": self.fio_debug})
        fio_thread.start()
        sleep_n_sec(8)
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0], threads=[fio_thread], timeout=800)

        # Step 3: Create snapshots and clones in batches
        batch_count = 0
        while True:
            try:
                self.logger.info(f"Starting batch {batch_count + 1}")
                self.create_snapshot_clone_batch(lvol_id, batch_count)
                batch_count += 1
            except Exception as e:
                self.logger.error(f"Error occurred: {e}")
                traceback.print_exc()
                break

        self.logger.info("Finished running all batches or stopped due to an error.")
        self.cleanup()

    def create_snapshot_clone_batch(self, lvol_id, batch_count):
        """Creates a batch of snapshots and clones from the LVOL."""
        for i in range(1, self.batch_size + 1):
            snapshot_name = f"{self.snapshot_name_prefix}_batch{batch_count + 1}_ss{i}"
            clone_name = f"{self.clone_name_prefix}_batch{batch_count + 1}_cl{i}"
            self.logger.info(f"Creating snapshot {snapshot_name} from LVOL {lvol_id}")

            # Create a snapshot
            self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0], lvol_id=lvol_id, snapshot_name=snapshot_name)

            # Create a clone from the snapshot
            snapshot_id = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0], snapshot_name=snapshot_name)
            self.logger.info(f"Creating clone {clone_name} from snapshot {snapshot_name}")
            self.ssh_obj.add_clone(node=self.mgmt_nodes[0], snapshot_id=snapshot_id, clone_name=clone_name)

        # Test the clone
        for i in range(1, self.batch_size + 1):
            clone_name = f"{self.clone_name_prefix}_batch{batch_count + 1}_cl{i}"
            self.connect_and_test_clone(clone_name)
        
        for clone_name, clone_detail in self.clone_details.items():
            self.logger.info(f"Unmounting disk {clone_detail['disk']}")
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                      device=clone_detail["disk"])
            self.logger.info(f"Removing directory {clone_detail['mount']}")
            self.ssh_obj.remove_dir(node=self.mgmt_nodes[0],
                                    dir_path=clone_detail['mount'])
            self.disconnect_lvol(clone_detail['id'])

    def connect_and_test_clone(self, clone_name):
        """Connects the clone, mounts it, lists files, runs FIO, and then disconnects it."""
        clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
        self.clone_details[clone_name] = {
            "id": clone_id,
            "disk": None,
            "mount": None
        }

        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
        for connect_str in connect_ls:
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

        final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
        clone_device = None
        for device in final_devices:
            if device not in initial_devices:
                clone_device = f"/dev/{device.strip()}"
                break

        mount_point = f"{self.mount_path}/{clone_name}"
        
        if self.fs_type == "xfs":
            cmd=f"sudo xfs_admin -U generate {clone_device}"
            self.logger.info(f"Run command (before mounting dev formatted in xfs: {cmd}")
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=cmd)
            self.logger.info(f"Mounting clone device: {clone_device} at {mount_point}")
            cmd = f"sudo mount {clone_device} {mount_point} -o nouuid"
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=cmd)
        else:
            self.logger.info(f"Mounting clone {clone_name} at {mount_point}")
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=clone_device, mount_path=mount_point)
        
        self.clone_details[clone_name]["disk"] = clone_device
        self.clone_details[clone_name]["mount"] = mount_point

        # Create a test file using FIO
        self.logger.info(f"Running FIO workload to create a test file on clone {mount_point}")
        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                      args=(self.mgmt_nodes[0], None, mount_point),
                                      kwargs={"name": f"fio_{clone_name}",
                                              "size": "1GiB",
                                              "runtime": 100,
                                              "nrfiles": 1,
                                              "bs": "256K",
                                              "debug": self.fio_debug})
        fio_thread.start()

        # Add sleep and manage FIO threads
        sleep_n_sec(8)
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0], threads=[fio_thread], timeout=800)

        # Disconnect the clone
        self.logger.info(f"Finished FIO for {clone_name}.")

    def disconnect_lvol(self, lvol_device):
        """Disconnects the logical volume."""
        nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0],
                                                    nqn_filter=lvol_device)
        for nqn in nqn_lvol:
            self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
            self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=nqn)

    def cleanup(self):
        """Cleans up by unmounting, disconnecting, and deleting all logical volumes and snapshots."""
        self.logger.info("Starting cleanup process.")
        self.unmount_all()
        self.remove_mount_dirs()
        self.disconnect_lvols()
        self.delete_snapshots()
        self.sbcli_utils.delete_all_lvols()
        self.sbcli_utils.delete_all_storage_pools()

