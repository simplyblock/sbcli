import time
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

class TestManyClonesFromSameSnapshot(TestClusterBase):
    """
    This script creates a storage pool and logical volumes, then iteratively creates clones from snapshots of 
    the logical volumes, formats and mounts them, runs FIO workloads, and measures the time taken for each operation. 
    The script then cleans up by unmounting, disconnecting, and deleting logical volumes, snapshots, and pools.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "50G"
        self.fs_type = "ext4"
        self.mount_path = "/mnt"
        self.num_iterations = 40
        self.timings = []

    def run(self):
        """Performs each step of the test case"""
        self.logger.info("Inside run function")

        # Create Storage Pool
        self.logger.info(f"Creating pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        # Create LVOL
        lvol_name = "lvol_2_1"
        self.logger.info(f"Creating logical volume: {lvol_name} with configuration 2+1 and snapshot capability")
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

        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
        self.connect_and_mount_lvol(lvol_name, lvol_id)

        # Run FIO workload on the LVOL
        mount_point = f"{self.mount_path}/{lvol_name}"
        self.run_fio_workload(mount_point)

        # Create a snapshot of the LVOL
        snapshot_name = f"{lvol_name}_snapshot"
        self.logger.info(f"Creating snapshot: {snapshot_name} for LVOL ID: {lvol_id}")
        self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0], lvol_id=lvol_id, snapshot_name=snapshot_name)

        # Iterate and perform operations with clones
        for i in range(1, self.num_iterations + 1):
            clone_name = f"{lvol_name}_{i}_clone"
            self.logger.info(f"Iteration {i} of {self.num_iterations}: Performing operation with clone: {clone_name}")
            self.create_clone_and_run_fio(snapshot_name, clone_name)

        self.logger.info("Script execution completed")
        self.cleanup()

        # Print timings
        self.print_timings()

    def connect_and_mount_lvol(self, lvol_name, lvol_id, pre_initial_devices=None):
        """Connects and mounts a logical volume.

        Args:
            pre_initial_devices: If provided, use this as the baseline device
                set for diff detection (should be captured before clone creation
                so namespaced clones that auto-appear are caught).
        """
        node = self.mgmt_nodes[0]
        if pre_initial_devices is not None:
            initial_devices = pre_initial_devices
        else:
            initial_devices = set(self.ssh_obj.get_devices(node=node))

        # Connect LVOL
        self.logger.info(f"Connecting logical volume: {lvol_id}")
        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
        start_time = time.time()
        already_connected = False
        for connect_str in connect_ls:
            _, error = self.ssh_obj.exec_command(node=node, command=connect_str)
            if error and "already connected" in error.lower():
                already_connected = True
        end_time = time.time()

        time_taken = end_time - start_time
        self.timings.append(f"Connect {lvol_name} - {time_taken} seconds")

        final_devices = set(self.ssh_obj.get_devices(node=node))
        new_devices = list(final_devices - initial_devices)
        lvol_device = None
        if new_devices:
            lvol_device = f"/dev/{new_devices[0].strip()}"

        if not lvol_device and already_connected:
            # Namespaced clone shares parent's NQN — ns-rescan needed
            out, _ = self.ssh_obj.exec_command(
                node,
                "ls /dev/nvme[0-9]* 2>/dev/null | grep -oP 'nvme\\d+$' "
                "| sort -u",
                supress_logs=True,
            )
            for ctrl in (out or "").strip().splitlines():
                ctrl = ctrl.strip()
                if ctrl:
                    self.ssh_obj.exec_command(
                        node, f"sudo nvme ns-rescan /dev/{ctrl}",
                        supress_logs=True,
                    )
            sleep_n_sec(3)
            rescan_devices = set(self.ssh_obj.get_devices(node=node))
            new_after_rescan = list(rescan_devices - initial_devices)
            if new_after_rescan:
                lvol_device = f"/dev/{new_after_rescan[0].strip()}"

        # Format LVOL (skip for clones — they already have data)
        is_clone = "cl" in lvol_name
        if not is_clone:
            self.format_fs(lvol_device, self.fs_type)

        # Mount LVOL
        mount_point = f"{self.mount_path}/{lvol_name}"
        if self.fs_type == "xfs" and is_clone:
            self.ssh_obj.clone_mount_gen_uuid(node, lvol_device)
        self.ssh_obj.mount_path(node=node, device=lvol_device, mount_path=mount_point)

        return lvol_device

    def format_fs(self, device, fs_type):
        """Formats the device with the specified filesystem type"""
        self.logger.info(f"Formatting device: {device} with filesystem: {fs_type}")
        start_time = time.time()
        self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=device, fs_type=fs_type)
        end_time = time.time()
        time_taken = end_time - start_time
        self.timings.append(f"Format {device} - {time_taken} seconds")

    def run_fio_workload(self, mount_point):
        """Runs FIO workload on the mounted logical volume"""
        self.logger.info(f"Running FIO workload on {mount_point}")
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
        self.timings.append(f"FIO {mount_point} - {time_taken} seconds")

    def create_clone_and_run_fio(self, snapshot_name, clone_name):
        """Creates a clone from a snapshot and runs FIO workload"""
        snapshot_id = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0], snapshot_name=snapshot_name)
        self.logger.info(f"Creating clone from snapshot: {snapshot_name} with clone name {clone_name}")

        # Capture device list before clone creation — namespaced clones
        # may auto-appear if parent subsystem is already connected.
        initial_devices = set(self.ssh_obj.get_devices(node=self.mgmt_nodes[0]))
        self.ssh_obj.add_clone(node=self.mgmt_nodes[0], snapshot_id=snapshot_id, clone_name=clone_name)

        # Get the clone's LVOL ID and connect it
        clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
        lvol_device = self.connect_and_mount_lvol(clone_name, clone_id, pre_initial_devices=initial_devices)

        # Run FIO workload on the clone
        mount_point_clone = f"{self.mount_path}/{clone_name}"
        self.run_fio_workload(mount_point_clone)

        self.logger.info(f"Unmounting disk {lvol_device}")
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=lvol_device)
        self.logger.info(f"Removing directory {mount_point_clone}")
        self.ssh_obj.remove_dir(node=self.mgmt_nodes[0],
                                dir_path=mount_point_clone)
        self.disconnect_lvol(clone_id)

    def cleanup(self):
        """Cleans up by unmounting, disconnecting, and deleting logical volumes, snapshots, and pools"""
        self.logger.info("Starting cleanup process")
        self.unmount_all()
        self.remove_mount_dirs()
        self.disconnect_lvols()
        self.delete_snapshots()

    def print_timings(self):
        """Prints the timings for operations"""
        self.logger.info("Printing timings for operations")
        for timing in self.timings:
            self.logger.info(timing)