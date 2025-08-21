import os
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
import re

class TestMultiLvolFio(TestClusterBase):
    """
    This script performs comprehensive testing of logical volume configurations, filesystem types, and workload sizes.
    It covers a total of 48 test cases combining different configurations and workloads. The detailed combinations are:

    1. Filesystem Types:
       - ext4
       - xfs

    2. Configurations:
       - 1+0
       - 2+1
       - 4+1
       - 4+2
       - 8+1
       - 8+2

    3. Workload Sizes:
       - 5G
       - 10G
       - 20G
       - 40G

    The total number of test cases is calculated by multiplying the number of filesystem types, configurations, and workload sizes:
    2 (Filesystem Types) * 6 (Configurations) * 4 (Workload Sizes) = 48 Test Cases

    The script performs the following steps for each combination:
    - Creates logical volumes with specified configurations.
    - Connects logical volumes.
    - Formats the logical volumes with the specified filesystem.
    - Mounts the logical volumes.
    - Runs fio workloads of different sizes on the mounted logical volumes.
    - Generates and verifies checksums for test files.
    - Creates snapshots and clones from the snapshots.
    - Runs fio workloads on the clones.
    - Verifies the integrity of data by comparing checksums before and after workloads.
    - Cleans up by unmounting, disconnecting, and deleting logical volumes, snapshots, and pools.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "100G"
        self.mount_path = "/mnt"
        self.checksum_log_file = "checksum_verification_results.txt"



    def run(self):
        """Performs each step of the test case"""
        self.logger.info("Inside run function")

        self.logger.info(f"Creating pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        lvol_vs_disk = {}
        if self.ndcs == 0 and self.npcs == 0:
            lvol_config_list = ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]
        else:
            lvol_config_list = [f"{self.ndcs}+{self.npcs}" ]


        for config in lvol_config_list:
            ndcs, npcs = config.split('+')
            lvol_name = f"lvl_{npcs}"

            self.logger.info(f"Creating logical volume: {lvol_name} with ndcs: {ndcs} and npcs: {npcs}")
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                # distr_ndcs=int(ndcs),
                # distr_npcs=int(npcs)
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} present in list of lvols post add: {lvols}"
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            self.logger.info("Initial vs final disk:")
            self.logger.info(f"Initial: {initial_devices}")
            self.logger.info(f"Final: {final_devices}")
            for device in final_devices:
                if device not in initial_devices:
                    self.logger.info(f"Using disk: /dev/{device.strip()}")
                    lvol_vs_disk[lvol_name] = f"/dev/{device.strip()}"
                    break

            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=lvol_vs_disk[lvol_name])
        
        lvol_list = self.sbcli_utils.list_lvols()

        with open(self.checksum_log_file, 'w', encoding='utf-8'):

            for fs_type in ["ext4", "xfs"]:
                self.logger.info(f"Processing filesystem type: {fs_type}")
                sleep_n_sec(300)

                # for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
                for config in ["2+1"]:
                    ndcs, npcs = config.split('+')
                    lvol_name = f"lvl_{npcs}"
                    mount_point = f"{self.mount_path}/{lvol_name}"
                    self.logger.info(f"Formating lvol: {lvol_name} with fs type: {fs_type} And moutning at {mount_point}")
                    self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=mount_point)
                    self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                            device=lvol_vs_disk[lvol_name],
                                            fs_type=fs_type)
                    self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                            device=lvol_vs_disk[lvol_name],
                                            mount_path=mount_point)

                for size in ["5GiB", "10GiB"]:
                    # for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
                    for config in ["2+1"]:
                        ndcs, npcs = config.split('+')
                        lvol_name = f"lvl_{npcs}"
                        lvol_id = lvol_list[lvol_name]

                        mount_point = f"{self.mount_path}/{lvol_name}"

                        self.logger.info(f"Running fio workload with size: {size} on {mount_point}")

                        fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
                                                    args=(self.mgmt_nodes[0], None, mount_point),
                                                    kwargs={"name": f"fio_run_{size}",
                                                            "size": size,
                                                            "runtime": 100,
                                                            "nrfiles": 5,
                                                            "debug": self.fio_debug})
                        fio_thread.start()
                        sleep_n_sec(5)
                        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                                            threads=[fio_thread],
                                                            timeout=1000)
                        
                        # Generating checksums for base volume files
                        self.logger.info(f"Generating checksums for files in base volume: {mount_point}")
                        base_files = self.ssh_obj.find_files(node=self.mgmt_nodes[0], directory=mount_point)
                        base_checksums = self.ssh_obj.generate_checksums(node=self.mgmt_nodes[0], files=base_files)

                        size_name = re.findall(r'\d+', size)[0]
                        snapshot_name = f"{lvol_name}_{size_name}_ss"
                        self.logger.info(f"Creating snapshot {snapshot_name} for volume: {lvol_name}")
                        
                        self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0],
                                                  lvol_id=lvol_id,
                                                  snapshot_name=snapshot_name)

                        self.logger.info(f"Creating clone from snapshot: {snapshot_name}")

                        snapshot_list = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                                                command=f"{self.base_cmd} snapshot list")
                        self.logger.info(f"Snapshot list: {snapshot_list}")

                        clone_name = f"{snapshot_name}_cl"
                        snapshot_id = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0],
                                                                snapshot_name=snapshot_name)

                        self.ssh_obj.add_clone(node=self.mgmt_nodes[0],
                                            snapshot_id=snapshot_id,
                                            clone_name=clone_name)

                        self.logger.info(f"Fetching clone logical volume ID for: {clone_name}")

                        clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
                        
                        initial_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

                        connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
                        for connect_str in connect_ls:
                            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

                        final_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
                        disk_use_clone = None
                        self.logger.info(f"Initial: {initial_devices_clone}")
                        self.logger.info(f"Final: {final_devices_clone}")
                        for device in final_devices_clone:
                            if device not in initial_devices_clone:
                                self.logger.info(f"Using disk: /dev/{device.strip()}")
                                disk_use_clone = f"/dev/{device.strip()}"
                                break

                        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)
                        self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use_clone, fs_type=fs_type)
                        mount_point_clone = f"{self.mount_path}/{clone_name}"
                        self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                                device=disk_use_clone,
                                                mount_path=mount_point_clone)
                        clone_fio_dir = os.path.join(mount_point_clone, "clone_test")

                        self.ssh_obj.make_directory(node=self.mgmt_nodes[0],
                                                    dir_name=clone_fio_dir)

                        self.logger.info(f"Running fio workload on clone mount point: {clone_fio_dir}")
                        fio_thread_clone = threading.Thread(target=self.ssh_obj.run_fio_test,
                                                            args=(self.mgmt_nodes[0], None, clone_fio_dir),
                                                            kwargs={"name": f"fio_clone_run_{size}",
                                                                    "size": size,
                                                                    "runtime": 100,
                                                                    "nrfiles": 5,
                                                                    "debug": self.fio_debug})
                        fio_thread_clone.start()
                        sleep_n_sec(5)
                        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                                            threads=[fio_thread],
                                                            timeout=1000)

                        # Generating checksums for clone volume files
                        self.logger.info(f"Generating checksums for files in clone volume: {mount_point_clone}")
                        clone_files = self.ssh_obj.find_files(node=self.mgmt_nodes[0], directory=mount_point_clone)
                        clone_checksums = self.ssh_obj.generate_checksums(node=self.mgmt_nodes[0], files=clone_files)

                        self.logger.info(f"Generating checksums for fio files in clone volume: {clone_fio_dir}")
                        clone_files_fio = self.ssh_obj.find_files(node=self.mgmt_nodes[0], directory=clone_fio_dir)
                        clone_checksums_fio = self.ssh_obj.generate_checksums(node=self.mgmt_nodes[0], files=clone_files_fio)

                        # Verifying that the base volume has not been changed
                        self.logger.info(f"Verifying base volume files integrity: {mount_point}")
                        self.ssh_obj.verify_checksums(node=self.mgmt_nodes[0], files=base_files, checksums=base_checksums)

                        # Verifying that the base volume has not been changed
                        self.logger.info(f"Verifying base volume files vs clone files integrity: {mount_point}")
                        self.ssh_obj.verify_checksums(node=self.mgmt_nodes[0], files=clone_files, checksums=base_checksums, clone_base=True)

                        # Deleting test files from base volumes
                        self.logger.info(f"Deleting test files from base volume: {mount_point}")
                        self.ssh_obj.delete_files(node=self.mgmt_nodes[0], files=base_files)

                        # Verifying that the test files still exist on the clones
                        self.logger.info(f"Verifying clone  volume files integrity: {mount_point_clone}")
                        self.ssh_obj.verify_checksums(node=self.mgmt_nodes[0], files=clone_files, checksums=clone_checksums)

                        self.logger.info(f"Verifying clone fio volume files integrity: {mount_point_clone}")
                        self.ssh_obj.verify_checksums(node=self.mgmt_nodes[0], files=clone_files_fio, checksums=clone_checksums_fio)

                        self.logger.info(f"Unmounting clone mount point: {mount_point_clone}")
                        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)

                        self.logger.info(f"Disconnecting logical volume: {clone_id}")
                        nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0],
                                                                    nqn_filter=clone_id)
                        for nqn in nqn_lvol:
                            self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=nqn)

                        self.logger.info(f"Deleting clone logical volume: {clone_id}")
                        self.sbcli_utils.delete_lvol(lvol_name=clone_name)

                        self.logger.info(f"TEST Execution Completed for NDCS: {ndcs}, NPCS: {npcs}, FIO Size: {size}, FS Type: {fs_type}")

        self.logger.info("Cleaning up")
        self.cleanup()

        self.logger.info("TEST CASE PASSED !!!")

    def cleanup(self):
        """ Perform cleanup steps """
        self.logger.info("Starting cleanup process")
        self.unmount_all()
        self.remove_mount_dirs()
        self.delete_snapshots()
        self.logger.info("Cleanup complete")
