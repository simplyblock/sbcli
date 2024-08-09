# # import json
# # import os
# # import random
# # import threading
# # import time
# # from utils.common_utils import sleep_n_sec
# # from logger_config import setup_logger
# # from e2e_tests.cluster_test_base import TestClusterBase


# # class TestLvolOperations(TestClusterBase):
# #     def __init__(self, **kwargs):
# #         super().__init__(**kwargs)
# #         self.logger = setup_logger(__name__)
# #         self.mount_path = "/mnt"
# #         self.fs_types = ["ext4", "xfs"]
# #         self.configurations = ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]
# #         self.workload_sizes = ["5G", "10G", "20G", "40G"]
# #         self.lvol_devices = {}

# #     def create_lvol(self, name, ndcs, npcs):
# #         self.sbcli_utils.add_lvol(
# #             lvol_name=name,
# #             pool_name=self.pool_name,
# #             size="160G",
# #             distr_ndcs=ndcs,
# #             distr_npcs=npcs
# #         )
# #         device = self.ssh_obj.get_device_from_lvol_name(node=self.mgmt_nodes[0], lvol_name=name)
# #         self.lvol_devices[name] = {"Device": device, "Path": f"{self.mount_path}/{name}", "Log": f"{self.mount_path}/{name}/log.json"}

# #     def format_fs(self, device, fs_type):
# #         self.ssh_obj.format_disk(device=device, fs_type=fs_type)

# #     def run_fio_workload(self, mount_point, size):
# #         self.ssh_obj.run_fio_test(
# #             node=self.mgmt_nodes[0],
# #             directory=mount_point,
# #             readwrite="write",
# #             bs="4K-128K",
# #             size=size,
# #             name="test",
# #             numjobs=3
# #         )

# #     def verify_checksums(self, files):
# #         checksums = {}
# #         for file in files:
# #             checksums[file] = self.ssh_obj.calculate_md5sum(node=self.mgmt_nodes[0], file=file)
# #         return checksums

# #     def run(self):
# #         self.logger.info("Inside run function")
# #         for config in self.configurations:
# #             ndcs, npcs = map(int, config.split('+'))
# #             lvol_name = f"lvol_{ndcs}_{npcs}"
# #             self.create_lvol(lvol_name, ndcs, npcs)
# #             device = self.lvol_devices[lvol_name]["Device"]

# #             for fs_type in self.fs_types:
# #                 self.format_fs(device, fs_type)

# #                 mount_point = self.lvol_devices[lvol_name]["Path"]
# #                 self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=device, mount_path=mount_point)

# #                 for size in self.workload_sizes:
# #                     self.run_fio_workload(mount_point, size)
# #                     test_files = self.ssh_obj.list_files(node=self.mgmt_nodes[0], directory=mount_point)
# #                     checksums = self.verify_checksums(test_files)

# #                     snapshot_name = f"{lvol_name}_snapshot"
# #                     self.ssh_obj.create_snapshot(node=self.mgmt_nodes[0], lvol_name=lvol_name, snapshot_name=snapshot_name)
# #                     self.ssh_obj.list_snapshots(node=self.mgmt_nodes[0])

# #                     clone_name = f"{snapshot_name}_clone"
# #                     self.ssh_obj.create_clone(node=self.mgmt_nodes[0], snapshot_name=snapshot_name, clone_name=clone_name)
# #                     self.ssh_obj.list_clones(node=self.mgmt_nodes[0])

# #                     clone_device = self.ssh_obj.get_device_from_lvol_name(node=self.mgmt_nodes[0], lvol_name=clone_name)
# #                     clone_mount_point = f"{self.mount_path}/{clone_name}"
# #                     self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=clone_device, mount_path=clone_mount_point)

# #                     clone_files = self.ssh_obj.list_files(node=self.mgmt_nodes[0], directory=clone_mount_point)
# #                     clone_checksums = self.verify_checksums(clone_files)

# #                     for file, checksum in checksums.items():
# #                         if checksum != clone_checksums.get(file):
# #                             self.logger.error(f"Checksum mismatch for {file} on clone")

# #                     clone_workload_dir = f"{clone_mount_point}/clone_test"
# #                     self.ssh_obj.create_directory(node=self.mgmt_nodes[0], directory=clone_workload_dir)
# #                     self.run_fio_workload(clone_workload_dir, size)

# #                     base_checksums_after = self.verify_checksums(test_files)
# #                     for file, checksum in checksums.items():
# #                         if checksum != base_checksums_after.get(file):
# #                             self.logger.error(f"Base volume {file} changed after clone workload")

# #                     self.ssh_obj.delete_files(node=self.mgmt_nodes[0], files=test_files)
# #                     clone_files_after = self.ssh_obj.list_files(node=self.mgmt_nodes[0], directory=clone_mount_point)
# #                     clone_checksums_after = self.verify_checksums(clone_files_after)

# #                     for file, checksum in clone_checksums.items():
# #                         if checksum != clone_checksums_after.get(file):
# #                             self.logger.error(f"Checksum mismatch for {file} after workload")

# #                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=mount_point)
# #                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=clone_mount_point)


# import os
# import threading
# from e2e_tests.cluster_test_base import TestClusterBase
# from utils.common_utils import sleep_n_sec
# from logger_config import setup_logger


# class TestMultiLvolFio(TestClusterBase):
#     """
#     This script performs comprehensive testing of logical volume configurations, filesystem types, and workload sizes.
#     It covers a total of 48 test cases combining different configurations and workloads. The detailed combinations are:

#     1. Filesystem Types:
#        - ext4
#        - xfs

#     2. Configurations:
#        - 1+0
#        - 2+1
#        - 4+1
#        - 4+2
#        - 8+1
#        - 8+2

#     3. Workload Sizes:
#        - 5G
#        - 10G
#        - 20G
#        - 40G

#     The total number of test cases is calculated by multiplying the number of filesystem types, configurations, and workload sizes:
#     2 (Filesystem Types) * 6 (Configurations) * 4 (Workload Sizes) = 48 Test Cases
#     """

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.logger = setup_logger(__name__)

#     def run(self):
#         """ Performs each step of the test case """
#         self.logger.info("Inside run function")
#         initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

#         self.logger.info("Fetching cluster ID")
#         cluster_id = self.sbcli_utils.get_cluster_id()
#         self.logger.info(f"Cluster ID: {cluster_id}")

#         self.logger.info(f"Creating pool: {self.pool_name} with cluster ID: {cluster_id}")
#         self.sbcli_utils.add_storage_pool(
#             pool_name=self.pool_name
#         )

#         for fs_type in ["ext4", "xfs"]:
#             self.logger.info(f"Processing filesystem type: {fs_type}")

#             for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
#                 ndcs, npcs = config.split('+')
#                 lvol_name = f"lvol_{ndcs}_{npcs}"

#                 self.logger.info(f"Creating logical volume: {lvol_name} with ndcs: {ndcs} and npcs: {npcs}")
#                 self.sbcli_utils.add_lvol(
#                     lvol_name=lvol_name,
#                     pool_name=self.pool_name,
#                     size=self.lvol_size,
#                     ndcs=int(ndcs),
#                     npcs=int(npcs)
#                 )

#                 lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
#                 connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

#                 self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

#                 final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#                 disk_use = None
#                 self.logger.info("Initial vs final disk:")
#                 self.logger.info(f"Initial: {initial_devices}")
#                 self.logger.info(f"Final: {final_devices}")
#                 for device in final_devices:
#                     if device not in initial_devices:
#                         self.logger.info(f"Using disk: /dev/{device.strip()}")
#                         disk_use = f"/dev/{device.strip()}"
#                         break

#                 self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
#                 self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use, fs_type=fs_type)
#                 mount_point = f"{self.mount_path}/{lvol_name}"
#                 self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use, mount_path=mount_point)

#                 for size in ["5G", "10G", "20G", "40G"]:
#                     self.logger.info(f"Running fio workload with size: {size} on {mount_point}")

#                     fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                                   args=(self.mgmt_nodes[0], None, mount_point, self.log_path),
#                                                   kwargs={"name": f"fio_run_{size}",
#                                                           "size": size,
#                                                           "runtime": 360,
#                                                           "debug": self.fio_debug})
#                     fio_thread.start()
#                     fio_thread.join()

#                     self.logger.info(f"Creating snapshot for volume: {lvol_name}")
#                     snapshot_name = f"{lvol_name}_snapshot"
#                     self.sbcli_utils.add_snapshot(lvol_id=lvol_id, snapshot_name=snapshot_name)

#                     self.logger.info(f"Creating clone from snapshot: {snapshot_name}")
#                     snapshot_id = self.sbcli_utils.get_snapshot_id(snapshot_name=snapshot_name)
#                     clone_name = f"{snapshot_name}_clone"
#                     self.sbcli_utils.clone_snapshot(snapshot_id=snapshot_id, clone_name=clone_name)

#                     self.logger.info(f"Fetching clone logical volume ID for: {clone_name}")
#                     clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
#                     connect_str_clone = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)

#                     self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str_clone)

#                     final_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#                     disk_use_clone = None
#                     self.logger.info(f"Initial: {initial_devices}")
#                     self.logger.info(f"Final: {final_devices_clone}")
#                     for device in final_devices_clone:
#                         if device not in initial_devices:
#                             self.logger.info(f"Using disk: /dev/{device.strip()}")
#                             disk_use_clone = f"/dev/{device.strip()}"
#                             break

#                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)
#                     self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use_clone, fs_type=fs_type)
#                     mount_point_clone = f"{self.mount_path}/{clone_name}"
#                     self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use_clone, mount_path=mount_point_clone)

#                     self.logger.info(f"Running fio workload on clone mount point: {mount_point_clone}")
#                     fio_thread_clone = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                                         args=(self.mgmt_nodes[0], None, mount_point_clone, self.log_path),
#                                                         kwargs={"name": f"fio_clone_run_{size}",
#                                                                 "size": size,
#                                                                 "runtime": 360,
#                                                                 "debug": self.fio_debug})
#                     fio_thread_clone.start()
#                     fio_thread_clone.join()

#                     self.logger.info(f"Unmounting clone mount point: {mount_point_clone}")
#                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)

#                     self.logger.info(f"Disconnecting logical volume: {clone_id}")
#                     self.ssh_obj.disconnect_lvol(node=self.mgmt_nodes[0], lvol_id=clone_id)

#                     self.logger.info(f"Deleting clone logical volume: {clone_id}")
#                     self.sbcli_utils.delete_lvol(lvol_id=clone_id)

#         self.logger.info("Cleaning up")
#         self.cleanup()

#         self.logger.info("TEST CASE PASSED !!!")

#     def cleanup(self):
#         """ Perform cleanup steps """
#         self.logger.info("Starting cleanup process")
#         self.unmount_all()
#         self.remove_mount_dirs()
#         self.disconnect_lvols()
#         self.delete_snapshots()
#         self.delete_lvols()
#         self.delete_pool()
#         self.logger.info("Cleanup complete")

#     def unmount_all(self):
#         """ Unmount all mount points """
#         self.logger.info("Unmounting all mount points")
#         mount_points = self.ssh_obj.get_mount_points(node=self.mgmt_nodes[0], base_path=self.mount_path)
#         for mount_point in mount_points:
#             self.logger.info(f"Unmounting {mount_point}")
#             self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], mount_path=mount_point)

#     def remove_mount_dirs(self):
#         """ Remove all mount point directories """
#         self.logger.info("Removing all mount point directories")
#         mount_dirs = self.ssh_obj.get_mount_dirs(node=self.mgmt_nodes[0], base_path=self.mount_path)
#         for mount_dir in mount_dirs:
#             self.logger.info(f"Removing directory {mount_dir}")
#             self.ssh_obj.remove_dir(node=self.mgmt_nodes[0], dir_path=mount_dir)

#     def disconnect_lvols(self):
#         """ Disconnect all NVMe devices with NQN containing 'lvol' """
#         self.logger.info("Disconnecting all NVMe devices with NQN containing 'lvol'")
#         subsystems = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0], filter="lvol")
#         for subsys in subsystems:
#             self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
#             self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], subsys=subsys)

#     def delete_snapshots(self):
#         """ Delete all snapshots """
#         self.logger.info("Deleting all snapshots")
#         snapshots = self.sbcli_utils.list_snapshots()
#         for snapshot in snapshots:
#             self.logger.info(f"Deleting snapshot: {snapshot}")
#             self.sbcli_utils.delete_snapshot(snapshot_name=snapshot)

#     def delete_lvols(self):
#         """ Delete all logical volumes """
#         self.logger.info("Deleting all logical volumes, including clones")
#         lvols = self.sbcli_utils.list_lvols()
#         for lvol in lvols:
#             self.logger.info(f"Deleting logical volume: {lvol}")
#             self.sbcli_utils.delete_lvol(lvol_id=lvol)

#     def delete_pool(self):
#         """ Delete storage pool """
#         self.logger.info(f"Deleting pool: {self.pool_name}")
#         self.sbcli_utils.delete_storage_pool(pool_name=self.pool_name)


# import os
# import threading
# from e2e_tests.cluster_test_base import TestClusterBase
# from utils.common_utils import sleep_n_sec
# from logger_config import setup_logger


# class TestMultiLvolFio(TestClusterBase):
#     """
#     This script performs comprehensive testing of logical volume configurations, filesystem types, and workload sizes.
#     It covers a total of 48 test cases combining different configurations and workloads. The detailed combinations are:

#     1. Filesystem Types:
#        - ext4
#        - xfs

#     2. Configurations:
#        - 1+0
#        - 2+1
#        - 4+1
#        - 4+2
#        - 8+1
#        - 8+2

#     3. Workload Sizes:
#        - 5G
#        - 10G
#        - 20G
#        - 40G

#     The total number of test cases is calculated by multiplying the number of filesystem types, configurations, and workload sizes:
#     2 (Filesystem Types) * 6 (Configurations) * 4 (Workload Sizes) = 48 Test Cases
#     """

#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.logger = setup_logger(__name__)
#         self.lvol_size="100G"

#     def run(self):
#         """ Performs each step of the test case """
#         self.logger.info("Inside run function")

#         self.logger.info(f"Creating pool: {self.pool_name}")
#         self.sbcli_utils.add_storage_pool(
#             pool_name=self.pool_name
#         )

#         for fs_type in ["ext4", "xfs"]:
#             self.logger.info(f"Processing filesystem type: {fs_type}")

#             for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
#                 ndcs, npcs = config.split('+')
#                 lvol_name = f"lvol_{ndcs}_{npcs}"

#                 self.logger.info(f"Creating logical volume: {lvol_name} with ndcs: {ndcs} and npcs: {npcs}")
#                 self.sbcli_utils.add_lvol(
#                     lvol_name=lvol_name,
#                     pool_name=self.pool_name,
#                     size=self.lvol_size,
#                     distr_ndcs=int(ndcs),
#                     distr_npcs=int(npcs)
#                 )

#                 lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
#                 connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

#                 initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

#                 self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

#                 final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#                 disk_use = None
#                 self.logger.info("Initial vs final disk:")
#                 self.logger.info(f"Initial: {initial_devices}")
#                 self.logger.info(f"Final: {final_devices}")
#                 for device in final_devices:
#                     if device not in initial_devices:
#                         self.logger.info(f"Using disk: /dev/{device.strip()}")
#                         disk_use = f"/dev/{device.strip()}"
#                         break

#                 self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
#                 self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use, fs_type=fs_type)
#                 mount_point = f"{self.mount_path}/{lvol_name}"
#                 self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use, mount_path=mount_point)

#                 for size in ["5G", "10G", "20G", "40G"]:
#                     self.logger.info(f"Running fio workload with size: {size} on {mount_point}")

#                     fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                                   args=(self.mgmt_nodes[0], None, mount_point, self.log_path),
#                                                   kwargs={"name": f"fio_run_{size}",
#                                                           "size": size,
#                                                           "runtime": 200,
#                                                           "debug": self.fio_debug})
#                     fio_thread.start()
#                     self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
#                                                          threads=[fio_thread],
#                                                          timeout=400)

#                     self.logger.info(f"Creating snapshot for volume: {lvol_name}")
#                     snapshot_name = f"{lvol_name}_ss"
#                     self.ssh_obj.add_snapshot(node=self.mgmt_nodes[0],
#                                               lvol_id=lvol_id,
#                                               snapshot_name=snapshot_name)

#                     self.logger.info(f"Creating clone from snapshot: {snapshot_name}")
                    
#                     clone_name = f"{snapshot_name}_cl"
#                     snapshot_id = self.ssh_obj.get_snapshot_id(node=self.mgmt_nodes[0],
#                                                                snapshot_name=snapshot_name)
                    
#                     self.ssh_obj.add_clone(node=self.mgmt_nodes[0],
#                                            snapshot_id=snapshot_id,
#                                            clone_name=clone_name)

#                     self.logger.info(f"Fetching clone logical volume ID for: {clone_name}")
                    
#                     clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
#                     connect_str_clone = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
#                     initial_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

#                     self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str_clone)

#                     final_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
#                     disk_use_clone = None
#                     self.logger.info(f"Initial: {initial_devices_clone}")
#                     self.logger.info(f"Final: {final_devices_clone}")
#                     for device in final_devices_clone:
#                         if device not in initial_devices_clone:
#                             self.logger.info(f"Using disk: /dev/{device.strip()}")
#                             disk_use_clone = f"/dev/{device.strip()}"
#                             break

#                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)
#                     self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use_clone, fs_type=fs_type)
#                     mount_point_clone = f"{self.mount_path}/{clone_name}"
#                     self.ssh_obj.mount_path(node=self.mgmt_nodes[0], 
#                                             device=disk_use_clone,
#                                             mount_path=mount_point_clone)

#                     self.logger.info(f"Running fio workload on clone mount point: {mount_point_clone}")
#                     fio_thread_clone = threading.Thread(target=self.ssh_obj.run_fio_test,
#                                                         args=(self.mgmt_nodes[0], None, mount_point_clone, self.log_path),
#                                                         kwargs={"name": f"fio_clone_run_{size}",
#                                                                 "size": size,
#                                                                 "runtime": 200,
#                                                                 "debug": self.fio_debug})
#                     fio_thread_clone.start()
#                     self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
#                                                          threads=[fio_thread],
#                                                          timeout=400)
#                     fio_thread_clone.join()

#                     self.logger.info(f"Unmounting clone mount point: {mount_point_clone}")
#                     self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use_clone)

#                     self.logger.info(f"Disconnecting logical volume: {clone_id}")
#                     nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0],
#                                                                 nqn_filter=clone_id)
#                     for nqn in nqn_lvol:
#                         self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=clone_id)

#                     self.logger.info(f"Deleting clone logical volume: {clone_id}")
#                     self.sbcli_utils.delete_lvol(lvol_name=clone_name)

#         self.logger.info("Cleaning up")
#         self.cleanup()

#         self.logger.info("TEST CASE PASSED !!!")

#     def cleanup(self):
#         """ Perform cleanup steps """
#         self.logger.info("Starting cleanup process")
#         self.unmount_all()
#         self.remove_mount_dirs()
#         self.delete_snapshots()
#         self.logger.info("Cleanup complete")


import os
import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger

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

    def run(self):
        """Performs each step of the test case"""
        self.logger.info("Inside run function")

        self.logger.info(f"Creating pool: {self.pool_name}")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
            ndcs, npcs = config.split('+')
            lvol_name = f"lvol_{ndcs}_{npcs}"

            self.logger.info(f"Creating logical volume: {lvol_name} with ndcs: {ndcs} and npcs: {npcs}")
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                distr_ndcs=int(ndcs),
                distr_npcs=int(npcs)
            )
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

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

            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
        
        lvol_list = self.sbcli_utils.list_lvols()

        for fs_type in ["ext4", "xfs"]:
            self.logger.info(f"Processing filesystem type: {fs_type}")

            for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
                ndcs, npcs = config.split('+')
                lvol_name = f"lvol_{ndcs}_{npcs}"
                mount_point = f"{self.mount_path}/{lvol_name}"
                self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
                self.logger.info(f"Formating lvol: {lvol_name} with fs type: {fs_type} And moutning at {mount_point}")
                self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use, fs_type=fs_type)
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use, mount_path=mount_point)

            for size in ["5G", "10G", "20G", "40G"]:
                for config in ["1+0", "2+1", "4+1", "4+2", "8+1", "8+2"]:
                    ndcs, npcs = config.split('+')
                    lvol_name = f"lvol_{ndcs}_{npcs}"
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
                                                         timeout=400)
                    
                    # Generating checksums for base volume files
                    self.logger.info(f"Generating checksums for files in base volume: {mount_point}")
                    base_files = self.ssh_obj.find_files(node=self.mgmt_nodes[0], directory=mount_point)
                    base_checksums = self.ssh_obj.generate_checksums(node=self.mgmt_nodes[0], files=base_files)

                    self.logger.info(f"Creating snapshot for volume: {lvol_name}")
                    snapshot_name = f"{lvol_name}_{size}_ss"
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
                    connect_str_clone = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
                    initial_devices_clone = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

                    self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str_clone)

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
                                                         timeout=400)

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
                    self.ssh_obj.verify_checksums(node=self.mgmt_nodes[0], files=clone_files, checksums=base_checksums)

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
