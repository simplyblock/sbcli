### simplyblock e2e tests

import threading
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger


class TestSingleNodeResizeLvolCone(TestClusterBase):
    """
    Steps:
    1. Create Storage Pool and Delete Storage pool
    2. Create storage pool
    3. Create 5 LVOLs
    4. Connect LVOLs
    5. Mount Devices
    6. Start FIO tests
    7. While FIO is running, validate this scenario:
        a. create snapshot clones, connect, run fio
        b. perform resize on lvols and clones for 10 times per lvol and clone
    8. Wait for fio completion.
    9. Check fio logs for errors.
    10. Get checksums from lvol and clone files.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.snapshot_name = "snapshot"
        self.logger = setup_logger(__name__)
        self.test_name = "single_node_outage"

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info("Inside run function")

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name in list(pools.keys()), \
            f"Pool {self.pool_name} not present in list of pools: {pools}"
        sleep_n_sec(10)
        self.sbcli_utils.delete_storage_pool(
            pool_name=self.pool_name
        )
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name not in list(pools.keys()), \
            f"Pool {self.pool_name} present in list of pools post delete: {pools}"

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        fio_threads = []

        lvol_size = 5

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            mount_path = f"{self.mount_path}_{i}"
            log_path = f"{self.log_path}_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size="5G"
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} is not present in list of lvols post add: {lvols}"

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
                                    mount_path=mount_path)

            fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.mgmt_nodes[0], None, mount_path, log_path,),
                                        kwargs={"name": f"fio_run_{i}",
                                                "runtime": 350,
                                                "debug": self.fio_debug})
            fio_thread.start()
            fio_threads.append(fio_thread)

        
        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            snap_name = f"{lvol_name}_snap"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_cl_{i}"
            log_path = f"{self.log_path}_cl_{i}"
            self.logger.info("Taking snapshot")
            self.sbcli_utils.add_snapshot(
                lvol_id=self.sbcli_utils.get_lvol_id(lvol_name),
                snapshot_name=snap_name
            )
            sleep_n_sec(5)
            snapshot_id = self.sbcli_utils.get_snapshot_id(snap_name=snap_name)
            
            self.sbcli_utils.add_clone(snapshot_id=snapshot_id, clone_name=clone_name)
            
            clone = self.sbcli_utils.list_lvols()
            assert clone_name in list(clone.keys()), \
                f"Clone {clone_name} is not present in list of lvols post add: {lvols}"
            
            initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
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
            self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                    device=disk_use,
                                    mount_path=mount_path)
            
            fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.mgmt_nodes[0], None, mount_path, log_path,),
                                        kwargs={"name": f"fio_run_cl_{i}",
                                                "runtime": 350,
                                                "debug": self.fio_debug})
            fio_thread.start()
            fio_threads.append(fio_thread)

        for node in self.storage_nodes:
            files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
            self.logger.info(f"Files in /etc/simplyblock: {files}")
            if "core" in files and "tmp_cores" not in files:
                raise Exception("Core file present! Not starting resize!!")
            
        for i in range(1, 11):
            for j in range(1, 6):
                lvol_name = f"{self.lvol_name}_{j}"
                clone_name = f"{lvol_name}_clone"
                self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(lvol_name),
                                            new_size=f"{lvol_size + i}G")
                self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(clone_name),
                                            new_size=f"{lvol_size + i}G")
                
                for node in self.storage_nodes:
                    files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                    self.logger.info(f"Files in /etc/simplyblock: {files}")
                    if "core" in files and "tmp_cores" not in files:
                        raise Exception("Core file present! Not continuing resize!!")
            
        lvol_size = lvol_size + 20
        
        self.common_utils.manage_fio_threads(node=self.mgmt_nodes[0],
                                             threads=fio_threads,
                                             timeout=1000)
        
        for i in range(1,6):
            log_path = f"{self.log_path}_{i}"
            cl_log_path = f"{self.log_path}_cl_{i}"
            self.common_utils.validate_fio_test(node=self.mgmt_nodes,log_file=log_path)
            self.common_utils.validate_fio_test(node=self.mgmt_nodes,log_file=cl_log_path)
        
        lvol_check_sum = {}
        
        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_{i}"
            cl_mount_path = f"{self.mount_path}_cl_{i}"
            
            lvol_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=mount_path)
            original_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], lvol_files)

            clone_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=cl_mount_path)
            cl_original_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], clone_files)

            lvol_check_sum[lvol_name] = original_checksum
            lvol_check_sum[clone_name] = cl_original_checksum

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_{i}"
            cl_mount_path = f"{self.mount_path}_cl_{i}"
            self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(lvol_name),
                                        new_size=f"{lvol_size}G")
            self.sbcli_utils.resize_lvol(lvol_id=self.sbcli_utils.get_lvol_id(clone_name),
                                        new_size=f"{lvol_size}G")
            
            lvol_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=mount_path)
            final_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], lvol_files)

            clone_files = self.ssh_obj.find_files(self.mgmt_nodes[0], directory=cl_mount_path)
            cl_final_checksum = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], clone_files)

            original_checksum = lvol_check_sum[lvol_name]
            cl_original_checksum = lvol_check_sum[clone_name]

            self.logger.info(f"Original checksum: {original_checksum}")
            self.logger.info(f"Final checksum: {final_checksum}")
            original_checksum = set(original_checksum.values())
            final_checksum = set(final_checksum.values())

            self.logger.info(f"Set Original checksum: {original_checksum}")
            self.logger.info(f"Set Final checksum: {final_checksum}")

            assert original_checksum == final_checksum, "Checksum mismatch for lvol after resize!!"

            self.logger.info(f"Clone Original checksum: {cl_original_checksum}")
            self.logger.info(f"Clone Final checksum: {cl_final_checksum}")
            cl_original_checksum = set(cl_original_checksum.values())
            cl_final_checksum = set(cl_final_checksum.values())

            self.logger.info(f"Set Clone Original checksum: {cl_original_checksum}")
            self.logger.info(f"Set Clone Final checksum: {cl_final_checksum}")

            assert cl_original_checksum == cl_final_checksum, "Checksum mismatch for clone after resize!!"

        self.logger.info("TEST CASE PASSED !!!")
