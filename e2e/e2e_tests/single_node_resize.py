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
        self.test_name = "single_node_resize"

    def run(self):
        """ Performs each step of the testcase
        """
        self.logger.info("Inside run function")

        self._add_pool_dual(pool_name=self.pool_name)
        self._verify_pool_exists_dual()

        if not self.k8s_test:
            sleep_n_sec(10)
            self.sbcli_utils.delete_storage_pool(
                pool_name=self.pool_name
            )
            pools = self.sbcli_utils.list_storage_pools()
            assert self.pool_name not in list(pools.keys()), \
                f"Pool {self.pool_name} present in list of pools post delete: {pools}"
            self._add_pool_dual(pool_name=self.pool_name)

        if self.k8s_test:
            self._k8s_ensure_storage_class()

        fio_handles = []

        lvol_size = 5
        node_id = self.sbcli_utils.get_node_without_lvols()

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            mount_path = f"{self.mount_path}_{i}"
            log_path = f"{self.log_path}_{i}"

            self._create_lvol_dual(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size="5G",
                host_id=node_id,
            )
            lvols = self.sbcli_utils.list_lvols()
            assert lvol_name in list(lvols.keys()), \
                f"Lvol {lvol_name} is not present in list of lvols post add: {lvols}"

            device, mount = self._connect_and_mount_dual(
                lvol_name, mount_path=mount_path
            )

            fio_handle = self._run_fio_dual(
                lvol_name=lvol_name,
                mount_path=mount if not self.k8s_test else None,
                log_path=log_path if not self.k8s_test else None,
                name=f"fio_run_{i}",
                runtime=350,
                time_based=True,
            )
            fio_handles.append(fio_handle)

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            snap_name = f"{lvol_name}_snap"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_cl_{i}"
            log_path = f"{self.log_path}_cl_{i}"

            self.logger.info("Taking snapshot")
            snapshot_id = self._create_snapshot_dual(lvol_name, snap_name)
            sleep_n_sec(5)

            _, cl_mount = self._create_clone_dual(
                snapshot_id=snapshot_id,
                clone_name=clone_name,
                mount_path=mount_path if not self.k8s_test else None,
                format_disk=False,
            )

            clone = self.sbcli_utils.list_lvols()
            assert clone_name in list(clone.keys()), \
                f"Clone {clone_name} is not present in list of lvols post add: {clone}"

            fio_handle = self._run_fio_dual(
                lvol_name=clone_name,
                mount_path=cl_mount if not self.k8s_test else None,
                log_path=log_path if not self.k8s_test else None,
                name=f"fio_run_cl_{i}",
                runtime=350,
                time_based=True,
            )
            fio_handles.append(fio_handle)

        if not self.k8s_test:
            for node in self.storage_nodes:
                files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                self.logger.info(f"Files in /etc/simplyblock: {files}")
                if "core.react" in files:
                    raise Exception("Core file present! Not starting resize!!")

        for i in range(1, 11):
            for j in range(1, 6):
                lvol_name = f"{self.lvol_name}_{j}"
                clone_name = f"{lvol_name}_clone"
                self._resize_lvol_dual(lvol_name, f"{lvol_size + i}G")
                sleep_n_sec(10)
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                        self.logger.info(f"Files in /etc/simplyblock: {files}")
                        if "core.react" in files:
                            raise Exception("Core file present after lvol resize! Not continuing resize!!")

                self._resize_lvol_dual(clone_name, f"{lvol_size + i}G")
                sleep_n_sec(10)
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                        self.logger.info(f"Files in /etc/simplyblock: {files}")
                        if "core.react" in files:
                            raise Exception("Core file present after clone resize! Not continuing resize!!")

        lvol_size = lvol_size + 20

        self._wait_fio_dual(fio_handles, timeout=1000)
        if not self.k8s_test:
            for h in fio_handles:
                if isinstance(h, threading.Thread):
                    h.join()

        for i in range(1, 6):
            lvol_handle = fio_handles[i - 1] if (i - 1) < len(fio_handles) else None
            cl_handle = fio_handles[i + 4] if (i + 4) < len(fio_handles) else None
            if lvol_handle:
                self._validate_fio_dual(lvol_handle, log_path=f"{self.log_path}_{i}")
            if cl_handle:
                self._validate_fio_dual(cl_handle, log_path=f"{self.log_path}_cl_{i}")

        lvol_check_sum = {}

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_{i}"
            cl_mount_path = f"{self.mount_path}_cl_{i}"

            original_checksum = self._generate_checksums_dual(
                lvol_name,
                directory=mount_path if not self.k8s_test else None,
            )
            cl_original_checksum = self._generate_checksums_dual(
                clone_name,
                directory=cl_mount_path if not self.k8s_test else None,
            )

            lvol_check_sum[lvol_name] = original_checksum
            lvol_check_sum[clone_name] = cl_original_checksum

        for i in range(1, 6):
            lvol_name = f"{self.lvol_name}_{i}"
            clone_name = f"{lvol_name}_clone"
            mount_path = f"{self.mount_path}_{i}"
            cl_mount_path = f"{self.mount_path}_cl_{i}"
            self._resize_lvol_dual(lvol_name, f"{lvol_size}G")
            sleep_n_sec(10)
            if not self.k8s_test:
                for node in self.storage_nodes:
                    files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                    self.logger.info(f"Files in /etc/simplyblock: {files}")
                    if "core.react" in files:
                        raise Exception("Core file present after lvol resize! Not continuing resize!!")
            self._resize_lvol_dual(clone_name, f"{lvol_size}G")
            sleep_n_sec(10)
            if not self.k8s_test:
                for node in self.storage_nodes:
                    files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
                    self.logger.info(f"Files in /etc/simplyblock: {files}")
                    if "core.react" in files:
                        raise Exception("Core file present after clone resize! Not continuing resize!!")

            final_checksum = self._generate_checksums_dual(
                lvol_name,
                directory=mount_path if not self.k8s_test else None,
            )
            cl_final_checksum = self._generate_checksums_dual(
                clone_name,
                directory=cl_mount_path if not self.k8s_test else None,
            )

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
