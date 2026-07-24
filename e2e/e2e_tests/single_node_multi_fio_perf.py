from datetime import datetime
import os
from pathlib import Path
from e2e_tests.cluster_test_base import TestClusterBase
import threading
import json

class TestLvolFioBase(TestClusterBase):
    """
    Base class for handling common LVOL and FIO logic for different npcs configurations.
    Inherits from TestClusterBase, which handles setup and teardown.
    """

    def setup(self):
        """Call setup from TestClusterBase and then create the storage pool."""
        self.test_name = "single_node_fio_perf"

        super().setup()

        self.lvol_devices = {}
        self.fio_handles = {}

        if not self.k8s_test:
            pools = self.sbcli_utils.list_storage_pools()
            assert self.pool_name not in list(pools.keys()), \
                f"Pool {self.pool_name} present in list of pools post delete: {pools}"

        self._add_pool_dual(
            pool_name=self.pool_name,
            cluster_id=self.cluster_id,
        )

        self._verify_pool_exists_dual()

        if self.k8s_test:
            self._k8s_ensure_storage_class()

    def create_lvols(self, lvol_configs):
        """
        Create multiple LVOLs, connect them, and mount them
        based on the provided configurations.
        """
        self.logger.info("Creating LVOLs based on the provided configurations")

        for config in lvol_configs:
            lvol_name = config['lvol_name']
            ndcs = config.get('ndcs')
            npcs = config.get('npcs')

            self._create_lvol_dual(
                lvol_name=lvol_name,
                size=config['size'],
                pool_name=self.pool_name,
                ndcs=ndcs,
                npcs=npcs,
            )

            mount_path = None
            if config["mount"]:
                mount_path = f"{Path.home()}/test_location_{lvol_name}"

            device, mount = self._connect_and_mount_dual(
                lvol_name, mount_path=mount_path, format_disk=config["mount"]
            )

            self.lvol_devices[lvol_name] = {"Device": device, "MountPath": mount}

    def run_fio_on_lvol(self, lvol_name, mount_path=None, device=None, readwrite="randrw"):
        """Run FIO tests on a specific LVOL with the given readwrite operation."""
        self.logger.info(f"Starting FIO test on {lvol_name} with readwrite={readwrite}")
        handle = self._run_fio_dual(
            lvol_name=lvol_name,
            mount_path=mount_path,
            name=f"fio_{lvol_name}",
            rw=readwrite,
            iodepth=1,
            bs="4096",
            size="2G",
            time_based=True,
            runtime=300,
            nrfiles=5,
        )
        self.fio_handles[lvol_name] = handle
        return handle

    def validate_fio_output(self, lvol_name, read_check=False, write_check=False,
                            trim_check=False):
        """Validate the FIO output for IOPS and MB/s."""
        if self.k8s_test:
            handle = self.fio_handles.get(lvol_name)
            if handle:
                self._validate_fio_dual(handle)
            self.logger.info(f"[k8s] FIO validation passed for {lvol_name}")
            return

        log_file = f"{Path.home()}/{lvol_name}_log.json"
        output = self.ssh_obj.read_file(node=self.client_machines[0], file_name=log_file)
        fio_result = ""
        self.logger.info(f"FIO output for {lvol_name}: {output}")

        start_index = output.find('{')  # Find the first opening brace
        if start_index != -1:
            json_content = output[start_index:]  # Extract everything starting from the JSON
            try:
                self.logger.info(f"Removed str FIO output for {lvol_name}: {fio_result}")
                # Parse the extracted JSON
                fio_result = json.loads(json_content)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                return None
        else:
            print("No JSON content found in the file.")
            return None
        # fio_result = json.loads(output)
        self.logger.info(f"FIO output for {lvol_name}: {fio_result}")

        job = fio_result['jobs'][0]
        job_name = job['job options']['name']
        file_name = job['job options'].get("directory", job['job options'].get("filename", None))
        read_iops = job['read']['iops']
        write_iops = job['write']['iops']
        trim_iops = job['trim']['iops']
        total_iops = read_iops + write_iops + trim_iops
        disk_name = fio_result['disk_util'][0]['name']

        read_bw_kb = job['read']['bw']
        write_bw_kb = job['write']['bw']
        trim_bw_kb = job['trim']['bw']
        read_bw_mib = read_bw_kb / 1024
        write_bw_mib = write_bw_kb / 1024
        trim_bw_mib = trim_bw_kb / 1024

        # Write LVOL details to the text file
        with open(os.path.join("logs" ,"fio_test_results.log"),
                  "a", encoding="utf-8") as log_file:
            log_file.write(f"LVOL: {lvol_name}, Total IOPS: {total_iops}, Read BW: "
                           f"{read_bw_mib} MiB/s, Write BW: {write_bw_mib} MiB/s, "
                           f"Trim BW: {trim_bw_mib} MiB/s\n\n")

            self.logger.info(f"Performing validation for FIO job: {job_name} on device: "
                            f"{disk_name} mounted on: {file_name}")

        assert  total_iops != 0 , \
            f"Total IOPS {total_iops} can not be 0"

        if total_iops < 350:
            self.logger.warning(f"Total IOPS {total_iops} is leas than 350)")
        # TODO: Uncomment when issue is fixed
        # assert 4.5 < read_bw_mib < 5.5, f"Read BW {read_bw_mib} out of range (4.5-5.5 MiB/s)"
        # assert 4.5 < write_bw_mib < 5.5, f"Write BW {write_bw_mib} out of range (4.5-5.5 MiB/s)"
        if read_check:
            assert read_bw_mib > 0, f"Read BW {read_bw_mib} less than or equal to 0MiB"
        if write_check:
            assert write_bw_mib > 0, f"Write BW {write_bw_mib} less than or equal to 0MiB"
        if trim_check:
            assert trim_bw_mib > 0, f"Trim BW {trim_bw_mib} less than or equal to 0MiB"

    def cleanup_lvols(self, lvol_configs):
        """Unmount, remove directory, and delete LVOLs for cleanup."""
        if self.k8s_test:
            # K8s cleanup: FIO jobs cleaned up after wait, PVCs in teardown
            for config in lvol_configs:
                lvol_name = config['lvol_name']
                handle = self.fio_handles.get(lvol_name)
                if handle:
                    self._cleanup_fio_k8s(handle)
            self.logger.info("[k8s] Cleanup: FIO jobs deleted, PVCs handled by teardown")
            return

        self.logger.info("Starting cleanup of LVOLs")
        for config in lvol_configs:
            lvol_name = config['lvol_name']
            if config['mount']:
                self.ssh_obj.unmount_path(node=self.client_machines[0],
                                          device=self.lvol_devices[lvol_name]['MountPath'])
                self.ssh_obj.remove_dir(node=self.client_machines[0],
                                        dir_path=self.lvol_devices[lvol_name]['MountPath'])
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            subsystems = self.ssh_obj.get_nvme_subsystems(node=self.client_machines[0],
                                                          nqn_filter=lvol_id)
            for subsys in subsystems:
                self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                self.ssh_obj.disconnect_nvme(node=self.client_machines[0], nqn_grep=subsys)
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name)
        self.logger.info("Cleanup completed")

    def _run_scenario(self, lvol_configs):
        """Common scenario runner: create lvols, run FIO, wait, validate, cleanup."""
        self.create_lvols(lvol_configs)

        lvol_name_1 = lvol_configs[0]['lvol_name']
        lvol_name_2 = lvol_configs[1]['lvol_name']

        fio_handles = []
        fio_handles.append(self.run_fio_on_lvol(
            lvol_name_1,
            mount_path=self.lvol_devices[lvol_name_1]["MountPath"],
            readwrite="randrw"))
        fio_handles.append(self.run_fio_on_lvol(
            lvol_name_2,
            device=self.lvol_devices[lvol_name_2]["Device"],
            readwrite="write"))

        self._wait_fio_dual(fio_handles, timeout=600)

        if not self.k8s_test:
            for handle in fio_handles:
                if isinstance(handle, threading.Thread):
                    handle.join()

        self.validate_fio_output(lvol_name_1, read_check=True, write_check=True)
        self.validate_fio_output(lvol_name_2, read_check=False, write_check=True,
                                 trim_check=False)
        self.cleanup_lvols(lvol_configs)


class TestLvolFioNpcs0(TestLvolFioBase):
    """
    Test class for LVOLs with npcs=0 configuration.
    Inherits from TestLvolFioBase.
    """

    def run(self):
        """Test scenario for npcs=0 with ndcs=1."""
        scenarios = [
            {"npcs": 0, "ndcs": 1}
        ]

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs" ,"fio_test_results.log"),
                  "a", encoding="utf-8") as log_file:
            log_file.write(f"Date & Time: {current_time}\n")

        for scenario in scenarios:
            lvol_configs = [
                {"lvol_name": f"lvol1_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": True},
                {"lvol_name": f"lvol2_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": False}
            ]
            self._run_scenario(lvol_configs)
            self.logger.info(f"Test Passed with scenario npcs: {scenario['npcs']} "
                             f" and ndcs: {scenario['ndcs']}")
        self.logger.info(f"All Test Scenarios Passed with npcs: {scenario['npcs']}")


class TestLvolFioNpcs1(TestLvolFioBase):
    """
    Test class for LVOLs with npcs=1 configuration.
    Inherits from TestLvolFioBase.
    """

    def run(self):
        """Test scenario for npcs=1 with different ndcs."""
        scenarios = [
            {"npcs": 1, "ndcs": 1},
        ]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs" ,"fio_test_results.log"),
                  "a", encoding="utf-8") as log_file:
            log_file.write(f"Date & Time: {current_time}\n")

        for scenario in scenarios:
            lvol_configs = [
                {"lvol_name": f"lvol1_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": True},
                {"lvol_name": f"lvol2_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": False}
            ]
            self._run_scenario(lvol_configs)
            self.logger.info(f"Test Passed with scenario npcs: {scenario['npcs']} "
                             f" and ndcs: {scenario['ndcs']}")
        self.logger.info(f"All Test Scenarios Passed with npcs: {scenario['npcs']}")


class TestLvolFioNpcs2(TestLvolFioBase):
    """
    Test class for LVOLs with npcs=2 configuration.
    Inherits from TestLvolFioBase.
    """

    def run(self):
        """Test scenario for npcs=2 with different ndcs."""
        scenarios = [
            {"npcs": 2, "ndcs": 1},
        ]
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs" ,"fio_test_results.log"),
                  "a", encoding="utf-8") as log_file:
            log_file.write(f"Date & Time: {current_time}\n")

        for scenario in scenarios:
            lvol_configs = [
                {"lvol_name": f"lvol1_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": True},
                {"lvol_name": f"lvol2_npcs_{scenario['npcs']}_ndcs_{scenario['ndcs']}",
                 "ndcs": scenario['ndcs'], "npcs": scenario['npcs'],
                 "size": "4G", "mount": False}
            ]
            self._run_scenario(lvol_configs)
            self.logger.info(f"Test Passed with scenario npcs: {scenario['npcs']} "
                             f" and ndcs: {scenario['ndcs']}")
        self.logger.info(f"All Test Scenarios Passed with npcs: {scenario['npcs']}")

class TestLvolFioNpcsCustom(TestLvolFioBase):
    """
    Test class for LVOLs without requiring ndcs or npcs.
    Inherits from TestLvolFioBase.
    """

    def run(self):
        """Custom test scenario without requiring ndcs or npcs."""
        lvol_configs = [
            {"lvol_name": "lvol_custom_1", "size": "4G", "mount": True},
            {"lvol_name": "lvol_custom_2", "size": "4G", "mount": False}
        ]
        self._run_scenario(lvol_configs)
        self.logger.info("Test Case Passed.")
