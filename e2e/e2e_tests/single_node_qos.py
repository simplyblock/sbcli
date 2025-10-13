from datetime import datetime
import os
from pathlib import Path
from e2e_tests.cluster_test_base import TestClusterBase
import threading
import json
from utils.common_utils import sleep_n_sec

class TestLvolQOSBase(TestClusterBase):
    """
    Base class for handling common LVOL and FIO logic for different npcs configurations.
    Inherits from TestClusterBase, which handles setup and teardown.
    """

    def setup(self):
        """Call setup from TestClusterBase and then create the storage pool."""
        self.test_name = "single_node_qos_perf"
        
        super().setup()

        self.lvol_devices = {}
        self.mount_path = "/mnt/"

    def create_pools(self, bw=False):
        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name not in list(pools.keys()), \
            f"Pool {self.pool_name} present in list of pools post delete: {pools}"
        
        assert f"{self.pool_name}_qos" not in list(pools.keys()), \
            f"Pool {self.pool_name}_qos present in list of pools post delete: {pools}"

        if bw:
            self.sbcli_utils.add_storage_pool(
                pool_name=f"{self.pool_name}_qos",
                cluster_id=self.cluster_id,
                max_r_mbytes=10,
                max_w_mbytes=10
            )
        else:
            self.sbcli_utils.add_storage_pool(
                pool_name=f"{self.pool_name}_qos",
                cluster_id=self.cluster_id,
                max_rw_iops=3000
            )

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name,
            cluster_id=self.cluster_id,
        )

        pools = self.sbcli_utils.list_storage_pools()
        assert self.pool_name in list(pools.keys()), \
            f"Pool {self.pool_name} not present in list of pools post add: {pools}"
        
        assert f"{self.pool_name}_qos" in list(pools.keys()), \
            f"Pool {self.pool_name}_qos not present in list of pools post add: {pools}"
    
    def create_lvols(self, lvol_configs, pool_qos=False, bw=False):
        """
        Create multiple LVOLs, connect them, and mount them 
        based on the provided configurations.
        """
        self.logger.info("Creating LVOLs based on the provided configurations")

        for config in lvol_configs:
            lvol_name = config['lvol_name']
            if "ndcs" in lvol_configs:
                self.sbcli_utils.add_lvol(
                    lvol_name=lvol_name,
                    pool_name=self.pool_name,
                    size=config['size'],
                    distr_ndcs=config['ndcs'],
                    distr_npcs=config['npcs'],
                    distr_bs=4096,
                    distr_chunk_bs=4096,
                )
            else:
                # Create LVOL
                if pool_qos:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name,
                        pool_name=f"{self.pool_name}_qos",
                        size=config['size'],
                    )
                else:
                    if bw:
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name,
                            pool_name=self.pool_name,
                            size=config['size'],
                            max_r_mbytes=10,
                            max_w_mbytes=10
                        )
                    else:
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name,
                            pool_name=self.pool_name,
                            size=config['size'],
                            max_rw_iops=1000
                        )


            initial_devices = self.ssh_obj.get_devices(node=self.fio_node[0])

            # Get LVOL connection string
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.fio_node[0], command=connect_str)

            # Identify the newly connected device
            sleep_n_sec(10)
            final_devices = self.ssh_obj.get_devices(node=self.fio_node[0])
            disk_use = None

            for device in final_devices:
                if device not in initial_devices:
                    self.logger.info(f"Using disk: /dev/{device.strip()}")
                    disk_use = f"/dev/{device.strip()}"
                    break

            # Unmount, format, and mount the device
            self.ssh_obj.unmount_path(node=self.fio_node[0], device=disk_use)
            mount_path = None
            if config["mount"]:
                self.ssh_obj.format_disk(node=self.fio_node[0], device=disk_use)
                mount_path = f"{self.mount_path}/test_location_{lvol_name}"
                self.ssh_obj.mount_path(node=self.fio_node[0], device=disk_use, mount_path=mount_path)

            # Store device information
            self.lvol_devices[lvol_name] = {"Device": disk_use, "MountPath": mount_path}

    def run_fio_on_lvol(self, lvol_name, mount_path=None, device=None, readwrite="randrw"):
        """Run FIO tests on a specific LVOL with the given readwrite operation."""
        self.logger.info(f"Starting FIO test on {lvol_name} with readwrite={readwrite}")
        fio_thread = threading.Thread(
            target=self.ssh_obj.run_fio_test,
            args=(self.fio_node[0], device, mount_path, None),
            kwargs={
                "name": f"fio_{lvol_name}",
                "rw": readwrite,
                "ioengine": "libaio",
                "iodepth": 256,
                "bs": 4096,
                "size": "1G",
                "numjobs": 16,
                "time_based": True,
                "runtime": 100,
                "output_format": "json",
                "output_file": f"{self.log_path}/{lvol_name}_log.json",
                "nrfiles": 5,
                "debug": self.fio_debug
            }
        )
        fio_thread.start()
        return fio_thread

    # def validate_fio_output(self, pool_qos_lvols_config, lvol_qos_config, bw=False):
    #     """Validate the FIO output for IOPS and MB/s."""
    #     total_qos_iops = 0
    #     total_qos_read_bw = 0
    #     total_qos_write_bw = 0

    #     for lvol_configs in [pool_qos_lvols_config, lvol_qos_config]:
    #         for config in lvol_configs:
    #             lvol_name = config["lvol_name"]
    #             log_file = f"{self.log_path}/{lvol_name}_log.json"
    #             output = self.ssh_obj.read_file(node=self.fio_node[0], file_name=log_file)
    #             fio_result = ""
    #             self.logger.info(f"FIO output for {lvol_name}: {output}")

    #             start_index = output.find('{')  # Find the first opening brace
    #             if start_index != -1:
    #                 json_content = output[start_index:]  # Extract everything starting from the JSON
    #                 try:
    #                     self.logger.info(f"Removed str FIO output for {lvol_name}: {fio_result}")
    #                     # Parse the extracted JSON
    #                     fio_result = json.loads(json_content)
    #                 except json.JSONDecodeError as e:
    #                     print(f"Error decoding JSON: {e}")
    #                     return None
    #             else:
    #                 print("No JSON content found in the file.")
    #                 return None
    #             # fio_result = json.loads(output)
    #             self.logger.info(f"FIO output for {lvol_name}: {fio_result}")

    #             jobs = fio_result['jobs']
    #             i = 0
    #             for job in jobs:
    #                 job_name = job['job options']['name']
    #                 file_name = job['job options'].get("directory", job['job options'].get("filename", None))
    #                 read_iops = job['read']['iops']
    #                 write_iops = job['write']['iops']
    #                 total_iops = read_iops + write_iops
    #                 disk_name = fio_result['disk_util'][0]['name']

    #                 read_bw_kb = job['read']['bw']
    #                 write_bw_kb = job['write']['bw']
    #                 trim_bw_kb = job['trim']['bw']
    #                 read_bw_mib = read_bw_kb / 1024
    #                 write_bw_mib = write_bw_kb / 1024
    #                 trim_bw_mib = trim_bw_kb / 1024
    #                 total_qos_iops += total_iops
    #                 total_qos_read_bw += read_bw_mib
    #                 total_qos_write_bw += write_bw_mib

    #                 # Write LVOL details to the text file
    #                 with open(os.path.join("logs" ,"fio_test_results.log"),
    #                         "a", encoding="utf-8") as log_file:
    #                     log_file.write(f"LVOL: {lvol_name}, Job={i+1},  Total IOPS: {total_iops}, Read BW: "
    #                                 f"{read_bw_mib} MiB/s, Write BW: {write_bw_mib} MiB/s, "
    #                                 f"Trim BW: {trim_bw_mib} MiB/s\n\n")
    #                 i+=1
                    
    #         self.logger.info(f"Performing validation for FIO job: {job_name} on device: "
    #                         f"{disk_name} mounted on: {file_name}")
            
    #     with open(os.path.join("logs" ,"fio_test_results.log"),
    #               "a", encoding="utf-8") as log_file:
    #         log_file.write(f"Total QOS IOPS: {total_qos_iops}, Read BW: "
    #                        f"{total_qos_read_bw} MiB/s, Write BW: {total_qos_write_bw} MiB/s")

    #     if bw:
    #         assert 20 < total_qos_read_bw < 50, f"Read BW {total_qos_read_bw} out of range (20-50 MiB/s)"
    #         assert 20 < total_qos_write_bw < 50, f"Write BW {total_qos_write_bw} out of range (20-50 MiB/s)"
    #     else:
    #         assert  4000 < total_qos_iops < 6500 , \
    #             f"Total IOPS {total_qos_iops} can not be more than 6500, should not be less than 4000"

    def _first_complete_json(self, s):
        start = s.find('{')
        if start == -1:
            return None
        depth = 0
        for i, ch in enumerate(s[start:], start):
            if ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    return s[start:i+1]
        return None  # not complete yet

    def _read_complete_json(self, node, path, retries=15, sleep_s=0.2):
        raw = ""
        for _ in range(retries):
            raw = self.ssh_obj.read_file(node=node, file_name=path) or ""
            js = self._first_complete_json(raw)
            if js:
                try:
                    return json.loads(js)
                except json.JSONDecodeError:
                    pass
            sleep_n_sec(sleep_s)
        return None  # give up after retries

    def validate_fio_output(self, pool_qos_lvols_config, lvol_qos_config, bw=False):
        total_qos_iops = 0.0
        total_qos_read_bw = 0.0
        total_qos_write_bw = 0.0
        errors = []

        for cfg in [*(pool_qos_lvols_config or []), *(lvol_qos_config or [])]:
            lvol_name = cfg["lvol_name"]
            log_file = f"{self.log_path}/{lvol_name}_log.json"

            fio_result = self._read_complete_json(self.fio_node[0], log_file)
            if not fio_result:
                errors.append(f"{lvol_name}: JSON incomplete or invalid")
                continue

            jobs = fio_result.get("jobs") or []
            if not jobs:
                errors.append(f"{lvol_name}: no jobs in JSON")
                continue

            for idx, job in enumerate(jobs, 1):
                r_iops = float(job.get("read", {}).get("iops", 0))
                w_iops = float(job.get("write", {}).get("iops", 0))
                total_iops = r_iops + w_iops

                # use *_bytes to get exact MiB/s
                r_mib = int(job.get("read", {}).get("bw_bytes", 0)) / (1024**2)
                w_mib = int(job.get("write", {}).get("bw_bytes", 0)) / (1024**2)
                t_mib = int(job.get("trim", {}).get("bw_bytes", 0)) / (1024**2)

                total_qos_iops     += total_iops
                total_qos_read_bw  += r_mib
                total_qos_write_bw += w_mib

                with open(os.path.join("logs","fio_test_results.log"), "a", encoding="utf-8") as lf:
                    lf.write(
                        f"LVOL: {lvol_name}, Job={idx},  Total IOPS: {total_iops}, "
                        f"Read BW: {r_mib} MiB/s, Write BW: {w_mib} MiB/s, Trim BW: {t_mib} MiB/s\n\n"
                    )

        # Write totals and any warnings no matter what happened above
        with open(os.path.join("logs","fio_test_results.log"), "a", encoding="utf-8") as lf:
            lf.write(
                f"Total QOS IOPS: {total_qos_iops}, Read BW: {total_qos_read_bw} MiB/s, "
                f"Write BW: {total_qos_write_bw} MiB/s\n"
            )
            if errors:
                lf.write("WARNINGS: " + "; ".join(errors) + "\n")

        # Assertions come last so logging is not skipped
        if bw:
            assert 20 < total_qos_read_bw < 50,  f"Read BW {total_qos_read_bw} out of range (20-50 MiB/s)"
            assert 20 < total_qos_write_bw < 50, f"Write BW {total_qos_write_bw} out of range (20-50 MiB/s)"
        else:
            assert 4000 < total_qos_iops < 6500, f"Total IOPS {total_qos_iops} out of range (4000-6500)"

    def cleanup_lvols(self, lvol_configs):
        """Unmount, remove directory, and delete LVOLs for cleanup."""
        self.logger.info("Starting cleanup of LVOLs")
        for config in lvol_configs:
            lvol_name = config['lvol_name']
            self.ssh_obj.unmount_path(node=self.fio_node[0], 
                                      device=self.lvol_devices[lvol_name]['MountPath'])
            self.ssh_obj.remove_dir(node=self.fio_node[0], 
                                    dir_path=self.lvol_devices[lvol_name]['MountPath'])
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=lvol_name)
            subsystems = self.ssh_obj.get_nvme_subsystems(node=self.fio_node[0], 
                                                          nqn_filter=lvol_id)
            for subsys in subsystems:
                self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                self.ssh_obj.disconnect_nvme(node=self.fio_node[0], nqn_grep=subsys)
            self.sbcli_utils.delete_lvol(lvol_name=lvol_name)
        self.logger.info("Cleanup completed")


class TestLvolFioQOSBW(TestLvolQOSBase):
    """
    Test class for LVOLs with QOS setting from pool level and lvol level
    """

    def run(self):
        """Custom test scenario without requiring ndcs or npcs."""
        # Define custom test configurations that don't require ndcs or npcs
        pool_qos_lvol_configs = [
            {"lvol_name": "lvol_qos1", "size": "30G", "mount": True},
            {"lvol_name": "lvol_qos2", "size": "30G", "mount": True},
            {"lvol_name": "lvol_qos3", "size": "30G", "mount": True},
        ]

        lvol_configs = [
            {"lvol_name": "lvol_non_qos1", "size": "30G", "mount": True},
            {"lvol_name": "lvol_non_qos2", "size": "30G", "mount": True},
            {"lvol_name": "lvol_non_qos3", "size": "30G", "mount": True},
        ]

        self.create_pools(bw=True)
        
        # Create LVOLs
        self.create_lvols(pool_qos_lvol_configs, pool_qos=True)
        self.create_lvols(lvol_configs, bw=True)

        # Run FIO tests
        fio_threads = []
        for lvol_config in pool_qos_lvol_configs:
            fio_threads.append(self.run_fio_on_lvol(lvol_config["lvol_name"],
                                                    mount_path=self.lvol_devices[lvol_config["lvol_name"]]["MountPath"],
                                                    readwrite="randrw"))
        for lvol_config in lvol_configs:
            fio_threads.append(self.run_fio_on_lvol(lvol_config["lvol_name"],
                                                    mount_path=self.lvol_devices[lvol_config["lvol_name"]]["MountPath"],
                                                    readwrite="randrw"))

        self.common_utils.manage_fio_threads(
            node=self.fio_node[0], threads=fio_threads, timeout=600
        )

        for thread in fio_threads:
            thread.join()

        # Validate FIO outputs
        self.validate_fio_output(pool_qos_lvol_configs, lvol_configs, bw=True)

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} lvol list")
        
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} pool list")

        # Cleanup after running FIO
        self.cleanup_lvols(lvol_configs)
        self.cleanup_lvols(pool_qos_lvol_configs)

        self.logger.info("Test Case Passed.")

class TestLvolFioQOSIOPS(TestLvolQOSBase):
    """
    Test class for LVOLs with QOS setting from pool level and lvol level
    """

    def run(self):
        """Custom test scenario without requiring ndcs or npcs."""
        # Define custom test configurations that don't require ndcs or npcs
        pool_qos_lvol_configs = [
            {"lvol_name": "lvol_qos1", "size": "30G", "mount": True},
            {"lvol_name": "lvol_qos2", "size": "30G", "mount": True},
            {"lvol_name": "lvol_qos3", "size": "30G", "mount": True},
        ]

        lvol_configs = [
            {"lvol_name": "lvol_non_qos1", "size": "30G", "mount": True},
            {"lvol_name": "lvol_non_qos2", "size": "30G", "mount": True},
            {"lvol_name": "lvol_non_qos3", "size": "30G", "mount": True},
        ]
        
        self.create_pools(bw=False)
        # Create LVOLs
        self.create_lvols(pool_qos_lvol_configs, pool_qos=True)
        self.create_lvols(lvol_configs, bw=False)

        # Run FIO tests
        fio_threads = []
        for lvol_config in pool_qos_lvol_configs:
            fio_threads.append(self.run_fio_on_lvol(lvol_config["lvol_name"],
                                                    mount_path=self.lvol_devices[lvol_config["lvol_name"]]["MountPath"],
                                                    readwrite="randrw"))
        for lvol_config in lvol_configs:
            fio_threads.append(self.run_fio_on_lvol(lvol_config["lvol_name"],
                                                    mount_path=self.lvol_devices[lvol_config["lvol_name"]]["MountPath"],
                                                    readwrite="randrw"))

        self.common_utils.manage_fio_threads(
            node=self.fio_node[0], threads=fio_threads, timeout=1000
        )

        for thread in fio_threads:
            thread.join()

        # Validate FIO outputs
        self.validate_fio_output(pool_qos_lvol_configs, lvol_configs, bw=False)

        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} lvol list")
        
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=f"{self.base_cmd} pool list")

        # Cleanup after running FIO
        self.cleanup_lvols(lvol_configs)
        self.cleanup_lvols(pool_qos_lvol_configs)

        self.logger.info("Test Case Passed.")

