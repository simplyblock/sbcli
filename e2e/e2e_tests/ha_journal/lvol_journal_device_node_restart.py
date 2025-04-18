from pathlib import Path
import threading
import json
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
import traceback
from requests.exceptions import HTTPError


class LvolJournalManager:
    def __init__(self):
        self.sn_journal_map = {}

    def add_lvol_journals(self, sn_uuid, jm_names):
        """
        Adds the journal mapping for the sn.
        :param jm_names: List of journal names (1 primary and 2 secondary journals)
        """
        if len(jm_names) != 3:
            raise ValueError(f"Expected 3 journal entries, got {len(jm_names)}")

        self.sn_journal_map[sn_uuid] = {
            'primary_journal': [jm_names[0]],   # Primary jm_ entry
            'secondary_journal_1': [jm_names[1]],  # First remote_jm entry
            'secondary_journal_2': [jm_names[2]]   # Second remote_jm entry
        }

    def get_journal_info(self, sn_uuid):
        """
        Returns the journal information for a given sn.
        :param sn_uuid: Storage Node UUID
        :return: Dictionary with primary and secondary journals
        """
        return self.sn_journal_map.get(sn_uuid, None)

    def get_all_sn(self):
        """
        Returns all sn names with their journal mappings.
        :return: Dictionary with all sn journal mappings
        """
        return self.sn_journal_map
        

class TestDeviceNodeRestart(TestClusterBase):
    """
    This test automates the following steps:
    1. Create 4 lvols on the 1st node.
    2. Start fio workloads with different block sizes.
    3. Stop device on the 1st node and expect IO to continue (inducing errors).
    4. Restart the device on the 1st node and wait for temporary migration to complete.
    5. Stop device on the 2nd node, which contains the secondary journal for node 1, and expect IO to continue.
    6. Restart the device on the 2nd node and wait for temporary migration to complete.
    7. Forcefully stop the 3rd node, which contains the secondary journal for node 1, and expect IO to continue.
    8. Restart the 3rd node and wait for migration to complete.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.journal_manager = LvolJournalManager()
        self.lvol_sn_node = None
        self.mount_path = "/mnt"

    def run(self):
        self.logger.info("Starting test case: Device and Node Restart with Journals")

        # Step 1: Create 4 lvols on the 1st node
        self.logger.info("Creating 4 logical volumes on the 1st node.")
        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )
        
        self.lvol_sn_node = self.sbcli_utils.get_node_without_lvols()

        for i in range(4):
            lvol_name = f"test_lvol_{i+1}"
            self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="10G",
                                      host_id=self.lvol_sn_node)

        lvols = self.sbcli_utils.list_lvols()
        self.logger.info(f"Created lvols: {lvols}")

        # Step 2: Fetch and store journal info for each lvol
        self.fetch_and_store_journal_info()

        initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])

        # Step 4: Mount 3 devices for randrw, write, and read workloads
        self.logger.info("Mounting 3 devices for randrw, write, and read workloads and just connecting one for trim")
        lvol_fio_path = {}
        for i in range(4):
            disk_use = None
            lvol_name = f"test_lvol_{i+1}"
            lvol_fio_path[lvol_name] = {
                "mount_path": None,
                "disk": None
            }
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
                    disk_use = f"/dev/{device.strip()}"
                    break
            initial_devices = final_devices
            if i < 3:
                self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                          device=disk_use)
                self.ssh_obj.format_disk(node=self.mgmt_nodes[0],
                                         device=disk_use)
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                        device=disk_use,
                                        mount_path=f"/mnt/device_{i+1}")
                lvol_fio_path[lvol_name]["mount_path"] = f"/mnt/device_{i+1}"
                lvol_fio_path[lvol_name]["disk"] = disk_use
            else:
                lvol_fio_path[lvol_name]["disk"] = disk_use

        # Step 3: Start fio workloads on each lvol
        self.logger.info("Starting fio workloads on the logical volumes with different configurations.")
        fio_threads = []
        fio_configs = [("randrw", "4K"), ("read", "8K"), ("write", "16K"), ("randtrimwrite", "32K")]

        for i, (rw, bs) in enumerate(fio_configs):
            lvol_name = f"test_lvol_{i+1}"
            if i < 3:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], None, lvol_fio_path[lvol_name]["mount_path"], None),
                                kwargs={
                                    "name": f"fio_{lvol_name}",
                                    "rw": rw,
                                    "ioengine": "libaio",
                                    "iodepth": 1,
                                    "bs": bs,
                                    "size": "2G",
                                    "time_based": True,
                                    "runtime": 200,
                                    "output_file": f"{Path.home()}/{lvol_name}_log.json",
                                    "nrfiles": 5,
                                    "debug": self.fio_debug
                                }
                            )
            else:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], lvol_fio_path[lvol_name]["disk"], None, None),
                                kwargs={
                                    "name": f"fio_{lvol_name}",
                                    "rw": rw,
                                    "ioengine": "libaio",
                                    "iodepth": 1,
                                    "bs": bs,
                                    "size": "2G",
                                    "time_based": True,
                                    "runtime": 200,
                                    "output_file": f"{Path.home()}/{lvol_name}_log.json",
                                    "nrfiles": 5,
                                    "debug": self.fio_debug
                                }
                            )
            fio_threads.append(thread)
            thread.start()

        # Step 4: Stop and restart nodes based on the journal info
        self.stop_and_restart_based_on_journals()

        self.common_utils.manage_fio_threads(
            node=self.mgmt_nodes[0],
            threads=fio_threads,
            timeout=1000
        )

        # Wait for fio threads to complete
        for thread in fio_threads:
            thread.join()

        self.logger.info("Taking MD5 checksums of all fio test files.")
        fio_testfiles = []
        for i in range(3):
            lvol_name = f"test_lvol_{i+1}"
            mount_path = lvol_fio_path[lvol_name]["mount_path"]
            fio_testfiles += self.ssh_obj.find_files(self.mgmt_nodes[0], mount_path)
        initial_checksums = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], fio_testfiles)

        self.logger.info("Unmounting devices and disconnecting NVMe.")
        for i in range(3):
            lvol_name = f"test_lvol_{i+1}"
            self.ssh_obj.unmount_path(self.mgmt_nodes[0], lvol_fio_path[lvol_name]["mount_path"])
            sleep_n_sec(5)
        nvme_subs = self.ssh_obj.get_nvme_subsystems(self.mgmt_nodes[0], "lvol")
        for nvme in nvme_subs:
            self.ssh_obj.disconnect_nvme(self.mgmt_nodes[0], nvme)
            sleep_n_sec(20)

        # Step 8: Gracefully shutdown node 1
        self.logger.info("Gracefully shutting down node 1.")
        self.sbcli_utils.suspend_node(self.lvol_sn_node)
        sleep_n_sec(10)
        self.sbcli_utils.shutdown_node(self.lvol_sn_node)
        sleep_n_sec(30)

        # Step 9: Restart node 1 and reconnect NVMe devices
        self.logger.info("Restarting node 1 and reconnecting NVMe devices.")
        self.sbcli_utils.restart_node(self.lvol_sn_node)
        sleep_n_sec(60)

        self.logger.info(f"Waiting for node to become online, {self.lvol_sn_node}")
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_sn_node,
                                                      "online",
                                                      timeout=300)

        for i in range(4):
            lvol_name = f"test_lvol_{i+1}"
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)
            sleep_n_sec(10)
            if i < 3:
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                        device=lvol_fio_path[lvol_name]["disk"],
                                        mount_path=lvol_fio_path[lvol_name]["mount_path"])
                sleep_n_sec(10)
                
        # Step 11: List the fio test files and compare MD5 checksums
        self.logger.info("Listing fio test files and comparing MD5 checksums.")
        print(f"Initial_Checksums: {initial_checksums}")
        remounted_testfiles = []
        for i in range(3):
            lvol_name = f"test_lvol_{i+1}"
            mount_path = lvol_fio_path[lvol_name]["mount_path"]
            remounted_testfiles += self.ssh_obj.find_files(self.mgmt_nodes[0], mount_path)
        print(f"Remounted Files: {remounted_testfiles}")

        self.ssh_obj.verify_checksums(self.mgmt_nodes[0], remounted_testfiles, initial_checksums)

        # Step 12: Delete two test files and create one new file, take another MD5 checksum
        self.logger.info("Deleting two files and creating one new test file.")
        self.ssh_obj.delete_files(self.mgmt_nodes[0], remounted_testfiles[:2])
        self.ssh_obj.exec_command(self.mgmt_nodes[0], 
                                  f"sudo dd if=/dev/zero of={lvol_fio_path['test_lvol_1']['mount_path']}/new_testfile bs=1M count=10")

        remounted_testfiles[1] = f'{lvol_fio_path["test_lvol_1"]["mount_path"]}/new_testfile'
        remounted_testfiles = remounted_testfiles[1:]

        self.logger.info("Taking new MD5 checksums after file manipulations.")
        updated_checksums = self.ssh_obj.generate_checksums(self.mgmt_nodes[0], remounted_testfiles)

        for i in range(3):
            lvol_name = f"test_lvol_{i+1}"
            self.ssh_obj.unmount_path(self.mgmt_nodes[0], lvol_fio_path[lvol_name]["mount_path"])
            sleep_n_sec(10)
        nvme_subs = self.ssh_obj.get_nvme_subsystems(self.mgmt_nodes[0], "lvol")
        for nvme in nvme_subs:
            self.ssh_obj.disconnect_nvme(self.mgmt_nodes[0], nvme)
            sleep_n_sec(10)


        # Step 13: Ungracefully stop node 1 (container shutdown)
        self.logger.info("Stopping container on node 1 (ungraceful shutdown).")
        node_ip = self.journal_manager.sn_journal_map[self.lvol_sn_node]['primary_journal'][1]
        self.ssh_obj.stop_spdk_process(node_ip)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_sn_node,
                                                      "unreachable",
                                                      timeout=300)

        sleep_n_sec(420)

        self.sbcli_utils.restart_node(node_uuid=self.lvol_sn_node)

        sleep_n_sec(420)

        self.sbcli_utils.restart_node(node_uuid=self.lvol_sn_node)

        # Step 14: Restart node 1, reconnect NVMe devices, and re-mount
        self.logger.info(f"Waiting for node to become online, {self.lvol_sn_node}")
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_sn_node,
                                                      "online",
                                                      timeout=300)

        sleep_n_sec(30)
        
        for i in range(4):
            lvol_name = f"test_lvol_{i+1}"
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)
            if i < 3:
                self.ssh_obj.mount_path(node=self.mgmt_nodes[0],
                                        device=lvol_fio_path[lvol_name]["disk"],
                                        mount_path=lvol_fio_path[lvol_name]["mount_path"])
                sleep_n_sec(10)

        # Step 15: List the fio test files and compare MD5 checksums again
        self.logger.info("Listing fio test files and comparing MD5 checksums after ungraceful restart.")
        remounted_testfiles = []
        for i in range(3):
            lvol_name = f"test_lvol_{i+1}"
            mount_path = lvol_fio_path[lvol_name]["mount_path"]
            remounted_testfiles += self.ssh_obj.find_files(self.mgmt_nodes[0], mount_path)
        self.ssh_obj.verify_checksums(self.mgmt_nodes[0], remounted_testfiles, updated_checksums)

        # Validate fio results
        self.logger.info("Validating fio test results.")
        for i in range(4):
            lvol_name = f"test_lvol_{i+1}"
            output_file = f"{Path.home()}/{lvol_name}_log.json",
            self.common_utils.validate_fio_test(self.mgmt_nodes[0], output_file)
        self.logger.info("Test case passed.")
    
    def fetch_and_store_journal_info(self):
        """
        Fetches lvol details from the API and stores the journal mapping.
        """
        sn_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_sn_node)[0]
        self.logger.info(f"Storage node details: {sn_details} ")
        jm_names = sn_details["lvstore_stack"][0]["params"]["jm_names"]
        print(f"JM_NAMES:{jm_names}")
        print(f'Lvol stack: {sn_details["lvstore_stack"][0]}')
        for i, jm_name in enumerate(jm_names):
            if "n1" == jm_name[-2:]:
                jm_names[i] = jm_name[:len(jm_names[i])-2]
        print(f"JM_NAMES:{jm_names}")
        self.journal_manager.add_lvol_journals(self.lvol_sn_node, jm_names)

        print(f'JM DETAILS: {self.journal_manager.sn_journal_map}')

        primary_ip = self.get_node_ip(self.extract_node_from_journal(jm_names[0]))
        secondary_ip1 = self.get_node_ip(self.extract_node_from_journal(jm_names[1]))
        secondary_ip2 = self.get_node_ip(self.extract_node_from_journal(jm_names[2]))

        self.journal_manager.sn_journal_map[self.lvol_sn_node]["primary_journal"].append(primary_ip)
        self.journal_manager.sn_journal_map[self.lvol_sn_node]["secondary_journal_1"].append(secondary_ip1)
        self.journal_manager.sn_journal_map[self.lvol_sn_node]["secondary_journal_2"].append(secondary_ip2)
        
        # Log the journal map for all lvols
        self.logger.info(f"Lvol Journal Map: {self.journal_manager.get_all_sn()}")

    def stop_and_restart_based_on_journals(self):
        """
        Stops and restarts nodes based on the journal mapping for each lvol.
        """
        for sn_name, journal_info in self.journal_manager.get_all_sn().items():
            primary_journal = journal_info['primary_journal']
            secondary_journal_1 = journal_info['secondary_journal_1']
            secondary_journal_2 = journal_info['secondary_journal_2']

            self.logger.info(f"Handling Storage Node: {sn_name}")
            self.logger.info(f"Primary Journal: {primary_journal}")
            self.logger.info(f"Secondary Journal 1: {secondary_journal_1}")
            self.logger.info(f"Secondary Journal 2: {secondary_journal_2}")

            # Step 1: Stop device on the primary node
            primary_node = self.extract_node_from_journal(primary_journal[0])
            self.logger.info(f"Stopping device on node: {primary_node} (primary journal)")
            self.ssh_obj.remove_jm_device(self.mgmt_nodes[0], primary_node)
            sleep_n_sec(15)

            # Step 2: Restart the primary node and wait for migration
            self.logger.info(f"Restarting device on node: {primary_node}")
            self.ssh_obj.restart_jm_device(self.mgmt_nodes[0], primary_node)
            sleep_n_sec(30)

            # Step 3: Stop device on the secondary journal 1 node
            secondary_node_1 = self.extract_node_from_journal(secondary_journal_1[0])
            self.logger.info(f"Stopping device on node: {secondary_node_1} (secondary journal 1)")
            self.ssh_obj.remove_jm_device(self.mgmt_nodes[0], secondary_node_1)
            sleep_n_sec(15)

            # Step 2: Restart the secondary JM and wait for migration
            self.logger.info(f"Restarting device on node: {secondary_node_1}")
            self.ssh_obj.restart_jm_device(self.mgmt_nodes[0], secondary_node_1)
            sleep_n_sec(30)

            # Step 5: Force stop the node with secondary journal 2
            secondary_node_2 = self.extract_node_from_journal(secondary_journal_2[0])
            self.logger.info(f"Forcefully stopping node: {secondary_node_2} (secondary journal 2)")
            self.ssh_obj.stop_spdk_process(node=secondary_journal_2[1])
            sleep_n_sec(420)

            self.sbcli_utils.restart_node(node_uuid=secondary_node_2)

            self.logger.info(f"Waiting for node to become online, {secondary_node_2}")
            self.sbcli_utils.wait_for_storage_node_status(secondary_node_2,
                                                          "online",
                                                          timeout=300)

            sleep_n_sec(30)

    def extract_node_from_journal(self, journal_name):
        """
        Extract the node from the journal name.
        For example: 'remote_jm_<node_id>' or 'jm_<node_id>'
        """
        return journal_name.split("_")[-1]  # Extract node ID from the journal name
