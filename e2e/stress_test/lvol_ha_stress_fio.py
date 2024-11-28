import random
import threading
from utils.common_utils import sleep_n_sec
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from logger_config import setup_logger
from datetime import datetime


class TestLvolHACluster(FioWorkloadTest):
    """
    High-volume stress test for a 3-node cluster.
    Operations:
    - Create 500 lvols (mix of crypto and non-crypto) on a single node.
    - Create 3000 snapshots.
    - Fill volumes to about 9 TiB.
    - Run FIO for storage.
    - Handle graceful shutdown, container stop, and network stop.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_size = "25G"
        self.fio_size = "18GiB"
        self.total_lvols = 10
        self.total_snapshots = 30
        self.lvol_name = "lvol"
        self.snapshot_name = "snapshot"
        self.node = None
        self.lvol_node = None
        self.mount_path = "/mnt/"
        self.lvol_mount_details = {}
        self.log_path = "/home/ec2-user/"
    
    def create_lvols(self):
        """Create 500 lvols with mixed crypto and non-crypto."""
        self.logger.info("Creating 500 lvols.")
        for i in range(1, self.total_lvols + 1):
            fs_type = random.choice(["xfs", "ext4"])
            is_crypto = random.choice([False, False])
            lvol_name = f"{self.lvol_name}_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=is_crypto,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
                host_id=self.lvol_node
            )
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            self.lvol_mount_details[lvol_id] = {
                   "Name": lvol_name,
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{lvol_name}.log"
            }
            self.lvol_mount_details[lvol_id]["Command"] = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            initial_devices = self.ssh_obj.get_devices(node=self.node)
            self.ssh_obj.exec_command(node=self.node, command=self.lvol_mount_details[lvol_id]["Command"])
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=self.node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise Exception("LVOL did not connect")
            self.lvol_mount_details[lvol_id]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=self.node, device=lvol_device, fs_type=fs_type)

            # Mount and Run FIO
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=self.node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_id]["Mount"] = mount_point
            
        self.logger.info("Completed lvol creation.")

    def create_snapshots(self):
        """Create 3000 snapshots for existing lvols."""
        self.logger.info("Creating 3000 snapshots.")
        for idx, lvol_id in enumerate(list(self.lvol_mount_details.keys())):
            if idx >= self.total_snapshots:
                break
            snapshot_name = f"{self.snapshot_name}_{idx + 1}"
            self.ssh_obj.add_snapshot(node=self.node, lvol_id=lvol_id, snapshot_name=snapshot_name)
        self.logger.info("Snapshots created.")

    def fill_volumes(self):
        """Fill lvols with data."""
        self.logger.info("Filling volumes with data.")
        fio_threads = []
        for _, lvol in self.lvol_mount_details.items():
            fio_thread = threading.Thread(target=self.ssh_obj.run_fio_test, args=(self.node, None, lvol["Mount"], lvol["Log"]),
                                          kwargs={"size": self.fio_size,
                                                  "name": f"{lvol['Name']}_fio",
                                                  "rw": "write",
                                                  "bs": "4K-128K",})
            fio_thread.start()
            fio_threads.append(fio_thread)
        self.common_utils.manage_fio_threads(node=self.node,
                                             threads=fio_threads,
                                             timeout=10000)
        for thread in fio_threads:
            thread.join()
        self.logger.info("Data filling completed.")
    
    def calculate_md5(self):
        "Calculate Checksums"
        for lvol_id, lvol in self.lvol_mount_details.items():
            self.logger.info(f"Generating checksums for files in base volume: {lvol['Mount']}")
            base_files = self.ssh_obj.find_files(node=self.node, directory=lvol['Mount'])
            base_checksums = self.ssh_obj.generate_checksums(node=self.node, files=base_files)
            self.logger.info(f"Base Checksum for lvol {lvol['Name']}: {base_checksums}")
            self.lvol_mount_details[lvol_id]["MD5"] = base_checksums
    
    def validate_checksums(self):
        "Validating checksums"
        for lvol_id, lvol in self.lvol_mount_details.items():
            self.ssh_obj.unmount_path(node=self.node, device=lvol["Mount"])
            # filter_nqn = self.ssh_obj.get_nvme_subsystems(node=self.node, nqn_filter=lvol_id)
            # for nqn in filter_nqn:
            #     self.ssh_obj.disconnect_nvme(node=self.node, nqn_grep=nqn)
            # self.ssh_obj.remove_dir(node=self.node, dir_path=lvol["Mount"])
        
        for lvol_id, lvol in self.lvol_mount_details.items():
            # initial_devices = self.ssh_obj.get_devices(node=self.node)
            # self.ssh_obj.exec_command(node=self.node, command=lvol["Command"])
            # sleep_n_sec(3)
            # final_devices = self.ssh_obj.get_devices(node=self.node)
            # lvol_device = None
            # for device in final_devices:
            #     if device not in initial_devices:
            #         lvol_device = f"/dev/{device.strip()}"
            #         break
            # if not lvol_device:
            #     raise Exception("LVOL did not connect")
            # self.lvol_mount_details[lvol_id]["Device"] = lvol_device

            # Mount and Run FIO
            device = lvol["Device"][0:-1] + str(int(lvol["Device"][-1]) + 1)
            final_devices = self.ssh_obj.get_devices(node=self.node)
            lvol_device = None
            found_device = False
            for cur_device in final_devices:
                lvol_device = f"/dev/{cur_device.strip()}"
                if lvol_device == device:
                    found_device = True
                    break
            if found_device:
                self.lvol_mount_details[lvol_id]["Device"] = device
            self.ssh_obj.mount_path(node=self.node, 
                                    device=self.lvol_mount_details[lvol_id]["Device"],
                                    mount_path=lvol["Mount"])
                
        for _, lvol in self.lvol_mount_details.items():
            final_files = self.ssh_obj.find_files(node=self.node, directory=lvol['Mount'])
            final_checksums = self.ssh_obj.generate_checksums(node=self.node, files=final_files)
            
            assert final_checksums == lvol["MD5"], f"Checksum validation for {lvol['Name']} is not successful. Intial: {lvol['MD5']}, Final: {final_checksums}"

    def run_scenarios(self):
        """Run failure scenarios."""
        
        self.logger.info("Running failure scenarios.")
        
        # Sce-1 Graceful shutdown and restart
        self.logger.info("Graceful shutdown and restart.")
        timestamp = int(datetime.now().timestamp())

        self.sbcli_utils.suspend_node(node_uuid=self.lvol_node)
        sleep_n_sec(10)
        self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      ["unreachable", "offline"],
                                                      timeout=800)
        sleep_n_sec(30)
        self.sbcli_utils.restart_node(node_uuid=self.lvol_node)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=800)
        # self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        # self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        # self.validate_migration_for_node(timestamp, 5000, None)
        # sleep_n_sec(30)

        self.validate_checksums()

        # Sce-2 Container stop and restart
        # self.logger.info("Container stop and restart.")
        # timestamp = int(datetime.now().timestamp())
        # node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        # node_ip = node_details[0]["mgmt_ip"]
        # self.ssh_obj.stop_spdk_process(node_ip)
        # self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
        #                                               "online",
        #                                               timeout=800)
        # self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        # self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        # self.validate_migration_for_node(timestamp, 5000, None)
        # sleep_n_sec(30)

        # self.validate_checksums()

        # Sce-3 Network stop and restart
        # self.logger.info("Network stop and restart.")
        # timestamp = int(datetime.now().timestamp())
        # cmd = 'nohup sh -c "sudo ifdown eth0 && sleep 30 && sudo ifup eth0" &'
        # self.ssh_obj.exec_command(self.node, command=cmd)
        # self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
        #                                               "online",
        #                                               timeout=800)
        
        # self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        # self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        # self.validate_migration_for_node(timestamp, 5000, None)
        # sleep_n_sec(30)

        # self.validate_checksums()
        
        self.logger.info("Completed failure scenarios.")

    def run(self):
        """Main execution."""
        self.logger.info("Starting high-volume stress test.")
        self.node = self.mgmt_nodes[0]
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()
        self.run_scenarios()
        self.logger.info("Stress test completed.")
