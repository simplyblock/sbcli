import random
import threading
from utils.common_utils import sleep_n_sec
from e2e_tests.data_migration.data_migration_ha_fio import FioWorkloadTest
from logger_config import setup_logger
from datetime import datetime
from exceptions.custom_exception import LvolNotConnectException


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
        self.snapshot_per_lvol = 6
        self.lvol_name = "lvl"
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
            is_crypto = random.choice([True, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            self.logger.info(f"Creating lvol with Name: {lvol_name}, fs type: {fs_type}, crypto: {is_crypto}")
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
                raise LvolNotConnectException("LVOL did not connect")
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
            for snap_idx in range(1, self.snapshot_per_lvol + 1):
                snapshot_name = f"{self.snapshot_name}_{idx + 1}_{snap_idx}"
                self.ssh_obj.add_snapshot(node=self.node, lvol_id=lvol_id, snapshot_name=snapshot_name)
        self.logger.info("Snapshots created.")

    def fill_volumes(self):
        """Fill lvols with data in batches of 10."""
        self.logger.info("Filling volumes with data in batches of 10.")
        fio_threads = []
        lvol_items = list(self.lvol_mount_details.items())
        batch_size = 10

        # Process lvols in batches
        for batch_start in range(0, len(lvol_items), batch_size):
            self.logger.info(f"Processing batch {batch_start // batch_size + 1}.")
            batch = lvol_items[batch_start:batch_start + batch_size]

            # Run FIO in parallel for the current batch
            for _, lvol in batch:
                fio_thread = threading.Thread(
                    target=self.ssh_obj.run_fio_test,
                    args=(self.node, None, lvol["Mount"], lvol["Log"]),
                    kwargs={
                        "size": self.fio_size,
                        "name": f"{lvol['Name']}_fio",
                        "rw": "write",
                        "bs": "4K-128K",
                        "time_based": False,
                    },
                )
                fio_thread.start()
                fio_threads.append(fio_thread)

            # Manage and join threads for the current batch
            self.common_utils.manage_fio_threads(
                node=self.node,
                threads=fio_threads,
                timeout=10000
            )
            for thread in fio_threads:
                thread.join()
            fio_threads = []  # Clear threads for the next batch

        self.logger.info("Data filling for all batches completed.")

    
    def calculate_md5(self):
        "Calculate Checksums"
        for lvol_id, lvol in self.lvol_mount_details.items():
            self.logger.info(f"Generating checksums for files in base volume: {lvol['Mount']}")
            base_files = self.ssh_obj.find_files(node=self.node, directory=lvol['Mount'])
            base_checksums = self.ssh_obj.generate_checksums(node=self.node, files=base_files)
            self.logger.info(f"Base Checksum for lvol {lvol['Name']}: {base_checksums}")
            self.lvol_mount_details[lvol_id]["MD5"] = base_checksums

    def wait_for_all_devices(self, existing_devices):
        "Waiting for devices to reconnect"
        for device in existing_devices:
            retry = 10
            while retry > 0:
                devices = self.ssh_obj.get_devices(node=self.node)
                devices = [dev[0:-1] for dev in devices]
                if device in devices:
                    break
                retry -= 1
                sleep_n_sec(10)
    
    def validate_checksums(self):
        "Validating checksums"
        existing_devices = []
        for lvol_id, lvol in self.lvol_mount_details.items():
            self.ssh_obj.unmount_path(node=self.node, device=lvol["Mount"])
            existing_devices.append(lvol["Device"][5:-1])

        self.wait_for_all_devices(existing_devices)
        
        for lvol_id, lvol in self.lvol_mount_details.items():
            device = lvol["Device"][5:-1]
            final_devices = self.ssh_obj.get_devices(node=self.node)
            lvol_device = None
            for cur_device in final_devices:
                if device in cur_device:
                    lvol_device = cur_device
                    break
            if lvol_device:
                self.lvol_mount_details[lvol_id]["Device"] = f"/dev/{lvol_device}"
            self.ssh_obj.mount_path(node=self.node, 
                                    device=self.lvol_mount_details[lvol_id]["Device"],
                                    mount_path=lvol["Mount"])
                
        for _, lvol in self.lvol_mount_details.items():
            final_files = self.ssh_obj.find_files(node=self.node, directory=lvol['Mount'])
            final_checksums = self.ssh_obj.generate_checksums(node=self.node, files=final_files)
            
            assert final_checksums == lvol["MD5"], f"Checksum validation for {lvol['Name']} is not successful. Intial: {lvol['MD5']}, Final: {final_checksums}"


class TestLvolHAClusterGracefulShutdown(TestLvolHACluster):
    """Tests Graceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-1: Starting high-volume stress test.")
        self.node = self.mgmt_nodes[0]
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Graceful shutdown and restart.")
        timestamp = int(datetime.now().timestamp())

        self.sbcli_utils.suspend_node(node_uuid=self.lvol_node)
        sleep_n_sec(10)
        self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      ["unreachable", "offline"],
                                                      timeout=800)
        sleep_n_sec(30)
        restart_start_time = datetime.now()
        self.sbcli_utils.restart_node(node_uuid=self.lvol_node)
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=800)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=800)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()
        
        self.validate_checksums()

        sleep_n_sec(30)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 5000, None)
        sleep_n_sec(30)

        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Graceful shutdown and start total time: {time_mins}")
        
        self.logger.info("Stress test completed.")


class TestLvolHAClusterStorageNodeCrash(TestLvolHACluster):
    """Tests Ungraceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-2: Starting high-volume stress test.")
        self.node = self.mgmt_nodes[0]
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Container stop and restart.")
        timestamp = int(datetime.now().timestamp())
        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]
        
        self.ssh_obj.stop_spdk_process(node_ip)
        restart_start_time = datetime.now()
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=800)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=800)
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()
        
        self.validate_checksums()
        
        sleep_n_sec(30)

        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 5000, None)
        sleep_n_sec(30)

        time_secs = node_up_time - restart_start_time
        time_mins = time_secs.seconds / 60
        self.logger.info(f"Crash and start total time: {time_mins}")
        
        self.logger.info("Stress test completed.")


class TestLvolHAClusterNetworkInterrupt(TestLvolHACluster):
    """Tests Graceful shutdown for LVstore recover
    """
    def run(self):
        """Main execution."""
        self.logger.info(f"Mount details: {self.lvol_mount_details}")
        self.logger.info("SCE-3: Starting high-volume stress test.")
        self.node = self.mgmt_nodes[0]
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.fill_volumes()
        self.calculate_md5()

        self.logger.info("Network stop and restart.")
        timestamp = int(datetime.now().timestamp())
        cmd = (
            'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 120 && '
            'sudo nmcli dev connect eth0" &'
        )

        node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
        node_ip = node_details[0]["mgmt_ip"]

        unavailable_thread = threading.Thread(
            target=lambda: self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "schedulable", 300)
        )

        unavailable_thread.start()

        disconnect_start_time = datetime.now()
        
        self.ssh_obj.exec_command(node_ip, command=cmd)
        
        unavailable_thread.join()
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node,
                                                      "online",
                                                      timeout=800)
        self.sbcli_utils.wait_for_health_status(self.lvol_node,
                                                True,
                                                timeout=800)
        
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=self.lvol_node)
        actual_status = node_details[0]["health_check"]
        self.logger.info(f"Node health check is: {actual_status}")
        
        node_up_time = datetime.now()
        
        self.logger.info(f"Fetching migration tasks for cluster {self.cluster_id}.")
        output, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], 
                                              command=f"{self.base_cmd} cluster list-tasks {self.cluster_id}")
        self.logger.info(f"Data migration output: {output}")

        self.logger.info(f"Validating migration tasks for node {self.lvol_node}.")
        self.validate_migration_for_node(timestamp, 5000, None)
        sleep_n_sec(120)

        self.validate_checksums()

        time_secs = node_up_time - disconnect_start_time
        time_mins = (time_secs.seconds - 120) / 60
        self.logger.info(f"Network reconnect and node online total time: {time_mins}")
        
        self.logger.info("Stress test completed.")