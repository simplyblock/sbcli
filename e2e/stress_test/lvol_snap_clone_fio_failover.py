from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
import threading
from exceptions.custom_exception import LvolNotConnectException
import random
import string


def random_char(len):
    """Generate number of characters

    Args:
        len (int): NUmber of characters in string

    Returns:
        str: random string with given length
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(len))


class TestLvolHAClusterWithClones(TestLvolHACluster):
    """
    Extends the TestLvolHACluster class to add test cases for handling lvols, clones, and failover scenarios.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.lvol_name = f"lvl{random_char(3)}"
        self.clone_name = f"cln{random_char(3)}"
        self.snapshot_name = f"snap{random_char(3)}"
        self.lvol_size = "50G"
        self.fio_size = "1G"
        self.total_lvols = 5
        self.snapshot_per_lvol = 10
        self.total_clones_per_lvol = 1
        self.fio_threads = []
        self.lvol_node = None
        self.clone_mount_details = {}
        self.lvol_mount_details = {}
        self.node_vs_lvol = []

    def create_clones(self):
        """Create clones for snapshots."""
        self.logger.info("Creating clones from snapshots.")
        for idx, lvol_id in enumerate(list(self.lvol_mount_details.keys())):
            snapshot_name = self.lvol_mount_details[lvol_id]["snapshots"][0]
            snapshot_id = self.ssh_obj.get_snapshot_id(node=self.node, snapshot_name=snapshot_name)
            self.lvol_mount_details[lvol_id]["clones"] = []
            for clone_idx in range(1, self.total_clones_per_lvol + 1):
                clone_name = f"{self.clone_name}_{idx + 1}_{clone_idx}"
                self.ssh_obj.add_clone(
                    node=self.node, snapshot_id=snapshot_id, clone_name=clone_name
                )
                clone_id = self.sbcli_utils.get_lvol_id(lvol_name=clone_name)
                connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)

                self.lvol_mount_details[lvol_id]["clones"].append(clone_id)
                self.clone_mount_details[clone_id] = {
                   "Name": clone_name,
                   "Command": connect_ls,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": self.lvol_mount_details[lvol_id]["FS"],
                   "Log": f"{self.log_path}/{clone_name}.log",
                   "snapshots": snapshot_name
                }

                initial_devices = self.ssh_obj.get_devices(node=self.node)
                for connect_str in connect_ls:
                    self.ssh_obj.exec_command(node=self.node, command=connect_str)

                sleep_n_sec(3)
                final_devices = self.ssh_obj.get_devices(node=self.node)
                clone_device = None
                for device in final_devices:
                    if device not in initial_devices:
                        clone_device = f"/dev/{device.strip()}"
                        break
                if not clone_device:
                    raise LvolNotConnectException("Clone did not connect")
                self.clone_mount_details[clone_id]["Device"] = clone_device

                # Mount and Run FIO
                mount_point = f"{self.mount_path}/{clone_name}"
                self.ssh_obj.mount_path(node=self.node, device=clone_device, mount_path=mount_point)
                self.clone_mount_details[clone_id]["Mount"] = mount_point
        self.logger.info("Clones created successfully.")

    def run_fio_on_lvols_clones(self):
        """Run FIO workloads on all lvols and clones."""
        self.logger.info("Running FIO workloads on lvols and clones.")
        
        for _, lvol_details in self.lvol_mount_details.items():
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.node, None, lvol_details["Mount"], lvol_details["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_details['Name']}_fio",
                    "rw": "randrw",
                    "bs": "4K-128K",
                    "numjobs": 32,
                    "iodepth": 1,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

        for _, clone_details in self.clone_mount_details.items():
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.node, None, clone_details["Mount"], clone_details["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{clone_details['Name']}_fio",
                    "rw": "randrw",
                    "bs": "4K-128K",
                    "numjobs": 32,
                    "iodepth": 1,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

        self.logger.info("FIO workloads on lvols and clones started.")

class TestFailoverScenariosStorageNodes(TestLvolHAClusterWithClones):
    """
    Test class to handle all failover scenarios on storage nodes.
    """

    def run(self):
        """Main execution for all failover scenarios on storage nodes."""
        self.logger.info("Starting failover scenarios for storage nodes.")

        # Ensure the setup is performed once
        self.logger.info("Performing initial setup.")
        self.node = self.mgmt_nodes[0]

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        # Run failover scenarios sequentially
        self.logger.info("Running failover scenarios.")
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            if result["is_secondary_node"] is False:
                self.lvol_node = result["uuid"]
                self.fio_threads = []
                self.clone_mount_details = {}
                self.node_vs_lvol = []
                self.lvol_mount_details = {}
                self.create_lvols()
                self.create_snapshots()
                self.create_clones()
                self.run_fio_on_lvols_clones()
                self.run_failover_scenario(failover_type="graceful_shutdown")
                # self.run_failover_scenario(failover_type="container_stop")
                # self.run_failover_scenario(failover_type="network_interrupt")
                # self.run_failover_scenario(failover_type="instance_stop")

        for thread in self.fio_threads:
            thread.join()

    def run_failover_scenario(self, failover_type):
        """Run specific failover scenario."""
        self.logger.info(f"Running {failover_type} failover scenario on node {self.lvol_node}.")
        timestamp = int(datetime.now().timestamp())

        if failover_type == "graceful_shutdown":
            self.sbcli_utils.suspend_node(node_uuid=self.lvol_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "suspended", timeout=4000)
            sleep_n_sec(10)
            self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "offline", timeout=4000)
            sleep_n_sec(300)
            self.sbcli_utils.restart_node(node_uuid=self.lvol_node, expected_error_code=[503])
        elif failover_type == "container_stop":
            node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.stop_spdk_process(node_ip)
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        elif failover_type == "network_interrupt":
            cmd = (
                'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 300 && '
                'sudo nmcli dev connect eth0" &'
            )
            node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.exec_command(node_ip, command=cmd)
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        elif failover_type == "instance_stop":
            self.logger.info("Stopping EC2 instance.")
            self.common_utils.stop_ec2_instance(self.ec2_resource, self.instance_id)
            sleep_n_sec(300)
            self.logger.info("Starting EC2 instance.")
            self.common_utils.start_ec2_instance(self.ec2_resource, self.instance_id)

        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)

        sleep_n_sec(1000)

        end_timestamp = int(datetime.now().timestamp())
        time_duration = self.common_utils.calculate_time_duration(
            start_timestamp=timestamp,
            end_timestamp=end_timestamp
        )

        # Validate I/O stats during and after failover
        self.common_utils.validate_io_stats(
            cluster_id=self.cluster_id,
            start_timestamp=timestamp,
            end_timestamp=end_timestamp,
            time_duration=time_duration
        )

        self.logger.info("Waiting for data migration to complete.")
        self.validate_migration_for_node(timestamp, 2000, None)
        self.common_utils.manage_fio_threads(node=self.node, threads=self.fio_threads, timeout=10000)

        sleep_n_sec(60)

        for _, lvol_details in self.lvol_mount_details.items():
            self.common_utils.validate_fio_test(self.node, lvol_details["Log"])
        
        for _, clone_details in self.clone_mount_details.items():
            self.common_utils.validate_fio_test(self.node, clone_details["Log"])

        self.logger.info(f"{failover_type} failover scenario completed.")
