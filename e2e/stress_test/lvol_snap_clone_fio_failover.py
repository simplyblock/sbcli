from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
from lvol_ha_stress_fio import TestLvolHACluster
import threading

class TestLvolHAClusterWithClones(TestLvolHACluster):
    """
    Extends the TestLvolHACluster class to add test cases for handling lvols, clones, and failover scenarios.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.clone_name = "clone"
        self.total_clones = 10

    def create_clones(self):
        """Create clones for snapshots."""
        self.logger.info("Creating clones from snapshots.")
        for idx, lvol_id in enumerate(list(self.lvol_mount_details.keys())):
            snapshot_name = f"{self.snapshot_name}_{idx + 1}_1"
            for clone_idx in range(1, self.total_clones + 1):
                clone_name = f"{self.clone_name}_{idx + 1}_{clone_idx}"
                self.ssh_obj.create_clone(
                    node=self.node, snapshot_name=snapshot_name, clone_name=clone_name
                )
        self.logger.info("Clones created successfully.")

    def run_fio_on_clones(self):
        """Run FIO workloads on all clones."""
        self.logger.info("Running FIO workloads on clones.")
        fio_threads = []

        for idx, lvol_id in enumerate(self.lvol_mount_details.keys()):
            for clone_idx in range(1, self.total_clones + 1):
                clone_name = f"{self.clone_name}_{idx + 1}_{clone_idx}"
                mount_path = f"{self.mount_path}/{clone_name}"
                log_file = f"{self.log_path}/{clone_name}.log"

                fio_thread = threading.Thread(
                    target=self.ssh_obj.run_fio_test,
                    args=(self.node, None, mount_path, log_file),
                    kwargs={
                        "size": self.fio_size,
                        "name": f"{clone_name}_fio",
                        "rw": "randrw",
                        "bs": "4K-128K",
                        "numjobs": 32,
                        "iodepth": 512,
                    },
                )
                fio_thread.start()
                fio_threads.append(fio_thread)

        for thread in fio_threads:
            thread.join()

        self.logger.info("FIO workloads on clones completed.")

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
        self.ssh_obj.make_directory(node=self.node, dir_name=self.log_path)
        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.lvol_node = self.sbcli_utils.get_node_without_lvols()

        self.create_lvols()
        self.create_snapshots()
        self.create_clones()
        self.fill_volumes()
        self.run_fio_on_clones()

        # Run failover scenarios sequentially
        self.logger.info("Running failover scenarios.")
        self.run_failover_scenario(failover_type="graceful_shutdown")
        # self.run_failover_scenario(failover_type="container_stop")
        # self.run_failover_scenario(failover_type="network_interrupt")
        # self.run_failover_scenario(failover_type="instance_stop")

    def run_failover_scenario(self, failover_type):
        """Run specific failover scenario."""
        self.logger.info(f"Running {failover_type} failover scenario.")
        timestamp = int(datetime.now().timestamp())

        if failover_type == "graceful_shutdown":
            self.sbcli_utils.suspend_node(node_uuid=self.lvol_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "suspended", timeout=4000)
            sleep_n_sec(10)
            self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "offline", timeout=4000)
            sleep_n_sec(30)
            self.sbcli_utils.restart_node(node_uuid=self.lvol_node, expected_error_code=[503])
        elif failover_type == "container_stop":
            node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.stop_spdk_process(node_ip)
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        elif failover_type == "network_interrupt":
            cmd = (
                'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 60 && '
                'sudo nmcli dev connect eth0" &'
            )
            node_details = self.sbcli_utils.get_storage_node_details(self.lvol_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.exec_command(node_ip, command=cmd)
            self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        elif failover_type == "instance_stop":
            self.logger.info("Stopping EC2 instance.")
            self.common_utils.stop_ec2_instance(self.ec2_resource, self.instance_id)
            sleep_n_sec(10)
            self.logger.info("Starting EC2 instance.")
            self.common_utils.start_ec2_instance(self.ec2_resource, self.instance_id)

        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)

        self.logger.info("Waiting for data migration to complete.")
        self.validate_migration_for_node(timestamp, timeout=4000, max_retries=10)

        self.logger.info(f"{failover_type} failover scenario completed.")
