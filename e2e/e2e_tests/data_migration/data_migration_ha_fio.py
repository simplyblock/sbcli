import threading
import json
from e2e_tests.cluster_test_base import TestClusterBase
from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
import traceback
from requests.exceptions import HTTPError
import random



class FioWorkloadTest(TestClusterBase):
    """
    This test automates:
    1. Create lvols on each node, connect lvols, check devices, and mount them.
    2. Run fio workloads.
    3. Shutdown and restart nodes, remount and check fio processes.
    4. Validate migration tasks for a specific node, ensuring fio continues running on unaffected nodes.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("Starting test case: FIO Workloads with lvol connections and migrations.")

        # Step 1: Create 4 lvols on each node and connect them
        lvol_fio_path = {}
        sn_lvol_data = {}

        self.sbcli_utils.add_storage_pool(
            pool_name=self.pool_name
        )

        for i in range(0, len(self.storage_nodes)):
            node_uuid = self.sbcli_utils.get_node_without_lvols()
            sn_lvol_data[node_uuid] = []
            self.logger.info(f"Creating 2 lvols on node {node_uuid}.")
            for j in range(2):
                lvol_name = f"test_lvol_{i+1}_{j+1}"
                self.sbcli_utils.add_lvol(lvol_name=lvol_name, pool_name=self.pool_name, size="10G", host_id=node_uuid)
                sn_lvol_data[node_uuid].append(lvol_name)
                lvol_fio_path[lvol_name] = {"mount_path": None,
                                            "disk": None}
        
        trim_node = random.choice(list(sn_lvol_data.keys()))
        mount = True
        skip_mount = False

        device_count = 1
        for node, lvol_list in sn_lvol_data.items():
            # node_ip = self.get_node_ip(node)
            # Step 2: Connect lvol to the node
            for lvol in lvol_list:
                initial_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
            
                connect_str = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol)
                self.ssh_obj.exec_command(self.mgmt_nodes[0], connect_str)
                sleep_n_sec(5)

                # Step 3: Check for new device after connecting the lvol
                final_devices = self.ssh_obj.get_devices(node=self.mgmt_nodes[0])
                self.logger.info(f"Initial vs final disk on node {node}:")
                self.logger.info(f"Initial: {initial_devices}")
                self.logger.info(f"Final: {final_devices}")

                # Step 4: Identify the new device and mount it (if applicable)
                fs_type = "xfs" if lvol[-1] == "1" else "ext4"
                for device in final_devices:
                    if device not in initial_devices:
                        disk_use = f"/dev/{device.strip()}"
                        lvol_fio_path[lvol]["disk"] = disk_use
                        if trim_node == node and mount:
                            skip_mount = True
                            mount  = False
                        if not skip_mount:
                            # Unmount, format, and mount the device
                            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=disk_use)
                            sleep_n_sec(2)
                            self.ssh_obj.format_disk(node=self.mgmt_nodes[0], device=disk_use, fs_type=fs_type)
                            sleep_n_sec(2)
                            mount_path = f"/mnt/device_{device_count}"
                            device_count += 1
                            self.ssh_obj.mount_path(node=self.mgmt_nodes[0], device=disk_use, mount_path=mount_path)
                            lvol_fio_path[lvol]["mount_path"] = mount_path
                            sleep_n_sec(2)
                            break
                        skip_mount = False

        print(f"SN List: {sn_lvol_data}")
        print(f"LVOL Mounts: {lvol_fio_path}")

        # Step 5: Run fio workloads with different configurations
        fio_threads = self.run_fio(lvol_fio_path)

        # Step 6: Continue with node shutdown, restart, and migration task validation
        affected_node = list(sn_lvol_data.keys())[0]
        self.logger.info(f"Shutting down node {affected_node}.")
        self.shutdown_node_and_verify(affected_node)

        sleep_n_sec(60)

        # Step 4: Fetch and validate migration tasks for the specific node via API
        cluster_id = self.cluster_id
        self.logger.info(f"Fetching migration tasks for cluster {cluster_id}.")
        tasks = self.sbcli_utils.get_cluster_tasks(cluster_id)

        self.logger.info(f"Validating migration tasks for node {affected_node}.")
        self.validate_migration_for_node(tasks, affected_node)

        # # Step 5: Stop container on another node
        # another_node = self.storage_nodes[1]
        # self.logger.info(f"Killing SPDK container on node {another_node}.")
        # self.ssh_obj.stop_docker_containers(another_node, "spdk")

        # # Step 6: Stop and remove the instance on the third node
        # third_node = self.storage_nodes[2]
        # self.logger.info(f"Stopping instance on node {third_node}.")
        # self.sbcli_utils.shutdown_node(third_node)
        # self.logger.info(f"Removing node {third_node}.")
        # # Add logic for removing node here...

        # # Step 7: Check storage utilization across devices
        # self.logger.info("Checking storage utilization.")
        # utilization = self.sbcli_utils.get_device_utilization()
        # self.logger.info(f"Utilization: {utilization}")

        # Wait for all fio threads to finish
        for thread in fio_threads:
            thread.join()

        self.logger.info("Test completed successfully.")

    def get_node_ip(self, node_id):
        return self.sbcli_utils.get_storage_node_details(node_id)[0]["mgmt_ip"]

    def run_fio(self, lvol_fio_path):
        self.logger.info("Starting fio workloads on the logical volumes with different configurations.")
        fio_threads = []
        fio_configs = [("randrw", "4K"), ("read", "32K"), ("write", "64K"), ("trimwrite", "16K")]
        for lvol, data in lvol_fio_path.items():
            fio_run = random.choice(fio_configs)
            if data["mount_path"]:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], None, data["mount_path"], None),
                                kwargs={
                                    "name": f"fio_{lvol}",
                                    "rw": fio_run[0],
                                    "ioengine": "libaio",
                                    "iodepth": 64,
                                    "bs": fio_run[1],
                                    "size": "1G",
                                    "time_based": True,
                                    "runtime": 3600,
                                    "output_file": f"/home/ec2-user/{lvol}.log",
                                    "numjobs": 2,
                                    "debug": self.fio_debug
                                }
                            )
            else:
                thread = threading.Thread(
                                target=self.ssh_obj.run_fio_test,
                                args=(self.mgmt_nodes[0], lvol_fio_path[lvol]["disk"], None, None),
                                kwargs={
                                    "name": f"fio_{lvol}",
                                    "rw": fio_configs[3][0],
                                    "ioengine": "libaio",
                                    "iodepth": 64,
                                    "bs": fio_configs[3][1],
                                    "size": "1G",
                                    "time_based": True,
                                    "runtime": 3600,
                                    "output_file": f"/home/ec2-user/{lvol}.log",
                                    "nrfiles": 2,
                                    "debug": self.fio_debug
                                }
                            )
            fio_threads.append(thread)
            thread.start()
        return fio_threads

    def shutdown_node_and_verify(self, node_id):
        """Shutdown the node and ensure fio is uninterrupted."""
        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        print(f"Mount paths before shutdown: {output[0].strip().split('\n')}")
        self.sbcli_utils.suspend_node(node_id)
        self.logger.info(f"Node {node_id} suspended successfully.")

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        print(f"Mount paths after suspend: {output[0].strip().split('\n')}")

        sleep_n_sec(30)

        self.sbcli_utils.shutdown_node(node_id)
        self.logger.info(f"Node {node_id} shut down successfully.")

        self.sbcli_utils.wait_for_storage_node_status(node_id=node_id, status="offline",
                                                      timeout=500)

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        print(f"Mount paths after shutdown: {output[0].strip().split('\n')}")
        
        # Validate fio is running on other nodes
        fio_process = self.ssh_obj.find_process_name(self.mgmt_nodes[0], 'fio')
        if not fio_process:
            raise RuntimeError("FIO process was interrupted on unaffected nodes.")
        self.logger.info("FIO process is running uninterrupted.")

        sleep_n_sec(30)

        # Restart node
        self.sbcli_utils.restart_node(node_id)
        self.logger.info(f"Node {node_id} restarted successfully.")

        self.sbcli_utils.wait_for_storage_node_status(node_id=node_id, status="online",
                                                      timeout=500)

        output = self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command="sudo df -h")
        print(f"Mount paths after restart: {output[0].strip().split('\n')}")

    def filter_migration_tasks_for_node(self, tasks, node_id):
        """
        Filters `device_migration` tasks for a specific node.

        Args:
            tasks (list): List of task dictionaries from the API response.
            node_id (str): The UUID of the node to check for migration tasks.

        Returns:
            list: List of `device_migration` tasks for the specific node.
        """
        return [task for task in tasks if task['function_name'] == 'device_migration' and task['node_id'] == node_id]

    def validate_migration_for_node(self, tasks, node_id):
        """
        Validate that all `device_migration` tasks for a specific node have completed successfully.

        Args:
            tasks (list): List of task dictionaries from the API response.
            node_id (str): The UUID of the node to check for migration tasks.

        Raises:
            RuntimeError: If any migration task failed or did not run.
        """
        print(f"Migration tasks: {tasks}")
        node_tasks = self.filter_migration_tasks_for_node(tasks, node_id)

        if not node_tasks:
            raise RuntimeError(f"No migration tasks found for node {node_id}.")
        
        for task in node_tasks:
            if task['status'] != 'done' or task['function_result'] != 'Done':
                raise RuntimeError(f"Migration task {task['id']} on node {node_id} failed or is incomplete.")
        
        self.logger.info(f"All migration tasks for node {node_id} completed successfully.")