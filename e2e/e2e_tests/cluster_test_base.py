import os
import boto3
from datetime import datetime
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils
from utils.common_utils import CommonUtils
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
import traceback
import threading


class TestClusterBase:
    def __init__(self, **kwargs):
        self.cluster_secret = os.environ.get("CLUSTER_SECRET")
        self.cluster_id = os.environ.get("CLUSTER_ID")

        self.api_base_url = os.environ.get("API_BASE_URL")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"{self.cluster_id} {self.cluster_secret}"
        }
        self.bastion_server = os.environ.get("BASTION_SERVER", None)

        self.ssh_obj = SshUtils(bastion_server=self.bastion_server)
        self.logger = setup_logger(__name__)
        self.sbcli_utils = SbcliUtils(
            cluster_api_url=self.api_base_url,
            cluster_id=self.cluster_id,
            cluster_secret=self.cluster_secret
        )
        self.common_utils = CommonUtils(self.sbcli_utils, self.ssh_obj)
        self.mgmt_nodes = None
        self.storage_nodes = None
        self.ndcs = kwargs.get("ndcs", 1)
        self.npcs = kwargs.get("npcs", 1)
        self.bs = kwargs.get("bs", 4096)
        self.chunk_bs = kwargs.get("chunk_bs", 4096)
        self.k8s_test = kwargs.get("k8s_run", False)
        self.pool_name = "test_pool"
        self.lvol_name = f"test_lvl_{self.ndcs}_{self.npcs}"
        self.mount_path = "/home/ec2-user/test_location"
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")
        self.fio_debug = kwargs.get("fio_debug", False)
        self.ec2_resource = None
        self.lvol_crypt_keys = ["7b3695268e2a6611a25ac4b1ee15f27f9bf6ea9783dada66a4a730ebf0492bfd",
                                "78505636c8133d9be42e347f82785b81a879cd8133046f8fc0b36f17b078ad0c"]

    def setup(self):
        """Contains setup required to run the test case
        """
        self.logger.info("Inside setup function")
        retry = 30
        while retry > 0:
            try:
                self.mgmt_nodes, self.storage_nodes = self.sbcli_utils.get_all_nodes_ip()
                self.sbcli_utils.list_lvols()
                self.sbcli_utils.list_storage_pools()
                break
            except Exception as e:
                self.logger.debug(f"API call failed with error:{e}")
                retry -= 1
                if retry == 0:
                    self.logger.info(f"Retry attemp exhausted. API failed with: {e}. Exiting")
                    raise e
                self.logger.info(f"Retrying Base APIs before starting tests. Attempt: {30 - retry + 1}")
        for node in self.mgmt_nodes:
            self.logger.info(f"**Connecting to management nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=self.bastion_server,
            )
        for node in self.storage_nodes:
            self.logger.info(f"**Connecting to storage nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=self.bastion_server,
            )
        # command = "python3 -c \"from importlib.metadata import version;print(f'SBCLI Version: {version('''sbcli-dev''')}')\""
        # self.ssh_obj.exec_command(
        #     self.mgmt_nodes[0], command=command
        # )
        sleep_n_sec(2)
        self.unmount_all(base_path=self.mount_path)
        sleep_n_sec(2)
        self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                  device=self.mount_path)
        sleep_n_sec(2)
        self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
        sleep_n_sec(2)
        self.disconnect_lvols()
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_lvols()
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_storage_pools()
        session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("AWS_REGION")
        )
        self.ec2_resource = session.resource('ec2')

        self.container_log_path = f"{os.path.dirname(self.mount_path)}/container_logs"
        os.makedirs(self.container_log_path, exist_ok=True)

        running_containers = self.get_running_containers()
        self.log_threads = []

        for stg_ip, containers in running_containers.items():
            for container in containers:
                log_thread = threading.Thread(
                    target=self.monitor_docker_logs,
                    args=(stg_ip, container, self.container_log_path)
                )
                log_thread.daemon = True  # Ensure threads stop when the main program exits
                log_thread.start()
                self.log_threads.append(log_thread)

        self.logger.info("Started log monitoring for all storage nodes.")

    def stop_docker_logs_collect(self):
        for thread in self.log_threads:
            if thread.is_alive():
                thread.join(timeout=5)  # Wait for the thread to finish
        self.logger.info("All log monitoring threads stopped.")

    def teardown(self):
        """Contains teradown required post test case execution
        """
        self.logger.info("Inside teardown function")
        
        self.ssh_obj.kill_processes(node=self.mgmt_nodes[0],
                                    process_name="fio")
        retry_check = 100
        while retry_check:
            fio_process = self.ssh_obj.find_process_name(
                node=self.mgmt_nodes[0],
                process_name="fio"
            )
            if len(fio_process) <= 2:
                break
            self.logger.info(f"Fio process should exit after kill. Still waiting: {fio_process}")
            retry_check -= 1
            sleep_n_sec(10)

        if retry_check <=0:
            self.logger.info("FIO did not exit completely after kill and wait. "
                             "Some hanging mount points could be present. "
                             "Needs manual cleanup.")

        try:
            self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
            sleep_n_sec(2)
            lvols = self.sbcli_utils.list_lvols()
            self.unmount_all(base_path=self.mount_path)
            self.unmount_all(base_path="/mnt/")
            sleep_n_sec(2)
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                    device=self.mount_path)
            sleep_n_sec(2)
            if lvols is not None:
                for _, lvol_id in lvols.items():
                    lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                    nqn = lvol_details[0]["nqn"]
                    self.ssh_obj.unmount_path(node=self.mgmt_nodes[0],
                                              device=self.mount_path)
                    sleep_n_sec(2)
                    self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                            command=f"sudo nvme disconnect -n {nqn}")
                    sleep_n_sec(2)
                self.disconnect_lvols()
                sleep_n_sec(2)
                self.sbcli_utils.delete_all_lvols()
                sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
            sleep_n_sec(2)
            self.ssh_obj.remove_dir(self.mgmt_nodes[0], "/mnt/*")
            for node, ssh in self.ssh_obj.ssh_connections.items():
                self.logger.info(f"Closing node ssh connection for {node}")
                ssh.close()
        except Exception as _:
            self.logger.info(traceback.format_exc())
        try:
            if self.ec2_resource:
                instance_id = self.common_utils.get_instance_id_by_name(ec2_resource=self.ec2_resource,
                                                                        instance_name="e2e-new-instance")
                if instance_id:
                    self.common_utils.terminate_instance(ec2_resource=self.ec2_resource,
                                                         instance_id=instance_id)
        except Exception as e:
            self.logger.info(f"Error while deleting instance: {e}")
            self.logger.info(traceback.format_exc())

    def validations(self, node_uuid, node_status, device_status, lvol_status,
                    health_check_status):
        """Validates node, devices, lvol status with expected status

        Args:
            node_uuid (str): UUID of node to validate
            node_status (str): Expected node status
            device_status (str): Expected device status
            lvol_status (str): Expected lvol status
            health_check_status (bool): Expected health check status
        """
        node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node_uuid)
        self.logger.info(f"Storage Node Details: {node_details}")
        device_details = self.sbcli_utils.get_device_details(storage_node_id=node_uuid)
        lvol_id = self.sbcli_utils.get_lvol_id(lvol_name=self.lvol_name)
        lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)

        offline_device = []

        if isinstance(node_status, list):
            assert node_details[0]["status"] in node_status, \
                f"Node {node_uuid} is not in {node_status} state. Actual: {node_details[0]['status']}"
        else:
            assert node_details[0]["status"] == node_status, \
                f"Node {node_uuid} is not in {node_status} state. Actual: {node_details[0]['status']}"
        offline_device_detail = self.sbcli_utils.wait_for_device_status(node_id=node_uuid,
                                                                        status=device_status,
                                                                        timeout=300)
        for device in offline_device_detail:
            # if "jm" in device["jm_bdev"]:
            #     assert device["status"] == "JM_DEV", \
            #         f"JM Device {device['id']} is not in JM_DEV state. {device['status']}"
            # else:
            assert device["status"] == device_status, \
                f"Device {device['id']} is not in {device_status} state. Actual {device['status']}"
            offline_device.append(device['id'])

        for lvol in lvol_details:
            assert lvol["status"] == lvol_status, \
                f"Lvol {lvol['id']} is not in {lvol_status} state. Actual: {lvol['status']}"

        storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        health_check_status = health_check_status if isinstance(health_check_status, list)\
              else [health_check_status]
        for node in storage_nodes:
            node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
            if node["id"] == node_uuid and node_details[0]['status'] == "offline":
                node = self.sbcli_utils.wait_for_health_status(node['id'], status=health_check_status,
                                                               timeout=300)
                assert node["health_check"] in health_check_status, \
                    f"Node {node['id']} health-check is not {health_check_status}. Actual: {node['health_check']}. Node Status: {node_details[0]['status']}"
            else:
                node = self.sbcli_utils.wait_for_health_status(node['id'], status=True,
                                                               timeout=300)
                assert node["health_check"] is True, \
                    f"Node {node['id']} health-check is not True. Actual:  {node['health_check']}.  Node Status: {node_details[0]['status']}"
            if node['id'] == node_uuid:
                device_details = offline_device_detail
            else:
                device_details = self.sbcli_utils.get_device_details(storage_node_id=node['id'])
            node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
            for device in device_details:
                if device['id'] in offline_device and node_details[0]['status'] == "offline":
                    device = self.sbcli_utils.wait_for_health_status(node['id'], status=health_check_status,
                                                                     device_id=device['id'],
                                                                     timeout=300)
                    assert device["health_check"] in health_check_status, \
                        f"Device {device['id']} health-check is not {health_check_status}. Actual:  {device['health_check']}"
                else:
                    device = self.sbcli_utils.wait_for_health_status(node['id'], status=True,
                                                                     device_id=device['id'],
                                                                     timeout=300)
                    assert device["health_check"] is True, \
                        f"Device {device['id']} health-check is not True. Actual:  {device['health_check']}"

        command = f"{self.base_cmd} sn get-cluster-map {lvol_details[0]['node_id']}"
        lvol_cluster_map_details, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                                                command=command)
        self.logger.info(f"LVOL Cluster map: {lvol_cluster_map_details}")
        cluster_map_nodes, cluster_map_devices = self.common_utils.parse_lvol_cluster_map_output(lvol_cluster_map_details)
        
        for node_id, node in cluster_map_nodes.items():
            if node_id == node_uuid:
                if isinstance(node_status, list):
                    assert node["Reported Status"] in node_status, \
                    f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
                    assert node["Actual Status"] in node_status, \
                        f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
                else:
                    assert node["Reported Status"] == node_status, \
                    f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
                    assert node["Actual Status"] == node_status, \
                        f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
                    
            else:
                assert node["Reported Status"] == "online", \
                    f"Node {node_uuid} is not in online state. Actual: {node['Reported Status']}"
                assert node["Actual Status"] == "online", \
                    f"Node {node_uuid} is not in online state. Actual: {node['Actual Status']}"

        if device_status is not None:
            for device_id, device in cluster_map_devices.items():
                if device_id in offline_device:
                    assert device["Reported Status"] == device_status, \
                        f"Device {device_id} is not in {device_status} state. Actual: {device['Reported Status']}"
                    assert device["Actual Status"] == device_status, \
                        f"Device {device_id} is not in {device_status} state. Actual: {device['Actual Status']}"
                else:
                    assert device["Reported Status"] == "online", \
                        f"Device {device_id} is not in online state. Actual: {device['Reported Status']}"
                    assert device["Actual Status"] == "online", \
                        f"Device {device_id} is not in online state. {device['Actual Status']}"

    def unmount_all(self, base_path=None):
        """ Unmount all mount points """
        self.logger.info("Unmounting all mount points")
        if not base_path:
            base_path = self.mount_path
        mount_points = self.ssh_obj.get_mount_points(node=self.mgmt_nodes[0], base_path=base_path)
        for mount_point in mount_points:
            self.logger.info(f"Unmounting {mount_point}")
            self.ssh_obj.unmount_path(node=self.mgmt_nodes[0], device=mount_point)

    def remove_mount_dirs(self):
        """ Remove all mount point directories """
        self.logger.info("Removing all mount point directories")
        mount_dirs = self.ssh_obj.get_mount_points(node=self.mgmt_nodes[0], base_path=self.mount_path)
        for mount_dir in mount_dirs:
            self.logger.info(f"Removing directory {mount_dir}")
            self.ssh_obj.remove_dir(node=self.mgmt_nodes[0], dir_path=mount_dir)
    
    def disconnect_lvol(self, lvol_device):
        """Disconnects the logical volume."""
        nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0],
                                                    nqn_filter=lvol_device)
        for nqn in nqn_lvol:
            self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
            self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=nqn)

    def disconnect_lvols(self):
        """ Disconnect all NVMe devices with NQN containing 'lvol' """
        self.logger.info("Disconnecting all NVMe devices with NQN containing 'lvol'")
        subsystems = self.ssh_obj.get_nvme_subsystems(node=self.mgmt_nodes[0], nqn_filter="lvol")
        for subsys in subsystems:
            self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
            self.ssh_obj.disconnect_nvme(node=self.mgmt_nodes[0], nqn_grep=subsys)

    def delete_snapshots(self):
        """ Delete all snapshots """
        self.logger.info("Deleting all snapshots")
        snapshots = self.ssh_obj.get_snapshots(node=self.mgmt_nodes[0])
        for snapshot in snapshots:
            self.logger.info(f"Deleting snapshot: {snapshot}")
            delete_snapshot_command = f"sbcli-lvol snapshot delete {snapshot} --force"
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=delete_snapshot_command)
            
    def get_running_containers(self):
        """Get a list of all running Docker containers on all storage nodes.

        Returns:
            dict: A dictionary with storage node IPs as keys and lists of container names as values.
        """
        running_containers = {}
        cmd = "sudo docker ps --format '{{.Names}}'"

        for stg_ip in self.storage_nodes:
            try:
                output, error = self.ssh_obj.exec_command(stg_ip, cmd)
                if error:
                    self.logger.error(f"Error fetching containers on {stg_ip}: {error}")
                    continue
                containers = output.strip().splitlines()
                running_containers[stg_ip] = containers
                self.logger.info(f"Running containers on {stg_ip}: {containers}")
            except Exception as e:
                self.logger.error(f"Error fetching running containers from {stg_ip}: {e}")

        return running_containers
    
    def is_container_running(self, stg_ip, container_name):
        """Check if a Docker container is running on a specific storage node.

        Args:
            stg_ip (str): IP of the storage node.
            container_name (str): Name of the Docker container.

        Returns:
            bool: True if the container is running, False otherwise.
        """
        cmd = f"sudo docker inspect -f '{{{{.State.Running}}}}' {container_name}"
        try:
            output, error = self.ssh_obj.exec_command(stg_ip, cmd)
            if error:
                self.logger.error(f"Error checking container status on {stg_ip}: {error}")
                return False
            return output.strip().lower() == 'true'
        except Exception as e:
            self.logger.error(f"Error checking container status on {stg_ip}: {e}")
            return False

    def monitor_docker_logs(self, stg_ip, container_name, log_path):
        """Monitor Docker logs for a container on a specific storage node.

        Args:
            stg_ip (str): IP of the storage node.
            container_name (str): Name of the Docker container.
            log_path (str): Directory to save the log files.
        """
        log_file_path = f"{log_path}/{stg_ip}_{container_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.ssh_obj.make_directory(log_path)  # Ensure the log directory exists

        def stream_callback(chunk, is_error):
            """Callback function to handle streaming logs."""
            with open(log_file_path, 'a') as log_file:
                log_type = "STDERR" if is_error else "STDOUT"
                self.logger.debug(f"[{log_type}] {chunk.strip()}")
                log_file.write(chunk)

        while True:
            if self.is_container_running(stg_ip, container_name):
                self.logger.info(f"Starting log collection for container '{container_name}' on node {stg_ip}")
                try:
                    cmd = f"sudo docker logs --follow {container_name}"
                    self.ssh_obj.exec_command(
                        node=stg_ip,
                        command=cmd,
                        stream_callback=stream_callback
                    )
                except Exception as e:
                    self.logger.error(f"Error while collecting logs for container '{container_name}' on node {stg_ip}: {e}")
            else:
                self.logger.warning(f"Container '{container_name}' on node {stg_ip} is not running. Retrying in 5 seconds.")
                sleep_n_sec(5)  # Wait before checking if the container is back online

