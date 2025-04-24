import os
import boto3
from utils.sbcli_utils import SbcliUtils
from utils.ssh_utils import SshUtils, RunnerK8sLog
from utils.common_utils import CommonUtils
from logger_config import setup_logger
from utils.common_utils import sleep_n_sec
from exceptions.custom_exception import CoreFileFoundException
import traceback
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
import string, random


def generate_random_sequence(length):
    letters = string.ascii_uppercase  # A-Z
    numbers = string.digits  # 0-9
    all_chars = letters + numbers  # Allowed characters

    first_char = random.choice(letters)  # First character must be a letter
    remaining_chars = ''.join(random.choices(all_chars, k=length-1))  # Next 14 characters

    return first_char + remaining_chars

class TestClusterBase:
    def __init__(self, **kwargs):
        self.cluster_secret = os.environ.get("CLUSTER_SECRET")
        self.cluster_id = os.environ.get("CLUSTER_ID")

        self.api_base_url = os.environ.get("API_BASE_URL")
        self.client_machines = os.environ.get("CLIENT_IP", "")
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
        self.fio_node = None
        self.ndcs = kwargs.get("ndcs", 1)
        self.npcs = kwargs.get("npcs", 1)
        self.bs = kwargs.get("bs", 4096)
        self.chunk_bs = kwargs.get("chunk_bs", 4096)
        self.k8s_test = kwargs.get("k8s_run", False)
        self.pool_name = "test_pool"
        self.lvol_name = f"test_lvl_{generate_random_sequence(4)}"
        self.mount_path = f"{Path.home()}/test_location"
        self.log_path = f"{os.path.dirname(self.mount_path)}/log_file.log"
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")
        self.fio_debug = kwargs.get("fio_debug", False)
        self.ec2_resource = None
        self.lvol_crypt_keys = ["7b3695268e2a6611a25ac4b1ee15f27f9bf6ea9783dada66a4a730ebf0492bfd",
                                "78505636c8133d9be42e347f82785b81a879cd8133046f8fc0b36f17b078ad0c"]
        self.log_threads = []
        self.test_name = ""
        self.container_nodes = {}
        self.docker_logs_path = ""
        self.runner_k8s_log = ""

    def setup(self):
        """Contains setup required to run the test case
        """
        self.logger.info("Inside setup function")
        retry = 30
        while retry > 0:
            try:
                print("getting all storage nodes")
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
            sleep_n_sec(2)
            self.ssh_obj.set_aio_max_nr(node)
        for node in self.storage_nodes:
            self.logger.info(f"**Connecting to storage nodes** - {node}")
            self.ssh_obj.connect(
                address=node,
                bastion_server_address=self.bastion_server,
            )
            sleep_n_sec(2)
            self.ssh_obj.set_aio_max_nr(node)
        if self.client_machines:
            self.client_machines = self.client_machines.strip().split(" ")
            for client in self.client_machines:
                self.logger.info(f"**Connecting to client machine** - {client}")
                self.ssh_obj.connect(
                    address=client,
                    bastion_server_address=self.bastion_server,
                )
                sleep_n_sec(2)

        self.fio_node = self.client_machines if self.client_machines else [self.mgmt_nodes[0]]

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        # Construct the logs path with test name and timestamp
        self.docker_logs_path = os.path.join(Path.home(), "container-logs", f"{self.test_name}-{timestamp}")
        self.runner_k8s_log = RunnerK8sLog(
                log_dir=self.docker_logs_path,
                test_name=self.test_name
            )

        # command = "python3 -c \"from importlib.metadata import version;print(f'SBCLI Version: {version('''sbcli-dev''')}')\""
        # self.ssh_obj.exec_command(
        #     self.mgmt_nodes[0], command=command
        # )
        self.disconnect_lvols()
        sleep_n_sec(2)
        self.unmount_all(base_path=self.mount_path)
        sleep_n_sec(2)
        for node in self.fio_node:
            self.ssh_obj.unmount_path(node=node,
                                      device=self.mount_path)
            sleep_n_sec(2)
        self.disconnect_lvols()
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_lvols()
        sleep_n_sec(2)
        self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
        sleep_n_sec(2)
        self.sbcli_utils.delete_all_storage_pools()
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID", None)
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
        if aws_access_key and aws_secret_key:
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=os.environ.get("AWS_REGION")
            )
            self.ec2_resource = session.resource('ec2')

        for node in self.storage_nodes:
            self.ssh_obj.delete_old_folders(
                node=node,
                folder_path=os.path.join(Path.home(), "container-logs"),
                days=3
            )
            self.ssh_obj.make_directory(node=node, dir_name=self.docker_logs_path)
            containers = self.ssh_obj.get_running_containers(node_ip=node)
            self.container_nodes[node] = containers
            self.ssh_obj.check_tmux_installed(node_ip=node)
            self.ssh_obj.exec_command(node=node,
                                    command="sudo tmux kill-server")
            
            self.ssh_obj.start_resource_monitors(node_ip=node, log_dir=self.docker_logs_path)

            if not self.k8s_test:
                self.ssh_obj.start_docker_logging(node_ip=node,
                                                containers=containers,
                                                log_dir=self.docker_logs_path,
                                                test_name=self.test_name
                                                )

            self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=self.docker_logs_path)
            self.ssh_obj.start_netstat_dmesg_logging(node_ip=node,
                                                    log_dir=self.docker_logs_path)
            if not self.k8s_test:
                self.ssh_obj.reset_iptables_in_spdk(node_ip=node)
        
        if self.k8s_test:
            self.runner_k8s_log.start_logging()
            self.runner_k8s_log.monitor_pod_logs()
        # for node in self.storage_nodes:
        #     self.ssh_obj.monitor_container_logs(
        #         node_ip=node,
        #         containers=self.container_nodes[node],
        #         log_dir=self.docker_logs_path,
        #         test_name=self.test_name
        #     )

        for node in self.mgmt_nodes:
            self.ssh_obj.delete_old_folders(
                node=node,
                folder_path=os.path.join(Path.home(), "container-logs"),
                days=3
            )
            self.ssh_obj.make_directory(node=node, dir_name=self.docker_logs_path)
            containers = self.ssh_obj.get_running_containers(node_ip=node)
            self.container_nodes[node] = containers
            self.ssh_obj.check_tmux_installed(node_ip=node)
            self.ssh_obj.exec_command(node=node,
                                    command="sudo tmux kill-server")
            
            self.ssh_obj.start_resource_monitors(node_ip=node, log_dir=self.docker_logs_path)

            self.ssh_obj.start_docker_logging(node_ip=node,
                                              containers=containers,
                                              log_dir=self.docker_logs_path,
                                              test_name=self.test_name
                                              )

            self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=self.docker_logs_path)
            self.ssh_obj.start_netstat_dmesg_logging(node_ip=node,
                                                     log_dir=self.docker_logs_path)
        # for node in self.mgmt_nodes:
        #     self.ssh_obj.monitor_container_logs(
        #         node_ip=node,
        #         containers=self.container_nodes[node],
        #         log_dir=self.docker_logs_path,
        #         test_name=self.test_name
        #     )
        
        for node in self.fio_node:
            self.ssh_obj.delete_old_folders(
                node=node,
                folder_path=os.path.join(Path.home(), "container-logs"),
                days=3
            )

            self.ssh_obj.make_directory(node=node, dir_name=self.docker_logs_path)

            self.ssh_obj.check_tmux_installed(node_ip=node)

            self.ssh_obj.exec_command(node=node,
                                      command="sudo tmux kill-server")
            self.ssh_obj.start_tcpdump_logging(node_ip=node,
                                               log_dir=self.docker_logs_path)
            self.ssh_obj.start_netstat_dmesg_logging(node_ip=node,
                                                     log_dir=self.docker_logs_path)

        self.logger.info("Started log monitoring for all storage nodes.")

        sleep_n_sec(120)

    def configure_sysctl_settings(self):
        """Configure TCP kernel parameters on the node."""
        sysctl_commands = [
            'echo "net.core.rmem_max=16777216" | sudo tee -a /etc/sysctl.conf',
            'echo "net.core.rmem_default=87380" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_rmem=4096 87380 16777216" | sudo tee -a /etc/sysctl.conf',
            'echo "net.core.somaxconn=1024" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_max_syn_backlog=4096" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_window_scaling=1" | sudo tee -a /etc/sysctl.conf',
            'echo "net.ipv4.tcp_retries2=8" | sudo tee -a /etc/sysctl.conf',
            'sudo sysctl -p'
        ]
        for node in self.storage_nodes:
            for cmd in sysctl_commands:
                self.ssh_obj.exec_command(node, cmd)
        for cmd in sysctl_commands:
            for node in self.fio_node:
                self.ssh_obj.exec_command(node, cmd)
        for node in self.fio_node:
            self.ssh_obj.set_aio_max_nr(node)
    
        self.logger.info("Configured TCP sysctl settings on all the nodes!!")

    def cleanup_logs(self):
        """Cleans logs
        """
        base_path = Path.home()
        for node in self.fio_node:
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.log*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.state*", recursive=True)
        # self.ssh_obj.delete_file_dir(self.mgmt_nodes[0], entity="/etc/simplyblock/*", recursive=True)
        self.ssh_obj.delete_file_dir(self.mgmt_nodes[0], entity=f"{base_path}/*.txt*", recursive=True)
        for node in self.storage_nodes:
            self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/[0-9]*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/*core*.zst", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity="/etc/simplyblock/LVS*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/distrib*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.txt*", recursive=True)
            self.ssh_obj.delete_file_dir(node, entity=f"{base_path}/*.log*", recursive=True)

    def stop_docker_logs_collect(self):
        for node in self.storage_nodes:
            self.ssh_obj.stop_container_log_monitor(node)
            pids = self.ssh_obj.find_process_name(
                node=node,
                process_name="docker logs --follow",
                return_pid=True
            )
            for pid in pids:
                self.ssh_obj.kill_processes(node=node, pid=pid)
        
        for node in self.mgmt_nodes:
            self.ssh_obj.stop_container_log_monitor(node)
            pids = self.ssh_obj.find_process_name(
                node=node,
                process_name="docker logs --follow",
                return_pid=True
            )
            for pid in pids:
                self.ssh_obj.kill_processes(node=node, pid=pid)
        self.logger.info("All log monitoring threads stopped.")
    
    def stop_k8s_log_collect(self):
        self.runner_k8s_log.stop_log_monitor()
        self.runner_k8s_log.stop_logging()

    def fetch_all_nodes_distrib_log(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            if result['is_secondary_node'] is False:
                self.ssh_obj.fetch_distrib_logs(result["mgmt_ip"], result["uuid"])

    def collect_management_details(self):
        base_path = Path.home()
        cmd = f"{self.base_cmd} cluster list >& {base_path}/cluster_list.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster status {self.cluster_id} >& {base_path}/cluster_status.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster get-logs {self.cluster_id} --limit 0 >& {base_path}/cluster_get_logs.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0 >& {base_path}/cluster_list_tasks.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} sn list >& {base_path}/sn_list.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        cmd = f"{self.base_cmd} cluster get-capacity {self.cluster_id} >& {base_path}/cluster_capacity.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster get-capacity {self.cluster_id} >& {base_path}/cluster_capacity.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        cmd = f"{self.base_cmd} cluster show {self.cluster_id} >& {base_path}/cluster_show.txt"
        self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                  command=cmd)
        
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        node=1
        for result in storage_nodes['results']:
            cmd = f"{self.base_cmd} sn list-devices {result['uuid']} >& {base_path}/node{node}_list_devices.txt"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            cmd = f"{self.base_cmd} sn check {result['uuid']} >& {base_path}/node{node}_check.txt"
            self.ssh_obj.exec_command(self.mgmt_nodes[0], cmd)

            node+=1
        for node in self.fio_node:
            cmd = f"journalctl -k >& {base_path}/jounalctl_{node}.txt"
            self.ssh_obj.exec_command(node, cmd)
            cmd = f"dmesg -T >& {base_path}/dmesg_{node}.txt"
            self.ssh_obj.exec_command(node, cmd)
            
    def teardown(self):
        """Contains teradown required post test case execution
        """
        self.logger.info("Inside teardown function")

        for node in self.storage_nodes:
            self.ssh_obj.exec_command(node=node,
                                      command="sudo tmux kill-server")
            result = self.ssh_obj.check_remote_spdk_logs_for_keyword(node_ip=node, 
                                                                     log_dir=self.docker_logs_path, 
                                                                     test_name=self.test_name)

            for file, lines in result.items():
                if lines:
                    self.logger.info(f"\n{file}:")
                    for line in lines:
                        self.logger.info(f"  -> {line}")

        for node in self.fio_node:
            self.ssh_obj.exec_command(node=node,
                                      command="sudo tmux kill-server")
        
            self.ssh_obj.kill_processes(node=node,
                                        process_name="fio")
        

        retry_check = 100
        while retry_check:
            exit_while = True
            for node in self.fio_node:
                fio_process = self.ssh_obj.find_process_name(
                    node=node,
                    process_name="fio --name"
                )
                exit_while = exit_while and len(fio_process) <= 2
            if exit_while:
                break
            else:
                self.logger.info(f"Fio process should exit after kill. Still waiting: {fio_process}")
                retry_check -= 1
                sleep_n_sec(10)

        if retry_check <=0:
            self.logger.info("FIO did not exit completely after kill and wait. "
                             "Some hanging mount points could be present. "
                             "Needs manual cleanup.")

        try:
            lvols = self.sbcli_utils.list_lvols()
            self.unmount_all(base_path=self.mount_path)
            self.unmount_all(base_path="/mnt/")
            sleep_n_sec(2)
            for node in self.fio_node:
                self.ssh_obj.unmount_path(node=node,
                                          device=self.mount_path)
            sleep_n_sec(2)
            if lvols is not None:
                for _, lvol_id in lvols.items():
                    lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=lvol_id)
                    nqn = lvol_details[0]["nqn"]
                    for node in self.fio_node:
                        self.ssh_obj.unmount_path(node=node,
                                                  device=self.mount_path)
                        sleep_n_sec(2)
                        self.ssh_obj.exec_command(node=node,
                                                  command=f"sudo nvme disconnect -n {nqn}")
                        sleep_n_sec(2)
                self.disconnect_lvols()
                sleep_n_sec(2)
                self.sbcli_utils.delete_all_lvols()
                sleep_n_sec(2)
            self.ssh_obj.delete_all_snapshots(node=self.mgmt_nodes[0])
            sleep_n_sec(2)
            self.sbcli_utils.delete_all_storage_pools()
            sleep_n_sec(2)
            latest_util = self.get_latest_cluster_util()
            size_used = latest_util["size_used"]
            is_less_than_500mb = size_used < 500 * 1024 * 1024
            if not is_less_than_500mb:
                raise Exception("Cluster capacity more than 500MB after cleanup!!")
            for node in self.fio_node:
                self.ssh_obj.remove_dir(node, "/mnt/*")
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
                    health_check_status, device_health_check):
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
        
        # TODO: Issue during validations: Uncomment once fixed
        # https://simplyblock.atlassian.net/browse/SFAM-1930
        # https://simplyblock.atlassian.net/browse/SFAM-1929
        # offline_device_detail = self.sbcli_utils.wait_for_device_status(node_id=node_uuid,
        #                                                                 status=device_status,
        #                                                                 timeout=300)
        # for device in offline_device_detail:
        #     # if "jm" in device["jm_bdev"]:
        #     #     assert device["status"] == "JM_DEV", \
        #     #         f"JM Device {device['id']} is not in JM_DEV state. {device['status']}"
        #     # else:
        #     assert device["status"] == device_status, \
        #         f"Device {device['id']} is not in {device_status} state. Actual {device['status']}"
        #     offline_device.append(device['id'])

        # for lvol in lvol_details:
        #     assert lvol["status"] == lvol_status, \
        #         f"Lvol {lvol['id']} is not in {lvol_status} state. Actual: {lvol['status']}"

        # storage_nodes = self.sbcli_utils.get_storage_nodes()["results"]
        # health_check_status = health_check_status if isinstance(health_check_status, list)\
        #       else [health_check_status]
        # if not device_health_check:
        #     device_health_check = [True, False]
        # device_health_check = device_health_check if isinstance(device_health_check, list)\
        #       else [device_health_check]
        # for node in storage_nodes:
        #     node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
        #     if node["id"] == node_uuid and node_details[0]['status'] == "offline":
        #         node = self.sbcli_utils.wait_for_health_status(node['id'], status=health_check_status,
        #                                                        timeout=300)
        #         assert node["health_check"] in health_check_status, \
        #             f"Node {node['id']} health-check is not {health_check_status}. Actual: {node['health_check']}. Node Status: {node_details[0]['status']}"
        #     else:
        #         node = self.sbcli_utils.wait_for_health_status(node['id'], status=True,
        #                                                        timeout=300)
        #         assert node["health_check"] is True, \
        #             f"Node {node['id']} health-check is not True. Actual:  {node['health_check']}.  Node Status: {node_details[0]['status']}"
        #     if node['id'] == node_uuid:
        #         device_details = offline_device_detail
        #     else:
        #         device_details = self.sbcli_utils.get_device_details(storage_node_id=node['id'])
        #     node_details = self.sbcli_utils.get_storage_node_details(storage_node_id=node['id'])
        #     for device in device_details:
        #         device = self.sbcli_utils.wait_for_health_status(node['id'], status=device_health_check,
        #                                                             device_id=device['id'],
        #                                                             timeout=300)
        #         assert device["health_check"] in device_health_check, \
        #             f"Device {device['id']} health-check is not {device_health_check}. Actual:  {device['health_check']}"

        # TODO: Change cluster map validations
        # command = f"{self.base_cmd} sn get-cluster-map {lvol_details[0]['node_id']}"
        # lvol_cluster_map_details, _ = self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
        #                                                         command=command)
        # self.logger.info(f"LVOL Cluster map: {lvol_cluster_map_details}")
        # cluster_map_nodes, cluster_map_devices = self.common_utils.parse_lvol_cluster_map_output(lvol_cluster_map_details)
        
        # for node_id, node in cluster_map_nodes.items():
        #     if node_id == node_uuid:
        #         if isinstance(node_status, list):
        #             assert node["Reported Status"] in node_status, \
        #             f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
        #             assert node["Actual Status"] in node_status, \
        #                 f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
        #         else:
        #             assert node["Reported Status"] == node_status, \
        #             f"Node {node_id} is not in {node_status} reported state. Actual:  {node['Reported Status']}"
        #             assert node["Actual Status"] == node_status, \
        #                 f"Node {node_id} is not in {node_status} state. Actual:  {node['Actual Status']}"
                    
        #     else:
        #         assert node["Reported Status"] == "online", \
        #             f"Node {node_uuid} is not in online state. Actual: {node['Reported Status']}"
        #         assert node["Actual Status"] == "online", \
        #             f"Node {node_uuid} is not in online state. Actual: {node['Actual Status']}"

        # if device_status is not None:
        #     for device_id, device in cluster_map_devices.items():
        #         if device_id in offline_device:
        #             assert device["Reported Status"] == device_status, \
        #                 f"Device {device_id} is not in {device_status} state. Actual: {device['Reported Status']}"
        #             assert device["Actual Status"] == device_status, \
        #                 f"Device {device_id} is not in {device_status} state. Actual: {device['Actual Status']}"
        #         else:
        #             assert device["Reported Status"] == "online", \
        #                 f"Device {device_id} is not in online state. Actual: {device['Reported Status']}"
        #             assert device["Actual Status"] == "online", \
        #                 f"Device {device_id} is not in online state. {device['Actual Status']}"

    def unmount_all(self, base_path=None):
        """ Unmount all mount points """
        self.logger.info("Unmounting all mount points")
        if not base_path:
            base_path = self.mount_path
        for node in self.fio_node:
            mount_points = self.ssh_obj.get_mount_points(node=node, base_path=base_path)
            for mount_point in mount_points:
                self.logger.info(f"Unmounting {mount_point}")
                self.ssh_obj.unmount_path(node=node, device=mount_point)

    def remove_mount_dirs(self):
        """ Remove all mount point directories """
        self.logger.info("Removing all mount point directories")
        for node in self.fio_node:
            mount_dirs = self.ssh_obj.get_mount_points(node=node, base_path=self.mount_path)
            for mount_dir in mount_dirs:
                self.logger.info(f"Removing directory {mount_dir}")
                self.ssh_obj.remove_dir(node=node, dir_path=mount_dir)
    
    def disconnect_lvol(self, lvol_device):
        """Disconnects the logical volume."""
        if isinstance(self.fio_node, list):
            for node in self.fio_node:
                nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=node,
                                                            nqn_filter=lvol_device)
                for nqn in nqn_lvol:
                    self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
                    self.ssh_obj.disconnect_nvme(node=node, nqn_grep=nqn)
        else:
            nqn_lvol = self.ssh_obj.get_nvme_subsystems(node=self.fio_node,
                                                        nqn_filter=lvol_device)
            for nqn in nqn_lvol:
                self.logger.info(f"Disconnecting NVMe subsystem: {nqn}")
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=nqn)

    def disconnect_lvols(self):
        """ Disconnect all NVMe devices with NQN containing 'lvol' """
        self.logger.info("Disconnecting all NVMe devices with NQN containing 'lvol'")
        if isinstance(self.fio_node, list):  
            for node in self.fio_node:
                subsystems = self.ssh_obj.get_nvme_subsystems(node=node, nqn_filter="lvol")
                for subsys in subsystems:
                    self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                    self.ssh_obj.disconnect_nvme(node=node, nqn_grep=subsys)
        else:
            subsystems = self.ssh_obj.get_nvme_subsystems(node=self.fio_node, nqn_filter="lvol")
            for subsys in subsystems:
                self.logger.info(f"Disconnecting NVMe subsystem: {subsys}")
                self.ssh_obj.disconnect_nvme(node=self.fio_node, nqn_grep=subsys)

    def delete_snapshots(self):
        """ Delete all snapshots """
        self.logger.info("Deleting all snapshots")
        snapshots = self.ssh_obj.get_snapshots(node=self.mgmt_nodes[0])
        for snapshot in snapshots:
            self.logger.info(f"Deleting snapshot: {snapshot}")
            delete_snapshot_command = f"sbcli-lvol snapshot delete {snapshot} --force"
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=delete_snapshot_command)

    def filter_migration_tasks(self, tasks, node_id, timestamp):
        """
        Filters `device_migration` tasks for a specific node and timestamp.

        Args:
            tasks (list): List of task dictionaries from the API response.
            node_id (str): The UUID of the node to check for migration tasks.
            timestamp (int): The timestamp to filter tasks created after this time.

        Returns:
            list: List of `device_migration` tasks for the specific node created after the given timestamp.
        """
        filtered_tasks = [
            task for task in tasks
            if task['function_name'] == 'device_migration' and task['date'] > timestamp
            and (node_id is None or task['node_id'] == node_id)
        ]
        return filtered_tasks

    def validate_migration_for_node(self, timestamp, timeout, node_id=None, check_interval=60, no_task_ok=False):
        """
        Validate that all `device_migration` tasks for a specific node have completed successfully 
        and check for stuck tasks until the timeout is reached.

        Args:
            timestamp (int): The timestamp to filter tasks created after this time.
            timeout (int): Maximum time in seconds to keep checking for task completion.
            node_id (str): The UUID of the node to check for migration tasks (or None for all nodes).
            check_interval (int): Time interval in seconds to wait between checks.

        Raises:
            RuntimeError: If any migration task failed, is incomplete, is stuck, or if the timeout is reached.
        """
        start_time = datetime.now(timezone.utc)  # Aware datetime
        end_time = start_time + timedelta(seconds=timeout)

        output = None
        while output is None:
            output, _ = self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0], 
                command=f"{self.base_cmd} cluster list-tasks {self.cluster_id} --limit 0"
            )
            self.logger.info(f"Data migration output: {output}")
            if no_task_ok:
                break

        while datetime.now(timezone.utc) < end_time:
            tasks = self.sbcli_utils.get_cluster_tasks(self.cluster_id)
            filtered_tasks = self.filter_migration_tasks(tasks, node_id, timestamp)
            
            if not filtered_tasks and not no_task_ok:
                raise RuntimeError(
                    f"No migration tasks found for {'node ' + node_id if node_id else 'the cluster'} "
                    f"after the specified timestamp."
                )

            self.logger.info(f"Checking migration tasks: {filtered_tasks}")
            all_done = True
            completed_count = 0

            for task in filtered_tasks:
                try:
                    # Convert to aware datetime (UTC) for comparison
                    updated_at = datetime.fromisoformat(task['updated_at']).astimezone(timezone.utc)
                except ValueError as e:
                    self.logger.error(f"Error parsing timestamp for task {task['id']}: {e}")
                    continue

                # Check if task is stuck (not updated for more than 65 minutes)
                if datetime.now(timezone.utc) - updated_at > timedelta(minutes=65) and task["status"] != "done":
                    raise RuntimeError(
                        f"Migration task {task['id']} is stuck (last updated at {updated_at.isoformat()})."
                    )

                # Check if task is completed
                if task['status'] == 'done':
                    completed_count += 1
                else:
                    all_done = False

            # Logging the counts after each check
            total_tasks = len(filtered_tasks)
            remaining_tasks = total_tasks - completed_count
            self.logger.info(
                f"Total migration tasks: {total_tasks}, Completed: {completed_count}, Remaining: {remaining_tasks}"
            )

            # If all tasks are done, break out of the loop
            if all_done:
                self.logger.info(
                    f"All migration tasks for {'node ' + node_id if node_id else 'the cluster'} "
                    f"completed successfully without any stuck tasks."
                )
                return

            # Wait for the next check
            sleep_n_sec(check_interval)

        # If the loop exits without completing all tasks, raise a timeout error
        raise RuntimeError(
            f"Timeout reached: Not all migration tasks completed within the specified timeout of {timeout} seconds."
        )

    
    def check_core_dump(self):
        for node in self.storage_nodes:
            files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
            self.logger.info(f"Files in /etc/simplyblock: {files}")
            if "core" in files and "tmp_cores" not in files:
                cur_date = datetime.now().strftime("%Y-%m-%d")
                self.logger.info(f"Core file found on storage node {node} at {cur_date}")
        
        for node in self.mgmt_nodes:
            files = self.ssh_obj.list_files(node, "/etc/simplyblock/")
            self.logger.info(f"Files in /etc/simplyblock: {files}")
            if "core" in files and "tmp_cores" not in files:
                cur_date = datetime.now().strftime("%Y-%m-%d")
                self.logger.info(f"Core file found on management node {node} at {cur_date}")

    def get_latest_cluster_util(self):
        result = self.sbcli_utils.get_cluster_capacity()
        sorted_results = sorted(result, key=lambda x: x["date"], reverse=True)
        latest_entry = sorted_results[0]

        return latest_entry