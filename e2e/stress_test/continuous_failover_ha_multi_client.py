from utils.common_utils import sleep_n_sec
from datetime import datetime
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random
import os


def generate_random_sequence(length):
    letters = string.ascii_uppercase  # A-Z
    numbers = string.digits  # 0-9
    all_chars = letters + numbers  # Allowed characters

    first_char = random.choice(letters)  # First character must be a letter
    remaining_chars = ''.join(random.choices(all_chars, k=length-1))  # Next 14 characters

    return first_char + remaining_chars

class RandomMultiClientFailoverTest(TestLvolHACluster):
    """
    Extends the TestLvolHAClusterWithClones class to add a random failover and stress testing scenario.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 30
        self.lvol_name = f"lvl{generate_random_sequence(15)}"
        self.clone_name = f"cln{generate_random_sequence(15)}"
        self.snapshot_name = f"snap{generate_random_sequence(15)}"
        self.lvol_size = "10G"
        self.int_lvol_size = 10
        self.fio_size = "1G"
        self.fio_threads = []
        self.clone_mount_details = {}
        self.lvol_mount_details = {}
        self.sn_nodes = []
        self.current_outage_node = None
        self.snapshot_names = []
        self.disconnect_thread = None
        self.outage_start_time = None
        self.outage_end_time = None
        self.node_vs_lvol = {}
        self.sn_nodes_with_sec = []
        self.sn_primary_secondary_map = {}
        self.lvol_node = ""
        self.secondary_outage = False
        self.lvols_without_sec_connect = []
        self.test_name = "continuous_random_failover_multi_client_ha"
        # self.outage_types = ["interface_full_network_interrupt", interface_partial_network_interrupt,
        #                       "partial_nw", "partial_nw_single_port",
        #                       "port_network_interrupt", "container_stop", "graceful_shutdown",
        #                       "lvol_disconnect_primary"]
        # self.outage_types = ["graceful_shutdown", "container_stop", "interface_full_network_interrupt",
        #                      "interface_partial_network_interrupt",
        #                      "partial_nw"]
        self.outage_types = ["graceful_shutdown", "container_stop", "interface_full_network_interrupt",
                             "interface_partial_network_interrupt"]
        self.blocked_ports = None
        self.outage_log_file = os.path.join("logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self._initialize_outage_log()

    def _initialize_outage_log(self):
        """Create or initialize the outage log file."""
        with open(self.outage_log_file, 'w') as log:
            log.write("Timestamp,Node,Outage_Type,Event\n")

    def log_outage_event(self, node, outage_type, event):
        """Log an outage event to the outage log file.

        Args:
            node (str): Node UUID or IP where the event occurred.
            outage_type (str): Type of outage (e.g., port_network_interrupt, container_stop, graceful_shutdown).
            event (str): Event description (e.g., 'Outage started', 'Node restarted').
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.outage_log_file, 'a') as log:
            log.write(f"{timestamp},{node},{outage_type},{event}\n")


    def create_lvols_with_fio(self, count):
        """Create lvols and start FIO with random configurations."""
        for i in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([False, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            while lvol_name in self.lvol_mount_details:
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            self.logger.info(f"Creating lvol with Name: {lvol_name}, fs type: {fs_type}, crypto: {is_crypto}")
            try:
                if self.current_outage_node:
                    skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] == self.current_outage_node]
                    skip_nodes.append(self.current_outage_node)
                    host_id = [node for node in self.sn_nodes_with_sec if node not in skip_nodes]
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                        host_id=host_id[0]
                    )
                else:
                    self.sbcli_utils.add_lvol(
                        lvol_name=lvol_name,
                        pool_name=self.pool_name,
                        size=self.lvol_size,
                        crypto=is_crypto,
                        key1=self.lvol_crypt_keys[0],
                        key2=self.lvol_crypt_keys[1],
                    )
            except Exception as e:
                self.logger.warning(f"Lvol creation fails with {str(e)}. Retrying with different name.")
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
                try:
                    if self.current_outage_node:
                        skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] == self.current_outage_node]
                        skip_nodes.append(self.current_outage_node)
                        host_id = [node for node in self.sn_nodes_with_sec if node not in skip_nodes]
                        self.logger.info(f"Skipping Nodes: {skip_nodes}")
                        self.logger.info(f"Host Nodes: {host_id}")
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name,
                            pool_name=self.pool_name,
                            size=self.lvol_size,
                            crypto=is_crypto,
                            key1=self.lvol_crypt_keys[0],
                            key2=self.lvol_crypt_keys[1],
                            host_id=host_id[0]
                        )
                    else:
                        self.sbcli_utils.add_lvol(
                            lvol_name=lvol_name,
                            pool_name=self.pool_name,
                            size=self.lvol_size,
                            crypto=is_crypto,
                            key1=self.lvol_crypt_keys[0],
                            key2=self.lvol_crypt_keys[1],
                        )
                except Exception as exp:
                    self.logger.warning(f"Retry Lvol creation fails with {str(exp)}.")
                    continue

            self.lvol_mount_details[lvol_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(lvol_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{lvol_name}.log",
                   "snapshots": [],
                   "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog"
            }

            self.logger.info(f"Created lvol {lvol_name}.")

            sleep_n_sec(3)
            
            self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                      command=f"{self.base_cmd} lvol list")

            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol_name]["ID"])[0]["node_id"]
            
            if lvol_node_id in self.node_vs_lvol:
                self.node_vs_lvol[lvol_node_id].append(lvol_name)
            else:
                self.node_vs_lvol[lvol_node_id] = [lvol_name]

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            self.lvol_mount_details[lvol_name]["Command"] = connect_ls

            client_node = random.choice(self.fio_node)
            self.lvol_mount_details[lvol_name]["Client"] = client_node

            # if self.secondary_outage:
            #     connect_ls = [connect_ls[0]]
            #     self.lvols_without_sec_connect.append(lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=client_node)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client_node, command=connect_str)
                if error:
                    lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=self.lvol_mount_details[lvol_name]["ID"])
                    nqn = lvol_details[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client_node, nqn_grep=nqn)
                    self.logger.info(f"Connecting lvol {lvol_name} has error: {error}. Disconnect all connections for that lvol and cleaning that lvol!!")
                    self.sbcli_utils.delete_lvol(lvol_name=lvol_name)
                    sleep_n_sec(30)
                    del self.lvol_mount_details[lvol_name]
                    self.node_vs_lvol[lvol_node_id].remove(lvol_name)
                    continue

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client_node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.lvol_mount_details[lvol_name]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=client_node, device=lvol_device, fs_type=fs_type)

            # Mount and Run FIO
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/{lvol_name}_fio_iolog"])

            sleep_n_sec(5)

            # Start FIO
            # fio_thread = threading.Thread(
            #     target=self.ssh_obj.run_fio_test,
            #     args=(client_node, None, self.lvol_mount_details[lvol_name]["Mount"], self.lvol_mount_details[lvol_name]["Log"]),
            #     kwargs={
            #         "size": self.fio_size,
            #         "name": f"{lvol_name}_fio",
            #         "rw": "randrw",
            #         "bs": f"{2 ** random.randint(2, 7)}K",
            #         "nrfiles": 16,
            #         "iodepth": 1,
            #         "numjobs": 5,
            #         "time_based": True,
            #         "runtime": 2000,
            #         "log_avg_msec": 1000,
            #         "iolog_file": self.lvol_mount_details[lvol_name]["iolog_base_path"],
            #         "debug": True,
            #     },
            # )
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, self.lvol_mount_details[lvol_name]["Mount"], self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.lvol_mount_details[lvol_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            sleep_n_sec(10)

    def perform_random_outage(self):
        """Perform a random outage on the cluster."""
        random.shuffle(self.outage_types)
        random.shuffle(self.sn_nodes)
        outage_type = self.outage_types[0]
        self.current_outage_node = self.sn_nodes[0]

        # if "partial_nw" in outage_type:
        #     while self.sbcli_utils.is_secondary_node(self.current_outage_node):
        #         random.shuffle(self.sn_nodes)
        #         self.current_outage_node = self.sn_nodes[0]

        self.lvol_node = self.current_outage_node
        if self.sbcli_utils.is_secondary_node(self.lvol_node):
            self.lvol_node = random.choice(list(self.node_vs_lvol.keys()))
            self.secondary_outage = True


        self.outage_start_time = int(datetime.now().timestamp())
        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]
        node_rpc_port = node_details[0]["rpc_port"]

        sleep_n_sec(120)
        for node in self.sn_nodes_with_sec:
            self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
                                      storage_node_id=node)
        
        for node in self.sn_nodes_with_sec:
            cur_node_details = self.sbcli_utils.get_storage_node_details(node)
            cur_node_ip = cur_node_details[0]["mgmt_ip"]
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=cur_node_ip,
                storage_node_id=node
            )

        self.logger.info(f"Performing {outage_type} on node {self.current_outage_node}.")
        self.log_outage_event(self.current_outage_node, outage_type, "Outage started")
        if outage_type == "graceful_shutdown":
            # self.sbcli_utils.suspend_node(node_uuid=self.current_outage_node, expected_error_code=[503])
            # self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "suspended", timeout=4000)
            # sleep_n_sec(10)
            # self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, expected_error_code=[503])
            # self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "offline", timeout=4000)
            max_retries = 10
            retry_delay = 10  # seconds
            # Retry mechanism for suspending the node
            for attempt in range(max_retries):
                try:
                    if attempt == max_retries - 1:
                        self.logger.info("[CHECK] Suspending Node via CLI as via API Fails.")
                        self.ssh_obj.suspend_node(node=self.mgmt_nodes[0],
                                                  node_id=self.current_outage_node)
                    else:
                        self.sbcli_utils.suspend_node(node_uuid=self.current_outage_node, expected_error_code=[503])
                    self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "suspended", timeout=1000)
                    break  # Exit loop if successful
                except Exception as _:
                    if attempt < max_retries - 2:
                        self.logger.info(f"Attempt {attempt + 1} failed to suspend node. Retrying in {retry_delay} seconds...")
                        sleep_n_sec(retry_delay)
                    elif attempt < max_retries - 1:
                        self.logger.info(f"Attempt {attempt + 1} failed to suspend node via API. Retrying in {retry_delay} seconds via CMD...")
                        sleep_n_sec(retry_delay)
                    else:
                        self.logger.info("Max retries reached. Failed to suspend node.")
                        raise  # Rethrow the last exception

            sleep_n_sec(10)  # Wait before shutting down

            # Retry mechanism for shutting down the node
            for attempt in range(max_retries):
                try:
                    if attempt == max_retries - 1:
                        self.logger.info("[CHECK] Shutting down Node via CLI as via API Fails.")
                        self.ssh_obj.shutdown_node(node=self.mgmt_nodes[0],
                                                   node_id=self.current_outage_node,
                                                   force=True)
                    else:
                        self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, expected_error_code=[503])
                    self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "offline", timeout=1000)
                    break  # Exit loop if successful
                except Exception as _:
                    if attempt < max_retries - 2:
                        self.logger.info(f"Attempt {attempt + 1} failed to shutdown node. Retrying in {retry_delay} seconds...")
                        sleep_n_sec(retry_delay)
                    elif attempt < max_retries - 1:
                        self.logger.info(f"Attempt {attempt + 1} failed to shutdown node via API. Retrying in {retry_delay} seconds via CMD...")
                        sleep_n_sec(retry_delay)
                    else:
                        self.logger.info("Max retries reached. Failed to shutdown node.")
                        raise  # Rethrow the last exception
        elif outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port)
        elif outage_type == "port_network_interrupt":
            # cmd = (
            #     'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 300 && '
            #     'sudo nmcli dev connect eth0" &'
            # )
            # def execute_disconnect():
            #     self.logger.info(f"Executing disconnect command on node {node_ip}.")
            #     self.ssh_obj.exec_command(node_ip, command=cmd)

            # self.logger.info("Network stop and restart.")
            # self.disconnect_thread = threading.Thread(target=execute_disconnect)
            # self.disconnect_thread.start()
            self.logger.info("Handling port based network interruption...")
            # active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
            # self.disconnect_thread = threading.Thread(
            #     target=self.ssh_obj.disconnect_all_active_interfaces,
            #     args=(node_ip, active_interfaces),
            # )
            # self.disconnect_thread.start()
            ports_to_block = [4420, 80, 8080, 5000, 2270, 2377, 7946]
            node_data_nic_ip = []

            data_nics = node_details[0]["data_nics"]
            for data_nic in data_nics:
                node_data_nic_ip.append(data_nic["ip4_address"])

            nodes_check_ports_on = self.fio_node
            nodes_check_ports_on.append(self.mgmt_nodes[0])

            self.blocked_ports = self.ssh_obj.perform_nw_outage(node_ip=node_ip, node_data_nic_ip=node_data_nic_ip,
                                                                nodes_check_ports_on=nodes_check_ports_on,
                                                                block_ports=ports_to_block, block_all_ss_ports=True)
        elif outage_type == "interface_full_network_interrupt":
            self.logger.info("Handling full interface based network interruption...")
            active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
            
            self.disconnect_thread = threading.Thread(
                target=self.ssh_obj.disconnect_all_active_interfaces,
                args=(node_ip, active_interfaces, 600),
            )
            self.disconnect_thread.start()
        elif outage_type == "interface_partial_network_interrupt":
            self.logger.info("Handling partial interface based network interruption...")
            active_interfaces = []
            data_nics = node_details[0]["data_nics"]
            for data_nic in data_nics:
                active_interfaces.append(data_nic["if_name"])
            
            self.disconnect_thread = threading.Thread(
                target=self.ssh_obj.disconnect_all_active_interfaces,
                args=(node_ip, active_interfaces, 600),
            )
            self.disconnect_thread.start()
        elif outage_type == "partial_nw":
            lvol_ports = node_details[0]["lvol_subsys_port"]
            rpc_port = node_details[0]["rpc_port"]
            if self.secondary_outage:
                lvol_ports = list(range(9090, 9090 + len(self.storage_nodes) - 1))
            if not isinstance(lvol_ports, list):
                lvol_ports = [lvol_ports]
            ports_to_block = [int(port) for port in lvol_ports]
            ports_to_block.append(4420)
            ports_to_block.append(rpc_port)
            self.blocked_ports = self.ssh_obj.perform_nw_outage(node_ip=node_ip,
                                                                block_ports=ports_to_block,
                                                                block_all_ss_ports=False)
            # lvols = self.node_vs_lvol.get(self.lvol_node, [])
            # self.logger.info(f"Picking lvols of node {self.lvol_node} for outage of node {self.current_outage_node}!!")
            # for lvol in lvols:
            #     self.ssh_obj.disconnect_lvol_node_device(node=self.fio_node, device=self.lvol_mount_details[lvol]["Device"])
            sleep_n_sec(60)
        
        elif outage_type == "partial_nw_single_port":
            lvol_ports = node_details[0]["lvol_subsys_port"]
            if self.secondary_outage:
                lvol_ports = list(range(9090, 9090 + len(self.storage_nodes) - 1))
            if not isinstance(lvol_ports, list):
                lvol_ports = [lvol_ports]
            ports_to_block = [int(port) for port in lvol_ports]
            self.blocked_ports = self.ssh_obj.perform_nw_outage(node_ip=node_ip,
                                                                block_ports=ports_to_block,
                                                                block_all_ss_ports=False)
            # lvols = self.node_vs_lvol.get(self.lvol_node, [])
            # self.logger.info(f"Picking lvols of node {self.lvol_node} for outage of node {self.current_outage_node}!!")
            # for lvol in lvols:
            #     self.ssh_obj.disconnect_lvol_node_device(node=self.fio_node, device=self.lvol_mount_details[lvol]["Device"])
            sleep_n_sec(60)
        
        elif outage_type == "lvol_disconnect_primary":
            lvols = self.node_vs_lvol.get(self.lvol_node, [])
            self.logger.info(f"Picking lvols of node {self.lvol_node} for outage of node {self.current_outage_node}!!")
            for lvol in lvols:
                self.ssh_obj.disconnect_lvol_node_device(node=self.lvol_mount_details[lvol]["Client"], device=self.lvol_mount_details[lvol]["Device"])
            
        if outage_type != "partial_nw" or outage_type != "partial_nw_single_port":
            sleep_n_sec(120)
        
        return outage_type
    
    
    def restart_nodes_after_failover(self, outage_type):
        """Perform steps for node restart."""
        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]
        self.logger.info(f"Performing/Waiting for {outage_type} restart on node {self.current_outage_node}.")
        if outage_type == "graceful_shutdown":
            max_retries = 10
            retry_delay = 10  # seconds

            # Retry mechanism for restarting the node
            for attempt in range(max_retries):
                try:
                    if attempt == max_retries - 1:
                        self.logger.info("[CHECK] Restarting Node via CLI as via API Fails.")
                        self.ssh_obj.restart_node(node=self.mgmt_nodes[0],
                                                  node_id=self.current_outage_node,
                                                  force=True)
                    else:
                        self.sbcli_utils.restart_node(node_uuid=self.current_outage_node, expected_error_code=[503])
                    self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=1000)
                    break  # Exit loop if successful
                except Exception as _:
                    if attempt < max_retries - 2:
                        self.logger.info(f"Attempt {attempt + 1} failed to restart node. Retrying in {retry_delay} seconds...")
                        sleep_n_sec(retry_delay)
                    elif attempt < max_retries - 1:
                        self.logger.info(f"Attempt {attempt + 1} failed to restart node via API. Retrying in {retry_delay} seconds via CMD...")
                        sleep_n_sec(retry_delay)
                    else:
                        self.logger.info("Max retries reached. Failed to restart node.")
                        raise  # Rethrow the last exception

        elif outage_type == "port_network_interrupt":
            # self.disconnect_thread.join()
            self.ssh_obj.remove_nw_outage(node_ip=node_ip, blocked_ports=self.blocked_ports)
            sleep_n_sec(100)
        
        elif outage_type == "partial_nw":
            self.ssh_obj.remove_nw_outage(node_ip=node_ip, blocked_ports=self.blocked_ports)
            sleep_n_sec(100)
        
        elif outage_type == "partial_nw_single_port":
            self.ssh_obj.remove_nw_outage(node_ip=node_ip, blocked_ports=self.blocked_ports)
            sleep_n_sec(100)
        
        elif outage_type == "lvol_disconnect_primary":
            lvols = self.node_vs_lvol[self.lvol_node]
            for lvol in lvols:
                connect = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol)[0]
                self.ssh_obj.exec_command(node=self.lvol_mount_details[lvol]["Client"], command=connect)

        self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=1000)
        # Log the restart event
        self.log_outage_event(self.current_outage_node, outage_type, "Node restarted")

        
        if not self.k8s_test:
            for node in self.storage_nodes:
                self.ssh_obj.restart_docker_logging(
                    node_ip=node,
                    containers=self.container_nodes[node],
                    log_dir=self.docker_logs_path,
                    test_name=self.test_name
                )
        else:
            self.runner_k8s_log.restart_logging()

        self.sbcli_utils.wait_for_health_status(self.current_outage_node, True, timeout=1000)
        self.outage_end_time = int(datetime.now().timestamp())

        if self.secondary_outage:
            for lvol in self.lvols_without_sec_connect:
                command = self.lvol_mount_details.get(lvol, self.clone_mount_details.get(lvol, None))["Command"][1]
                if command:
                    self.ssh_obj.exec_command(self.lvol_mount_details[lvol], command=command)
                else:
                    raise Exception(f"Lvol/Clone {lvol} not found to connect")
        
            self.secondary_outage = False
            self.lvols_without_sec_connect = []

        search_start_iso = datetime.fromtimestamp(self.outage_start_time - 30).isoformat(timespec='microseconds')
        search_end_iso = datetime.fromtimestamp(self.outage_end_time + 10).isoformat(timespec='microseconds')

        self.logger.info(f"Fetching dmesg logs on {node_ip} from {search_start_iso} to {search_end_iso}")

        # Get dmesg logs with ISO timestamps
        for node in self.fio_node:
            dmesg_logs = self.ssh_obj.get_dmesg_logs_within_iso_window(
                node, search_start_iso, search_end_iso
            )

        nvme_issues = [
            line for line in dmesg_logs if "nvme" in line.lower() or "connection" in line.lower()
        ]

        if nvme_issues:
            self.logger.warning(f"Potential NVMe issues detected on {node_ip}:")
            for issue in nvme_issues:
                self.logger.warning(issue)
        else:
            self.logger.info(f"No NVMe issues found on {node_ip} between {search_start_iso} and {search_end_iso}")

        if outage_type == "partial_nw" or outage_type == "partial_nw_single_port":
            sleep_n_sec(600)
            # lvols = self.node_vs_lvol[self.lvol_node]
            # for lvol in lvols:
            #     connect = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol)[0]
            #     self.ssh_obj.exec_command(node=self.fio_node, command=connect)
            # sleep_n_sec(30)

        for node in self.sn_nodes_with_sec:
            self.ssh_obj.dump_lvstore(node_ip=self.mgmt_nodes[0],
                                      storage_node_id=node)


    def create_snapshots_and_clones(self):
        """Create snapshots and clones during an outage."""
        # Filter lvols on nodes that are not in outage
        self.int_lvol_size += 1
        skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] == self.current_outage_node]
        skip_nodes.append(self.current_outage_node)
        self.logger.info(f"Skipping Nodes: {skip_nodes}")
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items() if node not in skip_nodes for lvol in lvols
        ]
        self.logger.info(f"Available Lvols: {available_lvols}")
        if not available_lvols:
            self.logger.warning("No available lvols to create snapshots and clones.")
            return
        for _ in range(3):
            random.shuffle(available_lvols)
            lvol = available_lvols[0]
            snapshot_name = f"snap_{generate_random_sequence(15)}"
            temp_name = generate_random_sequence(5)
            if snapshot_name in self.snapshot_names:
                snapshot_name = f"{snapshot_name}_{temp_name}"
            try:
                output, error = self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.lvol_mount_details[lvol]["ID"], snapshot_name)
                if "(False," in output:
                    raise Exception(output)
                if "(False," in error:
                    raise Exception(error)
            except Exception as e:
                self.logger.warning(f"Snap creation fails with {str(e)}. Retrying with different name.")
                try:
                    snapshot_name = f"snap_{lvol}"
                    temp_name = generate_random_sequence(5)
                    snapshot_name = f"{snapshot_name}_{temp_name}"
                    self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.lvol_mount_details[lvol]["ID"], snapshot_name)
                except Exception as exp:
                    self.logger.warning(f"Retry Snap creation fails with {str(exp)}.")
                    continue
                
            self.snapshot_names.append(snapshot_name)
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_name)
            clone_name = f"clone_{generate_random_sequence(15)}"
            if clone_name in list(self.clone_mount_details):
                clone_name = f"{clone_name}_{temp_name}"
            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)
            try:
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)
            except Exception as e:
                self.logger.warning(f"Clone creation fails with {str(e)}. Retrying with different name.")
                try:
                    clone_name = f"clone_{generate_random_sequence(15)}"
                    temp_name = generate_random_sequence(5)
                    clone_name = f"{clone_name}_{temp_name}"
                    self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)
                except Exception as exp:
                    self.logger.warning(f"Retry Clone creation fails with {str(exp)}.")
                    continue
            fs_type = self.lvol_mount_details[lvol]["FS"]
            client = self.lvol_mount_details[lvol]["Client"]
            self.clone_mount_details[clone_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(clone_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{clone_name}.log",
                   "snapshot": snapshot_name,
                   "Client": client,
                   "iolog_base_path": f"{self.log_path}/{clone_name}_fio_iolog"
            }

            self.logger.info(f"Created clone {clone_name}.")

            sleep_n_sec(3)

            self.ssh_obj.exec_command(node=self.mgmt_nodes[0],
                                      command=f"{self.base_cmd} lvol list")

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            self.clone_mount_details[clone_name]["Command"] = connect_ls

            # if self.secondary_outage:
            #     connect_ls = [connect_ls[0]]
            #     self.lvols_without_sec_connect.append(clone_name)

            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
                if error:
                    lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=self.clone_mount_details[clone_name]["ID"])
                    nqn = lvol_details[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)
                    self.logger.info(f"Connecting clone {clone_name} has error: {error}. Disconnect all connections for that clone!!")
                    self.sbcli_utils.delete_lvol(lvol_name=clone_name)
                    sleep_n_sec(30)
                    del self.clone_mount_details[clone_name]
                    continue

            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.clone_mount_details[clone_name]["Device"] = lvol_device

            # Mount and Run FIO
            if fs_type == "xfs":
                self.ssh_obj.clone_mount_gen_uuid(client, lvol_device)
            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.clone_mount_details[clone_name]["Mount"] = mount_point

            # clone_node_id = self.sbcli_utils.get_lvol_details(
            #     lvol_id=self.lvol_mount_details[clone_name]["ID"])[0]["node_id"]
            
            # self.node_vs_lvol[clone_node_id].append(clone_name)

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/{clone_name}_fio_iolog*"])

            sleep_n_sec(5)

            # Start FIO
            # fio_thread = threading.Thread(
            #     target=self.ssh_obj.run_fio_test,
            #     args=(client, None, self.clone_mount_details[clone_name]["Mount"], self.clone_mount_details[clone_name]["Log"]),
            #     kwargs={
            #         "size": self.fio_size,
            #         "name": f"{clone_name}_fio",
            #         "rw": "randrw",
            #         "bs": f"{2 ** random.randint(2, 7)}K",
            #         "nrfiles": 16,
            #         "iodepth": 1,
            #         "numjobs": 5,
            #         "time_based": True,
            #         "runtime": 2000,
            #         "log_avg_msec": 1000,
            #         "iolog_file": self.clone_mount_details[clone_name]["iolog_base_path"],
            #         "debug": True,
            #     },
            # )
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, self.clone_mount_details[clone_name]["Mount"], self.clone_mount_details[clone_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{clone_name}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.clone_mount_details[clone_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            self.logger.info(f"Created snapshot {snapshot_name} and clone {clone_name}.")

            # self.sbcli_utils.resize_lvol(lvol_id=self.lvol_mount_details[lvol]["ID"],
            #                              new_size=f"{self.int_lvol_size}G")
            # sleep_n_sec(10)
            # self.sbcli_utils.resize_lvol(lvol_id=self.clone_mount_details[clone_name]["ID"],
            #                              new_size=f"{self.int_lvol_size}G")
            

    def delete_random_lvols(self, count):
        """Delete random lvols during an outage."""
        skip_nodes = [node for node in self.sn_primary_secondary_map if self.sn_primary_secondary_map[node] == self.current_outage_node]
        skip_nodes.append(self.current_outage_node)
        self.logger.info(f"Skipping Nodes: {skip_nodes}")
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items() if node not in skip_nodes for lvol in lvols
        ]
        self.logger.info(f"Available Lvols: {available_lvols}")
        if len(available_lvols) < count:
            self.logger.warning("Not enough lvols available to delete the requested count.")
            count = len(available_lvols)

        for lvol in random.sample(available_lvols, count):
            self.logger.info(f"Deleting lvol {lvol}.")
            snapshots = self.lvol_mount_details[lvol]["snapshots"]
            to_delete = []
            for clone_name, clone_details in self.clone_mount_details.items():
                if clone_details["snapshot"] in snapshots:
                    self.common_utils.validate_fio_test(clone_details["Client"],
                                                        log_file=clone_details["Log"])
                    self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                    fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(clone_details["Client"], pid=pid)
                    attempt = 1
                    while len(fio_pids) > 2:
                        self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                        fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                        if attempt >= 30:
                            raise Exception("FIO not killed on clone")
                        attempt += 1
                        sleep_n_sec(10)
                    
                    self.disconnect_lvol(clone_details['ID'])
                    sleep_n_sec(10)
                    self.ssh_obj.unmount_path(clone_details["Client"], f"/mnt/{clone_name}")
                    self.ssh_obj.remove_dir(clone_details["Client"], dir_path=f"/mnt/{clone_name}")
                    self.sbcli_utils.delete_lvol(clone_name)
                    sleep_n_sec(30)
                    if clone_name in self.lvols_without_sec_connect:
                        self.lvols_without_sec_connect.remove(clone_name)
                    to_delete.append(clone_name)
                    self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone_name}_fio*"])
                    self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone_name}_fio_iolog*"])
                    self.ssh_obj.delete_files(clone_details["Client"], [f"/mnt/{clone_name}/*"])
            for del_key in to_delete:
                del self.clone_mount_details[del_key]
            for snapshot in snapshots:
                snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot)
                self.ssh_obj.delete_snapshot(self.mgmt_nodes[0], snapshot_id=snapshot_id)
                self.snapshot_names.remove(snapshot)

            self.common_utils.validate_fio_test(self.lvol_mount_details[lvol]["Client"],
                                                log_file=self.lvol_mount_details[lvol]["Log"])
            self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=False)
            fio_pids = self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=True)
            for pid in fio_pids:
                self.ssh_obj.kill_processes(self.lvol_mount_details[lvol]["Client"], pid=pid)
            attempt = 1
            while len(fio_pids) > 2:
                self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=False)
                fio_pids = self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=True)
                if attempt >= 30:
                    raise Exception("FIO not killed on lvols")
                attempt += 1
                sleep_n_sec(10)

            self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
            sleep_n_sec(10)
            self.ssh_obj.unmount_path(self.lvol_mount_details[lvol]["Client"], f"/mnt/{lvol}")
            self.ssh_obj.remove_dir(self.lvol_mount_details[lvol]["Client"], dir_path=f"/mnt/{lvol}")
            self.sbcli_utils.delete_lvol(lvol)
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"/mnt/{lvol}/*"])
            if lvol in self.lvols_without_sec_connect:
                self.lvols_without_sec_connect.remove(lvol)
            del self.lvol_mount_details[lvol]
            for _, lvols in self.node_vs_lvol.items():
                if lvol in lvols:
                    lvols.remove(lvol)
                    break
        sleep_n_sec(60)

    def perform_failover_during_outage(self):
        """Perform failover during an outage and manage lvols, clones, and snapshots."""
        self.logger.info("Performing failover during outage.")

        # Randomly select a node and outage type for failover
        for node in self.sn_nodes_with_sec:
            cur_node_details = self.sbcli_utils.get_storage_node_details(node)
            cur_node_ip = cur_node_details[0]["mgmt_ip"]
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=cur_node_ip,
                storage_node_id=node
            )
        outage_type = self.perform_random_outage()
        
        if not self.sbcli_utils.is_secondary_node(self.current_outage_node):
            self.delete_random_lvols(6)
            if not self.k8s_test:
                for node in self.storage_nodes:
                    self.ssh_obj.restart_docker_logging(
                        node_ip=node,
                        containers=self.container_nodes[node],
                        log_dir=self.docker_logs_path,
                        test_name=self.test_name
                    )
            else:
                self.runner_k8s_log.restart_logging()
            self.logger.info("Creating 5 new lvols, clones, and snapshots.")
            for node in self.sn_nodes_with_sec:
                cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                cur_node_ip = cur_node_details[0]["mgmt_ip"]
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=cur_node_ip,
                    storage_node_id=node
                )
            self.create_lvols_with_fio(3)
            if not self.k8s_test:
                for node in self.storage_nodes:
                    self.ssh_obj.restart_docker_logging(
                        node_ip=node,
                        containers=self.container_nodes[node],
                        log_dir=self.docker_logs_path,
                        test_name=self.test_name
                    )
            else:
                self.runner_k8s_log.restart_logging()
            if not self.k8s_test:
                for node in self.storage_nodes:
                    self.ssh_obj.restart_docker_logging(
                        node_ip=node,
                        containers=self.container_nodes[node],
                        log_dir=self.docker_logs_path,
                        test_name=self.test_name
                    )
            else:
                self.runner_k8s_log.restart_logging()
            self.create_snapshots_and_clones()
        else:
            self.logger.info(f"Current outage node: {self.current_outage_node} is secondary node. Skipping delete or create")

        if outage_type != "partial_nw" or outage_type != "partial_nw_single_port":
            sleep_n_sec(280)

        self.logger.info("Failover during outage completed.")
        for node in self.sn_nodes_with_sec:
            cur_node_details = self.sbcli_utils.get_storage_node_details(node)
            cur_node_ip = cur_node_details[0]["mgmt_ip"]
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=cur_node_ip,
                storage_node_id=node
            )
        self.restart_nodes_after_failover(outage_type)
        
        for node in self.sn_nodes_with_sec:
            cur_node_details = self.sbcli_utils.get_storage_node_details(node)
            cur_node_ip = cur_node_details[0]["mgmt_ip"]
            self.ssh_obj.fetch_distrib_logs(
                storage_node_ip=cur_node_ip,
                storage_node_id=node
            )

        return outage_type
    
    def validate_iostats_continuously(self):
        """Continuously validates I/O stats while FIO is running, checking every 60 seconds."""
        self.logger.info("Starting continuous I/O stats validation thread.")
        
        while True:
            try:
                start_timestamp = datetime.now().timestamp()  # Current time as start time
                end_timestamp = start_timestamp + 300  # End time is 5 minutes (300 seconds) later

                self.common_utils.validate_io_stats(
                    cluster_id=self.cluster_id,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    time_duration=None  # Not needed in this case
                )

                sleep_n_sec(300)  # Sleep for 60 seconds before the next validation
            except Exception as e:
                self.logger.error(f"Error in continuous I/O stats validation: {str(e)}")
                break  # Exit the thread on failure

    def restart_fio(self, iteration):
        """ Restart FIO on all clones and lvols """
        for lvol, lvol_details in self.lvol_mount_details.items():
            sleep_n_sec(10)

            mount_point = lvol_details["Mount"]
            log_file = f"{self.log_path}/{lvol}-{iteration}.log"
            iolog_base_path = f"{self.log_path}/{lvol}_fio_iolog_{iteration}"

            self.ssh_obj.delete_files(lvol_details["Client"], [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/local-{lvol}*"])
            self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])

            sleep_n_sec(5)
            self.lvol_mount_details[lvol]["Log"] = log_file
            self.lvol_mount_details[lvol]["iolog_base_path"] = iolog_base_path

            # Start FIO
            # fio_thread = threading.Thread(
            #     target=self.ssh_obj.run_fio_test,
            #     args=(lvol_details["Client"], None, mount_point, log_file),
            #     kwargs={
            #         "size": self.fio_size,
            #         "name": f"{lvol}_fio",
            #         "rw": "randrw",
            #         "bs": f"{2 ** random.randint(2, 7)}K",
            #         "nrfiles": 16,
            #         "iodepth": 1,
            #         "numjobs": 5,
            #         "time_based": True,
            #         "runtime": 2000,
            #         "log_avg_msec": 1000,
            #         "iolog_file": self.lvol_mount_details[lvol]["iolog_base_path"],
            #         "debug": True,
            #     },
            # )
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(lvol_details["Client"], None, mount_point, log_file),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.lvol_mount_details[lvol]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

        for clone, clone_details in self.clone_mount_details.items():
            sleep_n_sec(10)

            mount_point = clone_details["Mount"]
            log_file = f"{self.log_path}/{clone}-{iteration}.log"
            iolog_base_path = f"{self.log_path}/{clone}_fio_iolog_{iteration}"

            self.ssh_obj.delete_files(clone_details["Client"], [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone}_fio*"])
            self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone}_fio_iolog*"])

            self.clone_mount_details[clone]["Log"] = log_file
            self.clone_mount_details[clone]["iolog_base_path"] = iolog_base_path

            sleep_n_sec(5)

            # Start FIO
            # fio_thread = threading.Thread(
            #     target=self.ssh_obj.run_fio_test,
            #     args=(clone_details["Client"], None, mount_point, log_file),
            #     kwargs={
            #         "size": self.fio_size,
            #         "name": f"{clone}_fio",
            #         "rw": "randrw",
            #         "bs": f"{2 ** random.randint(2, 7)}K",
            #         "nrfiles": 16,
            #         "iodepth": 1,
            #         "numjobs": 5,
            #         "time_based": True,
            #         "runtime": 2000,
            #         "log_avg_msec": 1000,
            #         "iolog_file": self.clone_mount_details[clone]["iolog_base_path"],
            #         "debug": True,
            #     },
            # )
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(clone_details["Client"], None, mount_point, log_file),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{clone}_fio",
                    "rw": "randrw",
                    "bs": f"{2 ** random.randint(2, 7)}K",
                    "nrfiles": 16,
                    "iodepth": 1,
                    "numjobs": 5,
                    "time_based": True,
                    "runtime": 2000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.clone_mount_details[clone]["iolog_base_path"],
                },
            )            
            fio_thread.start()
            self.fio_threads.append(fio_thread)


    def run(self):
        """Main execution loop for the random failover test."""
        self.logger.info("Starting random failover test.")
        iteration = 1

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        self.create_lvols_with_fio(self.total_lvols)
        storage_nodes = self.sbcli_utils.get_storage_nodes()

        for result in storage_nodes['results']:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
            self.sn_primary_secondary_map[result["uuid"]] = result["secondary_node_id"]
        self.logger.info(f"Secondary node map: {self.sn_primary_secondary_map}")
        
        sleep_n_sec(30)
        
        while True:
            validation_thread = threading.Thread(target=self.validate_iostats_continuously, daemon=True)
            validation_thread.start()
            if iteration > 1:
                self.restart_fio(iteration=iteration)
            outage_type = self.perform_random_outage()
            if not self.sbcli_utils.is_secondary_node(self.current_outage_node):
                self.delete_random_lvols(8)
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=node,
                            containers=self.container_nodes[node],
                            log_dir=self.docker_logs_path,
                            test_name=self.test_name
                        )
                else:
                    self.runner_k8s_log.restart_logging()

                for node in self.sn_nodes_with_sec:
                    cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                    cur_node_ip = cur_node_details[0]["mgmt_ip"]
                    self.ssh_obj.fetch_distrib_logs(
                        storage_node_ip=cur_node_ip,
                        storage_node_id=node
                    )
                self.create_lvols_with_fio(6)
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=node,
                            containers=self.container_nodes[node],
                            log_dir=self.docker_logs_path,
                            test_name=self.test_name
                        )
                else:
                    self.runner_k8s_log.restart_logging()
                self.create_snapshots_and_clones()
                if not self.k8s_test:
                    for node in self.storage_nodes:
                        self.ssh_obj.restart_docker_logging(
                            node_ip=node,
                            containers=self.container_nodes[node],
                            log_dir=self.docker_logs_path,
                            test_name=self.test_name
                        )
                else:
                    self.runner_k8s_log.restart_logging()
            else:
                self.logger.info(f"Current outage node: {self.current_outage_node} is secondary node. Skipping delete and create")
            if outage_type != "partial_nw" or outage_type != "partial_nw_single_port":
                sleep_n_sec(280)
            for node in self.sn_nodes_with_sec:
                cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                cur_node_ip = cur_node_details[0]["mgmt_ip"]
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=cur_node_ip,
                    storage_node_id=node
                )
            self.restart_nodes_after_failover(outage_type)
            for node in self.sn_nodes_with_sec:
                cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                cur_node_ip = cur_node_details[0]["mgmt_ip"]
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=cur_node_ip,
                    storage_node_id=node
                )
            self.logger.info("Waiting for fallback.")
            if outage_type != "partial_nw" or outage_type != "partial_nw_single_port":
                sleep_n_sec(100)
            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            self.check_core_dump()

            # Validate I/O stats during and after failover
            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )
            no_task_ok = outage_type in {"partial_nw", "partial_nw_single_port", "lvol_disconnect_primary"}
            if not self.sbcli_utils.is_secondary_node(self.current_outage_node):
                self.validate_migration_for_node(self.outage_start_time, 2000, None, 60, no_task_ok=no_task_ok)

            for clone, clone_details in self.clone_mount_details.items():
                self.common_utils.validate_fio_test(clone_details["Client"],
                                                    log_file=clone_details["Log"])
                # self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone}_fio*"])
                # self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone}_fio_iolog*"])
            
            for lvol, lvol_details in self.lvol_mount_details.items():
                self.common_utils.validate_fio_test(lvol_details["Client"],
                                                    log_file=lvol_details["Log"])
                # self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
                # self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])

            # Perform failover and manage resources during outage
            outage_type = self.perform_failover_during_outage()
            if outage_type != "partial_nw" or outage_type != "partial_nw_single_port":
                sleep_n_sec(100)
            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            self.check_core_dump()

            # Validate I/O stats during and after failover
            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )
            no_task_ok = outage_type in {"partial_nw", "partial_nw_single_port", "lvol_disconnect_primary"}
            if not self.sbcli_utils.is_secondary_node(self.current_outage_node):
                self.validate_migration_for_node(self.outage_start_time, 2000, None, 60, no_task_ok=no_task_ok)
            
            self.common_utils.manage_fio_threads(self.fio_node, self.fio_threads, timeout=100000)

            for lvol_name, lvol_details in self.lvol_mount_details.items():
                self.common_utils.validate_fio_test(
                    lvol_details["Client"],
                    lvol_details["Log"]
                )
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/local-{lvol_name}_fio*"])
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/{lvol_name}_fio_iolog*"])
            for clone_name, clone_details in self.clone_mount_details.items():
                self.common_utils.validate_fio_test(
                    clone_details["Client"],
                    clone_details["Log"]
                )
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone_name}_fio*"])
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone_name}_fio_iolog*"])

            self.logger.info(f"Failover iteration {iteration} complete.")
            for node in self.sn_nodes_with_sec:
                cur_node_details = self.sbcli_utils.get_storage_node_details(node)
                cur_node_ip = cur_node_details[0]["mgmt_ip"]
                self.ssh_obj.fetch_distrib_logs(
                    storage_node_ip=cur_node_ip,
                    storage_node_id=node
                )
            iteration += 1
