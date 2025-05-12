from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
from stress_test.continuous_failover_ha_multi_client import RandomMultiClientFailoverTest
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random
import os


def generate_random_sequence(length):
    letters = string.ascii_uppercase
    numbers = string.digits
    all_chars = letters + numbers
    first_char = random.choice(letters)
    remaining_chars = ''.join(random.choices(all_chars, k=length - 1))
    return first_char + remaining_chars


class RandomMultiClientMultiFailoverTest(RandomMultiClientFailoverTest):
    """
    Extended for N+K configuration: performs K parallel outages (K=self.npcs),
    skipping secondary outages. All existing logic for lvols, clones, fio continues as-is.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 20
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
        self.current_outage_nodes = []
        self.snapshot_names = []
        self.disconnect_thread = None
        self.outage_start_time = None
        self.outage_end_time = None
        self.node_vs_lvol = {}
        self.sn_nodes_with_sec = []
        self.lvol_node = ""
        self.secondary_outage = False
        self.lvols_without_sec_connect = []
        self.test_name = "n_plus_k_failover_multi_client_ha"
        self.outage_types = [
            "container_stop",
            "graceful_shutdown",
            "interface_partial_network_interrupt",
            "interface_full_network_interrupt"
        ]
        self.blocked_ports = None
        self.outage_log_file = os.path.join("logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self._initialize_outage_log()

    def _initialize_outage_log(self):
        with open(self.outage_log_file, 'w') as log:
            log.write("Timestamp,Node,Outage_Type,Event\n")

    def log_outage_event(self, node, outage_type, event):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(self.outage_log_file, 'a') as log:
            log.write(f"{timestamp},{node},{outage_type},{event}\n")

    def perform_n_plus_k_outages(self):
        """
        Perform K (self.npcs) parallel outages as part of N+K configuration.
        Ensure only primary nodes are selected for outage.
        """
        primary_nodes = [node for node in self.sn_nodes if not self.sbcli_utils.is_secondary_node(node)]

        if len(primary_nodes) < self.npcs:
            raise Exception(f"Not enough primary nodes to perform {self.npcs} outages. Found only {len(primary_nodes)}.")

        outage_nodes = random.sample(primary_nodes, k=self.npcs)
        outage_combinations = []

        for node in outage_nodes:
            outage_type = random.choice(self.outage_types)
            node_details = self.sbcli_utils.get_storage_node_details(node)
            node_ip = node_details[0]["mgmt_ip"]
            node_rpc_port = node_details[0]["rpc_port"]

            self.logger.info(f"Performing {outage_type} on primary node {node}.")
            self.log_outage_event(node, outage_type, "Outage started")

            if outage_type == "container_stop":
                self.ssh_obj.stop_spdk_process(node_ip, node_rpc_port)
            elif outage_type == "graceful_shutdown":
                self._graceful_shutdown_node(node)
            elif outage_type == "interface_partial_network_interrupt":
                self._disconnect_partial_interface(node, node_ip)
            elif outage_type == "interface_full_network_interrupt":
                self._disconnect_full_interface(node, node_ip)

            outage_combinations.append((node, outage_type))
            self.current_outage_nodes.append(node)

        self.outage_start_time = int(datetime.now().timestamp())
        return outage_combinations

    def _graceful_shutdown_node(self, node):
        try:
            self.sbcli_utils.suspend_node(node_uuid=node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(node, "suspended", timeout=1000)
            self.sbcli_utils.shutdown_node(node_uuid=node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(node, "offline", timeout=1000)
        except Exception as e:
            self.logger.error(f"Failed graceful shutdown for node {node}: {str(e)}")

    def _disconnect_partial_interface(self, node, node_ip):
        active_interfaces = [nic["if_name"] for nic in self.sbcli_utils.get_storage_node_details(node)[0]["data_nics"]]
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, 600)
        )
        self.disconnect_thread.start()

    def _disconnect_full_interface(self, node, node_ip):
        active_interfaces = self.ssh_obj.get_active_interfaces(node_ip)
        self.disconnect_thread = threading.Thread(
            target=self.ssh_obj.disconnect_all_active_interfaces,
            args=(node_ip, active_interfaces, 600)
        )
        self.disconnect_thread.start()

    def delete_random_lvols(self, count):
        """Delete random lvols during an outage, skipping lvols on any outage node."""
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items()
            if node not in self.current_outage_nodes for lvol in lvols
        ]
        if len(available_lvols) < count:
            self.logger.warning("Not enough lvols available to delete the requested count.")
            count = len(available_lvols)

        for lvol in random.sample(available_lvols, count):
            self.logger.info(f"Deleting lvol {lvol}")
            snapshots = self.lvol_mount_details[lvol]["snapshots"]
            to_delete = []

            # Handle dependent clones
            for clone_name, clone_details in self.clone_mount_details.items():
                if clone_details["snapshot"] in snapshots:
                    self.common_utils.validate_fio_test(clone_details["Client"], clone_details["Log"])
                    self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                    fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(clone_details["Client"], pid=pid)
                    self.ssh_obj.unmount_path(clone_details["Client"], f"/mnt/{clone_name}")
                    self.ssh_obj.remove_dir(clone_details["Client"], dir_path=f"/mnt/{clone_name}")
                    self.disconnect_lvol(clone_details['ID'])
                    self.sbcli_utils.delete_lvol(clone_name)
                    if clone_name in self.lvols_without_sec_connect:
                        self.lvols_without_sec_connect.remove(clone_name)
                    to_delete.append(clone_name)

            for del_key in to_delete:
                del self.clone_mount_details[del_key]

            # Delete snapshots
            for snapshot in snapshots:
                snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot)
                self.ssh_obj.delete_snapshot(self.mgmt_nodes[0], snapshot_id=snapshot_id)
                self.snapshot_names.remove(snapshot)

            # Stop FIO and cleanup lvol
            self.common_utils.validate_fio_test(self.lvol_mount_details[lvol]["Client"], self.lvol_mount_details[lvol]["Log"])
            self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=False)
            fio_pids = self.ssh_obj.find_process_name(self.lvol_mount_details[lvol]["Client"], f"{lvol}_fio", return_pid=True)
            for pid in fio_pids:
                self.ssh_obj.kill_processes(self.lvol_mount_details[lvol]["Client"], pid=pid)
            self.ssh_obj.unmount_path(self.lvol_mount_details[lvol]["Client"], f"/mnt/{lvol}")
            self.ssh_obj.remove_dir(self.lvol_mount_details[lvol]["Client"], dir_path=f"/mnt/{lvol}")
            self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
            self.sbcli_utils.delete_lvol(lvol)
            if lvol in self.lvols_without_sec_connect:
                self.lvols_without_sec_connect.remove(lvol)
            del self.lvol_mount_details[lvol]
            for _, lvols in self.node_vs_lvol.items():
                if lvol in lvols:
                    lvols.remove(lvol)
                    break
        sleep_n_sec(60)

    def create_snapshots_and_clones(self):
        """Create snapshots and clones during an outage, avoiding lvols on outage nodes."""
        self.int_lvol_size += 1
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items()
            if node not in self.current_outage_nodes for lvol in lvols
        ]
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
                if "(False," in output or "(False," in error:
                    raise Exception(output or error)
            except Exception as e:
                self.logger.warning(f"Snapshot creation failed: {e}")
                continue

            self.snapshot_names.append(snapshot_name)
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_name)

            clone_name = f"clone_{generate_random_sequence(15)}"
            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)
            try:
                self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)
            except Exception as e:
                self.logger.warning(f"Clone creation failed: {e}")
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
                "Client": client
            }

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)
            self.clone_mount_details[clone_name]["Command"] = connect_ls
            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
                if error:
                    self.logger.warning(f"Clone connect failed: {error}")
                    continue

            final_devices = self.ssh_obj.get_devices(node=client)
            lvol_device = next((f"/dev/{d.strip()}" for d in final_devices if d not in initial_devices), None)
            if not lvol_device:
                raise LvolNotConnectException("Clone device not found")
            self.clone_mount_details[clone_name]["Device"] = lvol_device

            if fs_type == "xfs":
                self.ssh_obj.clone_mount_gen_uuid(client, lvol_device)

            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.clone_mount_details[clone_name]["Mount"] = mount_point

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, mount_point, self.clone_mount_details[clone_name]["Log"]),
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
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

            self.logger.info(f"Created snapshot {snapshot_name} and clone {clone_name}")
            # self.sbcli_utils.resize_lvol(self.lvol_mount_details[lvol]["ID"], f"{self.int_lvol_size}G")
            # self.sbcli_utils.resize_lvol(self.clone_mount_details[clone_name]["ID"], f"{self.int_lvol_size}G")


    def run(self):
        """Main N+K failover test loop. Performs lvol creation, fio, clone/snapshot, and multiple outages."""
        self.logger.info("Starting N+K failover test.")
        iteration = 1

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.create_lvols_with_fio(self.total_lvols)

        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])

        sleep_n_sec(30)

        while True:
            validation_thread = threading.Thread(target=self.validate_iostats_continuously, daemon=True)
            validation_thread.start()

            if iteration > 1:
                self.restart_fio(iteration=iteration)

            outage_events = self.perform_n_plus_k_outages()

            self.delete_random_lvols(5)
            self.create_lvols_with_fio(5)
            self.create_snapshots_and_clones()
            sleep_n_sec(280)

            for node, outage_type in outage_events:
                self.current_outage_node = node
                self.restart_nodes_after_failover(outage_type)

            self.logger.info("Waiting for fallback recovery.")
            sleep_n_sec(100)

            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            self.check_core_dump()

            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )

            no_task_ok = outage_type in {"partial_nw", "partial_nw_single_port", "lvol_disconnect_primary"}
            # for node, outage_type in outage_events:
            #     if not self.sbcli_utils.is_secondary_node(node):
            self.validate_migration_for_node(self.outage_start_time, 2000, None, 60, no_task_ok=no_task_ok)

            for clone, clone_details in self.clone_mount_details.items():
                self.common_utils.validate_fio_test(clone_details["Client"], clone_details["Log"])

            for lvol, lvol_details in self.lvol_mount_details.items():
                self.common_utils.validate_fio_test(lvol_details["Client"], lvol_details["Log"])

            self.logger.info(f"N+K failover iteration {iteration} complete.")
            iteration += 1
