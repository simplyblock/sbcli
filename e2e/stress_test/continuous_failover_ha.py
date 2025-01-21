from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random


def random_char(len):
    """Generate number of characters

    Args:
        len (int): NUmber of characters in string

    Returns:
        str: random string with given length
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(len))


class RandomFailoverTest(TestLvolHACluster):
    """
    Extends the TestLvolHAClusterWithClones class to add a random failover and stress testing scenario.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 20
        self.lvol_name = f"lvl{random_char(3)}"
        self.clone_name = f"cln{random_char(3)}"
        self.snapshot_name = f"snap{random_char(3)}"
        self.lvol_size = "50G"
        self.fio_size = "5G"
        self.fio_threads = []
        self.lvol_node = None
        self.clone_mount_details = {}
        self.lvol_mount_details = {}
        self.node_vs_lvol = []
        self.node = None
        self.sn_nodes = []
        self.current_outage_node = None
        self.disconnect_thread = None
        self.outage_start_time = None
        self.outage_end_time = None
        # self.outage_types = ["partial_nw", "full_nw", "spdk_crash", "graceful_shutdown"]
        self.outage_types = ["network_interrupt", "container_stop", "graceful_shutdown"]

    def create_lvols_with_fio(self, count):
        """Create lvols and start FIO with random configurations."""
        for i in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            while lvol_name in self.lvol_mount_details:
                self.lvol_name = f"lvl{random_char(3)}"
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
            self.lvol_mount_details[lvol_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(lvol_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{lvol_name}.log",
                   "snapshots": []
            }

            self.logger.info(f"Created lvol {lvol_name}.")

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=self.node)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            self.lvol_mount_details[lvol_name]["Command"] = connect_ls
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=self.node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.lvol_mount_details[lvol_name]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=self.node, device=lvol_device, fs_type=fs_type)

            # Mount and Run FIO
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=self.node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)

            # Start FIO
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.node, None, self.lvol_mount_details[lvol_name]["Mount"], self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "bs": f"{random.randint(4, 128)}K",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 4,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            sleep_n_sec(10)

    def perform_random_outage(self):
        """Perform a random outage on the cluster."""
        outage_type = random.choice(self.outage_types)
        if self.current_outage_node and self.current_outage_node in self.sn_nodes:
            self.sn_nodes.remove(self.current_outage_node)
        self.current_outage_node = random.choice(self.sn_nodes)

        self.outage_start_time = int(datetime.now().timestamp())
        node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
        node_ip = node_details[0]["mgmt_ip"]

        self.logger.info(f"Performing {outage_type} on node {self.current_outage_node}.")
        if outage_type == "graceful_shutdown":
            self.sbcli_utils.suspend_node(node_uuid=self.current_outage_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "suspended", timeout=4000)
            sleep_n_sec(10)
            self.sbcli_utils.shutdown_node(node_uuid=self.current_outage_node, expected_error_code=[503])
            self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "offline", timeout=4000)
            sleep_n_sec(300)
        elif outage_type == "container_stop":
            self.ssh_obj.stop_spdk_process(node_ip)
        elif outage_type == "network_interrupt":
            cmd = (
                'nohup sh -c "sudo nmcli dev disconnect eth0 && sleep 300 && '
                'sudo nmcli dev connect eth0" &'
            )
            def execute_disconnect():
                self.logger.info(f"Executing disconnect command on node {node_ip}.")
                self.ssh_obj.exec_command(node_ip, command=cmd)

            self.logger.info("Network stop and restart.")
            self.disconnect_thread = threading.Thread(target=execute_disconnect)
            self.disconnect_thread.start()
        
        return outage_type
    
    def restart_nodes_after_failover(self, outage_type):
        """Perform steps for node restart."""
        self.logger.info(f"Performing/Waiting for {outage_type} restart on node {self.current_outage_node}.")
        if outage_type == "graceful_shutdown":
            self.sbcli_utils.restart_node(node_uuid=self.current_outage_node, expected_error_code=[503])
        elif outage_type == "network_interrupt":
            self.disconnect_thread.join()
        self.sbcli_utils.wait_for_storage_node_status(self.current_outage_node, "online", timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)
        self.outage_end_time = int(datetime.now().timestamp())
            

    def create_snapshots_and_clones(self):
        """Create snapshots and clones during an outage."""
        for _ in range(5):
            lvol = random.choice(list(self.lvol_mount_details.keys()))
            snapshot_name = f"snap_{lvol}"
            self.ssh_obj.add_snapshot(self.node, self.lvol_mount_details[lvol]["ID"], snapshot_name)
            snapshot_id = self.ssh_obj.get_snapshot_id(self.node, snapshot_name)
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_id)
            clone_name = f"clone_{lvol}"
            self.ssh_obj.add_clone(self.node, snapshot_id, clone_name)
            self.clone_mount_details[clone_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(clone_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "Log": f"{self.log_path}/{clone_name}.log",
                   "snapshot": snapshot_id
            }

            self.logger.info(f"Created clone {clone_name}.")

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)

            initial_devices = self.ssh_obj.get_devices(node=self.node)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=self.mgmt_nodes[0], command=connect_str)

            self.clone_mount_details[clone_name]["Command"] = connect_ls
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=self.node)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.clone_mount_details[clone_name]["Device"] = lvol_device

            # Mount and Run FIO
            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=self.node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[clone_name]["Mount"] = mount_point

            # Start FIO
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(self.node, None, self.clone_mount_details[clone_name]["Mount"], self.clone_mount_details[clone_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{clone_name}_fio",
                    "rw": "randrw",
                    "bs": f"{random.randint(4, 128)}K",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 4,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            self.logger.info(f"Created snapshot {snapshot_name} and clone {clone_name}.")

    def delete_random_lvols(self, count):
        """Delete random lvols during an outage."""
        for lvol in random.sample(list(self.lvol_mount_details.keys()), count):
            self.logger.info(f"Deleting lvol {lvol}.")
            snapshots = self.lvol_mount_details[lvol]["snapshots"]
            for clone_name, clone_details in self.clone_mount_details.items():
                if clone_details["snapshot"] in snapshots:
                    self.ssh_obj.kill_processes(self.node, process_name=clone_name)
                    self.ssh_obj.unmount_path(self.node, f"/mnt/{clone_name}")
                    self.ssh_obj.remove_dir(self.node, dir_path=f"/mnt/{clone_name}")
                    self.sbcli_utils.delete_lvol(clone_name)
                    del self.clone_mount_details[clone_name]
            for snapshot in snapshots:
                self.ssh_obj.delete_snapshot(self.node, snapshot_id=snapshot)

            self.ssh_obj.kill_processes(self.node, process_name=lvol)
            self.ssh_obj.unmount_path(self.node, f"/mnt/{lvol}")
            self.sbcli_utils.delete_lvol(lvol)
            self.ssh_obj.remove_dir(self.node, dir_path=f"/mnt/{lvol}")
            del self.lvol_mount_details[lvol]

    def perform_failover_during_outage(self):
        """Perform failover during an outage and manage lvols, clones, and snapshots."""
        self.logger.info("Performing failover during outage.")

        # Randomly select a node and outage type for failover
        self.sn_nodes.append(self.current_outage_node)
        outage_type = self.perform_random_outage()

        self.delete_random_lvols(5)

        self.logger.info("Creating 5 new lvols, clones, and snapshots.")
        self.create_lvols_with_fio(5)
        self.create_snapshots_and_clones()

        self.logger.info("Failover during outage completed.")
        self.restart_nodes_after_failover(outage_type)

    def run(self):
        """Main execution loop for the random failover test."""
        self.logger.info("Starting random failover test.")
        self.node = self.mgmt_nodes[0]
        iteration = 1

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        self.create_lvols_with_fio(self.total_lvols)
        storage_nodes = self.sbcli_utils.get_storage_nodes()

        for result in storage_nodes['results']:
            if result["is_secondary_node"] is False:
                self.sn_nodes.append(result["uuid"])
        
        while True:
            outage_type = self.perform_random_outage()
            self.create_lvols_with_fio(5)
            self.delete_random_lvols(5)
            self.create_snapshots_and_clones()
            self.restart_nodes_after_failover(outage_type)
            self.logger.info("Waiting for fallback.")
            sleep_n_sec(1000)
            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            # Validate I/O stats during and after failover
            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )
            self.validate_migration_for_node(self.outage_start_time, 4000, None)

            # Perform failover and manage resources during outage
            self.perform_failover_during_outage()
            sleep_n_sec(1000)
            time_duration = self.common_utils.calculate_time_duration(
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time
            )

            # Validate I/O stats during and after failover
            self.common_utils.validate_io_stats(
                cluster_id=self.cluster_id,
                start_timestamp=self.outage_start_time,
                end_timestamp=self.outage_end_time,
                time_duration=time_duration
            )
            self.validate_migration_for_node(self.outage_start_time, 4000, None)
            self.common_utils.manage_fio_threads(self.node, self.fio_threads, timeout=10000)

            self.logger.info(f"Failover iteration {iteration} complete.")
            iteration += 1
        # self.logger.info("Complete outage scenario complete")
