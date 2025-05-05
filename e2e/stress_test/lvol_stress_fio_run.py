from utils.common_utils import sleep_n_sec
from logger_config import setup_logger
from datetime import datetime
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random
import os


def random_char(len):
    """Generate number of characters

    Args:
        len (int): NUmber of characters in string

    Returns:
        str: random string with given length
    """
    return ''.join(random.choice(string.ascii_letters) for _ in range(len))


class TestStressLvolCloneClusterFioRun(TestLvolHACluster):
    """
    Stress testing scenario without failover
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 10
        self.lvol_name = f"lvl{random_char(3)}"
        self.clone_name = f"cln{random_char(3)}"
        self.snapshot_name = f"snap{random_char(3)}"
        self.lvol_size = "10G"
        self.fio_size = "1G"
        self.fio_threads = []
        self.clone_mount_details = {}
        self.lvol_mount_details = {}
        self.node_vs_lvol = []
        self.sn_nodes = []
        self.snapshot_names = []
        self.node_vs_lvol = {}
        self.sn_nodes_with_sec = []
        self.test_name = "lvol_clone_stress_fio"


    def create_lvols_with_fio(self, count):
        """Create lvols and start FIO with random configurations."""
        for i in range(count):
            client = random.choice(self.fio_node)
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([False, False])
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
            )
            self.lvol_mount_details[lvol_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(lvol_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_path}/{lvol_name}.log",
                   "snapshots": [],
                   "Client": client,
                   "iolog_base_path": f"{self.log_path}/{lvol_name}_fio_iolog"
            }

            self.logger.info(f"Created lvol {lvol_name}.")

            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol_name]["ID"])[0]["node_id"]
            
            if lvol_node_id in self.node_vs_lvol:
                self.node_vs_lvol[lvol_node_id].append(lvol_name)
            else:
                self.node_vs_lvol[lvol_node_id] = [lvol_name]

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)

            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=client, command=connect_str)

            self.lvol_mount_details[lvol_name]["Command"] = connect_ls
            sleep_n_sec(3)
            final_devices = self.ssh_obj.get_devices(node=client)
            lvol_device = None
            for device in final_devices:
                if device not in initial_devices:
                    lvol_device = f"/dev/{device.strip()}"
                    break
            if not lvol_device:
                raise LvolNotConnectException("LVOL did not connect")
            self.lvol_mount_details[lvol_name]["Device"] = lvol_device
            self.ssh_obj.format_disk(node=client, device=lvol_device, fs_type=fs_type)

            # Mount and Run FIO
            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{lvol_name}_fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/{lvol_name}_fio_iolog"])

            sleep_n_sec(5)

            # Start FIO
            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client, None, self.lvol_mount_details[lvol_name]["Mount"], self.lvol_mount_details[lvol_name]["Log"]),
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

    def create_snapshots_and_clones(self):
        """Create snapshots and clones during an outage."""
        # Filter lvols on nodes that are not in outage
        available_lvols = [
            lvol for _, lvols in self.node_vs_lvol.items() for lvol in lvols
        ]
        if not available_lvols:
            self.logger.warning("No available lvols to create snapshots and clones.")
            return
        for _ in range(5):
            lvol = random.choice(available_lvols)
            snapshot_name = f"snap_{lvol}"
            temp_name = f"{lvol}_{random_char(2)}"
            if snapshot_name in self.snapshot_names:
                snapshot_name = f"{snapshot_name}_{temp_name}"
            self.ssh_obj.add_snapshot(self.mgmt_nodes[0], self.lvol_mount_details[lvol]["ID"], snapshot_name)
            self.snapshot_names.append(snapshot_name)
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_name)
            clone_name = f"clone_{lvol}"
            if clone_name in list(self.clone_mount_details):
                clone_name = f"{clone_name}_{temp_name}"
            snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot_name)
            self.ssh_obj.add_clone(self.mgmt_nodes[0], snapshot_id, clone_name)
            client = self.lvol_mount_details[lvol]["Client"]
            self.clone_mount_details[clone_name] = {
                   "ID": self.sbcli_utils.get_lvol_id(clone_name),
                   "Command": None,
                   "Mount": None,
                   "Device": None,
                   "MD5": None,
                   "Log": f"{self.log_path}/{clone_name}.log",
                   "snapshot": snapshot_name,
                   "Client": client,
                   "iolog_base_path": f"{self.log_path}/{clone_name}_fio_iolog"
            }

            self.logger.info(f"Created clone {clone_name}.")

            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=clone_name)

            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                self.ssh_obj.exec_command(node=client, command=connect_str)

            self.clone_mount_details[clone_name]["Command"] = connect_ls
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
            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.clone_mount_details[clone_name]["Mount"] = mount_point
            
            sleep_n_sec(10)

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/{clone_name}_fio_iolog"])

            sleep_n_sec(4)

            # Start FIO
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

    def delete_random_lvols(self, count):
        """Delete random lvols"""
        available_lvols = [
            lvol for _, lvols in self.node_vs_lvol.items() for lvol in lvols
        ]
        if len(available_lvols) < count:
            self.logger.warning("Not enough lvols available to delete the requested count.")
            count = len(available_lvols)

        for lvol in random.sample(available_lvols, count):
            self.logger.info(f"Deleting lvol {lvol}.")
            snapshots = self.lvol_mount_details[lvol]["snapshots"]
            to_delete = []
            client = self.lvol_mount_details[lvol]["Client"]
            for clone_name, clone_details in self.clone_mount_details.items():
                if clone_details["snapshot"] in snapshots:
                    self.common_utils.validate_fio_test(client,
                                                        log_file=clone_details["Log"])
                    self.ssh_obj.find_process_name(client, f"{clone_name}_fio", return_pid=False)
                    fio_pids = self.ssh_obj.find_process_name(client, f"{clone_name}_fio", return_pid=True)
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(client, pid=pid)
                    attempt = 1
                    while len(fio_pids) > 2:
                        self.ssh_obj.find_process_name(client, f"{clone_name}_fio", return_pid=False)
                        fio_pids = self.ssh_obj.find_process_name(client, f"{clone_name}_fio", return_pid=True)
                        if attempt >= 30:
                            raise Exception("FIO not killed on clone")
                        attempt += 1
                        sleep_n_sec(10)
                        
                    self.ssh_obj.unmount_path(client, f"/mnt/{clone_name}")
                    self.ssh_obj.remove_dir(client, dir_path=f"/mnt/{clone_name}")
                    self.disconnect_lvol(clone_details['ID'])
                    self.sbcli_utils.delete_lvol(clone_name)
                    to_delete.append(clone_name)
                    self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone_name}_fio*"])
                    self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone_name}_fio_iolog*"])
            for del_key in to_delete:
                del self.clone_mount_details[del_key]
            for snapshot in snapshots:
                snapshot_id = self.ssh_obj.get_snapshot_id(self.mgmt_nodes[0], snapshot)
                self.ssh_obj.delete_snapshot(self.mgmt_nodes[0], snapshot_id=snapshot_id)
                self.snapshot_names.remove(snapshot)

            self.common_utils.validate_fio_test(client,
                                                log_file=self.lvol_mount_details[lvol]["Log"])
            self.ssh_obj.find_process_name(client, f"{lvol}_fio", return_pid=False)
            fio_pids = self.ssh_obj.find_process_name(client, f"{lvol}_fio", return_pid=True)
            for pid in fio_pids:
                self.ssh_obj.kill_processes(client, pid=pid)
            attempt = 1
            while len(fio_pids) > 2:
                self.ssh_obj.find_process_name(client, f"{lvol}_fio", return_pid=False)
                fio_pids = self.ssh_obj.find_process_name(client, f"{lvol}_fio", return_pid=True)
                if attempt >= 30:
                    raise Exception("FIO not killed on lvols")
                attempt += 1
                sleep_n_sec(10)

            self.ssh_obj.unmount_path(client, f"/mnt/{lvol}")
            self.ssh_obj.remove_dir(client, dir_path=f"/mnt/{lvol}")
            self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
            self.sbcli_utils.delete_lvol(lvol)
            self.ssh_obj.delete_files(self.lvol_mount_details["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
            self.ssh_obj.delete_files(self.lvol_mount_details["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])
            del self.lvol_mount_details[lvol]
            for _, lvols in self.node_vs_lvol.items():
                if lvol in lvols:
                    lvols.remove(lvol)
                    break
    
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


    def run(self):
        """Main execution loop for the random failover test."""
        self.logger.info("Starting random failover test.")
        iteration = 1

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)

        self.create_lvols_with_fio(self.total_lvols)
        storage_nodes = self.sbcli_utils.get_storage_nodes()

        for result in storage_nodes['results']:
            if result['is_secondary_node'] is False:
                self.sn_nodes.append(result["uuid"])
            self.sn_nodes_with_sec.append(result["uuid"])
        
        sleep_n_sec(30)
        
        while True:
            validation_thread = threading.Thread(target=self.validate_iostats_continuously, daemon=True)
            validation_thread.start()
            sleep_n_sec(600)
            self.delete_random_lvols(7)
            self.create_lvols_with_fio(5)
            self.create_snapshots_and_clones()

            self.common_utils.manage_fio_threads(
                node=self.fio_node,
                threads=self.fio_threads,
                timeout=100000
            )

            for clone, clone_details in self.clone_mount_details.items():
                self.common_utils.validate_fio_test(clone_details["Client"],
                                                    log_file=clone_details["Log"])
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/local-{clone}_fio*"])
                self.ssh_obj.delete_files(clone_details["Client"], [f"{self.log_path}/{clone}_fio_iolog*"])
            
            for lvol, lvol_details in self.lvol_mount_details.items():
                self.common_utils.validate_fio_test(lvol_details["Client"],
                                                    log_file=lvol_details["Log"])
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
                self.ssh_obj.delete_files(lvol_details["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])
                

            self.logger.info(f"Iteration {iteration} complete.")
            iteration += 1
