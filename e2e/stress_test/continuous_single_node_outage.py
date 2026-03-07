from utils.common_utils import sleep_n_sec
from datetime import datetime
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from exceptions.custom_exception import LvolNotConnectException
import threading
import string
import random
import os


generated_sequences = set()

def generate_random_sequence(length):
    letters = string.ascii_uppercase
    numbers = string.digits
    all_chars = letters + numbers

    while True:
        first_char = random.choice(letters)
        remaining_chars = ''.join(random.choices(all_chars, k=length-1))
        result = first_char + remaining_chars
        if result not in generated_sequences:
            generated_sequences.add(result)
            return result

class RandomMultiClientSingleNodeTest(TestLvolHACluster):
    """
    Single-node non-HA stress test with device-level outages.
    Outage types:
      - device_remove_logical: remove device via sbcli CLI, recover with restart-device
      - device_remove_physical: remove device via PCI sysfs, recover with PCI rescan + restart-device
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.total_lvols = 40
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
        self.outage_device_id = None
        self.outage_device_pcie = None
        self.snapshot_names = []
        self.outage_start_time = None
        self.outage_end_time = None
        self.node_vs_lvol = {}
        self.snap_vs_node = {}
        self.outage_types = ["device_remove_logical", "device_remove_physical"]
        self.test_name = "continuous_single_node_device_outage"
        self.outage_log_file = os.path.join("logs", f"outage_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self._initialize_outage_log()

    def _initialize_outage_log(self):
        """Create or initialize the outage log file."""
        with open(self.outage_log_file, 'w') as log:
            log.write("Timestamp,Node,Outage_Type,Event\n")

    def log_outage_event(self, node, outage_type, event, outage_time=0):
        """Log an outage event to the outage log file."""
        if outage_time:
            base_epoch = getattr(self, "outage_start_time", None)
            if isinstance(base_epoch, (int, float)) and base_epoch > 0:
                ts_dt = datetime.fromtimestamp(int(base_epoch) + int(outage_time) * 60)
            else:
                ts_dt = datetime.now()
        else:
            ts_dt = datetime.now()
        timestamp = ts_dt.strftime('%Y-%m-%d %H:%M:%S')
        with open(self.outage_log_file, 'a') as log:
            log.write(f"{timestamp},{node},{outage_type},{event}\n")

    def create_lvols_with_fio(self, count):
        """Create lvols and start FIO with random configurations."""
        for i in range(count):
            fs_type = random.choice(["ext4", "xfs"])
            is_crypto = random.choice([True, False])
            lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            while lvol_name in self.lvol_mount_details:
                self.lvol_name = f"lvl{generate_random_sequence(15)}"
                lvol_name = f"{self.lvol_name}_{i}" if not is_crypto else f"c{self.lvol_name}_{i}"
            self.logger.info(f"Creating lvol with Name: {lvol_name}, fs type: {fs_type}, crypto: {is_crypto}")
            try:
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

            initial_devices = self.ssh_obj.get_devices(node=client_node)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client_node, command=connect_str)
                if error:
                    self.logger.warning(f"NVMe connect for {lvol_name} failed: {error}")
                    sleep_n_sec(30)
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

            mount_point = f"{self.mount_path}/{lvol_name}"
            self.ssh_obj.mount_path(node=client_node, device=lvol_device, mount_path=mount_point)
            self.lvol_mount_details[lvol_name]["Mount"] = mount_point

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client_node, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/local-{lvol_name}_fio*"])
            self.ssh_obj.delete_files(client_node, [f"{self.log_path}/{lvol_name}_fio_iolog"])

            sleep_n_sec(5)

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
                    "runtime": 3000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.lvol_mount_details[lvol_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            sleep_n_sec(10)

    def perform_random_outage(self):
        """Perform a random device outage on the storage node."""
        outage_type = random.choice(self.outage_types)
        self.current_outage_node = random.choice(self.sn_nodes)

        devices = self.sbcli_utils.get_device_details(storage_node_id=self.current_outage_node)
        online_devices = [d for d in devices if d["status"] == "online"]
        if not online_devices:
            self.logger.warning(f"No online devices on node {self.current_outage_node}, skipping outage")
            return outage_type

        device = random.choice(online_devices)
        self.outage_device_id = device["id"]
        self.outage_device_pcie = device.get("pcie_address", "")

        self.outage_start_time = int(datetime.now().timestamp())
        self.logger.info(
            f"Performing {outage_type} on device {self.outage_device_id} "
            f"(PCI: {self.outage_device_pcie}) on node {self.current_outage_node}"
        )
        self.log_outage_event(self.current_outage_node, outage_type, "Outage started")

        if outage_type == "device_remove_logical":
            self.ssh_obj.exec_command(
                node=self.mgmt_nodes[0],
                command=f"{self.base_cmd} sn remove-device {self.outage_device_id}"
            )
        elif outage_type == "device_remove_physical":
            node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.exec_command(
                node=node_ip,
                command=f"echo 1 | sudo tee /sys/bus/pci/devices/{self.outage_device_pcie}/remove"
            )

        sleep_n_sec(10)
        return outage_type

    def restart_nodes_after_failover(self, outage_type):
        """Recover the device after an outage."""
        self.logger.info(f"Recovering from {outage_type} for device {self.outage_device_id}")
        self.log_outage_event(self.current_outage_node, outage_type, "Recovery started")

        if outage_type == "device_remove_logical":
            self.ssh_obj.restart_device(
                node=self.mgmt_nodes[0],
                device_id=self.outage_device_id
            )
        elif outage_type == "device_remove_physical":
            node_details = self.sbcli_utils.get_storage_node_details(self.current_outage_node)
            node_ip = node_details[0]["mgmt_ip"]
            self.ssh_obj.exec_command(
                node=node_ip,
                command="echo 1 | sudo tee /sys/bus/pci/rescan"
            )
            sleep_n_sec(10)
            self.ssh_obj.restart_device(
                node=self.mgmt_nodes[0],
                device_id=self.outage_device_id
            )

        self.sbcli_utils.wait_for_device_status(
            node_id=self.current_outage_node,
            status="online",
            timeout=600
        )
        self.sbcli_utils.wait_for_health_status(self.current_outage_node, True, timeout=600)
        self.outage_end_time = int(datetime.now().timestamp())
        self.log_outage_event(self.current_outage_node, outage_type, "Device recovered")

        search_start_iso = datetime.fromtimestamp(self.outage_start_time - 30).isoformat(timespec='microseconds')
        search_end_iso = datetime.fromtimestamp(self.outage_end_time + 10).isoformat(timespec='microseconds')
        self.logger.info(f"Fetching dmesg logs from {search_start_iso} to {search_end_iso}")
        for node in self.fio_node:
            dmesg_logs = self.ssh_obj.get_dmesg_logs_within_iso_window(node, search_start_iso, search_end_iso)
            nvme_issues = [line for line in dmesg_logs if "nvme" in line.lower() or "connection" in line.lower()]
            if nvme_issues:
                self.logger.warning(f"NVMe issues on {node}:")
                for issue in nvme_issues:
                    self.logger.warning(issue)
            else:
                self.logger.info(f"No NVMe issues found on {node}")

    def create_snapshots_and_clones(self):
        """Create snapshots and clones."""
        self.int_lvol_size += 1
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items() for lvol in lvols
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
            lvol_node_id = self.sbcli_utils.get_lvol_details(
                lvol_id=self.lvol_mount_details[lvol]["ID"])[0]["node_id"]
            self.snap_vs_node[snapshot_name] = lvol_node_id
            self.lvol_mount_details[lvol]["snapshots"].append(snapshot_name)
            clone_name = f"clone_{generate_random_sequence(15)}"
            if clone_name in list(self.clone_mount_details):
                clone_name = f"{clone_name}_{temp_name}"
            sleep_n_sec(30)
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

            initial_devices = self.ssh_obj.get_devices(node=client)
            for connect_str in connect_ls:
                _, error = self.ssh_obj.exec_command(node=client, command=connect_str)
                if error:
                    lvol_details = self.sbcli_utils.get_lvol_details(lvol_id=self.clone_mount_details[clone_name]["ID"])
                    nqn = lvol_details[0]["nqn"]
                    self.ssh_obj.disconnect_nvme(node=client, nqn_grep=nqn)
                    self.logger.info(f"Connecting clone {clone_name} has error: {error}. Disconnecting and cleaning up.")
                    self.sbcli_utils.delete_lvol(lvol_name=clone_name, max_attempt=20, skip_error=True)
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

            if fs_type == "xfs":
                self.ssh_obj.clone_mount_gen_uuid(client, lvol_device)
            mount_point = f"{self.mount_path}/{clone_name}"
            self.ssh_obj.mount_path(node=client, device=lvol_device, mount_path=mount_point)
            self.clone_mount_details[clone_name]["Mount"] = mount_point

            sleep_n_sec(10)

            self.ssh_obj.delete_files(client, [f"{mount_point}/*fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/local-{clone_name}_fio*"])
            self.ssh_obj.delete_files(client, [f"{self.log_path}/{clone_name}_fio_iolog*"])

            sleep_n_sec(5)

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
                    "runtime": 3000,
                    "log_avg_msec": 1000,
                    "iolog_file": self.clone_mount_details[clone_name]["iolog_base_path"],
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
            self.logger.info(f"Created snapshot {snapshot_name} and clone {clone_name}.")

            if self.lvol_mount_details[lvol]["ID"]:
                self.sbcli_utils.resize_lvol(lvol_id=self.lvol_mount_details[lvol]["ID"],
                                             new_size=f"{self.int_lvol_size}G")
            sleep_n_sec(10)
            if self.clone_mount_details[clone_name]["ID"]:
                self.sbcli_utils.resize_lvol(lvol_id=self.clone_mount_details[clone_name]["ID"],
                                             new_size=f"{self.int_lvol_size}G")

    def delete_random_lvols(self, count):
        """Delete random lvols."""
        available_lvols = [
            lvol for node, lvols in self.node_vs_lvol.items() for lvol in lvols
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
                    sleep_n_sec(10)
                    for pid in fio_pids:
                        self.ssh_obj.kill_processes(clone_details["Client"], pid=pid)
                    attempt = 1
                    while len(fio_pids) > 2:
                        self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=False)
                        fio_pids = self.ssh_obj.find_process_name(clone_details["Client"], f"{clone_name}_fio", return_pid=True)
                        if attempt >= 30:
                            raise Exception("FIO not killed on clone")
                        attempt += 1
                        sleep_n_sec(20)

                    sleep_n_sec(10)
                    self.ssh_obj.unmount_path(clone_details["Client"], f"/mnt/{clone_name}")
                    self.ssh_obj.remove_dir(clone_details["Client"], dir_path=f"/mnt/{clone_name}")
                    self.disconnect_lvol(clone_details['ID'])
                    self.sbcli_utils.delete_lvol(clone_name, max_attempt=20, skip_error=True)
                    sleep_n_sec(30)
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
            sleep_n_sec(10)
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
                sleep_n_sec(20)

            sleep_n_sec(10)
            self.ssh_obj.unmount_path(self.lvol_mount_details[lvol]["Client"], f"/mnt/{lvol}")
            self.ssh_obj.remove_dir(self.lvol_mount_details[lvol]["Client"], dir_path=f"/mnt/{lvol}")
            self.disconnect_lvol(self.lvol_mount_details[lvol]['ID'])
            self.sbcli_utils.delete_lvol(lvol, max_attempt=20, skip_error=True)
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/local-{lvol}_fio*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"{self.log_path}/{lvol}_fio_iolog*"])
            self.ssh_obj.delete_files(self.lvol_mount_details[lvol]["Client"], [f"/mnt/{lvol}/*"])
            del self.lvol_mount_details[lvol]
            for _, lvols in self.node_vs_lvol.items():
                if lvol in lvols:
                    lvols.remove(lvol)
                    break
        sleep_n_sec(60)

    def validate_iostats_continuously(self):
        """Continuously validates I/O stats while FIO is running, checking every 300 seconds."""
        self.logger.info("Starting continuous I/O stats validation thread.")
        while True:
            try:
                start_timestamp = datetime.now().timestamp()
                end_timestamp = start_timestamp + 300
                self.common_utils.validate_io_stats(
                    cluster_id=self.cluster_id,
                    start_timestamp=start_timestamp,
                    end_timestamp=end_timestamp,
                    time_duration=None
                )
                sleep_n_sec(300)
            except Exception as e:
                self.logger.error(f"Error in continuous I/O stats validation: {str(e)}")
                break

    def restart_fio(self, iteration):
        """Restart FIO on all clones and lvols."""
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
                    "runtime": 3000,
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

    def perform_failover_during_outage(self):
        """Perform a second outage cycle during active FIO workloads."""
        self.logger.info("Performing failover during outage.")
        outage_type = self.perform_random_outage()
        self.delete_random_lvols(4)
        self.logger.info("Creating 5 new lvols, clones, and snapshots.")
        self.create_lvols_with_fio(5)
        self.create_snapshots_and_clones()
        sleep_n_sec(280)
        self.logger.info("Failover during outage completed.")
        self.restart_nodes_after_failover(outage_type)
        return outage_type

    def run(self):
        """Main execution loop for the single-node device outage stress test."""
        self.logger.info("Starting single-node device outage stress test.")
        iteration = 1

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        self.create_lvols_with_fio(self.total_lvols)

        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            self.sn_nodes.append(result["uuid"])
        self.logger.info(f"Storage nodes: {self.sn_nodes}")

        sleep_n_sec(30)

        while True:
            validation_thread = threading.Thread(target=self.validate_iostats_continuously, daemon=True)
            validation_thread.start()
            if iteration > 1:
                self.restart_fio(iteration=iteration)

            outage_type = self.perform_random_outage()
            self.delete_random_lvols(5)
            self.create_lvols_with_fio(3)
            self.create_snapshots_and_clones()

            sleep_n_sec(100)
            self.restart_nodes_after_failover(outage_type)
            self.logger.info("Waiting for fallback.")
            sleep_n_sec(15)

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

            outage_type = self.perform_failover_during_outage()
            sleep_n_sec(15)

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

            self.logger.info(f"Outage iteration {iteration} complete.")
            iteration += 1
