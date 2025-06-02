import random
import threading
import csv
from logger_config import setup_logger
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from stress_test.lvol_ha_stress_fio import TestLvolHACluster
from utils.common_utils import sleep_n_sec


class TestLvolOutageLoadTest(TestLvolHACluster):
    """
    Graceful shutdown + restart test measuring time at scale from 600 to 1200 lvols
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.logger = setup_logger(__name__)
        self.output_file = Path(kwargs.get("output_file", "lvol_outage_log.csv"))
        self.max_lvols = kwargs.get("max_lvols", 1200)
        self.step = kwargs.get("step", 100)
        self.read_only = kwargs.get("read_only", False)
        self.continue_from_log = kwargs.get("continue_from_log", False)
        self.start_from = kwargs.get("start_from", 600)
        self.lvol_size = "3G"
        self.storage_nodes_uuid = []
        self.lvol_node = None
        self.fio_size = "250M"
        self.mount_base = "/mnt/"
        self.log_base = f"{Path.home()}/"
        self.fio_threads = []
        self.logger.info(f"Running load test with Max Lvol:{self.max_lvols}, Start: {self.start_from}, Step:{self.step}")

    def setup_environment(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            self.storage_nodes_uuid.append(result["uuid"])
        self.lvol_node = random.choice(self.storage_nodes_uuid)

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        base_lvol_name = "load_lvol"
        for i in range(1, self.start_from + 1):
            fs_type = random.choice(["ext4", "xfs"])
            client_node = random.choice(self.fio_node)
            lvol_name = f"{base_lvol_name}_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=False,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
                host_id=self.lvol_node
            )
            sleep_n_sec(2)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(client_node, cmd)
            device = self.ssh_obj.get_lvol_vs_device(client_node, lvol_id)
            mount_path = f"{self.mount_base}/{lvol_name}"
            self.lvol_mount_details[lvol_name] = {
                   "ID": lvol_id,
                   "Command": connect_ls,
                   "Mount": mount_path,
                   "Device": device,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_base}/{lvol_name}.log",
                   "snapshots": [],
                   "Client": client_node
            }
            
            self.ssh_obj.format_disk(client_node, device)
            self.ssh_obj.mount_path(client_node, device, mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_path, self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 6,
                    "runtime": 500,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)

    def create_and_shutdown_restart(self, count):
        base_lvol_name = "load_lvol"
        for i in range(count + 1, count + self.step + 1):
            fs_type = random.choice(["ext4", "xfs"])
            client_node = random.choice(self.fio_node)
            lvol_name = f"{base_lvol_name}_{i}"
            self.sbcli_utils.add_lvol(
                lvol_name=lvol_name,
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=False,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1],
                host_id=self.lvol_node
            )
            sleep_n_sec(2)
            lvol_id = self.sbcli_utils.get_lvol_id(lvol_name)
            connect_ls = self.sbcli_utils.get_lvol_connect_str(lvol_name=lvol_name)
            for cmd in connect_ls:
                self.ssh_obj.exec_command(client_node, cmd)
            device = self.ssh_obj.get_lvol_vs_device(client_node, lvol_id)
            mount_path = f"{self.mount_base}/{lvol_name}"
            self.lvol_mount_details[lvol_name] = {
                   "ID": lvol_id,
                   "Command": connect_ls,
                   "Mount": mount_path,
                   "Device": device,
                   "MD5": None,
                   "FS": fs_type,
                   "Log": f"{self.log_base}/{lvol_name}.log",
                   "snapshots": [],
                   "Client": client_node
            }
            
            self.ssh_obj.format_disk(client_node, device)
            self.ssh_obj.mount_path(client_node, device, mount_path)

            fio_thread = threading.Thread(
                target=self.ssh_obj.run_fio_test,
                args=(client_node, None, mount_path, self.lvol_mount_details[lvol_name]["Log"]),
                kwargs={
                    "size": self.fio_size,
                    "name": f"{lvol_name}_fio",
                    "rw": "randrw",
                    "nrfiles": 5,
                    "iodepth": 1,
                    "numjobs": 6,
                    "runtime": 500,
                },
            )
            fio_thread.start()
            self.fio_threads.append(fio_thread)
        
        count = count + self.step

        self.common_utils.manage_fio_threads(
            self.fio_node, self.fio_threads, timeout=1000
        )

        self.logger.info(f"[{count}] Initiating graceful shutdown {self.lvol_node}...")
        shutdown_start = datetime.now()
        self.sbcli_utils.suspend_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "suspended", timeout=4000)
        sleep_n_sec(10)
        self.sbcli_utils.shutdown_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "offline", timeout=4000)
        shutdown_end = datetime.now()

        self.logger.info(f"[{count}] Shutdown complete, restarting node {self.lvol_node}...")
        restart_start = datetime.now()
        self.sbcli_utils.restart_node(node_uuid=self.lvol_node, expected_error_code=[503])
        self.sbcli_utils.wait_for_storage_node_status(self.lvol_node, "online", timeout=4000)
        self.sbcli_utils.wait_for_health_status(self.lvol_node, True, timeout=4000)
        restart_end = datetime.now()

        return shutdown_end - shutdown_start, restart_end - restart_start

    def write_to_log(self, lvol_count, shutdown_time, restart_time):
        entry = [lvol_count, shutdown_time.total_seconds() - 10, restart_time.total_seconds()]
        write_header = not self.output_file.exists()
        with open(self.output_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(["lvol_count", "shutdown_time_sec", "restart_time_sec"])
            writer.writerow(entry)

    def parse_existing_log(self):
        results = []
        if self.output_file.exists():
            with open(self.output_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    results.append({
                        "lvol_count": int(row['lvol_count']),
                        "shutdown_time_sec": float(row['shutdown_time_sec']),
                        "restart_time_sec": float(row['restart_time_sec'])
                    })
        return results

    def generate_graph(self, results):
        x = [r['lvol_count'] for r in results]
        shutdown = [r['shutdown_time_sec'] for r in results]
        restart = [r['restart_time_sec'] for r in results]

        plt.plot(x, shutdown, marker='o', label='Shutdown Time (s)')
        plt.plot(x, restart, marker='x', label='Restart Time (s)')
        for i in range(len(x)):
            plt.annotate(f"{shutdown[i]:.1f}s", (x[i], shutdown[i]), textcoords="offset points", xytext=(0,5), ha='center')
            plt.annotate(f"{restart[i]:.1f}s", (x[i], restart[i]), textcoords="offset points", xytext=(0,-10), ha='center')

        plt.title("Node Outage Time vs. Number of lvols")
        plt.xlabel("Number of lvols")
        plt.ylabel("Time (seconds)")
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.savefig("lvol_shutdown_restart_graph.png")
        plt.show()

    def run(self):
        if self.read_only:
            results = self.parse_existing_log()
            self.generate_graph(results)
            return

        self.setup_environment()
        start = self.start_from
        if self.continue_from_log:
            previous = self.parse_existing_log()
            if previous:
                start = max([entry['lvol_count'] for entry in previous])

        for count in range(start, self.max_lvols + 1, self.step):
            shutdown_time, restart_time = self.create_and_shutdown_restart(count)
            self.write_to_log(count, shutdown_time, restart_time)

        results = self.parse_existing_log()
        self.generate_graph(results)
