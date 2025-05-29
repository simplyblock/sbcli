import random
import threading
import csv
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
        self.output_file = Path(kwargs.get("output_file", "lvol_outage_log.csv"))
        self.max_lvols = kwargs.get("max_lvols", 1200)
        self.step = kwargs.get("step", 100)
        self.read_only = kwargs.get("read_only", False)
        self.continue_from_log = kwargs.get("continue_from_log", False)
        self.start_from = 600
        self.lvol_size = "500M"
        self.storage_nodes_uuid = []
        self.lvol_node = None

    def setup_environment(self):
        storage_nodes = self.sbcli_utils.get_storage_nodes()
        for result in storage_nodes['results']:
            self.storage_nodes_uuid.append(result["uuid"])

        self.sbcli_utils.add_storage_pool(pool_name=self.pool_name)
        lvol_name = "load_lvol"
        for i in range(1, self.start_from + 1):
            self.sbcli_utils.add_lvol(
                lvol_name=f"{lvol_name}_{i}",
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=False,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1]
            )
            sleep_n_sec(2)

    def create_and_shutdown_restart(self, count):
        lvol_name = "load_lvol"
        for i in range(count + 1, count + self.step + 1):
            self.sbcli_utils.add_lvol(
                lvol_name=f"{lvol_name}_{i}",
                pool_name=self.pool_name,
                size=self.lvol_size,
                crypto=False,
                key1=self.lvol_crypt_keys[0],
                key2=self.lvol_crypt_keys[1]
            )
            sleep_n_sec(2)
        
        self.lvol_node = random.choice(self.storage_nodes_uuid)
        count = count + self.step

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
