import argparse
import subprocess
import matplotlib.pyplot as plt
import os
import time
import numpy as np
import threading
import psutil
import csv
from collections import defaultdict


class ManagementStressUtils:
    """Utility class for common methods like executing commands and gathering data."""

    @staticmethod
    def log_system_psutil_resources(label, log_file, csv_log_file, batch_index):
        cpu = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        with open(log_file, 'a') as f:
            f.write(f"\n--- {label} ---\n")
            f.write(f"CPU usage (%): {cpu}\n")
            f.write(f"Memory usage (%): {mem.percent}, Available: {mem.available / (1024 ** 3):.2f} GB\n")
            f.write(f"Disk usage (%): {disk.percent}, Free: {disk.free / (1024 ** 3):.2f} GB\n")

        write_header = not os.path.exists(csv_log_file)
        with open(csv_log_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            if write_header:
                writer.writerow(["batch_index", "cpu_percent", "memory_percent", "memory_available_GB", "disk_percent", "disk_free_GB"])
            writer.writerow([
                batch_index, cpu, mem.percent, mem.available / (1024 ** 3),
                disk.percent, disk.free / (1024 ** 3)
            ])

    # @staticmethod
    # def plot_psutil_resource_usage(csv_file):
    #     batches = []
    #     cpu_vals, mem_vals, disk_vals = [], [], []

    #     with open(csv_file, 'r') as f:
    #         reader = csv.DictReader(f)
    #         for row in reader:
    #             batches.append(int(row['batch_index']))
    #             cpu_vals.append(float(row['cpu_percent']))
    #             mem_vals.append(float(row['memory_percent']))
    #             disk_vals.append(float(row['disk_percent']))

    #     os.makedirs("logs", exist_ok=True)

    #     # CPU Graph
    #     plt.figure(figsize=(10, 5))
    #     plt.plot(batches, cpu_vals, label='CPU %', marker='o', color='tab:red')
    #     for i in range(len(batches)):
    #         plt.annotate(f"{cpu_vals[i]:.1f}%", (batches[i], cpu_vals[i]), textcoords="offset points", xytext=(0,5), ha='center')
    #     plt.xlabel('Batch Index')
    #     plt.ylabel('CPU Usage (%)')
    #     plt.title('CPU Usage Per Batch')
    #     plt.grid(True)
    #     plt.tight_layout()
    #     plt.savefig("logs/cpu_usage_per_batch.png")

    #     # Memory Graph
    #     plt.figure(figsize=(10, 5))
    #     plt.plot(batches, mem_vals, label='Memory %', marker='s', color='tab:blue')
    #     for i in range(len(batches)):
    #         plt.annotate(f"{mem_vals[i]:.1f}%", (batches[i], mem_vals[i]), textcoords="offset points", xytext=(0,5), ha='center')
    #     plt.xlabel('Batch Index')
    #     plt.ylabel('Memory Usage (%)')
    #     plt.title('Memory Usage Per Batch')
    #     plt.grid(True)
    #     plt.tight_layout()
    #     plt.savefig("logs/memory_usage_per_batch.png")

    #     # Disk Graph
    #     plt.figure(figsize=(10, 5))
    #     plt.plot(batches, disk_vals, label='Disk %', marker='^', color='tab:green')
    #     for i in range(len(batches)):
    #         plt.annotate(f"{disk_vals[i]:.1f}%", (batches[i], disk_vals[i]), textcoords="offset points", xytext=(0,5), ha='center')
    #     plt.xlabel('Batch Index')
    #     plt.ylabel('Disk Usage (%)')
    #     plt.title('Disk Usage Per Batch')
    #     plt.grid(True)
    #     plt.tight_layout()
    #     plt.savefig("logs/disk_usage_per_batch.png")

    #     plt.show()

    @staticmethod
    def plot_psutil_resource_usage_both(csv_file, total_batches):
        """
        Plot system resource usage:
        1) LVOL phase (per batch)
        2) Post phase (over time in minutes)
        Save both plots in the current working directory.
        """
        batch_indices = []
        time_intervals = []
        cpu_batches, cpu_time = [], []
        mem_batches, mem_time = [], []
        disk_batches, disk_time = [], []

        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                idx = int(row['batch_index'])
                cpu = float(row['cpu_percent'])
                mem = float(row['memory_percent'])
                disk = float(row['disk_percent'])
                if idx > 0 and idx <= total_batches:
                    batch_indices.append(idx)
                    cpu_batches.append(cpu)
                    mem_batches.append(mem)
                    disk_batches.append(disk)
                else:
                    time_intervals.append(idx)
                    cpu_time.append(cpu)
                    mem_time.append(mem)
                    disk_time.append(disk)

        # 1️. Plot batch-wise
        plt.figure(figsize=(12, 6))
        plt.plot(batch_indices, cpu_batches, marker='o', label='CPU %')
        plt.plot(batch_indices, mem_batches, marker='s', label='Memory %')
        plt.plot(batch_indices, disk_batches, marker='^', label='Disk %')
        plt.title("System Resource Usage Per Batch (LVOL Phase)")
        plt.xlabel("Batch Index")
        plt.ylabel("Usage (%)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("psutil_resource_batch_wise.png")
        plt.close()

        # 2️. Plot time-wise (post phase)
        plt.figure(figsize=(12, 6))
        plt.plot(time_intervals, cpu_time, marker='o', label='CPU %')
        plt.plot(time_intervals, mem_time, marker='s', label='Memory %')
        plt.plot(time_intervals, disk_time, marker='^', label='Disk %')
        plt.title("System Resource Usage Over Time (Post-Monitor Phase)")
        plt.xlabel("Time (minutes)")
        plt.ylabel("Usage (%)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("psutil_resource_time_wise.png")
        plt.close()

    @staticmethod
    def log_all_container_resources(label, log_file, csv_log_file, batch_index):
        """
        Log CPU and Memory usage for ALL containers on the host.
        Write both human-readable log and structured CSV for later graphing.
        """
        # Get ALL containers and stats in one go
        cmd = "sudo docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}'"
        result = ManagementStressUtils.exec_cmd(cmd)

        write_header = not os.path.exists(csv_log_file)
        with open(csv_log_file, 'a', newline='') as csvfile, open(log_file, 'a') as logf:
            writer = csv.writer(csvfile)
            if write_header:
                writer.writerow(["batch_index", "container_name", "cpu_percent", "memory_usage", "memory_percent"])

            logf.write(f"\n--- {label} ---\n")
            for line in result.strip().splitlines():
                name, cpu_perc, mem_usage, mem_perc = line.split(",")
                logf.write(f"{name}: CPU={cpu_perc}, Mem={mem_usage} ({mem_perc})\n")

                # Remove % and normalize mem_usage to plain text (e.g., 32.4MiB / 1GiB)
                writer.writerow([batch_index, name, cpu_perc.strip("%"), mem_usage.strip(), mem_perc.strip("%")])

    @staticmethod
    def plot_all_container_resources_both(csv_file, total_batches):
        """
        Plot all container CPU & Memory usage:
        1) Per Batch (LVOL phase)
        2) Over Time (Post-monitor phase)
        Save all plots in current directory.
        """
        data_batches = defaultdict(lambda: {"batch": [], "cpu": [], "mem": []})
        data_time = defaultdict(lambda: {"time": [], "cpu": [], "mem": []})

        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                idx = int(row['batch_index'])
                name = row['container_name']
                cpu = float(row['cpu_percent'])
                mem = float(row['memory_percent'])

                if idx > 0 and idx <= total_batches:
                    data_batches[name]["batch"].append(idx)
                    data_batches[name]["cpu"].append(cpu)
                    data_batches[name]["mem"].append(mem)
                else:
                    data_time[name]["time"].append(idx)
                    data_time[name]["cpu"].append(cpu)
                    data_time[name]["mem"].append(mem)

        # 1️. Plot BATCH-WISE for each container
        for name, values in data_batches.items():
            plt.figure(figsize=(10, 5))
            plt.plot(values["batch"], values["cpu"], marker='o', label='CPU %')
            plt.plot(values["batch"], values["mem"], marker='s', label='Memory %')
            plt.title(f"Container: {name} - Resource Usage Per Batch")
            plt.xlabel("Batch Index")
            plt.ylabel("Usage (%)")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            safe_name = name.replace("/", "_").replace(":", "_")
            plt.savefig(f"{safe_name}_resource_usage_batch_wise.png")
            plt.close()

        # 2️. Plot TIME-WISE for each container
        for name, values in data_time.items():
            plt.figure(figsize=(10, 5))
            plt.plot(values["time"], values["cpu"], marker='o', label='CPU %')
            plt.plot(values["time"], values["mem"], marker='s', label='Memory %')
            plt.title(f"Container: {name} - Resource Usage Over Time")
            plt.xlabel("Time (minutes)")
            plt.ylabel("Usage (%)")
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            safe_name = name.replace("/", "_").replace(":", "_")
            plt.savefig(f"{safe_name}_resource_usage_time_wise.png")
            plt.close()

    # @staticmethod
    # def plot_all_container_resources(csv_file):
    #     """
    #     Generate separate CPU and Memory usage graphs per container, batch-wise.
    #     """
    #     import csv
    #     from collections import defaultdict
    #     import os

    #     data = defaultdict(lambda: {"batch": [], "cpu": [], "mem_percent": []})

    #     with open(csv_file, 'r') as f:
    #         reader = csv.DictReader(f)
    #         for row in reader:
    #             name = row['container_name']
    #             data[name]["batch"].append(int(row['batch_index']))
    #             data[name]["cpu"].append(float(row['cpu_percent']))
    #             data[name]["mem_percent"].append(float(row['memory_percent']))

    #     os.makedirs("logs", exist_ok=True)

    #     for name, values in data.items():
    #         plt.figure(figsize=(10, 5))
    #         plt.plot(values["batch"], values["cpu"], marker='o', label='CPU %')
    #         plt.plot(values["batch"], values["mem_percent"], marker='s', label='Memory %')
    #         plt.title(f"Container: {name} Resource Usage")
    #         plt.xlabel("Batch Index")
    #         plt.ylabel("Usage (%)")
    #         plt.legend()
    #         plt.grid(True)
    #         plt.tight_layout()
    #         safe_name = name.replace("/", "_").replace(":", "_")
    #         plt.savefig(f"logs/{safe_name}_resource_usage.png")
    #         plt.close()

    @staticmethod
    def exec_cmd(cmd, error_ok=False):
        """Execute a command locally."""
        try:
            result = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Error executing command '{cmd}': {e.stderr.strip()}")
            if error_ok:
                return ""
            raise e

    @staticmethod
    def measure_cmd_time(cmd):
        """Measure the execution time of a command."""
        start_time = time.time()
        result = ManagementStressUtils.exec_cmd(cmd)
        print(f"Command: {cmd}")
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
        return elapsed_time, result

    @staticmethod
    def get_fdb_size():
        """Get FDB size."""
        cmd = 'fdbcli --exec "status" | grep "Disk space used" | awk \'{print $5, $6}\''
        result = ManagementStressUtils.exec_cmd(cmd)
        try:
            size = result.upper().strip()
            return ManagementStressUtils.convert_to_gb(size)
        except ValueError:
            print(f"Invalid FDB size output: {result}")
            return 0

    @staticmethod
    def get_directory_size(path):
        """Get size of a directory in GB."""
        cmd = f"sudo du -sh {path} | awk '{{print $1}}'"
        result = ManagementStressUtils.exec_cmd(cmd)
        try:
            size = result.upper()
            return ManagementStressUtils.convert_to_gb(size)
        except ValueError:
            print(f"Invalid directory size output for {path}: {result}")
            return 0

    @staticmethod
    def convert_to_gb(size_str):
        """Convert memory sizes (e.g., MiB, GiB) to GB."""
        try:
            size = size_str.upper()
            if "K" in size:
                size = size.replace("KIB", "")
                size = size.replace("KB", "")
                size = size.replace("K", "")
                return round(float(size) / 1024 / 1024, 2)
            elif "M" in size:
                size = size.replace("MIB", "")
                size = size.replace("MB", "")
                size = size.replace("M", "")
                return round(float(size) / 1024, 2)
            elif "G" in size:
                size = size.replace("GIB", "")
                size = size.replace("GB", "")
                size = size.replace("G", "")
                return round(float(size), 2)
            elif "T" in size:
                size = size.replace("TIB", "")
                size = size.replace("TB", "")
                size = size.replace("T", "")
                return round(float(size) * 1024, 2)
            return round(float(size) / 1024 / 1024, 2)  # Assume bytes if no unit
        except ValueError:
            print(f"Invalid size format: {size_str}")
            return 0

    @staticmethod
    def get_container_name(base_name):
        """Retrieve the container name dynamically based on its base name."""
        cmd = "sudo docker ps --format '{{.Names}}'"
        result = ManagementStressUtils.exec_cmd(cmd)
        containers = result.split("\n")
        for container in containers:
            if base_name in container:
                return container
        raise ValueError(f"Container with base name '{base_name}' not found.")

    @staticmethod
    def parse_docker_stats(container_name):
        """Parse docker stats for memory and CPU usage."""
        cmd = "sudo docker stats %s --no-stream --format '{{.CPUPerc}}-{{.MemUsage}}-{{.MemPerc}}'" %container_name
        result = ManagementStressUtils.exec_cmd(cmd)
        try:
            cpu_usage, mem_usage, mem_perc = result.split("-")
            cpu_usage = float(cpu_usage.replace("%", "").strip())
            mem_current, mem_limit = mem_usage.split("/")
            mem_current_gb = ManagementStressUtils.convert_to_gb(mem_current.strip())
            mem_limit_gb = ManagementStressUtils.convert_to_gb(mem_limit.strip())
            mem_usage_perc = float(mem_perc.replace("%", "").strip())
            return cpu_usage, mem_current_gb, mem_limit_gb, mem_usage_perc
        except Exception as e:
            print(f"Error parsing docker stats for {container_name}: {e}")
            return 0, 0, 0, 0

    @staticmethod
    def gather_system_metrics():
        """Collect CPU usage, memory used, and buffer memory from the 'top' command."""
        try:
            cmd = "top -b -n 1 | grep MiB"
            result = ManagementStressUtils.exec_cmd(cmd=cmd)
            lines = result.split('\n')
            mem_line = lines[0]

            mem_data = [float(value) for value in mem_line.split() if value.replace('.', '', 1).isdigit()]
            _total_memory, used_memory, buffer_memory = mem_data[0], mem_data[2], mem_data[3]
            cmd = "top -bn1 | grep Cpu | awk '{print $2}'"
            cpu_usage = float(ManagementStressUtils.exec_cmd(cmd=cmd))
            used_memory = ManagementStressUtils.convert_to_gb(f"{used_memory} MB")
            buffer_memory = ManagementStressUtils.convert_to_gb(f"{buffer_memory} MB")
            return cpu_usage, used_memory, buffer_memory
        except ValueError:
            return 0, 0, 0


class TestLvolMemory:
    """Test case class for creating lvols and tracking memory consumption."""

    def __init__(self, sbcli_cmd, cluster_id, utils,
                 log_file="test_lvol_memory.log", size_change_log="size_change.log",
                 timings_log="timings.log", pool_name="pool1", total_batches=100, batch_size=25,
                 csv_log_file="resource_log.csv"):
        self.utils = utils
        self.sbcli_cmd = sbcli_cmd
        self.cluster_id = cluster_id
        self.pool_name = pool_name
        self.total_batches = total_batches
        self.batch_size = batch_size
        self.fdb_sizes = []
        self.prometheus_sizes = []
        self.graylog_sizes = []
        self.lvol_create_times = []
        self.sn_list_times = []
        self.lvol_list_times = []
        self.log_file = log_file
        self.size_change_log = size_change_log
        self.timings_log = timings_log
        self.last_lvol_with_sizes = None  # Stores the last lvol with recorded sizes
        self.cpu_usages = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.memory_usages = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.cpu_usages_lvol = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.memory_usages_lvol = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.container_names = {}
        # self.intervals = [10, 20, 30, 40, 50, 60]  # Time intervals in minutes
        self.intervals = []
        self.lvol_counts = []
        self.total_lvols = self.batch_size * self.total_batches

        self.continuous_cpu_usages = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.continuous_memory_usages = {"fdb": [], "prometheus": [], "graylog": [], "graylog_os": []}
        self.continuous_system_cpu_usages = []
        self.continuous_system_memory_usages = []
        self.continuous_system_memory_buffer = []
        self.continuous_sn_list_times = []
        self.continuous_lvol_list_times = []
        self.container_map = {
            "fdb": "app_fdb-server",
            # "prometheus": "monitoring_prometheus",
            "graylog": "monitoring_graylog",
            "graylog_os": "monitoring_opensearch",
        }
        self.milestones = []
        self.time_durations = []
        self.csv_log_file = csv_log_file

        self.initialize_logs()

    def initialize_logs(self):
        """Initialize the log files."""
        with open(self.log_file, "w", encoding="utf-8") as log:
            log.write("Lvol Memory and Timings Log\n")
            log.write("=" * 50 + "\n")

        with open(self.size_change_log, "w", encoding="utf-8") as log:
            log.write("Size Change Log\n")
            log.write("=" * 50 + "\n")

        with open(self.timings_log, "w", encoding="utf-8") as log:
            log.write("Command Timings Log\n")
            log.write("=" * 50 + "\n")

    def log(self, message):
        """Write a message to the main log file and print it to the console."""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)  # Print to console
        with open(self.log_file, "a", encoding="utf-8") as log:
            log.write(log_entry + "\n")

    def log_timing(self, message):
        """Write a message to the timings log file and print it to the console."""
        print(message)  # Print to console
        with open(self.timings_log, "a", encoding="utf-8") as log:
            log.write(message + "\n")

    def log_size_change(self, batch_idx, lvol_idx, last_sizes, current_sizes):
        """Log size differences between lvols to the size change log."""
        log_message = (
            f"Size difference between Batch {last_sizes['batch']}, Lvol {last_sizes['lvol']}: "
            f"FDB={last_sizes['fdb']} GB, Prometheus={last_sizes['prometheus']} GB, "
            f"Graylog={last_sizes['graylog']} GB\n"
            f"and Batch {batch_idx}, Lvol {lvol_idx}: "
            f"FDB={current_sizes['fdb']} GB, Prometheus={current_sizes['prometheus']} GB, "
            f"Graylog={current_sizes['graylog']} GB\n"
            f"Difference: FDB={current_sizes['fdb'] - last_sizes['fdb']} GB, "
            f"Prometheus={current_sizes['prometheus'] - last_sizes['prometheus']} GB, "
            f"Graylog={current_sizes['graylog'] - last_sizes['graylog']} GB\n"
        )
        print(log_message)
        with open(self.size_change_log, "a", encoding="utf-8") as log:
            log.write(log_message + "\n")
    
    def gather_docker_stats_after_script(self):
        """Gather memory and CPU usage for each container."""
        for key, base_name in self.container_map.items():
            container_name = self.utils.get_container_name(base_name)
            self.log(f"Container name: {container_name}")
            cpu, memory_used, memory_total, memory_perc = self.utils.parse_docker_stats(container_name)
            self.cpu_usages[key].append(cpu)
            self.memory_usages[key].append([memory_used, memory_total, memory_total, memory_perc])
            self.log(f"{key.capitalize()} - CPU: {cpu}%, Memory(%): {[memory_used, memory_total, memory_total, memory_perc]}%")
    
    def gather_docker_stats_lvol(self):
        """Gather memory and CPU usage for each container during lvol creation."""
        for key, base_name in self.container_map.items():
            container_name = self.utils.get_container_name(base_name)
            self.log(f"Container name: {container_name}")
            cpu, memory_used, memory_total, memory_perc = self.utils.parse_docker_stats(container_name)
            self.cpu_usages_lvol[key].append(cpu)
            self.memory_usages_lvol[key].append([memory_used, memory_total, memory_total, memory_perc])
            self.log(f"{key.capitalize()} - CPU: {cpu}%, Memory(%): {[memory_used, memory_total, memory_total, memory_perc]}%")

    def monitor_time_based_data(self):
        """Monitor and log data over time intervals after script completion."""
        for interval in range(0, 61, 10):
            self.log(f"Monitoring data after {interval} minutes...")
            if interval != 0:
                time.sleep(600)
            self.intervals.append(interval)
            self.gather_docker_stats_after_script()
            ManagementStressUtils.log_system_psutil_resources(
                f"Post-Monitor at {interval} mins",
                self.log_file,
                "resource_data.csv",     # SAME system log file
                1000 + interval                 # Use interval as index
            )

            ManagementStressUtils.log_all_container_resources(
                f"Post-Monitor at {interval} mins - All Containers",
                self.log_file,
                "all_containers_resource.csv",  # SAME container log file
                1000 + interval                        # Use interval as index
            )

            
            self.log(f"Data collected for {interval} minutes.")

    def detect_size_change(self, batch_idx, lvol_idx, fdb_size, prometheus_size, graylog_size):
        """Detect size changes and log them."""
        current_sizes = {
            "batch": batch_idx,
            "lvol": lvol_idx,
            "fdb": fdb_size,
            "prometheus": prometheus_size,
            "graylog": graylog_size,
        }

        if self.last_lvol_with_sizes:
            last_sizes = self.last_lvol_with_sizes
            if (
                current_sizes["fdb"] != last_sizes["fdb"] or
                current_sizes["prometheus"] != last_sizes["prometheus"] or
                current_sizes["graylog"] != last_sizes["graylog"]
            ):
                self.log_size_change(batch_idx, lvol_idx, last_sizes, current_sizes)

        self.last_lvol_with_sizes = current_sizes

    def gather_data(self):
        """Collect FDB, Prometheus, and Graylog memory usage."""
        fdb_size = self.utils.get_fdb_size()
        prometheus_size = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_prometheus_data/")
        graylog_journal = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_graylog_journal/")
        graylog_data = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_graylog_data/")
        graylog_mongodb = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_mongodb_data/")
        graylog_os = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_os_data/")
        graylog_total = graylog_journal + graylog_data +graylog_mongodb + graylog_os
        return fdb_size, prometheus_size, graylog_total

    def create_lvol(self, lvol_name):
        """Create an lvol and measure its response time."""
        cmd = f"{self.sbcli_cmd} lvol add {lvol_name} 200M {self.pool_name}"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        self.lvol_create_times.append(elapsed_time)
        self.log_timing(f"Lvol Create Time: {elapsed_time:.2f} ms")
        return elapsed_time

    def measure_sn_list(self, continuous=False):
        """Measure `sn list` command timing."""
        cmd = f"{self.sbcli_cmd} sn list"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        if not continuous:
            self.sn_list_times.append(elapsed_time)
        self.log_timing(f"SN List Time: {elapsed_time:.2f} ms")
        return elapsed_time

    def measure_lvol_list(self, continuous=False):
        """Measure `lvol list` command timing."""
        cmd = f"{self.sbcli_cmd} lvol list"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        if not continuous:
            self.lvol_list_times.append(elapsed_time)
        self.log_timing(f"Lvol List Time: {elapsed_time:.2f} ms")
        return elapsed_time
    
    def collect_continuous_data(self, stop_event):
        """
        Continuously collect container CPU, memory, and timing data.
        Includes updating milestones dynamically and writing data incrementally.
        """
        start_time = time.time()  # Global start time
        batch_times = []  # Store start and end times for each batch
        lvol_created = 0  # Track total LVOLs created
        last_milestone = 0  # Initialize last milestone

        while not stop_event.is_set():
            batch_start_time = time.time()  # Start time for this batch

            # Collect time elapsed
            elapsed_time = int((time.time() - start_time) / 60)  # Convert to minutes

            # Collect container stats
            for key, base_name in self.container_map.items():
                container_name = self.utils.get_container_name(base_name)
                cpu, memory_used, memory_total, memory_perc = self.utils.parse_docker_stats(container_name)
                self.continuous_cpu_usages[key].append(cpu)
                self.continuous_memory_usages[key].append(memory_used)

            # Collect timings for `sn list` and `lvol list`
            sn_time = self.measure_sn_list(continuous=True)
            lvol_time = self.measure_lvol_list(continuous=True)
            self.continuous_sn_list_times.append(sn_time)
            self.continuous_lvol_list_times.append(lvol_time)
            system_cpu_usage, system_usage_memory, system_buffer_memory = self.utils.gather_system_metrics()
            self.log(f"System - CPU: {system_cpu_usage}%, Memory(MB): {[system_usage_memory, system_buffer_memory]}")
            self.continuous_system_cpu_usages.append(system_cpu_usage)
            self.continuous_system_memory_usages.append(system_usage_memory)
            self.continuous_system_memory_buffer.append(system_buffer_memory)

            # Check LVOL milestones
            lvol_created = len(self.lvol_counts)
            next_milestone = last_milestone + 100  # Calculate the next milestone
            while lvol_created >= next_milestone:  # Handle multiple milestones in one go
                milestone_entry = (elapsed_time, next_milestone)
                self.milestones.append(milestone_entry)  # Update milestones dynamically
                self.log(f"Milestone reached: {milestone_entry}")
                last_milestone = next_milestone
                next_milestone += 100

            # Log every 3-minute interval
            if elapsed_time % 3 == 0:
                self.log(f"Continuous data collection at {elapsed_time} mins: LVOLs = {lvol_created}, Milestones: {self.milestones}")
                self.write_continuous_data_to_files()  # Save continuous data incrementally

            # End time for this batch
            batch_end_time = time.time()
            batch_times.append((batch_start_time, batch_end_time))  # Log batch start and end times

            # Sleep for 10 seconds
            time.sleep(10)

        # Final save at the end of data collection
        self.log(f"Final data collection at {elapsed_time} mins: LVOLs = {lvol_created}, Milestones: {self.milestones}")
        self.write_continuous_data_to_files()

        # Write batch times to a file
        with open("time_durations.txt", "w") as f:
            total_duration = time.time() - start_time
            f.write(f"Total Test Duration: {total_duration:.2f} seconds\n")
            f.write("Batch Start and End Times:\n")
            for idx, (start, end) in enumerate(batch_times, 1):
                duration = end - start
                f.write(f"Batch {idx}: Start = {start:.2f}, End = {end:.2f}, Duration = {duration:.2f} seconds\n")




    # def collect_continuous_data(self, stop_event):
    #     """Continuously collect container CPU, memory, and timing data."""
    #     milestones = []  # Track (time, lvol_count) for milestones
    #     start_time = time.time()
    #     lvol_created = 0  # Track total LVOLs created

    #     while not stop_event.is_set():
    #         # Collect time elapsed
    #         elapsed_time = int((time.time() - start_time) / 60)  # Convert to minutes

    #         # Collect container stats
    #         for key, base_name in self.container_map.items():
    #             container_name = self.utils.get_container_name(base_name)
    #             cpu, memory_used, memory_total, memory_perc = self.utils.parse_docker_stats(container_name)
    #             self.continuous_cpu_usages[key].append(cpu)
    #             self.continuous_memory_usages[key].append(memory_used)

    #         # Collect timings for `sn list` and `lvol list`
    #         sn_time = self.measure_sn_list(continuous=True)
    #         lvol_time = self.measure_lvol_list(continuous=True)
    #         self.continuous_sn_list_times.append(sn_time)
    #         self.continuous_lvol_list_times.append(lvol_time)

    #         # Check LVOL milestones
    #         if lvol_created < len(self.lvol_counts):  # New LVOLs created
    #             lvol_created = len(self.lvol_counts)
    #             # if lvol_created % 100 == 0 and lvol_created not in [m[1] for m in milestones]:
    #             #     milestones.append((elapsed_time, lvol_created))
    #             if lvol_created > 0 and lvol_created % 100 == 0 and lvol_created not in [m[1] for m in self.milestones]:
    #                 self.milestones.append((elapsed_time, lvol_created))  # Append milestones here


    #         # Explicitly add a milestone for the total LVOL count after creation
    #         if lvol_created == (self.total_batches * self.batch_size):
    #             self.milestones.append((elapsed_time, lvol_created))

    #         # Log every 3-minute interval
    #         if elapsed_time % 3 == 0:
    #             self.log(f"Continuous data collection at {elapsed_time} mins: LVOLs = {lvol_created}")

    #         # Sleep for 10 seconds
    #         time.sleep(10)

    # def collect_continuous_data(self, stop_event):
    #     """Continuously collect container CPU, memory, and timing data."""
    #     start_time = time.time()
    #     lvol_created = 0  # Track total LVOLs created
    #     last_logged_minute = -1  # To prevent duplicate log entries

    #     # Initialize self.milestones if not already done
    #     if not hasattr(self, "milestones") or self.milestones is None:
    #         self.milestones = []

    #     while not stop_event.is_set():
    #         # Collect time elapsed
    #         elapsed_time = int((time.time() - start_time) / 60)  # Convert to minutes

    #         # Collect container stats
    #         for key, base_name in self.container_map.items():
    #             container_name = self.utils.get_container_name(base_name)
    #             cpu, memory_used, memory_total, memory_perc = self.utils.parse_docker_stats(container_name)
    #             self.continuous_cpu_usages[key].append(cpu)
    #             self.continuous_memory_usages[key].append(memory_used)

    #         # Collect timings for `sn list` and `lvol list`
    #         sn_time = self.measure_sn_list(continuous=True)
    #         lvol_time = self.measure_lvol_list(continuous=True)
    #         self.continuous_sn_list_times.append(sn_time)
    #         self.continuous_lvol_list_times.append(lvol_time)

    #         # Check LVOL milestones
    #         new_lvol_count = len(self.lvol_counts)
    #         if new_lvol_count > lvol_created:  # New LVOLs created
    #             lvol_created = new_lvol_count
    #             if lvol_created % 100 == 0 and all(m[1] != lvol_created for m in self.milestones):
    #                 self.milestones.append((elapsed_time, lvol_created))
    #                 self.log(f"Added milestone: Time = {elapsed_time} mins, LVOLs = {lvol_created}")

    #         # Log every 3-minute interval without duplication
    #         if elapsed_time % 3 == 0 and elapsed_time != last_logged_minute:
    #             self.log(f"Continuous data collection at {elapsed_time} mins: LVOLs = {lvol_created}")
    #             last_logged_minute = elapsed_time

    #         # Sleep for 10 seconds, but exit promptly if stop_event is set
    #         for _ in range(10):
    #             if stop_event.is_set():
    #                 break
    #             time.sleep(1)

    def write_continuous_data_to_files(self):
        """Save continuous data to text files for analysis."""
        # Ensure all lists in continuous_cpu_usages have the same length
        max_length = max(len(values) for values in self.continuous_cpu_usages.values())
        cpu_data = [
            values + [-1.0] * (max_length - len(values))  # Pad with -1.0 to signify missing values
            for values in self.continuous_cpu_usages.values()
        ]
        np.savetxt(
            "continuous_cpu_usages.txt",
            np.array(cpu_data).T,
            fmt="%.2f",
            header=" ".join(self.container_map.keys())
        )

        # Ensure memory usage data is the same length
        max_length = max(len(values) for values in self.continuous_memory_usages.values())
        memory_data = [
            values + [-1.0] * (max_length - len(values))
            for values in self.continuous_memory_usages.values()
        ]
        np.savetxt(
            "continuous_memory_usages.txt",
            np.array(memory_data).T,
            fmt="%.2f",
            header=" ".join(self.container_map.keys())
        )

        # Save LVOL and SN list times
        np.savetxt("continuous_lvol_list_times.txt", np.array(self.continuous_lvol_list_times + [-1.0] * (max_length - len(self.continuous_lvol_list_times))), fmt="%.2f")
        np.savetxt("continuous_sn_list_times.txt", np.array(self.continuous_sn_list_times + [-1.0] * (max_length - len(self.continuous_sn_list_times))), fmt="%.2f")

        np.savetxt("continuous_system_cpu_usage.txt", np.array(self.continuous_system_cpu_usages + [-1.0] * (max_length - len(self.continuous_system_cpu_usages))), fmt="%.2f")
        np.savetxt("continuous_system_memory_usage.txt", np.array(self.continuous_system_memory_usages + [-1.0] * (max_length - len(self.continuous_system_memory_usages))), fmt="%.2f")
        np.savetxt("continuous_system_buffer_memory.txt", np.array(self.continuous_system_memory_buffer + [-1.0] * (max_length - len(self.continuous_system_memory_buffer))), fmt="%.2f")

        # Save milestones
        if self.milestones:
            np.savetxt("milestones.txt", np.array(self.milestones), fmt="%.2f %d")
        else:
            self.log("No milestones to save.")

        self.log("Continuous data saved to files.")

    def read_continuous_data_from_files(self):
        """Read continuous data from text files for analysis."""
        # Read CPU usage data
        cpu_data = np.loadtxt("continuous_cpu_usages.txt")
        self.continuous_cpu_usages = {
            key: list(cpu_data[:, idx][cpu_data[:, idx] != -1.0])  # Remove padding (-1.0)
            for idx, key in enumerate(self.container_map.keys())
        }

        # Read memory usage data
        memory_data = np.loadtxt("continuous_memory_usages.txt")
        self.continuous_memory_usages = {
            key: list(memory_data[:, idx][memory_data[:, idx] != -1.0])  # Remove padding (-1.0)
            for idx, key in enumerate(self.container_map.keys())
        }

        # Read LVOL and SN list times
        self.continuous_lvol_list_times = list(np.loadtxt("continuous_lvol_list_times.txt"))
        self.continuous_sn_list_times = list(np.loadtxt("continuous_sn_list_times.txt"))
        self.continuous_system_cpu_usages = list(np.loadtxt("continuous_system_cpu_usage.txt"))
        self.continuous_system_memory_usages = list(np.loadtxt("continuous_system_memory_usage.txt"))
        self.continuous_system_memory_buffer = list(np.loadtxt("continuous_system_buffer_memory.txt"))

        # Read milestones
        try:
            self.milestones = [
                tuple(map(float, line.split())) for line in open("milestones.txt").readlines()
            ]
        except OSError:
            self.milestones = []
            self.log("No milestones file found. Defaulting to empty list.")

        # Read total test duration from `time_duration.txt`
        self.time_durations = []
        try:
            with open("time_durations.txt", "r") as f:
                for line in f:
                    if "Total Test Duration:" in line:
                        self.time_durations.append(float(line.split(":")[-1].replace("seconds", "").strip()))
                        break
                    # else:
                    #     self.time_durations.append(float(line.replace("seconds", "").strip()))
        except OSError:
            self.log("No time_durations.txt file found. Defaulting to empty durations list.")



    def write_data_to_files(self):
        """Save data collected during LVOL creation to files."""
        # Write LVOL count data
        np.savetxt("lvol_counts.txt", np.array(self.lvol_counts), fmt="%d")

        np.savetxt("fdb_sizes.txt", np.array(self.fdb_sizes), fmt="%.2f")
        np.savetxt("prometheus_sizes.txt", np.array(self.prometheus_sizes), fmt="%.2f")
        np.savetxt("graylog_sizes.txt", np.array(self.graylog_sizes), fmt="%.2f")

        # Write CPU usage data with padding
        max_length = max(len(values) for values in self.cpu_usages.values())
        cpu_data = np.full((max_length, len(self.cpu_usages)), -1.0)  # Initialize with padding
        for idx, key in enumerate(self.cpu_usages.keys()):
            cpu_data[:len(self.cpu_usages[key]), idx] = self.cpu_usages[key]
        np.savetxt(
            "cpu_usages.txt",
            cpu_data,
            fmt="%.2f",
            header=" ".join(self.cpu_usages.keys())
        )

        # Write memory usage data with padding
        max_length = max(len(values) for values in self.memory_usages.values())
        memory_data = np.full((max_length, len(self.memory_usages)), -1.0)  # Initialize with padding
        for idx, key in enumerate(self.memory_usages.keys()):
            memory_data[:len(self.memory_usages[key]), idx] = [record[0] for record in self.memory_usages[key]]  # Save memory_used
        np.savetxt(
            "memory_usages.txt",
            memory_data,
            fmt="%.2f",
            header=" ".join(self.memory_usages.keys())
        )

        # Write LVOL and SN list times
        lvol_list_padded = self.lvol_list_times + [-1.0] * (max_length - len(self.lvol_list_times))
        sn_list_padded = self.sn_list_times + [-1.0] * (max_length - len(self.sn_list_times))
        np.savetxt("lvol_list_times.txt", np.array(lvol_list_padded), fmt="%.2f")
        np.savetxt("sn_list_times.txt", np.array(sn_list_padded), fmt="%.2f")

        lvol_create_padded = self.lvol_create_times + [-1.0] * (max_length - len(self.lvol_create_times))
        np.savetxt("lvol_create_times.txt", np.array(lvol_create_padded), fmt="%.2f")

        # Save CPU and memory usage during LVOL creation
        for key in self.cpu_usages_lvol:
            np.savetxt(f"{key}_cpu_usage_lvol.txt", np.array(self.cpu_usages_lvol[key]), fmt="%.2f")
            memory_data = [stat[0] for stat in self.memory_usages_lvol[key]]  # Save memory_used
            np.savetxt(f"{key}_memory_usage_lvol.txt", np.array(memory_data), fmt="%.2f")

        # Save intervals for LVOL processing
        np.savetxt("intervals.txt", np.array(self.intervals), fmt="%d")

        self.log("All data saved to files.")

    def read_data_from_files(self):
        """Read data from files for analysis."""
        # Read LVOL count data
        self.lvol_counts = list(np.loadtxt("lvol_counts.txt", dtype=int))

        self.fdb_sizes = list(np.loadtxt("fdb_sizes.txt"))
        self.prometheus_sizes = list(np.loadtxt("prometheus_sizes.txt"))
        self.graylog_sizes = list(np.loadtxt("graylog_sizes.txt"))

        # Read CPU usage data and handle padding
        cpu_data = np.loadtxt("cpu_usages.txt")
        self.cpu_usages = {
            key: list(cpu_data[:, idx][cpu_data[:, idx] != -1.0])  # Filter out padding
            for idx, key in enumerate(self.container_map.keys())
        }

        # Read memory usage data and handle padding
        memory_data = np.loadtxt("memory_usages.txt")
        self.memory_usages = {
            key: list(memory_data[:, idx][memory_data[:, idx] != -1.0])  # Filter out padding
            for idx, key in enumerate(self.container_map.keys())
        }

        # Read LVOL and SN list times
        self.lvol_list_times = list(np.loadtxt("lvol_list_times.txt"))
        self.sn_list_times = list(np.loadtxt("sn_list_times.txt"))
        self.lvol_create_times = list(np.loadtxt("lvol_create_times.txt"))

        # Read CPU and memory usage during LVOL creation
        self.cpu_usages_lvol = {}
        self.memory_usages_lvol = {}
        for key in self.container_map.keys():
            try:
                self.cpu_usages_lvol[key] = list(np.loadtxt(f"{key}_cpu_usage_lvol.txt"))
                self.memory_usages_lvol[key] = list(np.loadtxt(f"{key}_memory_usage_lvol.txt"))
            except OSError:
                self.cpu_usages_lvol[key] = []
                self.memory_usages_lvol[key] = []
                self.log(f"No LVOL CPU or memory data found for container: {key}.")

        # Read intervals for LVOL processing
        try:
            self.intervals = list(np.loadtxt("intervals.txt", dtype=int))
        except OSError:
            self.intervals = []
            self.log("No intervals file found. Defaulting to empty list.")

        self.log("Data read from files.")


    def run(self):
        stop_event = threading.Event()
        continuous_thread = None

        time.sleep(60)
        self.log("Creating pool...")
        cmd = f"{self.sbcli_cmd} pool add {self.pool_name} {self.cluster_id}"
        self.utils.exec_cmd(cmd, error_ok=True)

        for batch in range(1, self.total_batches + 1):
            self.log(f"Starting batch: {batch}")
            lvol_idx = 0
            for lvol in range(1, self.batch_size + 1):
                lvol_idx = (batch - 1) * self.batch_size + lvol
                lvol_name = f"test_lvol_{lvol_idx}"
                self.create_lvol(lvol_name)

                if not continuous_thread:
                    continuous_thread = threading.Thread(target=self.collect_continuous_data, args=(stop_event,))
                    continuous_thread.start()

                # Gather data after each lvol creation
                fdb_size, prometheus_size, graylog_size = self.gather_data()
                self.fdb_sizes.append(fdb_size)
                self.prometheus_sizes.append(prometheus_size)
                self.graylog_sizes.append(graylog_size)
                self.lvol_counts.append(lvol_idx)
                self.gather_docker_stats_lvol()

                self.measure_sn_list()
                self.measure_lvol_list()

                self.log(f"Total Lvols: {len(self.lvol_counts)}, Lvol {lvol_idx}: FDB={fdb_size} GB, Prometheus={prometheus_size} GB, Graylog={graylog_size} GB")

                self.detect_size_change(batch, lvol_idx, fdb_size, prometheus_size, graylog_size)

                time.sleep(2)

            self.log(f"Completed batch {batch}. Waiting for 120 seconds...\n")
            ManagementStressUtils.log_system_psutil_resources(f"Batch {batch} (lvols {lvol_idx - self.batch_size}-{lvol_idx - 1})", self.log_file, self.csv_log_file, batch)
            ManagementStressUtils.log_all_container_resources(
                f"Batch {batch} - All Containers",
                self.log_file,
                "all_containers_resource.csv",
                batch
            )

            time.sleep(120)
            self.write_data_to_files()
            self.write_continuous_data_to_files()  # Save continuous data incrementally
            self.log("Moving to next batch")

        self.monitor_time_based_data()

        self.write_data_to_files()
        stop_event.set()
        continuous_thread.join()
        ManagementStressUtils.plot_psutil_resource_usage_both(
            self.csv_log_file,
            self.total_batches
        )
        ManagementStressUtils.plot_all_container_resources_both(
            "all_containers_resource.csv",
            self.total_batches
        )
        self.plot_results()


    def plot_from_files(self):
        """Read data from files and generate graphs."""
        self.read_data_from_files()
        self.read_continuous_data_from_files()
        self.plot_results()
        ManagementStressUtils.plot_psutil_resource_usage_both(csv_file=self.csv_log_file, total_batches=self.total_batches)
        ManagementStressUtils.plot_all_container_resources_both(csv_file="all_containers_resource.csv", total_batches=self.total_batches)

    def plot_results(self):
        """Plot memory usage, CPU usage, and timings versus number of lvols created."""
        # LVOL-wise plots
        self.plot_single(self.lvol_counts, self.fdb_sizes, "Number of Lvols Created", "FDB Size (GB)", "fdb_consumption_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.prometheus_sizes, "Number of Lvols Created", "Prometheus Size (GB)", "prometheus_consumption_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.graylog_sizes, "Number of Lvols Created", "Graylog Size (GB)", "graylog_consumption_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.lvol_create_times, "Number of Lvols Created", "Lvol Create Times (ms)", "lvol_create_times_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.sn_list_times, "Number of Lvols Created", "SN List Times (ms)", "sn_list_times_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.lvol_list_times, "Number of Lvols Created", "Lvol List Times (ms)", "lvol_list_times_lvol_wise.png")

        # # CPU usage LVOL-wise
        # self.plot_single(self.lvol_counts, self.cpu_usages_lvol["fdb"], "Number of Lvols Created", "FDB CPU Usage (%)", "fdb_cpu_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.cpu_usages_lvol["prometheus"], "Number of Lvols Created", "Prometheus CPU Usage (%)", "prometheus_cpu_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.cpu_usages_lvol["graylog"], "Number of Lvols Created", "Graylog CPU Usage (%)", "graylog_cpu_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.cpu_usages_lvol["graylog_os"], "Number of Lvols Created", "Graylog OS CPU Usage (%)", "graylog_os_cpu_usage_lvol_wise.png")

        # # Memory usage LVOL-wise
        # self.plot_single(self.lvol_counts, self.memory_usages_lvol["fdb"], "Number of Lvols Created", "FDB Memory Usage (MB)", "fdb_memory_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.memory_usages_lvol["prometheus"], "Number of Lvols Created", "Prometheus Memory Usage (MB)", "prometheus_memory_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.memory_usages_lvol["graylog"], "Number of Lvols Created", "Graylog Memory Usage (MB)", "graylog_memory_usage_lvol_wise.png")
        # self.plot_single(self.lvol_counts, self.memory_usages_lvol["graylog_os"], "Number of Lvols Created", "Graylog OS Memory Usage (MB)", "graylog_os_memory_usage_lvol_wise.png")
        
        # # CPU usage time-wise
        # self.plot_single(self.intervals, self.cpu_usages["fdb"], "Time(mins)", "FDB CPU Usage (%)", "fdb_cpu_usage_time_wise.png")
        # self.plot_single(self.intervals, self.cpu_usages["prometheus"], "Time(mins)", "Prometheus CPU Usage (%)", "prometheus_cpu_usage_time_wise.png")
        # self.plot_single(self.intervals, self.cpu_usages["graylog"], "Time(mins)", "Graylog CPU Usage (%)", "graylog_cpu_usage_time_wise.png")
        # self.plot_single(self.intervals, self.cpu_usages["graylog_os"], "Time(mins)", "Graylog OS CPU Usage (%)", "graylog_os_cpu_usage_time_wise.png")

        # # Memory usage time-wise
        # self.plot_single(self.intervals, self.memory_usages["fdb"], "Time(mins)", "FDB Memory Usage (MB)", "fdb_memory_usage_time_wise.png")
        # self.plot_single(self.intervals, self.memory_usages["prometheus"], "Time(mins)", "Prometheus Memory Usage (MB)", "prometheus_memory_usage_time_wise.png")
        # self.plot_single(self.intervals, self.memory_usages["graylog"], "Time(mins)", "Graylog Memory Usage (MB)", "graylog_memory_usage_time_wise.png")
        # self.plot_single(self.intervals, self.memory_usages["graylog_os"], "Time(mins)", "Graylog OS Memory Usage (MB)", "graylog_os_memory_usage_time_wise.png")

        for key, value in self.memory_usages.items():
            # Plot memory usage time wise
            if len(value):
                if isinstance(value[0], list):
                    memory_data = [stat[0] for stat in value]
                else:
                    memory_data = value

                self.plot_single(self.intervals, memory_data, "Time(mins)", f"{key.capitalize()} Memory (GB)", f"{key}_memory_usage_time.png")
            
        for key, value in self.memory_usages_lvol.items():
            # Plot memory usage lvol wise
            if len(value):
                if isinstance(value[0], list):
                    memory_data = [stat[0] for stat in value]
                else:
                    memory_data = value
                self.plot_single(self.lvol_counts, memory_data, "Number of LVOLs created", f"{key.capitalize()} Memory (GB)", f"{key}_memory_usage_lvol.png")

        for key, value in self.cpu_usages.items():
            # Plot cpu usage time wise
            if len(value):
                cpu_data = [stat for stat in value]
                self.plot_single(self.intervals, cpu_data, "Time(mins)", f"{key.capitalize()} CPU Usage (%)", f"{key}_cpu_usage_time.png")
            
        for key, value in self.cpu_usages_lvol.items():
            # Plot cpu usage lvol wise
            if len(value):
                cpu_data = [stat for stat in value]
                self.plot_single(self.lvol_counts, cpu_data, "Number of LVOLs created", f"{key.capitalize()} CPU Usage (%)", f"{key}_cpu_usage_lvol.png")

        # Batch-wise plots
        self.plot_batch_wise()
        self.plot_timewise()

    # def calculate_window_averages(self, data, window_size):
    #     """
    #     Calculate windowed averages for the given data.
        
    #     Args:
    #         data (list or array): The input data to calculate averages.
    #         window_size (int): The size of the window to calculate the moving average.

    #     Returns:
    #         list: A list of averaged values for each window.
    #     """
    #     if window_size <= 0:
    #         raise ValueError("Window size must be greater than 0.")
        
    #     averaged_values = []
    #     for i in range(0, len(data), window_size):
    #         window = data[i:i + window_size]
    #         average = sum(window) / len(window) if window else 0
    #         averaged_values.append(average)
        
    #     return averaged_values


    def plot_timewise(self):
        """Plot time-wise CPU, memory, SN list times, and LVOL list times with LVOL milestones."""
        if not self.milestones:
            self.log("No milestones available for plotting.")
            return

        # Compute cumulative time from time_durations or fallback to default
        if self.time_durations:
            cumulative_times = np.cumsum([0] + self.time_durations) / 60  # Convert seconds to minutes
            total_duration_minutes = cumulative_times[-1]  # Get the total duration as a scalar
        else:
            self.log("Time durations not available. Using default time.")
            total_duration_minutes = len(self.continuous_cpu_usages[next(iter(self.continuous_cpu_usages))]) * 3
            cumulative_times = np.arange(0, total_duration_minutes + 3, 3)  # Generate default cumulative times

        # Extend cumulative_time to cover the full duration
        cumulative_time = np.arange(0, total_duration_minutes + 1, 3)  # 3-minute intervals

        # def calculate_window_averages(data, window_size):
        #     """Calculate averages for a given window size."""
        #     return [
        #         np.mean(data[i * window_size:(i + 1) * window_size])
        #         for i in range(len(data) // window_size)
        #     ]
        def calculate_window_averages(data, window_size):
            """Calculate averages for a given window size."""
            if not data:
                return []
            
            total_windows = len(data) // window_size
            averaged_values = [
                np.mean(data[i * window_size:(i + 1) * window_size])
                for i in range(total_windows)
            ]
            
            # Handle leftover data (if any) by averaging the remainder
            if len(data) % window_size != 0:
                remainder = data[total_windows * window_size:]
                averaged_values.append(np.mean(remainder))
            
            return averaged_values


        window_size = 10  # Adjust window size as needed

        # Helper function to plot with milestones
        def plot_with_milestones(adjusted_time, values, label, ylabel, file_name):
            plt.figure(figsize=(18, 9))  # Increase graph size
            plt.plot(adjusted_time, values, label=label, linestyle="-", marker="o")
            
            for elapsed_time, lvol_count in self.milestones:
                if elapsed_time <= adjusted_time[-1]:  # Ensure milestone is within range
                    # Find the y-value on the graph at this milestone's x
                    y_value = np.interp(elapsed_time, adjusted_time, values)
                    plt.axvline(x=elapsed_time, color='red', linestyle='--', linewidth=1)
                    plt.scatter(elapsed_time, y_value, color='red', zorder=5)
                    plt.text(elapsed_time, y_value * 1.01, f"{lvol_count} LVOLs", color='red', fontsize=10)

            plt.title(f"{label} Over Time")
            plt.xlabel("Time (minutes)")
            plt.ylabel(ylabel)
            plt.legend()
            plt.grid()
            plt.tight_layout()
            plt.savefig(file_name)
            self.log(f"Saved plot: {file_name}")
            plt.close()

        # Plot CPU and Memory Usage
        for metric, data, ylabel in [
            ("CPU Usage (%)", self.continuous_cpu_usages, "CPU Usage (%)"),
            ("Memory Usage (GB)", self.continuous_memory_usages, "Memory Usage (GB)")
        ]:
            for key, values in data.items():
                print(f"Metric: {metric}, key: {key}")
                if len(values):
                    averaged_values = calculate_window_averages(values, window_size)
                    # Adjust cumulative_time to match averaged_values length
                    adjusted_time_in_minutes = cumulative_time[:len(averaged_values)]

                    # If lengths mismatch due to rounding errors, truncate the longer one
                    if len(adjusted_time_in_minutes) > len(averaged_values):
                        adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_values)]
                    elif len(adjusted_time_in_minutes) < len(averaged_values):
                        averaged_values = averaged_values[:len(adjusted_time_in_minutes)]

                    plot_with_milestones(
                        adjusted_time_in_minutes,
                        averaged_values,
                        f"{key.capitalize()} {metric}",
                        ylabel,
                        f"{key}_{metric.replace(' ', '_')}_timewise_avg.png"
                    )

        averaged_system_cpu_usage = calculate_window_averages(self.continuous_system_cpu_usages, window_size)
        adjusted_time_in_minutes = cumulative_time[:len(averaged_system_cpu_usage)]
        # If lengths mismatch due to rounding errors, truncate the longer one
        if len(adjusted_time_in_minutes) > len(averaged_system_cpu_usage):
            adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_system_cpu_usage)]
        elif len(adjusted_time_in_minutes) < len(averaged_system_cpu_usage):
            averaged_system_cpu_usage = averaged_system_cpu_usage[:len(adjusted_time_in_minutes)]
        plot_with_milestones(
            adjusted_time_in_minutes,
            averaged_system_cpu_usage,
            "System CPU usage",
            "CPU Usage (%)",
            "system_cpu_usage_average.png"
        )

        averaged_system_memory_usage = calculate_window_averages(self.continuous_system_memory_usages, window_size)
        adjusted_time_in_minutes = cumulative_time[:len(averaged_system_memory_usage)]
        # If lengths mismatch due to rounding errors, truncate the longer one
        if len(adjusted_time_in_minutes) > len(averaged_system_memory_usage):
            adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_system_memory_usage)]
        elif len(adjusted_time_in_minutes) < len(averaged_system_memory_usage):
            averaged_system_memory_usage = averaged_system_memory_usage[:len(adjusted_time_in_minutes)]
        plot_with_milestones(
            adjusted_time_in_minutes,
            averaged_system_memory_usage,
            "System Memory usage",
            "Memory Usage (GB)",
            "system_memory_usage_average.png"
        )

        averaged_system_buffer_usage = calculate_window_averages(self.continuous_system_memory_buffer, window_size)
        adjusted_time_in_minutes = cumulative_time[:len(averaged_system_buffer_usage)]
        # If lengths mismatch due to rounding errors, truncate the longer one
        if len(adjusted_time_in_minutes) > len(averaged_system_buffer_usage):
            adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_system_buffer_usage)]
        elif len(adjusted_time_in_minutes) < len(averaged_system_buffer_usage):
            averaged_system_buffer_usage = averaged_system_buffer_usage[:len(adjusted_time_in_minutes)]
        plot_with_milestones(
            adjusted_time_in_minutes,
            averaged_system_buffer_usage,
            "System Buffer usage",
            "Buffer Usage (GB)",
            "system_buffer_usage_average.png"
        )

        # Plot SN List Times
        averaged_sn_times = calculate_window_averages(self.continuous_sn_list_times, window_size)
        adjusted_time_in_minutes = cumulative_time[:len(averaged_sn_times)]
        # If lengths mismatch due to rounding errors, truncate the longer one
        if len(adjusted_time_in_minutes) > len(averaged_sn_times):
            adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_sn_times)]
        elif len(adjusted_time_in_minutes) < len(averaged_sn_times):
            averaged_sn_times = averaged_sn_times[:len(adjusted_time_in_minutes)]
        plot_with_milestones(
            adjusted_time_in_minutes,
            averaged_sn_times,
            "SN List Times",
            "SN List Times (ms)",
            "SN_List_Times_timewise_avg.png"
        )

        # Plot LVOL List Times
        averaged_lvol_times = calculate_window_averages(self.continuous_lvol_list_times, window_size)
        adjusted_time_in_minutes = cumulative_time[:len(averaged_lvol_times)]
        # If lengths mismatch due to rounding errors, truncate the longer one
        if len(adjusted_time_in_minutes) > len(averaged_lvol_times):
            adjusted_time_in_minutes = adjusted_time_in_minutes[:len(averaged_lvol_times)]
        elif len(adjusted_time_in_minutes) < len(averaged_lvol_times):
            averaged_lvol_times = averaged_lvol_times[:len(adjusted_time_in_minutes)]
        plot_with_milestones(
            adjusted_time_in_minutes,
            averaged_lvol_times,
            "LVOL List Times",
            "LVOL List Times (ms)",
            "LVOL_List_Times_timewise_avg.png"
        )

        self.log("Time-wise plots generated successfully.")




    # def plot_timewise(self):
    #     """Plot time-wise CPU, memory, and timing data with LVOL milestones averaged over 3-minute intervals."""
    #     interval_minutes = 3  # 3-minute intervals
    #     total_time_minutes = len(self.continuous_cpu_usages['fdb']) * 10 // 60  # Total time in minutes
    #     num_intervals = total_time_minutes // interval_minutes

    #     # Function to calculate averages over a window
    #     def calculate_window_averages(data, window_size):
    #         return [np.mean(data[i * window_size:(i + 1) * window_size]) for i in range(len(data) // window_size)]

    #     # Create averaged time intervals
    #     averaged_time_intervals = [(i + 1) * interval_minutes for i in range(num_intervals)]

    #     # Plot CPU and Memory Usage
    #     for metric, data in [("CPU Usage (%)", self.continuous_cpu_usages), ("Memory Usage (GB)", self.continuous_memory_usages)]:
    #         for key, value in data.items():
    #             # Calculate averages over 3-minute windows
    #             window_size = interval_minutes * 6  # 3 minutes = 18 * 10-second data points
    #             averaged_values = calculate_window_averages(value, window_size)

    #             plt.figure(figsize=(12, 6))
    #             plt.plot(averaged_time_intervals, averaged_values, label=f"{key.capitalize()} {metric}", linestyle="-", marker="o")

    #             # Add LVOL milestone markers
    #             for elapsed_time, lvol_count in self.milestones:
    #                 # Validate milestone within range
    #                 if elapsed_time // interval_minutes - 1 < len(averaged_values):
    #                     plt.axvline(x=elapsed_time, color='red', linestyle='--', linewidth=1)
    #                     plt.scatter(elapsed_time, averaged_values[elapsed_time // interval_minutes - 1], color='red', zorder=5)
    #                     plt.text(elapsed_time, averaged_values[elapsed_time // interval_minutes - 1] * 1.05,
    #                             f"{lvol_count} LVOLs", color='red', fontsize=9)

    #             plt.title(f"{key.capitalize()} {metric} Over Time (3-Minute Averages)")
    #             plt.xlabel("Time (minutes)")
    #             plt.ylabel(metric)
    #             plt.legend()
    #             plt.grid()
    #             plt.tight_layout()
    #             plt.savefig(f"{key}_{metric.replace(' ', '_')}_timewise_avg_3min.png")
    #             self.log(f"Saved 3-minute averaged time-wise plot: {key}_{metric.replace(' ', '_')}_timewise_avg_3min.png")
    #             plt.close()

    # def plot_timewise(self, interval_minutes=6):
    #     """Plot time-wise CPU, memory, and timing data with LVOL milestones averaged over a specified interval."""
    #     interval_seconds = interval_minutes * 60  # Convert interval to seconds
    #     window_size = interval_seconds // 10  # Convert interval to number of data points (10-second intervals)

    #     def calculate_window_averages(data, window_size):
    #         """Calculate averages for a given window size."""
    #         averages = [np.mean(data[i * window_size:(i + 1) * window_size]) for i in range(len(data) // window_size)]
    #         if len(data) % window_size != 0:  # Include leftover data in the final average
    #             averages.append(np.mean(data[(len(data) // window_size) * window_size:]))
    #         return averages

    #     # Calculate CPU and memory averages
    #     for metric, data in [("CPU Usage (%)", self.continuous_cpu_usages), ("Memory Usage (GB)", self.continuous_memory_usages)]:
    #         for key, value in data.items():
    #             averaged_values = calculate_window_averages(value, window_size)
    #             averaged_time_intervals = [i * interval_minutes for i in range(1, len(averaged_values) + 1)]

    #             plt.figure(figsize=(12, 6))
    #             plt.plot(averaged_time_intervals, averaged_values, label=f"{key.capitalize()} {metric}", linestyle="-", marker="o")

    #             # Add LVOL milestone markers
    #             for elapsed_time, lvol_count in self.milestones:
    #                 milestone_time = elapsed_time / 60  # Convert seconds to minutes
    #                 if milestone_time <= averaged_time_intervals[-1]:  # Ensure milestone is within the plot range
    #                     nearest_index = min(range(len(averaged_time_intervals)),
    #                                         key=lambda i: abs(averaged_time_intervals[i] - milestone_time))
    #                     plt.axvline(x=averaged_time_intervals[nearest_index], color='red', linestyle='--', linewidth=1)
    #                     plt.scatter(averaged_time_intervals[nearest_index], averaged_values[nearest_index], color='red', zorder=5)
    #                     plt.text(averaged_time_intervals[nearest_index], averaged_values[nearest_index] * 1.05,
    #                             f"{lvol_count} LVOLs", color='red', fontsize=9)

    #             plt.title(f"{key.capitalize()} {metric} Over Time ({interval_minutes}-Minute Averages)")
    #             plt.xlabel("Time (minutes)")
    #             plt.ylabel(metric)
    #             plt.legend()
    #             plt.grid()
    #             plt.tight_layout()
    #             plt.savefig(f"{key}_{metric.replace(' ', '_')}_timewise_avg_{interval_minutes}min.png")
    #             self.log(f"Saved {interval_minutes}-minute averaged time-wise plot: {key}_{metric.replace(' ', '_')}_timewise_avg_{interval_minutes}min.png")
    #             plt.close()

    #     # Plot LVOL List Times
    #     lvol_averaged_values = calculate_window_averages(self.continuous_lvol_list_times, window_size)
    #     averaged_time_intervals = [i * interval_minutes for i in range(1, len(lvol_averaged_values) + 1)]
    #     plt.figure(figsize=(12, 6))
    #     plt.plot(averaged_time_intervals, lvol_averaged_values, label="LVOL List Times (ms)", linestyle="-", marker="o", color="green")
    #     for elapsed_time, lvol_count in self.milestones:
    #         milestone_time = elapsed_time / 60
    #         if milestone_time <= averaged_time_intervals[-1]:
    #             nearest_index = min(range(len(averaged_time_intervals)),
    #                                 key=lambda i: abs(averaged_time_intervals[i] - milestone_time))
    #             plt.axvline(x=averaged_time_intervals[nearest_index], color='red', linestyle='--', linewidth=1)
    #             plt.scatter(averaged_time_intervals[nearest_index], lvol_averaged_values[nearest_index], color='red', zorder=5)
    #             plt.text(averaged_time_intervals[nearest_index], lvol_averaged_values[nearest_index] * 1.05,
    #                     f"{lvol_count} LVOLs", color='red', fontsize=9)
    #     plt.title("LVOL List Times Over Time (6-Minute Averages)")
    #     plt.xlabel("Time (minutes)")
    #     plt.ylabel("LVOL List Times (ms)")
    #     plt.legend()
    #     plt.grid()
    #     plt.tight_layout()
    #     plt.savefig("lvol_list_times_timewise_avg_6min.png")
    #     self.log("Saved 6-minute averaged LVOL List Times plot: lvol_list_times_timewise_avg_6min.png")
    #     plt.close()


    def plot_batch_wise(self):
        """Plot batch-wise average timings, disk usage, CPU usage, and memory usage."""
        batches = np.arange(1, self.total_batches + 1)
        batch_create_avg = np.mean(np.split(np.array(self.lvol_create_times), self.total_batches), axis=1)
        batch_sn_list_avg = np.mean(np.split(np.array(self.sn_list_times), self.total_batches), axis=1)
        batch_lvol_list_avg = np.mean(np.split(np.array(self.lvol_list_times), self.total_batches), axis=1)
        batch_fdb_avg = np.mean(np.split(np.array(self.fdb_sizes), self.total_batches), axis=1)
        batch_prometheus_avg = np.mean(np.split(np.array(self.prometheus_sizes), self.total_batches), axis=1)
        batch_graylog_avg = np.mean(np.split(np.array(self.graylog_sizes), self.total_batches), axis=1)

        # Plot batch-wise timings
        self.plot_single(batches, batch_create_avg, "Batch Number", "Avg Lvol Create Times (ms)", "lvol_create_times_batch_wise.png")
        self.plot_single(batches, batch_sn_list_avg, "Batch Number", "Avg SN List Times (ms)", "sn_list_times_batch_wise.png")
        self.plot_single(batches, batch_lvol_list_avg, "Batch Number", "Avg Lvol List Times (ms)", "lvol_list_times_batch_wise.png")

        # Plot batch-wise disk usage
        self.plot_single(batches, batch_fdb_avg, "Batch Number", "Avg FDB Size (GB)", "fdb_consumption_batch_wise.png")
        self.plot_single(batches, batch_prometheus_avg, "Batch Number", "Avg Prometheus Size (GB)", "prometheus_consumption_batch_wise.png")
        self.plot_single(batches, batch_graylog_avg, "Batch Number", "Avg Graylog Size (GB)", "graylog_consumption_batch_wise.png")

        # # Plot batch-wise CPU usage
        # self.plot_single(batches, batch_fdb_cpu_avg, "Avg FDB CPU Usage (%)", "fdb_cpu_usage_batch_wise.png")
        # self.plot_single(batches, batch_prometheus_cpu_avg, "Avg Prometheus CPU Usage (%)", "prometheus_cpu_usage_batch_wise.png")
        # self.plot_single(batches, batch_graylog_cpu_avg, "Avg Graylog CPU Usage (%)", "graylog_cpu_usage_batch_wise.png")
        # self.plot_single(batches, batch_graylog_os_cpu_avg, "Avg Graylog OS CPU Usage (%)", "graylog_os_cpu_usage_batch_wise.png")

        # # Plot batch-wise memory usage
        # self.plot_single(batches, batch_fdb_mem_avg, "Avg FDB Memory Usage (MB)", "fdb_memory_usage_batch_wise.png")
        # self.plot_single(batches, batch_prometheus_mem_avg, "Avg Prometheus Memory Usage (MB)", "prometheus_memory_usage_batch_wise.png")
        # self.plot_single(batches, batch_graylog_mem_avg, "Avg Graylog Memory Usage (MB)", "graylog_memory_usage_batch_wise.png")
        # self.plot_single(batches, batch_graylog_os_mem_avg, "Avg Graylog OS Memory Usage (MB)", "graylog_os_memory_usage_batch_wise.png")

        # Log batch-wise averages
        self.log("Batch-wise averages (Disk Usage in GB):")
        for i, (fdb, prometheus, graylog) in enumerate(zip(batch_fdb_avg, batch_prometheus_avg, batch_graylog_avg), start=1):
            self.log(
                f"Batch {i}: "
                f"FDB Avg={fdb:.2f} GB, Prometheus Avg={prometheus:.2f} GB, Graylog Avg={graylog:.2f} GB"
            )

    def plot_single(self, x, y, xlabel, ylabel, filename):
        """Plot a single type of memory usage or timing."""
        plt.figure(figsize=(12, 6))
        plt.plot(x, y, label=ylabel, linestyle="-", marker="o")
        plt.title(f"{ylabel} vs {xlabel}")
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig(filename)
        self.log(f"Saved plot: {filename}")
        plt.close()


# Main function to parse arguments and execute the test
def main():
    parser = argparse.ArgumentParser(description="Run or plot lvol memory tracking test.")
    parser.add_argument("--sbcli_cmd", default="sbcli-mock", help="Command to execute sbcli (default: sbcli-mock).")
    parser.add_argument("--cluster_id", required=True, help="Cluster ID for the test.")
    parser.add_argument("--total_lvols", type=int, default=100, help="Total number of LVOLs to create.")
    parser.add_argument("--batch_size", type=int, default=25, help="Number of lvols to create per batch (default: 25).")
    parser.add_argument("--continue_from_log", action="store_true", help="Resume test from last recorded lvol count in log file.")
    parser.add_argument("--plot", action="store_true", help="Generate graphs from saved data files without creating LVOLs.")
    parser.add_argument("--csv_log_file", default="resource_data.csv", help="CSV file to store structured resource data.")

    args = parser.parse_args()

    utils = ManagementStressUtils()

    # Determine how many lvols already exist (for resume support)
    resume_count = 0
    if args.continue_from_log and os.path.exists("lvol_counts.txt"):
        try:
            lvol_counts = np.loadtxt("lvol_counts.txt", dtype=int)
            if isinstance(lvol_counts, np.ndarray):
                resume_count = int(lvol_counts[-1])
            else:
                resume_count = int(lvol_counts)
            print(f"Resuming from lvol index: {resume_count + 1}")
        except Exception as e:
            print(f"Could not parse lvol_counts.txt for resume: {e}")

    lvols_to_create = max(0, args.total_lvols - resume_count)
    total_batches = (lvols_to_create + args.batch_size - 1) // args.batch_size

    test = TestLvolMemory(
        sbcli_cmd=args.sbcli_cmd,
        cluster_id=args.cluster_id,
        utils=utils,
        total_batches=total_batches,
        batch_size=args.batch_size,
        csv_log_file=args.csv_log_file
    )

    # If resuming, pre-populate lvol_counts to avoid overwrite
    if resume_count > 0:
        test.read_data_from_files()
        test.read_continuous_data_from_files()

    if args.plot:
        test.plot_from_files()
    else:
        test.run()


if __name__ == "__main__":
    main()
