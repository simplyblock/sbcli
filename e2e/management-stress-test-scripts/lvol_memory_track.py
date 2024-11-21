import argparse
import subprocess
import matplotlib.pyplot as plt
import time
import numpy as np


class ManagementStressUtils:
    """Utility class for common methods like executing commands and gathering data."""

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
        end_time = time.time()
        elapsed_time = (end_time - start_time) * 1000  # Convert to milliseconds
        return elapsed_time, result

    @staticmethod
    def get_fdb_size():
        """Get FDB size."""
        cmd = 'fdbcli --exec "status" | grep "Disk space used" | awk \'{print $5}\''
        result = ManagementStressUtils.exec_cmd(cmd)
        try:
            size = result.upper().strip()
            if "K" in size:
                return int(float(size.replace("K", "")) / 1024)
            if "G" in size:
                return int(float(size.replace("G", "")) * 1024)  # Convert GB to MB
            elif "M" in size:
                return int(float(size.replace("M", "")))
            return int(size)
        except ValueError:
            print(f"Invalid FDB size output: {result}")
            return 0

    @staticmethod
    def get_directory_size(path):
        """Get size of a directory in MB."""
        cmd = f"sudo du -sh {path} | awk '{{print $1}}'"
        result = ManagementStressUtils.exec_cmd(cmd)
        try:
            size = result.upper()
            if "K" in size:
                return int(float(size.replace("K", "")) / 1024)
            elif "M" in size:
                return int(float(size.replace("M", "")))
            elif "G" in size:
                return int(float(size.replace("G", "")) * 1024)
            return int(size)
        except ValueError:
            print(f"Invalid directory size output for {path}: {result}")
            return 0


class TestLvolMemory:
    """Test case class for creating lvols and tracking memory consumption."""

    def __init__(self, sbcli_cmd, cluster_id, utils,
                 log_file="test_lvol_memory.log", size_change_log="size_change.log",
                 timings_log="timings.log", pool_name="pool1", total_batches=100, batch_size=25):
        self.utils = utils
        self.sbcli_cmd = sbcli_cmd
        self.cluster_id = cluster_id
        self.pool_name = pool_name
        self.total_batches = total_batches
        self.batch_size = batch_size
        self.fdb_sizes = []
        self.prometheus_sizes = []
        self.graylog_sizes = []
        self.lvol_counts = []
        self.lvol_create_times = []
        self.sn_list_times = []
        self.lvol_list_times = []
        self.log_file = log_file
        self.size_change_log = size_change_log
        self.timings_log = timings_log
        self.last_lvol_with_sizes = None  # Stores the last lvol with recorded sizes
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
        print(message)  # Print to console
        with open(self.log_file, "a", encoding="utf-8") as log:
            log.write(message + "\n")

    def log_timing(self, message):
        """Write a message to the timings log file and print it to the console."""
        print(message)  # Print to console
        with open(self.timings_log, "a", encoding="utf-8") as log:
            log.write(message + "\n")

    def log_size_change(self, batch_idx, lvol_idx, last_sizes, current_sizes):
        """Log size differences between lvols to the size change log."""
        log_message = (
            f"Size difference between Batch {last_sizes['batch']}, Lvol {last_sizes['lvol']}: "
            f"FDB={last_sizes['fdb']} MB, Prometheus={last_sizes['prometheus']} MB, "
            f"Graylog={last_sizes['graylog']} MB\n"
            f"and Batch {batch_idx}, Lvol {lvol_idx}: "
            f"FDB={current_sizes['fdb']} MB, Prometheus={current_sizes['prometheus']} MB, "
            f"Graylog={current_sizes['graylog']} MB\n"
            f"Difference: FDB={current_sizes['fdb'] - last_sizes['fdb']} MB, "
            f"Prometheus={current_sizes['prometheus'] - last_sizes['prometheus']} MB, "
            f"Graylog={current_sizes['graylog'] - last_sizes['graylog']} MB\n"
        )
        print(log_message)
        with open(self.size_change_log, "a", encoding="utf-8") as log:
            log.write(log_message + "\n")

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
        graylog_mongodb = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_mongodb_data/")
        graylog_os = self.utils.get_directory_size("/var/lib/docker/volumes/monitoring_os_data/")
        graylog_total = graylog_journal + graylog_mongodb + graylog_os
        return fdb_size, prometheus_size, graylog_total

    def create_lvol(self, lvol_name):
        """Create an lvol and measure its response time."""
        cmd = f"{self.sbcli_cmd} lvol add {lvol_name} 200M {self.pool_name}"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        self.lvol_create_times.append(elapsed_time)
        self.log_timing(f"Lvol Create Time: {elapsed_time:.2f} ms")
        return elapsed_time

    def measure_sn_list(self):
        """Measure `sn list` command timing."""
        cmd = f"{self.sbcli_cmd} sn list"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        self.sn_list_times.append(elapsed_time)
        self.log_timing(f"SN List Time: {elapsed_time:.2f} ms")
        return elapsed_time

    def measure_lvol_list(self):
        """Measure `lvol list` command timing."""
        cmd = f"{self.sbcli_cmd} lvol list"
        elapsed_time, _ = self.utils.measure_cmd_time(cmd)
        self.lvol_list_times.append(elapsed_time)
        self.log_timing(f"Lvol List Time: {elapsed_time:.2f} ms")
        return elapsed_time

    def run(self):
        """Main function to execute the lvol creation and data gathering."""
        self.log("Creating pool...")
        cmd = f"{self.sbcli_cmd} pool add {self.pool_name} {self.cluster_id}"
        self.utils.exec_cmd(cmd, error_ok=True)

        for batch in range(1, self.total_batches + 1):
            for lvol in range(1, self.batch_size + 1):
                lvol_idx = (batch - 1) * self.batch_size + lvol
                lvol_name = f"test_lvol_{lvol_idx}"
                batch_lvl_num_msg = f"Batch: {batch}, LVOL: {lvol}"
                print(batch_lvl_num_msg + "\n")
                with open(self.timings_log, "a", encoding="utf-8") as log:
                    log.write(batch_lvl_num_msg + "\n")
                self.create_lvol(lvol_name)

                # Gather data after each lvol creation
                fdb_size, prometheus_size, graylog_size = self.gather_data()
                self.fdb_sizes.append(fdb_size)
                self.prometheus_sizes.append(prometheus_size)
                self.graylog_sizes.append(graylog_size)
                self.lvol_counts.append(lvol_idx)

                # Measure timings for sn list and lvol list
                self.measure_sn_list()
                self.measure_lvol_list()

                # Log memory sizes

                self.log(f"Lvol {lvol_idx}: FDB={fdb_size} MB, Prometheus={prometheus_size} MB, Graylog={graylog_size} MB")
                # Detect and log size changes
                self.detect_size_change(batch, lvol_idx, fdb_size, prometheus_size, graylog_size)

                # Wait between lvols
                time.sleep(2)

            # Process batch completion
            self.log(f"Completed batch {batch}. Waiting for 120 seconds...\n")
            time.sleep(120)

        # Plot results
        self.plot_results()

    def plot_results(self):
        """Plot memory usage and timings versus number of lvols created."""
        self.plot_single(self.lvol_counts, self.fdb_sizes, "FDB Size (MB)", "fdb_consumption_vs_lvols.png")
        self.plot_single(self.lvol_counts, self.prometheus_sizes, "Prometheus Size (MB)", "prometheus_consumption_vs_lvols.png")
        self.plot_single(self.lvol_counts, self.graylog_sizes, "Graylog Size (MB)", "graylog_consumption_vs_lvols.png")
        self.plot_single(self.lvol_counts, self.lvol_create_times, "Lvol Create Times (ms)", "lvol_create_times_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.sn_list_times, "SN List Times (ms)", "sn_list_times_lvol_wise.png")
        self.plot_single(self.lvol_counts, self.lvol_list_times, "Lvol List Times (ms)", "lvol_list_times_lvol_wise.png")

        # Plot batch-wise results
        self.plot_batch_wise()

    def plot_batch_wise(self):
        """Plot batch-wise average timings and disk usage."""
        batches = np.arange(1, self.total_batches + 1)
        batch_create_avg = np.mean(np.split(np.array(self.lvol_create_times), self.total_batches), axis=1)
        batch_sn_list_avg = np.mean(np.split(np.array(self.sn_list_times), self.total_batches), axis=1)
        batch_lvol_list_avg = np.mean(np.split(np.array(self.lvol_list_times), self.total_batches), axis=1)
        batch_fdb_avg = np.mean(np.split(np.array(self.fdb_sizes), self.total_batches), axis=1)
        batch_prometheus_avg = np.mean(np.split(np.array(self.prometheus_sizes), self.total_batches), axis=1)
        batch_graylog_avg = np.mean(np.split(np.array(self.graylog_sizes), self.total_batches), axis=1)

        # Plot timings
        self.plot_single(batches, batch_create_avg, "Avg Lvol Create Times (ms)", "lvol_create_times_batch_wise.png")
        self.plot_single(batches, batch_sn_list_avg, "Avg SN List Times (ms)", "sn_list_times_batch_wise.png")
        self.plot_single(batches, batch_lvol_list_avg, "Avg Lvol List Times (ms)", "lvol_list_times_batch_wise.png")

        # Plot disk usage
        self.plot_single(batches, batch_fdb_avg, "Avg FDB Size (MB)", "fdb_consumption_batch_wise.png")
        self.plot_single(batches, batch_prometheus_avg, "Avg Prometheus Size (MB)", "prometheus_consumption_batch_wise.png")
        self.plot_single(batches, batch_graylog_avg, "Avg Graylog Size (MB)", "graylog_consumption_batch_wise.png")

        # Log batch-wise averages
        self.log("Batch-wise averages (in MB and ms):")
        for i, (fdb, prometheus, graylog, create_time, sn_time, lvol_time) in enumerate(
                zip(batch_fdb_avg, batch_prometheus_avg, batch_graylog_avg, batch_create_avg, batch_sn_list_avg, batch_lvol_list_avg), start=1):
            self.log(
                f"Batch {i}: FDB Avg={fdb:.2f}, Prometheus Avg={prometheus:.2f}, Graylog Avg={graylog:.2f}, "
                f"Lvol Create Avg={create_time:.2f} ms, SN List Avg={sn_time:.2f} ms, Lvol List Avg={lvol_time:.2f} ms"
            )

    def plot_single(self, x, y, label, filename):
        """Plot a single type of memory usage or timing."""
        plt.figure(figsize=(12, 6))
        plt.plot(x, y, label=label, linestyle="-", marker="o")
        plt.title(f"{label} vs Number of Lvols Created")
        plt.xlabel("Batch Number" if not isinstance(x[0], int) else "Number of Lvols Created")
        plt.ylabel(label)
        plt.legend()
        plt.grid()
        plt.tight_layout()
        plt.savefig(filename)
        self.log(f"Saved plot: {filename}")
        plt.close()


# Main function to parse arguments and execute the test
def main():
    parser = argparse.ArgumentParser(description="Run lvol memory tracking test.")
    parser.add_argument("--sbcli_cmd", default="sbcli-mock", help="Command to execute sbcli (default: sbcli-mock).")
    parser.add_argument("--cluster_id", required=True, help="Cluster ID for the test.")
    args = parser.parse_args()

    utils = ManagementStressUtils()
    test = TestLvolMemory(sbcli_cmd=args.sbcli_cmd, cluster_id=args.cluster_id, utils=utils)
    test.run()


if __name__ == "__main__":
    main()
