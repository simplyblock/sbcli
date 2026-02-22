import os
import argparse
from datetime import datetime
from utils.ssh_utils import SshUtils, RunnerK8sLog
from logger_config import setup_logger


class ContinuousLogCollector:
    def __init__(self, test_name: str, log_dir: str = "/mnt/nfs_share"):
        self.logger = setup_logger("ContinuousLogCollector")

        self.bastion = os.environ.get("BASTION_IP")
        self.user = os.getenv("USER", "root")

        self.storage_nodes = os.getenv("STORAGE_PRIVATE_IPS", "").split()
        self.sec_storage_nodes = os.getenv("SEC_STORAGE_PRIVATE_IPS", "").split()
        self.mgmt_nodes = os.getenv("MNODES", "").split()
        self.client_ips = os.getenv("CLIENTNODES", os.getenv("MNODES", "")).split()

        self.k8s_test = os.environ.get("K8S_TEST", "false").lower() == "true"

        self.ssh_obj = SshUtils(bastion_server=self.bastion)
        self.runner_k8s_log = None

        self.test_name = test_name

        # Treat provided log_dir as BASE directory, always create <test>-<timestamp> inside it
        self.base_log_dir = log_dir or "/mnt/nfs_share"
        self.docker_logs_path = self.get_log_directory()

    def get_log_directory(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        safe_test = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in self.test_name)
        return os.path.join(self.base_log_dir, f"{safe_test}-{timestamp}")

    def collect_logs(self, test_name: str):
        all_nodes = set()
        all_nodes.update(self.mgmt_nodes)
        all_nodes.update(self.storage_nodes)
        all_nodes.update(self.sec_storage_nodes)
        all_nodes.update(self.client_ips)
        all_nodes = list(filter(None, all_nodes))

        for node in all_nodes:
            try:
                self.logger.info(f"Setting up logging on: {node}")
                self.ssh_obj.connect(address=node, bastion_server_address=self.bastion)

                # Ensure log dir exists on node
                self.ssh_obj.make_directory(node, dir_name=self.docker_logs_path)

                self.ssh_obj.check_tmux_installed(node_ip=node)
                self.ssh_obj.exec_command(node, "sudo tmux kill-server")

                containers = self.ssh_obj.get_running_containers(node_ip=node)

                # Automatically monitor new containers with polling
                self.ssh_obj.monitor_container_logs(
                    node_ip=node,
                    containers=containers,
                    log_dir=self.docker_logs_path,
                    test_name=test_name,
                    poll_interval=60  # checks every 60 seconds
                )

                self.ssh_obj.start_tcpdump_logging(node_ip=node, log_dir=self.docker_logs_path)
                self.ssh_obj.start_netstat_dmesg_logging(node_ip=node, log_dir=self.docker_logs_path)

            except Exception as e:
                self.logger.warning(f"Skipping node {node} due to error: {e}")

        if self.k8s_test:
            try:
                self.logger.info("Starting k8s pod logging...")
                self.runner_k8s_log = RunnerK8sLog(log_dir=self.docker_logs_path, test_name=test_name)
                self.runner_k8s_log.start_logging()
                self.runner_k8s_log.monitor_pod_logs()
            except Exception as e:
                self.logger.warning(f"Failed to start k8s logging: {e}")

        self.logger.info(f"Continuous logging started at: {self.docker_logs_path}")
        self.logger.info("Kill this process with 'kill -9' when done.")

        # Infinite loop
        while True:
            pass


def parse_args():
    parser = argparse.ArgumentParser(description="Continuous log collector")
    parser.add_argument(
        "--test-name",
        required=True,
        help="Test case name (used in log folder + log tagging)",
    )
    parser.add_argument(
        "--log-dir",
        default="/mnt/nfs_share",
        help="Base log directory (a <test-name>-<timestamp> subfolder will be created inside it)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    collector = ContinuousLogCollector(test_name=args.test_name, log_dir=args.log_dir)
    collector.collect_logs(test_name=args.test_name)

