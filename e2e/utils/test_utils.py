from logger_config import setup_logger
import re


class TestUtils:
    """Contains common validations and parsers
    """
    def __init__(self, sbcli_utils, ssh_utils):
        self.sbcli_utils = sbcli_utils
        self.ssh_utils = ssh_utils
        self.logger = setup_logger(__name__)

    def validate_event_logs(self, cluster_id, operations):
        """Validates event logs for cluster

        Args:
            cluster_id (str): Cluster id to check logs on
            operations (Dict): Steps performed for each type of entity
        """
        logs = self.sbcli_utils.get_cluster_logs(cluster_id)
        actual_logs = []
        for log in logs:
            actual_logs.append(log["Message"])
        to_check_in_logs = []
        for type, steps in operations.items():
            prev_step = None
            if type == "Storage Node":
                for step in steps:
                    if step == "suspended":
                        to_check_in_logs.append("Storage node status changed from: online to: suspended")
                        prev_step = "suspended"
                    if step == "shutdown":
                        if prev_step == "suspended":
                            to_check_in_logs.append("Storage node status changed from: suspended to: in_shutdown")
                            to_check_in_logs.append("Storage node status changed from: in_shutdown to: offline")
                        else:
                            to_check_in_logs.append("Storage node status changed from: online to: in_shutdown")
                            to_check_in_logs.append("Storage node status changed from: in_shutdown to: offline")
                        prev_step = "shutdown"
                    if step == "restart":
                        to_check_in_logs.append("Storage node status changed from: offline to: in_restart")
                        to_check_in_logs.append("Storage node status changed from: in_restart to: online")
            if type == "Device":
                for step in steps:
                    if step == "restart":
                        to_check_in_logs.append("Device status changed from: online to: unavailable")
                        # TODO: Change from unavailable to online once bug is fixed.
                        to_check_in_logs.append("Device status changed from: online to: online")

        for expected_log in to_check_in_logs:
            assert expected_log in actual_logs, f"Expected event/log {expected_log} not found in Actual logs: {actual_logs}"

    def validate_fio_test(self, node, log_file):
        """Validates interruptions in FIO log

        Args:
            node (str): Node Host Name to check log file on
            log_file (str): Path to log file

        Raises:
            RuntimeError: If there are interruptions
        """
        file_data = self.ssh_utils.read_file(node, log_file)
        fail_words = ["error", "fail", "latency", "throughput"]
        for word in fail_words:
            if word in file_data:
                raise RuntimeError("FIO Test has interuupts")
            
    
    def parse_lvol_cluster_map_output(self, output):
        """Parses LVOL cluster map output

        Args:
            output (str): Command Output for get-cluster map

        Returns:
            Dict, Dict: Details about Nodes and Devices
        """
        nodes = {}
        devices = {}

        # Regular expression patterns
        node_pattern = re.compile(r'\| Node \s*\|\s*([0-9a-f-]+)\s*\|\s*(\w+)\s*\|\s*(\w+)\s*\|\s*(\w+)\s*\|')
        device_pattern = re.compile(r'\| Device \s*\|\s*([0-9a-f-]+)\s*\|\s*(\w+)\s*\|\s*(\w+)\s*\|\s*(\w+)\s*\|')

        # Find all nodes and devices in the table
        for line in output.split('\n'):
            node_match = node_pattern.match(line)
            device_match = device_pattern.match(line)
            if node_match:
                uuid, reported_status, actual_status, results = node_match.groups()
                nodes[uuid] = {
                    "Kind": "Node",
                    "UUID": uuid,
                    "Reported Status": reported_status,
                    "Actual Status": actual_status,
                    "Results": results
                }
            if device_match:
                uuid, reported_status, actual_status, results = device_match.groups()
                devices[uuid] = {
                    "Kind": "Device",
                    "UUID": uuid,
                    "Reported Status": reported_status,
                    "Actual Status": actual_status,
                    "Results": results
                }
        self.logger.info("Nodes:")
        for uuid, node in nodes.items():
            self.logger.info(node)

        self.logger.info("Devices:")
        for uuid, device in devices.items():
            self.logger.info(device)

        return nodes, devices