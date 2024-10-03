import time
from logger_config import setup_logger
import re


class CommonUtils:
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
        actual_logs = [log["Message"] for log in logs]
        
        status_patterns = {
            "Storage Node": {
                "suspended": re.compile(r"Storage node status changed from: .+ to: suspended"),
                "shutdown": [
                    re.compile(r"Storage node status changed from: .+ to: in_shutdown"),
                    re.compile(r"Storage node status changed from: in_shutdown to: offline")
                ],
                "restart": [
                    re.compile(r"Storage node status changed from: offline to: in_restart"),
                    re.compile(r"Storage node status changed from: offline to: online")
                ]
            },
            "Device": {
                "restart": [
                    re.compile(r"Device status changed from: .+ to: unavailable"),
                    # TODO: Change from unavailable to online once bug is fixed.
                    re.compile(r"Device restarted")
                ]
            }
        }
        
        for entity_type, steps in operations.items():
            for step in steps:
                patterns = status_patterns.get(entity_type, {}).get(step, [])
                if not isinstance(patterns, list):
                    patterns = [patterns]
                for pattern in patterns:
                    if not any(pattern.search(log) for log in actual_logs):
                        raise ValueError(f"Expected pattern not found for {entity_type} step '{step}': {pattern.pattern}")

    def validate_fio_test(self, node, log_file):
        """Validates interruptions in FIO log

        Args:
            node (str): Node Host Name to check log file on
            log_file (str): Path to log file

        Raises:
            RuntimeError: If there are interruptions
        """
        file_data = self.ssh_utils.read_file(node, log_file)
        fail_words = ["error", "fail", "throughput", "interrupt", "terminate"]
        for word in fail_words:
            if word in file_data:
                raise RuntimeError("FIO Test has interuupts")

    def manage_fio_threads(self, node, threads, timeout=100):
        """Run till fio process is complete and joins the thread

        Args:
            node (str): Node IP where fio is running
            threads (list): List of threads
            timeout (int): Time to check for completion

        Raises:
            RuntimeError: If fio process hang
        """
        self.logger.info("Waiting for FIO processes to complete!")
        sleep_n_sec(10)
        while True:
            process = self.ssh_utils.find_process_name(node=node,
                                                       process_name="fio")
            process_fio = [element for element in process if "grep" not in element]
            self.logger.info(f"Process info: {process_fio}")
            
            if len(process_fio) == 0:
                break
            if timeout <= 0:
                break
            sleep_n_sec(10)
            timeout = timeout - 10
            
        for thread in threads:
            thread.join(timeout=30)
        end_time = time.time()

        process_list_after = self.ssh_utils.find_process_name(node=node,
                                                              process_name="fio")
        self.logger.info(f"Process List: {process_list_after}")

        process_fio = [element for element in process_list_after if "grep" not in element]

        assert len(process_fio) == 0, f"FIO process list not empty: {process_list_after}"

        return end_time
            
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
    
    def start_ec2_instance(self, ec2_client, instance_id):
        """Start ec2 instance

        Args:
            ec2_client (EC2): EC2 class object from boto3
            instance_id (str): Instance id to start
        """
        response = ec2_client.start_instances(InstanceIds=[instance_id])
        print(f'Successfully started instance {instance_id}: {response}')

        start_waiter = ec2_client.get_waiter('instance_running')
        self.logger.info(f"Waiting for instance {instance_id} to start...")
        start_waiter.wait(InstanceIds=[instance_id])
        self.logger.info(f'Instance {instance_id} has been successfully started.')

        sleep_n_sec(30)

    def stop_ec2_instance(self, ec2_client, instance_id):
        """Stop ec2 instance

        Args:
            ec2_client (EC2): EC2 class object from boto3
            instance_id (str): Instance id to stop
        """
        response = ec2_client.stop_instances(InstanceIds=[instance_id])
        self.logger.info(f'Successfully stopped instance {instance_id}: {response}')
        stop_waiter = ec2_client.get_waiter('instance_stopped')
        self.logger.info(f"Waiting for instance {instance_id} to stop...")
        stop_waiter.wait(InstanceIds=[instance_id])
        self.logger.info((f'Instance {instance_id} has been successfully stopped.'))
        
        sleep_n_sec(30)
    

def sleep_n_sec(seconds):
    """Sleeps for given seconds

    Args:
        seconds (int): Seconds to sleep for
    """
    logger = setup_logger(__name__)
    logger.info(f"Sleeping for {seconds} seconds.")
    time.sleep(seconds)
