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
                            to_check_in_logs.append("Storage node status changed from: online to: offline")
                            to_check_in_logs.append("Storage node status changed from: offline to: in_shutdown")
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
                        to_check_in_logs.append("Device restarted")

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
        fail_words = ["error", "fail", "throughput", "interrupt", "terminate"]
        for word in fail_words:
            if word in file_data:
                raise RuntimeError("FIO Test has interuupts")
    
    def validate_fio_json_output(self, output):
        """Validates JSON fio output

        Args:
            output (str): JSON output to validate
        """
        job = output['jobs'][0]
        job_name = job['job options']['name']
        file_name = job['job options']['directory']
        read_iops = job['read']['iops']
        write_iops = job['write']['iops']
        total_iops = read_iops + write_iops
        disk_name = output['disk_util'][0]['name']

        read_bw_kb = job['read']['bw']
        write_bw_kb = job['write']['bw']
        read_bw_mib = read_bw_kb / 1024
        write_bw_mib = write_bw_kb / 1024

        self.logger.info(f"Performign validation for FIO job: {job_name} on device: "
                         f"{disk_name} mounted on: {file_name}")
        assert 550 < total_iops < 650, f"Total IOPS {total_iops} out of range (550-650)"
        # TODO: Uncomment when issue is fixed
        # assert 4.5 < read_bw_mib < 5.5, f"Read BW {read_bw_mib} out of range (4.5-5.5 MiB/s)"
        # assert 4.5 < write_bw_mib < 5.5, f"Write BW {write_bw_mib} out of range (4.5-5.5 MiB/s)"
        assert read_bw_mib > 0, f"Read BW {read_bw_mib} less than or equal to 0MiB"
        assert write_bw_mib > 0, f"Write BW {write_bw_mib} less than or equal to 0MiB"

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
        start_time = time.time()
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