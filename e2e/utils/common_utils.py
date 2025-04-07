import time
from logger_config import setup_logger
from utils import proxmox
import re
import os
import requests
import json


class CommonUtils:
    """Contains common validations and parsers
    """
    def __init__(self, sbcli_utils, ssh_utils):
        self.sbcli_utils = sbcli_utils
        self.ssh_utils = ssh_utils
        self.logger = setup_logger(__name__)
        self.slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL")  # Load from environment variable

    def send_slack_summary(self, subject, body):
        """
        Sends a Slack message with the test summary.

        Args:
            subject (str): The title of the Slack message
            body (str): The content of the Slack message
        """
        if not self.slack_webhook_url:
            self.logger.error("SLACK_WEBHOOK_URL is not set. Cannot send Slack notification.")
            return

        # Format Slack message
        slack_message = {
            "text": f"*{subject}*\n{body}"
        }

        try:
            response = requests.post(self.slack_webhook_url, 
                                     data=json.dumps(slack_message), 
                                     headers={"Content-Type": "application/json"},
                                     timeout=30)
            if response.status_code == 200:
                self.logger.info("Slack notification sent successfully.")
            else:
                self.logger.error(f"Failed to send Slack notification. Response: {response.text}")
        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}")

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
                    re.compile(r"Storage node status changed from: in_restart to: online")
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
        if not isinstance(node, list):
            node = [node]
        while True:
            fio_count = 0
            for n in node:
                process = self.ssh_utils.find_process_name(node=n,
                                                           process_name="fio --name")
                process_fio = [element for element in process if "grep" not in element and not element.startswith("kworker")]
                fio_count += len(process_fio)
                self.logger.info(f"Process info: {process_fio}")
            
            if fio_count == 0:
                break
            if timeout <= 0:
                break
            sleep_n_sec(10)
            timeout = timeout - 10
            
        for thread in threads:
            thread.join(timeout=30)
        end_time = time.time()
        fio_count = 0
        for n in node:
            process_list_after = self.ssh_utils.find_process_name(node=n,
                                                                  process_name="fio --name")
            self.logger.info(f"Process List: {process_list_after}")

            process_fio = [element for element in process_list_after if "grep" not in element and not element.startswith("kworker")]
            fio_count += len(process_fio)

        assert fio_count == 0, f"FIO process list not empty: {process_list_after}"
        self.logger.info(f"FIO Running: {process_fio}")

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
    
    def start_ec2_instance(self, ec2_resource, instance_id):
        """Start ec2 instance

        Args:
            ec2_resource (EC2): EC2 class object from boto3
            instance_id (str): Instance id to start
        """
        instance = ec2_resource.Instance(instance_id)
        instance.start()
        self.logger.info(f"Starting instance {instance_id}.")
        instance.wait_until_running()  # Wait until the instance is fully running
        self.logger.info(f"Instance {instance_id} is now running.")

        sleep_n_sec(30)

    def stop_ec2_instance(self, ec2_resource, instance_id):
        """Stop ec2 instance

        Args:
            ec2_resource (EC2): EC2 class object from boto3
            instance_id (str): Instance id to stop
        """
        instance = ec2_resource.Instance(instance_id)
        instance.stop()
        self.logger.info(f"Stopping instance {instance_id}.")
        instance.wait_until_stopped()  # Wait until the instance is fully stopped
        self.logger.info(f"Instance {instance_id} has stopped.") 
        sleep_n_sec(30)

    def reboot_ec2_instance(self, ec2_resource, instance_id, timeout=300, wait_interval=10):
        """
        Reboots the specified EC2 instance and verifies that it is back up within a given timeout.

        Args:
            instance_id (str): The ID of the EC2 instance to reboot.
            timeout (int): Maximum time (in seconds) to wait for the instance to be available.
            wait_interval (int): Time interval (in seconds) between status checks.
        """
        try:
            ec2_client = ec2_resource.meta.client

            # Initiate reboot
            print(f"Rebooting instance {instance_id}...")
            ec2_client.reboot_instances(InstanceIds=[instance_id])

            # Start timeout tracking
            start_time = time.time()

            print(f"Waiting for instance {instance_id} to pass status checks...")

            while (time.time() - start_time) < timeout:
                instance = ec2_resource.Instance(instance_id)
                instance.load()  # Refresh state

                # Fetch instance status checks
                status_response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
                if status_response['InstanceStatuses']:
                    instance_status = status_response['InstanceStatuses'][0]
                    system_status_ok = instance_status['SystemStatus']['Status'] == 'ok'
                    instance_status_ok = instance_status['InstanceStatus']['Status'] == 'ok'

                    if system_status_ok and instance_status_ok:
                        print(f"Instance {instance_id} is fully online and healthy!")
                        sleep_n_sec(30)
                        return

                elapsed_time = int(time.time() - start_time)
                print(f"[{elapsed_time}s elapsed] Instance state: '{instance.state['Name']}'. Waiting for AWS status checks...")
                time.sleep(wait_interval)

            print(f"Error: Instance {instance_id} did not become available within {timeout} seconds.")
            raise RuntimeError(f"Error: Instance {instance_id} did not become available within {timeout} seconds.")

        except Exception as e:
            print(f"Error rebooting instance {instance_id}: {e}")
            raise e

    def reboot_proxmox_node(self, ip):
        """Reboots a Proxmox node.

        Args:
            node (str): Node name or IP address to reboot.
        """
        proxmox_id, vm_id = proxmox.get_proxmox(ip)
        proxmox.stop_vm(proxmox_id, vm_id)
        time.sleep(120)
        proxmox.start_vm(proxmox_id, vm_id)

    def terminate_instance(self, ec2_resource, instance_id):
        # Terminate the given instance
        instance = ec2_resource.Instance(instance_id)
        instance.terminate()
        self.logger.info(f"Terminating instance {instance_id}.")
        instance.wait_until_terminated()  # Wait until the instance is fully terminated
        self.logger.info(f"Instance {instance_id} has been terminated.")
        sleep_n_sec(30)
    
    def create_instance_from_existing(self, ec2_resource, instance_id, instance_name):
        # Get the existing instance information
        instance = ec2_resource.Instance(instance_id)

        # Get key details from the existing instance
        instance_type = instance.instance_type
        image_id = instance.image_id
        key_name = instance.key_name
        security_groups = instance.security_groups
        subnet_id = instance.subnet_id
        
        # Get block device mappings (volumes) from the source instance
        block_device_mappings = instance.block_device_mappings
        
        # Prepare the block device mappings for the new instance
        new_block_device_mappings = []
        for device in block_device_mappings:
            volume_id = device['Ebs']['VolumeId']
            
            # Fetch the volume using the ec2_resource
            volume = ec2_resource.Volume(volume_id)
            
            # Extract necessary information for the new instance
            volume_size = volume.size
            volume_type = volume.volume_type
            encrypted = volume.encrypted

            # Create the new block device mapping
            ebs_config = {
                'DeleteOnTermination': device['Ebs']['DeleteOnTermination'],
                'VolumeSize': volume_size,
                'VolumeType': volume_type,
                'Encrypted': encrypted
            }
            if volume.snapshot_id:
                ebs_config['SnapshotId'] = volume.snapshot_id

            new_block_device_mappings.append({
                'DeviceName': device['DeviceName'],
                'Ebs': ebs_config
            })

        # Create a new instance with the same details and give it a name tag
        self.logger.info(f"Block Device mapping: {new_block_device_mappings}")
        new_instance = ec2_resource.create_instances(
            ImageId=image_id,
            InstanceType=instance_type,
            KeyName=key_name,
            SecurityGroupIds=[sg['GroupId'] for sg in security_groups],
            SubnetId=subnet_id,
            MinCount=1,
            MaxCount=1,
            BlockDeviceMappings=new_block_device_mappings,  # Add the block device mappings here
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': instance_name
                        }
                    ]
                }
            ]
        )
        
        new_instance_id = new_instance[0].id
        new_instance[0].wait_until_running()  # Wait until the instance is running to get the private IP
        new_instance[0].reload()  # Refresh the instance attributes after it is running
        
        private_ip = new_instance[0].private_ip_address
        
        self.logger.info(f"New instance created with ID: {new_instance[0].id}")
        return new_instance_id, private_ip

    def get_instance_id_by_name(self, ec2_resource, instance_name):
        instances = ec2_resource.instances.filter(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [instance_name]
                }
            ]
        )
        
        # Retrieve the instance ID
        for instance in instances:
            if instance.state['Name'] != 'terminated':  # Skip terminated instances
                self.logger.info(f"Instance found: ID = {instance.id}, Name = {instance_name}")
                return instance.id
        
        self.logger.info(f"No running instance found with the name: {instance_name}")
        return None
    
    def calculate_time_duration(self, start_timestamp, end_timestamp):
        """
        Calculate time duration between start and end timestamps in 'XhrYm' format.
        Args:
            start_timestamp (int): Start time in Unix timestamp
            end_timestamp (int): End time in Unix timestamp

        Returns:
            str: Time duration in the format 'XhrYm'
        """
        duration_seconds = end_timestamp - start_timestamp
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        time_duration = f"{hours}h{minutes}m" if hours > 0 else f"{minutes}m"
        self.logger.info(f"Calculated time duration: {time_duration}")
        return time_duration
    
    def validate_io_stats(self, cluster_id, start_timestamp, end_timestamp, time_duration=None):
        """
        Validate I/O stats ensuring all metrics are non-zero within the failover time range.
        Args:
            cluster_id (str): Cluster ID
            start_timestamp (int): Start of failover in Unix timestamp
            end_timestamp (int): End of failover in Unix timestamp
            time_duration (str): Time duration for API call (e.g., '1hr30m')
        """
        self.logger.info(f"Validating I/O stats for cluster {cluster_id} during {time_duration}.")
        self.logger.info(f"Start Date: {start_timestamp}, {end_timestamp}")

        # Fetch I/O stats from the API
        io_stats = self.sbcli_utils.get_io_stats(cluster_id, time_duration)
        self.logger.info(f"IO Stats: {io_stats}")

        if not io_stats:
            self.logger.error("No I/O stats found within the specified time range.")
            raise AssertionError("No I/O stats found within the specified time range.")

        # Validate non-zero values for relevant metrics
        for stat in io_stats:
            self.logger.info(f"Validating I/O stats for record with date: {stat['date']}")
            self.assert_non_zero_io_stat(stat, "read_bytes")
            self.assert_non_zero_io_stat(stat, "write_bytes")
            self.assert_non_zero_io_stat(stat, "read_io")
            self.assert_non_zero_io_stat(stat, "write_io")
            # self.assert_non_zero_io_stat(stat, ["write_io_ps", "read_io_ps"])
        self.logger.info("All I/O stats are valid and non-zero within the failover time range.")

    def assert_non_zero_io_stat(self, stat, key):
        """
        Assert that a specific I/O stat key is non-zero.
        Args:
            stat (dict): I/O stat record
            key (str): Key to validate
        """
        value = 0
        if isinstance(key, list):
            for k in key:
                value += stat.get(k, 0)
        else:
            value = stat.get(key, 0)
        if value == 0:
            self.logger.error(f"{key} is 0 for record: {stat}")
            raise AssertionError(f"{key} is 0 for record: {stat}")
        self.logger.info(f"{key}: {value} is valid.")


    

def sleep_n_sec(seconds):
    """Sleeps for given seconds

    Args:
        seconds (int): Seconds to sleep for
    """
    logger = setup_logger(__name__)
    logger.info(f"Sleeping for {seconds} seconds.")
    time.sleep(seconds)

def convert_bytes_to_gb_tb(bytes_value):
    GB = 10**9  # 1 GB = 1 billion bytes
    TB = 10**12  # 1 TB = 1 trillion bytes
    
    if bytes_value >= TB:
        return f"{bytes_value // TB}T"
    else:
        return f"{bytes_value // GB}G"
