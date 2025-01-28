import time
import paramiko
# paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
import os
import json
import paramiko.ssh_exception
from logger_config import setup_logger
from pathlib import Path
from datetime import datetime



SSH_KEY_LOCATION = os.path.join(Path.home(), ".ssh", os.environ.get("KEY_NAME"))


class SshUtils:
    """Class to perform all ssh level operationa
    """

    def __init__(self, bastion_server):
        self.ssh_connections = dict()
        self.bastion_server = bastion_server
        self.base_cmd = os.environ.get("SBCLI_CMD", "sbcli-dev")
        self.logger = setup_logger(__name__)
        self.fio_runtime = {}
        self.ssh_user = os.environ.get("SSH_USER", None)

    def connect(self, address: str, port: int = 22,
                bastion_server_address: str = None,
                username: str = "ec2-user",
                is_bastion_server: bool = False):
        """Connect to cluster nodes

        Args:
            address (str): Address of the cluster node
            port (int): Port of the node to connect at
            bastion_server_address (str): Address of bastion server node
            username (str): Username to connect node at
            is_bastion_server (bool): Address given is of bastion server or not
        """
        # Initialize the SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        username = self.ssh_user if self.ssh_user else username

        # Load the private key
        if not os.path.exists(SSH_KEY_LOCATION):
            raise FileNotFoundError(f"SSH private key not found at {SSH_KEY_LOCATION}")
        
        private_key = paramiko.Ed25519Key(filename=SSH_KEY_LOCATION)

        # Check if we need to connect to the bastion server
        bastion_server_address = bastion_server_address or self.bastion_server
        if not bastion_server_address:
            # Direct connection to the target server
            self.logger.info(f"Connecting directly to {address} on port {port}...")
            username = "root"
            ssh.connect(hostname=address,
                        username=username,
                        port=port,
                        pkey=private_key,
                        timeout=10000)
            self.logger.info("Connected directly to the target server.")

            # Store the connection
            if self.ssh_connections.get(address, None):
                self.ssh_connections[address].close()
            self.ssh_connections[address] = ssh
            return

        # Connect to the bastion server
        self.logger.info(f"Connecting to bastion server {bastion_server_address}...")
        ssh.connect(hostname=bastion_server_address,
                    username=username,
                    port=port,
                    pkey=private_key,
                    timeout=10000)
        self.logger.info("Connected to bastion server.")

        # Store bastion server connection
        if self.ssh_connections.get(bastion_server_address, None):
            self.ssh_connections[bastion_server_address].close()
        self.ssh_connections[bastion_server_address] = ssh

        if is_bastion_server:
            return

        # Setup the transport to the target server through the proxy
        self.logger.info(f"Connecting to target server {address} through bastion server...")
        transport = ssh.get_transport()
        dest_addr = (address, port)
        local_addr = ('localhost', 0)
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)

        # Connect to the target server through the proxy channel
        target_ssh = paramiko.SSHClient()
        target_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_ssh.connect(address,
                        username=username,
                        port=port,
                        sock=channel,
                        pkey=private_key,
                        timeout=10000)
        self.logger.info("Connected to target server through proxy.")

        # Store the connection
        if self.ssh_connections.get(address, None):
            self.ssh_connections[address].close()
        self.ssh_connections[address] = target_ssh


    def exec_command(self, node, command, timeout=360, max_retries=3, stream_callback=None):
        """Executes a command on a given machine with streaming output and retry mechanism.

        Args:
            node (str): Machine to run command on.
            command (str): Command to run.
            timeout (int): Timeout in seconds.
            max_retries (int): Number of retries in case of failures.
            stream_callback (callable, optional): A callback function for streaming output. Defaults to None.

        Returns:
            tuple: Final output and error strings after command execution.
        """
        retry_count = 0
        while retry_count < max_retries:
            ssh_connection = self.ssh_connections.get(node)
            try:
                # Ensure the SSH connection is active, otherwise reconnect
                if not ssh_connection or not ssh_connection.get_transport().is_active() or retry_count > 0:
                    self.logger.info(f"Reconnecting SSH to node {node}")
                    self.connect(
                        address=node,
                        is_bastion_server=True if node == self.bastion_server else False
                    )
                    ssh_connection = self.ssh_connections[node]

                self.logger.info(f"Executing command: {command}")
                stdin, stdout, stderr = ssh_connection.exec_command(command, timeout=timeout)

                output = []
                error = []

                # Read stdout and stderr dynamically if stream_callback is provided
                if stream_callback:
                    while not stdout.channel.exit_status_ready():
                        # Process stdout
                        if stdout.channel.recv_ready():
                            chunk = stdout.channel.recv(1024).decode()
                            output.append(chunk)
                            stream_callback(chunk, is_error=False)  # Callback for stdout

                        # Process stderr
                        if stderr.channel.recv_stderr_ready():
                            chunk = stderr.channel.recv_stderr(1024).decode()
                            error.append(chunk)
                            stream_callback(chunk, is_error=True)  # Callback for stderr

                        time.sleep(0.1)

                    # Finalize any remaining output
                    if stdout.channel.recv_ready():
                        chunk = stdout.channel.recv(1024).decode()
                        output.append(chunk)
                        stream_callback(chunk, is_error=False)

                    if stderr.channel.recv_stderr_ready():
                        chunk = stderr.channel.recv_stderr(1024).decode()
                        error.append(chunk)
                        stream_callback(chunk, is_error=True)
                else:
                    # Default behavior: Read the entire output at once
                    output = stdout.read().decode()
                    error = stderr.read().decode()

                # Combine the output into strings
                output = "".join(output) if isinstance(output, list) else output
                error = "".join(error) if isinstance(error, list) else error

                # Log the results
                if output:
                    self.logger.info(f"Command output: {output}")
                if error:
                    self.logger.error(f"Command error: {error}")

                if not output and not error:
                    self.logger.warning(f"Command '{command}' executed but returned no output or error.")

                return output, error

            except EOFError as e:
                self.logger.error(f"EOFError occurred while executing command '{command}': {e}. Retrying ({retry_count + 1}/{max_retries})...")
                retry_count += 1
                time.sleep(2)  # Short delay before retrying

            except paramiko.SSHException as e:
                self.logger.error(f"SSH command failed: {e}. Retrying ({retry_count + 1}/{max_retries})...")
                retry_count += 1
                time.sleep(2)  # Short delay before retrying

        # If we exhaust retries, return failure
        self.logger.error(f"Failed to execute command '{command}' on node {node} after {max_retries} retries.")
        return "", "Command failed after max retries"

    
    def format_disk(self, node, device, fs_type="ext4"):
        """Format disk on the given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
        """
        force = "-F"
        if fs_type == "xfs":
            force = "-f"
        command = f"sudo mkfs.{fs_type} {force} {device}"
        self.exec_command(node, command)

    def mount_path(self, node, device, mount_path):
        """Mount device to given path on given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
            mount_path (_type_): Mount path to perform mount on
        """
        try:
            command = f"sudo rm -rf {mount_path}"
            self.exec_command(node, command)
        except Exception as e:
            self.logger.info(e)
        
        time.sleep(3)

        self.make_directory(node=node, dir_name=mount_path)
        
        time.sleep(3)

        command = f"sudo mount {device} {mount_path}"
        self.exec_command(node, command)

    def unmount_path(self, node, device):
        """Unmount device to given path on given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
        """
        command = f"sudo umount {device}"
        self.exec_command(node, command)
    
    def get_devices(self, node):
        """Get devices on a machine

        Args:
            node (str): Node to perform ssh operation on
        """
        command = "lsblk -dn -o NAME"
        output, _ = self.exec_command(node, command)

        return output.strip().split()
    
    def run_fio_test(self, node, device=None, directory=None, log_file=None, **kwargs):
        """Run FIO Tests with given params

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path. Defaults to None.
            directory (str, optional): Directory to run test on. Defaults to None.
            log_file (str, optional): Log file to redirect output to. Defaults to None.
        """
        location = ""
        if device:
            location = f"--filename={device}"
        if directory:
            location = f"--directory={directory}"
        
        runtime = kwargs.get("runtime", 3600)
        rw = kwargs.get("rw", "randrw")
        name = kwargs.get("name", "test")
        ioengine = kwargs.get("ioengine", "libaio")
        iodepth = kwargs.get("iodepth", 1)
        bs = kwargs.get("bs", "4k")
        rwmixread = kwargs.get("rwmixread", 70)
        size = kwargs.get("size", "10MiB")
        time_based = kwargs.get("time_based", True)
        time_based = "--time_based" if time_based else ""
        numjobs = kwargs.get("numjobs", 1)
        nrfiles = kwargs.get("nrfiles", 1)
        
        output_format = kwargs.get("output_format", '')
        output_format = f' --output-format={output_format} ' if output_format else ''

        output_file = kwargs.get("output_file", '')
        output_file = f" --output={output_file} " if output_file else ''

        # command = (f"sudo fio --name={name} {location} --ioengine={ioengine} --direct=1 --iodepth={iodepth} "
        #           f"{time_based} --runtime={runtime} --rw={rw} --bs={bs} --size={size} --rwmixread={rwmixread} "
        #           f"--verify=md5 --numjobs={numjobs} --nrfiles={nrfiles} --verify_dump=1 --verify_fatal=1 "
        #           f"--verify_state_save=1 --verify_backlog=10 --group_reporting{output_format}{output_file}")

        command = (f"sudo fio --name={name} {location} --ioengine={ioengine} --direct=1 --iodepth={iodepth} "
                   f"{time_based} --runtime={runtime} --rw={rw} --bs={bs} --size={size} --rwmixread={rwmixread} "
                   f"--verify=md5 --verify_backlog=1 --verify_fatal=1 --verify_interval={bs} --numjobs={numjobs} --nrfiles={nrfiles} "
                   f"{output_format}{output_file}")
        
        if kwargs.get("debug", None):
            command = f"{command} --debug=all"
        if log_file:
            command = f"{command} >> {log_file} 2>&1"

        self.logger.info(f"{command}")

        start_time = time.time()
        output, error = self.exec_command(node=node, command=command, timeout=runtime * 2)
        end_time = time.time()

        total_time = end_time - start_time
        self.fio_runtime[name] = start_time
        self.logger.info(f"Total time taken to run the command: {total_time:.2f} seconds")
    
    def find_process_name(self, node, process_name, return_pid=False):
        if return_pid:
            command = "ps -ef | grep -i %s | awk '{print $2}'" % process_name
        else:
            command = "ps -ef | grep -i %s" % process_name
        output, error = self.exec_command(node=node,
                                          command=command)
                                    
        data = output.strip().split("\n")

        return data
    
    def kill_processes(self, node, pid=None, process_name=None):
        """Kill the given process

        Args:
            node (str): Node to kill process on
            pid (int, optional): Kill the given pid. Defaults to None.
            process_name (str, optional): Kill the process with name. Defaults to None.
        """
        if pid:
            kill_command = f"sudo kill -9 {pid}"
            self.exec_command(node, kill_command)
        if process_name:
            kill_command = f"sudo pkill {process_name}"
            self.exec_command(node, kill_command)

    def read_file(self, node, file_name):
        """Read the given file

        Args:
            node (str): Machine to read file from
            file_name (str): File path

        Returns:
            str: Output of file name
        """
        cmd = f"cat {file_name}"
        output, _ = self.exec_command(node=node, command=cmd)
        return output
    
    def delete_file_dir(self, node, entity, recursive=False):
        """Deletes file or directory

        Args:
            node (str): Node to delete entity on
            entity (str): Path to file or directory
            recursive (bool, optional): Delete with all its content. Defaults to False.

        Returns:
            _type_: _description_
        """
        rec = "r" if recursive else ""
        cmd = f'sudo rm -{rec}f {entity}'
        output, _ = self.exec_command(node=node, command=cmd)
        return output
    
    def list_files(self, node, location):
        """List the entities in given location on a node
        Args:
            node (str): Node IP
            location (str): Location to perform ls
        """
        cmd = f"sudo ls -l {location}"
        output, error = self.exec_command(node=node, command=cmd)
        return output
    
    def stop_spdk_process(self, node):
        """Stops spdk process

        Args:
            node (str): Node IP
        """

        cmd = "curl 0.0.0.0:5000/snode/spdk_process_kill"
        output, error = self.exec_command(node=node, command=cmd)
        return output

    def get_mount_points(self, node, base_path):
        """Get all mount points on the node."""
        cmd = "mount | grep %s | awk '{print $3}'" % base_path
        output, error = self.exec_command(node=node, command=cmd)
        return output.strip().split()

    def remove_dir(self, node, dir_path):
        """Remove directory on the node."""
        cmd = f"sudo rm -rf {dir_path}"
        output, error = self.exec_command(node=node, command=cmd)
        return output, error

    def disconnect_nvme(self, node, nqn_grep):
        """Disconnect NVMe device on the node."""
        cmd = f"sudo nvme disconnect -n {nqn_grep}"
        output, error = self.exec_command(node=node, command=cmd)
        return output, error

    def get_nvme_subsystems(self, node, nqn_filter="lvol"):
        """Get NVMe subsystems on the node."""
        cmd = "sudo nvme list-subsys | grep -i %s | awk '{print $3}' | cut -d '=' -f 2" % nqn_filter
        output, error = self.exec_command(node=node, command=cmd)
        return output.strip().split()

    def get_snapshots(self, node):
        """Get all snapshots on the node."""
        cmd = "%s snapshot list | grep -i ss | awk '{{print $2}}'" % self.base_cmd
        output, error = self.exec_command(node=node, command=cmd)
        return output.strip().split()

    def get_lvol_id(self, node, lvol_name):
        """Get logical volume IDs on the node."""
        cmd = "%s lvol list | grep -i '%s ' | awk '{{print $2}}'" % (self.base_cmd, lvol_name)
        output, error = self.exec_command(node=node, command=cmd)
        return output.strip().split()
    
    def get_snapshot_id(self, node, snapshot_name):
        cmd = "%s snapshot list | grep -i '%s ' | awk '{print $2}'" % (self.base_cmd, snapshot_name)
        output, error = self.exec_command(node=node, command=cmd)

        return output.strip()

    def add_snapshot(self, node, lvol_id, snapshot_name):
        cmd = f"{self.base_cmd} snapshot add {lvol_id} {snapshot_name}"
        self.exec_command(node=node, command=cmd)
    
    def add_clone(self, node, snapshot_id, clone_name):
        cmd = f"{self.base_cmd} snapshot clone {snapshot_id} {clone_name}"
        self.exec_command(node=node, command=cmd)

    def delete_snapshot(self, node, snapshot_id):
        cmd = "%s snapshot list | grep -i '%s' | awk '{print $4}'" % (self.base_cmd, snapshot_id)
        output, error = self.exec_command(node=node, command=cmd)
        self.logger.info(f"Deleting snapshot: {output}")
        cmd = f"{self.base_cmd} snapshot delete {snapshot_id} --force"
        output, error = self.exec_command(node=node, command=cmd)

        return output, error

    def delete_all_snapshots(self, node):
        cmd = "%s snapshot list | grep -i snapshot | awk '{print $2}'" % self.base_cmd
        output, error = self.exec_command(node=node, command=cmd)

        list_snapshot = output.strip().split()
        for snapshot_id in list_snapshot:
            self.delete_snapshot(node=node, snapshot_id=snapshot_id)

        
        cmd = "%s snapshot list | grep -i ss | awk '{print $2}'" % self.base_cmd
        output, error = self.exec_command(node=node, command=cmd)

        list_snapshot = output.strip().split()
        for snapshot_id in list_snapshot:
            self.delete_snapshot(node=node, snapshot_id=snapshot_id)

    def find_files(self, node, directory):
        command = f"sudo find {directory} -maxdepth 1 -type f"
        stdout, _ = self.exec_command(node, command)
        return stdout.splitlines()

    def generate_checksums(self, node, files):
        checksums = {}
        for file in files:
            command = f"md5sum {file}"
            stdout, _ = self.exec_command(node, command)
            checksum, _ = stdout.split()
            checksums[file] = checksum
        return checksums

    def verify_checksums(self, node, files, checksums, clone_base=False):
        for file in files:
            command = f"md5sum {file}"
            stdout, _ = self.exec_command(node, command)
            checksum, _ = stdout.split()
            if clone_base:
                file_name = file.split("/")[-1]
                base_dir_name = file.split("/")[1].split("_")
                base_file_complete = base_dir_name[0] + "_" + base_dir_name[1] + "/" + file_name
                self.logger.info(f"Checksum for file {file}: Actual: {checksum}, Expected: {checksums[base_file_complete]}")
                if checksum != checksums[base_file_complete]:
                    raise ValueError(f"Checksum mismatch for file {file}")
                else:
                    self.logger.info(f"Checksum match for file: {file}")
            else:
                self.logger.info(f"Checksum for file {file}: Actual: {checksum}, Expected: {checksums[file]}")
                if checksum != checksums[file]:
                    raise ValueError(f"Checksum mismatch for file {file}")
                else:
                    self.logger.info(f"Checksum match for file: {file}")

    def delete_files(self, node, files):
        for file in files:
            command = f"sudo rm -f {file}"
            self.exec_command(node, command)

    def make_directory(self, node, dir_name):
        cmd = f"sudo mkdir -p {dir_name}"
        self.exec_command(node, cmd)

    def restart_device_with_errors(self, node, device_id):
        # Induce errors on the device
        command = f"{self.base_cmd} sn device test-mode {device_id} --error rw"
        self.exec_command(node, command)

    def restart_jm_device(self, node, jm_device_id):
        command = f"{self.base_cmd} sn restart-jm-device {jm_device_id}"
        self.exec_command(node, command)

    def remove_jm_device(self, node, jm_device_id):
        command = f"{self.base_cmd} sn remove-jm-device {jm_device_id}"
        self.exec_command(node, command)
        
    def restart_device(self, node, device_id):
        command = f"{self.base_cmd} sn restart-device {device_id}"
        self.exec_command(node, command)

    def get_lvol_vs_device(self, node, lvol_id=None):
        command = "sudo nvme list --output-format=json"
        output, _ = self.exec_command(node=node, command=command)
        data = json.loads(output)
        nvme_dict = {}
        self.logger.info(f"LVOL DEVICE output: {data}")
        for device in data.get('Devices', []):
            device_path = device.get('DevicePath')
            model_number = device.get('ModelNumber')
            if device_path and model_number:
                nvme_dict[model_number] = device_path
        self.logger.info(f"LVOL vs device dict output: {nvme_dict}")
        if lvol_id:
            return nvme_dict.get(lvol_id)
        return nvme_dict
    
    # def get_already_mounted_points(self, node, mount_point):
    #     command = f"sudo df -h | grep ${mount_point}"
    #     output, _ = self.exec_command(node=node, command=command)
    #     lines = output.splitlines()
    #     filesystem = []
    #     for line in lines[1:]:
    #         columns = line.split()
    #         if len(columns) > 1:
    #             filesystem.append(columns[0])
    #     return filesystem

    def deploy_storage_node(self, node):
        cmd = "sudo yum install -y pip jq"
        self.exec_command(node=node, command=cmd)

        cmd = f"pip install {self.base_cmd}"
        self.exec_command(node=node, command=cmd)

        cmd = "pip list"
        self.exec_command(node=node, command=cmd)

        cmd = f"{self.base_cmd} sn deploy"
        self.exec_command(node=node, command=cmd, timeout=1200)

    def add_storage_node(self, node, cluster_id, node_ip, ifname, max_lvol, max_prov, max_snap,
                         number_of_distribs, number_of_devices, partitions, jm_percent,
                         disable_ha_jm, enable_test_device, spdk_debug, spdk_image, spdk_cpu_mask):

        
        cmd = (f"{self.base_cmd} storage-node add-node --max-lvol {max_lvol} --max-snap {max_snap} --max-prov {max_prov} "
               f"--number-of-devices {number_of_devices} --number-of-distribs {number_of_distribs} "
               f"--partitions {partitions} --jm-percent {jm_percent} "
               f" --cpu-mask {spdk_cpu_mask} --spdk-image {spdk_image}")
        
        if disable_ha_jm:
            cmd = f"{cmd} --disable-ha-jm"
        if enable_test_device:
            cmd = f"{cmd} --enable-test-device"
        if spdk_debug:
            cmd = f"{cmd} --spdk-debug"
    
        add_node_cmd = f"{cmd} {cluster_id} {node_ip}:5000 {ifname}"
        self.exec_command(node=node, command=add_node_cmd)

    def create_random_files(self, node, mount_path, file_size, file_prefix="random_file", file_count=1):
        for i in range(1, file_count + 1):
            file_path = f"{mount_path}/{file_prefix}_{i}"
            command = f"sudo dd if=/dev/urandom of={file_path} bs=512K count={int(file_size[:-1]) * 2048} status=none"
            retries = 3
            for attempt in range(retries):
                try:
                    self.logger.info(f"Executing cmd: {command} (Attempt {attempt + 1}/{retries})")
                    output, error = self.exec_command(node, command, timeout=10000)
                    if error:
                        raise Exception(error)
                    break
                except Exception as e:
                    self.logger.error(f"Error during `dd` command: {e}. Retrying...")
                    if attempt == retries - 1:
                        self.logger.error(f"Failed after {retries} retries. Aborting.")

    def get_active_interfaces(self, node_ip):
        """
        Get the list of active physical network interfaces on the node.

        Args:
            node_ip (str): IP of the target node.
        Returns:
            list: List of active physical network interfaces.
        """
        try:
            cmd = (
                "sudo nmcli device status | grep 'connected' | awk '{print $1}' | grep -Ev '^(docker|lo)'"
            )
            output, error = self.exec_command(node_ip, cmd)
            if error:
                self.logger.error(f"Error fetching active interfaces on {node_ip}: {error}")
                return []
            interfaces = output.strip().split("\n")
            self.logger.info(f"Filtered active interfaces on {node_ip}: {interfaces}")
            return interfaces
        except Exception as e:
            self.logger.error(f"Failed to fetch active interfaces on {node_ip}: {e}")
            return []
        

    def disconnect_all_active_interfaces(self, node_ip, interfaces):
        """
        Disconnect all active network interfaces on a node in a single SSH call.

        Args:
            node_ip (str): IP of the target node.
            interfaces (list): List of active network interfaces to disconnect.
        """
        if not interfaces:
            self.logger.warning(f"No active interfaces to disconnect on node {node_ip}.")
            return

        # Combine disconnect commands for all interfaces
        disconnect_cmds = " && ".join([f"sudo nmcli dev disconnect {iface}" for iface in interfaces])
        reconnect_cmds = " && ".join([f"sudo nmcli dev connect {iface}" for iface in interfaces])

        cmd = (
            f'nohup sh -c "{disconnect_cmds} && sleep 300 && {reconnect_cmds}" &'
        )
        self.logger.info(f"Executing combined disconnect command on node {node_ip}: {cmd}")
        try:
            self.exec_command(node_ip, cmd)
        except Exception as e:
            self.logger.error(f"Failed to execute combined disconnect command on {node_ip}: {e}")

    def start_docker_logging(self, node_ip, containers, log_dir, test_name):
        """
        Start continuous Docker logs collection for all containers on a node.

        Args:
            ssh_obj (object): SSH utility object.
            node_ip (str): IP of the target node.
            containers (list): List of container names to log.
            log_dir (str): Directory to save log files.
            test_name (str): Name of the test for log identification.
        """
        try:
            check_tmux_command = "command -v tmux"
            output, _ = self.exec_command(node_ip, check_tmux_command)
            if not output.strip():
                self.logger.info(f"'tmux' is not installed on {node_ip}. Installing...")
                install_tmux_command = (
                    "sudo apt-get update -y && sudo apt-get install -y tmux"
                    " || sudo yum install -y tmux"
                )
                self.exec_command(node_ip, install_tmux_command)
                self.logger.info(f"'tmux' installed successfully on {node_ip}.")
            # Ensure the log directory exists
            command_mkdir = f"sudo mkdir -p {log_dir} && sudo chmod 777 {log_dir}"
            self.exec_command(node_ip, command_mkdir)  # Do not wait for a response

            for container in containers:
                # Construct the log file path
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = f"{log_dir}/{container}_{test_name}_{timestamp}_before_outage.txt"

                # Run the Docker log collection command with `setsid` to ensure persistence
                # command_logs = (
                #     f"sudo nohup setsid docker logs --follow {container} > {log_file} 2>&1 &"
                # )
                tmux_session_name = f"{container}_logs"
                command_logs = (
                    f"sudo tmux new-session -d -s {tmux_session_name} "
                    f"\"docker logs --follow {container} > {log_file} 2>&1\""
                )
                self.exec_command(node_ip, command_logs)  # Start the process without waiting

                # Verify if the process is running (optional but helpful for debugging)
                # verify_command = f"ps aux | grep 'docker logs --follow {container}'"
                # output, _ = self.exec_command(node_ip, verify_command)
                # if output:
                #     output = output.strip()

                # if not output:
                #     raise RuntimeError("Docker logging process failed to start.")
                
                print(f"Docker logging started successfully for container '{container}'.")

        except Exception as e:
            raise RuntimeError(f"Failed to start Docker logging: {e}")


    def restart_docker_logging(self, node_ip, containers, log_dir, test_name):
        """
        Restart Docker logs collection after an outage.

        Args:
            node_ip (str): IP of the target node.
            containers (list): List of container names to log.
            log_dir (str): Directory to save log files.
            test_name (str): Name of the test for log identification.
        """
        try:
            self.exec_command(node_ip, f"sudo mkdir -p {log_dir} && sudo chmod 777 {log_dir}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            for container in containers:
                log_file = f"{log_dir}/{container}_{test_name}_{timestamp}_after_outage.txt"
                tmux_session_name = f"{container}_logs"
                command_logs = (
                    f"sudo tmux new-session -d -s {tmux_session_name} "
                    f"\"docker logs --follow {container} > {log_file} 2>&1\""
                )
                self.logger.info(f"Restarting Docker log collection for container '{container}' on {node_ip}. Command: {command_logs}")
                self.exec_command(node_ip, command_logs)
                # # Verify if the process is running (optional but helpful for debugging)
                # verify_command = f"ps aux | grep 'docker logs --follow {container}'"
                # output, _ = self.exec_command(node_ip, verify_command)
                # if output:
                #     output = output.strip()

                # if not output:
                #     raise RuntimeError("Docker logging process failed to start.")
                
                print(f"Docker logging started successfully for container '{container}'.")
        except Exception as e:
            self.logger.error(f"Failed to restart Docker log collection on node {node_ip}: {e}")

    def get_running_containers(self, node_ip):
        """
        Fetch running containers from all storage nodes.

        Returns:
            dict: A dictionary mapping storage node IPs to a list of running container names.
        """
        containers_by_node = []
        try:
            cmd = "sudo docker ps --format '{{.Names}}'"
            output, error = self.exec_command(node_ip, cmd)
            if error:
                self.logger.error(f"Error fetching containers on {node_ip}: {error}")

            containers = output.strip().split("\n")
            containers_by_node = [c for c in containers if c and "monitoring" not in c]  # Filter out empty names
        except Exception as e:
            self.logger.error(f"Error fetching running containers on {node_ip}: {e}")
        return containers_by_node
    
    def reboot_node(self, node_ip, wait_time=300):
        """
        Reboot a node using SSH and wait for it to come online.

        Args:
            node_ip (str): IP address of the node to reboot.
            wait_time (int): Maximum time (in seconds) to wait for the node to come online.
        """
        try:
            self.logger.info(f"Initiating reboot for node: {node_ip}")
            # Execute the reboot command
            reboot_command = "sudo reboot"
            self.exec_command(node=node_ip, command=reboot_command)
            self.logger.info(f"Reboot command executed for node: {node_ip}")
            
            # Disconnect the current SSH connection
            if node_ip in self.ssh_connections:
                self.ssh_connections[node_ip].close()
                del self.ssh_connections[node_ip]
            
            time.sleep(10)
            
            # Wait for the node to come online
            self.logger.info(f"Waiting for node {node_ip} to come online...")
            start_time = time.time()
            while time.time() - start_time < wait_time:
                try:
                    # Attempt to reconnect
                    self.connect(address=node_ip,
                                 bastion_server_address=self.bastion_server)
                    self.logger.info(f"Node {node_ip} is back online.")
                    return True
                except Exception as e:
                    self.logger.info(f"Node {node_ip} is not online yet: {e}")
                    time.sleep(10)  # Wait before retrying
            
            self.logger.error(f"Node {node_ip} failed to come online within {wait_time} seconds.")
            return False

        except Exception as e:
            self.logger.error(f"Error during node reboot for {node_ip}: {e}")
            return False
        
    def partial_nw_outage(self, node_ip, mgmt_ip=None, block_ports=None, block_all_ss_ports=False):
        """
        Simulate a partial network outage by blocking specific ports.
        Optionally, block all ports listed by `ss` command for the given management IP.

        Args:
            node_ip (str): IP address of the target node.
            mgmt_ip (str, optional): Management IP address to filter the `ss` command output. Required if block_all_ss_ports is True.
            block_ports (list): List of ports to block.
            block_all_ss_ports (bool): If True, block all ports listed by the `ss` command for the given mgmt_ip.

        Returns:
            list: List of all blocked ports (unique).
        """
        blocked_ports = set()  # Use a set to ensure uniqueness
        try:
            if block_ports is None:
                block_ports = []

            # Block explicitly provided ports
            for port in block_ports:
                if port not in blocked_ports:
                    self.exec_command(node_ip, f"sudo iptables -A OUTPUT -p tcp --sport {port} -j DROP")
                    self.exec_command(node_ip, f"sudo iptables -A INPUT -p tcp --dport {port} -j DROP")
                    self.logger.info(f"Blocked port {port} on {node_ip}.")
                    blocked_ports.add(port)

            # If flag is set, block all ports from the `ss` command filtered by mgmt_ip
            if block_all_ss_ports:
                if not mgmt_ip:
                    raise ValueError("mgmt_ip must be provided when block_all_ss_ports is True.")
                cmd = "ss -tnp | grep %s | awk '{print $4}'" % mgmt_ip
                ss_output, _ = self.exec_command(node_ip, cmd)
                ip_with_ports = ss_output.split()
                ports_to_block = set([r.split(":")[1] for r in ip_with_ports])
                for port in ports_to_block:
                    if port not in blocked_ports:
                        self.exec_command(node_ip, f"sudo iptables -A OUTPUT -p tcp --sport {port} -j DROP")
                        self.exec_command(node_ip, f"sudo iptables -A INPUT -p tcp --dport {port} -j DROP")
                        self.logger.info(f"Blocked port {port} for mgmt_ip {mgmt_ip} on {node_ip}.")
                        blocked_ports.add(port)

        except Exception as e:
            self.logger.error(f"Failed to block ports on {node_ip}: {e}")

        return list(blocked_ports)  # Return as a list for consistency

    def remove_partial_nw_outage(self, node_ip, blocked_ports):
        """
        Remove partial network outage by unblocking specified ports.

        Args:
            node_ip (str): IP address of the target node.
            blocked_ports (list): List of ports to unblock.

        Returns:
            None
        """
        try:
            for port in blocked_ports:
                self.exec_command(node_ip, f"sudo iptables -D OUTPUT -p tcp --sport {port} -j DROP")
                self.exec_command(node_ip, f"sudo iptables -D INPUT -p tcp --dport {port} -j DROP")
                self.logger.info(f"Unblocked port {port} on {node_ip}.")
        except Exception as e:
            self.logger.error(f"Failed to unblock ports on {node_ip}: {e}")
