import time
import paramiko
# paramiko.common.logging.basicConfig(level=paramiko.common.DEBUG)
import os
import json
import paramiko.buffered_pipe
import paramiko.ssh_exception
from logger_config import setup_logger
from pathlib import Path
from datetime import datetime
import threading
import random, string, re
import subprocess


SSH_KEY_LOCATION = os.path.join(Path.home(), ".ssh", os.environ.get("KEY_NAME"))

def generate_random_string(length=6):
    """Generate a random string of uppercase letters and digits."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))


def get_parent_device(partition_path: str) -> str:
    # Match typical partition patterns (e.g. /dev/nvme1n1 -> /dev/nvme1)
    match = re.match(r"(/dev/nvme\d+)", partition_path)
    if match:
        return match.group(1)
    elif partition_path.startswith("/dev/"):
        # For other devices like /dev/sda1 -> /dev/sda
        return re.sub(r"\d+$", "", partition_path)
    else:
        raise ValueError(f"Invalid partition path: {partition_path}")

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
        self.log_monitor_threads = {}
        self.log_monitor_stop_flags = {}
        self.ssh_semaphore = threading.Semaphore(10)  # Max 10 SSH calls in parallel (tune as needed)

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


    def exec_command(self, node, command, timeout=360, max_retries=3, stream_callback=None, supress_logs=False):
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
            with self.ssh_semaphore:
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
                    
                    if not supress_logs:
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
                        if not supress_logs:
                            self.logger.info(f"Command output: {output}")
                    if error:
                        if not supress_logs:
                            self.logger.error(f"Command error: {error}")

                    if not output and not error:
                        if not supress_logs:
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

                except paramiko.buffered_pipe.PipeTimeout as e:
                    self.logger.error(f"SSH command failed: {e}. Retrying ({retry_count + 1}/{max_retries})...")
                    retry_count += 1
                    time.sleep(2)  # Short delay before retrying

                except Exception as e:
                    self.logger.error(f"SSH command failed (General Exception): {e}. Retrying ({retry_count + 1}/{max_retries})...")
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
        """Run FIO Tests with given params and proper logging for MD5 error timestamp tracing.

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
        time_based = "--time_based" if kwargs.get("time_based", True) else ""
        numjobs = kwargs.get("numjobs", 1)
        nrfiles = kwargs.get("nrfiles", 1)

        output_format = f' --output-format={kwargs["output_format"]} ' if kwargs.get("output_format") else ''
        output_file = f" --output={kwargs['output_file']} " if kwargs.get("output_file") else ''

        log_avg_msec = kwargs.get("log_avg_msec", 1000)
        log_avg_msec_opt = f"--log_avg_msec={log_avg_msec}" if log_avg_msec else ""

        iolog_base = kwargs.get("iolog_file", f"/tmp/{name}_iolog.txt")
        iolog_opt = f"--write_iolog={iolog_base}" if iolog_base else ""

        command = (
            f"sudo fio --name={name} {location} --ioengine={ioengine} --direct=1 --iodepth={iodepth} "
            f"{time_based} --runtime={runtime} --rw={rw} --bs={bs} --size={size} --rwmixread={rwmixread} "
            f"--verify=md5 --verify_fatal=1 --verify_dump=1 --numjobs={numjobs} --nrfiles={nrfiles} "
            f"{log_avg_msec_opt} {iolog_opt} "
            f"{output_format}{output_file}"
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_file or f"/tmp/{name}_{timestamp}.log"

        if kwargs.get("debug"):
            command += " --debug=all"
        else:
            command += " --debug=verify"
        
        awk_ts = " | awk '{ print strftime(\"[%Y-%m-%d %H:%M:%S]\"), $0; fflush(); }' | "
        command += awk_ts
        command += f"tee {log_file}"

        self.logger.info(f"Executing FIO command:\n{command}")

        start_time = time.time()
        output, error = self.exec_command(node=node, command=command, timeout=runtime * 2)
        end_time = time.time()

        total_time = end_time - start_time
        self.fio_runtime[name] = start_time
        self.logger.info(f"Total time taken to run the command: {total_time:.2f} seconds")

        # Return all generated iolog files (one per job)
        iolog_files = [f"{iolog_base}.{i}" for i in range(numjobs)]
        return {
            "output": output,
            "error": error,
            "start_time": start_time,
            "end_time": end_time,
            "iolog_files": iolog_files,
        }

    
    def find_process_name(self, node, process_name, return_pid=False):
        if return_pid:
            command = "ps -ef | grep -i '%s' | awk '{print $2}'" % process_name
        else:
            command = "ps -ef | grep -i '%s'" % process_name
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
        self.logger.info(f"Delete command: {cmd}")
        output, _ = self.exec_command(node=node, command=cmd)
        return output
    
    def delete_old_folders(self, node, folder_path, days=3):
        """
        Deletes folders older than the given number of days on a remote machine.

        Args:
            node (str): The IP address of the remote machine.
            folder_path (str): The base directory to check for old folders.
            days (int): The number of days beyond which folders should be deleted.
        """
        # Get the current date from the remote machine
        get_date_command = "date +%s"
        remote_timestamp, error = self.exec_command(node, get_date_command)
        
        if error:
            self.logger.error(f"Failed to fetch remote date from {node}: {error}")
            return
        
        # Convert remote timestamp to an integer
        remote_timestamp = int(remote_timestamp.strip())
        
        # Calculate threshold timestamp in seconds
        threshold_timestamp = remote_timestamp - (days * 86400)
        
        # Construct the remote find command using remote timestamps
        command = f"""
            find {folder_path} -mindepth 1 -maxdepth 1 -type d \
            -printf '%T@ %p\n' | awk '$1 < {threshold_timestamp} {{print $2}}' | xargs -I {{}} rm -rf {{}}
        """

        self.logger.info(f"Executing remote folder cleanup on {node}: {command}")
        
        _, error = self.exec_command(node, command)

        if error:
            self.logger.error(f"Failed to delete old folders on {node}: {error}")
        else:
            self.logger.info(f"Old folders deleted successfully on {node}.")

    
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
        """Stops spdk process and waits until spdk_* containers are either exited or no longer listed.
        
        If containers are not killed within 20 seconds, the kill command is retried.
        A maximum of 50 kill attempts is allowed.

        Args:
            node (str): Node IP
        """
        max_attempts = 50
        attempt = 0

        kill_cmd = "curl 0.0.0.0:5000/snode/spdk_process_kill"
        output, error = self.exec_command(node=node, command=kill_cmd)
        # record the time when the kill command was last sent
        last_kill_time = time.time()

        while attempt < max_attempts:
            # Command to check the status of containers matching "spdk_"
            status_cmd = "docker ps -a --filter 'name=spdk_' --format '{{.Status}}'"
            status_output, err = self.exec_command(node=node, command=status_cmd)
            status_output = status_output.strip()

            # If no containers found, exit the loop
            if not status_output:
                break

            statuses = status_output.splitlines()
            # Determine if every container is in an "Exited" state (e.g., "Exited (0)")
            all_exited = all("Exited" in status for status in statuses)
            if all_exited:
                break

            # If 20 seconds have passed since the last kill command, retry the kill command.
            if time.time() - last_kill_time >= 20:
                output, error = self.exec_command(node=node, command=kill_cmd)
                last_kill_time = time.time()
                attempt += 1

            # Wait a short period before checking again
            time.sleep(2)

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
    
    def disconnect_lvol_node_device(self, node, device):
        """Disconnects given lvol nqn for a specific device.
        Used to disconnect either on primary or secondary. not both.

        Device format: /dev/nvme1
        """
        device = get_parent_device(device)
        cmd = f"sudo nvme disconnect -d {device}"
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
    
    def suspend_node(self, node, node_id):
        """Suspend node."""
        cmd = f"{self.base_cmd} -d sn suspend {node_id}"
        output, _ = self.exec_command(node=node, command=cmd)
        return output.strip().split()
    
    def shutdown_node(self, node, node_id, force=False):
        """Shutdown Node."""
        force_cmd = " --force" if force else ""
        cmd = f"{self.base_cmd} -d sn shutdown {node_id}{force_cmd}"
        output, _ = self.exec_command(node=node, command=cmd)
        return output.strip().split()
    
    def restart_node(self, node, node_id, force=False):
        """Shutdown Node."""
        force_cmd = " --force" if force else ""
        cmd = f"{self.base_cmd} -d sn restart {node_id}{force_cmd}"
        output, _ = self.exec_command(node=node, command=cmd)
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
        cmd = f"{self.base_cmd} -d snapshot add {lvol_id} {snapshot_name}"
        output, error = self.exec_command(node=node, command=cmd)
        return output, error
    
    def add_clone(self, node, snapshot_id, clone_name):
        cmd = f"{self.base_cmd} -d snapshot clone {snapshot_id} {clone_name}"
        output, error = self.exec_command(node=node, command=cmd)
        return output, error

    def delete_snapshot(self, node, snapshot_id):
        cmd = "%s snapshot list | grep -i '%s' | awk '{print $4}'" % (self.base_cmd, snapshot_id)
        output, error = self.exec_command(node=node, command=cmd)
        self.logger.info(f"Deleting snapshot: {output}")
        cmd = f"{self.base_cmd} -d snapshot delete {snapshot_id} --force"
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

        cmd = f"{self.base_cmd} -d sn deploy"
        self.exec_command(node=node, command=cmd, timeout=1200)

    def add_storage_node(self, node, cluster_id, node_ip, ifname, max_lvol, max_prov, max_snap,
                         number_of_distribs, number_of_devices, partitions, jm_percent,
                         disable_ha_jm, enable_test_device, spdk_debug, spdk_image, spdk_cpu_mask):

        
        cmd = (f"{self.base_cmd} -d storage-node add-node --max-lvol {max_lvol} --max-snap {max_snap} --max-prov {max_prov} "
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
        

    def disconnect_all_active_interfaces(self, node_ip, interfaces, reconnect_time=300):
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
        disconnect_cmds = " && ".join([f"sudo nmcli connection down {iface}" for iface in interfaces])
        reconnect_cmds = " && ".join([f"sudo nmcli connection up {iface}" for iface in interfaces])

        cmd = (
            f'nohup sh -c "{disconnect_cmds} && sleep {reconnect_time} && {reconnect_cmds}" &'
        )
        self.logger.info(f"Executing combined disconnect command on node {node_ip}: {cmd}")
        try:
            self.exec_command(node_ip, cmd)
        except Exception as e:
            self.logger.error(f"Failed to execute combined disconnect command on {node_ip}: {e}")

    def check_tmux_installed(self, node_ip):
        """Check tmux installation
        """
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
            # Ensure the log directory exists
            command_mkdir = f"sudo mkdir -p {log_dir} && sudo chmod 777 {log_dir}"
            self.exec_command(node_ip, command_mkdir)  # Do not wait for a response

            for container in containers:
                # Construct the log file path
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = f"{log_dir}/{container}_{test_name}_{node_ip}_{timestamp}_before_outage.txt"

                # Run the Docker log collection command with `setsid` to ensure persistence
                # command_logs = (
                #     f"sudo nohup setsid docker logs --follow {container} > {log_file} 2>&1 &"
                # )
                random_suffix = generate_random_string()
                tmux_session_name = f"{container}_logs_{random_suffix}"
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
                log_file = f"{log_dir}/{container}_{test_name}_{node_ip}_{timestamp}_after_outage.txt"
                random_suffix = generate_random_string()
                tmux_session_name = f"{container}_logs_{random_suffix}"
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
        
    def perform_nw_outage(self, node_ip, node_data_nic_ip=None, nodes_check_ports_on=None, block_ports=None, block_all_ss_ports=False):
        """
        Simulate a partial network outage by blocking multiple ports at once using multiport matching.
        Optionally, block all ports listed by `ss` command for the given management IP.

        Args:
            node_ip (str): IP address of the target node.
            mgmt_ip (list, optional): IP addresses used to filter the `ss` command output.
            block_ports (list): List of ports to block.
            block_all_ss_ports (bool): If True, block all ports from the `ss` command for mgmt_ip.

        Returns:
            list: List of all blocked ports (unique).
        """
        try:
            if block_ports is None:
                block_ports = []
            else:
                block_ports = [str(port) for port in block_ports]

            # If flag is set, fetch and add all ports from the `ss` command filtered by mgmt_ip
            if block_all_ss_ports:
                if (not node_data_nic_ip) and (not nodes_check_ports_on):
                    raise ValueError("node_data_nic_ip and nodes_check_ports_on must be provided when block_all_ss_ports is True.")
                for node in nodes_check_ports_on:
                    source_node_ips = list(node_data_nic_ip)
                    source_node_ips.append(node_ip)
                    for source_node in source_node_ips:
                        cmd = "ss -tnp | grep %s | awk '{print $5}'" % source_node
                        self.logger.info(f"Executing {cmd} on node: {node}")
                        ss_output, _ = self.exec_command(node, cmd)
                        self.logger.info(f"Output: {ss_output}")
                        ip_with_ports = ss_output.split()
                        ports_to_block = [str(r.split(":")[1]) for r in ip_with_ports]
                        block_ports.extend(ports_to_block)

            # Remove duplicates
            block_ports = [str(port) for port in block_ports]
            block_ports = list(dict.fromkeys(block_ports))
            block_ports = sorted(block_ports)

            block_ports = [port.strip() for port in block_ports if port.strip()]
            
            if "22" in block_ports:
                block_ports.remove("22")

            if block_ports:
                # Construct a single iptables rule for both INPUT & OUTPUT chains
                ports_str = ",".join(block_ports)
                # block_command = (
                #     f"sudo iptables -A INPUT -p tcp -m multiport --sports {ports_str} --dports {ports_str} -j DROP && "
                #     f"sudo iptables -A OUTPUT -p tcp -m multiport --sports {ports_str} --dports {ports_str} -j DROP"
                # )

                block_command = f"""
                    sudo iptables -A INPUT -p tcp -m multiport --dports {ports_str} -j DROP;
                    sudo iptables -A INPUT -p tcp -m multiport --sports {ports_str} -j DROP;
                    sudo iptables -A OUTPUT -p tcp -m multiport --dports {ports_str} -j DROP;
                    sudo iptables -A OUTPUT -p tcp -m multiport --sports {ports_str} -j DROP;
                """

                # for port in block_ports:
                #     block_command = (f"sudo iptables -A INPUT -p tcp --sport {port} --dport {port} -j DROP && "
                #                      f"sudo iptables -A OUTPUT -p tcp --sport {port} --dport {port} -j DROP"
                #                      )
                
                self.exec_command(node_ip, block_command)
                self.logger.info(f"Blocked ports {ports_str} on {node_ip}.")

            time.sleep(5)
            self.logger.info("Network outage: IPTable Rules List:")
            self.exec_command(node_ip, "sudo iptables -L -v -n --line-numbers")

        except Exception as e:
            self.logger.error(f"Failed to block ports on {node_ip}: {e}")

        return block_ports


    def remove_nw_outage(self, node_ip, blocked_ports):
        """
        Remove partial network outage by unblocking multiple ports at once.

        Args:
            node_ip (str): IP address of the target node.
            blocked_ports (list): List of ports to unblock.

        Returns:
            None
        """
        try:
            if blocked_ports:
                blocked_ports = list(dict.fromkeys(blocked_ports))
                blocked_ports = sorted(blocked_ports)
                ports_str = ",".join(blocked_ports)
                unblock_command = f"""
                    sudo iptables -D OUTPUT -p tcp -m multiport --sports {ports_str} -j DROP;
                    sudo iptables -D OUTPUT -p tcp -m multiport --dports {ports_str} -j DROP;
                    sudo iptables -D INPUT -p tcp -m multiport --dports {ports_str} -j DROP;
                    sudo iptables -D INPUT -p tcp -m multiport --sports {ports_str} -j DROP;
                """

                # for port in blocked_ports:
                #     unblock_command = (f"sudo iptables -D OUTPUT -p tcp --sport {port} --dport {port} -j DROP && "
                #                        f"sudo iptables -D INPUT -p tcp --sport {port} --dport {port} -j DROP"
                #                        )
                self.exec_command(node_ip, unblock_command)
                self.logger.info(f"Unblocked ports {ports_str} on {node_ip}.")

            time.sleep(5)
            self.logger.info("Network outage: IPTable Rules List:")
            self.exec_command(node_ip, "sudo iptables -L -v -n --line-numbers")

        except Exception as e:
            self.logger.error(f"Failed to unblock ports on {node_ip}: {e}")


    def set_aio_max_nr(self, node_ip, value=1048576):
        """
        Set the aio-max-nr value on the target node.

        Args:
            node_ip (str): IP address of the target node.
            value (int, optional): The aio-max-nr value to set. Defaults to 1048576.
        """
        try:
            # Check the current aio-max-nr value
            check_cmd = "cat /proc/sys/fs/aio-max-nr"
            current_value, _ = self.exec_command(node_ip, check_cmd)

            if current_value.strip() == str(value):
                self.logger.info(f"aio-max-nr is already set to {value} on {node_ip}. No changes needed.")
                return
            
            self.logger.info(f"Updating aio-max-nr to {value} on {node_ip}.")

            # Set the new aio-max-nr value
            update_cmd = f'echo "fs.aio-max-nr = {value}" | sudo tee /etc/sysctl.d/99-sysctl.conf'
            self.exec_command(node_ip, update_cmd)

            # Apply the new setting
            apply_cmd = "sudo sysctl -p /etc/sysctl.d/99-sysctl.conf"
            self.exec_command(node_ip, apply_cmd)

            self.logger.info(f"Successfully updated aio-max-nr to {value} on {node_ip}.")

        except Exception as e:
            self.logger.error(f"Failed to update aio-max-nr on {node_ip}: {e}")

    def dump_lvstore(self, node_ip, storage_node_id):
        """
        Runs 'sn dump-lvstore' on a given storage node and extracts the LVS dump file path.

        Args:
            node_ip (str): IP address of the target node.
            storage_node_id (str): The Storage Node ID to dump lvstore.

        Returns:
            str: The extracted LVS dump file path, or None if not found.
        """
        try:
            command = f"{self.base_cmd} --dev -d sn dump-lvstore {storage_node_id} | grep 'LVS dump file will be here'"
            self.logger.info(f"Executing '{self.base_cmd} --dev -d sn dump-lvstore' on {node_ip} for Storage Node ID: {storage_node_id}")
            
            output, error = self.exec_command(node_ip, command)

            if error:
                self.logger.error(f"Error executing '{self.base_cmd} --dev -d sn dump-lvstore' on {node_ip}: {error}")
                return None

            # Extract only the LVS dump file path
            dump_file_path = None
            for line in output.split("\n"):
                if "LVS dump file will be here" in line:
                    dump_file_path = line.strip()
                    break

            if dump_file_path:
                self.logger.info(f"LVS dump file located: {dump_file_path}")
                return dump_file_path
            else:
                self.logger.warning(f"No LVS dump file found in the output from {node_ip}.")
                return None

        except Exception as e:
            self.logger.error(f"Failed to dump lvstore on {node_ip}: {e}")
            return None
        
    def fetch_distrib_logs(self, storage_node_ip, storage_node_id):
        """
        Fetch distrib names, generate and execute RPC JSON, and copy logs from SPDK container.

        Args:
            storage_node_ip (str): IP of the storage node
            storage_node_id (str): ID of the storage node
        """
        self.logger.info(f"Fetching distrib logs for Storage Node ID: {storage_node_id} on {storage_node_ip}")
        
        # Fetch lvstore_stack JSON output
        command = f"{self.base_cmd} sn get {storage_node_id} | jq .lvstore_stack"
        output, error = self.exec_command(storage_node_ip, command)
        
        if error:
            self.logger.error(f"Error fetching lvstore stack: {error}")
            return
        
        # Parse JSON output
        try:
            lvstore_stack = json.loads(output)
            distribs = [entry["distribs_list"] for entry in lvstore_stack if "distribs_list" in entry]
            distribs = distribs[0] if distribs else []
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON Parsing Error: {e}")
            return
        
        if not distribs:
            self.logger.warning("No distrib names found.")
            return
        
        self.logger.info(f"Distributions found: {distribs}")
        
        for distrib in distribs:
            self.logger.info(f"Processing distrib: {distrib}")

            # Create JSON for the RPC call
            rpc_json = {
                "subsystems": [
                    {
                        "subsystem": "distr",
                        "config": [
                            {
                                "method": "distr_debug_placement_map_dump",
                                "params": {"name": distrib}
                            }
                        ]
                    }
                ]
            }

            # Convert JSON object to string and escape quotes for bash command
            # rpc_json_str = json.dumps(rpc_json).replace('"', '\\"')
            rpc_json_str = json.dumps(rpc_json)
            remote_json_path = "/tmp/stack.json"

            # Create JSON file on the storage node
            create_json_command = f"echo '{rpc_json_str}' > {remote_json_path}"
            self.exec_command(storage_node_ip, create_json_command)

            find_container_cmd = "docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$'"
            container_name_output, _ = self.exec_command(storage_node_ip, find_container_cmd)
            if container_name_output:
                container_name = container_name_output.strip()
                # Save JSON inside SPDK container
                rpc_script_path = "/tmp/stack.json"
                create_json_command = f"sudo docker cp {remote_json_path} {container_name}:{rpc_script_path}"
                self.exec_command(storage_node_ip, create_json_command)

                # Execute RPC call inside SPDK Docker container
                rpc_command = f"sudo docker exec {container_name} bash -c 'python scripts/rpc_sock.py {rpc_script_path}'"
                self.exec_command(storage_node_ip, rpc_command)

                # Find log file name dynamically
                find_log_command = f"sudo docker exec {container_name} ls /tmp/ | grep distrib"
                log_file_name, _ = self.exec_command(storage_node_ip, find_log_command)
                log_file_name = log_file_name.strip().replace("\r", "").replace("\n", "")

                if not log_file_name:
                    self.logger.error(f"No log file found for distrib {distrib} in /tmp/.")
                    continue

                log_file_path = f"/tmp/{log_file_name}"
                destination_path = f"{Path.home()}/{log_file_name}_{storage_node_ip}"

                # Copy log file from inside container to host machine
                copy_command = f"sudo docker cp {container_name}:{log_file_path} {destination_path}"
                self.exec_command(storage_node_ip, copy_command)

                self.logger.info(f"Processed {distrib}: Logs copied to {destination_path}")

                # Remove log file from container
                delete_command = f"sudo docker exec {container_name} rm -f {log_file_path}"
                self.exec_command(storage_node_ip, delete_command)
                self.logger.info(f"Processed {distrib}: Logs copied to {destination_path} and deleted from container.")
            else:
                self.logger.warning(f"No SPDK container found on {storage_node_ip}")

        self.logger.info("All logs retrieved successfully!")

    def clone_mount_gen_uuid(self, node, device):
        """Repair the XFS filesystem and generate a new UUID.
        Args:
            node (str): Node to perform operations on.
            device (str): Device path to modify.

        """
        self.logger.info(f"Repairing XFS filesystem on {device} (forcing log removal).")
        self.exec_command(node, f"sudo xfs_repair -L {device}")  # Force repair and clear log

        self.logger.info(f"Generating new UUID for {device} on {node}.")
        self.exec_command(node, f"sudo xfs_admin -U generate {device}")  # Generate new UUID

    def check_and_install_tcpdump(self, node_ip):
        """Installs tcpdump on given node ip
        """
        output, _ = self.exec_command(node_ip, "which tcpdump")
        if not output:
            self.logger.info("tcpdump not found, installing...")
            install_tcpdump_command = (
                "sudo apt-get update -y && sudo apt-get install -y tcpdump"
                " || sudo yum install -y tcpdump"
            )
            output, _ = self.exec_command(node_ip, install_tcpdump_command)
            self.logger.info(f"tcpdump installed successfully: {output}")

    def check_and_install_tshark(self, node_ip):
        """Check if tshark is installed on the remote node and install it if missing."""
        output, _ = self.exec_command(node_ip, "which tshark")
        if not output:
            self.logger.info("tshark not found, installing...")
            install_tcpdump_command = (
                "sudo apt-get update -y && sudo apt-get install -y tshark"
                " || sudo yum install -y wireshark"
            )
            output, _ = self.exec_command(node_ip, install_tcpdump_command)
            self.logger.info(f"tshark installed successfully: {output}")


    def start_tcpdump_logging(self, node_ip, log_dir):
        """Start tcpdump logging for various TCP anomalies on a remote node with proper background handling."""
        self.check_and_install_tcpdump(node_ip=node_ip)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define log file names for each tcpdump command
        syn_timeout_log = f"{log_dir}/tcpdump_syn_timeout_{node_ip}_{timestamp}.txt"
        rcv_buffer_full_log = f"{log_dir}/tcpdump_rcv_buffer_full_{node_ip}_{timestamp}.txt"
        conn_reset_log = f"{log_dir}/tcpdump_conn_reset_{node_ip}_{timestamp}.txt"
        ack_timeout_log = f"{log_dir}/tcpdump_ack_timeout_{node_ip}_{timestamp}.txt"

        # Create tcpdump commands with `nohup` to detach from the SSH session
        tcpdump_commands = [
            f"sudo tmux new-session -d -s sync_timeout_log_session \"tcpdump -i ens16 -nn '(tcp[tcpflags] == 2 and tcp[14:2] > 1)' > {syn_timeout_log} 2>&1\"",
            f"sudo tmux new-session -d -s rcv_buffer_log_session \"tcpdump -i ens16 -nn -v '(tcp[13] & 0x10 != 0 and tcp[14:2] == 0)' > {rcv_buffer_full_log} 2>&1\"",
            f"sudo tmux new-session -d -s conn_reset_log_session \"tcpdump -i ens16 -nn '(tcp[13] & 0x04 != 0)' > {conn_reset_log} 2>&1\"",
            (
                "sudo tmux new-session -d -s ack_timeout_log_session "
                "\"tcpdump -i ens16 -nn -tttt | awk "
                "'/Flags \\\\[.\\\\]/ { "
                "if (prev_time != \\\"\\\") { "
                "diff = \\$1 - prev_time; "
                "if (diff > 0.5) print prev_time, \\\"->\\\", \\$1, \\\"ACK timeout:\\\", diff, \\\"sec\\\"; "
                "} "
                "prev_time = \\$1; "
                "}' > %s 2>&1\""
            ) % ack_timeout_log

        ]

        # Execute each tcpdump command remotely
        for cmd in tcpdump_commands:
            self.exec_command(node_ip, cmd)

        # Log the output filenames for reference
        self.logger.info(f"Started tcpdump for SYN timeouts on {node_ip}, saving to {syn_timeout_log}")
        self.logger.info(f"Started tcpdump for RCV buffer full on {node_ip}, saving to {rcv_buffer_full_log}")
        self.logger.info(f"Started tcpdump for Connection resets on {node_ip}, saving to {conn_reset_log}")
        self.logger.info(f"Started tcpdump for ACK timeouts on {node_ip}, saving to {ack_timeout_log}")

    def start_tshark_logging(self, node_ip, log_dir):
        """Start tshark logging for various TCP anomalies on a remote node with proper UTC timestamps."""
        self.check_and_install_tshark(node_ip=node_ip)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Define log file names for each tshark command
        syn_timeout_log = f"{log_dir}/tshark_syn_timeout_{node_ip}_{timestamp}.log"
        rcv_buffer_full_log = f"{log_dir}/tshark_rcv_buffer_full_{node_ip}_{timestamp}.log"
        conn_reset_log = f"{log_dir}/tshark_conn_reset_{node_ip}_{timestamp}.log"
        ack_timeout_log = f"{log_dir}/tshark_ack_timeout_{node_ip}_{timestamp}.log"

        # Tshark commands with fixed timestamps and proper filtering
        tshark_commands = [
            f"sudo tmux new-session -d -s sync_timeout_log_session \"tshark -i ens16 -Y 'tcp.flags.syn == 1 && tcp.window_size_value > 1' -t ud > {syn_timeout_log} 2>&1\"",
            f"sudo tmux new-session -d -s rcv_buffer_log_session \"tshark -i ens16 -Y 'tcp.flags.ack == 1 && tcp.window_size_value == 0' -t ud > {rcv_buffer_full_log} 2>&1\"",
            f"sudo tmux new-session -d -s conn_reset_log_session \"tshark -i ens16 -Y 'tcp.flags.reset == 1' -t ud > {conn_reset_log} 2>&1\"",
            f"sudo tmux new-session -d -s ack_timeout_log_session \"tshark -i ens16 -Y 'tcp.analysis.ack_rtt > 0.5' -t ud -T fields -e frame.time -e ip.src -e ip.dst -e tcp.seq -e tcp.ack -e tcp.analysis.ack_rtt > {ack_timeout_log} 2>&1\""
        ]


        # Execute each tshark command remotely
        for cmd in tshark_commands:
            self.exec_command(node_ip, cmd)

        # Log the output filenames for reference
        self.logger.info(f"Started tshark for SYN timeouts on {node_ip}, saving to {syn_timeout_log}")
        self.logger.info(f"Started tshark for RCV buffer full on {node_ip}, saving to {rcv_buffer_full_log}")
        self.logger.info(f"Started tshark for Connection resets on {node_ip}, saving to {conn_reset_log}")
        self.logger.info(f"Started tshark for ACK timeouts on {node_ip}, saving to {ack_timeout_log}")

    def stop_all_tcpdump(self, node_ip):
        """Kill all tcpdump processes on a remote node."""
        stop_command = """
        sudo pkill -f tcpdump && echo "All tcpdump processes stopped" || echo "No tcpdump process found"
        """
        self.exec_command(node_ip, stop_command)
        self.logger.info(f"Stopped all tcpdump processes on {node_ip}")

    def stop_all_tshark(self, node_ip):
        """Kill all tshark processes on a remote node."""
        stop_command = """
        sudo pkill -f tshark && echo "All tshark processes stopped" || echo "No tshark process found"
        """
        self.exec_command(node_ip, stop_command)
        self.logger.info(f"Stopped all tshark processes on {node_ip}")

    def get_dmesg_logs_within_iso_window(self, node_ip, start_iso, end_iso):
        """
        Fetch dmesg logs with ISO timestamps on a remote node within a time window.

        Args:
            node_ip (str): Node IP to fetch logs from.
            start_iso (str): Start time in ISO 8601 format.
            end_iso (str): End time in ISO 8601 format.

        Returns:
            list: List of filtered dmesg log lines.
        """
        # Get dmesg logs in ISO format
        cmd = "sudo dmesg --time-format=iso"
        output, error = self.exec_command(node_ip, cmd)

        if error:
            self.logger.error(f"Error fetching dmesg logs from {node_ip}: {error}")
            return []

        logs_in_window = []
        start_time = datetime.fromisoformat(start_iso)
        end_time = datetime.fromisoformat(end_iso)

        for line in output.splitlines():
            try:
                timestamp_str = line.split()[0]
                log_time = datetime.fromisoformat(timestamp_str.replace(',', '.'))

                if start_time <= log_time <= end_time:
                    logs_in_window.append(line)
            except Exception as e:
                self.logger.debug(f"Skipping malformed dmesg line: {line} ({e})")

        return logs_in_window
    
    def start_netstat_dmesg_logging(self, node_ip, log_dir):
        """Start continuous netstat and dmesg logging without using watch."""
        # Ensure netstat is installed
        self.exec_command(node_ip, 'sudo apt-get update && sudo apt-get install -y net-tools || sudo yum install -y net-tools')

        # Start logging netstat and dmesg by directly redirecting output to files
        netstat_log = f"{log_dir}/netstat_segments_{node_ip}.log"
        dmesg_log = f"{log_dir}/dmesg_tcp_{node_ip}.log"
        journalctl_log = f"{log_dir}/journalctl_{node_ip}.log"

        self.exec_command(node_ip, f"sudo tmux new-session -d -s netstat_log 'bash -c \"while true; do netstat -s | grep \\\"segments dropped\\\" >> {netstat_log}; sleep 5; done\"'")
        self.exec_command(node_ip, f"sudo tmux new-session -d -s dmesg_log 'bash -c \"while true; do sudo dmesg | grep -i \\\"tcp\\\" >> {dmesg_log}; sleep 5; done\"'")
        self.exec_command(node_ip, f"sudo tmux new-session -d -s journalctl_log 'bash -c \"while true; do sudo journalctl -k | grep -i \\\"tcp\\\" >> {journalctl_log}; sleep 5; done\"'")

    def reset_iptables_in_spdk(self, node_ip):
        """
        Resets iptables rules inside the SPDK container on a given node.

        Args:
            node_ip (str): The IP address of the target node.
        """
        try:
            self.logger.info(f"Resetting iptables inside SPDK container on {node_ip}.")

            find_container_cmd = "docker ps --format '{{.Names}}' | grep -E '^spdk_[0-9]+$'"

            container_name_output, _ = self.exec_command(node_ip, find_container_cmd)

            if container_name_output:
                container_name = container_name_output.strip()
                # Commands to run inside the SPDK container
                iptables_reset_cmds = [
                    f"sudo docker exec {container_name} iptables -L -v -n",
                    f"sudo docker exec {container_name} iptables -P INPUT ACCEPT",
                    f"sudo docker exec {container_name} iptables -P OUTPUT ACCEPT",
                    f"sudo docker exec {container_name} iptables -P FORWARD ACCEPT",
                    f"sudo docker exec {container_name} iptables -F",
                    f"sudo docker exec {container_name} iptables -L -v -n"
                ]

                # Execute each command
                for cmd in iptables_reset_cmds:
                    self.exec_command(node_ip, cmd)

                self.logger.info(f"Successfully reset iptables inside SPDK container on {node_ip}.")
            else:
                self.logger.warning(f"No SPDK container found on {node_ip}")
        except Exception as e:
            self.logger.error(f"Failed to reset iptables in SPDK container on {node_ip}: {e}")

    def check_remote_spdk_logs_for_keyword(self, node_ip, log_dir, test_name, keyword="ALCEMLD"):
        """
        Checks all 'spdk_{test_name}*.txt' files in log_dir on a remote node for the given keyword.
        If found, logs the timestamp and the full line containing the keyword.

        Args:
            node_ip (str): IP address of the remote node.
            log_dir (str): Directory where log files are stored.
            test_name (str): Name of the test (used to identify relevant log files).
            keyword (str, optional): The keyword to search for. Defaults to "ALCEMLD".

        Returns:
            dict: A dictionary with filenames as keys and a list of matching log lines (timestamp + error line).
        """
        try:
            # Find all log files matching 'spdk_{test_name}*.txt' pattern
            find_command = f"ls {log_dir}/spdk_{test_name}*.txt 2>/dev/null"
            output, _ = self.exec_command(node_ip, find_command)

            log_files = output.strip().split("\n") if output else []
            keyword_matches = {}

            for log_file in log_files:
                if not log_file:
                    continue  # Skip empty lines
                
                # Extract the full log line that contains the keyword, including the timestamp
                grep_command = f"grep '{keyword}' {log_file} || true"
                grep_output, _ = self.exec_command(node_ip, grep_command)

                if grep_output:
                    matched_lines = grep_output.strip().split("\n")
                    keyword_matches[log_file] = matched_lines  # Store all matched lines with timestamps
                else:
                    keyword_matches[log_file] = []

            return keyword_matches

        except Exception as e:
            self.logger.error(f"Failed to check logs for keyword '{keyword}' on node {node_ip}: {e}")
            return {}

    def get_container_id(self, node_ip, container):
        """Fetch container ID by name"""
        cmd = f"docker inspect --format='{{{{.Id}}}}' {container}"
        output, error = self.exec_command(node_ip, cmd, supress_logs=True)
        return output.strip() if output else None

    def monitor_container_logs(self, node_ip, containers, log_dir, test_name, poll_interval=10):
        """Monitor container logs and auto-detect new containers."""
        container_ids = {}
        known_containers = set(containers)
        stop_flag = threading.Event()

        def _monitor():
            while not stop_flag.is_set():
                try:
                    # Get current list of running containers
                    current_containers = self.get_running_containers(node_ip)

                    # Start logging for newly found containers
                    for container in current_containers:
                        if container not in known_containers:
                            self.logger.info(f"[{node_ip}] New container detected: {container}")
                            known_containers.add(container)

                    # Now monitor for restarts of all known containers
                    for container in list(known_containers):
                        try:
                            new_id = self.get_container_id(node_ip, container)
                            old_id = container_ids.get(container)

                            if not new_id:
                                continue  # container might have exited

                            if new_id != old_id:
                                container_ids[container] = new_id
                                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                                log_file = f"{log_dir}/{container}_{test_name}_{node_ip}_{timestamp}_restart.log"
                                session = f"{container}_restart_{generate_random_string()}"
                                self.logger.info(f"[{node_ip}] Logging for container: {container}")
                                cmd = (
                                    f"sudo tmux new-session -d -s {session} "
                                    f"\"docker logs --follow {container} > {log_file} 2>&1\""
                                )
                                self.exec_command(node_ip, cmd, supress_logs=True)
                        except Exception as e:
                            self.logger.error(f"Error monitoring container {container} on {node_ip}: {e}")

                except Exception as outer_e:
                    self.logger.error(f"[{node_ip}] Error during container polling: {outer_e}")

                time.sleep(poll_interval)

        thread = threading.Thread(target=_monitor, daemon=True)
        thread.start()

        self.log_monitor_threads[node_ip] = thread
        self.log_monitor_stop_flags[node_ip] = stop_flag
        self.logger.info(f"Started background log monitor on {node_ip} with poll interval {poll_interval}s")


    def stop_container_log_monitor(self, node_ip):
        """Stop Monitoring thread in teardown"""
        if node_ip in self.log_monitor_stop_flags:
            self.log_monitor_stop_flags[node_ip].set()
            self.logger.info(f"Stopping container log monitor thread for {node_ip}")

    def get_node_version(self, node):
        """
        Fetches sbcli command version from all storage nodes.

        Returns:
            str: Current sbcli version string
        """
        version = None
        try:
            version_cmd = f"pip show {self.base_cmd} | grep Version"
            output, _ = self.exec_command(node=node, command=version_cmd)
            version = output.strip().split(":")[1].strip() if ":" in output else "UNKNOWN"
        except Exception as e:
            self.logger.error(f"Failed to fetch sbcli-dev version from node {node}: {e}")
        return version if version else "ERROR"

    def get_image_dict(self, node):
        """Get images dictionary

        Args:
            node (str): Node IP to check docker images list on

        Returns:
            dict: Image name vs the Image hash
        """
        cmd = "docker images --format '{{.Repository}}:{{.Tag}} {{.ID}}'"
        output, _ = self.exec_command(node=node, command=cmd)
        image_map = {}
        for line in output.strip().split('\n'):
            if line:
                name_tag, img_id = line.strip().split()
                image_map[name_tag] = img_id
        return image_map
    
    def start_resource_monitors(self, node_ip, log_dir):
        """
        Starts background resource monitoring for:
        1. Root partition usage
        2. Container-wise memory usage

        Each logs every 10s to a separate file in the log_dir.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        root_log = f"{log_dir}/root_partition_usage_{node_ip}_{timestamp}.txt"
        docker_mem_log = f"{log_dir}/docker_mem_usage_{node_ip}_{timestamp}.txt"

        # Ensure log directory exists
        self.exec_command(node_ip, f"sudo mkdir -p {log_dir} && sudo chmod 777 {log_dir}")

        # Root partition utilization monitor (df -h /)
        df_cmd = f"""sudo tmux new-session -d -s root_usage_monitor \
        'bash -c "while true; do date >> {root_log}; df -h / >> {root_log}; echo >> {root_log}; sleep 10; done"'"""


        # Docker memory usage monitor (docker stats --no-stream)
        docker_cmd = "sudo tmux new-session -d -s docker_mem_monitor " \
                     "'while true; do " \
                     "date >> %s; " \
                     "docker stats --no-stream --format \"table {{.Name}}\\t{{.MemUsage}}\" >> %s; " \
                     "echo >> %s; " \
                     "sleep 10; " \
                     "done'" % (docker_mem_log, docker_mem_log, docker_mem_log)


        self.exec_command(node_ip, df_cmd)
        self.exec_command(node_ip, docker_cmd)

        self.logger.info(f"Started root partition and container memory logging on {node_ip}")



    # def stop_netstat_dmesg_logging(self, node_ip):
    #     """Stop continuous netstat and dmesg logging without using watch."""
    #     # Ensure netstat is installed

    #     self.exec_command(node_ip, f"sudo tmux new-session -d -s netstat_log 'bash -c \"while true; do netstat -s | grep \\\"segments dropped\\\" >> {netstat_log}; sleep 5; done\"'")
    #     self.exec_command(node_ip, f"sudo tmux new-session -d -s dmesg_log 'bash -c \"while true; do sudo dmesg | grep -i \\\"tcp\\\" >> {dmesg_log}; sleep 5; done\"'")


class RunnerK8sLog:
    """
    RunnerLog: A utility class for managing Kubernetes pod logging and debugging.

    Methods:
        - start_logging(): Starts continuous logging for running Kubernetes pods.
        - restart_logging(): Restarts logging after an outage.
        - stop_logging(): Stops all running log sessions.
        - store_pod_descriptions(): Saves 'kubectl describe' outputs for all running pods.
        - get_running_pods(): Fetches all currently running pods in a namespace.
    """

    def __init__(self, namespace="spdk-csi", log_dir="/var/logs", test_name="test_run"):
        """
        Initialize the RunnerLog class.

        Args:
            namespace (str): Kubernetes namespace.
            log_dir (str): Directory to store log files.
            test_name (str): Name of the test or session.
        """
        self.namespace = namespace
        self.log_dir = log_dir
        self.test_name = test_name
        self._monitor_thread = None
        self._monitor_stop_flag = threading.Event()
        self._pod_container_map = {}

        # Ensure log directory exists
        os.makedirs(self.log_dir, exist_ok=True)

        self._check_and_install_tmux()

    def _check_and_install_tmux(self):
        """
        Check if tmux is installed on the runner. If not, install it.
        """
        try:
            subprocess.run(["tmux", "-V"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print("tmux is already installed.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("tmux is not installed. Installing now...")
            install_cmd = "sudo apt-get update -y && sudo apt-get install -y tmux || sudo yum install -y tmux"
            subprocess.run(install_cmd, shell=True, check=True)
            print("tmux installed successfully.")

    def generate_random_string(self, length=6):
        """Generate a random string of uppercase letters and digits."""
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

    def get_running_pods(self):
        """
        Fetch running pods in the specified namespace.

        Returns:
            list: A list of running pod names.
        """
        try:
            cmd = ["kubectl", "get", "pods", "-n", self.namespace, "--no-headers", "-o", "custom-columns=:metadata.name"]
            output = subprocess.check_output(cmd, universal_newlines=True).strip()
            return output.split("\n") if output else []
        except subprocess.CalledProcessError as e:
            print(f"Error fetching running pods: {e}")
            return []

    def start_logging(self):
        """
        Start continuous logging for all running Kubernetes pods (before outage).
        """
        self._log_pods("before_outage")

    def restart_logging(self):
        """
        Restart Kubernetes logging after an outage (after outage).
        """
        self._log_pods("after_outage")

    def _log_pods(self, outage_type):
        """
        Internal method to start logging for Kubernetes pods and all their containers.

        Args:
            outage_type (str): "before_outage" or "after_outage".
        """
        pods = self.get_running_pods()
        if not pods:
            print(f"No running pods found for logging ({outage_type}).")
            return

        for pod in pods:
            # Filter pods based on prefixes
            if not (pod.startswith("storage-node-ds") or pod.startswith("snode-spdk-deployment") or pod.startswith("storage-node-handler-")):
                continue

            # Get all containers in the pod
            container_list_cmd = ["kubectl", "get", "pod", pod, "-n", self.namespace, "-o", "jsonpath={.spec.containers[*].name}"]
            try:
                containers = subprocess.check_output(container_list_cmd, universal_newlines=True).strip().split()
            except subprocess.CalledProcessError as e:
                print(f"Error fetching containers for pod {pod}: {e}")
                continue

            for container in containers:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                log_file = f"{self.log_dir}/{pod}_{container}_{self.test_name}_{timestamp}_{outage_type}.log"
                session_name = f"{pod}_{container}_logs_{self.generate_random_string()}"
                container_id = self._get_container_id(pod, container)
                key = f"{pod}:{container}"
                self._pod_container_map[key] = container_id

                command_logs = [
                    "tmux", "new-session", "-d", "-s", session_name,
                    "bash", "-c",
                    f"kubectl logs --follow {pod} -c {container} -n {self.namespace} > {log_file} 2>&1"
                ]

                subprocess.Popen(command_logs)
                print(f"Started logging for pod '{pod}', container '{container}' ({outage_type}), logs stored at {log_file}.")


    def stop_logging(self):
        """
        Stop all Kubernetes logging processes.
        """
        stop_command = ["tmux", "kill-server"]
        subprocess.run(stop_command)
        print("Stopped all Kubernetes logging processes.")

    def store_pod_descriptions(self):
        """
        Store 'kubectl describe' outputs for all running pods.
        """
        pods = self.get_running_pods()
        if not pods:
            print("No running pods found for descriptions.")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for pod in pods:
            describe_file = f"{self.log_dir}/{pod}_{self.test_name}_{timestamp}_describe.log"
            describe_command = ["kubectl", "describe", "pod", pod, "-n", self.namespace]

            with open(describe_file, "w") as f:
                subprocess.run(describe_command, stdout=f, stderr=subprocess.STDOUT)

            print(f"Stored pod description for '{pod}' at {describe_file}.")

    def _get_container_id(self, pod, container):
        try:
            cmd = [
                "kubectl", "get", "pod", pod, "-n", self.namespace,
                "-o", f"jsonpath={{.status.containerStatuses[?(@.name=='{container}')].containerID}}"
            ]
            output = subprocess.check_output(cmd, universal_newlines=True).strip()
            return output.split("//")[-1] if output else None
        except subprocess.CalledProcessError:
            return None

    def monitor_pod_logs(self, poll_interval=10):
        """
        Continuously monitor running pods and their containers for restarts.
        Starts new kubectl log sessions if containers change.
        """

        def _monitor():
            while not self._monitor_stop_flag.is_set():
                pods = self.get_running_pods()
                for pod in pods:
                    if not (pod.startswith("storage-node-ds") or pod.startswith("snode-spdk-deployment") or pod.startswith("storage-node-handler-")):
                        continue

                    cmd = ["kubectl", "get", "pod", pod, "-n", self.namespace,
                        "-o", "jsonpath={.spec.containers[*].name}"]
                    try:
                        containers = subprocess.check_output(cmd, universal_newlines=True).strip().split()
                    except subprocess.CalledProcessError:
                        continue

                    for container in containers:
                        key = f"{pod}:{container}"
                        current_id = self._get_container_id(pod, container)
                        prev_id = self._pod_container_map.get(key)

                        if not current_id:
                            continue

                        if current_id != prev_id:
                            self._pod_container_map[key] = current_id
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            log_file = f"{self.log_dir}/{pod}_{container}_{self.test_name}_{timestamp}_restart.log"
                            session_name = f"{pod}_{container}_restart_{self.generate_random_string()}"

                            cmd = [
                                "tmux", "new-session", "-d", "-s", session_name,
                                "bash", "-c",
                                f"kubectl logs --follow {pod} -c {container} -n {self.namespace} > {log_file} 2>&1"
                            ]
                            subprocess.Popen(cmd)
                            print(f"[K8s] Restarted log collection for {pod}:{container} due to new container instance.")

                time.sleep(poll_interval)

        self._monitor_thread = threading.Thread(target=_monitor, daemon=True)
        self._monitor_thread.start()
        print("Started background K8s log monitor.")

    def stop_log_monitor(self):
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_stop_flag.set()
            self._monitor_thread.join(timeout=10)
            print("K8s log monitor thread stopped.")


