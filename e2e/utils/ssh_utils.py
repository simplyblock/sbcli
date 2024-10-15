import time
import paramiko
import os
import json

import paramiko.ssh_exception
from logger_config import setup_logger
from pathlib import Path


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

    def connect(self, address: str, port: int=22,
                bastion_server_address: str=None,
                username: str="ec2-user",
                is_bastion_server: bool=False):
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

        # Load the private key
        if not os.path.exists(SSH_KEY_LOCATION):
            raise FileNotFoundError(f"SSH private key not found at {SSH_KEY_LOCATION}")
        private_key = paramiko.Ed25519Key(filename=SSH_KEY_LOCATION)

        # Connect to the proxy server first
        if not bastion_server_address:
            bastion_server_address = self.bastion_server
        ssh.connect(hostname=bastion_server_address,
                    username=username,
                    port=port,
                    pkey=private_key,
                    timeout=360)
        self.logger.info("Connected to bastion server.")

        if self.ssh_connections.get(bastion_server_address, None):
            self.ssh_connections[bastion_server_address].close()

        self.ssh_connections[bastion_server_address] = ssh

        if is_bastion_server:
            return

        # Setup the transport to the target server through the proxy
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
                           timeout=3600)
        self.logger.info("Connected to target server through proxy.")

        if self.ssh_connections.get(address, None):
            self.ssh_connections[address].close()
        self.ssh_connections[address] = target_ssh

    def exec_command(self, node, command, timeout=360, max_retries=3):
        """Executes command on given machine with a retry mechanism in case of failure.

        Args:
            node (str): Machine to run command on
            command (str): Command to run
            timeout (int): Timeout in seconds
            max_retries (int): Number of retries in case of failures

        Returns:
            str: Output of command
        """
        retry_count = 0
        while retry_count < max_retries:
            ssh_connection = self.ssh_connections.get(node)
            try:
                # Ensure the SSH connection is active, otherwise reconnect
                if not ssh_connection or not ssh_connection.get_transport().is_active() \
                    or retry_count > 0:
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
                if "sudo fio" in command:
                    self.logger.info("Inside while loop")
                    # Read stdout and stderr in a non-blocking way
                    while not stdout.channel.exit_status_ready():
                        if stdout.channel.recv_ready():
                            output.append(stdout.channel.recv(1024).decode())
                        if stderr.channel.recv_stderr_ready():
                            error.append(stderr.channel.recv_stderr(1024).decode())
                        time.sleep(0.1)
                    output = " ".join(output)
                    error = " ".join(error)

                else:
                    output = stdout.read().decode()
                    error = stderr.read().decode()

                if output:
                    self.logger.debug(f"Command output: {output}")
                if error:
                    self.logger.debug(f"Command error: {error}")

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
                   f"--numjobs={numjobs} --nrfiles={nrfiles} --group_reporting{output_format}{output_file}")
        
        if kwargs.get("debug", None):
            command = f"{command} --debug=all"
        if log_file:
            command = f"{command} >> {log_file}"

        self.logger.info(f"{command}")

        start_time = time.time()
        output, error = self.exec_command(node=node, command=command, timeout=runtime)
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
        kill_command = "sudo kill -9 %s"
        if pid:
            command = kill_command % pid
            self.exec_command(node, command)
        if process_name:
            pids = self.find_process_name(node=node,
                                          process_name=process_name,
                                          return_pid=True)
            for pid in pids:
                command = kill_command % pid.strip()
                self.exec_command(node, command)

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
        cmd = "%s lvol list | grep -i %s | awk '{{print $2}}'" % (self.base_cmd, lvol_name)
        output, error = self.exec_command(node=node, command=cmd)
        return output.strip().split()
    
    def get_snapshot_id(self, node, snapshot_name):
        cmd = "%s snapshot list | grep -i '%s' | awk '{print $2}'" % (self.base_cmd, snapshot_name)
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