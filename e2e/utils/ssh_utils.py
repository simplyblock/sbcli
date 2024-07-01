import time
import paramiko
import os
from logger_config import setup_logger
from pathlib import Path


SSH_KEY_LOCATION = os.path.join(Path.home(), ".ssh", os.environ.get("KEY_NAME"))


class SshUtils:
    """Class to perform all ssh level operationa
    """

    def __init__(self, bastion_server):
        self.ssh_connections = dict()
        self.bastion_server = bastion_server
        self.logger = setup_logger(__name__)

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
                    timeout=3600)
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

    def exec_command(self, node, command, timeout=3600):
        """Executes command on given machine with a timeout.

        Args:
            node (str): Machine to run command on
            command (str): Command to run
            timeout (int): Timeout in seconds

        Returns:
            str: Output of command
        """
        ssh_connection = self.ssh_connections[node]
        if not ssh_connection.get_transport().is_active():
            self.logger.info(f"Reconnecting SSH to node {node}")
            self.connect(
                address=node,
                is_bastion_server=True if node == self.bastion_server else False
            )
            ssh_connection = self.ssh_connections[node]

        self.logger.info(f"Command: {command}")
        try:
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

            self.logger.debug(f"Command output: {output}")
            self.logger.debug(f"Command error: {error}")

            if not output and not error:
                self.logger.warning(f"Command '{command}' executed but returned no output or error.")

            return output, error
        except paramiko.SSHException as e:
            self.logger.error(f"SSH command failed: {e}")
            return "", str(e)
        except paramiko.ssh_exception.SSHException as e:
            self.logger.error(f"SSH connection failed: {e}")
            return "", str(e)

    def format_disk(self, node, device):
        """Format disk on the given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
        """
        command = f"sudo mkfs.ext4 {device}"
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

        command = f"sudo mkdir {mount_path}"
        self.exec_command(node, command)

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
        
        output_format = kwargs.get("output_format", '')
        output_format = f' --output-format={output_format} ' if output_format else ''

        output_file = kwargs.get("output_file", '')
        output_file = f" --output={output_file} " if output_file else ''

        command = (f"sudo fio --name={name} {location} --ioengine={ioengine} --direct=1 --iodepth={iodepth} "
                   f"{time_based} --runtime={runtime} --rw={rw} --bs={bs} --size={size} --rwmixread={rwmixread} "
                   "--verify=md5 --numjobs=1 --verify_dump=1 --verify_fatal=1 --verify_state_save=1 --verify_backlog=10 "
                   f"--group_reporting{output_format}{output_file}")
        
        if kwargs.get("debug", None):
            command = f"{command} --debug=all"
        if log_file:
            command = f"{command} >> {log_file}"

        self.logger.info(f"{command}")

        start_time = time.time()
        output, error = self.exec_command(node=node, command=command, timeout=runtime + 300)
        end_time = time.time()

        total_time = end_time - start_time
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
        rec = "r" if recursive else ""
        cmd = f'sudo rm -{rec}f {entity}'
        output, _ = self.exec_command(node=node, command=cmd)
        return output
