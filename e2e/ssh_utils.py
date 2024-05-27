import paramiko


SSH_KEY_LOCATION = '/mnt/c/Users/Raunak Jalan/.ssh/simplyblock-us-east-2.pem'

class SshUtils:
    """Class to perform all ssh level operationa
    """

    def __init__(self):
        self.ssh_connections = dict()

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
        private_key = paramiko.Ed25519Key(filename=SSH_KEY_LOCATION)

        # Connect to the proxy server first
        ssh.connect(hostname=bastion_server_address,
                    username=username,
                    port=port,
                    pkey=private_key)
        print("Connected to bastion server.")

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
                            pkey=private_key)
        print("Connected to target server through proxy.")

        # Run a command on the target server
        stdin, stdout, stderr = target_ssh.exec_command('uname -a')
        print("Output: ", stdout.read().decode())

        # stdin, stdout, stderr = target_ssh.exec_command('sudo lsblk')
        # print("Output: ", stdout.read().decode())
        
        if self.ssh_connections.get(address, None):
            self.ssh_connections[address].close()
        self.ssh_connections[address] = target_ssh

    def format_disk(self, node, device):
        """Format disk on the given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
        """
        command = f"sudo mkfs.xfs {device}"
        ssh_connection = self.ssh_connections[node]
        stdin, stdout, stderr = ssh_connection.exec_command(command)
        print("Output: ", stdout.read().decode())

    def mount_path(self, node, device, mount_path):
        """Mount device to given path on given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
            mount_path (_type_): Mount path to perform mount on
        """

        ssh_connection = self.ssh_connections[node]
        try:
            stdin, stdout, stderr = ssh_connection.exec_command(f"sudo rm -rf {mount_path}")
            print("Output: ", stdout.read().decode())
            print("Error: ", stderr.read().decode())
        except Exception as e:
            print(e)
        stdin, stdout, stderr = ssh_connection.exec_command(f"sudo rm -rf {mount_path}")
        print("Output: ", stdout.read().decode())

        command = f"sudo mount {device} {mount_path}"
        # stdin, stdout, stderr = ssh_connection.exec_command(command)
        # print("Output: ", stdout.read().decode())

    def unmount_path(self, node, device):
        """Unmount device to given path on given node

        Args:
            node (str): Node to perform ssh operation on
            device (str): Device path
        """
        command = f"sudo umount {device}"
        ssh_connection = self.ssh_connections[node]
        stdin, stdout, stderr = ssh_connection.exec_command(command)
        print("Output: ", stdout.read().decode())