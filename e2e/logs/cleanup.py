import os
import paramiko
import time

# SSH Configuration
BASTION_IP = os.getenv("BASTION_IP")
KEY_PATH = os.path.expanduser(f"~/.ssh/{os.environ.get('KEY_NAME', 'simplyblock-us-east-2.pem')}")
USER = os.getenv("USER", "root")

# Node Lists
STORAGE_PRIVATE_IPS = os.getenv("STORAGE_PRIVATE_IPS", "").split()
SEC_STORAGE_PRIVATE_IPS = os.getenv("SEC_STORAGE_PRIVATE_IPS", "").split()
MNODES = os.getenv("MNODES", "").split()
CLIENTNODES = os.getenv("CLIENTNODES", os.getenv("MNODES", "")).split()
ALL_NODES = MNODES + STORAGE_PRIVATE_IPS + SEC_STORAGE_PRIVATE_IPS + CLIENTNODES
HOME_DIR = os.path.expanduser("~")


# Function to establish SSH connection with bastion support
def connect_ssh(target_ip, bastion_ip=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if not os.path.exists(KEY_PATH):
                raise FileNotFoundError(f"SSH private key not found at {KEY_PATH}")

            private_key = paramiko.Ed25519Key(filename=KEY_PATH)

            if bastion_ip:
                bastion = paramiko.SSHClient()
                bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                bastion.connect(hostname=bastion_ip, username=USER, pkey=private_key, timeout=30)

                transport = bastion.get_transport()
                channel = transport.open_channel("direct-tcpip", (target_ip, 22), ("localhost", 0))

                ssh.connect(target_ip, username=USER, sock=channel, pkey=private_key, timeout=30)
                return ssh
            else:
                ssh.connect(target_ip, username=USER, pkey=private_key, timeout=30)
                return ssh

        except Exception as e:
            print(f"[ERROR] SSH connection to {target_ip} failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)

    raise Exception(f"[ERROR] Failed to connect to {target_ip} after {retries} retries.")

# Function to execute SSH commands with retries
def exec_command(ssh, command, retries=3):
    """Execute an SSH command with retries and print output."""
    for attempt in range(retries):
        try:
            print(f"[INFO] Executing on remote machine: {command}")
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()

            if output:
                print(f"[OUTPUT] {output}")
            if error:
                print(f"[ERROR] {error}")

            return output, error
        except Exception as e:
            print(f"[ERROR] Command execution failed ({attempt+1}/{retries}): {e}")
            time.sleep(2)

    raise Exception(f"[ERROR] Failed to execute command after {retries} retries.")

# Function to clean up logs on remote machines
def cleanup_remote_logs(ssh, node):
    """Deletes all uploaded logs from remote VM to free up space."""
    print(f"[INFO] Cleaning up logs on {node}...")

    # Cleanup commands
    cleanup_commands = [
        f"sudo rm -rf {HOME_DIR}/container-logs.tar.gz",  # Remove compressed tar file
        f"sudo rm -rf {HOME_DIR}/container-logs/*",  # Remove container logs content
        f"sudo rm -rf {HOME_DIR}/*.txt* {HOME_DIR}/*.log {HOME_DIR}/*.state {HOME_DIR}/*iolog*",  # Remove uploaded logs
        "sudo rm -rf /etc/simplyblock/[0-9]*",  # Remove dump logs
        "sudo rm -rf /etc/simplyblock/*core*.zst",  # Remove dump logs
        "sudo rm -rf /etc/simplyblock/LVS*",  # Remove dump logs
        f"sudo rm -rf {HOME_DIR}/upload_to_minio.py"  # Remove temporary upload script
    ]

    for cmd in cleanup_commands:
        exec_command(ssh, cmd)

    print(f"[SUCCESS] Cleaned up logs on {node}.")

# Function to clean up logs on local runner machine
def cleanup_local_logs():
    """Deletes all uploaded logs from local runner (PWD/logs/)."""
    logs_dir = os.path.join(os.getcwd(), "logs")
    local_k8s_log_dir = "/tmp/k8s_logs"

    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. No cleanup needed.")
        return

    print(f"[INFO] Cleaning up local logs from {logs_dir}...")
    os.system(f"sudo rm -rf {logs_dir}/*.log")
    os.system(f"sudo rm -rf {logs_dir}/*.txt")

    print("[SUCCESS] Local logs cleaned up.")


    print(f"[INFO] Cleaning up Kubernetes logs directory: {local_k8s_log_dir}...")
    os.system(f"sudo rm -rf {local_k8s_log_dir}/*")
    print("[SUCCESS] Kubernetes logs directory cleaned up.")


# **Step 1: Cleanup Logs on Remote Machines**
for node in ALL_NODES:
    try:
        ssh = connect_ssh(node, bastion_ip=BASTION_IP)
        cleanup_remote_logs(ssh, node)
        ssh.close()
        print(f"[SUCCESS] Completed cleanup on {node}")
    except Exception as e:
        print(f"[ERROR] Failed to clean up {node}: {e}")

# **Step 2: Cleanup Local Logs**
cleanup_local_logs()

print("[INFO] Log cleanup completed on all machines.")
