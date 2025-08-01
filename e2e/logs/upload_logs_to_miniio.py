import os
import paramiko
import boto3
import argparse
import time
import subprocess

# Parse arguments
parser = argparse.ArgumentParser(description="Fetch and upload logs from Docker and/or Kubernetes.")
parser.add_argument("--k8s", action="store_true", help="Use Kubernetes logs for storage nodes instead of Docker.")
parser.add_argument("--no_client", action="store_true", help="Do not get client logs.")
args = parser.parse_args()

# MinIO Configuration
MINIO_ENDPOINT = "http://192.168.10.164:9000"
MINIO_BUCKET = "e2e-run-logs"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# SSH and Node Details
BASTION_IP = os.getenv("BASTION_IP")
KEY_PATH = os.path.expanduser(f"~/.ssh/{os.environ.get('KEY_NAME', 'simplyblock-us-east-2.pem')}")
USER = os.getenv("USER", "root")


# Node List
STORAGE_PRIVATE_IPS = os.getenv("STORAGE_PRIVATE_IPS", "").split()
MNODES = os.getenv("MNODES", "").split()
CLIENTNODES = os.getenv("CLIENTNODES", os.getenv("MNODES", "")).split()

# Upload Folder
UPLOAD_FOLDER = os.getenv("GITHUB_RUN_ID", time.strftime("%Y-%m-%d_%H-%M-%S"))
HOME_DIR = os.getenv("HOME_PATH", os.path.expanduser("~"))

# Initialize MinIO Client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Ensure MinIO Bucket Exists
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
except Exception:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

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
            print(f"[ERROR] SSH connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)

    raise Exception(f"[ERROR] Failed to connect to {target_ip} after {retries} retries.")

# Function to execute SSH commands with retries
def exec_command(ssh, command, retries=3):
    """Execute an SSH command with retries and print output."""
    for attempt in range(retries):
        try:
            print(f"[INFO] Executing: {command}")  # Print every command executed
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode().strip()
            error = stderr.read().decode().strip()

            if output:
                print(f"[OUTPUT] {output}")  # Print output for debugging
            if error:
                print(f"[ERROR] {error}")  # Print error output

            return output, error
        except Exception as e:
            print(f"[ERROR] Command execution failed ({attempt+1}/{retries}): {e}")
            time.sleep(2)

    raise Exception(f"[ERROR] Failed to execute command after {retries} retries.")



def install_boto_on_target(ssh):
    check_boto_cmd = "python3 -c 'import boto3' || echo 'missing'"
    install_boto_cmd = "python3 -m pip install boto3"

    stdout, _ = exec_command(ssh, check_boto_cmd)
    if "missing" in stdout:
        exec_command(ssh, install_boto_cmd)

# Upload Files from Remote VMs
def upload_from_remote(ssh, node, node_type):
    print(f"[INFO] Uploading logs from {node} to MinIO...")

    install_boto_on_target(ssh)  # Ensure boto3 is installed

    # **Zip and Upload container-logs Folder**
    container_logs_path = f"{HOME_DIR}/container-logs/"
    stdout, _ = exec_command(ssh, f"ls -d {container_logs_path} 2>/dev/null || echo 'missing'")
    
    if "missing" in stdout:
        print(f"[WARNING] {container_logs_path} does not exist on {node}. Skipping container logs...")
    else:
        tar_path = f"{HOME_DIR}/container-logs.tar.gz"
        print(f"[INFO] Zipping {container_logs_path} on {node}...")
        exec_command(ssh, f"tar -czf {tar_path} -C {HOME_DIR}/container-logs")

        file_key = f"{UPLOAD_FOLDER}/{node}-{node_type}/container-logs.tar.gz"
        print(f"[INFO] Uploading {tar_path} → MinIO as {file_key}...")

        # Generate the MinIO upload script on the remote VM
        upload_script = f"""import boto3
import os
s3_client = boto3.client("s3", endpoint_url="{MINIO_ENDPOINT}", 
                         aws_access_key_id="{MINIO_ACCESS_KEY}", 
                         aws_secret_access_key="{MINIO_SECRET_KEY}")

file_path = "{tar_path}"
file_key = "{file_key}"

try:
    if os.path.exists(file_path):
        s3_client.upload_file(file_path, "{MINIO_BUCKET}", file_key)
        print("[SUCCESS] Uploaded:", file_key)
    else:
        print("[ERROR] File not found:", file_path)
except Exception as e:
    print("[ERROR] Upload failed:", e)
"""

        temp_script_path = f"{HOME_DIR}/upload_to_minio.py"

        print(f"[INFO] Writing upload script to {node}...")
        exec_command(ssh, f"cat <<EOF > {temp_script_path}\n{upload_script}\nEOF")

        print(f"[INFO] Executing upload script on {node}...")
        exec_command(ssh, f"python3 {temp_script_path}")

    # **Upload Other Logs Separately**
    for remote_path in [
        f"{HOME_DIR}/*.txt*",
        f"{HOME_DIR}/*.log",
        f"{HOME_DIR}/*.state",
        f"{HOME_DIR}/*fio_iolog*",
        "/etc/simplyblock/*",
        "/var/simplyblock/*"
    ]:
        print(f"[INFO] Checking if {remote_path} exists on {node}...")
        stdout, _ = exec_command(ssh, f"ls -1 {remote_path} 2>/dev/null")

        files = stdout.split("\n") if stdout else []
        if not files:
            print(f"[WARNING] No files found in {remote_path} on {node}. Skipping...")
            continue

        for file in files:
            if not file:
                continue

            remote_file = file.strip()

            # Fix: Assign proper subfolders for different paths
            if "/etc/simplyblock/" in remote_file:
                subfolder = "dump"
            else:
                subfolder = "root-logs"

            file_key = f"{UPLOAD_FOLDER}/{node}-{node_type}/{subfolder}/{os.path.basename(remote_file)}"
            print(f"[INFO] Preparing to upload {remote_file} → MinIO as {file_key}...")

            # Generate the MinIO upload script on the remote VM
            upload_script = f"""import boto3
import os
s3_client = boto3.client("s3", endpoint_url="{MINIO_ENDPOINT}", 
                         aws_access_key_id="{MINIO_ACCESS_KEY}", 
                         aws_secret_access_key="{MINIO_SECRET_KEY}")

file_path = "{remote_file}"
file_key = "{file_key}"

try:
    if os.path.exists(file_path):
        s3_client.upload_file(file_path, "{MINIO_BUCKET}", file_key)
        print("[SUCCESS] Uploaded:", file_key)
    else:
        print("[ERROR] File not found:", file_path)
except Exception as e:
    print("[ERROR] Upload failed:", e)
"""

            temp_script_path = f"{HOME_DIR}/upload_to_minio.py"

            print(f"[INFO] Writing upload script to {node}...")
            exec_command(ssh, f"cat <<EOF > {temp_script_path}\n{upload_script}\nEOF")

            print(f"[INFO] Executing upload script on {node}...")
            exec_command(ssh, f"python3 {temp_script_path}")


def upload_k8s_logs():
    """Fetches Kubernetes logs from the runner node and uploads them to MinIO."""
    print("[INFO] Fetching Kubernetes logs from runner node...")

    local_k8s_log_dir = "/tmp/k8s_logs"
    os.makedirs(local_k8s_log_dir, exist_ok=True)

    # Get all namespaces
    namespace = "simplyblk"

    print(f"[INFO] Processing namespace: {namespace}")

    # Get all pods in the namespace
    pods = subprocess.run(f"kubectl get pods -n {namespace} --no-headers -o custom-columns=:metadata.name",
                            shell=True, check=True, capture_output=True, text=True).stdout.splitlines()

    if not pods:
        print(f"[WARNING] No pods found in namespace {namespace}.")

    for pod in pods:
        # Get all containers inside the pod
        containers = subprocess.run(f"kubectl get pod {pod} -n {namespace} -o jsonpath='{{.spec.containers[*].name}}'",
                                    shell=True, check=True, capture_output=True, text=True).stdout.split()

        if not containers:
            print(f"[WARNING] No containers found in pod {pod} (Namespace: {namespace}).")
            continue

        for container in containers:
            log_file = f"{local_k8s_log_dir}/{namespace}_{pod}_{container}.log"

            print(f"[INFO] Fetching logs for Pod: {pod}, Container: {container}, Namespace: {namespace}...")

            # Fetch logs for the specific container
            subprocess.run(f"kubectl logs {pod} -n {namespace} -c {container} --timestamps > {log_file}",
                            shell=True, check=False)

    # Upload all collected logs
    for file in os.listdir(local_k8s_log_dir):
        file_path = os.path.join(local_k8s_log_dir, file)
        file_key = f"{UPLOAD_FOLDER}/runner-node/k8s_logs/{file}"

        print(f"[INFO] Uploading {file_path} → MinIO as {file_key}...")

        try:
            s3_client.upload_file(file_path, MINIO_BUCKET, file_key)
            print(f"[SUCCESS] Uploaded: {file_key}")
        except Exception as e:
            print(f"[ERROR] Failed to upload {file}: {e}")

    print("[SUCCESS] Kubernetes logs uploaded successfully.")


# **Upload Logs from PWD/logs Directory to MinIO**
def upload_local_logs(k8s=False):
    """Uploads log files from the local 'logs/' directory to MinIO.
    
    If k8s=True, it creates and uploads a tar archive of the 'container-logs/' directory in $HOME.
    """
    logs_dir = os.path.join(os.getcwd(), "logs")  # Get the full path to 'logs/'

    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. Skipping local log upload.")
    else:
        print(f"[INFO] Uploading local logs from {logs_dir} to MinIO...")

        # Iterate over all files in the logs directory
        for file in os.listdir(logs_dir):
            local_file_path = os.path.join(logs_dir, file)

            # Skip directories, only upload files
            if os.path.isdir(local_file_path):
                continue

            # Define MinIO file path
            file_key = f"{UPLOAD_FOLDER}/runner-node/logs/{file}"

            try:
                # Upload to MinIO
                s3_client.upload_file(local_file_path, MINIO_BUCKET, file_key)
                print(f"[SUCCESS] Uploaded: {file_key}")
            except Exception as e:
                print(f"[ERROR] Failed to upload {file}: {e}")

    # **Kubernetes-Specific Handling**
    if k8s:
        home_dir = os.path.expanduser("~")  # Get $HOME
        container_logs_dir = os.path.join(home_dir, "container-logs")
        tar_file_path = os.path.join(home_dir, "container-logs.tar.gz")

        if os.path.exists(container_logs_dir):
            print(f"[INFO] Creating tar archive of {container_logs_dir}...")
            
            # Run tar command to compress the directory
            tar_command = f"tar -czf {tar_file_path} -C {home_dir} container-logs"
            result = subprocess.run(tar_command, shell=True, capture_output=True, text=True)

            if result.returncode == 0:
                print(f"[SUCCESS] Created tar file: {tar_file_path}")

                # Define MinIO path for tar file
                file_key = f"{UPLOAD_FOLDER}/runner-node/container-logs.tar.gz"

                try:
                    # Upload tar file to MinIO
                    s3_client.upload_file(tar_file_path, MINIO_BUCKET, file_key)
                    print(f"[SUCCESS] Uploaded: {file_key}")

                    # Cleanup the tar file after upload
                    os.remove(tar_file_path)
                    print(f"[INFO] Removed local tar file: {tar_file_path}")
                except Exception as e:
                    print(f"[ERROR] Failed to upload tar file: {e}")
            else:
                print(f"[ERROR] Failed to create tar archive: {result.stderr}")
        else:
            print(f"[WARNING] {container_logs_dir} does not exist. Skipping tar creation.")

def cleanup_remote_logs(ssh, node):
    """Deletes all uploaded logs from remote VM to free up space."""
    print(f"[INFO] Cleaning up logs on {node}...")

    # Remove uploaded logs
    cleanup_commands = [
        f"rm -rf {HOME_DIR}/container-logs.tar.gz",  # Remove compressed tar file
        f"rm -rf {HOME_DIR}/container-logs/*",  # Remove container logs content
        f"rm -rf {HOME_DIR}/*.txt {HOME_DIR}/*.log {HOME_DIR}/*.state {HOME_DIR}/*fio_iolog*",  # Remove uploaded logs
        "rm -rf /etc/simplyblock/[0-9]*",  # Remove dump logs
        "rm -rf /etc/simplyblock/*core*.zst",  # Remove dump logs
        "rm -rf /etc/simplyblock/LVS*",  # Remove dump logs
        f"rm -rf {HOME_DIR}/upload_to_minio.py"  # Remove temporary upload script
    ]

    for cmd in cleanup_commands:
        exec_command(ssh, cmd)
    
    print(f"[SUCCESS] Cleaned up logs on {node}.")


def cleanup_local_logs():
    """Deletes all uploaded logs from local runner (PWD/logs/)."""
    logs_dir = os.path.join(os.getcwd(), "logs")  # Get the full path to 'logs/'

    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. No cleanup needed.")
        return

    print(f"[INFO] Cleaning up local logs from {logs_dir}...")
    subprocess.run(f"rm -rf {logs_dir}/*.log", shell=True, check=True)
    subprocess.run(f"rm -rf {logs_dir}/*.txt", shell=True, check=True)
    print("[SUCCESS] Local logs cleaned up.")


# **Step 1: Process Management Node (Same for both Docker & Kubernetes mode)**
for node in MNODES:
    try:
        ssh = connect_ssh(node, bastion_ip=BASTION_IP)
        print(f"[INFO] Processing Management Node {node}...")

        stdout, _ = exec_command(ssh, "sudo docker ps -aq")
        container_ids = stdout.strip().split("\n")

        for container_id in container_ids:
            if not container_id:
                continue

            stdout, _ = exec_command(ssh, f'sudo docker inspect --format="{{{{.Name}}}}" {container_id}')
            container_name = stdout.strip().replace("/", "")

            log_file = f"{HOME_DIR}/{container_name}_{container_id}_{node}.txt"
            exec_command(ssh, f"sudo docker logs {container_id} &> {log_file}")

        upload_from_remote(ssh, node, node_type="mgmt")

        ssh.close()
        print(f"[SUCCESS] Successfully processed Management Node {node}")

    except Exception as e:
        print(f"[ERROR] Error processing Management Node {node}: {e}")

# **Step 2: Process Storage Node**
for node in STORAGE_PRIVATE_IPS:
    try:
        ssh = connect_ssh(node, bastion_ip=BASTION_IP)
        print(f"[INFO] Processing Storage Node {node}...")

        stdout, _ = exec_command(ssh, "sudo docker ps -aq")
        container_ids = stdout.strip().split("\n")

        for container_id in container_ids:
            if not container_id:
                continue

            stdout, _ = exec_command(ssh, f'sudo docker inspect --format="{{{{.Name}}}}" {container_id}')
            container_name = stdout.strip().replace("/", "")

            log_file = f"{HOME_DIR}/{container_name}_{container_id}_{node}.txt"
            exec_command(ssh, f"sudo docker logs {container_id} &> {log_file}")
            upload_from_remote(ssh, node, node_type="storage")
        ssh.close()
        print(f"[SUCCESS] Successfully processed Storage Node {node}")
    except Exception as e:
        print(f"[ERROR] Error processing Storage Node {node}: {e}")

if not args.no_client:
    for node in CLIENTNODES:
        try:
            ssh = connect_ssh(node, bastion_ip=BASTION_IP)
            print(f"[INFO] Processing Client Node {node}...")

            upload_from_remote(ssh, node, node_type="client")

            ssh.close()
            print(f"[SUCCESS] Successfully processed Client Node {node}")

        except Exception as e:
            print(f"[ERROR] Error processing Client Node {node}: {e}")
else:
    print("!! Skipping Clients as no client flag is set !!")

# **Step 3: Process Kubernetes Nodes (Upload logs directly from runner)**
if args.k8s:
    upload_k8s_logs()

# **Step 4: Upload Local Logs After Remote Processing**
if args.k8s:
    upload_local_logs(k8s=True)
else:
    upload_local_logs()
