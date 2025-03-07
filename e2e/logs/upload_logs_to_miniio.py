import os
import paramiko
import boto3
import argparse
import time
import subprocess

# Parse arguments
parser = argparse.ArgumentParser(description="Fetch and upload logs from Docker and/or Kubernetes.")
parser.add_argument("--k8s", action="store_true", help="Use Kubernetes logs for storage nodes instead of Docker.")
args = parser.parse_args()

# MinIO Configuration
MINIO_ENDPOINT = "http://192.168.10.164:9000"
MINIO_BUCKET = "e2e-run-logs"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# SSH and Node Details
BASTION_IP = os.getenv("BASTION_IP")
KEY_PATH = os.path.expanduser("~/.ssh/simplyblock-us-east-2.pem")
USER = os.getenv("USER", "root")


# Node List
STORAGE_PRIVATE_IPS = os.getenv("STORAGE_PRIVATE_IPS", "").split()
SEC_STORAGE_PRIVATE_IPS = os.getenv("SEC_STORAGE_PRIVATE_IPS", "").split()
MNODES = os.getenv("MNODES", "").split()

DOCKER_NODES = MNODES if args.k8s else STORAGE_PRIVATE_IPS + SEC_STORAGE_PRIVATE_IPS + MNODES

# Upload Folder
UPLOAD_FOLDER = os.getenv("GITHUB_RUN_ID", time.strftime("%Y-%m-%d_%H-%M-%S"))

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
def upload_from_remote(ssh, node):
    print(f"[INFO] Uploading logs from {node} to MinIO...")

    install_boto_on_target(ssh)  # Ensure boto3 is installed

    # Get HOME directory
    stdout, _ = exec_command(ssh, "echo $HOME")
    home_dir = stdout.strip()

    # **Zip and Upload container-logs Folder**
    container_logs_path = f"{home_dir}/container-logs/"
    stdout, _ = exec_command(ssh, f"ls -d {container_logs_path} 2>/dev/null || echo 'missing'")
    
    if "missing" in stdout:
        print(f"[WARNING] {container_logs_path} does not exist on {node}. Skipping container logs...")
    else:
        tar_path = f"{home_dir}/container-logs.tar.gz"
        print(f"[INFO] Zipping {container_logs_path} on {node}...")
        exec_command(ssh, f"tar -czf {tar_path} -C {home_dir} container-logs")

        file_key = f"{UPLOAD_FOLDER}/{node}/container-logs.tar.gz"
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

        temp_script_path = f"{home_dir}/upload_to_minio.py"

        print(f"[INFO] Writing upload script to {node}...")
        exec_command(ssh, f"cat <<EOF > {temp_script_path}\n{upload_script}\nEOF")

        print(f"[INFO] Executing upload script on {node}...")
        exec_command(ssh, f"python3 {temp_script_path}")

    # **Upload Other Logs Separately**
    for remote_path in [
        f"{home_dir}/*.txt*",
        f"{home_dir}/*.log",
        f"{home_dir}/*.state",
        f"/etc/simplyblock/*"
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
            if f"{home_dir}/etc/simplyblock/" in remote_file:
                subfolder = "dump"
            else:
                subfolder = "root-logs"

            file_key = f"{UPLOAD_FOLDER}/{node}/{subfolder}/{os.path.basename(remote_file)}"
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

            temp_script_path = f"{home_dir}/upload_to_minio.py"

            print(f"[INFO] Writing upload script to {node}...")
            exec_command(ssh, f"cat <<EOF > {temp_script_path}\n{upload_script}\nEOF")

            print(f"[INFO] Executing upload script on {node}...")
            exec_command(ssh, f"python3 {temp_script_path}")




# Upload Kubernetes logs (Directly from runner node)
def upload_k8s_logs():
    print("[INFO] Fetching Kubernetes logs from runner node...")

    local_k8s_log_path = "/tmp/k8s_logs.txt"
    subprocess.run(f"kubectl logs -A --timestamps > {local_k8s_log_path}", shell=True, check=True)

    file_key = f"{UPLOAD_FOLDER}/runner-node/k8s_logs.txt"
    s3_client.upload_file(local_k8s_log_path, MINIO_BUCKET, file_key)

    print(f"[SUCCESS] Kubernetes logs uploaded: {file_key}")

# **Upload Logs from PWD/logs Directory to MinIO**
def upload_local_logs():
    """Uploads log files from the local 'logs/' directory to MinIO."""
    logs_dir = os.path.join(os.getcwd(), "logs")  # Get the full path to 'logs/'
    
    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. Skipping local log upload.")
        return
    
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

            log_file = f"/root/{container_name}_{container_id}_{node}.txt"
            exec_command(ssh, f"sudo docker logs {container_id} &> {log_file}")

        upload_from_remote(ssh, node)

        ssh.close()
        print(f"[SUCCESS] Successfully processed Management Node {node}")

    except Exception as e:
        print(f"[ERROR] Error processing Management Node {node}: {e}")

# **Step 2: Process Storage Nodes (Only for Docker mode)**
if not args.k8s:
    for node in STORAGE_PRIVATE_IPS + SEC_STORAGE_PRIVATE_IPS:
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

                log_file = f"/root/{container_name}_{container_id}_{node}.txt"
                exec_command(ssh, f"sudo docker logs {container_id} &> {log_file}")

            upload_from_remote(ssh, node)
            ssh.close()
            print(f"[SUCCESS] Successfully processed Storage Node {node}")
        except Exception as e:
            print(f"[ERROR] Error processing Storage Node {node}: {e}")

# **Step 3: Process Kubernetes Nodes (Upload logs directly from runner)**
if args.k8s:
    upload_k8s_logs()

# **Step 4: Upload Local Logs After Remote Processing**
upload_local_logs()
