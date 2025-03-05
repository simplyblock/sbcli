import os
import paramiko
import subprocess
import boto3
import argparse
from datetime import datetime

# Parse arguments
parser = argparse.ArgumentParser(description="Fetch and upload logs from Docker and/or Kubernetes.")
parser.add_argument("--k8s", action="store_true", help="Use Kubernetes logs for storage nodes instead of Docker.")
args = parser.parse_args()

# Define MinIO details
MINIO_ENDPOINT = "http://192.168.10.164:9000"
MINIO_BUCKET = "e2e-run-logs"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")  # Replace with actual credentials
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")  # Replace with actual credentials

# Define SSH and Node details
BASTION_IP = os.getenv("BASTION_IP")
KEY_PATH = os.path.expanduser("~/.ssh/simplyblock-us-east-2.pem")
USER = "root"
HOME_DIR = os.path.expanduser("~")  # Dynamic home path

# Get node IPs from environment variables
STORAGE_PRIVATE_IPS = os.getenv("STORAGE_PRIVATE_IPS", "").split()
SEC_STORAGE_PRIVATE_IPS = os.getenv("SEC_STORAGE_PRIVATE_IPS", "").split()
MNODES = os.getenv("MNODES", "").split()  # Management logs nodes

# Determine whether to use Docker or Kubernetes logs
if args.k8s:
    K8S_NODES = STORAGE_PRIVATE_IPS + SEC_STORAGE_PRIVATE_IPS  # Use K8s for storage nodes
    DOCKER_NODES = MNODES  # Mgmt logs always in Docker
else:
    K8S_NODES = []  # No K8s logs if --k8s is not passed
    DOCKER_NODES = STORAGE_PRIVATE_IPS + SEC_STORAGE_PRIVATE_IPS + MNODES  # Use Docker for all

# Determine Upload Folder
GITHUB_RUN_ID = os.getenv("GITHUB_RUN_ID", None)
UPLOAD_FOLDER = GITHUB_RUN_ID if GITHUB_RUN_ID else datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Initialize MinIO Client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Ensure the bucket exists
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
except Exception:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

# Establish SSH connection with Proxy Jump
def get_ssh_client():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    proxy = paramiko.ProxyCommand(f"ssh -i {KEY_PATH} -W %h:%p -o StrictHostKeyChecking=no {USER}@{BASTION_IP}")

    return ssh, proxy

# Function to execute SSH command
def execute_ssh_command(ssh, command):
    stdin, stdout, stderr = ssh.exec_command(command)
    return stdout.read().decode(), stderr.read().decode()

# Function to SCP and Upload to MinIO
def scp_and_upload(node, remote_path):
    temp_local_file = f"/tmp/{os.path.basename(remote_path)}"

    # Copy file using SCP
    cmd = f"scp -i {KEY_PATH} -o StrictHostKeyChecking=no -o ProxyCommand='ssh -i {KEY_PATH} -W %h:%p -o StrictHostKeyChecking=no {USER}@{BASTION_IP}' {USER}@{node}:{remote_path} {temp_local_file}"
    subprocess.run(cmd, shell=True, check=False)

    # Upload file to MinIO
    try:
        file_key = f"{UPLOAD_FOLDER}/{node}/{os.path.basename(remote_path)}"
        s3_client.upload_file(temp_local_file, MINIO_BUCKET, file_key)
        print(f"Uploaded {temp_local_file} to MinIO as {file_key}")
    except Exception as e:
        print(f"Failed to upload {temp_local_file} to MinIO: {e}")

    # Remove temporary file
    os.remove(temp_local_file)

# Function to upload all logs from $HOME/container-logs and then clean the directory
def upload_and_cleanup_logs(node):
    logs_dir = os.path.join(HOME_DIR, "container-logs")
    
    if not os.path.exists(logs_dir):
        print(f"No logs directory found on {node}, skipping upload.")
        return
    
    for root, _, files in os.walk(logs_dir):
        for file in files:
            file_path = os.path.join(root, file)
            file_key = f"{UPLOAD_FOLDER}/{node}/container-logs/{file}"

            try:
                s3_client.upload_file(file_path, MINIO_BUCKET, file_key)
                print(f"Uploaded {file_path} to MinIO as {file_key}")
            except Exception as e:
                print(f"Failed to upload {file_path} to MinIO: {e}")

    # Clean up logs after upload
    subprocess.run(f"rm -rf {logs_dir}/*", shell=True)
    print(f"Cleaned up {logs_dir} after upload.")

# Fetch Docker logs from all nodes in DOCKER_NODES
for node in DOCKER_NODES:
    try:
        ssh, proxy = get_ssh_client()
        ssh.connect(node, username=USER, key_filename=KEY_PATH, sock=proxy)

        # Get Docker container IDs
        stdout, _ = execute_ssh_command(ssh, "sudo docker ps -aq")
        container_ids = stdout.strip().split("\n")

        # Remove old logs
        for container_id in container_ids:
            execute_ssh_command(ssh, f"sudo rm -rf {HOME_DIR}/{container_id}_*.txt")

        # Save new logs
        for container_id in container_ids:
            execute_ssh_command(ssh, f"sudo docker logs {container_id} &> {HOME_DIR}/{container_id}_{node}.txt")

        # Upload all logs and clean up
        upload_and_cleanup_logs(node)

        ssh.close()

    except Exception as e:
        print(f"Error processing node {node}: {e}")

# Function to fetch and upload Kubernetes logs for each container in each pod
def fetch_and_upload_k8s_logs():
    namespace = "spdk-csi"
    try:
        # Get all pod names
        cmd = f"kubectl get pods -n {namespace} --no-headers -o custom-columns=:metadata.name"
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        pods = result.stdout.strip().split("\n")

        for pod in pods:
            # Get all containers in the pod
            cmd = f"kubectl get pod {pod} -n {namespace} -o jsonpath='{{.spec.containers[*].name}}'"
            result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
            containers = result.stdout.strip().split()

            for container in containers:
                temp_local_file = f"/tmp/{pod}_{container}_logs.txt"
                cmd = f"kubectl logs {pod} -n {namespace} -c {container} --timestamps > {temp_local_file}"
                subprocess.run(cmd, shell=True, check=True)

                # Upload to MinIO
                file_key = f"{UPLOAD_FOLDER}/k8s/{pod}/{container}_logs.txt"
                s3_client.upload_file(temp_local_file, MINIO_BUCKET, file_key)
                print(f"Uploaded {temp_local_file} to MinIO as {file_key}")

                os.remove(temp_local_file)

        # Upload and cleanup Kubernetes logs directory
        upload_and_cleanup_logs("vm-runner")

    except Exception as e:
        print(f"Failed to fetch or upload Kubernetes logs: {e}")

# Fetch Kubernetes logs if --k8s flag is passed
if args.k8s:
    fetch_and_upload_k8s_logs()
