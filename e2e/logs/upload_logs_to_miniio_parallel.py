#!/usr/bin/env python3
"""
Parallel MinIO uploader with:
- Remote + local uploads
- Byte-level global progress bars (tqdm if available)
- Per-file retries with exponential backoff + jitter
- Robust JSON embedding via SFTP (no giant heredocs)
- Remote uploader accepts [src, key] OR {"src":..., "key":...}
"""

import os
import json
import argparse
import time
import subprocess
import random
import threading
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import paramiko
import boto3
from boto3.s3.transfer import TransferConfig

# Optional tqdm for byte-level progress
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

# -------------------- CLI --------------------
parser = argparse.ArgumentParser(description="Fetch & upload logs to MinIO with parallel, retry, and byte-level progress.")
parser.add_argument("--k8s", action="store_true", help="Use Kubernetes logs for storage nodes in addition to Docker.")
parser.add_argument("--no_client", action="store_true", help="Do not get client logs.")
args = parser.parse_args()

# -------------------- Config --------------------
# MinIO / S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://192.168.10.164:9000")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",   "e2e-run-logs")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")

# Concurrency
MAX_PARALLEL_UPLOADS = int(os.getenv("MAX_PARALLEL_UPLOADS", "100"))  # files in flight
PER_FILE_THREADS     = int(os.getenv("PER_FILE_THREADS", "8"))        # threads per multipart transfer

# Retry/backoff
UPLOAD_RETRIES    = int(os.getenv("UPLOAD_RETRIES", "5"))
BACKOFF_BASE_SECS = float(os.getenv("BACKOFF_BASE_SECS", "1.0"))
BACKOFF_MAX_SECS  = float(os.getenv("BACKOFF_MAX_SECS", "30.0"))

transfer_config = TransferConfig(
    max_concurrency=PER_FILE_THREADS,
    multipart_threshold=8*1024*1024,
    multipart_chunksize=8*1024*1024,
    use_threads=True
)

# SSH & Nodes
BASTION_IP = os.getenv("BASTION_IP")
KEY_PATH = os.path.expanduser(f"~/.ssh/{os.environ.get('KEY_NAME', 'simplyblock-us-east-2.pem')}")
USER = os.getenv("USER", "root")

STORAGE_PRIVATE_IPS = os.getenv("STORAGE_PRIVATE_IPS", "").split()
MNODES = os.getenv("MNODES", "").split()
CLIENTNODES = os.getenv("CLIENTNODES", os.getenv("MNODES", "")).split()

UPLOAD_FOLDER = os.getenv("GITHUB_RUN_ID", time.strftime("%Y-%m-%d_%H-%M-%S"))
HOME_DIR = os.getenv("HOME_PATH", os.path.expanduser("~"))
REMOTE_TMP = f"{HOME_DIR}/.minio_uploader"  # small workspace on remote nodes

# S3 client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Ensure bucket exists
try:
    s3_client.head_bucket(Bucket=MINIO_BUCKET)
except Exception:
    s3_client.create_bucket(Bucket=MINIO_BUCKET)

# -------------------- Progress helpers --------------------
class GlobalByteProgress:
    """Thread-safe, aggregate byte-level progress bar for multiple concurrent uploads."""
    def __init__(self, total_bytes: int, desc: str):
        self.total = int(total_bytes)
        self.lock = threading.Lock()
        if tqdm and self.total > 0:
            self.bar = tqdm(total=self.total, unit="B", unit_scale=True, unit_divisor=1024, desc=desc)
        else:
            self.bar = None
            print(f"[INFO] {desc}: total {self.total} bytes (tqdm not installed; aggregate only)")

    def update(self, n: int):
        if not self.bar:
            return
        with self.lock:
            self.bar.update(n)

    def callback(self, bytes_amount: int):
        self.update(bytes_amount)

    def close(self):
        if self.bar:
            self.bar.close()

def safe_filesize(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0

def compute_total_bytes(pairs):
    # pairs: list of (local_path, key)
    return sum(safe_filesize(p) for p, _ in pairs)

# -------------------- SSH helpers --------------------
def _load_private_key(path: str):
    try:
        return paramiko.Ed25519Key(filename=path)
    except Exception:
        return paramiko.RSAKey.from_private_key_file(path)

def connect_ssh(target_ip, bastion_ip=None, retries=3, delay=5) -> paramiko.SSHClient:
    for attempt in range(retries):
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if not os.path.exists(KEY_PATH):
                raise FileNotFoundError(f"SSH private key not found at {KEY_PATH}")
            key = _load_private_key(KEY_PATH)
            if bastion_ip:
                bastion = paramiko.SSHClient()
                bastion.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                bastion.connect(hostname=bastion_ip, username=USER, pkey=key, timeout=30)
                transport = bastion.get_transport()
                channel = transport.open_channel("direct-tcpip", (target_ip, 22), ("localhost", 0))
                ssh.connect(target_ip, username=USER, sock=channel, pkey=key, timeout=30)
            else:
                ssh.connect(target_ip, username=USER, pkey=key, timeout=30)
            return ssh
        except Exception as e:
            print(f"[ERROR] SSH connection failed ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception(f"[ERROR] Failed to connect to {target_ip} after {retries} retries.")

def exec_command(ssh: paramiko.SSHClient, command: str, retries=3) -> Tuple[str, str]:
    for attempt in range(retries):
        try:
            print(f"[INFO] Executing: {command}")
            stdin, stdout, stderr = ssh.exec_command(command)
            output = stdout.read().decode(errors="replace").strip()
            error  = stderr.read().decode(errors="replace").strip()
            if output:
                print(f"[OUTPUT] {output}")
            if error:
                print(f"[ERROR] {error}")
            return output, error
        except Exception as e:
            print(f"[ERROR] Command execution failed ({attempt+1}/{retries}): {e}")
            time.sleep(2)
    raise Exception(f"[ERROR] Failed to execute command after {retries} retries.")

def open_sftp(ssh: paramiko.SSHClient):
    for attempt in range(3):
        try:
            return ssh.open_sftp()
        except Exception as e:
            print(f"[ERROR] open_sftp failed ({attempt+1}/3): {e}")
            time.sleep(1)
    raise Exception("[ERROR] open_sftp: giving up")

def sftp_write_text(ssh: paramiko.SSHClient, remote_path: str, content: str):
    sftp = open_sftp(ssh)
    try:
        with sftp.file(remote_path, "w") as f:
            f.write(content)
    finally:
        sftp.close()

def sftp_write_bytes(ssh: paramiko.SSHClient, remote_path: str, data: bytes):
    sftp = open_sftp(ssh)
    try:
        with sftp.file(remote_path, "wb") as f:
            f.write(data)
    finally:
        sftp.close()

def sftp_write_json(ssh: paramiko.SSHClient, remote_path: str, obj):
    sftp_write_bytes(ssh, remote_path, json.dumps(obj).encode())

def ensure_remote_dir(ssh: paramiko.SSHClient, directory: str):
    exec_command(ssh, f"mkdir -p '{directory}'")

# -------------------- Install deps on remote --------------------
def install_boto_on_target(ssh: paramiko.SSHClient):
    check_boto_cmd = "python3 -c 'import boto3' || echo 'missing'"
    stdout, _ = exec_command(ssh, check_boto_cmd)
    if "missing" in stdout:
        exec_command(ssh, "python3 -m pip install --quiet boto3 tqdm")

# -------------------- Retryable local upload with byte callback --------------------
def _sleep_backoff(attempt: int):
    delay = min(BACKOFF_MAX_SECS, BACKOFF_BASE_SECS * (2 ** attempt))
    delay = delay * (0.5 + random.random())  # jitter
    time.sleep(delay)

def upload_file_with_retry(local_path: str, key: str, progress: Optional[GlobalByteProgress]):
    last_err = None
    for attempt in range(UPLOAD_RETRIES):
        try:
            s3_client.upload_file(local_path, MINIO_BUCKET, key, Config=transfer_config,
                                  Callback=(progress.callback if progress else None))
            return True
        except Exception as e:
            last_err = e
            print(f"[WARN] Upload failed (attempt {attempt+1}/{UPLOAD_RETRIES}): {local_path} -> {key}: {e}")
            if attempt < UPLOAD_RETRIES - 1:
                _sleep_backoff(attempt)
    print(f"[ERROR] Giving up after {UPLOAD_RETRIES} attempts: {local_path} -> {key}: {last_err}")
    return False

def parallel_upload_filelist(pairs, max_workers=MAX_PARALLEL_UPLOADS, label="uploads"):
    if not pairs:
        return
    total_bytes = compute_total_bytes(pairs)
    progress = GlobalByteProgress(total_bytes, desc=f"{label} (bytes)")
    print(f"[INFO] Starting parallel {label}: {len(pairs)} files, {total_bytes} bytes,"
          f" max_workers={max_workers}, per_file_threads={PER_FILE_THREADS}")
    succeeded = failed = 0
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(upload_file_with_retry, src, key, progress) for (src, key) in pairs]
            for f in as_completed(futs):
                ok = f.result()
                succeeded += 1 if ok else 0
                failed    += 0 if ok else 1
    finally:
        progress.close()
    print(f"[INFO] Completed {label}. OK={succeeded}, Failed={failed}")

# -------------------- Remote uploader (standalone; reads pairs from JSON path) --------------------
REMOTE_UPLOADER_SCRIPT = r"""#!/usr/bin/env python3
import os, sys, json, time, random, threading, boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from boto3.s3.transfer import TransferConfig
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

if len(sys.argv) < 6:
    print("Usage: upload_parallel_minio.py <endpoint> <bucket> <ak> <sk> <pairs_json_path> [max_workers] [per_file_threads] [retries] [backoff_base] [backoff_max]")
    sys.exit(2)

endpoint = sys.argv[1]
bucket   = sys.argv[2]
ak       = sys.argv[3]
sk       = sys.argv[4]
pairs_path = sys.argv[5]

# optional knobs
max_workers      = int(sys.argv[6]) if len(sys.argv) > 6 else 100
per_file_threads = int(sys.argv[7]) if len(sys.argv) > 7 else 8
retries          = int(sys.argv[8]) if len(sys.argv) > 8 else 5
backoff_base     = float(sys.argv[9]) if len(sys.argv) > 9 else 1.0
backoff_max      = float(sys.argv[10]) if len(sys.argv) > 10 else 30.0

with open(pairs_path, "r") as f:
    pairs = json.load(f)

s3  = boto3.client('s3', endpoint_url=endpoint, aws_access_key_id=ak, aws_secret_access_key=sk)
cfg = TransferConfig(max_concurrency=per_file_threads, multipart_threshold=8*1024*1024, multipart_chunksize=8*1024*1024, use_threads=True)

def as_src_key(item):
    if isinstance(item, (list, tuple)) and len(item) >= 2:
        return item[0], item[1]
    if isinstance(item, dict):
        src = item.get("src") or item.get("path") or item.get("file") or item.get("source")
        key = item.get("key") or item.get("dest") or item.get("destination")
        if src and key:
            return src, key
    raise ValueError(f"Bad pair entry (expected [src,key] or {{'src':..,'key':..}}): {{item!r}}")

def safe_size(p):
    try:
        return os.path.getsize(p)
    except Exception:
        return 0

try:
    norm_pairs = [as_src_key(x) for x in pairs]
except Exception as e:
    print(f"[ERROR] Failed to normalize pairs: {{e}}")
    sys.exit(2)

total_bytes = sum(safe_size(p[0]) for p in norm_pairs)

class GlobalByteProgress:
    def __init__(self, total, desc):
        self.total = int(total)
        self.lock = threading.Lock()
        if tqdm and self.total > 0:
            self.bar = tqdm(total=self.total, unit="B", unit_scale=True, unit_divisor=1024, desc=desc)
        else:
            self.bar = None
            print(f"[INFO] {{desc}}: total {{self.total}} bytes (tqdm not installed)")
    def update(self, n):
        if not self.bar:
            return
        with self.lock:
            self.bar.update(n)
    def callback(self, n):
        self.update(n)
    def close(self):
        if self.bar:
            self.bar.close()

def sleep_backoff(attempt):
    delay = min(backoff_max, backoff_base * (2 ** attempt))
    delay = delay * (0.5 + random.random())
    time.sleep(delay)

def up_one(src, key, progress):
    if not os.path.exists(src):
        return False, f"[ERROR] Missing: {{src}}"
    last_err = None
    for attempt in range(retries):
        try:
            s3.upload_file(src, bucket, key, Config=cfg, Callback=(progress.callback if progress else None))
            return True, f"[SUCCESS] {{src}} -> {{key}}"
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                sleep_backoff(attempt)
    return False, f"[ERROR] {{src}} -> {{key}}: {{last_err}}"

if not norm_pairs:
    print("[INFO] No files to upload.")
    sys.exit(0)

progress = GlobalByteProgress(total_bytes, desc="remote uploads (bytes)")
ok = fail = 0
try:
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(up_one, p[0], p[1], progress) for p in norm_pairs]
        for f in as_completed(futs):
            success, msg = f.result()
            print(msg)
            ok  += 1 if success else 0
            fail+= 0 if success else 1
finally:
    progress.close()

print(f"[INFO] Remote parallel upload completed. OK={{ok}}, Failed={{fail}}")
sys.exit(0 if fail == 0 else 1)
"""

def deploy_remote_uploader(ssh: paramiko.SSHClient):
    ensure_remote_dir(ssh, REMOTE_TMP)
    script_path = f"{REMOTE_TMP}/upload_parallel_minio.py"
    # upload the script via SFTP
    sftp_write_text(ssh, script_path, REMOTE_UPLOADER_SCRIPT)
    exec_command(ssh, f"chmod +x {script_path}")
    return script_path

def run_remote_uploader(ssh: paramiko.SSHClient, pairs, node: str):
    """Upload pairs.json via SFTP and invoke the remote script with arguments."""
    install_boto_on_target(ssh)
    ensure_remote_dir(ssh, REMOTE_TMP)
    script_path = deploy_remote_uploader(ssh)

    # write JSON with pairs
    pairs_json_path = f"{REMOTE_TMP}/pairs.json"
    sftp_write_json(ssh, pairs_json_path, pairs)

    # build command with args (avoid heredocs)
    cmd = (
        f"python3 {script_path} "
        f"'{MINIO_ENDPOINT}' '{MINIO_BUCKET}' '{MINIO_ACCESS_KEY}' '{MINIO_SECRET_KEY}' "
        f"'{pairs_json_path}' {MAX_PARALLEL_UPLOADS} {PER_FILE_THREADS} "
        f"{UPLOAD_RETRIES} {BACKOFF_BASE_SECS} {BACKOFF_MAX_SECS}; echo __EC:$?"
    )
    out, _ = exec_command(ssh, cmd)
    ec_lines = []
    for line in out.splitlines():
        if line.startswith("__EC:"):
            ec_lines.append(line)

    ec = int(ec_lines[-1].split(":")[1]) if ec_lines else 0
    if ec != 0:
        raise RuntimeError(f"Remote uploader exited with code {ec} on node {node}")

# -------------------- Build remote upload pairs --------------------
def upload_from_remote(ssh: paramiko.SSHClient, node: str, node_type: str):
    print(f"[INFO] Uploading logs from {node} to MinIO (parallel, retry, byte-progress)…")

    upload_pairs = []

    # 1) container-logs tar if present
    container_logs_path = f"{HOME_DIR}/container-logs/"
    stdout, _ = exec_command(ssh, f"ls -d {container_logs_path} 2>/dev/null || echo 'missing'")
    if "missing" in stdout:
        print(f"[WARNING] {container_logs_path} does not exist on {node}. Skipping container logs…")
    else:
        tar_path = f"{HOME_DIR}/container-logs.tar.gz"
        print(f"[INFO] Zipping {container_logs_path} on {node}…")
        exec_command(ssh, f"tar -czf {tar_path} -C {HOME_DIR} container-logs")
        file_key = f"{UPLOAD_FOLDER}/{node}-{node_type}/container-logs.tar.gz"
        upload_pairs.append([tar_path, file_key])

    # 2) other logs
    candidate_globs = [
        f"{HOME_DIR}/*.txt*",
        f"{HOME_DIR}/*.log",
        f"{HOME_DIR}/*.state",
        f"{HOME_DIR}/*.json",
        f"{HOME_DIR}/*fio_iolog*",
        "/etc/simplyblock/*",
        "/var/simplyblock/*"
    ]
    for remote_path in candidate_globs:
        stdout, _ = exec_command(ssh, f"ls -1 {remote_path} 2>/dev/null || true")
        files = [x for x in stdout.split("\n") if x.strip()]
        for remote_file in files:
            subfolder = "dump" if (remote_file.startswith("/etc/simplyblock/") or remote_file.startswith("/var/simplyblock/")) else "root-logs"
            key = f"{UPLOAD_FOLDER}/{node}-{node_type}/{subfolder}/{os.path.basename(remote_file)}"
            upload_pairs.append([remote_file, key])

    if upload_pairs:
        print(f"[INFO] Scheduling {len(upload_pairs)} files for remote parallel upload on {node}…")
        run_remote_uploader(ssh, upload_pairs, node)
    else:
        print(f"[INFO] Nothing to upload from {node}.")

# -------------------- Kubernetes logs (runner) --------------------
def upload_k8s_logs():
    print("[INFO] Fetching Kubernetes logs from runner node…")
    local_k8s_log_dir = "/tmp/k8s_logs"
    os.makedirs(local_k8s_log_dir, exist_ok=True)

    namespace = "simplyblk"
    print(f"[INFO] Processing namespace: {namespace}")

    try:
        pods = subprocess.run(
            f"kubectl get pods -n {namespace} --no-headers -o custom-columns=:metadata.name",
            shell=True, check=True, capture_output=True, text=True
        ).stdout.splitlines()
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] kubectl get pods failed: {e}")
        pods = []

    if not pods:
        print(f"[WARNING] No pods found in namespace {namespace}.")

    for pod in pods:
        try:
            containers = subprocess.run(
                f"kubectl get pod {pod} -n {namespace} -o jsonpath='{{{{.spec.containers[*].name}}}}'",
                shell=True, check=True, capture_output=True, text=True
            ).stdout.split()
        except subprocess.CalledProcessError:
            containers = []

        if not containers:
            print(f"[WARNING] No containers found in pod {pod} (ns: {namespace}).")
            continue

        for container in containers:
            log_file = f"{local_k8s_log_dir}/{namespace}_{pod}_{container}.log"
            print(f"[INFO] Fetching logs for Pod={pod}, Container={container}, ns={namespace}…")
            subprocess.run(
                f"kubectl logs {pod} -n {namespace} -c {container} --timestamps > {log_file}",
                shell=True, check=False
            )

    pairs = []
    for fname in os.listdir(local_k8s_log_dir):
        file_path = os.path.join(local_k8s_log_dir, fname)
        key = f"{UPLOAD_FOLDER}/runner-node/k8s_logs/{fname}"
        pairs.append((file_path, key))
    parallel_upload_filelist(pairs, label="k8s log uploads")
    print("[SUCCESS] Kubernetes logs uploaded successfully.")

# -------------------- Local logs (runner) --------------------
def upload_local_logs(k8s=False):
    logs_dir = os.path.join(os.getcwd(), "logs")
    pairs = []

    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. Skipping local log upload.")
    else:
        print(f"[INFO] Uploading local logs from {logs_dir} to MinIO (parallel + byte-progress)…")
        for file in os.listdir(logs_dir):
            local_file_path = os.path.join(logs_dir, file)
            if os.path.isdir(local_file_path):
                continue
            key = f"{UPLOAD_FOLDER}/runner-node/logs/{file}"
            pairs.append((local_file_path, key))

    if pairs:
        parallel_upload_filelist(pairs, label="local runner logs")

    if k8s:
        home_dir = os.path.expanduser("~")
        container_logs_dir = os.path.join(home_dir, "container-logs")
        tar_file_path = os.path.join(home_dir, "container-logs.tar.gz")

        if os.path.exists(container_logs_dir):
            print(f"[INFO] Creating tar archive of {container_logs_dir}…")
            result = subprocess.run(
                f"tar -czf {tar_file_path} -C {home_dir} container-logs",
                shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                print(f"[SUCCESS] Created tar file: {tar_file_path}")
                key = f"{UPLOAD_FOLDER}/runner-node/container-logs.tar.gz"
                parallel_upload_filelist([(tar_file_path, key)], label="runner container-logs tar")
                try:
                    os.remove(tar_file_path)
                    print(f"[INFO] Removed local tar file: {tar_file_path}")
                except Exception as e:
                    print(f"[WARNING] Failed to remove tar: {e}")
            else:
                print(f"[ERROR] Failed to create tar archive: {result.stderr}")
        else:
            print(f"[WARNING] {container_logs_dir} does not exist. Skipping tar creation.")

# -------------------- Cleanup helpers (optional use) --------------------
def cleanup_remote_logs(ssh, node):
    print(f"[INFO] Cleaning up logs on {node}…")
    cleanup_commands = [
        f"rm -rf {HOME_DIR}/container-logs.tar.gz",
        f"rm -rf {HOME_DIR}/container-logs/*",
        f"rm -rf {HOME_DIR}/*.txt {HOME_DIR}/*.log {HOME_DIR}/*.state {HOME_DIR}/*fio_iolog*",
        "rm -rf /etc/simplyblock/[0-9]*",
        "rm -rf /etc/simplyblock/*core*.zst",
        "rm -rf /etc/simplyblock/LVS*",
        f"rm -rf {REMOTE_TMP}"
    ]
    for cmd in cleanup_commands:
        exec_command(ssh, cmd)
    print(f"[SUCCESS] Cleaned up logs on {node}.")

def cleanup_local_logs():
    logs_dir = os.path.join(os.getcwd(), "logs")
    if not os.path.exists(logs_dir):
        print(f"[WARNING] {logs_dir} does not exist. No cleanup needed.")
        return
    print(f"[INFO] Cleaning up local logs from {logs_dir}…")
    subprocess.run(f"rm -rf {logs_dir}/*.log", shell=True, check=False)
    subprocess.run(f"rm -rf {logs_dir}/*.txt", shell=True, check=False)
    print("[SUCCESS] Local logs cleaned up.")

# ==================== MAIN FLOW ====================
# Step 1: Management nodes
for node in MNODES:
    try:
        ssh = connect_ssh(node, bastion_ip=BASTION_IP)
        print(f"[INFO] Processing Management Node {node}…")

        stdout, _ = exec_command(ssh, "sudo docker ps -aq || true")
        container_ids = [x for x in stdout.strip().split("\n") if x.strip()]
        for container_id in container_ids:
            stdout, _ = exec_command(ssh, f'sudo docker inspect --format="{{{{.Name}}}}" {container_id} || true')
            container_name = (stdout.strip().replace("/", "") or "container")
            log_file = f"{HOME_DIR}/{container_name}_{container_id}_{node}.txt"
            exec_command(ssh, f"sudo docker logs {container_id} &> {log_file} || true")

        upload_from_remote(ssh, node, node_type="mgmt")
        ssh.close()
        print(f"[SUCCESS] Successfully processed Management Node {node}")
    except Exception as e:
        print(f"[ERROR] Error processing Management Node {node}: {e}")

# Step 2: Storage nodes
for node in STORAGE_PRIVATE_IPS:
    try:
        ssh = connect_ssh(node, bastion_ip=BASTION_IP)
        print(f"[INFO] Processing Storage Node {node}…")

        stdout, _ = exec_command(ssh, "sudo docker ps -aq || true")
        container_ids = [x for x in stdout.strip().split("\n") if x.strip()]
        for container_id in container_ids:
            stdout, _ = exec_command(ssh, f'sudo docker inspect --format="{{{{.Name}}}}" {container_id} || true')
            container_name = (stdout.strip().replace("/", "") or "container")
            log_file = f"{HOME_DIR}/{container_name}_{container_id}_{node}.txt"
            exec_command(ssh, f"sudo docker logs {container_id} &> {log_file} || true")

        upload_from_remote(ssh, node, node_type="storage")
        ssh.close()
        print(f"[SUCCESS] Successfully processed Storage Node {node}")
    except Exception as e:
        print(f"[ERROR] Error processing Storage Node {node}: {e}")

# Step 2.5: Client nodes (optional)
if not args.no_client:
    for node in CLIENTNODES:
        try:
            ssh = connect_ssh(node, bastion_ip=BASTION_IP)
            print(f"[INFO] Processing Client Node {node}…")
            upload_from_remote(ssh, node, node_type="client")
            ssh.close()
            print(f"[SUCCESS] Successfully processed Client Node {node}")
        except Exception as e:
            print(f"[ERROR] Error processing Client Node {node}: {e}")
else:
    print("!! Skipping Clients as --no_client flag is set !!")

# Step 3: K8s logs (runner)
if args.k8s:
    upload_k8s_logs()

# Step 4: Local logs (runner)
if args.k8s:
    upload_local_logs(k8s=True)
else:
    upload_local_logs()

# (Optional) Cleanup
# for node in MNODES + STORAGE_PRIVATE_IPS + ([] if args.no_client else CLIENTNODES):
#     ssh = connect_ssh(node, bastion_ip=BASTION_IP)
#     cleanup_remote_logs(ssh, node)
#     ssh.close()
# cleanup_local_logs()
