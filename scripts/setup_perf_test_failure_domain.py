import argparse
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import paramiko
import time
import re
import json
import select

# --- INPUT PARAMETERS ---
AMI_ID = "ami-0dfc569a8686b9320"  # Rocky 9 us-east-1
KEY_NAME = "mtes01"
KEY_PATH = os.path.expanduser("~/.ssh/mtes01.pem")
AZ = "us-east-1a"
SG_NAME = "default"
# The failure-domain feature (cluster --enable-failure-domain + per-node
# --failure-domain) lives on this branch. Installing from it also pulls the
# matching SPDK ultra image pinned in simplyblock_core/env_var.
BRANCH = "main"
MAX_LVOL = "100"
# --- Manual Network Config ---
# Replace this with your actual Subnet ID (e.g., "subnet-0593459d6b931ee4c")
SUBNET_ID = "subnet-0593459d6b931ee4c"
STORAGE_SG_ID = "sg-02e89a1372e9f39e9"
SN_TYPE = "i3en.2xlarge"
# Failure-domain layout selectors:
#   --fd N                : number of failure domains (default 2)
#   --nodes-per-domain M  : storage nodes per domain (default 16)
#   --ndcs / --npcs       : data / parity chunks per stripe (default 1+1 —
#                           mirroring across the two domains)
# Default deployment: 32 nodes = 2 failure domains x 16 nodes, 1+1.
# The previous small layouts remain reachable, e.g.:
#   --nodes-per-domain 3 --ndcs 2 --npcs 2          (6 nodes, 2 domains, 2+2)
#   --fd 4 --nodes-per-domain 2 --ndcs 2 --npcs 2   (8 nodes, 4 domains, 2+2)
_parser = argparse.ArgumentParser(
    description="Deploy a perf-test cluster with failure domains "
                "(default: 32 nodes, 2 domains x 16, 1+1).",
)
_parser.add_argument(
    "--fd", type=int, default=2,
    help="Number of failure domains. Default: 2.",
)
_parser.add_argument(
    "--nodes-per-domain", type=int, default=16,
    help="Storage nodes per failure domain. Default: 16 (32 nodes with --fd 2).",
)
_parser.add_argument(
    "--ndcs", type=int, default=1,
    help="--data-chunks-per-stripe for cluster create. Default: 1.",
)
_parser.add_argument(
    "--npcs", type=int, default=1,
    help="--parity-chunks-per-stripe for cluster create. Default: 1.",
)
_args = _parser.parse_args()
NUM_FAILURE_DOMAINS = _args.fd
NODES_PER_DOMAIN = _args.nodes_per_domain
NDCS = _args.ndcs
NPCS = _args.npcs
MGMT_TYPE = "m6i.2xlarge"
# --- Selectable Client Specification ---
CLIENT_COUNT = 1            # How many separate EC2 instances to launch
CLIENT_TYPE = "m6in.8xlarge"

# --- Failure-domain layout ---
# One failure-domain id per storage node, indexed by launch order: the first
# NODES_PER_DOMAIN nodes go to domain 0, the next block to domain 1, etc.
FAILURE_DOMAINS = [
    domain
    for domain in range(NUM_FAILURE_DOMAINS)
    for _ in range(NODES_PER_DOMAIN)
]
SN_COUNT = len(FAILURE_DOMAINS)

ec2 = boto3.resource('ec2', region_name='us-east-1')

USER = "ec2-user"
AZ = "us-east-1a"
IFACE = "eth0"
MAX_LVOL = "100"

VOLUME_PLAN = [
    {"idx": 0, "node_idx": 0, "qty": 5, "size": "100G", "client": "client1", "io_queues": 12},
    {"idx": 1, "node_idx": 1, "qty": 5, "size": "100G", "client": "client2", "io_queues": 12},
]


# --- Helper: Management Node with 30GB Root ---
def launch_mgmt():
    print("Launching Management Node with 30GB Root Volume...")
    return ec2.create_instances(
        KeyName=KEY_NAME,
        MinCount=1,
        MaxCount=1,
        ImageId=AMI_ID,
        InstanceType=MGMT_TYPE,
        Placement={'AvailabilityZone': AZ},
        BlockDeviceMappings=[{
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'VolumeSize': 30,
                'DeleteOnTermination': True,
                'VolumeType': 'gp3'
            }
        }]
    )

def wait_for_ssh(ip, timeout=300):
    print(f"--> Attempting SSH handshake on {ip}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            # allow_agent=False is critical to avoid local Zenbook SSH interference
            ssh.connect(ip, username="ec2-user", key_filename=KEY_PATH,
                        timeout=5, banner_timeout=10,
                        allow_agent=False, look_for_keys=False)
            ssh.close()
            print(f"SUCCESS: {ip} is ready.")
            return True
        except Exception:
            # We don't print the error every time to keep the console clean
            pass
        time.sleep(2)
    print(f"FAILURE: Timed out on {ip}")
    return False


def _ssh_connect_with_retry(ip, retries=15, delay=10, timeout=15):
    """Open an SSH connection, retrying transient connect failures.

    With 32 storage nodes brought up and SSH'd in parallel, a single slow
    boot or a transient TCP timeout ([Errno 110]) to ONE node must not abort
    the whole deployment. wait_for_ssh() runs earlier but a fresh connect can
    still time out under the parallel load, so every ssh_exec connect is
    retried here with backoff before giving up.
    """
    last_exc = None
    for attempt in range(retries):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(ip, username='ec2-user', key_filename=KEY_PATH,
                        timeout=timeout, banner_timeout=30, auth_timeout=30,
                        allow_agent=False, look_for_keys=False)
            return ssh
        except Exception as e:
            last_exc = e
            try:
                ssh.close()
            except Exception:
                pass
            if attempt < retries - 1:
                print(f"  [{ip}] connect failed ({type(e).__name__}), "
                      f"retry {attempt + 2}/{retries} in {delay}s")
                time.sleep(delay)
    raise RuntimeError(f"SSH connect to {ip} failed after {retries} attempts: {last_exc}")


def ssh_exec(ip, cmds, get_output=False, check=False, cmd_timeout=600,
             reconnect_retries=3):
    # cmd_timeout bounds each remote command. 600s suits quick ops, but the
    # long 32-node operations (sn deploy = concurrent docker image pull +
    # SPDK bring-up, cluster activate = full mesh/hublvol wiring) need much
    # more; callers pass a larger value for those.
    #
    # Like the connect-time retries in _ssh_connect_with_retry, an
    # established transport can also die mid-session under 30+ parallel
    # long-lived connections (e.g. [WinError 10054] reset while opening a
    # channel) and must not abort the whole deployment. On transport death
    # the current command is re-run on a fresh connection, so every command
    # routed through here has to stay idempotent. reconnect_retries=0
    # disables this for commands where re-running is wrong (e.g. reboot).
    ssh = _ssh_connect_with_retry(ip)
    results = []
    for cmd in cmds:
        for attempt in range(reconnect_retries + 1):
            print(f"  [{ip}] $ {cmd}")
            try:
                stdin, stdout, stderr = ssh.exec_command(cmd, timeout=cmd_timeout)
                out = stdout.read().decode('utf-8')
                err = stderr.read().decode('utf-8')
                rc = stdout.channel.recv_exit_status()
                break
            except (paramiko.SSHException, EOFError, OSError) as e:
                if isinstance(e, TimeoutError):
                    # Command exceeded cmd_timeout; re-running won't help.
                    ssh.close()
                    raise
                try:
                    ssh.close()
                except Exception:
                    pass
                if attempt >= reconnect_retries:
                    raise RuntimeError(
                        f"SSH transport to {ip} died {reconnect_retries + 1} "
                        f"times running: {cmd}: {e}") from e
                print(f"  [{ip}] connection died mid-command "
                      f"({type(e).__name__}: {e}), reconnecting and re-running "
                      f"(retry {attempt + 2}/{reconnect_retries + 1})")
                time.sleep(10)
                ssh = _ssh_connect_with_retry(ip)
        if get_output:
            results.append(out)
        if rc != 0:
            print(f"  [{ip}] FAILED (rc={rc}): {cmd}")
            if out.strip():
                print(f"    --- stdout ({len(out.splitlines())} lines) ---")
                for line in out.rstrip().split('\n'):
                    print(f"    stdout: {line}")
            if err.strip():
                print(f"    --- stderr ({len(err.splitlines())} lines) ---")
                for line in err.rstrip().split('\n'):
                    print(f"    stderr: {line}")
            if check:
                ssh.close()
                raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
        else:
            # Show last 2 lines of output on success
            lines = out.strip().split('\n')
            for line in lines[-2:]:
                if line.strip():
                    print(f"    {line}")
    ssh.close()
    return results


def ssh_exec_stream(ip, cmd, check=False, cmd_timeout=600):
    ssh = _ssh_connect_with_retry(ip)
    print(f"  [{ip}] $ {cmd}")

    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=cmd_timeout)
    channel = stdout.channel
    out_chunks = []
    err_chunks = []

    while True:
        read_list = []
        if channel.recv_ready():
            read_list.append(channel)
        if channel.recv_stderr_ready():
            read_list.append(channel)

        if read_list:
            select.select(read_list, [], [], 0.1)

        while channel.recv_ready():
            chunk = channel.recv(4096).decode('utf-8', errors='replace')
            out_chunks.append(chunk)
            print(chunk, end='')

        while channel.recv_stderr_ready():
            chunk = channel.recv_stderr(4096).decode('utf-8', errors='replace')
            err_chunks.append(chunk)
            print(chunk, end='')

        if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
            break

        time.sleep(0.1)

    rc = channel.recv_exit_status()
    ssh.close()

    out = ''.join(out_chunks)
    err = ''.join(err_chunks)
    if rc != 0 and check:
        raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    return out, err


def get_sn_uuids(mgmt_ip):
    print("Fetching Storage Node UUIDs...")
    # Get the raw table output
    node_list_raw = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d sn list"], get_output=True)[0]

    uuids = []
    for line in node_list_raw.splitlines():
        # Look for lines that start with '|' and have a UUID-like string in the first cell
        # We strip whitespace and split by '|'
        parts = [p.strip() for p in line.split('|')]

        # parts[0] is empty (before the first |), parts[1] is the UUID column
        if len(parts) > 1:
            potential_uuid = parts[1]
            # Match standard UUID pattern: 8-4-4-4-12 hex chars
            if re.match(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', potential_uuid):
                uuids.append(potential_uuid)

    if not uuids:
        print("DEBUG: Raw table received:\n", node_list_raw)
        raise Exception("Failed to parse Node UUIDs from table.")

    return uuids


def fetch_cluster_topology(mgmt_ip, cluster_uuid):
    script = f"""sudo python3 - <<'PY'
import json
from simplyblock_core.db_controller import DBController
from simplyblock_core.models.storage_node import StorageNode


def normalize_ref(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list) and value:
        first = value[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            for key in ("node_id", "uuid", "id"):
                if first.get(key):
                    return first[key]
    if isinstance(value, dict):
        for key in ("node_id", "uuid", "id"):
            if value.get(key):
                return value[key]
    return ""


db = DBController()
cluster = db.get_cluster_by_id({cluster_uuid!r})
nodes = db.get_storage_nodes_by_cluster_id({cluster_uuid!r}) or []
by_id = {{node.get_id(): node for node in nodes}}

node_items = []
lvstores = {{}}

for node in nodes:
    sec_ref = normalize_ref(
        getattr(node, "lvstore_stack_secondary", "")
        or getattr(node, "lvstore_stack_secondary_1", "")
    )
    tert_ref = normalize_ref(
        getattr(node, "lvstore_stack_tertiary", "")
        or getattr(node, "lvstore_stack_secondary_2", "")
    )

    node_lvs = []
    if getattr(node, "lvstore", ""):
        node_lvs.append({{"name": node.lvstore, "role": "primary"}})
    if sec_ref and sec_ref in by_id and getattr(by_id[sec_ref], "lvstore", ""):
        node_lvs.append({{"name": by_id[sec_ref].lvstore, "role": "secondary"}})
    if tert_ref and tert_ref in by_id and getattr(by_id[tert_ref], "lvstore", ""):
        node_lvs.append({{"name": by_id[tert_ref].lvstore, "role": "tertiary"}})

    node_items.append(
        {{
            "uuid": node.get_id(),
            "hostname": getattr(node, "hostname", ""),
            "management_ip": getattr(node, "mgmt_ip", ""),
            "failure_domain": getattr(node, "failure_domain", -1),
            "lvs": node_lvs,
            "lvs_display": [f"{{item['name']}} ({{item['role']}})" for item in node_lvs],
        }}
    )

    lvs_name = getattr(node, "lvstore", "")
    if not lvs_name:
        continue

    hublvol = getattr(node, "hublvol", None)
    hublvol_nqn = getattr(hublvol, "nqn", "") or StorageNode.hublvol_nqn_for_lvstore(
        cluster.nqn, lvs_name
    )
    lvstores[lvs_name] = {{
        "hublvol_nqn": hublvol_nqn,
        "client_port": node.get_lvol_subsys_port(lvs_name),
        "hublvol_port": node.get_hublvol_port(lvs_name),
    }}

result = {{
    "cluster_uuid": cluster.uuid,
    "cluster_nqn": cluster.nqn,
    "enable_failure_domain": getattr(cluster, "enable_failure_domain", False),
    "nodes": node_items,
    "lvstores": dict(sorted(lvstores.items())),
}}
print(json.dumps(result, indent=2))
PY"""
    output = ssh_exec(mgmt_ip, [script], get_output=True, check=True)[0]
    return json.loads(output)


def create_aws_clients(count, instance_type):
    session = boto3.Session()
    ec2_res = session.resource('ec2')
    session.client('ec2')

    print(f"  Targeting Subnet: {SUBNET_ID}")
    # Launch the instances
    print(f"  Launching {count} {instance_type} instances...")
    instances = ec2_res.create_instances(
        ImageId=AMI_ID,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=KEY_NAME,

        NetworkInterfaces=[{
            'DeviceIndex': 0,
            'SubnetId': SUBNET_ID,
            'Groups': [STORAGE_SG_ID],
            'AssociatePublicIpAddress': True
        }],
        TagSpecifications=[{
            'ResourceType': 'instance',
            'Tags': [{'Key': 'Name', 'Value': 'SB-Client'}]
        }]
    )
    return instances

def deploy_storage_nodes(count=SN_COUNT, instance_type=SN_TYPE):
    # ... session setup ...

    print(f"Deploying {count} Storage Nodes into subnet: {SUBNET_ID}")

    instances = ec2.create_instances(
        ImageId=AMI_ID,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=KEY_NAME,
        # This is where the subnet is manually specified:
        NetworkInterfaces=[{
            'DeviceIndex': 0,
            'SubnetId': SUBNET_ID,
            'Groups': [STORAGE_SG_ID],
            'AssociatePublicIpAddress': True  # Set to False if you want internal-only nodes
        }],
        BlockDeviceMappings=[{
            'DeviceName': '/dev/sda1',
            'Ebs': {
                'VolumeSize': 30,
                'DeleteOnTermination': True,
                'VolumeType': 'gp3'
            }
        }],
        TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': 'SB-Storage-Node'}]}]
    )
    return instances


class PersistentSSH:
    def __init__(self, ip, retries=10, delay=5):
        self.ip = ip
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        for i in range(retries):
            try:
                # Use absolute path for the key
                full_key_path = os.path.expanduser(KEY_PATH)
                self.client.connect(
                    hostname=self.ip,
                    username=USER,
                    key_filename=full_key_path,
                    timeout=10,
                    allow_agent=False,
                    look_for_keys=False
                )
                return  # Success
            except Exception as e:
                print(f"    [SSH] {self.ip} not ready (Attempt {i + 1}/{retries}): {e}")
                time.sleep(delay)
        raise Exception(f"Failed to connect to {self.ip} after {retries} retries.")

    def close(self):
        self.client.close()




class _TeeStdout:
    """Mirror stdout to a log file so a failed run leaves forensics behind
    (which node's [ip] line preceded the traceback, timings, command output).
    Thread-safe because ssh_exec prints from ThreadPoolExecutor workers."""

    def __init__(self, path):
        self._file = open(path, "a", buffering=1, encoding="utf-8")
        self._stdout = sys.stdout
        self._lock = threading.Lock()

    def write(self, data):
        with self._lock:
            self._stdout.write(data)
            self._file.write(data)

    def flush(self):
        self._stdout.flush()
        self._file.flush()


def main():
    log_path = f"setup_perf_fd_{datetime.now():%Y%m%d_%H%M%S}.log"
    sys.stdout = sys.stderr = _TeeStdout(log_path)
    print(f"Logging to {log_path}")

    # Launch Mgmt Node
    print("Launching Management Node...")
    mgmt_instances = launch_mgmt()  # Assumed to return a list [obj]

    # Launch Storage Nodes
    print("Launching Storage Nodes...")
    sns = deploy_storage_nodes(count=SN_COUNT, instance_type=SN_TYPE)  # Assumed to return [obj, obj, obj]

    # Handle Clients (Create or Load)
    client_data = {}
    client_data = create_aws_clients(CLIENT_COUNT, CLIENT_TYPE)
    all_instances = mgmt_instances + sns + client_data

    print(f"Syncing state for {len(all_instances)} nodes...")
    for inst in all_instances:
        inst.wait_until_running()
        inst.reload()  # This ensures .public_ip_address is populated

    mgmt_ip = mgmt_instances[0].public_ip_address
    sn_ips = [inst.public_ip_address for inst in sns]
    sn_priv_ips = [inst.private_ip_address for inst in sns]
    client_pub_ips = [c.public_ip_address for c in client_data]

    all_setup_ips = [mgmt_ip] + sn_ips
    print(f"Waiting for SSH readiness on {len(all_setup_ips)} nodes...")
    for ip in all_setup_ips:
        wait_for_ssh(ip)

    # --- 4. Parallel Setup (Phase 1) ---
    install_cmds = [
        "sudo dnf install git python3-pip nvme-cli -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH} --upgrade --force --ignore-installed requests",
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc"
    ]

    print("Phase 1: Starting Universal Parallel Setup...")
    with ThreadPoolExecutor(max_workers=len(all_setup_ips)) as executor:
        setup_tasks = [executor.submit(ssh_exec, ip, install_cmds, check=True) for ip in all_setup_ips]
        for t in setup_tasks:
            t.result()  # Will raise if any failed
    print("Phase 1: DONE - all nodes have sbcli installed.")

    # --- 5. Cluster Configuration (Phase 2) ---
    # Step 5a: Create cluster on mgmt (sequential, must complete first)
    # --enable-failure-domain turns on rack/cabinet/DC anti-affinity; every
    # storage node must then be added with a --failure-domain <id> tag below.
    print(f"Phase 2a: Creating cluster on management node (failure-domain enabled, "
          f"{NDCS}+{NPCS})...")
    ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl -d cluster create --enable-node-affinity --enable-failure-domain"
        f" --data-chunks-per-stripe {NDCS} --parity-chunks-per-stripe {NPCS}"
    ], check=True)
    print("Phase 2a: DONE - cluster created.")

    # Step 5b: Configure and deploy storage nodes in parallel
    print("Phase 2b: Configuring storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn configure --max-subsys {MAX_LVOL}"
        ], check=True, cmd_timeout=1200) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2b: DONE - all SNs configured.")

    print("Phase 2c: Deploying storage nodes...")
    # 32 nodes pull the SPDK image and bring up SPDK concurrently; on a cold
    # image cache with AWS registry throttling this can take well beyond the
    # 600s default, so allow 30 min per node.
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {IFACE}"
        ], check=True, cmd_timeout=1800) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2c: DONE - all SNs deployed. Rebooting...")

    # Reboot all SNs in parallel (reboot returns non-zero, don't check).
    # reconnect_retries=0: the reboot is EXPECTED to kill the connection;
    # a reconnect-retry would reboot the node a second time.
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        [executor.submit(ssh_exec, ip, ["sudo reboot"], reconnect_retries=0)
         for ip in sn_ips]

    print("Waiting for SN reboot recovery...")
    time.sleep(30)
    for ip in sn_ips:
        wait_for_ssh(ip)
    print("All storage nodes back online after reboot.")

    # Wait for SNodeAPI (port 5000) to be ready after reboot. With 32 nodes
    # each restarting SPDK + SNodeAPI, 60s was too tight; give them longer so
    # add-node/activate below don't hit a not-yet-ready SNodeAPI.
    print("Waiting 150s for SPDK containers to start...")
    time.sleep(150)

    # --- 6. Cluster Activation & Node Addition ---
    cluster_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d cluster list"], get_output=True)[0]
    cluster_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', cluster_list)
    if not cluster_match:
        raise Exception("Could not find Cluster UUID")
    cluster_uuid = cluster_match.group(1)
    print(f"Cluster UUID: {cluster_uuid}")

    # Map each storage node to its failure domain (by launch order): the first
    # NODES_PER_DOMAIN nodes go to domain 0, the next block to domain 1, etc.
    #
    # add-node runs IN PARALLEL: the control plane supports concurrent
    # sn add-node (mesh wiring is serialized per cluster behind
    # ClusterAddNodeLock and port allocation behind PortReservation), and
    # 32 sequential adds would take the better part of an hour. Worker
    # count is capped at 8 so the parallel ssh sessions stay under mgmt
    # sshd's MaxStartups throttling.
    print("Phase 3: Adding storage nodes to cluster IN PARALLEL "
          "(with --failure-domain tags)...")

    def _add_one_node(idx_priv):
        idx, priv_ip = idx_priv
        fd = FAILURE_DOMAINS[idx]
        print(f"  Node {idx} ({priv_ip}) -> failure domain {fd}")
        for attempt in range(5):
            try:
                # add-node wires the new node into the mesh (serialized behind
                # ClusterAddNodeLock); at 32 nodes a single add can queue on
                # that lock for minutes, so allow 20 min per attempt.
                ssh_exec(mgmt_ip, [
                    f"sudo /usr/local/bin/sbctl -d sn add-node {cluster_uuid} {priv_ip}:5000 {IFACE}"
                    f" --ha-jm-count 4 --failure-domain {fd}"
                ], check=True, cmd_timeout=1200)
                return
            except RuntimeError:
                if attempt < 4:
                    print(f"  Retrying add-node for {priv_ip} in 30s (attempt {attempt+2}/5)...")
                    time.sleep(30)
                else:
                    raise

    with ThreadPoolExecutor(max_workers=min(8, len(sn_priv_ips))) as executor:
        add_tasks = [executor.submit(_add_one_node, item)
                     for item in enumerate(sn_priv_ips)]
        for t in add_tasks:
            t.result()  # raises if any node failed all 5 attempts
    print(f"Phase 3: DONE - {SN_COUNT} nodes added across {NUM_FAILURE_DOMAINS} failure domains.")

    # Verify all nodes are visible
    print("Verifying node status...")
    sn_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d sn list"], get_output=True)[0]
    print(sn_list)
    online_count = sn_list.count("online")
    if online_count < SN_COUNT:
        raise Exception(f"Only {online_count} nodes online, expected {SN_COUNT}")
    print(f"Verified: {online_count} nodes online.")

    print("Phase 4: Activating cluster...")
    time.sleep(10)
    # Activating a 32-node cluster builds every lvstore's distrib stack +
    # hublvol mesh; this is by far the longest single step, so allow 1h.
    ssh_exec_stream(
        mgmt_ip,
        f"sudo /usr/local/bin/sbctl -d cluster activate {cluster_uuid}",
        check=True,
        cmd_timeout=3600,
    )
    print("Phase 4: DONE - cluster activated.")

    print("Creating pool...")
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl -d pool add pool01 {cluster_uuid}"
    ], check=True)
    print("Pool created.")

    # Commands for Performance Clients
    client_prep_cmds = [
        "sudo dnf install nvme-cli fio -y",
        "sudo modprobe nvme-tcp",
        "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf"
    ]


    print("Prepping clients...")
    with ThreadPoolExecutor(max_workers=max(1, len(client_pub_ips))) as executor:
        futures = [executor.submit(ssh_exec, ip, client_prep_cmds, check=True) for ip in client_pub_ips]
        for f in futures:
            f.result()

    # --- 7. Save Comprehensive Metadata ---
    client_metadata = []
    for inst in client_data:
        client_metadata.append({
            "instance_id": inst.id,
            "public_ip": inst.public_ip_address,
            "private_ip": inst.private_ip_address,
            "security_group_id": inst.security_groups[0]['GroupId'] if inst.security_groups else None
        })

    storage_metadata = []
    for i, inst in enumerate(sns):
        storage_metadata.append({
            "instance_id": inst.id,
            "private_ip": inst.private_ip_address,
            "public_ip": inst.public_ip_address,
            "subnet_id": inst.subnet_id,
            "failure_domain": FAILURE_DOMAINS[i],
            "security_group_id": inst.security_groups[0]['GroupId'] if inst.security_groups else None
        })

    topology = fetch_cluster_topology(mgmt_ip, cluster_uuid)

    final_metadata = {
        "mgmt": {
            "instance_id": mgmt_instances[0].id,
            "public_ip": mgmt_ip,
            "private_ip": mgmt_instances[0].private_ip_address,
            "subnet_id": mgmt_instances[0].subnet_id,
            "security_group_id": mgmt_instances[0].security_groups[0]['GroupId'] if mgmt_instances[
                0].security_groups else None
        },
        "storage_nodes": storage_metadata,
        "clients": client_metadata,
        "subnet_id": SUBNET_ID,
        "target_group": STORAGE_SG_ID,
        "cluster_uuid": cluster_uuid,
        "enable_failure_domain": True,
        "failure_domains": FAILURE_DOMAINS,
        "ndcs": NDCS,
        "npcs": NPCS,
        "topology": topology,
        "user": USER,
        "key_path": KEY_PATH
    }

    with open("cluster_metadata_failure_domain.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    print("\n--- Setup Complete ---")
    print(f"Cluster {cluster_uuid} is active with {NUM_FAILURE_DOMAINS} failure domains "
          f"({SN_COUNT} nodes, layout {FAILURE_DOMAINS}). Metadata saved.")




if __name__ == "__main__":
    main()
