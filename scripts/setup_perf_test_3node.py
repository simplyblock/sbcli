import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko
import time
import re
import json
import select

# --- INPUT PARAMETERS ---
# 3 x i3en.12xlarge storage nodes (48 vCPU / 2 NUMA sockets each), single mgmt,
# NO client. Branch: main. FTT 1+1 (ndcs=1, npcs=1). simplyblock deployed on
# NUMA socket 0 only, using 50% of that socket's cores (--cores-percentage is
# per-socket -> 12 of 24 socket-0 cores to SPDK; the rest of socket 0 plus
# socket 1's 24 cores left for the OS/rest).
AMI_ID = "ami-0dfc569a8686b9320"  # Rocky 9 us-east-1
KEY_NAME = "mtes01"
KEY_PATH = os.path.expanduser("~/.ssh/mtes01.pem")
AZ = "us-east-1a"
SG_NAME = "default"
BRANCH = "main"
MAX_LVOL = "100"
# --- Manual Network Config ---
SUBNET_ID = "subnet-0593459d6b931ee4c"
STORAGE_SG_ID = "sg-02e89a1372e9f39e9"
SN_TYPE = "i3en.12xlarge"
SN_COUNT = 3
MGMT_TYPE = "m6i.2xlarge"

# --- simplyblock NUMA / core allocation (per-socket) ---
SOCKETS_TO_USE = "0"      # deploy SPDK on NUMA socket 0 only
NODES_PER_SOCKET = 1
CORES_PERCENTAGE = 50     # % of socket-0 cores for SPDK (CLI range is [0,99))

# --- Fault tolerance: 1+1 (ndcs, npcs) ---
NDCS = 1
NPCS = 1
HA_JM_COUNT = 3           # FTT=1 uses 3 HA journals

ec2 = boto3.resource('ec2', region_name='us-east-1')

USER = "ec2-user"
AZ = "us-east-1a"
IFACE = "eth0"
MAX_LVOL = "100"


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
            pass
        time.sleep(2)
    print(f"FAILURE: Timed out on {ip}")
    return False


def ssh_exec(ip, cmds, get_output=False, check=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username='ec2-user', key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
        out = stdout.read().decode('utf-8')
        err = stderr.read().decode('utf-8')
        rc = stdout.channel.recv_exit_status()
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
            lines = out.strip().split('\n')
            for line in lines[-2:]:
                if line.strip():
                    print(f"    {line}")
    ssh.close()
    return results


def ssh_exec_stream(ip, cmd, check=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username='ec2-user', key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    print(f"  [{ip}] $ {cmd}")

    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
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
    "nodes": node_items,
    "lvstores": dict(sorted(lvstores.items())),
}}
print(json.dumps(result, indent=2))
PY"""
    output = ssh_exec(mgmt_ip, [script], get_output=True, check=True)[0]
    return json.loads(output)


def deploy_storage_nodes(count=SN_COUNT, instance_type=SN_TYPE):
    print(f"Deploying {count} Storage Nodes into subnet: {SUBNET_ID}")
    instances = ec2.create_instances(
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


def main():
    # Launch Mgmt Node (single)
    print("Launching Management Node...")
    mgmt_instances = launch_mgmt()

    # Launch Storage Nodes
    print("Launching Storage Nodes...")
    sns = deploy_storage_nodes(count=SN_COUNT, instance_type=SN_TYPE)

    # No client in this topology.
    all_instances = list(mgmt_instances) + list(sns)

    print(f"Syncing state for {len(all_instances)} nodes...")
    for inst in all_instances:
        inst.wait_until_running()
        inst.reload()

    mgmt_ip = mgmt_instances[0].public_ip_address
    sn_ips = [inst.public_ip_address for inst in sns]
    sn_priv_ips = [inst.private_ip_address for inst in sns]

    all_setup_ips = [mgmt_ip] + sn_ips
    print(f"Waiting for SSH readiness on {len(all_setup_ips)} nodes...")
    for ip in all_setup_ips:
        wait_for_ssh(ip)

    # --- Phase 1: Parallel install ---
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
            t.result()
    print("Phase 1: DONE - all nodes have sbcli installed.")

    # --- Phase 2a: Create cluster (FTT 1+1) ---
    print("Phase 2a: Creating cluster on management node...")
    ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl -d cluster create --enable-node-affinity"
        f" --data-chunks-per-stripe {NDCS} --parity-chunks-per-stripe {NPCS}"
    ], check=True)
    print("Phase 2a: DONE - cluster created.")

    # --- Phase 2b: Configure storage nodes (socket 0 only, 50% of its cores) ---
    print("Phase 2b: Configuring storage nodes...")
    configure_cmd = (
        f"sudo /usr/local/bin/sbctl -d sn configure --max-lvol {MAX_LVOL}"
        f" --sockets-to-use {SOCKETS_TO_USE}"
        f" --nodes-per-socket {NODES_PER_SOCKET}"
        f" --cores-percentage {CORES_PERCENTAGE}"
    )
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [configure_cmd], check=True) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2b: DONE - all SNs configured.")

    print("Phase 2c: Deploying storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {IFACE}"
        ], check=True) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2c: DONE - all SNs deployed. Rebooting...")

    # Reboot all SNs in parallel (reboot returns non-zero, don't check)
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        [executor.submit(ssh_exec, ip, ["sudo reboot"]) for ip in sn_ips]

    print("Waiting for SN reboot recovery...")
    time.sleep(30)
    for ip in sn_ips:
        wait_for_ssh(ip)
    print("All storage nodes back online after reboot.")

    print("Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # --- Phase 3: Add nodes ---
    cluster_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d cluster list"], get_output=True)[0]
    cluster_match = re.search(r'([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})', cluster_list)
    if not cluster_match:
        raise Exception("Could not find Cluster UUID")
    cluster_uuid = cluster_match.group(1)
    print(f"Cluster UUID: {cluster_uuid}")

    print("Phase 3: Adding storage nodes to cluster...")
    for priv_ip in sn_priv_ips:
        for attempt in range(5):
            try:
                ssh_exec(mgmt_ip, [
                    f"sudo /usr/local/bin/sbctl -d sn add-node {cluster_uuid} {priv_ip}:5000 {IFACE} --ha-jm-count {HA_JM_COUNT}"
                ], check=True)
                break
            except RuntimeError:
                if attempt < 4:
                    print(f"  Retrying add-node for {priv_ip} in 30s (attempt {attempt+2}/5)...")
                    time.sleep(30)
                else:
                    raise
    print("Phase 3: DONE - all nodes added.")

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
    ssh_exec_stream(
        mgmt_ip,
        f"sudo /usr/local/bin/sbctl -d cluster activate {cluster_uuid}",
        check=True,
    )
    print("Phase 4: DONE - cluster activated.")

    print("Creating pool...")
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl -d pool add pool01 {cluster_uuid}"
    ], check=True)
    print("Pool created.")

    # --- Save metadata ---
    storage_metadata = []
    for inst in sns:
        storage_metadata.append({
            "instance_id": inst.id,
            "private_ip": inst.private_ip_address,
            "public_ip": inst.public_ip_address,
            "subnet_id": inst.subnet_id,
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
        "clients": [],
        "subnet_id": SUBNET_ID,
        "target_group": STORAGE_SG_ID,
        "cluster_uuid": cluster_uuid,
        "topology": topology,
        "user": USER,
        "key_path": KEY_PATH
    }

    with open("cluster_metadata_3node.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    print("\n--- Setup Complete ---")
    print(f"Cluster {cluster_uuid} is active (3x i3en.12xlarge, FTT 1+1, "
          f"SPDK on socket {SOCKETS_TO_USE} @ {CORES_PERCENTAGE}%). Metadata saved.")


if __name__ == "__main__":
    main()
