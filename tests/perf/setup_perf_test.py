import os
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko
import time
import re
import json

# --- INPUT PARAMETERS ---
AMI_ID = "ami-0dfc569a8686b9320"  # Rocky 9 us-east-1
KEY_NAME = "mtes01"
KEY_PATH = os.path.expanduser("~/.ssh/mtes01.pem")
AZ = "us-east-1a"
SG_NAME = "default"
BRANCH = "feature-lvol-migration"
MAX_LVOL = "50"
# --- Manual Network Config ---
# Replace this with your actual Subnet ID (e.g., "subnet-0593459d6b931ee4c")
SUBNET_ID = "subnet-0593459d6b931ee4c"
STORAGE_SG_ID = "sg-02e89a1372e9f39e9"
SN_TYPE = "i3en.2xlarge"
SN_COUNT = 6
MGMT_TYPE = "m6i.2xlarge"
# --- Selectable Client Specification ---
CLIENT_COUNT = 1            # How many separate EC2 instances to launch
CLIENT_TYPE = "m6in.8xlarge"

ec2 = boto3.resource('ec2', region_name='us-east-1')

USER = "ec2-user"
AZ = "us-east-1a"
IFACE = "eth0"
MAX_LVOL = "10"

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
                for line in out.strip().split('\n')[-5:]:
                    print(f"    stdout: {line}")
            if err.strip():
                for line in err.strip().split('\n')[-5:]:
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


def get_sn_uuids(mgmt_ip):
    print("Fetching Storage Node UUIDs...")
    # Get the raw table output
    node_list_raw = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl sn list"], get_output=True)[0]

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




def main():


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
    print("Phase 2a: Creating cluster on management node...")
    ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl cluster create --enable-node-affinity"
        " --data-chunks-per-stripe 2 --parity-chunks-per-stripe 2"
        " --max-fault-tolerance 2"
    ], check=True)
    print("Phase 2a: DONE - cluster created.")

    # Step 5b: Configure and deploy storage nodes in parallel
    print("Phase 2b: Configuring storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl sn configure --max-lvol {MAX_LVOL}"
        ], check=True) for ip in sn_ips]
        for t in tasks:
            t.result()
    print("Phase 2b: DONE - all SNs configured.")

    print("Phase 2c: Deploying storage nodes...")
    with ThreadPoolExecutor(max_workers=len(sn_ips)) as executor:
        tasks = [executor.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl sn deploy --isolate-cores --ifname {IFACE}"
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

    # Wait for SNodeAPI (port 5000) to be ready after reboot
    print("Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # --- 6. Cluster Activation & Node Addition ---
    cluster_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl cluster list"], get_output=True)[0]
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
                    f"sudo /usr/local/bin/sbctl sn add-node {cluster_uuid} {priv_ip}:5000 {IFACE} --ha-jm-count 4"
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
    sn_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl sn list"], get_output=True)[0]
    print(sn_list)
    online_count = sn_list.count("online")
    if online_count < SN_COUNT:
        raise Exception(f"Only {online_count} nodes online, expected {SN_COUNT}")
    print(f"Verified: {online_count} nodes online.")

    print("Phase 4: Activating cluster...")
    time.sleep(10)
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl cluster activate {cluster_uuid}"
    ], check=True)
    print("Phase 4: DONE - cluster activated.")

    print("Creating pool...")
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl pool add pool01 {cluster_uuid}"
    ], check=True)
    print("Pool created.")

    # Commands for Performance Clients
    client_prep_cmds = [
        "sudo dnf install nvme-cli fio -y",
        "sudo modprobe nvme-tcp",
        "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf"
    ]


    print("Prepping clients...")
    with ThreadPoolExecutor(max_workers=2) as executor:
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
    for inst in sns:
        storage_metadata.append({
            "instance_id": inst.id,
            "private_ip": inst.private_ip_address,
            "public_ip": inst.public_ip_address,
            "subnet_id": inst.subnet_id,
            "security_group_id": inst.security_groups[0]['GroupId'] if inst.security_groups else None
        })

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
        "user": USER,
        "key_path": KEY_PATH
    }

    with open("cluster_metadata.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    print("\n--- Setup Complete ---")
    print(f"Cluster {cluster_uuid} is active. Metadata saved.")




if __name__ == "__main__":
    main()
