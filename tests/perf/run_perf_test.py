import json, re, time, os, statistics, argparse, socket, boto3
import paramiko
from concurrent.futures import ThreadPoolExecutor

import threading

# Global lock for thread-safe log writes
log_lock = threading.Lock()

# --- 1. Configuration & Argument Parsing ---
def parse_args():
    parser = argparse.ArgumentParser(description="SimplyBlock Performance Test Script")
    parser.add_argument('--create-clients', action='store_true', help="Provision new EC2 client instances.")
    return parser.parse_args()


# --- 2. Metadata Loader ---
METADATA_FILE = "cluster_metadata.json"


def load_metadata():
    if not os.path.exists(METADATA_FILE):
        return {}
    with open(METADATA_FILE, "r") as f:
        return json.load(f)



# --- 3. Persistent SSH Class ---
class PersistentSSH:
    def __init__(self, ip, user, key_path):
        self.ip = ip
        self.user = user
        self.key_path = os.path.expanduser(key_path)
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.connect()

    def connect(self):
        for attempt in range(15):
            try:
                self.client.connect(hostname=self.ip, username=self.user, key_filename=self.key_path, timeout=10)
                return
            except:
                time.sleep(5)
        raise Exception(f"Failed to connect to {self.ip} after 15 attempts.")

    def exec(self, cmd, timeout=600):
        stdin, stdout, stderr = self.client.exec_command(cmd, timeout=timeout)
        return stdout.read().decode('utf-8'), stderr.read().decode('utf-8')

    def close(self):
        if self.client: self.client.close()


# --- 4. AWS Client Provisioning ---
def create_aws_clients(count, instance_type, target_subnet, target_group, key_name, ami_id, run_id):
    ec2_res = boto3.resource('ec2')
    print(f"  [AWS] Launching {count} {instance_type} in {target_subnet} with SG {target_group}...")

    instances = ec2_res.create_instances(
        ImageId=ami_id,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=key_name,
        NetworkInterfaces=[{
            'DeviceIndex': 0,
            'SubnetId': target_subnet,
            'Groups': [target_group],
            'AssociatePublicIpAddress': True
        }],
        TagSpecifications=[{'ResourceType': 'instance', 'Tags': [{'Key': 'Name', 'Value': f'SB-Client-{run_id}'}]}]
    )

    client_list = []
    for i, inst in enumerate(instances):
        print(f"    Waiting for client {i + 1} ({inst.id}) to start...")
        inst.wait_until_running()
        inst.reload()
        client_list.append({
            "instance_id": inst.id,
            "public_ip": inst.public_ip_address,
            "private_ip": inst.private_ip_address,
            "security_group_id": target_group
        })
    return client_list

def collect_spdk_logs(storage_nodes, node_numbers, output_file, user, key_path):
    global log_lock
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")

    for node_num in node_numbers:
        if node_num < 1 or node_num > len(storage_nodes):
            continue

        node = storage_nodes[node_num - 1]
        node_ip = node["public_ip"]

        header = f"""
==============================
NODE: {node_num}
IP: {node_ip}
TIMESTAMP: {timestamp}
==============================
"""

        try:
            sn_ssh = PersistentSSH(node_ip, user, key_path)

            cmd = """
for c in $(docker ps --format '{{.Names}}' | grep '^spdk_'); do
    echo "---- CONTAINER: $c ----"
    docker logs --since 10m $c 2>&1
    echo ""
done
"""
            stdout, stderr = sn_ssh.exec(cmd, timeout=600)
            sn_ssh.close()

        except Exception as e:
            stdout = f"ERROR collecting logs from {node_ip}: {str(e)}\n"

        # Thread-safe write
        with log_lock:
            with open(output_file, "a") as f:
                f.write(header)
                f.write(stdout)
                f.write("\n")

def initialize_client_software(ip, user, key_path):
    print(f"  [Init] Installing dependencies on {ip}...")
    ssh = PersistentSSH(ip, user, key_path)
    # Ensure NVMe tools and FIO are present
    cmds = [
        "sudo dnf install nvme-cli fio -y || sudo apt-get install nvme-cli fio -y",
        "sudo modprobe nvme-tcp"
    ]
    for cmd in cmds:
        ssh.exec(cmd)
    ssh.close()

def build_sn_env_exports(sn_uuids):
    exports = []
    for i, uid in enumerate(sn_uuids):
        exports.append(f"export n{i+1}={uid}")
    exports.append(f"export SN_COUNT={len(sn_uuids)}")
    return "\n".join(exports) + "\n"

def run_mgmt_script(mgmt_ssh, script_content, sn_uuids):
    if not script_content:
        return

    env_block = build_sn_env_exports(sn_uuids)

    full_script = f"""
set -e
{env_block}

{script_content}
"""

    stdin, stdout, stderr = mgmt_ssh.client.exec_command("bash")
    stdin.write(full_script)
    stdin.flush()
    stdin.channel.shutdown_write()

    rc = stdout.channel.recv_exit_status()

    if rc != 0:
        print("[!] Mgmt script failed:")
        print(stderr.read().decode())

def extract_fio_essentials(output):
    lines = output.splitlines()
    capture = []
    recording = False

    for line in lines:
        if "1.00th=" in line:
            recording = True
        if recording:
            capture.append(line)
        if "iops" in line:
            break

    return "\n".join(capture)

def run_fio_sequence(client_ssh, mgmt_ssh, client_name, volume_name,
                     mount_point, fio_sequence, output_file,
                     storage_nodes, sn_uuids, log_nodes, user, key_path):
    for job in fio_sequence:

        fio_cmd = job["fio_cmd"].format(mount=mount_point)
        pre = job.get("pre_mgmt")
        post = job.get("post_mgmt")

        fio_cmd = fio_cmd.strip()

        cleanup = False
        cleanup_token = "cleanup;"

        if fio_cmd.startswith(cleanup_token):
            cleanup = True
            fio_cmd = fio_cmd[len(cleanup_token):].lstrip()

        # now:
        # cleanup -> True/False
        # fio_cmd -> cleaned command without the token


        # --- PRE MGMT ---
        if pre:
            run_mgmt_script(mgmt_ssh, pre, sn_uuids)

        # --- RUN FIO ---
        print(f"[FIO] {client_name} {volume_name}")
        if cleanup:
            cmd = f"cd {mount_point} && rm -rf -- * && {fio_cmd}"
        else:
            cmd = f"cd {mount_point} && {fio_cmd}"

        stdout, stderr = client_ssh.exec(cmd, timeout=7200)

        essentials = extract_fio_essentials(stdout)

        header = f"""
==============================
DATE: {time.strftime("%Y-%m-%d %H:%M:%S")}
CLIENT: {client_name}
VOLUME: {volume_name}
MOUNT: {mount_point}
FIO_CMD: {fio_cmd}
==============================
"""

        with open(output_file, "a") as f:
            f.write(header)
            f.write("\n")
            f.write(essentials)
            f.write("\n\n")

        # --- COLLECT SPDK LOGS ---
        # --- COLLECT SPDK LOGS ---
        collect_spdk_logs(
            storage_nodes,
            log_nodes,
            output_file,
            user,
            key_path
        )
        # --- POST MGMT ---
        if post:
            run_mgmt_script(mgmt_ssh, post, sn_uuids)

# --- 5. Main Logic ---
def main():
    args = parse_args()
    meta = load_metadata()
    storage_nodes = meta["storage_nodes"]
    if not meta:
        print("Error: No metadata found. Please run the setup script first.")
        return

    # Constants & Metadata extraction
    MGMT_IP = meta['mgmt']['public_ip']
    USER = meta['user']
    KEY_PATH = meta['key_path']
    TARGET_SUBNET = meta['subnet_id']
    TARGET_GROUP = meta['target_group']
    AMI_ID = "ami-0dfc569a8686b9320"
    KEY_NAME = "mtes01"
    CLIENT_COUNT = 1
    CLIENT_TYPE = "m6in.8xlarge"
    RUN_ID = str(int(time.time()))[-6:]

    # --- Step 1: Client Provisioning Logic ---
    if args.create_clients:
        print("Step 1: Re-provisioning Client Instances...")
        # Optional: Terminate old clients first if they exist in meta
        new_clients = create_aws_clients(CLIENT_COUNT, CLIENT_TYPE, TARGET_SUBNET, TARGET_GROUP, KEY_NAME, AMI_ID,
                                         RUN_ID)

        # Parallel initialization of software

        # Step 1: Parallel initialization of software
        executor_pool = ThreadPoolExecutor(max_workers=max(1, CLIENT_COUNT))

        futures = []
        for c in new_clients:
            futures.append(executor_pool.submit(initialize_client_software, c['public_ip'], USER, KEY_PATH))

        # Wait for all tasks to complete
        for f in futures:
            f.result()

        # Cleanly shutdown the executor
        executor_pool.shutdown()
        meta['clients'] = new_clients
        with open(METADATA_FILE, "w") as f:
            json.dump(meta, f, indent=4)
        print("  [v] New clients provisioned and metadata updated.")

    CLIENT_IPS = {f"client{i + 1}": c['public_ip'] for i, c in enumerate(meta['clients'])}

    # --- Step 2: Cluster Cleanup ---
    print("Step 2: Cleaning up existing volumes on Cluster...")
    mgmt = PersistentSSH(MGMT_IP, USER, KEY_PATH)
    raw_list, _ = mgmt.exec("sudo /usr/local/bin/sbctl lvol list")
    uuids = re.findall(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', raw_list)
    for uid in uuids:
        mgmt.exec(f"sudo /usr/local/bin/sbctl lvol delete {uid}")

    sn_list, _ = mgmt.exec("sudo /usr/local/bin/sbctl sn list")
    sn_uuids = re.findall(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', sn_list)

    # --- Step 2: Retrieve Storage Nodes ---
    print("Step 2: Retrieving Storage Nodes...")
    mgmt = PersistentSSH(MGMT_IP, USER, KEY_PATH)

    sn_list_raw, _ = mgmt.exec("sudo /usr/local/bin/sbctl sn list")

    # Extract UUIDs from table rows only (ignore header/separator)
    sn_uuids = []

    for line in sn_list_raw.splitlines():
        if line.startswith("|"):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) > 2:
                candidate = parts[1]
                if re.match(r'^[a-f0-9-]{36}$', candidate):
                    sn_uuids.append(candidate)

    print(f"  Found {len(sn_uuids)} storage nodes:")
    for i, uid in enumerate(sn_uuids):
        print(f"    n{i + 1} = {uid}")

    # --- Step 3: Volume Provisioning & Script Generation ---
    VOLUME_PLAN = [
        {"idx": 0, "node_idx": 0, "qty": 1, "size": "100G", "client": "client1", "io_queues": 12}
    ]

    # Will hold runtime info for performance stage
    volume_runtime_map = []

    client_scripts = {
        name: [
            "#!/bin/bash", "set -e",
            "sudo pkill -9 fio || true",
            "sudo nvme disconnect-all || true",
            "sudo modprobe nvme-tcp",
            f"sudo rm -rf /home/{USER}/mnt_* || true"
        ] for name in CLIENT_IPS.keys()
    }

    print("Step 3: Provisioning Volumes and Building Scripts...")
    for plan in VOLUME_PLAN:
        target_sn = sn_uuids[plan['node_idx']]
        for i in range(plan['qty']):
            vol_name = f"v_{RUN_ID}_{plan['idx']}_{i}"
            out, _ = mgmt.exec(
                f"sudo /usr/local/bin/sbctl lvol add {vol_name} {plan['size']} pool01 --host-id {target_sn}")
            lvol_uuid = re.findall(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', out)[-1]

            conn_raw, _ = mgmt.exec(f"sudo /usr/local/bin/sbctl lvol connect {lvol_uuid}")
            connect_cmds = [line.strip().replace("--nr-io-queues=3", f"--nr-io-queues={plan['io_queues']}")
                            for line in conn_raw.splitlines() if "nvme connect" in line]

            mnt = f"/home/{USER}/mnt_{plan['idx']}_{i}"
            volume_runtime_map.append({
                "client": plan['client'],
                "volume_name": vol_name,
                "mount": mnt,
                "idx": plan['idx']
            })
            find_dev = f"DEV=$(ls -l /dev/disk/by-id/ | grep {lvol_uuid} | awk '{{print $NF}}' | sed 's|../../|/dev/|' | head -n 1)"

            client_scripts[plan['client']].extend([
                                                      f"echo 'Connecting {vol_name}...'",
                                                  ] + connect_cmds + [
                                                      "sudo udevadm settle", "sleep 1", find_dev,
                                                      f"sudo mkfs.xfs -f $DEV", f"sudo mkdir -p {mnt}",
                                                      f"sudo mount $DEV {mnt}", f"sudo chown {USER}:{USER} {mnt}"
                                                  ])

    # --- Step 4: Deploy Scripts ---
    print("Step 4: Deploying Scripts to Clients...")
    for name, ip in CLIENT_IPS.items():
        script_text = "\n".join(client_scripts[name])
        print (script_text)
        c = PersistentSSH(ip, USER, KEY_PATH)
        stdin, stdout, stderr = c.client.exec_command("sudo bash")
        stdin.write(script_text);
        stdin.flush();
        stdin.channel.shutdown_write()
        if stdout.channel.recv_exit_status() != 0:
            print(f"  [!] {name} Failed: {stderr.read().decode()}")
        c.close()

    # --- Step 5: Performance Batches ---
    # --- Step 5: Performance Batches ---
    # --- Step 5: Performance Batches ---
    print("Step 5: Running Performance Sequences...")

    OUTPUT_FILE = f"fio_results_{RUN_ID}.log"

    PERF_PLAN = [
        {
            "volume_match_idx": 0,
            "fio_sequence": [
                {
                    "pre_mgmt": """
       """,
                    "fio_cmd": """
       cleanup;fio --name=rwrite --rw=write --bs=128k --numjobs=12 --iodepth=16 --direct=1 --ioengine=aiolib --size=6G --group_reporting
       """,
                    "post_mgmt": """
       echo "Initialized!"
       """
                },
                {
                    "pre_mgmt": """
        set -e
        sbctl sn shutdown $n1 --force
        """,
                    "fio_cmd": """
        fio --name=rwrite --rw=randwrite --bs=4k --numjobs=12 --iodepth=16 --direct=1 --ioengine=aiolib --size=6G --group_reporting
        """,
                    "post_mgmt": """
        set -e
        sbctl sn restart $n1 
        sleep 180
        """
                },
                {
                    "pre_mgmt": """
        set -e
        sbctl sn shutdown $n1 --force
        """,
                    "fio_cmd": """
        fio --name=rwrite --rw=randwrite --bs=4k --numjobs=12 --iodepth=32 --direct=1 --ioengine=aiolib --size=6G --group_reporting
        """,
                    "post_mgmt": """
        set -e
        sbctl sn restart $n1 
        sleep 180
        """
                },
                {
                    "pre_mgmt": """
        set -e
        sbctl sn shutdown $n1 --force
        """,
                    "fio_cmd": """
        fio --name=rwrite --rw=randwrite --bs=4k --numjobs=12 --iodepth=64 --direct=1 --ioengine=aiolib --size=6G --group_reporting
        """,
                    "post_mgmt": """
        set -e
        sbctl sn restart $n1 
        sleep 180
        """
                },
                {
                    "pre_mgmt": """
        set -e
        sbctl sn shutdown $n1 --force
        """,
                    "fio_cmd": """
        fio --name=rwrite --rw=randwrite --bs=4k --numjobs=12 --iodepth=128 --direct=1 --ioengine=aiolib --size=6G --group_reporting
        """,
                    "post_mgmt": """
        set -e
        sbctl sn restart $n1 
        sleep 180
        """
                },

            ]
        }
    ]
    print("Step 5: Running Performance Sequences...")

    with ThreadPoolExecutor(max_workers=len(volume_runtime_map)) as executor:
        futures = []

        for vol in volume_runtime_map:
            client_ssh = PersistentSSH(CLIENT_IPS[vol['client']], USER, KEY_PATH)

            # Use vol['idx'] to pick the correct FIO sequence plan
            fio_sequence = PERF_PLAN[vol['idx']]['fio_sequence']

            futures.append(
                executor.submit(
                    run_fio_sequence,
                    client_ssh,
                    mgmt,
                    vol['client'],
                    vol['volume_name'],
                    vol['mount'],
                    fio_sequence,
                    OUTPUT_FILE,
                    storage_nodes,
                    sn_uuids,
                    [1, 2, 3],  # choose nodes here
                    USER,
                    KEY_PATH
                )
            )

        # Wait for all FIO sequences to finish
        for f in futures:
            f.result()

    print(f"[v] Results stored in {OUTPUT_FILE}")

    mgmt.close()
    print("Finished.")


if __name__ == "__main__":
    main()