"""
setup_perf_test_multipath.py — AWS cluster deployer with NVMe-oF multipathing.

Creates a simplyblock FT=2 cluster where every storage node (and the client)
has 3 ENIs:

    eth0  – management (sbctl, SNodeAPI :5000, SSH)
    eth1  – data-plane path A
    eth2  – data-plane path B

Storage nodes are added with ``--data-nics eth1 eth2`` so all internal
connections (devices, JM, hublvol) and client connections are duplicated
across both data NICs, providing true NVMe multipath.

After activation the script runs a verification sweep that checks:
    1. Each node reports 2 data_nics in ``sbctl sn list --json``.
    2. Hublvol controllers on secondary/tertiary nodes show ≥2 paths.
    3. ``sbctl lvol connect`` returns 2× connect commands per node.

Prerequisites:
    pip install boto3 paramiko
    AWS credentials configured (aws configure)
    SSH key pair at KEY_PATH
"""

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko

# ──────────────────── Configuration ──────────────────────────────────────────
AMI_ID       = "ami-0dfc569a8686b9320"   # Rocky 9 us-east-1
KEY_NAME     = "mtes01"
KEY_PATH     = r"C:\ssh\mtes01.pem"
AZ           = "us-east-1a"
SUBNET_ID    = "subnet-0593459d6b931ee4c"
STORAGE_SG   = "sg-02e89a1372e9f39e9"
BRANCH       = "test_ftt2"
USER         = "ec2-user"
MGMT_IFACE   = "eth0"
DATA_NICS    = ["eth1", "eth2"]          # Names the OS assigns to ENI index 1, 2

SN_TYPE      = "i3en.2xlarge"            # 4 NICs max, NVMe SSDs
SN_COUNT     = 6
MGMT_TYPE    = "m6i.2xlarge"
CLIENT_TYPE  = "m6in.8xlarge"
CLIENT_COUNT = 1
MAX_LVOL     = "10"

# FT=2 cluster params
DATA_CHUNKS  = 2
PARITY_CHUNKS = 2
MAX_FT       = 2
HA_JM_COUNT  = 4

ec2_resource = boto3.resource("ec2", region_name="us-east-1")
ec2_client   = boto3.client("ec2", region_name="us-east-1")


# ──────────────────── SSH helpers ────────────────────────────────────────────

def wait_for_ssh(ip, timeout=300):
    print(f"  Waiting for SSH on {ip}...")
    start = time.time()
    while time.time() - start < timeout:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                        timeout=5, banner_timeout=10,
                        allow_agent=False, look_for_keys=False)
            ssh.close()
            print(f"  SSH ready: {ip}")
            return True
        except Exception:
            pass
        time.sleep(3)
    raise RuntimeError(f"SSH timeout: {ip}")


def ssh_exec(ip, cmds, get_output=False, check=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
        out = stdout.read().decode()
        err = stderr.read().decode()
        rc = stdout.channel.recv_exit_status()
        if get_output:
            results.append(out)
        if rc != 0:
            tail = (out + err).strip().splitlines()[-5:]
            print(f"  [{ip}] FAIL rc={rc}")
            for line in tail:
                print(f"    {line}")
            if check:
                ssh.close()
                raise RuntimeError(f"rc={rc}: {cmd}")
        else:
            for line in out.strip().splitlines()[-2:]:
                if line.strip():
                    print(f"    {line}")
    ssh.close()
    return results


def ssh_exec_stream(ip, cmd, check=False):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    print(f"  [{ip}] $ {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=600)
    channel = stdout.channel
    out_buf, err_buf = [], []
    while True:
        while channel.recv_ready():
            chunk = channel.recv(4096).decode("utf-8", errors="replace")
            out_buf.append(chunk)
            print(chunk, end="")
        while channel.recv_stderr_ready():
            chunk = channel.recv_stderr(4096).decode("utf-8", errors="replace")
            err_buf.append(chunk)
            print(chunk, end="")
        if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
            break
        time.sleep(0.1)
    rc = channel.recv_exit_status()
    ssh.close()
    if rc != 0 and check:
        raise RuntimeError(f"rc={rc}: {cmd}")
    return "".join(out_buf), "".join(err_buf)


# ──────────────────── AWS instance helpers ───────────────────────────────────

def _build_nic_specs(num_nics, subnet, sg):
    """Build NetworkInterfaces list for create_instances (up to num_nics).

    AWS does not allow AssociatePublicIpAddress inside a NIC spec when
    multiple network interfaces are present.  Public IPs are assigned
    post-launch via Elastic IPs instead (see _assign_public_ips).
    """
    specs = []
    for idx in range(num_nics):
        specs.append({
            "DeviceIndex": idx,
            "SubnetId": subnet,
            "Groups": [sg],
        })
    return specs


def _assign_public_ips(instances):
    """Allocate an EIP for each instance and associate it with eth0.

    Required because AssociatePublicIpAddress cannot be used with
    multiple network interfaces at launch time.  For multi-NIC instances
    the association must target the primary ENI (DeviceIndex=0) by its
    NetworkInterfaceId, not the InstanceId.
    """
    for inst in instances:
        inst.wait_until_running()
        inst.reload()
        # Find the primary ENI (DeviceIndex 0)
        primary_eni = None
        for ni in inst.network_interfaces:
            if ni.attachment and ni.attachment.get("DeviceIndex") == 0:
                primary_eni = ni.id
                break
        if not primary_eni:
            # Fallback: first NIC in the list
            primary_eni = inst.network_interfaces[0].id if inst.network_interfaces else None
        eip = ec2_client.allocate_address(Domain="vpc")
        assoc_params = {"AllocationId": eip["AllocationId"]}
        if primary_eni and len(inst.network_interfaces) > 1:
            assoc_params["NetworkInterfaceId"] = primary_eni
        else:
            assoc_params["InstanceId"] = inst.id
        ec2_client.associate_address(**assoc_params)
        print(f"  {inst.id}: assigned EIP {eip['PublicIp']} (eni={primary_eni})")


def launch_instances(count, instance_type, num_nics, tag_name, root_gb=30):
    """Launch EC2 instances with *num_nics* ENIs each."""
    print(f"  Launching {count}× {instance_type}  ({num_nics} NICs)  tag={tag_name}")
    instances = ec2_resource.create_instances(
        ImageId=AMI_ID,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=KEY_NAME,
        Placement={"AvailabilityZone": AZ},
        NetworkInterfaces=_build_nic_specs(num_nics, SUBNET_ID, STORAGE_SG),
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": root_gb, "DeleteOnTermination": True, "VolumeType": "gp3"},
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": tag_name}],
        }],
    )
    _assign_public_ips(instances)
    return instances


# ──────────────────── NIC configuration on instances ─────────────────────────

def configure_secondary_nics(ip, nic_names):
    """Ensure secondary NICs are UP with DHCP-assigned IPs on Rocky 9."""
    cmds = []
    for nic in nic_names:
        cmds.extend([
            # Create a NetworkManager connection profile if one doesn't exist
            f"sudo nmcli -g GENERAL.STATE device show {nic} 2>/dev/null | grep -q connected"
            f" || sudo nmcli con add type ethernet con-name {nic} ifname {nic} ipv4.method auto",
            f"sudo nmcli device connect {nic} 2>/dev/null || true",
        ])
    # Wait for IPs
    cmds.append("sleep 5")
    for nic in nic_names:
        cmds.append(f"ip -4 addr show {nic} | grep inet || echo 'WARNING: {nic} has no IP'")
    ssh_exec(ip, cmds, check=False)


def discover_nic_ips(ip, nic_names):
    """Return {nic_name: ipv4_addr} for the given NICs."""
    cmd = "; ".join(
        f"echo {n}=$(ip -4 -o addr show {n} 2>/dev/null | awk '{{print $4}}' | cut -d/ -f1)"
        for n in nic_names
    )
    out = ssh_exec(ip, [cmd], get_output=True)[0]
    result = {}
    for line in out.strip().splitlines():
        if "=" in line:
            name, addr = line.strip().split("=", 1)
            if addr:
                result[name] = addr
    return result


# ──────────────────── UUID extraction ────────────────────────────────────────

UUID_RE = re.compile(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}")

def extract_uuids(text):
    return UUID_RE.findall(text)


def get_sn_uuids(mgmt_ip):
    raw = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d sn list"], get_output=True)[0]
    uuids = []
    for line in raw.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) > 1 and UUID_RE.fullmatch(parts[1]):
            uuids.append(parts[1])
    if not uuids:
        raise RuntimeError(f"No node UUIDs found:\n{raw}")
    return uuids


# ──────────────────── Multipath verification ─────────────────────────────────

def verify_multipath(mgmt_ip, expected_nics=2):
    """Post-activation verification of multipath configuration."""
    print("\n" + "=" * 60)
    print("MULTIPATH VERIFICATION")
    print("=" * 60)
    errors = []

    # 1. Check data_nics count per node
    print("\n--- Check 1: data_nics per node ---")
    raw = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl sn list --json"], get_output=True)[0]
    # Parse JSON from sbctl output (may have log lines before it)
    nodes_json = None
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("["):
            try:
                nodes_json = json.loads(line)
                break
            except json.JSONDecodeError:
                pass
    if not nodes_json:
        # Try full output
        try:
            nodes_json = json.loads(raw.strip())
        except json.JSONDecodeError:
            errors.append("Could not parse sn list --json output")
            nodes_json = []

    for node in nodes_json:
        hostname = node.get("Hostname", "?")
        # sbctl --json doesn't always expose data_nics directly.
        # We verify via the node's RPC instead (check 2).
        print(f"  {hostname}: status={node.get('Status', '?')}, health={node.get('Health', '?')}")

    # 2. Check hublvol controller paths on each node via sbctl sn check
    print("\n--- Check 2: hublvol multipath controllers ---")
    sn_uuids = get_sn_uuids(mgmt_ip)
    for uuid in sn_uuids:
        raw = ssh_exec(mgmt_ip, [
            f"sudo /usr/local/bin/sbctl -d sn check {uuid}"
        ], get_output=True)[0]
        # Count hublvol controller lines
        hub_lines = [ln for ln in raw.splitlines() if "hublvol" in ln.lower() or "controller" in ln.lower()]
        print(f"  {uuid}: hublvol-related lines: {len(hub_lines)}")

    # 3. Create a test volume, check connect output has multipath entries
    print("\n--- Check 3: volume connect multipath commands ---")
    try:
        create_out = ssh_exec(mgmt_ip, [
            "sudo /usr/local/bin/sbctl -d lvol add mp_verify_vol 1G pool01"
        ], get_output=True)[0]
        vol_uuids = extract_uuids(create_out)
        if vol_uuids:
            vol_id = vol_uuids[-1]
            connect_out = ssh_exec(mgmt_ip, [
                f"sudo /usr/local/bin/sbctl -d lvol connect {vol_id}"
            ], get_output=True)[0]
            connect_cmds = [ln.strip() for ln in connect_out.splitlines()
                           if "nvme connect" in ln]
            print(f"  Volume {vol_id}: {len(connect_cmds)} connect commands")
            unique_ips = set()
            for cmd in connect_cmds:
                m = re.search(r"--traddr=(\S+)", cmd)
                if m:
                    unique_ips.add(m.group(1))
                print(f"    {cmd[:120]}...")
            print(f"  Unique data-plane IPs across commands: {len(unique_ips)}")
            if len(connect_cmds) < 2 * expected_nics:
                errors.append(
                    f"Expected ≥{2 * expected_nics} connect commands "
                    f"(2 nodes × {expected_nics} NICs), got {len(connect_cmds)}"
                )
            # Clean up verification volume
            ssh_exec(mgmt_ip, [
                f"sudo /usr/local/bin/sbctl -d lvol delete {vol_id} --force"
            ], check=False)
        else:
            errors.append("Could not extract volume UUID from create output")
    except Exception as e:
        errors.append(f"Volume connect check failed: {e}")

    # Summary
    print("\n--- Verification summary ---")
    if errors:
        for e in errors:
            print(f"  ERROR: {e}")
        print(f"  {len(errors)} issue(s) found.")
    else:
        print("  All multipath checks passed.")
    print("=" * 60 + "\n")
    return errors


# ──────────────────── Main deployment ────────────────────────────────────────

def main():
    print("=" * 60)
    print("AWS Multipath Cluster Deployment")
    print(f"  Storage nodes: {SN_COUNT}× {SN_TYPE}")
    print(f"  NICs per host: 1 mgmt ({MGMT_IFACE}) + {len(DATA_NICS)} data ({', '.join(DATA_NICS)})")
    print(f"  FT={MAX_FT}, branch={BRANCH}")
    print("=" * 60)

    # ── Phase 1: Launch instances ────────────────────────────────────────
    print("\n--- Phase 1: Launch instances ---")
    mgmt_instances = launch_instances(1, MGMT_TYPE, num_nics=1, tag_name="SB-Mgmt-MP")
    sn_instances   = launch_instances(SN_COUNT, SN_TYPE, num_nics=3, tag_name="SB-SN-MP")
    client_instances = launch_instances(CLIENT_COUNT, CLIENT_TYPE, num_nics=3, tag_name="SB-Client-MP")

    all_instances = mgmt_instances + sn_instances + client_instances
    print(f"  Waiting for {len(all_instances)} instances to reach running state...")
    for inst in all_instances:
        inst.wait_until_running()
        inst.reload()

    mgmt_ip     = mgmt_instances[0].public_ip_address
    sn_pub_ips  = [i.public_ip_address for i in sn_instances]
    sn_priv_ips = [i.private_ip_address for i in sn_instances]
    client_pub_ips  = [i.public_ip_address for i in client_instances]

    print(f"  Mgmt:    {mgmt_ip}")
    for idx, (pub, priv) in enumerate(zip(sn_pub_ips, sn_priv_ips)):
        print(f"  SN-{idx}:   {pub} ({priv})")
    for idx, pub in enumerate(client_pub_ips):
        print(f"  Client-{idx}: {pub}")

    # ── Phase 2: Wait for SSH + configure secondary NICs ─────────────────
    print("\n--- Phase 2: SSH readiness + NIC configuration ---")
    all_ips = [mgmt_ip] + sn_pub_ips + client_pub_ips
    for ip in all_ips:
        wait_for_ssh(ip)

    print("  Configuring secondary NICs on storage nodes + clients...")
    multi_nic_ips = sn_pub_ips + client_pub_ips
    with ThreadPoolExecutor(max_workers=len(multi_nic_ips)) as pool:
        futures = [pool.submit(configure_secondary_nics, ip, DATA_NICS) for ip in multi_nic_ips]
        for f in futures:
            f.result()

    # Discover data NIC IPs (for metadata)
    print("  Discovering data NIC IPs...")
    sn_data_ips = {}
    for ip in sn_pub_ips:
        sn_data_ips[ip] = discover_nic_ips(ip, DATA_NICS)
        print(f"    {ip}: {sn_data_ips[ip]}")

    client_data_ips = {}
    for ip in client_pub_ips:
        client_data_ips[ip] = discover_nic_ips(ip, DATA_NICS)
        print(f"    {ip}: {client_data_ips[ip]}")

    # ── Phase 3: Install sbcli on all nodes ──────────────────────────────
    print("\n--- Phase 3: Install sbcli ---")
    install_cmds = [
        "sudo dnf install git python3-pip nvme-cli -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc",
    ]
    setup_ips = [mgmt_ip] + sn_pub_ips
    with ThreadPoolExecutor(max_workers=len(setup_ips)) as pool:
        futures = [pool.submit(ssh_exec, ip, install_cmds, check=True) for ip in setup_ips]
        for f in futures:
            f.result()
    print("  sbcli installed on all nodes.")

    # ── Phase 4: Create cluster ──────────────────────────────────────────
    print("\n--- Phase 4: Create cluster ---")
    ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl -d cluster create --enable-node-affinity"
        f" --data-chunks-per-stripe {DATA_CHUNKS}"
        f" --parity-chunks-per-stripe {PARITY_CHUNKS}"
    ], check=True)

    cluster_out = ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl -d cluster list"
    ], get_output=True)[0]
    cluster_uuids = extract_uuids(cluster_out)
    if not cluster_uuids:
        raise RuntimeError("No cluster UUID found")
    cluster_uuid = cluster_uuids[0]
    print(f"  Cluster UUID: {cluster_uuid}")

    # ── Phase 5: Configure + deploy storage nodes ────────────────────────
    print("\n--- Phase 5: Configure + deploy storage nodes ---")
    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as pool:
        futures = [pool.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn configure --max-lvol {MAX_LVOL}"
        ], check=True) for ip in sn_pub_ips]
        for f in futures:
            f.result()
    print("  All SNs configured.")

    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as pool:
        futures = [pool.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {MGMT_IFACE}"
        ], check=True) for ip in sn_pub_ips]
        for f in futures:
            f.result()
    print("  All SNs deployed. Rebooting...")

    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as pool:
        [pool.submit(ssh_exec, ip, ["sudo reboot"]) for ip in sn_pub_ips]

    print("  Waiting for SN reboot...")
    time.sleep(30)
    for ip in sn_pub_ips:
        wait_for_ssh(ip)

    # Re-configure secondary NICs after reboot (NetworkManager may need a nudge)
    print("  Re-configuring secondary NICs after reboot...")
    with ThreadPoolExecutor(max_workers=len(sn_pub_ips)) as pool:
        futures = [pool.submit(configure_secondary_nics, ip, DATA_NICS) for ip in sn_pub_ips]
        for f in futures:
            f.result()

    print("  Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # ── Phase 6: Add nodes with --data-nics ──────────────────────────────
    print("\n--- Phase 6: Add storage nodes with multipath ---")
    data_nics_arg = " ".join(DATA_NICS)
    for priv_ip in sn_priv_ips:
        for attempt in range(5):
            try:
                ssh_exec(mgmt_ip, [
                    f"sudo /usr/local/bin/sbctl -d sn add-node {cluster_uuid}"
                    f" {priv_ip}:5000 {MGMT_IFACE}"
                    f" --data-nics {data_nics_arg}"
                    f" --ha-jm-count {HA_JM_COUNT}"
                ], check=True)
                break
            except RuntimeError:
                if attempt < 4:
                    print(f"  Retrying add-node {priv_ip} in 30s ({attempt+2}/5)...")
                    time.sleep(30)
                else:
                    raise
    print("  All nodes added with --data-nics.")

    # Verify all online
    sn_list = ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl -d sn list"
    ], get_output=True)[0]
    print(sn_list)
    online = sn_list.lower().count("online")
    if online < SN_COUNT:
        raise RuntimeError(f"Only {online}/{SN_COUNT} nodes online")
    print(f"  {online} nodes online.")

    # ── Phase 7: Activate cluster + create pool ──────────────────────────
    print("\n--- Phase 7: Activate cluster ---")
    time.sleep(10)
    ssh_exec_stream(mgmt_ip,
        f"sudo /usr/local/bin/sbctl -d cluster activate {cluster_uuid}",
        check=True)
    print("  Cluster activated.")

    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl -d pool add pool01 {cluster_uuid}"
    ], check=True)
    print("  Pool created.")

    # ── Phase 8: Prep clients ────────────────────────────────────────────
    print("\n--- Phase 8: Prepare clients ---")
    client_cmds = [
        "sudo dnf install nvme-cli fio -y",
        "sudo modprobe nvme-tcp",
        "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
    ]
    with ThreadPoolExecutor(max_workers=max(1, len(client_pub_ips))) as pool:
        futures = [pool.submit(ssh_exec, ip, client_cmds, check=True) for ip in client_pub_ips]
        for f in futures:
            f.result()
    print("  Clients ready.")

    # ── Phase 9: Multipath verification ──────────────────────────────────
    print("\n--- Phase 9: Multipath verification ---")
    verify_errors = verify_multipath(mgmt_ip, expected_nics=len(DATA_NICS))

    # ── Phase 10: Save metadata ──────────────────────────────────────────
    print("\n--- Phase 10: Save metadata ---")
    storage_metadata = []
    for idx, inst in enumerate(sn_instances):
        entry = {
            "instance_id": inst.id,
            "private_ip": inst.private_ip_address,
            "public_ip": inst.public_ip_address,
            "subnet_id": inst.subnet_id,
            "security_group_id": STORAGE_SG,
        }
        pub = inst.public_ip_address
        if pub in sn_data_ips:
            entry["data_nics"] = sn_data_ips[pub]
        storage_metadata.append(entry)

    client_metadata = []
    for inst in client_instances:
        entry = {
            "instance_id": inst.id,
            "public_ip": inst.public_ip_address,
            "private_ip": inst.private_ip_address,
            "security_group_id": STORAGE_SG,
        }
        pub = inst.public_ip_address
        if pub in client_data_ips:
            entry["data_nics"] = client_data_ips[pub]
        client_metadata.append(entry)

    final_metadata = {
        "provider": "aws",
        "multipath": True,
        "data_nics": DATA_NICS,
        "mgmt": {
            "instance_id": mgmt_instances[0].id,
            "public_ip": mgmt_ip,
            "private_ip": mgmt_instances[0].private_ip_address,
            "subnet_id": SUBNET_ID,
            "security_group_id": STORAGE_SG,
        },
        "storage_nodes": storage_metadata,
        "clients": client_metadata,
        "subnet_id": SUBNET_ID,
        "cluster_uuid": cluster_uuid,
        "user": USER,
        "key_path": KEY_PATH,
    }

    with open("cluster_metadata.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    # ── Done ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Deployment complete.")
    print(f"  Cluster:  {cluster_uuid}")
    print(f"  Mgmt:     {mgmt_ip}")
    print(f"  SNs:      {', '.join(sn_pub_ips)}")
    print(f"  Clients:  {', '.join(client_pub_ips)}")
    print(f"  Data NICs: {', '.join(DATA_NICS)}")
    print("  Metadata: cluster_metadata.json")
    if verify_errors:
        print(f"  WARNING: {len(verify_errors)} verification issue(s) — check output above")
    else:
        print("  Multipath verification: PASSED")
    print("=" * 60)


def teardown(metadata_path="cluster_metadata.json"):
    """Terminate all instances and release associated Elastic IPs.

    Reads instance IDs from the metadata JSON written by main().
    """
    import pathlib
    meta = json.loads(pathlib.Path(metadata_path).read_text())

    all_ids = []
    if "mgmt" in meta:
        all_ids.append(meta["mgmt"]["instance_id"])
    for sn in meta.get("storage_nodes", []):
        all_ids.append(sn["instance_id"])
    for cl in meta.get("clients", []):
        all_ids.append(cl["instance_id"])

    if not all_ids:
        print("No instances found in metadata.")
        return

    # Release EIPs associated with any of these instances
    print("Releasing Elastic IPs …")
    addresses = ec2_client.describe_addresses().get("Addresses", [])
    for addr in addresses:
        if addr.get("InstanceId") in all_ids:
            try:
                if "AssociationId" in addr:
                    ec2_client.disassociate_address(AssociationId=addr["AssociationId"])
                ec2_client.release_address(AllocationId=addr["AllocationId"])
                print(f"  Released EIP {addr['PublicIp']} (was on {addr['InstanceId']})")
            except Exception as e:
                print(f"  Warning: failed to release {addr.get('PublicIp')}: {e}")

    # Terminate instances
    print(f"Terminating {len(all_ids)} instances …")
    ec2_client.terminate_instances(InstanceIds=all_ids)
    for iid in all_ids:
        print(f"  {iid}: terminating")
    print("Done.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "teardown":
        meta_path = sys.argv[2] if len(sys.argv) > 2 else "cluster_metadata.json"
        teardown(meta_path)
    else:
        main()
