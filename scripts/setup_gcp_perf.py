#!/usr/bin/env python3
"""
setup_gcp_perf.py – GCP cluster deployer for simplyblock performance testing.

Topology:
  - 5 × c3d-standard-8-lssd storage nodes, each with 1 bundled NVMe local SSD (375 GB),
    all in the same zone and subnet
  - 1 × management node (same zone/subnet)
  - 1 × client node (same zone/subnet)
  - Cluster: ndcs=1 npcs=1 FTT=1

NVMe queue pair rationale:
  - c3d-standard-8-lssd has 1 bundled SSD exposed as a separate PCI controller (NCQA=2)
  - c3d-standard-30-lssd (2 SSDs) was tried but failed: each controller only supports NCQA=2
    and simplyblock needs 3 queue pairs when 2 SSDs share a node (data alceml + JM alceml
    + one more). With 1 SSD per node the 2 available queues are exactly sufficient.
  - Attached NVMe SSDs (--local-ssd interface=NVME) on all other GCP machine types share
    a single PCI controller with multiple namespaces — not supported by simplyblock SPDK.

GCP local SSD notes:
  - Each NVMe local SSD is exactly 375 GB (size is fixed by GCP, not configurable)
  - Local SSDs survive reboot but are lost on stop/start; fine for storage nodes
  - C3D instances only support NVME interface (not SCSI)
  - Ensure your chosen ZONE supports C3D machine types (e.g. us-central1-b)

Prerequisites:
  - gcloud CLI installed and authenticated:
      gcloud auth login
      gcloud config set project YOUR_PROJECT
  - paramiko installed: pip install paramiko
  - SSH key pair: C:\\ssh\\gcp_sbcli (private) and C:\\ssh\\gcp_sbcli.pub (public)
    Generated with: ssh-keygen -t ed25519 -f C:/ssh/gcp_sbcli -N "" -C "sbadmin"
    Fingerprint: SHA256:bz74duCKwTbBHP9lCGEil/jN/VsKhZYV7FVg0BEnrhc
"""

import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

# On Windows, gcloud is a .cmd file and needs to be invoked via cmd /c
_GCLOUD_CMD = ["cmd", "/c", "gcloud"] if sys.platform == "win32" else ["gcloud"]

# ---------------------------------------------------------------------------
# INPUT PARAMETERS — edit these before running
# ---------------------------------------------------------------------------

PROJECT_ID   = "devmichael"                   # gcloud project ID
ZONE         = "us-central1-b"               # Must support C3D (c3d-standard-*)
REGION       = ZONE.rsplit("-", 1)[0]         # Derived from ZONE (e.g. us-central1)
SUBNET       = "default"                      # Subnetwork name in REGION
NETWORK      = "default"                      # VPC network name

IMAGE_PROJECT = "rocky-linux-cloud"
IMAGE_FAMILY  = "rocky-linux-9"

SSH_KEY_PATH     = r"C:\ssh\gcp_sbcli"        # ed25519 private key (no passphrase)
SSH_PUB_KEY_PATH = r"C:\ssh\gcp_sbcli.pub"   # Public key — injected into instance metadata
SSH_USER         = "sbadmin"                  # Linux user GCP creates from key metadata

BRANCH   = "lvol-migration-fresh"
MAX_LVOL = "50"
IFACE    = "eth0"                             # Primary NIC name on GCP VMs (c3d instances use eth0)

SN_COUNT        = 5
LOCAL_SSD_COUNT = 0                           # Bundled in machine type (c3d-standard-8-lssd = 1 × 375 GB)
SN_MACHINE_TYPE     = "c3d-standard-8-lssd"  # C3D with 1 bundled NVMe local SSD (375 GB per node)
MGMT_MACHINE_TYPE   = "n2-standard-4"
CLIENT_MACHINE_TYPE = "n2-standard-8"

# Network tag applied to all SB instances — used to scope firewall rules
CLUSTER_TAG  = "sb-cluster"
NAME_PREFIX  = "sb"

# ---------------------------------------------------------------------------
# gcloud helpers
# ---------------------------------------------------------------------------

def _gcloud(args, check=True, capture=True):
    """Run a gcloud command. Returns parsed JSON if output is JSON, else raw str."""
    cmd = _GCLOUD_CMD + ["--project", PROJECT_ID, "--quiet"] + args
    result = subprocess.run(
        cmd, capture_output=capture, text=True,
        check=False,
    )
    if check and result.returncode != 0:
        print(f"  [gcloud] FAILED: {' '.join(cmd)}")
        if result.stderr:
            print(f"  stderr: {result.stderr.strip()}")
        raise RuntimeError(f"gcloud command failed (rc={result.returncode})")
    if capture and result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout
    return result.stdout or None


def _read_pub_key():
    pub_path = os.path.expanduser(SSH_PUB_KEY_PATH)
    with open(pub_path) as f:
        return f.read().strip()


def _ssh_key_metadata():
    """Return the --metadata value for SSH key injection."""
    return f"ssh-keys={SSH_USER}:{_read_pub_key()}"


# ---------------------------------------------------------------------------
# Firewall
# ---------------------------------------------------------------------------

def _gcloud_idempotent(args):
    """Run a gcloud create command, silently succeeding if resource already exists."""
    result = subprocess.run(
        _GCLOUD_CMD + ["--project", PROJECT_ID, "--quiet"] + args,
        capture_output=True, text=True, check=False,
    )
    if result.returncode == 0:
        return
    if "already exists" in result.stderr or "alreadyExists" in result.stderr:
        return  # idempotent — rule already present from a previous run
    print(f"  [gcloud] FAILED: {' '.join(args[:4])}")
    if result.stderr:
        print(f"  stderr: {result.stderr.strip()}")
    raise RuntimeError(f"gcloud command failed (rc={result.returncode})")


def ensure_firewall_rules():
    """Idempotently create firewall rules for the cluster tag."""
    ssh_rule = f"{NAME_PREFIX}-allow-ssh"
    internal_rule = f"{NAME_PREFIX}-allow-internal"

    print(f"  Ensuring firewall rule: {ssh_rule}")
    _gcloud_idempotent([
        "compute", "firewall-rules", "create", ssh_rule,
        "--network", NETWORK,
        "--allow", "tcp:22",
        "--target-tags", CLUSTER_TAG,
        "--source-ranges", "0.0.0.0/0",
        "--description", "Allow SSH to simplyblock cluster nodes",
    ])

    print(f"  Ensuring firewall rule: {internal_rule}")
    _gcloud_idempotent([
        "compute", "firewall-rules", "create", internal_rule,
        "--network", NETWORK,
        "--allow", "all",
        "--target-tags", CLUSTER_TAG,
        "--source-tags", CLUSTER_TAG,
        "--description", "Allow all traffic between simplyblock cluster nodes",
    ])


# ---------------------------------------------------------------------------
# Instance launch
# ---------------------------------------------------------------------------

def _local_ssd_flags(count):
    """Return repeated --local-ssd flags for gcloud."""
    flags = []
    for _ in range(count):
        flags += ["--local-ssd", "interface=NVME"]
    return flags


def get_instance(name):
    """Return existing instance dict or None if not found."""
    result = _gcloud([
        "compute", "instances", "describe", name,
        "--zone", ZONE, "--format=json",
    ], check=False)
    if isinstance(result, dict):
        return result
    return None


def launch_instance(name, machine_type, local_ssds=0, boot_disk_gb=50):
    """Create a single GCP instance, or return existing one. Returns parsed instance dict."""
    existing = get_instance(name)
    if existing:
        print(f"  {name} already exists — reusing.")
        return existing
    print(f"  Launching {name} ({machine_type})...")
    cmd = [
        "compute", "instances", "create", name,
        "--zone", ZONE,
        "--machine-type", machine_type,
        "--subnet", SUBNET,
        "--image-project", IMAGE_PROJECT,
        "--image-family", IMAGE_FAMILY,
        "--boot-disk-size", f"{boot_disk_gb}GB",
        "--boot-disk-type", "pd-ssd",
        "--tags", CLUSTER_TAG,
        "--metadata", _ssh_key_metadata(),
        "--format=json",
    ] + _local_ssd_flags(local_ssds)
    result = _gcloud(cmd)
    instances = result if isinstance(result, list) else [result]
    return instances[0]


def launch_instances_batch(names, machine_type, local_ssds=0, boot_disk_gb=50):
    """Create multiple GCP instances in one gcloud call, skipping existing ones."""
    to_create = []
    existing = {}
    for name in names:
        inst = get_instance(name)
        if inst:
            print(f"  {name} already exists — reusing.")
            existing[name] = inst
        else:
            to_create.append(name)
    results = list(existing.values())
    if to_create:
        print(f"  Launching {len(to_create)} × {machine_type} instances: {to_create}")
        cmd = [
            "compute", "instances", "create", *to_create,
            "--zone", ZONE,
            "--machine-type", machine_type,
            "--subnet", SUBNET,
            "--image-project", IMAGE_PROJECT,
            "--image-family", IMAGE_FAMILY,
            "--boot-disk-size", f"{boot_disk_gb}GB",
            "--boot-disk-type", "pd-ssd",
            "--tags", CLUSTER_TAG,
            "--metadata", _ssh_key_metadata(),
            "--format=json",
        ] + _local_ssd_flags(local_ssds)
        result = _gcloud(cmd)
        created = result if isinstance(result, list) else [result]
        results += created
    # Return in original order
    name_to_inst = {inst["name"]: inst for inst in results}
    return [name_to_inst[n] for n in names]


def get_instance_ips(instance_dict):
    """Extract (external_ip, internal_ip) from a gcloud instance dict."""
    iface = instance_dict["networkInterfaces"][0]
    internal_ip = iface["networkIP"]
    access_configs = iface.get("accessConfigs", [])
    external_ip = access_configs[0].get("natIP", "") if access_configs else ""
    return external_ip, internal_ip


# ---------------------------------------------------------------------------
# SSH helpers (identical pattern to AWS script)
# ---------------------------------------------------------------------------

def wait_for_ssh(ip, timeout=300):
    import paramiko
    print(f"--> Attempting SSH handshake on {ip}...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(
                ip, username=SSH_USER,
                key_filename=os.path.expanduser(SSH_KEY_PATH),
                timeout=5, banner_timeout=10,
                allow_agent=False, look_for_keys=False,
            )
            ssh.close()
            print(f"SUCCESS: {ip} is ready.")
            return True
        except Exception:
            pass
        time.sleep(2)
    print(f"FAILURE: Timed out waiting for SSH on {ip}")
    return False


def ssh_exec(ip, cmds, get_output=False, check=False):
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        ip, username=SSH_USER,
        key_filename=os.path.expanduser(SSH_KEY_PATH),
        allow_agent=False, look_for_keys=False,
    )
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        _, stdout, stderr = ssh.exec_command(cmd, timeout=600)
        out = stdout.read().decode("utf-8")
        err = stderr.read().decode("utf-8")
        rc = stdout.channel.recv_exit_status()
        if get_output:
            results.append(out)
        if rc != 0:
            print(f"  [{ip}] FAILED (rc={rc}): {cmd}")
            for line in out.strip().split("\n")[-5:]:
                if line.strip():
                    print(f"    stdout: {line}")
            for line in err.strip().split("\n")[-5:]:
                if line.strip():
                    print(f"    stderr: {line}")
            if check:
                ssh.close()
                raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
        else:
            for line in out.strip().split("\n")[-2:]:
                if line.strip():
                    print(f"    {line}")
    ssh.close()
    return results


def ssh_exec_stream(ip, cmd, check=False):
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        ip, username=SSH_USER,
        key_filename=os.path.expanduser(SSH_KEY_PATH),
        allow_agent=False, look_for_keys=False,
    )
    print(f"  [{ip}] $ {cmd}")
    _, stdout, stderr = ssh.exec_command(cmd, timeout=600)
    channel = stdout.channel
    out_chunks, err_chunks = [], []

    while True:
        while channel.recv_ready():
            chunk = channel.recv(4096).decode("utf-8", errors="replace")
            out_chunks.append(chunk)
            print(chunk, end="")
        while channel.recv_stderr_ready():
            chunk = channel.recv_stderr(4096).decode("utf-8", errors="replace")
            err_chunks.append(chunk)
            print(chunk, end="")
        if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
            break
        time.sleep(0.1)

    rc = channel.recv_exit_status()
    ssh.close()
    out = "".join(out_chunks)
    err = "".join(err_chunks)
    if rc != 0 and check:
        raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    return out, err


# ---------------------------------------------------------------------------
# sbctl helpers
# ---------------------------------------------------------------------------

def get_sn_uuids(mgmt_ip):
    print("Fetching Storage Node UUIDs...")
    raw = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl -d sn list"], get_output=True)[0]
    uuids = []
    for line in raw.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) > 1:
            candidate = parts[1]
            if re.match(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", candidate):
                uuids.append(candidate)
    if not uuids:
        print("DEBUG: Raw output:\n", raw)
        raise Exception("Failed to parse Node UUIDs from sbctl output.")
    return uuids


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Deploying simplyblock cluster on GCP")
    print(f"  Project : {PROJECT_ID}")
    print(f"  Zone    : {ZONE}")
    print(f"  Subnet  : {SUBNET}")
    print(f"  SN type : {SN_MACHINE_TYPE}  ×{SN_COUNT}  +{LOCAL_SSD_COUNT}× NVMe local SSD")
    print(f"  Mgmt    : {MGMT_MACHINE_TYPE}")
    print(f"  Client  : {CLIENT_MACHINE_TYPE}")
    print("=" * 60)

    # --- 1. Firewall rules ---
    print("\n[1/7] Ensuring firewall rules...")
    ensure_firewall_rules()

    # --- 2. Launch instances ---
    print("\n[2/7] Launching instances...")

    mgmt_inst = launch_instance(
        f"{NAME_PREFIX}-mgmt", MGMT_MACHINE_TYPE, local_ssds=0, boot_disk_gb=50)

    sn_names = [f"{NAME_PREFIX}-sn-{i}" for i in range(SN_COUNT)]
    sn_insts = launch_instances_batch(
        sn_names, SN_MACHINE_TYPE, local_ssds=LOCAL_SSD_COUNT, boot_disk_gb=50)

    client_inst = launch_instance(
        f"{NAME_PREFIX}-client", CLIENT_MACHINE_TYPE, local_ssds=0, boot_disk_gb=50)

    # --- 3. Extract IPs ---
    print("\n[3/7] Collecting instance IPs...")
    mgmt_pub_ip, mgmt_priv_ip = get_instance_ips(mgmt_inst)

    sn_pub_ips, sn_priv_ips = [], []
    for inst in sn_insts:
        pub, priv = get_instance_ips(inst)
        sn_pub_ips.append(pub)
        sn_priv_ips.append(priv)

    client_pub_ip, client_priv_ip = get_instance_ips(client_inst)

    print(f"  Mgmt    : {mgmt_pub_ip}  (internal: {mgmt_priv_ip})")
    for i, (pub, priv) in enumerate(zip(sn_pub_ips, sn_priv_ips)):
        print(f"  SN-{i}    : {pub}  (internal: {priv})")
    print(f"  Client  : {client_pub_ip}  (internal: {client_priv_ip})")

    # --- 4. Wait for SSH ---
    print("\n[4/7] Waiting for SSH readiness...")
    setup_ips = [mgmt_pub_ip] + sn_pub_ips
    for ip in setup_ips:
        wait_for_ssh(ip)

    # --- 5. Phase 1: Install sbcli on all setup nodes in parallel ---
    print("\n[5/7] Phase 1: Installing sbcli on all nodes...")
    install_cmds = [
        "sudo dnf install git python3-pip nvme-cli pciutils -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc",
    ]
    with ThreadPoolExecutor(max_workers=len(setup_ips)) as ex:
        futures = [ex.submit(ssh_exec, ip, install_cmds, check=True) for ip in setup_ips]
        for f in futures:
            f.result()
    print("Phase 1: DONE — sbcli installed on all nodes.")

    # --- 6. Phase 2: Cluster setup ---
    # 6a. Create cluster on mgmt node
    # 3 nodes → ndcs=2 npcs=1 FTT=1 (need ndcs+npcs+1 ≤ SN_COUNT)
    print("\n[6/7] Phase 2a: Creating cluster on management node...")
    ssh_exec(mgmt_pub_ip, [
        "sudo /usr/local/bin/sbctl -d cluster create"
        " --enable-node-affinity"
        " --data-chunks-per-stripe 1"
        " --parity-chunks-per-stripe 1"
    ], check=True)
    print("Phase 2a: DONE — cluster created.")

    # 6b. Configure storage nodes in parallel
    print("Phase 2b: Configuring storage nodes...")
    with ThreadPoolExecutor(max_workers=SN_COUNT) as ex:
        futures = [ex.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn configure --max-lvol {MAX_LVOL}"
        ], check=True) for ip in sn_pub_ips]
        for f in futures:
            f.result()
    print("Phase 2b: DONE — all SNs configured.")

    # 6c. Deploy storage nodes in parallel
    print("Phase 2c: Deploying storage nodes...")
    with ThreadPoolExecutor(max_workers=SN_COUNT) as ex:
        futures = [ex.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {IFACE}"
        ], check=True) for ip in sn_pub_ips]
        for f in futures:
            f.result()
    print("Phase 2c: DONE — all SNs deployed. Rebooting...")

    # Reboot all SNs (reboot exits non-zero, don't check)
    with ThreadPoolExecutor(max_workers=SN_COUNT) as ex:
        [ex.submit(ssh_exec, ip, ["sudo reboot"]) for ip in sn_pub_ips]

    print("Waiting for SN reboot recovery...")
    time.sleep(30)
    for ip in sn_pub_ips:
        wait_for_ssh(ip)
    print("All storage nodes back online after reboot.")

    print("Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # --- 7. Phase 3: Activate cluster and add nodes ---
    print("\n[7/7] Phase 3: Getting cluster UUID...")
    cluster_list_raw = ssh_exec(
        mgmt_pub_ip, ["sudo /usr/local/bin/sbctl -d cluster list"], get_output=True)[0]
    cluster_match = re.search(
        r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})",
        cluster_list_raw)
    if not cluster_match:
        raise Exception("Could not find Cluster UUID in sbctl output")
    cluster_uuid = cluster_match.group(1)
    print(f"Cluster UUID: {cluster_uuid}")

    print("Phase 3: Adding storage nodes to cluster...")
    for priv_ip in sn_priv_ips:
        for attempt in range(5):
            try:
                ssh_exec(mgmt_pub_ip, [
                    f"sudo /usr/local/bin/sbctl -d sn add-node"
                    f" {cluster_uuid} {priv_ip}:5000 {IFACE} --ha-jm-count 3"
                ], check=True)
                break
            except RuntimeError:
                if attempt < 4:
                    print(f"  Retrying add-node for {priv_ip} in 30s (attempt {attempt+2}/5)...")
                    time.sleep(30)
                else:
                    raise
    print("Phase 3: DONE — all storage nodes added.")

    # Verify nodes online
    sn_list_raw = ssh_exec(
        mgmt_pub_ip, ["sudo /usr/local/bin/sbctl -d sn list"], get_output=True)[0]
    print(sn_list_raw)
    online_count = sn_list_raw.count("online")
    if online_count < SN_COUNT:
        raise Exception(f"Only {online_count}/{SN_COUNT} nodes online")
    print(f"Verified: {online_count} nodes online.")

    print("Phase 4: Activating cluster...")
    time.sleep(10)
    ssh_exec_stream(
        mgmt_pub_ip,
        f"sudo /usr/local/bin/sbctl -d cluster activate {cluster_uuid}",
        check=True,
    )
    print("Phase 4: DONE — cluster activated.")

    print("Creating pool...")
    ssh_exec(mgmt_pub_ip, [
        f"sudo /usr/local/bin/sbctl -d pool add pool01 {cluster_uuid}"
    ], check=True)
    print("Pool created.")

    # Prep client node
    print("Prepping client...")
    wait_for_ssh(client_pub_ip)
    ssh_exec(client_pub_ip, [
        "sudo dnf install nvme-cli fio -y",
        "sudo modprobe nvme-tcp",
        "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
    ], check=True)
    print("Client prepped.")

    # --- Save metadata ---
    final_metadata = {
        "provider": "gcp",
        "project": PROJECT_ID,
        "zone": ZONE,
        "subnet": SUBNET,
        "cluster_uuid": cluster_uuid,
        "user": SSH_USER,
        "key_path": SSH_KEY_PATH,
        "iface": IFACE,
        "mgmt": {
            "instance_name": mgmt_inst["name"],
            "public_ip": mgmt_pub_ip,
            "private_ip": mgmt_priv_ip,
        },
        "storage_nodes": [
            {
                "instance_name": inst["name"],
                "public_ip": pub,
                "private_ip": priv,
                "local_ssds": LOCAL_SSD_COUNT,
                "local_ssd_size_gb": 375,
            }
            for inst, pub, priv in zip(sn_insts, sn_pub_ips, sn_priv_ips)
        ],
        "clients": [
            {
                "instance_name": client_inst["name"],
                "public_ip": client_pub_ip,
                "private_ip": client_priv_ip,
            }
        ],
    }

    with open("cluster_metadata.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    print("\n" + "=" * 60)
    print("Setup Complete")
    print(f"  Cluster UUID : {cluster_uuid}")
    print(f"  Mgmt node    : {mgmt_pub_ip}")
    print(f"  Storage nodes: {', '.join(sn_pub_ips)}")
    print(f"  Client       : {client_pub_ip}")
    print("  Metadata saved to cluster_metadata.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
