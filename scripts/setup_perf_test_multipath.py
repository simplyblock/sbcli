"""
setup_perf_test_multipath.py — AWS cluster deployer with NVMe-oF multipathing.

Creates a simplyblock FT=2 cluster where every storage node (and the client)
has 3 ENIs:

    eth0  – management (sbctl, SNodeAPI :5000, SSH)
    eth1  – data-plane path A
    eth2  – data-plane path B

Storage nodes are added with ``--data-nics eth1,eth2`` so all internal
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
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko

# Silence paramiko's Transport-thread tracebacks during cloud-init waits.
# It logs "Error reading SSH protocol banner" at ERROR level whenever a
# fresh sshd closes the connection mid-startup — the retry loop in
# wait_for_ssh handles this fine, but the stack traces drown real output.
logging.getLogger("paramiko").setLevel(logging.CRITICAL)
logging.getLogger("paramiko.transport").setLevel(logging.CRITICAL)

# ──────────────────── Configuration ──────────────────────────────────────────
AMI_ID       = "ami-0dfc569a8686b9320"   # Rocky 9 us-east-1
KEY_NAME     = "mtes01"
KEY_PATH     = os.path.expanduser("~/.ssh/mtes01.pem")
AZ           = "us-east-1a"
# eth0 stays on the mgmt subnet with the default/shared SG
MGMT_SUBNET_ID = "subnet-0593459d6b931ee4c"
MGMT_SG        = "sg-02e89a1372e9f39e9"
# eth0 of SN/client lives in a private subnet whose route table sends
# 0.0.0.0/0 to a NAT gateway. SNs/clients have no public IPs, but they
# can still reach RHUI / GitHub / pip mirrors through NAT for the
# bootstrap install. Mgmt stays in the public subnet (IGW route) so
# its return SSH path uses its own public IP, not NAT's.
PRIVATE_SUBNET_ID = "subnet-073c4e619eb3b69be"  # 172.31.98.0/24, NAT-routed
# Each data NIC is in its own isolated subnet + SG — no cross-subnet routing,
# forces inter-node data-plane traffic through the intended NIC.
DATA1_SUBNET_ID = "subnet-0bc107204ccb6c2df"  # 172.31.96.0/24
DATA1_SG        = "sg-007ad0bd943abbefd"      # allow only from 172.31.96.0/24
DATA2_SUBNET_ID = "subnet-09dabfde67a5ae7a0"  # 172.31.97.0/24
DATA2_SG        = "sg-069a5f96309b8dbdd"      # allow only from 172.31.97.0/24
# Kept for backwards compat with any existing consumer of these names.
SUBNET_ID      = MGMT_SUBNET_ID
STORAGE_SG     = MGMT_SG
BRANCH       = "main"
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

def _ssh_connect(ip, jump_ip=None, timeout=None, banner_timeout=None):
    """Open a paramiko SSHClient to ``ip``. When ``jump_ip`` is given,
    tunnel through it via ``direct-tcpip`` (ProxyJump): the jump host
    only needs a standard sshd — it acts as a TCP forwarder while the
    second SSH handshake terminates at ``ip``. All paramiko operations
    happen on the local workstation; the jump host has no paramiko.

    Storage nodes and clients in the multipath layout have no public
    IPs — their SSH must hop via mgmt (``jump_ip=mgmt_ip``).
    """
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if jump_ip is None or jump_ip == ip:
        kwargs = dict(username=USER, key_filename=KEY_PATH,
                      allow_agent=False, look_for_keys=False)
        if timeout is not None:
            kwargs["timeout"] = timeout
        if banner_timeout is not None:
            kwargs["banner_timeout"] = banner_timeout
        ssh.connect(ip, **kwargs)
        return ssh
    jump = paramiko.SSHClient()
    jump.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    jump_kwargs = dict(username=USER, key_filename=KEY_PATH,
                       allow_agent=False, look_for_keys=False)
    if timeout is not None:
        jump_kwargs["timeout"] = timeout
    if banner_timeout is not None:
        jump_kwargs["banner_timeout"] = banner_timeout
    jump.connect(jump_ip, **jump_kwargs)
    transport = jump.get_transport()
    channel = transport.open_channel(
        "direct-tcpip",
        dest_addr=(ip, 22),
        src_addr=("127.0.0.1", 0),
        timeout=timeout if timeout is not None else 30,
    )
    target_kwargs = dict(username=USER, key_filename=KEY_PATH,
                         allow_agent=False, look_for_keys=False, sock=channel)
    if timeout is not None:
        target_kwargs["timeout"] = timeout
    if banner_timeout is not None:
        target_kwargs["banner_timeout"] = banner_timeout
    ssh.connect(ip, **target_kwargs)
    # Stash the jump client on the target client so we can close both.
    ssh._jump_client = jump
    return ssh


def _ssh_close(ssh):
    jump = getattr(ssh, "_jump_client", None)
    try:
        ssh.close()
    finally:
        if jump is not None:
            try:
                jump.close()
            except Exception:
                pass


def wait_for_ssh(ip, timeout=600, jump_ip=None):
    label = f"{ip} (via {jump_ip})" if jump_ip and jump_ip != ip else ip
    print(f"  Waiting for SSH on {label}...")
    start = time.time()
    last_heartbeat = start
    last_err = None
    while time.time() - start < timeout:
        try:
            ssh = _ssh_connect(ip, jump_ip=jump_ip, timeout=5, banner_timeout=10)
            _ssh_close(ssh)
            print(f"  SSH ready: {label} ({int(time.time() - start)}s)")
            return True
        except Exception as e:
            last_err = e
        now = time.time()
        if now - last_heartbeat > 30:
            elapsed = int(now - start)
            err_repr = f"{type(last_err).__name__}: {last_err}" if last_err else "?"
            print(f"  ... still waiting for SSH on {label} ({elapsed}s) — last: {err_repr}")
            last_heartbeat = now
        time.sleep(3)
    raise RuntimeError(f"SSH timeout: {label}")


def wait_for_cloud_init(ip, jump_ip=None, timeout=600):
    """Block until cloud-init has finished on the target. Without this, SSH
    is reachable but the install / sbctl deploy steps can race against
    cloud-init's own dnf locks and NetworkManager reconfigs. Rocky 9 ships
    ``cloud-init status --wait`` which exits 0 once cloud-init is done
    (even after it's been done for a while). Tolerates absence of
    cloud-init by treating non-zero rc as 'not present, skip'.
    """
    label = f"{ip} (via {jump_ip})" if jump_ip and jump_ip != ip else ip
    print(f"  Waiting for cloud-init on {label}...")
    start = time.time()
    out = ssh_exec(
        ip,
        [f"timeout {timeout} sudo cloud-init status --wait 2>&1 || true"],
        get_output=True,
        check=False,
        jump_ip=jump_ip,
    )
    elapsed = int(time.time() - start)
    tail = (out[0].strip().splitlines() or ["(no output)"])[-1]
    print(f"  cloud-init done on {label} ({elapsed}s): {tail}")


def ssh_exec(ip, cmds, get_output=False, check=False, jump_ip=None):
    ssh = _ssh_connect(ip, jump_ip=jump_ip)
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=1200)
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
                _ssh_close(ssh)
                raise RuntimeError(f"rc={rc}: {cmd}")
        else:
            for line in out.strip().splitlines()[-2:]:
                if line.strip():
                    print(f"    {line}")
    _ssh_close(ssh)
    return results


def ssh_exec_stream(ip, cmd, check=False, jump_ip=None):
    ssh = _ssh_connect(ip, jump_ip=jump_ip)
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
    _ssh_close(ssh)
    if rc != 0 and check:
        raise RuntimeError(f"rc={rc}: {cmd}")
    return "".join(out_buf), "".join(err_buf)


# ──────────────────── AWS instance helpers ───────────────────────────────────

def _build_nic_specs(num_nics, public_ip):
    """Build NetworkInterfaces list for create_instances.

    Two cases:
      * ``num_nics == 1`` and ``public_ip`` — single eth0 in the
        public mgmt subnet (IGW-routed) with
        ``AssociatePublicIpAddress=True``. Yields a regular
        auto-assigned public IP, not an EIP.
      * ``num_nics > 1`` and ``public_ip is False`` — eth0 in the
        private NAT-routed subnet (so dnf / pip can reach the
        internet without a public IP) plus eth1/eth2 in their
        isolated data subnets. AWS forbids
        ``AssociatePublicIpAddress`` on a multi-NIC launch, so none
        of the NICs get a public IP. Reach the instance from the
        workstation via mgmt as a ProxyJump.

    Storage nodes and clients take the second case; mgmt the first.
    """
    if num_nics == 1:
        return [{
            "DeviceIndex": 0,
            "SubnetId": MGMT_SUBNET_ID,
            "Groups": [MGMT_SG],
            "AssociatePublicIpAddress": bool(public_ip),
        }]
    if public_ip:
        raise ValueError(
            "AssociatePublicIpAddress is not allowed with multiple NICs at launch; "
            "use num_nics=1 for the public-IP host (mgmt) and num_nics>1 with "
            "public_ip=False for storage nodes and clients."
        )
    nic_map = {
        0: (PRIVATE_SUBNET_ID, MGMT_SG),      # eth0 private (NAT-routed)
        1: (DATA1_SUBNET_ID, DATA1_SG),       # eth1 isolated
        2: (DATA2_SUBNET_ID, DATA2_SG),       # eth2 isolated
    }
    specs = []
    for idx in range(num_nics):
        subnet, sg = nic_map[idx]
        specs.append({
            "DeviceIndex": idx,
            "SubnetId": subnet,
            "Groups": [sg],
        })
    return specs


def launch_instances(count, instance_type, num_nics, tag_name, root_gb=30, public_ip=False):
    """Launch EC2 instances. Only ``num_nics==1`` instances may receive
    an auto-assigned public IP (``public_ip=True``) — used for the mgmt
    node. Multi-NIC instances (storage nodes, clients) launch fully
    private; reach them via their private IPs.
    """
    print(f"  Launching {count}× {instance_type}  ({num_nics} NIC{'s' if num_nics != 1 else ''}, "
          f"public_ip={public_ip})  tag={tag_name}")
    return ec2_resource.create_instances(
        ImageId=AMI_ID,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=KEY_NAME,
        Placement={"AvailabilityZone": AZ},
        NetworkInterfaces=_build_nic_specs(num_nics, public_ip),
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": root_gb, "DeleteOnTermination": True, "VolumeType": "gp3"},
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Name", "Value": tag_name}],
        }],
    )


# ──────────────────── NIC configuration on instances ─────────────────────────

def configure_secondary_nics(ip, nic_names, jump_ip=None):
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
    ssh_exec(ip, cmds, check=False, jump_ip=jump_ip)


def discover_nic_ips(ip, nic_names, jump_ip=None):
    """Return {nic_name: ipv4_addr} for the given NICs."""
    cmd = "; ".join(
        f"echo {n}=$(ip -4 -o addr show {n} 2>/dev/null | awk '{{print $4}}' | cut -d/ -f1)"
        for n in nic_names
    )
    out = ssh_exec(ip, [cmd], get_output=True, jump_ip=jump_ip)[0]
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
    # Only the mgmt node gets a (regular auto-assigned) public IP. SNs and
    # clients launch fully private; the workstation reaches them via
    # paramiko ProxyJump through mgmt (jump_ip=mgmt_ip on every SSH).
    print("\n--- Phase 1: Launch instances ---")
    mgmt_instances = launch_instances(1, MGMT_TYPE, num_nics=1, tag_name="SB-Mgmt-MP",
                                      root_gb=80, public_ip=True)
    sn_instances   = launch_instances(SN_COUNT, SN_TYPE, num_nics=3, tag_name="SB-SN-MP",
                                      public_ip=False)
    client_instances = launch_instances(CLIENT_COUNT, CLIENT_TYPE, num_nics=3,
                                        tag_name="SB-Client-MP", public_ip=False)

    all_instances = mgmt_instances + sn_instances + client_instances
    print(f"  Waiting for {len(all_instances)} instances to reach running state...")
    for inst in all_instances:
        inst.wait_until_running()
        inst.reload()

    mgmt_ip      = mgmt_instances[0].public_ip_address
    sn_priv_ips  = [i.private_ip_address for i in sn_instances]
    client_priv_ips = [i.private_ip_address for i in client_instances]

    print(f"  Mgmt:    {mgmt_ip}  (public)")
    for idx, priv in enumerate(sn_priv_ips):
        print(f"  SN-{idx}:   {priv}  (private; reached via mgmt)")
    for idx, priv in enumerate(client_priv_ips):
        print(f"  Client-{idx}: {priv}  (private; reached via mgmt)")

    # ── Phase 2: Wait for SSH + configure secondary NICs ─────────────────
    print("\n--- Phase 2: SSH readiness + NIC configuration ---")
    # Direct SSH to mgmt so we know the jump host is up before tunneling.
    # cloud-init is what stretches launch-to-ready: poll explicitly so the
    # subsequent install / configure steps don't race its dnf locks.
    wait_for_ssh(mgmt_ip)
    wait_for_cloud_init(mgmt_ip)
    for ip in sn_priv_ips + client_priv_ips:
        wait_for_ssh(ip, jump_ip=mgmt_ip)
    # cloud-init wait on SN/clients runs in parallel via the jump host.
    with ThreadPoolExecutor(max_workers=len(sn_priv_ips) + len(client_priv_ips)) as pool:
        futures = [pool.submit(wait_for_cloud_init, ip, mgmt_ip)
                   for ip in sn_priv_ips + client_priv_ips]
        for f in futures:
            f.result()

    print("  Configuring secondary NICs on storage nodes + clients...")
    multi_nic_ips = sn_priv_ips + client_priv_ips
    with ThreadPoolExecutor(max_workers=len(multi_nic_ips)) as pool:
        futures = [pool.submit(configure_secondary_nics, ip, DATA_NICS, jump_ip=mgmt_ip)
                   for ip in multi_nic_ips]
        for f in futures:
            f.result()

    # Discover data NIC IPs (for metadata)
    print("  Discovering data NIC IPs...")
    sn_data_ips = {}
    for ip in sn_priv_ips:
        sn_data_ips[ip] = discover_nic_ips(ip, DATA_NICS, jump_ip=mgmt_ip)
        print(f"    {ip}: {sn_data_ips[ip]}")

    client_data_ips = {}
    for ip in client_priv_ips:
        client_data_ips[ip] = discover_nic_ips(ip, DATA_NICS, jump_ip=mgmt_ip)
        print(f"    {ip}: {client_data_ips[ip]}")

    # ── Phase 3: Install sbcli on all nodes ──────────────────────────────
    print("\n--- Phase 3: Install sbcli ---")
    install_cmds = [
        "sudo dnf install git python3-pip nvme-cli -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
        "echo 'export PATH=/usr/local/bin:$PATH' >> ~/.bashrc",
    ]
    install_targets = [(mgmt_ip, None)] + [(ip, mgmt_ip) for ip in sn_priv_ips]
    with ThreadPoolExecutor(max_workers=len(install_targets)) as pool:
        futures = [pool.submit(ssh_exec, ip, install_cmds, check=True, jump_ip=jump)
                   for ip, jump in install_targets]
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
    with ThreadPoolExecutor(max_workers=len(sn_priv_ips)) as pool:
        futures = [pool.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn configure --max-lvol {MAX_LVOL}"
        ], check=True, jump_ip=mgmt_ip) for ip in sn_priv_ips]
        for f in futures:
            f.result()
    print("  All SNs configured.")

    with ThreadPoolExecutor(max_workers=len(sn_priv_ips)) as pool:
        futures = [pool.submit(ssh_exec, ip, [
            f"sudo /usr/local/bin/sbctl -d sn deploy --isolate-cores --ifname {MGMT_IFACE}"
        ], check=True, jump_ip=mgmt_ip) for ip in sn_priv_ips]
        for f in futures:
            f.result()
    print("  All SNs deployed. Rebooting...")

    with ThreadPoolExecutor(max_workers=len(sn_priv_ips)) as pool:
        [pool.submit(ssh_exec, ip, ["sudo reboot"], jump_ip=mgmt_ip) for ip in sn_priv_ips]

    print("  Waiting for SN reboot...")
    time.sleep(30)
    for ip in sn_priv_ips:
        wait_for_ssh(ip, jump_ip=mgmt_ip)
        # Post-reboot cloud-init final stage may still be running NIC setup.
        wait_for_cloud_init(ip, jump_ip=mgmt_ip)

    # Re-configure secondary NICs after reboot (NetworkManager may need a nudge)
    print("  Re-configuring secondary NICs after reboot...")
    with ThreadPoolExecutor(max_workers=len(sn_priv_ips)) as pool:
        futures = [pool.submit(configure_secondary_nics, ip, DATA_NICS, jump_ip=mgmt_ip)
                   for ip in sn_priv_ips]
        for f in futures:
            f.result()

    print("  Waiting 60s for SPDK containers to start...")
    time.sleep(60)

    # ── Phase 6: Add nodes with --data-nics ──────────────────────────────
    print("\n--- Phase 6: Add storage nodes with multipath ---")
    # sbctl --data-nics takes a single value: a comma-separated list of
    # NIC names. Space-separating spills extra NICs into argv as
    # positional args which sbctl rejects with "unrecognized arguments".
    data_nics_arg = ",".join(DATA_NICS)
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
    with ThreadPoolExecutor(max_workers=max(1, len(client_priv_ips))) as pool:
        futures = [pool.submit(ssh_exec, ip, client_cmds, check=True, jump_ip=mgmt_ip)
                   for ip in client_priv_ips]
        for f in futures:
            f.result()
    print("  Clients ready.")

    # ── Phase 9: Multipath verification ──────────────────────────────────
    print("\n--- Phase 9: Multipath verification ---")
    verify_errors = verify_multipath(mgmt_ip, expected_nics=len(DATA_NICS))

    # ── Phase 10: Save metadata ──────────────────────────────────────────
    # SN/client public_ip is intentionally None (no public IPs assigned).
    # Metadata consumers (soak scripts) already prefer ``private_ip``;
    # if you need to SSH to one from a workstation, use mgmt as a jump
    # host (paramiko ProxyJump or ``ssh -J``).
    print("\n--- Phase 10: Save metadata ---")
    storage_metadata = []
    for idx, inst in enumerate(sn_instances):
        entry = {
            "instance_id": inst.id,
            "private_ip": inst.private_ip_address,
            "public_ip": inst.public_ip_address,  # None for multipath SNs
            "subnet_id": inst.subnet_id,
            "security_group_id": STORAGE_SG,
        }
        if inst.private_ip_address in sn_data_ips:
            entry["data_nics"] = sn_data_ips[inst.private_ip_address]
        storage_metadata.append(entry)

    client_metadata = []
    for inst in client_instances:
        entry = {
            "instance_id": inst.id,
            "public_ip": inst.public_ip_address,  # None for multipath clients
            "private_ip": inst.private_ip_address,
            "security_group_id": STORAGE_SG,
        }
        if inst.private_ip_address in client_data_ips:
            entry["data_nics"] = client_data_ips[inst.private_ip_address]
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

    with open("cluster_metadata_mp.json", "w") as f:
        json.dump(final_metadata, f, indent=4)

    # ── Done ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("Deployment complete.")
    print(f"  Cluster:  {cluster_uuid}")
    print(f"  Mgmt:     {mgmt_ip}")
    print(f"  SNs:      {', '.join(sn_priv_ips)}  (private; via mgmt jump)")
    print(f"  Clients:  {', '.join(client_priv_ips)}  (private; via mgmt jump)")
    print(f"  Data NICs: {', '.join(DATA_NICS)}")
    print("  Metadata: cluster_metadata_mp.json")
    if verify_errors:
        print(f"  WARNING: {len(verify_errors)} verification issue(s) — check output above")
    else:
        print("  Multipath verification: PASSED")
    print("=" * 60)


def teardown(metadata_path="cluster_metadata_mp.json"):
    """Terminate all instances. No EIPs to release — mgmt uses a regular
    auto-assigned public IP (freed on terminate); SNs/clients have no
    public IPs at all.
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

    print(f"Terminating {len(all_ids)} instances …")
    ec2_client.terminate_instances(InstanceIds=all_ids)
    for iid in all_ids:
        print(f"  {iid}: terminating")
    print("Done.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "teardown":
        meta_path = sys.argv[2] if len(sys.argv) > 2 else "cluster_metadata_mp.json"
        teardown(meta_path)
    else:
        main()
