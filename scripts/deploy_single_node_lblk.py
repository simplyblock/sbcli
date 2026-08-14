#!/usr/bin/env python3
"""Deploy a SINGLE-NODE lblk cluster on AWS in one of three device configs.

Configs (the three shapes the single-node partition soak exercises):
  2ssd   two EBS volumes as whole disks -> journal-on-device (smallest
         disk becomes the journal), 1 data device, EC 1+0
  2part  one EBS volume carrying 2 GPT partitions -> `sn configure --lblk
         --blk-names p1,p2` splits the smallest partition into journal +
         data at configure time, 2 data devices, EC 1+1
  4part  one EBS volume carrying 4 GPT partitions -> split of the smallest
         partition, 4 data devices, EC 2+1

The cluster is created with --is-single-node --device-mode lblk: activation
configures it non-HA with a single local journal regardless of the EC
schema, physical labels stay 0, and every lvol lifecycle op runs on the one
node (ha_type downgrade).

Usage:
  ./deploy_single_node_lblk.py --config 2part [--keep-cluster-metadata FILE]

Writes cluster_metadata_single_node_<config>.json next to this script
(mgmt/SN IPs + instance ids, cluster uuid, config) for the soak driver.
"""
import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import boto3
import paramiko

# --- lab constants (us-east-1 test account; override via env) ---------------
AMI_ID = os.environ.get("SB_AMI_ID", "ami-0dfc569a8686b9320")  # Rocky 9
KEY_NAME = os.environ.get("SB_KEY_NAME", "mtes01")
KEY_PATH = os.path.expanduser(os.environ.get("SB_KEY_PATH", "~/.ssh/mtes01.pem"))
SUBNET_ID = os.environ.get("SB_SUBNET_ID", "subnet-0593459d6b931ee4c")
SG_ID = os.environ.get("SB_SG_ID", "sg-02e89a1372e9f39e9")
REGION = os.environ.get("SB_REGION", "us-east-1")
BRANCH = os.environ.get("SB_BRANCH", "md-journal")
USER = "ec2-user"
IFACE = "eth0"
MAX_LVOL = "50"
INSTANCE_TYPE = os.environ.get("SB_INSTANCE_TYPE", "m6i.2xlarge")
EBS_IOPS, EBS_TPUT = 6000, 500

CONFIGS = {
    # volumes: (size_gb, ...) attached beyond the 30G root
    # partitions: None = whole disks; N = create N equal GPT partitions on
    #             the single data volume and select them by name
    # ec: (ndcs, npcs) for cluster create
    "2ssd": {"volumes": (30, 100), "partitions": None, "ec": (1, 0)},
    "2part": {"volumes": (160,), "partitions": 2, "ec": (1, 1)},
    "4part": {"volumes": (220,), "partitions": 4, "ec": (2, 1)},
}

SBCTL = "sudo /usr/local/bin/sbctl -d"


# --- ssh helpers -------------------------------------------------------------

def ssh_exec(ip, cmds, get_output=False, check=False, timeout=900):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False, timeout=60)
    results = []
    try:
        for cmd in cmds:
            print(f"  [{ip}] $ {cmd}")
            _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
            out = stdout.read().decode()
            err = stderr.read().decode()
            rc = stdout.channel.recv_exit_status()
            if get_output:
                results.append(out)
            if rc != 0:
                print(f"  [{ip}] rc={rc}: {cmd}")
                for line in (out.strip().splitlines() or [])[-15:]:
                    print(f"    stdout: {line}")
                for line in (err.strip().splitlines() or [])[-15:]:
                    print(f"    stderr: {line}")
                if check:
                    raise RuntimeError(f"Command failed on {ip} (rc={rc}): {cmd}")
    finally:
        ssh.close()
    return results


def wait_for_ssh(ip, timeout=600):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ssh_exec(ip, ["true"])
            return
        except Exception:
            time.sleep(10)
    raise TimeoutError(f"SSH not ready on {ip} within {timeout}s")


# --- aws ---------------------------------------------------------------------

def _block_mappings(volume_sizes):
    mappings = [{"DeviceName": "/dev/sda1",
                 "Ebs": {"VolumeSize": 30, "DeleteOnTermination": True,
                         "VolumeType": "gp3"}}]
    for i, size in enumerate(volume_sizes):
        mappings.append({
            "DeviceName": f"/dev/sd{chr(ord('b') + i)}",
            "Ebs": {"VolumeSize": size, "DeleteOnTermination": True,
                    "VolumeType": "gp3", "Iops": EBS_IOPS,
                    "Throughput": EBS_TPUT},
        })
    return mappings


def launch_instances(config_name):
    ec2 = boto3.resource("ec2", region_name=REGION)
    cfg = CONFIGS[config_name]

    def launch(name, mappings):
        return ec2.create_instances(
            ImageId=AMI_ID, InstanceType=INSTANCE_TYPE, MinCount=1, MaxCount=1,
            KeyName=KEY_NAME,
            NetworkInterfaces=[{"DeviceIndex": 0, "SubnetId": SUBNET_ID,
                                "Groups": [SG_ID],
                                "AssociatePublicIpAddress": True}],
            BlockDeviceMappings=mappings,
            TagSpecifications=[{"ResourceType": "instance",
                                "Tags": [{"Key": "Name", "Value": name}]}])[0]

    mgmt = launch(f"SB-1N-Mgmt-{config_name}", _block_mappings(()))
    sn = launch(f"SB-1N-SN-{config_name}", _block_mappings(cfg["volumes"]))
    for inst in (mgmt, sn):
        inst.wait_until_running()
        inst.reload()
    return mgmt, sn


# --- device preparation -------------------------------------------------------

def data_disks(sn_ip):
    """Non-root whole disks on the SN, name -> size_bytes."""
    out = ssh_exec(sn_ip, ["lsblk -bdno NAME,SIZE,TYPE"], get_output=True)[0]
    root = ssh_exec(sn_ip, ["lsblk -no PKNAME $(findmnt -no SOURCE /) | head -1"],
                    get_output=True)[0].strip()
    disks = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[2] == "disk" and parts[0] != root:
            disks[parts[0]] = int(parts[1])
    return disks


def make_partitions(sn_ip, disk, count):
    """Create `count` equal GPT partitions on /dev/<disk>; return names."""
    cmds = [f"sudo sgdisk --zap-all /dev/{disk}"]
    step = 100 // count
    for i in range(count):
        start = f"{i * step}%"
        end = f"{(i + 1) * step}%" if i < count - 1 else "100%"
        cmds.append(f"sudo parted -s /dev/{disk} mkpart p{i + 1} {start} {end}")
    cmds.insert(1, f"sudo parted -s /dev/{disk} mklabel gpt")
    cmds += [f"sudo partprobe /dev/{disk}", "sudo udevadm settle -t 10"]
    ssh_exec(sn_ip, cmds, check=True)
    out = ssh_exec(sn_ip, [f"lsblk -no NAME /dev/{disk} --raw"], get_output=True)[0]
    names = [n for n in out.split() if n != disk]
    if len(names) != count:
        raise RuntimeError(f"expected {count} partitions on {disk}, got {names}")
    return names


# --- deployment ---------------------------------------------------------------

def install_sbcli(ips):
    cmds = [
        "sudo dnf install git python3-pip nvme-cli fio gdisk parted -y",
        "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
        "sudo /usr/bin/python3 -m pip install ruamel.yaml",
        f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH}"
        " --upgrade --force --ignore-installed requests",
    ]
    with ThreadPoolExecutor(max_workers=len(ips)) as ex:
        for t in [ex.submit(ssh_exec, ip, cmds, False, True) for ip in ips]:
            t.result()


def get_cluster_uuid(mgmt_ip):
    out = ssh_exec(mgmt_ip, [f"{SBCTL} cluster list"], get_output=True)[0]
    m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", out)
    if not m:
        raise RuntimeError(f"no cluster uuid in: {out}")
    return m.group(1)


def get_storage_node_uuid(mgmt_ip):
    out = ssh_exec(mgmt_ip, [f"{SBCTL} sn list"], get_output=True)[0]
    m = re.search(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})", out)
    if not m:
        raise RuntimeError(f"no storage node uuid in: {out}")
    return m.group(1)


def deploy(config_name, keep_metadata_path=None):
    cfg = CONFIGS[config_name]
    print(f"=== single-node lblk deploy: config={config_name}, "
          f"branch={BRANCH}, ec={cfg['ec']} ===")

    mgmt, sn = launch_instances(config_name)
    mgmt_ip, sn_ip = mgmt.public_ip_address, sn.public_ip_address
    sn_priv_ip = sn.private_ip_address
    print(f"mgmt={mgmt_ip} sn={sn_ip} (priv {sn_priv_ip})")
    for ip in (mgmt_ip, sn_ip):
        wait_for_ssh(ip)

    install_sbcli([mgmt_ip, sn_ip])

    ndcs, npcs = cfg["ec"]
    ssh_exec(mgmt_ip, [
        f"{SBCTL} cluster create"
        " --device-mode lblk --is-single-node"
        f" --data-chunks-per-stripe {ndcs} --parity-chunks-per-stripe {npcs}"
    ], check=True, timeout=2400)

    # configure: whole disks (auto-selection) or explicit partitions
    if cfg["partitions"] is None:
        configure = f"{SBCTL} sn configure --max-subsys {MAX_LVOL} --lblk"
    else:
        disks = data_disks(sn_ip)
        disk = max(disks, key=lambda d: disks[d])
        names = make_partitions(sn_ip, disk, cfg["partitions"])
        configure = (f"{SBCTL} sn configure --max-subsys {MAX_LVOL} --lblk"
                     f" --blk-names {','.join(names)}")
    ssh_exec(sn_ip, [configure], check=True)

    ssh_exec(sn_ip, [f"{SBCTL} sn deploy --isolate-cores --ifname {IFACE}"],
             check=True)
    ssh_exec(sn_ip, ["sudo reboot"])
    time.sleep(30)
    wait_for_ssh(sn_ip)
    print("SN back after reboot; waiting for SNodeAPI...")
    time.sleep(60)

    cluster_uuid = get_cluster_uuid(mgmt_ip)
    for attempt in range(5):
        try:
            ssh_exec(mgmt_ip, [
                f"{SBCTL} sn add-node {cluster_uuid} {sn_priv_ip}:5000 {IFACE}"
                " --enable-journal-device"
            ], check=True, timeout=1800)
            break
        except RuntimeError:
            if attempt == 4:
                raise
            print(f"  retrying add-node in 30s ({attempt + 2}/5)")
            time.sleep(30)

    sn_list = ssh_exec(mgmt_ip, [f"{SBCTL} sn list"], get_output=True)[0]
    if "online" not in sn_list:
        raise RuntimeError(f"storage node not online:\n{sn_list}")

    ssh_exec(mgmt_ip, [f"{SBCTL} cluster activate {cluster_uuid}"],
             check=True, timeout=2400)
    ssh_exec(mgmt_ip, [f"{SBCTL} pool add pool01 {cluster_uuid}"], check=True)

    meta = {
        "config": config_name,
        "cluster_uuid": cluster_uuid,
        "node_uuid": get_storage_node_uuid(mgmt_ip),
        "mgmt_ip": mgmt_ip,
        "sn_ip": sn_ip,
        "sn_private_ip": sn_priv_ip,
        "instance_ids": [mgmt.id, sn.id],
        "ec": list(cfg["ec"]),
        "branch": BRANCH,
    }
    path = keep_metadata_path or os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        f"cluster_metadata_single_node_{config_name}.json")
    with open(path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"=== deploy DONE: {json.dumps(meta, indent=2)}")
    return meta


def terminate(meta):
    ec2 = boto3.client("ec2", region_name=REGION)
    print(f"Terminating {meta['instance_ids']}")
    ec2.terminate_instances(InstanceIds=meta["instance_ids"])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", choices=sorted(CONFIGS), required=True)
    ap.add_argument("--metadata", help="metadata output path")
    args = ap.parse_args()
    deploy(args.config, args.metadata)


if __name__ == "__main__":
    sys.exit(main())
