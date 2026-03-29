#!/usr/bin/env python3
"""
End-to-end live volume migration test on AWS.

Deploys a 3-node i3en.2xlarge cluster (ndcs=1, npcs=1), creates a volume,
runs fio on it, takes 6 snapshots at 30s intervals, migrates the volume
to another storage node, and verifies fio stayed alive throughout.

Usage:
    python3 tests/perf/test_lvol_migration_e2e.py             # full run
    python3 tests/perf/test_lvol_migration_e2e.py --teardown   # destroy only
"""

import boto3
import paramiko
import time
import re
import json
import os
import argparse
from concurrent.futures import ThreadPoolExecutor

# --- AWS config (same subnet/SG as perf tests) ---
AMI_ID = "ami-0dfc569a8686b9320"  # Rocky 9 us-east-1
KEY_NAME = "mtes01"
KEY_PATH = os.path.expanduser("~/.ssh/mtes01.pem")
AZ = "us-east-1a"
SUBNET_ID = "subnet-0593459d6b931ee4c"
STORAGE_SG_ID = "sg-02e89a1372e9f39e9"
BRANCH = "feature-lvol-migration"

SN_COUNT = 3
SN_TYPE = "i3en.2xlarge"
MGMT_TYPE = "m6i.2xlarge"
CLIENT_COUNT = 1
CLIENT_TYPE = "m5.xlarge"

USER = "ec2-user"
IFACE = "eth0"
MAX_LVOL = "10"
METADATA_FILE = "migration_test_metadata.json"

ec2 = boto3.resource("ec2", region_name="us-east-1")


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

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
    raise TimeoutError(f"SSH timeout: {ip}")


def ssh_exec(ip, cmds, get_output=False, check=False, timeout=600):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=USER, key_filename=KEY_PATH,
                allow_agent=False, look_for_keys=False)
    results = []
    for cmd in cmds:
        print(f"  [{ip}] $ {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode()
        err = stderr.read().decode()
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
            for line in out.strip().split("\n")[-3:]:
                if line.strip():
                    print(f"    {line}")
    ssh.close()
    return results


# ---------------------------------------------------------------------------
# AWS provisioning
# ---------------------------------------------------------------------------

def launch_instances(count, instance_type, tag):
    print(f"  Launching {count}x {instance_type} ({tag})...")
    return ec2.create_instances(
        ImageId=AMI_ID,
        InstanceType=instance_type,
        MinCount=count,
        MaxCount=count,
        KeyName=KEY_NAME,
        NetworkInterfaces=[{
            "DeviceIndex": 0,
            "SubnetId": SUBNET_ID,
            "Groups": [STORAGE_SG_ID],
            "AssociatePublicIpAddress": True,
        }],
        BlockDeviceMappings=[{
            "DeviceName": "/dev/sda1",
            "Ebs": {"VolumeSize": 30, "DeleteOnTermination": True, "VolumeType": "gp3"},
        }],
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [
                {"Key": "Name", "Value": f"SB-MigTest-{tag}"},
                {"Key": "test", "Value": "lvol-migration"},
            ],
        }],
    )


def terminate_all():
    """Terminate all instances tagged test=lvol-migration."""
    ec2_client = boto3.client("ec2", region_name="us-east-1")
    resp = ec2_client.describe_instances(Filters=[
        {"Name": "tag:test", "Values": ["lvol-migration"]},
        {"Name": "instance-state-name", "Values": ["running", "stopped", "pending"]},
    ])
    ids = []
    for r in resp["Reservations"]:
        for i in r["Instances"]:
            ids.append(i["InstanceId"])
    if ids:
        print(f"Terminating {len(ids)} instances: {ids}")
        ec2_client.terminate_instances(InstanceIds=ids)
        print("Termination initiated.")
    else:
        print("No migration-test instances found.")


def parse_uuid(text):
    m = re.search(r"([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})", text)
    return m.group(1) if m else None


def parse_all_uuids(text):
    return re.findall(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}", text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--teardown", action="store_true", help="Destroy cluster only")
    parser.add_argument("--skip-deploy", action="store_true",
                        help="Skip deployment, load metadata from file")
    args = parser.parse_args()

    if args.teardown:
        terminate_all()
        return

    if args.skip_deploy:
        with open(METADATA_FILE) as f:
            meta = json.load(f)
        mgmt_ip = meta["mgmt"]["public_ip"]
        sn_ips = [s["public_ip"] for s in meta["storage_nodes"]]
        sn_priv_ips = [s["private_ip"] for s in meta["storage_nodes"]]
        client_ip = meta["clients"][0]["public_ip"]
        cluster_uuid = meta["cluster_uuid"]
    else:
        # ====== PHASE 1: Launch EC2 instances ======
        print("\n=== PHASE 1: Launching EC2 instances ===")
        mgmt_instances = launch_instances(1, MGMT_TYPE, "Mgmt")
        sns = launch_instances(SN_COUNT, SN_TYPE, "Storage")
        clients = launch_instances(CLIENT_COUNT, CLIENT_TYPE, "Client")

        all_inst = mgmt_instances + sns + clients
        print(f"Waiting for {len(all_inst)} instances to start...")
        for inst in all_inst:
            inst.wait_until_running()
            inst.reload()

        mgmt_ip = mgmt_instances[0].public_ip_address
        sn_ips = [i.public_ip_address for i in sns]
        sn_priv_ips = [i.private_ip_address for i in sns]
        client_ip = clients[0].public_ip_address

        all_ips = [mgmt_ip] + sn_ips + [client_ip]
        for ip in all_ips:
            wait_for_ssh(ip)

        # ====== PHASE 2: Install sbcli on all nodes ======
        print("\n=== PHASE 2: Installing sbcli ===")
        install_cmds = [
            "sudo dnf install git python3-pip nvme-cli -y",
            "sudo /usr/bin/python3 -m pip install --upgrade pip setuptools wheel",
            "sudo /usr/bin/python3 -m pip install ruamel.yaml",
            f"sudo pip install git+https://github.com/simplyblock-io/sbcli@{BRANCH}"
            " --upgrade --force --ignore-installed requests",
        ]
        setup_ips = [mgmt_ip] + sn_ips
        with ThreadPoolExecutor(max_workers=len(setup_ips)) as pool:
            futs = [pool.submit(ssh_exec, ip, install_cmds, check=True) for ip in setup_ips]
            for f in futs:
                f.result()
        print("sbcli installed on all nodes.")

        # ====== PHASE 3: Create cluster (ndcs=1, npcs=1) ======
        print("\n=== PHASE 3: Creating cluster ===")
        ssh_exec(mgmt_ip, [
            "sudo /usr/local/bin/sbctl cluster create"
            " --data-chunks-per-stripe 1 --parity-chunks-per-stripe 1"
        ], check=True)

        cluster_list = ssh_exec(mgmt_ip,
                                ["sudo /usr/local/bin/sbctl cluster list"],
                                get_output=True)[0]
        cluster_uuid = parse_uuid(cluster_list)
        if not cluster_uuid:
            raise RuntimeError("Could not find cluster UUID")
        print(f"Cluster UUID: {cluster_uuid}")

        # ====== PHASE 4: Configure + deploy storage nodes ======
        print("\n=== PHASE 4: Storage node setup ===")
        with ThreadPoolExecutor(max_workers=SN_COUNT) as pool:
            futs = [pool.submit(ssh_exec, ip, [
                f"sudo /usr/local/bin/sbctl sn configure --max-lvol {MAX_LVOL}",
            ], check=True) for ip in sn_ips]
            for f in futs:
                f.result()

        with ThreadPoolExecutor(max_workers=SN_COUNT) as pool:
            futs = [pool.submit(ssh_exec, ip, [
                f"sudo /usr/local/bin/sbctl sn deploy --isolate-cores --ifname {IFACE}",
            ], check=True) for ip in sn_ips]
            for f in futs:
                f.result()

        # Reboot storage nodes
        print("Rebooting storage nodes...")
        with ThreadPoolExecutor(max_workers=SN_COUNT) as pool:
            [pool.submit(ssh_exec, ip, ["sudo reboot"]) for ip in sn_ips]
        time.sleep(30)
        for ip in sn_ips:
            wait_for_ssh(ip)
        print("Waiting 60s for SPDK containers...")
        time.sleep(60)

        # ====== PHASE 5: Add nodes + activate ======
        print("\n=== PHASE 5: Add nodes & activate ===")
        for priv_ip in sn_priv_ips:
            for attempt in range(5):
                try:
                    ssh_exec(mgmt_ip, [
                        f"sudo /usr/local/bin/sbctl sn add-node {cluster_uuid}"
                        f" {priv_ip}:5000 {IFACE}"
                        f" --data-nics eth0"
                    ], check=True)
                    break
                except RuntimeError:
                    if attempt < 4:
                        print(f"  Retrying add-node {priv_ip} in 30s...")
                        time.sleep(30)
                    else:
                        raise

        sn_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl sn list"],
                           get_output=True)[0]
        print(sn_list)

        time.sleep(10)
        ssh_exec(mgmt_ip, [
            f"sudo /usr/local/bin/sbctl cluster activate {cluster_uuid}"
        ], check=True)
        print("Cluster activated.")

        # Create pool
        ssh_exec(mgmt_ip, [
            f"sudo /usr/local/bin/sbctl pool add pool01 {cluster_uuid}"
        ], check=True)

        # ====== Prep client ======
        print("\n=== Preparing client ===")
        ssh_exec(client_ip, [
            "sudo dnf install nvme-cli fio xfsprogs libaio libaio-devel -y",
            "sudo modprobe nvme-tcp",
            "echo 'nvme-tcp' | sudo tee /etc/modules-load.d/nvme-tcp.conf",
        ], check=True)

        # Save metadata
        meta = {
            "mgmt": {
                "instance_id": mgmt_instances[0].id,
                "public_ip": mgmt_ip,
                "private_ip": mgmt_instances[0].private_ip_address,
            },
            "storage_nodes": [
                {"instance_id": sns[i].id, "public_ip": sn_ips[i],
                 "private_ip": sn_priv_ips[i]}
                for i in range(SN_COUNT)
            ],
            "clients": [{
                "instance_id": clients[0].id,
                "public_ip": client_ip,
                "private_ip": clients[0].private_ip_address,
            }],
            "cluster_uuid": cluster_uuid,
        }
        with open(METADATA_FILE, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"Metadata saved to {METADATA_FILE}")

    # ==================================================================
    # TEST PHASE: Volume, FIO, Snapshots, Migration
    # ==================================================================

    print("\n=== TEST: Get storage node UUIDs ===")
    sn_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl sn list"],
                       get_output=True)[0]
    parse_all_uuids(sn_list)
    # Filter to only the node UUIDs (first UUID on each table row)
    node_uuids = []
    for line in sn_list.splitlines():
        parts = [p.strip() for p in line.split("|")]
        if len(parts) > 1:
            u = parse_uuid(parts[1])
            if u:
                node_uuids.append(u)
    print(f"Storage node UUIDs: {node_uuids}")
    if len(node_uuids) < 2:
        raise RuntimeError("Need at least 2 storage nodes for migration test")

    src_node = node_uuids[0]
    tgt_node = node_uuids[1]
    print(f"  Source node: {src_node}")
    print(f"  Target node: {tgt_node}")

    # --- Create volume ---
    print("\n=== TEST: Create volume ===")
    import random
    vol_name = f"lvol_mig_{random.randint(1000,9999)}"
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl lvol add {vol_name} 10G pool01"
    ], check=True)

    lvol_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl lvol list"],
                         get_output=True)[0]
    lvol_uuid = None
    lvol_nqn = None
    for line in lvol_list.splitlines():
        if vol_name in line:
            parts = [p.strip() for p in line.split("|")]
            lvol_uuid = parse_uuid(parts[1]) if len(parts) > 1 else None
            # Find NQN column
            for p in parts:
                if p.startswith("nqn."):
                    lvol_nqn = p
                    break
            break
    if not lvol_uuid:
        raise RuntimeError("Could not find lvol UUID")
    print(f"  LVol UUID: {lvol_uuid}")
    print(f"  LVol NQN:  {lvol_nqn}")

    # --- Get connect string and connect from client ---
    print("\n=== TEST: Connect volume to client ===")
    connect_out = ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl lvol connect {lvol_uuid}"
    ], get_output=True)[0]
    print(f"  Connect command: {connect_out.strip()}")

    # The connect command may output multiple nvme connect commands (HA paths)
    connect_cmds = [line.strip() for line in connect_out.strip().splitlines()
                    if line.strip().startswith("sudo nvme connect")]
    for cmd in connect_cmds:
        ssh_exec(client_ip, [cmd], check=True)
    time.sleep(3)

    # Find the NVMe device
    nvme_out = ssh_exec(client_ip, ["sudo nvme list -o json"], get_output=True)[0]
    nvme_data = json.loads(nvme_out)
    nvme_dev = None
    for dev in nvme_data.get("Devices", []):
        if "simplyblock" in dev.get("ModelNumber", "").lower() or \
           "lvol" in dev.get("DevicePath", "").lower():
            nvme_dev = dev["DevicePath"]
            break
    if not nvme_dev:
        # Fallback: find the non-root nvme device
        lsblk = ssh_exec(client_ip, ["lsblk -d -n -o NAME,TYPE"], get_output=True)[0]
        for line in lsblk.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1] == "disk" and parts[0].startswith("nvme"):
                candidate = f"/dev/{parts[0]}"
                if candidate != "/dev/nvme0n1":
                    nvme_dev = candidate
                    break
    if not nvme_dev:
        raise RuntimeError("Could not find NVMe device on client")
    print(f"  NVMe device: {nvme_dev}")

    # --- Format and mount ---
    print("\n=== TEST: Format XFS and mount ===")
    ssh_exec(client_ip, [
        f"sudo mkfs.xfs -f {nvme_dev}",
        "sudo mkdir -p /mnt/migtest",
        f"sudo mount {nvme_dev} /mnt/migtest",
        "df -h /mnt/migtest",
    ], check=True)

    # --- Start fio in background ---
    print("\n=== TEST: Start fio (background, 1 hour) ===")
    ssh_exec(client_ip, ["sudo dnf install libaio -y 2>/dev/null || true"])

    # Start fio via systemd-run so it fully detaches from the SSH session
    ssh_exec(client_ip, [
        "sudo systemd-run --unit=fio-migtest --remain-after-exit"
        " fio --name=lvol_migration_test --directory=/mnt/migtest --direct=1"
        " --rw=randrw --bs=4K --size=1G --numjobs=4 --iodepth=16 --ioengine=libaio"
        " --time_based --runtime=3600 --group_reporting"
        " --output=/tmp/fio_output.log"
    ])
    time.sleep(8)

    # Verify fio is running
    fio_check = ssh_exec(client_ip, ["pgrep fio | wc -l"], get_output=True)[0]
    fio_count = int(fio_check.strip().split('\n')[-1])
    if fio_count == 0:
        ssh_exec(client_ip, ["cat /tmp/fio_stderr.log 2>/dev/null || true",
                              "cat /tmp/fio_output.log 2>/dev/null || true"])
        raise RuntimeError("fio is not running!")
    print(f"  fio running ({fio_count} processes)")

    # --- Take 6 snapshots at 30s intervals ---
    print("\n=== TEST: Taking 6 snapshots (30s intervals) ===")
    snap_uuids = []
    for i in range(6):
        if i > 0:
            print(f"  Waiting 30s before snapshot {i+1}...")
            time.sleep(30)

        # Sync filesystem before snapshot
        ssh_exec(client_ip, ["sudo sync"])

        snap_name = f"snap_mig_{i}"
        ssh_exec(mgmt_ip, [
            f"sudo /usr/local/bin/sbctl snapshot add {lvol_uuid} {snap_name}"
        ], check=True)
        print(f"  Snapshot {i+1}/6: {snap_name}")

        # Get snapshot UUID
        snap_list = ssh_exec(mgmt_ip, ["sudo /usr/local/bin/sbctl snapshot list"],
                             get_output=True)[0]
        for line in snap_list.splitlines():
            if snap_name in line:
                u = parse_uuid(line)
                if u:
                    snap_uuids.append(u)
                    break

    print(f"  Snapshot UUIDs: {snap_uuids}")

    # Verify fio still running
    fio_check = ssh_exec(client_ip, ["pgrep fio | wc -l"], get_output=True)[0]
    print(f"  fio still running: {fio_check.strip().split(chr(10))[-1]} processes")

    # --- Trigger lvol migration ---
    print(f"\n=== TEST: Migrate volume {src_node} → {tgt_node} ===")
    ssh_exec(mgmt_ip, [
        f"sudo /usr/local/bin/sbctl lvol migrate {lvol_uuid} {tgt_node}"
    ], check=True)

    # --- Poll migration status ---
    print("\n=== TEST: Polling migration status ===")
    max_polls = 120  # 10 minutes max
    for poll in range(max_polls):
        mig_list = ssh_exec(mgmt_ip, [
            "sudo /usr/local/bin/sbctl lvol migrate-list"
        ], get_output=True)[0]
        print(f"  Poll {poll+1}: {mig_list.strip().split(chr(10))[-2] if mig_list.strip() else 'empty'}")

        if "done" in mig_list.lower() or "completed" in mig_list.lower():
            print("  Migration COMPLETED!")
            break
        if "failed" in mig_list.lower():
            print("  Migration FAILED!")
            print(mig_list)
            break
        time.sleep(5)
    else:
        print("  Migration did not complete within timeout")

    # --- Verify fio is still running ---
    print("\n=== TEST: Verify fio survived migration ===")
    fio_check = ssh_exec(client_ip, ["pgrep fio | wc -l"], get_output=True)[0]
    fio_count = int(fio_check.strip().split('\n')[-1])
    if fio_count > 0:
        print(f"  SUCCESS: fio still running ({fio_count} processes)")
    else:
        print("  FAILURE: fio is no longer running!")
        fio_log = ssh_exec(client_ip, ["tail -50 /tmp/fio_output.log"],
                           get_output=True)[0]
        print(f"  fio output:\n{fio_log}")

    # --- Show final state ---
    print("\n=== Final state ===")
    ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl lvol list",
        "sudo /usr/local/bin/sbctl snapshot list",
        "sudo /usr/local/bin/sbctl lvol migrate-list",
    ])

    # --- Stop fio ---
    print("\n=== Stopping fio ===")
    ssh_exec(client_ip, ["sudo killall fio || true"])
    ssh_exec(client_ip, ["sudo umount /mnt/migtest || true"])

    # --- Teardown (only on success) ---
    mig_list_final = ssh_exec(mgmt_ip, [
        "sudo /usr/local/bin/sbctl lvol migrate-list"
    ], get_output=True)[0]
    if "done" in mig_list_final.lower() or "completed" in mig_list_final.lower():
        print("\n=== Tearing down cluster (migration succeeded) ===")
        terminate_all()
    else:
        print("\n=== Keeping cluster alive for debugging ===")
        print(f"  Mgmt: {mgmt_ip}")
        print("  To tear down manually: python3 tests/perf/test_lvol_migration_e2e.py --teardown")

    print("\nDONE.")


if __name__ == "__main__":
    main()
