#!/usr/bin/env python3
"""
probe_nvme_queues.py — Check NVMe I/O queue pair support on various GCP instance types.

The simplyblock SPDK stack needs 3 I/O queue pairs per NVMe controller:
  qid:1  data alceml worker thread
  qid:2  RAID superblock probe (transient, during init)
  qid:3  JM alceml worker thread (uses both SSDs' queues simultaneously)

c3d-standard-30-lssd bundled SSDs only report NCQR=2 → cluster init fails.
This script probes candidate machine types with attached NVMe local SSDs to
find which ones report NCQR >= 3.

Usage:
    python probe_nvme_queues.py

Cleanup:  instances are deleted automatically after probing.
Cost:     each probe runs a small VM for ~3-5 minutes.
"""

import json
import os
import re
import subprocess
import sys
import time

# ── config ────────────────────────────────────────────────────────────────────
PROJECT_ID   = "devmichael"
ZONE         = "us-central1-b"
SSH_KEY_PATH     = r"C:\ssh\gcp_sbcli"
SSH_PUB_KEY_PATH = r"C:\ssh\gcp_sbcli.pub"
SSH_USER         = "sbadmin"
IMAGE_PROJECT    = "rocky-linux-cloud"
IMAGE_FAMILY     = "rocky-linux-9"
CLUSTER_TAG      = "sb-cluster"           # has the sb-allow-ssh firewall rule

_GCLOUD_CMD = ["cmd", "/c", "gcloud"] if sys.platform == "win32" else ["gcloud"]

# Candidates: (machine_type, attached_local_ssd_count)
# All use --local-ssd interface=NVME (NOT bundled SSDs)
# Key requirement: SSDs must appear as separate PCI controllers (not namespaces
# on a shared controller), AND NCQR >= 3.
CANDIDATES = [
    ("n1-standard-2",        1),
    ("n2-standard-2",        1),
    ("n2d-standard-2",       1),
    ("c2-standard-4",        1),
    ("c2d-standard-2",       1),
    ("c3-standard-4-lssd",    1),
    ("c3d-standard-8-lssd",   1),
    ("c3d-standard-16-lssd",  1),
    ("c4-standard-4-lssd",    1),
    ("c4-standard-8-lssd",    1),
    ("c4a-standard-4-lssd",   1),
    ("c4d-standard-8-lssd",   1),
    ("c4d-standard-16-lssd",  1),
]
# ──────────────────────────────────────────────────────────────────────────────


def _gcloud(args, check=True):
    cmd = _GCLOUD_CMD + ["--project", PROJECT_ID, "--quiet"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if check and result.returncode != 0:
        print(f"  [gcloud FAILED] {' '.join(args[:5])}")
        print(f"  stderr: {result.stderr.strip()[:400]}")
        raise RuntimeError(f"gcloud rc={result.returncode}")
    if result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return result.stdout
    return None


def _gcloud_nocheck(args):
    return _gcloud(args, check=False)


def _read_pub_key():
    with open(os.path.expanduser(SSH_PUB_KEY_PATH)) as f:
        return f.read().strip()


def _ssh_meta():
    return f"ssh-keys={SSH_USER}:{_read_pub_key()}"


KNOWN_LOCAL_SSD_COUNTS = {
    "n1-standard-2": 1,
    "n2-standard-2": 1,
    "n2d-standard-2": 1,
    "c2-standard-4": 1,
    "c2d-standard-2": 1,
    "c3-standard-4-lssd": 1,
    "c3d-standard-8": 0,
    "c3d-standard-8-lssd": 1,
    "c3d-standard-16-lssd": 1,
    "c3d-standard-30-lssd": 2,
    "c3d-standard-60-lssd": 4,
    "c3d-standard-90-lssd": 8,
    "c4-standard-4-lssd": 1,
    "c4-standard-8-lssd": 1,
    "c4a-standard-4-lssd": 1,
    "c4d-standard-8-lssd": 1,
    "c4d-standard-16-lssd": 1,
}


def _normalize_machine_type(machine_type):
    return machine_type.strip()


def _validate_candidate(machine_type, local_ssds):
    expected = KNOWN_LOCAL_SSD_COUNTS.get(machine_type)
    if expected is not None and expected != local_ssds:
        raise ValueError(
            f"{machine_type} requires {expected} local SSD(s), "
            f"but CANDIDATES requests {local_ssds}"
        )


def _boot_disk_type(machine_type):
    if machine_type.startswith(("c4-", "c4a-", "c4d-")):
        return "hyperdisk-balanced"
    return "pd-ssd"


# ── instance lifecycle ────────────────────────────────────────────────────────

def launch(name, machine_type, local_ssds):
    machine_type = _normalize_machine_type(machine_type)
    cmd = [
        "compute", "instances", "create", name,
        "--zone", ZONE,
        "--machine-type", machine_type,
        "--image-project", IMAGE_PROJECT,
        "--image-family", IMAGE_FAMILY,
        "--boot-disk-size", "20GB",
        "--boot-disk-type", _boot_disk_type(machine_type),
        "--tags", CLUSTER_TAG,
        "--metadata", _ssh_meta(),
        "--format=json",
    ]
    for _ in range(local_ssds):
        cmd += ["--local-ssd", "interface=NVME"]
    result = _gcloud(cmd)
    instances = result if isinstance(result, list) else [result]
    iface = instances[0]["networkInterfaces"][0]
    ext_ip = iface.get("accessConfigs", [{}])[0].get("natIP", "")
    return ext_ip


def delete(name):
    _gcloud_nocheck([
        "compute", "instances", "delete", name,
        "--zone", ZONE, "--delete-disks=all",
    ])


# ── SSH ───────────────────────────────────────────────────────────────────────

def wait_ssh(ip, timeout=180):
    import paramiko
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=SSH_USER,
                        key_filename=os.path.expanduser(SSH_KEY_PATH),
                        timeout=5, banner_timeout=10,
                        allow_agent=False, look_for_keys=False)
            ssh.close()
            return True
        except Exception:
            time.sleep(3)
    return False


def ssh_run(ip, cmd):
    import paramiko
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=SSH_USER,
                key_filename=os.path.expanduser(SSH_KEY_PATH),
                allow_agent=False, look_for_keys=False)
    _, stdout, stderr = ssh.exec_command(cmd, timeout=120)
    out = stdout.read().decode()
    err = stderr.read().decode()
    rc = stdout.channel.recv_exit_status()
    ssh.close()
    return rc, out, err


# ── probe ─────────────────────────────────────────────────────────────────────

PROBE_CMDS = """
sudo dnf install -y nvme-cli pciutils 2>&1 | tail -3
echo '=== PCI NVMe devices (class 0108) ==='
lspci -Dnn | grep '0108' || echo '  (none found)'
echo '=== block devices ==='
lsblk -o NAME,TYPE,SIZE,ROTA,TRAN
echo '=== nvme list ==='
sudo nvme list
echo '=== nvme controllers (separate PCI addrs?) ==='
for ctrl in /sys/class/nvme/nvme*; do
  name=$(basename $ctrl)
  addr=$(cat $ctrl/address 2>/dev/null || echo unknown)
  nspath=$(ls -d $ctrl/${name}n* 2>/dev/null | head -3 | tr '\n' ' ')
  echo "  $name  PCI=$addr  namespaces=$nspath"
done
echo '=== nvme id-ctrl per controller ==='
for dev in $(ls /dev/nvme[0-9] 2>/dev/null); do
  echo "--- $dev ---"
  sudo nvme id-ctrl "$dev" | grep -E 'nsqr|ncqr|sqes|cqes|nn '
done
echo '=== nvme queue feature (negotiated) per controller ==='
for dev in $(ls /dev/nvme[0-9] 2>/dev/null); do
  echo "--- $dev ---"
  sudo nvme get-feature "$dev" -f 0x07 -H 2>/dev/null || true
done
"""


def probe(machine_type, local_ssds):
    machine_type = _normalize_machine_type(machine_type)
    _validate_candidate(machine_type, local_ssds)
    name = "probe-nvme-" + machine_type.replace("-", "")
    print(f"\n{'='*60}")
    print(f"  Probing: {machine_type}  ({local_ssds} NVMe local SSDs)")
    print(f"  Instance: {name}")
    print(f"{'='*60}")

    # Launch
    try:
        print(f"  [1/4] Launching...")
        ext_ip = launch(name, machine_type, local_ssds)
        print(f"  External IP: {ext_ip}")
    except RuntimeError as e:
        print(f"  SKIP: launch failed — {e}")
        return {"machine_type": machine_type, "error": str(e)}

    try:
        # Wait for SSH
        print(f"  [2/4] Waiting for SSH...")
        if not wait_ssh(ext_ip):
            raise RuntimeError("SSH timed out")
        print(f"  SSH ready.")

        # Run probe
        print(f"  [3/4] Running probe...")
        rc, out, err = ssh_run(ext_ip, PROBE_CMDS)
        if rc != 0:
            print(f"  Probe failed (rc={rc}): {err[:200]}")
        print(out)

        # Parse queue counts from the runtime "Number of Queues" feature output.
        controller_models = {}
        in_nvme_list = False
        current_dev = None
        queue_values = []

        for line in out.splitlines():
            if line.startswith("=== nvme list ==="):
                in_nvme_list = True
                continue
            if in_nvme_list and line.startswith("==="):
                in_nvme_list = False
            if in_nvme_list:
                match = re.match(r"^(/dev/nvme\d+n\d+)\s+\S+\s+\S+\s+(\S+)", line)
                if match:
                    namespace_dev, model = match.groups()
                    controller_dev = re.sub(r"n\d+$", "", namespace_dev)
                    controller_models[controller_dev] = model

            if line.startswith("--- /dev/nvme"):
                current_dev = line.strip("- ")
                continue

            match = re.search(r"NCQA\):\s*(\d+)", line)
            if match and current_dev:
                model = controller_models.get(current_dev, "")
                queue_values.append((current_dev, match.group(1), model))

        return {"machine_type": machine_type, "queues": queue_values, "output": out}

    finally:
        print(f"  [4/4] Deleting {name}...")
        delete(name)
        print(f"  Deleted.")


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    print("NVMe I/O queue pair probe")
    print("Need NCQR >= 3 per controller for simplyblock SPDK")
    print()

    results = []
    for machine_type, local_ssds in CANDIDATES:
        try:
            r = probe(machine_type, local_ssds)
        except Exception as e:
            r = {"machine_type": machine_type.strip(), "error": str(e)}
        results.append(r)

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"{'Machine type':<28}  {'Controller':<15}  {'NCQA':<6}  {'OK (>=3)?'}")
    print("-"*70)
    for r in results:
        mt = r["machine_type"]
        if "error" in r:
            print(f"{mt:<28}  FAILED: {r['error'][:30]}")
            continue
        local_queues = [(dev, q) for dev, q, model in r.get("queues", [])
                        if "pd" not in model.lower()]
        if not local_queues:
            print(f"{mt:<28}  (no local-SSD queue data found)")
            continue
        for dev, ncqr in local_queues:
            try:
                n = int(ncqr)
                ok = "YES ✓" if n >= 3 else f"NO  ✗ (only {n})"
            except ValueError:
                ok = f"? ({ncqr})"
            print(f"{mt:<28}  {dev:<15}  {ncqr:<6}  {ok}")

    print()
    suitable = [r["machine_type"] for r in results
                if not r.get("error")
                and r.get("queues")
                and all(int(v) >= 3
                        for _, v, model in r["queues"]
                        if v.isdigit() and "pd" not in model.lower())]
    if suitable:
        print(f"Suitable machine types: {', '.join(suitable)}")
    else:
        print("No suitable machine types found in this batch.")


if __name__ == "__main__":
    main()
