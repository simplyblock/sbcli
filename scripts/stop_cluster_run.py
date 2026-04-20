#!/usr/bin/env python3
"""Stop a running cluster test: recycle bad/all nodes, replace clients, delete lvols.

Reads a cluster metadata JSON (e.g. tests/perf/cluster_metadata_base.json) and:

  1. Inspect cluster + storage-node status via `sbctl`.
       - cluster suspended -> force-shutdown ALL nodes, then restart ALL.
       - cluster active    -> force-shutdown only nodes not in 'online' state,
                              then restart them.
  2. Per client in metadata: launch a replacement EC2 instance (same AMI/type/
     key/subnet and multipath ENIs), wait for SSH, install nvme-cli+fio,
     modprobe nvme-tcp, update the metadata JSON with the new IPs, then
     terminate the old instance.
  3. Delete every lvol in the cluster.
  4. Wait for storage nodes to come back online; only call cluster activate if
     the cluster did not auto-activate.

Run this ON THE MANAGEMENT NODE — `sbctl` is invoked locally; `aws ec2` and ssh
to the new client run locally too.

Usage:
  sudo python3 stop_cluster_run.py <metadata.json> [--key /path/key.pem]
                                                   [--user ec2-user]
                                                   [--region us-east-1]
                                                   [--restart-timeout 900]
                                                   [--skip-client-replace]
                                                   [--skip-lvol-delete]
                                                   [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

SBCTL = os.environ.get("SBCTL_BIN", "/usr/local/bin/sbctl")

RETRY_MARKERS = (
    "migration", "migrat", "rebalanc",
    "active task", "running task",
    "in_progress", "in progress",
)


# --------------------------------------------------------------------------- #
# small helpers
# --------------------------------------------------------------------------- #

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run(argv: list[str], timeout: int = 120, check: bool = False) -> tuple[int, str, str]:
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"{' '.join(argv)} -> rc={proc.returncode}\nstderr: {proc.stderr}")
    return proc.returncode, proc.stdout, proc.stderr


# --------------------------------------------------------------------------- #
# sbctl wrappers (run locally on mgmt)
# --------------------------------------------------------------------------- #

def sbctl(args: list[str], timeout: int = 300, check: bool = False) -> tuple[int, str, str]:
    argv = ["sudo", SBCTL, *args]
    log(f"mgmt: RUN {' '.join(shlex.quote(a) for a in args)}")
    return run(argv, timeout=timeout, check=check)


def sbctl_json(args: list[str], timeout: int = 120) -> Any:
    rc, out, err = sbctl([*args, "--json"], timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"sbctl {' '.join(args)} --json failed rc={rc}: {err}")
    # sbctl sometimes prefixes warning lines before the JSON body
    blob = out.strip()
    first = min((i for i in (blob.find("["), blob.find("{")) if i >= 0), default=-1)
    if first < 0:
        raise RuntimeError(f"sbctl {' '.join(args)}: no JSON in output:\n{out}")
    return json.loads(blob[first:])


def _cluster_uuid_field(c: dict) -> str:
    return str(c.get("UUID") or c.get("uuid") or c.get("Id") or c.get("id") or "")


def resolve_cluster(cluster_uuid: str) -> tuple[str, str]:
    """Return (resolved_uuid, status) for the given UUID, or auto-pick if there
    is exactly one cluster and the hinted UUID isn't present."""
    clusters = sbctl_json(["cluster", "list"]) or []
    for c in clusters:
        if _cluster_uuid_field(c).lower() == cluster_uuid.lower():
            return _cluster_uuid_field(c), str(c.get("Status", "")).lower()

    # UUID not found — useful diagnostic, then fall back if only one cluster.
    log(f"hinted cluster_uuid {cluster_uuid} not in sbctl cluster list; got:")
    for c in clusters:
        log(f"  {_cluster_uuid_field(c)}  status={c.get('Status', '')}  name={c.get('Name', '')}")
    if len(clusters) == 1:
        only = clusters[0]
        uuid = _cluster_uuid_field(only)
        log(f"only one cluster present — using {uuid}")
        return uuid, str(only.get("Status", "")).lower()
    raise RuntimeError(
        f"cluster {cluster_uuid} not found and {len(clusters)} clusters listed; "
        f"pass --cluster-uuid to pick one explicitly"
    )


def list_nodes() -> list[dict]:
    data = sbctl_json(["sn", "list"])
    out: list[dict] = []
    for n in data or []:
        out.append({
            "uuid":    n["UUID"],
            "status":  str(n.get("Status", "")).lower(),
            "mgmt_ip": n.get("Mgmt IP") or n.get("mgmt_ip") or "",
        })
    return out


def list_lvol_uuids(cluster_uuid: str) -> list[str]:
    # `sbctl lvol list --json` returns rows keyed by "Id" (uppercase I, lowercase d).
    data = sbctl_json(["lvol", "list", "--cluster-id", cluster_uuid])
    uuids: list[str] = []
    for d in data or []:
        uid = d.get("Id") or d.get("ID") or d.get("UUID") or d.get("uuid") or d.get("id")
        if uid:
            uuids.append(uid)
    if not uuids:
        log(f"lvol list returned 0 usable entries; raw payload: {data!r}")
    return uuids


# --------------------------------------------------------------------------- #
# node recycle
# --------------------------------------------------------------------------- #

def force_shutdown(node_uuid: str) -> None:
    """`sbctl sn shutdown --force`, retrying on migration/rebalance blockers."""
    while True:
        rc, out, err = sbctl(["sn", "shutdown", node_uuid, "--force"], timeout=300)
        if rc == 0:
            return
        blob = (out + err).lower()
        if any(m in blob for m in RETRY_MARKERS):
            log(f"force-shutdown {node_uuid} blocked by migration/task; retry in 15s")
            time.sleep(15)
            continue
        raise RuntimeError(f"sn shutdown --force {node_uuid} failed rc={rc}: {err.strip()}")


def restart_node(node_uuid: str) -> None:
    rc, _, err = sbctl(["sn", "restart", node_uuid], timeout=900)
    if rc != 0:
        raise RuntimeError(f"sn restart {node_uuid} failed rc={rc}: {err.strip()}")


def wait_for_all_online(expected: int, timeout: int, poll: int = 10) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            nodes = list_nodes()
        except Exception as exc:
            log(f"sn list failed ({exc}); retry in {poll}s")
            time.sleep(poll)
            continue
        statuses = [(n["uuid"][:8], n["status"]) for n in nodes]
        if len(nodes) == expected and all(s == "online" for _, s in statuses):
            log(f"all {expected} nodes online")
            return
        log(f"waiting for online: {statuses}")
        time.sleep(poll)
    raise RuntimeError(f"timeout after {timeout}s waiting for nodes online")


# --------------------------------------------------------------------------- #
# client
# --------------------------------------------------------------------------- #

SSH_VERBOSE = False


def _ssh_argv(key: str, host: str, user: str) -> list[str]:
    argv = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=15",
        "-o", "ServerAliveInterval=15",
        "-o", "ServerAliveCountMax=3",
        "-i", key,
        f"{user}@{host}",
    ]
    if SSH_VERBOSE:
        argv.insert(1, "-v")
    return argv


def ssh_run_client(host: str, user: str, key: str, command: str, label: str, timeout: int = 120) -> tuple[int, str, str]:
    argv = _ssh_argv(key, host, user) + [f"bash -lc {shlex.quote(command)}"]
    log(f"client[{host}]: RUN {label}")
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        return 124, exc.stdout or "", exc.stderr or "timeout"
    return proc.returncode, proc.stdout, proc.stderr


def ssh_fire_and_forget(host: str, user: str, key: str, command: str, label: str, timeout: int = 15) -> None:
    argv = _ssh_argv(key, host, user) + [f"bash -lc {shlex.quote(command)}"]
    log(f"client[{host}]: FIRE {label}")
    try:
        subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        pass


# Verified cleanup: kills fio, unmounts non-system NVMe mounts, nvme-disconnect-all,
# and FAILS loudly if anything didn't take. Exits 0 only on success so the caller
# can decide whether to reboot. Shell heredoc script runs under `bash -lc`.
CLIENT_CLEANUP = r"""
set -u
fail=0

system_mount() {
  case "$1" in
    /|/boot|/boot/*|/var|/var/*|/usr|/usr/*|/etc|/etc/*|/tmp|/home|/opt|/srv|/run|/run/*|/sys|/sys/*|/proc|/proc/*|/dev|/dev/*) return 0 ;;
  esac
  return 1
}

echo "--- before ---"
pgrep -a fio || echo 'no fio'
findmnt -nr -o TARGET,SOURCE | awk '$2 ~ /^\/dev\/nvme/' || true
sudo nvme list-subsys 2>/dev/null | grep -i simplyblock || true

echo "--- killing fio ---"
# -x = exact process name; -f would match our own bash since the script
# contains the string "fio" and would kill the ssh session.
sudo pkill -9 -x fio || true
for i in 1 2 3 4 5 6 7 8 9 10; do
  pgrep -x fio >/dev/null || break
  sleep 1
done
if pgrep -x fio >/dev/null; then
  echo "ERROR: fio still running after SIGKILL:"
  pgrep -a fio
  fail=1
else
  echo "  fio stopped"
fi

echo "--- umount nvme mounts ---"
awk '$1 ~ /^\/dev\/nvme/ {print $1" "$2}' /proc/mounts | while read -r dev mp; do
  if system_mount "$mp"; then
    echo "  skip $mp (system)"
    continue
  fi
  echo "  umount $mp ($dev)"
  if sudo umount "$mp" 2>/dev/null; then
    echo "    umount ok"
  elif sudo umount -l "$mp"; then
    echo "    umount -l (lazy) ok"
  else
    echo "    ERROR: umount $mp failed"
    exit 98   # exits the subshell only; parent still checks /proc/mounts below
  fi
done
# Verify no non-system NVMe mounts remain
remaining="$(awk '$1 ~ /^\/dev\/nvme/ {print $2}' /proc/mounts | while read -r mp; do
  case "$mp" in
    /|/boot|/boot/*|/var|/var/*|/usr|/usr/*|/etc|/etc/*|/tmp|/home|/opt|/srv|/run|/run/*|/sys|/sys/*|/proc|/proc/*|/dev|/dev/*) ;;
    *) echo "$mp" ;;
  esac
done)"
if [ -n "$remaining" ]; then
  echo "ERROR: non-system NVMe mount points still present:"
  echo "$remaining"
  fail=1
else
  echo "  all non-system NVMe mounts cleared"
fi

echo "--- nvme disconnect-all ---"
if sudo nvme disconnect-all; then
  echo "  nvme disconnect-all ok"
else
  echo "  ERROR: nvme disconnect-all rc=$?"
  fail=1
fi
if sudo nvme list-subsys 2>/dev/null | grep -iq simplyblock; then
  echo "ERROR: simplyblock nvme subsystems still connected:"
  sudo nvme list-subsys 2>&1 | grep -i simplyblock
  fail=1
else
  echo "  no simplyblock subsystems remain"
fi

echo "--- after ---"
pgrep -a fio || echo 'no fio'
findmnt -nr -o TARGET,SOURCE | awk '$2 ~ /^\/dev\/nvme/' || true
sudo nvme list-subsys 2>/dev/null | grep -i simplyblock || true

if [ $fail -eq 0 ]; then
  echo CLEANUP_OK
  exit 0
else
  echo CLEANUP_FAILED
  exit 1
fi
""".strip()

# systemd-run --scope detaches from the ssh session so the SIGHUP doesn't race
# the kernel going down.
CLIENT_REBOOT = (
    "sudo systemd-run --scope --quiet bash -c 'sleep 1; reboot -f' "
    "> /dev/null 2>&1 < /dev/null &"
)


def reboot_client(host: str, user: str, key: str) -> None:
    """Verify-then-reboot: run the strict cleanup script; only reboot if it
    reports CLEANUP_OK. Raises on failure."""
    rc, out, err = ssh_run_client(host, user, key, CLIENT_CLEANUP, "cleanup", timeout=300)
    log(f"client[{host}] cleanup rc={rc}")
    log(f"client[{host}] stdout:\n{out.rstrip() or '(empty)'}")
    if err.strip():
        log(f"client[{host}] stderr:\n{err.rstrip()}")
    if rc != 0 or "CLEANUP_OK" not in out:
        raise RuntimeError(
            f"client {host} cleanup did NOT complete cleanly (rc={rc}); refusing to reboot"
        )
    log(f"client[{host}]: cleanup verified, issuing reboot")
    ssh_fire_and_forget(host, user, key, CLIENT_REBOOT, "reboot", timeout=15)


# --------------------------------------------------------------------------- #
# AWS EC2 helpers (CLI subprocess)
# --------------------------------------------------------------------------- #

import shutil as _shutil


def _require_aws_cli() -> None:
    if _shutil.which("aws") is None:
        raise RuntimeError(
            "`aws` CLI not found on PATH. Install it on this mgmt node, e.g.:\n"
            "  sudo dnf install -y awscli\n"
            "  # or (AWS CLI v2):\n"
            "  curl -o /tmp/awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip && "
            "unzip -q /tmp/awscliv2.zip -d /tmp && sudo /tmp/aws/install\n"
            "Then confirm credentials with: aws sts get-caller-identity"
        )


def aws_ec2(args: list[str], region: str, timeout: int = 180, check: bool = True) -> Any:
    _require_aws_cli()
    argv = ["aws", "ec2", *args, "--region", region, "--output", "json"]
    log(f"aws ec2 {' '.join(args)}")
    proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise RuntimeError(f"aws ec2 {' '.join(args)} rc={proc.returncode}\nstderr: {proc.stderr.strip()}")
    if not proc.stdout.strip():
        return {}
    return json.loads(proc.stdout)


def describe_instance(instance_id: str, region: str) -> dict:
    data = aws_ec2(["describe-instances", "--instance-ids", instance_id], region)
    try:
        return data["Reservations"][0]["Instances"][0]
    except (KeyError, IndexError) as exc:
        raise RuntimeError(f"could not describe {instance_id}: {data!r}") from exc


def wait_instance_running(instance_id: str, region: str, timeout: int = 600, poll: int = 10) -> dict:
    t0 = time.time()
    while time.time() - t0 < timeout:
        inst = describe_instance(instance_id, region)
        state = inst["State"]["Name"]
        if state == "running":
            return inst
        if state in ("terminated", "shutting-down"):
            raise RuntimeError(f"{instance_id} is {state}")
        log(f"waiting for {instance_id}: state={state}")
        time.sleep(poll)
    raise RuntimeError(f"{instance_id} did not reach running within {timeout}s")


def wait_ssh_ready(host: str, user: str, key: str, timeout: int = 600, poll: int = 10) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            proc = subprocess.run(
                _ssh_argv(key, host, user) + ["echo ok"],
                capture_output=True, text=True, timeout=15,
            )
            if proc.returncode == 0 and "ok" in proc.stdout:
                log(f"ssh ready on {host}")
                return
        except subprocess.TimeoutExpired:
            pass
        log(f"ssh not ready on {host}, retry in {poll}s")
        time.sleep(poll)
    raise RuntimeError(f"ssh never came up on {host} within {timeout}s")


def install_client_packages(host: str, user: str, key: str) -> None:
    cmd = (
        "set -e; "
        "if command -v dnf >/dev/null 2>&1; then "
        "  sudo dnf install -y nvme-cli fio xfsprogs; "
        "elif command -v yum >/dev/null 2>&1; then "
        "  sudo yum install -y nvme-cli fio xfsprogs; "
        "else "
        "  sudo apt-get update && sudo apt-get install -y nvme-cli fio xfsprogs; "
        "fi; "
        "sudo modprobe nvme-tcp; "
        "sudo modprobe nvme-fabrics || true; "
        "lsmod | grep -E '^nvme_tcp' && echo nvme-tcp-loaded"
    )
    rc, out, err = ssh_run_client(host, user, key, cmd, "install nvme-cli/fio + modprobe nvme-tcp", timeout=900)
    log(f"client[{host}] install stdout:\n{out.rstrip() or '(empty)'}")
    if err.strip():
        log(f"client[{host}] install stderr:\n{err.rstrip()}")
    if rc != 0:
        raise RuntimeError(f"package install failed on {host}: rc={rc}")


def launch_replacement(src: dict, region: str) -> str:
    """Launch a new instance mirroring src's AMI/type/key and its primary ENI's subnet+SG."""
    primary = next(n for n in src["NetworkInterfaces"] if n["Attachment"]["DeviceIndex"] == 0)
    sgs = [g["GroupId"] for g in primary["Groups"]]
    args = [
        "run-instances",
        "--image-id", src["ImageId"],
        "--instance-type", src["InstanceType"],
        "--key-name", src["KeyName"],
        "--subnet-id", primary["SubnetId"],
        "--security-group-ids", *sgs,
        "--count", "1",
        "--associate-public-ip-address",
        "--tag-specifications",
        "ResourceType=instance,Tags=[{Key=Name,Value=simplyblock-soak-client-replacement}]",
    ]
    data = aws_ec2(args, region, timeout=120)
    new_id = data["Instances"][0]["InstanceId"]
    log(f"launched {new_id}")
    return new_id


def attach_additional_enis(new_id: str, src: dict, region: str) -> dict[str, str]:
    """Mirror src's non-primary ENIs onto new_id. Returns {ethN: private_ip}."""
    data_nics: dict[str, str] = {}
    additional = sorted(
        (e for e in src["NetworkInterfaces"] if e["Attachment"]["DeviceIndex"] != 0),
        key=lambda e: e["Attachment"]["DeviceIndex"],
    )
    for eni in additional:
        di  = eni["Attachment"]["DeviceIndex"]
        sgs = [g["GroupId"] for g in eni["Groups"]]
        create = aws_ec2([
            "create-network-interface",
            "--subnet-id", eni["SubnetId"],
            "--groups", *sgs,
            "--description", f"soak-client-eth{di}",
        ], region)
        new_eni = create["NetworkInterface"]
        new_eni_id = new_eni["NetworkInterfaceId"]
        new_eni_ip = new_eni["PrivateIpAddress"]
        aws_ec2([
            "attach-network-interface",
            "--instance-id", new_id,
            "--network-interface-id", new_eni_id,
            "--device-index", str(di),
        ], region)
        # Ensure the ENI gets destroyed when the instance is terminated.
        aws_ec2([
            "modify-network-interface-attribute",
            "--network-interface-id", new_eni_id,
            "--attachment", f"AttachmentId={_get_attachment_id(new_eni_id, region)},DeleteOnTermination=true",
        ], region)
        data_nics[f"eth{di}"] = new_eni_ip
        log(f"attached eni {new_eni_id} ({new_eni_ip}) as eth{di}")
    return data_nics


def _get_attachment_id(eni_id: str, region: str) -> str:
    data = aws_ec2(["describe-network-interfaces", "--network-interface-ids", eni_id], region)
    return data["NetworkInterfaces"][0]["Attachment"]["AttachmentId"]


def save_metadata(metadata: dict, metadata_path: Path) -> None:
    metadata_path.write_text(json.dumps(metadata, indent=4), encoding="utf-8")
    log(f"metadata saved to {metadata_path}")


def replace_client(client_meta: dict, metadata: dict, metadata_path: Path,
                   user: str, key: str, region: str) -> dict:
    old_id = client_meta["instance_id"]
    log(f"describing current client {old_id}")
    src = describe_instance(old_id, region)

    log(f"launching replacement for {old_id}")
    new_id = launch_replacement(src, region)
    wait_instance_running(new_id, region)

    data_nics = attach_additional_enis(new_id, src, region)

    # Re-describe to pick up public IP that AWS assigns post-boot.
    new_inst = describe_instance(new_id, region)
    primary = next(n for n in new_inst["NetworkInterfaces"] if n["Attachment"]["DeviceIndex"] == 0)
    new_pub = primary.get("Association", {}).get("PublicIp") or new_inst.get("PublicIpAddress", "")
    new_priv = primary["PrivateIpAddress"]
    new_sg = primary["Groups"][0]["GroupId"]
    log(f"new client: id={new_id} public_ip={new_pub} private_ip={new_priv}")

    # Update metadata in place and persist BEFORE terminating old so a crash
    # doesn't leave us IP-less.
    client_meta["instance_id"] = new_id
    client_meta["public_ip"] = new_pub
    client_meta["private_ip"] = new_priv
    client_meta["security_group_id"] = new_sg
    if data_nics:
        client_meta["data_nics"] = data_nics
    save_metadata(metadata, metadata_path)

    log(f"waiting for ssh on {new_pub}")
    wait_ssh_ready(new_pub, user, key)

    log(f"installing nvme-cli, fio, modprobe nvme-tcp on {new_pub}")
    install_client_packages(new_pub, user, key)

    log(f"terminating old client {old_id}")
    aws_ec2(["terminate-instances", "--instance-ids", old_id], region)
    log(f"old client termination initiated")
    return client_meta


# --------------------------------------------------------------------------- #
# lvol cleanup
# --------------------------------------------------------------------------- #

def delete_all_lvols(cluster_uuid: str) -> None:
    uuids = list_lvol_uuids(cluster_uuid)
    log(f"deleting {len(uuids)} lvols")
    for u in uuids:
        rc, _, err = sbctl(["lvol", "delete", u, "--force"], timeout=300)
        if rc != 0:
            log(f"  WARN: lvol delete {u} rc={rc}: {err.strip()}")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("metadata", type=Path, help="cluster metadata JSON")
    ap.add_argument("--key", default=None, help="override SSH key path for clients (JSON's key_path is Windows-only)")
    ap.add_argument("--user", default=None, help="override SSH user for clients (default: metadata['user'] or ec2-user)")
    ap.add_argument("--cluster-uuid", default=None, help="override cluster UUID from the metadata (when the JSON is stale)")
    ap.add_argument("--region", default="us-east-1", help="AWS region for EC2 operations (default us-east-1)")
    ap.add_argument("--restart-timeout", type=int, default=900, help="seconds to wait for nodes to come back online (default 900)")
    ap.add_argument("--client-mode", choices=("reboot", "replace", "none"), default="reboot",
                    help="how to handle each client: 'reboot' (verified cleanup + reboot, no AWS needed), "
                         "'replace' (terminate + relaunch EC2 instance, needs aws CLI), or 'none' (skip). "
                         "Default: reboot.")
    ap.add_argument("--skip-client-replace", "--skip-client-reboot", dest="skip_client_legacy", action="store_true",
                    help="legacy alias for --client-mode none")
    ap.add_argument("--skip-lvol-delete", action="store_true", help="skip the lvol-delete phase")
    ap.add_argument("--dry-run", action="store_true", help="print actions without executing mutating commands")
    ap.add_argument("-v", "--verbose", action="store_true", help="pass -v to ssh calls for diagnostic output")
    args = ap.parse_args()

    global SSH_VERBOSE
    SSH_VERBOSE = args.verbose

    if args.dry_run:
        log("*** DRY RUN — no mutating commands will be executed ***")

    meta = json.loads(args.metadata.read_text(encoding="utf-8"))
    cluster_uuid = args.cluster_uuid or meta["cluster_uuid"]
    clients = meta.get("clients") or []
    user = args.user or meta.get("user") or "ec2-user"
    key = args.key or meta.get("key_path") or ""
    if args.skip_client_legacy:
        args.client_mode = "none"
    if clients and args.client_mode != "none" and not key:
        log("ERROR: no SSH key for clients. Pass --key or put 'key_path' in metadata.")
        return 2
    expected_nodes = len(meta.get("storage_nodes") or []) or None

    # --- 1) inspect ---
    cluster_uuid, status = resolve_cluster(cluster_uuid)
    nodes = list_nodes()
    log(f"cluster {cluster_uuid} status={status}  nodes={len(nodes)}")
    for n in nodes:
        log(f"  node {n['uuid']} status={n['status']} mgmt_ip={n['mgmt_ip']}")

    # --- 2) pick recycle targets ---
    if status == "suspended":
        targets = [n["uuid"] for n in nodes]
        log(f"cluster suspended -> recycle ALL {len(targets)} nodes")
    else:
        targets = [n["uuid"] for n in nodes if n["status"] != "online"]
        log(f"cluster active -> recycle {len(targets)} non-online nodes")

    if args.dry_run:
        log(f"[dry-run] would force-shutdown+restart: {targets}")
    else:
        for u in targets:
            force_shutdown(u)
        for u in targets:
            restart_node(u)

    # --- 3) client handling ---
    if not clients:
        log("no clients in metadata; skipping client phase")
    elif args.client_mode == "none":
        log("client_mode=none: skipping client phase")
    elif args.client_mode == "reboot":
        for c in clients:
            host = c.get("public_ip") or c.get("private_ip")
            if not host:
                log(f"client entry without IP, skipping: {c}")
                continue
            if args.dry_run:
                log(f"[dry-run] would verify-cleanup+reboot {user}@{host}")
                continue
            reboot_client(host, user, key)
    elif args.client_mode == "replace":
        for c in clients:
            old_id = c.get("instance_id")
            if not old_id:
                log(f"client entry has no instance_id, skipping: {c}")
                continue
            if args.dry_run:
                log(f"[dry-run] would replace client {old_id} via aws ec2 in {args.region}")
                continue
            try:
                replace_client(c, meta, args.metadata, user, key, args.region)
            except Exception as exc:
                log(f"ERROR: client replacement failed for {old_id}: {exc}")
                log("  old instance NOT terminated; fix and re-run with --client-mode none to continue")
                raise

    # --- 4) delete all lvols in the cluster ---
    if not args.skip_lvol_delete:
        if args.dry_run:
            try:
                uuids = list_lvol_uuids(cluster_uuid)
                log(f"[dry-run] would delete {len(uuids)} lvols: {uuids}")
            except Exception as exc:
                log(f"[dry-run] lvol list failed: {exc}")
        else:
            delete_all_lvols(cluster_uuid)
    else:
        log("skipping lvol delete phase (--skip-lvol-delete)")

    # --- 5) wait for nodes online ---
    if args.dry_run:
        log(f"[dry-run] would wait up to {args.restart_timeout}s for all nodes online")
    else:
        wait_for_all_online(expected_nodes or len(nodes), timeout=args.restart_timeout)

    # --- 6) reactivate cluster (only if not already active) ---
    if args.dry_run:
        log(f"[dry-run] would check cluster status and run: sbctl cluster activate {cluster_uuid} if not active")
    else:
        # Re-check status after nodes are back online. The cluster often
        # auto-activates; only call activate when it's still suspended/degraded.
        try:
            _, current_status = resolve_cluster(cluster_uuid)
        except Exception as exc:
            current_status = ""
            log(f"WARN: could not re-check cluster status: {exc}")
        log(f"cluster status after restarts: {current_status!r}")
        if current_status == "active":
            log("cluster already active — skipping activate call")
        else:
            rc, out, err = sbctl(["cluster", "activate", cluster_uuid], timeout=180)
            if rc == 0:
                log("cluster activate ok")
            else:
                log(f"WARN: cluster activate rc={rc}: {(err.strip() or out.strip())}")

    log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
