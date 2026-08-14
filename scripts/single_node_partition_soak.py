#!/usr/bin/env python3
"""Single-node lblk soak across the three device configurations.

For each config (2ssd, 2part, 4part — see deploy_single_node_lblk.py):
  1. deploy a fresh 1-node cluster (non-HA, single journal),
  2. create lvols and connect them on the mgmt instance (nvme-tcp client),
  3. lay down a crc32c-verified data region on each volume, then run a
     mixed random-IO workload on a separate region,
  4. gracefully restart the storage node (`sn restart`), wait for the node
     to come back online and the cluster to return to active,
  5. re-run the crc32c verify-only pass over the phase-3 region — data must
     be available and UNCORRUPTED after the restart — plus a short mixed
     workload to prove the volume is writable again,
  6. tear the fleet down (kept running on failure for debugging).

Exit code 0 only if every configured config passes every check.

Usage:
  ./single_node_partition_soak.py                 # all three configs
  ./single_node_partition_soak.py --configs 2part,4part
  ./single_node_partition_soak.py --keep          # never terminate fleets
"""
import argparse
import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from deploy_single_node_lblk import (  # noqa: E402
    CONFIGS, SBCTL, deploy, ssh_exec, terminate,
)

LVOL_COUNT = 2
LVOL_SIZE = "20G"
VERIFY_REGION = "4G"      # crc32c-stamped region checked across the restart
MIX_REGION_OFFSET = "5G"  # mixed workload region, disjoint from the verify one
MIX_RUNTIME_S = 120
NODE_RESTART_TIMEOUT_S = 1200

FIO_COMMON = ("--direct=1 --ioengine=libaio --group_reporting --time_based=0"
              " --randrepeat=0 --thread")


def _uuids(text):
    return re.findall(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text)


def create_and_connect_lvols(meta):
    """Create LVOL_COUNT lvols and nvme-connect them on the mgmt instance.
    Returns [(lvol_uuid, /dev/nvmeXnY), ...]."""
    mgmt = meta["mgmt_ip"]
    ssh_exec(mgmt, ["sudo modprobe nvme-tcp"], check=True)
    lvols = []
    for i in range(LVOL_COUNT):
        name = f"soak_vol{i + 1}"
        ssh_exec(mgmt, [f"{SBCTL} lvol add {name} {LVOL_SIZE} pool01"],
                 check=True, timeout=600)
        out = ssh_exec(mgmt, [f"{SBCTL} lvol list | grep {name}"],
                       get_output=True)[0]
        ids = _uuids(out)
        if not ids:
            raise RuntimeError(f"no uuid for lvol {name}: {out}")
        lvols.append(ids[0])

    devices = []
    for lvol_id in lvols:
        before = set(ssh_exec(
            mgmt, ["ls /dev/nvme*n1 2>/dev/null || true"],
            get_output=True)[0].split())
        connect_cmds = ssh_exec(
            mgmt, [f"{SBCTL} lvol connect {lvol_id}"], get_output=True)[0]
        ran = False
        for line in connect_cmds.splitlines():
            line = line.strip()
            if line.startswith("sudo nvme connect") or line.startswith("nvme connect"):
                cmd = line if line.startswith("sudo") else f"sudo {line}"
                # generous loss tolerance: the restart window must not drop
                # the controller
                if "ctrl-loss-tmo" not in cmd:
                    cmd += " --ctrl-loss-tmo=600"
                ssh_exec(mgmt, [cmd], check=True)
                ran = True
        if not ran:
            raise RuntimeError(f"lvol connect emitted no nvme connect command:"
                               f"\n{connect_cmds}")
        time.sleep(3)
        after = set(ssh_exec(
            mgmt, ["ls /dev/nvme*n1 2>/dev/null || true"],
            get_output=True)[0].split())
        new = sorted(after - before)
        if len(new) != 1:
            raise RuntimeError(f"expected one new nvme device, got {new}")
        devices.append((lvol_id, new[0]))
        print(f"  lvol {lvol_id[:8]} -> {new[0]}")
    return devices


# The verify pass must REPLAY the write job with --verify_only: fio then
# skips the writes and only reads back and checks the crc32c headers it
# wrote. A --rw=read job would not reproduce the same pattern layout and
# would verify nothing, so both jobs share one parameter string.
_FIO_VERIFY_JOB = (f"--name=stamp {FIO_COMMON} --rw=write --bs=256k"
                   f" --iodepth=8 --size={VERIFY_REGION}"
                   " --verify=crc32c --verify_state_save=0")


def fio_verify_write(mgmt, dev):
    ssh_exec(mgmt, [
        f"sudo fio {_FIO_VERIFY_JOB} --filename={dev} --do_verify=0"
    ], check=True, timeout=3600)


def fio_verify_read(mgmt, dev):
    """crc32c verify-only pass over the stamped region — fails on any
    corruption or read error."""
    ssh_exec(mgmt, [
        f"sudo fio {_FIO_VERIFY_JOB} --filename={dev}"
        " --verify_only --verify_fatal=1"
    ], check=True, timeout=3600)


def fio_mixed(mgmt, dev, runtime=MIX_RUNTIME_S):
    ssh_exec(mgmt, [
        f"sudo fio --name=mix {FIO_COMMON} --filename={dev} --rw=randrw"
        f" --rwmixread=70 --bs=16k --iodepth=16"
        f" --offset={MIX_REGION_OFFSET} --size=4G"
        f" --time_based=1 --runtime={runtime}"
    ], check=True, timeout=runtime + 600)


def restart_storage_node(meta):
    mgmt, node_id = meta["mgmt_ip"], meta["node_uuid"]
    print(f"  restarting storage node {node_id[:8]} ...")
    ssh_exec(mgmt, [f"{SBCTL} sn restart {node_id}"], check=True,
             timeout=NODE_RESTART_TIMEOUT_S)
    deadline = time.time() + NODE_RESTART_TIMEOUT_S
    while time.time() < deadline:
        sn_list = ssh_exec(mgmt, [f"{SBCTL} sn list"], get_output=True)[0]
        status = ssh_exec(mgmt, [f"{SBCTL} cluster list"], get_output=True)[0]
        # `sn list` prints the node status lowercase, `cluster list` prints
        # the cluster status uppercase (ACTIVE) — compare case-insensitively.
        if "online" in sn_list.lower() and "active" in status.lower():
            print("  node online, cluster active")
            return
        time.sleep(15)
    raise RuntimeError("storage node did not return to online/active in time")


def check_health(meta):
    mgmt = meta["mgmt_ip"]
    sn_list = ssh_exec(mgmt, [f"{SBCTL} sn list"], get_output=True)[0]
    if "online" not in sn_list:
        raise RuntimeError(f"node not online:\n{sn_list}")
    lvol_list = ssh_exec(mgmt, [f"{SBCTL} lvol list"], get_output=True)[0]
    for i in range(LVOL_COUNT):
        if f"soak_vol{i + 1}" not in lvol_list:
            raise RuntimeError(f"lvol soak_vol{i + 1} missing:\n{lvol_list}")


def run_config(config_name, keep=False):
    print(f"\n########## config {config_name} ##########")
    meta = deploy(config_name)
    try:
        devices = create_and_connect_lvols(meta)

        print("--- phase A: stamp crc32c regions + mixed workload ---")
        for _, dev in devices:
            fio_verify_write(meta["mgmt_ip"], dev)
        for _, dev in devices:
            fio_mixed(meta["mgmt_ip"], dev)
        for _, dev in devices:
            fio_verify_read(meta["mgmt_ip"], dev)
        print("--- phase A OK (pre-restart data verified) ---")

        print("--- phase B: node restart ---")
        restart_storage_node(meta)
        check_health(meta)

        print("--- phase C: post-restart integrity ---")
        time.sleep(10)  # allow nvme reconnect to settle
        for _, dev in devices:
            fio_verify_read(meta["mgmt_ip"], dev)
        for _, dev in devices:
            fio_mixed(meta["mgmt_ip"], dev, runtime=60)
        check_health(meta)
        print(f"--- config {config_name} PASSED ---")
        result = True
    except Exception as e:
        print(f"!!! config {config_name} FAILED: {e}")
        print(f"    fleet kept for debugging: {json.dumps(meta, indent=2)}")
        return False, meta
    if not keep:
        terminate(meta)
    return result, meta


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--configs", default=",".join(sorted(CONFIGS)),
                    help="comma-separated subset of: " + ",".join(sorted(CONFIGS)))
    ap.add_argument("--keep", action="store_true",
                    help="keep fleets running even on success")
    args = ap.parse_args()

    configs = [c.strip() for c in args.configs.split(",") if c.strip()]
    unknown = set(configs) - set(CONFIGS)
    if unknown:
        ap.error(f"unknown config(s): {sorted(unknown)}")

    results = {}
    for config_name in configs:
        ok, _meta = run_config(config_name, keep=args.keep)
        results[config_name] = ok

    print("\n========== single-node partition soak summary ==========")
    for config_name, ok in results.items():
        print(f"  {config_name}: {'PASS' if ok else 'FAIL'}")
    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
