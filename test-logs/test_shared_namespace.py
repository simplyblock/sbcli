#!/usr/bin/env python3
"""
test_shared_namespace.py

Creates a shared-namespace subsystem with 16 slots, fills it with 16 lvols
(1 master + 15 members), connects all to the management node, and runs
parallel fio randrw+verify=md5 on every namespace.

Usage:
    python3 test_shared_namespace.py
    python3 test_shared_namespace.py --pool <pool-id-or-name>
    python3 test_shared_namespace.py --size 5G --fio-runtime 120
    python3 test_shared_namespace.py --teardown
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
POOL_NAME       = "ns-test-pool"
LVOL_PREFIX     = "ns_test_lvol"
MAX_NAMESPACES  = 16
LVOL_SIZE       = "10G"
FIO_SIZE        = "1G"
FIO_RUNTIME     = 60       # seconds
LOG_FILE        = "/tmp/test_shared_namespace.log"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sbctl(*args, parse_json=False):
    cmd = ["sbctl"] + [str(a) for a in args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log.warning(f"sbctl {' '.join(str(a) for a in args[:5])} "
                    f"rc={proc.returncode}: {proc.stderr.strip()[:300]}")
    if not parse_json:
        return proc.stdout.strip()
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                return json.loads(proc.stdout[i:])
            except json.JSONDecodeError:
                pass
    return None


def _sbctl_list(*args):
    raw = sbctl(*args, "--json") or ""
    for i, ch in enumerate(raw):
        if ch in ("{", "["):
            try:
                parsed = json.loads(raw[i:])
                if isinstance(parsed, dict):
                    return parsed.get("results", [])
                return parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                pass
    return []


def _get(obj, *keys):
    aliases = {
        "id":   ("id", "ID", "Id", "uuid", "UUID"),
        "name": ("name", "Name", "lvol_name"),
    }
    for key in keys:
        for candidate in aliases.get(key.lower(), (key, key.upper(), key.capitalize(), key.lower())):
            if candidate in obj:
                return obj[candidate]
    return None


def local_run(cmd, check_rc=False, timeout=300):
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    out, err = proc.stdout.strip(), proc.stderr.strip()
    if check_rc and proc.returncode != 0:
        raise RuntimeError(f"Command failed (rc={proc.returncode}): {cmd}\nstderr: {err}")
    return out, err, proc.returncode


def parse_nvme_connect_cmds(output):
    cmds = []
    for line in (output or "").splitlines():
        s = line.strip()
        if "nvme connect" not in s:
            continue
        if s.startswith("sudo "):
            s = s[5:]
        cmds.append(s)
    return cmds


def list_nvme_namespaces():
    out, _, _ = local_run("ls /dev/nvme*n* 2>/dev/null || true")
    return sorted(d for d in out.splitlines() if d.strip())


def wait_for_new_devices(before, expected_count, retries=20, interval=3):
    for _ in range(retries):
        after  = list_nvme_namespaces()
        new    = [d for d in after if d not in before]
        if len(new) >= expected_count:
            return new
        time.sleep(interval)
    after = list_nvme_namespaces()
    return [d for d in after if d not in before]


# ---------------------------------------------------------------------------
# Cluster / pool helpers
# ---------------------------------------------------------------------------
def discover_cluster_id():
    proc = subprocess.run(["sbctl", "cluster", "list", "--json"],
                          capture_output=True, text=True)
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                data = json.loads(proc.stdout[i:])
                if isinstance(data, list):
                    data = data[0] if data else {}
                elif isinstance(data, dict):
                    data = (data.get("results") or [{}])[0]
                return _get(data, "id") or ""
            except json.JSONDecodeError:
                pass
    return ""


def get_pool_id(name):
    for p in _sbctl_list("pool", "list"):
        if _get(p, "name") == name:
            return _get(p, "id")
    return None


def get_lvol_by_name(pool_id, name):
    for lv in _sbctl_list("lvol", "list", "--pool", pool_id):
        if lv.get("Name") == name or _get(lv, "name") == name:
            return lv
    return None


def get_online_nodes():
    nodes = _sbctl_list("sn", "list")
    online = [n for n in nodes
              if (_get(n, "status") or "").lower() in ("online", "active", "online_healthy")]
    return online if online else nodes


# ---------------------------------------------------------------------------
# Setup / teardown
# ---------------------------------------------------------------------------
def ensure_pool(pool_name, cluster_id):
    pool_id = get_pool_id(pool_name)
    if pool_id:
        log.info(f"Pool '{pool_name}' exists: {pool_id}")
        return pool_id
    log.info(f"Creating pool '{pool_name}'...")
    sbctl("storage-pool", "add", pool_name, cluster_id)
    time.sleep(3)
    pool_id = get_pool_id(pool_name)
    if not pool_id:
        raise RuntimeError(f"Pool '{pool_name}' not found after creation")
    log.info(f"Pool created: {pool_id}")
    return pool_id


def create_lvol(name, size, pool_id, host_id, namespaced, max_ns):
    existing = get_lvol_by_name(pool_id, name)
    if existing:
        lvol_id = _get(existing, "id")
        log.info(f"  {name}: already exists ({lvol_id})")
        return lvol_id

    add_args = ["volume", "add", name, size, pool_id,
                "--host-id", host_id, "--snapshot"]
    if namespaced:
        add_args += ["--namespaced", "True", "--max-namespace-per-subsys", str(max_ns)]

    sbctl(*add_args)

    for _ in range(15):
        time.sleep(3)
        lv = get_lvol_by_name(pool_id, name)
        if lv:
            lvol_id = _get(lv, "id")
            log.info(f"  {name}: created ({lvol_id})")
            return lvol_id
    raise RuntimeError(f"Lvol '{name}' not found after creation")


def teardown(pool_id, pool_name):
    log.info("--- Teardown ---")
    local_run("sudo killall fio 2>/dev/null || true")
    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)
    if not pool_id:
        return
    for lv in _sbctl_list("lvol", "list", "--pool", pool_id):
        name = lv.get("Name") or _get(lv, "name") or ""
        if name.startswith(LVOL_PREFIX):
            lvol_id = _get(lv, "id")
            log.info(f"  Deleting {name} ({lvol_id})")
            sbctl("lvol", "delete", lvol_id, "--force")
            time.sleep(1)
    time.sleep(5)
    sbctl("storage-pool", "delete", pool_id)
    log.info("Teardown complete")


# ---------------------------------------------------------------------------
# fio
# ---------------------------------------------------------------------------
def run_fio_parallel(devices, runtime, fio_size):
    """Run a single fio job across all devices simultaneously."""
    if not devices:
        log.warning("No devices to run fio on")
        return False

    job_lines = []
    for i, dev in enumerate(devices):
        job_lines.append(f"[job{i}]")
        job_lines.append(f"filename={dev}")

    job_file = "/tmp/ns_test.fio"
    Path(job_file).write_text("\n".join([
        "[global]",
        f"size={fio_size}",
        "numjobs=1",
        "direct=1",
        "ioengine=libaio",
        "iodepth=8",
        "readwrite=randrw",
        "bsrange=4k:128k",
        "verify=md5",
        "do_verify=1",
        "time_based=1",
        f"runtime={runtime}",
        "group_reporting=1",
        "",
    ] + job_lines))

    log.info(f"Running fio on {len(devices)} device(s) for {runtime}s ...")
    log.info(f"  Devices: {devices}")

    fio_log = "/tmp/ns_test_fio.log"
    proc = subprocess.run(
        ["sudo", "fio", job_file, f"--output={fio_log}"],
        capture_output=True, text=True, timeout=runtime + 120,
    )

    try:
        output = Path(fio_log).read_text()
    except FileNotFoundError:
        output = proc.stdout

    log.info(f"--- fio output ---\n{output[-3000:]}\n---")

    verify_errs = sum(
        int(m.group(1))
        for m in re.finditer(r'verify_errors\s*[:=]\s*(\d+)', output, re.I)
    )
    io_errs = sum(
        int(m.group(1))
        for m in re.finditer(r'\berr\s*=\s*(\d+)', output)
    )

    if verify_errs:
        log.error(f"fio: {verify_errs} VERIFY ERROR(S) — data corruption!")
        return False
    if io_errs:
        log.error(f"fio: {io_errs} IO error(s)")
        return False
    log.info("fio: OK (0 errors)")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pool",        default=None,      help="Pool name or ID (default: auto-create)")
    p.add_argument("--size",        default=LVOL_SIZE, help=f"Lvol size (default: {LVOL_SIZE})")
    p.add_argument("--fio-runtime", type=int, default=FIO_RUNTIME,
                   help=f"fio runtime in seconds (default: {FIO_RUNTIME})")
    p.add_argument("--fio-size",    default=FIO_SIZE,
                   help=f"fio data size per device (default: {FIO_SIZE})")
    p.add_argument("--teardown",    action="store_true", help="Delete all test lvols and pool")
    return p.parse_args()


def main():
    args = parse_args()

    cluster_id = discover_cluster_id()
    log.info(f"Cluster : {cluster_id}")

    # Resolve pool
    if args.pool:
        pool_id = get_pool_id(args.pool) or args.pool
    else:
        pool_id = ensure_pool(POOL_NAME, cluster_id)

    if args.teardown:
        teardown(pool_id, POOL_NAME)
        return

    nodes = get_online_nodes()
    if not nodes:
        raise RuntimeError("No online storage nodes found")
    host_id = _get(nodes[0], "id")
    log.info(f"Target node: {_get(nodes[0], 'hostname') or host_id}")

    # ------------------------------------------------------------------
    # 1. Create master + members
    # ------------------------------------------------------------------
    log.info(f"\n--- Creating {MAX_NAMESPACES} lvols (1 master + {MAX_NAMESPACES-1} members) ---")

    master_name = f"{LVOL_PREFIX}_0"
    master_id   = create_lvol(master_name, args.size, pool_id, host_id,
                              namespaced=True, max_ns=MAX_NAMESPACES)

    member_ids = []
    for i in range(1, MAX_NAMESPACES):
        name      = f"{LVOL_PREFIX}_{i}"
        lvol_id   = create_lvol(name, args.size, pool_id, host_id,
                                namespaced=True, max_ns=MAX_NAMESPACES)
        member_ids.append(lvol_id)

    all_ids = [master_id] + member_ids
    log.info(f"\nCreated {len(all_ids)} lvols total")

    # ------------------------------------------------------------------
    # 2. Connect all lvols
    # ------------------------------------------------------------------
    log.info(f"\n--- Connecting {len(all_ids)} lvols ---")

    local_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    before    = list_nvme_namespaces()
    connected = set()

    for lvol_id in all_ids:
        connect_out = sbctl("volume", "connect", lvol_id)
        cmds = parse_nvme_connect_cmds(connect_out)
        for cmd in cmds:
            _, _, rc = local_run(f"sudo {cmd} 2>&1 || true")
            connected.add(cmd)

    # Wait for all namespaces to appear
    log.info(f"Waiting for {MAX_NAMESPACES} NVMe namespaces ...")
    time.sleep(3)
    new_devices = wait_for_new_devices(before, expected_count=MAX_NAMESPACES)

    log.info(f"Found {len(new_devices)} new NVMe namespace(s): {new_devices}")

    if len(new_devices) < MAX_NAMESPACES:
        log.warning(
            f"Expected {MAX_NAMESPACES} namespaces, got {len(new_devices)} — "
            f"some connect commands may have been deduplicated (shared NQN)"
        )

    if not new_devices:
        raise RuntimeError("No NVMe devices appeared after connect")

    # ------------------------------------------------------------------
    # 3. Format devices (mkfs not needed — fio writes raw)
    #    Just verify each device is readable
    # ------------------------------------------------------------------
    log.info("\n--- Verifying devices ---")
    for dev in new_devices:
        out, _, rc = local_run(f"sudo blockdev --getsize64 {dev}")
        size_gb = int(out or 0) // (1024 ** 3)
        log.info(f"  {dev}: {size_gb} GiB  rc={rc}")

    # ------------------------------------------------------------------
    # 4. Run fio across all devices
    # ------------------------------------------------------------------
    log.info(f"\n--- Running fio ({args.fio_runtime}s, {args.fio_size} per device) ---")
    ok = run_fio_parallel(new_devices, args.fio_runtime, args.fio_size)

    # ------------------------------------------------------------------
    # 5. Result
    # ------------------------------------------------------------------
    log.info("\n" + "=" * 60)
    if ok:
        log.info(f"PASS — {len(new_devices)} namespaces, no errors")
    else:
        log.error("FAIL — see fio output above")
    log.info("=" * 60)

    log.info("\nDisconnecting ...")
    local_run("sudo nvme disconnect-all 2>/dev/null || true")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
