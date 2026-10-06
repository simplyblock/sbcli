#!/usr/bin/env python3
"""
test_lvol_migration.py

Tests the two-phase migration flow on a plain lvol (no snapshots).

  Phase 1 — start
    sbctl --dev lvol migrate <lvol_id> <target_node_id>
  Phase 2 — continue
    sbctl --dev lvol migrate-continue <migration_id>

fio runs continuously in the background from mount through migration cutover.
It is cancelled 30 s after migration completes and its output checked for errors.

  fio: randrw, bs=4k-128k, verify=md5, direct=1, libaio — exercises data
  integrity under active IO during the ANA flip.

Overlap modes:
  no-overlap  (default) — TGT shares no node with SRC's HA pair
  a           (Case A)  — TGT-prim IS SRC-sec
  b           (Case B)  — SRC-prim IS TGT-sec
  c           (Case C)  — TGT-prim IS SRC-tertiary
  d           (Case D)  — SRC-prim IS TGT-tertiary

Usage:
  python3 test_lvol_migration.py [--target no-overlap|a|b|c|d|<hostname>]
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
import logging
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLUSTER_ID  = "260c1a4c-276f-497e-8bb8-da5d2e3a90ce"

POOL_NAME   = "mig-plain-pool"
LVOL_NAME   = "mig_plain_lvol"
LVOL_SIZE   = "10G"

MOUNT_POINT    = "/mnt/mig_plain"
FIO_OUTPUT_LOG = "/root/fio_output.log"

# fio job — runs continuously during migration, cancelled 30 s after done
FIO_FILE      = f"{MOUNT_POINT}/job1.0.0"
FIO_PREFILL_LOG = "/root/fio_prefill.log"

# Pre-fill: sequential write with md5 checksums — run once, blocking, before migration.
FIO_PREFILL_CMD = [
    "sudo", "fio",
    "--name=job1",
    f"--filename={FIO_FILE}",
    "--size=2G",
    "--numjobs=1",
    "--direct=1",
    "--ioengine=libaio",
    "--iodepth=8",
    "--rw=write",
    "--bs=128k",
    "--verify=md5",
    "--do_verify=0",
    "--end_fsync=1",
    f"--output={FIO_PREFILL_LOG}",
]

# Background randrw: file already exists, so fio starts IOs immediately.
FIO_CMD = [
    "sudo", "fio",
    "--name=job1",
    f"--filename={FIO_FILE}",
    "--size=2G",
    "--numjobs=1",
    "--direct=1",
    "--ioengine=libaio",
    "--iodepth=1",
    "--verify=md5",
    "--readwrite=randrw",
    "--bsrange=4k:128k",
    "--time_based",
    "--runtime=7200",
    "--status-interval=10",
    f"--output={FIO_OUTPUT_LOG}",
]
FIO_POST_MIGRATION_WAIT = 30   # seconds to keep fio running after migration

MIGRATION_TIMEOUT = 600
MIGRATION_POLL    = 5

SNAP_PREFIX = "mig_snap"
SNAP_MAX    = 20   # max snapshots ever created; cleanup sweeps up to this many

# Precreate test artifacts to clean up together
_PRECREATE_POOL_NAME  = "mig-precreate-pool"
_PRECREATE_LVOL_NAME  = "mig_precreate_lvol"
_PRECREATE_SNAP_NAMES = ["snap1_precreate", "snap2_precreate"]
_PRECREATE_MOUNT      = "/mnt/mig_precreate"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR  = Path("/tmp/migration_test_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"plain_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

_passes   = []
_failures = []


def step(msg):
    log.info("")
    log.info("=" * 60)
    log.info(f"STEP: {msg}")
    log.info("=" * 60)


def check(passed, msg):
    if passed:
        log.info(f"PASS: {msg}")
        _passes.append(msg)
    else:
        log.error(f"FAIL: {msg}")
        _failures.append(msg)


# ---------------------------------------------------------------------------
# sbctl helpers
# ---------------------------------------------------------------------------
def sbctl(*args, parse_json=False):
    cmd = ["sbctl"] + [str(a) for a in args]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        log.warning(f"sbctl {' '.join(str(a) for a in args[:4])} rc={proc.returncode}: "
                    f"{proc.stderr.strip()[:200]}")
    if not parse_json:
        return proc.stdout.strip()
    for i, ch in enumerate(proc.stdout):
        if ch in ("{", "["):
            try:
                return json.loads(proc.stdout[i:])
            except json.JSONDecodeError:
                pass
    log.warning("Could not parse JSON from sbctl output")
    return None


def parse_migration_id(output):
    m = re.search(r'Migration ID:\s*([0-9a-f-]{36})', output, re.IGNORECASE)
    return m.group(1) if m else None


def parse_nvme_connect_cmds(output):
    cmds = []
    for line in output.splitlines():
        stripped = line.strip()
        if "nvme connect" not in stripped:
            continue
        if stripped.startswith("sudo "):
            stripped = stripped[5:]
        cmds.append(stripped)
    return cmds


# ---------------------------------------------------------------------------
# Local command runner
# ---------------------------------------------------------------------------
def ssh_run(cmd, check_rc=False, timeout=300):
    proc = subprocess.run(
        cmd, shell=True, capture_output=True, text=True, timeout=timeout
    )
    out, err = proc.stdout.strip(), proc.stderr.strip()
    if check_rc and proc.returncode != 0:
        raise RuntimeError(f"Command failed (rc={proc.returncode}): {cmd}\nstderr: {err}")
    return out, err, proc.returncode


# ---------------------------------------------------------------------------
# Device helpers
# ---------------------------------------------------------------------------
def list_nvme_namespaces():
    out, _, _ = ssh_run("ls /dev/nvme*n* 2>/dev/null || true")
    return sorted(d for d in out.splitlines() if d.strip())


def wait_for_new_device(before, retries=15, interval=3):
    for attempt in range(retries):
        after = list_nvme_namespaces()
        new = [d for d in after if d not in before]
        if new:
            log.info(f"New NVMe device detected: {new[0]}")
            return new[0]
        log.debug(f"Waiting for new device (attempt {attempt+1}/{retries})...")
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for new NVMe device")


def get_nvme_subsystem_nqn(device):
    dev_name = device.split("/")[-1]
    out, _, _ = ssh_run(
        f"subsys=$(readlink -f /sys/class/block/{dev_name} 2>/dev/null "
        f"| grep -oP 'nvme-subsys[0-9]+' | head -1 || true); "
        f"[ -n \"$subsys\" ] && "
        f"cat /sys/class/nvme-subsystem/$subsys/subsysnqn 2>/dev/null || true"
    )
    if out.strip():
        return out.strip()
    ctrl_name = re.sub(r'n\d+$', '', dev_name)
    out2, _, _ = ssh_run(
        f"cat /sys/class/nvme/{ctrl_name}/subsysnqn 2>/dev/null || true"
    )
    return out2.strip() if out2.strip() else ""


# ---------------------------------------------------------------------------
# Cluster data helpers
# ---------------------------------------------------------------------------
def _get(obj, *keys):
    for key in keys:
        for candidate in (key, key.upper(), key.capitalize(),
                          key.lower(), "UUID" if key.lower() == "id" else key):
            if candidate in obj:
                return obj[candidate]
    return None


def get_online_nodes():
    nodes = sbctl("storage-node", "list", "--cluster-id", CLUSTER_ID,
                  "--json", parse_json=True)
    if isinstance(nodes, dict):
        nodes = nodes.get("results", [])
    if not nodes:
        raise RuntimeError("No storage nodes returned")
    online = [n for n in nodes
              if (_get(n, "status") or "").lower() in ("online", "active", "online_healthy")]
    nodes = online if online else nodes
    enriched = []
    for n in nodes:
        node_id = _get(n, "id")
        if node_id:
            proc = subprocess.run(["sbctl", "--dev", "sn", "get", str(node_id)],
                                  capture_output=True, text=True)
            if proc.returncode == 0 and proc.stdout.strip():
                try:
                    full = json.loads(proc.stdout.strip())
                    if isinstance(full, dict) and isinstance(n, dict):
                        merged: dict = {}
                        merged.update(n)
                        merged.update(full)
                        n = merged
                except Exception:
                    pass
        enriched.append(n)
    return enriched


def get_node_secondary_id(node_id, nodes):
    _sec_keys = ("Secondary node ID", "secondary_node_id", "SecondaryNodeId",
                 "secondary_node", "HA Secondary", "ha_secondary")
    for n in nodes:
        if _get(n, "id") == node_id:
            for key in _sec_keys:
                val = n.get(key)
                if val:
                    return val
    return None


def get_node_tertiary_id(node_id, nodes):
    _ter_keys = ("Tertiary node ID", "tertiary_node_id", "TertiaryNodeId",
                 "tertiary_node", "HA Tertiary", "ha_tertiary")
    for n in nodes:
        if _get(n, "id") == node_id:
            for key in _ter_keys:
                val = n.get(key)
                if val:
                    return val
    return None


def pick_source_node(nodes):
    """Return the first online node that has both secondary and tertiary configured."""
    for n in nodes:
        nid = _get(n, "id")
        if not nid:
            continue
        sec = get_node_secondary_id(nid, nodes)
        ter = get_node_tertiary_id(nid, nodes)
        if sec and ter:
            log.info(f"Source: {nid}  sec={sec}  ter={ter}")
            return nid
    raise RuntimeError("No online node with both secondary and tertiary configured")


def pick_target_node(source_node_id, nodes, target_arg):
    candidates = [n for n in nodes if _get(n, "id") != source_node_id]
    if not candidates:
        raise RuntimeError("No candidate target nodes found")

    secondary_id = get_node_secondary_id(source_node_id, nodes)
    log.info(f"Source secondary: {secondary_id or 'unknown'}")

    if target_arg == "no-overlap":
        tertiary_id = get_node_tertiary_id(source_node_id, nodes)
        ha_ids = {secondary_id, tertiary_id} - {None}
        non_overlap = [n for n in candidates if _get(n, "id") not in ha_ids]
        chosen = non_overlap[0] if non_overlap else candidates[0]
        log.info(f"Target (no-overlap): {_get(chosen, 'id')}  excluded={ha_ids}")
        return _get(chosen, "id")

    if target_arg == "a":
        if secondary_id:
            for n in candidates:
                if _get(n, "id") == secondary_id:
                    log.info(f"Target (a, TGT-prim=SRC-sec): {secondary_id}")
                    return secondary_id
        chosen = candidates[0]
        log.info(f"Target (a fallback): {_get(chosen, 'id')}")
        return _get(chosen, "id")

    if target_arg == "b":
        for n in candidates:
            n_id  = _get(n, "id")
            n_sec = get_node_secondary_id(n_id, nodes)
            if n_sec == source_node_id:
                log.info(f"Target (b, SRC-prim=TGT-sec): {n_id}")
                return n_id
        raise RuntimeError(
            f"b: no node found whose secondary is source ({source_node_id})")

    if target_arg == "c":
        src_tertiary = get_node_tertiary_id(source_node_id, nodes)
        if not src_tertiary:
            raise RuntimeError(
                f"c: source node {source_node_id} has no tertiary configured")
        for n in candidates:
            if _get(n, "id") == src_tertiary:
                log.info(f"Target (c, TGT-prim=SRC-tertiary): {src_tertiary}")
                return src_tertiary
        raise RuntimeError(
            f"c: source tertiary {src_tertiary} not in candidate list")

    if target_arg == "d":
        for n in candidates:
            n_id  = _get(n, "id")
            n_ter = get_node_tertiary_id(n_id, nodes)
            if n_ter == source_node_id:
                log.info(f"Target (d, SRC-prim=TGT-tertiary): {n_id}")
                return n_id
        raise RuntimeError(
            f"d: no node found whose tertiary is source ({source_node_id})")

    for n in candidates:
        nid      = _get(n, "id") or ""
        hostname = (_get(n, "hostname") or "").lower()
        if nid == target_arg or hostname.startswith(target_arg.lower()):
            log.info(f"Target (explicit '{target_arg}'): {nid}")
            return nid

    raise RuntimeError(
        f"--target '{target_arg}' did not match any online node.\n"
        f"Available: {[(_get(n, 'hostname') or '', _get(n, 'id')) for n in candidates]}"
    )


def get_pool_id(name):
    pools = sbctl("storage-pool", "list", "--cluster-id", CLUSTER_ID,
                  "--json", parse_json=True)
    if isinstance(pools, dict):
        pools = pools.get("results", [])
    for p in (pools or []):
        if _get(p, "name") == name:
            return _get(p, "id")
    return None


def get_lvol(pool_id, name):
    lvols = sbctl("volume", "list", "--pool", pool_id, "--json", parse_json=True)
    if isinstance(lvols, dict):
        lvols = lvols.get("results", [])
    for lv in (lvols or []):
        if _get(lv, "name") == name:
            return lv
    return None


def get_lvol_node(lvol_id):
    d = sbctl("volume", "get", lvol_id, "--json", parse_json=True)
    if isinstance(d, list):
        d = d[0] if d else {}
    return _get(d or {}, "node_id", "primary_node_id", "Node ID")


def get_snapshot(cluster_id, name):
    UUID_RE = re.compile(
        r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    raw = sbctl("snapshot", "list", "--cluster-id", cluster_id) or ""
    for line in raw.splitlines():
        if "|" not in line or line.strip().startswith("+"):
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) < 4:
            continue
        row_id, row_name = parts[0], parts[3]
        if UUID_RE.match(row_id) and row_name == name:
            return {"id": row_id, "UUID": row_id, "name": row_name}
    return None


# ---------------------------------------------------------------------------
# Migration polling
# ---------------------------------------------------------------------------
def wait_for_migration(lvol_id, terminal_only=False):
    log.info(f"Polling migration for lvol {lvol_id}"
             + (" (terminal only)" if terminal_only else "") + "...")
    deadline = time.time() + MIGRATION_TIMEOUT
    while time.time() < deadline:
        migrations = sbctl("--dev", "lvol", "migrate-list",
                           "--cluster-id", CLUSTER_ID, "--json", parse_json=True)
        if isinstance(migrations, dict):
            migrations = migrations.get("results", [])
        for m in (migrations or []):
            mid = (_get(m, "lvol_id") or _get(m, "volume_id") or
                   _get(m, "Lvol ID") or _get(m, "Volume ID") or "")
            if mid == lvol_id:
                status = (_get(m, "status") or "unknown").lower()
                snaps  = _get(m, "Snaps") or _get(m, "snaps") or ""
                log.info(f"  migration status={status}  snaps={snaps}")
                if status in ("done", "completed", "failed", "error", "cancelled"):
                    return status
                if status == "cutover" and not terminal_only:
                    return "cutover"
        time.sleep(MIGRATION_POLL)
    return "timeout"


# ---------------------------------------------------------------------------
# NVMe state helpers
# ---------------------------------------------------------------------------
def collect_full_dmesg():
    ssh_run("sudo dmesg -T > /tmp/dmesg_plain.txt 2>/dev/null || true", timeout=30)
    log.info("dmesg saved to /tmp/dmesg_plain.txt")


def log_client_nvme_state(label=""):
    tag = f" [{label}]" if label else ""
    log.info(f"--- NVMe client state{tag} ---")
    out, _, _ = ssh_run("nvme list-subsys 2>/dev/null || true")
    log.info(f"nvme list-subsys:\n{out}")
    out, _, _ = ssh_run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  n=$(basename $ns); "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); "
        "  echo \"$n: ana_state=$ana\"; "
        "done 2>/dev/null || true"
    )
    log.info(f"ANA states:\n{out}")
    log.info("--- end NVMe state ---")


def _get_nvme_controllers_for_nqn(nqn):
    out, _, _ = ssh_run("nvme list-subsys 2>/dev/null || true")
    result = {}
    current_nqn = None
    for line in out.splitlines():
        stripped = line.strip()
        if "NQN=" in stripped:
            m = re.search(r"NQN=(\S+)", stripped)
            current_nqn = m.group(1).rstrip(",") if m else None
            continue
        if current_nqn == nqn and re.match(r"[\\+|`\-]+-?\s+nvme\d", stripped):
            tokens = stripped.split()
            if len(tokens) < 3:
                continue
            ctrl_name = tokens[1]
            addr_block = next((t for t in tokens if "traddr=" in t), "")
            traddr = trsvcid = ""
            for token in addr_block.split(","):
                if token.startswith("traddr="):
                    traddr = token[7:]
                elif token.startswith("trsvcid="):
                    trsvcid = token[8:]
            result[ctrl_name] = {"traddr": traddr, "trsvcid": trsvcid}
    return result


def cleanup_src_controllers(lvol_id, nqn):
    log.info("Cleaning up SRC controllers...")
    connect_out = sbctl("volume", "connect", lvol_id) or ""
    tgt_endpoints = set()
    for cmd in parse_nvme_connect_cmds(connect_out):
        traddr = trsvcid = ""
        for token in cmd.split():
            if token.startswith("--traddr="):
                traddr = token[9:]
            elif token.startswith("--trsvcid="):
                trsvcid = token[10:]
        if traddr and trsvcid:
            tgt_endpoints.add((traddr, trsvcid))
    log.info(f"TGT endpoints: {tgt_endpoints}")

    ctrl_now = _get_nvme_controllers_for_nqn(nqn)
    for ctrl, info in ctrl_now.items():
        ep = (info["traddr"], info["trsvcid"])
        if ep not in tgt_endpoints:
            ssh_run(f"sudo nvme disconnect -d /dev/{ctrl} 2>&1 || true")
            log.info(f"Disconnected SRC controller {ctrl} ({ep[0]}:{ep[1]})")

    time.sleep(2)
    log_client_nvme_state("post-cutover")


# ---------------------------------------------------------------------------
# fio background helpers
# ---------------------------------------------------------------------------
def start_fio_background():
    """Launch fio in a new process group so the whole group can be signalled."""
    log.info(f"Starting fio in background: {' '.join(FIO_CMD)}")
    proc = subprocess.Popen(
        FIO_CMD,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,   # own process group — lets us killpg sudo+fio together
    )
    log.info(f"fio PID: {proc.pid}  output: {FIO_OUTPUT_LOG}")
    time.sleep(2)
    if proc.poll() is not None:
        _, err = proc.communicate()
        raise RuntimeError(f"fio exited immediately (rc={proc.returncode}): "
                           f"{(err or b'').decode()[:400]}")
    return proc


def stop_fio(fio_proc, post_wait=FIO_POST_MIGRATION_WAIT):
    """Wait post_wait seconds, SIGINT the whole fio process group, collect results."""
    log.info(f"Waiting {post_wait}s after migration before stopping fio...")
    time.sleep(post_wait)

    if fio_proc.poll() is not None:
        log.warning(f"fio already exited with rc={fio_proc.returncode} before cancel")
        return fio_proc.returncode

    log.info(f"Sending SIGINT to fio process group (pgid={fio_proc.pid})...")
    try:
        os.killpg(fio_proc.pid, signal.SIGINT)  # type: ignore[attr-defined]
    except ProcessLookupError:
        log.warning("fio process group already gone")

    try:
        _, stderr = fio_proc.communicate(timeout=30)
        if stderr:
            log.info(f"fio stderr:\n{stderr.decode()[:1000]}")
    except subprocess.TimeoutExpired:
        log.warning("fio did not exit after SIGINT — sending SIGKILL")
        try:
            os.killpg(fio_proc.pid, 9)  # type: ignore[attr-defined]
        except ProcessLookupError:
            pass
        fio_proc.wait()

    rc = fio_proc.returncode
    log.info(f"fio exit code: {rc}")
    return rc


def check_fio_output(log_path):
    """Read fio output log and check for verify errors. Returns (ok, error_count)."""
    try:
        with open(log_path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        log.warning(f"fio output log not found: {log_path}")
        return False, -1

    log.info(f"--- fio output ({log_path}) ---\n{content[-3000:]}\n--- end fio output ---")

    # fio reports verify_errors and total errors per job
    verify_errors = 0
    for m in re.finditer(r'verify_errors\s*[:=]\s*(\d+)', content, re.IGNORECASE):
        verify_errors += int(m.group(1))
    for m in re.finditer(r'err\s*=\s*(\d+)', content):
        verify_errors += int(m.group(1))

    ok = verify_errors == 0
    if ok:
        log.info("fio verify: no errors found")
    else:
        log.error(f"fio verify: {verify_errors} error(s) found")
    return ok, verify_errors


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def _wait_lvol_deleted(lvol_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        lvols = sbctl("volume", "list", "--cluster-id", CLUSTER_ID,
                      "--json", parse_json=True)
        if isinstance(lvols, dict):
            lvols = lvols.get("results", [])
        matching = [lv for lv in (lvols or []) if _get(lv, "id") == lvol_id]
        if not matching:
            return True
        status = (_get(matching[0], "status") or "").lower()
        if status not in ("online", "in_deletion", "deleting"):
            return True
        time.sleep(3)
    sbctl("volume", "delete", lvol_id, "--force")
    time.sleep(5)
    return False


def _cleanup_pool_and_lvols(pool_name, lvol_names, snap_names=None, mount=None):
    if mount:
        ssh_run(f"sudo umount {mount} 2>/dev/null || true")
        ssh_run(f"sudo rm -rf {mount} 2>/dev/null || true")

    for snap_name in (snap_names or []):
        snap = get_snapshot(CLUSTER_ID, snap_name)
        if snap:
            sbctl("snapshot", "delete", _get(snap, "id"), "--force")
            time.sleep(2)

    pool_id = get_pool_id(pool_name)
    if pool_id:
        raw = sbctl("volume", "list", "--pool", pool_id, "--json", parse_json=True)
        if isinstance(raw, dict):
            raw = raw.get("results", [])
        for lv in (raw or []):
            if not lvol_names or _get(lv, "name") in lvol_names:
                lvol_id = _get(lv, "id")
                if (_get(lv, "status") or "").lower() != "in_deletion":
                    sbctl("volume", "delete", lvol_id, "--force")
                _wait_lvol_deleted(lvol_id)

        for _ in range(10):
            out = sbctl("storage-pool", "delete", pool_id) or ""
            if "not empty" not in out.lower() and "lvols found" not in out.lower():
                break
            time.sleep(5)
        time.sleep(2)


def cleanup_previous_run():
    log.info("--- Cleaning up previous run (own + precreate) ---")
    ssh_run("sudo killall fio 2>/dev/null || true")
    ssh_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    all_snap_names = [f"{SNAP_PREFIX}{i}" for i in range(1, SNAP_MAX + 1)]
    _cleanup_pool_and_lvols(POOL_NAME, [LVOL_NAME],
                            snap_names=all_snap_names, mount=MOUNT_POINT)
    _cleanup_pool_and_lvols(_PRECREATE_POOL_NAME, [_PRECREATE_LVOL_NAME],
                            snap_names=_PRECREATE_SNAP_NAMES, mount=_PRECREATE_MOUNT)

    log.info("--- Cleanup complete ---")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="E2E lvol migration test (fio randrw verify).")
    p.add_argument(
        "--target", default="no-overlap",
        metavar="MODE_OR_NODE",
        help="'no-overlap' (default), 'a' (Case A), 'b' (Case B), "
             "'c' (Case C), 'd' (Case D), or hostname prefix/UUID",
    )
    p.add_argument(
        "--crypto", action="store_true", default=False,
        help="Create the lvol with inline encryption (--encrypt).",
    )
    p.add_argument(
        "--snapshots", type=int, default=0, metavar="N",
        help="Number of snapshots to create before migration (0–20, default: 0).",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    target_arg    = args.target
    use_crypto    = args.crypto
    snap_count = min(args.snapshots, SNAP_MAX)

    log.info(f"Log file  : {LOG_FILE}")
    log.info(f"Cluster   : {CLUSTER_ID}")
    log.info("Client    : local (mgmt)")
    log.info(f"Target    : {target_arg}")
    log.info(f"Crypto    : {use_crypto}")
    log.info(f"Snapshots : {snap_count}")
    log.info(f"fio cmd   : {' '.join(FIO_CMD)}")

    fio_proc = None

    # -----------------------------------------------------------------------
    step("0. Cleanup previous run")
    # -----------------------------------------------------------------------
    cleanup_previous_run()

    # -----------------------------------------------------------------------
    step("Pre-flight: verify tools on client")
    # -----------------------------------------------------------------------
    ssh_run("command -v nvme || sudo dnf install -y nvme-cli", check_rc=True)
    ssh_run("command -v fio  || sudo dnf install -y fio",      check_rc=True)
    ssh_run("sudo modprobe nvme-tcp 2>/dev/null || true")
    log.info("Pre-flight OK")

    # -----------------------------------------------------------------------
    step("1. Create pool and pick source/target nodes")
    # -----------------------------------------------------------------------
    out = sbctl("storage-pool", "add", POOL_NAME, CLUSTER_ID)
    log.info(f"storage-pool add output: {out!r}")
    time.sleep(2)
    pool_id = get_pool_id(POOL_NAME)
    if not pool_id:
        raise RuntimeError(f"Pool '{POOL_NAME}' not found after creation")
    log.info(f"Pool: {pool_id}")

    nodes = get_online_nodes()
    if len(nodes) < 2:
        raise RuntimeError(f"Need at least 2 online nodes, got {len(nodes)}")

    source_node = pick_source_node(nodes)

    target_node = pick_target_node(source_node, nodes, target_arg)
    log.info(f"Source: {source_node}")
    log.info(f"Target: {target_node}")

    secondary_id     = get_node_secondary_id(source_node, nodes)
    tertiary_id      = get_node_tertiary_id(source_node, nodes)
    tgt_secondary_id = get_node_secondary_id(target_node, nodes)
    tgt_tertiary_id  = get_node_tertiary_id(target_node, nodes)

    target_is_secondary        = (secondary_id is not None and target_node == secondary_id)
    source_is_target_secondary = (tgt_secondary_id is not None and tgt_secondary_id == source_node)
    target_is_tertiary         = (tertiary_id is not None and target_node == tertiary_id)
    source_is_target_tertiary  = (tgt_tertiary_id is not None and tgt_tertiary_id == source_node)

    log.info(f"Source secondary: {secondary_id or 'none'}  tertiary: {tertiary_id or 'none'}")
    log.info(f"Target secondary: {tgt_secondary_id or 'none'}  tertiary: {tgt_tertiary_id or 'none'}")
    log.info(f"Overlap: target_is_secondary={target_is_secondary}  source_is_target_secondary={source_is_target_secondary}")
    log.info(f"Overlap: target_is_tertiary={target_is_tertiary}  source_is_target_tertiary={source_is_target_tertiary}")

    # -----------------------------------------------------------------------
    step("2. Create lvol on source node")
    # -----------------------------------------------------------------------
    add_args = ["volume", "add", LVOL_NAME, LVOL_SIZE, pool_id, "--host-id", source_node]
    if use_crypto:
        add_args.append("--encrypt")
        log.info("Creating encrypted lvol (--encrypt)")
    sbctl(*add_args)
    lvol = None
    for _ in range(10):
        time.sleep(3)
        lvol = get_lvol(pool_id, LVOL_NAME)
        if lvol:
            break
    if not lvol:
        raise RuntimeError("Could not retrieve lvol after creation")
    lvol_id = _get(lvol, "id")
    log.info(f"LVOL: {LVOL_NAME} → {lvol_id}")

    # -----------------------------------------------------------------------
    step("3. Connect lvol, format xfs, mount")
    # -----------------------------------------------------------------------
    ssh_run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    before = list_nvme_namespaces()
    connect_out = sbctl("volume", "connect", lvol_id)
    src_cmds = parse_nvme_connect_cmds(connect_out)
    if not src_cmds:
        raise RuntimeError("Could not parse source nvme connect commands")
    for cmd in src_cmds:
        ssh_run(f"sudo {cmd}", check_rc=True)
    dev = wait_for_new_device(before)
    nqn = get_nvme_subsystem_nqn(dev)
    log.info(f"Device: {dev}  NQN: {nqn}")

    ssh_run(f"sudo mkfs.xfs -f {dev}", check_rc=True)
    ssh_run(f"sudo mkdir -p {MOUNT_POINT}", check_rc=True)
    ssh_run(f"sudo mount {dev} {MOUNT_POINT}", check_rc=True)
    log.info(f"Mounted {dev} → {MOUNT_POINT}")

    log_client_nvme_state("after-source-connect")

    # -----------------------------------------------------------------------
    step("4a. Pre-fill fio file with md5 checksums (blocking)")
    # -----------------------------------------------------------------------
    log.info(f"Pre-filling {FIO_FILE} (9G sequential write + verify headers)...")
    log.info(f"Command: {' '.join(FIO_PREFILL_CMD)}")
    prefill = subprocess.run(FIO_PREFILL_CMD, capture_output=True, text=True, timeout=3600)
    if prefill.returncode != 0:
        log.warning(f"fio prefill rc={prefill.returncode}: {prefill.stderr.strip()[:400]}")
    check(prefill.returncode == 0, f"fio pre-fill completed (rc={prefill.returncode})")
    log.info("Pre-fill done — fio file ready for randrw verify")

    # -----------------------------------------------------------------------
    step("4b. Start fio (randrw + verify=md5) in background")
    # -----------------------------------------------------------------------
    fio_proc = start_fio_background()
    log.info("fio running — will cancel 30 s after migration completes")

    # -----------------------------------------------------------------------
    snap_ids = []
    if snap_count > 0:
        step(f"4c. Create {snap_count} snapshot(s) before migration")
        for i in range(1, snap_count + 1):
            name = f"{SNAP_PREFIX}{i}"
            sbctl("volume", "create-snapshot", lvol_id, name)
            time.sleep(2)
            snap = get_snapshot(CLUSTER_ID, name)
            sid = _get(snap, "id") if snap else None
            snap_ids.append(sid)
            check(bool(sid), f"Snapshot {name} created")
        log.info(f"Snapshots created: {list(zip([f'{SNAP_PREFIX}{i}' for i in range(1, snap_count+1)], snap_ids))}")

    # -----------------------------------------------------------------------
    step(f"5. Start migration: {LVOL_NAME} → {target_node}  (fio randrw running)")
    # -----------------------------------------------------------------------
    start_out = sbctl("--dev", "lvol", "migrate", lvol_id, target_node)
    log.info(f"migrate output:\n{start_out}")

    migration_id = parse_migration_id(start_out)
    if not migration_id:
        if fio_proc:
            fio_proc.kill()
        raise RuntimeError("Could not parse Migration ID from migrate output")
    log.info(f"Migration ID: {migration_id}")
    check(bool(migration_id), "Got migration ID from start")

    tgt_cmds = parse_nvme_connect_cmds(start_out)
    log.info(f"Target connect strings ({len(tgt_cmds)}):")
    for cmd in tgt_cmds:
        log.info(f"  {cmd}")

    expected_tgt_paths = 1 + bool(tgt_secondary_id) + bool(tgt_tertiary_id)
    check(len(tgt_cmds) == expected_tgt_paths,
          f"Got {expected_tgt_paths} TGT connect strings (got {len(tgt_cmds)})")

    log.info("Connecting target paths (inaccessible ANA state)...")
    for cmd in tgt_cmds:
        out, _, rc = ssh_run(f"sudo {cmd} 2>&1 || true")
        log.info(f"  nvme connect → rc={rc}  {(out or '').strip()[:80]}")
    time.sleep(2)

    log_client_nvme_state("after-start-connect")

    ana_out, _, _ = ssh_run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); "
        "  echo $ana; "
        "done 2>/dev/null || true"
    )
    inaccessible_count = sum(1 for l in ana_out.splitlines() if "inaccessible" in l)
    check(inaccessible_count == expected_tgt_paths,
          f"Exactly {expected_tgt_paths} inaccessible paths after start connect "
          f"(got {inaccessible_count})")

    # -----------------------------------------------------------------------
    step(f"6. Continue migration: migrate-continue {migration_id}")
    # -----------------------------------------------------------------------
    migration_start_ts = datetime.now().strftime('%H:%M:%S')
    sbctl("--dev", "lvol", "migrate-continue", migration_id)
    log.info(f"Migration continue sent at {migration_start_ts}")

    # -----------------------------------------------------------------------
    step("7. Wait for cutover / done  (fio still running)")
    # -----------------------------------------------------------------------
    migration_status = wait_for_migration(lvol_id)

    if migration_status == "cutover":
        log.info("STATUS_CUTOVER — ANA states flipped by task runner")
        log_client_nvme_state("at-cutover")
        cleanup_src_controllers(lvol_id, nqn)
        migration_status = wait_for_migration(lvol_id, terminal_only=True)
    else:
        log_client_nvme_state("post-migration")
        cleanup_src_controllers(lvol_id, nqn)

    migration_ok = migration_status in ("done", "completed")
    check(migration_ok, f"Migration completed (status={migration_status})")

    # -----------------------------------------------------------------------
    step(f"8. Cancel fio {FIO_POST_MIGRATION_WAIT}s after migration, collect results")
    # -----------------------------------------------------------------------
    collect_full_dmesg()

    fio_rc = stop_fio(fio_proc)
    fio_proc = None

    fio_ok, fio_errors = check_fio_output(FIO_OUTPUT_LOG)
    check(fio_rc in (0, -2, -9, 128),  # 0=clean, -2/128=SIGINT (fio exits 128), -9=SIGKILL
          f"fio exit code acceptable (rc={fio_rc})")
    check(fio_ok,
          f"fio verify: 0 errors (found {fio_errors})")

    if migration_ok:
        lvol_node = get_lvol_node(lvol_id)
        check(lvol_node == target_node,
              f"lvol on target node {target_node} (got: {lvol_node})")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    log.info("")
    log.info("=" * 60)
    log.info("TEST SUMMARY")
    log.info("=" * 60)
    log.info(f"  Cluster      : {CLUSTER_ID}")
    log.info(f"  Source       : {source_node}")
    if target_is_secondary:
        overlap_label = "Case A overlap (TGT-prim=SRC-sec)"
    elif source_is_target_secondary:
        overlap_label = "Case B overlap (SRC-prim=TGT-sec)"
    elif target_is_tertiary:
        overlap_label = "Case C overlap (TGT-prim=SRC-tertiary)"
    elif source_is_target_tertiary:
        overlap_label = "Case D overlap (SRC-prim=TGT-tertiary)"
    else:
        overlap_label = "non-overlap"
    log.info(f"  Target       : {target_node}  (mode: {target_arg}  {overlap_label})")
    log.info(f"  Src topology : sec={secondary_id or 'none'}  ter={tertiary_id or 'none'}")
    log.info(f"  Tgt topology : sec={tgt_secondary_id or 'none'}  ter={tgt_tertiary_id or 'none'}")
    log.info(f"  LVOL         : {LVOL_NAME} ({lvol_id})  crypto={use_crypto}")
    if snap_count > 0:
        log.info(f"  Snapshots    : {snap_count}  ids={snap_ids}")
    log.info(f"  Migration ID : {migration_id}")
    log.info(f"  Status       : {migration_status}")
    log.info(f"  fio output   : {FIO_OUTPUT_LOG}")
    log.info(f"  fio errors   : {fio_errors}")
    log.info("")
    for msg in _passes:
        log.info(f"  PASS : {msg}")
    for msg in _failures:
        log.error(f"  FAIL : {msg}")
    log.info("")
    log.info(f"  Result : {len(_passes)} passed, {len(_failures)} failed")
    log.info(f"  Log    : {LOG_FILE}")
    log.info("=" * 60)

    sys.exit(1 if _failures else 0)


if __name__ == "__main__":
    main()
