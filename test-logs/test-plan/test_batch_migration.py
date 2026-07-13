#!/usr/bin/env python3
"""
test_batch_migration.py

End-to-end test for shared-namespace (batch) lvol migration.

Creates a shared-namespace subsystem with N lvols (1 master + N-1 members),
connects them all, runs parallel fio randrw+verify=md5 in the background,
then migrates the entire group to a target node using the two-phase batch API:

  Phase 1 — pre-create target subsystem
    sbctl --dev lvol migrate <any_member_id> <target_node_id> --batch

  Phase 2 — launch worker tasks + orchestrator
    sbctl --dev lvol migrate-continue <group_id> --batch

fio runs continuously through the ANA flip, then is cancelled 30 s after
the group reaches COMPLETED status.

Usage:
  python3 test_batch_migration.py
  python3 test_batch_migration.py --namespaces 4 --target no-overlap
  python3 test_batch_migration.py --target a --snapshots 3
  python3 test_batch_migration.py --teardown
"""

import argparse
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
POOL_NAME      = "batch-mig-pool"
LVOL_PREFIX    = "batch_mig_lvol"
LVOL_SIZE      = "10G"
MAX_NAMESPACES = 4          # default; override with --namespaces (1 master + N-1 members)

FIO_OUTPUT_LOG  = "/root/fio_batch_mig.log"
FIO_PREFILL_LOG = "/root/fio_batch_mig_prefill.log"
FIO_MOUNT       = "/mnt/batch_mig"
FIO_FILE        = f"{FIO_MOUNT}/job1.0.0"
FIO_SIZE        = "1G"

FIO_PREFILL_CMD = [
    "sudo", "fio",
    "--name=job1",
    f"--filename={FIO_FILE}",
    f"--size={FIO_SIZE}",
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

FIO_CMD = [
    "sudo", "fio",
    "--name=job1",
    f"--filename={FIO_FILE}",
    f"--size={FIO_SIZE}",
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

FIO_POST_MIGRATION_WAIT = 30   # seconds after group completes before stopping fio

GROUP_POLL_INTERVAL = 10       # seconds between group status polls
GROUP_TIMEOUT       = 1800     # seconds before giving up on a group

SNAP_PREFIX = "batch_mig_snap"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_DIR  = Path("/tmp/batch_migration_test_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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


def parse_group_id(output):
    m = re.search(r'Migration Group ID:\s*([0-9a-f-]{36})', output, re.IGNORECASE)
    return m.group(1) if m else None


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


# ---------------------------------------------------------------------------
# Local shell helpers
# ---------------------------------------------------------------------------
def run(cmd, check_rc=False, timeout=300):
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
    out, err = proc.stdout.strip(), proc.stderr.strip()
    if check_rc and proc.returncode != 0:
        raise RuntimeError(f"Command failed (rc={proc.returncode}): {cmd}\nstderr: {err}")
    return out, err, proc.returncode


def list_nvme_namespaces():
    out, _, _ = run("ls /dev/nvme*n* 2>/dev/null || true")
    return sorted(d for d in out.splitlines() if d.strip())


def wait_for_new_devices(before, expected_count, retries=20, interval=3):
    for _ in range(retries):
        after = list_nvme_namespaces()
        new   = [d for d in after if d not in before]
        if len(new) >= expected_count:
            return new
        time.sleep(interval)
    after = list_nvme_namespaces()
    return [d for d in after if d not in before]


def wait_for_new_device(before, retries=15, interval=3):
    for attempt in range(retries):
        after = list_nvme_namespaces()
        new   = [d for d in after if d not in before]
        if new:
            return new[0]
        log.debug(f"Waiting for NVMe device (attempt {attempt+1}/{retries})...")
        time.sleep(interval)
    raise RuntimeError("Timed out waiting for new NVMe device")


# ---------------------------------------------------------------------------
# Cluster / pool helpers
# ---------------------------------------------------------------------------
def _get(obj, *keys):
    aliases = {
        "id":   ("id", "ID", "Id", "uuid", "UUID"),
        "name": ("name", "Name", "lvol_name", "hostname", "Hostname"),
    }
    for key in keys:
        for candidate in aliases.get(key.lower(), (key, key.upper(), key.capitalize(), key.lower())):
            if candidate in obj:
                return obj[candidate]
    return None


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


def get_online_nodes(cluster_id):
    nodes = sbctl("storage-node", "list", "--cluster-id", cluster_id,
                  "--json", parse_json=True)
    if isinstance(nodes, dict):
        nodes = nodes.get("results", [])
    nodes = nodes or []
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
                    if isinstance(full, dict):
                        merged = {}
                        merged.update(n)
                        merged.update(full)
                        n = merged
                except Exception:
                    pass
        enriched.append(n)
    return enriched


def _node_rel(node_id, nodes, keys):
    for n in nodes:
        if _get(n, "id") == node_id:
            for key in keys:
                val = n.get(key)
                if val:
                    return val
    return None


def get_node_secondary_id(node_id, nodes):
    return _node_rel(node_id, nodes,
                     ("Secondary node ID", "secondary_node_id", "SecondaryNodeId",
                      "secondary_node", "HA Secondary", "ha_secondary"))


def get_node_tertiary_id(node_id, nodes):
    return _node_rel(node_id, nodes,
                     ("Tertiary node ID", "tertiary_node_id", "TertiaryNodeId",
                      "tertiary_node", "HA Tertiary", "ha_tertiary"))


def pick_source_node(nodes):
    for n in nodes:
        nid = _get(n, "id")
        if not nid:
            continue
        if get_node_secondary_id(nid, nodes) and get_node_tertiary_id(nid, nodes):
            log.info(f"Source: {nid}  sec={get_node_secondary_id(nid, nodes)}  "
                     f"ter={get_node_tertiary_id(nid, nodes)}")
            return nid
    # fallback: any node
    for n in nodes:
        nid = _get(n, "id")
        if nid:
            return nid
    raise RuntimeError("No online nodes found")


def pick_target_node(source_node_id, nodes, target_arg):
    candidates = [n for n in nodes if _get(n, "id") != source_node_id]
    if not candidates:
        raise RuntimeError("No candidate target nodes found")

    secondary_id = get_node_secondary_id(source_node_id, nodes)
    tertiary_id  = get_node_tertiary_id(source_node_id, nodes)

    if target_arg == "no-overlap":
        ha_ids   = {secondary_id, tertiary_id} - {None}
        filtered = [n for n in candidates if _get(n, "id") not in ha_ids]
        chosen   = filtered[0] if filtered else candidates[0]
        log.info(f"Target (no-overlap): {_get(chosen, 'id')}  excluded={ha_ids}")
        return _get(chosen, "id")

    if target_arg == "a":
        if secondary_id:
            for n in candidates:
                if _get(n, "id") == secondary_id:
                    log.info(f"Target (a, TGT-prim=SRC-sec): {secondary_id}")
                    return secondary_id
        return _get(candidates[0], "id")

    if target_arg == "b":
        for n in candidates:
            n_id = _get(n, "id")
            if get_node_secondary_id(n_id, nodes) == source_node_id:
                log.info(f"Target (b, SRC-prim=TGT-sec): {n_id}")
                return n_id
        raise RuntimeError(f"b: no node whose secondary is source ({source_node_id})")

    if target_arg == "c":
        if not tertiary_id:
            raise RuntimeError(f"c: source {source_node_id} has no tertiary")
        for n in candidates:
            if _get(n, "id") == tertiary_id:
                log.info(f"Target (c, TGT-prim=SRC-tertiary): {tertiary_id}")
                return tertiary_id
        raise RuntimeError(f"c: source tertiary {tertiary_id} not in candidates")

    if target_arg == "d":
        for n in candidates:
            n_id = _get(n, "id")
            if get_node_tertiary_id(n_id, nodes) == source_node_id:
                log.info(f"Target (d, SRC-prim=TGT-tertiary): {n_id}")
                return n_id
        raise RuntimeError(f"d: no node whose tertiary is source ({source_node_id})")

    for n in candidates:
        nid      = _get(n, "id") or ""
        hostname = (_get(n, "hostname") or "").lower()
        if nid == target_arg or hostname.startswith(target_arg.lower()):
            log.info(f"Target (explicit '{target_arg}'): {nid}")
            return nid

    raise RuntimeError(
        f"--target '{target_arg}' matched no online node.\n"
        f"Available: {[(_get(n, 'hostname') or '', _get(n, 'id')) for n in candidates]}"
    )


def get_pool_id(name, cluster_id):
    pools = sbctl("storage-pool", "list", "--cluster-id", cluster_id,
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


def get_lvol_by_id(cluster_id, lvol_id):
    d = sbctl("volume", "get", lvol_id, "--json", parse_json=True)
    if isinstance(d, list):
        d = d[0] if d else {}
    return d or {}


def get_lvol_node(lvol_id):
    d = get_lvol_by_id("", lvol_id)
    return _get(d, "node_id", "primary_node_id", "Node ID")


def create_lvol(name, size, pool_id, host_id, namespaced, max_ns):
    existing = get_lvol(pool_id, name)
    if existing:
        lvol_id = _get(existing, "id")
        log.info(f"  {name}: already exists ({lvol_id})")
        return lvol_id

    add_args = ["volume", "add", name, size, pool_id,
                "--host-id", host_id, "--snapshot"]
    if namespaced:
        add_args += ["--namespaced", "True", "--max-namespace-per-subsys", str(max_ns)]

    sbctl(*add_args)

    for _ in range(20):
        time.sleep(3)
        lv = get_lvol(pool_id, name)
        if lv:
            lvol_id = _get(lv, "id")
            log.info(f"  {name}: created ({lvol_id})")
            return lvol_id
    raise RuntimeError(f"Lvol '{name}' not found after creation")


def get_nvme_subsystem_nqn(device):
    dev_name = device.split("/")[-1]
    out, _, _ = run(
        f"subsys=$(readlink -f /sys/class/block/{dev_name} 2>/dev/null "
        f"| grep -oP 'nvme-subsys[0-9]+' | head -1 || true); "
        f"[ -n \"$subsys\" ] && "
        f"cat /sys/class/nvme-subsystem/$subsys/subsysnqn 2>/dev/null || true"
    )
    if out.strip():
        return out.strip()
    ctrl_name = re.sub(r'n\d+$', '', dev_name)
    out2, _, _ = run(f"cat /sys/class/nvme/{ctrl_name}/subsysnqn 2>/dev/null || true")
    return out2.strip()


def log_nvme_state(label=""):
    tag = f" [{label}]" if label else ""
    log.info(f"--- NVMe client state{tag} ---")
    out, _, _ = run("nvme list-subsys 2>/dev/null || true")
    log.info(f"nvme list-subsys:\n{out}")
    out, _, _ = run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  n=$(basename $ns); "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); "
        "  echo \"$n: ana_state=$ana\"; "
        "done 2>/dev/null || true"
    )
    log.info(f"ANA states:\n{out}")
    log.info("--- end NVMe state ---")


# ---------------------------------------------------------------------------
# Group status polling
# ---------------------------------------------------------------------------
def poll_batch_group(group_id):
    """Return (phase, status, error_message) for a batch migration group."""
    raw = sbctl("--dev", "lvol", "migrate-group-list", "--json", parse_json=True)
    if isinstance(raw, dict):
        raw = raw.get("results", raw)
    for g in (raw or []):
        gid = g.get("group_id") or g.get("uuid") or _get(g, "id") or ""
        if gid == group_id:
            return (
                g.get("phase", ""),
                g.get("status", ""),
                g.get("error_message", ""),
            )
    return "", "", ""


def wait_for_batch_group(group_id, timeout=GROUP_TIMEOUT, poll=GROUP_POLL_INTERVAL):
    """Poll until group reaches a terminal phase. Returns (phase, status, error)."""
    terminal_phases = {"completed", "failed", "cancelled", "cleanup_source",
                       "cleanup_target"}
    terminal_statuses = {"done", "failed", "cancelled"}

    deadline = time.time() + timeout
    while time.time() < deadline:
        phase, status, error = poll_batch_group(group_id)
        log.info(f"  group phase={phase!r}  status={status!r}  error={error!r}")

        if status.lower() in terminal_statuses:
            return phase, status, error
        if phase.lower() in terminal_phases and status.lower() in terminal_statuses:
            return phase, status, error

        time.sleep(poll)

    return "timeout", "timeout", ""


# ---------------------------------------------------------------------------
# NVMe controller cleanup
# ---------------------------------------------------------------------------
def _get_nvme_controllers_for_nqn(nqn):
    out, _, _ = run("nvme list-subsys 2>/dev/null || true")
    result, current_nqn = {}, None
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
            ctrl_name  = tokens[1]
            addr_block = next((t for t in tokens if "traddr=" in t), "")
            traddr = trsvcid = ""
            for token in addr_block.split(","):
                if token.startswith("traddr="):
                    traddr = token[7:]
                elif token.startswith("trsvcid="):
                    trsvcid = token[8:]
            result[ctrl_name] = {"traddr": traddr, "trsvcid": trsvcid}
    return result


def cleanup_src_controllers(nqn, tgt_connect_cmds):
    log.info("Cleaning up SRC NVMe controllers ...")
    tgt_endpoints = set()
    for cmd in tgt_connect_cmds:
        traddr = trsvcid = ""
        for token in cmd.split():
            if token.startswith("--traddr="):
                traddr = token[9:]
            elif token.startswith("--trsvcid="):
                trsvcid = token[10:]
        if traddr and trsvcid:
            tgt_endpoints.add((traddr, trsvcid))
    log.info(f"  TGT endpoints to keep: {tgt_endpoints}")

    ctrl_now = _get_nvme_controllers_for_nqn(nqn)
    for ctrl, info in ctrl_now.items():
        ep = (info["traddr"], info["trsvcid"])
        if ep not in tgt_endpoints:
            run(f"sudo nvme disconnect -d /dev/{ctrl} 2>&1 || true")
            log.info(f"  Disconnected SRC controller {ctrl} ({ep[0]}:{ep[1]})")
    time.sleep(2)


# ---------------------------------------------------------------------------
# fio helpers
# ---------------------------------------------------------------------------
def start_fio_background():
    log.info(f"Starting fio: {' '.join(FIO_CMD)}")
    proc = subprocess.Popen(
        FIO_CMD,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    log.info(f"fio PID: {proc.pid}  output: {FIO_OUTPUT_LOG}")
    time.sleep(2)
    if proc.poll() is not None:
        _, err = proc.communicate()
        raise RuntimeError(f"fio exited immediately (rc={proc.returncode}): "
                           f"{(err or b'').decode()[:400]}")
    return proc


def stop_fio(fio_proc, post_wait=FIO_POST_MIGRATION_WAIT):
    log.info(f"Waiting {post_wait}s after migration before stopping fio...")
    time.sleep(post_wait)
    if fio_proc.poll() is not None:
        log.warning(f"fio already exited (rc={fio_proc.returncode})")
        return fio_proc.returncode
    log.info(f"Sending SIGINT to fio process group (pgid={fio_proc.pid})...")
    try:
        os.killpg(fio_proc.pid, signal.SIGINT)   # type: ignore[attr-defined]
    except ProcessLookupError:
        log.warning("fio process group already gone")
    try:
        _, stderr = fio_proc.communicate(timeout=30)
        if stderr:
            log.info(f"fio stderr:\n{stderr.decode()[:1000]}")
    except subprocess.TimeoutExpired:
        log.warning("fio did not exit — sending SIGKILL")
        try:
            os.killpg(fio_proc.pid, 9)            # type: ignore[attr-defined]
        except ProcessLookupError:
            pass
        fio_proc.wait()
    rc = fio_proc.returncode
    log.info(f"fio exit code: {rc}")
    return rc


def check_fio_output(log_path):
    try:
        content = Path(log_path).read_text()
    except FileNotFoundError:
        log.warning(f"fio output log not found: {log_path}")
        return False, -1
    log.info(f"--- fio output ({log_path}) ---\n{content[-3000:]}\n--- end ---")
    verify_errors = sum(
        int(m.group(1))
        for m in re.finditer(r'verify_errors\s*[:=]\s*(\d+)', content, re.IGNORECASE)
    )
    for m in re.finditer(r'\berr\s*=\s*(\d+)', content):
        verify_errors += int(m.group(1))
    ok = verify_errors == 0
    if ok:
        log.info("fio verify: no errors")
    else:
        log.error(f"fio verify: {verify_errors} error(s)")
    return ok, verify_errors


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
def _wait_lvol_deleted(cluster_id, lvol_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        lvols = sbctl("volume", "list", "--cluster-id", cluster_id,
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


def cleanup_previous_run(cluster_id, pool_name, max_ns):
    log.info("--- Cleanup previous run ---")
    run("sudo killall fio 2>/dev/null || true")
    run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)
    run(f"sudo umount {FIO_MOUNT} 2>/dev/null || true")

    # Cancel any active batch migration groups for this pool
    groups = sbctl("--dev", "lvol", "migrate-group-list", "--json", parse_json=True)
    if isinstance(groups, dict):
        groups = groups.get("results", groups)
    for g in (groups or []):
        status = (g.get("status") or "").lower()
        if status not in ("done", "failed", "cancelled"):
            gid = g.get("group_id") or _get(g, "id") or ""
            if gid:
                log.info(f"  Cancelling leftover group {gid}")
                sbctl("--dev", "lvol", "migrate-cancel", gid, "--batch")
                time.sleep(2)

    pool_id = get_pool_id(pool_name, cluster_id)
    if pool_id:
        raw = sbctl("volume", "list", "--pool", pool_id, "--json", parse_json=True)
        if isinstance(raw, dict):
            raw = raw.get("results", [])
        for lv in (raw or []):
            name = _get(lv, "name") or ""
            if name.startswith(LVOL_PREFIX):
                lvol_id = _get(lv, "id")
                if (_get(lv, "status") or "").lower() != "in_deletion":
                    sbctl("volume", "delete", lvol_id, "--force")
                _wait_lvol_deleted(cluster_id, lvol_id)

        for _ in range(10):
            out = sbctl("storage-pool", "delete", pool_id) or ""
            if "not empty" not in out.lower() and "lvols found" not in out.lower():
                break
            time.sleep(5)

    log.info("--- Cleanup complete ---")


# ---------------------------------------------------------------------------
# Args
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--namespaces", type=int, default=MAX_NAMESPACES,
                   help=f"Total namespace count (master + members). Default: {MAX_NAMESPACES}")
    p.add_argument("--size",       default=LVOL_SIZE,
                   help=f"Lvol size. Default: {LVOL_SIZE}")
    p.add_argument("--target",     default="no-overlap", metavar="MODE_OR_NODE",
                   help="'no-overlap' (default), 'a','b','c','d', or node hostname/UUID")
    p.add_argument("--snapshots",  type=int, default=0, metavar="N",
                   help="Snapshots to create on master before migration (default: 0)")
    p.add_argument("--teardown",   action="store_true",
                   help="Cancel any running groups, delete test lvols and pool, then exit")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    n_namespaces = max(2, args.namespaces)   # need at least master + 1 member
    snap_count   = args.snapshots
    target_arg   = args.target

    log.info(f"Log file   : {LOG_FILE}")
    log.info(f"Namespaces : {n_namespaces}  (1 master + {n_namespaces - 1} member(s))")
    log.info(f"Target     : {target_arg}")
    log.info(f"Snapshots  : {snap_count}")

    cluster_id = discover_cluster_id()
    log.info(f"Cluster    : {cluster_id}")

    # -----------------------------------------------------------------------
    step("0. Cleanup previous run")
    # -----------------------------------------------------------------------
    cleanup_previous_run(cluster_id, POOL_NAME, n_namespaces)

    if args.teardown:
        log.info("Teardown complete — exiting.")
        return

    # -----------------------------------------------------------------------
    step("Pre-flight")
    # -----------------------------------------------------------------------
    run("command -v nvme || sudo dnf install -y nvme-cli", check_rc=True)
    run("command -v fio  || sudo dnf install -y fio",      check_rc=True)
    run("sudo modprobe nvme-tcp 2>/dev/null || true")

    # -----------------------------------------------------------------------
    step("1. Create pool and pick source / target nodes")
    # -----------------------------------------------------------------------
    sbctl("storage-pool", "add", POOL_NAME, cluster_id)
    time.sleep(2)
    pool_id = get_pool_id(POOL_NAME, cluster_id)
    if not pool_id:
        raise RuntimeError(f"Pool '{POOL_NAME}' not found after creation")
    log.info(f"Pool: {pool_id}")

    nodes = get_online_nodes(cluster_id)
    if len(nodes) < 2:
        raise RuntimeError(f"Need at least 2 online nodes, got {len(nodes)}")

    source_node = pick_source_node(nodes)
    target_node = pick_target_node(source_node, nodes, target_arg)
    log.info(f"Source: {source_node}")
    log.info(f"Target: {target_node}")

    secondary_id     = get_node_secondary_id(source_node, nodes)
    tertiary_id      = get_node_tertiary_id(source_node, nodes)
    tgt_secondary_id = get_node_secondary_id(target_node, nodes)

    # -----------------------------------------------------------------------
    step(f"2. Create {n_namespaces} shared-namespace lvols")
    # -----------------------------------------------------------------------
    master_name = f"{LVOL_PREFIX}_0"
    master_id   = create_lvol(master_name, args.size, pool_id, source_node,
                              namespaced=True, max_ns=n_namespaces)

    member_ids = []
    for i in range(1, n_namespaces):
        name     = f"{LVOL_PREFIX}_{i}"
        lvol_id  = create_lvol(name, args.size, pool_id, source_node,
                               namespaced=True, max_ns=n_namespaces)
        member_ids.append(lvol_id)

    all_ids = [master_id] + member_ids
    log.info(f"Created {len(all_ids)} lvols: {all_ids}")
    check(len(all_ids) == n_namespaces, f"All {n_namespaces} lvols created")

    # -----------------------------------------------------------------------
    step("3. Connect all lvols, format master, mount")
    # -----------------------------------------------------------------------
    run("sudo nvme disconnect-all 2>/dev/null || true")
    time.sleep(2)

    before       = list_nvme_namespaces()
    tgt_connect_cmds_seen = set()

    for lvol_id in all_ids:
        connect_out = sbctl("volume", "connect", lvol_id)
        cmds = parse_nvme_connect_cmds(connect_out)
        for cmd in cmds:
            if cmd not in tgt_connect_cmds_seen:
                run(f"sudo {cmd} 2>&1 || true")
                tgt_connect_cmds_seen.add(cmd)

    log.info(f"Waiting for {n_namespaces} NVMe namespaces ...")
    time.sleep(3)
    new_devices = wait_for_new_devices(before, expected_count=n_namespaces)
    log.info(f"Found {len(new_devices)} new device(s): {new_devices}")
    check(len(new_devices) >= 1, f"At least 1 NVMe device appeared (got {len(new_devices)})")

    if not new_devices:
        raise RuntimeError("No NVMe devices appeared after connect")

    # Use the first device for fio (master namespace)
    fio_dev = new_devices[0]
    nqn     = get_nvme_subsystem_nqn(fio_dev)
    log.info(f"fio device: {fio_dev}  NQN: {nqn}")

    run(f"sudo mkfs.xfs -f {fio_dev}", check_rc=True)
    run(f"sudo mkdir -p {FIO_MOUNT}", check_rc=True)
    run(f"sudo mount {fio_dev} {FIO_MOUNT}", check_rc=True)
    log.info(f"Mounted {fio_dev} → {FIO_MOUNT}")

    log_nvme_state("after-src-connect")

    # -----------------------------------------------------------------------
    if snap_count > 0:
        step(f"3b. Create {snap_count} snapshot(s) on master before migration")
        for i in range(1, snap_count + 1):
            name = f"{SNAP_PREFIX}{i}"
            sbctl("volume", "create-snapshot", master_id, name)
            time.sleep(2)
            log.info(f"  Snapshot {name} created")

    # -----------------------------------------------------------------------
    step("4a. Pre-fill fio file with md5 checksums (blocking)")
    # -----------------------------------------------------------------------
    log.info(f"Pre-filling {FIO_FILE} ...")
    prefill = subprocess.run(FIO_PREFILL_CMD, capture_output=True, text=True, timeout=3600)
    if prefill.returncode != 0:
        log.warning(f"fio prefill rc={prefill.returncode}: {prefill.stderr.strip()[:400]}")
    check(prefill.returncode == 0, f"fio pre-fill (rc={prefill.returncode})")

    # -----------------------------------------------------------------------
    step("4b. Start fio (randrw + verify=md5) in background")
    # -----------------------------------------------------------------------
    fio_proc = start_fio_background()
    log.info("fio running — will cancel 30s after migration completes")

    # -----------------------------------------------------------------------
    step(f"5. Start batch migration: {n_namespaces} lvols → {target_node}")
    # -----------------------------------------------------------------------
    start_out = sbctl("--dev", "lvol", "migrate", master_id, target_node, "--batch")
    log.info(f"batch-migrate output:\n{start_out}")

    group_id = parse_group_id(start_out)
    if not group_id:
        if fio_proc:
            fio_proc.kill()
        raise RuntimeError("Could not parse Batch Migration Group ID from output")
    log.info(f"Group ID: {group_id}")
    check(bool(group_id), "Got group ID from batch-migrate")

    tgt_cmds = parse_nvme_connect_cmds(start_out)
    log.info(f"Target connect strings ({len(tgt_cmds)}):")
    for cmd in tgt_cmds:
        log.info(f"  {cmd}")

    expected_tgt_paths = 1 + bool(tgt_secondary_id) + bool(get_node_tertiary_id(target_node, nodes))
    check(len(tgt_cmds) == expected_tgt_paths,
          f"Got {expected_tgt_paths} TGT connect strings (got {len(tgt_cmds)})")

    log.info("Connecting target paths (inaccessible ANA state) ...")
    for cmd in tgt_cmds:
        out, _, rc = run(f"sudo {cmd} 2>&1 || true")
        log.info(f"  nvme connect → rc={rc}  {(out or '').strip()[:80]}")
    time.sleep(2)

    log_nvme_state("after-tgt-connect")

    ana_out, _, _ = run(
        "for ns in /sys/class/nvme/nvme[0-9]*/nvme[0-9]*n[0-9]*; do "
        "  ana=$(cat $ns/ana_state 2>/dev/null || echo n/a); echo $ana; "
        "done 2>/dev/null || true"
    )
    inaccessible = sum(1 for l in ana_out.splitlines() if "inaccessible" in l)
    check(inaccessible >= expected_tgt_paths,
          f"At least {expected_tgt_paths} inaccessible path(s) after TGT connect "
          f"(got {inaccessible})")

    # -----------------------------------------------------------------------
    step(f"6. Continue batch migration: batch-migrate-continue {group_id}")
    # -----------------------------------------------------------------------
    sbctl("--dev", "lvol", "migrate-continue", group_id, "--batch")
    log.info("batch-migrate-continue sent")

    # -----------------------------------------------------------------------
    step("7. Wait for batch group to complete  (fio still running)")
    # -----------------------------------------------------------------------
    phase, status, error = wait_for_batch_group(group_id)
    log.info(f"Final group state: phase={phase!r}  status={status!r}  error={error!r}")

    migration_ok = status.lower() in ("done", "completed")
    check(migration_ok, f"Batch migration completed (status={status})")
    if error:
        log.error(f"Group error: {error}")

    # After ANA flip the target controllers become optimized — disconnect source
    if migration_ok:
        cleanup_src_controllers(nqn, tgt_cmds)
        log_nvme_state("post-migration")

    # -----------------------------------------------------------------------
    step(f"8. Cancel fio {FIO_POST_MIGRATION_WAIT}s after migration, collect results")
    # -----------------------------------------------------------------------
    run("sudo dmesg -T > /tmp/dmesg_batch_mig.txt 2>/dev/null || true", timeout=30)
    log.info("dmesg saved to /tmp/dmesg_batch_mig.txt")

    fio_rc = stop_fio(fio_proc)
    fio_proc = None

    fio_ok, fio_errors = check_fio_output(FIO_OUTPUT_LOG)
    check(fio_rc in (0, -2, -9, 128),
          f"fio exit code acceptable (rc={fio_rc})")
    check(fio_ok, f"fio verify: 0 errors (found {fio_errors})")

    # -----------------------------------------------------------------------
    step("9. Verify all lvols are on target node")
    # -----------------------------------------------------------------------
    if migration_ok:
        for lvol_id in all_ids:
            lvol_node = get_lvol_node(lvol_id)
            check(lvol_node == target_node,
                  f"Lvol {lvol_id} on target node {target_node} (got: {lvol_node})")

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    log.info("")
    log.info("=" * 60)
    log.info("TEST SUMMARY")
    log.info("=" * 60)
    log.info(f"  Cluster    : {cluster_id}")
    log.info(f"  Source     : {source_node}  sec={secondary_id or 'none'}  ter={tertiary_id or 'none'}")
    log.info(f"  Target     : {target_node}  (mode: {target_arg})")
    log.info(f"  Namespaces : {n_namespaces}  ({len(all_ids)} lvols)")
    log.info(f"  Snapshots  : {snap_count}")
    log.info(f"  Group ID   : {group_id}")
    log.info(f"  Phase      : {phase}")
    log.info(f"  Status     : {status}")
    log.info(f"  fio errors : {fio_errors}")
    log.info(f"  Log        : {LOG_FILE}")
    log.info("")
    for msg in _passes:
        log.info(f"  PASS : {msg}")
    for msg in _failures:
        log.error(f"  FAIL : {msg}")
    log.info("")
    log.info(f"  Result : {len(_passes)} passed, {len(_failures)} failed")
    log.info("=" * 60)

    sys.exit(1 if _failures else 0)


if __name__ == "__main__":
    main()
